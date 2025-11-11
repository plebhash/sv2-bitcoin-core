use crate::BitcoinCoreSv2;

use crate::error::BitcoinCoreSv2Error;
use std::sync::atomic::Ordering;
use stratum_core::{
    parsers_sv2::TemplateDistribution,
    template_distribution_sv2::{
        CoinbaseOutputConstraints, RequestTransactionData, RequestTransactionDataError,
        SubmitSolution,
    },
};
use tokio_util::sync::CancellationToken;

impl BitcoinCoreSv2 {
    pub async fn handle_coinbase_output_constraints(
        &mut self,
        coinbase_output_constraints: CoinbaseOutputConstraints,
    ) -> Result<(), BitcoinCoreSv2Error> {
        tracing::debug!("handle_coinbase_output_constraints() called");
        tracing::debug!("Cancelling template_ipc_client_cancellation_token");
        self.template_ipc_client_cancellation_token.cancel();

        tracing::debug!("Creating new template IPC client with new constraints");
        let template_ipc_client = match self
            .new_template_ipc_client(
                coinbase_output_constraints.coinbase_output_max_additional_size,
                coinbase_output_constraints.coinbase_output_max_additional_sigops,
            )
            .await
        {
            Ok(new_template_ipc_client) => new_template_ipc_client,
            Err(e) => {
                tracing::error!("Failed to create new template IPC client: {:?}", e);
                return Err(e);
            }
        };

        let mut current_template_ipc_client_guard = self.current_template_ipc_client.borrow_mut();
        *current_template_ipc_client_guard = Some(template_ipc_client);
        tracing::debug!("Updated current_template_ipc_client");

        self.template_ipc_client_cancellation_token = CancellationToken::new();
        tracing::debug!("Created new template_ipc_client_cancellation_token");

        // todo: remove this once https://github.com/bitcoin/bitcoin/issues/33575 is implemented
        self.coinbase_output_constraints_counter
            .fetch_add(1, Ordering::SeqCst);
        let new_count = self
            .coinbase_output_constraints_counter
            .load(Ordering::SeqCst);
        tracing::debug!(
            "coinbase_output_constraints_counter incremented to: {}",
            new_count
        );

        tracing::debug!("Spawning new monitor_ipc_templates() task");
        self.monitor_ipc_templates();

        Ok(())
    }

    pub async fn handle_request_transaction_data(
        &self,
        request_transaction_data: RequestTransactionData,
    ) -> Result<(), BitcoinCoreSv2Error> {
        tracing::debug!(
            "handle_request_transaction_data() called for template_id: {}",
            request_transaction_data.template_id
        );

        let is_stale = {
            let stale_template_ids_guard = match self.stale_template_ids.read() {
                Ok(guard) => guard,
                Err(e) => {
                    tracing::error!("Failed to acquire read lock on stale_template_ids: {:?}", e);
                    return Err(
                        BitcoinCoreSv2Error::FailedToSendRequestTransactionDataResponseMessage,
                    );
                }
            };
            stale_template_ids_guard.contains(&request_transaction_data.template_id)
        };
        if is_stale {
            tracing::debug!(
                "Template {} is stale, sending error response",
                request_transaction_data.template_id
            );
            let request_transaction_data_error = RequestTransactionDataError {
                template_id: request_transaction_data.template_id,
                error_code: "stale-template-id"
                    .to_string()
                    .try_into()
                    .expect("error code must be valid string"),
            };

            if let Err(e) = self
                .outgoing_messages
                .send(TemplateDistribution::RequestTransactionDataError(
                    request_transaction_data_error.clone(),
                ))
                .await
            {
                tracing::error!(
                    "Failed to send RequestTransactionDataError message: {:?}",
                    e
                );
                return Err(BitcoinCoreSv2Error::FailedToSendRequestTransactionDataResponseMessage);
            }

            return Ok(());
        }

        let response_message = {
            let template_data_guard = match self.template_data.read() {
                Ok(guard) => guard,
                Err(e) => {
                    tracing::error!("Failed to acquire read lock on template_data: {:?}", e);
                    return Err(
                        BitcoinCoreSv2Error::FailedToSendRequestTransactionDataResponseMessage,
                    );
                }
            };
            match template_data_guard.get(&request_transaction_data.template_id) {
                Some(template_data) => {
                    tracing::debug!(
                        "Template {} found, sending success response",
                        request_transaction_data.template_id
                    );
                    TemplateDistribution::RequestTransactionDataSuccess(
                        template_data.get_request_transaction_data_success_message(),
                    )
                }
                None => {
                    tracing::debug!(
                        "Template {} not found, sending error response",
                        request_transaction_data.template_id
                    );
                    TemplateDistribution::RequestTransactionDataError(RequestTransactionDataError {
                        template_id: request_transaction_data.template_id,
                        error_code: "template-id-not-found"
                            .to_string()
                            .try_into()
                            .expect("error code must be valid string"),
                    })
                }
            }
        };

        if let Err(e) = self.outgoing_messages.send(response_message.clone()).await {
            tracing::error!("Failed to send message: {:?}", e);
            return Err(BitcoinCoreSv2Error::FailedToSendRequestTransactionDataResponseMessage);
        }

        Ok(())
    }

    pub async fn handle_submit_solution(
        &self,
        submit_solution: SubmitSolution<'static>,
    ) -> Result<(), BitcoinCoreSv2Error> {
        tracing::debug!(
            "handle_submit_solution() called for template_id: {}",
            submit_solution.template_id
        );
        let template_data = {
            let template_data_guard = match self.template_data.read() {
                Ok(guard) => guard,
                Err(e) => {
                    tracing::error!("Failed to acquire read lock on template_data: {:?}", e);
                    return Err(BitcoinCoreSv2Error::TemplateNotFound);
                }
            };

            let Some(template_data) = template_data_guard.get(&submit_solution.template_id) else {
                tracing::error!(
                    "Template data not found for template id: {}",
                    submit_solution.template_id
                );
                tracing::debug!(
                    "Available template IDs: {:?}",
                    template_data_guard.keys().collect::<Vec<_>>()
                );
                return Err(BitcoinCoreSv2Error::TemplateNotFound);
            };
            template_data.clone()
        };
        tracing::debug!("Found template data for solution submission");

        tracing::debug!("Submitting solution to Bitcoin Core");
        let Err(e) = template_data
            .submit_solution(submit_solution, self.thread_ipc_client.clone())
            .await
        else {
            tracing::debug!("Solution submitted successfully");
            return Ok(());
        };
        tracing::error!("Failed to submit solution: {:?}", e);
        Err(BitcoinCoreSv2Error::FailedToSubmitSolution)
    }
}
