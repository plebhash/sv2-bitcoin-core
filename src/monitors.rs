use crate::BitcoinCoreSv2;

use std::collections::HashSet;
use stratum_core::parsers_sv2::TemplateDistribution;
use tracing::info;

impl BitcoinCoreSv2 {
    pub fn monitor_ipc_templates(&self) {
        let self_clone = self.clone();

        tokio::task::spawn_local(async move {
            tracing::debug!("monitor_ipc_templates() task started");
            // a dedicated thread_ipc_client is used for waitNext requests
            // this is because waitNext requests are blocking, and we don't want to block the main
            // thread where other requests are handled
            //
            // as soon as this task is cancelled, the blocking_thread_ipc_client is dropped,
            // which cleans up the thread on the Bitcoin Core side
            tracing::debug!("Creating dedicated blocking_thread_ipc_client for waitNext requests");
            let blocking_thread_ipc_client = {
                let blocking_thread_ipc_client_request =
                    self_clone.thread_map.make_thread_request();
                let blocking_thread_ipc_client_response =
                    match blocking_thread_ipc_client_request.send().promise.await {
                        Ok(thread_ipc_client) => thread_ipc_client,
                        Err(e) => {
                            tracing::error!("Failed to make thread request: {}", e);
                            tracing::warn!("Terminating Sv2 Bitcoin Core IPC Connection");
                            self_clone.global_cancellation_token.cancel();
                            return;
                        }
                    };

                let blocking_thread_ipc_client_result =
                    match blocking_thread_ipc_client_response.get() {
                        Ok(thread_ipc_client_result) => thread_ipc_client_result,
                        Err(e) => {
                            tracing::error!("Failed to get thread IPC client: {}", e);
                            tracing::warn!("Terminating Sv2 Bitcoin Core IPC Connection");
                            self_clone.global_cancellation_token.cancel();
                            return;
                        }
                    };

                match blocking_thread_ipc_client_result.get_result() {
                    Ok(thread_ipc_client) => {
                        tracing::debug!("blocking_thread_ipc_client successfully created");
                        thread_ipc_client
                    }
                    Err(e) => {
                        tracing::error!("Failed to get thread IPC client: {}", e);
                        tracing::warn!("Terminating Sv2 Bitcoin Core IPC Connection");
                        self_clone.global_cancellation_token.cancel();
                        return;
                    }
                }
            };

            tracing::debug!("monitor_ipc_templates() entering main loop");
            loop {
                tracing::debug!("monitor_ipc_templates() loop iteration start");
                let template_ipc_client =
                    match self_clone.current_template_ipc_client.borrow().clone() {
                        Some(template_ipc_client) => template_ipc_client,
                        None => {
                            tracing::error!("Template IPC client not found");
                            tracing::warn!("Terminating Sv2 Bitcoin Core IPC Connection");
                            self_clone.global_cancellation_token.cancel();
                            return;
                        }
                    };

                // Create a new request for each iteration
                let mut wait_next_request = template_ipc_client.wait_next_request();

                match wait_next_request.get().get_context() {
                    Ok(mut context) => context.set_thread(blocking_thread_ipc_client.clone()),
                    Err(e) => {
                        tracing::error!("Failed to set thread: {}", e);
                        tracing::warn!("Terminating Sv2 Bitcoin Core IPC Connection");
                        self_clone.global_cancellation_token.cancel();
                        return;
                    }
                }

                let mut wait_next_request_options = match wait_next_request.get().get_options() {
                    Ok(options) => options,
                    Err(e) => {
                        tracing::error!("Failed to get waitNext request options: {}", e);
                        tracing::warn!("Terminating Sv2 Bitcoin Core IPC Connection");
                        self_clone.global_cancellation_token.cancel();
                        return;
                    }
                };

                wait_next_request_options.set_fee_threshold(self_clone.fee_threshold as i64);

                // 10 seconds timeout for waitNext requests
                // please note that this is NOT how often we expect to get new templates
                // it's just the max time we'll wait for the current waitNext request to complete
                wait_next_request_options.set_timeout(10_000.0);

                tokio::select! {
                    _ = self_clone.global_cancellation_token.cancelled() => {
                        let interrupt_wait_request = template_ipc_client.interrupt_wait_request();
                        let _interrupt_wait_request_response = interrupt_wait_request.send().promise.await;
                        tracing::debug!("Interrupted waitNext request");
                        
                        tracing::warn!("Exiting mempool change monitoring loop");
                        break;
                    }
                    _ = self_clone.template_ipc_client_cancellation_token.cancelled() => {
                        let interrupt_wait_request = template_ipc_client.interrupt_wait_request();
                        let _interrupt_wait_request_response = interrupt_wait_request.send().promise.await;
                        tracing::debug!("Interrupted waitNext request");

                        tracing::debug!("Exiting mempool change monitoring loop");
                        break;
                    }
                    wait_next_request_response = wait_next_request.send().promise => {
                        tracing::debug!("waitNext request completed");
                        match wait_next_request_response {
                            Ok(response) => {
                                let result = match response.get() {
                                    Ok(result) => result,
                                    Err(e) => {
                                        tracing::error!("Failed to get response: {}", e);
                                        tracing::warn!("Terminating Sv2 Bitcoin Core IPC Connection");
                                        self_clone.global_cancellation_token.cancel();
                                        break;
                                    }
                                };

                                let new_template_ipc_client = match result.get_result() {
                                    Ok(new_template_ipc_client) => {
                                        tracing::debug!("waitNext returned new template IPC client");
                                        new_template_ipc_client
                                    },
                                    Err(e) => {
                                        match e.kind {
                                            capnp::ErrorKind::MessageContainsNullCapabilityPointer => {
                                                tracing::debug!("waitNext timed out (no mempool changes)");
                                                tracing::debug!("Continuing to next waitNext iteration");
                                                continue; // Go back to the start of the loop
                                            }
                                            _ => {
                                                tracing::error!("Failed to get new template IPC client: {}", e);
                                                tracing::warn!("Terminating Sv2 Bitcoin Core IPC Connection");
                                                self_clone.global_cancellation_token.cancel();
                                                break;
                                            }
                                        }
                                    }
                                };

                                {
                                    let mut current_template_ipc_client_guard = self_clone.current_template_ipc_client.borrow_mut();
                                    *current_template_ipc_client_guard = Some(new_template_ipc_client);
                                    tracing::debug!("Updated current_template_ipc_client with new template");
                                }

                                tracing::debug!("Fetching new template data...");
                                let new_template_data = match self_clone.fetch_template_data().await {
                                    Ok(new_template_data) => new_template_data,
                                    Err(e) => {
                                        tracing::error!("Failed to fetch template data: {:?}", e);
                                        tracing::warn!("Terminating Sv2 Bitcoin Core IPC Connection");
                                        self_clone.global_cancellation_token.cancel();
                                        break;
                                    }
                                };

                                let new_prev_hash = new_template_data.get_prev_hash();
                                let current_prev_hash = match self_clone.current_prev_hash.borrow().clone() {
                                    Some(prev_hash) => prev_hash,
                                    None => {
                                        tracing::error!("current_prev_hash is not set");
                                        tracing::warn!("Terminating Sv2 Bitcoin Core IPC Connection");
                                        self_clone.global_cancellation_token.cancel();
                                        break;
                                    }
                                };

                                if new_prev_hash != current_prev_hash {
                                    info!("⛓️ Chain Tip changed! New prev_hash: {}", new_prev_hash);
                                    tracing::debug!("CHAIN TIP CHANGE DETECTED - old: {}, new: {}", current_prev_hash, new_prev_hash);
                                    self_clone.current_prev_hash.replace(Some(new_prev_hash));

                                    // save stale template ids, cleanup and save the new template data
                                    let template_data_to_destroy = {
                                        let mut template_data_guard = match self_clone.template_data.write() {
                                            Ok(guard) => guard,
                                            Err(e) => {
                                                tracing::error!("Failed to acquire write lock on template_data: {:?}", e);
                                                tracing::warn!("Terminating Sv2 Bitcoin Core IPC Connection");
                                                self_clone.global_cancellation_token.cancel();
                                                break;
                                            }
                                        };
                                        let mut stale_template_ids_guard = match self_clone.stale_template_ids.write() {
                                            Ok(guard) => guard,
                                            Err(e) => {
                                                tracing::error!("Failed to acquire write lock on stale_template_ids: {:?}", e);
                                                tracing::warn!("Terminating Sv2 Bitcoin Core IPC Connection");
                                                self_clone.global_cancellation_token.cancel();
                                                break;
                                            }
                                        };

                                        // save stale template ids
                                        let stale_ids: HashSet<_> = template_data_guard.clone().into_keys().collect();
                                        tracing::debug!("Marking {} templates as stale: {:?}", stale_ids.len(), stale_ids);
                                        *stale_template_ids_guard = stale_ids;

                                        // collect template data to destroy (clone before dropping guard)
                                        let template_data_to_destroy: Vec<_> = template_data_guard.values().cloned().collect();

                                        // no point in keeping the old templates around
                                        tracing::debug!("Clearing old template data");
                                        template_data_guard.clear();

                                        // save the new template data
                                        tracing::debug!("Saving new template data with template_id: {}", new_template_data.get_template_id());
                                        template_data_guard.insert(new_template_data.get_template_id(), new_template_data.clone());

                                        template_data_to_destroy
                                    };

                                    // destroy each template ipc client (after guard is dropped)
                                    for template_data in template_data_to_destroy {
                                        if let Err(e) = template_data.destroy_ipc_client(self_clone.thread_ipc_client.clone()).await {
                                            tracing::error!("Failed to destroy template IPC client: {:?}", e);
                                            tracing::warn!("Terminating Sv2 Bitcoin Core IPC Connection");
                                            self_clone.global_cancellation_token.cancel();
                                            break;
                                        }
                                    }

                                    // send the future NewTemplate message
                                    let future_template = new_template_data.get_new_template_message(true);
                                    tracing::debug!("Sending NewTemplate (future=true) after chain tip change");

                                    if let Err(e) = self_clone.outgoing_messages.send(TemplateDistribution::NewTemplate(future_template.clone())).await {
                                        tracing::error!("Failed to send future NewTemplate message: {:?}", e);
                                        tracing::warn!("Terminating Sv2 Bitcoin Core IPC Connection");
                                        self_clone.global_cancellation_token.cancel();
                                        break;
                                    }

                                    // send the SetNewPrevHash message
                                    let set_new_prev_hash = new_template_data.get_set_new_prev_hash_message();
                                    tracing::debug!("Sending SetNewPrevHash after chain tip change");

                                    match self_clone.outgoing_messages.send(TemplateDistribution::SetNewPrevHash(set_new_prev_hash.clone())).await {
                                        Ok(_) => {
                                            tracing::debug!("Successfully sent SetNewPrevHash");
                                        },
                                        Err(e) => {
                                            tracing::error!("Failed to send SetNewPrevHash message: {:?}", e);
                                            tracing::warn!("Terminating Sv2 Bitcoin Core IPC Connection");
                                            self_clone.global_cancellation_token.cancel();
                                            break;
                                        }
                                    }
                                } else {
                                    info!("💹 Mempool fees increased! Sending NewTemplate message.");
                                    tracing::debug!("MEMPOOL FEE CHANGE DETECTED - sending non-future template");

                                    // send the non-future NewTemplate message
                                    let non_future_template = new_template_data.get_new_template_message(false);
                                    tracing::debug!("Sending NewTemplate (future=false) after fee change");

                                    if let Err(e) = self_clone.outgoing_messages.send(TemplateDistribution::NewTemplate(non_future_template.clone())).await {
                                        tracing::error!("Failed to send future NewTemplate message: {:?}", e);
                                        tracing::warn!("Terminating Sv2 Bitcoin Core IPC Connection");
                                        self_clone.global_cancellation_token.cancel();
                                        break;
                                    }
                                }

                                // save the new template data
                                tracing::debug!("Saving template data for template_id: {}", new_template_data.get_template_id());
                                let mut template_data_guard = match self_clone.template_data.write() {
                                    Ok(guard) => guard,
                                    Err(e) => {
                                        tracing::error!("Failed to acquire write lock on template_data: {:?}", e);
                                        tracing::warn!("Terminating Sv2 Bitcoin Core IPC Connection");
                                        self_clone.global_cancellation_token.cancel();
                                        break;
                                    }
                                };
                                template_data_guard.insert(new_template_data.get_template_id(), new_template_data.clone());

                            }
                            Err(e) => {
                                tracing::debug!("waitNext request failed with error: {}", e);
                                tracing::error!("Failed to get response: {}", e);
                                tracing::warn!("Terminating Sv2 Bitcoin Core IPC Connection");
                                self_clone.global_cancellation_token.cancel();
                                break;
                            }
                        }
                    }
                }
            }
            tracing::debug!("monitor_ipc_templates() task exiting");
        });
    }

    pub fn monitor_incoming_messages(&self) {
        let mut self_clone = self.clone();

        tokio::task::spawn_local(async move {
            tracing::debug!("monitor_incoming_messages() task started");
            loop {
                tokio::select! {
                    _ = self_clone.global_cancellation_token.cancelled() => {
                        tracing::warn!("Exiting incoming messages loop");
                        tracing::debug!("monitor_incoming_messages() exiting due to cancellation");
                        break;
                    }
                    Ok(incoming_message) = self_clone.incoming_messages.recv() => {
                        tracing::info!("Received: {}", incoming_message);
                        tracing::debug!("monitor_incoming_messages() processing message");

                        match incoming_message {
                            TemplateDistribution::CoinbaseOutputConstraints(coinbase_output_constraints) => {
                                tracing::debug!("Received CoinbaseOutputConstraints - max_additional_size: {}, max_additional_sigops: {}",
                                    coinbase_output_constraints.coinbase_output_max_additional_size,
                                    coinbase_output_constraints.coinbase_output_max_additional_sigops);
                                if let Err(e) = self_clone.handle_coinbase_output_constraints(coinbase_output_constraints).await {
                                    tracing::error!("Failed to handle coinbase output constraints: {:?}", e);
                                    tracing::warn!("Terminating Sv2 Bitcoin Core IPC Connection");
                                    self_clone.global_cancellation_token.cancel();
                                    break;
                                }
                            }
                            TemplateDistribution::RequestTransactionData(request_transaction_data) => {
                                tracing::debug!("Received RequestTransactionData for template_id: {}", request_transaction_data.template_id);
                                if let Err(e) = self_clone.handle_request_transaction_data(request_transaction_data).await {
                                    tracing::error!("Failed to handle request transaction data: {:?}", e);
                                    tracing::warn!("Terminating Sv2 Bitcoin Core IPC Connection");
                                    self_clone.global_cancellation_token.cancel();
                                    break;
                                }
                            }
                            TemplateDistribution::SubmitSolution(submit_solution) => {
                                tracing::debug!("Received SubmitSolution for template_id: {}", submit_solution.template_id);
                                if let Err(e) = self_clone.handle_submit_solution(submit_solution).await {
                                    tracing::error!("Failed to handle submit solution: {:?}", e);
                                    tracing::warn!("Terminating Sv2 Bitcoin Core IPC Connection");
                                    self_clone.global_cancellation_token.cancel();
                                    break;
                                }
                            }
                            _ => {
                                tracing::error!("Received unexpected message: {}", incoming_message);
                                tracing::warn!("Ignoring message");
                                continue;
                            }
                        }
                    }
                }
            }
        });
    }
}
