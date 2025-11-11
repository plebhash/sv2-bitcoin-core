//! # Bitcoin Core Sv2 Library
//!
//! A Rust library that integrates [Bitcoin Core](https://bitcoin.org/en/bitcoin-core/) with the
//! [Stratum V2 Template Distribution Protocol](https://github.com/stratum-mining/sv2-spec/blob/main/07-Template-Distribution-Protocol.md)
//! via IPC over a UNIX socket.
//!
//! ## Overview
//!
//! `bitcoin_core_sv2` allows for the official Bitcoin Core distribution to be leveraged for the
//! following use-cases:
//!
//! - Building Sv2 applications that act as a Client under the Template Distribution Protocol (e.g.:
//!   Pool or JDC) while connecting directly to the Bitcoin Core node.
//! - Building a Sv2 Template Provider application that acts as a Template Distribution Protocol
//!   Server while leveraging a Bitcoin Core node as source of truth.
//!
//! ## Design
//!
//! The [`BitcoinCoreSv2`] struct is designed to be a single point of entry for interacting with
//! Bitcoin Core via Sv2 Template Distribution Protocol. It is instantiated with a UNIX socket
//! path, a fee threshold, and two channels for incoming and outgoing messages.
//!
//! The struct operates with three main IO paths:
//!
//! 1. **Incoming Sv2 Messages** (`incoming_messages` channel):
//!    - Receives [`TemplateDistribution`] messages from the Sv2 protocol side
//!    - Handles [`CoinbaseOutputConstraints`] to configure coinbase output limits
//!    - Processes [`RequestTransactionData`] requests and responds with transaction data
//!    - Accepts [`SubmitSolution`] messages and forwards them to Bitcoin Core via IPC
//!    - Processed asynchronously by the `monitor_incoming_messages()` task
//!
//! 2. **Outgoing Sv2 Messages** (`outgoing_messages` channel):
//!    - Sends [`TemplateDistribution`] messages to the Sv2 protocol side
//!    - Distributes `NewTemplate` messages when templates change (chain tip or mempool updates)
//!    - Sends `SetNewPrevHash` messages when the chain tip changes
//!    - Responds to transaction data requests with `RequestTransactionDataSuccess` or
//!      `RequestTransactionDataError`
//!    - Triggered by Bitcoin Core events or in response to incoming messages
//!
//! 3. **Bitcoin Core IPC Communication** (UNIX socket):
//!    - Establishes a Cap'n Proto RPC connection over a UNIX socket during initialization
//!    - Uses multiple IPC clients: `thread_ipc_client` for execution context, `mining_ipc_client`
//!      for mining operations, and `template_ipc_client` for block templates
//!    - The `monitor_ipc_templates()` task continuously polls Bitcoin Core via `waitNext` requests
//!      to detect:
//!      - **Chain tip changes**: When a new block is mined, detected by comparing prev_hash values
//!      - **Mempool fee changes**: When mempool fees exceed the configured `fee_threshold`
//!    - Fetches block templates via `get_block_request()` and deserializes them into Bitcoin blocks
//!    - Submits mining solutions back to Bitcoin Core through the template IPC client
//!
//! The architecture enables bidirectional communication: Bitcoin Core events flow through IPC to
//! the struct, which then distributes them via the outgoing channel, while incoming Sv2 protocol
//! messages are processed and forwarded to Bitcoin Core as needed.
//!
//! ## Important Notes
//!
//! ### `LocalSet` Requirement
//!
//! Due to limitations in the `capnp-rpc` dependency, [`BitcoinCoreSv2`] must be run within a
//! [`tokio::task::LocalSet`]. The crate examples demonstrate the proper setup pattern.
//!
//! ### Fee Threshold
//!
//! The `fee_threshold` parameter (in satoshis) determines when a new template is distributed due
//! to mempool changes. When the mempool fee delta exceeds this threshold, a new `NewTemplate`
//! message is sent.
//!
//! ## Usage
//!
//! The main entry point is the [`BitcoinCoreSv2`] struct, which provides an async interface for
//! interacting with Bitcoin Core. See the crate examples for complete usage patterns.
//!
//! ## IPC Communication
//!
//! This library leverages [`bitcoin_capnp_types`](https://github.com/2140-dev/bitcoin-capnp-types) to interact with Bitcoin Core via IPC over a
//! UNIX socket. The connection is established during [`BitcoinCoreSv2::new`] and maintained
//! throughout the lifetime of the instance.

use crate::template_data::TemplateData;
use async_channel::{Receiver, Sender};
use bitcoin_capnp_types::{
    init_capnp::init::Client as InitIpcClient,
    mining_capnp::{
        block_template::Client as BlockTemplateIpcClient, mining::Client as MiningIpcClient,
    },
    proxy_capnp::{thread::Client as ThreadIpcClient, thread_map::Client as ThreadMapIpcClient},
};
use capnp_rpc::{RpcSystem, rpc_twoparty_capnp, twoparty};
use error::BitcoinCoreSv2Error;
use std::{
    cell::RefCell,
    collections::{HashMap, HashSet},
    path::Path,
    rc::Rc,
    sync::atomic::{AtomicU64, Ordering},
};
use stratum_core::{
    binary_sv2::U256,
    bitcoin::{block::Block, consensus::deserialize},
    parsers_sv2::TemplateDistribution,
};

use std::sync::RwLock;
use tokio::net::UnixStream;
use tokio_util::compat::*;
pub use tokio_util::sync::CancellationToken;
use tracing::info;

pub mod error;
mod handlers;
mod monitors;
mod template_data;

const WEIGHT_FACTOR: u32 = 4;
const MIN_BLOCK_RESERVED_WEIGHT: u64 = 2000;

/// The main abstraction for interacting with Bitcoin Core via Sv2 Template Distribution Protocol.
///
/// It is instantiated with:
/// - A `&`[`std::path::Path`] to the Bitcoin Core UNIX socket
/// - A `u64` for the fee delta threshold in satoshis
/// - A [`async_channel::Receiver`] for incoming [`TemplateDistribution`] messages (handles
///   [`CoinbaseOutputConstraints`], [`RequestTransactionData`], and [`SubmitSolution`])
/// - A [`async_channel::Sender`] for outgoing [`TemplateDistribution`] messages
/// - A [`tokio_util::sync::CancellationToken`] to stop the internally spawned tasks
///
/// The instance waits for the first [`CoinbaseOutputConstraints`] message to be received via the
/// incoming channel before initializing the template IPC client. Upon receiving this message and
/// successfully initializing, the [`BitcoinCoreSv2`] instance sends a `NewTemplate` followed by a
/// corresponding `SetNewPrevHash` message over the outgoing channel.
///
/// As configured via `fee_threshold`, the [`BitcoinCoreSv2`] instance will monitor the mempool for
/// changes and send a `NewTemplate` message if the fee delta is greater than the configured
/// threshold.
///
/// When there's a new Chain Tip, the [`BitcoinCoreSv2`] instance will send a `NewTemplate` followed
/// by a corresponding `SetNewPrevHash` message over the outgoing channel.
///
/// Incoming [`RequestTransactionData`] messages are used to request transactions relative to a
/// specific template, for which a corresponding `RequestTransactionDataSuccess` or
/// `RequestTransactionDataError` message is sent over the outgoing channel.
///
/// Incoming [`SubmitSolution`] messages are used to submit solutions to a specific template.
#[derive(Clone)]
pub struct BitcoinCoreSv2 {
    fee_threshold: u64,
    thread_map: ThreadMapIpcClient,
    thread_ipc_client: ThreadIpcClient,
    mining_ipc_client: MiningIpcClient,
    current_template_ipc_client: Rc<RefCell<Option<BlockTemplateIpcClient>>>,
    current_prev_hash: Rc<RefCell<Option<U256<'static>>>>,
    template_data: Rc<RwLock<HashMap<u64, TemplateData>>>,
    stale_template_ids: Rc<RwLock<HashSet<u64>>>,
    template_id_factory: Rc<AtomicU64>,
    incoming_messages: Receiver<TemplateDistribution<'static>>,
    outgoing_messages: Sender<TemplateDistribution<'static>>,
    global_cancellation_token: CancellationToken,
    template_ipc_client_cancellation_token: CancellationToken,
}

impl BitcoinCoreSv2 {
    /// Creates a new [`BitcoinCoreSv2`] instance.
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        bitcoin_core_unix_socket_path: &Path,
        fee_threshold: u64,
        incoming_messages: Receiver<TemplateDistribution<'static>>,
        outgoing_messages: Sender<TemplateDistribution<'static>>,
        global_cancellation_token: CancellationToken,
    ) -> Result<Self, BitcoinCoreSv2Error> {
        info!(
            "Creating new Sv2 Bitcoin Core Connection via IPC over UNIX socket: {}",
            bitcoin_core_unix_socket_path.display()
        );

        let stream = UnixStream::connect(bitcoin_core_unix_socket_path)
            .await
            .map_err(|_| {
                BitcoinCoreSv2Error::CannotConnectToUnixSocket(bitcoin_core_unix_socket_path.into())
            })?;
        let (reader, writer) = stream.into_split();
        let reader_compat = reader.compat();
        let writer_compat = writer.compat_write();

        let rpc_network = Box::new(twoparty::VatNetwork::new(
            reader_compat,
            writer_compat,
            rpc_twoparty_capnp::Side::Client,
            Default::default(),
        ));

        let mut rpc_system = RpcSystem::new(rpc_network, None);
        let bootstrap_client: InitIpcClient =
            rpc_system.bootstrap(rpc_twoparty_capnp::Side::Server);

        tokio::task::spawn_local(rpc_system);

        let construct_response = bootstrap_client.construct_request().send().promise.await?;

        let thread_map: ThreadMapIpcClient = construct_response.get()?.get_thread_map()?;
        let thread_request = thread_map.make_thread_request();
        let thread_response = thread_request.send().promise.await?;

        let thread_ipc_client: ThreadIpcClient = thread_response.get()?.get_result()?;

        info!("IPC execution thread client successfully created.");

        let mut mining_client_request = bootstrap_client.make_mining_request();
        mining_client_request
            .get()
            .get_context()?
            .set_thread(thread_ipc_client.clone());
        let mining_client_response = mining_client_request.send().promise.await?;
        let mining_ipc_client: MiningIpcClient = mining_client_response.get()?.get_result()?;

        info!("IPC mining client successfully created.");

        let template_ipc_client_cancellation_token = CancellationToken::new();

        Ok(Self {
            fee_threshold,
            thread_map,
            thread_ipc_client,
            mining_ipc_client,
            template_id_factory: Rc::new(AtomicU64::new(0)),
            current_template_ipc_client: Rc::new(RefCell::new(None)),
            current_prev_hash: Rc::new(RefCell::new(None)),
            template_data: Rc::new(RwLock::new(HashMap::new())),
            stale_template_ids: Rc::new(RwLock::new(HashSet::new())),
            global_cancellation_token,
            incoming_messages,
            outgoing_messages,
            template_ipc_client_cancellation_token,
        })
    }

    /// Runs the [`BitcoinCoreSv2`] instance, monitoring for:
    /// - Chain Tip changes, for which it will send a `NewTemplate` message, followed by a
    ///   `SetNewPrevHash` message
    /// - incoming [`RequestTransactionData`] messages, for which it will send a
    ///   `RequestTransactionDataSuccess` or `RequestTransactionDataError` message as a response
    /// - incoming [`SubmitSolution`] messages, for which it will submit the solution to the Bitcoin
    ///   Core IPC client
    /// - incoming [`CoinbaseOutputConstraints`] messages, for which it will update the coinbase
    ///   output constraints
    ///
    /// Blocks until the cancellation token is activated.
    pub async fn run(&mut self) {
        // wait for first CoinbaseOutputConstraints message
        tracing::info!("Waiting for first CoinbaseOutputConstraints message");
        tracing::debug!("run() started, waiting for initial CoinbaseOutputConstraints");
        loop {
            tokio::select! {
                _ = self.global_cancellation_token.cancelled() => {
                    tracing::warn!("Exiting run");
                    tracing::debug!("run() early exit - global cancellation token activated before first CoinbaseOutputConstraints");
                    return;
                }
                Ok(message) = self.incoming_messages.recv() => {
                    tracing::debug!("run() received message during initial loop: {:?}", message);
                    match message {
                        TemplateDistribution::CoinbaseOutputConstraints(coinbase_output_constraints) => {
                            tracing::info!("Received: {:?}", coinbase_output_constraints);
                            tracing::debug!("First CoinbaseOutputConstraints received - max_additional_size: {}, max_additional_sigops: {}",
                                coinbase_output_constraints.coinbase_output_max_additional_size,
                                coinbase_output_constraints.coinbase_output_max_additional_sigops);

                            let template_ipc_client = match self.new_template_ipc_client(coinbase_output_constraints.coinbase_output_max_additional_size, coinbase_output_constraints.coinbase_output_max_additional_sigops).await {
                                Ok(template_ipc_client) => {
                                    tracing::debug!("Successfully created initial template IPC client");
                                    template_ipc_client
                                },
                                Err(e) => {
                                    tracing::error!("Failed to create new template IPC client: {:?}", e);
                                    tracing::warn!("Terminating Sv2 Bitcoin Core IPC Connection");
                                    self.global_cancellation_token.cancel();
                                    return;
                                }
                            };

                            let mut current_template_ipc_client_guard = self.current_template_ipc_client.borrow_mut();
                            *current_template_ipc_client_guard = Some(template_ipc_client);
                            tracing::debug!("Set current_template_ipc_client to initial template");

                            break;
                        }
                        _ => {
                            tracing::warn!("Received unexpected message: {:?}", message);
                            tracing::warn!("Ignoring...");
                            continue;
                        }
                    }
                }
            }
        }

        // bootstrap the first template
        {
            tracing::debug!("Bootstrapping first template...");
            let template_data = match self.fetch_template_data().await {
                Ok(template_data) => {
                    tracing::debug!(
                        "Successfully fetched initial template data - template_id: {}",
                        template_data.get_template_id()
                    );
                    template_data
                }
                Err(e) => {
                    tracing::error!("Failed to fetch template data: {:?}", e);
                    tracing::warn!("Terminating Sv2 Bitcoin Core IPC Connection");
                    self.global_cancellation_token.cancel();
                    return;
                }
            };

            // send the future NewTemplate message
            let future_template = template_data.get_new_template_message(true);
            tracing::debug!(
                "Sending initial NewTemplate (future=true) with template_id: {}",
                template_data.get_template_id()
            );

            match self
                .outgoing_messages
                .send(TemplateDistribution::NewTemplate(future_template.clone()))
                .await
            {
                Ok(_) => {
                    tracing::debug!("Successfully sent initial NewTemplate message");
                }
                Err(e) => {
                    tracing::error!("Failed to send future template message: {:?}", e);
                    tracing::warn!("Terminating Sv2 Bitcoin Core IPC Connection");
                    self.global_cancellation_token.cancel();
                    return;
                }
            }

            // send the SetNewPrevHash message
            let set_new_prev_hash = template_data.get_set_new_prev_hash_message();
            tracing::debug!(
                "Sending initial SetNewPrevHash with prev_hash: {}",
                template_data.get_prev_hash()
            );

            match self
                .outgoing_messages
                .send(TemplateDistribution::SetNewPrevHash(
                    set_new_prev_hash.clone(),
                ))
                .await
            {
                Ok(_) => {
                    tracing::debug!("Successfully sent initial SetNewPrevHash message");
                }
                Err(e) => {
                    tracing::error!("Failed to send set new prev hash message: {:?}", e);
                    tracing::warn!("Terminating Sv2 Bitcoin Core IPC Connection");
                    self.global_cancellation_token.cancel();
                    return;
                }
            }

            // save the template data
            let mut template_data_guard = match self.template_data.write() {
                Ok(guard) => guard,
                Err(e) => {
                    tracing::error!("Failed to acquire write lock on template_data: {:?}", e);
                    tracing::warn!("Terminating Sv2 Bitcoin Core IPC Connection");
                    self.global_cancellation_token.cancel();
                    return;
                }
            };
            template_data_guard.insert(template_data.get_template_id(), template_data.clone());
            tracing::debug!(
                "Saved initial template data with template_id: {}",
                template_data.get_template_id()
            );

            // save the current prev hash
            self.current_prev_hash
                .replace(Some(template_data.get_prev_hash()));
            tracing::debug!(
                "Set current_prev_hash to: {}",
                template_data.get_prev_hash()
            );
        }

        // spawn the monitoring tasks
        tracing::debug!("Spawning monitoring tasks...");
        self.monitor_ipc_templates();
        tracing::debug!("monitor_ipc_templates() spawned");
        self.monitor_incoming_messages();
        tracing::debug!("monitor_incoming_messages() spawned");

        // block until the global cancellation token is activated
        tracing::debug!("run() entering main blocking wait for global_cancellation_token");
        self.global_cancellation_token.cancelled().await;

        tracing::debug!("run() exiting");
    }

    async fn fetch_template_data(&self) -> Result<TemplateData, BitcoinCoreSv2Error> {
        tracing::debug!("Fetching template data over IPC");
        let template_id = self.template_id_factory.fetch_add(1, Ordering::Relaxed);
        tracing::debug!(
            "fetch_template_data() - assigned template_id: {}",
            template_id
        );

        // clone the current template IPC client so it's stored in the template data HashMap
        // this is important in case we need to submit a solution relative to this specific template
        // by the time self.current_template_ipc_client might have already changed
        let template_ipc_client = match self.current_template_ipc_client.borrow().clone() {
            Some(template_ipc_client) => template_ipc_client,
            None => {
                tracing::error!("Template IPC client not found");
                return Err(BitcoinCoreSv2Error::TemplateIpcClientNotFound);
            }
        };

        let mut template_block_request = template_ipc_client.get_block_request();
        template_block_request
            .get()
            .get_context()?
            .set_thread(self.thread_ipc_client.clone());

        let template_block_bytes = template_block_request
            .send()
            .promise
            .await?
            .get()?
            .get_result()?
            .to_vec();

        // Deserialize the complete block template from Bitcoin Core's serialization format
        tracing::debug!(
            "Deserializing block template ({} bytes)",
            template_block_bytes.len()
        );
        let block: Block = deserialize(&template_block_bytes)?;
        tracing::debug!(
            "Block deserialized - prev_hash from header: {:?}",
            block.header.prev_blockhash
        );

        // Create the template data structure
        let template_data = TemplateData::new(template_id, block, template_ipc_client);
        tracing::debug!("TemplateData created successfully");

        Ok(template_data)
    }

    async fn new_template_ipc_client(
        &self,
        coinbase_output_max_additional_size: u32,
        coinbase_output_max_additional_sigops: u16,
    ) -> Result<BlockTemplateIpcClient, BitcoinCoreSv2Error> {
        tracing::debug!(
            "new_template_ipc_client() called - max_size: {}, max_sigops: {}",
            coinbase_output_max_additional_size,
            coinbase_output_max_additional_sigops
        );

        let mut template_ipc_client_request = self.mining_ipc_client.create_new_block_request();
        let mut template_ipc_client_request_options =
            match template_ipc_client_request.get().get_options() {
                Ok(options) => options,
                Err(e) => {
                    tracing::error!("Failed to get template IPC client request options: {e}");
                    return Err(BitcoinCoreSv2Error::CapnpError(e));
                }
            };

        let coinbase_weight = (coinbase_output_max_additional_size * WEIGHT_FACTOR) as u64;
        let block_reserved_weight = coinbase_weight.max(MIN_BLOCK_RESERVED_WEIGHT); // 2000 is the minimum block reserved weight
        tracing::debug!("Setting block_reserved_weight: {block_reserved_weight}");
        template_ipc_client_request_options.set_block_reserved_weight(block_reserved_weight);
        template_ipc_client_request_options.set_coinbase_output_max_additional_sigops(
            coinbase_output_max_additional_sigops as u64,
        );
        template_ipc_client_request_options.set_use_mempool(true);

        tracing::debug!("Sending createNewBlock request to Bitcoin Core");
        let template_ipc_client_response = match template_ipc_client_request.send().promise.await {
            Ok(response) => response,
            Err(e) => {
                tracing::error!("Failed to send template IPC client request: {}", e);
                return Err(BitcoinCoreSv2Error::CapnpError(e));
            }
        };

        let template_ipc_client_result = match template_ipc_client_response.get() {
            Ok(result) => result,
            Err(e) => {
                tracing::error!("Failed to get template IPC client result: {}", e);
                return Err(BitcoinCoreSv2Error::CapnpError(e));
            }
        };

        let template_ipc_client = match template_ipc_client_result.get_result() {
            Ok(result) => {
                tracing::debug!("Successfully created new template IPC client");
                result
            }
            Err(e) => {
                tracing::error!("Failed to get template IPC client result: {}", e);
                return Err(BitcoinCoreSv2Error::CapnpError(e));
            }
        };

        Ok(template_ipc_client)
    }
}
