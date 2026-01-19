//! Benchmark crate for Bitcoin Core IPC template fetching strategies
//!
//! This crate benchmarks different strategies for fetching block template data
//! from Bitcoin Core via IPC:
//!
//! 1. **Sequential**: Use a single thread IPC client for all three calls
//! 2. **Concurrent**: Use dedicated thread IPC clients for each call and execute concurrently

use bitcoin::{Transaction, block::Header, consensus::deserialize};
use bitcoin_capnp_types::{
    init_capnp::init::Client as InitIpcClient,
    mining_capnp::mining::Client as MiningIpcClient,
    proxy_capnp::{thread::Client as ThreadIpcClient, thread_map::Client as ThreadMapIpcClient},
};
use capnp_rpc::{RpcSystem, rpc_twoparty_capnp, twoparty};
use std::path::Path;
use tokio::net::UnixStream;
use tokio_util::compat::*;

pub type Result<T> = std::result::Result<T, anyhow::Error>;

/// Minimal context for IPC communication with Bitcoin Core
pub struct IpcContext {
    pub thread_map: ThreadMapIpcClient,
    pub mining_client: MiningIpcClient,
    pub block_template_client: bitcoin_capnp_types::mining_capnp::block_template::Client,
}

impl IpcContext {
    /// Connect to Bitcoin Core via IPC and create a template client
    pub async fn new<P: AsRef<Path>>(unix_socket_path: P) -> Result<Self> {
        let stream = UnixStream::connect(unix_socket_path).await?;
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

        // Create a thread IPC client (for context)
        let thread_request = thread_map.make_thread_request();
        let thread_response = thread_request.send().promise.await?;
        let thread_ipc_client: ThreadIpcClient = thread_response.get()?.get_result()?;

        // Create mining client
        let mut mining_client_request = bootstrap_client.make_mining_request();
        mining_client_request
            .get()
            .get_context()?
            .set_thread(thread_ipc_client.clone());
        let mining_client_response = mining_client_request.send().promise.await?;
        let mining_client: MiningIpcClient = mining_client_response.get()?.get_result()?;

        // Create a template client
        let mut template_request = mining_client.create_new_block_request();
        let mut template_options = template_request.get().get_options()?;
        template_options.set_block_reserved_weight(2000);
        template_options.set_coinbase_output_max_additional_sigops(0);
        template_options.set_use_mempool(true);

        let template_response = template_request.send().promise.await?;
        let block_template_client = template_response.get()?.get_result()?;

        Ok(Self {
            thread_map,
            mining_client,
            block_template_client,
        })
    }

    /// Create a new thread IPC client
    pub async fn new_thread_client(&self) -> Result<ThreadIpcClient> {
        let thread_request = self.thread_map.make_thread_request();
        let thread_response = thread_request.send().promise.await?;
        Ok(thread_response.get()?.get_result()?)
    }
}

/// Template data fetched from Bitcoin Core
#[derive(Debug)]
pub struct TemplateData {
    pub header: Header,
    pub coinbase_tx: Transaction,
    pub merkle_path: Vec<Vec<u8>>,
}

/// Sequential strategy: use a single thread client for all three calls
pub async fn fetch_template_sequential(ctx: &IpcContext) -> Result<TemplateData> {
    let thread_client = ctx.new_thread_client().await?;

    // Call 1: Get block header
    let mut header_request = ctx.block_template_client.get_block_header_request();
    header_request
        .get()
        .get_context()?
        .set_thread(thread_client.clone());
    let header_bytes = header_request
        .send()
        .promise
        .await?
        .get()?
        .get_result()?
        .to_vec();
    let header: Header = deserialize(&header_bytes)?;

    // Call 2: Get coinbase tx
    let mut coinbase_request = ctx.block_template_client.get_coinbase_tx_request();
    coinbase_request
        .get()
        .get_context()?
        .set_thread(thread_client.clone());
    let coinbase_bytes = coinbase_request
        .send()
        .promise
        .await?
        .get()?
        .get_result()?
        .to_vec();
    let coinbase_tx: Transaction = deserialize(&coinbase_bytes)?;

    // Call 3: Get merkle path
    let mut merkle_request = ctx.block_template_client.get_coinbase_merkle_path_request();
    merkle_request
        .get()
        .get_context()?
        .set_thread(thread_client.clone());
    let merkle_path: Vec<Vec<u8>> = merkle_request
        .send()
        .promise
        .await?
        .get()?
        .get_result()?
        .iter()
        .map(|x| x.map(|slice| slice.to_vec()))
        .collect::<std::result::Result<Vec<_>, _>>()?;

    Ok(TemplateData {
        header,
        coinbase_tx,
        merkle_path,
    })
}

/// Concurrent strategy: use dedicated pre-allocated thread clients for each call and run concurrently
pub async fn fetch_template_concurrent(
    ctx: &IpcContext,
    header_thread: &ThreadIpcClient,
    coinbase_thread: &ThreadIpcClient,
    merkle_thread: &ThreadIpcClient,
) -> Result<TemplateData> {
    let (header, coinbase_tx, merkle_path) = tokio::try_join!(
        async {
            let mut header_request = ctx.block_template_client.get_block_header_request();
            header_request
                .get()
                .get_context()?
                .set_thread(header_thread.clone());
            let header_bytes = header_request
                .send()
                .promise
                .await?
                .get()?
                .get_result()?
                .to_vec();
            let header: Header = deserialize(&header_bytes)?;
            Ok::<_, anyhow::Error>(header)
        },
        async {
            let mut coinbase_request = ctx.block_template_client.get_coinbase_tx_request();
            coinbase_request
                .get()
                .get_context()?
                .set_thread(coinbase_thread.clone());
            let coinbase_bytes = coinbase_request
                .send()
                .promise
                .await?
                .get()?
                .get_result()?
                .to_vec();
            let coinbase_tx: Transaction = deserialize(&coinbase_bytes)?;
            Ok::<_, anyhow::Error>(coinbase_tx)
        },
        async {
            let mut merkle_request = ctx.block_template_client.get_coinbase_merkle_path_request();
            merkle_request
                .get()
                .get_context()?
                .set_thread(merkle_thread.clone());
            let merkle_path: Vec<Vec<u8>> = merkle_request
                .send()
                .promise
                .await?
                .get()?
                .get_result()?
                .iter()
                .map(|x| x.map(|slice| slice.to_vec()))
                .collect::<std::result::Result<Vec<_>, _>>()?;
            Ok::<_, anyhow::Error>(merkle_path)
        }
    )?;

    Ok(TemplateData {
        header,
        coinbase_tx,
        merkle_path,
    })
}
