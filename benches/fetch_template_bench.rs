use bitcoin_core_bench::{fetch_template_concurrent, fetch_template_sequential, IpcContext};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use std::env;

/// Get the Bitcoin Core IPC socket path from environment variable
fn get_socket_path() -> String {
    env::var("BITCOIN_CORE_IPC_SOCKET").unwrap_or_else(|_| {
        // Default path, adjust as needed
        let home = env::var("HOME").expect("HOME environment variable not set");
        format!("{}/.bitcoin/node.sock", home)
    })
}

fn benchmark_strategies(c: &mut Criterion) {
    let socket_path = get_socket_path();
    println!("Using Bitcoin Core IPC socket: {}", socket_path);

    let mut group = c.benchmark_group("fetch_template");
    group.sample_size(20); // Reduce sample size for IPC benchmarks

    // Sequential strategy
    let socket_path_clone = socket_path.clone();
    group.bench_function(BenchmarkId::new("sequential", "single_thread"), |b| {
        let socket_path = socket_path_clone.clone();
        b.to_async(tokio::runtime::Runtime::new().unwrap())
            .iter_custom(|iters| {
                let socket_path = socket_path.clone();
                async move {
                    let local = tokio::task::LocalSet::new();
                    local
                        .run_until(async move {
                            // Bootstrap IPC context once (outside timing)
                            let ctx = IpcContext::new(&socket_path)
                                .await
                                .expect("Failed to create IPC context");

                            // Time only the actual fetch operations
                            let start = std::time::Instant::now();
                            for _ in 0..iters {
                                fetch_template_sequential(&ctx)
                                    .await
                                    .expect("Sequential fetch failed");
                            }
                            start.elapsed()
                        })
                        .await
                }
            });
    });

    // Concurrent strategy with pre-allocated thread clients
    let socket_path_clone = socket_path.clone();
    group.bench_function(BenchmarkId::new("concurrent", "multiple_threads"), |b| {
        let socket_path = socket_path_clone.clone();
        b.to_async(tokio::runtime::Runtime::new().unwrap())
            .iter_custom(|iters| {
                let socket_path = socket_path.clone();
                async move {
                    let local = tokio::task::LocalSet::new();
                    local
                        .run_until(async move {
                            // Bootstrap IPC context once (outside timing)
                            let ctx = IpcContext::new(&socket_path)
                                .await
                                .expect("Failed to create IPC context");

                            // Pre-allocate thread clients (outside timing)
                            let header_thread = ctx
                                .new_thread_client()
                                .await
                                .expect("Failed to create header thread");
                            let coinbase_thread = ctx
                                .new_thread_client()
                                .await
                                .expect("Failed to create coinbase thread");
                            let merkle_thread = ctx
                                .new_thread_client()
                                .await
                                .expect("Failed to create merkle thread");

                            // Time only the actual fetch operations
                            let start = std::time::Instant::now();
                            for _ in 0..iters {
                                fetch_template_concurrent(
                                    &ctx,
                                    &header_thread,
                                    &coinbase_thread,
                                    &merkle_thread,
                                )
                                .await
                                .expect("Concurrent fetch failed");
                            }
                            start.elapsed()
                        })
                        .await
                }
            });
    });

    group.finish();
}

criterion_group!(benches, benchmark_strategies);
criterion_main!(benches);
