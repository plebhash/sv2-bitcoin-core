# Bitcoin Core IPC Benchmark

A focused benchmark crate to compare different strategies for fetching block template data from Bitcoin Core via IPC.

Following @Sjors suggestion: https://github.com/bitcoin/bitcoin/issues/33923#issuecomment-3569919239

## Purpose

This crate benchmarks two strategies for fetching template data:

1. **Sequential**: Uses a single thread IPC client for all three calls (`get_block_header`, `get_coinbase_tx`, `get_coinbase_merkle_path`)
2. **Concurrent**: Uses 3 dedicated thread IPC clients for each call and executes them concurrently using `tokio::try_join!`

## Running Benchmarks

### Using default socket path

```bash
cargo bench
```

### Using custom socket path

```bash
BITCOIN_CORE_IPC_SOCKET=/path/to/node.sock cargo bench
```

### Example for testnet4

```bash
BITCOIN_CORE_IPC_SOCKET=~/.bitcoin/testnet4/node.sock cargo bench
```

## Output

Criterion will generate detailed benchmark reports in `target/criterion/fetch_template/`.

You can view the HTML report by opening:
```
target/criterion/fetch_template/report/index.html
```

## Understanding Results

The benchmark measures the total time to fetch:
- Block header
- Coinbase transaction
- Coinbase merkle path

Lower times indicate better performance. The concurrent strategy may show improvements if Bitcoin Core can handle parallel IPC calls efficiently.

## Notes

- This crate intentionally avoids all Stratum V2 dependencies and complexity
- No template storage, no solution submission, no UX - just pure performance measurement
- The benchmark uses a sample size of 100 iterations to balance accuracy with IPC overhead
- Results may vary based on Bitcoin Core's current state (syncing, mempool size, etc.)

