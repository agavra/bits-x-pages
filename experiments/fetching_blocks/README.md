# fetching_blocks

Benchmarks for disk I/O operations, demonstrating that sub-block reads (e.g., 2KB) take the same time as reading a full 4KB block because disks read in 4KB blocks.

## Prerequisites

- Rust toolchain (install via [rustup](https://rustup.rs/))
- macOS (uses `F_NOCACHE` to bypass OS cache)

## Build

```bash
cargo build --release
```

## Run Benchmarks

```bash
cargo bench
```

This runs the `disk_io` benchmark which compares read times for 1KB, 2KB, 3KB, and 4KB reads using `F_NOCACHE` to bypass the OS page cache.

## How It Works

The benchmark uses:
- `F_NOCACHE` (macOS) to bypass the OS file cache and measure actual disk I/O
- `pread` for reading data at specific offsets
- Criterion for statistical benchmarking

The test creates a 4KB file in `/tmp/disk_io_bench_subblock/` and measures read latency for different sizes within that single block.
