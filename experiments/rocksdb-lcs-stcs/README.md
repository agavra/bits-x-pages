# RocksDB Compaction Strategy Benchmark: LCS vs STCS

This benchmark compares **Leveled Compaction Strategy (LCS)** vs **Size-Tiered Compaction Strategy (STCS)** in RocksDB, measuring the real-world tradeoffs in read, write, and space amplification.

## Background

LSM trees can use different compaction strategies with different tradeoffs:

- **Leveled Compaction (LCS)**: Lower read and space amplification, but higher write amplification. Each level is a single sorted run.
- **Universal Compaction (STCS)**: Lower write amplification, but higher read and space amplification. Each tier can have multiple overlapping sorted runs.

This benchmark runs identical workloads on both strategies using direct I/O (bypassing OS cache) to measure actual disk I/O.

## Prerequisites

- CMake 3.16+
- C++17 compiler
- RocksDB library (install via Homebrew: `brew install rocksdb`)

## Build

```bash
cmake -S experiments/rocksdb-lcs-stcs -B build/rocksdb-lcs-stcs
cmake --build build/rocksdb-lcs-stcs -j
```

## Run

```bash
./build/rocksdb-lcs-stcs/compaction_bench [OPTIONS]
```

### Options

| Option | Default | Description |
|--------|---------|-------------|
| `--db_root=PATH` | `./compaction_bench_runs` | Directory for database files |
| `--data_size_mb=N` | `512` | Logical data size in MB |
| `--value_size=N` | `128` | Value size in bytes |
| `--read_percent=N` | `50` | Read percentage in mixed workload |
| `--duration_secs=N` | `60` | Duration of mixed workload phase |
| `--threads=N` | `4` | Number of worker threads |
| `--keep_dbs` | false | Keep database directories after benchmark |
| `--verbose, -v` | false | Print verbose progress |

### Examples

Quick test run:
```bash
./build/rocksdb-lcs-stcs/compaction_bench --data_size_mb=256 --duration_secs=30
```

Full benchmark with larger dataset:
```bash
./build/rocksdb-lcs-stcs/compaction_bench --data_size_mb=2048 --duration_secs=120 --verbose
```

Write-heavy workload:
```bash
./build/rocksdb-lcs-stcs/compaction_bench --read_percent=10 --duration_secs=60
```

## What's Measured

### Write Amplification
Ratio of total bytes written to disk (including compaction) vs. user bytes written.
- **LCS**: Higher write amp (~10-30x) due to repeated rewriting during compaction
- **STCS**: Lower write amp (~3-7x) because data moves to larger tiers exponentially

### Read Amplification
Ratio of bytes read from disk per logical read operation.
- **LCS**: Lower read amp (~1-2x) because each level has one sorted run
- **STCS**: Higher read amp (~2-5x) because multiple overlapping runs must be checked

### Space Amplification
Ratio of peak disk usage vs. logical data size.
- **LCS**: Lower space amp (~1.1x) due to aggressive merging
- **STCS**: Higher space amp (~1.5-2x) due to overlapping sorted runs

## Sample Output

```
================================================================================
                    COMPACTION STRATEGY COMPARISON
================================================================================

Metric                                   Leveled         Universal
------------------------------------------------------------------

--- Write Amplification ---
User Writes                              512 MB            512 MB
Total Bytes Written                     2.56 GB           896 MB
Write Amplification                       5.00x             1.75x

--- Read Amplification ---
User Reads                                 1.2M              1.1M
Bytes Read                               768 MB           1.32 GB
Read Amplification                        4.00x             7.50x

--- Space Amplification ---
Logical Data Size                        512 MB            512 MB
Peak Disk Usage                          576 MB            896 MB
Final Disk Usage                         544 MB            640 MB
Space Amplification (peak)                1.13x             1.75x

--- Throughput ---
Read Ops/sec                              20.0K             18.3K
Write Ops/sec                             15.2K             17.8K

================================================================================
```

## Implementation Notes

- Uses direct I/O (`use_direct_reads`, `use_direct_io_for_flush_and_compaction`) to bypass OS cache
- Block cache is disabled to measure actual disk reads
- No compression to isolate compaction behavior
- Statistics collected via RocksDB's built-in statistics counters
- Benchmark phases:
  1. **Bulk load**: Write all keys sequentially
  2. **Wait for compaction**: Let background compaction settle
  3. **Mixed workload**: Run random reads + updates
  4. **Final compaction**: Wait for all compactions to complete
