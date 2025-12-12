# RocksDB Merge Operator vs Read-Modify-Write Benchmark

Goal: quantify read/write throughput trade-offs when counting events via RocksDB's merge operator versus doing explicit read-update-write (RMW) cycles.

## Workload

* Keys: configurable (default 10,000). Pre-populated with `0`; the small key space plus a large memtable (512 MB) let merge operands pile up without being flushed away so reads must replay many deltas.
* Writes: increment the counter for a random key. The RMW implementation performs `Get` + `Put`. The merge implementation calls `Merge` with `+1` and relies on a custom associative merge operator that sums deltas.
* Reads: random `Get` calls.
* Concurrency: configurable number of threads (default 8). Each workload runs for a configurable duration (default 15s).
* Mixes: the tool exercises 20/80, 50/50, and 80/20 read/write ratios.

## Build

```
cmake -S experiments/rocksdb-merge-bench -B build/rocksdb-merge-bench
cmake --build build/rocksdb-merge-bench -j
```

## Run

```
./build/rocksdb-merge-bench/merge_bench \
  [--db_root=dir] [--keys=N] [--threads=N] [--seconds=N] [--mix=ratio]
```

* `--db_root`: directory for temporary RocksDB data (default `./merge_bench_runs`).
* `--keys`: number of pre-populated keys.
* `--threads`: number of concurrent client threads.
* `--seconds`: duration per workload mix.
* `--mix`: optional filter to run only a specific ratio (e.g., `--mix=90/10`).

The tool prints two tables (RMW and Merge) summarizing per-mix read/write ops per second plus the average number of outstanding merge operands per key, so you can see how deferred merges accumulate and penalize reads.
