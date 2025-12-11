# RocksDB Block Size Space Amplification Benchmark

Bulk-load 4 GiB of deterministic key/value pairs into RocksDB with different block sizes to illustrate how more index entries increase both on-disk footprint and table-reader memory. After ingestion, the tool optionally issues random point-lookups with the block cache disabled **and** direct I/O enabled so every read comes from storage rather than the OS cache.

## Dataset

- Raw payload: 4 GiB spread over 33,554,432 entries.
- Key/value sizes: 32 B zero-padded keys and 96 B deterministic values (128 B per row).
- Data is deterministic for reproducibility.

## Building

From the repo root, configure and build into an experiment-specific directory:

```bash
cmake -S experiments/lsm-space-amp -B build/lsm-space-amp
cmake --build build/lsm-space-amp -j
```

The benchmark binary will be at `build/lsm-space-amp/space_amp`.

## Running

```
./build/lsm-space-amp/space_amp [--block_sizes=csv] [--db_root=path] [--keep_dbs] [--read_ops=N]
```

- `--block_sizes` sets a comma-separated list of block sizes in bytes (default: `4096,8192,16384,32768,65536`).
- `--db_root` controls the directory that holds the RocksDB instances (default: `./space_amp_runs`).
- `--keep_dbs` skips the cleanup step so you can inspect the generated SST files.
- `--read_ops` controls how many random point-lookups are issued after load with the block cache disabled and RocksDB direct I/O enabled (default: `200000`). Set to `0` to skip the read benchmark.

Sample output:

```
Raw payload bytes: 4294967296 (33554432 entries)
Block Size       Total SST      Amplif.         Est. Keys        Table Mem        Reads/s
4KB                 5.8GB          1.44          33554432          52MB           825000
...
```

`Table Mem` reports RocksDB's `rocksdb.estimate-table-readers-mem`, i.e. the heap memory RocksDB keeps for table readers (indexes, filters) when cached. The read-throughput column comes from a follow-up benchmark that reopens the DB with the block cache disabled and direct I/O enabled, forcing the point-lookups to fetch data blocks from storage. `Amplif.` equals `total_sst_bytes / raw_payload_bytes` so you can see the on-disk overhead directly.
