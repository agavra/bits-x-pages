#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <memory>
#include <numeric>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include <rocksdb/cache.h>
#include <rocksdb/compaction_filter.h>
#include <rocksdb/db.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/statistics.h>
#include <rocksdb/table.h>
#include <rocksdb/write_batch.h>

namespace {

// Default workload parameters
constexpr size_t kKeySize = 32;
constexpr size_t kValueSize = 128;
constexpr size_t kEntryBytes = kKeySize + kValueSize;

struct Config {
  std::filesystem::path db_root = std::filesystem::path{"./compaction_bench_runs"};
  uint64_t data_size_mb = 2048;       // Logical data size in MB (2GB for deeper LSM tree)
  uint64_t key_count = 0;             // Computed from data_size_mb
  size_t value_size = kValueSize;
  int read_percent = 50;              // Percentage of reads in mixed workload
  int update_rate = 100;              // 0 = all new keys, 100 = all updates to existing keys
  int duration_secs = 60;             // Duration of mixed workload phase
  int threads = 4;                    // Number of worker threads
  bool keep_dbs = false;
  bool reuse_db = false;              // Skip bulk load, reuse existing database
  bool no_compaction = false;         // Disable background compaction (for clean read tests)
  bool verbose = false;
};

struct IOStats {
  uint64_t bytes_written = 0;
  uint64_t bytes_read = 0;
  uint64_t compact_bytes_read = 0;
  uint64_t compact_bytes_written = 0;
  uint64_t flush_bytes_written = 0;
  uint64_t block_reads = 0;
  uint64_t block_read_bytes = 0;
};

struct Result {
  std::string strategy_name;

  // Write amplification metrics
  uint64_t user_bytes_written = 0;
  uint64_t total_bytes_written = 0;
  double write_amp = 0.0;

  // Read amplification metrics
  uint64_t user_reads = 0;
  uint64_t bytes_read = 0;
  double read_amp = 0.0;

  // Space amplification metrics
  uint64_t logical_data_size = 0;
  uint64_t peak_disk_usage = 0;
  uint64_t final_disk_usage = 0;
  double space_amp = 0.0;

  // Throughput
  double read_ops_per_sec = 0.0;
  double write_ops_per_sec = 0.0;

  // Duration
  double load_duration_secs = 0.0;
  double workload_duration_secs = 0.0;
};

std::string HumanBytes(uint64_t bytes) {
  static constexpr const char* kUnits[] = {"B", "KB", "MB", "GB", "TB"};
  double value = static_cast<double>(bytes);
  int unit = 0;
  while (value >= 1024.0 && unit < 4) {
    value /= 1024.0;
    ++unit;
  }
  std::ostringstream oss;
  if (value >= 100) {
    oss << std::fixed << std::setprecision(0) << value << " " << kUnits[unit];
  } else if (value >= 10) {
    oss << std::fixed << std::setprecision(1) << value << " " << kUnits[unit];
  } else {
    oss << std::fixed << std::setprecision(2) << value << " " << kUnits[unit];
  }
  return oss.str();
}

std::string HumanNumber(uint64_t n) {
  std::ostringstream oss;
  if (n >= 1'000'000'000) {
    oss << std::fixed << std::setprecision(2) << (n / 1'000'000'000.0) << "B";
  } else if (n >= 1'000'000) {
    oss << std::fixed << std::setprecision(2) << (n / 1'000'000.0) << "M";
  } else if (n >= 1'000) {
    oss << std::fixed << std::setprecision(2) << (n / 1'000.0) << "K";
  } else {
    oss << n;
  }
  return oss.str();
}

Config ParseArguments(int argc, char** argv) {
  Config cfg;
  for (int i = 1; i < argc; ++i) {
    std::string_view arg(argv[i]);
    if (arg.rfind("--db_root=", 0) == 0) {
      cfg.db_root = std::string(arg.substr(std::string_view("--db_root=").size()));
    } else if (arg.rfind("--data_size_mb=", 0) == 0) {
      cfg.data_size_mb = std::stoull(std::string(arg.substr(std::string_view("--data_size_mb=").size())));
    } else if (arg.rfind("--value_size=", 0) == 0) {
      cfg.value_size = std::stoull(std::string(arg.substr(std::string_view("--value_size=").size())));
    } else if (arg.rfind("--read_percent=", 0) == 0) {
      cfg.read_percent = std::stoi(std::string(arg.substr(std::string_view("--read_percent=").size())));
    } else if (arg.rfind("--update_rate=", 0) == 0) {
      cfg.update_rate = std::stoi(std::string(arg.substr(std::string_view("--update_rate=").size())));
    } else if (arg.rfind("--duration_secs=", 0) == 0) {
      cfg.duration_secs = std::stoi(std::string(arg.substr(std::string_view("--duration_secs=").size())));
    } else if (arg.rfind("--threads=", 0) == 0) {
      cfg.threads = std::stoi(std::string(arg.substr(std::string_view("--threads=").size())));
    } else if (arg == "--keep_dbs") {
      cfg.keep_dbs = true;
    } else if (arg == "--reuse_db") {
      cfg.reuse_db = true;
      cfg.keep_dbs = true;  // Implied: must keep DBs to reuse them
    } else if (arg == "--no_compaction") {
      cfg.no_compaction = true;
    } else if (arg == "--verbose" || arg == "-v") {
      cfg.verbose = true;
    } else if (arg == "--help" || arg == "-h") {
      std::cout << "Usage: compaction_bench [OPTIONS]\n\n"
                << "Compares Leveled (LCS) vs Universal (STCS) compaction strategies.\n\n"
                << "Options:\n"
                << "  --db_root=PATH       Directory for database files (default: ./compaction_bench_runs)\n"
                << "  --data_size_mb=N     Logical data size in MB (default: 2048)\n"
                << "  --value_size=N       Value size in bytes (default: 128)\n"
                << "  --read_percent=N     Read percentage in mixed workload (default: 50)\n"
                << "  --update_rate=N      0 = all new keys, 100 = all updates (default: 100)\n"
                << "  --duration_secs=N    Duration of mixed workload phase (default: 60)\n"
                << "  --threads=N          Number of worker threads (default: 4)\n"
                << "  --keep_dbs           Keep database directories after benchmark\n"
                << "  --reuse_db           Reuse existing DB, skip bulk load (measures updates only)\n"
                << "  --no_compaction      Disable background compaction (for clean read tests)\n"
                << "  --verbose, -v        Print verbose progress\n"
                << "  --help, -h           Show this help message\n";
      std::exit(EXIT_SUCCESS);
    } else {
      std::cerr << "Unknown argument: " << arg << "\n";
      std::exit(EXIT_FAILURE);
    }
  }
  // Compute key count from data size
  size_t entry_size = kKeySize + cfg.value_size;
  cfg.key_count = (cfg.data_size_mb * 1024ull * 1024ull) / entry_size;
  return cfg;
}

void FormatKey(uint64_t index, std::array<char, kKeySize + 1>* buffer) {
  std::snprintf(buffer->data(), buffer->size(), "%032llu",
                static_cast<unsigned long long>(index));
}

void FillValue(uint64_t index, char* buffer, size_t size) {
  for (size_t i = 0; i < size; ++i) {
    buffer[i] = static_cast<char>('a' + ((index + i) % 26));
  }
}

IOStats GetIOStats(const std::shared_ptr<rocksdb::Statistics>& stats) {
  IOStats io;
  if (!stats) return io;

  io.bytes_written = stats->getTickerCount(rocksdb::BYTES_WRITTEN);
  io.bytes_read = stats->getTickerCount(rocksdb::BYTES_READ);
  io.compact_bytes_read = stats->getTickerCount(rocksdb::COMPACT_READ_BYTES);
  io.compact_bytes_written = stats->getTickerCount(rocksdb::COMPACT_WRITE_BYTES);
  io.flush_bytes_written = stats->getTickerCount(rocksdb::FLUSH_WRITE_BYTES);
  io.block_reads = stats->getTickerCount(rocksdb::BLOCK_CACHE_MISS);
  io.block_read_bytes = stats->getTickerCount(rocksdb::BLOCK_CACHE_BYTES_READ);

  return io;
}

uint64_t GetDiskUsage(rocksdb::DB* db) {
  uint64_t size = 0;
  db->GetAggregatedIntProperty("rocksdb.total-sst-files-size", &size);
  return size;
}

uint64_t GetDiskUsageFromPath(const std::filesystem::path& path) {
  uint64_t total = 0;
  if (std::filesystem::exists(path)) {
    for (const auto& entry : std::filesystem::recursive_directory_iterator(path)) {
      if (entry.is_regular_file()) {
        total += entry.file_size();
      }
    }
  }
  return total;
}

rocksdb::Options BuildLeveledOptions(const Config& cfg,
                                      std::shared_ptr<rocksdb::Statistics> stats) {
  rocksdb::Options options;
  options.create_if_missing = true;
  options.error_if_exists = true;
  options.statistics = stats;

  // Disable compression to isolate compaction behavior
  options.compression = rocksdb::CompressionType::kNoCompression;
  options.bottommost_compression = rocksdb::CompressionType::kNoCompression;

  // Leveled compaction (default)
  options.compaction_style = rocksdb::kCompactionStyleLevel;
  options.level_compaction_dynamic_level_bytes = false;  // Disable optimization to match theoretical model
  options.num_levels = 7;
  options.max_bytes_for_level_base = 64ull * 1024ull * 1024ull;  // 64 MB - smaller to force deeper tree
  options.max_bytes_for_level_multiplier = 4;  // 4x ratio for more levels

  // Write buffer settings
  options.write_buffer_size = 64ull * 1024ull * 1024ull;  // 64 MB
  options.max_write_buffer_number = 4;
  options.min_write_buffer_number_to_merge = 2;

  // File settings
  options.target_file_size_base = 64ull * 1024ull * 1024ull;  // 64 MB
  options.target_file_size_multiplier = 1;

  // Background jobs
  options.max_background_compactions = 4;
  options.max_background_flushes = 2;

  // Optionally disable auto compaction for clean read tests
  options.disable_auto_compactions = cfg.no_compaction;

  // Direct I/O - bypass OS cache
  options.use_direct_reads = true;
  options.use_direct_io_for_flush_and_compaction = true;
  options.compaction_readahead_size = 2 * 1024 * 1024;

  // Block-based table options - use tiny block cache to track misses
  rocksdb::BlockBasedTableOptions table_options;
  table_options.block_size = 4 * 1024;  // 4 KB blocks
  // Enable tiny cache (8KB) so BLOCK_CACHE_MISS gets tracked
  // Everything will miss, giving us accurate disk read counts
  table_options.block_cache = rocksdb::NewLRUCache(8 * 1024);
  table_options.cache_index_and_filter_blocks = false;
  table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, false));

  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  return options;
}

rocksdb::Options BuildUniversalOptions(const Config& cfg,
                                        std::shared_ptr<rocksdb::Statistics> stats) {
  rocksdb::Options options;
  options.create_if_missing = true;
  options.error_if_exists = true;
  options.statistics = stats;

  // Disable compression
  options.compression = rocksdb::CompressionType::kNoCompression;
  options.bottommost_compression = rocksdb::CompressionType::kNoCompression;

  // Universal compaction (size-tiered)
  options.compaction_style = rocksdb::kCompactionStyleUniversal;

  // Universal compaction options
  rocksdb::CompactionOptionsUniversal universal_opts;
  universal_opts.size_ratio = 100;                  // Trigger compaction when size ratio >= 100% (default)
  universal_opts.min_merge_width = 2;               // Minimum files to merge
  universal_opts.max_merge_width = UINT_MAX;        // No limit on merge width
  universal_opts.max_size_amplification_percent = 200;  // Allow 2x space amp before forcing compaction
  universal_opts.compression_size_percent = -1;    // Disable compression
  universal_opts.stop_style = rocksdb::kCompactionStopStyleTotalSize;
  options.compaction_options_universal = universal_opts;

  // Write buffer settings (same as leveled)
  options.write_buffer_size = 64ull * 1024ull * 1024ull;
  options.max_write_buffer_number = 4;
  options.min_write_buffer_number_to_merge = 2;

  // File settings
  options.target_file_size_base = 64ull * 1024ull * 1024ull;

  // Background jobs
  options.max_background_compactions = 4;
  options.max_background_flushes = 2;

  // Optionally disable auto compaction for clean read tests
  options.disable_auto_compactions = cfg.no_compaction;

  // Direct I/O
  options.use_direct_reads = true;
  options.use_direct_io_for_flush_and_compaction = true;
  options.compaction_readahead_size = 2 * 1024 * 1024;

  // Block-based table options - use tiny block cache to track misses
  rocksdb::BlockBasedTableOptions table_options;
  table_options.block_size = 4 * 1024;
  // Enable tiny cache (8KB) so BLOCK_CACHE_MISS gets tracked
  table_options.block_cache = rocksdb::NewLRUCache(8 * 1024);
  table_options.cache_index_and_filter_blocks = false;
  table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, false));

  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  return options;
}

void WaitForCompactions(rocksdb::DB* db, int timeout_secs = 300) {
  // Wait for all compactions to finish (with timeout)
  auto start = std::chrono::steady_clock::now();
  auto timeout = std::chrono::seconds(timeout_secs);

  uint64_t pending = 1;
  while (pending > 0) {
    db->GetAggregatedIntProperty("rocksdb.compaction-pending", &pending);
    uint64_t running = 0;
    db->GetAggregatedIntProperty("rocksdb.num-running-compactions", &running);
    if (pending == 0 && running == 0) break;

    // Check timeout
    if (std::chrono::steady_clock::now() - start > timeout) {
      std::cout << "    (compaction timeout after " << timeout_secs << "s, "
                << pending << " pending, " << running << " running)\n";
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
}

struct WorkloadStats {
  std::atomic<uint64_t> reads{0};
  std::atomic<uint64_t> writes{0};
  std::atomic<uint64_t> bytes_written{0};
  std::atomic<uint64_t> next_new_key{0};  // Counter for new keys (starts at key_count)
  std::atomic<bool> stop{false};
};

void RunMixedWorkload(rocksdb::DB* db, const Config& cfg, WorkloadStats* stats, int thread_id) {
  std::mt19937_64 rng(0xC0FFEE + thread_id);
  std::uniform_int_distribution<uint64_t> key_dist(0, cfg.key_count - 1);
  std::uniform_int_distribution<int> op_dist(0, 99);
  std::uniform_int_distribution<int> update_dist(0, 99);

  std::array<char, kKeySize + 1> key_buffer{};
  std::vector<char> value_buffer(cfg.value_size);

  rocksdb::ReadOptions read_opts;
  read_opts.fill_cache = false;
  read_opts.verify_checksums = false;

  rocksdb::WriteOptions write_opts;
  write_opts.disableWAL = false;

  rocksdb::PinnableSlice value;

  while (!stats->stop.load(std::memory_order_relaxed)) {
    if (op_dist(rng) < cfg.read_percent) {
      // Read operation - always read from existing keys
      uint64_t key_idx = key_dist(rng);
      FormatKey(key_idx, &key_buffer);
      rocksdb::Slice key_slice(key_buffer.data(), kKeySize);
      auto status = db->Get(read_opts, db->DefaultColumnFamily(), key_slice, &value);
      if (status.ok() || status.IsNotFound()) {
        stats->reads.fetch_add(1, std::memory_order_relaxed);
      }
      value.Reset();
    } else {
      // Write operation - decide between update or new key based on update_rate
      uint64_t key_idx;
      if (update_dist(rng) < cfg.update_rate) {
        // Update existing key
        key_idx = key_dist(rng);
      } else {
        // Write new key (expanding keyspace)
        key_idx = stats->next_new_key.fetch_add(1, std::memory_order_relaxed);
      }
      FormatKey(key_idx, &key_buffer);
      FillValue(key_idx + rng(), value_buffer.data(), cfg.value_size);
      rocksdb::Slice key_slice(key_buffer.data(), kKeySize);
      rocksdb::Slice value_slice(value_buffer.data(), cfg.value_size);
      auto status = db->Put(write_opts, key_slice, value_slice);
      if (status.ok()) {
        stats->writes.fetch_add(1, std::memory_order_relaxed);
        stats->bytes_written.fetch_add(kKeySize + cfg.value_size, std::memory_order_relaxed);
      }
    }
  }
}

Result RunBenchmark(const Config& cfg, const std::string& strategy_name,
                    rocksdb::Options (*build_options)(const Config&, std::shared_ptr<rocksdb::Statistics>)) {
  Result result;
  result.strategy_name = strategy_name;
  result.logical_data_size = cfg.data_size_mb * 1024ull * 1024ull;

  const std::filesystem::path db_path = cfg.db_root / strategy_name;
  std::filesystem::create_directories(cfg.db_root);

  bool db_exists = std::filesystem::exists(db_path);

  // Handle reuse_db mode
  if (cfg.reuse_db && !db_exists) {
    throw std::runtime_error("--reuse_db specified but database does not exist at " + db_path.string() +
                             "\nRun once without --reuse_db to create the database first.");
  }

  if (!cfg.reuse_db && db_exists) {
    std::filesystem::remove_all(db_path);
  }

  auto stats = rocksdb::CreateDBStatistics();
  rocksdb::Options options = build_options(cfg, stats);

  // Allow opening existing database when reusing
  if (cfg.reuse_db) {
    options.create_if_missing = false;
    options.error_if_exists = false;
  }

  rocksdb::DB* raw_db = nullptr;
  auto status = rocksdb::DB::Open(options, db_path.string(), &raw_db);
  if (!status.ok()) {
    throw std::runtime_error("Failed to open RocksDB at " + db_path.string() + ": " + status.ToString());
  }
  std::unique_ptr<rocksdb::DB> db(raw_db);

  uint64_t user_bytes_written = 0;
  uint64_t peak_disk = 0;
  IOStats io_after_load{};  // Zero-initialized for reuse_db mode

  // Phase 1: Bulk load (skip if reusing existing database)
  if (!cfg.reuse_db) {
    std::cout << "  [" << strategy_name << "] Phase 1: Bulk loading "
              << HumanNumber(cfg.key_count) << " keys (" << cfg.data_size_mb << " MB)...\n";

    // Shuffle keys to create overlapping SSTs and force real compaction work
    auto load_start = std::chrono::steady_clock::now();

    std::cout << "  [" << strategy_name << "] Generating shuffled key order...\n";
    std::vector<uint64_t> key_indices(cfg.key_count);
    std::iota(key_indices.begin(), key_indices.end(), 0);
    std::mt19937_64 shuffle_rng(0xDEADBEEF);  // Fixed seed for reproducibility
    std::shuffle(key_indices.begin(), key_indices.end(), shuffle_rng);

    rocksdb::WriteOptions write_opts;
    write_opts.disableWAL = true;  // Faster bulk load
    rocksdb::WriteBatch batch;
    const size_t batch_size = 1000;

    std::array<char, kKeySize + 1> key_buffer{};
    std::vector<char> value_buffer(cfg.value_size);

    for (uint64_t i = 0; i < cfg.key_count; ++i) {
      uint64_t key_idx = key_indices[i];
      FormatKey(key_idx, &key_buffer);
      FillValue(key_idx, value_buffer.data(), cfg.value_size);
      rocksdb::Slice key_slice(key_buffer.data(), kKeySize);
      rocksdb::Slice value_slice(value_buffer.data(), cfg.value_size);
      batch.Put(key_slice, value_slice);
      user_bytes_written += kKeySize + cfg.value_size;

      if (batch.Count() >= static_cast<int>(batch_size)) {
        status = db->Write(write_opts, &batch);
        if (!status.ok()) {
          throw std::runtime_error("Write failed: " + status.ToString());
        }
        batch.Clear();

        // Track peak disk usage during load
        uint64_t current_disk = GetDiskUsage(db.get());
        peak_disk = std::max(peak_disk, current_disk);
      }

      // Progress indicator
      if (cfg.verbose && (i + 1) % (cfg.key_count / 10) == 0) {
        std::cout << "    " << ((i + 1) * 100 / cfg.key_count) << "% loaded\n";
      }
    }

    if (batch.Count() > 0) {
      status = db->Write(write_opts, &batch);
      if (!status.ok()) {
        throw std::runtime_error("Write failed: " + status.ToString());
      }
      batch.Clear();
    }

    // Flush and wait for compactions to settle
    rocksdb::FlushOptions flush_opts;
    flush_opts.wait = true;
    db->Flush(flush_opts);

    std::cout << "  [" << strategy_name << "] Waiting for compactions to settle...\n";
    WaitForCompactions(db.get());

    auto load_end = std::chrono::steady_clock::now();
    result.load_duration_secs = std::chrono::duration<double>(load_end - load_start).count();

    // Get stats after bulk load
    io_after_load = GetIOStats(stats);
    uint64_t disk_after_load = GetDiskUsage(db.get());
    peak_disk = std::max(peak_disk, disk_after_load);

    std::cout << "  [" << strategy_name << "] Bulk load complete. Disk usage: "
              << HumanBytes(disk_after_load) << "\n";
  } else {
    std::cout << "  [" << strategy_name << "] Reusing existing database. Disk usage: "
              << HumanBytes(GetDiskUsage(db.get())) << "\n";
    peak_disk = GetDiskUsage(db.get());
  }

  // Phase 2: Mixed workload
  std::cout << "  [" << strategy_name << "] Phase 2: Running mixed workload for "
            << cfg.duration_secs << "s (" << cfg.read_percent << "% reads)...\n";

  // Reset stats for workload phase
  stats->Reset();

  WorkloadStats workload_stats;
  workload_stats.next_new_key.store(cfg.key_count, std::memory_order_relaxed);  // New keys start after existing
  std::vector<std::thread> threads;

  auto workload_start = std::chrono::steady_clock::now();

  for (int t = 0; t < cfg.threads; ++t) {
    threads.emplace_back(RunMixedWorkload, db.get(), std::cref(cfg), &workload_stats, t);
  }

  // Run workload and periodically check disk usage
  auto deadline = workload_start + std::chrono::seconds(cfg.duration_secs);
  while (std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    uint64_t current_disk = GetDiskUsage(db.get());
    peak_disk = std::max(peak_disk, current_disk);
  }

  workload_stats.stop.store(true, std::memory_order_relaxed);
  for (auto& t : threads) {
    t.join();
  }

  auto workload_end = std::chrono::steady_clock::now();
  result.workload_duration_secs = std::chrono::duration<double>(workload_end - workload_start).count();

  // Wait for final compactions
  std::cout << "  [" << strategy_name << "] Waiting for final compactions...\n";
  WaitForCompactions(db.get());

  // Final measurements
  uint64_t final_disk = GetDiskUsage(db.get());
  peak_disk = std::max(peak_disk, final_disk);

  IOStats io_final = GetIOStats(stats);

  // Calculate results
  result.user_reads = workload_stats.reads.load();

  // In reuse_db mode, only count workload phase (no bulk load stats)
  if (cfg.reuse_db) {
    result.user_bytes_written = workload_stats.bytes_written.load();
    result.total_bytes_written = io_final.flush_bytes_written + io_final.compact_bytes_written;
  } else {
    result.user_bytes_written = user_bytes_written + workload_stats.bytes_written.load();
    // Total physical bytes written = flush writes + compaction writes
    // (FLUSH_WRITE_BYTES captures memtable->SST, COMPACT_WRITE_BYTES captures SST->SST)
    result.total_bytes_written = io_after_load.flush_bytes_written + io_after_load.compact_bytes_written +
                                 io_final.flush_bytes_written + io_final.compact_bytes_written;
  }

  // Read I/O measured via block cache misses (each miss = one block read from disk)
  // With tiny 8KB cache, nearly everything misses, giving accurate disk read count
  constexpr uint64_t kBlockSize = 4 * 1024;  // 4KB blocks
  result.bytes_read = io_final.block_reads * kBlockSize;

  result.peak_disk_usage = peak_disk;
  result.final_disk_usage = final_disk;

  // Amplification calculations
  if (result.user_bytes_written > 0) {
    result.write_amp = static_cast<double>(result.total_bytes_written) /
                       static_cast<double>(result.user_bytes_written);
  }

  if (result.user_reads > 0) {
    result.read_amp = static_cast<double>(result.bytes_read) /
                      static_cast<double>(result.user_reads * (kKeySize + cfg.value_size));
  }

  if (result.logical_data_size > 0) {
    result.space_amp = static_cast<double>(result.peak_disk_usage) /
                       static_cast<double>(result.logical_data_size);
  }

  // Throughput
  if (result.workload_duration_secs > 0) {
    result.read_ops_per_sec = static_cast<double>(result.user_reads) / result.workload_duration_secs;
    result.write_ops_per_sec = static_cast<double>(workload_stats.writes.load()) / result.workload_duration_secs;
  }

  std::cout << "  [" << strategy_name << "] Complete. Final disk: " << HumanBytes(final_disk)
            << ", Peak: " << HumanBytes(peak_disk) << "\n";

  db.reset();

  if (!cfg.keep_dbs) {
    std::filesystem::remove_all(db_path);
  }

  return result;
}

void PrintResults(const std::vector<Result>& results) {
  std::cout << "\n";
  std::cout << "================================================================================\n";
  std::cout << "                    COMPACTION STRATEGY COMPARISON\n";
  std::cout << "================================================================================\n\n";

  // Header
  std::cout << std::left << std::setw(30) << "Metric";
  for (const auto& r : results) {
    std::cout << std::right << std::setw(18) << r.strategy_name;
  }
  std::cout << "\n";
  std::cout << std::string(30 + results.size() * 18, '-') << "\n";

  // Write amplification section
  std::cout << "\n--- Write Amplification ---\n";

  std::cout << std::left << std::setw(30) << "User Writes";
  for (const auto& r : results) {
    std::cout << std::right << std::setw(18) << HumanBytes(r.user_bytes_written);
  }
  std::cout << "\n";

  std::cout << std::left << std::setw(30) << "Total Bytes Written";
  for (const auto& r : results) {
    std::cout << std::right << std::setw(18) << HumanBytes(r.total_bytes_written);
  }
  std::cout << "\n";

  std::cout << std::left << std::setw(30) << "Write Amplification";
  for (const auto& r : results) {
    std::ostringstream oss;
    oss << std::fixed << std::setprecision(2) << r.write_amp << "x";
    std::cout << std::right << std::setw(18) << oss.str();
  }
  std::cout << "\n";

  // Read amplification section
  std::cout << "\n--- Read Amplification ---\n";

  std::cout << std::left << std::setw(30) << "User Reads";
  for (const auto& r : results) {
    std::cout << std::right << std::setw(18) << HumanNumber(r.user_reads);
  }
  std::cout << "\n";

  std::cout << std::left << std::setw(30) << "Bytes Read";
  for (const auto& r : results) {
    std::cout << std::right << std::setw(18) << HumanBytes(r.bytes_read);
  }
  std::cout << "\n";

  std::cout << std::left << std::setw(30) << "Read Amplification";
  for (const auto& r : results) {
    std::ostringstream oss;
    oss << std::fixed << std::setprecision(2) << r.read_amp << "x";
    std::cout << std::right << std::setw(18) << oss.str();
  }
  std::cout << "\n";

  // Space amplification section
  std::cout << "\n--- Space Amplification ---\n";

  std::cout << std::left << std::setw(30) << "Logical Data Size";
  for (const auto& r : results) {
    std::cout << std::right << std::setw(18) << HumanBytes(r.logical_data_size);
  }
  std::cout << "\n";

  std::cout << std::left << std::setw(30) << "Peak Disk Usage";
  for (const auto& r : results) {
    std::cout << std::right << std::setw(18) << HumanBytes(r.peak_disk_usage);
  }
  std::cout << "\n";

  std::cout << std::left << std::setw(30) << "Final Disk Usage";
  for (const auto& r : results) {
    std::cout << std::right << std::setw(18) << HumanBytes(r.final_disk_usage);
  }
  std::cout << "\n";

  std::cout << std::left << std::setw(30) << "Space Amplification (peak)";
  for (const auto& r : results) {
    std::ostringstream oss;
    oss << std::fixed << std::setprecision(2) << r.space_amp << "x";
    std::cout << std::right << std::setw(18) << oss.str();
  }
  std::cout << "\n";

  // Throughput section
  std::cout << "\n--- Throughput ---\n";

  std::cout << std::left << std::setw(30) << "Read Ops/sec";
  for (const auto& r : results) {
    std::cout << std::right << std::setw(18) << HumanNumber(static_cast<uint64_t>(r.read_ops_per_sec));
  }
  std::cout << "\n";

  std::cout << std::left << std::setw(30) << "Write Ops/sec";
  for (const auto& r : results) {
    std::cout << std::right << std::setw(18) << HumanNumber(static_cast<uint64_t>(r.write_ops_per_sec));
  }
  std::cout << "\n";

  std::cout << "\n================================================================================\n";
}

}  // namespace

int main(int argc, char** argv) {
  try {
    Config cfg = ParseArguments(argc, argv);

    std::cout << "RocksDB Compaction Strategy Benchmark\n";
    std::cout << "======================================\n";
    std::cout << "Data size: " << cfg.data_size_mb << " MB (" << HumanNumber(cfg.key_count) << " keys)\n";
    std::cout << "Value size: " << cfg.value_size << " bytes\n";
    std::cout << "Mixed workload: " << cfg.read_percent << "% reads, "
              << (100 - cfg.read_percent) << "% writes\n";
    std::cout << "Update rate: " << cfg.update_rate << "% (0=all new keys, 100=all updates)\n";
    std::cout << "Duration: " << cfg.duration_secs << " seconds\n";
    std::cout << "Threads: " << cfg.threads << "\n";
    std::cout << "DB root: " << cfg.db_root << "\n\n";

    std::vector<Result> results;

    // Run Leveled compaction benchmark
    std::cout << "Running Leveled Compaction (LCS) benchmark...\n";
    results.push_back(RunBenchmark(cfg, "Leveled", BuildLeveledOptions));
    std::cout << "\n";

    // Run Universal compaction benchmark
    std::cout << "Running Universal Compaction (STCS) benchmark...\n";
    results.push_back(RunBenchmark(cfg, "Universal", BuildUniversalOptions));

    PrintResults(results);

  } catch (const std::exception& ex) {
    std::cerr << "Error: " << ex.what() << "\n";
    return EXIT_FAILURE;
  }
  return EXIT_SUCCESS;
}
