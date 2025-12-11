#include <array>
#include <chrono>
#include <cctype>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <memory>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include <rocksdb/cache.h>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/table.h>
#include <rocksdb/write_batch.h>

namespace {

constexpr uint64_t kRawPayloadBytes = 4ull * 1024ull * 1024ull * 1024ull;
constexpr size_t kKeySize = 32;
constexpr size_t kValueSize = 96;
constexpr size_t kEntryBytes = kKeySize + kValueSize;
static_assert(kRawPayloadBytes % kEntryBytes == 0, "payload must be divisible by entry size");
constexpr uint64_t kEntryCount = kRawPayloadBytes / kEntryBytes;  // 33,554,432 entries.

struct Config {
  std::vector<int> block_sizes;
  std::filesystem::path db_root = std::filesystem::path{"./space_amp_runs"};
  bool keep_dbs = false;
  uint64_t read_ops = 200'000;
};

struct Result {
  int block_size = 0;
  uint64_t total_sst_bytes = 0;
  uint64_t estimated_keys = 0;
  uint64_t table_readers_mem = 0;
  double amplification = 0.0;
  double read_ops_per_sec = 0.0;
};

std::string HumanBytes(double bytes) {
  static constexpr const char* kUnits[] = {"B", "KB", "MB", "GB", "TB"};
  int unit = 0;
  while (bytes >= 1024.0 && unit < 4) {
    bytes /= 1024.0;
    ++unit;
  }
  std::ostringstream oss;
  oss << std::fixed << std::setprecision(bytes >= 100 ? 0 : (bytes >= 10 ? 1 : 2)) << bytes
      << kUnits[unit];
  return oss.str();
}

std::vector<int> DefaultBlockSizes() {
  return {4 * 1024, 8 * 1024, 16 * 1024, 32 * 1024, 64 * 1024};
}

std::vector<int> ParseBlockSizes(std::string_view csv) {
  if (csv.empty()) {
    return DefaultBlockSizes();
  }
  std::vector<int> values;
  std::string current;
  for (char c : csv) {
    if (c == ',') {
      if (!current.empty()) {
        values.push_back(std::stoi(current));
        current.clear();
      }
    } else if (!std::isspace(static_cast<unsigned char>(c))) {
      current.push_back(c);
    }
  }
  if (!current.empty()) {
    values.push_back(std::stoi(current));
  }
  if (values.empty()) {
    values = DefaultBlockSizes();
  }
  return values;
}

Config ParseArguments(int argc, char** argv) {
  Config cfg;
  for (int i = 1; i < argc; ++i) {
    std::string_view arg(argv[i]);
    if (arg.rfind("--block_sizes=", 0) == 0) {
      cfg.block_sizes = ParseBlockSizes(arg.substr(std::string_view("--block_sizes=").size()));
    } else if (arg.rfind("--db_root=", 0) == 0) {
      cfg.db_root = std::string(arg.substr(std::string_view("--db_root=").size()));
    } else if (arg == "--keep_dbs") {
      cfg.keep_dbs = true;
    } else if (arg.rfind("--read_ops=", 0) == 0) {
      std::string_view value = arg.substr(std::string_view("--read_ops=").size());
      cfg.read_ops = std::stoull(std::string(value));
    } else if (arg == "--help" || arg == "-h") {
      std::cout << "Usage: space_amp [--block_sizes=csv] [--db_root=dir] [--keep_dbs] [--read_ops=N]\n";
      std::exit(EXIT_SUCCESS);
    } else {
      std::cerr << "Unknown argument: " << arg << "\n";
      std::exit(EXIT_FAILURE);
    }
  }
  if (cfg.block_sizes.empty()) {
    cfg.block_sizes = DefaultBlockSizes();
  }
  return cfg;
}

void FormatKey(uint64_t index, std::array<char, kKeySize + 1>* buffer) {
  std::snprintf(buffer->data(), buffer->size(), "%032llu",
                static_cast<unsigned long long>(index));
}

void FillValue(uint64_t index, std::array<char, kValueSize>* buffer) {
  for (size_t i = 0; i < kValueSize; ++i) {
    (*buffer)[i] = static_cast<char>('a' + ((index + i) % 26));
  }
}

rocksdb::Options BuildOptions(int block_size) {
  rocksdb::Options options;
  options.create_if_missing = true;
  options.error_if_exists = true;
  options.compression = rocksdb::CompressionType::kNoCompression;
  options.bottommost_compression = rocksdb::CompressionType::kNoCompression;
  options.level_compaction_dynamic_level_bytes = true;
  options.write_buffer_size = 256ull * 1024ull * 1024ull;
  options.max_write_buffer_number = 4;
  options.target_file_size_base = 512ull * 1024ull * 1024ull;
  options.max_background_compactions = 4;
  options.max_background_flushes = 2;
  options.disable_auto_compactions = false;
  options.use_direct_reads = true;
  options.use_direct_io_for_flush_and_compaction = true;
  options.compaction_readahead_size = 2 * 1024 * 1024;
  rocksdb::BlockBasedTableOptions table_options;
  table_options.block_size = block_size;
  table_options.cache_index_and_filter_blocks = false;
  table_options.pin_l0_filter_and_index_blocks_in_cache = false;
  table_options.block_cache = nullptr;
  table_options.no_block_cache = true;
  table_options.filter_policy.reset();

  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  return options;
}

struct ReadStats {
  double ops_per_sec = 0.0;
};

ReadStats BenchmarkReads(const std::filesystem::path& db_path, const rocksdb::Options& template_options,
                         uint64_t read_ops) {
  ReadStats stats;
  if (read_ops == 0) {
    return stats;
  }

  rocksdb::Options options = template_options;
  options.create_if_missing = false;
  options.error_if_exists = false;
  rocksdb::DB* raw_db = nullptr;
  auto status = rocksdb::DB::OpenForReadOnly(options, db_path.string(), &raw_db);
  if (!status.ok()) {
    throw std::runtime_error("Failed to reopen RocksDB for reads at " + db_path.string() + ": " +
                             status.ToString());
  }
  std::unique_ptr<rocksdb::DB> db(raw_db);

  rocksdb::ReadOptions read_options;
  read_options.fill_cache = false;
  read_options.verify_checksums = false;

  std::mt19937_64 rng(0xC0FFEE);
  std::uniform_int_distribution<uint64_t> dist(0, kEntryCount - 1);
  std::array<char, kKeySize + 1> key_buffer{};
  rocksdb::PinnableSlice value;

  auto start = std::chrono::steady_clock::now();
  for (uint64_t i = 0; i < read_ops; ++i) {
    uint64_t key_index = dist(rng);
    FormatKey(key_index, &key_buffer);
    rocksdb::Slice key_slice(key_buffer.data(), kKeySize);
    status = db->Get(read_options, db->DefaultColumnFamily(), key_slice, &value);
    if (!status.ok()) {
      throw std::runtime_error("Read failed: " + status.ToString());
    }
    value.Reset();
  }
  auto end = std::chrono::steady_clock::now();
  double seconds = std::chrono::duration<double>(end - start).count();
  if (seconds <= 0.0) {
    seconds = 1e-9;
  }
  stats.ops_per_sec = static_cast<double>(read_ops) / seconds;
  return stats;
}

Result RunOnce(const Config& cfg, int block_size) {
  Result result;
  result.block_size = block_size;
  const std::filesystem::path db_path = cfg.db_root / ("block_" + std::to_string(block_size));
  std::filesystem::create_directories(cfg.db_root);
  if (std::filesystem::exists(db_path)) {
    std::filesystem::remove_all(db_path);
  }

  rocksdb::Options options = BuildOptions(block_size);
  rocksdb::DB* raw_db = nullptr;
  auto status = rocksdb::DB::Open(options, db_path.string(), &raw_db);
  if (!status.ok()) {
    throw std::runtime_error("Failed to open RocksDB at " + db_path.string() + ": " + status.ToString());
  }
  std::unique_ptr<rocksdb::DB> db(raw_db);

  rocksdb::WriteOptions write_options;
  write_options.disableWAL = true;
  rocksdb::WriteBatch batch;
  const size_t batch_size = 1'000;
  std::array<char, kKeySize + 1> key_buffer{};
  std::array<char, kValueSize> value_buffer{};

  for (uint64_t i = 0; i < kEntryCount; ++i) {
    FormatKey(i, &key_buffer);
    FillValue(i, &value_buffer);
    rocksdb::Slice key_slice(key_buffer.data(), kKeySize);
    rocksdb::Slice value_slice(value_buffer.data(), kValueSize);
    batch.Put(key_slice, value_slice);
    if (batch.Count() >= static_cast<int>(batch_size)) {
      status = db->Write(write_options, &batch);
      if (!status.ok()) {
        throw std::runtime_error("Write failed: " + status.ToString());
      }
      batch.Clear();
    }
  }
  if (batch.Count() > 0) {
    status = db->Write(write_options, &batch);
    if (!status.ok()) {
      throw std::runtime_error("Write failed: " + status.ToString());
    }
    batch.Clear();
  }

  rocksdb::FlushOptions flush_options;
  flush_options.wait = true;
  status = db->Flush(flush_options);
  if (!status.ok()) {
    throw std::runtime_error("Flush failed: " + status.ToString());
  }
  status = db->CompactRange(rocksdb::CompactRangeOptions(), nullptr, nullptr);
  if (!status.ok()) {
    throw std::runtime_error("CompactRange failed: " + status.ToString());
  }
  if (!db->GetAggregatedIntProperty("rocksdb.total-sst-files-size", &result.total_sst_bytes)) {
    throw std::runtime_error("Failed to get rocksdb.total-sst-files-size");
  }
  if (!db->GetAggregatedIntProperty("rocksdb.estimate-num-keys", &result.estimated_keys)) {
    throw std::runtime_error("Failed to get rocksdb.estimate-num-keys");
  }
  if (!db->GetAggregatedIntProperty("rocksdb.estimate-table-readers-mem", &result.table_readers_mem)) {
    throw std::runtime_error("Failed to get rocksdb.estimate-table-readers-mem");
  }
  result.amplification = static_cast<double>(result.total_sst_bytes) /
                         static_cast<double>(kRawPayloadBytes);

  db.reset();
  if (cfg.read_ops > 0) {
    std::cout << "[block=" << block_size << "] ingest complete, starting read benchmark ("
              << cfg.read_ops << " ops)...\n";
    ReadStats read_stats = BenchmarkReads(db_path, options, cfg.read_ops);
    result.read_ops_per_sec = read_stats.ops_per_sec;
  }
  if (!cfg.keep_dbs) {
    std::filesystem::remove_all(db_path);
  }
  return result;
}

}  // namespace

int main(int argc, char** argv) {
  try {
    Config cfg = ParseArguments(argc, argv);
    std::vector<Result> results;
    results.reserve(cfg.block_sizes.size());
    for (int block_size : cfg.block_sizes) {
      if (block_size <= 0) {
        std::cerr << "Block size must be positive: " << block_size << "\n";
        return EXIT_FAILURE;
      }
      results.push_back(RunOnce(cfg, block_size));
    }

    std::cout << "Raw payload bytes: " << kRawPayloadBytes << " ("
              << kEntryCount << " entries)\n";
    std::cout << std::left << std::setw(12) << "Block Size"
              << std::right << std::setw(16) << "Total SST"
              << std::setw(12) << "Amplif."
              << std::setw(18) << "Est. Keys"
              << std::setw(14) << "Table Mem"
              << std::setw(12) << "Reads/s" << "\n";
    for (const auto& r : results) {
      std::cout << std::left << std::setw(12) << HumanBytes(r.block_size)
                << std::right << std::setw(16) << HumanBytes(static_cast<double>(r.total_sst_bytes))
                << std::setw(12) << std::fixed << std::setprecision(2) << r.amplification
                << std::setw(18) << r.estimated_keys
                << std::setw(14) << HumanBytes(static_cast<double>(r.table_readers_mem))
                << std::setw(12) << std::setprecision(0) << std::fixed << r.read_ops_per_sec
                << "\n";
    }
  } catch (const std::exception& ex) {
    std::cerr << "Error: " << ex.what() << "\n";
    return EXIT_FAILURE;
  }
  return EXIT_SUCCESS;
}
