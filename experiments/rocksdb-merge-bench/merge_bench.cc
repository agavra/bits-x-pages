#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <memory>
#include <random>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <rocksdb/db.h>
#include <rocksdb/merge_operator.h>
#include <rocksdb/options.h>

namespace {

struct Config {
  std::filesystem::path db_root = std::filesystem::path{"./merge_bench_runs"};
  uint64_t key_space = 10'000;
  int threads = 8;
  int seconds_per_phase = 15;
  std::string mix_filter;
};

struct Workload {
  std::string name;
  double read_ratio;  // Between 0 and 1.
};

struct Metrics {
  double read_ops_per_sec = 0.0;
  double write_ops_per_sec = 0.0;
  double avg_merge_ops_per_key = 0.0;
};

const std::vector<Workload> kWorkloads = {
    {"10/90", 0.10}, {"50/50", 0.50}, {"90/10", 0.90},
};

std::vector<Workload> SelectWorkloads(const std::string& filter) {
  if (filter.empty()) {
    return kWorkloads;
  }
  std::vector<Workload> selected;
  for (const auto& w : kWorkloads) {
    if (w.name == filter) {
      selected.push_back(w);
    }
  }
  if (selected.empty()) {
    throw std::runtime_error("Unknown workload mix filter: " + filter);
  }
  return selected;
}

uint64_t Decode(const rocksdb::Slice& value) {
  uint64_t x = 0;
  std::memcpy(&x, value.data(), std::min<size_t>(value.size(), sizeof(uint64_t)));
  return x;
}

std::string Encode(uint64_t value) {
  std::string out(sizeof(uint64_t), '\0');
  std::memcpy(out.data(), &value, sizeof(uint64_t));
  return out;
}

class CountMergeOperator : public rocksdb::MergeOperator {
 public:
  bool FullMergeV2(const MergeOperationInput& merge_in,
                   MergeOperationOutput* merge_out) const override {
    uint64_t accumulator = 0;
    if (merge_in.existing_value != nullptr) {
      accumulator += Decode(*merge_in.existing_value);
    }
    for (const auto& operand : merge_in.operand_list) {
      accumulator += Decode(operand);
    }
    merge_out->new_value = Encode(accumulator);
    return true;
  }

  bool PartialMerge(const rocksdb::Slice& key, const rocksdb::Slice& left_operand,
                    const rocksdb::Slice& right_operand, std::string* new_value,
                    rocksdb::Logger* logger) const override {
    (void)key;
    (void)left_operand;
    (void)right_operand;
    (void)new_value;
    (void)logger;
    return false;  // Disable partial merges so operands accumulate.
  }

  const char* Name() const override { return "CountMergeOperator"; }
};

rocksdb::Options BuildOptions(bool use_merge) {
  rocksdb::Options options;
  options.create_if_missing = true;
  options.error_if_exists = true;
  options.compression = rocksdb::CompressionType::kNoCompression;
  options.bottommost_compression = rocksdb::CompressionType::kNoCompression;
  options.IncreaseParallelism(std::thread::hardware_concurrency());
  options.OptimizeLevelStyleCompaction();
  if (use_merge) {
    options.merge_operator = std::make_shared<CountMergeOperator>();
  }
  options.write_buffer_size = 512ull * 1024ull * 1024ull;
  options.max_write_buffer_number = 2;
  options.target_file_size_base = 512ull * 1024ull * 1024ull;
  options.max_background_flushes = 2;
  options.disable_auto_compactions = true;
  options.use_direct_reads = true;
  options.use_direct_io_for_flush_and_compaction = true;
  options.compaction_readahead_size = 2 * 1024 * 1024;
  return options;
}

void Prepopulate(rocksdb::DB* db, uint64_t key_space) {
  rocksdb::WriteOptions write_options;
  write_options.disableWAL = true;
  for (uint64_t i = 0; i < key_space; ++i) {
    auto status = db->Put(write_options, std::to_string(i), Encode(0));
    if (!status.ok()) {
      throw std::runtime_error("Failed to prepopulate key " + std::to_string(i) + ": " + status.ToString());
    }
  }
}

struct ThreadStats {
  uint64_t reads = 0;
  uint64_t writes = 0;
  uint64_t merge_operands = 0;
};

ThreadStats RunWorker(rocksdb::DB* db, bool use_merge, double read_ratio, uint64_t key_space,
                      const std::chrono::steady_clock::time_point& end_time) {
  ThreadStats stats;
  std::mt19937_64 rng(std::random_device{}());
  std::uniform_real_distribution<double> action_dist(0.0, 1.0);
  std::uniform_int_distribution<uint64_t> key_dist(0, key_space - 1);
  rocksdb::ReadOptions read_options;
  read_options.fill_cache = false;
  rocksdb::WriteOptions write_options;
  while (std::chrono::steady_clock::now() < end_time) {
    const double pick = action_dist(rng);
    const std::string key = std::to_string(key_dist(rng));
    if (pick < read_ratio) {
      std::string value;
      auto status = db->Get(read_options, key, &value);
      if (!status.ok() && !status.IsNotFound()) {
        throw std::runtime_error("Read failed: " + status.ToString());
      }
      ++stats.reads;
    } else {
      if (use_merge) {
        auto status = db->Merge(write_options, key, Encode(1));
        if (!status.ok()) {
          throw std::runtime_error("Merge failed: " + status.ToString());
        }
        ++stats.merge_operands;
      } else {
        std::string value;
        uint64_t current = 0;
        auto get_status = db->Get(read_options, key, &value);
        if (get_status.ok()) {
          current = Decode(rocksdb::Slice(value));
        } else if (!get_status.IsNotFound()) {
          throw std::runtime_error("RMW read failed: " + get_status.ToString());
        }
        auto status = db->Put(write_options, key, Encode(current + 1));
        if (!status.ok()) {
          throw std::runtime_error("Put failed: " + status.ToString());
        }
      }
      ++stats.writes;
    }
  }
  return stats;
}

std::vector<Metrics> RunPhase(const Config& cfg, rocksdb::DB* db, bool use_merge,
                              const std::vector<Workload>& workloads) {
  std::vector<Metrics> metrics;
  metrics.reserve(workloads.size());
  for (const auto& workload : workloads) {
    std::vector<std::thread> threads;
    std::vector<ThreadStats> thread_stats(cfg.threads);
    auto end_time = std::chrono::steady_clock::now() + std::chrono::seconds(cfg.seconds_per_phase);
    for (int t = 0; t < cfg.threads; ++t) {
      threads.emplace_back([&, t]() {
        thread_stats[t] = RunWorker(db, use_merge, workload.read_ratio, cfg.key_space, end_time);
      });
    }
    for (auto& th : threads) {
      th.join();
    }
    uint64_t total_reads = 0;
    uint64_t total_writes = 0;
    uint64_t total_merge_ops = 0;
    for (const auto& s : thread_stats) {
      total_reads += s.reads;
      total_writes += s.writes;
      total_merge_ops += s.merge_operands;
    }
    const double seconds = static_cast<double>(cfg.seconds_per_phase);
    double merge_ops_per_key = use_merge && cfg.key_space > 0
                                   ? static_cast<double>(total_merge_ops) /
                                         static_cast<double>(cfg.key_space)
                                   : 0.0;
    metrics.push_back(Metrics{total_reads / seconds, total_writes / seconds, merge_ops_per_key});
  }
  return metrics;
}

void PrintResults(const std::string& title, const std::vector<Metrics>& metrics,
                  const std::vector<Workload>& workloads) {
  std::cout << "== " << title << " ==\n";
  std::cout << std::setw(10) << "Mix" << std::setw(15) << "Reads/s" << std::setw(15) << "Writes/s"
            << std::setw(20) << "Merge Ops/Key" << "\n";
  for (std::size_t i = 0; i < workloads.size(); ++i) {
    std::cout << std::setw(10) << workloads[i].name
              << std::setw(15) << std::llround(metrics[i].read_ops_per_sec)
              << std::setw(15) << std::llround(metrics[i].write_ops_per_sec)
              << std::setw(20) << std::fixed << std::setprecision(2) << metrics[i].avg_merge_ops_per_key
              << "\n";
  }
}

void RunBenchmark(const Config& cfg, bool use_merge, const std::vector<Workload>& workloads) {
  const std::filesystem::path db_path = cfg.db_root / (use_merge ? "merge" : "rmw");
  if (std::filesystem::exists(db_path)) {
    std::filesystem::remove_all(db_path);
  }
  std::filesystem::create_directories(cfg.db_root);
  rocksdb::Options options = BuildOptions(use_merge);
  rocksdb::DB* raw_db = nullptr;
  auto status = rocksdb::DB::Open(options, db_path.string(), &raw_db);
  if (!status.ok()) {
    throw std::runtime_error("Failed to open DB: " + status.ToString());
  }
  std::unique_ptr<rocksdb::DB> db(raw_db);
  Prepopulate(db.get(), cfg.key_space);
  auto metrics = RunPhase(cfg, db.get(), use_merge, workloads);
  PrintResults(use_merge ? "Merge" : "Read-Modify-Write", metrics, workloads);
  db.reset();
  std::filesystem::remove_all(db_path);
}

Config ParseArguments(int argc, char** argv) {
  Config cfg;
  for (int i = 1; i < argc; ++i) {
    std::string arg(argv[i]);
    if (arg.rfind("--db_root=", 0) == 0) {
      cfg.db_root = arg.substr(std::string("--db_root=").size());
    } else if (arg.rfind("--keys=", 0) == 0) {
      cfg.key_space = std::stoull(arg.substr(std::string("--keys=").size()));
    } else if (arg.rfind("--threads=", 0) == 0) {
      cfg.threads = std::stoi(arg.substr(std::string("--threads=").size()));
    } else if (arg.rfind("--seconds=", 0) == 0) {
      cfg.seconds_per_phase = std::stoi(arg.substr(std::string("--seconds=").size()));
    } else if (arg.rfind("--mix=", 0) == 0) {
      cfg.mix_filter = arg.substr(std::string("--mix=").size());
    } else if (arg == "--help" || arg == "-h") {
      std::cout << "Usage: merge_bench [--db_root=dir] [--keys=N] [--threads=N] [--seconds=N] [--mix=ratio]\n";
      std::exit(EXIT_SUCCESS);
    } else {
      std::cerr << "Unknown argument: " << arg << "\n";
      std::exit(EXIT_FAILURE);
    }
  }
  return cfg;
}

}  // namespace

int main(int argc, char** argv) {
  try {
    Config cfg = ParseArguments(argc, argv);
    auto workloads = SelectWorkloads(cfg.mix_filter);
    RunBenchmark(cfg, /*use_merge=*/false, workloads);
    RunBenchmark(cfg, /*use_merge=*/true, workloads);
  } catch (const std::exception& ex) {
    std::cerr << "Error: " << ex.what() << "\n";
    return EXIT_FAILURE;
  }
  return EXIT_SUCCESS;
}
