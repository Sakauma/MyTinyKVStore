#ifndef KVSTORE_H
#define KVSTORE_H

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

struct Value {
    std::vector<uint8_t> bytes;

    Value() = default;
    explicit Value(std::vector<uint8_t> data) : bytes(std::move(data)) {}

    bool operator==(const Value& other) const {
        return bytes == other.bytes;
    }
};

class KVStoreError : public std::runtime_error {
public:
    explicit KVStoreError(const std::string& message) : std::runtime_error(message) {}
};

enum class KVStoreProfile {
    kBalanced,
    kWriteHeavy,
    kReadHeavy,
    kLowLatency,
};

struct KVStoreOptions {
    size_t max_batch_size = 64;
    uint64_t max_batch_wal_bytes = 0;
    uint32_t max_batch_delay_us = 1000;
    size_t adaptive_recent_window_batches = 64;
    size_t adaptive_recent_write_sample_limit = 512;
    bool adaptive_objective_enabled = false;
    uint32_t adaptive_objective_queue_weight = 1;
    uint32_t adaptive_objective_latency_weight = 2;
    uint32_t adaptive_objective_read_weight = 1;
    uint32_t adaptive_objective_throughput_weight = 1;
    size_t adaptive_objective_target_batch_size = 0;
    uint32_t adaptive_objective_fsync_weight = 1;
    uint32_t adaptive_objective_compaction_weight = 1;
    uint32_t adaptive_objective_wal_growth_weight = 1;
    uint64_t adaptive_objective_short_delay_score_threshold = 1000;
    uint64_t adaptive_objective_long_delay_score_threshold = 1000;
    uint32_t adaptive_objective_short_delay_divisor = 2;
    uint32_t adaptive_objective_long_delay_multiplier = 2;
    uint32_t adaptive_objective_max_batch_delay_us = 0;
    uint32_t adaptive_read_heavy_read_per_1000_ops_threshold = 0;
    uint32_t adaptive_read_heavy_delay_divisor = 2;
    size_t adaptive_read_heavy_batch_size_divisor = 2;
    bool adaptive_flush_enabled = false;
    size_t adaptive_flush_queue_depth_threshold = 8;
    uint32_t adaptive_flush_delay_divisor = 4;
    uint32_t adaptive_flush_min_batch_delay_us = 100;
    uint64_t adaptive_latency_target_p95_us = 0;
    uint64_t adaptive_fsync_pressure_per_1000_writes_threshold = 0;
    uint32_t adaptive_fsync_pressure_delay_multiplier = 2;
    uint32_t adaptive_fsync_pressure_max_batch_delay_us = 0;
    uint32_t adaptive_compaction_pressure_obsolete_ratio_percent_threshold = 0;
    uint32_t adaptive_compaction_pressure_delay_multiplier = 2;
    uint64_t adaptive_wal_growth_bytes_per_batch_threshold = 0;
    uint32_t adaptive_wal_growth_delay_multiplier = 2;
    uint32_t adaptive_wal_growth_max_batch_delay_us = 0;
    uint64_t auto_compact_wal_bytes_threshold = 0;
    uint32_t auto_compact_invalid_wal_ratio_percent = 0;
    bool adaptive_batching_enabled = false;
    size_t adaptive_queue_depth_threshold = 32;
    size_t adaptive_batch_size_multiplier = 4;
    uint64_t adaptive_batch_wal_bytes_multiplier = 4;
};

constexpr size_t kWriteLatencyBucketCount = 12;

struct KVStoreMetrics {
    uint64_t read_requests = 0;
    uint64_t enqueued_write_requests = 0;
    uint64_t committed_write_requests = 0;
    uint64_t committed_write_batches = 0;
    uint64_t compact_requests = 0;
    uint64_t wal_fsync_calls = 0;
    uint64_t wal_bytes_written = 0;
    uint64_t wal_bytes_since_compaction = 0;
    uint64_t live_wal_bytes_since_compaction = 0;
    uint64_t obsolete_wal_bytes_since_compaction = 0;
    uint64_t last_committed_batch_size = 0;
    uint64_t max_committed_batch_size = 0;
    uint64_t last_committed_batch_wal_bytes = 0;
    uint64_t max_committed_batch_wal_bytes = 0;
    uint64_t pending_queue_depth = 0;
    uint64_t max_pending_queue_depth = 0;
    uint64_t manual_compactions_completed = 0;
    uint64_t auto_compactions_completed = 0;
    uint64_t adaptive_batches_completed = 0;
    uint64_t adaptive_flush_batches_completed = 0;
    uint64_t adaptive_latency_target_batches_completed = 0;
    uint64_t adaptive_fsync_pressure_batches_completed = 0;
    uint64_t adaptive_read_heavy_batches_completed = 0;
    uint64_t adaptive_compaction_pressure_batches_completed = 0;
    uint64_t adaptive_wal_growth_batches_completed = 0;
    uint64_t adaptive_objective_short_delay_batches_completed = 0;
    uint64_t adaptive_objective_long_delay_batches_completed = 0;
    uint64_t adaptive_objective_throughput_batches_completed = 0;
    uint64_t writer_wait_events = 0;
    uint64_t writer_wait_time_us = 0;
    uint64_t last_effective_batch_delay_us = 0;
    uint64_t min_effective_batch_delay_us = 0;
    uint64_t max_effective_batch_delay_us = 0;
    uint64_t observed_fsync_pressure_per_1000_writes = 0;
    uint64_t last_objective_pressure_score = 0;
    uint64_t last_objective_cost_score = 0;
    uint64_t last_objective_throughput_score = 0;
    int64_t last_objective_balance_score = 0;
    int64_t last_objective_mode = 0;
    uint64_t total_snapshot_bytes_written = 0;
    uint64_t total_wal_bytes_reclaimed_by_compaction = 0;
    uint64_t approx_write_latency_p50_us = 0;
    uint64_t approx_write_latency_p95_us = 0;
    uint64_t approx_write_latency_p99_us = 0;
    uint64_t recent_read_requests = 0;
    uint64_t recent_write_requests = 0;
    uint64_t recent_read_ratio_per_1000_ops = 0;
    uint64_t recent_observed_write_latency_p95_us = 0;
    uint64_t recent_peak_queue_depth = 0;
    uint64_t recent_avg_batch_size = 0;
    uint64_t recent_batch_fill_per_1000 = 0;
    uint64_t recent_avg_batch_wal_bytes = 0;
    uint64_t recent_window_batch_count = 0;
    uint64_t observed_obsolete_wal_ratio_percent = 0;
    std::array<uint64_t, kWriteLatencyBucketCount> write_latency_histogram {};
};

std::string MetricsToJson(const KVStoreMetrics& metrics);
KVStoreOptions RecommendedOptions(KVStoreProfile profile);
std::string OptionsToJson(const KVStoreOptions& options);

class KVStore {
public:
    explicit KVStore(const std::string& db_path);
    KVStore(const std::string& db_path, KVStoreOptions options);
    ~KVStore();

    void Put(int key, Value value);
    void Put(const std::string& key, Value value);
    std::optional<Value> Get(int key);
    std::optional<Value> Get(const std::string& key);
    void Delete(int key);
    void Delete(const std::string& key);
    std::vector<std::pair<std::string, Value>> Scan(const std::string& start_key, const std::string& end_key);
    void Compact();
    KVStoreMetrics GetMetrics();

private:
    class Impl;
    std::unique_ptr<Impl> pimpl_;
};

#endif  // KVSTORE_H
