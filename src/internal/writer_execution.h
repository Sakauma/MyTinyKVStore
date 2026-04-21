#ifndef KVSTORE_INTERNAL_WRITER_EXECUTION_H
#define KVSTORE_INTERNAL_WRITER_EXECUTION_H

#include "kvstore.h"
#include "request_runtime.h"
#include "writer_policy.h"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <vector>

namespace kvstore::internal {

using StateMap = std::map<std::string, Value>;

uint64_t current_wal_file_size(const std::string& wal_file_path);
void open_wal_for_append(int& wal_fd, const std::string& wal_file_path);
void open_wal_for_append_if_needed(int& wal_fd, const std::string& wal_file_path);
uint64_t append_wal_record(int wal_fd, WalRecordType type, const std::string& key, const Value& value);
uint64_t wal_record_size_for_request(const Request& request);

void collect_write_batch(
    std::unique_lock<std::mutex>& lock,
    RequestQueue& request_queue,
    bool stop,
    const BatchPolicy& batch_policy,
    const std::chrono::steady_clock::time_point& deadline,
    const std::function<bool(std::unique_lock<std::mutex>&, const std::chrono::steady_clock::time_point&)>&
        wait_for_queue_activity_until,
    std::vector<RequestPtr>& write_batch);

struct WriteBatchExecutionContext {
    int& wal_fd;
    const std::string& wal_file_path;
    StateMap& state;
    std::shared_mutex& state_mutex;
    std::atomic<uint64_t>& committed_write_requests;
    std::atomic<uint64_t>& committed_write_batches;
    std::atomic<uint64_t>& wal_fsync_calls;
    std::atomic<uint64_t>& wal_bytes_written;
    std::atomic<uint64_t>& wal_bytes_since_compaction;
    std::atomic<uint64_t>& last_committed_batch_size;
    std::atomic<uint64_t>& max_committed_batch_size;
    std::atomic<uint64_t>& last_committed_batch_wal_bytes;
    std::atomic<uint64_t>& max_committed_batch_wal_bytes;
    std::atomic<uint64_t>& last_effective_batch_delay_us;
    std::atomic<uint64_t>& min_effective_batch_delay_us;
    std::atomic<uint64_t>& max_effective_batch_delay_us;
    std::atomic<uint64_t>& observed_fsync_pressure_per_1000_writes;
    uint32_t current_batch_delay_us;
    uint64_t current_batch_queue_depth;
    const KVStoreOptions& options;
    uint64_t current_total_read_requests;
    uint64_t& last_total_read_requests_seen;
    std::deque<uint64_t>& recent_batch_sizes;
    std::deque<uint64_t>& recent_batch_queue_depths;
    std::deque<uint64_t>& recent_batch_wal_bytes;
    uint64_t& recent_batch_wal_bytes_sum;
    std::deque<uint64_t>& recent_read_deltas;
    std::deque<uint64_t>& recent_write_deltas;
    uint64_t& recent_read_sum;
    uint64_t& recent_write_sum;
    uint64_t& recent_batch_size_sum;
    std::atomic<uint64_t>& recent_read_requests;
    std::atomic<uint64_t>& recent_write_requests;
    std::atomic<uint64_t>& recent_read_ratio_per_1000_ops;
    std::atomic<uint64_t>& recent_peak_queue_depth;
    std::atomic<uint64_t>& recent_avg_batch_size;
    std::atomic<uint64_t>& recent_batch_fill_per_1000;
    std::atomic<uint64_t>& recent_avg_batch_wal_bytes;
    std::atomic<uint64_t>& recent_window_batch_count;
    const RequestCompletionCallbacks& completion_callbacks;
    std::function<void(const std::string&, uint64_t)> note_latest_wal_record;
};

void process_write_batch(const std::vector<RequestPtr>& batch, WriteBatchExecutionContext context);

struct CompactionExecutionContext {
    int& wal_fd;
    const std::string& db_file_path;
    const std::string& wal_file_path;
    StateMap& state;
    std::shared_mutex& state_mutex;
    std::atomic<uint64_t>& wal_bytes_since_compaction;
    std::atomic<uint64_t>& live_wal_bytes_since_compaction;
    std::map<std::string, uint64_t>& latest_wal_record_bytes;
    std::atomic<uint64_t>& total_snapshot_bytes_written;
    std::atomic<uint64_t>& total_wal_bytes_reclaimed_by_compaction;
    std::atomic<uint64_t>& manual_compactions_completed;
    std::atomic<uint64_t>& auto_compactions_completed;
    const RequestCompletionCallbacks& completion_callbacks;
};

void process_compaction_request(
    const RequestPtr& request,
    bool is_auto_compaction,
    CompactionExecutionContext context);

struct AutoCompactionExecutionContext {
    const KVStoreOptions& options;
    std::atomic<uint64_t>& wal_bytes_since_compaction;
    std::atomic<uint64_t>& live_wal_bytes_since_compaction;
    std::function<void(const RequestPtr&, bool)> process_compaction_request;
};

bool maybe_auto_compact(AutoCompactionExecutionContext context);

}  // namespace kvstore::internal

#endif  // KVSTORE_INTERNAL_WRITER_EXECUTION_H
