#ifndef KVSTORE_INTERNAL_RECENT_METRICS_H
#define KVSTORE_INTERNAL_RECENT_METRICS_H

#include "kvstore.h"

#include <atomic>
#include <cstdint>
#include <deque>

namespace kvstore::internal {

void update_observed_fsync_pressure_metric(
    std::atomic<uint64_t>& observed_fsync_pressure_per_1000_writes,
    size_t batch_size);

struct RecentBatchHistoryContext {
    const KVStoreOptions& options;
    uint64_t total_read_requests;
    uint64_t current_batch_wal_bytes;
    size_t batch_size;
    uint64_t queue_depth;
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
    std::atomic<uint64_t>& observed_fsync_pressure_per_1000_writes;
};

void update_recent_batch_history(const RecentBatchHistoryContext& context);

}  // namespace kvstore::internal

#endif  // KVSTORE_INTERNAL_RECENT_METRICS_H
