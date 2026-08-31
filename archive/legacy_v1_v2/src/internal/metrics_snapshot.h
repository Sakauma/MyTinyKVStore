#ifndef KVSTORE_INTERNAL_METRICS_SNAPSHOT_H
#define KVSTORE_INTERNAL_METRICS_SNAPSHOT_H

#include "kvstore.h"

#include <array>
#include <atomic>
#include <cstdint>

namespace kvstore::internal {

struct MetricsSnapshotInputs {
    const std::atomic<uint64_t>* read_requests;
    const std::atomic<uint64_t>* enqueued_write_requests;
    const std::atomic<uint64_t>* committed_write_requests;
    const std::atomic<uint64_t>* committed_write_batches;
    const std::atomic<uint64_t>* compact_requests;
    const std::atomic<uint64_t>* wal_fsync_calls;
    const std::atomic<uint64_t>* wal_bytes_written;
    const std::atomic<uint64_t>* wal_bytes_since_compaction;
    const std::atomic<uint64_t>* live_wal_bytes_since_compaction;
    const std::atomic<uint64_t>* last_committed_batch_size;
    const std::atomic<uint64_t>* max_committed_batch_size;
    const std::atomic<uint64_t>* last_committed_batch_wal_bytes;
    const std::atomic<uint64_t>* max_committed_batch_wal_bytes;
    const std::atomic<uint64_t>* max_pending_queue_depth;
    const std::atomic<uint64_t>* manual_compactions_completed;
    const std::atomic<uint64_t>* auto_compactions_completed;
    const std::atomic<uint64_t>* adaptive_batches_completed;
    const std::atomic<uint64_t>* adaptive_flush_batches_completed;
    const std::atomic<uint64_t>* adaptive_latency_target_batches_completed;
    const std::atomic<uint64_t>* adaptive_fsync_pressure_batches_completed;
    const std::atomic<uint64_t>* adaptive_read_heavy_batches_completed;
    const std::atomic<uint64_t>* adaptive_compaction_pressure_batches_completed;
    const std::atomic<uint64_t>* adaptive_wal_growth_batches_completed;
    const std::atomic<uint64_t>* adaptive_objective_short_delay_batches_completed;
    const std::atomic<uint64_t>* adaptive_objective_long_delay_batches_completed;
    const std::atomic<uint64_t>* adaptive_objective_throughput_batches_completed;
    const std::atomic<uint64_t>* writer_wait_events;
    const std::atomic<uint64_t>* writer_wait_time_us;
    const std::atomic<uint64_t>* last_effective_batch_delay_us;
    const std::atomic<uint64_t>* min_effective_batch_delay_us;
    const std::atomic<uint64_t>* max_effective_batch_delay_us;
    const std::atomic<uint64_t>* observed_fsync_pressure_per_1000_writes;
    const std::atomic<uint64_t>* last_objective_pressure_score;
    const std::atomic<uint64_t>* last_objective_cost_score;
    const std::atomic<uint64_t>* last_objective_throughput_score;
    const std::atomic<int64_t>* last_objective_balance_score;
    const std::atomic<int64_t>* last_objective_mode;
    const std::atomic<uint64_t>* total_snapshot_bytes_written;
    const std::atomic<uint64_t>* total_wal_bytes_reclaimed_by_compaction;
    const std::array<std::atomic<uint64_t>, kWriteLatencyBucketCount>* write_latency_histogram;
    const std::atomic<uint64_t>* recent_read_requests;
    const std::atomic<uint64_t>* recent_write_requests;
    const std::atomic<uint64_t>* recent_read_ratio_per_1000_ops;
    const std::atomic<uint64_t>* recent_observed_write_latency_p95_us;
    const std::atomic<uint64_t>* recent_peak_queue_depth;
    const std::atomic<uint64_t>* recent_avg_batch_size;
    const std::atomic<uint64_t>* recent_batch_fill_per_1000;
    const std::atomic<uint64_t>* recent_avg_batch_wal_bytes;
    const std::atomic<uint64_t>* recent_window_batch_count;
    uint64_t observed_obsolete_wal_ratio_percent;
    uint64_t pending_queue_depth;
};

KVStoreMetrics collect_metrics_snapshot(const MetricsSnapshotInputs& inputs);

}  // namespace kvstore::internal

#endif  // KVSTORE_INTERNAL_METRICS_SNAPSHOT_H
