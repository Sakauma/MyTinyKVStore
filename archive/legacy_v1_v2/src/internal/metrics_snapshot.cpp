#include "metrics_snapshot.h"

#include "metrics_helpers.h"

namespace kvstore::internal {

KVStoreMetrics collect_metrics_snapshot(const MetricsSnapshotInputs& inputs) {
    KVStoreMetrics metrics;
    metrics.read_requests = inputs.read_requests->load(std::memory_order_relaxed);
    metrics.enqueued_write_requests = inputs.enqueued_write_requests->load(std::memory_order_relaxed);
    metrics.committed_write_requests = inputs.committed_write_requests->load(std::memory_order_relaxed);
    metrics.committed_write_batches = inputs.committed_write_batches->load(std::memory_order_relaxed);
    metrics.compact_requests = inputs.compact_requests->load(std::memory_order_relaxed);
    metrics.wal_fsync_calls = inputs.wal_fsync_calls->load(std::memory_order_relaxed);
    metrics.wal_bytes_written = inputs.wal_bytes_written->load(std::memory_order_relaxed);
    metrics.wal_bytes_since_compaction = inputs.wal_bytes_since_compaction->load(std::memory_order_relaxed);
    metrics.live_wal_bytes_since_compaction =
        inputs.live_wal_bytes_since_compaction->load(std::memory_order_relaxed);
    metrics.obsolete_wal_bytes_since_compaction =
        metrics.wal_bytes_since_compaction >= metrics.live_wal_bytes_since_compaction
            ? metrics.wal_bytes_since_compaction - metrics.live_wal_bytes_since_compaction
            : 0;
    metrics.last_committed_batch_size = inputs.last_committed_batch_size->load(std::memory_order_relaxed);
    metrics.max_committed_batch_size = inputs.max_committed_batch_size->load(std::memory_order_relaxed);
    metrics.last_committed_batch_wal_bytes = inputs.last_committed_batch_wal_bytes->load(std::memory_order_relaxed);
    metrics.max_committed_batch_wal_bytes = inputs.max_committed_batch_wal_bytes->load(std::memory_order_relaxed);
    metrics.max_pending_queue_depth = inputs.max_pending_queue_depth->load(std::memory_order_relaxed);
    metrics.manual_compactions_completed = inputs.manual_compactions_completed->load(std::memory_order_relaxed);
    metrics.auto_compactions_completed = inputs.auto_compactions_completed->load(std::memory_order_relaxed);
    metrics.adaptive_batches_completed = inputs.adaptive_batches_completed->load(std::memory_order_relaxed);
    metrics.adaptive_flush_batches_completed =
        inputs.adaptive_flush_batches_completed->load(std::memory_order_relaxed);
    metrics.adaptive_latency_target_batches_completed =
        inputs.adaptive_latency_target_batches_completed->load(std::memory_order_relaxed);
    metrics.adaptive_fsync_pressure_batches_completed =
        inputs.adaptive_fsync_pressure_batches_completed->load(std::memory_order_relaxed);
    metrics.adaptive_read_heavy_batches_completed =
        inputs.adaptive_read_heavy_batches_completed->load(std::memory_order_relaxed);
    metrics.adaptive_compaction_pressure_batches_completed =
        inputs.adaptive_compaction_pressure_batches_completed->load(std::memory_order_relaxed);
    metrics.adaptive_wal_growth_batches_completed =
        inputs.adaptive_wal_growth_batches_completed->load(std::memory_order_relaxed);
    metrics.adaptive_objective_short_delay_batches_completed =
        inputs.adaptive_objective_short_delay_batches_completed->load(std::memory_order_relaxed);
    metrics.adaptive_objective_long_delay_batches_completed =
        inputs.adaptive_objective_long_delay_batches_completed->load(std::memory_order_relaxed);
    metrics.adaptive_objective_throughput_batches_completed =
        inputs.adaptive_objective_throughput_batches_completed->load(std::memory_order_relaxed);
    metrics.writer_wait_events = inputs.writer_wait_events->load(std::memory_order_relaxed);
    metrics.writer_wait_time_us = inputs.writer_wait_time_us->load(std::memory_order_relaxed);
    metrics.last_effective_batch_delay_us = inputs.last_effective_batch_delay_us->load(std::memory_order_relaxed);
    metrics.min_effective_batch_delay_us = inputs.min_effective_batch_delay_us->load(std::memory_order_relaxed);
    metrics.max_effective_batch_delay_us = inputs.max_effective_batch_delay_us->load(std::memory_order_relaxed);
    metrics.observed_fsync_pressure_per_1000_writes =
        inputs.observed_fsync_pressure_per_1000_writes->load(std::memory_order_relaxed);
    metrics.last_objective_pressure_score = inputs.last_objective_pressure_score->load(std::memory_order_relaxed);
    metrics.last_objective_cost_score = inputs.last_objective_cost_score->load(std::memory_order_relaxed);
    metrics.last_objective_throughput_score = inputs.last_objective_throughput_score->load(std::memory_order_relaxed);
    metrics.last_objective_balance_score = inputs.last_objective_balance_score->load(std::memory_order_relaxed);
    metrics.last_objective_mode = inputs.last_objective_mode->load(std::memory_order_relaxed);
    metrics.total_snapshot_bytes_written = inputs.total_snapshot_bytes_written->load(std::memory_order_relaxed);
    metrics.total_wal_bytes_reclaimed_by_compaction =
        inputs.total_wal_bytes_reclaimed_by_compaction->load(std::memory_order_relaxed);
    for (size_t i = 0; i < kWriteLatencyBucketCount; ++i) {
        metrics.write_latency_histogram[i] = (*inputs.write_latency_histogram)[i].load(std::memory_order_relaxed);
    }
    metrics.approx_write_latency_p50_us = approximate_latency_percentile_us(metrics.write_latency_histogram, 50, 100);
    metrics.approx_write_latency_p95_us = approximate_latency_percentile_us(metrics.write_latency_histogram, 95, 100);
    metrics.approx_write_latency_p99_us = approximate_latency_percentile_us(metrics.write_latency_histogram, 99, 100);
    metrics.recent_read_requests = inputs.recent_read_requests->load(std::memory_order_relaxed);
    metrics.recent_write_requests = inputs.recent_write_requests->load(std::memory_order_relaxed);
    metrics.recent_read_ratio_per_1000_ops = inputs.recent_read_ratio_per_1000_ops->load(std::memory_order_relaxed);
    metrics.recent_observed_write_latency_p95_us =
        inputs.recent_observed_write_latency_p95_us->load(std::memory_order_relaxed);
    metrics.recent_peak_queue_depth = inputs.recent_peak_queue_depth->load(std::memory_order_relaxed);
    metrics.recent_avg_batch_size = inputs.recent_avg_batch_size->load(std::memory_order_relaxed);
    metrics.recent_batch_fill_per_1000 = inputs.recent_batch_fill_per_1000->load(std::memory_order_relaxed);
    metrics.recent_avg_batch_wal_bytes = inputs.recent_avg_batch_wal_bytes->load(std::memory_order_relaxed);
    metrics.recent_window_batch_count = inputs.recent_window_batch_count->load(std::memory_order_relaxed);
    metrics.observed_obsolete_wal_ratio_percent = inputs.observed_obsolete_wal_ratio_percent;
    metrics.pending_queue_depth = inputs.pending_queue_depth;
    return metrics;
}

}  // namespace kvstore::internal
