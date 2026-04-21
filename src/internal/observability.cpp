#include "observability.h"

#include <sstream>

namespace kvstore::internal {

std::string metrics_to_json(const KVStoreMetrics& metrics) {
    std::ostringstream out;
    out << '{'
        << "\"read_requests\":" << metrics.read_requests
        << ",\"enqueued_write_requests\":" << metrics.enqueued_write_requests
        << ",\"committed_write_requests\":" << metrics.committed_write_requests
        << ",\"committed_write_batches\":" << metrics.committed_write_batches
        << ",\"compact_requests\":" << metrics.compact_requests
        << ",\"wal_fsync_calls\":" << metrics.wal_fsync_calls
        << ",\"wal_bytes_written\":" << metrics.wal_bytes_written
        << ",\"wal_bytes_since_compaction\":" << metrics.wal_bytes_since_compaction
        << ",\"live_wal_bytes_since_compaction\":" << metrics.live_wal_bytes_since_compaction
        << ",\"obsolete_wal_bytes_since_compaction\":" << metrics.obsolete_wal_bytes_since_compaction
        << ",\"last_committed_batch_size\":" << metrics.last_committed_batch_size
        << ",\"max_committed_batch_size\":" << metrics.max_committed_batch_size
        << ",\"last_committed_batch_wal_bytes\":" << metrics.last_committed_batch_wal_bytes
        << ",\"max_committed_batch_wal_bytes\":" << metrics.max_committed_batch_wal_bytes
        << ",\"pending_queue_depth\":" << metrics.pending_queue_depth
        << ",\"max_pending_queue_depth\":" << metrics.max_pending_queue_depth
        << ",\"manual_compactions_completed\":" << metrics.manual_compactions_completed
        << ",\"auto_compactions_completed\":" << metrics.auto_compactions_completed
        << ",\"adaptive_batches_completed\":" << metrics.adaptive_batches_completed
        << ",\"adaptive_flush_batches_completed\":" << metrics.adaptive_flush_batches_completed
        << ",\"adaptive_latency_target_batches_completed\":" << metrics.adaptive_latency_target_batches_completed
        << ",\"adaptive_fsync_pressure_batches_completed\":" << metrics.adaptive_fsync_pressure_batches_completed
        << ",\"adaptive_read_heavy_batches_completed\":" << metrics.adaptive_read_heavy_batches_completed
        << ",\"adaptive_compaction_pressure_batches_completed\":" << metrics.adaptive_compaction_pressure_batches_completed
        << ",\"adaptive_wal_growth_batches_completed\":" << metrics.adaptive_wal_growth_batches_completed
        << ",\"adaptive_objective_short_delay_batches_completed\":"
        << metrics.adaptive_objective_short_delay_batches_completed
        << ",\"adaptive_objective_long_delay_batches_completed\":"
        << metrics.adaptive_objective_long_delay_batches_completed
        << ",\"adaptive_objective_throughput_batches_completed\":"
        << metrics.adaptive_objective_throughput_batches_completed
        << ",\"writer_wait_events\":" << metrics.writer_wait_events
        << ",\"writer_wait_time_us\":" << metrics.writer_wait_time_us
        << ",\"last_effective_batch_delay_us\":" << metrics.last_effective_batch_delay_us
        << ",\"min_effective_batch_delay_us\":" << metrics.min_effective_batch_delay_us
        << ",\"max_effective_batch_delay_us\":" << metrics.max_effective_batch_delay_us
        << ",\"observed_fsync_pressure_per_1000_writes\":"
        << metrics.observed_fsync_pressure_per_1000_writes
        << ",\"last_objective_pressure_score\":" << metrics.last_objective_pressure_score
        << ",\"last_objective_cost_score\":" << metrics.last_objective_cost_score
        << ",\"last_objective_throughput_score\":" << metrics.last_objective_throughput_score
        << ",\"last_objective_balance_score\":" << metrics.last_objective_balance_score
        << ",\"last_objective_mode\":" << metrics.last_objective_mode
        << ",\"total_snapshot_bytes_written\":" << metrics.total_snapshot_bytes_written
        << ",\"total_wal_bytes_reclaimed_by_compaction\":" << metrics.total_wal_bytes_reclaimed_by_compaction
        << ",\"approx_write_latency_p50_us\":" << metrics.approx_write_latency_p50_us
        << ",\"approx_write_latency_p95_us\":" << metrics.approx_write_latency_p95_us
        << ",\"approx_write_latency_p99_us\":" << metrics.approx_write_latency_p99_us
        << ",\"recent_read_requests\":" << metrics.recent_read_requests
        << ",\"recent_write_requests\":" << metrics.recent_write_requests
        << ",\"recent_read_ratio_per_1000_ops\":" << metrics.recent_read_ratio_per_1000_ops
        << ",\"recent_observed_write_latency_p95_us\":" << metrics.recent_observed_write_latency_p95_us
        << ",\"recent_peak_queue_depth\":" << metrics.recent_peak_queue_depth
        << ",\"recent_avg_batch_size\":" << metrics.recent_avg_batch_size
        << ",\"recent_batch_fill_per_1000\":" << metrics.recent_batch_fill_per_1000
        << ",\"recent_avg_batch_wal_bytes\":" << metrics.recent_avg_batch_wal_bytes
        << ",\"recent_window_batch_count\":" << metrics.recent_window_batch_count
        << ",\"observed_obsolete_wal_ratio_percent\":" << metrics.observed_obsolete_wal_ratio_percent
        << ",\"write_latency_histogram\":[";
    for (size_t i = 0; i < metrics.write_latency_histogram.size(); ++i) {
        if (i != 0) {
            out << ',';
        }
        out << metrics.write_latency_histogram[i];
    }
    out << "]}";
    return out.str();
}

KVStoreOptions recommended_options(KVStoreProfile profile) {
    KVStoreOptions options;
    options.max_batch_size = 32;
    options.max_batch_wal_bytes = 1 << 20;
    options.max_batch_delay_us = 2000;
    options.adaptive_recent_window_batches = 64;
    options.adaptive_recent_write_sample_limit = 512;
    options.auto_compact_wal_bytes_threshold = 1 << 20;
    options.auto_compact_invalid_wal_ratio_percent = 60;
    options.adaptive_batching_enabled = true;
    options.adaptive_queue_depth_threshold = 8;
    options.adaptive_batch_size_multiplier = 4;
    options.adaptive_batch_wal_bytes_multiplier = 4;
    options.adaptive_flush_enabled = true;
    options.adaptive_flush_queue_depth_threshold = 8;
    options.adaptive_flush_delay_divisor = 4;
    options.adaptive_flush_min_batch_delay_us = 100;
    options.adaptive_latency_target_p95_us = 12000;
    options.adaptive_fsync_pressure_per_1000_writes_threshold = 350;
    options.adaptive_fsync_pressure_delay_multiplier = 2;
    options.adaptive_fsync_pressure_max_batch_delay_us = 8000;
    options.adaptive_compaction_pressure_obsolete_ratio_percent_threshold = 50;
    options.adaptive_compaction_pressure_delay_multiplier = 2;
    options.adaptive_wal_growth_bytes_per_batch_threshold = 200;
    options.adaptive_wal_growth_delay_multiplier = 2;
    options.adaptive_wal_growth_max_batch_delay_us = 6000;
    options.adaptive_objective_enabled = true;
    options.adaptive_objective_queue_weight = 1;
    options.adaptive_objective_latency_weight = 3;
    options.adaptive_objective_read_weight = 2;
    options.adaptive_objective_throughput_weight = 2;
    options.adaptive_objective_target_batch_size = 16;
    options.adaptive_objective_fsync_weight = 2;
    options.adaptive_objective_compaction_weight = 1;
    options.adaptive_objective_wal_growth_weight = 1;
    options.adaptive_objective_short_delay_score_threshold = 500;
    options.adaptive_objective_long_delay_score_threshold = 500;
    options.adaptive_objective_short_delay_divisor = 2;
    options.adaptive_objective_long_delay_multiplier = 2;
    options.adaptive_objective_max_batch_delay_us = 8000;
    options.adaptive_read_heavy_read_per_1000_ops_threshold = 700;
    options.adaptive_read_heavy_delay_divisor = 4;
    options.adaptive_read_heavy_batch_size_divisor = 2;

    switch (profile) {
        case KVStoreProfile::kBalanced:
            return options;
        case KVStoreProfile::kWriteHeavy:
            options.max_batch_size = 64;
            options.max_batch_wal_bytes = 2 << 20;
            options.max_batch_delay_us = 4000;
            options.adaptive_objective_target_batch_size = 32;
            options.adaptive_objective_throughput_weight = 4;
            options.adaptive_objective_fsync_weight = 3;
            options.adaptive_flush_queue_depth_threshold = 16;
            options.auto_compact_wal_bytes_threshold = 4 << 20;
            options.auto_compact_invalid_wal_ratio_percent = 70;
            return options;
        case KVStoreProfile::kReadHeavy:
            options.max_batch_size = 16;
            options.max_batch_delay_us = 1000;
            options.adaptive_objective_read_weight = 4;
            options.adaptive_objective_latency_weight = 4;
            options.adaptive_objective_throughput_weight = 1;
            options.adaptive_read_heavy_read_per_1000_ops_threshold = 600;
            options.adaptive_read_heavy_delay_divisor = 8;
            options.adaptive_read_heavy_batch_size_divisor = 4;
            options.adaptive_flush_queue_depth_threshold = 4;
            return options;
        case KVStoreProfile::kLowLatency:
            options.max_batch_size = 8;
            options.max_batch_wal_bytes = 64 << 10;
            options.max_batch_delay_us = 500;
            options.adaptive_objective_latency_weight = 5;
            options.adaptive_objective_short_delay_divisor = 4;
            options.adaptive_objective_long_delay_score_threshold = 1500;
            options.adaptive_objective_target_batch_size = 8;
            options.adaptive_objective_throughput_weight = 1;
            options.adaptive_flush_queue_depth_threshold = 2;
            options.adaptive_flush_delay_divisor = 8;
            options.adaptive_flush_min_batch_delay_us = 50;
            options.adaptive_latency_target_p95_us = 4000;
            return options;
    }

    return options;
}

std::string options_to_json(const KVStoreOptions& options) {
    std::ostringstream out;
    out << '{'
        << "\"max_batch_size\":" << options.max_batch_size
        << ",\"max_batch_wal_bytes\":" << options.max_batch_wal_bytes
        << ",\"max_batch_delay_us\":" << options.max_batch_delay_us
        << ",\"adaptive_recent_window_batches\":" << options.adaptive_recent_window_batches
        << ",\"adaptive_recent_write_sample_limit\":" << options.adaptive_recent_write_sample_limit
        << ",\"adaptive_objective_enabled\":" << (options.adaptive_objective_enabled ? "true" : "false")
        << ",\"adaptive_objective_queue_weight\":" << options.adaptive_objective_queue_weight
        << ",\"adaptive_objective_latency_weight\":" << options.adaptive_objective_latency_weight
        << ",\"adaptive_objective_read_weight\":" << options.adaptive_objective_read_weight
        << ",\"adaptive_objective_throughput_weight\":" << options.adaptive_objective_throughput_weight
        << ",\"adaptive_objective_target_batch_size\":" << options.adaptive_objective_target_batch_size
        << ",\"adaptive_objective_fsync_weight\":" << options.adaptive_objective_fsync_weight
        << ",\"adaptive_objective_compaction_weight\":" << options.adaptive_objective_compaction_weight
        << ",\"adaptive_objective_wal_growth_weight\":" << options.adaptive_objective_wal_growth_weight
        << ",\"adaptive_objective_short_delay_score_threshold\":"
        << options.adaptive_objective_short_delay_score_threshold
        << ",\"adaptive_objective_long_delay_score_threshold\":"
        << options.adaptive_objective_long_delay_score_threshold
        << ",\"adaptive_objective_short_delay_divisor\":" << options.adaptive_objective_short_delay_divisor
        << ",\"adaptive_objective_long_delay_multiplier\":" << options.adaptive_objective_long_delay_multiplier
        << ",\"adaptive_objective_max_batch_delay_us\":" << options.adaptive_objective_max_batch_delay_us
        << ",\"adaptive_read_heavy_read_per_1000_ops_threshold\":"
        << options.adaptive_read_heavy_read_per_1000_ops_threshold
        << ",\"adaptive_read_heavy_delay_divisor\":" << options.adaptive_read_heavy_delay_divisor
        << ",\"adaptive_read_heavy_batch_size_divisor\":" << options.adaptive_read_heavy_batch_size_divisor
        << ",\"adaptive_flush_enabled\":" << (options.adaptive_flush_enabled ? "true" : "false")
        << ",\"adaptive_flush_queue_depth_threshold\":" << options.adaptive_flush_queue_depth_threshold
        << ",\"adaptive_flush_delay_divisor\":" << options.adaptive_flush_delay_divisor
        << ",\"adaptive_flush_min_batch_delay_us\":" << options.adaptive_flush_min_batch_delay_us
        << ",\"adaptive_latency_target_p95_us\":" << options.adaptive_latency_target_p95_us
        << ",\"adaptive_fsync_pressure_per_1000_writes_threshold\":"
        << options.adaptive_fsync_pressure_per_1000_writes_threshold
        << ",\"adaptive_fsync_pressure_delay_multiplier\":" << options.adaptive_fsync_pressure_delay_multiplier
        << ",\"adaptive_fsync_pressure_max_batch_delay_us\":" << options.adaptive_fsync_pressure_max_batch_delay_us
        << ",\"adaptive_compaction_pressure_obsolete_ratio_percent_threshold\":"
        << options.adaptive_compaction_pressure_obsolete_ratio_percent_threshold
        << ",\"adaptive_compaction_pressure_delay_multiplier\":" << options.adaptive_compaction_pressure_delay_multiplier
        << ",\"adaptive_wal_growth_bytes_per_batch_threshold\":"
        << options.adaptive_wal_growth_bytes_per_batch_threshold
        << ",\"adaptive_wal_growth_delay_multiplier\":" << options.adaptive_wal_growth_delay_multiplier
        << ",\"adaptive_wal_growth_max_batch_delay_us\":" << options.adaptive_wal_growth_max_batch_delay_us
        << ",\"auto_compact_wal_bytes_threshold\":" << options.auto_compact_wal_bytes_threshold
        << ",\"auto_compact_invalid_wal_ratio_percent\":" << options.auto_compact_invalid_wal_ratio_percent
        << ",\"adaptive_batching_enabled\":" << (options.adaptive_batching_enabled ? "true" : "false")
        << ",\"adaptive_queue_depth_threshold\":" << options.adaptive_queue_depth_threshold
        << ",\"adaptive_batch_size_multiplier\":" << options.adaptive_batch_size_multiplier
        << ",\"adaptive_batch_wal_bytes_multiplier\":" << options.adaptive_batch_wal_bytes_multiplier
        << '}';
    return out.str();
}

}  // namespace kvstore::internal
