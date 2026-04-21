#include "writer_policy.h"

#include "format.h"
#include "metrics_helpers.h"

#include <algorithm>

namespace kvstore::internal {

BatchPolicy compute_batch_policy(const KVStoreOptions& options, const WriterPolicySignals& signals) {
    BatchPolicy policy {
        options.max_batch_size,
        options.max_batch_wal_bytes,
        options.max_batch_delay_us,
        signals.request_queue_depth,
        0,
        0,
        0,
        0,
        0,
        false,
        false,
        false,
        false,
        false,
        false,
        false,
        false,
        false,
    };

    if (options.adaptive_batching_enabled &&
        (signals.request_queue_depth >= options.adaptive_queue_depth_threshold ||
         signals.recent_peak_queue_depth >= options.adaptive_queue_depth_threshold)) {
        policy.adaptive_batching = true;
        policy.max_batch_size = saturating_multiply(options.max_batch_size, options.adaptive_batch_size_multiplier);
        if (policy.max_batch_size == 0) {
            policy.max_batch_size = 1;
        }
        if (options.max_batch_wal_bytes == 0) {
            policy.max_batch_wal_bytes = 0;
        } else {
            policy.max_batch_wal_bytes = saturating_multiply(
                options.max_batch_wal_bytes,
                options.adaptive_batch_wal_bytes_multiplier);
        }
    }

    if (!options.adaptive_objective_enabled &&
        options.adaptive_flush_enabled &&
        options.max_batch_delay_us > 0 &&
        (signals.request_queue_depth >= options.adaptive_flush_queue_depth_threshold ||
         signals.recent_peak_queue_depth >= options.adaptive_flush_queue_depth_threshold)) {
        policy.adaptive_flush = true;
        size_t pressure_steps = signals.request_queue_depth / options.adaptive_flush_queue_depth_threshold;
        if (pressure_steps == 0) {
            pressure_steps = std::max<size_t>(
                1,
                signals.recent_peak_queue_depth / options.adaptive_flush_queue_depth_threshold);
        }
        uint32_t effective_delay = options.max_batch_delay_us;
        for (size_t step = 0; step < pressure_steps; ++step) {
            effective_delay = std::max(
                options.adaptive_flush_min_batch_delay_us,
                effective_delay / options.adaptive_flush_delay_divisor);
            if (effective_delay <= options.adaptive_flush_min_batch_delay_us) {
                break;
            }
        }
        policy.batch_delay_us = effective_delay;
    }

    if (options.adaptive_objective_enabled) {
        const uint64_t observed_queue_depth =
            std::max<uint64_t>(signals.request_queue_depth, signals.recent_peak_queue_depth);
        const uint64_t queue_scale =
            options.adaptive_batching_enabled
                ? options.adaptive_queue_depth_threshold
                : options.max_batch_size;
        const uint64_t pressure_score =
            weighted_signal_score(observed_queue_depth, queue_scale, options.adaptive_objective_queue_weight) +
            weighted_signal_score(
                signals.observed_p95_us,
                options.adaptive_latency_target_p95_us,
                options.adaptive_objective_latency_weight) +
            weighted_signal_score(
                signals.recent_read_ratio,
                options.adaptive_read_heavy_read_per_1000_ops_threshold,
                options.adaptive_objective_read_weight);
        const uint64_t throughput_score =
            weighted_deficit_score(
                signals.recent_avg_batch_size,
                options.adaptive_objective_target_batch_size,
                options.adaptive_objective_throughput_weight);
        const uint64_t cost_score =
            throughput_score +
            weighted_signal_score(
                signals.observed_fsync_pressure,
                options.adaptive_fsync_pressure_per_1000_writes_threshold,
                options.adaptive_objective_fsync_weight) +
            weighted_signal_score(
                signals.current_obsolete_ratio,
                options.adaptive_compaction_pressure_obsolete_ratio_percent_threshold,
                options.adaptive_objective_compaction_weight) +
            weighted_signal_score(
                signals.recent_avg_batch_wal_bytes,
                options.adaptive_wal_growth_bytes_per_batch_threshold,
                options.adaptive_objective_wal_growth_weight);
        policy.objective_pressure_score = pressure_score;
        policy.objective_cost_score = cost_score;
        policy.objective_throughput_score = throughput_score;
        policy.objective_balance_score =
            static_cast<int64_t>(pressure_score) - static_cast<int64_t>(cost_score);

        if (pressure_score >= cost_score + options.adaptive_objective_short_delay_score_threshold &&
            policy.batch_delay_us > options.adaptive_flush_min_batch_delay_us) {
            policy.objective_short_delay_adjusted = true;
            policy.objective_mode = 1;
            policy.batch_delay_us = std::max(
                options.adaptive_flush_min_batch_delay_us,
                policy.batch_delay_us / options.adaptive_objective_short_delay_divisor);
        } else if (cost_score >= pressure_score + options.adaptive_objective_long_delay_score_threshold &&
                   options.adaptive_objective_long_delay_multiplier > 1) {
            policy.objective_long_delay_adjusted = true;
            policy.objective_mode = -1;
            uint32_t boosted_delay = saturating_multiply(
                policy.batch_delay_us,
                options.adaptive_objective_long_delay_multiplier);
            if (options.adaptive_objective_max_batch_delay_us > 0) {
                boosted_delay = std::min(
                    boosted_delay,
                    options.adaptive_objective_max_batch_delay_us);
            }
            if (boosted_delay > 0) {
                policy.batch_delay_us = boosted_delay;
            }
        }
        return policy;
    }

    if (options.adaptive_read_heavy_read_per_1000_ops_threshold > 0 &&
        signals.recent_read_ratio >= options.adaptive_read_heavy_read_per_1000_ops_threshold) {
        policy.read_heavy_adjusted = true;
        policy.batch_delay_us = std::max(
            options.adaptive_flush_min_batch_delay_us,
            policy.batch_delay_us / options.adaptive_read_heavy_delay_divisor);
        policy.max_batch_size = std::max<size_t>(1, policy.max_batch_size / options.adaptive_read_heavy_batch_size_divisor);
        if (policy.max_batch_wal_bytes > 0) {
            policy.max_batch_wal_bytes =
                std::max<uint64_t>(sizeof(WalRecordHeader), policy.max_batch_wal_bytes / options.adaptive_read_heavy_batch_size_divisor);
        }
    }

    if (options.adaptive_latency_target_p95_us > 0 &&
        signals.observed_p95_us > options.adaptive_latency_target_p95_us &&
        policy.batch_delay_us > options.adaptive_flush_min_batch_delay_us) {
        policy.adaptive_flush = true;
        policy.latency_target_adjusted = true;
        policy.batch_delay_us = std::max(
            options.adaptive_flush_min_batch_delay_us,
            policy.batch_delay_us / std::max<uint32_t>(2, options.adaptive_flush_delay_divisor));
    }

    if (!policy.read_heavy_adjusted && !policy.latency_target_adjusted &&
        options.adaptive_fsync_pressure_per_1000_writes_threshold > 0 &&
        signals.observed_fsync_pressure >= options.adaptive_fsync_pressure_per_1000_writes_threshold &&
        options.adaptive_fsync_pressure_delay_multiplier > 1) {
        policy.fsync_pressure_adjusted = true;
        uint32_t boosted_delay = saturating_multiply(
            policy.batch_delay_us,
            options.adaptive_fsync_pressure_delay_multiplier);
        if (options.adaptive_fsync_pressure_max_batch_delay_us > 0) {
            boosted_delay = std::min(
                boosted_delay,
                options.adaptive_fsync_pressure_max_batch_delay_us);
        }
        if (boosted_delay > 0) {
            policy.batch_delay_us = boosted_delay;
        }
    }

    if (!policy.read_heavy_adjusted &&
        options.adaptive_compaction_pressure_obsolete_ratio_percent_threshold > 0 &&
        signals.current_obsolete_ratio >= options.adaptive_compaction_pressure_obsolete_ratio_percent_threshold) {
        policy.compaction_pressure_adjusted = true;
        uint32_t boosted_delay = saturating_multiply(
            policy.batch_delay_us,
            options.adaptive_compaction_pressure_delay_multiplier);
        if (boosted_delay > 0) {
            policy.batch_delay_us = boosted_delay;
        }
    }

    if (!policy.read_heavy_adjusted &&
        options.adaptive_wal_growth_bytes_per_batch_threshold > 0 &&
        signals.recent_avg_batch_wal_bytes >= options.adaptive_wal_growth_bytes_per_batch_threshold) {
        policy.wal_growth_adjusted = true;
        uint32_t boosted_delay = saturating_multiply(
            policy.batch_delay_us,
            options.adaptive_wal_growth_delay_multiplier);
        if (options.adaptive_wal_growth_max_batch_delay_us > 0) {
            boosted_delay = std::min(boosted_delay, options.adaptive_wal_growth_max_batch_delay_us);
        }
        if (boosted_delay > 0) {
            policy.batch_delay_us = boosted_delay;
        }
    }

    return policy;
}

}  // namespace kvstore::internal
