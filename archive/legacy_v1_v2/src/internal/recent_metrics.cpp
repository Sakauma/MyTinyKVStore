#include "recent_metrics.h"

#include <algorithm>

namespace kvstore::internal {

void update_observed_fsync_pressure_metric(
    std::atomic<uint64_t>& observed_fsync_pressure_per_1000_writes,
    size_t batch_size) {
    if (batch_size == 0) {
        return;
    }
    const uint64_t instantaneous_pressure = (1000 + batch_size - 1) / batch_size;
    const uint64_t previous_pressure =
        observed_fsync_pressure_per_1000_writes.load(std::memory_order_relaxed);
    const uint64_t next_pressure =
        previous_pressure == 0 ? instantaneous_pressure : ((previous_pressure * 3) + instantaneous_pressure) / 4;
    observed_fsync_pressure_per_1000_writes.store(next_pressure, std::memory_order_relaxed);
}

void update_recent_batch_history(const RecentBatchHistoryContext& context) {
    const uint64_t read_delta =
        context.total_read_requests >= context.last_total_read_requests_seen
            ? context.total_read_requests - context.last_total_read_requests_seen
            : 0;
    context.last_total_read_requests_seen = context.total_read_requests;

    context.recent_batch_sizes.push_back(context.batch_size);
    context.recent_batch_queue_depths.push_back(context.queue_depth);
    context.recent_batch_wal_bytes.push_back(context.current_batch_wal_bytes);
    context.recent_read_deltas.push_back(read_delta);
    context.recent_write_deltas.push_back(context.batch_size);
    context.recent_batch_size_sum += context.batch_size;
    context.recent_batch_wal_bytes_sum += context.current_batch_wal_bytes;
    context.recent_read_sum += read_delta;
    context.recent_write_sum += context.batch_size;

    while (context.recent_batch_sizes.size() > context.options.adaptive_recent_window_batches) {
        context.recent_batch_size_sum -= context.recent_batch_sizes.front();
        context.recent_batch_sizes.pop_front();
        context.recent_batch_queue_depths.pop_front();
        context.recent_batch_wal_bytes_sum -= context.recent_batch_wal_bytes.front();
        context.recent_batch_wal_bytes.pop_front();
        context.recent_read_sum -= context.recent_read_deltas.front();
        context.recent_read_deltas.pop_front();
        context.recent_write_sum -= context.recent_write_deltas.front();
        context.recent_write_deltas.pop_front();
    }

    context.recent_window_batch_count.store(context.recent_batch_sizes.size(), std::memory_order_relaxed);
    if (!context.recent_batch_sizes.empty()) {
        context.recent_avg_batch_size.store(
            context.recent_batch_size_sum / context.recent_batch_sizes.size(),
            std::memory_order_relaxed);
        const uint64_t avg_batch_size =
            context.recent_batch_size_sum / context.recent_batch_sizes.size();
        const uint64_t batch_fill_scale =
            std::max<uint64_t>(1, context.options.adaptive_objective_target_batch_size > 0
                                      ? context.options.adaptive_objective_target_batch_size
                                      : context.options.max_batch_size);
        context.recent_batch_fill_per_1000.store(
            std::min<uint64_t>(1000, (avg_batch_size * 1000) / batch_fill_scale),
            std::memory_order_relaxed);
        context.recent_avg_batch_wal_bytes.store(
            context.recent_batch_wal_bytes_sum / context.recent_batch_sizes.size(),
            std::memory_order_relaxed);
    }
    context.recent_read_requests.store(context.recent_read_sum, std::memory_order_relaxed);
    context.recent_write_requests.store(context.recent_write_sum, std::memory_order_relaxed);
    const uint64_t total_recent_ops = context.recent_read_sum + context.recent_write_sum;
    const uint64_t read_ratio = total_recent_ops == 0 ? 0 : (context.recent_read_sum * 1000) / total_recent_ops;
    context.recent_read_ratio_per_1000_ops.store(read_ratio, std::memory_order_relaxed);

    uint64_t peak_queue_depth = 0;
    for (uint64_t value : context.recent_batch_queue_depths) {
        peak_queue_depth = std::max(peak_queue_depth, value);
    }
    context.recent_peak_queue_depth.store(peak_queue_depth, std::memory_order_relaxed);

    if (!context.recent_batch_sizes.empty() && context.recent_batch_size_sum > 0) {
        const uint64_t recent_pressure =
            (context.recent_batch_sizes.size() * 1000 + context.recent_batch_size_sum - 1) /
            context.recent_batch_size_sum;
        context.observed_fsync_pressure_per_1000_writes.store(recent_pressure, std::memory_order_relaxed);
    }
}

}  // namespace kvstore::internal
