#include "latency_metrics.h"

#include "metrics_helpers.h"

namespace kvstore::internal {

void record_write_latency_sample(const LatencyMetricsContext& context) {
    size_t bucket = kWriteLatencyBucketCount - 1;
    for (size_t i = 0; i + 1 < kWriteLatencyBucketCount; ++i) {
        if (context.elapsed_us <= kWriteLatencyBucketUpperBoundsUs[i]) {
            bucket = i;
            break;
        }
    }
    context.write_latency_histogram[bucket].fetch_add(1, std::memory_order_relaxed);

    context.recent_latency_buckets.push_back(bucket);
    context.recent_latency_bucket_counts[bucket] += 1;
    while (context.recent_latency_buckets.size() > context.recent_write_sample_limit) {
        const size_t old_bucket = context.recent_latency_buckets.front();
        context.recent_latency_buckets.pop_front();
        context.recent_latency_bucket_counts[old_bucket] -= 1;
    }
    context.recent_observed_write_latency_p95_us.store(
        approximate_latency_percentile_us(context.recent_latency_bucket_counts, 95, 100),
        std::memory_order_relaxed);
}

}  // namespace kvstore::internal
