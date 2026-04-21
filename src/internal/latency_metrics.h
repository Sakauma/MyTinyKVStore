#ifndef KVSTORE_INTERNAL_LATENCY_METRICS_H
#define KVSTORE_INTERNAL_LATENCY_METRICS_H

#include "kvstore.h"

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <deque>

namespace kvstore::internal {

struct LatencyMetricsContext {
    uint64_t elapsed_us;
    size_t recent_write_sample_limit;
    std::deque<size_t>& recent_latency_buckets;
    std::array<uint64_t, kWriteLatencyBucketCount>& recent_latency_bucket_counts;
    std::array<std::atomic<uint64_t>, kWriteLatencyBucketCount>& write_latency_histogram;
    std::atomic<uint64_t>& recent_observed_write_latency_p95_us;
};

void record_write_latency_sample(const LatencyMetricsContext& context);

}  // namespace kvstore::internal

#endif  // KVSTORE_INTERNAL_LATENCY_METRICS_H
