#include "metrics_helpers.h"

#include <limits>

namespace kvstore::internal {

const std::array<uint64_t, kWriteLatencyBucketCount> kWriteLatencyBucketUpperBoundsUs = {
    50,
    100,
    250,
    500,
    1000,
    2500,
    5000,
    10000,
    25000,
    50000,
    100000,
    100001,
};

size_t latency_bucket(uint64_t microseconds) {
    for (size_t index = 0; index + 1 < kWriteLatencyBucketCount; ++index) {
        if (microseconds <= kWriteLatencyBucketUpperBoundsUs[index]) {
            return index;
        }
    }
    return kWriteLatencyBucketCount - 1;
}

uint64_t latency_bucket_upper_bound_us(size_t bucket) {
    if (bucket < kWriteLatencyBucketCount) {
        return kWriteLatencyBucketUpperBoundsUs[bucket];
    }
    return kWriteLatencyBucketUpperBoundsUs.back();
}

uint64_t approximate_latency_percentile_us(
    const std::array<uint64_t, kWriteLatencyBucketCount>& histogram,
    uint64_t numerator,
    uint64_t denominator) {
    uint64_t total = 0;
    for (uint64_t value : histogram) {
        total += value;
    }
    if (total == 0) {
        return 0;
    }

    const uint64_t target = (total * numerator + denominator - 1) / denominator;
    uint64_t cumulative = 0;
    for (size_t i = 0; i < histogram.size(); ++i) {
        cumulative += histogram[i];
        if (cumulative >= target) {
            return latency_bucket_upper_bound_us(i);
        }
    }
    return latency_bucket_upper_bound_us(histogram.size() - 1);
}

uint64_t capped_ratio_milli(uint64_t value, uint64_t scale) {
    if (value == 0 || scale == 0) {
        return 0;
    }
    if (value / scale >= 4) {
        return 4000;
    }
    if (value > std::numeric_limits<uint64_t>::max() / 1000) {
        return 4000;
    }
    return (value * 1000) / scale;
}

uint64_t weighted_signal_score(uint64_t value, uint64_t scale, uint32_t weight) {
    if (weight == 0) {
        return 0;
    }
    return saturating_multiply<uint64_t>(capped_ratio_milli(value, scale), weight);
}

uint64_t weighted_deficit_score(uint64_t observed, uint64_t target, uint32_t weight) {
    if (weight == 0 || target == 0 || observed >= target) {
        return 0;
    }
    return saturating_multiply<uint64_t>(((target - observed) * 1000) / target, weight);
}

}  // namespace kvstore::internal
