#ifndef KVSTORE_INTERNAL_METRICS_HELPERS_H
#define KVSTORE_INTERNAL_METRICS_HELPERS_H

#include "kvstore.h"

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <limits>

namespace kvstore::internal {

extern const std::array<uint64_t, kWriteLatencyBucketCount - 1> kWriteLatencyBucketUpperBoundsUs;

template <typename T>
T saturating_multiply(T lhs, T rhs) {
    if (lhs == 0 || rhs == 0) {
        return 0;
    }
    if (lhs > std::numeric_limits<T>::max() / rhs) {
        return std::numeric_limits<T>::max();
    }
    return lhs * rhs;
}

void update_atomic_max(std::atomic<uint64_t>& metric, uint64_t value);
uint64_t latency_bucket_upper_bound_us(size_t bucket);
uint64_t approximate_latency_percentile_us(
    const std::array<uint64_t, kWriteLatencyBucketCount>& histogram,
    uint64_t numerator,
    uint64_t denominator);
uint64_t capped_ratio_milli(uint64_t value, uint64_t scale);
uint64_t weighted_signal_score(uint64_t value, uint64_t scale, uint32_t weight);
uint64_t weighted_deficit_score(uint64_t observed, uint64_t target, uint32_t weight);

}  // namespace kvstore::internal

#endif  // KVSTORE_INTERNAL_METRICS_HELPERS_H
