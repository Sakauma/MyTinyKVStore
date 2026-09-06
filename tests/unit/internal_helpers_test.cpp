#include "tests/unit/test_registry.h"

#include "internal/io.h"
#include "internal/key_codec.h"
#include "internal/metrics_helpers.h"
#include "internal/storage_format.h"
#include "internal/writer_policy.h"

#include <array>
#include <cstdint>
#include <string>
#include <vector>

namespace kvstore::tests::unit {
namespace {

using test_support::require;

void test_internal_format_helpers_round_trip_keys() {
    const std::string int_key = kvstore::internal::encode_int_key(42);
    const std::string string_key = kvstore::internal::encode_string_key("alpha");
    const std::string binary_key =
        kvstore::internal::encode_binary_key(std::vector<uint8_t> {0x00, 0x7F, 0xFF});

    require(!int_key.empty() && int_key.front() == kvstore::internal::kIntKeyTag,
            "encoded int keys should carry the int namespace tag");
    require(kvstore::internal::is_string_key(string_key),
            "encoded string keys should be recognized as string keys");
    require(kvstore::internal::decode_string_key(string_key) == "alpha",
            "string key helpers should round-trip the original string key");
    require(!binary_key.empty() && binary_key.front() == kvstore::internal::kBinaryKeyTag,
            "encoded binary keys should carry the binary namespace tag");
}

void test_internal_metrics_helpers_compute_percentiles_and_ratios() {
    std::array<uint64_t, kWriteLatencyBucketCount> histogram {};
    histogram[0] = 1;
    histogram[4] = 2;
    histogram[7] = 1;

    require(kvstore::internal::approximate_latency_percentile_us(histogram, 1, 2) == 1000,
            "p50 helper should map into the first bucket that crosses the 50th percentile");
    require(kvstore::internal::approximate_latency_percentile_us(histogram, 95, 100) == 10000,
            "p95 helper should map into the tail bucket that crosses the percentile");
    histogram.fill(0);
    histogram[kWriteLatencyBucketCount - 2] = 1;
    require(kvstore::internal::approximate_latency_percentile_us(histogram, 99, 100) == 100000,
            "latency histogram should keep 100000us as the penultimate bucket boundary");
    histogram.fill(0);
    histogram[kWriteLatencyBucketCount - 1] = 1;
    require(kvstore::internal::approximate_latency_percentile_us(histogram, 99, 100) == 100001,
            "latency histogram should reserve 100001us for values above 100000us");
    require(kvstore::internal::latency_bucket(100000) == kWriteLatencyBucketCount - 2 &&
                kvstore::internal::latency_bucket(100001) == kWriteLatencyBucketCount - 1,
            "latency bucket assignment should match percentile boundaries at 100000us");
    require(kvstore::internal::capped_ratio_milli(16, 4) == 4000,
            "ratio helper should cap values at 4x");
    require(kvstore::internal::weighted_signal_score(200, 100, 3) == 6000,
            "weighted signal score should scale the capped ratio by the given weight");
    require(kvstore::internal::weighted_deficit_score(4, 8, 2) == 1000,
            "weighted deficit score should reflect the observed gap to target");
}

void test_objective_mode_keeps_read_heavy_batch_cap_without_delay_rule() {
    KVStoreOptions options;
    options.max_batch_size = 64;
    options.max_batch_wal_bytes = 4096;
    options.max_batch_delay_us = 1000;
    options.adaptive_objective_enabled = true;
    options.adaptive_objective_read_weight = 0;
    options.adaptive_read_heavy_read_per_1000_ops_threshold = 600;
    options.adaptive_read_heavy_delay_divisor = 8;
    options.adaptive_read_heavy_batch_size_divisor = 4;
    options.adaptive_flush_min_batch_delay_us = 100;

    const kvstore::internal::WriterPolicySignals signals {
        0,
        0,
        800,
        0,
        16,
        0,
        0,
        0,
    };
    const kvstore::internal::BatchPolicy policy = kvstore::internal::compute_batch_policy(options, signals);

    require(policy.read_heavy_adjusted,
            "objective mode should retain read-heavy batch-size adjustment");
    require(policy.max_batch_size == 16,
            "read-heavy batch-size divisor should apply in objective mode");
    require(policy.max_batch_wal_bytes == 1024,
            "read-heavy WAL cap divisor should apply in objective mode");
    require(policy.batch_delay_us == options.max_batch_delay_us,
            "objective mode should leave read-heavy delay to the objective controller");
}

void test_adaptive_flush_policy_threshold_boundary() {
    KVStoreOptions options;
    options.max_batch_size = 16;
    options.max_batch_delay_us = 50000;
    options.adaptive_flush_enabled = true;
    options.adaptive_flush_queue_depth_threshold = 2;
    options.adaptive_flush_delay_divisor = 10;
    options.adaptive_flush_min_batch_delay_us = 1000;

    const kvstore::internal::WriterPolicySignals below_threshold {
        1,
        1,
        0,
        0,
        0,
        0,
        0,
        0,
    };
    const kvstore::internal::BatchPolicy below_policy =
        kvstore::internal::compute_batch_policy(options, below_threshold);
    require(!below_policy.adaptive_flush && below_policy.batch_delay_us == 50000,
            "adaptive flush should remain off below its queue-depth threshold");

    const kvstore::internal::WriterPolicySignals at_threshold {
        2,
        2,
        0,
        0,
        0,
        0,
        0,
        0,
    };
    const kvstore::internal::BatchPolicy at_policy =
        kvstore::internal::compute_batch_policy(options, at_threshold);
    require(at_policy.adaptive_flush,
            "adaptive flush should activate at its queue-depth threshold");
    require(at_policy.batch_delay_us == 5000,
            "adaptive flush should divide the base delay once at threshold two");
}

}  // namespace

void register_internal_helpers_tests(TestCases& tests) {
    tests.push_back({"internal format helpers round trip keys", test_internal_format_helpers_round_trip_keys});
    tests.push_back({"internal metrics helpers compute percentiles and ratios",
                     test_internal_metrics_helpers_compute_percentiles_and_ratios});
    tests.push_back({"objective mode keeps read-heavy batch cap without delay rule",
                     test_objective_mode_keeps_read_heavy_batch_cap_without_delay_rule});
    tests.push_back({"adaptive flush policy threshold boundary",
                     test_adaptive_flush_policy_threshold_boundary});
}

}  // namespace kvstore::tests::unit
