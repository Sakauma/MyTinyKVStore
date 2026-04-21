#include "tests/unit/test_registry.h"

#include "internal/format.h"
#include "internal/metrics_helpers.h"

#include <array>
#include <cstdint>
#include <string>
#include <vector>

namespace kvstore::tests::unit {
namespace {

using test_support::require;
using test_support::text;

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

void test_internal_format_helpers_checksum_distinguishes_payloads() {
    const std::string key = kvstore::internal::encode_string_key("checksum");
    const Value first = text("value_a");
    const Value second = text("value_b");

    const uint32_t first_checksum =
        kvstore::internal::checksum_record(kvstore::internal::WalRecordType::kPut, key, first);
    const uint32_t second_checksum =
        kvstore::internal::checksum_record(kvstore::internal::WalRecordType::kPut, key, second);
    const uint32_t delete_checksum =
        kvstore::internal::checksum_record(kvstore::internal::WalRecordType::kDelete, key, Value {});

    require(first_checksum != second_checksum,
            "checksum helper should change when the payload changes");
    require(first_checksum != delete_checksum,
            "checksum helper should change when the record type changes");
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
    require(kvstore::internal::capped_ratio_milli(16, 4) == 4000,
            "ratio helper should cap values at 4x");
    require(kvstore::internal::weighted_signal_score(200, 100, 3) == 6000,
            "weighted signal score should scale the capped ratio by the given weight");
    require(kvstore::internal::weighted_deficit_score(4, 8, 2) == 1000,
            "weighted deficit score should reflect the observed gap to target");
}

}  // namespace

void register_internal_helpers_tests(TestCases& tests) {
    tests.push_back({"internal format helpers round trip keys", test_internal_format_helpers_round_trip_keys});
    tests.push_back({"internal format helpers checksum distinguishes payloads",
                     test_internal_format_helpers_checksum_distinguishes_payloads});
    tests.push_back({"internal metrics helpers compute percentiles and ratios",
                     test_internal_metrics_helpers_compute_percentiles_and_ratios});
}

}  // namespace kvstore::tests::unit
