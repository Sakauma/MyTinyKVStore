#include "tests/unit/test_registry.h"

#include "internal/value_cache.h"

#include <atomic>
#include <string>
#include <thread>
#include <vector>

namespace kvstore::tests::unit {
namespace {

using test_support::require;
using test_support::ThreadFailureCollector;

Value repeated_value(uint8_t byte, size_t size) {
    return Value(std::vector<uint8_t>(size, byte));
}

void test_value_cache_capacity_and_segment_selection() {
    kvstore::internal::ValueCache disabled(0);
    disabled.Insert("key", 1, repeated_value(1, 16));
    require(disabled.SegmentCount() == 0 && disabled.ChargeBytes() == 0,
            "Zero capacity should fully disable the value cache");
    require(!disabled.Lookup("key", 1).has_value(),
            "A disabled value cache must always miss");

    kvstore::internal::ValueCache small(4ULL * 64ULL * 1024ULL);
    require(small.SegmentCount() == 4,
            "The cache should allocate one power-of-two segment per 64 KiB");
    kvstore::internal::ValueCache defaults(256ULL * 1024ULL * 1024ULL);
    require(defaults.SegmentCount() == 64,
            "The default cache should cap segmentation at 64 segments");
}

void test_value_cache_is_lsn_scoped_and_strictly_bounded() {
    constexpr uint64_t kCapacity = 256ULL * 1024ULL;
    kvstore::internal::ValueCache cache(kCapacity);
    cache.Insert("same", 1, repeated_value(1, 64));
    cache.Insert("same", 2, repeated_value(2, 64));
    const auto first = cache.Lookup("same", 1);
    const auto second = cache.Lookup("same", 2);
    require(first.has_value() && first->bytes.front() == 1 &&
                second.has_value() && second->bytes.front() == 2,
            "Cache identity must include both normalized key and LSN");
    cache.Insert("same", 2, repeated_value(3, 96));
    const auto replacement = cache.Lookup("same", 2);
    require(replacement.has_value() && replacement->bytes.size() == 96 &&
                replacement->bytes.front() == 3,
            "A later mutation with the same transaction LSN must replace the cached value");

    ThreadFailureCollector failures;
    std::vector<std::thread> threads;
    for (int thread_id = 0; thread_id < 8; ++thread_id) {
        threads.emplace_back(failures.guard([&cache, thread_id]() {
            for (int index = 0; index < 500; ++index) {
                const std::string key = "key_" + std::to_string(thread_id) + "_" +
                                        std::to_string(index);
                const uint64_t lsn = static_cast<uint64_t>(thread_id * 1000 + index + 1);
                cache.Insert(key, lsn, repeated_value(static_cast<uint8_t>(thread_id), 512));
                (void)cache.Lookup(key, lsn);
            }
        }));
    }
    for (auto& thread : threads) {
        thread.join();
    }
    failures.rethrow_first();
    require(cache.ChargeBytes() <= cache.CapacityBytes() &&
                cache.ChargeBytes() <= kCapacity,
            "Concurrent CLOCK insertion and eviction must never exceed configured capacity");
}

}  // namespace

void register_value_cache_tests(TestCases& tests) {
    tests.push_back({"value cache capacity selects bounded segments", test_value_cache_capacity_and_segment_selection});
    tests.push_back({"value cache is lsn scoped and strictly bounded", test_value_cache_is_lsn_scoped_and_strictly_bounded});
}

}  // namespace kvstore::tests::unit
