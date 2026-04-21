#include "tests/unit/test_registry.h"

#include "internal/wal_accounting.h"

#include <atomic>
#include <map>

namespace kvstore::tests::unit {
namespace {

using namespace kvstore::internal;
using test_support::require;

void test_track_latest_wal_record_updates_live_bytes() {
    std::map<std::string, uint64_t> latest_records;
    std::atomic<uint64_t> live_bytes {0};

    track_latest_wal_record(latest_records, live_bytes, "alpha", 10);
    track_latest_wal_record(latest_records, live_bytes, "alpha", 16);
    track_latest_wal_record(latest_records, live_bytes, "beta", 7);

    require(live_bytes.load(std::memory_order_relaxed) == 23,
            "WAL accounting should replace overwritten record sizes instead of double-counting them");
    require(latest_records["alpha"] == 16 && latest_records["beta"] == 7,
            "WAL accounting should retain the latest size per key");
}

void test_should_auto_compact_considers_obsolete_ratio() {
    KVStoreOptions options;
    options.auto_compact_invalid_wal_ratio_percent = 40;
    require(should_auto_compact(options, 100, 50),
            "auto compaction should trigger when obsolete WAL ratio crosses the configured threshold");
    require(!should_auto_compact(options, 100, 80),
            "auto compaction should stay off when obsolete WAL ratio remains below threshold");
}

}  // namespace

void register_wal_accounting_tests(TestCases& tests) {
    tests.push_back({"wal accounting tracks latest live bytes", test_track_latest_wal_record_updates_live_bytes});
    tests.push_back({"wal accounting respects obsolete ratio threshold", test_should_auto_compact_considers_obsolete_ratio});
}

}  // namespace kvstore::tests::unit
