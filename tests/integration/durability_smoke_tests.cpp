#include "tests/integration/test_registry.h"

#include "kvstore.h"

#include <string>

namespace kvstore::tests::integration {
namespace {

using test_support::as_string;
using test_support::file_size_or_zero;
using test_support::require;
using test_support::TestDir;
using test_support::text;

void test_ordering_and_updates() {
    TestDir dir("ordering");
    const std::string db_path = dir.file("store.dat");

    {
        KVStore store(db_path);
        store.Put(3, text("v1"));
        store.Delete(3);
        store.Put(3, text("v2"));
        store.Put(4, text("keep"));
    }

    KVStore reopened(db_path);
    const auto latest = reopened.Get(3);
    const auto other = reopened.Get(4);
    require(latest.has_value(), "key 3 should exist after final put");
    require(other.has_value(), "key 4 should exist after replay");
    require(as_string(*latest) == "v2", "operations must replay in commit order");
    require(as_string(*other) == "keep", "other keys should be unaffected");
}

void test_compaction_persists_snapshot_and_resets_wal() {
    TestDir dir("compaction");
    const std::string db_path = dir.file("store.dat");
    const std::string wal_path = db_path + ".wal";

    {
        KVStore store(db_path);
        store.Put(1, text("one"));
        store.Put(2, text("two"));
        store.Delete(1);
        store.Compact();
    }

    require(file_size_or_zero(db_path) > 16, "compaction should materialize live data into the snapshot");
    require(file_size_or_zero(wal_path) == 0, "compaction should rotate to an empty WAL");

    KVStore reopened(db_path);
    require(!reopened.Get(1).has_value(), "deleted key must not reappear after compaction");
    const auto survivor = reopened.Get(2);
    require(survivor.has_value(), "live key must survive compaction");
    require(as_string(*survivor) == "two", "compaction should keep the latest snapshot value");
}

}  // namespace

void register_durability_smoke_tests(TestCases& tests) {
    tests.push_back({"ordering and updates", test_ordering_and_updates});
    tests.push_back({"compaction persists snapshot and resets WAL", test_compaction_persists_snapshot_and_resets_wal});
}

}  // namespace kvstore::tests::integration
