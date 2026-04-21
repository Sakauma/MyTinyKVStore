#include "tests/unit/test_registry.h"

#include "internal/format.h"
#include "internal/request_runtime.h"
#include "internal/writer_execution.h"

#include <mutex>

namespace kvstore::tests::unit {
namespace {

using namespace kvstore::internal;
using test_support::require;

void test_wal_record_size_for_request_matches_payload() {
    Request put;
    put.type = RequestType::kPut;
    put.key = "alpha";
    put.value = test_support::text("value");
    const uint64_t expected_put = sizeof(WalRecordHeader) + put.key.size() + put.value.bytes.size();
    require(wal_record_size_for_request(put) == expected_put,
            "put WAL record size should include header, key, and value bytes");

    Request batch;
    batch.type = RequestType::kBatch;
    batch.batch_operations = {
        BatchMutation {WalRecordType::kPut, "beta", test_support::text("one")},
        BatchMutation {WalRecordType::kDelete, "gamma", Value {}},
    };
    const uint64_t expected_batch =
        sizeof(WalRecordHeader) + 4 + 3 + sizeof(WalRecordHeader) + 5;
    require(wal_record_size_for_request(batch) == expected_batch,
            "batch WAL record size should sum every embedded operation");
}

void test_collect_write_batch_respects_request_and_wal_limits() {
    std::mutex mutex;
    RequestQueue queue;
    auto first = std::make_shared<Request>();
    first->type = RequestType::kPut;
    first->key = "a";
    first->value = test_support::text("1111");

    auto second = std::make_shared<Request>();
    second->type = RequestType::kPut;
    second->key = "bb";
    second->value = test_support::text("2222");

    auto compact = std::make_shared<Request>();
    compact->type = RequestType::kCompact;

    queue.push_back(first);
    queue.push_back(second);
    queue.push_back(compact);

    BatchPolicy policy {
        4,
        wal_record_size_for_request(*first),
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        false,
        false,
        false,
        false,
        false,
        false,
        false,
        false,
        false,
    };

    std::unique_lock<std::mutex> lock(mutex);
    std::vector<RequestPtr> batch;
    collect_write_batch(
        lock,
        queue,
        false,
        policy,
        std::chrono::steady_clock::now(),
        [](std::unique_lock<std::mutex>&, const std::chrono::steady_clock::time_point&) { return false; },
        batch);
    lock.unlock();

    require(batch.size() == 1, "collect_write_batch should stop when the WAL byte limit is hit");
    require(batch.front() == first, "collect_write_batch should preserve queue order");
    require(queue.size() == 2 && queue.front() == second,
            "collect_write_batch should leave the remaining queue intact");
}

void test_maybe_auto_compact_creates_compaction_request() {
    KVStoreOptions options;
    options.auto_compact_wal_bytes_threshold = 16;

    std::atomic<uint64_t> wal_bytes_since_compaction {64};
    std::atomic<uint64_t> live_wal_bytes_since_compaction {8};
    bool called = false;
    RequestPtr captured;
    const bool triggered = maybe_auto_compact(AutoCompactionExecutionContext {
        options,
        wal_bytes_since_compaction,
        live_wal_bytes_since_compaction,
        [&](const RequestPtr& request, bool is_auto_compaction) {
            called = is_auto_compaction;
            captured = request;
        },
    });

    require(triggered, "auto compaction should trigger when the WAL threshold is exceeded");
    require(called, "auto compaction should invoke the compaction callback");
    require(captured && captured->type == RequestType::kCompact,
            "auto compaction should synthesize a compact request");
}

}  // namespace

void register_writer_execution_tests(TestCases& tests) {
    tests.push_back({"writer execution computes WAL record sizes", test_wal_record_size_for_request_matches_payload});
    tests.push_back({"writer execution collects batches within limits", test_collect_write_batch_respects_request_and_wal_limits});
    tests.push_back({"writer execution triggers auto compaction", test_maybe_auto_compact_creates_compaction_request});
}

}  // namespace kvstore::tests::unit
