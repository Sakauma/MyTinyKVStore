#include "tests/integration/test_registry.h"

#include "internal/storage_format.h"
#include "kvstore.h"
#include "tests/common/runtime_entrypoints.h"
#include "tests/common/test_support.h"

#include <array>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include <unistd.h>

namespace kvstore::tests::integration {
namespace {

using test_support::as_string;
using test_support::file_size_or_zero;
using test_support::require;
using test_support::TestDir;
using test_support::text;
using test_support::ThreadFailureCollector;

void test_soak_profiles_are_distinct() {
    const SoakProfileSummary balanced = soak_profile_summary_entrypoint("balanced");
    const SoakProfileSummary write_heavy = soak_profile_summary_entrypoint("write-heavy");
    const SoakProfileSummary read_heavy = soak_profile_summary_entrypoint("read-heavy");

    require(write_heavy.writer_count > balanced.writer_count,
            "write-heavy soak profile should use more writers than balanced");
    require(read_heavy.reader_count > balanced.reader_count,
            "read-heavy soak profile should use more readers than balanced");
    require(write_heavy.max_batch_size > balanced.max_batch_size,
            "write-heavy soak profile should allow larger batches");
    require(read_heavy.max_batch_delay_us < balanced.max_batch_delay_us,
            "read-heavy soak profile should use shorter batch delays");
}

void test_crash_after_wal_fsync_recovers_latest_write() {
    TestDir dir("crash_wal_fsync");
    const std::string db_path = dir.file("store.dat");

    run_failpoint_child_entrypoint("wal_after_fsync", db_path);

    KVStore reopened(db_path);
    const auto stable = reopened.Get(1);
    const auto latest = reopened.Get(2);
    require(stable.has_value(), "stable key should persist after WAL-fsync crash");
    require(latest.has_value(), "latest WAL-synced key should be recovered after crash");
    require(as_string(*stable) == "stable", "stable key should preserve its value");
    require(as_string(*latest) == "latest", "replayed WAL should restore the latest synced value");
}

void test_crash_after_wal_fsync_recovers_latest_batch() {
    TestDir dir("crash_wal_batch");
    const std::string db_path = dir.file("store.dat");

    run_failpoint_child_entrypoint("wal_after_fsync_batch", db_path);

    KVStore reopened(db_path);
    const auto one = reopened.Get(1);
    const auto two = reopened.Get(2);
    const auto alpha = reopened.Get(std::string("alpha"));
    require(!one.has_value(), "batch delete should recover from WAL after crash");
    require(two.has_value() && as_string(*two) == "latest", "batch int put should recover from WAL after crash");
    require(alpha.has_value() && as_string(*alpha) == "batch-value",
            "batch string put should recover from WAL after crash");
}

void test_transaction_frame_crash_boundary_matrix_is_atomic() {
    TestDir dir("frame_crash_matrix");
    const std::vector<std::string> scenarios {
        "frame_before_write",
        "frame_after_header",
        "frame_after_payload",
        "frame_after_footer",
        "frame_before_sync",
        "frame_after_sync",
    };
    for (const auto& scenario : scenarios) {
        const std::string db_path = dir.file(scenario + ".dat");
        run_failpoint_child_entrypoint(scenario, db_path);

        KVStore reopened(db_path);
        const auto stable = reopened.Get(1);
        const auto integer = reopened.Get(2);
        const auto string = reopened.Get(std::string("batch-string"));
        require(stable.has_value() && as_string(*stable) == "stable",
                "the confirmed transaction before a frame failpoint must survive");
        require(integer.has_value() == string.has_value(),
                "a frame-boundary crash must not recover half of a batch");
        if (integer.has_value()) {
            require(as_string(*integer) == "batch-int" &&
                        as_string(*string) == "batch-string-value",
                    "a recovered frame-boundary batch must contain complete values");
        }
    }
}

void test_compaction_crash_boundary_matrix_preserves_state() {
    TestDir dir("compaction_crash_matrix");
    const std::vector<std::string> scenarios {
        "compaction_after_checkpoint",
        "compaction_before_temp_sync",
        "compaction_after_temp_sync",
        "compaction_after_rename",
        "compaction_after_directory_sync",
        "compaction_before_entry_migration",
        "compaction_after_entry_migration",
    };
    for (const auto& scenario : scenarios) {
        const std::string db_path = dir.file(scenario + ".dat");
        run_failpoint_child_entrypoint(scenario, db_path);

        KVStore reopened(db_path);
        const auto eleven = reopened.Get(11);
        const auto twenty_two = reopened.Get(22);
        require(eleven.has_value() && twenty_two.has_value(),
                "every compaction crash boundary must recover a complete old or new container");
        require(as_string(*eleven) == "eleven" && as_string(*twenty_two) == "twenty-two",
                "compaction crash recovery must preserve confirmed values");
    }
}

void test_crash_after_snapshot_rename_recovers_consistent_state() {
    TestDir dir("crash_snapshot_rename");
    const std::string db_path = dir.file("store.dat");

    run_failpoint_child_entrypoint("snapshot_after_rename", db_path);

    KVStore reopened(db_path);
    const auto one = reopened.Get(1);
    const auto two = reopened.Get(2);
    require(one.has_value() && two.has_value(),
            "snapshot-rename crash should preserve all compacted keys");
    require(as_string(*one) == "one", "recovered state should keep key 1");
    require(as_string(*two) == "two", "recovered state should keep key 2");
}

void test_crash_before_snapshot_rename_replays_old_wal() {
    TestDir dir("crash_before_snapshot_rename");
    const std::string db_path = dir.file("store.dat");

    run_failpoint_child_entrypoint("snapshot_before_rename", db_path);

    KVStore reopened(db_path);
    const auto seven = reopened.Get(7);
    const auto eight = reopened.Get(8);
    require(seven.has_value() && eight.has_value(),
            "crash before snapshot rename should fall back to old snapshot plus WAL");
    require(as_string(*seven) == "seven", "recovered state should keep key 7");
    require(as_string(*eight) == "eight", "recovered state should keep key 8");
}

void test_crash_after_wal_rotation_recovers_snapshot() {
    TestDir dir("crash_wal_rotation");
    const std::string db_path = dir.file("store.dat");

    run_failpoint_child_entrypoint("wal_rotation_before_reopen", db_path);

    KVStore reopened(db_path);
    const auto ten = reopened.Get(10);
    const auto twenty = reopened.Get(20);
    require(ten.has_value() && twenty.has_value(),
            "crash after WAL rotation should preserve compacted snapshot state");
    require(as_string(*ten) == "ten", "recovered state should keep key 10");
    require(as_string(*twenty) == "twenty", "recovered state should keep key 20");
}

void test_concurrent_reads_and_writes() {
    TestDir dir("concurrency");
    const std::string db_path = dir.file("store.dat");
    auto store = std::make_unique<KVStore>(db_path);
    ThreadFailureCollector thread_failures;

    std::thread writer(thread_failures.guard([&store]() {
        for (int i = 0; i < 200; ++i) {
            store->Put(i, text("value_" + std::to_string(i)));
        }
    }));

    std::thread reader(thread_failures.guard([&store]() {
        for (int i = 0; i < 400; ++i) {
            const int key = i % 200;
            const auto value = store->Get(key);
            if (value.has_value()) {
                require(as_string(*value).rfind("value_", 0) == 0, "reader should only observe complete values");
            }
        }
    }));

    writer.join();
    reader.join();
    thread_failures.rethrow_first();
    store.reset();

    KVStore reopened(db_path);
    for (int i = 0; i < 200; ++i) {
        const auto value = reopened.Get(i);
        require(value.has_value(), "all committed keys should persist after reopen");
    }
}

void test_many_concurrent_writers() {
    TestDir dir("many_writers");
    const std::string db_path = dir.file("store.dat");
    auto store = std::make_unique<KVStore>(db_path);

    constexpr int kWriterCount = 8;
    constexpr int kWritesPerThread = 150;
    std::vector<std::thread> writers;
    ThreadFailureCollector thread_failures;

    for (int writer_id = 0; writer_id < kWriterCount; ++writer_id) {
        writers.emplace_back(thread_failures.guard([&store, writer_id]() {
            for (int index = 0; index < kWritesPerThread; ++index) {
                const int key = writer_id * 10000 + index;
                store->Put(key, text("writer_" + std::to_string(writer_id) + "_" + std::to_string(index)));
            }
        }));
    }

    for (auto& writer : writers) {
        writer.join();
    }
    thread_failures.rethrow_first();
    store.reset();

    KVStore reopened(db_path);
    for (int writer_id = 0; writer_id < kWriterCount; ++writer_id) {
        for (int index = 0; index < kWritesPerThread; ++index) {
            const int key = writer_id * 10000 + index;
            const auto value = reopened.Get(key);
            require(value.has_value(), "all concurrent writer keys should persist");
            require(as_string(*value) == "writer_" + std::to_string(writer_id) + "_" + std::to_string(index),
                    "concurrent writers must preserve each committed value");
        }
    }
}

void run_mixed_producer_model_case(int producer_count) {
    TestDir directory("mixed_producers_" + std::to_string(producer_count));
    const std::string path = directory.file("store.dat");
    constexpr int kRounds = 4;

    {
        KVStoreOptions options;
        options.worker_threads = 8;
        options.max_batch_size = 64;
        options.max_batch_delay_us = 2000;
        KVStore store(path, options);
        std::vector<std::thread> producers;
        ThreadFailureCollector thread_failures;
        for (int producer_id = 0; producer_id < producer_count; ++producer_id) {
            producers.emplace_back(thread_failures.guard([&store, producer_id]() {
                for (int round = 0; round < kRounds; ++round) {
                    const int base = producer_id * 100000 + round * 10;
                    store.Put(base, text("point"));
                    store.WriteBatch({
                        BatchWriteOperation::PutInt(base + 1, text("batch")),
                        BatchWriteOperation::PutInt(base + 2, text("temporary")),
                    });

                    bool committed = false;
                    for (int attempt = 0; attempt < 1000 && !committed; ++attempt) {
                        auto transaction = store.BeginTransaction();
                        transaction.Put(base, text("transaction-final"));
                        transaction.Delete(base + 2);
                        transaction.Put(base + 3, text("transaction-paired"));
                        try {
                            transaction.Commit();
                            committed = true;
                        } catch (const KVStoreConflictError&) {
                            std::this_thread::yield();
                        }
                    }
                    require(committed, "mixed producer transaction should eventually commit");
                    store.Delete(base + 1);
                    store.Put(base + 1, text("batch-final"));
                }
            }));
        }
        for (auto& producer : producers) {
            producer.join();
        }
        thread_failures.rethrow_first();
        store.Flush();
    }

    KVStore reopened(path);
    for (int producer_id = 0; producer_id < producer_count; ++producer_id) {
        for (int round = 0; round < kRounds; ++round) {
            const int base = producer_id * 100000 + round * 10;
            require(reopened.Get(base).has_value() &&
                        as_string(*reopened.Get(base)) == "transaction-final",
                    "mixed producer model should preserve the transaction update");
            require(reopened.Get(base + 1).has_value() &&
                        as_string(*reopened.Get(base + 1)) == "batch-final",
                    "mixed producer model should preserve the final point update");
            require(!reopened.Get(base + 2).has_value(),
                    "mixed producer model should preserve the transaction delete");
            require(reopened.Get(base + 3).has_value() &&
                        as_string(*reopened.Get(base + 3)) == "transaction-paired",
                    "mixed producer model should preserve the paired transaction write");
        }
    }
}

void test_mixed_8_16_32_producer_models() {
    for (int producer_count : std::array<int, 3> {8, 16, 32}) {
        run_mixed_producer_model_case(producer_count);
    }
}

void test_small_write_history_is_linearizable() {
    TestDir directory("small_linearizable_history");
    const std::string path = directory.file("store.dat");
    KVStore store(path);
    struct Interval {
        int begin = -1;
        int end = -1;
    };
    std::array<Interval, 8> history {};
    std::atomic<int> logical_clock {0};
    ThreadFailureCollector thread_failures;

    const auto run_wave = [&](int first, int last) {
        std::vector<std::thread> writers;
        for (int writer_id = first; writer_id < last; ++writer_id) {
            writers.emplace_back(thread_failures.guard([&, writer_id]() {
                history[writer_id].begin = logical_clock.fetch_add(1, std::memory_order_seq_cst);
                store.Put(777, text("writer_" + std::to_string(writer_id)));
                history[writer_id].end = logical_clock.fetch_add(1, std::memory_order_seq_cst);
            }));
        }
        for (auto& writer : writers) {
            writer.join();
        }
        thread_failures.rethrow_first();
    };

    run_wave(0, 4);
    run_wave(4, 8);
    const auto final = store.Get(777);
    require(final.has_value(), "linearizability history should leave a final value");
    const std::string final_text = as_string(*final);
    require(final_text.rfind("writer_", 0) == 0,
            "linearizability history should return a value from a completed write");
    const int final_writer = std::stoi(final_text.substr(7));
    require(final_writer >= 0 && final_writer < static_cast<int>(history.size()),
            "linearizability history should identify a known writer");
    for (size_t writer_id = 0; writer_id < history.size(); ++writer_id) {
        if (static_cast<int>(writer_id) == final_writer) {
            continue;
        }
        require(history[writer_id].begin <= history[final_writer].end,
                "no write invoked after the final writer completed may be ordered before it");
    }
    require(final_writer >= 4,
            "a write from the second real-time wave must follow every completed first-wave write");
}

void test_mixed_history_has_a_real_time_consistent_serialization() {
    TestDir directory("mixed_linearizable_history");
    KVStoreOptions options;
    options.max_batch_delay_us = 0;
    KVStore store(directory.file("store.dat"), options);
    store.Put(900, text("seed"));

    enum class Kind {
        kPut,
        kDelete,
        kGet,
        kBatch,
    };
    struct Record {
        Kind kind;
        int begin = -1;
        int end = -1;
        std::optional<std::string> observed {};
    };
    std::array<Record, 4> history {{
        {Kind::kPut},
        {Kind::kDelete},
        {Kind::kGet},
        {Kind::kBatch},
    }};
    std::atomic<int> ready {0};
    std::atomic<bool> start {false};
    std::atomic<int> clock {0};
    ThreadFailureCollector failures;
    std::vector<std::thread> operations;
    for (size_t index = 0; index < history.size(); ++index) {
        operations.emplace_back(failures.guard([&, index] {
            ready.fetch_add(1, std::memory_order_release);
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            history[index].begin = clock.fetch_add(1, std::memory_order_seq_cst);
            switch (history[index].kind) {
                case Kind::kPut:
                    store.Put(900, text("put"));
                    break;
                case Kind::kDelete:
                    store.Delete(900);
                    break;
                case Kind::kGet: {
                    const auto value = store.Get(900);
                    history[index].observed = value.has_value()
                                                  ? std::optional<std::string>(as_string(*value))
                                                  : std::nullopt;
                    break;
                }
                case Kind::kBatch:
                    store.WriteBatch({
                        BatchWriteOperation::PutInt(900, text("batch")),
                        BatchWriteOperation::PutInt(901, text("paired")),
                    });
                    break;
            }
            history[index].end = clock.fetch_add(1, std::memory_order_seq_cst);
        }));
    }
    while (ready.load(std::memory_order_acquire) < static_cast<int>(history.size())) {
        std::this_thread::yield();
    }
    start.store(true, std::memory_order_release);
    for (auto& operation : operations) {
        operation.join();
    }
    failures.rethrow_first();

    std::map<int, std::string> final_state;
    if (const auto value = store.Get(900); value.has_value()) {
        final_state[900] = as_string(*value);
    }
    if (const auto value = store.Get(901); value.has_value()) {
        final_state[901] = as_string(*value);
    }

    std::array<bool, 4> used {};
    bool linearizable = false;
    std::function<void(size_t, std::map<int, std::string>)> search;
    search = [&](size_t depth, std::map<int, std::string> model) {
        if (linearizable) {
            return;
        }
        if (depth == history.size()) {
            linearizable = model == final_state;
            return;
        }
        for (size_t candidate = 0; candidate < history.size(); ++candidate) {
            if (used[candidate]) {
                continue;
            }
            bool predecessor_missing = false;
            for (size_t predecessor = 0; predecessor < history.size(); ++predecessor) {
                if (!used[predecessor] && predecessor != candidate &&
                    history[predecessor].end < history[candidate].begin) {
                    predecessor_missing = true;
                    break;
                }
            }
            if (predecessor_missing) {
                continue;
            }

            auto candidate_model = model;
            bool response_matches = true;
            switch (history[candidate].kind) {
                case Kind::kPut:
                    candidate_model[900] = "put";
                    break;
                case Kind::kDelete:
                    candidate_model.erase(900);
                    break;
                case Kind::kGet: {
                    const auto found = candidate_model.find(900);
                    const std::optional<std::string> expected =
                        found == candidate_model.end()
                            ? std::nullopt
                            : std::optional<std::string>(found->second);
                    response_matches = expected == history[candidate].observed;
                    break;
                }
                case Kind::kBatch:
                    candidate_model[900] = "batch";
                    candidate_model[901] = "paired";
                    break;
            }
            if (!response_matches) {
                continue;
            }
            used[candidate] = true;
            search(depth + 1, std::move(candidate_model));
            used[candidate] = false;
        }
    };
    search(0, {{900, "seed"}});
    require(linearizable,
            "Put/Delete/Get/WriteBatch history must admit a legal real-time serial order");

    store.Put(902, text("transaction-base"));
    auto transaction = store.BeginTransaction();
    const auto transaction_read = transaction.Get(902);
    transaction.Put(903, text("must-not-publish"));
    store.Put(902, text("concurrent-update"));
    bool conflicted = false;
    try {
        transaction.Commit();
    } catch (const KVStoreConflictError&) {
        conflicted = true;
    }
    require(transaction_read.has_value() &&
                as_string(*transaction_read) == "transaction-base" && conflicted &&
                !store.Get(903).has_value(),
            "The checked mixed history must also reject a stale OCC transaction atomically");
}

void test_concurrent_scan_observes_complete_batches() {
    TestDir directory("concurrent_scan_batches");
    KVStoreOptions options;
    options.max_batch_size = 32;
    options.max_batch_delay_us = 1000;
    KVStore store(directory.file("store.dat"), options);

    constexpr int kKeyCount = 32;
    const auto make_key = [](int index) {
        std::string suffix = std::to_string(index);
        suffix.insert(suffix.begin(), 3 - suffix.size(), '0');
        return std::string("scan-key-") + suffix;
    };
    const auto make_batch = [&](int version) {
        std::vector<BatchWriteOperation> batch;
        batch.reserve(kKeyCount);
        for (int index = 0; index < kKeyCount; ++index) {
            batch.push_back(BatchWriteOperation::Put(
                make_key(index),
                text("version_" + std::to_string(version))));
        }
        return batch;
    };
    store.WriteBatch(make_batch(0));

    ThreadFailureCollector thread_failures;
    std::atomic<bool> writer_done {false};
    std::thread writer(thread_failures.guard([&] {
        for (int version = 1; version <= 40; ++version) {
            store.WriteBatch(make_batch(version));
        }
        writer_done.store(true, std::memory_order_release);
    }));
    std::thread scanner(thread_failures.guard([&] {
        int scans = 0;
        while (!writer_done.load(std::memory_order_acquire) || scans < 40) {
            const auto rows = store.Scan(make_key(0), make_key(kKeyCount - 1));
            require(rows.size() == kKeyCount,
                    "a concurrent scan should return every key in the requested range");
            const std::string observed_version = as_string(rows.front().second);
            for (const auto& [key, value] : rows) {
                (void)key;
                require(as_string(value) == observed_version,
                        "a concurrent scan must not expose half of a cross-shard batch");
            }
            ++scans;
        }
    }));

    writer.join();
    scanner.join();
    thread_failures.rethrow_first();
}

void test_hot_shard_readers_race_safely_with_clock_eviction() {
    TestDir directory("hot_shard_clock_cache");
    KVStoreOptions options;
    options.shard_count = 1;
    options.worker_threads = 8;
    options.value_cache_bytes = 4ULL * 64ULL * 1024ULL;
    options.max_batch_delay_us = 0;
    KVStore store(directory.file("store.dat"), options);
    store.Put(std::string("hot"), text("hot_0"));

    std::atomic<bool> writer_done {false};
    ThreadFailureCollector failures;
    std::thread writer(failures.guard([&] {
        for (int version = 1; version <= 300; ++version) {
            store.Put(std::string("hot"), text("hot_" + std::to_string(version)));
            store.Put(std::string("churn_" + std::to_string(version)),
                      Value(std::vector<uint8_t>(1024, static_cast<uint8_t>(version))));
        }
        writer_done.store(true, std::memory_order_release);
    }));

    std::vector<std::thread> readers;
    for (int reader_id = 0; reader_id < 8; ++reader_id) {
        readers.emplace_back(failures.guard([&] {
            int reads = 0;
            while (!writer_done.load(std::memory_order_acquire) || reads < 500) {
                const auto value = store.Get(std::string("hot"));
                require(value.has_value() && as_string(*value).rfind("hot_", 0) == 0,
                        "Hot-key readers must observe a complete committed value");
                ++reads;
            }
        }));
    }

    writer.join();
    for (auto& reader : readers) {
        reader.join();
    }
    failures.rethrow_first();
    require(store.GetMetrics().value_cache_hits > 0,
            "Concurrent hot-key reads should exercise shared-lock CLOCK hits");
}

void test_concurrent_compaction_with_writes() {
    TestDir dir("compact_with_writes");
    const std::string db_path = dir.file("store.dat");
    auto store = std::make_unique<KVStore>(db_path);

    constexpr int kWriterCount = 4;
    constexpr int kWritesPerThread = 120;

    std::vector<std::thread> writers;
    ThreadFailureCollector thread_failures;
    for (int writer_id = 0; writer_id < kWriterCount; ++writer_id) {
        writers.emplace_back(thread_failures.guard([&store, writer_id]() {
            for (int index = 0; index < kWritesPerThread; ++index) {
                const int key = writer_id * 10000 + index;
                store->Put(key, text("value_" + std::to_string(writer_id) + "_" + std::to_string(index)));
                if (index % 20 == 0) {
                    store->Delete(key);
                    store->Put(key, text("value_" + std::to_string(writer_id) + "_" + std::to_string(index) + "_final"));
                }
            }
        }));
    }

    std::thread compactor(thread_failures.guard([&store]() {
        for (int round = 0; round < 6; ++round) {
            store->Compact();
        }
    }));

    for (auto& writer : writers) {
        writer.join();
    }
    compactor.join();
    thread_failures.rethrow_first();
    store->Compact();
    store.reset();

    KVStore reopened(db_path);
    for (int writer_id = 0; writer_id < kWriterCount; ++writer_id) {
        for (int index = 0; index < kWritesPerThread; ++index) {
            const int key = writer_id * 10000 + index;
            const auto value = reopened.Get(key);
            require(value.has_value(), "keys written during compaction should persist");
            std::string expected = "value_" + std::to_string(writer_id) + "_" + std::to_string(index);
            if (index % 20 == 0) {
                expected += "_final";
            }
            require(as_string(*value) == expected, "compaction must preserve the latest committed value");
        }
    }
}

void test_compaction_delta_preserves_overwrites_deletes_transactions_and_scans() {
    TestDir directory("compaction_delta_migration");
    const std::string path = directory.file("store.dat");
    KVStoreOptions options;
    options.auto_compact_wal_bytes_threshold = 0;
    options.auto_compact_invalid_wal_ratio_percent = 0;
    options.max_batch_delay_us = 0;
    auto store = std::make_unique<KVStore>(path, options);

    std::vector<BatchWriteOperation> seed;
    const Value large_value(std::vector<uint8_t>(256 * 1024, 0x5A));
    for (int key = 0; key < 64; ++key) {
        seed.push_back(BatchWriteOperation::PutInt(10000 + key, large_value));
    }
    for (int key = 0; key < 32; ++key) {
        seed.push_back(BatchWriteOperation::Put(
            "scan_" + std::to_string(key), text("scan-value-" + std::to_string(key))));
    }
    seed.push_back(BatchWriteOperation::PutInt(7000, text("before-compaction")));
    seed.push_back(BatchWriteOperation::PutInt(7001, text("delete-during-compaction")));
    store->WriteBatch(seed);

    std::atomic<bool> stop_scans {false};
    std::atomic<bool> compaction_done {false};
    ThreadFailureCollector failures;
    std::thread scanner(failures.guard([&] {
        while (!stop_scans.load(std::memory_order_acquire)) {
            const auto values = store->Scan("scan_", "scan_zzzz");
            require(values.size() == 32,
                    "A Scan concurrent with compaction migration must keep a complete view");
            std::this_thread::yield();
        }
    }));
    std::thread compactor(failures.guard([&] {
        try {
            store->Compact();
        } catch (...) {
            compaction_done.store(true, std::memory_order_release);
            throw;
        }
        compaction_done.store(true, std::memory_order_release);
    }));

    const std::string temp_path = path + ".compact." +
                                  std::to_string(static_cast<long long>(::getpid())) + ".0";
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    bool observed_temp = false;
    while (std::chrono::steady_clock::now() < deadline &&
           !compaction_done.load(std::memory_order_acquire)) {
        if (std::filesystem::exists(temp_path)) {
            observed_temp = true;
            break;
        }
        std::this_thread::yield();
    }
    if (!observed_temp) {
        stop_scans.store(true, std::memory_order_release);
        compactor.join();
        scanner.join();
        failures.rethrow_first();
        require(false,
                "The compaction test must observe the temporary generation before issuing delta writes");
        return;
    }

    store->Put(7000, text("delta-one"));
    store->Put(7000, text("delta-two"));
    store->Delete(7000);
    store->Put(7000, text("delta-final"));
    store->Delete(7001);
    store->WriteBatch({
        BatchWriteOperation::PutInt(7002, text("batch-live")),
        BatchWriteOperation::PutInt(7003, text("batch-delete")),
        BatchWriteOperation::DeleteInt(7003),
    });
    std::optional<Value> observed;
    {
        auto transaction = store->BeginTransaction();
        observed = transaction.Get(7000);
        transaction.Put(7004, text("transaction-live"));
        transaction.Commit();
    }

    compactor.join();
    stop_scans.store(true, std::memory_order_release);
    scanner.join();
    failures.rethrow_first();
    require(observed.has_value() && as_string(*observed) == "delta-final",
            "A transaction concurrent with migration must read the latest committed delta");
    require(store->Get(7000).has_value() &&
                as_string(*store->Get(7000)) == "delta-final" &&
                !store->Get(7001).has_value() &&
                store->Get(7002).has_value() &&
                !store->Get(7003).has_value() &&
                store->Get(7004).has_value(),
            "Compaction migration must preserve delta overwrites, deletes, batches, and transactions");

    const KVStoreMetrics before_restart = store->GetMetrics();
    require(before_restart.wal_bytes_since_compaction ==
                before_restart.live_wal_bytes_since_compaction +
                    before_restart.obsolete_wal_bytes_since_compaction &&
                before_restart.obsolete_wal_bytes_since_compaction > 0,
            "The switched WAL epoch must exactly account for overwritten compaction delta frames");
    const auto inspection = kvstore::internal::inspect_file(path);
    require(inspection.journal_frames >= 7,
            "Writes issued after the compaction cut must remain as complete journal frames");

    store.reset();
    KVStore reopened(path, options);
    require(reopened.Get(7000).has_value() &&
                as_string(*reopened.Get(7000)) == "delta-final" &&
                !reopened.Get(7001).has_value() &&
                reopened.Get(7002).has_value() &&
                !reopened.Get(7003).has_value() &&
                reopened.Get(7004).has_value(),
            "Restart after compaction migration must replay the complete journal delta");
    const KVStoreMetrics recovered = reopened.GetMetrics();
    require(recovered.wal_bytes_since_compaction == before_restart.wal_bytes_since_compaction &&
                recovered.live_wal_bytes_since_compaction ==
                    before_restart.live_wal_bytes_since_compaction &&
                recovered.obsolete_wal_bytes_since_compaction ==
                    before_restart.obsolete_wal_bytes_since_compaction,
            "Restart must reconstruct the switched compaction WAL accounting epoch");
}

void test_recommended_profiles_are_distinct() {
    const KVStoreOptions balanced = RecommendedOptions(KVStoreProfile::kBalanced);
    const KVStoreOptions write_heavy = RecommendedOptions(KVStoreProfile::kWriteHeavy);
    const KVStoreOptions read_heavy = RecommendedOptions(KVStoreProfile::kReadHeavy);
    const KVStoreOptions low_latency = RecommendedOptions(KVStoreProfile::kLowLatency);

    require(write_heavy.max_batch_size > balanced.max_batch_size,
            "write-heavy profile should favor larger batches than balanced");
    require(read_heavy.max_batch_size < balanced.max_batch_size,
            "read-heavy profile should favor smaller batches than balanced");
    require(low_latency.max_batch_delay_us < balanced.max_batch_delay_us,
            "low-latency profile should shorten batch delay");
    require(write_heavy.auto_compact_wal_bytes_threshold > balanced.auto_compact_wal_bytes_threshold,
            "write-heavy profile should tolerate a larger WAL before compaction");
}

void test_concurrency_stress_profiles_are_distinct() {
    const ConcurrencyStressProfileSummary balanced =
        concurrency_stress_profile_summary_entrypoint("balanced");
    const ConcurrencyStressProfileSummary write_heavy =
        concurrency_stress_profile_summary_entrypoint("write-heavy");
    const ConcurrencyStressProfileSummary compaction_heavy =
        concurrency_stress_profile_summary_entrypoint("compaction-heavy");
    const ConcurrencyStressProfileSummary recovery_heavy =
        concurrency_stress_profile_summary_entrypoint("recovery-heavy");

    require(write_heavy.writer_count > balanced.writer_count,
            "write-heavy stress profile should use more writers than balanced");
    require(write_heavy.max_batch_size > balanced.max_batch_size,
            "write-heavy stress profile should favor larger batches than balanced");
    require(compaction_heavy.compactor_count > balanced.compactor_count,
            "compaction-heavy stress profile should run more compactor threads");
    require(compaction_heavy.compaction_interval_ms < balanced.compaction_interval_ms,
            "compaction-heavy stress profile should compact more frequently than balanced");
    require(compaction_heavy.auto_compact_wal_bytes_threshold < balanced.auto_compact_wal_bytes_threshold,
            "compaction-heavy stress profile should compact at a smaller WAL threshold");
    require(recovery_heavy.recovery_reopen_cycles > balanced.recovery_reopen_cycles,
            "recovery-heavy stress profile should repeat reopen validation more often than balanced");
    require(recovery_heavy.reader_count < balanced.reader_count,
            "recovery-heavy stress profile should trade reader threads for reopen checks");
    require(recovery_heavy.max_batch_delay_us < balanced.max_batch_delay_us,
            "recovery-heavy stress profile should prefer shorter delays around recovery checks");
}

}  // namespace

void register_runtime_concurrency_tests(TestCases& tests) {
    tests.push_back({"soak profiles are distinct", test_soak_profiles_are_distinct});
    tests.push_back({"crash after wal fsync recovers latest write", test_crash_after_wal_fsync_recovers_latest_write});
    tests.push_back({"crash after wal fsync recovers latest batch", test_crash_after_wal_fsync_recovers_latest_batch});
    tests.push_back({"transaction frame crash boundary matrix is atomic",
                     test_transaction_frame_crash_boundary_matrix_is_atomic});
    tests.push_back({"compaction crash boundary matrix preserves state",
                     test_compaction_crash_boundary_matrix_preserves_state});
    tests.push_back({"crash after snapshot rename recovers consistent state",
                     test_crash_after_snapshot_rename_recovers_consistent_state});
    tests.push_back({"crash before snapshot rename replays old wal", test_crash_before_snapshot_rename_replays_old_wal});
    tests.push_back({"crash after wal rotation recovers snapshot", test_crash_after_wal_rotation_recovers_snapshot});
    tests.push_back({"concurrent reads and writes", test_concurrent_reads_and_writes});
    tests.push_back({"many concurrent writers", test_many_concurrent_writers});
    tests.push_back({"mixed 8/16/32 producer models", test_mixed_8_16_32_producer_models});
    tests.push_back({"small write history is linearizable", test_small_write_history_is_linearizable});
    tests.push_back({"mixed history has a legal exhaustive serialization", test_mixed_history_has_a_real_time_consistent_serialization});
    tests.push_back({"concurrent scan observes complete batches", test_concurrent_scan_observes_complete_batches});
    tests.push_back({"hot shard readers race safely with clock eviction", test_hot_shard_readers_race_safely_with_clock_eviction});
    tests.push_back({"concurrent compaction with writes", test_concurrent_compaction_with_writes});
    tests.push_back({"compaction delta preserves concurrent operations", test_compaction_delta_preserves_overwrites_deletes_transactions_and_scans});
    tests.push_back({"recommended profiles are distinct", test_recommended_profiles_are_distinct});
    tests.push_back({"concurrency stress profiles are distinct", test_concurrency_stress_profiles_are_distinct});
}

}  // namespace kvstore::tests::integration
