#include "tests/integration/test_registry.h"

#include "kvstore.h"
#include "tests/common/runtime_entrypoints.h"
#include "tests/common/test_support.h"

#include <array>
#include <atomic>
#include <memory>
#include <string>
#include <thread>
#include <vector>

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
    tests.push_back({"concurrent scan observes complete batches", test_concurrent_scan_observes_complete_batches});
    tests.push_back({"concurrent compaction with writes", test_concurrent_compaction_with_writes});
    tests.push_back({"recommended profiles are distinct", test_recommended_profiles_are_distinct});
    tests.push_back({"concurrency stress profiles are distinct", test_concurrency_stress_profiles_are_distinct});
}

}  // namespace kvstore::tests::integration
