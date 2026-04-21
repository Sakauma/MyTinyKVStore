#include "tests/integration/test_registry.h"

#include "kvstore.h"
#include "tests/common/runtime_entrypoints.h"
#include "tests/common/test_support.h"

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
    KVStore store(db_path);

    std::thread writer([&store]() {
        for (int i = 0; i < 200; ++i) {
            store.Put(i, text("value_" + std::to_string(i)));
        }
    });

    std::thread reader([&store]() {
        for (int i = 0; i < 400; ++i) {
            const int key = i % 200;
            const auto value = store.Get(key);
            if (value.has_value()) {
                require(as_string(*value).rfind("value_", 0) == 0, "reader should only observe complete values");
            }
        }
    });

    writer.join();
    reader.join();

    KVStore reopened(db_path);
    for (int i = 0; i < 200; ++i) {
        const auto value = reopened.Get(i);
        require(value.has_value(), "all committed keys should persist after reopen");
    }
}

void test_many_concurrent_writers() {
    TestDir dir("many_writers");
    const std::string db_path = dir.file("store.dat");
    KVStore store(db_path);

    constexpr int kWriterCount = 8;
    constexpr int kWritesPerThread = 150;
    std::vector<std::thread> writers;

    for (int writer_id = 0; writer_id < kWriterCount; ++writer_id) {
        writers.emplace_back([&store, writer_id]() {
            for (int index = 0; index < kWritesPerThread; ++index) {
                const int key = writer_id * 10000 + index;
                store.Put(key, text("writer_" + std::to_string(writer_id) + "_" + std::to_string(index)));
            }
        });
    }

    for (auto& writer : writers) {
        writer.join();
    }

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

void test_concurrent_compaction_with_writes() {
    TestDir dir("compact_with_writes");
    const std::string db_path = dir.file("store.dat");
    KVStore store(db_path);

    constexpr int kWriterCount = 4;
    constexpr int kWritesPerThread = 120;

    std::vector<std::thread> writers;
    for (int writer_id = 0; writer_id < kWriterCount; ++writer_id) {
        writers.emplace_back([&store, writer_id]() {
            for (int index = 0; index < kWritesPerThread; ++index) {
                const int key = writer_id * 10000 + index;
                store.Put(key, text("value_" + std::to_string(writer_id) + "_" + std::to_string(index)));
                if (index % 20 == 0) {
                    store.Delete(key);
                    store.Put(key, text("value_" + std::to_string(writer_id) + "_" + std::to_string(index) + "_final"));
                }
            }
        });
    }

    std::thread compactor([&store]() {
        for (int round = 0; round < 6; ++round) {
            store.Compact();
        }
    });

    for (auto& writer : writers) {
        writer.join();
    }
    compactor.join();
    store.Compact();

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
    tests.push_back({"crash after snapshot rename recovers consistent state",
                     test_crash_after_snapshot_rename_recovers_consistent_state});
    tests.push_back({"crash before snapshot rename replays old wal", test_crash_before_snapshot_rename_replays_old_wal});
    tests.push_back({"crash after wal rotation recovers snapshot", test_crash_after_wal_rotation_recovers_snapshot});
    tests.push_back({"concurrent reads and writes", test_concurrent_reads_and_writes});
    tests.push_back({"many concurrent writers", test_many_concurrent_writers});
    tests.push_back({"concurrent compaction with writes", test_concurrent_compaction_with_writes});
    tests.push_back({"recommended profiles are distinct", test_recommended_profiles_are_distinct});
    tests.push_back({"concurrency stress profiles are distinct", test_concurrency_stress_profiles_are_distinct});
}

}  // namespace kvstore::tests::integration
