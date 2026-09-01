#include "tests/integration/test_registry.h"

#include "kvstore.h"
#include "internal/key_codec.h"
#include "internal/storage_format.h"
#include "tests/common/test_support.h"

#include <atomic>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <filesystem>
#include <fstream>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <sys/resource.h>
#include <sys/wait.h>
#include <unistd.h>

namespace kvstore::tests::integration {
namespace {

using test_support::as_string;
using test_support::require;
using test_support::TestDir;
using test_support::text;

std::pair<int, int> keys_on_distinct_default_shards() {
    for (int left = 1; left < 128; ++left) {
        const size_t left_shard = static_cast<size_t>(
            kvstore::internal::stable_key_hash(kvstore::internal::encode_int_key(left)) & 255ULL);
        for (int right = left + 1; right < 256; ++right) {
            const size_t right_shard = static_cast<size_t>(
                kvstore::internal::stable_key_hash(kvstore::internal::encode_int_key(right)) & 255ULL);
            if (left_shard != right_shard) {
                return {left, right};
            }
        }
    }
    throw KVStoreError("Unable to find keys on distinct default shards");
}

void test_transaction_reads_its_writes_and_rolls_back() {
    TestDir directory("transaction_read_own_write");
    KVStore store(directory.file("store.dat"));
    store.Put(1, text("before"));

    auto transaction = store.BeginTransaction();
    transaction.Put(1, text("inside"));
    transaction.Put(std::string("new"), text("created"));
    require(transaction.Get(1).has_value() && as_string(*transaction.Get(1)) == "inside",
            "transaction should read its pending put");
    transaction.Delete(1);
    require(!transaction.Get(1).has_value(), "transaction should read its pending delete");
    transaction.Rollback();

    require(store.Get(1).has_value() && as_string(*store.Get(1)) == "before",
            "rollback must preserve the committed value");
    require(!store.Get(std::string("new")).has_value(), "rollback must discard new keys");
    require(store.GetMetrics().transaction_rollbacks == 1, "rollback should be observable");
}

void test_moved_from_transaction_rejects_operations() {
    TestDir directory("transaction_moved_from");
    KVStore store(directory.file("store.dat"));

    auto original = store.BeginTransaction();
    original.Put(1, text("pending"));
    auto active = std::move(original);

    bool get_rejected = false;
    try {
        (void)original.Get(1);
    } catch (const KVStoreError&) {
        get_rejected = true;
    }
    bool put_rejected = false;
    try {
        original.Put(2, text("invalid"));
    } catch (const KVStoreError&) {
        put_rejected = true;
    }
    bool commit_rejected = false;
    try {
        original.Commit();
    } catch (const KVStoreError&) {
        commit_rejected = true;
    }

    require(get_rejected && put_rejected && commit_rejected,
            "all operations on a moved-from transaction should fail deterministically");
    active.Rollback();
    require(!store.Get(1).has_value(), "rolling back the moved-to transaction must discard pending writes");
}

void test_malformed_batch_operations_are_rejected() {
    TestDir directory("malformed_batch");
    KVStore store(directory.file("store.dat"));

    BatchWriteOperation malformed_integer;
    malformed_integer.type = BatchWriteOperation::Type::kPut;
    malformed_integer.key_kind = BatchWriteOperation::KeyKind::kInt;
    malformed_integer.key = "12-trailing";
    malformed_integer.value = text("invalid");

    bool integer_rejected = false;
    try {
        store.WriteBatch({malformed_integer});
    } catch (const KVStoreError&) {
        integer_rejected = true;
    }

    BatchWriteOperation malformed_delete = BatchWriteOperation::Delete("alpha");
    malformed_delete.value = text("unexpected");
    bool delete_rejected = false;
    try {
        store.WriteBatch({malformed_delete});
    } catch (const KVStoreError&) {
        delete_rejected = true;
    }

    BatchWriteOperation unknown_type = BatchWriteOperation::Put("beta", text("invalid"));
    unknown_type.type = static_cast<BatchWriteOperation::Type>(255);
    bool type_rejected = false;
    try {
        store.WriteBatch({unknown_type});
    } catch (const KVStoreError&) {
        type_rejected = true;
    }

    require(integer_rejected && delete_rejected && type_rejected,
            "malformed public batch descriptors must be rejected before enqueue");
    require(!store.Get(12).has_value() && !store.Get(std::string("alpha")).has_value() &&
                !store.Get(std::string("beta")).has_value(),
            "rejected batch descriptors must not publish partial state");
}

void test_invalid_durability_mode_is_rejected() {
    TestDir directory("invalid_durability");
    KVStoreOptions options;
    options.durability = static_cast<DurabilityMode>(255);
    bool rejected = false;
    try {
        KVStore store(directory.file("store.dat"), options);
        (void)store;
    } catch (const KVStoreError&) {
        rejected = true;
    }
    require(rejected, "an invalid durability enum must not silently behave like kNoSync");
}

void test_transaction_commit_is_atomic_and_persistent() {
    TestDir directory("transaction_commit");
    const std::string path = directory.file("store.dat");
    {
        KVStore store(path);
        auto transaction = store.BeginTransaction();
        transaction.Put(1, text("one"));
        transaction.Put(std::string("alpha"), text("two"));
        transaction.Put(std::vector<uint8_t> {0x10, 0x20}, text("three"));
        transaction.Commit();
        require(store.GetMetrics().transaction_commits == 1, "transaction commit should be observable");
    }

    KVStore reopened(path);
    require(reopened.Get(1).has_value(), "integer transaction value should persist");
    require(reopened.Get(std::string("alpha")).has_value(), "string transaction value should persist");
    require(reopened.Get(std::vector<uint8_t> {0x10, 0x20}).has_value(),
            "binary transaction value should persist");
}

void test_transaction_conflict_fails_without_partial_write() {
    TestDir directory("transaction_conflict");
    KVStore store(directory.file("store.dat"));
    store.Put(1, text("initial"));
    store.Put(2, text("unchanged"));

    auto older = store.BeginTransaction();
    require(older.Get(1).has_value(), "older transaction should observe the initial value");
    older.Put(2, text("must-not-commit"));

    auto winner = store.BeginTransaction();
    require(winner.Get(1).has_value(), "winner should observe the initial value");
    winner.Put(1, text("winner"));
    winner.Commit();

    bool conflicted = false;
    try {
        older.Commit();
    } catch (const KVStoreConflictError&) {
        conflicted = true;
    }
    require(conflicted, "OCC should reject a transaction whose read shard changed");
    require(as_string(*store.Get(1)) == "winner", "winning transaction must remain visible");
    require(as_string(*store.Get(2)) == "unchanged", "conflicted transaction must publish no writes");
    require(store.GetMetrics().transaction_conflicts == 1, "transaction conflict should be observable");
}

void test_read_only_transaction_validates_versions() {
    TestDir directory("read_only_transaction");
    KVStore store(directory.file("store.dat"));
    store.Put(7, text("seven"));

    auto reader = store.BeginTransaction();
    require(reader.Get(7).has_value(), "read-only transaction should read committed state");
    store.Put(7, text("updated"));
    bool conflicted = false;
    try {
        reader.Commit();
    } catch (const KVStoreConflictError&) {
        conflicted = true;
    }
    require(conflicted, "read-only commit should validate its observed shard version");
}

void test_write_skew_is_rejected_across_shards() {
    TestDir directory("write_skew");
    KVStore store(directory.file("store.dat"));
    const auto [left_key, right_key] = keys_on_distinct_default_shards();
    store.Put(left_key, text("on"));
    store.Put(right_key, text("on"));

    auto left_transaction = store.BeginTransaction();
    auto right_transaction = store.BeginTransaction();
    require(left_transaction.Get(left_key).has_value() &&
                left_transaction.Get(right_key).has_value() &&
                right_transaction.Get(left_key).has_value() &&
                right_transaction.Get(right_key).has_value(),
            "both write-skew transactions should observe the initial invariant");
    left_transaction.Put(left_key, text("off"));
    right_transaction.Put(right_key, text("off"));

    left_transaction.Commit();
    bool conflicted = false;
    try {
        right_transaction.Commit();
    } catch (const KVStoreConflictError&) {
        conflicted = true;
    }

    require(conflicted, "serializable OCC should reject the second write-skew transaction");
    require(as_string(*store.Get(left_key)) == "off" &&
                as_string(*store.Get(right_key)) == "on",
            "a rejected cross-shard transaction must preserve the invariant without partial writes");
}

void test_concurrent_transaction_workers_propagate_exceptions() {
    TestDir directory("transaction_workers");
    KVStoreOptions options;
    options.worker_threads = 4;
    options.max_batch_size = 16;
    options.max_batch_delay_us = 5000;
    KVStore store(directory.file("store.dat"), options);

    constexpr int kThreadCount = 8;
    constexpr int kTransactionsPerThread = 40;
    std::mutex error_mutex;
    std::vector<std::exception_ptr> errors;
    std::vector<std::thread> threads;
    for (int thread_id = 0; thread_id < kThreadCount; ++thread_id) {
        threads.emplace_back([&store, &error_mutex, &errors, thread_id]() {
            try {
                for (int index = 0; index < kTransactionsPerThread; ++index) {
                    const int key = thread_id * 10000 + index;
                    bool committed = false;
                    while (!committed) {
                        auto transaction = store.BeginTransaction();
                        transaction.Put(key, text("value_" + std::to_string(key)));
                        transaction.Put(key + 1000, text("paired_" + std::to_string(key)));
                        try {
                            transaction.Commit();
                            committed = true;
                        } catch (const KVStoreConflictError&) {
                            std::this_thread::yield();
                        }
                    }
                }
            } catch (...) {
                std::lock_guard<std::mutex> lock(error_mutex);
                errors.push_back(std::current_exception());
            }
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }
    if (!errors.empty()) {
        std::rethrow_exception(errors.front());
    }
    const KVStoreMetrics metrics = store.GetMetrics();
    require(metrics.transaction_commits == kThreadCount * kTransactionsPerThread,
            "all independent transactions should commit through the worker pool");
    require(metrics.worker_tasks_completed >= metrics.transaction_commits,
            "worker task count should cover committed transactions");
    require(metrics.max_active_workers > 0 && metrics.max_active_workers <= metrics.configured_worker_threads,
            "worker utilization metrics should report a bounded active-worker high watermark");
    require(metrics.worker_utilization_per_1000 <= 1000,
            "worker utilization should be reported as a per-thousand ratio");
}

void test_second_store_instance_is_rejected() {
    TestDir directory("exclusive_file_lock");
    const std::string path = directory.file("store.dat");
    KVStore first(path);
    bool rejected = false;
    try {
        KVStore second(path);
        (void)second;
    } catch (const KVStoreError&) {
        rejected = true;
    }
    require(rejected, "a second process/instance must not acquire the database file");
}

void test_flush_is_a_durability_barrier_for_no_sync_mode() {
    TestDir directory("flush_barrier");
    const std::string path = directory.file("store.dat");
    {
        KVStoreOptions options;
        options.durability = DurabilityMode::kNoSync;
        KVStore store(path, options);
        store.Put(1, text("flushed"));
        require(store.GetMetrics().wal_fsync_calls == 0,
                "kNoSync should not sync an ordinary commit");
        store.Flush();
        require(store.GetMetrics().wal_fsync_calls >= 1,
                "Flush should issue a durability barrier in kNoSync mode");
    }
    KVStore reopened(path);
    require(reopened.Get(1).has_value(), "explicitly flushed data should survive reopen");
}

void test_periodic_mode_syncs_dirty_data() {
    TestDir directory("periodic_sync");
    KVStoreOptions options;
    options.durability = DurabilityMode::kPeriodic;
    options.periodic_sync_interval_ms = 5;
    KVStore store(directory.file("store.dat"), options);
    store.Put(1, text("periodic"));
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (store.GetMetrics().wal_fsync_calls == 0 && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    require(store.GetMetrics().wal_fsync_calls >= 1,
            "kPeriodic should sync dirty journal data within the configured interval");
}

void test_value_cache_is_bounded_and_reports_hits_and_misses() {
    TestDir directory("bounded_value_cache");
    KVStoreOptions options;
    options.shard_count = 1;
    options.value_cache_bytes = 384;
    KVStore store(directory.file("store.dat"), options);
    store.Put(1, Value(std::vector<uint8_t>(24, 0x11)));
    store.Put(2, Value(std::vector<uint8_t>(24, 0x22)));

    const KVStoreMetrics before = store.GetMetrics();
    require(store.Get(1).has_value(), "evicted value should be loaded from its file offset");
    require(store.Get(1).has_value(), "recently loaded value should remain in the bounded cache");
    const KVStoreMetrics after = store.GetMetrics();
    require(after.value_cache_misses > before.value_cache_misses,
            "reading an evicted value should increment cache misses");
    require(after.value_cache_hits > before.value_cache_hits,
            "reading the same value again should increment cache hits");
}

void test_offset_index_survives_compaction_with_cache_disabled() {
    TestDir directory("offset_index_compaction");
    const std::string path = directory.file("store.dat");
    {
        KVStoreOptions options;
        options.value_cache_bytes = 0;
        KVStore store(path, options);
        store.Put(1, text("checkpoint-value"));
        store.Compact();
        require(store.Get(1).has_value() && as_string(*store.Get(1)) == "checkpoint-value",
                "offset index should read a value from the compacted object region");
        store.Put(2, text("journal-value"));
        require(store.Get(2).has_value() && as_string(*store.Get(2)) == "journal-value",
                "offset index should read a value from the in-file journal");
        store.Compact();
        require(store.Get(1).has_value() && store.Get(2).has_value(),
                "offsets should be rebased when the compacted file replaces the old inode");
    }
    KVStore reopened(path);
    require(reopened.Get(1).has_value() && reopened.Get(2).has_value(),
            "rebased offset index contents should survive recovery");
}

void test_read_corruption_enters_sticky_fatal_state() {
    TestDir directory("sticky_read_fatal");
    const std::string path = directory.file("store.dat");
    KVStoreOptions options;
    options.value_cache_bytes = 0;
    KVStore store(path, options);
    store.Put(1, text("checksum-value"));

    const auto inspection = kvstore::internal::inspect_file(path);
    const uint64_t value_offset = inspection.superblock.journal_offset +
                                  sizeof(kvstore::internal::FrameHeader) +
                                  sizeof(kvstore::internal::MutationHeader) + 5;
    {
        std::fstream file(path, std::ios::binary | std::ios::in | std::ios::out);
        require(file.is_open(), "test database should be writable for corruption injection");
        file.seekg(static_cast<std::streamoff>(value_offset));
        char byte = 0;
        file.read(&byte, 1);
        require(file.gcount() == 1, "test value byte should exist");
        byte ^= static_cast<char>(0x7F);
        file.seekp(static_cast<std::streamoff>(value_offset));
        file.write(&byte, 1);
    }

    std::string original_error;
    try {
        (void)store.Get(1);
    } catch (const KVStoreError& error) {
        original_error = error.what();
    }
    require(original_error.find("checksum") != std::string::npos,
            "a backing-value checksum failure should be reported");

    std::string later_error;
    try {
        store.Put(2, text("must-not-commit"));
    } catch (const KVStoreError& error) {
        later_error = error.what();
    }
    require(later_error == original_error,
            "all later operations should report the original sticky fatal error");
}

void test_short_write_tail_is_atomic_and_sticky() {
    TestDir directory("short_write_fatal");
    const std::string path = directory.file("store.dat");
    const pid_t child = ::fork();
    require(child >= 0, "fork should start short-write fault test");
    if (child == 0) {
        ::signal(SIGXFSZ, SIG_IGN);
        try {
            KVStore store(path);
            store.Put(1, text("stable"));
            const uint64_t current_size = std::filesystem::file_size(path);
            struct rlimit limit {};
            if (::getrlimit(RLIMIT_FSIZE, &limit) != 0) {
                ::_exit(20);
            }
            const rlim_t partial_limit = static_cast<rlim_t>(
                current_size + sizeof(kvstore::internal::FrameHeader) + 7);
            if (limit.rlim_max != RLIM_INFINITY && partial_limit > limit.rlim_max) {
                ::_exit(21);
            }
            limit.rlim_cur = partial_limit;
            if (::setrlimit(RLIMIT_FSIZE, &limit) != 0) {
                ::_exit(22);
            }

            std::string write_error;
            try {
                store.Put(2, Value(std::vector<uint8_t>(1024, 0x5A)));
            } catch (const KVStoreError& error) {
                write_error = error.what();
            }
            if (write_error.empty()) {
                ::_exit(23);
            }
            std::string later_error;
            try {
                (void)store.Get(1);
            } catch (const KVStoreError& error) {
                later_error = error.what();
            }
            ::_exit(later_error == write_error ? 0 : 24);
        } catch (...) {
            ::_exit(25);
        }
    }

    int status = 0;
    require(::waitpid(child, &status, 0) == child, "waitpid should return short-write child");
    require(WIFEXITED(status) && WEXITSTATUS(status) == 0,
            "short write must surface an error and enter sticky fatal state");

    KVStore recovered(path);
    require(recovered.Get(1).has_value(), "the transaction before a short write must survive");
    require(!recovered.Get(2).has_value(), "a partially written transaction must not be recovered");
}

void test_disk_full_error_is_sticky_and_does_not_publish() {
    TestDir directory("disk_full_fatal");
    const std::string path = directory.file("store.dat");
    {
        KVStore store(path);
        store.Put(1, text("stable"));
    }

    const pid_t child = ::fork();
    require(child >= 0, "fork should start the ENOSPC fault test");
    if (child == 0) {
        ::setenv("KVSTORE_FAILPOINT", "before_journal_write", 1);
        ::setenv("KVSTORE_FAIL_ACTION", "enospc", 1);
        try {
            KVStore store(path);
            std::string write_error;
            try {
                store.Put(2, text("must-not-commit"));
            } catch (const KVStoreError& error) {
                write_error = error.what();
            }
            if (write_error.find(std::strerror(ENOSPC)) == std::string::npos) {
                ::_exit(30);
            }
            std::string later_error;
            try {
                (void)store.Get(1);
            } catch (const KVStoreError& error) {
                later_error = error.what();
            }
            ::_exit(later_error == write_error ? 0 : 31);
        } catch (...) {
            ::_exit(32);
        }
    }

    int status = 0;
    require(::waitpid(child, &status, 0) == child, "waitpid should return the ENOSPC child");
    require(WIFEXITED(status) && WEXITSTATUS(status) == 0,
            "ENOSPC should surface as the original sticky fatal error");

    KVStore recovered(path);
    require(recovered.Get(1).has_value(), "the transaction confirmed before ENOSPC must survive");
    require(!recovered.Get(2).has_value(), "an ENOSPC-rejected transaction must not be published");
}

}  // namespace

void register_transaction_tests(TestCases& tests) {
    tests.push_back({"transaction reads own writes and rolls back", test_transaction_reads_its_writes_and_rolls_back});
    tests.push_back({"moved-from transaction rejects operations", test_moved_from_transaction_rejects_operations});
    tests.push_back({"malformed batch operations are rejected", test_malformed_batch_operations_are_rejected});
    tests.push_back({"invalid durability mode is rejected", test_invalid_durability_mode_is_rejected});
    tests.push_back({"transaction commit is atomic and persistent", test_transaction_commit_is_atomic_and_persistent});
    tests.push_back({"transaction conflict has no partial write", test_transaction_conflict_fails_without_partial_write});
    tests.push_back({"read-only transaction validates versions", test_read_only_transaction_validates_versions});
    tests.push_back({"write skew is rejected across shards", test_write_skew_is_rejected_across_shards});
    tests.push_back({"concurrent transaction workers propagate exceptions", test_concurrent_transaction_workers_propagate_exceptions});
    tests.push_back({"second store instance is rejected", test_second_store_instance_is_rejected});
    tests.push_back({"Flush is a no-sync durability barrier", test_flush_is_a_durability_barrier_for_no_sync_mode});
    tests.push_back({"periodic mode syncs dirty data", test_periodic_mode_syncs_dirty_data});
    tests.push_back({"value cache is bounded and observable", test_value_cache_is_bounded_and_reports_hits_and_misses});
    tests.push_back({"offset index survives compaction", test_offset_index_survives_compaction_with_cache_disabled});
    tests.push_back({"read corruption enters sticky fatal state", test_read_corruption_enters_sticky_fatal_state});
    tests.push_back({"short write tail is atomic and sticky", test_short_write_tail_is_atomic_and_sticky});
    tests.push_back({"disk full is sticky and atomic", test_disk_full_error_is_sticky_and_does_not_publish});
}

}  // namespace kvstore::tests::integration
