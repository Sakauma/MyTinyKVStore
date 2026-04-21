#include "kvstore.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <map>
#include <mutex>
#include <optional>
#include <random>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <unistd.h>
#include <sys/wait.h>

namespace {

#pragma pack(push, 1)
struct SnapshotHeader {
    char magic[8];
    uint32_t version;
    uint32_t reserved;
};

struct SnapshotEntryHeaderV1 {
    uint32_t magic;
    int32_t key;
    uint32_t value_size;
};

struct WalRecordHeader {
    uint32_t magic;
    uint16_t version;
    uint8_t type;
    uint8_t reserved;
    uint32_t key_size;
    uint32_t value_size;
    uint32_t checksum;
};

struct WalRecordHeaderV1 {
    uint32_t magic;
    uint16_t version;
    uint8_t type;
    uint8_t reserved;
    int32_t key;
    uint32_t value_size;
    uint32_t checksum;
};
#pragma pack(pop)

constexpr char kSnapshotMagic[8] = {'K', 'V', 'S', 'N', 'A', 'P', '0', '1'};
constexpr uint32_t kWalMagic = 0x4B565741;
constexpr uint32_t kSnapshotEntryMagic = 0x4B565345;

Value text(const std::string& input) {
    return Value(std::vector<uint8_t>(input.begin(), input.end()));
}

std::string as_string(const Value& value) {
    return std::string(value.bytes.begin(), value.bytes.end());
}

void require(bool condition, const std::string& message) {
    if (!condition) {
        throw std::runtime_error(message);
    }
}

class TestDir {
public:
    explicit TestDir(const std::string& name) : path_(std::filesystem::temp_directory_path() / ("kvstore_" + name + "_" + std::to_string(::getpid()))) {
        std::filesystem::remove_all(path_);
        std::filesystem::create_directories(path_);
    }

    ~TestDir() {
        std::error_code ec;
        std::filesystem::remove_all(path_, ec);
    }

    std::string file(const std::string& name) const {
        return (path_ / name).string();
    }

private:
    std::filesystem::path path_;
};

void append_bytes(const std::string& path, std::initializer_list<uint8_t> data) {
    std::ofstream out(path, std::ios::binary | std::ios::app);
    for (uint8_t byte : data) {
        out.put(static_cast<char>(byte));
    }
}

void flip_byte(const std::string& path, std::streamoff offset) {
    std::fstream file(path, std::ios::binary | std::ios::in | std::ios::out);
    file.seekg(offset);
    const int original = file.get();
    require(original != EOF, "Expected byte to corrupt");
    file.seekp(offset);
    file.put(static_cast<char>(original ^ 0xFF));
}

void write_u32_at(const std::string& path, std::streamoff offset, uint32_t value) {
    std::fstream file(path, std::ios::binary | std::ios::in | std::ios::out);
    require(file.is_open(), "Expected file to exist for overwrite");
    file.seekp(offset);
    file.write(reinterpret_cast<const char*>(&value), sizeof(value));
}

uintmax_t file_size_or_zero(const std::string& path) {
    std::error_code ec;
    const auto size = std::filesystem::file_size(path, ec);
    return ec ? 0 : size;
}

uint64_t histogram_total(const std::array<uint64_t, kWriteLatencyBucketCount>& histogram) {
    uint64_t total = 0;
    for (uint64_t value : histogram) {
        total += value;
    }
    return total;
}

uint32_t fnv1a_append(uint32_t seed, const void* data, size_t size) {
    const auto* bytes = static_cast<const uint8_t*>(data);
    uint32_t hash = seed;
    for (size_t i = 0; i < size; ++i) {
        hash ^= bytes[i];
        hash *= 16777619u;
    }
    return hash;
}

uint32_t legacy_checksum(uint8_t type, int32_t key, const Value& value) {
    uint32_t hash = 2166136261u;
    const uint32_t value_size = static_cast<uint32_t>(value.bytes.size());
    hash = fnv1a_append(hash, &type, sizeof(type));
    hash = fnv1a_append(hash, &key, sizeof(key));
    hash = fnv1a_append(hash, &value_size, sizeof(value_size));
    if (!value.bytes.empty()) {
        hash = fnv1a_append(hash, value.bytes.data(), value.bytes.size());
    }
    return hash;
}

void write_legacy_snapshot_v1(const std::string& path, const std::vector<std::pair<int32_t, Value>>& entries) {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    SnapshotHeader header {};
    std::memcpy(header.magic, kSnapshotMagic, sizeof(kSnapshotMagic));
    header.version = 1;
    header.reserved = 0;
    out.write(reinterpret_cast<const char*>(&header), sizeof(header));
    for (const auto& [key, value] : entries) {
        const SnapshotEntryHeaderV1 entry {
            kSnapshotEntryMagic,
            key,
            static_cast<uint32_t>(value.bytes.size()),
        };
        out.write(reinterpret_cast<const char*>(&entry), sizeof(entry));
        if (!value.bytes.empty()) {
            out.write(reinterpret_cast<const char*>(value.bytes.data()), static_cast<std::streamsize>(value.bytes.size()));
        }
    }
}

void append_legacy_wal_record_v1(
    const std::string& path,
    uint8_t type,
    int32_t key,
    const Value& value) {
    std::ofstream out(path, std::ios::binary | std::ios::app);
    const WalRecordHeaderV1 header {
        kWalMagic,
        1,
        type,
        0,
        key,
        static_cast<uint32_t>(value.bytes.size()),
        legacy_checksum(type, key, value),
    };
    out.write(reinterpret_cast<const char*>(&header), sizeof(header));
    if (!value.bytes.empty()) {
        out.write(reinterpret_cast<const char*>(value.bytes.data()), static_cast<std::streamsize>(value.bytes.size()));
    }
}

void wait_for_start(std::atomic<int>& ready, std::atomic<bool>& start_signal, int target_count) {
    ready.fetch_add(1, std::memory_order_relaxed);
    while (ready.load(std::memory_order_acquire) < target_count) {
        std::this_thread::yield();
    }
    while (!start_signal.load(std::memory_order_acquire)) {
        std::this_thread::yield();
    }
}

template <typename Predicate>
void wait_until(Predicate predicate, const std::string& failure_message, int timeout_ms = 2000) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) {
            return;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    if (!predicate()) {
        throw std::runtime_error(failure_message);
    }
}

std::string g_program_path;

void run_failpoint_child(const std::string& scenario, const std::string& db_path) {
    pid_t child = ::fork();
    require(child >= 0, "fork should succeed");
    if (child == 0) {
        ::execl(
            g_program_path.c_str(),
            g_program_path.c_str(),
            "fault-inject",
            scenario.c_str(),
            db_path.c_str(),
            static_cast<char*>(nullptr));
        ::_exit(127);
    }

    int status = 0;
    require(::waitpid(child, &status, 0) == child, "waitpid should return the child pid");
    require(WIFEXITED(status), "fault-injection child should exit normally");
    require(WEXITSTATUS(status) == 86, "fault-injection child should terminate at the configured failpoint");
}

int run_fault_injection_scenario(const std::string& scenario, const std::string& db_path) {
    ::setenv("KVSTORE_FAIL_ACTION", "crash", 1);

    if (scenario == "wal_after_fsync") {
        KVStore store(db_path);
        store.Put(1, text("stable"));
        ::setenv("KVSTORE_FAILPOINT", "after_wal_fsync_before_apply", 1);
        store.Put(2, text("latest"));
        return 2;
    }

    if (scenario == "wal_after_fsync_batch") {
        KVStore store(db_path);
        store.Put(1, text("stable"));
        ::setenv("KVSTORE_FAILPOINT", "after_wal_fsync_before_apply", 1);
        store.WriteBatch({
            BatchWriteOperation::PutInt(2, text("latest")),
            BatchWriteOperation::Put("alpha", text("batch-value")),
            BatchWriteOperation::DeleteInt(1),
        });
        return 6;
    }

    if (scenario == "snapshot_after_rename") {
        KVStore store(db_path);
        store.Put(1, text("one"));
        store.Put(2, text("two"));
        ::setenv("KVSTORE_FAILPOINT", "after_snapshot_rename_before_wal_reset", 1);
        store.Compact();
        return 3;
    }

    if (scenario == "wal_rotation_before_reopen") {
        KVStore store(db_path);
        store.Put(10, text("ten"));
        store.Put(20, text("twenty"));
        ::setenv("KVSTORE_FAILPOINT", "after_wal_rotation_before_reopen", 1);
        store.Compact();
        return 4;
    }

    if (scenario == "snapshot_before_rename") {
        KVStore store(db_path);
        store.Put(7, text("seven"));
        store.Put(8, text("eight"));
        ::setenv("KVSTORE_FAILPOINT", "after_snapshot_fsync_before_rename", 1);
        store.Compact();
        return 5;
    }

    std::cerr << "Unknown fault-injection scenario: " << scenario << '\n';
    return 1;
}

int run_inspect_format(const std::string& db_path) {
    const std::string wal_path = db_path + ".wal";
    std::ifstream snapshot(db_path, std::ios::binary);
    if (!snapshot.is_open()) {
        std::cerr << "snapshot_exists=0" << std::endl;
        return 1;
    }

    SnapshotHeader snapshot_header {};
    snapshot.read(reinterpret_cast<char*>(&snapshot_header), sizeof(snapshot_header));
    if (snapshot.gcount() != static_cast<std::streamsize>(sizeof(snapshot_header))) {
        std::cerr << "snapshot_exists=1 snapshot_truncated=1" << std::endl;
        return 2;
    }

    std::cout << "snapshot_exists=1"
              << " snapshot_magic_ok=" << (std::memcmp(snapshot_header.magic, kSnapshotMagic, sizeof(kSnapshotMagic)) == 0 ? 1 : 0)
              << " snapshot_version=" << snapshot_header.version
              << " snapshot_size=" << file_size_or_zero(db_path);

    std::ifstream wal(wal_path, std::ios::binary);
    if (!wal.is_open()) {
        std::cout << " wal_exists=0" << std::endl;
        return 0;
    }

    WalRecordHeader wal_header {};
    wal.read(reinterpret_cast<char*>(&wal_header), sizeof(wal_header));
    if (wal.gcount() == 0) {
        std::cout << " wal_exists=1 wal_empty=1 wal_size=" << file_size_or_zero(wal_path) << std::endl;
        return 0;
    }
    if (wal.gcount() != static_cast<std::streamsize>(sizeof(wal_header))) {
        std::cout << " wal_exists=1 wal_truncated=1 wal_size=" << file_size_or_zero(wal_path) << std::endl;
        return 0;
    }

    std::cout << " wal_exists=1"
              << " wal_empty=0"
              << " wal_magic=0x" << std::hex << wal_header.magic << std::dec
              << " wal_version=" << wal_header.version
              << " wal_first_type=" << static_cast<int>(wal_header.type)
              << " wal_size=" << file_size_or_zero(wal_path)
              << std::endl;
    return 0;
}

int run_rewrite_format(const std::string& db_path) {
    KVStore store(db_path);
    store.Compact();
    return 0;
}

std::optional<KVStoreProfile> parse_profile_name(const std::string& name) {
    if (name == "balanced") {
        return KVStoreProfile::kBalanced;
    }
    if (name == "write-heavy") {
        return KVStoreProfile::kWriteHeavy;
    }
    if (name == "read-heavy") {
        return KVStoreProfile::kReadHeavy;
    }
    if (name == "low-latency") {
        return KVStoreProfile::kLowLatency;
    }
    return std::nullopt;
}

int run_profile_json(const std::string& name) {
    const auto profile = parse_profile_name(name);
    if (!profile.has_value()) {
        std::cerr << "Unknown profile: " << name << '\n';
        return 1;
    }
    std::cout << OptionsToJson(RecommendedOptions(*profile)) << std::endl;
    return 0;
}

void test_basic_persistence() {
    TestDir dir("basic");
    const std::string db_path = dir.file("store.dat");

    {
        KVStore store(db_path);
        store.Put(1, text("alpha"));
        store.Put(2, text("beta"));
    }

    KVStore reopened(db_path);
    const auto value1 = reopened.Get(1);
    const auto value2 = reopened.Get(2);
    require(value1.has_value(), "key 1 should exist after reopen");
    require(value2.has_value(), "key 2 should exist after reopen");
    require(as_string(*value1) == "alpha", "key 1 should keep its value");
    require(as_string(*value2) == "beta", "key 2 should keep its value");
}

void test_string_keys_do_not_collide_with_int_keys() {
    TestDir dir("string_keys");
    const std::string db_path = dir.file("store.dat");

    {
        KVStore store(db_path);
        store.Put(42, text("int-key"));
        store.Put(std::string("42"), text("string-key"));
        store.Put(std::string("alpha"), text("word"));
    }

    KVStore reopened(db_path);
    const auto int_value = reopened.Get(42);
    const auto string_value = reopened.Get(std::string("42"));
    const auto alpha_value = reopened.Get(std::string("alpha"));
    require(int_value.has_value(), "int key should persist");
    require(string_value.has_value(), "string key should persist");
    require(alpha_value.has_value(), "string word key should persist");
    require(as_string(*int_value) == "int-key", "int key should keep its own namespace");
    require(as_string(*string_value) == "string-key", "string key should not collide with int namespace");
    require(as_string(*alpha_value) == "word", "string API should round-trip normal string keys");
}

void test_string_scan_returns_sorted_range() {
    TestDir dir("string_scan");
    const std::string db_path = dir.file("store.dat");
    KVStore store(db_path);

    store.Put(std::string("apple"), text("a"));
    store.Put(std::string("banana"), text("b"));
    store.Put(std::string("carrot"), text("c"));
    store.Put(std::string("date"), text("d"));
    store.Put(7, text("int-should-not-appear"));

    const auto results = store.Scan("banana", "date");
    require(results.size() == 3, "scan should return all string keys in the requested inclusive range");
    require(results[0].first == "banana" && as_string(results[0].second) == "b", "scan should start at banana");
    require(results[1].first == "carrot" && as_string(results[1].second) == "c", "scan should keep lexical order");
    require(results[2].first == "date" && as_string(results[2].second) == "d", "scan should include the upper bound");
}

void test_write_batch_persists_mixed_key_types() {
    TestDir dir("batch_mixed_keys");
    const std::string db_path = dir.file("store.dat");

    {
        KVStore store(db_path);
        store.WriteBatch({
            BatchWriteOperation::PutInt(42, text("int-key")),
            BatchWriteOperation::Put("42", text("string-key")),
            BatchWriteOperation::Put("alpha", text("word")),
        });
    }

    KVStore reopened(db_path);
    const auto int_value = reopened.Get(42);
    const auto string_value = reopened.Get(std::string("42"));
    const auto alpha_value = reopened.Get(std::string("alpha"));
    require(int_value.has_value(), "batch int key should persist");
    require(string_value.has_value(), "batch string key should persist");
    require(alpha_value.has_value(), "batch string key should persist");
    require(as_string(*int_value) == "int-key", "batch int key should keep its namespace");
    require(as_string(*string_value) == "string-key", "batch string key should not collide with int namespace");
    require(as_string(*alpha_value) == "word", "batch string key should round-trip");
}

void test_write_batch_mixes_put_and_delete() {
    TestDir dir("batch_put_delete");
    const std::string db_path = dir.file("store.dat");
    KVStore store(db_path);

    store.Put(1, text("old"));
    store.Put(9, text("remove-me"));
    store.Put(std::string("alpha"), text("old-alpha"));

    store.WriteBatch({
        BatchWriteOperation::PutInt(1, text("new")),
        BatchWriteOperation::Delete("alpha"),
        BatchWriteOperation::Put("beta", text("beta-value")),
        BatchWriteOperation::DeleteInt(9),
    });

    const auto one = store.Get(1);
    const auto nine = store.Get(9);
    const auto alpha = store.Get(std::string("alpha"));
    const auto beta = store.Get(std::string("beta"));
    require(one.has_value() && as_string(*one) == "new", "batch put should replace existing int values");
    require(!nine.has_value(), "batch delete should remove int keys");
    require(!alpha.has_value(), "batch delete should remove string keys");
    require(beta.has_value() && as_string(*beta) == "beta-value", "batch put should insert new string keys");
}

void test_write_batch_preserves_operation_order() {
    TestDir dir("batch_order");
    const std::string db_path = dir.file("store.dat");

    {
        KVStore store(db_path);
        store.WriteBatch({
            BatchWriteOperation::PutInt(7, text("first")),
            BatchWriteOperation::DeleteInt(7),
            BatchWriteOperation::PutInt(7, text("second")),
            BatchWriteOperation::Put("gamma", text("a")),
            BatchWriteOperation::Put("gamma", text("b")),
        });
    }

    KVStore reopened(db_path);
    const auto seven = reopened.Get(7);
    const auto gamma = reopened.Get(std::string("gamma"));
    require(seven.has_value() && as_string(*seven) == "second", "batch operations should apply in-order for int keys");
    require(gamma.has_value() && as_string(*gamma) == "b", "batch operations should apply in-order for string keys");
}

void test_put_copies_input_value() {
    TestDir dir("copy");
    const std::string db_path = dir.file("store.dat");
    KVStore store(db_path);

    Value original = text("alpha");
    store.Put(7, original);
    original.bytes[0] = static_cast<uint8_t>('x');

    const auto stored = store.Get(7);
    require(stored.has_value(), "copied value should exist");
    require(as_string(*stored) == "alpha", "stored value should not alias caller buffer");
}

void test_recovery_from_wal_without_compaction() {
    TestDir dir("wal_recovery");
    const std::string db_path = dir.file("store.dat");
    const std::string wal_path = db_path + ".wal";

    {
        KVStore store(db_path);
        store.Put(10, text("first"));
        store.Put(11, text("second"));
        store.Delete(10);
    }

    require(file_size_or_zero(db_path) == 16, "snapshot should still only contain the header before compaction");
    require(file_size_or_zero(wal_path) > sizeof(WalRecordHeader), "WAL should contain persisted operations");

    KVStore reopened(db_path);
    require(!reopened.Get(10).has_value(), "deleted key should stay deleted after WAL replay");
    const auto value = reopened.Get(11);
    require(value.has_value(), "remaining key should survive WAL replay");
    require(as_string(*value) == "second", "WAL replay should restore the latest value");
}

void test_legacy_v1_snapshot_and_wal_are_readable() {
    TestDir dir("legacy_v1_read");
    const std::string db_path = dir.file("store.dat");
    const std::string wal_path = db_path + ".wal";

    write_legacy_snapshot_v1(db_path, {
        {1, text("legacy-one")},
        {2, text("legacy-two")},
    });
    append_legacy_wal_record_v1(wal_path, 1, 3, text("legacy-three"));
    append_legacy_wal_record_v1(wal_path, 2, 2, Value {});

    KVStore store(db_path);
    const auto one = store.Get(1);
    const auto two = store.Get(2);
    const auto three = store.Get(3);
    require(one.has_value() && as_string(*one) == "legacy-one", "v1 snapshot key 1 should load");
    require(!two.has_value(), "v1 WAL delete should apply on top of snapshot state");
    require(three.has_value() && as_string(*three) == "legacy-three", "v1 WAL put should replay");
}

void test_legacy_v1_rewrite_upgrades_snapshot_to_v2() {
    TestDir dir("legacy_v1_upgrade");
    const std::string db_path = dir.file("store.dat");
    const std::string wal_path = db_path + ".wal";

    write_legacy_snapshot_v1(db_path, {
        {7, text("seven")},
    });
    append_legacy_wal_record_v1(wal_path, 1, 9, text("nine"));

    require(run_rewrite_format(db_path) == 0, "rewrite-format should succeed on legacy data");

    std::ifstream snapshot(db_path, std::ios::binary);
    SnapshotHeader header {};
    snapshot.read(reinterpret_cast<char*>(&header), sizeof(header));
    require(snapshot.gcount() == static_cast<std::streamsize>(sizeof(header)), "rewritten snapshot should contain a full header");
    require(header.version == 2, "rewrite-format should upgrade legacy snapshot files to version 2");
    require(file_size_or_zero(wal_path) == 0, "rewrite-format should rotate WAL back to empty");

    KVStore reopened(db_path);
    const auto seven = reopened.Get(7);
    const auto nine = reopened.Get(9);
    require(seven.has_value() && as_string(*seven) == "seven", "rewritten data should preserve snapshot value");
    require(nine.has_value() && as_string(*nine) == "nine", "rewritten data should preserve WAL value");
}

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

void test_truncated_wal_tail_is_ignored() {
    TestDir dir("truncated_tail");
    const std::string db_path = dir.file("store.dat");
    const std::string wal_path = db_path + ".wal";

    {
        KVStore store(db_path);
        store.Put(9, text("stable"));
    }

    append_bytes(wal_path, {0xAA, 0xBB, 0xCC});

    KVStore reopened(db_path);
    const auto value = reopened.Get(9);
    require(value.has_value(), "valid WAL records should survive a truncated tail");
    require(as_string(*value) == "stable", "truncated WAL tail must be ignored");
}

void test_corrupted_wal_record_throws() {
    TestDir dir("corrupt_wal");
    const std::string db_path = dir.file("store.dat");
    const std::string wal_path = db_path + ".wal";

    {
        KVStore store(db_path);
        store.Put(5, text("first"));
        store.Put(6, text("second"));
    }

    flip_byte(wal_path, static_cast<std::streamoff>(sizeof(WalRecordHeader) + 1));

    bool threw = false;
    try {
        KVStore reopened(db_path);
        (void)reopened;
    } catch (const KVStoreError&) {
        threw = true;
    }
    require(threw, "corrupted WAL payload should raise KVStoreError");
}

void test_corrupted_snapshot_throws() {
    TestDir dir("corrupt_snapshot");
    const std::string db_path = dir.file("store.dat");

    {
        KVStore store(db_path);
        store.Put(1, text("snap"));
        store.Compact();
    }

    flip_byte(db_path, 0);

    bool threw = false;
    try {
        KVStore reopened(db_path);
        (void)reopened;
    } catch (const KVStoreError&) {
        threw = true;
    }
    require(threw, "corrupted snapshot magic should raise KVStoreError");
}

void test_unsupported_snapshot_version_throws() {
    TestDir dir("snapshot_version");
    const std::string db_path = dir.file("store.dat");

    {
        KVStore store(db_path);
        store.Put(1, text("versioned"));
        store.Compact();
    }

    write_u32_at(db_path, 8, 99);

    bool threw = false;
    try {
        KVStore reopened(db_path);
        (void)reopened;
    } catch (const KVStoreError&) {
        threw = true;
    }
    require(threw, "unsupported snapshot version should raise KVStoreError");
}

void test_crash_after_wal_fsync_recovers_latest_write() {
    TestDir dir("crash_wal_fsync");
    const std::string db_path = dir.file("store.dat");

    run_failpoint_child("wal_after_fsync", db_path);

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

    run_failpoint_child("wal_after_fsync_batch", db_path);

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

    run_failpoint_child("snapshot_after_rename", db_path);

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

    run_failpoint_child("snapshot_before_rename", db_path);

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

    run_failpoint_child("wal_rotation_before_reopen", db_path);

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

void test_batching_metrics_are_reported() {
    TestDir dir("batch_metrics");
    const std::string db_path = dir.file("store.dat");
    KVStoreOptions options;
    options.max_batch_size = 8;
    options.max_batch_delay_us = 20000;
    KVStore store(db_path, options);

    constexpr int kWriterCount = 8;
    std::atomic<int> ready {0};
    std::atomic<bool> start_signal {false};
    std::vector<std::thread> writers;

    for (int writer_id = 0; writer_id < kWriterCount; ++writer_id) {
        writers.emplace_back([&store, &ready, &start_signal, writer_id]() {
            wait_for_start(ready, start_signal, kWriterCount);
            store.Put(writer_id, text("value_" + std::to_string(writer_id)));
        });
    }

    while (ready.load(std::memory_order_acquire) < kWriterCount) {
        std::this_thread::yield();
    }
    start_signal.store(true, std::memory_order_release);

    for (auto& writer : writers) {
        writer.join();
    }

    const KVStoreMetrics metrics = store.GetMetrics();
    require(metrics.enqueued_write_requests == kWriterCount, "metrics should count all enqueued writes");
    require(metrics.committed_write_requests == kWriterCount, "metrics should count all committed writes");
    require(metrics.committed_write_batches >= 1, "writer should report at least one committed batch");
    require(metrics.max_committed_batch_size > 1, "batched writer should group concurrent writes");
    require(metrics.wal_fsync_calls < metrics.committed_write_requests, "batching should reduce fsync count below write count");
    require(metrics.pending_queue_depth == 0, "queue should drain after all writers finish");
    require(histogram_total(metrics.write_latency_histogram) == metrics.committed_write_requests,
            "latency histogram should account for every committed write");
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

void test_options_to_json_reports_profile_fields() {
    const std::string json = OptionsToJson(RecommendedOptions(KVStoreProfile::kBalanced));
    require(!json.empty() && json.front() == '{' && json.back() == '}', "options json should be a JSON object");
    require(json.find("\"max_batch_size\":") != std::string::npos,
            "options json should include batching fields");
    require(json.find("\"adaptive_objective_enabled\":true") != std::string::npos,
            "options json should include boolean profile settings");
    require(json.find("\"auto_compact_wal_bytes_threshold\":") != std::string::npos,
            "options json should include compaction thresholds");
}

void test_metrics_to_json_reports_core_fields() {
    TestDir dir("metrics_json");
    const std::string db_path = dir.file("store.dat");
    KVStore store(db_path);

    store.Put(1, text("json"));
    store.Delete(1);

    const std::string json = MetricsToJson(store.GetMetrics());
    require(!json.empty() && json.front() == '{' && json.back() == '}', "metrics json should be a JSON object");
    require(json.find("\"committed_write_requests\":2") != std::string::npos,
            "metrics json should include committed write counts");
    require(json.find("\"write_latency_histogram\":[") != std::string::npos,
            "metrics json should include the latency histogram array");
    require(json.find("\"wal_fsync_calls\":") != std::string::npos,
            "metrics json should include fsync metrics");
}

void test_auto_compaction_triggers_and_preserves_state() {
    TestDir dir("auto_compaction");
    const std::string db_path = dir.file("store.dat");
    const std::string wal_path = db_path + ".wal";

    KVStoreOptions options;
    options.max_batch_size = 4;
    options.max_batch_delay_us = 5000;
    options.auto_compact_wal_bytes_threshold = 128;

    {
        KVStore store(db_path, options);
        for (int i = 0; i < 12; ++i) {
            store.Put(i, text("auto_value_" + std::to_string(i)));
        }

        wait_until(
            [&store, &options]() {
                const KVStoreMetrics metrics = store.GetMetrics();
                return metrics.auto_compactions_completed >= 1 &&
                       metrics.wal_bytes_since_compaction < options.auto_compact_wal_bytes_threshold;
            },
            "auto compaction did not settle within the expected time");

        const KVStoreMetrics metrics = store.GetMetrics();
        require(metrics.auto_compactions_completed >= 1, "auto compaction should trigger after WAL crosses the threshold");
        require(metrics.wal_bytes_since_compaction < options.auto_compact_wal_bytes_threshold,
                "auto compaction should reset accumulated WAL bytes");
        require(file_size_or_zero(wal_path) < options.auto_compact_wal_bytes_threshold,
                "WAL should be rotated down after auto compaction");
    }

    KVStore reopened(db_path);
    for (int i = 0; i < 12; ++i) {
        const auto value = reopened.Get(i);
        require(value.has_value(), "auto compaction must preserve committed values");
        require(as_string(*value) == "auto_value_" + std::to_string(i),
                "reopened state should match values written before auto compaction");
    }
}

void test_manual_and_auto_compaction_metrics_coexist() {
    TestDir dir("manual_auto_metrics");
    const std::string db_path = dir.file("store.dat");

    KVStoreOptions options;
    options.max_batch_size = 2;
    options.max_batch_delay_us = 0;
    options.auto_compact_wal_bytes_threshold = 96;

    KVStore store(db_path, options);
    for (int i = 0; i < 6; ++i) {
        store.Put(i, text("payload_" + std::to_string(i)));
    }
    store.Compact();

    const KVStoreMetrics metrics = store.GetMetrics();
    require(metrics.auto_compactions_completed >= 1, "auto compaction count should be visible in metrics");
    require(metrics.manual_compactions_completed == 1, "manual compaction count should be tracked separately");
    require(metrics.compact_requests == 1, "only explicit compaction requests should increment compact_requests");
}

void test_invalid_wal_ratio_triggers_auto_compaction() {
    TestDir dir("invalid_ratio_compaction");
    const std::string db_path = dir.file("store.dat");

    KVStoreOptions options;
    options.max_batch_size = 1;
    options.max_batch_delay_us = 0;
    options.auto_compact_invalid_wal_ratio_percent = 50;

    {
        KVStore store(db_path, options);
        for (int i = 0; i < 12; ++i) {
            store.Put(42, text("overwrite_" + std::to_string(i)));
        }

        wait_until(
            [&store]() {
                const KVStoreMetrics metrics = store.GetMetrics();
                return metrics.auto_compactions_completed >= 1 &&
                       metrics.obsolete_wal_bytes_since_compaction <= metrics.live_wal_bytes_since_compaction;
            },
            "invalid WAL ratio auto compaction did not settle within the expected time");

        const KVStoreMetrics metrics = store.GetMetrics();
        require(metrics.auto_compactions_completed >= 1, "invalid WAL ratio should trigger auto compaction");
    }

    KVStore reopened(db_path);
    const auto value = reopened.Get(42);
    require(value.has_value(), "latest overwritten value should survive invalid-ratio compaction");
    require(as_string(*value) == "overwrite_11", "reopened state should contain the latest overwritten value");
}

void test_wal_obsolete_byte_metrics_are_consistent() {
    TestDir dir("wal_byte_metrics");
    const std::string db_path = dir.file("store.dat");

    KVStore store(db_path);
    store.Put(1, text("first"));
    store.Put(1, text("second"));
    store.Delete(1);

    const KVStoreMetrics metrics = store.GetMetrics();
    require(metrics.wal_bytes_since_compaction == metrics.live_wal_bytes_since_compaction + metrics.obsolete_wal_bytes_since_compaction,
            "live and obsolete WAL bytes should partition the accumulated WAL size");
    require(metrics.obsolete_wal_bytes_since_compaction > 0,
            "overwrites and deletes should create obsolete WAL bytes");
}

void test_batch_wal_byte_limit_is_respected() {
    TestDir dir("batch_byte_limit");
    const std::string db_path = dir.file("store.dat");

    KVStoreOptions options;
    options.max_batch_size = 64;
    options.max_batch_wal_bytes = 60;
    options.max_batch_delay_us = 20000;

    KVStore store(db_path, options);

    constexpr int kWriterCount = 4;
    std::atomic<int> ready {0};
    std::atomic<bool> start_signal {false};
    std::vector<std::thread> writers;

    for (int writer_id = 0; writer_id < kWriterCount; ++writer_id) {
        writers.emplace_back([&store, &ready, &start_signal, writer_id]() {
            wait_for_start(ready, start_signal, kWriterCount);
            store.Put(writer_id, text("payload_1234567890"));
        });
    }

    while (ready.load(std::memory_order_acquire) < kWriterCount) {
        std::this_thread::yield();
    }
    start_signal.store(true, std::memory_order_release);

    for (auto& writer : writers) {
        writer.join();
    }

    const KVStoreMetrics metrics = store.GetMetrics();
    require(metrics.committed_write_requests == kWriterCount, "all writes should commit under byte-limited batching");
    require(metrics.max_committed_batch_size <= 1, "small WAL byte limit should prevent multi-request batches");
}

void test_latency_histogram_tracks_write_requests() {
    TestDir dir("latency_histogram");
    const std::string db_path = dir.file("store.dat");
    KVStore store(db_path);

    for (int i = 0; i < 10; ++i) {
        store.Put(i, text("latency_" + std::to_string(i)));
    }
    store.Delete(5);

    const KVStoreMetrics metrics = store.GetMetrics();
    require(histogram_total(metrics.write_latency_histogram) == metrics.committed_write_requests,
            "histogram total should equal the number of committed writes");
    require(histogram_total(metrics.write_latency_histogram) == 11,
            "histogram should include puts and deletes but not compaction");
}

void test_adaptive_batching_expands_batch_under_queue_pressure() {
    TestDir dir("adaptive_batching");
    const std::string db_path = dir.file("store.dat");

    KVStoreOptions options;
    options.max_batch_size = 2;
    options.max_batch_delay_us = 20000;
    options.adaptive_batching_enabled = true;
    options.adaptive_queue_depth_threshold = 4;
    options.adaptive_batch_size_multiplier = 4;

    KVStore store(db_path, options);

    constexpr int kWriterCount = 8;
    std::atomic<int> ready {0};
    std::atomic<bool> start_signal {false};
    std::vector<std::thread> writers;

    for (int writer_id = 0; writer_id < kWriterCount; ++writer_id) {
        writers.emplace_back([&store, &ready, &start_signal, writer_id]() {
            wait_for_start(ready, start_signal, kWriterCount);
            store.Put(writer_id, text("adaptive_" + std::to_string(writer_id)));
        });
    }

    while (ready.load(std::memory_order_acquire) < kWriterCount) {
        std::this_thread::yield();
    }
    start_signal.store(true, std::memory_order_release);

    for (auto& writer : writers) {
        writer.join();
    }

    const KVStoreMetrics metrics = store.GetMetrics();
    require(metrics.committed_write_requests == kWriterCount, "adaptive batching should still commit all queued writes");
    require(metrics.max_committed_batch_size > options.max_batch_size,
            "adaptive batching should grow batch size beyond the base limit under queue pressure");
    require(metrics.adaptive_batches_completed >= 1, "adaptive batching should report at least one adaptive batch");
}

void test_percentile_and_writer_metrics_are_reported() {
    TestDir dir("writer_metrics");
    const std::string db_path = dir.file("store.dat");

    KVStoreOptions options;
    options.max_batch_size = 4;
    options.max_batch_delay_us = 5000;
    KVStore store(db_path, options);

    constexpr int kWriterCount = 6;
    std::atomic<int> ready {0};
    std::atomic<bool> start_signal {false};
    std::vector<std::thread> writers;

    for (int writer_id = 0; writer_id < kWriterCount; ++writer_id) {
        writers.emplace_back([&store, &ready, &start_signal, writer_id]() {
            wait_for_start(ready, start_signal, kWriterCount);
            store.Put(writer_id, text("writer_metric_payload_" + std::to_string(writer_id)));
        });
    }

    while (ready.load(std::memory_order_acquire) < kWriterCount) {
        std::this_thread::yield();
    }
    start_signal.store(true, std::memory_order_release);

    for (auto& writer : writers) {
        writer.join();
    }

    wait_until(
        [&store]() {
            const KVStoreMetrics metrics = store.GetMetrics();
            return metrics.writer_wait_events >= 1 && metrics.writer_wait_time_us > 0;
        },
        "writer thread did not report idle waiting after draining the queue");

    const KVStoreMetrics metrics = store.GetMetrics();
    require(metrics.max_pending_queue_depth >= 1, "queue high-water mark should be tracked");
    require(metrics.last_committed_batch_wal_bytes > 0, "last committed batch WAL bytes should be reported");
    require(metrics.max_committed_batch_wal_bytes >= metrics.last_committed_batch_wal_bytes,
            "max batch WAL bytes should be at least the last batch size");
    require(metrics.approx_write_latency_p50_us > 0, "p50 write latency should be reported");
    require(metrics.approx_write_latency_p95_us >= metrics.approx_write_latency_p50_us,
            "p95 write latency should not be below p50");
    require(metrics.approx_write_latency_p99_us >= metrics.approx_write_latency_p95_us,
            "p99 write latency should not be below p95");
}

void test_adaptive_flush_shortens_batch_delay() {
    TestDir dir("adaptive_flush");
    const std::string db_path = dir.file("store.dat");

    KVStoreOptions options;
    options.max_batch_size = 16;
    options.max_batch_delay_us = 50000;
    options.adaptive_flush_enabled = true;
    options.adaptive_flush_queue_depth_threshold = 2;
    options.adaptive_flush_delay_divisor = 10;
    options.adaptive_flush_min_batch_delay_us = 1000;

    KVStore store(db_path, options);

    std::atomic<int> ready {0};
    std::atomic<bool> start_signal {false};

    std::thread writer1([&store, &ready, &start_signal]() {
        wait_for_start(ready, start_signal, 2);
        store.Put(1, text("flush_1"));
    });
    std::thread writer2([&store, &ready, &start_signal]() {
        wait_for_start(ready, start_signal, 2);
        store.Put(2, text("flush_2"));
    });

    while (ready.load(std::memory_order_acquire) < 2) {
        std::this_thread::yield();
    }
    start_signal.store(true, std::memory_order_release);

    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    std::thread writer3([&store]() {
        store.Put(3, text("flush_3"));
    });

    writer1.join();
    writer2.join();
    writer3.join();

    const KVStoreMetrics metrics = store.GetMetrics();
    require(metrics.committed_write_requests == 3, "adaptive flush test should commit all writes");
    require(metrics.committed_write_batches >= 2, "adaptive flush should flush the early batch before the delayed write arrives");
    require(metrics.adaptive_flush_batches_completed >= 1, "adaptive flush should record at least one shortened-delay batch");
    require(metrics.min_effective_batch_delay_us > 0 &&
                metrics.min_effective_batch_delay_us < options.max_batch_delay_us,
            "adaptive flush should reduce the effective batch delay below the configured base delay");
}

void test_compaction_long_term_metrics_accumulate() {
    TestDir dir("compaction_totals");
    const std::string db_path = dir.file("store.dat");

    KVStore store(db_path);
    store.Put(1, text("first"));
    store.Compact();
    store.Put(2, text("second"));
    store.Delete(1);
    store.Compact();

    const KVStoreMetrics metrics = store.GetMetrics();
    require(metrics.manual_compactions_completed == 2, "manual compaction count should reflect both runs");
    require(metrics.total_snapshot_bytes_written > sizeof(uint32_t),
            "compaction should accumulate snapshot bytes written over time");
    require(metrics.total_wal_bytes_reclaimed_by_compaction > 0,
            "compaction should accumulate reclaimed WAL bytes over time");
}

void test_latency_target_adaptive_flush_kicks_in() {
    TestDir dir("latency_target_flush");
    const std::string db_path = dir.file("store.dat");

    KVStoreOptions options;
    options.max_batch_size = 16;
    options.max_batch_delay_us = 50000;
    options.adaptive_flush_enabled = true;
    options.adaptive_flush_queue_depth_threshold = 1000;
    options.adaptive_flush_delay_divisor = 10;
    options.adaptive_flush_min_batch_delay_us = 1000;
    options.adaptive_latency_target_p95_us = 50;

    KVStore store(db_path, options);
    for (int i = 0; i < 4; ++i) {
        store.Put(i, text("warmup_" + std::to_string(i)));
    }

    std::atomic<int> ready {0};
    std::atomic<bool> start_signal {false};
    std::thread writer1([&store, &ready, &start_signal]() {
        wait_for_start(ready, start_signal, 2);
        store.Put(10, text("latency_target_a"));
    });
    std::thread writer2([&store, &ready, &start_signal]() {
        wait_for_start(ready, start_signal, 2);
        store.Put(11, text("latency_target_b"));
    });

    while (ready.load(std::memory_order_acquire) < 2) {
        std::this_thread::yield();
    }
    start_signal.store(true, std::memory_order_release);

    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    std::thread writer3([&store]() {
        store.Put(12, text("latency_target_c"));
    });

    writer1.join();
    writer2.join();
    writer3.join();

    const KVStoreMetrics metrics = store.GetMetrics();
    require(metrics.adaptive_latency_target_batches_completed >= 1,
            "latency target should trigger at least one earlier-flush batch");
    require(metrics.min_effective_batch_delay_us > 0 &&
                metrics.min_effective_batch_delay_us < options.max_batch_delay_us,
            "latency target should reduce effective batch delay below the base delay");
}

void test_fsync_pressure_can_relax_batch_delay() {
    TestDir dir("fsync_pressure");
    const std::string db_path = dir.file("store.dat");

    KVStoreOptions options;
    options.max_batch_size = 1;
    options.max_batch_delay_us = 1000;
    options.adaptive_fsync_pressure_per_1000_writes_threshold = 900;
    options.adaptive_fsync_pressure_delay_multiplier = 10;
    options.adaptive_fsync_pressure_max_batch_delay_us = 10000;

    KVStore store(db_path, options);
    for (int i = 0; i < 4; ++i) {
        store.Put(i, text("pressure_" + std::to_string(i)));
    }

    store.Put(100, text("pressure_probe"));

    const KVStoreMetrics metrics = store.GetMetrics();
    require(metrics.adaptive_fsync_pressure_batches_completed >= 1,
            "high fsync pressure should trigger a relaxed-delay batch");
    require(metrics.last_effective_batch_delay_us > options.max_batch_delay_us,
            "fsync pressure should expand the effective batch delay above the base delay");
    require(metrics.observed_fsync_pressure_per_1000_writes >=
                options.adaptive_fsync_pressure_per_1000_writes_threshold,
            "observed fsync pressure metric should reflect the singleton-write workload");
    require(metrics.max_effective_batch_delay_us >= metrics.last_effective_batch_delay_us,
            "max effective batch delay should track the expanded delay");
}

void test_recent_window_metrics_capture_bursts() {
    TestDir dir("recent_window");
    const std::string db_path = dir.file("store.dat");

    KVStoreOptions options;
    options.max_batch_size = 4;
    options.max_batch_delay_us = 5000;
    options.adaptive_recent_window_batches = 4;
    options.adaptive_recent_write_sample_limit = 16;
    KVStore store(db_path, options);

    constexpr int kWriterCount = 4;
    std::atomic<int> ready {0};
    std::atomic<bool> start_signal {false};
    std::vector<std::thread> writers;

    for (int writer_id = 0; writer_id < kWriterCount; ++writer_id) {
        writers.emplace_back([&store, &ready, &start_signal, writer_id]() {
            wait_for_start(ready, start_signal, kWriterCount);
            store.Put(writer_id, text("recent_" + std::to_string(writer_id)));
        });
    }

    while (ready.load(std::memory_order_acquire) < kWriterCount) {
        std::this_thread::yield();
    }
    start_signal.store(true, std::memory_order_release);

    for (auto& writer : writers) {
        writer.join();
    }

    const KVStoreMetrics metrics = store.GetMetrics();
    require(metrics.recent_window_batch_count >= 1, "recent batch window should track completed batches");
    require(metrics.recent_peak_queue_depth >= 2,
            "recent queue peak should reflect that the burst created queue pressure");
    require(metrics.recent_avg_batch_size >= 1, "recent batch window should track average batch size");
    require(metrics.recent_observed_write_latency_p95_us > 0,
            "recent latency window should expose a non-zero recent p95");
}

void test_read_heavy_signal_prefers_shorter_batches() {
    TestDir dir("read_heavy");
    const std::string db_path = dir.file("store.dat");

    KVStoreOptions options;
    options.max_batch_size = 16;
    options.max_batch_delay_us = 30000;
    options.adaptive_read_heavy_read_per_1000_ops_threshold = 800;
    options.adaptive_read_heavy_delay_divisor = 10;
    options.adaptive_read_heavy_batch_size_divisor = 4;
    options.adaptive_flush_min_batch_delay_us = 500;
    KVStore store(db_path, options);

    for (int i = 0; i < 128; ++i) {
        (void)store.Get(i);
    }

    std::thread writer1([&store]() {
        store.Put(1, text("read_heavy_a"));
    });
    std::thread writer2([&store]() {
        store.Put(2, text("read_heavy_b"));
    });
    writer1.join();
    writer2.join();

    const KVStoreMetrics metrics = store.GetMetrics();
    require(metrics.adaptive_read_heavy_batches_completed >= 1,
            "read-heavy workload should trigger reader-friendly batching");
    require(metrics.recent_read_ratio_per_1000_ops >= 800,
            "recent read ratio should reflect the read-heavy workload");
    require(metrics.last_effective_batch_delay_us < options.max_batch_delay_us,
            "read-heavy workload should shorten the effective batch delay");
}

void test_compaction_pressure_signal_relaxes_batch_delay() {
    TestDir dir("compaction_pressure");
    const std::string db_path = dir.file("store.dat");

    KVStoreOptions options;
    options.max_batch_size = 1;
    options.max_batch_delay_us = 1000;
    options.adaptive_compaction_pressure_obsolete_ratio_percent_threshold = 50;
    options.adaptive_compaction_pressure_delay_multiplier = 5;
    KVStore store(db_path, options);

    for (int i = 0; i < 6; ++i) {
        store.Put(7, text("obsolete_" + std::to_string(i)));
    }
    store.Put(8, text("pressure_probe"));

    const KVStoreMetrics metrics = store.GetMetrics();
    require(metrics.observed_obsolete_wal_ratio_percent >= 50,
            "obsolete WAL ratio should reflect repeated overwrites");
    require(metrics.adaptive_compaction_pressure_batches_completed >= 1,
            "high obsolete WAL ratio should relax batching before compaction");
    require(metrics.last_effective_batch_delay_us > options.max_batch_delay_us,
            "compaction pressure should expand the effective batch delay");
}

void test_wal_growth_signal_relaxes_batch_delay() {
    TestDir dir("wal_growth");
    const std::string db_path = dir.file("store.dat");

    KVStoreOptions options;
    options.max_batch_size = 1;
    options.max_batch_delay_us = 1000;
    options.adaptive_wal_growth_bytes_per_batch_threshold = 40;
    options.adaptive_wal_growth_delay_multiplier = 4;
    options.adaptive_wal_growth_max_batch_delay_us = 8000;
    options.adaptive_recent_window_batches = 8;
    KVStore store(db_path, options);

    for (int i = 0; i < 4; ++i) {
        store.Put(i, text("payload_abcdefghijklmnopqrstuvwxyz_" + std::to_string(i)));
    }
    store.Put(99, text("growth_probe"));

    const KVStoreMetrics metrics = store.GetMetrics();
    require(metrics.recent_avg_batch_wal_bytes >= options.adaptive_wal_growth_bytes_per_batch_threshold,
            "recent WAL growth metric should reflect large write batches");
    require(metrics.adaptive_wal_growth_batches_completed >= 1,
            "large recent WAL growth should relax the next batch delay");
    require(metrics.last_effective_batch_delay_us > options.max_batch_delay_us,
            "WAL growth signal should expand the effective batch delay");
}

void test_objective_policy_prefers_short_delay_under_latency_pressure() {
    TestDir dir("objective_short_delay");
    const std::string db_path = dir.file("store.dat");

    KVStoreOptions options;
    options.max_batch_size = 1;
    options.max_batch_delay_us = 20000;
    options.adaptive_objective_enabled = true;
    options.adaptive_objective_queue_weight = 0;
    options.adaptive_objective_latency_weight = 4;
    options.adaptive_objective_read_weight = 0;
    options.adaptive_objective_fsync_weight = 0;
    options.adaptive_objective_compaction_weight = 0;
    options.adaptive_objective_wal_growth_weight = 0;
    options.adaptive_objective_short_delay_score_threshold = 500;
    options.adaptive_objective_short_delay_divisor = 10;
    options.adaptive_flush_min_batch_delay_us = 500;
    options.adaptive_latency_target_p95_us = 1000;
    KVStore store(db_path, options);

    store.Put(1, text("objective_latency_a"));
    store.Put(2, text("objective_latency_b"));
    store.Put(3, text("objective_latency_probe"));

    const KVStoreMetrics metrics = store.GetMetrics();
    require(metrics.recent_observed_write_latency_p95_us >= options.adaptive_latency_target_p95_us,
            "recent p95 should exceed the configured latency target");
    require(metrics.adaptive_objective_short_delay_batches_completed >= 1,
            "objective policy should shorten the batch delay under latency pressure");
    require(metrics.last_effective_batch_delay_us < options.max_batch_delay_us,
            "objective pressure score should shorten the effective batch delay");
    require(metrics.last_objective_pressure_score > metrics.last_objective_cost_score,
            "objective pressure score should dominate the cost score in the latency-heavy scenario");
    require(metrics.last_objective_balance_score > 0,
            "objective balance should be positive when pressure favors shorter delays");
}

void test_objective_policy_prefers_long_delay_under_cost_pressure() {
    TestDir dir("objective_long_delay");
    const std::string db_path = dir.file("store.dat");

    KVStoreOptions options;
    options.max_batch_size = 1;
    options.max_batch_delay_us = 500;
    options.adaptive_recent_window_batches = 8;
    options.adaptive_objective_enabled = true;
    options.adaptive_objective_queue_weight = 0;
    options.adaptive_objective_latency_weight = 0;
    options.adaptive_objective_read_weight = 0;
    options.adaptive_objective_fsync_weight = 2;
    options.adaptive_objective_compaction_weight = 2;
    options.adaptive_objective_wal_growth_weight = 1;
    options.adaptive_objective_long_delay_score_threshold = 500;
    options.adaptive_objective_long_delay_multiplier = 6;
    options.adaptive_objective_max_batch_delay_us = 5000;
    options.adaptive_fsync_pressure_per_1000_writes_threshold = 800;
    options.adaptive_compaction_pressure_obsolete_ratio_percent_threshold = 50;
    options.adaptive_wal_growth_bytes_per_batch_threshold = 40;
    KVStore store(db_path, options);

    for (int i = 0; i < 6; ++i) {
        store.Put(42, text("objective_cost_payload_abcdefghijklmnopqrstuvwxyz_" + std::to_string(i)));
    }
    store.Put(99, text("objective_cost_probe_payload"));

    const KVStoreMetrics metrics = store.GetMetrics();
    require(metrics.observed_fsync_pressure_per_1000_writes >= options.adaptive_fsync_pressure_per_1000_writes_threshold,
            "recent fsync pressure should exceed the configured objective cost scale");
    require(metrics.observed_obsolete_wal_ratio_percent >= options.adaptive_compaction_pressure_obsolete_ratio_percent_threshold,
            "obsolete WAL ratio should exceed the configured objective cost scale");
    require(metrics.adaptive_objective_long_delay_batches_completed >= 1,
            "objective policy should relax the batch delay when cost signals dominate");
    require(metrics.last_effective_batch_delay_us > options.max_batch_delay_us,
            "objective cost score should expand the effective batch delay");
    require(metrics.last_objective_cost_score > metrics.last_objective_pressure_score,
            "objective cost score should dominate the pressure score in the cost-heavy scenario");
    require(metrics.last_objective_balance_score < 0,
            "objective balance should be negative when cost favors longer delays");
}

void test_objective_throughput_score_can_dominate_latency_pressure() {
    TestDir dir("objective_throughput");
    const std::string db_path = dir.file("store.dat");

    KVStoreOptions options;
    options.max_batch_size = 1;
    options.max_batch_delay_us = 500;
    options.adaptive_recent_window_batches = 8;
    options.adaptive_objective_enabled = true;
    options.adaptive_objective_queue_weight = 0;
    options.adaptive_objective_latency_weight = 1;
    options.adaptive_objective_read_weight = 0;
    options.adaptive_objective_throughput_weight = 6;
    options.adaptive_objective_target_batch_size = 8;
    options.adaptive_objective_fsync_weight = 0;
    options.adaptive_objective_compaction_weight = 0;
    options.adaptive_objective_wal_growth_weight = 0;
    options.adaptive_objective_long_delay_score_threshold = 250;
    options.adaptive_objective_long_delay_multiplier = 6;
    options.adaptive_objective_max_batch_delay_us = 5000;
    options.adaptive_flush_min_batch_delay_us = 100;
    options.adaptive_latency_target_p95_us = 50;
    KVStore store(db_path, options);

    store.Put(1, text("objective_tp_a"));
    store.Put(2, text("objective_tp_b"));
    store.Put(3, text("objective_tp_probe"));

    const KVStoreMetrics metrics = store.GetMetrics();
    require(metrics.recent_observed_write_latency_p95_us >= options.adaptive_latency_target_p95_us,
            "recent p95 should still reflect latency pressure");
    require(metrics.recent_avg_batch_size < options.adaptive_objective_target_batch_size,
            "recent average batch size should remain below the throughput target");
    require(metrics.adaptive_objective_throughput_batches_completed >= 1,
            "objective controller should record batches influenced by throughput deficit");
    require(metrics.last_objective_throughput_score > 0,
            "objective controller should expose a non-zero throughput score");
    require(metrics.last_objective_cost_score > metrics.last_objective_pressure_score,
            "throughput deficit should outweigh latency pressure in this scenario");
    require(metrics.last_objective_mode < 0,
            "objective mode should report a long-delay decision when throughput dominates");
    require(metrics.last_effective_batch_delay_us > options.max_batch_delay_us,
            "throughput-dominated objective control should expand the effective batch delay");
}

void run_soak_test(int duration_seconds) {
    TestDir dir("soak");
    const std::string db_path = dir.file("store.dat");

    KVStoreOptions options;
    options.max_batch_size = 32;
    options.max_batch_wal_bytes = 1 << 20;
    options.max_batch_delay_us = 2000;
    options.auto_compact_wal_bytes_threshold = 4096;
    options.auto_compact_invalid_wal_ratio_percent = 60;
    options.adaptive_flush_enabled = true;
    options.adaptive_flush_queue_depth_threshold = 8;
    options.adaptive_flush_delay_divisor = 4;
    options.adaptive_flush_min_batch_delay_us = 100;
    KVStore store(db_path, options);

    constexpr int kWriterCount = 4;
    constexpr int kReaderCount = 2;
    constexpr int kKeySpace = 256;

    std::atomic<bool> stop {false};
    std::map<int, std::optional<std::string>> oracle;
    std::mutex oracle_mutex;
    std::vector<std::thread> threads;

    for (int writer_id = 0; writer_id < kWriterCount; ++writer_id) {
        threads.emplace_back([&store, &stop, &oracle, &oracle_mutex, writer_id]() {
            std::mt19937 gen(9000 + writer_id);
            std::uniform_int_distribution<int> key_dist(0, kKeySpace - 1);
            std::uniform_int_distribution<int> op_dist(0, 9);
            uint64_t version = 0;
            while (!stop.load(std::memory_order_acquire)) {
                const int key = key_dist(gen);
                const bool do_delete = op_dist(gen) < 3;
                if (do_delete) {
                    store.Delete(key);
                    std::lock_guard<std::mutex> lock(oracle_mutex);
                    oracle[key] = std::nullopt;
                } else {
                    const std::string payload =
                        "value_" + std::to_string(writer_id) + "_" + std::to_string(key) + "_" + std::to_string(version++);
                    store.Put(key, text(payload));
                    std::lock_guard<std::mutex> lock(oracle_mutex);
                    oracle[key] = payload;
                }
            }
        });
    }

    for (int reader_id = 0; reader_id < kReaderCount; ++reader_id) {
        threads.emplace_back([&store, &stop, reader_id]() {
            std::mt19937 gen(12000 + reader_id);
            std::uniform_int_distribution<int> key_dist(0, kKeySpace - 1);
            while (!stop.load(std::memory_order_acquire)) {
                const auto value = store.Get(key_dist(gen));
                if (value.has_value()) {
                    require(as_string(*value).rfind("value_", 0) == 0, "soak readers should only observe complete values");
                }
            }
        });
    }

    threads.emplace_back([&store, &stop]() {
        while (!stop.load(std::memory_order_acquire)) {
            store.Compact();
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
    });

    std::this_thread::sleep_for(std::chrono::seconds(duration_seconds));
    stop.store(true, std::memory_order_release);

    for (auto& thread : threads) {
        thread.join();
    }

    store.Compact();

    std::map<int, std::optional<std::string>> expected;
    {
        std::lock_guard<std::mutex> lock(oracle_mutex);
        expected = oracle;
    }

    KVStore reopened(db_path);
    for (const auto& [key, expected_value] : expected) {
        const auto actual = reopened.Get(key);
        if (!expected_value.has_value()) {
            require(!actual.has_value(), "deleted keys should remain deleted after soak restart");
            continue;
        }
        require(actual.has_value(), "expected live key missing after soak restart");
        require(as_string(*actual) == *expected_value, "reopened state should match soak oracle");
    }
}

void run_benchmark() {
    TestDir dir("bench");
    const std::string db_path = dir.file("store.dat");
    KVStoreOptions options;
    options.max_batch_size = 32;
    options.max_batch_wal_bytes = 1 << 20;
    options.max_batch_delay_us = 2000;
    options.adaptive_recent_window_batches = 64;
    options.adaptive_recent_write_sample_limit = 512;
    options.adaptive_objective_enabled = true;
    options.adaptive_objective_queue_weight = 1;
    options.adaptive_objective_latency_weight = 3;
    options.adaptive_objective_read_weight = 2;
    options.adaptive_objective_throughput_weight = 2;
    options.adaptive_objective_target_batch_size = 16;
    options.adaptive_objective_fsync_weight = 2;
    options.adaptive_objective_compaction_weight = 1;
    options.adaptive_objective_wal_growth_weight = 1;
    options.adaptive_objective_short_delay_score_threshold = 500;
    options.adaptive_objective_long_delay_score_threshold = 500;
    options.adaptive_objective_short_delay_divisor = 2;
    options.adaptive_objective_long_delay_multiplier = 2;
    options.adaptive_objective_max_batch_delay_us = 8000;
    options.adaptive_read_heavy_read_per_1000_ops_threshold = 700;
    options.adaptive_read_heavy_delay_divisor = 4;
    options.adaptive_read_heavy_batch_size_divisor = 2;
    options.adaptive_flush_enabled = true;
    options.adaptive_flush_queue_depth_threshold = 8;
    options.adaptive_flush_delay_divisor = 4;
    options.adaptive_flush_min_batch_delay_us = 100;
    options.adaptive_latency_target_p95_us = 12000;
    options.adaptive_fsync_pressure_per_1000_writes_threshold = 350;
    options.adaptive_fsync_pressure_delay_multiplier = 2;
    options.adaptive_fsync_pressure_max_batch_delay_us = 8000;
    options.adaptive_compaction_pressure_obsolete_ratio_percent_threshold = 50;
    options.adaptive_compaction_pressure_delay_multiplier = 2;
    options.adaptive_wal_growth_bytes_per_batch_threshold = 200;
    options.adaptive_wal_growth_delay_multiplier = 2;
    options.adaptive_wal_growth_max_batch_delay_us = 6000;
    options.adaptive_batching_enabled = true;
    options.adaptive_queue_depth_threshold = 8;
    options.adaptive_batch_size_multiplier = 4;
    options.adaptive_batch_wal_bytes_multiplier = 4;
    KVStore store(db_path, options);

    constexpr int kWriterCount = 8;
    constexpr int kReaderCount = 4;
    constexpr int kDurationSeconds = 3;
    constexpr int kKeySpace = 50000;

    std::atomic<bool> stop {false};
    std::atomic<uint64_t> write_ops {0};
    std::atomic<uint64_t> read_ops {0};
    std::atomic<uint64_t> write_latency_ns {0};

    std::vector<std::thread> threads;
    for (int writer_id = 0; writer_id < kWriterCount; ++writer_id) {
        threads.emplace_back([&store, &stop, &write_ops, &write_latency_ns, writer_id]() {
            std::mt19937 gen(1337 + writer_id);
            std::uniform_int_distribution<int> key_dist(writer_id * 100000, writer_id * 100000 + kKeySpace - 1);
            while (!stop.load(std::memory_order_acquire)) {
                const int key = key_dist(gen);
                const auto begin = std::chrono::steady_clock::now();
                store.Put(key, text("payload_" + std::to_string(key)));
                const auto end = std::chrono::steady_clock::now();
                write_ops.fetch_add(1, std::memory_order_relaxed);
                write_latency_ns.fetch_add(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count(),
                    std::memory_order_relaxed);
            }
        });
    }

    for (int reader_id = 0; reader_id < kReaderCount; ++reader_id) {
        threads.emplace_back([&store, &stop, &read_ops, reader_id]() {
            std::mt19937 gen(4242 + reader_id);
            std::uniform_int_distribution<int> key_dist(0, kWriterCount * 100000 + kKeySpace - 1);
            while (!stop.load(std::memory_order_acquire)) {
                (void)store.Get(key_dist(gen));
                read_ops.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }

    const auto start = std::chrono::steady_clock::now();
    std::this_thread::sleep_for(std::chrono::seconds(kDurationSeconds));
    stop.store(true, std::memory_order_release);

    for (auto& thread : threads) {
        thread.join();
    }

    const auto end = std::chrono::steady_clock::now();
    const double seconds = std::chrono::duration<double>(end - start).count();
    const uint64_t writes = write_ops.load(std::memory_order_relaxed);
    const uint64_t reads = read_ops.load(std::memory_order_relaxed);
    const double avg_write_latency_us =
        writes == 0 ? 0.0 : static_cast<double>(write_latency_ns.load(std::memory_order_relaxed)) / writes / 1000.0;
    const KVStoreMetrics metrics = store.GetMetrics();

    std::cout << "[BENCH] duration_s=" << seconds
              << " writes=" << writes
              << " reads=" << reads
              << " write_ops_per_s=" << (writes / seconds)
              << " read_ops_per_s=" << (reads / seconds)
              << " avg_write_latency_us=" << avg_write_latency_us
              << " p50_write_latency_us=" << metrics.approx_write_latency_p50_us
              << " p95_write_latency_us=" << metrics.approx_write_latency_p95_us
              << " p99_write_latency_us=" << metrics.approx_write_latency_p99_us
              << " recent_read_ratio_per_1000_ops=" << metrics.recent_read_ratio_per_1000_ops
              << " recent_p95_write_latency_us=" << metrics.recent_observed_write_latency_p95_us
              << " recent_peak_queue_depth=" << metrics.recent_peak_queue_depth
              << " recent_avg_batch_size=" << metrics.recent_avg_batch_size
              << " recent_avg_batch_wal_bytes=" << metrics.recent_avg_batch_wal_bytes
              << " recent_window_batch_count=" << metrics.recent_window_batch_count
              << " observed_fsync_pressure_per_1000_writes=" << metrics.observed_fsync_pressure_per_1000_writes
              << " observed_obsolete_wal_ratio_percent=" << metrics.observed_obsolete_wal_ratio_percent
              << " objective_pressure_score=" << metrics.last_objective_pressure_score
              << " objective_cost_score=" << metrics.last_objective_cost_score
              << " objective_throughput_score=" << metrics.last_objective_throughput_score
              << " objective_balance_score=" << metrics.last_objective_balance_score
              << " objective_mode=" << metrics.last_objective_mode
              << " last_batch_delay_us=" << metrics.last_effective_batch_delay_us
              << " min_batch_delay_us=" << metrics.min_effective_batch_delay_us
              << " max_batch_delay_us=" << metrics.max_effective_batch_delay_us
              << " committed_batches=" << metrics.committed_write_batches
              << " max_batch_size=" << metrics.max_committed_batch_size
              << " max_batch_wal_bytes=" << metrics.max_committed_batch_wal_bytes
              << " adaptive_batches=" << metrics.adaptive_batches_completed
              << " adaptive_flush_batches=" << metrics.adaptive_flush_batches_completed
              << " latency_target_batches=" << metrics.adaptive_latency_target_batches_completed
              << " fsync_pressure_batches=" << metrics.adaptive_fsync_pressure_batches_completed
              << " read_heavy_batches=" << metrics.adaptive_read_heavy_batches_completed
              << " objective_throughput_batches=" << metrics.adaptive_objective_throughput_batches_completed
              << " compaction_pressure_batches=" << metrics.adaptive_compaction_pressure_batches_completed
              << " wal_growth_batches=" << metrics.adaptive_wal_growth_batches_completed
              << " objective_short_delay_batches=" << metrics.adaptive_objective_short_delay_batches_completed
              << " objective_long_delay_batches=" << metrics.adaptive_objective_long_delay_batches_completed
              << " wal_fsync_calls=" << metrics.wal_fsync_calls
              << " wal_bytes_written=" << metrics.wal_bytes_written
              << " snapshot_bytes_written_total=" << metrics.total_snapshot_bytes_written
              << " wal_bytes_reclaimed_total=" << metrics.total_wal_bytes_reclaimed_by_compaction
              << " writer_wait_events=" << metrics.writer_wait_events
              << " writer_wait_time_us=" << metrics.writer_wait_time_us
              << " queue_high_watermark=" << metrics.max_pending_queue_depth
              << " recent_batch_fill_per_1000=" << metrics.recent_batch_fill_per_1000
              << " latency_histogram=";
    for (size_t i = 0; i < metrics.write_latency_histogram.size(); ++i) {
        if (i != 0) {
            std::cout << ',';
        }
        std::cout << metrics.write_latency_histogram[i];
    }
    std::cout
              << std::endl;
}

void run_benchmark_json() {
    TestDir dir("bench_json");
    const std::string db_path = dir.file("store.dat");
    KVStoreOptions options;
    options.max_batch_size = 16;
    options.max_batch_delay_us = 1000;
    options.adaptive_flush_enabled = true;
    options.adaptive_flush_queue_depth_threshold = 4;
    options.adaptive_flush_delay_divisor = 4;
    options.adaptive_flush_min_batch_delay_us = 100;

    KVStore store(db_path, options);
    for (int i = 0; i < 128; ++i) {
        store.Put(i, text("bench_json_" + std::to_string(i)));
    }
    for (int i = 0; i < 256; ++i) {
        (void)store.Get(i % 128);
    }

    std::cout << MetricsToJson(store.GetMetrics()) << std::endl;
}

}  // namespace

int main(int argc, char* argv[]) {
    g_program_path = std::filesystem::absolute(argv[0]).string();
    if (argc > 1) {
        const std::string command = argv[1];
        if (command == "bench") {
            run_benchmark();
            return 0;
        }
        if (command == "bench-json") {
            run_benchmark_json();
            return 0;
        }
        if (command == "profile-json") {
            if (argc != 3) {
                std::cerr << "Usage: kv_test profile-json <balanced|write-heavy|read-heavy|low-latency>" << std::endl;
                return 1;
            }
            return run_profile_json(argv[2]);
        }
        if (command == "soak") {
            const int duration_seconds = argc > 2 ? std::stoi(argv[2]) : 10;
            run_soak_test(duration_seconds);
            return 0;
        }
        if (command == "inspect-format") {
            if (argc != 3) {
                std::cerr << "Usage: kv_test inspect-format <db_path>" << std::endl;
                return 1;
            }
            return run_inspect_format(argv[2]);
        }
        if (command == "rewrite-format") {
            if (argc != 3) {
                std::cerr << "Usage: kv_test rewrite-format <db_path>" << std::endl;
                return 1;
            }
            return run_rewrite_format(argv[2]);
        }
        if (command == "fault-inject") {
            if (argc != 4) {
                std::cerr << "Usage: kv_test fault-inject <scenario> <db_path>" << std::endl;
                return 1;
            }
            return run_fault_injection_scenario(argv[2], argv[3]);
        }
        std::cerr << "Unknown command: " << command << '\n';
        std::cerr << "Usage: kv_test [bench|bench-json|profile-json|soak|inspect-format|rewrite-format|fault-inject]" << std::endl;
        return 1;
    }

    const std::vector<std::pair<std::string, std::function<void()>>> tests = {
        {"basic persistence", test_basic_persistence},
        {"string keys do not collide with int keys", test_string_keys_do_not_collide_with_int_keys},
        {"string scan returns sorted range", test_string_scan_returns_sorted_range},
        {"batch write persists mixed key types", test_write_batch_persists_mixed_key_types},
        {"batch write mixes put and delete", test_write_batch_mixes_put_and_delete},
        {"batch write preserves operation order", test_write_batch_preserves_operation_order},
        {"put copies input value", test_put_copies_input_value},
        {"recovery from WAL without compaction", test_recovery_from_wal_without_compaction},
        {"legacy v1 snapshot and wal are readable", test_legacy_v1_snapshot_and_wal_are_readable},
        {"legacy v1 rewrite upgrades snapshot to v2", test_legacy_v1_rewrite_upgrades_snapshot_to_v2},
        {"ordering and updates", test_ordering_and_updates},
        {"compaction persists snapshot and resets WAL", test_compaction_persists_snapshot_and_resets_wal},
        {"truncated WAL tail is ignored", test_truncated_wal_tail_is_ignored},
        {"corrupted WAL record throws", test_corrupted_wal_record_throws},
        {"corrupted snapshot throws", test_corrupted_snapshot_throws},
        {"unsupported snapshot version throws", test_unsupported_snapshot_version_throws},
        {"crash after wal fsync recovers latest write", test_crash_after_wal_fsync_recovers_latest_write},
        {"crash after wal fsync recovers latest batch", test_crash_after_wal_fsync_recovers_latest_batch},
        {"crash after snapshot rename recovers consistent state",
         test_crash_after_snapshot_rename_recovers_consistent_state},
        {"crash before snapshot rename replays old wal", test_crash_before_snapshot_rename_replays_old_wal},
        {"crash after wal rotation recovers snapshot", test_crash_after_wal_rotation_recovers_snapshot},
        {"concurrent reads and writes", test_concurrent_reads_and_writes},
        {"many concurrent writers", test_many_concurrent_writers},
        {"concurrent compaction with writes", test_concurrent_compaction_with_writes},
        {"batching metrics are reported", test_batching_metrics_are_reported},
        {"recommended profiles are distinct", test_recommended_profiles_are_distinct},
        {"options to json reports profile fields", test_options_to_json_reports_profile_fields},
        {"metrics to json reports core fields", test_metrics_to_json_reports_core_fields},
        {"auto compaction triggers and preserves state", test_auto_compaction_triggers_and_preserves_state},
        {"manual and auto compaction metrics coexist", test_manual_and_auto_compaction_metrics_coexist},
        {"invalid wal ratio triggers auto compaction", test_invalid_wal_ratio_triggers_auto_compaction},
        {"wal obsolete byte metrics are consistent", test_wal_obsolete_byte_metrics_are_consistent},
        {"batch wal byte limit is respected", test_batch_wal_byte_limit_is_respected},
        {"latency histogram tracks write requests", test_latency_histogram_tracks_write_requests},
        {"adaptive batching expands batch under queue pressure", test_adaptive_batching_expands_batch_under_queue_pressure},
        {"percentile and writer metrics are reported", test_percentile_and_writer_metrics_are_reported},
        {"adaptive flush shortens batch delay", test_adaptive_flush_shortens_batch_delay},
        {"compaction long term metrics accumulate", test_compaction_long_term_metrics_accumulate},
        {"latency target adaptive flush kicks in", test_latency_target_adaptive_flush_kicks_in},
        {"fsync pressure can relax batch delay", test_fsync_pressure_can_relax_batch_delay},
        {"recent window metrics capture bursts", test_recent_window_metrics_capture_bursts},
        {"read heavy signal prefers shorter batches", test_read_heavy_signal_prefers_shorter_batches},
        {"compaction pressure signal relaxes batch delay", test_compaction_pressure_signal_relaxes_batch_delay},
        {"wal growth signal relaxes batch delay", test_wal_growth_signal_relaxes_batch_delay},
        {"objective policy prefers short delay under latency pressure",
         test_objective_policy_prefers_short_delay_under_latency_pressure},
        {"objective policy prefers long delay under cost pressure",
         test_objective_policy_prefers_long_delay_under_cost_pressure},
        {"objective throughput score can dominate latency pressure",
         test_objective_throughput_score_can_dominate_latency_pressure},
    };

    size_t passed = 0;
    for (const auto& [name, test] : tests) {
        try {
            test();
            ++passed;
            std::cout << "[PASS] " << name << '\n';
        } catch (const std::exception& ex) {
            std::cerr << "[FAIL] " << name << ": " << ex.what() << '\n';
            return 1;
        }
    }

    std::cout << "All " << passed << " tests passed." << std::endl;
    return 0;
}
