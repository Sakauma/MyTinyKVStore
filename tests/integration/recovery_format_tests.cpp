#include "tests/integration/test_registry.h"

#include "internal/format.h"
#include "kvstore.h"
#include "tests/common/cli_entrypoints.h"
#include "tests/common/test_support.h"

#include <cstring>
#include <fstream>
#include <initializer_list>
#include <iostream>
#include <map>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

namespace kvstore::tests::integration {
namespace {

using kvstore::internal::SnapshotEntryHeaderV1;
using kvstore::internal::SnapshotHeader;
using kvstore::internal::WalRecordHeader;
using kvstore::internal::WalRecordHeaderV1;
using test_support::append_bytes;
using test_support::as_string;
using test_support::file_size_or_zero;
using test_support::require;
using test_support::TestDir;
using test_support::text;

std::map<std::string, std::string> parse_kv_line(const std::string& line) {
    std::map<std::string, std::string> values;
    std::istringstream in(line);
    std::string token;
    while (in >> token) {
        const size_t equals = token.find('=');
        require(equals != std::string::npos, "Expected key=value token in inspect output");
        values[token.substr(0, equals)] = token.substr(equals + 1);
    }
    return values;
}

std::map<std::string, std::string> capture_inspect_format(const std::string& db_path, int expected_status = 0) {
    std::ostringstream out;
    auto* original = std::cout.rdbuf(out.rdbuf());
    const int status = run_inspect_format(db_path);
    std::cout.rdbuf(original);
    require(status == expected_status, "inspect-format should return the expected status");
    return parse_kv_line(out.str());
}

std::pair<int, std::string> capture_verify_format(const std::string& db_path) {
    std::ostringstream out;
    auto* original = std::cout.rdbuf(out.rdbuf());
    const int status = run_verify_format(db_path);
    std::cout.rdbuf(original);
    return {status, out.str()};
}

void write_legacy_snapshot_v1(const std::string& path, const std::vector<std::pair<int32_t, Value>>& entries) {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    require(out.is_open(), "Expected legacy snapshot file to be writable");

    SnapshotHeader header = kvstore::internal::make_snapshot_header();
    header.version = 1;
    out.write(reinterpret_cast<const char*>(&header), sizeof(header));

    for (const auto& [key, value] : entries) {
        const SnapshotEntryHeaderV1 entry {
            kvstore::internal::kSnapshotEntryMagic,
            key,
            static_cast<uint32_t>(value.bytes.size()),
        };
        out.write(reinterpret_cast<const char*>(&entry), sizeof(entry));
        if (!value.bytes.empty()) {
            out.write(reinterpret_cast<const char*>(value.bytes.data()),
                      static_cast<std::streamsize>(value.bytes.size()));
        }
    }
}

void append_legacy_wal_record_v1(
    const std::string& path,
    uint8_t type,
    int32_t key,
    const Value& value) {
    std::ofstream out(path, std::ios::binary | std::ios::app);
    require(out.is_open(), "Expected legacy WAL file to be writable");

    const WalRecordHeaderV1 header {
        kvstore::internal::kWalMagic,
        1,
        type,
        0,
        key,
        static_cast<uint32_t>(value.bytes.size()),
        kvstore::internal::checksum_record_v1(
            type == 1 ? kvstore::internal::WalRecordType::kPut : kvstore::internal::WalRecordType::kDelete,
            key,
            value),
    };
    out.write(reinterpret_cast<const char*>(&header), sizeof(header));
    if (!value.bytes.empty()) {
        out.write(reinterpret_cast<const char*>(value.bytes.data()),
                  static_cast<std::streamsize>(value.bytes.size()));
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

void test_inspect_format_reports_key_type_counts() {
    TestDir dir("inspect_counts");
    const std::string db_path = dir.file("store.dat");
    const std::vector<uint8_t> binary_snapshot_key = {0x10, 0x20, 0x30};
    const std::vector<uint8_t> binary_wal_key = {0x00, 0x01};

    {
        KVStore store(db_path);
        store.Put(1, text("int-snapshot"));
        store.Put(std::string("alpha"), text("string-snapshot"));
        store.Put(binary_snapshot_key, text("binary-snapshot"));
        store.Compact();
        store.Put(std::string("beta"), text("string-wal"));
        store.Put(binary_wal_key, text("binary-wal"));
        store.Delete(1);
    }

    const auto values = capture_inspect_format(db_path);
    require(values.at("snapshot_version") == "2", "inspect-format should report v2 snapshot");
    require(values.at("snapshot_entries") == "3", "inspect-format should count compacted snapshot entries");
    require(values.at("snapshot_int_keys") == "1", "inspect-format should count int snapshot keys");
    require(values.at("snapshot_string_keys") == "1", "inspect-format should count string snapshot keys");
    require(values.at("snapshot_binary_keys") == "1", "inspect-format should count binary snapshot keys");
    require(values.at("wal_version") == "2", "inspect-format should report v2 WAL");
    require(values.at("wal_records") == "3", "inspect-format should count WAL records after compaction");
    require(values.at("wal_put_records") == "2", "inspect-format should count WAL puts");
    require(values.at("wal_delete_records") == "1", "inspect-format should count WAL deletes");
    require(values.at("wal_int_keys") == "1", "inspect-format should count int WAL keys");
    require(values.at("wal_string_keys") == "1", "inspect-format should count string WAL keys");
    require(values.at("wal_binary_keys") == "1", "inspect-format should count binary WAL keys");
    require(values.at("rewrite_recommended") == "0", "current-format data should not require rewrite");
}

void test_inspect_format_recommends_rewrite_for_legacy_v1() {
    TestDir dir("inspect_legacy_v1");
    const std::string db_path = dir.file("store.dat");
    const std::string wal_path = db_path + ".wal";

    write_legacy_snapshot_v1(db_path, {
        {1, text("legacy-one")},
        {2, text("legacy-two")},
    });
    append_legacy_wal_record_v1(wal_path, 1, 3, text("legacy-three"));
    append_legacy_wal_record_v1(wal_path, 2, 2, Value {});

    const auto values = capture_inspect_format(db_path);
    require(values.at("snapshot_version") == "1", "inspect-format should report legacy snapshot version");
    require(values.at("snapshot_entries") == "2", "inspect-format should count legacy snapshot entries");
    require(values.at("snapshot_int_keys") == "2", "legacy snapshots should be recognized as int keys");
    require(values.at("wal_version") == "1", "inspect-format should report legacy WAL version");
    require(values.at("wal_records") == "2", "inspect-format should count legacy WAL records");
    require(values.at("wal_int_keys") == "2", "legacy WAL records should be recognized as int keys");
    require(values.at("rewrite_recommended") == "1", "legacy data should recommend rewrite");
}

void test_inspect_format_recommends_rewrite_for_truncated_wal() {
    TestDir dir("inspect_truncated_wal");
    const std::string db_path = dir.file("store.dat");
    const std::string wal_path = db_path + ".wal";

    {
        KVStore store(db_path);
        store.Put(1, text("one"));
        store.Compact();
        store.Put(2, text("two"));
    }

    append_bytes(wal_path, {0xAA, 0xBB, 0xCC});

    const auto values = capture_inspect_format(db_path);
    require(values.at("wal_truncated") == "1", "inspect-format should flag a truncated WAL tail");
    require(values.at("rewrite_recommended") == "1",
            "truncated WAL should explicitly recommend rewrite to restore a clean layout");
}

void test_verify_format_accepts_current_layout() {
    TestDir dir("verify_current");
    const std::string db_path = dir.file("store.dat");

    {
        KVStore store(db_path);
        store.Put(1, text("one"));
        store.Put(std::string("alpha"), text("two"));
        store.Put(std::vector<uint8_t> {0xAA, 0xBB}, text("three"));
        store.Compact();
    }

    const auto [status, output] = capture_verify_format(db_path);
    require(status == 0, "verify-format should accept current layout");
    require(output.find("rewrite_recommended=0") != std::string::npos,
            "verify-format should print inspect output for current layout");
    require(output.find("verify_reason=current_layout") != std::string::npos,
            "verify-format should expose the clean-layout reason for current data");
}

void test_verify_format_rejects_legacy_layout() {
    TestDir dir("verify_legacy");
    const std::string db_path = dir.file("store.dat");
    const std::string wal_path = db_path + ".wal";

    write_legacy_snapshot_v1(db_path, {
        {7, text("seven")},
    });
    append_legacy_wal_record_v1(wal_path, 1, 9, text("nine"));

    const auto [status, output] = capture_verify_format(db_path);
    require(status == 2, "verify-format should reject legacy layout and request rewrite");
    require(output.find("rewrite_recommended=1") != std::string::npos,
            "verify-format should expose the rewrite recommendation");
    require(output.find("verify_reason=migration_required") != std::string::npos,
            "verify-format should expose the migration-required reason for legacy data");
}

void test_rewrite_format_recovers_truncated_current_wal() {
    TestDir dir("rewrite_truncated_current");
    const std::string db_path = dir.file("store.dat");
    const std::string wal_path = db_path + ".wal";

    {
        KVStore store(db_path);
        store.Put(1, text("stable"));
        store.Compact();
        store.Put(2, text("latest"));
    }

    append_bytes(wal_path, {0xDE, 0xAD});

    const auto [before_status, before_output] = capture_verify_format(db_path);
    require(before_status == 2, "verify-format should reject a truncated current WAL layout");
    require(before_output.find("wal_truncated=1") != std::string::npos,
            "verify-format should surface the truncated WAL signal");
    require(before_output.find("verify_reason=wal_truncated") != std::string::npos,
            "verify-format should expose the wal_truncated reason");

    require(run_rewrite_format(db_path) == 0, "rewrite-format should recover a truncated WAL layout");

    const auto [after_status, after_output] = capture_verify_format(db_path);
    require(after_status == 0, "rewrite-format should restore a clean current layout");
    require(after_output.find("wal_truncated=0") != std::string::npos,
            "rewritten layout should no longer report WAL truncation");
    require(after_output.find("verify_reason=current_layout") != std::string::npos,
            "rewritten layout should return to the clean-layout reason");

    KVStore reopened(db_path);
    const auto stable = reopened.Get(1);
    const auto latest = reopened.Get(2);
    require(stable.has_value() && as_string(*stable) == "stable",
            "rewrite-format should preserve the snapshot portion of truncated current data");
    require(latest.has_value() && as_string(*latest) == "latest",
            "rewrite-format should preserve committed WAL records before the truncated tail");
}

void test_compatibility_matrix_command_succeeds() {
    require(run_compatibility_matrix() == 0, "compatibility matrix command should succeed");
}

void test_truncated_wal_tail_is_ignored() {
    TestDir dir("truncated_wal");
    const std::string db_path = dir.file("store.dat");
    const std::string wal_path = db_path + ".wal";

    {
        KVStore store(db_path);
        store.Put(1, text("stable"));
    }

    append_bytes(wal_path, {0xAA, 0xBB, 0xCC});

    KVStore reopened(db_path);
    const auto value = reopened.Get(1);
    require(value.has_value(), "committed value should survive truncated WAL tail");
    require(as_string(*value) == "stable", "truncated WAL tail must be ignored");
}

void test_corrupted_wal_record_throws() {
    TestDir dir("corrupted_wal");
    const std::string db_path = dir.file("store.dat");
    const std::string wal_path = db_path + ".wal";

    {
        KVStore store(db_path);
        store.Put(1, text("value"));
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
    TestDir dir("corrupted_snapshot");
    const std::string db_path = dir.file("store.dat");

    {
        KVStore store(db_path);
        store.Put(1, text("value"));
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
    require(threw, "corrupted snapshot header should raise KVStoreError");
}

void test_unsupported_snapshot_version_throws() {
    TestDir dir("snapshot_version");
    const std::string db_path = dir.file("store.dat");

    {
        KVStore store(db_path);
        store.Put(1, text("value"));
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

}  // namespace

void register_recovery_format_tests(TestCases& tests) {
    tests.push_back({"recovery from WAL without compaction", test_recovery_from_wal_without_compaction});
    tests.push_back({"legacy v1 snapshot and wal are readable", test_legacy_v1_snapshot_and_wal_are_readable});
    tests.push_back({"legacy v1 rewrite upgrades snapshot to v2", test_legacy_v1_rewrite_upgrades_snapshot_to_v2});
    tests.push_back({"inspect format reports key type counts", test_inspect_format_reports_key_type_counts});
    tests.push_back({"inspect format recommends rewrite for legacy v1", test_inspect_format_recommends_rewrite_for_legacy_v1});
    tests.push_back({"inspect format recommends rewrite for truncated wal", test_inspect_format_recommends_rewrite_for_truncated_wal});
    tests.push_back({"verify format accepts current layout", test_verify_format_accepts_current_layout});
    tests.push_back({"verify format rejects legacy layout", test_verify_format_rejects_legacy_layout});
    tests.push_back({"rewrite format recovers truncated current wal", test_rewrite_format_recovers_truncated_current_wal});
    tests.push_back({"compatibility matrix command succeeds", test_compatibility_matrix_command_succeeds});
    tests.push_back({"truncated WAL tail is ignored", test_truncated_wal_tail_is_ignored});
    tests.push_back({"corrupted WAL record throws", test_corrupted_wal_record_throws});
    tests.push_back({"corrupted snapshot throws", test_corrupted_snapshot_throws});
    tests.push_back({"unsupported snapshot version throws", test_unsupported_snapshot_version_throws});
}

}  // namespace kvstore::tests::integration
