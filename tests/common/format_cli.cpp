#include "tests/common/cli_entrypoints.h"

#include "kvstore.h"
#include "tests/common/test_support.h"

#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

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

struct SnapshotEntryHeader {
    uint32_t magic;
    uint32_t key_size;
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
constexpr char kIntKeyTag = '\x01';
constexpr char kStringKeyTag = '\x02';
constexpr char kBinaryKeyTag = '\x03';

struct FormatKeyStats {
    uint64_t total = 0;
    uint64_t int_keys = 0;
    uint64_t string_keys = 0;
    uint64_t binary_keys = 0;
    uint64_t unknown_keys = 0;
};

using test_support::append_bytes;
using test_support::file_size_or_zero;
using test_support::require;
using test_support::TestDir;
using test_support::text;

void classify_encoded_key(std::string_view encoded_key, FormatKeyStats& stats) {
    stats.total += 1;
    if (encoded_key.empty()) {
        stats.unknown_keys += 1;
        return;
    }

    switch (encoded_key.front()) {
        case kIntKeyTag:
            stats.int_keys += 1;
            break;
        case kStringKeyTag:
            stats.string_keys += 1;
            break;
        case kBinaryKeyTag:
            stats.binary_keys += 1;
            break;
        default:
            stats.unknown_keys += 1;
            break;
    }
}

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

int run_inspect_format_impl(const std::string& db_path) {
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

    FormatKeyStats snapshot_stats;
    bool snapshot_truncated = false;
    bool snapshot_entry_magic_ok = true;
    if (snapshot_header.version == 1) {
        while (true) {
            SnapshotEntryHeaderV1 entry {};
            snapshot.read(reinterpret_cast<char*>(&entry), sizeof(entry));
            if (snapshot.gcount() == 0) {
                break;
            }
            if (snapshot.gcount() != static_cast<std::streamsize>(sizeof(entry))) {
                snapshot_truncated = true;
                break;
            }
            if (entry.magic != kSnapshotEntryMagic) {
                snapshot_entry_magic_ok = false;
                break;
            }
            snapshot_stats.total += 1;
            snapshot_stats.int_keys += 1;
            snapshot.seekg(entry.value_size, std::ios::cur);
            if (!snapshot.good()) {
                snapshot_truncated = true;
                break;
            }
        }
    } else if (snapshot_header.version == 2) {
        while (true) {
            SnapshotEntryHeader entry {};
            snapshot.read(reinterpret_cast<char*>(&entry), sizeof(entry));
            if (snapshot.gcount() == 0) {
                break;
            }
            if (snapshot.gcount() != static_cast<std::streamsize>(sizeof(entry))) {
                snapshot_truncated = true;
                break;
            }
            if (entry.magic != kSnapshotEntryMagic) {
                snapshot_entry_magic_ok = false;
                break;
            }
            std::string key(entry.key_size, '\0');
            if (!key.empty()) {
                snapshot.read(key.data(), static_cast<std::streamsize>(key.size()));
                if (snapshot.gcount() != static_cast<std::streamsize>(key.size())) {
                    snapshot_truncated = true;
                    break;
                }
            }
            classify_encoded_key(key, snapshot_stats);
            snapshot.seekg(entry.value_size, std::ios::cur);
            if (!snapshot.good()) {
                snapshot_truncated = true;
                break;
            }
        }
    }

    bool rewrite_recommended = snapshot_header.version != 2;
    std::cout << "snapshot_exists=1"
              << " snapshot_magic_ok=" << (std::memcmp(snapshot_header.magic, kSnapshotMagic, sizeof(kSnapshotMagic)) == 0 ? 1 : 0)
              << " snapshot_version=" << snapshot_header.version
              << " snapshot_entries=" << snapshot_stats.total
              << " snapshot_int_keys=" << snapshot_stats.int_keys
              << " snapshot_string_keys=" << snapshot_stats.string_keys
              << " snapshot_binary_keys=" << snapshot_stats.binary_keys
              << " snapshot_unknown_key_types=" << snapshot_stats.unknown_keys
              << " snapshot_entry_magic_ok=" << (snapshot_entry_magic_ok ? 1 : 0)
              << " snapshot_truncated=" << (snapshot_truncated ? 1 : 0)
              << " snapshot_size=" << file_size_or_zero(db_path);

    std::ifstream wal(wal_path, std::ios::binary);
    if (!wal.is_open()) {
        std::cout << " wal_exists=0"
                  << " wal_records=0"
                  << " wal_put_records=0"
                  << " wal_delete_records=0"
                  << " wal_int_keys=0"
                  << " wal_string_keys=0"
                  << " wal_binary_keys=0"
                  << " wal_unknown_key_types=0"
                  << " wal_unknown_record_types=0"
                  << " wal_truncated=0"
                  << " rewrite_recommended=" << (rewrite_recommended ? 1 : 0)
                  << std::endl;
        return 0;
    }

    WalRecordHeader wal_header {};
    wal.read(reinterpret_cast<char*>(&wal_header), sizeof(wal_header));
    if (wal.gcount() == 0) {
        std::cout << " wal_exists=1 wal_empty=1 wal_size=" << file_size_or_zero(wal_path)
                  << " wal_records=0"
                  << " wal_put_records=0"
                  << " wal_delete_records=0"
                  << " wal_int_keys=0"
                  << " wal_string_keys=0"
                  << " wal_binary_keys=0"
                  << " wal_unknown_key_types=0"
                  << " wal_unknown_record_types=0"
                  << " wal_truncated=0"
                  << " rewrite_recommended=" << (rewrite_recommended ? 1 : 0)
                  << std::endl;
        return 0;
    }
    if (wal.gcount() != static_cast<std::streamsize>(sizeof(wal_header))) {
        std::cout << " wal_exists=1 wal_truncated=1 wal_size=" << file_size_or_zero(wal_path)
                  << " wal_records=0"
                  << " wal_put_records=0"
                  << " wal_delete_records=0"
                  << " wal_int_keys=0"
                  << " wal_string_keys=0"
                  << " wal_binary_keys=0"
                  << " wal_unknown_key_types=0"
                  << " wal_unknown_record_types=0"
                  << " rewrite_recommended=1"
                  << std::endl;
        return 0;
    }

    FormatKeyStats wal_stats;
    uint64_t wal_put_records = 0;
    uint64_t wal_delete_records = 0;
    uint64_t wal_unknown_record_types = 0;
    bool wal_truncated = false;
    const uint16_t wal_version = wal_header.version;
    rewrite_recommended = rewrite_recommended || wal_version != 2;
    wal.clear();
    wal.seekg(0, std::ios::beg);
    if (wal_version == 1) {
        while (true) {
            WalRecordHeaderV1 entry {};
            wal.read(reinterpret_cast<char*>(&entry), sizeof(entry));
            if (wal.gcount() == 0) {
                break;
            }
            if (wal.gcount() != static_cast<std::streamsize>(sizeof(entry))) {
                wal_truncated = true;
                break;
            }
            if (entry.type == 1) {
                wal_put_records += 1;
            } else if (entry.type == 2) {
                wal_delete_records += 1;
            } else {
                wal_unknown_record_types += 1;
            }
            wal_stats.total += 1;
            wal_stats.int_keys += 1;
            wal.seekg(entry.value_size, std::ios::cur);
            if (!wal.good()) {
                wal_truncated = true;
                break;
            }
        }
    } else {
        while (true) {
            WalRecordHeader entry {};
            wal.read(reinterpret_cast<char*>(&entry), sizeof(entry));
            if (wal.gcount() == 0) {
                break;
            }
            if (wal.gcount() != static_cast<std::streamsize>(sizeof(entry))) {
                wal_truncated = true;
                break;
            }
            if (entry.type == 1) {
                wal_put_records += 1;
            } else if (entry.type == 2) {
                wal_delete_records += 1;
            } else {
                wal_unknown_record_types += 1;
            }
            std::string key(entry.key_size, '\0');
            if (!key.empty()) {
                wal.read(key.data(), static_cast<std::streamsize>(key.size()));
                if (wal.gcount() != static_cast<std::streamsize>(key.size())) {
                    wal_truncated = true;
                    break;
                }
            }
            classify_encoded_key(key, wal_stats);
            wal.seekg(entry.value_size, std::ios::cur);
            if (!wal.good()) {
                wal_truncated = true;
                break;
            }
        }
    }

    rewrite_recommended = rewrite_recommended || wal_truncated;

    std::cout << " wal_exists=1"
              << " wal_empty=0"
              << " wal_magic=0x" << std::hex << wal_header.magic << std::dec
              << " wal_version=" << wal_header.version
              << " wal_first_type=" << static_cast<int>(wal_header.type)
              << " wal_size=" << file_size_or_zero(wal_path)
              << " wal_records=" << wal_stats.total
              << " wal_put_records=" << wal_put_records
              << " wal_delete_records=" << wal_delete_records
              << " wal_int_keys=" << wal_stats.int_keys
              << " wal_string_keys=" << wal_stats.string_keys
              << " wal_binary_keys=" << wal_stats.binary_keys
              << " wal_unknown_key_types=" << wal_stats.unknown_keys
              << " wal_unknown_record_types=" << wal_unknown_record_types
              << " wal_truncated=" << (wal_truncated ? 1 : 0)
              << " rewrite_recommended=" << (rewrite_recommended ? 1 : 0)
              << std::endl;
    return 0;
}

int run_rewrite_format_impl(const std::string& db_path) {
    KVStore store(db_path);
    store.Compact();
    return 0;
}

int run_verify_format_impl(const std::string& db_path) {
    std::ostringstream out;
    auto* original = std::cout.rdbuf(out.rdbuf());
    const int inspect_status = run_inspect_format_impl(db_path);
    std::cout.rdbuf(original);

    std::string inspect_output = out.str();
    if (!inspect_output.empty() && inspect_output.back() == '\n') {
        inspect_output.pop_back();
    }

    if (inspect_status != 0) {
        std::cout << inspect_output << " verify_reason=inspect_error" << std::endl;
        return inspect_status;
    }

    const auto values = parse_kv_line(inspect_output);
    const bool rewrite_recommended = values.count("rewrite_recommended") != 0 && values.at("rewrite_recommended") == "1";
    const bool snapshot_magic_ok = values.count("snapshot_magic_ok") == 0 || values.at("snapshot_magic_ok") == "1";
    const bool snapshot_entry_magic_ok =
        values.count("snapshot_entry_magic_ok") == 0 || values.at("snapshot_entry_magic_ok") == "1";
    const bool snapshot_truncated = values.count("snapshot_truncated") != 0 && values.at("snapshot_truncated") == "1";
    const bool wal_truncated = values.count("wal_truncated") != 0 && values.at("wal_truncated") == "1";

    std::string verify_reason = "current_layout";
    int status = 0;
    if (!snapshot_magic_ok) {
        verify_reason = "snapshot_magic";
        status = 2;
    } else if (!snapshot_entry_magic_ok) {
        verify_reason = "snapshot_entry_magic";
        status = 2;
    } else if (snapshot_truncated) {
        verify_reason = "snapshot_truncated";
        status = 2;
    } else if (wal_truncated) {
        verify_reason = "wal_truncated";
        status = 2;
    } else if (rewrite_recommended) {
        verify_reason = "migration_required";
        status = 2;
    }

    std::cout << inspect_output << " verify_reason=" << verify_reason << std::endl;
    return status;
}

int run_compatibility_matrix_impl() {
    TestDir dir("compatibility_matrix");
    const auto value_or = [](const std::map<std::string, std::string>& values,
                             const std::string& key,
                             std::string fallback) {
        const auto it = values.find(key);
        return it == values.end() ? fallback : it->second;
    };

    const std::string current_db_path = dir.file("current_v2.dat");
    {
        KVStore store(current_db_path);
        store.Put(1, text("one"));
        store.Put(std::string("alpha"), text("two"));
        store.Put(std::vector<uint8_t> {0x01, 0x02}, text("three"));
        store.Compact();
    }

    const auto current_inspect = capture_inspect_format(current_db_path);
    const auto [current_verify_status, current_verify_output] = capture_verify_format(current_db_path);
    const auto current_verify_values = parse_kv_line(current_verify_output);
    require(current_verify_status == 0, "current compatibility matrix case should verify cleanly");
    require(current_inspect.at("snapshot_version") == "2", "current case should report snapshot v2");
    require(current_inspect.at("rewrite_recommended") == "0", "current case should not recommend rewrite");
    require(current_verify_values.at("verify_reason") == "current_layout",
            "current compatibility matrix case should report a clean-layout reason");

    const std::string live_wal_db_path = dir.file("current_v2_live_wal.dat");
    {
        KVStore store(live_wal_db_path);
        store.Put(1, text("one"));
        store.Compact();
        store.Put(2, text("two"));
        store.Delete(1);
    }

    const auto live_wal_inspect = capture_inspect_format(live_wal_db_path);
    const auto [live_wal_verify_status, live_wal_verify_output] = capture_verify_format(live_wal_db_path);
    const auto live_wal_verify_values = parse_kv_line(live_wal_verify_output);
    require(live_wal_verify_status == 0, "current v2 layout with live WAL should verify cleanly");
    require(live_wal_inspect.at("snapshot_version") == "2", "live WAL case should keep snapshot v2");
    require(live_wal_inspect.at("wal_version") == "2", "live WAL case should report WAL v2");
    require(live_wal_inspect.at("wal_records") == "2", "live WAL case should retain post-snapshot WAL records");
    require(live_wal_inspect.at("rewrite_recommended") == "0",
            "live WAL case should not recommend rewrite");
    require(live_wal_verify_values.at("verify_reason") == "current_layout",
            "live WAL case should remain a clean current layout");

    const std::string truncated_db_path = dir.file("current_v2_truncated_wal.dat");
    const std::string truncated_wal_path = truncated_db_path + ".wal";
    {
        KVStore store(truncated_db_path);
        store.Put(1, text("stable"));
        store.Compact();
        store.Put(2, text("latest"));
    }
    append_bytes(truncated_wal_path, {0xAA, 0xBB, 0xCC});

    const auto truncated_inspect = capture_inspect_format(truncated_db_path);
    const auto [truncated_verify_status, truncated_verify_output] = capture_verify_format(truncated_db_path);
    const auto truncated_verify_values = parse_kv_line(truncated_verify_output);
    require(truncated_verify_status == 2, "truncated WAL case should require rewrite");
    require(truncated_inspect.at("snapshot_version") == "2", "truncated WAL case should keep snapshot v2");
    require(truncated_inspect.at("wal_truncated") == "1", "truncated WAL case should report WAL truncation");
    require(truncated_inspect.at("rewrite_recommended") == "1",
            "truncated WAL case should explicitly recommend rewrite");
    require(truncated_verify_values.at("verify_reason") == "wal_truncated",
            "truncated WAL case should expose the wal_truncated migration reason");

    require(run_rewrite_format_impl(truncated_db_path) == 0,
            "truncated WAL compatibility matrix case should rewrite successfully");
    const auto rewritten_truncated_inspect = capture_inspect_format(truncated_db_path);
    const auto [rewritten_truncated_verify_status, rewritten_truncated_verify_output] =
        capture_verify_format(truncated_db_path);
    const auto rewritten_truncated_verify_values = parse_kv_line(rewritten_truncated_verify_output);
    require(rewritten_truncated_verify_status == 0,
            "rewritten truncated WAL case should verify cleanly");
    require(rewritten_truncated_inspect.at("wal_truncated") == "0",
            "rewritten truncated WAL case should clear the truncation signal");
    require(rewritten_truncated_inspect.at("rewrite_recommended") == "0",
            "rewritten truncated WAL case should stop recommending rewrite");
    require(rewritten_truncated_verify_values.at("verify_reason") == "current_layout",
            "rewritten truncated WAL case should return to a clean-layout reason");

    const std::string legacy_db_path = dir.file("legacy_v1.dat");
    const std::string legacy_wal_path = legacy_db_path + ".wal";
    write_legacy_snapshot_v1(legacy_db_path, {
        {7, text("seven")},
        {8, text("eight")},
    });
    append_legacy_wal_record_v1(legacy_wal_path, 1, 9, text("nine"));

    const auto legacy_inspect = capture_inspect_format(legacy_db_path);
    const auto [legacy_verify_status, legacy_verify_output] = capture_verify_format(legacy_db_path);
    const auto legacy_verify_values = parse_kv_line(legacy_verify_output);
    require(legacy_verify_status == 2, "legacy compatibility matrix case should require rewrite");
    require(legacy_inspect.at("snapshot_version") == "1", "legacy case should report snapshot v1");
    require(legacy_inspect.at("rewrite_recommended") == "1", "legacy case should recommend rewrite");
    require(legacy_verify_values.at("verify_reason") == "migration_required",
            "legacy case should expose a migration-required reason");

    require(run_rewrite_format_impl(legacy_db_path) == 0, "legacy compatibility matrix case should rewrite successfully");
    const auto rewritten_inspect = capture_inspect_format(legacy_db_path);
    const auto [rewritten_verify_status, rewritten_verify_output] = capture_verify_format(legacy_db_path);
    const auto rewritten_verify_values = parse_kv_line(rewritten_verify_output);
    require(rewritten_verify_status == 0, "rewritten legacy case should verify cleanly");
    require(rewritten_inspect.at("snapshot_version") == "2", "rewritten legacy case should upgrade to snapshot v2");
    require(rewritten_inspect.at("rewrite_recommended") == "0", "rewritten legacy case should stop recommending rewrite");
    require(rewritten_verify_values.at("verify_reason") == "current_layout",
            "rewritten legacy case should return to a clean-layout reason");

    std::cout << "case=current_v2"
              << " snapshot_version=" << current_inspect.at("snapshot_version")
              << " wal_version=" << value_or(current_inspect, "wal_version", "empty")
              << " verify_status=" << current_verify_status
              << " verify_reason=" << current_verify_values.at("verify_reason")
              << " rewrite_recommended=" << current_inspect.at("rewrite_recommended")
              << " current_verify_output_bytes=" << current_verify_output.size()
              << '\n';
    std::cout << "case=legacy_v1"
              << " snapshot_version=" << legacy_inspect.at("snapshot_version")
              << " wal_version=" << value_or(legacy_inspect, "wal_version", "missing")
              << " verify_status=" << legacy_verify_status
              << " verify_reason=" << legacy_verify_values.at("verify_reason")
              << " rewrite_recommended=" << legacy_inspect.at("rewrite_recommended")
              << " legacy_verify_output_bytes=" << legacy_verify_output.size()
              << '\n';
    std::cout << "case=legacy_v1_after_rewrite"
              << " snapshot_version=" << rewritten_inspect.at("snapshot_version")
              << " wal_exists=" << rewritten_inspect.at("wal_exists")
              << " verify_status=" << rewritten_verify_status
              << " verify_reason=" << rewritten_verify_values.at("verify_reason")
              << " rewrite_recommended=" << rewritten_inspect.at("rewrite_recommended")
              << " rewritten_verify_output_bytes=" << rewritten_verify_output.size()
              << '\n';
    std::cout << "case=current_v2_live_wal"
              << " snapshot_version=" << live_wal_inspect.at("snapshot_version")
              << " wal_version=" << value_or(live_wal_inspect, "wal_version", "missing")
              << " wal_records=" << live_wal_inspect.at("wal_records")
              << " verify_status=" << live_wal_verify_status
              << " verify_reason=" << live_wal_verify_values.at("verify_reason")
              << " rewrite_recommended=" << live_wal_inspect.at("rewrite_recommended")
              << " live_wal_verify_output_bytes=" << live_wal_verify_output.size()
              << '\n';
    std::cout << "case=current_v2_truncated_wal"
              << " snapshot_version=" << truncated_inspect.at("snapshot_version")
              << " wal_truncated=" << truncated_inspect.at("wal_truncated")
              << " verify_status=" << truncated_verify_status
              << " verify_reason=" << truncated_verify_values.at("verify_reason")
              << " rewrite_recommended=" << truncated_inspect.at("rewrite_recommended")
              << " truncated_verify_output_bytes=" << truncated_verify_output.size()
              << '\n';
    std::cout << "case=current_v2_truncated_wal_after_rewrite"
              << " snapshot_version=" << rewritten_truncated_inspect.at("snapshot_version")
              << " wal_truncated=" << rewritten_truncated_inspect.at("wal_truncated")
              << " verify_status=" << rewritten_truncated_verify_status
              << " verify_reason=" << rewritten_truncated_verify_values.at("verify_reason")
              << " rewrite_recommended=" << rewritten_truncated_inspect.at("rewrite_recommended")
              << " rewritten_truncated_verify_output_bytes=" << rewritten_truncated_verify_output.size()
              << '\n';
    return 0;
}

}  // namespace

int run_inspect_format(const std::string& db_path) {
    return run_inspect_format_impl(db_path);
}

int run_rewrite_format(const std::string& db_path) {
    return run_rewrite_format_impl(db_path);
}

int run_verify_format(const std::string& db_path) {
    return run_verify_format_impl(db_path);
}

int run_compatibility_matrix() {
    return run_compatibility_matrix_impl();
}
