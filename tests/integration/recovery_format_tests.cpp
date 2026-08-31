#include "tests/integration/test_registry.h"

#include "internal/storage_format.h"
#include "tests/common/format_analysis.h"
#include "tests/common/test_support.h"

#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <limits>
#include <string>
#include <vector>

namespace kvstore::tests::integration {
namespace {

using test_support::append_bytes;
using test_support::as_string;
using test_support::require;
using test_support::TestDir;
using test_support::text;

void flip_byte(const std::string& path, std::streamoff offset) {
    std::fstream file(path, std::ios::binary | std::ios::in | std::ios::out);
    require(file.is_open(), "Expected file to be writable for corruption test");
    file.seekg(offset);
    char byte = 0;
    file.read(&byte, 1);
    require(file.gcount() == 1, "Expected corruption offset to exist");
    byte ^= static_cast<char>(0x5A);
    file.seekp(offset);
    file.write(&byte, 1);
}

template <typename T>
T read_object(const std::string& path, uint64_t offset) {
    std::ifstream file(path, std::ios::binary);
    require(file.is_open(), "Expected file to be readable");
    file.seekg(static_cast<std::streamoff>(offset));
    T value {};
    file.read(reinterpret_cast<char*>(&value), sizeof(value));
    require(file.gcount() == static_cast<std::streamsize>(sizeof(value)),
            "Expected complete object in test database");
    return value;
}

template <typename T>
void write_object(const std::string& path, uint64_t offset, const T& value) {
    std::fstream file(path, std::ios::binary | std::ios::in | std::ios::out);
    require(file.is_open(), "Expected file to be writable");
    file.seekp(static_cast<std::streamoff>(offset));
    file.write(reinterpret_cast<const char*>(&value), sizeof(value));
    require(file.good(), "Expected complete object write in corruption test");
}

void truncate_inside_last_frame(const std::string& path) {
    const uint64_t size = std::filesystem::file_size(path);
    require(size > sizeof(kvstore::internal::FrameFooter), "Expected a journal frame footer");
    std::ifstream input(path, std::ios::binary);
    input.seekg(static_cast<std::streamoff>(size - sizeof(kvstore::internal::FrameFooter)));
    kvstore::internal::FrameFooter footer {};
    input.read(reinterpret_cast<char*>(&footer), sizeof(footer));
    require(footer.magic == kvstore::internal::kFooterMagic, "Expected a complete final storage frame");
    require(footer.frame_bytes < size, "Expected checkpoint/superblocks before final frame");
    const uint64_t frame_start = size - footer.frame_bytes;
    const uint64_t truncated_size = frame_start + sizeof(kvstore::internal::FrameHeader) + 7;
    std::filesystem::resize_file(path, truncated_size);
}

void test_recovery_from_single_file_journal() {
    TestDir directory("storage_journal_recovery");
    const std::string path = directory.file("store.dat");
    {
        KVStore store(path);
        store.Put(7, text("seven"));
        store.Put(std::string("alpha"), text("string"));
    }
    require(!std::filesystem::exists(path + ".wal"), "storage runtime must not create an external WAL");

    KVStore reopened(path);
    require(reopened.Get(7).has_value() && as_string(*reopened.Get(7)) == "seven",
            "integer value should recover from the in-file journal");
    require(reopened.Get(std::string("alpha")).has_value(),
            "string value should recover from the in-file journal");
}

void test_write_batch_tail_is_all_or_nothing() {
    TestDir directory("storage_atomic_batch_tail");
    const std::string path = directory.file("store.dat");
    {
        KVStore store(path);
        store.Put(1, text("stable"));
        store.WriteBatch({
            BatchWriteOperation::PutInt(2, text("two")),
            BatchWriteOperation::PutInt(3, text("three")),
            BatchWriteOperation::DeleteInt(1),
        });
    }
    truncate_inside_last_frame(path);

    KVStore recovered(path);
    const auto stable = recovered.Get(1);
    require(stable.has_value() && as_string(*stable) == "stable",
            "the transaction before a torn batch should survive");
    require(!recovered.Get(2).has_value() && !recovered.Get(3).has_value(),
            "no operation from a torn WriteBatch may be applied");
}

void test_truncated_tail_is_reported_then_repaired() {
    TestDir directory("storage_truncated_tail");
    const std::string path = directory.file("store.dat");
    {
        KVStore store(path);
        store.Put(1, text("stable"));
    }
    append_bytes(path, {0xAA, 0xBB, 0xCC});
    require(verify_format_analysis(path).status == 2,
            "verify-format must reject an incomplete storage tail");

    {
        KVStore recovered(path);
        require(recovered.Get(1).has_value(), "runtime recovery should ignore the incomplete final tail");
    }
    require(verify_format_analysis(path).status == 0,
            "opening the database should truncate the incomplete tail to a valid frame boundary");
}

void test_corrupted_complete_frame_is_rejected() {
    TestDir directory("storage_corrupted_frame");
    const std::string path = directory.file("store.dat");
    {
        KVStore store(path);
        store.Put(1, text("payload"));
    }
    const auto inspection = kvstore::internal::inspect_file(path);
    const uint64_t frame_start = inspection.superblock.journal_offset;
    flip_byte(path, static_cast<std::streamoff>(frame_start + sizeof(kvstore::internal::FrameHeader) +
                                                sizeof(kvstore::internal::MutationHeader) + 2));
    require(verify_format_analysis(path).status == 2,
            "verify-format must reject the same checksum failure as runtime recovery");

    bool threw = false;
    try {
        KVStore reopened(path);
        (void)reopened;
    } catch (const KVStoreError&) {
        threw = true;
    }
    require(threw, "runtime must reject a complete frame with a bad checksum");
}

void test_duplicate_and_decreasing_lsns_are_rejected() {
    TestDir directory("storage_invalid_lsn");
    const std::vector<std::pair<std::string, uint64_t>> cases {
        {"duplicate", 1},
        {"decreasing", 0},
    };
    for (const auto& [name, forged_lsn] : cases) {
        const std::string path = directory.file(name + ".dat");
        {
            KVStore store(path);
            store.Put(1, text("first"));
            store.Put(2, text("second"));
        }
        const auto inspection = kvstore::internal::inspect_file(path);
        const uint64_t first_offset = inspection.superblock.journal_offset;
        const auto first_header = read_object<kvstore::internal::FrameHeader>(path, first_offset);
        const uint64_t second_offset = first_offset + first_header.frame_bytes;
        auto second_header = read_object<kvstore::internal::FrameHeader>(path, second_offset);
        const uint64_t footer_offset = second_offset + second_header.frame_bytes -
                                       sizeof(kvstore::internal::FrameFooter);
        auto second_footer = read_object<kvstore::internal::FrameFooter>(path, footer_offset);

        second_header.lsn = forged_lsn;
        second_header.header_checksum = 0;
        second_header.header_checksum = kvstore::internal::crc32c(&second_header, sizeof(second_header));
        second_footer.lsn = forged_lsn;
        second_footer.checksum = 0;
        second_footer.checksum = kvstore::internal::crc32c(&second_footer, sizeof(second_footer));
        write_object(path, second_offset, second_header);
        write_object(path, footer_offset, second_footer);

        require(verify_format_analysis(path).status == 2,
                "shared verifier must reject duplicate or decreasing transaction LSNs");
        bool rejected = false;
        try {
            KVStore reopened(path);
            (void)reopened;
        } catch (const KVStoreError& error) {
            rejected = std::string(error.what()).find("strictly increasing") != std::string::npos;
        }
        require(rejected, "runtime must reject duplicate or decreasing transaction LSNs");
    }
}

void test_checkpoint_object_corruption_is_rejected() {
    TestDir directory("storage_checkpoint_corruption");
    const std::string path = directory.file("store.dat");
    {
        KVStore store(path);
        store.Put(1, text("checkpoint-value"));
        store.Compact();
    }
    const auto inspection = kvstore::internal::inspect_file(path);
    require(inspection.superblock.object_length > 0, "compaction should create checkpoint objects");
    flip_byte(path, static_cast<std::streamoff>(inspection.superblock.object_offset));

    require(verify_format_analysis(path).status == 2,
            "shared verifier must reject checkpoint object corruption");
    bool rejected = false;
    try {
        KVStore reopened(path);
        (void)reopened;
    } catch (const KVStoreError& error) {
        rejected = std::string(error.what()).find("checkpoint checksum") != std::string::npos;
    }
    require(rejected, "runtime must reject a corrupted checkpoint object before publishing state");
}

void test_forged_operation_count_is_rejected_before_allocation() {
    TestDir directory("storage_forged_operation_count");
    const std::string path = directory.file("store.dat");
    {
        KVStore store(path);
        store.Put(1, text("small-payload"));
    }
    const auto inspection = kvstore::internal::inspect_file(path);
    const uint64_t frame_offset = inspection.superblock.journal_offset;
    auto header = read_object<kvstore::internal::FrameHeader>(path, frame_offset);
    header.operation_count = std::numeric_limits<uint32_t>::max();
    header.header_checksum = 0;
    header.header_checksum = kvstore::internal::crc32c(&header, sizeof(header));
    write_object(path, frame_offset, header);

    require(verify_format_analysis(path).status == 2,
            "shared verifier must reject an operation count that cannot fit in the frame payload");
    bool rejected = false;
    try {
        KVStore reopened(path);
        (void)reopened;
    } catch (const KVStoreError& error) {
        rejected = std::string(error.what()).find("operation count") != std::string::npos;
    }
    require(rejected, "runtime must reject forged counts before reserve/allocation");
}

void test_forged_region_length_is_rejected_before_allocation() {
    TestDir directory("storage_forged_region_length");
    const std::string path = directory.file("store.dat");
    {
        KVStore store(path);
        store.Put(1, text("value"));
    }
    auto superblock = read_object<kvstore::internal::Superblock>(path, 0);
    superblock.object_length = std::numeric_limits<uint64_t>::max();
    superblock.checksum = 0;
    superblock.checksum = kvstore::internal::crc32c(&superblock, sizeof(superblock));
    write_object(path, 0, superblock);
    write_object(path, kvstore::internal::kSuperblockBytes, superblock);

    bool rejected = false;
    try {
        KVStore reopened(path);
        (void)reopened;
    } catch (const KVStoreError& error) {
        rejected = std::string(error.what()).find("region boundaries") != std::string::npos;
    }
    require(rejected, "runtime must reject overflowing region lengths before reading or allocation");
}

void test_single_superblock_corruption_is_recoverable_but_degraded() {
    TestDir directory("storage_one_superblock");
    const std::string path = directory.file("store.dat");
    {
        KVStore store(path);
        store.Put(5, text("five"));
    }
    flip_byte(path, 0);
    require(verify_format_analysis(path).status == 2,
            "verify-format should flag a database with only one valid superblock");

    KVStore reopened(path);
    require(reopened.Get(5).has_value(), "runtime should recover through the second superblock copy");
}

void test_both_superblocks_corrupted_are_rejected() {
    TestDir directory("storage_both_superblocks");
    const std::string path = directory.file("store.dat");
    {
        KVStore store(path);
        store.Put(9, text("nine"));
    }
    flip_byte(path, 0);
    flip_byte(path, static_cast<std::streamoff>(kvstore::internal::kSuperblockBytes));

    bool threw = false;
    try {
        KVStore reopened(path);
        (void)reopened;
    } catch (const KVStoreError&) {
        threw = true;
    }
    require(threw, "runtime must reject a container when both superblocks are invalid");
}

void test_compaction_rewrites_checkpoint_and_preserves_state() {
    TestDir directory("storage_checkpoint");
    const std::string path = directory.file("store.dat");
    {
        KVStore store(path);
        for (int key = 0; key < 32; ++key) {
            store.Put(key, text("value_" + std::to_string(key)));
        }
        store.Compact();
    }
    const auto inspection = kvstore::internal::inspect_file(path);
    require(inspection.checkpoint_entries == 32, "compaction should move live values into the checkpoint index");
    require(inspection.journal_frames == 0, "synchronous compaction should leave an empty journal without concurrent writes");

    KVStore reopened(path);
    for (int key = 0; key < 32; ++key) {
        require(reopened.Get(key).has_value(), "checkpointed values must survive reopen");
    }
}

void test_inspect_and_verify_use_runtime_parser() {
    TestDir directory("storage_inspect");
    const std::string path = directory.file("store.dat");
    {
        KVStore store(path);
        store.Put(1, text("one"));
        store.Put(std::string("alpha"), text("two"));
        store.Put(std::vector<uint8_t> {0x01, 0x02}, text("three"));
    }
    const FormatInspectResult inspection = inspect_format_analysis(path);
    require(inspection.status == 0, "inspect-format should parse a healthy storage container");
    const auto fields = parse_kv_line(inspection.output_line);
    require(fields.at("container_version") == "3", "inspect-format should report the on-disk format version");
    require(fields.at("valid_superblocks") == "2", "inspect-format should validate both superblocks");
    require(fields.at("journal_frames") == "3", "inspect-format should count transaction frames");
    require(verify_format_analysis(path).status == 0, "verify-format should accept a healthy storage container");
}

}  // namespace

void register_recovery_format_tests(TestCases& tests) {
    tests.push_back({"storage recovers from its single-file journal", test_recovery_from_single_file_journal});
    tests.push_back({"torn WriteBatch is recovered all-or-nothing", test_write_batch_tail_is_all_or_nothing});
    tests.push_back({"truncated storage tail is reported then repaired", test_truncated_tail_is_reported_then_repaired});
    tests.push_back({"corrupted complete storage frame is rejected", test_corrupted_complete_frame_is_rejected});
    tests.push_back({"duplicate and decreasing storage LSNs are rejected", test_duplicate_and_decreasing_lsns_are_rejected});
    tests.push_back({"checkpoint object corruption is rejected", test_checkpoint_object_corruption_is_rejected});
    tests.push_back({"forged storage operation count is bounded", test_forged_operation_count_is_rejected_before_allocation});
    tests.push_back({"forged storage region length is bounded", test_forged_region_length_is_rejected_before_allocation});
    tests.push_back({"single superblock corruption is recoverable", test_single_superblock_corruption_is_recoverable_but_degraded});
    tests.push_back({"both superblocks corrupted are rejected", test_both_superblocks_corrupted_are_rejected});
    tests.push_back({"storage compaction checkpoint preserves state", test_compaction_rewrites_checkpoint_and_preserves_state});
    tests.push_back({"inspect and verify share the runtime parser", test_inspect_and_verify_use_runtime_parser});
}

}  // namespace kvstore::tests::integration
