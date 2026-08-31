#include "tests/unit/test_registry.h"

#include "internal/key_codec.h"
#include "internal/storage_format.h"

#include <cstring>
#include <string>
#include <vector>

namespace kvstore::tests::unit {
namespace {

using test_support::require;
using test_support::text;

void test_crc32c_matches_standard_vector() {
    const std::string input = "123456789";
    require(kvstore::internal::crc32c(input.data(), input.size()) == 0xE3069283U,
            "CRC32C should match the Castagnoli standard test vector");
}

void test_storage_transaction_frame_has_matching_commit_footer() {
    const std::vector<kvstore::internal::Mutation> operations {
        {kvstore::internal::MutationType::kPut,
         kvstore::internal::encode_int_key(7),
         text("seven")},
        {kvstore::internal::MutationType::kDelete,
         kvstore::internal::encode_string_key("gone"),
         Value {}},
    };
    const auto payload = kvstore::internal::serialize_payload(operations);
    const auto frame = kvstore::internal::serialize_frame(payload, operations.size(), 42);
    kvstore::internal::FrameHeader header {};
    kvstore::internal::FrameFooter footer {};
    std::memcpy(&header, frame.data(), sizeof(header));
    std::memcpy(&footer, frame.data() + frame.size() - sizeof(footer), sizeof(footer));
    require(header.magic == kvstore::internal::kFrameMagic &&
                footer.magic == kvstore::internal::kFooterMagic,
            "storage frame should contain both header and commit footer magic");
    require(header.lsn == footer.lsn && header.lsn == 42,
            "storage frame footer should commit the same LSN as its header");
    require(header.frame_bytes == footer.frame_bytes && header.frame_bytes == frame.size(),
            "storage frame header/footer should agree on the complete frame length");
    require(header.payload_checksum == footer.payload_checksum,
            "storage frame header/footer should agree on the payload checksum");
}

void test_storage_checkpoint_plans_single_file_regions() {
    const std::vector<kvstore::internal::Mutation> entries {
        {kvstore::internal::MutationType::kPut,
         kvstore::internal::encode_int_key(1),
         text("one")},
        {kvstore::internal::MutationType::kPut,
         kvstore::internal::encode_string_key("two"),
         text("two")},
    };
    const auto image = kvstore::internal::build_checkpoint(entries, 3, 9);
    require(kvstore::internal::valid_superblock(image.superblock),
            "checkpoint builder should produce a checksummed storage superblock");
    require(image.superblock.index_offset == kvstore::internal::kDataOffset,
            "checkpoint index should start after both fixed superblocks");
    require(image.superblock.object_offset == image.superblock.index_offset + image.index.size(),
            "checkpoint object region should follow its index");
    require(image.superblock.journal_offset == image.superblock.object_offset + image.objects.size(),
            "in-file journal should follow checkpoint objects");
}

void test_storage_superblock_checksum_detects_corruption() {
    auto superblock = kvstore::internal::make_superblock(
        1,
        0,
        kvstore::internal::kDataOffset,
        0,
        kvstore::internal::kDataOffset,
        0,
        kvstore::internal::kDataOffset,
        0);
    require(kvstore::internal::valid_superblock(superblock),
            "fresh storage superblock should validate");
    superblock.journal_offset += 1;
    require(!kvstore::internal::valid_superblock(superblock),
            "superblock checksum should reject a modified region boundary");
}

}  // namespace

void register_storage_format_tests(TestCases& tests) {
    tests.push_back({"storage CRC32C matches standard vector", test_crc32c_matches_standard_vector});
    tests.push_back({"storage frame has a matching commit footer", test_storage_transaction_frame_has_matching_commit_footer});
    tests.push_back({"storage checkpoint plans single-file regions", test_storage_checkpoint_plans_single_file_regions});
    tests.push_back({"storage superblock checksum detects corruption", test_storage_superblock_checksum_detects_corruption});
}

}  // namespace kvstore::tests::unit
