#include "tests/unit/test_registry.h"

#include "internal/container_format.h"

#include <cstdint>

namespace kvstore::tests::unit {
namespace {

using test_support::require;

void test_container_format_plans_aligned_non_overlapping_sections() {
    const auto layout = kvstore::internal::plan_container_layout(1024, 2048, 4096, 8192);

    require(layout.sections.size() == 4, "planned container layout should contain the four baseline sections");
    require(kvstore::internal::has_valid_container_magic(layout.header),
            "planned container header should use the expected container magic");
    require(layout.header.version == kvstore::internal::kContainerVersion,
            "planned container header should use the expected version");
    require(kvstore::internal::sections_are_non_overlapping(layout.sections),
            "planned container sections should not overlap");

    for (const auto& section : layout.sections) {
        require(section.offset % kvstore::internal::kContainerDefaultAlignment == 0,
                "planned container sections should respect default alignment");
    }

    require(layout.file_bytes >= layout.sections.back().offset + layout.sections.back().length,
            "planned file size should cover the last section");
}

void test_container_layout_checksum_changes_with_section_shape() {
    auto baseline = kvstore::internal::plan_container_layout(1024, 2048, 4096, 8192);
    auto changed = baseline;
    changed.sections[2].length += 4096;

    const uint32_t baseline_checksum =
        kvstore::internal::checksum_container_layout(baseline.header, baseline.sections);
    const uint32_t changed_checksum =
        kvstore::internal::checksum_container_layout(changed.header, changed.sections);

    require(baseline_checksum == baseline.header.checksum,
            "planned container header checksum should match the computed layout checksum");
    require(baseline_checksum != changed_checksum,
            "container layout checksum should change when the section shape changes");
}

void test_container_layout_detects_overlap() {
    std::vector<kvstore::internal::ContainerSectionDescriptor> sections;
    sections.push_back(kvstore::internal::make_container_section(
        kvstore::internal::ContainerSectionType::kMetadata, 4096, 1024));
    sections.push_back(kvstore::internal::make_container_section(
        kvstore::internal::ContainerSectionType::kIndex, 4608, 2048));

    require(!kvstore::internal::sections_are_non_overlapping(sections),
            "container section helper should reject overlapping ranges");
}

}  // namespace

void register_container_format_tests(TestCases& tests) {
    tests.push_back({"container format plans aligned non-overlapping sections",
                     test_container_format_plans_aligned_non_overlapping_sections});
    tests.push_back({"container layout checksum changes with section shape",
                     test_container_layout_checksum_changes_with_section_shape});
    tests.push_back({"container layout detects overlap", test_container_layout_detects_overlap});
}

}  // namespace kvstore::tests::unit
