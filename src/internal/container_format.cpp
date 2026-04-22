#include "container_format.h"

#include <algorithm>
#include <cstring>
#include <stdexcept>

namespace kvstore::internal {

static_assert(sizeof(ContainerHeader) == 32, "Unexpected container header size");
static_assert(sizeof(ContainerSectionDescriptor) == 24, "Unexpected container section descriptor size");

namespace {

uint32_t fnv1a_append(uint32_t seed, const void* data, size_t size) {
    const auto* bytes = static_cast<const uint8_t*>(data);
    uint32_t hash = seed;
    for (size_t i = 0; i < size; ++i) {
        hash ^= bytes[i];
        hash *= 16777619u;
    }
    return hash;
}

}  // namespace

uint64_t align_up(uint64_t value, uint64_t alignment) {
    if (alignment == 0) {
        throw std::invalid_argument("alignment must be non-zero");
    }
    const uint64_t remainder = value % alignment;
    if (remainder == 0) {
        return value;
    }
    return value + (alignment - remainder);
}

ContainerHeader make_container_header(uint32_t section_count) {
    ContainerHeader header {};
    std::memcpy(header.magic, kContainerMagic, sizeof(kContainerMagic));
    header.version = kContainerVersion;
    header.header_bytes = sizeof(ContainerHeader) + (section_count * sizeof(ContainerSectionDescriptor));
    header.section_count = section_count;
    header.flags = 0;
    header.checksum = 0;
    header.reserved = 0;
    return header;
}

ContainerSectionDescriptor make_container_section(ContainerSectionType type,
                                                  uint64_t offset,
                                                  uint64_t length,
                                                  uint32_t flags) {
    ContainerSectionDescriptor section {};
    section.type = static_cast<uint32_t>(type);
    section.flags = flags;
    section.offset = offset;
    section.length = length;
    return section;
}

uint32_t checksum_container_layout(const ContainerHeader& header,
                                   const std::vector<ContainerSectionDescriptor>& sections) {
    uint32_t hash = 2166136261u;
    ContainerHeader normalized = header;
    normalized.checksum = 0;
    hash = fnv1a_append(hash, &normalized, sizeof(normalized));
    for (const auto& section : sections) {
        hash = fnv1a_append(hash, &section, sizeof(section));
    }
    return hash;
}

bool has_valid_container_magic(const ContainerHeader& header) {
    return std::memcmp(header.magic, kContainerMagic, sizeof(kContainerMagic)) == 0;
}

bool sections_are_non_overlapping(const std::vector<ContainerSectionDescriptor>& sections) {
    std::vector<ContainerSectionDescriptor> sorted = sections;
    std::sort(sorted.begin(), sorted.end(), [](const auto& lhs, const auto& rhs) {
        return lhs.offset < rhs.offset;
    });

    uint64_t previous_end = 0;
    bool first = true;
    for (const auto& section : sorted) {
        if (!first && section.offset < previous_end) {
            return false;
        }
        previous_end = section.offset + section.length;
        first = false;
    }
    return true;
}

PlannedContainerLayout plan_container_layout(uint64_t metadata_bytes,
                                             uint64_t index_bytes,
                                             uint64_t object_bytes,
                                             uint64_t journal_bytes,
                                             uint64_t alignment) {
    PlannedContainerLayout layout;
    layout.header = make_container_header(4);
    uint64_t cursor = align_up(layout.header.header_bytes, alignment);

    auto append_section = [&](ContainerSectionType type, uint64_t size) {
        const uint64_t offset = cursor;
        layout.sections.push_back(make_container_section(type, offset, size));
        cursor = align_up(offset + size, alignment);
    };

    append_section(ContainerSectionType::kMetadata, metadata_bytes);
    append_section(ContainerSectionType::kIndex, index_bytes);
    append_section(ContainerSectionType::kObjectData, object_bytes);
    append_section(ContainerSectionType::kJournal, journal_bytes);
    layout.header.checksum = checksum_container_layout(layout.header, layout.sections);
    layout.file_bytes = cursor;
    return layout;
}

}  // namespace kvstore::internal
