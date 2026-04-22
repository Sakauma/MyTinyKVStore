#ifndef KVSTORE_INTERNAL_CONTAINER_FORMAT_H
#define KVSTORE_INTERNAL_CONTAINER_FORMAT_H

#include <cstdint>
#include <vector>

namespace kvstore::internal {

inline constexpr char kContainerMagic[8] = {'K', 'V', 'C', 'N', 'T', 'R', '0', '1'};
inline constexpr uint32_t kContainerVersion = 1;
inline constexpr uint64_t kContainerDefaultAlignment = 4096;

enum class ContainerSectionType : uint32_t {
    kMetadata = 1,
    kIndex = 2,
    kObjectData = 3,
    kJournal = 4,
};

#pragma pack(push, 1)
struct ContainerHeader {
    char magic[8];
    uint32_t version;
    uint32_t header_bytes;
    uint32_t section_count;
    uint32_t flags;
    uint32_t checksum;
    uint32_t reserved;
};

struct ContainerSectionDescriptor {
    uint32_t type;
    uint32_t flags;
    uint64_t offset;
    uint64_t length;
};
#pragma pack(pop)

struct PlannedContainerLayout {
    ContainerHeader header {};
    std::vector<ContainerSectionDescriptor> sections;
    uint64_t file_bytes = 0;
};

uint64_t align_up(uint64_t value, uint64_t alignment);
ContainerHeader make_container_header(uint32_t section_count);
ContainerSectionDescriptor make_container_section(ContainerSectionType type,
                                                  uint64_t offset,
                                                  uint64_t length,
                                                  uint32_t flags = 0);
uint32_t checksum_container_layout(const ContainerHeader& header,
                                   const std::vector<ContainerSectionDescriptor>& sections);
bool has_valid_container_magic(const ContainerHeader& header);
bool sections_are_non_overlapping(const std::vector<ContainerSectionDescriptor>& sections);
PlannedContainerLayout plan_container_layout(uint64_t metadata_bytes,
                                             uint64_t index_bytes,
                                             uint64_t object_bytes,
                                             uint64_t journal_bytes,
                                             uint64_t alignment = kContainerDefaultAlignment);

}  // namespace kvstore::internal

#endif  // KVSTORE_INTERNAL_CONTAINER_FORMAT_H
