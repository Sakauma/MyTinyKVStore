#include "kvstore.h"

#include "internal/format.h"
#include "internal/recovery.h"
#include "internal/v3_format.h"

#include <cstdint>
#include <filesystem>
#include <iostream>
#include <map>
#include <string>
#include <vector>

namespace {

int32_t decode_int_key(const std::string& encoded) {
    if (encoded.size() != 5 || encoded.front() != kvstore::internal::kIntKeyTag) {
        throw KVStoreError("Legacy migration encountered an invalid integer key encoding");
    }
    uint32_t value = 0;
    for (size_t index = 1; index < encoded.size(); ++index) {
        value = (value << 8U) | static_cast<unsigned char>(encoded[index]);
    }
    return static_cast<int32_t>(value);
}

BatchWriteOperation convert_entry(const std::string& encoded, const Value& value) {
    if (encoded.empty()) {
        throw KVStoreError("Legacy migration encountered an empty encoded key");
    }
    switch (encoded.front()) {
        case kvstore::internal::kIntKeyTag:
            return BatchWriteOperation::PutInt(decode_int_key(encoded), value);
        case kvstore::internal::kStringKeyTag:
            return BatchWriteOperation::Put(encoded.substr(1), value);
        case kvstore::internal::kBinaryKeyTag:
            return BatchWriteOperation::PutBinary(
                std::vector<uint8_t>(encoded.begin() + 1, encoded.end()),
                value);
        default:
            throw KVStoreError("Legacy migration encountered an unknown key namespace");
    }
}

void print_usage() {
    std::cerr << "Usage: kv_migrate --input <legacy_db> --output <v3_db>" << std::endl;
}

}  // namespace

int main(int argc, char* argv[]) {
    if (argc != 5 || std::string(argv[1]) != "--input" || std::string(argv[3]) != "--output") {
        print_usage();
        return 1;
    }
    const std::filesystem::path input = std::filesystem::absolute(argv[2]).lexically_normal();
    const std::filesystem::path output = std::filesystem::absolute(argv[4]).lexically_normal();
    if (input == output) {
        std::cerr << "Migration is never performed in-place." << std::endl;
        return 2;
    }
    if (!std::filesystem::exists(input)) {
        std::cerr << "Legacy input does not exist: " << input << std::endl;
        return 2;
    }
    if (std::filesystem::exists(output)) {
        std::cerr << "Output already exists and will not be overwritten: " << output << std::endl;
        return 2;
    }

    try {
        kvstore::internal::StateMap state;
        kvstore::internal::load_snapshot_into_state(input.string(), state);
        kvstore::internal::replay_wal_into_state(input.string() + ".wal", state, {});

        {
            KVStore target(output.string());
            std::vector<BatchWriteOperation> batch;
            batch.reserve(1024);
            for (const auto& [key, value] : state) {
                batch.push_back(convert_entry(key, value));
                if (batch.size() == 1024) {
                    target.WriteBatch(batch);
                    batch.clear();
                }
            }
            if (!batch.empty()) {
                target.WriteBatch(batch);
            }
            target.Compact();
            target.Flush();
        }

        const auto inspection = kvstore::internal::inspect_v3_file(output.string());
        if (inspection.valid_superblocks != 2 || inspection.truncated_tail) {
            throw KVStoreError("Generated v3 database did not pass post-migration verification");
        }
        std::cout << "status=pass input=" << input.string()
                  << " output=" << output.string()
                  << " migrated_entries=" << state.size()
                  << " container_version=3" << std::endl;
        return 0;
    } catch (const std::exception& error) {
        std::error_code ignored;
        std::filesystem::remove(output, ignored);
        std::cerr << "Migration failed: " << error.what() << std::endl;
        return 3;
    }
}
