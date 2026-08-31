#include "tests/common/format_analysis.h"

#include "internal/storage_format.h"
#include "tests/common/test_support.h"

#include <filesystem>
#include <iostream>
#include <map>
#include <sstream>
#include <string>

namespace {

std::string token_safe(std::string value) {
    for (char& character : value) {
        if (character == ' ' || character == '\n' || character == '\r' || character == '\t') {
            character = '_';
        }
    }
    return value;
}

}  // namespace

std::map<std::string, std::string> parse_kv_line(const std::string& line) {
    std::map<std::string, std::string> values;
    std::istringstream input(line);
    std::string token;
    while (input >> token) {
        const size_t equals = token.find('=');
        test_support::require(equals != std::string::npos, "Expected key=value token in inspect output");
        values[token.substr(0, equals)] = token.substr(equals + 1);
    }
    return values;
}

FormatInspectResult inspect_format_analysis(const std::string& db_path) {
    if (!std::filesystem::exists(db_path)) {
        return {1, "container_exists=0 valid=0"};
    }

    try {
        const kvstore::internal::RecoveryResult result = kvstore::internal::inspect_file(db_path);
        std::ostringstream output;
        output << "container_exists=1 valid=1"
               << " container_version=" << kvstore::internal::kFormatVersion
               << " generation=" << result.superblock.generation
               << " valid_superblocks=" << result.valid_superblocks
               << " checkpoint_lsn=" << result.superblock.checkpoint_lsn
               << " checkpoint_entries=" << result.checkpoint_entries
               << " journal_offset=" << result.superblock.journal_offset
               << " journal_frames=" << result.journal_frames
               << " journal_operations=" << result.journal_operations
               << " journal_put_operations=" << result.put_operations
               << " journal_delete_operations=" << result.delete_operations
               << " int_keys=" << result.int_keys
               << " string_keys=" << result.string_keys
               << " binary_keys=" << result.binary_keys
               << " last_lsn=" << result.last_lsn
               << " append_offset=" << result.append_offset
               << " truncated_tail=" << (result.truncated_tail ? 1 : 0)
               << " rewrite_recommended="
               << ((result.truncated_tail || result.valid_superblocks != 2) ? 1 : 0);
        return {0, output.str()};
    } catch (const std::exception& error) {
        return {2, "container_exists=1 valid=0 format_error=" + token_safe(error.what())};
    }
}

FormatVerifyResult verify_format_analysis(const std::string& db_path) {
    const FormatInspectResult inspection = inspect_format_analysis(db_path);
    if (inspection.status != 0) {
        return {inspection.status,
                inspection.output_line + " verify_reason=inspect_error",
                "inspect_error"};
    }
    const auto fields = parse_kv_line(inspection.output_line);
    std::string reason = "current_layout";
    int status = 0;
    if (fields.count("valid") == 0 || fields.at("valid") != "1") {
        reason = "invalid_container";
        status = 2;
    } else if (fields.count("valid_superblocks") == 0 || fields.at("valid_superblocks") != "2") {
        reason = "superblock_degraded";
        status = 2;
    } else if (fields.count("truncated_tail") != 0 && fields.at("truncated_tail") == "1") {
        reason = "truncated_tail";
        status = 2;
    }
    return {status, inspection.output_line + " verify_reason=" + reason, reason};
}

int rewrite_format_analysis(const std::string& db_path) {
    try {
        KVStore store(db_path);
        store.Compact();
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "rewrite-format failed: " << error.what() << std::endl;
        return 1;
    }
}
