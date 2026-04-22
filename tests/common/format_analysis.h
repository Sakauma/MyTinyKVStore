#ifndef KVSTORE_TESTS_COMMON_FORMAT_ANALYSIS_H
#define KVSTORE_TESTS_COMMON_FORMAT_ANALYSIS_H

#include "kvstore.h"

#include <cstdint>
#include <map>
#include <string>
#include <vector>

struct FormatInspectResult {
    int status = 0;
    std::string output_line;
};

struct FormatVerifyResult {
    int status = 0;
    std::string output_line;
    std::string verify_reason;
};

std::map<std::string, std::string> parse_kv_line(const std::string& line);

FormatInspectResult inspect_format_analysis(const std::string& db_path);
FormatVerifyResult verify_format_analysis(const std::string& db_path);
int rewrite_format_analysis(const std::string& db_path);
int compatibility_matrix_analysis();

void write_legacy_snapshot_v1(const std::string& path, const std::vector<std::pair<int32_t, Value>>& entries);
void append_legacy_wal_record_v1(
    const std::string& path,
    uint8_t type,
    int32_t key,
    const Value& value);

#endif  // KVSTORE_TESTS_COMMON_FORMAT_ANALYSIS_H
