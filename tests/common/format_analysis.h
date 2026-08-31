#ifndef KVSTORE_TESTS_COMMON_FORMAT_ANALYSIS_H
#define KVSTORE_TESTS_COMMON_FORMAT_ANALYSIS_H

#include <map>
#include <string>

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

#endif  // KVSTORE_TESTS_COMMON_FORMAT_ANALYSIS_H
