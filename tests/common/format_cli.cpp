#include "tests/common/cli_entrypoints.h"

#include "tests/common/format_analysis.h"

#include <iostream>
#include <string>

int run_inspect_format(const std::string& db_path) {
    const FormatInspectResult result = inspect_format_analysis(db_path);
    std::cout << result.output_line << std::endl;
    return result.status;
}

int run_rewrite_format(const std::string& db_path) {
    return rewrite_format_analysis(db_path);
}

int run_verify_format(const std::string& db_path) {
    const FormatVerifyResult result = verify_format_analysis(db_path);
    std::cout << result.output_line << std::endl;
    return result.status;
}

int run_compatibility_matrix() {
    return compatibility_matrix_analysis();
}
