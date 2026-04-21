#include "tests/common/benchmark_entrypoints.h"
#include "tests/common/cli_entrypoints.h"
#include "tests/common/runtime_entrypoints.h"
#include "tests/integration/test_registry.h"

#include <iostream>
#include <stdexcept>
#include <string>
int main(int argc, char* argv[]) {
    set_runtime_program_path_entrypoint(argv[0]);
    if (argc > 1) {
        const std::string command = argv[1];
        if (command == "bench") {
            run_benchmark_command();
            return 0;
        }
        if (command == "microbench") {
            run_microbench_command();
            return 0;
        }
        if (command == "microbench-json") {
            run_microbench_json_command();
            return 0;
        }
        if (command == "bench-json") {
            return run_benchmark_json_entrypoint();
        }
        if (command == "bench-baseline-json") {
            run_benchmark_baseline_json_command();
            return 0;
        }
        if (command == "compare-microbench") {
            if (argc < 4 || argc > 8) {
                std::cerr << "Usage: kv_test compare-microbench <baseline_json> <candidate_json> [min_ops_ratio_pct min_compaction_ratio_pct min_rewrite_ratio_pct min_recovery_ratio_pct]" << std::endl;
                return 1;
            }
            const double min_ops_ratio_pct = argc > 4 ? std::stod(argv[4]) : 80.0;
            const double min_compaction_ratio_pct = argc > 5 ? std::stod(argv[5]) : 75.0;
            const double min_rewrite_ratio_pct = argc > 6 ? std::stod(argv[6]) : 75.0;
            const double min_recovery_ratio_pct = argc > 7 ? std::stod(argv[7]) : 80.0;
            return run_compare_microbench_entrypoint(
                argv[2],
                argv[3],
                min_ops_ratio_pct,
                min_compaction_ratio_pct,
                min_rewrite_ratio_pct,
                min_recovery_ratio_pct);
        }
        if (command == "compare-baseline") {
            if (argc < 4 || argc > 7) {
                std::cerr << "Usage: kv_test compare-baseline <baseline_json> <candidate_json> [min_write_ratio_pct min_read_ratio_pct max_latency_ratio_pct]" << std::endl;
                return 1;
            }
            const double min_write_ratio_pct = argc > 4 ? std::stod(argv[4]) : 85.0;
            const double min_read_ratio_pct = argc > 5 ? std::stod(argv[5]) : 85.0;
            const double max_latency_ratio_pct = argc > 6 ? std::stod(argv[6]) : 125.0;
            return run_compare_benchmark_baseline_entrypoint(
                argv[2],
                argv[3],
                min_write_ratio_pct,
                min_read_ratio_pct,
                max_latency_ratio_pct);
        }
        if (command == "trend-baselines") {
            if (argc != 3 && argc != 4) {
                std::cerr << "Usage: kv_test trend-baselines <baseline_dir> [recent_window_count]" << std::endl;
                return 1;
            }
            const size_t recent_window = argc == 4 ? static_cast<size_t>(std::stoul(argv[3])) : 5;
            return run_benchmark_trend_entrypoint(argv[2], recent_window);
        }
        if (command == "trend-baselines-json") {
            if (argc != 3 && argc != 4) {
                std::cerr << "Usage: kv_test trend-baselines-json <baseline_dir> [recent_window_count]" << std::endl;
                return 1;
            }
            const size_t recent_window = argc == 4 ? static_cast<size_t>(std::stoul(argv[3])) : 5;
            return run_benchmark_trend_json_entrypoint(argv[2], recent_window);
        }
        if (command == "trend-microbench") {
            if (argc != 3 && argc != 4) {
                std::cerr << "Usage: kv_test trend-microbench <baseline_dir> [recent_window_count]" << std::endl;
                return 1;
            }
            const size_t recent_window = argc == 4 ? static_cast<size_t>(std::stoul(argv[3])) : 5;
            return run_microbench_trend_entrypoint(argv[2], recent_window);
        }
        if (command == "trend-microbench-json") {
            if (argc != 3 && argc != 4) {
                std::cerr << "Usage: kv_test trend-microbench-json <baseline_dir> [recent_window_count]" << std::endl;
                return 1;
            }
            const size_t recent_window = argc == 4 ? static_cast<size_t>(std::stoul(argv[3])) : 5;
            return run_microbench_trend_json_entrypoint(argv[2], recent_window);
        }
        if (command == "profile-json") {
            if (argc != 3) {
                std::cerr << "Usage: kv_test profile-json <balanced|write-heavy|read-heavy|low-latency>" << std::endl;
                return 1;
            }
            return run_profile_json_entrypoint(argv[2]);
        }
        if (command == "soak") {
            const int duration_seconds = argc > 2 ? std::stoi(argv[2]) : 10;
            const std::string profile_name = argc > 3 ? argv[3] : "balanced";
            return run_soak_command_entrypoint(duration_seconds, profile_name);
        }
        if (command == "concurrency-stress") {
            const int duration_seconds = argc > 2 ? std::stoi(argv[2]) : 10;
            const std::string profile_name = argc > 3 ? argv[3] : "balanced";
            return run_concurrency_stress_command_entrypoint(duration_seconds, profile_name);
        }
        if (command == "concurrency-stress-json") {
            const int duration_seconds = argc > 2 ? std::stoi(argv[2]) : 10;
            const std::string profile_name = argc > 3 ? argv[3] : "balanced";
            return run_concurrency_stress_json_entrypoint(duration_seconds, profile_name);
        }
        if (command == "inspect-format") {
            if (argc != 3) {
                std::cerr << "Usage: kv_test inspect-format <db_path>" << std::endl;
                return 1;
            }
            return run_inspect_format(argv[2]);
        }
        if (command == "rewrite-format") {
            if (argc != 3) {
                std::cerr << "Usage: kv_test rewrite-format <db_path>" << std::endl;
                return 1;
            }
            return run_rewrite_format(argv[2]);
        }
        if (command == "verify-format") {
            if (argc != 3) {
                std::cerr << "Usage: kv_test verify-format <db_path>" << std::endl;
                return 1;
            }
            return run_verify_format(argv[2]);
        }
        if (command == "compat-matrix") {
            return run_compatibility_matrix();
        }
        if (command == "fault-inject") {
            if (argc != 4) {
                std::cerr << "Usage: kv_test fault-inject <scenario> <db_path>" << std::endl;
                return 1;
            }
            return run_fault_injection_scenario_entrypoint(argv[2], argv[3]);
        }
        std::cerr << "Unknown command: " << command << '\n';
        std::cerr << "Usage: kv_test [bench|microbench|microbench-json|bench-json|bench-baseline-json|compare-microbench|compare-baseline|trend-baselines|trend-baselines-json|trend-microbench|trend-microbench-json|profile-json|soak|concurrency-stress|concurrency-stress-json|inspect-format|rewrite-format|verify-format|compat-matrix|fault-inject]" << std::endl;
        return 1;
    }

    kvstore::tests::integration::TestCases tests;
    kvstore::tests::integration::register_basic_kv_tests(tests);
    kvstore::tests::integration::register_benchmark_trend_tests(tests);
    kvstore::tests::integration::register_durability_smoke_tests(tests);
    kvstore::tests::integration::register_json_cli_tests(tests);
    kvstore::tests::integration::register_metrics_controller_tests(tests);
    kvstore::tests::integration::register_recovery_format_tests(tests);
    kvstore::tests::integration::register_runtime_concurrency_tests(tests);

    size_t passed = 0;
    for (const auto& [name, test] : tests) {
        try {
            test();
            ++passed;
            std::cout << "[PASS] " << name << '\n';
        } catch (const std::exception& ex) {
            std::cerr << "[FAIL] " << name << ": " << ex.what() << '\n';
            return 1;
        }
    }

    std::cout << "All " << passed << " tests passed." << std::endl;
    return 0;
}
