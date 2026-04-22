#include "tests/common/benchmark_analysis.h"

#include "tests/common/test_support.h"

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <map>
#include <sstream>
#include <string>
#include <vector>

namespace {

using test_support::require;

struct BaselineSummary {
    std::string file_name;
    double write_ops_per_s = 0.0;
    double read_ops_per_s = 0.0;
    double avg_write_latency_us = 0.0;
};

std::string read_text_file(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    require(in.is_open(), "Expected text file to be readable");
    std::ostringstream out;
    out << in.rdbuf();
    return out.str();
}

double extract_json_number(const std::string& json, const std::string& key) {
    const std::string needle = "\"" + key + "\":";
    const size_t pos = json.find(needle);
    require(pos != std::string::npos, "Expected numeric key in json: " + key);
    size_t cursor = pos + needle.size();
    while (cursor < json.size() && std::isspace(static_cast<unsigned char>(json[cursor]))) {
        ++cursor;
    }
    size_t end = cursor;
    while (end < json.size()) {
        const char ch = json[end];
        if ((ch >= '0' && ch <= '9') || ch == '-' || ch == '+' || ch == '.' || ch == 'e' || ch == 'E') {
            ++end;
            continue;
        }
        break;
    }
    require(end > cursor, "Expected numeric value in json for key: " + key);
    return std::stod(json.substr(cursor, end - cursor));
}

std::vector<MicrobenchCaseResult> parse_microbench_results_json(const std::string& json) {
    std::vector<MicrobenchCaseResult> results;
    size_t search_from = 0;
    while (true) {
        const size_t name_pos = json.find("\"name\":\"", search_from);
        if (name_pos == std::string::npos) {
            break;
        }
        const size_t value_begin = name_pos + 8;
        const size_t value_end = json.find('"', value_begin);
        require(value_end != std::string::npos, "microbench json should contain a closing name quote");

        const size_t object_begin = json.rfind('{', name_pos);
        const size_t object_end = json.find('}', value_end);
        require(object_begin != std::string::npos && object_end != std::string::npos,
                "microbench json should contain complete case objects");

        const std::string object_json = json.substr(object_begin, object_end - object_begin + 1);
        MicrobenchCaseResult result;
        result.name = json.substr(value_begin, value_end - value_begin);
        result.duration_s = extract_json_number(object_json, "duration_s");
        result.ops_per_s = extract_json_number(object_json, "ops_per_s");
        result.operations = static_cast<uint64_t>(extract_json_number(object_json, "operations"));
        result.bytes = static_cast<uint64_t>(extract_json_number(object_json, "bytes"));
        results.push_back(result);
        search_from = object_end + 1;
    }
    return results;
}

std::string classify_throughput_trend(double ratio_pct) {
    if (ratio_pct >= 105.0) {
        return "improving";
    }
    if (ratio_pct <= 95.0) {
        return "regressing";
    }
    return "stable";
}

std::string classify_latency_trend(double ratio_pct) {
    if (ratio_pct <= 95.0) {
        return "improving";
    }
    if (ratio_pct >= 105.0) {
        return "regressing";
    }
    return "stable";
}

BaselineSummary load_baseline_summary(const std::filesystem::path& path) {
    const std::string json = read_text_file(path.string());
    BaselineSummary summary;
    summary.file_name = path.filename().string();
    summary.write_ops_per_s = extract_json_number(json, "write_ops_per_s");
    summary.read_ops_per_s = extract_json_number(json, "read_ops_per_s");
    summary.avg_write_latency_us = extract_json_number(json, "avg_write_latency_us");
    return summary;
}

}  // namespace

BenchmarkBaselineComparison compare_benchmark_baseline(
    const std::string& baseline_path,
    const std::string& candidate_path,
    double min_write_ratio_pct,
    double min_read_ratio_pct,
    double max_latency_ratio_pct,
    double max_p95_latency_ratio_pct,
    double max_p99_latency_ratio_pct,
    double max_fsync_pressure_ratio_pct,
    double min_batch_fill_ratio_pct) {
    const std::string baseline_json = read_text_file(baseline_path);
    const std::string candidate_json = read_text_file(candidate_path);

    const double baseline_write_ops_per_s = extract_json_number(baseline_json, "write_ops_per_s");
    const double candidate_write_ops_per_s = extract_json_number(candidate_json, "write_ops_per_s");
    const double baseline_read_ops_per_s = extract_json_number(baseline_json, "read_ops_per_s");
    const double candidate_read_ops_per_s = extract_json_number(candidate_json, "read_ops_per_s");
    const double baseline_avg_write_latency_us = extract_json_number(baseline_json, "avg_write_latency_us");
    const double candidate_avg_write_latency_us = extract_json_number(candidate_json, "avg_write_latency_us");
    const double baseline_p95_latency_us = extract_json_number(baseline_json, "approx_write_latency_p95_us");
    const double candidate_p95_latency_us = extract_json_number(candidate_json, "approx_write_latency_p95_us");
    const double baseline_p99_latency_us = extract_json_number(baseline_json, "approx_write_latency_p99_us");
    const double candidate_p99_latency_us = extract_json_number(candidate_json, "approx_write_latency_p99_us");
    const double baseline_fsync_pressure =
        extract_json_number(baseline_json, "observed_fsync_pressure_per_1000_writes");
    const double candidate_fsync_pressure =
        extract_json_number(candidate_json, "observed_fsync_pressure_per_1000_writes");
    const double baseline_batch_fill = extract_json_number(baseline_json, "recent_batch_fill_per_1000");
    const double candidate_batch_fill = extract_json_number(candidate_json, "recent_batch_fill_per_1000");

    require(baseline_write_ops_per_s > 0.0, "baseline write throughput must be positive");
    require(baseline_read_ops_per_s > 0.0, "baseline read throughput must be positive");
    require(baseline_avg_write_latency_us > 0.0, "baseline write latency must be positive");
    require(baseline_p95_latency_us > 0.0, "baseline p95 write latency must be positive");
    require(baseline_p99_latency_us > 0.0, "baseline p99 write latency must be positive");
    require(baseline_fsync_pressure > 0.0, "baseline fsync pressure must be positive");
    require(baseline_batch_fill > 0.0, "baseline batch fill must be positive");

    BenchmarkBaselineComparison result;
    result.write_ratio_pct = (candidate_write_ops_per_s / baseline_write_ops_per_s) * 100.0;
    result.read_ratio_pct = (candidate_read_ops_per_s / baseline_read_ops_per_s) * 100.0;
    result.latency_ratio_pct = (candidate_avg_write_latency_us / baseline_avg_write_latency_us) * 100.0;
    result.p95_latency_ratio_pct = (candidate_p95_latency_us / baseline_p95_latency_us) * 100.0;
    result.p99_latency_ratio_pct = (candidate_p99_latency_us / baseline_p99_latency_us) * 100.0;
    result.fsync_pressure_ratio_pct = (candidate_fsync_pressure / baseline_fsync_pressure) * 100.0;
    result.batch_fill_ratio_pct = (candidate_batch_fill / baseline_batch_fill) * 100.0;

    const bool write_ok = result.write_ratio_pct >= min_write_ratio_pct;
    const bool read_ok = result.read_ratio_pct >= min_read_ratio_pct;
    const bool latency_ok = result.latency_ratio_pct <= max_latency_ratio_pct;
    const bool p95_latency_ok = result.p95_latency_ratio_pct <= max_p95_latency_ratio_pct;
    const bool p99_latency_ok = result.p99_latency_ratio_pct <= max_p99_latency_ratio_pct;
    const bool fsync_pressure_ok = result.fsync_pressure_ratio_pct <= max_fsync_pressure_ratio_pct;
    const bool batch_fill_ok = result.batch_fill_ratio_pct >= min_batch_fill_ratio_pct;
    result.pass = write_ok && read_ok && latency_ok && p95_latency_ok && p99_latency_ok &&
                  fsync_pressure_ok && batch_fill_ok;
    return result;
}

std::vector<MicrobenchComparisonResult> compare_microbench(
    const std::string& baseline_path,
    const std::string& candidate_path,
    double min_ops_ratio_pct,
    double min_compaction_ratio_pct,
    double min_rewrite_ratio_pct,
    double min_recovery_ratio_pct,
    bool* pass) {
    const std::vector<MicrobenchCaseResult> baseline_cases =
        parse_microbench_results_json(read_text_file(baseline_path));
    const std::vector<MicrobenchCaseResult> candidate_cases =
        parse_microbench_results_json(read_text_file(candidate_path));
    require(!baseline_cases.empty(), "microbench baseline must contain at least one case");
    require(!candidate_cases.empty(), "microbench candidate must contain at least one case");

    std::map<std::string, MicrobenchCaseResult> baseline_by_name;
    std::map<std::string, MicrobenchCaseResult> candidate_by_name;
    for (const auto& result : baseline_cases) {
        baseline_by_name[result.name] = result;
    }
    for (const auto& result : candidate_cases) {
        candidate_by_name[result.name] = result;
    }

    bool comparison_pass = true;
    std::vector<MicrobenchComparisonResult> comparisons;
    for (const auto& [name, baseline] : baseline_by_name) {
        const auto candidate_it = candidate_by_name.find(name);
        require(candidate_it != candidate_by_name.end(), "microbench candidate is missing a baseline case");
        require(baseline.ops_per_s > 0.0, "microbench baseline ops_per_s must be positive");

        double min_ratio_for_case = min_ops_ratio_pct;
        if (name == "compaction") {
            min_ratio_for_case = min_compaction_ratio_pct;
        } else if (name == "rewrite") {
            min_ratio_for_case = min_rewrite_ratio_pct;
        } else if (name == "recovery") {
            min_ratio_for_case = min_recovery_ratio_pct;
        }

        const double ops_ratio_pct = (candidate_it->second.ops_per_s / baseline.ops_per_s) * 100.0;
        comparisons.push_back({name, ops_ratio_pct, min_ratio_for_case});
        if (ops_ratio_pct < min_ratio_for_case) {
            comparison_pass = false;
        }
    }

    std::sort(comparisons.begin(), comparisons.end(), [](const auto& lhs, const auto& rhs) {
        return lhs.name < rhs.name;
    });
    if (pass != nullptr) {
        *pass = comparison_pass;
    }
    return comparisons;
}

BenchmarkTrendSummary collect_benchmark_trend_summary(const std::string& directory_path, size_t recent_window) {
    std::vector<std::filesystem::path> files;
    for (const auto& entry : std::filesystem::directory_iterator(directory_path)) {
        if (!entry.is_regular_file()) {
            continue;
        }
        if (entry.path().extension() != ".json") {
            continue;
        }
        if (entry.path().filename().string().rfind("microbench-", 0) == 0) {
            continue;
        }
        files.push_back(entry.path());
    }

    std::sort(files.begin(), files.end());
    require(!files.empty(), "benchmark trend requires at least one baseline json file");
    require(recent_window > 0, "benchmark trend recent window must be positive");

    std::vector<BaselineSummary> summaries;
    summaries.reserve(files.size());
    for (const auto& path : files) {
        summaries.push_back(load_baseline_summary(path));
    }

    const BaselineSummary& first = summaries.front();
    const BaselineSummary& latest = summaries.back();
    const size_t recent_count = std::min(recent_window, summaries.size());
    double min_write_ops_per_s = first.write_ops_per_s;
    double max_write_ops_per_s = first.write_ops_per_s;
    double min_read_ops_per_s = first.read_ops_per_s;
    double max_read_ops_per_s = first.read_ops_per_s;
    double min_avg_write_latency_us = first.avg_write_latency_us;
    double max_avg_write_latency_us = first.avg_write_latency_us;
    double sum_write_ops_per_s = 0.0;
    double sum_read_ops_per_s = 0.0;
    double sum_avg_write_latency_us = 0.0;

    for (const auto& summary : summaries) {
        min_write_ops_per_s = std::min(min_write_ops_per_s, summary.write_ops_per_s);
        max_write_ops_per_s = std::max(max_write_ops_per_s, summary.write_ops_per_s);
        min_read_ops_per_s = std::min(min_read_ops_per_s, summary.read_ops_per_s);
        max_read_ops_per_s = std::max(max_read_ops_per_s, summary.read_ops_per_s);
        min_avg_write_latency_us = std::min(min_avg_write_latency_us, summary.avg_write_latency_us);
        max_avg_write_latency_us = std::max(max_avg_write_latency_us, summary.avg_write_latency_us);
        sum_write_ops_per_s += summary.write_ops_per_s;
        sum_read_ops_per_s += summary.read_ops_per_s;
        sum_avg_write_latency_us += summary.avg_write_latency_us;
    }

    double recent_avg_write_ops_per_s = 0.0;
    double recent_avg_read_ops_per_s = 0.0;
    double recent_avg_write_latency_us = 0.0;
    for (size_t i = summaries.size() - recent_count; i < summaries.size(); ++i) {
        recent_avg_write_ops_per_s += summaries[i].write_ops_per_s;
        recent_avg_read_ops_per_s += summaries[i].read_ops_per_s;
        recent_avg_write_latency_us += summaries[i].avg_write_latency_us;
    }
    recent_avg_write_ops_per_s /= recent_count;
    recent_avg_read_ops_per_s /= recent_count;
    recent_avg_write_latency_us /= recent_count;

    const double avg_write_ops_per_s = sum_write_ops_per_s / summaries.size();
    const double avg_read_ops_per_s = sum_read_ops_per_s / summaries.size();
    const double avg_avg_write_latency_us = sum_avg_write_latency_us / summaries.size();
    const double latest_vs_first_write_ratio_pct =
        first.write_ops_per_s == 0.0 ? 0.0 : (latest.write_ops_per_s / first.write_ops_per_s) * 100.0;
    const double latest_vs_first_read_ratio_pct =
        first.read_ops_per_s == 0.0 ? 0.0 : (latest.read_ops_per_s / first.read_ops_per_s) * 100.0;
    const double latest_vs_first_latency_ratio_pct =
        first.avg_write_latency_us == 0.0 ? 0.0 : (latest.avg_write_latency_us / first.avg_write_latency_us) * 100.0;
    const double latest_vs_recent_avg_write_ratio_pct =
        recent_avg_write_ops_per_s == 0.0 ? 0.0 : (latest.write_ops_per_s / recent_avg_write_ops_per_s) * 100.0;
    const double latest_vs_recent_avg_read_ratio_pct =
        recent_avg_read_ops_per_s == 0.0 ? 0.0 : (latest.read_ops_per_s / recent_avg_read_ops_per_s) * 100.0;
    const double latest_vs_recent_avg_latency_ratio_pct =
        recent_avg_write_latency_us == 0.0 ? 0.0 : (latest.avg_write_latency_us / recent_avg_write_latency_us) * 100.0;

    BenchmarkTrendSummary trend;
    trend.count = summaries.size();
    trend.oldest_file = first.file_name;
    trend.latest_file = latest.file_name;
    trend.recent_window_count = recent_count;
    trend.avg_write_ops_per_s = avg_write_ops_per_s;
    trend.min_write_ops_per_s = min_write_ops_per_s;
    trend.max_write_ops_per_s = max_write_ops_per_s;
    trend.recent_avg_write_ops_per_s = recent_avg_write_ops_per_s;
    trend.avg_read_ops_per_s = avg_read_ops_per_s;
    trend.min_read_ops_per_s = min_read_ops_per_s;
    trend.max_read_ops_per_s = max_read_ops_per_s;
    trend.recent_avg_read_ops_per_s = recent_avg_read_ops_per_s;
    trend.avg_write_latency_us = avg_avg_write_latency_us;
    trend.min_write_latency_us = min_avg_write_latency_us;
    trend.max_write_latency_us = max_avg_write_latency_us;
    trend.recent_avg_write_latency_us = recent_avg_write_latency_us;
    trend.latest_vs_oldest_write_ratio_pct = latest_vs_first_write_ratio_pct;
    trend.write_trend = classify_throughput_trend(latest_vs_first_write_ratio_pct);
    trend.latest_vs_recent_avg_write_ratio_pct = latest_vs_recent_avg_write_ratio_pct;
    trend.recent_write_trend = classify_throughput_trend(latest_vs_recent_avg_write_ratio_pct);
    trend.latest_vs_oldest_read_ratio_pct = latest_vs_first_read_ratio_pct;
    trend.read_trend = classify_throughput_trend(latest_vs_first_read_ratio_pct);
    trend.latest_vs_recent_avg_read_ratio_pct = latest_vs_recent_avg_read_ratio_pct;
    trend.recent_read_trend = classify_throughput_trend(latest_vs_recent_avg_read_ratio_pct);
    trend.latest_vs_oldest_latency_ratio_pct = latest_vs_first_latency_ratio_pct;
    trend.latency_trend = classify_latency_trend(latest_vs_first_latency_ratio_pct);
    trend.latest_vs_recent_avg_latency_ratio_pct = latest_vs_recent_avg_latency_ratio_pct;
    trend.recent_latency_trend = classify_latency_trend(latest_vs_recent_avg_latency_ratio_pct);
    return trend;
}

std::string benchmark_trend_summary_to_json(const BenchmarkTrendSummary& summary) {
    std::ostringstream out;
    out << '{'
        << "\"count\":" << summary.count
        << ",\"oldest_file\":\"" << summary.oldest_file << "\""
        << ",\"latest_file\":\"" << summary.latest_file << "\""
        << ",\"recent_window_count\":" << summary.recent_window_count
        << ",\"avg_write_ops_per_s\":" << summary.avg_write_ops_per_s
        << ",\"min_write_ops_per_s\":" << summary.min_write_ops_per_s
        << ",\"max_write_ops_per_s\":" << summary.max_write_ops_per_s
        << ",\"recent_avg_write_ops_per_s\":" << summary.recent_avg_write_ops_per_s
        << ",\"avg_read_ops_per_s\":" << summary.avg_read_ops_per_s
        << ",\"min_read_ops_per_s\":" << summary.min_read_ops_per_s
        << ",\"max_read_ops_per_s\":" << summary.max_read_ops_per_s
        << ",\"recent_avg_read_ops_per_s\":" << summary.recent_avg_read_ops_per_s
        << ",\"avg_write_latency_us\":" << summary.avg_write_latency_us
        << ",\"min_write_latency_us\":" << summary.min_write_latency_us
        << ",\"max_write_latency_us\":" << summary.max_write_latency_us
        << ",\"recent_avg_write_latency_us\":" << summary.recent_avg_write_latency_us
        << ",\"latest_vs_oldest_write_ratio_pct\":" << summary.latest_vs_oldest_write_ratio_pct
        << ",\"write_trend\":\"" << summary.write_trend << "\""
        << ",\"latest_vs_recent_avg_write_ratio_pct\":" << summary.latest_vs_recent_avg_write_ratio_pct
        << ",\"recent_write_trend\":\"" << summary.recent_write_trend << "\""
        << ",\"latest_vs_oldest_read_ratio_pct\":" << summary.latest_vs_oldest_read_ratio_pct
        << ",\"read_trend\":\"" << summary.read_trend << "\""
        << ",\"latest_vs_recent_avg_read_ratio_pct\":" << summary.latest_vs_recent_avg_read_ratio_pct
        << ",\"recent_read_trend\":\"" << summary.recent_read_trend << "\""
        << ",\"latest_vs_oldest_latency_ratio_pct\":" << summary.latest_vs_oldest_latency_ratio_pct
        << ",\"latency_trend\":\"" << summary.latency_trend << "\""
        << ",\"latest_vs_recent_avg_latency_ratio_pct\":" << summary.latest_vs_recent_avg_latency_ratio_pct
        << ",\"recent_latency_trend\":\"" << summary.recent_latency_trend << "\""
        << '}';
    return out.str();
}

std::vector<MicrobenchTrendCaseSummary> collect_microbench_trend_summary(
    const std::string& directory_path,
    size_t recent_window) {
    std::vector<std::filesystem::path> files;
    for (const auto& entry : std::filesystem::directory_iterator(directory_path)) {
        if (!entry.is_regular_file()) {
            continue;
        }
        if (entry.path().extension() != ".json") {
            continue;
        }
        if (entry.path().filename().string().rfind("microbench-", 0) != 0) {
            continue;
        }
        files.push_back(entry.path());
    }

    std::sort(files.begin(), files.end());
    require(!files.empty(), "microbench trend requires at least one microbench json file");
    require(recent_window > 0, "microbench trend recent window must be positive");

    std::map<std::string, std::vector<double>> history_by_case;
    for (const auto& path : files) {
        for (const auto& result : parse_microbench_results_json(read_text_file(path.string()))) {
            history_by_case[result.name].push_back(result.ops_per_s);
        }
    }

    std::vector<MicrobenchTrendCaseSummary> summaries;
    for (const auto& [name, history] : history_by_case) {
        require(!history.empty(), "microbench trend history must not be empty");
        const size_t recent_count = std::min(recent_window, history.size());
        double min_ops_per_s = history.front();
        double max_ops_per_s = history.front();
        double sum_ops_per_s = 0.0;
        for (double value : history) {
            min_ops_per_s = std::min(min_ops_per_s, value);
            max_ops_per_s = std::max(max_ops_per_s, value);
            sum_ops_per_s += value;
        }
        double recent_avg_ops_per_s = 0.0;
        for (size_t i = history.size() - recent_count; i < history.size(); ++i) {
            recent_avg_ops_per_s += history[i];
        }
        recent_avg_ops_per_s /= recent_count;

        const double avg_ops_per_s = sum_ops_per_s / history.size();
        const double latest_vs_oldest_ratio_pct =
            history.front() == 0.0 ? 0.0 : (history.back() / history.front()) * 100.0;
        const double latest_vs_recent_avg_ratio_pct =
            recent_avg_ops_per_s == 0.0 ? 0.0 : (history.back() / recent_avg_ops_per_s) * 100.0;

        summaries.push_back({
            name,
            avg_ops_per_s,
            min_ops_per_s,
            max_ops_per_s,
            recent_avg_ops_per_s,
            latest_vs_oldest_ratio_pct,
            latest_vs_recent_avg_ratio_pct,
            classify_throughput_trend(latest_vs_oldest_ratio_pct),
            classify_throughput_trend(latest_vs_recent_avg_ratio_pct),
        });
    }

    std::sort(summaries.begin(), summaries.end(), [](const auto& lhs, const auto& rhs) {
        return lhs.name < rhs.name;
    });
    return summaries;
}

std::string microbench_trend_summary_to_json(const std::vector<MicrobenchTrendCaseSummary>& summaries) {
    std::ostringstream out;
    out << "{\"cases\":[";
    for (size_t i = 0; i < summaries.size(); ++i) {
        if (i != 0) {
            out << ',';
        }
        out << '{'
            << "\"name\":\"" << summaries[i].name << "\""
            << ",\"avg_ops_per_s\":" << summaries[i].avg_ops_per_s
            << ",\"min_ops_per_s\":" << summaries[i].min_ops_per_s
            << ",\"max_ops_per_s\":" << summaries[i].max_ops_per_s
            << ",\"recent_avg_ops_per_s\":" << summaries[i].recent_avg_ops_per_s
            << ",\"latest_vs_oldest_ratio_pct\":" << summaries[i].latest_vs_oldest_ratio_pct
            << ",\"latest_vs_recent_avg_ratio_pct\":" << summaries[i].latest_vs_recent_avg_ratio_pct
            << ",\"trend\":\"" << summaries[i].trend << "\""
            << ",\"recent_trend\":\"" << summaries[i].recent_trend << "\""
            << '}';
    }
    out << "]}";
    return out.str();
}
