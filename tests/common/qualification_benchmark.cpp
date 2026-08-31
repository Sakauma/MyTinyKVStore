#include "tests/common/qualification_benchmark.h"

#include "kvstore.h"
#include "tests/common/test_support.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <limits>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

namespace {

using Clock = std::chrono::steady_clock;
using test_support::require;
using test_support::TestDir;
using test_support::ThreadFailureCollector;

struct QualificationRound {
    double duration_s = 0.0;
    double operations_per_s = 0.0;
    double write_ops_per_s = 0.0;
    uint64_t put_operations = 0;
    uint64_t delete_operations = 0;
    uint64_t get_operations = 0;
    uint64_t write_p50_us = 0;
    uint64_t write_p95_us = 0;
    uint64_t write_p99_us = 0;
    KVStoreOptions options;
    KVStoreMetrics metrics;
};

Value qualification_value(size_t size, uint64_t salt) {
    std::vector<uint8_t> bytes(size);
    for (size_t index = 0; index < bytes.size(); ++index) {
        bytes[index] = static_cast<uint8_t>((salt * 1315423911ULL + index * 17ULL) & 0xFFU);
    }
    return Value(std::move(bytes));
}

uint64_t percentile(const std::vector<uint64_t>& sorted, uint64_t numerator) {
    if (sorted.empty()) {
        return 0;
    }
    const uint64_t rank = (sorted.size() * numerator + 99) / 100;
    return sorted[static_cast<size_t>(std::max<uint64_t>(1, rank) - 1)];
}

double median(std::vector<double> values) {
    std::sort(values.begin(), values.end());
    return values[values.size() / 2];
}

uint64_t median(std::vector<uint64_t> values) {
    std::sort(values.begin(), values.end());
    return values[values.size() / 2];
}

QualificationRound run_round(uint64_t prefill_keys,
                             uint64_t operations,
                             size_t writer_count,
                             size_t value_bytes,
                             const std::string& distribution,
                             bool compaction_enabled,
                             size_t round_index) {
    require(prefill_keys > 0 && prefill_keys <= static_cast<uint64_t>(std::numeric_limits<int>::max()),
            "qualification prefill key count must fit the integer-key API");
    require(operations > 0, "qualification operation count must be positive");
    require(writer_count > 0 && writer_count <= 256, "qualification writer count must be in [1, 256]");
    require(value_bytes > 0, "qualification value size must be positive");
    require(distribution == "uniform" || distribution == "hotspot",
            "qualification distribution must be uniform or hotspot");

    TestDir directory("qualification_benchmark_" + std::to_string(round_index));
    KVStoreOptions options;
    options.durability = DurabilityMode::kSync;
    options.max_batch_size = 64;
    options.max_batch_wal_bytes = 4ULL * 1024ULL * 1024ULL;
    options.max_batch_delay_us = 1000;
    options.adaptive_batching_enabled = false;
    options.adaptive_flush_enabled = false;
    options.adaptive_objective_enabled = false;
    if (compaction_enabled) {
        options.auto_compact_wal_bytes_threshold = 64ULL * 1024ULL * 1024ULL;
        options.auto_compact_invalid_wal_ratio_percent = 50;
    }
    KVStore store(directory.file("store.dat"), options);

    constexpr size_t kPrefillBatch = 1024;
    for (uint64_t begin = 0; begin < prefill_keys; begin += kPrefillBatch) {
        std::vector<BatchWriteOperation> batch;
        const uint64_t end = std::min<uint64_t>(prefill_keys, begin + kPrefillBatch);
        batch.reserve(static_cast<size_t>(end - begin));
        for (uint64_t key = begin; key < end; ++key) {
            batch.push_back(BatchWriteOperation::PutInt(
                static_cast<int>(key), qualification_value(value_bytes, key)));
        }
        store.WriteBatch(batch);
    }
    store.Flush();

    struct ThreadResult {
        uint64_t puts = 0;
        uint64_t deletes = 0;
        uint64_t gets = 0;
        std::vector<uint64_t> write_latencies_us;
    };
    std::vector<ThreadResult> thread_results(writer_count);
    std::vector<std::thread> threads;
    threads.reserve(writer_count);
    ThreadFailureCollector thread_failures;
    std::atomic<size_t> ready {0};
    std::atomic<bool> start {false};
    const uint64_t hot_key_count = std::max<uint64_t>(1, prefill_keys / 100);

    for (size_t writer_id = 0; writer_id < writer_count; ++writer_id) {
        threads.emplace_back(thread_failures.guard([&, writer_id]() {
            const uint64_t operation_begin = (operations * writer_id) / writer_count;
            const uint64_t operation_end = (operations * (writer_id + 1)) / writer_count;
            ThreadResult& result = thread_results[writer_id];
            result.write_latencies_us.reserve(
                static_cast<size_t>((operation_end - operation_begin) * 9 / 10 + 1));
            std::mt19937_64 random(0xC0FFEEULL + round_index * 4099ULL + writer_id);
            std::uniform_int_distribution<uint64_t> all_keys(0, prefill_keys - 1);
            std::uniform_int_distribution<uint64_t> hot_keys(0, hot_key_count - 1);
            std::uniform_int_distribution<int> hotspot_choice(0, 9);
            ready.fetch_add(1, std::memory_order_release);
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            for (uint64_t operation = operation_begin; operation < operation_end; ++operation) {
                const bool use_hot_key = distribution == "hotspot" && hotspot_choice(random) != 0;
                const int key = static_cast<int>(use_hot_key ? hot_keys(random) : all_keys(random));
                const uint64_t selector = operation % 10;
                if (selector < 8) {
                    const auto before = Clock::now();
                    store.Put(key, qualification_value(value_bytes, operation));
                    result.write_latencies_us.push_back(static_cast<uint64_t>(
                        std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() - before).count()));
                    ++result.puts;
                } else if (selector == 8) {
                    const auto before = Clock::now();
                    store.Delete(key);
                    result.write_latencies_us.push_back(static_cast<uint64_t>(
                        std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() - before).count()));
                    ++result.deletes;
                } else {
                    (void)store.Get(key);
                    ++result.gets;
                }
            }
        }));
    }

    while (ready.load(std::memory_order_acquire) != writer_count && !thread_failures.has_failure()) {
        std::this_thread::yield();
    }
    if (thread_failures.has_failure()) {
        start.store(true, std::memory_order_release);
        for (auto& thread : threads) {
            thread.join();
        }
        thread_failures.rethrow_first();
    }
    const auto benchmark_start = Clock::now();
    start.store(true, std::memory_order_release);
    for (auto& thread : threads) {
        thread.join();
    }
    const auto benchmark_end = Clock::now();
    thread_failures.rethrow_first();

    QualificationRound result;
    result.duration_s = std::chrono::duration<double>(benchmark_end - benchmark_start).count();
    std::vector<uint64_t> latencies;
    uint64_t latency_count = 0;
    for (const auto& thread_result : thread_results) {
        latency_count += thread_result.write_latencies_us.size();
    }
    latencies.reserve(static_cast<size_t>(latency_count));
    for (auto& thread_result : thread_results) {
        result.put_operations += thread_result.puts;
        result.delete_operations += thread_result.deletes;
        result.get_operations += thread_result.gets;
        latencies.insert(latencies.end(),
                         thread_result.write_latencies_us.begin(),
                         thread_result.write_latencies_us.end());
        std::vector<uint64_t>().swap(thread_result.write_latencies_us);
    }
    std::sort(latencies.begin(), latencies.end());
    result.write_p50_us = percentile(latencies, 50);
    result.write_p95_us = percentile(latencies, 95);
    result.write_p99_us = percentile(latencies, 99);
    result.operations_per_s = operations / result.duration_s;
    result.write_ops_per_s = (result.put_operations + result.delete_operations) / result.duration_s;
    result.options = options;
    result.metrics = store.GetMetrics();
    return result;
}

}  // namespace

int run_qualification_benchmark_json_entrypoint(uint64_t prefill_keys,
                                                uint64_t operations,
                                                size_t writer_count,
                                                size_t value_bytes,
                                                size_t rounds,
                                                const std::string& distribution,
                                                bool compaction_enabled) {
    require(rounds > 0 && rounds <= 9, "qualification round count must be in [1, 9]");
    std::vector<QualificationRound> results;
    results.reserve(rounds);
    for (size_t round = 0; round < rounds; ++round) {
        results.push_back(run_round(prefill_keys,
                                    operations,
                                    writer_count,
                                    value_bytes,
                                    distribution,
                                    compaction_enabled,
                                    round));
    }

    std::vector<double> operation_rates;
    std::vector<double> write_rates;
    std::vector<uint64_t> p50_values;
    std::vector<uint64_t> p95_values;
    std::vector<uint64_t> p99_values;
    for (const auto& result : results) {
        operation_rates.push_back(result.operations_per_s);
        write_rates.push_back(result.write_ops_per_s);
        p50_values.push_back(result.write_p50_us);
        p95_values.push_back(result.write_p95_us);
        p99_values.push_back(result.write_p99_us);
    }

    std::ostringstream out;
    out << std::setprecision(17)
        << '{'
        << "\"benchmark\":\"canonical-storage\""
        << ",\"prefill_keys\":" << prefill_keys
        << ",\"operations\":" << operations
        << ",\"writer_count\":" << writer_count
        << ",\"value_bytes\":" << value_bytes
        << ",\"round_count\":" << rounds
        << ",\"distribution\":\"" << distribution << "\""
        << ",\"durability\":\"sync\""
        << ",\"compaction_enabled\":" << (compaction_enabled ? "true" : "false")
        << ",\"options\":" << OptionsToJson(results.front().options)
        << ",\"put_ratio_percent\":80"
        << ",\"delete_ratio_percent\":10"
        << ",\"get_ratio_percent\":10"
        << ",\"rounds\":[";
    for (size_t index = 0; index < results.size(); ++index) {
        if (index != 0) {
            out << ',';
        }
        const auto& result = results[index];
        out << '{'
            << "\"duration_s\":" << result.duration_s
            << ",\"operations_per_s\":" << result.operations_per_s
            << ",\"write_ops_per_s\":" << result.write_ops_per_s
            << ",\"put_operations\":" << result.put_operations
            << ",\"delete_operations\":" << result.delete_operations
            << ",\"get_operations\":" << result.get_operations
            << ",\"write_p50_us\":" << result.write_p50_us
            << ",\"write_p95_us\":" << result.write_p95_us
            << ",\"write_p99_us\":" << result.write_p99_us
            << ",\"metrics\":" << MetricsToJson(result.metrics)
            << '}';
    }
    out << ']'
        << ",\"median_operations_per_s\":" << median(operation_rates)
        << ",\"median_write_ops_per_s\":" << median(write_rates)
        << ",\"median_write_p50_us\":" << median(p50_values)
        << ",\"median_write_p95_us\":" << median(p95_values)
        << ",\"median_write_p99_us\":" << median(p99_values)
        << '}';
    std::cout << out.str() << std::endl;
    return 0;
}
