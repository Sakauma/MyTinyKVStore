#include "tests/common/qualification_soak.h"

#include "kvstore.h"
#include "tests/common/test_support.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
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
using test_support::ThreadFailureCollector;

Value qualification_value(size_t size, uint64_t key) {
    std::vector<uint8_t> bytes(size);
    for (size_t index = 0; index < bytes.size(); ++index) {
        bytes[index] = static_cast<uint8_t>((key * 11400714819323198485ULL + index * 29ULL) & 0xFFU);
    }
    return Value(std::move(bytes));
}

uint64_t resident_set_kib() {
    std::ifstream status("/proc/self/status");
    std::string name;
    while (status >> name) {
        if (name == "VmRSS:") {
            uint64_t value = 0;
            status >> value;
            return value;
        }
        std::string ignored;
        std::getline(status, ignored);
    }
    return 0;
}

uint64_t open_fd_count() {
    uint64_t count = 0;
    std::error_code error;
    for (std::filesystem::directory_iterator iterator("/proc/self/fd", error), end;
         !error && iterator != end;
         iterator.increment(error)) {
        ++count;
    }
    return error ? 0 : count;
}

}  // namespace

int run_qualification_soak_json_entrypoint(const std::string& db_path,
                                           uint64_t minimum_duration_seconds,
                                           uint64_t required_unique_keys,
                                           size_t writer_count,
                                           size_t value_bytes) {
    require(minimum_duration_seconds > 0, "qualification duration must be positive");
    require(required_unique_keys > 0 &&
                required_unique_keys <= static_cast<uint64_t>(std::numeric_limits<int>::max()),
            "qualification unique-key count must fit the integer-key API");
    require(writer_count > 0 && writer_count <= 64, "qualification writer count must be in [1, 64]");
    require(value_bytes > 0, "qualification value size must be positive");
    require(!std::filesystem::exists(db_path), "qualification database path must not already exist");

    const uint64_t initial_rss_kib = resident_set_kib();
    const uint64_t initial_fd_count = open_fd_count();
    uint64_t peak_rss_kib = initial_rss_kib;
    uint64_t peak_fd_count = initial_fd_count;
    uint64_t stability_start_rss_kib = 0;
    uint64_t stability_end_rss_kib = 0;
    uint64_t stability_start_fd_count = 0;
    uint64_t stability_end_fd_count = 0;
    uint64_t unique_committed = 0;
    uint64_t total_committed = 0;
    uint64_t actual_duration_seconds = 0;
    KVStoreMetrics final_metrics;
    KVStoreOptions qualification_options;
    qualification_options.durability = DurabilityMode::kSync;
    qualification_options.max_batch_size = 128;
    qualification_options.max_batch_wal_bytes = 8ULL * 1024ULL * 1024ULL;
    qualification_options.max_batch_delay_us = 2000;
    qualification_options.auto_compact_wal_bytes_threshold = 256ULL * 1024ULL * 1024ULL;
    qualification_options.auto_compact_invalid_wal_ratio_percent = 0;
    qualification_options.adaptive_batching_enabled = false;
    qualification_options.adaptive_flush_enabled = false;
    qualification_options.adaptive_objective_enabled = false;
    const auto workload_start = Clock::now();

    {
        KVStore store(db_path, qualification_options);

        std::atomic<bool> stop {false};
        std::atomic<uint64_t> next_unique_key {0};
        std::atomic<uint64_t> committed_unique_keys {0};
        std::atomic<uint64_t> committed_writes {0};
        ThreadFailureCollector thread_failures;
        std::vector<std::thread> writers;
        writers.reserve(writer_count);
        for (size_t writer_id = 0; writer_id < writer_count; ++writer_id) {
            writers.emplace_back(thread_failures.guard([&, writer_id]() {
                std::mt19937_64 random(0x5A17ULL + writer_id);
                while (!stop.load(std::memory_order_acquire)) {
                    const uint64_t candidate = next_unique_key.fetch_add(1, std::memory_order_relaxed);
                    uint64_t key = candidate;
                    bool is_unique = candidate < required_unique_keys;
                    if (!is_unique) {
                        key = random() % required_unique_keys;
                    }
                    store.Put(static_cast<int>(key), qualification_value(value_bytes, key));
                    committed_writes.fetch_add(1, std::memory_order_relaxed);
                    if (is_unique) {
                        committed_unique_keys.fetch_add(1, std::memory_order_release);
                    }
                }
            }));
        }

        const auto deadline = workload_start + std::chrono::seconds(minimum_duration_seconds);
        const uint64_t stability_window_seconds = std::min<uint64_t>(
            300,
            std::max<uint64_t>(1, minimum_duration_seconds / 10));
        bool stability_started = false;
        Clock::time_point stability_deadline;
        while (!thread_failures.has_failure()) {
            const auto now = Clock::now();
            const uint64_t rss_kib = resident_set_kib();
            const uint64_t fd_count = open_fd_count();
            peak_rss_kib = std::max(peak_rss_kib, rss_kib);
            peak_fd_count = std::max(peak_fd_count, fd_count);
            if (!stability_started && now >= deadline &&
                committed_unique_keys.load(std::memory_order_acquire) >= required_unique_keys) {
                stability_started = true;
                stability_start_rss_kib = rss_kib;
                stability_start_fd_count = fd_count;
                stability_deadline = now + std::chrono::seconds(stability_window_seconds);
            }
            if (stability_started && now >= stability_deadline) {
                stability_end_rss_kib = rss_kib;
                stability_end_fd_count = fd_count;
                break;
            }
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
        stop.store(true, std::memory_order_release);
        for (auto& writer : writers) {
            writer.join();
        }
        thread_failures.rethrow_first();
        unique_committed = committed_unique_keys.load(std::memory_order_acquire);
        total_committed = committed_writes.load(std::memory_order_acquire);
        actual_duration_seconds = static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::seconds>(Clock::now() - workload_start).count());
        store.Compact();
        store.Flush();
        final_metrics = store.GetMetrics();
        peak_rss_kib = std::max(peak_rss_kib, resident_set_kib());
        peak_fd_count = std::max(peak_fd_count, open_fd_count());
    }

    const auto recovery_start = Clock::now();
    uint64_t verified_keys = 0;
    {
        KVStoreOptions options;
        options.value_cache_bytes = 0;
        KVStore reopened(db_path, options);
        for (uint64_t key = 0; key < required_unique_keys; ++key) {
            const auto value = reopened.Get(static_cast<int>(key));
            if (!value.has_value() || !(*value == qualification_value(value_bytes, key))) {
                throw std::runtime_error("qualification restart verification failed at key " +
                                         std::to_string(key));
            }
            ++verified_keys;
        }
    }
    const uint64_t recovery_and_verification_us = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() - recovery_start).count());
    const uint64_t final_rss_kib = resident_set_kib();
    const uint64_t final_fd_count = open_fd_count();
    const uint64_t rss_stability_allowance_kib = std::max<uint64_t>(
        64ULL * 1024ULL,
        stability_start_rss_kib / 10);
    const bool rss_stable = stability_start_rss_kib > 0 &&
                            stability_end_rss_kib <=
                                stability_start_rss_kib + rss_stability_allowance_kib;
    const bool fd_stable = stability_start_fd_count > 0 &&
                           stability_end_fd_count <= stability_start_fd_count + 2;
    const bool pass = actual_duration_seconds >= minimum_duration_seconds &&
                      unique_committed >= required_unique_keys &&
                      verified_keys == required_unique_keys &&
                      final_metrics.manual_compactions_completed > 0 &&
                      rss_stable && fd_stable &&
                      final_fd_count <= initial_fd_count + 2;

    std::ostringstream out;
    out << '{'
        << "\"status\":\"" << (pass ? "pass" : "fail") << "\""
        << ",\"database_path\":\"" << db_path << "\""
        << ",\"minimum_duration_seconds\":" << minimum_duration_seconds
        << ",\"actual_duration_seconds\":" << actual_duration_seconds
        << ",\"required_unique_keys\":" << required_unique_keys
        << ",\"unique_keys_committed\":" << unique_committed
        << ",\"total_writes_committed\":" << total_committed
        << ",\"verified_keys_after_restart\":" << verified_keys
        << ",\"writer_count\":" << writer_count
        << ",\"value_bytes\":" << value_bytes
        << ",\"options\":" << OptionsToJson(qualification_options)
        << ",\"recovery_and_verification_us\":" << recovery_and_verification_us
        << ",\"manual_compactions_completed\":" << final_metrics.manual_compactions_completed
        << ",\"auto_compactions_completed\":" << final_metrics.auto_compactions_completed
        << ",\"initial_rss_kib\":" << initial_rss_kib
        << ",\"peak_rss_kib\":" << peak_rss_kib
        << ",\"final_rss_kib\":" << final_rss_kib
        << ",\"stability_start_rss_kib\":" << stability_start_rss_kib
        << ",\"stability_end_rss_kib\":" << stability_end_rss_kib
        << ",\"rss_stability_allowance_kib\":" << rss_stability_allowance_kib
        << ",\"rss_stable\":" << (rss_stable ? "true" : "false")
        << ",\"initial_fd_count\":" << initial_fd_count
        << ",\"peak_fd_count\":" << peak_fd_count
        << ",\"final_fd_count\":" << final_fd_count
        << ",\"stability_start_fd_count\":" << stability_start_fd_count
        << ",\"stability_end_fd_count\":" << stability_end_fd_count
        << ",\"fd_stable\":" << (fd_stable ? "true" : "false")
        << ",\"metrics\":" << MetricsToJson(final_metrics)
        << '}';
    std::cout << out.str() << std::endl;
    return pass ? 0 : 2;
}
