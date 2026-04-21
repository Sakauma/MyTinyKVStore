#include "tests/integration/test_registry.h"

#include "kvstore.h"

#include <atomic>
#include <thread>
#include <vector>

namespace kvstore::tests::integration {
namespace {

using test_support::as_string;
using test_support::file_size_or_zero;
using test_support::require;
using test_support::TestDir;
using test_support::text;
using test_support::wait_for_start;
using test_support::wait_until;

uint64_t histogram_total(const std::array<uint64_t, kWriteLatencyBucketCount>& histogram) {
    uint64_t total = 0;
    for (uint64_t value : histogram) {
        total += value;
    }
    return total;
}

void test_batching_metrics_are_reported() {
    TestDir dir("batch_metrics");
    const std::string db_path = dir.file("store.dat");
    KVStoreOptions options;
    options.max_batch_size = 8;
    options.max_batch_delay_us = 20000;
    KVStore store(db_path, options);

    constexpr int kWriterCount = 8;
    std::atomic<int> ready {0};
    std::atomic<bool> start_signal {false};
    std::vector<std::thread> writers;

    for (int writer_id = 0; writer_id < kWriterCount; ++writer_id) {
        writers.emplace_back([&store, &ready, &start_signal, writer_id]() {
            wait_for_start(ready, start_signal, kWriterCount);
            store.Put(writer_id, text("value_" + std::to_string(writer_id)));
        });
    }

    while (ready.load(std::memory_order_acquire) < kWriterCount) {
        std::this_thread::yield();
    }
    start_signal.store(true, std::memory_order_release);

    for (auto& writer : writers) {
        writer.join();
    }

    const KVStoreMetrics metrics = store.GetMetrics();
    require(metrics.enqueued_write_requests == kWriterCount, "metrics should count all enqueued writes");
    require(metrics.committed_write_requests == kWriterCount, "metrics should count all committed writes");
    require(metrics.committed_write_batches >= 1, "writer should report at least one committed batch");
    require(metrics.max_committed_batch_size > 1, "batched writer should group concurrent writes");
    require(metrics.wal_fsync_calls < metrics.committed_write_requests, "batching should reduce fsync count below write count");
    require(metrics.pending_queue_depth == 0, "queue should drain after all writers finish");
    require(histogram_total(metrics.write_latency_histogram) == metrics.committed_write_requests,
            "latency histogram should account for every committed write");
}

void test_auto_compaction_triggers_and_preserves_state() {
    TestDir dir("auto_compaction");
    const std::string db_path = dir.file("store.dat");
    const std::string wal_path = db_path + ".wal";

    KVStoreOptions options;
    options.max_batch_size = 4;
    options.max_batch_delay_us = 5000;
    options.auto_compact_wal_bytes_threshold = 128;

    {
        KVStore store(db_path, options);
        for (int i = 0; i < 12; ++i) {
            store.Put(i, text("auto_value_" + std::to_string(i)));
        }

        wait_until(
            [&store, &options]() {
                const KVStoreMetrics metrics = store.GetMetrics();
                return metrics.auto_compactions_completed >= 1 &&
                       metrics.wal_bytes_since_compaction < options.auto_compact_wal_bytes_threshold;
            },
            "auto compaction did not settle within the expected time");

        const KVStoreMetrics metrics = store.GetMetrics();
        require(metrics.auto_compactions_completed >= 1, "auto compaction should trigger after WAL crosses the threshold");
        require(metrics.wal_bytes_since_compaction < options.auto_compact_wal_bytes_threshold,
                "auto compaction should reset accumulated WAL bytes");
        require(file_size_or_zero(wal_path) < options.auto_compact_wal_bytes_threshold,
                "WAL should be rotated down after auto compaction");
    }

    KVStore reopened(db_path);
    for (int i = 0; i < 12; ++i) {
        const auto value = reopened.Get(i);
        require(value.has_value(), "auto compaction must preserve committed values");
        require(as_string(*value) == "auto_value_" + std::to_string(i),
                "reopened state should match values written before auto compaction");
    }
}

void test_manual_and_auto_compaction_metrics_coexist() {
    TestDir dir("manual_auto_metrics");
    const std::string db_path = dir.file("store.dat");

    KVStoreOptions options;
    options.max_batch_size = 2;
    options.max_batch_delay_us = 0;
    options.auto_compact_wal_bytes_threshold = 96;

    KVStore store(db_path, options);
    for (int i = 0; i < 6; ++i) {
        store.Put(i, text("payload_" + std::to_string(i)));
    }
    store.Compact();

    const KVStoreMetrics metrics = store.GetMetrics();
    require(metrics.auto_compactions_completed >= 1, "auto compaction count should be visible in metrics");
    require(metrics.manual_compactions_completed == 1, "manual compaction count should be tracked separately");
    require(metrics.compact_requests == 1, "only explicit compaction requests should increment compact_requests");
}

void test_invalid_wal_ratio_triggers_auto_compaction() {
    TestDir dir("invalid_ratio_compaction");
    const std::string db_path = dir.file("store.dat");

    KVStoreOptions options;
    options.max_batch_size = 1;
    options.max_batch_delay_us = 0;
    options.auto_compact_invalid_wal_ratio_percent = 50;

    {
        KVStore store(db_path, options);
        for (int i = 0; i < 12; ++i) {
            store.Put(42, text("overwrite_" + std::to_string(i)));
        }

        wait_until(
            [&store]() {
                const KVStoreMetrics metrics = store.GetMetrics();
                return metrics.auto_compactions_completed >= 1 &&
                       metrics.obsolete_wal_bytes_since_compaction <= metrics.live_wal_bytes_since_compaction;
            },
            "invalid WAL ratio auto compaction did not settle within the expected time");

        const KVStoreMetrics metrics = store.GetMetrics();
        require(metrics.auto_compactions_completed >= 1, "invalid WAL ratio should trigger auto compaction");
    }

    KVStore reopened(db_path);
    const auto value = reopened.Get(42);
    require(value.has_value(), "latest overwritten value should survive invalid-ratio compaction");
    require(as_string(*value) == "overwrite_11", "reopened state should contain the latest overwritten value");
}

void test_wal_obsolete_byte_metrics_are_consistent() {
    TestDir dir("wal_byte_metrics");
    const std::string db_path = dir.file("store.dat");

    KVStore store(db_path);
    store.Put(1, text("first"));
    store.Put(1, text("second"));
    store.Delete(1);

    const KVStoreMetrics metrics = store.GetMetrics();
    require(metrics.wal_bytes_since_compaction ==
                metrics.live_wal_bytes_since_compaction + metrics.obsolete_wal_bytes_since_compaction,
            "live and obsolete WAL bytes should partition the accumulated WAL size");
    require(metrics.obsolete_wal_bytes_since_compaction > 0,
            "overwrites and deletes should create obsolete WAL bytes");
}

void test_batch_wal_byte_limit_is_respected() {
    TestDir dir("batch_byte_limit");
    const std::string db_path = dir.file("store.dat");

    KVStoreOptions options;
    options.max_batch_size = 64;
    options.max_batch_wal_bytes = 60;
    options.max_batch_delay_us = 20000;

    KVStore store(db_path, options);

    constexpr int kWriterCount = 4;
    std::atomic<int> ready {0};
    std::atomic<bool> start_signal {false};
    std::vector<std::thread> writers;

    for (int writer_id = 0; writer_id < kWriterCount; ++writer_id) {
        writers.emplace_back([&store, &ready, &start_signal, writer_id]() {
            wait_for_start(ready, start_signal, kWriterCount);
            store.Put(writer_id, text("payload_1234567890"));
        });
    }

    while (ready.load(std::memory_order_acquire) < kWriterCount) {
        std::this_thread::yield();
    }
    start_signal.store(true, std::memory_order_release);

    for (auto& writer : writers) {
        writer.join();
    }

    const KVStoreMetrics metrics = store.GetMetrics();
    require(metrics.committed_write_requests == kWriterCount, "all writes should commit under byte-limited batching");
    require(metrics.max_committed_batch_size <= 1, "small WAL byte limit should prevent multi-request batches");
}

void test_latency_histogram_tracks_write_requests() {
    TestDir dir("latency_histogram");
    const std::string db_path = dir.file("store.dat");
    KVStore store(db_path);

    for (int i = 0; i < 10; ++i) {
        store.Put(i, text("latency_" + std::to_string(i)));
    }
    store.Delete(5);

    const KVStoreMetrics metrics = store.GetMetrics();
    require(histogram_total(metrics.write_latency_histogram) == metrics.committed_write_requests,
            "histogram total should equal the number of committed writes");
    require(histogram_total(metrics.write_latency_histogram) == 11,
            "histogram should include puts and deletes but not compaction");
}

void test_adaptive_batching_expands_batch_under_queue_pressure() {
    TestDir dir("adaptive_batching");
    const std::string db_path = dir.file("store.dat");

    KVStoreOptions options;
    options.max_batch_size = 2;
    options.max_batch_delay_us = 20000;
    options.adaptive_batching_enabled = true;
    options.adaptive_queue_depth_threshold = 4;
    options.adaptive_batch_size_multiplier = 4;

    KVStore store(db_path, options);

    constexpr int kWriterCount = 8;
    std::atomic<int> ready {0};
    std::atomic<bool> start_signal {false};
    std::vector<std::thread> writers;

    for (int writer_id = 0; writer_id < kWriterCount; ++writer_id) {
        writers.emplace_back([&store, &ready, &start_signal, writer_id]() {
            wait_for_start(ready, start_signal, kWriterCount);
            store.Put(writer_id, text("adaptive_" + std::to_string(writer_id)));
        });
    }

    while (ready.load(std::memory_order_acquire) < kWriterCount) {
        std::this_thread::yield();
    }
    start_signal.store(true, std::memory_order_release);

    for (auto& writer : writers) {
        writer.join();
    }

    const KVStoreMetrics metrics = store.GetMetrics();
    require(metrics.committed_write_requests == kWriterCount, "adaptive batching should still commit all queued writes");
    require(metrics.max_committed_batch_size > options.max_batch_size,
            "adaptive batching should grow batch size beyond the base limit under queue pressure");
    require(metrics.adaptive_batches_completed >= 1, "adaptive batching should report at least one adaptive batch");
}

void test_percentile_and_writer_metrics_are_reported() {
    TestDir dir("writer_metrics");
    const std::string db_path = dir.file("store.dat");

    KVStoreOptions options;
    options.max_batch_size = 4;
    options.max_batch_delay_us = 5000;
    KVStore store(db_path, options);

    constexpr int kWriterCount = 6;
    std::atomic<int> ready {0};
    std::atomic<bool> start_signal {false};
    std::vector<std::thread> writers;

    for (int writer_id = 0; writer_id < kWriterCount; ++writer_id) {
        writers.emplace_back([&store, &ready, &start_signal, writer_id]() {
            wait_for_start(ready, start_signal, kWriterCount);
            store.Put(writer_id, text("writer_metric_payload_" + std::to_string(writer_id)));
        });
    }

    while (ready.load(std::memory_order_acquire) < kWriterCount) {
        std::this_thread::yield();
    }
    start_signal.store(true, std::memory_order_release);

    for (auto& writer : writers) {
        writer.join();
    }

    wait_until(
        [&store]() {
            const KVStoreMetrics metrics = store.GetMetrics();
            return metrics.writer_wait_events >= 1 && metrics.writer_wait_time_us > 0;
        },
        "writer thread did not report idle waiting after draining the queue");

    const KVStoreMetrics metrics = store.GetMetrics();
    require(metrics.max_pending_queue_depth >= 1, "queue high-water mark should be tracked");
    require(metrics.last_committed_batch_wal_bytes > 0, "last committed batch WAL bytes should be reported");
    require(metrics.max_committed_batch_wal_bytes >= metrics.last_committed_batch_wal_bytes,
            "max batch WAL bytes should be at least the last batch size");
    require(metrics.approx_write_latency_p50_us > 0, "p50 write latency should be reported");
    require(metrics.approx_write_latency_p95_us >= metrics.approx_write_latency_p50_us,
            "p95 write latency should not be below p50");
    require(metrics.approx_write_latency_p99_us >= metrics.approx_write_latency_p95_us,
            "p99 write latency should not be below p95");
}

void test_adaptive_flush_shortens_batch_delay() {
    TestDir dir("adaptive_flush");
    const std::string db_path = dir.file("store.dat");

    KVStoreOptions options;
    options.max_batch_size = 16;
    options.max_batch_delay_us = 50000;
    options.adaptive_flush_enabled = true;
    options.adaptive_flush_queue_depth_threshold = 2;
    options.adaptive_flush_delay_divisor = 10;
    options.adaptive_flush_min_batch_delay_us = 1000;

    KVStore store(db_path, options);

    std::atomic<int> ready {0};
    std::atomic<bool> start_signal {false};

    std::thread writer1([&store, &ready, &start_signal]() {
        wait_for_start(ready, start_signal, 2);
        store.Put(1, text("flush_1"));
    });
    std::thread writer2([&store, &ready, &start_signal]() {
        wait_for_start(ready, start_signal, 2);
        store.Put(2, text("flush_2"));
    });

    while (ready.load(std::memory_order_acquire) < 2) {
        std::this_thread::yield();
    }
    start_signal.store(true, std::memory_order_release);

    wait_until(
        [&store]() {
            const KVStoreMetrics metrics = store.GetMetrics();
            return metrics.committed_write_requests >= 2 &&
                   metrics.committed_write_batches >= 1 &&
                   metrics.adaptive_flush_batches_completed >= 1;
        },
        "adaptive flush should commit the first shortened-delay batch before the delayed write arrives");

    std::thread writer3([&store]() {
        store.Put(3, text("flush_3"));
    });

    writer1.join();
    writer2.join();
    writer3.join();

    const KVStoreMetrics metrics = store.GetMetrics();
    require(metrics.committed_write_requests == 3, "adaptive flush test should commit all writes");
    require(metrics.committed_write_batches >= 2, "adaptive flush should flush the early batch before the delayed write arrives");
    require(metrics.adaptive_flush_batches_completed >= 1, "adaptive flush should record at least one shortened-delay batch");
    require(metrics.min_effective_batch_delay_us > 0 &&
                metrics.min_effective_batch_delay_us < options.max_batch_delay_us,
            "adaptive flush should reduce the effective batch delay below the configured base delay");
}

void test_compaction_long_term_metrics_accumulate() {
    TestDir dir("compaction_totals");
    const std::string db_path = dir.file("store.dat");

    KVStore store(db_path);
    store.Put(1, text("first"));
    store.Compact();
    store.Put(2, text("second"));
    store.Delete(1);
    store.Compact();

    const KVStoreMetrics metrics = store.GetMetrics();
    require(metrics.manual_compactions_completed == 2, "manual compaction count should reflect both runs");
    require(metrics.total_snapshot_bytes_written > sizeof(uint32_t),
            "compaction should accumulate snapshot bytes written over time");
    require(metrics.total_wal_bytes_reclaimed_by_compaction > 0,
            "compaction should accumulate reclaimed WAL bytes over time");
}

void test_latency_target_adaptive_flush_kicks_in() {
    TestDir dir("latency_target_flush");
    const std::string db_path = dir.file("store.dat");

    KVStoreOptions options;
    options.max_batch_size = 16;
    options.max_batch_delay_us = 50000;
    options.adaptive_flush_enabled = true;
    options.adaptive_flush_queue_depth_threshold = 1000;
    options.adaptive_flush_delay_divisor = 10;
    options.adaptive_flush_min_batch_delay_us = 1000;
    options.adaptive_latency_target_p95_us = 50;

    KVStore store(db_path, options);
    for (int i = 0; i < 4; ++i) {
        store.Put(i, text("warmup_" + std::to_string(i)));
    }

    std::atomic<int> ready {0};
    std::atomic<bool> start_signal {false};
    std::thread writer1([&store, &ready, &start_signal]() {
        wait_for_start(ready, start_signal, 2);
        store.Put(10, text("latency_target_a"));
    });
    std::thread writer2([&store, &ready, &start_signal]() {
        wait_for_start(ready, start_signal, 2);
        store.Put(11, text("latency_target_b"));
    });

    while (ready.load(std::memory_order_acquire) < 2) {
        std::this_thread::yield();
    }
    start_signal.store(true, std::memory_order_release);

    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    std::thread writer3([&store]() {
        store.Put(12, text("latency_target_c"));
    });

    writer1.join();
    writer2.join();
    writer3.join();

    const KVStoreMetrics metrics = store.GetMetrics();
    require(metrics.adaptive_latency_target_batches_completed >= 1,
            "latency target should trigger at least one earlier-flush batch");
    require(metrics.min_effective_batch_delay_us > 0 &&
                metrics.min_effective_batch_delay_us < options.max_batch_delay_us,
            "latency target should reduce effective batch delay below the base delay");
}

void test_fsync_pressure_can_relax_batch_delay() {
    TestDir dir("fsync_pressure");
    const std::string db_path = dir.file("store.dat");

    KVStoreOptions options;
    options.max_batch_size = 1;
    options.max_batch_delay_us = 1000;
    options.adaptive_fsync_pressure_per_1000_writes_threshold = 900;
    options.adaptive_fsync_pressure_delay_multiplier = 10;
    options.adaptive_fsync_pressure_max_batch_delay_us = 10000;

    KVStore store(db_path, options);
    for (int i = 0; i < 4; ++i) {
        store.Put(i, text("pressure_" + std::to_string(i)));
    }

    store.Put(100, text("pressure_probe"));

    const KVStoreMetrics metrics = store.GetMetrics();
    require(metrics.adaptive_fsync_pressure_batches_completed >= 1,
            "high fsync pressure should trigger a relaxed-delay batch");
    require(metrics.last_effective_batch_delay_us > options.max_batch_delay_us,
            "fsync pressure should expand the effective batch delay above the base delay");
    require(metrics.observed_fsync_pressure_per_1000_writes >=
                options.adaptive_fsync_pressure_per_1000_writes_threshold,
            "observed fsync pressure metric should reflect the singleton-write workload");
    require(metrics.max_effective_batch_delay_us >= metrics.last_effective_batch_delay_us,
            "max effective batch delay should track the expanded delay");
}

void test_recent_window_metrics_capture_bursts() {
    TestDir dir("recent_window");
    const std::string db_path = dir.file("store.dat");

    KVStoreOptions options;
    options.max_batch_size = 4;
    options.max_batch_delay_us = 5000;
    options.adaptive_recent_window_batches = 4;
    options.adaptive_recent_write_sample_limit = 16;
    KVStore store(db_path, options);

    constexpr int kWriterCount = 4;
    std::atomic<int> ready {0};
    std::atomic<bool> start_signal {false};
    std::vector<std::thread> writers;

    for (int writer_id = 0; writer_id < kWriterCount; ++writer_id) {
        writers.emplace_back([&store, &ready, &start_signal, writer_id]() {
            wait_for_start(ready, start_signal, kWriterCount);
            store.Put(writer_id, text("recent_" + std::to_string(writer_id)));
        });
    }

    while (ready.load(std::memory_order_acquire) < kWriterCount) {
        std::this_thread::yield();
    }
    start_signal.store(true, std::memory_order_release);

    for (auto& writer : writers) {
        writer.join();
    }

    const KVStoreMetrics metrics = store.GetMetrics();
    require(metrics.recent_window_batch_count >= 1, "recent batch window should track completed batches");
    require(metrics.recent_peak_queue_depth >= 1,
            "recent queue peak should record at least one queued write during the burst");
    require(metrics.recent_peak_queue_depth >= 2 || metrics.recent_avg_batch_size >= 2 ||
                metrics.last_committed_batch_size >= 2,
            "recent window should capture the burst via queue buildup or multi-write batching");
    require(metrics.recent_avg_batch_size >= 1, "recent batch window should track average batch size");
    require(metrics.recent_observed_write_latency_p95_us > 0,
            "recent latency window should expose a non-zero recent p95");
}

void test_read_heavy_signal_prefers_shorter_batches() {
    TestDir dir("read_heavy");
    const std::string db_path = dir.file("store.dat");

    KVStoreOptions options;
    options.max_batch_size = 16;
    options.max_batch_delay_us = 30000;
    options.adaptive_read_heavy_read_per_1000_ops_threshold = 800;
    options.adaptive_read_heavy_delay_divisor = 10;
    options.adaptive_read_heavy_batch_size_divisor = 4;
    options.adaptive_flush_min_batch_delay_us = 500;
    KVStore store(db_path, options);

    for (int i = 0; i < 128; ++i) {
        (void)store.Get(i);
    }

    std::thread writer1([&store]() {
        store.Put(1, text("read_heavy_a"));
    });
    std::thread writer2([&store]() {
        store.Put(2, text("read_heavy_b"));
    });
    writer1.join();
    writer2.join();

    const KVStoreMetrics metrics = store.GetMetrics();
    require(metrics.adaptive_read_heavy_batches_completed >= 1,
            "read-heavy workload should trigger reader-friendly batching");
    require(metrics.recent_read_ratio_per_1000_ops >= 800,
            "recent read ratio should reflect the read-heavy workload");
    require(metrics.last_effective_batch_delay_us < options.max_batch_delay_us,
            "read-heavy workload should shorten the effective batch delay");
}

void test_compaction_pressure_signal_relaxes_batch_delay() {
    TestDir dir("compaction_pressure");
    const std::string db_path = dir.file("store.dat");

    KVStoreOptions options;
    options.max_batch_size = 1;
    options.max_batch_delay_us = 1000;
    options.adaptive_compaction_pressure_obsolete_ratio_percent_threshold = 50;
    options.adaptive_compaction_pressure_delay_multiplier = 5;
    KVStore store(db_path, options);

    for (int i = 0; i < 6; ++i) {
        store.Put(7, text("obsolete_" + std::to_string(i)));
    }
    store.Put(8, text("pressure_probe"));

    const KVStoreMetrics metrics = store.GetMetrics();
    require(metrics.observed_obsolete_wal_ratio_percent >= 50,
            "obsolete WAL ratio should reflect repeated overwrites");
    require(metrics.adaptive_compaction_pressure_batches_completed >= 1,
            "high obsolete WAL ratio should relax batching before compaction");
    require(metrics.last_effective_batch_delay_us > options.max_batch_delay_us,
            "compaction pressure should expand the effective batch delay");
}

void test_wal_growth_signal_relaxes_batch_delay() {
    TestDir dir("wal_growth");
    const std::string db_path = dir.file("store.dat");

    KVStoreOptions options;
    options.max_batch_size = 1;
    options.max_batch_delay_us = 1000;
    options.adaptive_wal_growth_bytes_per_batch_threshold = 40;
    options.adaptive_wal_growth_delay_multiplier = 4;
    options.adaptive_wal_growth_max_batch_delay_us = 8000;
    options.adaptive_recent_window_batches = 8;
    KVStore store(db_path, options);

    for (int i = 0; i < 4; ++i) {
        store.Put(i, text("payload_abcdefghijklmnopqrstuvwxyz_" + std::to_string(i)));
    }
    store.Put(99, text("growth_probe"));

    const KVStoreMetrics metrics = store.GetMetrics();
    require(metrics.recent_avg_batch_wal_bytes >= options.adaptive_wal_growth_bytes_per_batch_threshold,
            "recent WAL growth metric should reflect large write batches");
    require(metrics.adaptive_wal_growth_batches_completed >= 1,
            "large recent WAL growth should relax the next batch delay");
    require(metrics.last_effective_batch_delay_us > options.max_batch_delay_us,
            "WAL growth signal should expand the effective batch delay");
}

void test_objective_policy_prefers_short_delay_under_latency_pressure() {
    TestDir dir("objective_short_delay");
    const std::string db_path = dir.file("store.dat");

    KVStoreOptions options;
    options.max_batch_size = 1;
    options.max_batch_delay_us = 20000;
    options.adaptive_objective_enabled = true;
    options.adaptive_objective_queue_weight = 0;
    options.adaptive_objective_latency_weight = 4;
    options.adaptive_objective_read_weight = 0;
    options.adaptive_objective_fsync_weight = 0;
    options.adaptive_objective_compaction_weight = 0;
    options.adaptive_objective_wal_growth_weight = 0;
    options.adaptive_objective_short_delay_score_threshold = 500;
    options.adaptive_objective_short_delay_divisor = 10;
    options.adaptive_flush_min_batch_delay_us = 500;
    options.adaptive_latency_target_p95_us = 10;
    KVStore store(db_path, options);

    store.Put(1, text("objective_latency_a"));
    store.Put(2, text("objective_latency_b"));
    store.Put(3, text("objective_latency_probe"));

    const KVStoreMetrics metrics = store.GetMetrics();
    require(metrics.recent_observed_write_latency_p95_us >= options.adaptive_latency_target_p95_us,
            "recent p95 should exceed the configured latency target");
    require(metrics.adaptive_objective_short_delay_batches_completed >= 1,
            "objective policy should shorten the batch delay under latency pressure");
    require(metrics.last_effective_batch_delay_us < options.max_batch_delay_us,
            "objective pressure score should shorten the effective batch delay");
    require(metrics.last_objective_pressure_score > metrics.last_objective_cost_score,
            "objective pressure score should dominate the cost score in the latency-heavy scenario");
    require(metrics.last_objective_balance_score > 0,
            "objective balance should be positive when pressure favors shorter delays");
}

void test_objective_short_delay_owns_delay_decision_when_enabled() {
    TestDir dir("objective_owns_delay");
    const std::string db_path = dir.file("store.dat");

    KVStoreOptions options;
    options.max_batch_size = 1;
    options.max_batch_delay_us = 50000;
    options.adaptive_batching_enabled = true;
    options.adaptive_queue_depth_threshold = 1;
    options.adaptive_batch_size_multiplier = 4;
    options.adaptive_flush_min_batch_delay_us = 1000;
    options.adaptive_objective_enabled = true;
    options.adaptive_objective_queue_weight = 4;
    options.adaptive_objective_latency_weight = 0;
    options.adaptive_objective_read_weight = 0;
    options.adaptive_objective_fsync_weight = 0;
    options.adaptive_objective_compaction_weight = 0;
    options.adaptive_objective_wal_growth_weight = 0;
    options.adaptive_objective_short_delay_score_threshold = 100;
    options.adaptive_objective_short_delay_divisor = 10;

    KVStore store(db_path, options);

    constexpr int kWriterCount = 4;
    std::atomic<int> ready {0};
    std::atomic<bool> start_signal {false};
    std::vector<std::thread> writers;

    for (int writer_id = 0; writer_id < kWriterCount; ++writer_id) {
        writers.emplace_back([&store, &ready, &start_signal, writer_id]() {
            wait_for_start(ready, start_signal, kWriterCount);
            store.Put(writer_id, text("objective_owned_delay_" + std::to_string(writer_id)));
        });
    }

    while (ready.load(std::memory_order_acquire) < kWriterCount) {
        std::this_thread::yield();
    }
    start_signal.store(true, std::memory_order_release);

    for (auto& writer : writers) {
        writer.join();
    }

    const KVStoreMetrics metrics = store.GetMetrics();
    require(metrics.adaptive_objective_short_delay_batches_completed >= 1,
            "objective should own the short-delay decision when it is enabled");
    require(metrics.adaptive_flush_batches_completed == 0,
            "adaptive flush should not directly shorten delay when objective control is enabled");
    require(metrics.max_pending_queue_depth >= 1,
            "queue pressure should still be observed without adaptive flush");
    require(metrics.last_effective_batch_delay_us < options.max_batch_delay_us,
            "objective-owned short delay should still reduce the effective batch delay");
}

void test_objective_policy_prefers_long_delay_under_cost_pressure() {
    TestDir dir("objective_long_delay");
    const std::string db_path = dir.file("store.dat");

    KVStoreOptions options;
    options.max_batch_size = 1;
    options.max_batch_delay_us = 500;
    options.adaptive_recent_window_batches = 8;
    options.adaptive_objective_enabled = true;
    options.adaptive_objective_queue_weight = 0;
    options.adaptive_objective_latency_weight = 0;
    options.adaptive_objective_read_weight = 0;
    options.adaptive_objective_fsync_weight = 2;
    options.adaptive_objective_compaction_weight = 2;
    options.adaptive_objective_wal_growth_weight = 1;
    options.adaptive_objective_long_delay_score_threshold = 500;
    options.adaptive_objective_long_delay_multiplier = 6;
    options.adaptive_objective_max_batch_delay_us = 5000;
    options.adaptive_fsync_pressure_per_1000_writes_threshold = 800;
    options.adaptive_compaction_pressure_obsolete_ratio_percent_threshold = 50;
    options.adaptive_wal_growth_bytes_per_batch_threshold = 40;
    KVStore store(db_path, options);

    for (int i = 0; i < 6; ++i) {
        store.Put(42, text("objective_cost_payload_abcdefghijklmnopqrstuvwxyz_" + std::to_string(i)));
    }
    store.Put(99, text("objective_cost_probe_payload"));

    const KVStoreMetrics metrics = store.GetMetrics();
    require(metrics.observed_fsync_pressure_per_1000_writes >= options.adaptive_fsync_pressure_per_1000_writes_threshold,
            "recent fsync pressure should exceed the configured objective cost scale");
    require(metrics.observed_obsolete_wal_ratio_percent >= options.adaptive_compaction_pressure_obsolete_ratio_percent_threshold,
            "obsolete WAL ratio should exceed the configured objective cost scale");
    require(metrics.adaptive_objective_long_delay_batches_completed >= 1,
            "objective policy should relax the batch delay when cost signals dominate");
    require(metrics.last_effective_batch_delay_us > options.max_batch_delay_us,
            "objective cost score should expand the effective batch delay");
    require(metrics.last_objective_cost_score > metrics.last_objective_pressure_score,
            "objective cost score should dominate the pressure score in the cost-heavy scenario");
    require(metrics.last_objective_balance_score < 0,
            "objective balance should be negative when cost favors longer delays");
}

void test_objective_throughput_score_can_dominate_latency_pressure() {
    TestDir dir("objective_throughput");
    const std::string db_path = dir.file("store.dat");

    KVStoreOptions options;
    options.max_batch_size = 1;
    options.max_batch_delay_us = 500;
    options.adaptive_recent_window_batches = 8;
    options.adaptive_objective_enabled = true;
    options.adaptive_objective_queue_weight = 0;
    options.adaptive_objective_latency_weight = 1;
    options.adaptive_objective_read_weight = 0;
    options.adaptive_objective_throughput_weight = 6;
    options.adaptive_objective_target_batch_size = 8;
    options.adaptive_objective_fsync_weight = 0;
    options.adaptive_objective_compaction_weight = 0;
    options.adaptive_objective_wal_growth_weight = 0;
    options.adaptive_objective_long_delay_score_threshold = 250;
    options.adaptive_objective_long_delay_multiplier = 6;
    options.adaptive_objective_max_batch_delay_us = 5000;
    options.adaptive_flush_min_batch_delay_us = 100;
    options.adaptive_latency_target_p95_us = 50;
    KVStore store(db_path, options);

    store.Put(1, text("objective_tp_a"));
    store.Put(2, text("objective_tp_b"));
    store.Put(3, text("objective_tp_probe"));

    const KVStoreMetrics metrics = store.GetMetrics();
    require(metrics.recent_observed_write_latency_p95_us >= options.adaptive_latency_target_p95_us,
            "recent p95 should still reflect latency pressure");
    require(metrics.recent_avg_batch_size < options.adaptive_objective_target_batch_size,
            "recent average batch size should remain below the throughput target");
    require(metrics.adaptive_objective_throughput_batches_completed >= 1,
            "objective controller should record batches influenced by throughput deficit");
    require(metrics.last_objective_throughput_score > 0,
            "objective controller should expose a non-zero throughput score");
    require(metrics.last_objective_cost_score > metrics.last_objective_pressure_score,
            "throughput deficit should outweigh latency pressure in this scenario");
    require(metrics.last_objective_mode < 0,
            "objective mode should report a long-delay decision when throughput dominates");
    require(metrics.last_effective_batch_delay_us > options.max_batch_delay_us,
            "throughput-dominated objective control should expand the effective batch delay");
}

}  // namespace

void register_metrics_controller_tests(TestCases& tests) {
    tests.push_back({"batching metrics are reported", test_batching_metrics_are_reported});
    tests.push_back({"auto compaction triggers and preserves state", test_auto_compaction_triggers_and_preserves_state});
    tests.push_back({"manual and auto compaction metrics coexist", test_manual_and_auto_compaction_metrics_coexist});
    tests.push_back({"invalid wal ratio triggers auto compaction", test_invalid_wal_ratio_triggers_auto_compaction});
    tests.push_back({"wal obsolete byte metrics are consistent", test_wal_obsolete_byte_metrics_are_consistent});
    tests.push_back({"batch wal byte limit is respected", test_batch_wal_byte_limit_is_respected});
    tests.push_back({"latency histogram tracks write requests", test_latency_histogram_tracks_write_requests});
    tests.push_back({"adaptive batching expands batch under queue pressure", test_adaptive_batching_expands_batch_under_queue_pressure});
    tests.push_back({"percentile and writer metrics are reported", test_percentile_and_writer_metrics_are_reported});
    tests.push_back({"adaptive flush shortens batch delay", test_adaptive_flush_shortens_batch_delay});
    tests.push_back({"compaction long term metrics accumulate", test_compaction_long_term_metrics_accumulate});
    tests.push_back({"latency target adaptive flush kicks in", test_latency_target_adaptive_flush_kicks_in});
    tests.push_back({"fsync pressure can relax batch delay", test_fsync_pressure_can_relax_batch_delay});
    tests.push_back({"recent window metrics capture bursts", test_recent_window_metrics_capture_bursts});
    tests.push_back({"read heavy signal prefers shorter batches", test_read_heavy_signal_prefers_shorter_batches});
    tests.push_back({"compaction pressure signal relaxes batch delay", test_compaction_pressure_signal_relaxes_batch_delay});
    tests.push_back({"wal growth signal relaxes batch delay", test_wal_growth_signal_relaxes_batch_delay});
    tests.push_back({"objective policy prefers short delay under latency pressure",
                     test_objective_policy_prefers_short_delay_under_latency_pressure});
    tests.push_back({"objective short delay owns delay decision when enabled",
                     test_objective_short_delay_owns_delay_decision_when_enabled});
    tests.push_back({"objective policy prefers long delay under cost pressure",
                     test_objective_policy_prefers_long_delay_under_cost_pressure});
    tests.push_back({"objective throughput score can dominate latency pressure",
                     test_objective_throughput_score_can_dominate_latency_pressure});
}

}  // namespace kvstore::tests::integration
