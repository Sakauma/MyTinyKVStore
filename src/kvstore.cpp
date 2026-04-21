#include "kvstore.h"
#include "internal/format.h"
#include "internal/io.h"
#include "internal/metrics_helpers.h"
#include "internal/observability.h"
#include "internal/recovery.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <deque>
#include <filesystem>
#include <map>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

namespace {
using namespace kvstore::internal;

}  // namespace

class KVStore::Impl {
public:
    struct BatchMutation {
        WalRecordType type;
        std::string key;
        Value value;
    };

    struct BatchPolicy {
        size_t max_batch_size;
        uint64_t max_batch_wal_bytes;
        uint32_t batch_delay_us;
        uint64_t observed_queue_depth;
        uint64_t objective_pressure_score;
        uint64_t objective_cost_score;
        uint64_t objective_throughput_score;
        int64_t objective_balance_score;
        int64_t objective_mode;
        bool adaptive_batching;
        bool adaptive_flush;
        bool latency_target_adjusted;
        bool fsync_pressure_adjusted;
        bool read_heavy_adjusted;
        bool compaction_pressure_adjusted;
        bool wal_growth_adjusted;
        bool objective_short_delay_adjusted;
        bool objective_long_delay_adjusted;
    };

    Impl(std::string db_path, KVStoreOptions options)
        : db_file_path_(std::move(db_path)),
          wal_file_path_(db_file_path_ + ".wal"),
          options_(sanitize_options(options)),
          wal_fd_(-1),
          wal_bytes_since_compaction_(0),
          stop_(false),
          has_fatal_error_(false) {
        ensure_snapshot_file_exists(db_file_path_);
        {
            std::unique_lock<std::shared_mutex> lock(state_mutex_);
            load_snapshot_into_state(db_file_path_, state_);
            replay_wal_into_state(
                wal_file_path_,
                state_,
                [this](const std::string& key, uint64_t record_size) { note_latest_wal_record(key, record_size); });
        }
        wal_bytes_since_compaction_.store(current_wal_file_size(), std::memory_order_relaxed);
        open_wal_for_append();
        writer_thread_ = std::thread(&Impl::writer_loop, this);
    }

    ~Impl() {
        {
            std::lock_guard<std::mutex> lock(queue_mutex_);
            stop_ = true;
        }
        queue_cv_.notify_one();
        if (writer_thread_.joinable()) {
            writer_thread_.join();
        }
        close_if_open(wal_fd_);
    }

    void Put(const std::string& key, Value value) {
        auto request = std::make_shared<Request>();
        request->type = RequestType::kPut;
        request->key = key;
        request->value = std::move(value);
        enqueue_and_wait(std::move(request));
    }

    void WriteBatch(const std::vector<BatchMutation>& operations) {
        auto request = std::make_shared<Request>();
        request->type = RequestType::kBatch;
        request->batch_operations = operations;
        enqueue_and_wait(std::move(request));
    }

    std::optional<Value> Get(const std::string& key) {
        throw_if_fatal();
        read_requests_.fetch_add(1, std::memory_order_relaxed);
        std::shared_lock<std::shared_mutex> lock(state_mutex_);
        auto it = state_.find(key);
        if (it == state_.end()) {
            return std::nullopt;
        }
        return it->second;
    }

    void Delete(const std::string& key) {
        auto request = std::make_shared<Request>();
        request->type = RequestType::kDelete;
        request->key = key;
        enqueue_and_wait(std::move(request));
    }

    std::vector<std::pair<std::string, Value>> Scan(const std::string& start_key, const std::string& end_key) {
        std::vector<std::pair<std::string, Value>> result;
        const std::string encoded_start = encode_string_key(start_key);
        const std::string encoded_end = encode_string_key(end_key);
        std::shared_lock<std::shared_mutex> lock(state_mutex_);
        for (auto it = state_.lower_bound(encoded_start); it != state_.end(); ++it) {
            if (!is_string_key(it->first) || it->first > encoded_end) {
                break;
            }
            result.emplace_back(decode_string_key(it->first), it->second);
        }
        return result;
    }

    void Compact() {
        auto request = std::make_shared<Request>();
        request->type = RequestType::kCompact;
        enqueue_and_wait(std::move(request));
    }

    KVStoreMetrics GetMetrics() {
        KVStoreMetrics metrics;
        metrics.read_requests = read_requests_.load(std::memory_order_relaxed);
        metrics.enqueued_write_requests = enqueued_write_requests_.load(std::memory_order_relaxed);
        metrics.committed_write_requests = committed_write_requests_.load(std::memory_order_relaxed);
        metrics.committed_write_batches = committed_write_batches_.load(std::memory_order_relaxed);
        metrics.compact_requests = compact_requests_.load(std::memory_order_relaxed);
        metrics.wal_fsync_calls = wal_fsync_calls_.load(std::memory_order_relaxed);
        metrics.wal_bytes_written = wal_bytes_written_.load(std::memory_order_relaxed);
        metrics.wal_bytes_since_compaction = wal_bytes_since_compaction_.load(std::memory_order_relaxed);
        metrics.live_wal_bytes_since_compaction = live_wal_bytes_since_compaction_.load(std::memory_order_relaxed);
        metrics.obsolete_wal_bytes_since_compaction =
            metrics.wal_bytes_since_compaction >= metrics.live_wal_bytes_since_compaction
                ? metrics.wal_bytes_since_compaction - metrics.live_wal_bytes_since_compaction
                : 0;
        metrics.last_committed_batch_size = last_committed_batch_size_.load(std::memory_order_relaxed);
        metrics.max_committed_batch_size = max_committed_batch_size_.load(std::memory_order_relaxed);
        metrics.last_committed_batch_wal_bytes = last_committed_batch_wal_bytes_.load(std::memory_order_relaxed);
        metrics.max_committed_batch_wal_bytes = max_committed_batch_wal_bytes_.load(std::memory_order_relaxed);
        metrics.max_pending_queue_depth = max_pending_queue_depth_.load(std::memory_order_relaxed);
        metrics.manual_compactions_completed = manual_compactions_completed_.load(std::memory_order_relaxed);
        metrics.auto_compactions_completed = auto_compactions_completed_.load(std::memory_order_relaxed);
        metrics.adaptive_batches_completed = adaptive_batches_completed_.load(std::memory_order_relaxed);
        metrics.adaptive_flush_batches_completed = adaptive_flush_batches_completed_.load(std::memory_order_relaxed);
        metrics.adaptive_latency_target_batches_completed =
            adaptive_latency_target_batches_completed_.load(std::memory_order_relaxed);
        metrics.adaptive_fsync_pressure_batches_completed =
            adaptive_fsync_pressure_batches_completed_.load(std::memory_order_relaxed);
        metrics.adaptive_read_heavy_batches_completed =
            adaptive_read_heavy_batches_completed_.load(std::memory_order_relaxed);
        metrics.adaptive_compaction_pressure_batches_completed =
            adaptive_compaction_pressure_batches_completed_.load(std::memory_order_relaxed);
        metrics.adaptive_wal_growth_batches_completed =
            adaptive_wal_growth_batches_completed_.load(std::memory_order_relaxed);
        metrics.adaptive_objective_short_delay_batches_completed =
            adaptive_objective_short_delay_batches_completed_.load(std::memory_order_relaxed);
        metrics.adaptive_objective_long_delay_batches_completed =
            adaptive_objective_long_delay_batches_completed_.load(std::memory_order_relaxed);
        metrics.adaptive_objective_throughput_batches_completed =
            adaptive_objective_throughput_batches_completed_.load(std::memory_order_relaxed);
        metrics.writer_wait_events = writer_wait_events_.load(std::memory_order_relaxed);
        metrics.writer_wait_time_us = writer_wait_time_us_.load(std::memory_order_relaxed);
        metrics.last_effective_batch_delay_us = last_effective_batch_delay_us_.load(std::memory_order_relaxed);
        metrics.min_effective_batch_delay_us = min_effective_batch_delay_us_.load(std::memory_order_relaxed);
        metrics.max_effective_batch_delay_us = max_effective_batch_delay_us_.load(std::memory_order_relaxed);
        metrics.observed_fsync_pressure_per_1000_writes =
            observed_fsync_pressure_per_1000_writes_.load(std::memory_order_relaxed);
        metrics.last_objective_pressure_score =
            last_objective_pressure_score_.load(std::memory_order_relaxed);
        metrics.last_objective_cost_score =
            last_objective_cost_score_.load(std::memory_order_relaxed);
        metrics.last_objective_throughput_score =
            last_objective_throughput_score_.load(std::memory_order_relaxed);
        metrics.last_objective_balance_score =
            last_objective_balance_score_.load(std::memory_order_relaxed);
        metrics.last_objective_mode =
            last_objective_mode_.load(std::memory_order_relaxed);
        metrics.total_snapshot_bytes_written = total_snapshot_bytes_written_.load(std::memory_order_relaxed);
        metrics.total_wal_bytes_reclaimed_by_compaction =
            total_wal_bytes_reclaimed_by_compaction_.load(std::memory_order_relaxed);
        for (size_t i = 0; i < kWriteLatencyBucketCount; ++i) {
            metrics.write_latency_histogram[i] = write_latency_histogram_[i].load(std::memory_order_relaxed);
        }
        metrics.approx_write_latency_p50_us = approximate_latency_percentile_us(metrics.write_latency_histogram, 50, 100);
        metrics.approx_write_latency_p95_us = approximate_latency_percentile_us(metrics.write_latency_histogram, 95, 100);
        metrics.approx_write_latency_p99_us = approximate_latency_percentile_us(metrics.write_latency_histogram, 99, 100);
        metrics.recent_read_requests = recent_read_requests_.load(std::memory_order_relaxed);
        metrics.recent_write_requests = recent_write_requests_.load(std::memory_order_relaxed);
        metrics.recent_read_ratio_per_1000_ops =
            recent_read_ratio_per_1000_ops_.load(std::memory_order_relaxed);
        metrics.recent_observed_write_latency_p95_us =
            recent_observed_write_latency_p95_us_.load(std::memory_order_relaxed);
        metrics.recent_peak_queue_depth = recent_peak_queue_depth_.load(std::memory_order_relaxed);
        metrics.recent_avg_batch_size = recent_avg_batch_size_.load(std::memory_order_relaxed);
        metrics.recent_batch_fill_per_1000 = recent_batch_fill_per_1000_.load(std::memory_order_relaxed);
        metrics.recent_avg_batch_wal_bytes = recent_avg_batch_wal_bytes_.load(std::memory_order_relaxed);
        metrics.recent_window_batch_count = recent_window_batch_count_.load(std::memory_order_relaxed);
        metrics.observed_obsolete_wal_ratio_percent = current_obsolete_wal_ratio_percent();
        {
            std::lock_guard<std::mutex> lock(queue_mutex_);
            metrics.pending_queue_depth = request_queue_.size();
        }
        return metrics;
    }

private:
    enum class RequestType {
        kPut,
        kDelete,
        kBatch,
        kCompact,
    };

    struct Request {
        RequestType type {};
        std::string key;
        Value value;
        std::vector<BatchMutation> batch_operations;
        std::chrono::steady_clock::time_point enqueue_time = std::chrono::steady_clock::now();
        std::mutex done_mutex;
        std::condition_variable done_cv;
        bool done = false;
        std::string error;
    };

    static KVStoreOptions sanitize_options(KVStoreOptions options) {
        if (options.max_batch_size == 0) {
            options.max_batch_size = 1;
        }
        if (options.adaptive_objective_short_delay_divisor == 0) {
            options.adaptive_objective_short_delay_divisor = 1;
        }
        if (options.adaptive_objective_long_delay_multiplier == 0) {
            options.adaptive_objective_long_delay_multiplier = 1;
        }
        if (options.adaptive_recent_window_batches == 0) {
            options.adaptive_recent_window_batches = 1;
        }
        if (options.adaptive_recent_write_sample_limit == 0) {
            options.adaptive_recent_write_sample_limit = 1;
        }
        if (options.adaptive_read_heavy_delay_divisor == 0) {
            options.adaptive_read_heavy_delay_divisor = 1;
        }
        if (options.adaptive_read_heavy_batch_size_divisor == 0) {
            options.adaptive_read_heavy_batch_size_divisor = 1;
        }
        if (options.adaptive_flush_queue_depth_threshold == 0) {
            options.adaptive_flush_queue_depth_threshold = 1;
        }
        if (options.adaptive_flush_delay_divisor == 0) {
            options.adaptive_flush_delay_divisor = 1;
        }
        if (options.adaptive_fsync_pressure_delay_multiplier == 0) {
            options.adaptive_fsync_pressure_delay_multiplier = 1;
        }
        if (options.adaptive_compaction_pressure_delay_multiplier == 0) {
            options.adaptive_compaction_pressure_delay_multiplier = 1;
        }
        if (options.adaptive_wal_growth_delay_multiplier == 0) {
            options.adaptive_wal_growth_delay_multiplier = 1;
        }
        if (options.adaptive_queue_depth_threshold == 0) {
            options.adaptive_queue_depth_threshold = 1;
        }
        if (options.adaptive_batch_size_multiplier == 0) {
            options.adaptive_batch_size_multiplier = 1;
        }
        if (options.adaptive_batch_wal_bytes_multiplier == 0) {
            options.adaptive_batch_wal_bytes_multiplier = 1;
        }
        if (options.auto_compact_invalid_wal_ratio_percent > 100) {
            options.auto_compact_invalid_wal_ratio_percent = 100;
        }
        return options;
    }

    std::string db_file_path_;
    std::string wal_file_path_;
    KVStoreOptions options_;

    std::map<std::string, Value> state_;
    mutable std::shared_mutex state_mutex_;

    std::mutex queue_mutex_;
    std::condition_variable queue_cv_;
    std::deque<std::shared_ptr<Request>> request_queue_;
    std::thread writer_thread_;
    int wal_fd_;
    std::atomic<uint64_t> wal_bytes_since_compaction_;
    std::atomic<uint64_t> live_wal_bytes_since_compaction_ {0};
    std::atomic<uint64_t> read_requests_ {0};
    bool stop_;
    std::atomic<bool> has_fatal_error_;
    std::string fatal_error_message_;
    std::map<std::string, uint64_t> latest_wal_record_bytes_;

    std::atomic<uint64_t> enqueued_write_requests_ {0};
    std::atomic<uint64_t> committed_write_requests_ {0};
    std::atomic<uint64_t> committed_write_batches_ {0};
    std::atomic<uint64_t> compact_requests_ {0};
    std::atomic<uint64_t> wal_fsync_calls_ {0};
    std::atomic<uint64_t> wal_bytes_written_ {0};
    std::atomic<uint64_t> last_committed_batch_size_ {0};
    std::atomic<uint64_t> max_committed_batch_size_ {0};
    std::atomic<uint64_t> last_committed_batch_wal_bytes_ {0};
    std::atomic<uint64_t> max_committed_batch_wal_bytes_ {0};
    std::atomic<uint64_t> max_pending_queue_depth_ {0};
    std::atomic<uint64_t> manual_compactions_completed_ {0};
    std::atomic<uint64_t> auto_compactions_completed_ {0};
    std::atomic<uint64_t> adaptive_batches_completed_ {0};
    std::atomic<uint64_t> adaptive_flush_batches_completed_ {0};
    std::atomic<uint64_t> adaptive_latency_target_batches_completed_ {0};
    std::atomic<uint64_t> adaptive_fsync_pressure_batches_completed_ {0};
    std::atomic<uint64_t> adaptive_read_heavy_batches_completed_ {0};
    std::atomic<uint64_t> adaptive_compaction_pressure_batches_completed_ {0};
    std::atomic<uint64_t> adaptive_wal_growth_batches_completed_ {0};
    std::atomic<uint64_t> adaptive_objective_short_delay_batches_completed_ {0};
    std::atomic<uint64_t> adaptive_objective_long_delay_batches_completed_ {0};
    std::atomic<uint64_t> adaptive_objective_throughput_batches_completed_ {0};
    std::atomic<uint64_t> writer_wait_events_ {0};
    std::atomic<uint64_t> writer_wait_time_us_ {0};
    std::atomic<uint64_t> last_effective_batch_delay_us_ {0};
    std::atomic<uint64_t> min_effective_batch_delay_us_ {0};
    std::atomic<uint64_t> max_effective_batch_delay_us_ {0};
    std::atomic<uint64_t> observed_fsync_pressure_per_1000_writes_ {0};
    std::atomic<uint64_t> last_objective_pressure_score_ {0};
    std::atomic<uint64_t> last_objective_cost_score_ {0};
    std::atomic<uint64_t> last_objective_throughput_score_ {0};
    std::atomic<int64_t> last_objective_balance_score_ {0};
    std::atomic<int64_t> last_objective_mode_ {0};
    std::atomic<uint64_t> recent_observed_write_latency_p95_us_ {0};
    std::atomic<uint64_t> recent_read_requests_ {0};
    std::atomic<uint64_t> recent_write_requests_ {0};
    std::atomic<uint64_t> recent_read_ratio_per_1000_ops_ {0};
    std::atomic<uint64_t> recent_peak_queue_depth_ {0};
    std::atomic<uint64_t> recent_avg_batch_size_ {0};
    std::atomic<uint64_t> recent_batch_fill_per_1000_ {0};
    std::atomic<uint64_t> recent_avg_batch_wal_bytes_ {0};
    std::atomic<uint64_t> recent_window_batch_count_ {0};
    std::atomic<uint64_t> total_snapshot_bytes_written_ {0};
    std::atomic<uint64_t> total_wal_bytes_reclaimed_by_compaction_ {0};
    uint32_t current_batch_delay_us_ = 0;
    uint64_t current_batch_queue_depth_ = 0;
    uint64_t last_total_read_requests_seen_ = 0;
    std::deque<uint64_t> recent_batch_sizes_;
    std::deque<uint64_t> recent_batch_queue_depths_;
    std::deque<uint64_t> recent_batch_wal_bytes_;
    uint64_t recent_batch_wal_bytes_sum_ = 0;
    std::deque<uint64_t> recent_read_deltas_;
    std::deque<uint64_t> recent_write_deltas_;
    uint64_t recent_read_sum_ = 0;
    uint64_t recent_write_sum_ = 0;
    uint64_t recent_batch_size_sum_ = 0;
    std::deque<size_t> recent_latency_buckets_;
    std::array<uint64_t, kWriteLatencyBucketCount> recent_latency_bucket_counts_ {};
    std::array<std::atomic<uint64_t>, kWriteLatencyBucketCount> write_latency_histogram_ {};

    void enqueue_and_wait(std::shared_ptr<Request> request) {
        throw_if_fatal();
        {
            std::lock_guard<std::mutex> lock(queue_mutex_);
            throw_if_fatal_locked();
            request_queue_.push_back(request);
            update_atomic_max(max_pending_queue_depth_, request_queue_.size());
                if (request->type == RequestType::kCompact) {
                    compact_requests_.fetch_add(1, std::memory_order_relaxed);
                } else {
                    enqueued_write_requests_.fetch_add(1, std::memory_order_relaxed);
                }
        }
        queue_cv_.notify_one();

        std::unique_lock<std::mutex> done_lock(request->done_mutex);
        request->done_cv.wait(done_lock, [&request] { return request->done; });
        if (!request->error.empty()) {
            throw KVStoreError(request->error);
        }
    }

    void writer_loop() {
        while (true) {
            std::vector<std::shared_ptr<Request>> write_batch;
            std::shared_ptr<Request> compact_request;
            BatchPolicy batch_policy {
                options_.max_batch_size,
                options_.max_batch_wal_bytes,
                options_.max_batch_delay_us,
                0,
                0,
                0,
                0,
                0,
                0,
                false,
                false,
                false,
                false,
                false,
                false,
                false,
                false,
                false,
            };

            {
                std::unique_lock<std::mutex> lock(queue_mutex_);
                if (request_queue_.empty() && !stop_) {
                    wait_for_queue_activity(lock);
                }
                if (stop_ && request_queue_.empty()) {
                    return;
                }

                if (!request_queue_.empty() && request_queue_.front()->type != RequestType::kCompact) {
                    batch_policy = current_batch_policy_locked();
                    const auto deadline =
                        std::chrono::steady_clock::now() + std::chrono::microseconds(batch_policy.batch_delay_us);
                    collect_write_batch(lock, batch_policy, write_batch, deadline);
                }

                if (write_batch.empty() && !request_queue_.empty() && request_queue_.front()->type == RequestType::kCompact) {
                    compact_request = request_queue_.front();
                    request_queue_.pop_front();
                }
            }

            try {
                if (!write_batch.empty()) {
                    current_batch_delay_us_ = batch_policy.batch_delay_us;
                    current_batch_queue_depth_ = batch_policy.observed_queue_depth;
                    last_objective_pressure_score_.store(batch_policy.objective_pressure_score, std::memory_order_relaxed);
                    last_objective_cost_score_.store(batch_policy.objective_cost_score, std::memory_order_relaxed);
                    last_objective_throughput_score_.store(batch_policy.objective_throughput_score, std::memory_order_relaxed);
                    last_objective_balance_score_.store(batch_policy.objective_balance_score, std::memory_order_relaxed);
                    last_objective_mode_.store(batch_policy.objective_mode, std::memory_order_relaxed);
                    process_write_batch(write_batch);
                    if (batch_policy.adaptive_batching) {
                        adaptive_batches_completed_.fetch_add(1, std::memory_order_relaxed);
                    }
                    if (batch_policy.adaptive_flush) {
                        adaptive_flush_batches_completed_.fetch_add(1, std::memory_order_relaxed);
                    }
                    if (batch_policy.latency_target_adjusted) {
                        adaptive_latency_target_batches_completed_.fetch_add(1, std::memory_order_relaxed);
                    }
                    if (batch_policy.fsync_pressure_adjusted) {
                        adaptive_fsync_pressure_batches_completed_.fetch_add(1, std::memory_order_relaxed);
                    }
                    if (batch_policy.read_heavy_adjusted) {
                        adaptive_read_heavy_batches_completed_.fetch_add(1, std::memory_order_relaxed);
                    }
                    if (batch_policy.compaction_pressure_adjusted) {
                        adaptive_compaction_pressure_batches_completed_.fetch_add(1, std::memory_order_relaxed);
                    }
                    if (batch_policy.wal_growth_adjusted) {
                        adaptive_wal_growth_batches_completed_.fetch_add(1, std::memory_order_relaxed);
                    }
                    if (batch_policy.objective_short_delay_adjusted) {
                        adaptive_objective_short_delay_batches_completed_.fetch_add(1, std::memory_order_relaxed);
                    }
                    if (batch_policy.objective_long_delay_adjusted) {
                        adaptive_objective_long_delay_batches_completed_.fetch_add(1, std::memory_order_relaxed);
                    }
                    if (batch_policy.objective_throughput_score > 0) {
                        adaptive_objective_throughput_batches_completed_.fetch_add(1, std::memory_order_relaxed);
                    }
                    maybe_auto_compact();
                    continue;
                }
                if (compact_request) {
                    process_compaction_request(compact_request, false);
                }
            } catch (const KVStoreError& error) {
                if (!write_batch.empty()) {
                    fail_requests(write_batch, error.what());
                } else if (compact_request) {
                    complete_request(compact_request, error.what());
                }
                enter_fatal_state(error.what());
                return;
            } catch (const std::exception& error) {
                const std::string message = std::string("Unexpected writer failure: ") + error.what();
                if (!write_batch.empty()) {
                    fail_requests(write_batch, message);
                } else if (compact_request) {
                    complete_request(compact_request, message);
                }
                enter_fatal_state(message);
                return;
            }
        }
    }

    BatchPolicy current_batch_policy_locked() const {
        BatchPolicy policy {
            options_.max_batch_size,
            options_.max_batch_wal_bytes,
            options_.max_batch_delay_us,
            request_queue_.size(),
            0,
            0,
            0,
            0,
            0,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
        };
        const uint64_t recent_peak_queue_depth = recent_peak_queue_depth_.load(std::memory_order_relaxed);
        const uint64_t recent_read_ratio = effective_recent_read_ratio_per_1000_ops();
        const uint64_t current_obsolete_ratio = current_obsolete_wal_ratio_percent();
        const uint64_t recent_avg_batch_size = recent_avg_batch_size_.load(std::memory_order_relaxed);
        const uint64_t recent_avg_batch_wal_bytes = recent_avg_batch_wal_bytes_.load(std::memory_order_relaxed);
        if (options_.adaptive_batching_enabled &&
            (request_queue_.size() >= options_.adaptive_queue_depth_threshold ||
             recent_peak_queue_depth >= options_.adaptive_queue_depth_threshold)) {
            policy.adaptive_batching = true;
            policy.max_batch_size = saturating_multiply(options_.max_batch_size, options_.adaptive_batch_size_multiplier);
            if (policy.max_batch_size == 0) {
                policy.max_batch_size = 1;
            }
            if (options_.max_batch_wal_bytes == 0) {
                policy.max_batch_wal_bytes = 0;
            } else {
                policy.max_batch_wal_bytes = saturating_multiply(
                    options_.max_batch_wal_bytes,
                    options_.adaptive_batch_wal_bytes_multiplier);
            }
        }

        if (!options_.adaptive_objective_enabled &&
            options_.adaptive_flush_enabled &&
            options_.max_batch_delay_us > 0 &&
            (request_queue_.size() >= options_.adaptive_flush_queue_depth_threshold ||
             recent_peak_queue_depth >= options_.adaptive_flush_queue_depth_threshold)) {
            policy.adaptive_flush = true;
            size_t pressure_steps = request_queue_.size() / options_.adaptive_flush_queue_depth_threshold;
            if (pressure_steps == 0) {
                pressure_steps = std::max<size_t>(
                    1,
                    recent_peak_queue_depth / options_.adaptive_flush_queue_depth_threshold);
            }
            uint32_t effective_delay = options_.max_batch_delay_us;
            for (size_t step = 0; step < pressure_steps; ++step) {
                effective_delay = std::max(
                    options_.adaptive_flush_min_batch_delay_us,
                    effective_delay / options_.adaptive_flush_delay_divisor);
                if (effective_delay <= options_.adaptive_flush_min_batch_delay_us) {
                    break;
                }
            }
            policy.batch_delay_us = effective_delay;
        }

        const uint64_t observed_p95_us = recent_observed_write_latency_p95_us_.load(std::memory_order_relaxed);
        if (options_.adaptive_objective_enabled) {
            const uint64_t observed_pressure =
                observed_fsync_pressure_per_1000_writes_.load(std::memory_order_relaxed);
            const uint64_t observed_queue_depth =
                std::max<uint64_t>(request_queue_.size(), recent_peak_queue_depth);
            const uint64_t queue_scale =
                options_.adaptive_batching_enabled
                    ? options_.adaptive_queue_depth_threshold
                    : options_.max_batch_size;
            const uint64_t pressure_score =
                weighted_signal_score(observed_queue_depth, queue_scale, options_.adaptive_objective_queue_weight) +
                weighted_signal_score(
                    observed_p95_us,
                    options_.adaptive_latency_target_p95_us,
                    options_.adaptive_objective_latency_weight) +
                weighted_signal_score(
                    recent_read_ratio,
                    options_.adaptive_read_heavy_read_per_1000_ops_threshold,
                    options_.adaptive_objective_read_weight);
            const uint64_t throughput_score =
                weighted_deficit_score(
                    recent_avg_batch_size,
                    options_.adaptive_objective_target_batch_size,
                    options_.adaptive_objective_throughput_weight);
            const uint64_t cost_score =
                throughput_score +
                weighted_signal_score(
                    observed_pressure,
                    options_.adaptive_fsync_pressure_per_1000_writes_threshold,
                    options_.adaptive_objective_fsync_weight) +
                weighted_signal_score(
                    current_obsolete_ratio,
                    options_.adaptive_compaction_pressure_obsolete_ratio_percent_threshold,
                    options_.adaptive_objective_compaction_weight) +
                weighted_signal_score(
                    recent_avg_batch_wal_bytes,
                    options_.adaptive_wal_growth_bytes_per_batch_threshold,
                    options_.adaptive_objective_wal_growth_weight);
            policy.objective_pressure_score = pressure_score;
            policy.objective_cost_score = cost_score;
            policy.objective_throughput_score = throughput_score;
            policy.objective_balance_score =
                static_cast<int64_t>(pressure_score) - static_cast<int64_t>(cost_score);

            if (pressure_score >= cost_score + options_.adaptive_objective_short_delay_score_threshold &&
                policy.batch_delay_us > options_.adaptive_flush_min_batch_delay_us) {
                policy.objective_short_delay_adjusted = true;
                policy.objective_mode = 1;
                policy.batch_delay_us = std::max(
                    options_.adaptive_flush_min_batch_delay_us,
                    policy.batch_delay_us / options_.adaptive_objective_short_delay_divisor);
            } else if (cost_score >= pressure_score + options_.adaptive_objective_long_delay_score_threshold &&
                       options_.adaptive_objective_long_delay_multiplier > 1) {
                policy.objective_long_delay_adjusted = true;
                policy.objective_mode = -1;
                uint32_t boosted_delay = saturating_multiply(
                    policy.batch_delay_us,
                    options_.adaptive_objective_long_delay_multiplier);
                if (options_.adaptive_objective_max_batch_delay_us > 0) {
                    boosted_delay = std::min(
                        boosted_delay,
                        options_.adaptive_objective_max_batch_delay_us);
                }
                if (boosted_delay > 0) {
                    policy.batch_delay_us = boosted_delay;
                }
            }
            return policy;
        }

        if (options_.adaptive_read_heavy_read_per_1000_ops_threshold > 0 &&
            recent_read_ratio >= options_.adaptive_read_heavy_read_per_1000_ops_threshold) {
            policy.read_heavy_adjusted = true;
            policy.batch_delay_us = std::max(
                options_.adaptive_flush_min_batch_delay_us,
                policy.batch_delay_us / options_.adaptive_read_heavy_delay_divisor);
            policy.max_batch_size = std::max<size_t>(1, policy.max_batch_size / options_.adaptive_read_heavy_batch_size_divisor);
            if (policy.max_batch_wal_bytes > 0) {
                policy.max_batch_wal_bytes =
                    std::max<uint64_t>(sizeof(WalRecordHeader), policy.max_batch_wal_bytes / options_.adaptive_read_heavy_batch_size_divisor);
            }
        }

        if (options_.adaptive_latency_target_p95_us > 0 &&
            observed_p95_us > options_.adaptive_latency_target_p95_us &&
            policy.batch_delay_us > options_.adaptive_flush_min_batch_delay_us) {
            policy.adaptive_flush = true;
            policy.latency_target_adjusted = true;
            policy.batch_delay_us = std::max(
                options_.adaptive_flush_min_batch_delay_us,
                policy.batch_delay_us / std::max<uint32_t>(2, options_.adaptive_flush_delay_divisor));
        }

        if (!policy.read_heavy_adjusted && !policy.latency_target_adjusted &&
            options_.adaptive_fsync_pressure_per_1000_writes_threshold > 0) {
            const uint64_t observed_pressure =
                observed_fsync_pressure_per_1000_writes_.load(std::memory_order_relaxed);
            if (observed_pressure >= options_.adaptive_fsync_pressure_per_1000_writes_threshold &&
                options_.adaptive_fsync_pressure_delay_multiplier > 1) {
                policy.fsync_pressure_adjusted = true;
                uint32_t boosted_delay = saturating_multiply(
                    policy.batch_delay_us,
                    options_.adaptive_fsync_pressure_delay_multiplier);
                if (options_.adaptive_fsync_pressure_max_batch_delay_us > 0) {
                    boosted_delay = std::min(
                        boosted_delay,
                        options_.adaptive_fsync_pressure_max_batch_delay_us);
                }
                if (boosted_delay > 0) {
                    policy.batch_delay_us = boosted_delay;
                }
            }
        }

        if (!policy.read_heavy_adjusted &&
            options_.adaptive_compaction_pressure_obsolete_ratio_percent_threshold > 0 &&
            current_obsolete_ratio >= options_.adaptive_compaction_pressure_obsolete_ratio_percent_threshold) {
            policy.compaction_pressure_adjusted = true;
            uint32_t boosted_delay = saturating_multiply(
                policy.batch_delay_us,
                options_.adaptive_compaction_pressure_delay_multiplier);
            if (boosted_delay > 0) {
                policy.batch_delay_us = boosted_delay;
            }
        }

        if (!policy.read_heavy_adjusted &&
            options_.adaptive_wal_growth_bytes_per_batch_threshold > 0 &&
            recent_avg_batch_wal_bytes >= options_.adaptive_wal_growth_bytes_per_batch_threshold) {
            policy.wal_growth_adjusted = true;
            uint32_t boosted_delay = saturating_multiply(
                policy.batch_delay_us,
                options_.adaptive_wal_growth_delay_multiplier);
            if (options_.adaptive_wal_growth_max_batch_delay_us > 0) {
                boosted_delay = std::min(boosted_delay, options_.adaptive_wal_growth_max_batch_delay_us);
            }
            if (boosted_delay > 0) {
                policy.batch_delay_us = boosted_delay;
            }
        }
        return policy;
    }

    void collect_write_batch(
        std::unique_lock<std::mutex>& lock,
        const BatchPolicy& batch_policy,
        std::vector<std::shared_ptr<Request>>& write_batch,
        const std::chrono::steady_clock::time_point& deadline) {
        uint64_t batch_wal_bytes = 0;
        while (write_batch.size() < batch_policy.max_batch_size) {
            while (!request_queue_.empty() &&
                   request_queue_.front()->type != RequestType::kCompact &&
                   write_batch.size() < batch_policy.max_batch_size) {
                const uint64_t next_request_bytes = wal_record_size_for_request(*request_queue_.front());
                if (batch_policy.max_batch_wal_bytes > 0 &&
                    !write_batch.empty() &&
                    batch_wal_bytes + next_request_bytes > batch_policy.max_batch_wal_bytes) {
                    break;
                }
                batch_wal_bytes += next_request_bytes;
                write_batch.push_back(request_queue_.front());
                request_queue_.pop_front();
            }

            if (write_batch.size() >= batch_policy.max_batch_size ||
                (batch_policy.max_batch_wal_bytes > 0 && batch_wal_bytes >= batch_policy.max_batch_wal_bytes) ||
                !request_queue_.empty() ||
                batch_policy.batch_delay_us == 0) {
                break;
            }

            if (wait_for_queue_activity_until(lock, deadline)) {
                if (stop_ && request_queue_.empty()) {
                    break;
                }
                continue;
            }
            break;
        }
    }

    void wait_for_queue_activity(std::unique_lock<std::mutex>& lock) {
        const auto wait_begin = std::chrono::steady_clock::now();
        writer_wait_events_.fetch_add(1, std::memory_order_relaxed);
        queue_cv_.wait(lock, [this] { return stop_ || !request_queue_.empty(); });
        record_writer_wait(std::chrono::steady_clock::now() - wait_begin);
    }

    bool wait_for_queue_activity_until(
        std::unique_lock<std::mutex>& lock,
        const std::chrono::steady_clock::time_point& deadline) {
        const auto wait_begin = std::chrono::steady_clock::now();
        writer_wait_events_.fetch_add(1, std::memory_order_relaxed);
        const bool awakened = queue_cv_.wait_until(lock, deadline, [this] { return stop_ || !request_queue_.empty(); });
        record_writer_wait(std::chrono::steady_clock::now() - wait_begin);
        return awakened;
    }

    void record_writer_wait(std::chrono::steady_clock::duration wait_duration) {
        const auto waited_us = std::chrono::duration_cast<std::chrono::microseconds>(wait_duration).count();
        if (waited_us > 0) {
            writer_wait_time_us_.fetch_add(static_cast<uint64_t>(waited_us), std::memory_order_relaxed);
        }
    }

    void process_write_batch(const std::vector<std::shared_ptr<Request>>& batch) {
        open_wal_for_append_if_needed();
        uint64_t batch_bytes_written = 0;
        for (const auto& request : batch) {
            switch (request->type) {
                case RequestType::kPut:
                    batch_bytes_written += append_wal_record(WalRecordType::kPut, request->key, request->value);
                    break;
                case RequestType::kDelete: {
                    const Value empty;
                    batch_bytes_written += append_wal_record(WalRecordType::kDelete, request->key, empty);
                    break;
                }
                case RequestType::kBatch:
                    for (const auto& operation : request->batch_operations) {
                        batch_bytes_written += append_wal_record(operation.type, operation.key, operation.value);
                    }
                    break;
                case RequestType::kCompact:
                    throw KVStoreError("Compact request should not be part of a write batch");
            }
        }
        fsync_file(wal_fd_, wal_file_path_);
        maybe_trigger_failpoint("after_wal_fsync_before_apply");

        {
            std::unique_lock<std::shared_mutex> lock(state_mutex_);
            for (const auto& request : batch) {
                if (request->type == RequestType::kBatch) {
                    for (const auto& operation : request->batch_operations) {
                        if (operation.type == WalRecordType::kPut) {
                            state_[operation.key] = operation.value;
                        } else {
                            state_.erase(operation.key);
                        }
                    }
                    continue;
                }
                if (request->type == RequestType::kPut) {
                    state_[request->key] = request->value;
                } else {
                    state_.erase(request->key);
                }
            }
        }

        committed_write_requests_.fetch_add(batch.size(), std::memory_order_relaxed);
        committed_write_batches_.fetch_add(1, std::memory_order_relaxed);
        wal_fsync_calls_.fetch_add(1, std::memory_order_relaxed);
        wal_bytes_written_.fetch_add(batch_bytes_written, std::memory_order_relaxed);
        wal_bytes_since_compaction_.fetch_add(batch_bytes_written, std::memory_order_relaxed);
        for (const auto& request : batch) {
            if (request->type == RequestType::kBatch) {
                for (const auto& operation : request->batch_operations) {
                    note_latest_wal_record(operation.key, sizeof(WalRecordHeader) + operation.key.size() + operation.value.bytes.size());
                }
                continue;
            }
            note_latest_wal_record(request->key, wal_record_size_for_request(*request));
        }
        last_committed_batch_size_.store(batch.size(), std::memory_order_relaxed);
        update_atomic_max(max_committed_batch_size_, batch.size());
        last_committed_batch_wal_bytes_.store(batch_bytes_written, std::memory_order_relaxed);
        update_atomic_max(max_committed_batch_wal_bytes_, batch_bytes_written);
        last_effective_batch_delay_us_.store(current_batch_delay_us_, std::memory_order_relaxed);
        if (current_batch_delay_us_ > 0) {
            const uint64_t current_min_delay = min_effective_batch_delay_us_.load(std::memory_order_relaxed);
            if (current_min_delay == 0 || current_batch_delay_us_ < current_min_delay) {
                min_effective_batch_delay_us_.store(current_batch_delay_us_, std::memory_order_relaxed);
            }
            update_atomic_max(max_effective_batch_delay_us_, current_batch_delay_us_);
        }
        update_observed_fsync_pressure(batch.size());
        update_recent_batch_history(batch.size(), current_batch_queue_depth_);

        for (const auto& request : batch) {
            complete_request(request, "");
        }
    }

    void update_observed_fsync_pressure(size_t batch_size) {
        if (batch_size == 0) {
            return;
        }
        const uint64_t instantaneous_pressure = (1000 + batch_size - 1) / batch_size;
        const uint64_t previous_pressure = observed_fsync_pressure_per_1000_writes_.load(std::memory_order_relaxed);
        const uint64_t next_pressure =
            previous_pressure == 0 ? instantaneous_pressure : ((previous_pressure * 3) + instantaneous_pressure) / 4;
        observed_fsync_pressure_per_1000_writes_.store(next_pressure, std::memory_order_relaxed);
    }

    void update_recent_batch_history(size_t batch_size, uint64_t queue_depth) {
        const uint64_t total_read_requests = read_requests_.load(std::memory_order_relaxed);
        const uint64_t read_delta = total_read_requests >= last_total_read_requests_seen_
                                        ? total_read_requests - last_total_read_requests_seen_
                                        : 0;
        last_total_read_requests_seen_ = total_read_requests;

        recent_batch_sizes_.push_back(batch_size);
        recent_batch_queue_depths_.push_back(queue_depth);
        recent_batch_wal_bytes_.push_back(last_committed_batch_wal_bytes_.load(std::memory_order_relaxed));
        recent_read_deltas_.push_back(read_delta);
        recent_write_deltas_.push_back(batch_size);
        recent_batch_size_sum_ += batch_size;
        recent_batch_wal_bytes_sum_ += recent_batch_wal_bytes_.back();
        recent_read_sum_ += read_delta;
        recent_write_sum_ += batch_size;

        while (recent_batch_sizes_.size() > options_.adaptive_recent_window_batches) {
            recent_batch_size_sum_ -= recent_batch_sizes_.front();
            recent_batch_sizes_.pop_front();
            recent_batch_queue_depths_.pop_front();
            recent_batch_wal_bytes_sum_ -= recent_batch_wal_bytes_.front();
            recent_batch_wal_bytes_.pop_front();
            recent_read_sum_ -= recent_read_deltas_.front();
            recent_read_deltas_.pop_front();
            recent_write_sum_ -= recent_write_deltas_.front();
            recent_write_deltas_.pop_front();
        }

        recent_window_batch_count_.store(recent_batch_sizes_.size(), std::memory_order_relaxed);
        if (!recent_batch_sizes_.empty()) {
            recent_avg_batch_size_.store(
                recent_batch_size_sum_ / recent_batch_sizes_.size(),
                std::memory_order_relaxed);
            const uint64_t avg_batch_size = recent_batch_size_sum_ / recent_batch_sizes_.size();
            const uint64_t batch_fill_scale =
                std::max<uint64_t>(1, options_.adaptive_objective_target_batch_size > 0
                                          ? options_.adaptive_objective_target_batch_size
                                          : options_.max_batch_size);
            recent_batch_fill_per_1000_.store(
                std::min<uint64_t>(1000, (avg_batch_size * 1000) / batch_fill_scale),
                std::memory_order_relaxed);
            recent_avg_batch_wal_bytes_.store(
                recent_batch_wal_bytes_sum_ / recent_batch_sizes_.size(),
                std::memory_order_relaxed);
        }
        recent_read_requests_.store(recent_read_sum_, std::memory_order_relaxed);
        recent_write_requests_.store(recent_write_sum_, std::memory_order_relaxed);
        const uint64_t total_recent_ops = recent_read_sum_ + recent_write_sum_;
        const uint64_t read_ratio =
            total_recent_ops == 0 ? 0 : (recent_read_sum_ * 1000) / total_recent_ops;
        recent_read_ratio_per_1000_ops_.store(read_ratio, std::memory_order_relaxed);

        uint64_t peak_queue_depth = 0;
        for (uint64_t value : recent_batch_queue_depths_) {
            peak_queue_depth = std::max(peak_queue_depth, value);
        }
        recent_peak_queue_depth_.store(peak_queue_depth, std::memory_order_relaxed);

        if (!recent_batch_sizes_.empty() && recent_batch_size_sum_ > 0) {
            const uint64_t recent_pressure =
                (recent_batch_sizes_.size() * 1000 + recent_batch_size_sum_ - 1) / recent_batch_size_sum_;
            observed_fsync_pressure_per_1000_writes_.store(recent_pressure, std::memory_order_relaxed);
        }
    }

    uint64_t effective_recent_read_ratio_per_1000_ops() const {
        const uint64_t current_total_reads = read_requests_.load(std::memory_order_relaxed);
        const uint64_t pending_reads = current_total_reads >= last_total_read_requests_seen_
                                           ? current_total_reads - last_total_read_requests_seen_
                                           : 0;
        const uint64_t reads = recent_read_requests_.load(std::memory_order_relaxed) + pending_reads;
        const uint64_t writes = recent_write_requests_.load(std::memory_order_relaxed);
        const uint64_t total_ops = reads + writes;
        return total_ops == 0 ? 0 : (reads * 1000) / total_ops;
    }

    uint64_t current_obsolete_wal_ratio_percent() const {
        const uint64_t total_wal_bytes = wal_bytes_since_compaction_.load(std::memory_order_relaxed);
        if (total_wal_bytes == 0) {
            return 0;
        }
        const uint64_t live_wal_bytes = live_wal_bytes_since_compaction_.load(std::memory_order_relaxed);
        const uint64_t obsolete_wal_bytes = total_wal_bytes >= live_wal_bytes ? total_wal_bytes - live_wal_bytes : 0;
        return (obsolete_wal_bytes * 100) / total_wal_bytes;
    }

    void process_compaction_request(const std::shared_ptr<Request>& request, bool is_auto_compaction) {
        std::map<std::string, Value> snapshot_state;
        {
            std::shared_lock<std::shared_mutex> lock(state_mutex_);
            snapshot_state = state_;
        }

        const std::string temp_snapshot_path = db_file_path_ + ".compact.tmp";
        const std::string temp_wal_path = wal_file_path_ + ".tmp";
        uint64_t snapshot_bytes_written = sizeof(SnapshotHeader);
        for (const auto& [key, value] : snapshot_state) {
            snapshot_bytes_written += sizeof(SnapshotEntryHeader) + key.size() + value.bytes.size();
        }
        const uint64_t reclaimed_wal_bytes = wal_bytes_since_compaction_.load(std::memory_order_relaxed);

        int snapshot_fd = -1;
        int empty_wal_fd = -1;
        try {
            snapshot_fd = open_or_throw(temp_snapshot_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
            const SnapshotHeader snapshot_header = make_snapshot_header();
            write_all(snapshot_fd, &snapshot_header, sizeof(snapshot_header));
            for (const auto& [key, value] : snapshot_state) {
                const SnapshotEntryHeader entry = {
                    kSnapshotEntryMagic,
                    static_cast<uint32_t>(key.size()),
                    static_cast<uint32_t>(value.bytes.size()),
                };
                write_all(snapshot_fd, &entry, sizeof(entry));
                if (!key.empty()) {
                    write_all(snapshot_fd, key.data(), key.size());
                }
                if (!value.bytes.empty()) {
                    write_all(snapshot_fd, value.bytes.data(), value.bytes.size());
                }
            }
            fsync_file(snapshot_fd, temp_snapshot_path);
            maybe_trigger_failpoint("after_snapshot_fsync_before_rename");
            ::close(snapshot_fd);
            snapshot_fd = -1;

            if (::rename(temp_snapshot_path.c_str(), db_file_path_.c_str()) != 0) {
                throw io_error("rename", temp_snapshot_path);
            }
            fsync_directory(db_file_path_);
            maybe_trigger_failpoint("after_snapshot_rename_before_wal_reset");

            close_if_open(wal_fd_);
            wal_fd_ = -1;

            empty_wal_fd = open_or_throw(temp_wal_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
            fsync_file(empty_wal_fd, temp_wal_path);
            ::close(empty_wal_fd);
            empty_wal_fd = -1;

            if (::rename(temp_wal_path.c_str(), wal_file_path_.c_str()) != 0) {
                throw io_error("rename", temp_wal_path);
            }
            fsync_directory(wal_file_path_);
            maybe_trigger_failpoint("after_wal_rotation_before_reopen");
            open_wal_for_append();
            wal_bytes_since_compaction_.store(0, std::memory_order_relaxed);
            live_wal_bytes_since_compaction_.store(0, std::memory_order_relaxed);
            latest_wal_record_bytes_.clear();
            total_snapshot_bytes_written_.fetch_add(snapshot_bytes_written, std::memory_order_relaxed);
            total_wal_bytes_reclaimed_by_compaction_.fetch_add(reclaimed_wal_bytes, std::memory_order_relaxed);
        } catch (...) {
            close_if_open(snapshot_fd);
            close_if_open(empty_wal_fd);
            open_wal_for_append_if_needed();
            throw;
        }

        if (is_auto_compaction) {
            auto_compactions_completed_.fetch_add(1, std::memory_order_relaxed);
        } else {
            manual_compactions_completed_.fetch_add(1, std::memory_order_relaxed);
        }
        complete_request(request, "");
    }

    void maybe_auto_compact() {
        if (!should_auto_compact()) {
            return;
        }

        auto request = std::make_shared<Request>();
        request->type = RequestType::kCompact;
        process_compaction_request(request, true);
    }

    void complete_request(const std::shared_ptr<Request>& request, const std::string& error) {
        if (request->type != RequestType::kCompact && error.empty()) {
            record_write_latency(*request);
        }
        {
            std::lock_guard<std::mutex> lock(request->done_mutex);
            request->done = true;
            request->error = error;
        }
        request->done_cv.notify_one();
    }

    void fail_requests(const std::vector<std::shared_ptr<Request>>& requests, const std::string& error) {
        for (const auto& request : requests) {
            complete_request(request, error);
        }
    }

    void enter_fatal_state(const std::string& error) {
        std::deque<std::shared_ptr<Request>> pending_requests;
        {
            std::lock_guard<std::mutex> lock(queue_mutex_);
            if (has_fatal_error_.load(std::memory_order_relaxed)) {
                return;
            }
            fatal_error_message_ = error;
            has_fatal_error_.store(true, std::memory_order_release);
            pending_requests.swap(request_queue_);
        }

        for (const auto& request : pending_requests) {
            complete_request(request, error);
        }
    }

    void throw_if_fatal() {
        if (!has_fatal_error_.load(std::memory_order_acquire)) {
            return;
        }
        std::lock_guard<std::mutex> lock(queue_mutex_);
        throw KVStoreError(fatal_error_message_);
    }

    void throw_if_fatal_locked() {
        if (has_fatal_error_.load(std::memory_order_relaxed)) {
            throw KVStoreError(fatal_error_message_);
        }
    }

    void note_latest_wal_record(const std::string& key, uint64_t record_size) {
        const auto it = latest_wal_record_bytes_.find(key);
        if (it != latest_wal_record_bytes_.end()) {
            live_wal_bytes_since_compaction_.fetch_sub(it->second, std::memory_order_relaxed);
            it->second = record_size;
        } else {
            latest_wal_record_bytes_[key] = record_size;
        }
        live_wal_bytes_since_compaction_.fetch_add(record_size, std::memory_order_relaxed);
    }

    bool should_auto_compact() const {
        const uint64_t total_wal_bytes = wal_bytes_since_compaction_.load(std::memory_order_relaxed);
        const uint64_t live_wal_bytes = live_wal_bytes_since_compaction_.load(std::memory_order_relaxed);
        const uint64_t obsolete_wal_bytes = total_wal_bytes >= live_wal_bytes ? total_wal_bytes - live_wal_bytes : 0;

        if (options_.auto_compact_wal_bytes_threshold > 0 &&
            total_wal_bytes >= options_.auto_compact_wal_bytes_threshold) {
            return true;
        }

        if (options_.auto_compact_invalid_wal_ratio_percent == 0 || total_wal_bytes == 0) {
            return false;
        }

        return obsolete_wal_bytes * 100 >=
               static_cast<uint64_t>(options_.auto_compact_invalid_wal_ratio_percent) * total_wal_bytes;
    }

    uint64_t wal_record_size_for_request(const Request& request) const {
        switch (request.type) {
            case RequestType::kPut:
                return sizeof(WalRecordHeader) + request.key.size() + request.value.bytes.size();
            case RequestType::kDelete:
                return sizeof(WalRecordHeader) + request.key.size();
            case RequestType::kCompact:
                return 0;
            case RequestType::kBatch: {
                uint64_t total = 0;
                for (const auto& operation : request.batch_operations) {
                    total += sizeof(WalRecordHeader) + operation.key.size() + operation.value.bytes.size();
                }
                return total;
            }
        }
        return 0;
    }

    void record_write_latency(const Request& request) {
        const auto elapsed = std::chrono::steady_clock::now() - request.enqueue_time;
        const uint64_t elapsed_us =
            static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(elapsed).count());
        size_t bucket = kWriteLatencyBucketCount - 1;
        for (size_t i = 0; i + 1 < kWriteLatencyBucketCount; ++i) {
            if (elapsed_us <= kWriteLatencyBucketUpperBoundsUs[i]) {
                bucket = i;
                break;
            }
        }
        write_latency_histogram_[bucket].fetch_add(1, std::memory_order_relaxed);

        recent_latency_buckets_.push_back(bucket);
        recent_latency_bucket_counts_[bucket] += 1;
        while (recent_latency_buckets_.size() > options_.adaptive_recent_write_sample_limit) {
            const size_t old_bucket = recent_latency_buckets_.front();
            recent_latency_buckets_.pop_front();
            recent_latency_bucket_counts_[old_bucket] -= 1;
        }
        recent_observed_write_latency_p95_us_.store(
            approximate_latency_percentile_us(recent_latency_bucket_counts_, 95, 100),
            std::memory_order_relaxed);
    }

    void open_wal_for_append() {
        wal_fd_ = open_or_throw(wal_file_path_, O_WRONLY | O_CREAT | O_APPEND, 0644);
    }

    void open_wal_for_append_if_needed() {
        if (wal_fd_ < 0) {
            open_wal_for_append();
        }
    }

    uint64_t current_wal_file_size() const {
        std::error_code ec;
        const auto size = std::filesystem::file_size(wal_file_path_, ec);
        return ec ? 0 : static_cast<uint64_t>(size);
    }

    uint64_t append_wal_record(WalRecordType type, const std::string& key, const Value& value) {
        const WalRecordHeader header = {
            kWalMagic,
            kWalVersion,
            static_cast<uint8_t>(type),
            0,
            static_cast<uint32_t>(key.size()),
            static_cast<uint32_t>(value.bytes.size()),
            checksum_record(type, key, value),
        };
        write_all(wal_fd_, &header, sizeof(header));
        if (!key.empty()) {
            write_all(wal_fd_, key.data(), key.size());
        }
        if (!value.bytes.empty()) {
            write_all(wal_fd_, value.bytes.data(), value.bytes.size());
        }
        return sizeof(header) + key.size() + value.bytes.size();
    }
};

KVStore::KVStore(const std::string& db_path)
    : pimpl_(std::make_unique<Impl>(db_path, KVStoreOptions {})) {}

KVStore::KVStore(const std::string& db_path, KVStoreOptions options)
    : pimpl_(std::make_unique<Impl>(db_path, options)) {}

KVStore::~KVStore() = default;

BatchWriteOperation BatchWriteOperation::Put(std::string key, Value value) {
    BatchWriteOperation operation;
    operation.type = Type::kPut;
    operation.key_kind = KeyKind::kString;
    operation.key = std::move(key);
    operation.value = std::move(value);
    return operation;
}

BatchWriteOperation BatchWriteOperation::Delete(std::string key) {
    BatchWriteOperation operation;
    operation.type = Type::kDelete;
    operation.key_kind = KeyKind::kString;
    operation.key = std::move(key);
    return operation;
}

BatchWriteOperation BatchWriteOperation::PutInt(int key, Value value) {
    BatchWriteOperation operation;
    operation.type = Type::kPut;
    operation.key_kind = KeyKind::kInt;
    operation.key = std::to_string(key);
    operation.value = std::move(value);
    return operation;
}

BatchWriteOperation BatchWriteOperation::DeleteInt(int key) {
    BatchWriteOperation operation;
    operation.type = Type::kDelete;
    operation.key_kind = KeyKind::kInt;
    operation.key = std::to_string(key);
    return operation;
}

BatchWriteOperation BatchWriteOperation::PutBinary(std::vector<uint8_t> key, Value value) {
    BatchWriteOperation operation;
    operation.type = Type::kPut;
    operation.key_kind = KeyKind::kBinary;
    operation.binary_key = std::move(key);
    operation.value = std::move(value);
    return operation;
}

BatchWriteOperation BatchWriteOperation::DeleteBinary(std::vector<uint8_t> key) {
    BatchWriteOperation operation;
    operation.type = Type::kDelete;
    operation.key_kind = KeyKind::kBinary;
    operation.binary_key = std::move(key);
    return operation;
}

void KVStore::Put(int key, Value value) {
    pimpl_->Put(encode_int_key(key), std::move(value));
}

void KVStore::Put(const std::string& key, Value value) {
    pimpl_->Put(encode_string_key(key), std::move(value));
}

void KVStore::Put(const std::vector<uint8_t>& key, Value value) {
    pimpl_->Put(encode_binary_key(key), std::move(value));
}

void KVStore::WriteBatch(const std::vector<BatchWriteOperation>& operations) {
    if (operations.empty()) {
        return;
    }

    std::vector<Impl::BatchMutation> encoded_operations;
    encoded_operations.reserve(operations.size());
    for (const auto& operation : operations) {
        Impl::BatchMutation mutation;
        mutation.type = operation.type == BatchWriteOperation::Type::kPut ? WalRecordType::kPut : WalRecordType::kDelete;
        switch (operation.key_kind) {
            case BatchWriteOperation::KeyKind::kString:
                mutation.key = encode_string_key(operation.key);
                break;
            case BatchWriteOperation::KeyKind::kInt:
                try {
                    mutation.key = encode_int_key(std::stoi(operation.key));
                } catch (const std::exception&) {
                    throw KVStoreError("BatchWriteOperation integer key is invalid: " + operation.key);
                }
                break;
            case BatchWriteOperation::KeyKind::kBinary:
                mutation.key = encode_binary_key(operation.binary_key);
                break;
        }
        mutation.value = operation.value;
        encoded_operations.push_back(std::move(mutation));
    }

    pimpl_->WriteBatch(encoded_operations);
}

std::optional<Value> KVStore::Get(int key) {
    return pimpl_->Get(encode_int_key(key));
}

std::optional<Value> KVStore::Get(const std::string& key) {
    return pimpl_->Get(encode_string_key(key));
}

std::optional<Value> KVStore::Get(const std::vector<uint8_t>& key) {
    return pimpl_->Get(encode_binary_key(key));
}

void KVStore::Delete(int key) {
    pimpl_->Delete(encode_int_key(key));
}

void KVStore::Delete(const std::string& key) {
    pimpl_->Delete(encode_string_key(key));
}

void KVStore::Delete(const std::vector<uint8_t>& key) {
    pimpl_->Delete(encode_binary_key(key));
}

std::vector<std::pair<std::string, Value>> KVStore::Scan(const std::string& start_key, const std::string& end_key) {
    return pimpl_->Scan(start_key, end_key);
}

void KVStore::Compact() {
    pimpl_->Compact();
}

KVStoreMetrics KVStore::GetMetrics() {
    return pimpl_->GetMetrics();
}

std::string MetricsToJson(const KVStoreMetrics& metrics) {
    return kvstore::internal::metrics_to_json(metrics);
}

KVStoreOptions RecommendedOptions(KVStoreProfile profile) {
    return kvstore::internal::recommended_options(profile);
}

std::string OptionsToJson(const KVStoreOptions& options) {
    return kvstore::internal::options_to_json(options);
}
