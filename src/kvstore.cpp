#include "kvstore.h"
#include "internal/compaction.h"
#include "internal/format.h"
#include "internal/latency_metrics.h"
#include "internal/io.h"
#include "internal/metrics_helpers.h"
#include "internal/metrics_snapshot.h"
#include "internal/observability.h"
#include "internal/recent_metrics.h"
#include "internal/recovery.h"
#include "internal/request_runtime.h"
#include "internal/wal_accounting.h"
#include "internal/writer_execution.h"
#include "internal/writer_policy.h"
#include "internal/writer_wait.h"

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
        wal_bytes_since_compaction_.store(kvstore::internal::current_wal_file_size(wal_file_path_), std::memory_order_relaxed);
        kvstore::internal::open_wal_for_append(wal_fd_, wal_file_path_);
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
        kvstore::internal::enqueue_and_wait(request_runtime_state(), request);
    }

    void WriteBatch(const std::vector<BatchMutation>& operations) {
        auto request = std::make_shared<Request>();
        request->type = RequestType::kBatch;
        request->batch_operations = operations;
        kvstore::internal::enqueue_and_wait(request_runtime_state(), request);
    }

    std::optional<Value> Get(const std::string& key) {
        kvstore::internal::throw_if_fatal(request_runtime_state());
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
        kvstore::internal::enqueue_and_wait(request_runtime_state(), request);
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
        kvstore::internal::enqueue_and_wait(request_runtime_state(), request);
    }

    KVStoreMetrics GetMetrics() {
        const uint64_t obsolete_ratio = current_obsolete_wal_ratio_percent();
        uint64_t pending_queue_depth = 0;
        {
            std::lock_guard<std::mutex> lock(queue_mutex_);
            pending_queue_depth = request_queue_.size();
        }
        return collect_metrics_snapshot(MetricsSnapshotInputs {
            &read_requests_,
            &enqueued_write_requests_,
            &committed_write_requests_,
            &committed_write_batches_,
            &compact_requests_,
            &wal_fsync_calls_,
            &wal_bytes_written_,
            &wal_bytes_since_compaction_,
            &live_wal_bytes_since_compaction_,
            &last_committed_batch_size_,
            &max_committed_batch_size_,
            &last_committed_batch_wal_bytes_,
            &max_committed_batch_wal_bytes_,
            &max_pending_queue_depth_,
            &manual_compactions_completed_,
            &auto_compactions_completed_,
            &adaptive_batches_completed_,
            &adaptive_flush_batches_completed_,
            &adaptive_latency_target_batches_completed_,
            &adaptive_fsync_pressure_batches_completed_,
            &adaptive_read_heavy_batches_completed_,
            &adaptive_compaction_pressure_batches_completed_,
            &adaptive_wal_growth_batches_completed_,
            &adaptive_objective_short_delay_batches_completed_,
            &adaptive_objective_long_delay_batches_completed_,
            &adaptive_objective_throughput_batches_completed_,
            &writer_wait_events_,
            &writer_wait_time_us_,
            &last_effective_batch_delay_us_,
            &min_effective_batch_delay_us_,
            &max_effective_batch_delay_us_,
            &observed_fsync_pressure_per_1000_writes_,
            &last_objective_pressure_score_,
            &last_objective_cost_score_,
            &last_objective_throughput_score_,
            &last_objective_balance_score_,
            &last_objective_mode_,
            &total_snapshot_bytes_written_,
            &total_wal_bytes_reclaimed_by_compaction_,
            &write_latency_histogram_,
            &recent_read_requests_,
            &recent_write_requests_,
            &recent_read_ratio_per_1000_ops_,
            &recent_observed_write_latency_p95_us_,
            &recent_peak_queue_depth_,
            &recent_avg_batch_size_,
            &recent_batch_fill_per_1000_,
            &recent_avg_batch_wal_bytes_,
            &recent_window_batch_count_,
            obsolete_ratio,
            pending_queue_depth,
        });
    }

private:
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
    RequestQueue request_queue_;
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

    RequestRuntimeState request_runtime_state() {
        return RequestRuntimeState {
            queue_mutex_,
            queue_cv_,
            request_queue_,
            has_fatal_error_,
            fatal_error_message_,
            max_pending_queue_depth_,
            compact_requests_,
            enqueued_write_requests_,
        };
    }

    RequestCompletionCallbacks request_completion_callbacks() {
        return RequestCompletionCallbacks {
            [this](const Request& request) { record_write_latency(request); },
        };
    }

    void writer_loop() {
        while (true) {
            std::vector<RequestPtr> write_batch;
            RequestPtr compact_request;
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
                    kvstore::internal::wait_for_queue_activity(
                        queue_cv_,
                        writer_wait_events_,
                        writer_wait_time_us_,
                        lock,
                        [this] { return stop_ || !request_queue_.empty(); });
                }
                if (stop_ && request_queue_.empty()) {
                    return;
                }

                if (!request_queue_.empty() && request_queue_.front()->type != RequestType::kCompact) {
                    batch_policy = current_batch_policy_locked();
                    const auto deadline =
                        std::chrono::steady_clock::now() + std::chrono::microseconds(batch_policy.batch_delay_us);
                    kvstore::internal::collect_write_batch(
                        lock,
                        request_queue_,
                        stop_,
                        batch_policy,
                        deadline,
                        [this](std::unique_lock<std::mutex>& wait_lock,
                               const std::chrono::steady_clock::time_point& wait_deadline) {
                            return kvstore::internal::wait_for_queue_activity_until(
                                queue_cv_,
                                writer_wait_events_,
                                writer_wait_time_us_,
                                wait_lock,
                                wait_deadline,
                                [this] { return stop_ || !request_queue_.empty(); });
                        },
                        write_batch);
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
                    auto completion_callbacks = request_completion_callbacks();
                    kvstore::internal::process_write_batch(write_batch, WriteBatchExecutionContext {
                        wal_fd_,
                        wal_file_path_,
                        state_,
                        state_mutex_,
                        committed_write_requests_,
                        committed_write_batches_,
                        wal_fsync_calls_,
                        wal_bytes_written_,
                        wal_bytes_since_compaction_,
                        last_committed_batch_size_,
                        max_committed_batch_size_,
                        last_committed_batch_wal_bytes_,
                        max_committed_batch_wal_bytes_,
                        last_effective_batch_delay_us_,
                        min_effective_batch_delay_us_,
                        max_effective_batch_delay_us_,
                        observed_fsync_pressure_per_1000_writes_,
                        current_batch_delay_us_,
                        current_batch_queue_depth_,
                        options_,
                        read_requests_.load(std::memory_order_relaxed),
                        last_total_read_requests_seen_,
                        recent_batch_sizes_,
                        recent_batch_queue_depths_,
                        recent_batch_wal_bytes_,
                        recent_batch_wal_bytes_sum_,
                        recent_read_deltas_,
                        recent_write_deltas_,
                        recent_read_sum_,
                        recent_write_sum_,
                        recent_batch_size_sum_,
                        recent_read_requests_,
                        recent_write_requests_,
                        recent_read_ratio_per_1000_ops_,
                        recent_peak_queue_depth_,
                        recent_avg_batch_size_,
                        recent_batch_fill_per_1000_,
                        recent_avg_batch_wal_bytes_,
                        recent_window_batch_count_,
                        completion_callbacks,
                        [this](const std::string& key, uint64_t record_size) { note_latest_wal_record(key, record_size); },
                    });
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
                    kvstore::internal::maybe_auto_compact(AutoCompactionExecutionContext {
                        options_,
                        wal_bytes_since_compaction_,
                        live_wal_bytes_since_compaction_,
                        [this](const RequestPtr& request, bool is_auto_compaction) {
                            auto completion_callbacks = request_completion_callbacks();
                            kvstore::internal::process_compaction_request(request, is_auto_compaction, CompactionExecutionContext {
                                wal_fd_,
                                db_file_path_,
                                wal_file_path_,
                                state_,
                                state_mutex_,
                                wal_bytes_since_compaction_,
                                live_wal_bytes_since_compaction_,
                                latest_wal_record_bytes_,
                                total_snapshot_bytes_written_,
                                total_wal_bytes_reclaimed_by_compaction_,
                                manual_compactions_completed_,
                                auto_compactions_completed_,
                                completion_callbacks,
                            });
                        },
                    });
                    continue;
                }
                if (compact_request) {
                    auto completion_callbacks = request_completion_callbacks();
                    kvstore::internal::process_compaction_request(compact_request, false, CompactionExecutionContext {
                        wal_fd_,
                        db_file_path_,
                        wal_file_path_,
                        state_,
                        state_mutex_,
                        wal_bytes_since_compaction_,
                        live_wal_bytes_since_compaction_,
                        latest_wal_record_bytes_,
                        total_snapshot_bytes_written_,
                        total_wal_bytes_reclaimed_by_compaction_,
                        manual_compactions_completed_,
                        auto_compactions_completed_,
                        completion_callbacks,
                    });
                }
            } catch (const KVStoreError& error) {
                auto completion_callbacks = request_completion_callbacks();
                if (!write_batch.empty()) {
                    kvstore::internal::fail_requests(write_batch, error.what(), completion_callbacks);
                } else if (compact_request) {
                    kvstore::internal::complete_request(compact_request, error.what(), completion_callbacks);
                }
                kvstore::internal::enter_fatal_state(request_runtime_state(), error.what(), completion_callbacks);
                return;
            } catch (const std::exception& error) {
                const std::string message = std::string("Unexpected writer failure: ") + error.what();
                auto completion_callbacks = request_completion_callbacks();
                if (!write_batch.empty()) {
                    kvstore::internal::fail_requests(write_batch, message, completion_callbacks);
                } else if (compact_request) {
                    kvstore::internal::complete_request(compact_request, message, completion_callbacks);
                }
                kvstore::internal::enter_fatal_state(request_runtime_state(), message, completion_callbacks);
                return;
            }
        }
    }

    BatchPolicy current_batch_policy_locked() const {
        const WriterPolicySignals signals {
            request_queue_.size(),
            recent_peak_queue_depth_.load(std::memory_order_relaxed),
            effective_recent_read_ratio_per_1000_ops(),
            current_obsolete_wal_ratio_percent(),
            recent_avg_batch_size_.load(std::memory_order_relaxed),
            recent_avg_batch_wal_bytes_.load(std::memory_order_relaxed),
            recent_observed_write_latency_p95_us_.load(std::memory_order_relaxed),
            observed_fsync_pressure_per_1000_writes_.load(std::memory_order_relaxed),
        };
        return compute_batch_policy(options_, signals);
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
        const uint64_t live_wal_bytes = live_wal_bytes_since_compaction_.load(std::memory_order_relaxed);
        return obsolete_wal_ratio_percent(total_wal_bytes, live_wal_bytes);
    }

    void note_latest_wal_record(const std::string& key, uint64_t record_size) {
        track_latest_wal_record(
            latest_wal_record_bytes_,
            live_wal_bytes_since_compaction_,
            key,
            record_size);
    }

    bool should_auto_compact() const {
        const uint64_t total_wal_bytes = wal_bytes_since_compaction_.load(std::memory_order_relaxed);
        const uint64_t live_wal_bytes = live_wal_bytes_since_compaction_.load(std::memory_order_relaxed);
        return kvstore::internal::should_auto_compact(options_, total_wal_bytes, live_wal_bytes);
    }

    void record_write_latency(const Request& request) {
        const auto elapsed = std::chrono::steady_clock::now() - request.enqueue_time;
        const uint64_t elapsed_us =
            static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(elapsed).count());
        record_write_latency_sample(LatencyMetricsContext {
            elapsed_us,
            options_.adaptive_recent_write_sample_limit,
            recent_latency_buckets_,
            recent_latency_bucket_counts_,
            write_latency_histogram_,
            recent_observed_write_latency_p95_us_,
        });
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

    std::vector<BatchMutation> encoded_operations;
    encoded_operations.reserve(operations.size());
    for (const auto& operation : operations) {
        BatchMutation mutation;
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
