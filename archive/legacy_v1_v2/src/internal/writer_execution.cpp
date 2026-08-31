#include "writer_execution.h"

#include "compaction.h"
#include "format.h"
#include "io.h"
#include "metrics_helpers.h"
#include "recent_metrics.h"
#include "wal_accounting.h"

#include <filesystem>
#include <fcntl.h>

namespace kvstore::internal {

uint64_t current_wal_file_size(const std::string& wal_file_path) {
    std::error_code ec;
    const auto size = std::filesystem::file_size(wal_file_path, ec);
    return ec ? 0 : static_cast<uint64_t>(size);
}

void open_wal_for_append(int& wal_fd, const std::string& wal_file_path) {
    wal_fd = open_or_throw(wal_file_path, O_WRONLY | O_CREAT | O_APPEND, 0644);
}

void open_wal_for_append_if_needed(int& wal_fd, const std::string& wal_file_path) {
    if (wal_fd < 0) {
        open_wal_for_append(wal_fd, wal_file_path);
    }
}

uint64_t append_wal_record(int wal_fd, WalRecordType type, const std::string& key, const Value& value) {
    const WalRecordHeader header = {
        kWalMagic,
        kWalVersion,
        static_cast<uint8_t>(type),
        0,
        static_cast<uint32_t>(key.size()),
        static_cast<uint32_t>(value.bytes.size()),
        checksum_record(type, key, value),
    };
    write_all(wal_fd, &header, sizeof(header));
    if (!key.empty()) {
        write_all(wal_fd, key.data(), key.size());
    }
    if (!value.bytes.empty()) {
        write_all(wal_fd, value.bytes.data(), value.bytes.size());
    }
    return sizeof(header) + key.size() + value.bytes.size();
}

uint64_t wal_record_size_for_request(const Request& request) {
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

void collect_write_batch(
    std::unique_lock<std::mutex>& lock,
    RequestQueue& request_queue,
    bool stop,
    const BatchPolicy& batch_policy,
    const std::chrono::steady_clock::time_point& deadline,
    const std::function<bool(std::unique_lock<std::mutex>&, const std::chrono::steady_clock::time_point&)>&
        wait_for_queue_activity_until,
    std::vector<RequestPtr>& write_batch) {
    uint64_t batch_wal_bytes = 0;
    while (write_batch.size() < batch_policy.max_batch_size) {
        while (!request_queue.empty() &&
               request_queue.front()->type != RequestType::kCompact &&
               write_batch.size() < batch_policy.max_batch_size) {
            const uint64_t next_request_bytes = wal_record_size_for_request(*request_queue.front());
            if (batch_policy.max_batch_wal_bytes > 0 &&
                !write_batch.empty() &&
                batch_wal_bytes + next_request_bytes > batch_policy.max_batch_wal_bytes) {
                break;
            }
            batch_wal_bytes += next_request_bytes;
            write_batch.push_back(request_queue.front());
            request_queue.pop_front();
        }

        if (write_batch.size() >= batch_policy.max_batch_size ||
            (batch_policy.max_batch_wal_bytes > 0 && batch_wal_bytes >= batch_policy.max_batch_wal_bytes) ||
            !request_queue.empty() ||
            batch_policy.batch_delay_us == 0) {
            break;
        }

        if (wait_for_queue_activity_until(lock, deadline)) {
            if (stop && request_queue.empty()) {
                break;
            }
            continue;
        }
        break;
    }
}

void process_write_batch(const std::vector<RequestPtr>& batch, WriteBatchExecutionContext context) {
    open_wal_for_append_if_needed(context.wal_fd, context.wal_file_path);
    uint64_t batch_bytes_written = 0;
    for (const auto& request : batch) {
        switch (request->type) {
            case RequestType::kPut:
                batch_bytes_written += append_wal_record(context.wal_fd, WalRecordType::kPut, request->key, request->value);
                break;
            case RequestType::kDelete: {
                const Value empty;
                batch_bytes_written += append_wal_record(context.wal_fd, WalRecordType::kDelete, request->key, empty);
                break;
            }
            case RequestType::kBatch:
                for (const auto& operation : request->batch_operations) {
                    batch_bytes_written += append_wal_record(context.wal_fd, operation.type, operation.key, operation.value);
                }
                break;
            case RequestType::kCompact:
                throw KVStoreError("Compact request should not be part of a write batch");
        }
    }
    fsync_file(context.wal_fd, context.wal_file_path);
    maybe_trigger_failpoint("after_wal_fsync_before_apply");

    {
        std::unique_lock<std::shared_mutex> lock(context.state_mutex);
        for (const auto& request : batch) {
            if (request->type == RequestType::kBatch) {
                for (const auto& operation : request->batch_operations) {
                    if (operation.type == WalRecordType::kPut) {
                        context.state[operation.key] = operation.value;
                    } else {
                        context.state.erase(operation.key);
                    }
                }
                continue;
            }
            if (request->type == RequestType::kPut) {
                context.state[request->key] = request->value;
            } else {
                context.state.erase(request->key);
            }
        }
    }

    context.committed_write_requests.fetch_add(batch.size(), std::memory_order_relaxed);
    context.committed_write_batches.fetch_add(1, std::memory_order_relaxed);
    context.wal_fsync_calls.fetch_add(1, std::memory_order_relaxed);
    context.wal_bytes_written.fetch_add(batch_bytes_written, std::memory_order_relaxed);
    context.wal_bytes_since_compaction.fetch_add(batch_bytes_written, std::memory_order_relaxed);
    for (const auto& request : batch) {
        if (request->type == RequestType::kBatch) {
            for (const auto& operation : request->batch_operations) {
                context.note_latest_wal_record(
                    operation.key,
                    sizeof(WalRecordHeader) + operation.key.size() + operation.value.bytes.size());
            }
            continue;
        }
        context.note_latest_wal_record(request->key, wal_record_size_for_request(*request));
    }
    context.last_committed_batch_size.store(batch.size(), std::memory_order_relaxed);
    update_atomic_max(context.max_committed_batch_size, batch.size());
    context.last_committed_batch_wal_bytes.store(batch_bytes_written, std::memory_order_relaxed);
    update_atomic_max(context.max_committed_batch_wal_bytes, batch_bytes_written);
    context.last_effective_batch_delay_us.store(context.current_batch_delay_us, std::memory_order_relaxed);
    if (context.current_batch_delay_us > 0) {
        const uint64_t current_min_delay = context.min_effective_batch_delay_us.load(std::memory_order_relaxed);
        if (current_min_delay == 0 || context.current_batch_delay_us < current_min_delay) {
            context.min_effective_batch_delay_us.store(context.current_batch_delay_us, std::memory_order_relaxed);
        }
        update_atomic_max(context.max_effective_batch_delay_us, context.current_batch_delay_us);
    }
    update_observed_fsync_pressure_metric(context.observed_fsync_pressure_per_1000_writes, batch.size());
    update_recent_batch_history(RecentBatchHistoryContext {
        context.options,
        context.current_total_read_requests,
        context.last_committed_batch_wal_bytes.load(std::memory_order_relaxed),
        batch.size(),
        context.current_batch_queue_depth,
        context.last_total_read_requests_seen,
        context.recent_batch_sizes,
        context.recent_batch_queue_depths,
        context.recent_batch_wal_bytes,
        context.recent_batch_wal_bytes_sum,
        context.recent_read_deltas,
        context.recent_write_deltas,
        context.recent_read_sum,
        context.recent_write_sum,
        context.recent_batch_size_sum,
        context.recent_read_requests,
        context.recent_write_requests,
        context.recent_read_ratio_per_1000_ops,
        context.recent_peak_queue_depth,
        context.recent_avg_batch_size,
        context.recent_batch_fill_per_1000,
        context.recent_avg_batch_wal_bytes,
        context.recent_window_batch_count,
        context.observed_fsync_pressure_per_1000_writes,
    });

    for (const auto& request : batch) {
        complete_request(request, "", context.completion_callbacks);
    }
}

void process_compaction_request(
    const RequestPtr& request,
    bool is_auto_compaction,
    CompactionExecutionContext context) {
    StateMap snapshot_state;
    {
        std::shared_lock<std::shared_mutex> lock(context.state_mutex);
        snapshot_state = context.state;
    }

    const uint64_t reclaimed_wal_bytes = context.wal_bytes_since_compaction.load(std::memory_order_relaxed);

    try {
        close_if_open(context.wal_fd);
        context.wal_fd = -1;
        const uint64_t snapshot_bytes_written =
            rewrite_snapshot_and_reset_wal(context.db_file_path, context.wal_file_path, snapshot_state);
        open_wal_for_append(context.wal_fd, context.wal_file_path);
        context.wal_bytes_since_compaction.store(0, std::memory_order_relaxed);
        context.live_wal_bytes_since_compaction.store(0, std::memory_order_relaxed);
        context.latest_wal_record_bytes.clear();
        context.total_snapshot_bytes_written.fetch_add(snapshot_bytes_written, std::memory_order_relaxed);
        context.total_wal_bytes_reclaimed_by_compaction.fetch_add(reclaimed_wal_bytes, std::memory_order_relaxed);
    } catch (...) {
        open_wal_for_append_if_needed(context.wal_fd, context.wal_file_path);
        throw;
    }

    if (is_auto_compaction) {
        context.auto_compactions_completed.fetch_add(1, std::memory_order_relaxed);
    } else {
        context.manual_compactions_completed.fetch_add(1, std::memory_order_relaxed);
    }
    complete_request(request, "", context.completion_callbacks);
}

bool maybe_auto_compact(AutoCompactionExecutionContext context) {
    const uint64_t total_wal_bytes = context.wal_bytes_since_compaction.load(std::memory_order_relaxed);
    const uint64_t live_wal_bytes = context.live_wal_bytes_since_compaction.load(std::memory_order_relaxed);
    if (!should_auto_compact(context.options, total_wal_bytes, live_wal_bytes)) {
        return false;
    }

    auto request = std::make_shared<Request>();
    request->type = RequestType::kCompact;
    context.process_compaction_request(request, true);
    return true;
}

}  // namespace kvstore::internal
