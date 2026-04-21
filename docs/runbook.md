# Operations Runbook

## Recommended Profiles

通过 `kv_test profile-json <name>` 或 `bash scripts/profile.sh <name>` 获取推荐配置：

- `balanced`: 默认综合配置，适合一般读写混合负载。
- `write-heavy`: 更大的 batch 和更宽松的 compaction 阈值，优先吞吐。
- `read-heavy`: 更小的 batch 和更积极的 flush，优先读延迟。
- `low-latency`: 更短的 batch delay，更适合延迟敏感场景。

## Basic Workflow

### 1. Local Validation

- `bash scripts/ci-build.sh`
- `bash scripts/ci-sanitizers.sh`
- `bash scripts/bench.sh`

### 2. Reliability Validation

- `bash scripts/soak.sh 10`
- `./build/target/bin/kv_test fault-inject <scenario> <db_path>`

### 3. Format Inspection and Rewrite

- `bash scripts/inspect-format.sh <db_path>`
- `bash scripts/rewrite-format.sh <db_path>`

## Capacity and Compaction Guidance

- 读多写少：优先 `read-heavy` 或 `low-latency`。
- 写多且可容忍更高延迟：优先 `write-heavy`。
- 若 WAL 增长过快或无效比例长期偏高，调低 `auto_compact_wal_bytes_threshold` 或 `auto_compact_invalid_wal_ratio_percent`。
- 若 `wal_fsync_calls` 接近 `committed_write_requests`，说明 batching 效果不佳，应检查 batch size、delay 和负载模型。

## Metrics to Watch

- `wal_fsync_calls`
- `committed_write_batches`
- `recent_observed_write_latency_p95_us`
- `observed_fsync_pressure_per_1000_writes`
- `wal_bytes_since_compaction`
- `observed_obsolete_wal_ratio_percent`
- `last_objective_*`

## Troubleshooting

### WAL replay failure

- 先运行 `inspect-format` 确认 snapshot / WAL 版本。
- 若是 `checksum mismatch`，优先保留现场文件并检查是否存在外部截断或损坏。

### Latency unexpectedly high

- 检查 `recent_peak_queue_depth` 和 `writer_wait_time_us`。
- 检查 objective 是否被 throughput 或 fsync cost 拉向更长 delay。

### Compaction too frequent

- 检查 `wal_bytes_since_compaction` 和 `observed_obsolete_wal_ratio_percent`。
- 提高自动 compaction 阈值，或切换到 `write-heavy` 作为起点。
