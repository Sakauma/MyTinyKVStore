# Controller Audit

## Goal

在继续精简 writer 控制器之前，先明确当前有哪些信号、它们如何影响 `batch delay` / `batch size`，以及哪些规则已经存在明显重叠。

## Current Signal Families

- Queue pressure
  - 直接观察 `pending_queue_depth`、`recent_peak_queue_depth`
  - 影响 `adaptive batching` 与 `adaptive flush`
- Latency pressure
  - 观察近期 `p95` 写延迟和 latency histogram
  - 主要推动更短的 `batch delay`
- Fsync pressure
  - 观察 `observed_fsync_pressure_per_1000_writes`
  - 主要推动更长的 `batch delay`
- Read-heavy pressure
  - 观察 `recent_read_ratio_per_1000_ops`
  - 倾向更短 delay、更小 batch
- Compaction pressure
  - 观察 `observed_obsolete_wal_ratio_percent`
  - 倾向更长 delay，减少额外写放大
- WAL growth pressure
  - 观察 `recent_avg_batch_wal_bytes`
  - 倾向更长 delay
- Objective controller
  - 将 queue / latency / read / throughput / fsync / compaction / wal-growth 汇总成综合 score
  - 当前已经是最终裁决层

## Overlap Findings

- `adaptive flush` 与 objective 的 short-delay 决策都在响应 queue / latency 压力。
- `fsync pressure`、`compaction pressure`、`wal growth` 既有单独规则，也会进入 objective cost score。
- `read-heavy` 一方面单独收缩 batch，一方面又进入 objective pressure score。

这意味着当前控制器已经存在“双层调节”：
- 第一层是单信号规则
- 第二层是 objective 汇总规则

## Low-Risk Simplification Candidates

- 保留 objective 作为最终延迟决策层，逐步下线与其重叠的独立 delay 调整规则。
- 让 `adaptive batching` 继续负责 batch size / wal bytes 的扩张，不再同时承担太多 delay 决策。
- 将 `read-heavy` 保留为 batch-size 调节信号，把 delay 决策交给 objective。
- 保留 `adaptive flush_min_batch_delay_us` 作为安全下限，而不是完整独立策略。

## Proposed Next Step

`Phase 4.2` 应先做一次“只保留 objective + batching + safety floor”的实验分支，对比：

- 吞吐
- `p95/p99`
- `observed_fsync_pressure_per_1000_writes`
- `recent_batch_fill_per_1000`
- compaction 次数与 WAL 增长

如果这些指标没有明显恶化，再逐步删除旧的单信号 delay 分支。
