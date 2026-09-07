# Controller Audit

## 当前职责划分

Writer policy 使用同一组近期信号，但 delay 与 batch 容量的所有权已经明确：

- `adaptive batching` 根据 queue depth 扩大 `max_batch_size` 和 `max_batch_wal_bytes`。
- `read-heavy` 在读比例达到阈值时缩小 `max_batch_size`，并在 WAL cap 非零时同比缩小该 cap；两种 controller 模式都会执行这一步并设置 `read_heavy_adjusted`。
- `adaptive_objective_enabled=true` 时，objective 汇总 queue、latency、read、throughput、fsync、compaction 和 WAL growth 分数，独占最终 `batch_delay_us` 决策。独立的 adaptive-flush、fsync、compaction、WAL-growth 和 read-heavy delay 规则不会再叠加修改 delay。
- objective 关闭时保留旧的单信号 delay 路径；read-heavy 同时复用相同的 batch-size/WAL-cap 收缩，再按 `adaptive_read_heavy_delay_divisor` 缩短 delay。

因此，objective 模式没有跳过 read-heavy：它保留容量保护，只取消第二套 delay 调节。`adaptive_read_heavy_batch_size_divisor` 仍是推荐 profile 的有效参数；只服务旧 delay 路径的 profile 赋值已移除。

## 信号与可观测性

| 信号 | 主要输入 | 结果 |
|---|---|---|
| Queue pressure | `pending_queue_depth`、`recent_peak_queue_depth` | 扩大 batch；objective 可缩短 delay |
| Latency pressure | 近期写 `p95` | objective 倾向缩短 delay |
| Fsync pressure | `observed_fsync_pressure_per_1000_writes` | objective 或旧模式倾向延长 delay |
| Read-heavy | `recent_read_ratio_per_1000_ops` | 始终收缩 batch size/WAL cap；delay 由当前模式决定 |
| Compaction pressure | `observed_obsolete_wal_ratio_percent` | objective 或旧模式倾向延长 delay |
| WAL growth | `recent_avg_batch_wal_bytes` | objective 或旧模式倾向延长 delay |

诊断时同时观察 `last_objective_mode`、各 objective score、`last_effective_batch_delay_us`、`recent_avg_batch_size` 和 `recent_avg_batch_wal_bytes`。只看 delay 无法判断 read-heavy 容量限制是否生效。

## 保留边界

- `adaptive_flush_min_batch_delay_us` 是 delay 的安全下限，不是另一个最终裁决层。
- `max_batch_size` 始终至少为 1；非零 WAL cap 始终至少能容纳一个 `MutationHeader`。
- Objective 与旧模式必须共享同一个 read-heavy batch 限制函数，避免再次产生两套容量语义。
- 修改权重或阈值后，需要同时比较吞吐、p95/p99、batch fill、fsync pressure、WAL growth 和 compaction 次数；短时单一负载不能证明控制器稳定。
