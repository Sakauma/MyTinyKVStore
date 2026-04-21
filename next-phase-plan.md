# Next Phase Plan

## Goal

在当前“可恢复、可验证、可观测”的基础上，把仓库继续推进到更接近生产级的状态，优先补齐更细粒度的性能门槛、长期稳定性验证和格式演进治理。

## Phase A: Performance Guardrails

### A1. Finer Regression Gates

- 在现有 `write/read throughput + avg latency` 门槛之外，增加 `p95/p99`、`fsync` 压力和 batch 行为检查。
- 让 `compare-baseline` 和 CI gate 能发现“平均值正常但尾延迟恶化”的回归。

Exit criteria:
- `compare-baseline` 能输出更细粒度的性能比例。
- CI gate 会对尾延迟和批处理退化给出非零退出码。

Current status:
- 已完成。

### A2. Trend-Aware Summary

- 在 baseline 趋势汇总基础上，增加“改善 / 持平 / 退化”的摘要判断。
- 为后续可视化或历史归档留出稳定字段。

Exit criteria:
- `trend-baselines` 能输出机器可解析的趋势方向字段。

Current status:
- 已完成。

## Phase B: Long-Run Validation

### B1. Soak Profiles

- 把 soak test 扩展成多个负载 profile，而不只是一种默认混合负载。
- 固化至少一组 write-heavy 和一组 read-heavy 长时验证。

Current status:
- 已完成。

### B2. Concurrency Stress

- 补更强的并发稳定性回归，重点覆盖 writer 控制器和 compaction 交错场景。
- 如可行，增加 TSan 入口或等价的并发专向检查。

Exit criteria:
- 长时运行和并发 stress 有稳定脚本入口，且能重启校验。

Current status:
- 已完成。

## Phase C: Format Evolution

### C1. Compatibility Matrix Expansion

- 把现有 `current_v2` / `legacy_v1` 扩到更多格式情形，例如空 WAL、legacy after rewrite、截断 WAL。
- 明确每种情形的 `inspect-format` / `verify-format` / `rewrite-format` 期望行为。

Current status:
- 已完成。

### C2. Migration Policy

- 写清未来格式升级时允许和不允许的兼容策略。
- 明确何时必须 rewrite，何时允许只读兼容。

Current status:
- 已完成。

Exit criteria:
- 兼容矩阵和迁移策略都有文档与自动回归。

## Phase D: Advanced Semantics Decision

### D1. Transaction Readiness Review

- 在现有单 writer 内核下，重新评估是否值得进入事务实现。
- 若不进入实现，至少明确“为什么继续停留在 `WriteBatch`”。

### D2. Snapshot Read Review

- 评估快照读是否真的有用户价值，以及它与 compaction / 内存占用的权衡。

Exit criteria:
- 给出进入或暂缓事务 / 快照读实现的明确结论。

## Priority Order

1. `A1`: 更细粒度的 benchmark regression gate
2. `B1/B2`: 更强的长期稳定性和并发验证
3. `C1/C2`: 兼容矩阵扩展和迁移策略
4. `A2`: 趋势方向总结
5. `D1/D2`: 事务与快照读决策
