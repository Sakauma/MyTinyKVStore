# Post Production Plan

## Goal

在已有“可恢复、可验证、可观测”的基础上，把工程继续推进到“更易维护、可持续调优、可长期演进”的状态。接下来不再优先堆新功能，而是优先解决代码结构负担和性能治理闭环。

## Core Principles

- 先拆职责，再继续扩能力。
- 先让性能变化可持续观测，再做更激进的控制器优化。
- 每次结构性重构都必须配套回归测试、benchmark 对比和迁移说明。

## Phase 1: Architecture Slices

### 1.1 Split `src/kvstore.cpp`

- 按职责拆成独立模块：
- `format/`：snapshot、WAL、`inspect-format`、`verify-format`
- `recovery/`：启动恢复与 rewrite
- `writer/`：队列、批量、fsync、compaction 调度
- `metrics/`：指标、JSON 输出、趋势辅助
- `testing/`：failpoint 与测试专用辅助

Exit criteria:
- `src/kvstore.cpp` 不再承担所有职责。
- 每个模块有清晰边界，头文件依赖可解释。

### 1.2 Introduce Internal Design Notes

- 为 writer、format、recovery 三个核心模块各补一份内部设计文档。
- 明确“谁拥有状态”“谁能改磁盘”“谁能触发 compaction”。

Exit criteria:
- 关键模块都有对应设计说明，后续改动不再只能靠读实现猜语义。

Current status:
- 进行中。已补 writer / format / recovery 三份设计说明，后续继续随拆分同步更新。

## Phase 2: Refactor-Safe Test Harness

### 2.1 Stabilize Non-Deterministic Tests

- 继续清理依赖调度时序和固定 `sleep` 的脆弱断言。
- 把并发测试尽量改成“等待指标/状态达成”而不是“等待某个时间点”。

### 2.2 Add Module-Level Tests

- 为 format、recovery、writer policy 增加更细粒度测试。
- 减少所有问题都只能通过 `kv_test` 端到端发现的现状。

Exit criteria:
- 端到端测试继续保留，但模块级测试能更快定位失败点。

## Phase 3: Continuous Performance Governance

### 3.1 Separate Microbench and Stressbench

- 把当前 benchmark 拆成：
- `microbench`：单项能力，如 WAL append、compaction、scan
- `stressbench`：混合真实负载，如当前 `bench`

Current status:
- 已完成。当前仓库已提供 `microbench` / `microbench-json` 入口，覆盖 `wal_append`、`scan`、`recovery` 三类单项能力；原有 `bench` 继续承担混合负载 `stressbench` 角色。

### 3.2 Baseline History Discipline

- 规范 baseline 存档格式、命名和保留策略。
- 增加“本次结果相对最近 N 次均值”的输出，不只看 oldest/latest。

Current status:
- 进行中。趋势摘要已支持 recent-window 均值与 `latest_vs_recent_avg_*` 比例，下一步继续补 baseline 保留策略和 artifact 规范。

### 3.3 Performance Review Gate

- 把吞吐、尾延迟、`fsync` 压力、batch fill 之外，再逐步纳入：
- compaction 时间
- rewrite 时间
- recovery 时间

Exit criteria:
- 性能治理从“单次回归检查”升级成“多维、可追溯、可解释”的长期机制。

## Phase 4: Controller Simplification Before Further Tuning

### 4.1 Audit Current Adaptive Rules

- 盘点现有 queue / latency / fsync / read-heavy / compaction / growth / objective 各类策略。
- 明确哪些信号重叠、哪些分支已经不再值得保留。

Current status:
- 已推进。`microbench` 已补齐 `compaction` / `rewrite` case，并新增独立的 `microbench regression` 命令、脚本和 CI floor；下一步可继续细化各 case 的长期趋势和门槛策略。
- 已完成第一轮审计，见 `docs/internal/controller-audit.md`。当前主要结论是 delay 决策已出现“单信号规则 + objective 规则”双层重叠，下一步应优先收缩 delay 路径，而不是继续叠加新开关。

### 4.2 Reduce Policy Surface

- 优先合并重复调节逻辑，而不是继续新增开关。
- 在不损失可观测性的前提下减少配置项和路径分叉。

Exit criteria:
- 控制器更容易解释，新增调优不再建立在继续膨胀的规则树之上。

## Phase 5: Long-Run Evidence Pipeline

### 5.1 Repeatable Multi-Profile Runs

- 固化一组标准 profile：`balanced`、`write-heavy`、`compaction-heavy`、`recovery-heavy`。
- 每个 profile 都能输出统一结果摘要。

Current status:
- 已完成。`balanced`、`write-heavy`、`compaction-heavy`、`recovery-heavy` 现已全部支持统一的 JSON stress 摘要与批量脚本。

### 5.2 Artifact-Oriented Reporting

- 将 stress、compatibility、baseline、trend 结果收敛成稳定 artifact 文件。
- 为后续 CI 历史归档或外部可视化留接口。

Current status:
- 已完成。artifact 收集脚本现已统一落盘 `microbench`、`stressbench baseline`、`trend summary`、`compatibility matrix`，以及全部标准 stress profile 的 JSON 摘要。

Exit criteria:
- 长期运行结果不是只出现在终端，而是能被后续流程消费。

## Suggested Priority

1. `Phase 1`: 先拆 `src/kvstore.cpp`
2. `Phase 2`: 补 refactor-safe 测试支撑
3. `Phase 3`: 把性能治理升级成长期机制
4. `Phase 4`: 在数据足够后再收缩控制器
5. `Phase 5`: 最后补持续运行证据链

## Working Rules

- 每完成一个结构重构阶段，都必须重新跑 `kv_test`、`ctest`、compatibility matrix 和至少一轮 benchmark。
- 不在同一阶段同时推进“大重构”和“新语义实现”。
- 若后续确实重新评估事务或快照读，必须基于这份计划完成后的模块边界继续推进，而不是在当前单文件主线里硬加。
