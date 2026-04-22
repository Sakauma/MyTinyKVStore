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
- 已继续拆出 observability/profile 辅助模块，`MetricsToJson`、`OptionsToJson` 与 `RecommendedOptions` 不再直接堆在 `src/kvstore.cpp` 中。
- 已将启动恢复中的“建空快照 / 读快照 / 重放 WAL”提炼到内部 recovery 模块，`src/kvstore.cpp` 继续缩小为状态编排层。
- 已将 compaction 的快照重写与 WAL 轮转步骤提炼到内部 compaction 模块，主文件中对应流程继续收敛为编排逻辑。
- 已将 writer policy 的批处理/延迟决策提炼到内部 writer-policy 模块，`current_batch_policy_locked()` 现只负责采样信号并调用统一计算逻辑。
- 已将 metrics 聚合提炼到内部 metrics-snapshot 模块，`GetMetrics()` 现只保留即时队列深度和 obsolete ratio 的采样职责。
- 已将 WAL live/obsolete 字节计算与 auto-compaction 判定提炼到内部 wal-accounting 模块，主文件中的相关逻辑进一步收缩为状态读取。
- 已将 recent-window 批次统计与 fsync 压力更新提炼到内部 recent-metrics 模块，写入后路径中的纯聚合逻辑继续从主文件剥离。
- 已将写延迟直方图和近期 `p95` 更新提炼到内部 latency-metrics 模块，主文件中的写后统计路径继续简化。
- 已新增 `request-runtime`、`writer-wait`、`writer-execution` 三个内部模块，`src/kvstore.cpp` 中的请求生命周期、queue wait 记账、batch 执行/compaction 主体已拆出到独立 helper。

## Phase 2: Refactor-Safe Test Harness

### 2.1 Stabilize Non-Deterministic Tests

- 继续清理依赖调度时序和固定 `sleep` 的脆弱断言。
- 把并发测试尽量改成“等待指标/状态达成”而不是“等待某个时间点”。

### 2.2 Add Module-Level Tests

- 为 format、recovery、writer policy 增加更细粒度测试。
- 减少所有问题都只能通过 `kv_test` 端到端发现的现状。

Exit criteria:
- 端到端测试继续保留，但模块级测试能更快定位失败点。

Current status:
- 已完成第一轮分层。新增 `kv_unit_test`，当前 `ctest` 同时覆盖 `kv_unit_test` 与 `kv_test`。
- 已补 request runtime、writer wait、writer execution、wal accounting 的 module-level tests。
- 已引入 `tests/common/test_support.*`，开始把重复测试辅助从单体 `tests/main.cpp` 中抽出。
- 已将第一批基础 integration tests 提炼到 `tests/integration/basic_kv_tests.cpp`，`kv_test` 现在开始由 registry 聚合分散的 integration slices，而不是只依赖单个 `tests/main.cpp`。
- 已将 benchmark trend / baseline regression integration tests 提炼到 `tests/integration/benchmark_trend_tests.cpp`，并补上可复用的 benchmark entrypoint 头来复用 compare/trend CLI 逻辑。
- 已将 bench/microbench/profile/stress JSON integration tests 提炼到 `tests/integration/json_cli_tests.cpp`，并补上轻量 fixture wrapper，避免为验证 JSON 结构而直接执行完整 benchmark 负载。
- 已将第二批 metrics/controller integration tests 提炼到 `tests/integration/metrics_controller_tests.cpp`，`tests/main.cpp` 进一步收缩为 CLI 入口与未拆分 integration glue。
- 已将第三批 recovery/format migration integration tests 提炼到 `tests/integration/recovery_format_tests.cpp`，并补上可复用的 CLI entrypoint 头以支撑跨文件复用格式检查命令。
- 已将第四批 runtime/concurrency integration tests 提炼到 `tests/integration/runtime_concurrency_tests.cpp`，并补上 runtime entrypoint 头以跨文件复用 failpoint、soak profile 与 stress profile 配置入口。
- 已将 internal format/metrics helper tests 下沉到 `kv_unit_test`，并将 residual durability smoke tests 提炼到 `tests/integration/durability_smoke_tests.cpp`，`tests/main.cpp` 现进一步收缩为 CLI/fixture glue。
- 已将 inspect/verify/rewrite/compatibility 相关 CLI 实现提炼到 `tests/common/format_cli.cpp`，`tests/main.cpp` 不再直接持有磁盘格式探测与兼容矩阵实现。
- 已将 baseline compare、trend 汇总与 microbench trend 相关 CLI 逻辑提炼到 `tests/common/benchmark_cli.cpp`，`tests/main.cpp` 继续收缩为 benchmark 执行与运行时命令编排。
- 已将 `bench` / `microbench` 执行路径与 benchmark JSON fixture 提炼到 `tests/common/benchmark_runtime.cpp`，`tests/main.cpp` 现主要保留 runtime profile、stress/soak 和命令分发。
- 已将 runtime profile、stress/soak、failpoint child 与相关 JSON/summary entrypoint 提炼到 `tests/common/runtime_cli.cpp`，`tests/main.cpp` 进一步收缩为纯 CLI 分发与 integration registry。
- 已将 `kv_test` 的命令分发与 integration 聚合提炼到 `tests/common/test_driver.cpp` 和 `tests/integration/all_tests.cpp`，`tests/main.cpp` 现为最薄入口文件。
- 已将原来的 `tests/common/runtime_cli.cpp` 继续拆成 `tests/common/runtime_profiles.cpp` 与 `tests/common/runtime_stress.cpp`，把 profile/config 逻辑与 stress/fault-inject 执行路径分离。
- 已将 benchmark 共享模型、采集逻辑与 JSON 序列化提炼到 `tests/common/benchmark_shared.*`，`benchmark_runtime.cpp` 现只保留 CLI/fixture 包装。
- 已将 benchmark baseline/microbench 的 JSON 解析、比较与趋势汇总提炼到 `tests/common/benchmark_analysis.*`，`benchmark_cli.cpp` 进一步收缩为命令输出包装。
- 已将磁盘格式检查、verify、rewrite 支撑逻辑与 compatibility matrix 分析提炼到 `tests/common/format_analysis.*`，`format_cli.cpp` 进一步收缩为薄 CLI 包装。

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

Current status:
- 已推进。`microbench` 已补齐 `compaction` / `rewrite` case，并新增独立的 `microbench regression` 命令、脚本、CI floor 与 `microbench trend` 产物；后续可继续细化各 case 的门槛策略。

## Phase 4: Controller Simplification Before Further Tuning

### 4.1 Audit Current Adaptive Rules

- 盘点现有 queue / latency / fsync / read-heavy / compaction / growth / objective 各类策略。
- 明确哪些信号重叠、哪些分支已经不再值得保留。

Current status:
- 已完成第一轮审计，见 `docs/internal/controller-audit.md`。当前主要结论是 delay 决策已出现“单信号规则 + objective 规则”双层重叠，下一步应优先收缩 delay 路径，而不是继续叠加新开关。

### 4.2 Reduce Policy Surface

- 优先合并重复调节逻辑，而不是继续新增开关。
- 在不损失可观测性的前提下减少配置项和路径分叉。

Exit criteria:
- 控制器更容易解释，新增调优不再建立在继续膨胀的规则树之上。

Current status:
- 已开始第一刀简化：`objective` 启用时，`adaptive_flush` 不再直接修改 `batch_delay_us`，delay 缩短统一由 objective 决策；`adaptive_flush_min_batch_delay_us` 继续保留为 safety floor。
- 已完成第二刀去耦合：queue-pressure 的 objective 评分不再依赖 `adaptive_flush_queue_depth_threshold`，而是只由 queue 深度与 `adaptive_batching` 阈值驱动，进一步减少 delay 决策路径交叉。

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
