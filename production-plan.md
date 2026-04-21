# Production Readiness Plan

## Goal

将当前仓库从“可验证、可调优的小型持久化 KV 内核”逐步推进到更接近生产可用的状态，优先补齐可靠性验证、工程化、文件格式治理和数据模型能力。

## Current Gaps

### 1. Data Model

- 已支持 `int` 键、字符串键、二进制键和字符串范围扫描。
- 还缺少更完整的跨类型迁移与比较语义。

### 2. File Format and Compatibility

- 已有磁盘格式规范、版本检查、迁移工具、richer `inspect-format` 输出、`verify-format` 自动校验入口和可执行兼容矩阵。
- 还缺少更完整的跨版本兼容策略和迁移矩阵。

### 3. Reliability Validation

- 缺少系统性的故障注入测试。
- 缺少 compaction 中断、部分 WAL 写入、损坏恢复矩阵验证。
- 缺少长时间 soak test 和高压并发稳定性测试。

### 4. Concurrency and Control Strategy

- 当前自适应策略已经较丰富，但仍属于规则型/打分型控制器。
- 缺少长期负载下的稳定性验证和自动调参能力。

### 5. Engineering and CI

- 缺少稳定 CI 流程来持续执行构建、测试和 sanitizer。
- 已有 coverage 入口、baseline JSON 产出链路，以及基于默认阈值的 baseline 比较入口。
- 已有 CI 中的保守 benchmark regression gate，以及 baseline 趋势汇总入口。
- 还缺少更细粒度的性能门槛和长期可视化趋势对比。

### 6. Operations and Observability

- 现有 metrics 可用于本地分析，但缺少结构化导出和运维文档。
- 缺少容量规划、调参建议和故障排查手册。

### 7. Advanced Semantics

- 已有批量原子写接口。
- 已补充当前持久化/可见性/批量原子性语义文档。
- 已补充事务/快照读的设计边界文档。
- 还没有事务、快照读或更强隔离级别定义。

## Execution Phases

## Phase 1: Engineering Baseline

- 建立 CI，至少覆盖常规构建、`ctest`、ASan、UBSan。
- 固化 benchmark 命令和输出格式，形成性能基线。
- 增加覆盖率统计，重点关注 WAL、恢复、compaction、writer 路径。

Exit criteria:
- 每次提交都能自动完成构建、测试和 sanitizer。
- 仓库内有稳定可复用的 benchmark 和覆盖率入口。

## Phase 2: Reliability Hardening

- 增加崩溃注入测试：WAL 追加中断、`fsync` 前后中断、compaction 中断、rename 前后中断。
- 扩展损坏恢复测试矩阵：WAL 截断、WAL 中段损坏、快照损坏、版本不匹配。
- 增加长期并发压测和 soak test。

Exit criteria:
- 形成固定故障测试集并稳定通过。
- 长时间压测下无明显状态错误、死锁或崩溃。

## Phase 3: File Format Governance

- 编写磁盘格式规范，覆盖快照、WAL、版本号和校验语义。
- 增加格式版本兼容检查和升级路径。
- 提供迁移工具或迁移流程说明。

Exit criteria:
- 磁盘格式有明确文档。
- 新版本能够检测旧格式并给出明确迁移行为。

## Phase 4: Data Model Expansion

- 支持字符串键。
- 支持二进制键。
- 设计统一的键编码与比较语义，为范围扫描做准备。
- 在稳定编码格式基础上增加范围扫描接口。

Exit criteria:
- 字符串键、二进制键和范围扫描不破坏现有恢复、compaction 和并发语义。
- 新数据模型有完整测试覆盖。

## Phase 5: Adaptive Controller Upgrade

- 将当前 objective 控制器进一步扩展为更稳定的多目标控制器。
- 引入长期窗口、抖动抑制和自动调参机制。
- 建立不同负载模型下的性能与稳定性回归基线。

Exit criteria:
- 不同负载下控制器行为可预测、可解释。
- 性能回归有可比较的历史基线。

## Phase 6: Operations Readiness

- 提供结构化 metrics 导出方式。
- 编写运行手册，包括 compaction、恢复、调参、容量规划和排障建议。
- 明确推荐配置模板。

Exit criteria:
- 仅依赖仓库文档即可完成部署、压测和基本排障。

## Phase 7: Advanced Semantics

- 补齐批量原子写接口、恢复测试和文档。
- 评估事务支持边界。
- 明确定义一致性语义，例如快照读或读写可见性保证。

Exit criteria:
- 批量原子写有稳定测试覆盖并写入 README。
- 当前语义基线和事务/快照读边界都已文档化；后续再决定是否进入实现阶段。

## Priority Order

- `P0`: CI、sanitizer、故障注入、长期压测。
- `P1`: 文件格式版本化、迁移工具、字符串键。
- `P2`: 范围扫描、控制器升级、结构化监控。
- `P3`: 事务和更复杂的一致性语义。

## Working Rules

- 每个阶段先补测试和验收标准，再改实现。
- 不把“新功能扩展”和“核心可靠性修复”混在同一个阶段。
- 所有新增公开语义都必须同步更新 `README.md`、测试和 benchmark 输出。
