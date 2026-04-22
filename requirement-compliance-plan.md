# Requirement Compliance Plan

## Goal

把当前工程从“工程化较完善的小型持久化 KV 引擎”推进到“满足指定项目要求的达标实现”。本计划只对照以下三条硬性要求，不把现有扩展能力误算为达标项：

1. 基于 C/C++，支持 KV 形式的小对象存储，提供对象 `GET/PUT/DEL` 的多线程事务操作。
2. 所有小对象统一存储在一个大文件中；带内存缓存；`Key` 为整型，`Value` 支持任意数据结构。
3. 参考文件系统模型设计大文件的物理对象存储、元数据和寻址；支持多线程 `MPMC`；完成不少于 `12` 小时、插入不少于 `1000` 万对象的持续读写压力测试。

## Current Gap Summary

### Requirement 1: KV + GET/PUT/DEL + 多线程事务

- 当前已支持 `GET/PUT/DEL` 和并发调用。
- 当前只有 `WriteBatch` 原子批量写，不是严格意义上的事务系统。
- 当前写路径是“多生产者 + 单 writer 串行提交”，不是严格意义上的多消费者写执行模型。

Gap:
- 缺少事务边界定义与实现。
- 缺少真正的 `MPMC` 写入/提交架构或明确可替代的验收说明。

### Requirement 2: 单大文件 + 整型 Key + 任意 Value + Cache

- 当前 `Value` 以二进制字节形式存储，可承载任意序列化数据。
- 当前已有内存缓存和持久化。
- 当前存储模型是“快照文件 + WAL 文件”，不是单一大文件。
- 当前公开 API 同时支持 `int` / `string` / `binary` 三类键。

Gap:
- 缺少“单文件容器”存储模型。
- 缺少“整型键为主验收模式”的明确边界。

### Requirement 3: 文件系统式物理模型 + 12h/1000 万验证

- 当前已有版本化格式、恢复、compaction、格式检查和迁移工具。
- 当前尚未实现更接近文件系统的单文件对象布局、空闲空间管理和对象寻址模型。
- 当前已有 `soak` / `stress` 工具，但没有正式的 `12h` / `1000` 万对象达标证据。

Gap:
- 缺少单文件容器内的元数据区、对象区、空闲空间管理和对象寻址规范。
- 缺少正式压测脚本、机器配置、结果归档和验收记录。

## Non-Negotiable Acceptance Rules

- “快照文件 + WAL 文件”不算“统一存储在一个大文件中”。
- “支持整型键”不等于“以整型键为验收主模型”；必须明确整型键模式的 API 和磁盘编码。
- `WriteBatch` 原子提交不等于“事务操作”。
- 短时 `soak` / `stress` 不等于“已完成 12 小时、1000 万对象压测”。
- 仅有 benchmark 输出而无归档结果，不算通过最终验收。

## Phase 0: Freeze the Requirement Contract

### Refactor / Design Work

- 补一份正式的“达标口径”文档，明确：
- “事务操作”是指单操作原子持久化，还是需要多操作事务。
- “一个大文件”是否允许临时 compaction 文件在切换期间存在。
- “Value 支持任意数据结构”统一解释为“任意二进制序列化负载”。
- “MPMC”是指 API 层多线程，还是实际存储执行路径也必须多消费者。

### Experiments / Validation

- 无代码实验，先完成口径冻结。

### Exit Criteria

- 仓库内有明确文档，后续实现和验收都按同一口径执行。

## Phase 1: Converge to a Single-File Container

### Refactor / Design Work

- 将当前“双文件快照 + WAL”重构为“单文件容器”模型。
- 在单文件内划分至少四类逻辑区域：
- `superblock`：版本、校验、布局信息。
- `metadata/index`：整型 key 到对象位置的映射。
- `object data`：对象负载区。
- `journal`：崩溃恢复所需的日志区。
- 允许 compaction 过程中使用临时文件，但稳定运行态只能保留一个主数据文件。
- 增加单文件格式检查、升级和 rewrite 工具，替代现有 snapshot/WAL 分离式检查。

### Experiments / Validation

- 单文件创建、重启恢复、rewrite 后重启、异常中断后重启。
- 校验 steady-state 目录中只有一个主数据文件。

### Exit Criteria

- 稳态只保留一个主数据文件。
- 崩溃恢复不再依赖外部 WAL 辅助文件。
- 现有 `inspect/verify/rewrite` 能识别和校验单文件格式。

## Phase 2: Build a File-System-Like Object Layout

### Refactor / Design Work

- 参考文件系统设计对象物理布局，至少补齐：
- 对象元数据结构：对象长度、状态位、版本号、校验信息。
- 对象数据寻址：offset/extent 形式的寻址方法。
- 空闲空间管理：free list、bitmap 或 extent tree 三选一。
- 元数据与对象区的更新顺序和恢复语义。
- 明确缓存模型：
- metadata cache
- object/value cache
- eviction 策略
- 脏页/脏对象写回策略

### Experiments / Validation

- 大量随机 `PUT/GET/DEL` 后检查对象可达性和空间回收正确性。
- compaction 或空间整理后检查对象寻址不丢失。
- cache 命中/淘汰 correctness 测试。

### Exit Criteria

- 仓库内有完整的单文件物理布局文档。
- 对象元数据、数据寻址、空闲空间回收都有测试和格式检查支持。

## Phase 3: Narrow to Integer-Key Compliance Mode

### Refactor / Design Work

- 增加“整型键达标模式”，将其作为目标验收接口。
- 对外至少保留：
- `Put(int key, Value value)`
- `Get(int key)`
- `Delete(int key)`
- 若继续保留字符串/二进制键，只能作为扩展能力，不能影响整型键模式的格式、索引和压测口径。
- 确保整型键在磁盘上的编码、排序和索引路径独立清晰。

### Experiments / Validation

- 整型键模式下的功能、恢复、重启、压测、格式检查单独跑通。

### Exit Criteria

- 存在一套不依赖字符串/二进制键扩展能力的整型键达标路径。

## Phase 4: Add a Real Concurrency and Transaction Model

### Refactor / Design Work

- 明确是否要实现真正的事务：
- 如果要求“多线程事务操作”是严格事务，则新增显式事务接口，如 `Begin/Commit/Rollback` 或 `Txn`。
- 如果验收接受“单操作事务 + 批量原子事务”，则至少把 `WriteBatch` 升级为正式事务接口并写清隔离和可见性语义。
- 将当前单 writer 架构升级为满足要求的并发模型：
- 真正的 `MPMC` worker 队列，或
- 分片/分区后的多 writer + 全局事务协调。
- 增加并发控制层：
- key 级锁或分区锁
- 提交顺序与恢复顺序
- 冲突处理和死锁策略

### Experiments / Validation

- 多线程事务冲突测试。
- 并发 `GET/PUT/DEL` correctness 测试。
- 长时间并发稳定性测试。
- 若支持事务，补中止、回滚、崩溃恢复测试。

### Exit Criteria

- 仓库文档能明确回答：事务边界、隔离级别、并发模型、冲突语义。
- 并发执行路径不再只是“多生产者 + 单 writer”。

## Phase 5: Build the Formal 12h / 10M Stress Pipeline

### Refactor / Design Work

- 新增正式达标脚本，例如：
- `scripts/qualification-run.sh`
- 脚本需要固定：
- 运行时长：不少于 `12` 小时
- 目标插入数：不少于 `10000000`
- 线程模型：生产者数、消费者数、读写比例
- 统计输出：成功写入数、失败数、吞吐、尾延迟、恢复结果、最终对象数
- 增加归档目录，例如：
- `artifacts/qualification/<timestamp>/`
- 输出：
- 配置快照
- 指标 JSON
- 日志
- 最终校验结果
- 环境说明

### Experiments / Validation

- 至少一次完整 `12h` 正式跑。
- 至少一次中途中断后恢复再继续的扩展验证。
- 至少一次结束后重启校验。

### Exit Criteria

- 仓库内有可复现的正式达标脚本。
- 至少有一份归档结果证明：
- 持续运行 `>= 12h`
- 插入对象 `>= 10000000`
- 结束后数据校验通过

## Phase 6: Final Acceptance Freeze

### Refactor / Design Work

- 补最终验收文档，逐条映射三项要求到：
- 代码模块
- 运行命令
- 测试项
- 归档证据
- 把现有 README 中与“当前特性”相关的描述拆成：
- 当前工程能力
- 达标模式能力

### Experiments / Validation

- 重新执行一次最小验收集：
- 单文件格式检查
- 功能测试
- 并发事务测试
- 恢复测试
- `12h/10M` 正式压测

### Exit Criteria

- 任一外部评审者仅根据仓库文档和 artifact，就能判断该项目是否满足三条要求。

## Recommended Execution Order

1. `Phase 0`: 先冻结达标口径，避免边做边改要求。
2. `Phase 1`: 先把双文件模型收敛成单文件容器。
3. `Phase 2`: 再把单文件容器补成文件系统式物理布局。
4. `Phase 3`: 收紧整型键达标模式。
5. `Phase 4`: 再进入事务和真正 `MPMC` 并发模型。
6. `Phase 5`: 最后做正式 `12h/10M` 压测与归档。
7. `Phase 6`: 收口文档和最终验收。

## Immediate Next Actions

- 第一，先补一份“达标口径冻结”文档，特别是把“事务操作”和“MPMC”的验收定义写死。
- 第二，设计单文件容器格式草案，明确如何从现有 snapshot/WAL 迁移。
- 第三，单独做一份 `qualification` 压测方案，提前把 `12h/10M` 的实验环境、线程数、对象大小分布和校验方式固定下来。
