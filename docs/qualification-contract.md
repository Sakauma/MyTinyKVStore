# Qualification Contract

## Purpose

本文件冻结“满足目标要求”的验收口径。后续重构、压测和最终验收都以这里的定义为准，不再在开发过程中临时改变标准。

## Frozen Interpretations

### 1. Transaction Operation

- “对象 `GET/PUT/DEL` 的多线程事务操作”按严格口径解释。
- 最终达标版本必须提供显式事务边界，而不只是单操作原子性。
- 最低可接受事务接口：
- `BeginTransaction`
- `Commit`
- `Rollback`
- 单事务内至少支持多次 `Put/Delete`。
- `Get` 至少要明确：
- 是否允许事务内读取未提交写入
- 是否允许事务外并发读
- 事务的隔离级别和可见性语义必须写入仓库文档。

### 2. Single Large File

- “统一存储在一个大文件中”按稳态口径解释。
- 稳态运行和重启恢复依赖的主存储必须是单一大文件。
- compaction / rewrite 期间允许短暂存在临时文件，但切换完成后目录中不得继续依赖额外 WAL 文件。
- “快照文件 + WAL 文件”不计入达标。

### 3. Value Supports Arbitrary Data Structures

- 存储引擎按 schema-agnostic 方式处理 `Value`。
- 达标口径统一解释为：`Value` 支持任意应用层序列化后的二进制负载。
- 引擎不要求理解具体 C/C++ 结构体布局，但必须支持透明存取任意字节序列。

### 4. Integer-Key Compliance Mode

- 最终验收以整型键模式为准。
- 允许仓库继续保留字符串键或二进制键扩展能力，但它们不能成为达标路径的前置依赖。
- 单文件格式、索引、压测脚本和验收报告都必须能在仅使用整型键的模式下独立成立。

### 5. MPMC Concurrency

- “多生产者多消费者模型”按执行路径口径解释，而不只是 API 同时被多线程调用。
- 最终达标版本的写入/事务提交路径必须存在多个消费者工作线程，不能只依赖单 writer 串行提交。
- 允许按分区、分片或 worker 池实现，但必须在文档中明确：
- 任务如何分发
- 冲突如何处理
- 提交顺序和恢复顺序如何定义

### 6. 12h / 10M Qualification Run

- “持续读写压力测试”按正式归档口径解释。
- 至少一轮正式达标压测必须满足：
- 运行时间 `>= 12` 小时
- 成功插入对象数 `>= 10,000,000`
- 压测结束后重启校验通过
- 结果必须归档到仓库约定目录，并包含：
- 配置
- 机器环境
- 摘要指标
- 最终校验结果

## Non-Compliance Examples

- 只有 `WriteBatch`，没有显式事务接口：不达标。
- 只有 `snapshot + .wal`：不达标。
- 只有多线程调用入口，后台仍是单 writer：不达标。
- 跑过短时 soak / stress，但没有正式 `12h / 10M` 归档：不达标。

## Change Policy

- 这个口径文件只能通过显式评审修改。
- 若后续确需放宽或收紧要求，必须同步更新：
- [requirement-compliance-plan.md](/home/sakauma/code/lpue/requirement-compliance-plan.md)
- README 中的达标说明
- 最终验收文档和脚本
