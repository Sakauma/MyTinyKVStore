# Transaction and Snapshot Read Boundary

## Current Baseline

- 当前提交单元是单次公开 API 调用：`Put`、`Delete`、`WriteBatch`。
- `WriteBatch` 已经提供“单调用内多操作原子提交”，但不跨多个调用扩展。
- 当前读取模型是“已提交内存状态视图”，不是 MVCC，也不是可长期持有的快照。

## Transaction Scope Candidates

若后续实现事务，建议只评估下面这个最小范围：

- 单 writer 内核上的显式事务边界：`Begin -> Put/Delete... -> Commit/Abort`
- 事务提交仍以 WAL 持久化成功为准
- 首版只支持一个活动写事务，不引入并发写事务调度

不建议第一版直接实现：

- 多写事务并发
- 可配置隔离级别
- 分布式事务
- 长事务与后台 compaction 协调

## Snapshot Read Candidates

若后续实现快照读，建议只评估下面这个最小范围：

- 只读快照句柄在创建时绑定一个已提交状态版本
- `Get` / `Scan` 可基于该版本读取稳定视图
- 快照生命周期不跨进程，不承诺崩溃后恢复

不建议第一版直接实现：

- 基于 WAL 回放的历史时点查询
- 长生命周期 MVCC 版本链
- 跨 compaction 的历史版本保留

## Required Prerequisites

在真正进入事务或快照读实现前，至少应先满足：

- 当前格式迁移策略稳定
- 读写语义文档稳定
- 格式验证和恢复矩阵稳定
- 针对 writer 控制器已有长期压测基线

## Decision Rule

- 如果目标只是“把多个操作作为一个提交单元”，优先继续使用 `WriteBatch`，不要过早引入事务框架。
- 只有当用户场景明确需要 `Commit/Abort` 或稳定读视图时，才进入事务或快照读实现阶段。
