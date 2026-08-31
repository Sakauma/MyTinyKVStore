# 正式验收口径

## 状态定义

实现完成、单元测试通过和短时 stress 通过都不等于正式认证。只有本文件全部 gate 通过并归档 artifact 后，版本才可以标记为“高并发持久化 KV 引擎”。

当前仓库提供认证 harness；某一 checkout 是否已认证，必须由对应 commit 的 artifact 证明。

## 功能 gate

### 单文件

- 稳态只依赖一个主数据库文件。
- 外部 `.wal` 不属于运行时依赖。
- Compaction 临时文件可以短暂存在，但崩溃恢复不能依赖它们。

### 持久化

- 默认 `kSync`。
- 成功返回的同步事务必须恢复。
- 未确认事务可以出现或消失，但绝不能恢复半个事务。
- `Flush()` 是它之前全部提交的有序持久化屏障。
- 首次创建和 compaction rename 后必须同步父目录。
- I/O/校验错误必须进入 sticky fatal。

### MPMC 路径

- 多个 producer 可以并发入队。
- 有界 worker pool 是多个实际 consumer，负责并行准备请求。
- Ordered coordinator 只负责最终全局顺序、group write 和 sync。
- 队列满时阻塞，不静默丢弃。

### 事务

- 显式 move-only transaction 支持 int/string/binary `Get/Put/Delete`。
- Read-your-writes、rollback、只读 commit 验证必须成立。
- OCC 冲突不写 journal、不发布部分状态。
- 跨 shard 提交按固定锁顺序原子发布。

### 容量与 compaction

- 内存 entry 不强制持有完整 value。
- Value 可通过 offset/length/checksum `pread`。
- 默认有界 LRU 为 256 MiB，value 与估算的每项元数据开销都计入预算。
- 自动 compaction 在后台生成新容器；手动 `Compact()` 返回时完成。

## 正确性测试 gate

必须覆盖并通过：

- Header、payload、footer、write、sync、checkpoint、rename、目录同步边界的崩溃恢复。
- 双 superblock 损坏、checksum、伪造长度、operation count、中段损坏、重复/倒退 LSN。
- Short write/磁盘写错误、`EINTR` 重试路径和 sticky fatal。
- 8/16/32 producers 的点操作、batch 和 transaction 模型比对。
- Lost update、write skew、跨 shard 冲突、只读验证、read-your-writes、rollback、提交故障。
- 并发 Scan/compaction 与状态一致性。
- 工作线程异常通过 `exception_ptr` 汇总，不允许不透明 `std::terminate`。
- ASan、UBSan、TSan。TSan 配置、链接或运行时缺失都算失败。

## 标准性能 gate

同一机器、同一 Release 工具链、同一原生 Linux 文件系统、`kSync`：

- 预填充 100 万整数键。
- 16 writers。
- 1000 万操作。
- 80% Put、10% Delete、10% Get。
- 256B value。
- Uniform key 分布。
- 自动 compaction 关闭。
- 三轮逐指标取中位数。

通过条件：

- 写吞吐中位数至少为冻结改进前基线的 2 倍。
- 端到端写 p99 中位数不高于基线的 120%。

还必须归档 8/32 writers、64B/1KiB、hotspot 和 compaction-on 补充矩阵。

## 长时 gate

至少一轮：

- 实际写入负载持续 `>= 43200` 秒。
- 完成并保留 `>= 10,000,000` 个唯一整数键。
- Compaction 开启。
- 结束时同步 compact/flush。
- 强制关闭并重启。
- 逐项校验全部目标 key/value。
- 共享格式 verifier 通过。
- 两项主条件满足后继续执行最多 300 秒的覆盖写稳定窗口。
- 无已确认数据丢失、无文件损坏；稳定窗口末端 RSS 不高于起点加 `max(64 MiB, 10%)`，FD 不增加超过 2。

短时 smoke 即使状态为 pass，也必须标记为 `smoke-only`。

## Artifact 必备字段

- Git commit、dirty 状态。
- CPU、内存、磁盘、文件系统、内核。
- 编译器、CMake、构建类型。
- 完整 KVStore/workload 配置。
- 原始每轮结果和中位数。
- 基线文件及其相同环境证明。
- 格式验证和重启逐键校验结果。
- RSS/FD 初始、峰值、最终和稳定窗口起止采样，以及稳定性 gate 结论。

## 非达标示例

- 只跑 `/mnt/d` 快速 benchmark。
- 只有 worker pool，但最终仍按每请求一次 `fdatasync` 且未达到性能门槛。
- 只有 `WriteBatch`，没有显式事务。
- Snapshot + 外部 WAL 双文件。
- TSan 缺失时显示 SKIP。
- 跑 12 小时但只重复覆盖几百个 key。
- 写入 1000 万 key 但没有持续 12 小时。
- 没有重启逐项校验或没有 artifact 环境信息。

## 相关入口

- [性能基线流程](performance-baseline.md)
- [一致性与持久化语义](semantics.md)
- [事务边界](transaction-boundary.md)
- [仓库总览](../README.md)
