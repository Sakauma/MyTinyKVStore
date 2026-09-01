# 写路径设计

## 模块

主要实现位于 [storage_engine.cpp](../../src/internal/storage_engine.cpp)：

- bounded raw queue：生产者背压和请求序号。
- worker pool：并行 payload 序列化、payload/value CRC 和物理 WAL charge 计算。
- prepared map：容纳乱序完成的准备结果。
- ordered coordinator：OCC 验证、LSN、header/footer、分段 `pwritev`、group sync 和状态发布。
- shard state：点查哈希索引、字符串有序索引和版本。
- value cache：独立的分段 CLOCK，以规范化 key + LSN 为身份，命中不升级 shard 锁。

活动运行时所有权集中在 `StorageEngine::Impl`。历史 writer 原型已经归档，不参与默认构建或测试。

## 不变量

- 公开写 API 不直接修改文件或内存索引。
- 请求序号决定 coordinator 处理顺序。
- LSN 只分配给通过验证且实际写 journal 的事务。
- `fdatasync` 成功先于 `kSync` 状态发布。
- 多 shard 锁按 ID 排序。
- Group commit 聚合多个 frame，不合并事务边界。
- Compaction 与 coordinator 通过 `commit_mutex_` 协调最终 delta 和 inode 切换。
- Worker 计算的 value CRC 和 mutation charge 在发布状态时直接复用，coordinator 不重复扫描 value。

## 背压

Raw queue 默认容量 4096。队列满时 `submit_and_wait` 等待 `raw_not_full_`，不会丢弃或返回伪成功。Worker 数默认由硬件并发确定并限制为 32。

## 错误

Worker 的单请求序列化错误只完成该请求；journal write/sync、状态不变量、读 backing 或 compaction I/O 错误进入全局 sticky fatal。第一条根因在发布 fatal 标志前写入，避免并发调用看到空错误。

最近窗口由 mutex 保护的批次环和写完成延迟样本组成。窗口严格保留 `adaptive_recent_window_batches` 个批次（上限 4096），p95 只使用最近 `adaptive_recent_write_sample_limit` 个完成样本；`GetMetrics()` 与 adaptive policy 读取同一快照。
