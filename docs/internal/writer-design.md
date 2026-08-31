# 写路径设计

## 模块

主要实现位于 [storage_engine.cpp](../../src/internal/storage_engine.cpp)：

- bounded raw queue：生产者背压和请求序号。
- worker pool：并行 payload 校验与序列化。
- prepared map：容纳乱序完成的准备结果。
- ordered coordinator：OCC 验证、LSN、group write/sync、状态发布。
- shard state：点查哈希索引、字符串有序索引、LRU 和版本。

活动运行时所有权集中在 `StorageEngine::Impl`。历史 writer 原型已经归档，不参与默认构建或测试。

## 不变量

- 公开写 API 不直接修改文件或内存索引。
- 请求序号决定 coordinator 处理顺序。
- LSN 只分配给通过验证且实际写 journal 的事务。
- `fdatasync` 成功先于 `kSync` 状态发布。
- 多 shard 锁按 ID 排序。
- Group commit 聚合多个 frame，不合并事务边界。
- Compaction 与 coordinator 通过 `commit_mutex_` 协调最终 delta 和 inode 切换。

## 背压

Raw queue 默认容量 4096。队列满时 `submit_and_wait` 等待 `raw_not_full_`，不会丢弃或返回伪成功。Worker 数默认由硬件并发确定并限制为 32。

## 错误

Worker 的单请求序列化错误只完成该请求；journal write/sync、状态不变量、读 backing 或 compaction I/O 错误进入全局 sticky fatal。第一条根因在发布 fatal 标志前写入，避免并发调用看到空错误。
