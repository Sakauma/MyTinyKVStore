# 写路径设计

## 模块

主要实现位于 [storage_engine.cpp](../../src/internal/storage_engine.cpp)：

- total inflight gate 与 bounded raw queue：生产者背压和请求序号。
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

`request_queue_capacity` 默认 4096，限制从调用线程取得 token 起，到 coordinator 完成通知为止的总 inflight 请求；该数量覆盖 raw queue、worker、prepared map 和 coordinator 当前处理请求。达到上限时 `submit_and_wait` 等待，不会丢弃或返回伪成功。请求成功进入 raw queue 时才分配单调序号。`prepared_queue_depth` 暴露 prepared map 的当前大小，`inflight_request_count` 与 `max_inflight_request_count` 暴露总量和高水位。Worker 数默认由硬件并发确定并限制为 32。

线程启动是一个可回滚阶段。任一 worker、coordinator、auto-compaction 或 periodic thread 构造失败时，初始化路径会设置 stop 状态、通知所有等待点并 join 已启动线程，然后把原异常返回给构造调用方。自动 compaction 的两个阈值都为 0 时不创建对应后台线程。

## 错误

Worker 的单请求序列化错误只完成该请求；journal write/sync、状态不变量和主 backing 读取/校验错误进入全局 sticky fatal。Compaction 尚未发布新代际时的普通临时文件错误只失败该次操作；主代际读取错误或 rename 后错误进入 sticky fatal。第一条根因在发布 fatal 标志前写入，避免并发调用看到空错误。

Coordinator 从首个就绪请求确定 group deadline。每次条件等待返回后都先重新读取时钟；已到 deadline 时立即关闭当前批次，随后才到达或才完成准备的请求留给下一批，不能因为唤醒与取队列之间的竞态越过期限。

最近窗口由 mutex 保护的批次环和写完成延迟样本组成。窗口严格保留 `adaptive_recent_window_batches` 个批次（上限 4096），p95 只使用最近 `adaptive_recent_write_sample_limit` 个完成样本；`GetMetrics()` 与 adaptive policy 读取同一快照。`observed_fsync_pressure_per_1000_writes` 描述最后一个批次，`recent_fsync_pressure_per_1000_writes` 使用最近窗口内实际 fsync 次数和写请求数的总和，adaptive policy 使用后者。Benchmark 的全程门禁另以测量开始、结束时 `wal_fsync_calls` 与 `committed_write_requests` 的差值计算，避免末尾批次形状改变结果。
