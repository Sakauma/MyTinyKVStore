# 高级语义决策

## 当前结论

早期“暂缓事务”的决定已经被当前实现取代。当前正式语义是：

- 提供显式 move-only `KVTransaction`。
- 使用 shard-version OCC，提交时提供可串行化验证。
- 支持 read-your-writes、只读 commit 验证和 rollback。
- 不提供 MVCC snapshot read，也不提供事务内 `Scan`。

## 为什么实现事务

单靠 `WriteBatch` 无法表达“先读、根据结果写、提交时检测冲突”的场景。当前 transaction frame 已具备明确事务边界，ordered coordinator 也能在分配 LSN 和写盘前完成版本验证，因此最小 OCC 事务可以复用同一持久化路径，而不引入第二套日志协议。

## 为什么仍不实现 MVCC

- 当前 entry 只保留最新 LSN 和 value offset，不保留历史版本链。
- 长期 snapshot 会要求 compaction 保留被引用的旧对象或增加版本重写协议。
- 当前字符串 `Scan` 通过全 shard 共享锁提供短期一致视图，适合管理操作，但不是长期 snapshot 句柄。

只有明确出现历史时点查询、长期非阻塞范围读或跨 compaction snapshot 生命周期需求时，才重新评估 MVCC。

详细规则见[事务边界](transaction-boundary.md)。
