# Consistency and Durability Semantics

## Write Durability

- `Put`、`Delete` 和 `WriteBatch` 在成功返回前，都会把对应 WAL 记录追加并 `fsync` 到磁盘。
- 快照文件允许落后于最新写入；进程重启时会先加载快照，再顺序重放 WAL。
- 如果进程在 `WAL fsync` 之后、内存状态应用之前崩溃，已成功返回或已同步到 WAL 的写入仍会在重启后恢复。

## Visibility Model

- 对同一个 `KVStore` 实例，写请求成功返回后，后续 `Get` / `Scan` 必须能看到该次写入。
- `Get` 和 `Scan` 只读取已提交的内存状态，不会观察到单条写入或批量写入的中间态。
- 以上可见性语义对 `int`、`string` 和 `binary` 键一致成立；不同键类型通过编码标签隔离命名空间。
- `Scan` 在共享锁保护下遍历有序索引，因此返回的是一次一致的点时间视图；并发 writer 的状态应用会在 `Scan` 结束后继续。

## Batch Atomicity

- `WriteBatch` 中的多条操作会作为一个公开 API 请求进入 writer 队列。
- writer 会按调用方给定顺序追加该批次的多条 WAL 记录，并在同一次状态应用中完成整批更新。
- 同一批次内可混合 `put` / `delete`，也可重复操作同一个键；最终状态以批次内最后一次操作为准。
- 崩溃恢复时会按 WAL 顺序重放整批记录，因此 `WriteBatch` 的最终结果与正常执行路径一致。

## What Is Not Provided

- 当前不提供跨多个 API 调用的事务语义。
- 当前不提供 MVCC、快照读或可配置隔离级别。
- 当前也不提供“未返回写入对其他线程提前可见”的弱语义承诺；以 API 成功返回作为提交边界。

## Future Work Boundary

- 若后续要实现事务，应以当前“单次 API 调用是提交单元”的语义为基线扩展。
- 若后续要实现快照读，需要先明确 `Scan` 与 `Get` 的长期读一致性模型，而不是复用当前共享锁语义直接外推。
