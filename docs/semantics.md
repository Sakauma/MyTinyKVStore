# 一致性与持久化语义

## 提交顺序

所有 `Put`、`Delete`、`WriteBatch`、事务 `Commit()` 和 `Flush()` 在入队时获得单调序号。Worker 可以乱序完成 payload、payload/value CRC 和 WAL charge 准备，但 ordered coordinator 只按该序号处理已准备请求。

被接受的写事务再获得严格递增 LSN。磁盘 frame 顺序、状态发布顺序和调用方完成通知都遵守该 LSN 顺序。OCC 冲突请求不获得 LSN、不写 journal，也不发布任何状态。

## 持久化模式

### `kSync`

- 默认模式。
- 一组完整 frame 通过 `pwritev` 写入后执行一次 `fdatasync`。
- 只有同步成功后才发布 shard 状态并返回成功。
- 成功返回的事务在支持相应 POSIX 持久化语义的文件系统上必须能够恢复。

### `kPeriodic`

- Frame 写入并发布后即可返回。
- 后台线程默认每 10 ms 对 dirty 文件执行同步。
- 进程或机器崩溃可能丢失最近一个同步周期内已经返回的提交。
- `Flush()` 可以提前建立明确持久化屏障。

### `kNoSync`

- 普通提交不主动同步。
- 崩溃后允许丢失自上次 `Flush()`、compaction 或其他实际同步点之后的提交。
- `Flush()` 等待并同步序号在它之前的全部提交。

析构会尝试 best-effort 同步和关闭，但析构不能向调用方可靠报告错误。需要可报告持久化结果时必须显式调用 `Flush()`。

## 可见性与原子发布

- 同一实例中，写 API 成功返回后，随后开始的 `Get` 必须看到该提交或更晚状态。
- 每个事务涉及的 shard 按 shard ID 排序并同时持有独占锁；全部 mutation 应用完成后才释放锁。
- 任何单 key `Get` 都不会看到一个跨 shard 事务的中间更新。
- `WriteBatch` 和显式事务在恢复时也只有全有或全无两种结果。
- 同一事务内重复修改同一个 key 时，最后一条 mutation 决定最终状态。

多个独立事务可以在同一次 group commit 中同步，但仍按 LSN 逐事务发布。因此读线程可以观察到 group 中较早事务已发布、较晚事务尚未发布的合法中间点；group commit 本身不是跨请求的大事务。

## 点读取与 value cache

点读取只持有目标 shard 的共享锁。Entry 记录规范化 key 对应的 LSN、value offset/length/checksum 和文件代际；cache 以规范化 key 与 LSN 为身份，因此旧 LSN 的缓存值不会遮蔽新提交。

Cache 使用 1–64 个二次幂 segment 的 CLOCK 淘汰。命中在 segment 共享锁下设置 atomic reference bit，插入和淘汰只获取该 segment 的独占锁；总 charge 包含 key、value 和节点估算开销，严格不超过 `value_cache_bytes`。`Scan` 绕过 cache，避免范围读取冲掉热点。

## WAL 指标语义

`wal_bytes_since_compaction` 统计当前 journal 的完整物理 frame。每个 frame 的 header/footer 开销按 mutation 分摊，每个 key 只有最新 mutation charge 属于 live；delete tombstone 在下一次 compaction 前也属于 live。其余 charge 为 obsolete，始终满足：

```text
wal_bytes_since_compaction = live_wal_bytes_since_compaction
                           + obsolete_wal_bytes_since_compaction
```

Compaction 开始后同时维护当前和待切换 accounting epoch；切换时直接发布只包含切点后 delta 的 epoch，恢复会重建同样的数值。

## `Scan`

字符串 `Scan(start, end)` 按固定 shard 顺序获得全部共享锁，并对每个 shard 的有序字符串索引做 k-way merge：

- 结果按原始字符串 key 排序。
- 扫描期间得到一个一致的已提交视图。
- 写事务发布需要等待扫描释放相关 shard 锁。
- `Scan` 定位为低频管理路径；大范围或长时间扫描会提高写尾延迟。
- 显式事务内不支持 `Scan`。

## 崩溃恢复

- 完整且 checksum/footer 验证通过的 frame 才会重放。
- 不完整最终 frame 可以丢弃，即该事务可能已经写入部分字节但从未确认。
- 完整 frame 损坏、中段损坏或非单调 LSN 会拒绝打开，不能静默跳过。
- `kSync` 下，崩溃发生在同步之后、内存发布之前时，重启仍会恢复该已同步 frame；调用方可能因进程崩溃没有收到返回，因此它属于“结果未知但允许出现”的事务。
- Compaction 临时文件不是恢复依赖。Rename 前崩溃使用旧文件；rename 后只能使用完整新文件。

## Compaction 文件代际

每个 entry 持有引用计数的 backing file generation。Compaction 的最终提交暂停只覆盖 journal delta 复制、临时文件同步、rename、代际/accounting 发布和父目录同步，不包含全量 entry 遍历。暂停结束后，旧 inode 继续服务尚未迁移的读取，后台再按 shard 迁移 offset；并发新提交已经指向新代际，不会被旧 relocation 覆盖。手动 `Compact()` 在迁移结束且旧 inode 释放后返回。

## Sticky fatal state

运行时遇到 journal 写入、同步、value `pread`、checksum、compaction 或目录同步错误时会保存第一条根因并进入 sticky fatal：

- 后续 `Get`、`Scan`、`Put`、`Delete`、`WriteBatch`、事务提交、`Flush` 和 `Compact` 不再继续正常执行。
- 后续调用抛出同一原始错误，避免一部分线程继续在不可信状态上工作。
- 应停止流量、保留文件和错误信息，再通过只读 `verify-format` 排查。

## 进程边界

主文件在实例生命周期内持有非阻塞独占 `flock`。第二个进程或同进程第二个独立实例打开相同 inode 会失败。Compaction 临时文件在 rename 前先加锁，新 inode 成为主文件后继续持锁，不留并发打开窗口。

这不是多进程共享写协议，也不提供网络分布式一致性。

## 不提供的语义

- 不提供 MVCC 历史版本或任意时点读取。
- 不提供事务内范围扫描。
- 不提供跨进程事务、分布式事务、复制或共识。
- 不承诺 `kPeriodic/kNoSync` 已返回但未经过持久化屏障的提交在机器崩溃后保留。

显式事务的隔离与冲突规则见[事务边界](transaction-boundary.md)。
