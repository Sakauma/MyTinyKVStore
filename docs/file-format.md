# 单文件格式

## 适用范围

运行库只创建和读取当前单文件格式。历史格式实现已经归档；运行库不会读取、隐式迁移或原地覆盖历史数据。

当前格式面向 little-endian Linux ABI，以及提供所需 `flock`、`pread/pwrite`、原子 rename 和目录同步语义的原生 Linux POSIX 文件系统。WSL 的数据库必须位于 Linux 文件系统；`/mnt/*`、DrvFS、9p 和 fuseblk 不在持久化正确性支持范围。Packed C++ 结构按本机小端序写盘，因此它也不是跨大端机器的交换格式。

## 文件布局

```text
0                 4096              8192
+-----------------+-----------------+-------------------------+
| superblock A    | superblock B    | checkpoint index        |
+-----------------+-----------------+-------------------------+
                                    | checkpoint objects      |
                                    +-------------------------+
                                    | journal frame 1         |
                                    | journal frame 2 ...     |
                                    +-------------------------+
```

- 两个 superblock 各占 4096 字节，结构内容带 CRC32C。
- `index_offset/index_length` 指向 checkpoint index。
- `object_offset/object_length` 指向连续 value 对象区。
- `journal_offset` 指向追加事务日志的起点。
- 稳态恢复只依赖这一个主文件。Compaction 可以短暂创建临时文件。
- 恢复不扫描或采用 `.compact.*` 文件；遗留临时文件不是提交标记。

## Superblock

`Superblock` 的有效结构为 120 字节，其余 block 字节清零。主要字段：

| 字段 | 含义 |
| --- | --- |
| `magic` | `MTKV0003` |
| `version` | `3` |
| `generation` | 每次成功 compaction 递增 |
| `checkpoint_lsn` | checkpoint 对应的切点 LSN |
| `index_*` / `object_*` / `journal_offset` | 区域边界 |
| `checkpoint_checksum` | index 与 object 区串联计算的 CRC32C |
| `checksum` | superblock 结构自身 CRC32C |

打开文件时分别校验两个副本，选择有效副本中 generation 最大者。两个副本都无效时拒绝打开；只有一个有效时运行时可以恢复，但 `verify-format` 返回 degraded 状态。

所有区域边界在任何读取和分配之前检查：文件范围、加法溢出、平台 `off_t` 上限和格式上限都必须成立。

## Checkpoint index 与对象区

Index 以 32 字节 `IndexHeader` 开始：

- magic/version
- entry count
- entry bytes
- 全部 entry bytes 的 CRC32C
- header CRC32C

每个 index entry 使用 32 字节 `IndexEntryHeader`，随后紧跟规范化 key 字节。Header 记录：

- key 长度
- value 在 object 区内的相对 offset
- value 长度
- value CRC32C
- entry header + key CRC32C

对象区只连续保存 value 字节。内存索引恢复后保存绝对文件 offset、长度和 checksum；value 不要求常驻内存。

恢复 checkpoint 时，解析器按 entry 流式读取并校验对象，不把整个 object 区一次性分配进内存。Compaction 同样只快照 key 和 backing reference，并按文件代际与 offset 排序；相邻间隔不超过 64 KiB、总窗口不超过 1 MiB 的 value 会合并读取，其余 value 以 1 MiB 缓冲流式复制。

## Journal transaction frame

每次隐式或显式事务编码成一个 frame：

```text
+----------------------+----------------------+----------------------+
| FrameHeader (44B)    | mutation payload     | FrameFooter (32B)    |
+----------------------+----------------------+----------------------+
```

Frame header 包含：

- magic/version/header size
- frame 总长度
- 严格递增 LSN
- operation count
- payload CRC32C
- header CRC32C

Payload 由 `MutationHeader (20B) + key + value` 顺序组成。Mutation 支持 `put` 和 `delete`，每条记录也有独立 CRC32C。Delete 的 value 长度必须为零。

Commit footer 重复 LSN、frame 总长度和 payload checksum，并带自身 CRC32C。只有 header、payload、全部 mutation 和 footer 都完整且匹配的 frame 才会被应用。

`WriteBatch` 与显式事务只产生一个 frame，因此恢复结果只能是全部操作出现或全部消失。一个 group commit 可以在同一次 `pwritev/fdatasync` 中写多个 frame，但 frame 仍保持独立事务边界和严格 LSN 顺序。

Worker 在 LSN 分配前生成 payload、payload CRC、每个 value CRC 和 mutation 的物理 WAL charge。Coordinator 分配 LSN 后只填充 header/footer，并直接把三段 iovec 写入文件；分段写出的字节与连续 frame 编码完全一致。

## WAL 容量统计

- `wal_bytes_since_compaction` 是当前 journal 中所有完整物理 frame 的字节总和。
- 每个 frame 的 header/footer 固定开销按 operation count 平均分配，余数字节归前若干 mutation；所有 mutation charge 之和严格等于 frame 大小。
- 每个 key 只有最新 journal mutation 的 charge 属于 live。Put charge 保存在对应 entry 中；已删除 key 的最新 tombstone charge 保留到下一次 compaction。
- 更早的覆盖、delete 前的 value 和批内重复 mutation 都属于 obsolete。
- 运行时和恢复过程使用同一算法，始终满足 `wal = live + obsolete`；全新唯一 key 的 obsolete 为零。

## 恢复规则

1. 读取并选择 superblock。
2. 验证全部 checkpoint checksum 和 index/object 边界。
3. 把 checkpoint 条目应用为 `checkpoint_lsn` 状态。
4. 从 `journal_offset` 起按严格递增 LSN 解析 frame。
5. 文件末尾不足一个完整 frame 时视为未确认尾帧；读写打开会把文件截断到最后一个完整 frame。
6. 完整 frame checksum 失败、commit footer 失败、中段损坏、重复或倒退 LSN 时拒绝打开。

`inspect-format` 只读且不会修复尾部；`verify-format` 因截断尾部、单 superblock 或任意损坏返回非零。运行时读写打开允许修复可识别的不完整最终尾帧。

## 格式上限

| 项目 | 上限 |
| --- | ---: |
| 编码 key | 1 MiB |
| 单 value | 64 MiB |
| 单事务 payload | 256 MiB |
| checkpoint index | 4 GiB |

Operation count 还必须能够由实际 payload 容纳，禁止先按伪造 count 执行巨量 `reserve`。所有 value 分配都发生在长度和文件范围验证之后。

## Compaction 切换

1. 在 commit mutex 下记录 `start_lsn`、旧文件 `start_offset`，并启用第二个 WAL accounting epoch。
2. 逐 shard 捕获 live key 的 backing reference；仍匹配的 entry 写入 relocation epoch/offset。
3. 通过 unlink 后的 index/object spool 和 1 MiB 缓冲组装已加独占锁的临时主文件；checkpoint CRC 在顺序写入时增量计算，不重读完整输出。
4. 最终只持有 commit mutex，复制 `[start_offset, end_offset)` journal delta，`fdatasync` 临时文件，`rename` 主路径，发布新文件代际与 WAL epoch，并同步父目录。该阶段不获取全部 shard 锁，也不遍历 live key。
5. 释放 commit mutex 后逐 shard 迁移 entry：切点后的 value 按 journal delta 平移，切点前仍匹配 relocation epoch 的 value 指向新 checkpoint。被并发覆盖或删除的 entry 不会被旧 relocation 信息覆盖。
6. 全部 entry 迁移完成后释放 engine 对旧代际的持有。手动 `Compact()` 等待迁移完成；已经取得快照的 reader 可以继续持有旧 fd，旧 inode 在最后一个引用结束后自然回收。自动 compaction 在后台执行。

主文件旁的临时容器使用 `.compact.<pid>.<sequence>.<random nonce>` 命名并以 `O_EXCL` 创建。名称碰撞时换 nonce 重试，不删除碰撞文件；异常清理只 unlink 本次已经创建且尚未 rename 的路径。Rename 前的普通临时文件错误不会改变主代际，实例可以继续使用；读取当前主代际失败、主状态不变量错误和 rename 后错误会进入 sticky fatal。

临时文件不参与恢复。任意崩溃点只能留下旧主文件或已经自包含的新主文件。

## 相关实现

- [storage_format.h](../src/internal/storage_format.h)
- [storage_format.cpp](../src/internal/storage_format.cpp)
- [storage_engine.cpp](../src/internal/storage_engine.cpp)
