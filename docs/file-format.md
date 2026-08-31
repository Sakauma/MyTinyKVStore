# 单文件格式

## 适用范围

运行库只创建和读取当前单文件格式。历史格式实现已经归档；运行库不会读取、隐式迁移或原地覆盖历史数据。

当前格式面向 Linux/WSL POSIX 文件系统。实现把 packed C++ 结构按本机小端序写盘，因此正式支持范围是常见的 little-endian Linux ABI；它不是跨大端机器的交换格式。

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

恢复 checkpoint 时，解析器按 entry 流式读取并校验对象，不把整个 object 区一次性分配进内存。Compaction 同样只快照 key 和 backing reference，再按块从旧 inode 流式复制 value。

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

1. 在 commit mutex 下记录 `start_lsn` 和旧文件 `start_offset`。
2. 逐 shard 捕获 live key 的 backing reference，并在临时文件中流式生成 checkpoint。
3. 对复制的每个 value 再算 CRC32C，生成并校验新 checkpoint checksum。
4. 最终持有 commit mutex，复制 `[start_offset, end_offset)` journal delta，`fdatasync` 临时文件。
5. 临时文件先持有独占锁，再 `rename` 到主路径；运行时立即切换到新 fd 并重定位全部 live offset。
6. 同步父目录后释放提交暂停。

临时文件不参与恢复。任意崩溃点只能留下旧主文件或已经自包含的新主文件。

## 相关实现

- [storage_format.h](../src/internal/storage_format.h)
- [storage_format.cpp](../src/internal/storage_format.cpp)
- [storage_engine.cpp](../src/internal/storage_engine.cpp)
