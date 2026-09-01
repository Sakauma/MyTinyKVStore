# 单文件容器设计

早期 section-table 原型已经被当前运行时布局取代。容器固定为：

1. 两个 4 KiB superblock。
2. Checkpoint index。
3. Checkpoint object data。
4. 追加 transaction journal。

不再使用独立 metadata/section table；区域由 CRC32C superblock 的显式 offset/length 定义。

## 生命周期

- 新建：写双 superblock、同步文件、同步父目录。
- 提交：追加完整 frame，按 durability 策略同步。
- 恢复：checkpoint + journal。
- Compaction：通过 unlink 后的 spool 流式生成已锁定临时容器、复制 delta、同步、rename、发布新文件代际与 WAL epoch、同步父目录，再逐 shard 迁移 entry。

## 稳态约束

- 只有主文件是恢复依赖。
- 主文件全生命周期持有独占锁。
- 临时文件不能作为提交标记或恢复前置条件。
- Rename 后新 inode 的锁和 fd 立即成为运行时所有权。
- Entry 持有引用计数的文件代际。Rename 后旧 inode 可以继续服务尚未迁移的读取；手动 compaction 在全部 entry 切换并释放旧 fd 后返回。

字节级说明见[文件格式](../file-format.md)。
