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
- Compaction：通过 unlink 后的 spool 流式生成 checkpoint；在主文件旁用 `.compact.<pid>.<sequence>.<random nonce>` 原子创建已锁定临时容器，复制 delta、同步、rename、发布新文件代际与 WAL epoch、同步父目录，再逐 shard 迁移 entry。

## 稳态约束

- 只有主文件是恢复依赖。
- 主文件全生命周期持有独占锁。
- 临时文件不能作为提交标记或恢复前置条件。
- 临时文件名碰撞只换 nonce 重试，不删除既有路径；清理逻辑只 unlink 本次成功创建且尚未 rename 的文件。
- Rename 后新 inode 的锁和 fd 立即成为运行时所有权。
- Entry 与读快照持有引用计数的文件代际。Rename 后旧 inode 可以继续服务尚未迁移的 entry 和已开始读取；手动 compaction 在全部 entry 切换后返回，不等待 reader。旧 fd/inode 在最后一个代际引用释放后自然回收。

## Compaction 错误边界

- 临时文件创建、写入、同步或 rename 前注入的普通异常尚未发布新代际，清理本次临时文件后可以继续使用实例。
- 从当前主代际读取 checkpoint value 或 journal delta 时的 `pread`、EOF、checksum 和主状态不变量错误说明源状态不可信，即使在 rename 前也进入 sticky fatal。
- Rename 成功后的任何异常都进入 sticky fatal；调用方必须保留主文件、目录和首条错误再离线验证。
- 后台可恢复失败增加 `auto_compaction_failures`，至少退避 1 秒，并等待后续写入重新满足调度条件。

字节级说明见[文件格式](../file-format.md)。
