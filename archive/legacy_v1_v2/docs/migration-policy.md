# v1/v2 到 v3 的迁移策略

## 固定规则

- v3 运行库不读取 v1/v2，不隐式升级。
- 迁移必须离线进行，源数据库不能同时被旧进程写入。
- 迁移只读源 snapshot 和可选源 `.wal`。
- 输出必须是一个不存在的新路径。
- 禁止原地迁移，也禁止覆盖已有输出。
- 生成后必须重新打开并通过共享 v3 解析器验证。

这些规则把失败范围限制在新输出，旧文件始终保留为回退来源。

## 命令

```bash
cmake -S . -B build-release -DCMAKE_BUILD_TYPE=Release
cmake --build build-release --parallel --target kv_migrate

./build-release/target/bin/kv_migrate \
  --input /data/legacy.db \
  --output /data/migrated-v3.db
```

或使用：

```bash
bash scripts/migrate-format.sh /data/legacy.db /data/migrated-v3.db
```

成功输出包含 `status=pass`、输入路径、输出路径、迁移条目数和 `container_version=3`。

## 迁移过程

1. 把输入和输出转换为规范绝对路径并确认不同。
2. 确认输入存在、输出不存在。
3. 以只读方式加载 legacy snapshot。
4. 若 `<input>.wal` 存在，以只读方式按旧格式重放。
5. 使用 v3 `KVStore` 分批写入新输出。
6. 对输出执行 `Compact()` 和 `Flush()`，收敛为 checkpoint + 空 journal。
7. 关闭输出，再通过 `inspect_v3_file` 重新打开验证双 superblock 和完整尾部。
8. 任一步失败都删除未完成输出，不修改源文件。

迁移工具会保留 int/string/binary 三种规范化 key namespace。Value 始终作为不透明字节数组迁移。

## 上线步骤

1. 停止旧版本写流量。
2. 记录源 snapshot 和 `.wal` 的路径、大小与备份校验值。
3. 在同一 POSIX 文件系统的独立输出路径运行迁移。
4. 执行：

   ```bash
   bash scripts/verify-format.sh /data/migrated-v3.db
   ```

5. 用新版本应用做只读抽样或全量业务校验。
6. 原子切换应用配置指向新文件，而不是重命名覆盖源文件。
7. 保留源文件直到回滚窗口结束。

## `inspect-format` / `verify-format`

| 状态 | 含义 | 操作 |
| --- | --- | --- |
| v3、双 superblock、无尾部截断 | 健康 | 可直接打开 |
| v3、单 superblock | 可降级恢复但不健康 | 备份后用 v3 `Compact()` 重写 |
| v3、不完整最终 frame | 运行时可修复，验证不通过 | 备份现场后以运行库打开并再验证 |
| v3、完整 checksum/LSN/边界错误 | 损坏 | 停止写入并保留现场，不能自动跳过 |
| v1/v2 | 需要迁移 | 使用 `kv_migrate` |

`rewrite-format` 只会打开 v3 并调用 `Compact()`；因为运行库拒绝旧格式，它不能替代 `kv_migrate`。

## 禁止事项

- 不要把输出指向源路径。
- 不要先删除旧 `.wal` 再迁移。
- 不要在旧进程仍写入时复制或迁移。
- 不要把损坏的完整 frame 当作普通截断尾部强行跳过。
- 不要仅凭文件能打开就删除源；必须完成格式验证和业务数据验证。

格式细节见[v3 单文件格式](file-format.md)。
