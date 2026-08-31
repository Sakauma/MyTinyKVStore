# 格式兼容矩阵

## 运行时策略

| 输入 | 运行库打开 | `inspect-format` | `verify-format` | 正确操作 |
| --- | --- | --- | --- | --- |
| 健康 v3 | 接受 | 报告 v3 区域、LSN 和计数 | `0` | 直接使用 |
| v3 单 superblock | 可降级打开 | `valid_superblocks=1` | `2` | 备份后 v3 `Compact()` 重写 |
| v3 不完整最终 frame | 接受并截断修复 | `truncated_tail=1` | `2` | 先保留现场，再用运行库修复 |
| v3 完整 checksum/中段/LSN 损坏 | 拒绝 | `valid=0` | `2` | 停止写入，人工调查 |
| v1/v2 snapshot（可含 `.wal`） | 拒绝并提示 migration | `migration_required=1` | `2` | 离线 `kv_migrate` |

运行库不承诺 v1/v2 读取兼容。兼容性由独立迁移工具提供，避免主恢复路径长期背负旧格式分支。

## 自动化入口

```bash
./build-release/target/bin/kv_test compat-matrix
bash scripts/compatibility-matrix.sh
```

矩阵测试会构造健康 v3 和 legacy v1 样本，确认：

- 健康 v3 通过共享 verifier。
- Legacy 被标记为需要迁移。
- 运行库拒绝 legacy，而不是隐式改写。
- `kv_migrate` 输出双 superblock 的健康 v3，且源 snapshot/`.wal` 大小不变。

更细的恢复测试还覆盖：

- 单/双 superblock 损坏。
- Torn transaction tail。
- 完整 frame checksum 损坏。
- 伪造 operation count 和区域长度。
- `WriteBatch` 全有或全无。

## 工具含义

- `inspect-format`：只读分析，健康、degraded、truncated 和损坏都输出机器可解析字段；不会修改文件。
- `verify-format`：只有双 superblock、无截断尾部且全部校验通过的 v3 返回 `0`。
- `rewrite-format`：只对可打开的 v3 调用同步 `Compact()`；不能迁移 legacy。
- `kv_migrate`：只读 v1/v2，写入不存在的新 v3 路径，并在完成后重新验证。

## 更新规则

每次修改磁盘格式、恢复容错或迁移流程时，必须同步更新：

- [v3 文件格式](file-format.md)
- [迁移策略](migration-policy.md)
- `compat-matrix` 与恢复回归测试
