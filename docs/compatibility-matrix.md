# Compatibility Matrix

## Cases Covered

| Case | Snapshot | WAL | `inspect-format` | `verify-format` | `rewrite-format` |
| --- | --- | --- | --- | --- | --- |
| `current_v2` | v2 | empty | reports current layout and key-type stats | returns `0` | not required |
| `current_v2_live_wal` | v2 | v2, non-empty | reports current layout, `wal_version=2`, `rewrite_recommended=0` | returns `0` | not required |
| `current_v2_truncated_wal` | v2 | v2 + truncated tail | reports `wal_truncated=1` and `rewrite_recommended=1` | returns `2` | rewrites to clean current layout |
| `current_v2_truncated_wal_after_rewrite` | v2 | empty | reports clean current layout again | returns `0` | already rewritten |
| `legacy_v1` | v1 | v1 | reports legacy layout and `rewrite_recommended=1` | returns `2` | upgrades to current layout |
| `legacy_v1_after_rewrite` | v2 | empty | reports current layout | returns `0` | already upgraded |

## Automation

- `./build/target/bin/kv_test compat-matrix`
- `bash scripts/compatibility-matrix.sh`

这两个入口都会实际构造当前格式、live WAL、截断 WAL 和 legacy v1 样本数据，执行 `inspect-format`、`verify-format`、`rewrite-format` 的组合检查，并打印单行 `key=value` 结果。

## Expected Meanings

- `verify_status=0`：这份数据已经处于当前受支持格式。
- `verify_status=2`：这份数据仍建议重写，通常是旧格式或带有不稳定布局信号。
- `rewrite_recommended=1`：建议执行 `rewrite-format`，把数据重写到当前布局。
- `verify_reason=*`：`verify-format` 输出的机器可解析原因，用于区分 `migration_required`、`wal_truncated` 和快照结构问题。
- `wal_truncated=1`：说明 WAL 带有崩溃尾部或截断痕迹；当前策略会把它视为“可恢复但仍建议立即 rewrite”的状态。

## Update Rule

每次磁盘格式、兼容策略或迁移流程变化后，都应同时更新：

- 本文档
- [docs/file-format.md](/home/sakauma/code/lpue/docs/file-format.md)
- `compat-matrix` 的实现与回归测试
