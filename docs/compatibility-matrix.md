# Compatibility Matrix

## Cases Covered

| Case | Snapshot | WAL | `inspect-format` | `verify-format` | `rewrite-format` |
| --- | --- | --- | --- | --- | --- |
| `current_v2` | v2 | v2 or empty | reports current layout and key-type stats | returns `0` | not required |
| `legacy_v1` | v1 | v1 | reports legacy layout and `rewrite_recommended=1` | returns `2` | upgrades to current layout |
| `legacy_v1_after_rewrite` | v2 | empty | reports current layout | returns `0` | already upgraded |

## Automation

- `./build/target/bin/kv_test compat-matrix`
- `bash scripts/compatibility-matrix.sh`

这两个入口都会实际构造 `current_v2` 和 `legacy_v1` 两组样本数据，执行 `inspect-format`、`verify-format`、`rewrite-format` 的组合检查，并打印单行 `key=value` 结果。

## Expected Meanings

- `verify_status=0`：这份数据已经处于当前受支持格式。
- `verify_status=2`：这份数据仍建议重写，通常是旧格式或带有不稳定布局信号。
- `rewrite_recommended=1`：建议执行 `rewrite-format`，把数据重写到当前布局。

## Update Rule

每次磁盘格式、兼容策略或迁移流程变化后，都应同时更新：

- 本文档
- [docs/file-format.md](/home/sakauma/code/lpue/docs/file-format.md)
- `compat-matrix` 的实现与回归测试
