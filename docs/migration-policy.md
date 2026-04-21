# Migration Policy

## Goal

把“这份数据能不能直接继续跑”收敛成稳定规则，避免不同工具给出不一致结论。

## Current Policy

| `verify_reason` | Meaning | `verify-format` | Recommended Action |
| --- | --- | --- | --- |
| `current_layout` | 当前快照和 WAL 都处于受支持布局 | `0` | 继续使用，无需迁移 |
| `migration_required` | 格式本身仍是 legacy 或旧布局 | `2` | 运行 `rewrite-format` |
| `wal_truncated` | WAL 可恢复，但带有截断尾部 | `2` | 尽快运行 `rewrite-format`，把现场收敛回干净布局 |
| `snapshot_magic` / `snapshot_entry_magic` / `snapshot_truncated` | 快照结构异常或损坏 | `2` | 先保留现场，再做人工排查；不要直接覆盖原文件 |

## Allowed Compatibility

- v2 snapshot + 空 WAL：直接接受。
- v2 snapshot + v2 live WAL：直接接受。
- v1 snapshot / v1 WAL：允许只读兼容加载，但 `verify-format` 会要求 rewrite。
- v2 snapshot + 截断 WAL 尾部：运行时恢复允许忽略尾部，但 `verify-format` 会要求 rewrite。

## Not Allowed

- 不承诺继续扩展对无限历史格式的长期兼容。
- 不把“可恢复的截断 WAL”视为长期健康状态。
- 不在 `verify-format` 通过时隐式执行 rewrite。

## Operational Rule

1. 先运行 `inspect-format` 看布局、版本和截断信号。
2. 再运行 `verify-format` 看 `verify_reason`。
3. 若 `verify_status=2` 且原因为 `migration_required` 或 `wal_truncated`，执行 `rewrite-format`。
4. 若原因为快照结构异常，先保留原始文件，不要直接重写覆盖。
