# Disk Format Specification

## Overview

当前磁盘布局由两部分组成：

- 快照文件：`<db_path>`
- 预写日志：`<db_path>.wal`

启动时按“快照 -> WAL”顺序恢复。快照代表最近一次 compaction 后的完整状态，WAL 代表其后的增量更新。

## Snapshot File

### Header

- `magic[8]`: 固定为 `KVSNAP01`
- `version`: 当前为 `1`
- `reserved`: 保留字段，当前写入 `0`

### Entry Layout

每条快照记录按以下顺序编码：

- `entry_magic`: 固定值 `0x4B565345`
- `key`: `int32_t`
- `value_size`: `uint32_t`
- `value_bytes[value_size]`

### Validation Rules

- `magic` 不匹配时拒绝加载。
- `version` 不支持时拒绝加载。
- entry header 或 value 数据截断时拒绝加载。

## WAL File

### Record Header

每条 WAL 记录头按以下顺序编码：

- `magic`: 固定值 `0x4B565741`
- `version`: 当前为 `1`
- `type`: `1=Put`, `2=Delete`
- `reserved`: 保留字段，当前写入 `0`
- `key`: `int32_t`
- `value_size`: `uint32_t`
- `checksum`: 基于 `type + key + value_size + payload` 的 FNV-1a 校验

### Payload

- `Put`: 追加 `value_size` 字节的 value payload
- `Delete`: `value_size` 必须为 `0`

### Validation Rules

- `magic`、`version`、`type` 非法时拒绝加载。
- payload 截断且位于文件尾部时按崩溃尾巴忽略。
- checksum 不匹配时拒绝加载。

## Compatibility Policy

- 当前支持的 snapshot / WAL 版本均为 `1`。
- 不支持的版本会直接抛出 `KVStoreError`，不会尝试静默兼容。
- 推荐的升级流程是先用当前程序检查格式，再执行一次 `Compact()` 以重写快照和清空 WAL。

## Operational Tools

- `./build/target/bin/kv_test inspect-format <db_path>`：打印快照/WAL 的版本和基础信息。
- `./build/target/bin/kv_test rewrite-format <db_path>`：加载数据库并执行一次 `Compact()`，将数据重写为当前格式。
