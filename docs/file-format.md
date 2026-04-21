# Disk Format Specification

## Overview

当前磁盘布局由两部分组成：

- 快照文件：`<db_path>`
- 预写日志：`<db_path>.wal`

启动时按“快照 -> WAL”顺序恢复。快照代表最近一次 compaction 后的完整状态，WAL 代表其后的增量更新。

## Snapshot File

### Header

- `magic[8]`: 固定为 `KVSNAP01`
- `version`: 当前为 `2`
- `reserved`: 保留字段，当前写入 `0`

### Entry Layout

每条快照记录按以下顺序编码：

- `entry_magic`: 固定值 `0x4B565345`
- `key_size`: `uint32_t`
- `value_size`: `uint32_t`
- `key_bytes[key_size]`
- `value_bytes[value_size]`

`key_bytes` 是内部规范化键：

- `0x01 + 4字节 big-endian int32`：兼容 `int` 键 API
- `0x02 + 原始 UTF-8 / 字节串`：`std::string` 键 API

### Validation Rules

- `magic` 不匹配时拒绝加载。
- `version` 不支持时拒绝加载。
- entry header 或 value 数据截断时拒绝加载。

## WAL File

### Record Header

每条 WAL 记录头按以下顺序编码：

- `magic`: 固定值 `0x4B565741`
- `version`: 当前为 `2`
- `type`: `1=Put`, `2=Delete`
- `reserved`: 保留字段，当前写入 `0`
- `key_size`: `uint32_t`
- `value_size`: `uint32_t`
- `checksum`: 基于 `type + key + value_size + payload` 的 FNV-1a 校验

### Payload

- `Put`: 追加 `key_size` 字节的 key payload 和 `value_size` 字节的 value payload
- `Delete`: 追加 `key_size` 字节的 key payload，`value_size` 必须为 `0`

### Validation Rules

- `magic`、`version`、`type` 非法时拒绝加载。
- payload 截断且位于文件尾部时按崩溃尾巴忽略。
- checksum 不匹配时拒绝加载。

## Compatibility Policy

- 当前写入的 snapshot / WAL 版本均为 `2`。
- 运行时保留对版本 `1` 整型键格式的读取兼容，重写或 compact 后会升级到版本 `2`。
- 不支持的版本会直接抛出 `KVStoreError`，不会尝试静默兼容。
- 推荐的升级流程是先用当前程序检查格式，再执行一次 `Compact()` 以重写快照和清空 WAL。

## Operational Tools

- `./build/target/bin/kv_test inspect-format <db_path>`：打印快照/WAL 的版本、记录数量、键类型分布，以及 `rewrite_recommended` 建议位。
- `./build/target/bin/kv_test rewrite-format <db_path>`：加载数据库并执行一次 `Compact()`，将数据重写为当前格式。
- `./build/target/bin/kv_test verify-format <db_path>`：执行检查并返回机器可判定的状态码；若仍建议重写或检测到截断/损坏信号则返回非零。

### `inspect-format` Output Highlights

- `snapshot_entries` / `wal_records`：分别表示快照和 WAL 中可解析到的记录数量。
- `snapshot_int_keys` / `snapshot_string_keys` / `snapshot_binary_keys`：快照中的键类型分布。
- `wal_int_keys` / `wal_string_keys` / `wal_binary_keys`：WAL 中的键类型分布。
- `rewrite_recommended=1`：通常表示检测到旧版本格式、截断 WAL 或其它不属于当前稳定格式的情况，建议执行一次 `rewrite-format`。

### `verify-format` Exit Codes

- `0`：格式检查通过，当前数据已处于当前受支持布局。
- `2`：格式仍建议重写，或检测到截断/损坏信号，不应视为当前稳定布局。
- 其它非零值：底层 `inspect-format` 失败，例如文件不存在或头部无法读取。
