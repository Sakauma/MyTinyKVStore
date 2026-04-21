# Format Design

## Scope

格式层负责定义“磁盘上长什么样”，不负责决定何时写，也不负责恢复顺序控制。

当前对应实现主要落在：

- [src/internal/format.h](/home/sakauma/code/lpue/src/internal/format.h)
- [src/internal/format.cpp](/home/sakauma/code/lpue/src/internal/format.cpp)

## Owned Concepts

- snapshot / WAL 的 magic、version 和头结构
- key namespace tag：`int` / `string` / `binary`
- key 编码与解码
- WAL checksum 计算
- snapshot header 构造

## Non-Goals

- 不决定 WAL 何时 `fsync`
- 不决定 compaction 何时触发
- 不持有运行时状态

## Design Rule

- 所有磁盘格式常量必须集中在 format 层定义。
- 业务层只能消费编码/校验接口，不能重复内联格式细节。
- 若将来引入 v3/v4 格式，优先扩 format 层，而不是把版本分支散落到 writer/recovery 中。
