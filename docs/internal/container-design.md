# Single-File Container Design

## Scope

本设计说明描述未来“达标模式”使用的单文件容器骨架。当前代码仍在使用 snapshot/WAL 双文件模型；本设计和配套代码用于为迁移提供统一布局和校验基础。

## Layout

单文件容器从头到尾按下面顺序组织：

1. `superblock`
2. `section table`
3. `metadata`
4. `index`
5. `object data`
6. `journal`

## Sections

### `superblock`

- 文件 magic
- container version
- header bytes
- section count
- flags
- layout checksum

### `section table`

- 每个 section 的类型、标志位、偏移和长度
- 当前基础类型：
- `metadata`
- `index`
- `object data`
- `journal`

### `metadata`

- 容器级统计信息
- 空间管理元数据
- 未来的 compaction / rewrite 标记

### `index`

- 整型 key 到对象位置的映射
- 未来可扩展为分区索引或多级索引

### `object data`

- 实际对象负载
- 每个对象带长度、状态和校验信息

### `journal`

- 事务提交和恢复所需日志
- 最终目标是替代当前外部 WAL 文件

## Current Deliverable

当前阶段只把容器 header、section descriptor 和布局规划 helper 固化到内部代码中，不改变运行时持久化路径。

## Migration Intent

- 第一阶段：保持现有双文件引擎不变，只引入容器格式定义和单元测试。
- 第二阶段：让格式工具和 rewrite 工具可以识别容器格式。
- 第三阶段：把运行时恢复和提交路径迁移到单文件 journal。
