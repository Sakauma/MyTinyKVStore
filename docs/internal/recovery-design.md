# Recovery Design

## Scope

恢复层负责把“快照 + WAL”重新组装成内存状态，并为 rewrite / compatibility 提供稳定基础。

当前核心入口仍在 [src/kvstore.cpp](/home/sakauma/code/lpue/src/kvstore.cpp)：

- `ensure_snapshot_exists`
- `load_snapshot`
- `replay_wal`
- `current_wal_file_size`

## Recovery Order

1. 确保 snapshot 文件存在
2. 加载 snapshot 到内存状态
3. 顺序重放 WAL
4. 重新打开 WAL append 句柄

## Owned Responsibilities

- 检查 snapshot / WAL 版本与 magic
- 识别截断尾部与 checksum 错误
- 将 legacy v1 数据映射到当前内存 key 编码
- 在 rewrite 后收敛回当前布局

## Invariants

- 快照始终被视为“较旧但完整”的基线。
- WAL 是最近已提交状态的补丁层。
- 截断 WAL 尾部可以在恢复时忽略，但 `verify-format` 仍会要求 rewrite。

## Design Rule

- 恢复层只能重建状态，不能擅自调整 writer 策略。
- 任何新增格式版本都必须先补恢复路径，再开放写入。
