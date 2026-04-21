# Writer Design

## Scope

writer 层负责运行时写入调度，是当前内核里唯一能串行修改持久化状态的路径。

当前主入口仍在 [src/kvstore.cpp](/home/sakauma/code/lpue/src/kvstore.cpp)：

- `writer_loop`
- `collect_write_batch`
- `process_write_batch`
- `process_compaction_request`
- `maybe_auto_compact`

## Ownership

- 持有写请求队列
- 决定 batch 聚合、WAL append、`fsync` 和 apply 顺序
- 维护写入相关 metrics
- 决定手动 / 自动 compaction 的执行时机

## State Boundaries

- `queue_mutex_`：保护请求队列和 writer 唤醒
- `state_mutex_`：保护内存态 `state_`
- writer 线程：唯一允许顺序修改 WAL / snapshot 的执行点

## Invariants

- 公开写 API 不直接改磁盘，只入队并等待结果
- WAL 持久化先于内存提交可见
- compaction 仍然在 writer 串行路径中执行，不能绕开全局提交顺序

## Design Rule

- 任何新写语义都必须先判断是否需要进入 writer 队列
- 不允许在 reader 路径上直接触发磁盘写入
- 若后续继续拆模块，writer policy 与 writer execution 应继续分离，而不是把更多分支塞回主循环
