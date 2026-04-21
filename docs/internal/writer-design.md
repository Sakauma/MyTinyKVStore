# Writer Design

## Scope

writer 层负责运行时写入调度，是当前内核里唯一能串行修改持久化状态的路径。

当前 writer 主入口仍在 [src/kvstore.cpp](/home/sakauma/code/lpue/src/kvstore.cpp)，但运行时职责已经拆成四块：

- `writer_policy`：基于 queue / latency / fsync / recent-window 信号计算 `BatchPolicy`
- `writer_wait`：统一记录 queue wait event 和 wait time
- `request_runtime`：负责请求入队、完成、fatal-state 传播
- `writer_execution`：负责 batch 收集、WAL append、apply、compaction 执行

## Ownership

- `KVStore::Impl`
  - 拥有 queue、writer thread、state、WAL fd 和所有 metrics/state atomics
  - 负责线程启动/收尾、public API glue、异常分派
- `request_runtime`
  - 只处理请求生命周期，不拥有持久化状态
- `writer_execution`
  - 只执行串行写路径，不拥有 queue 生命周期

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
- 若后续继续拆模块，优先继续把 `writer_loop` 中的分支编排抽成 helper，而不是把执行细节重新塞回主循环
- request lifecycle、wait metrics、execution 三层都必须保持无状态 helper 形态，避免制造第二层内部状态所有权
