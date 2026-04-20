# KVStore Reliability Plan

## Goal

将当前仓库演进为一个“正确性优先”的小型持久化 KV 内核，优先保证写入顺序、崩溃恢复、明确错误语义和可重复测试。

## Phase 1: Baseline and Regression Tests

- 用确定性测试替换依赖 `sleep` 的旧测试。
- 覆盖基础持久化、WAL 恢复、顺序一致性、compaction、尾部截断和 WAL 损坏检测。
- 在 CMake 中启用常规告警，并保留 ASan / UBSan 构建开关。

## Phase 2: Reliable Storage Core

- 移除异步多消费者写入模型，改为同步串行 `Put` / `Delete`。
- 内存中的 `std::map<int, Value>` 成为运行时唯一事实来源。
- 将 `Value` 改为拥有型二进制容器，避免外部缓冲区生命周期问题。
- 用 `KVStoreError` 显式传播 I/O、损坏和格式错误。

## Phase 2.5: High-Concurrency Write Path

- 引入专用 writer 线程，将调用线程的写请求入队后按提交顺序处理。
- 将多个并发写请求合并成 WAL 批次，批量写入并执行单次 `fsync`。
- 用读写锁保护内存状态，使多个 `Get` 可以并发执行，而写入只在批次提交时短暂独占。
- 将 `Compact` 也纳入同一提交序列，确保它与普通写入在全局顺序上可推导。

## Phase 2.6: Batching Controls and Observability

- 暴露 `KVStoreOptions`，允许配置批量大小和批量等待时间。
- 暴露 `KVStoreMetrics`，统计写请求数、批次数、`fsync` 次数、WAL 字节数和队列深度。
- 提供简单 `bench` 入口，便于在本地快速观察吞吐、平均写延迟和批量效果。

## Phase 2.6.1: Byte-Based Batching and Latency Histogram

- 增加 `max_batch_wal_bytes`，让批量聚合同时受条数、字节数和等待时间约束。
- 为每个写请求记录从入队到完成的延迟，并汇总到固定桶直方图。
- 在测试和 benchmark 输出中验证批量体积限制与延迟分布指标。

## Phase 2.7: Automatic Compaction

- 暴露基于 WAL 累积字节数的自动 compaction 阈值。
- 保持自动 compaction 仍由 writer 串行执行，避免破坏全局提交顺序。
- 增加手动/自动 compaction 分离指标，并验证重启后状态正确。

## Phase 2.7.1: Invalid-WAL-Ratio Compaction Trigger

- 跟踪当前 WAL 中仍代表最新状态的记录字节数，以及已经失效的记录字节数。
- 增加基于无效 WAL 比例的自动 compaction 阈值，用于高覆盖率更新和删除场景。
- 在测试中验证覆盖写入场景会触发 compaction，且重启后状态保持正确。

## Phase 2.8: Adaptive Batching

- 在 writer 取批次时根据当前队列深度动态切换批量策略，而不是只使用固定上限。
- 自适应模式下同时放大“最大请求数”和“最大 WAL 字节数”预算，并继续受最大等待时间约束。
- 在指标中暴露自适应批次数，并在测试和 benchmark 中验证高队列压力下批次确实扩大。

## Phase 2.9: Percentiles and Writer Observability

- 将写入延迟直方图细化为更多固定桶，并从中导出近似 `p50/p95/p99`。
- 记录 writer 线程的等待次数、累计等待时间、队列高水位和批量 WAL 字节数。
- 在测试和 benchmark 中验证这些指标会随着并发压力和 writer 行为变化而更新。

## Phase 2.10: Adaptive Flush and Long-Running Counters

- 根据队列深度动态缩短 batch delay，不再只使用固定 `max_batch_delay_us`。
- 将自适应 flush 的命中次数、最近一次/最小实际 batch delay 暴露到指标。
- 为 compaction 增加长期累计计数，例如累计写入的快照字节数和累计回收的 WAL 字节数。
- 在测试和 benchmark 中验证高压下会提前 flush，且长期累计指标会随多次 compaction 增长。

## Phase 2.11: Latency Targets and Fsync Pressure

- 将近期近似 `p95` 延迟纳入批次策略，当尾延迟超过目标时进一步缩短 batch delay。
- 跟踪近期 `fsync` 压力，并在 batching 不足时适度放宽 batch delay，减少“几乎每次写都 fsync”的情况。
- 暴露基于尾延迟目标和 `fsync` 压力触发的批次数，以及当前观察到的 `fsync` 压力水平。
- 在测试和 benchmark 中分别验证“高尾延迟促使更早 flush”和“高 fsync 压力促使更长聚合等待”。

## Phase 2.12: Short-Window Load Signals

- 将近期队列峰值、近期平均 batch size 和近期 `p95` 延迟做成独立短窗口指标，而不是只依赖全局累计视图。
- 让 adaptive batching / adaptive flush 同时参考即时队列深度和短窗口队列峰值，提高对突发负载的响应速度。
- 在测试和 benchmark 中验证短窗口指标能捕获 burst，并影响后续批次策略。

## Phase 2.13: More Load Signals

- 将近期读占比纳入策略，在 read-heavy 场景下缩短 batch delay 并收缩 batch size，减少读线程受写批次影响的时间。
- 将近期平均每批 WAL 字节和当前无效 WAL 比例纳入策略，用于识别 WAL 增长速率和 compaction 压力。
- 为 read-heavy、WAL 增长和 compaction 压力分别暴露触发次数与观测指标。
- 在测试和 benchmark 中分别验证这三类信号能够独立触发策略调整。

## Phase 2.14: Objective-Driven Adaptive Control

- 增加可选的组合目标函数，将队列压力、近期 `p95`、读占比、`fsync` 压力、WAL 增长和 compaction 压力统一折算成 pressure score 与 cost score。
- 当 pressure score 明显高于 cost score 时，优先缩短 batch delay；当 cost score 明显高于 pressure score 时，优先放宽 batch delay。
- 暴露最近一次 pressure score、cost score、balance score，以及偏向短 delay / 长 delay 的批次数。
- 在测试和 benchmark 中分别验证“高尾延迟压力”和“高 `fsync`/compaction 成本压力”下的策略选择稳定可观测。

## Phase 2.15: Throughput-Aware Multi-Objective Control

- 将“近期平均 batch size 低于目标 batch size”的吞吐效率缺口纳入 objective cost score，而不再只依赖 `fsync`、WAL 增长和 compaction 成本。
- 暴露最近一次 throughput score、objective mode，以及近期 batch fill 比例，便于区分“吞吐导向延长 delay”和“成本导向延长 delay”。
- 在测试和 benchmark 中验证吞吐效率缺口能够压过延迟压力，稳定触发更长的 batch delay。

## Phase 3: WAL and Recovery

- WAL 记录包含 `magic`、`version`、`type`、`key`、`value_size` 和 `checksum`。
- 每次 `Put` / `Delete` 返回前，必须完成 WAL 追加并 `fsync`。
- 启动恢复流程固定为“加载快照 -> 顺序重放 WAL”。
- WAL 尾部不完整记录按崩溃尾巴安全忽略；校验失败按损坏处理。

## Phase 4: Snapshot and Compaction

- 快照文件保存最近一次 compaction 后的完整状态。
- compaction 必须写入临时文件、`fsync` 后再原子 `rename`。
- compaction 成功后轮转为空 WAL。
- README 和构建说明必须与实现语义保持一致。

## Next Improvements

- 支持字符串键
- 提供范围扫描接口
- 提供更细粒度的 flush 策略、延迟直方图和后台线程指标
- 将多目标控制器进一步扩展为带反馈学习的自调参策略
- 为自动 compaction 增加基于碎片率和更多长期运行信号的触发条件
- 设计文件格式升级和兼容策略
- 在稳定内核之上再评估事务支持
