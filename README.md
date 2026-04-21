# MyTinyKVStore

一个以正确性优先、并兼顾高并发吞吐的小型持久化键值存储引擎。当前实现采用“内存索引 + 快照文件 + WAL”模型，通过后台 writer 线程批量提交 WAL，在保证写入顺序和崩溃恢复的前提下减少并发写入争用。

## 当前特性

- 批量提交 `Put` / `Delete`：调用线程提交请求后由后台 writer 按顺序写入 WAL，并对同一批请求执行一次 `fsync`。
- 原子批量写入 `WriteBatch`：单个批量请求中的多条 `put/delete` 会按给定顺序写入 WAL，并作为一个公开 API 调用整体完成。
- 多类型键支持：当前公开 API 已支持 `int`、`std::string` 和 `std::vector<uint8_t>` 三类键。
- 可配置批量策略：可通过 `KVStoreOptions` 调整最大批次大小和批量等待时间。
- 可配置批量体积：可通过 `max_batch_wal_bytes` 控制单批 WAL 最大字节数，避免只按请求条数聚合。
- 自适应批量：队列积压达到阈值后，writer 会临时放大批次大小和 WAL 字节预算，在吞吐和 `fsync` 放大之间做动态折中。
- 自适应 flush：队列压力升高时，writer 会缩短当前批次的等待时间，避免固定 `batch delay` 拉高拥塞时延。
- 尾延迟/`fsync` 压力联动：writer 还会参考近期 `p95` 延迟和 `fsync` 压力，动态提前 flush 或放宽聚合等待。
- 短窗口观测：近期 `p95`、近期队列峰值和近期平均 batch size 都基于短窗口统计，能更快反映突发负载。
- 更多负载信号：策略还会识别 read-heavy 负载、WAL 增长速率和 compaction 压力，分别调整 batch delay。
- 组合目标函数：可把队列压力、尾延迟、读占比、`fsync` 压力、WAL 增长和 compaction 成本收敛成同一组 score，在“更快 flush”和“更大批量”之间做统一权衡。
- 吞吐导向控制：组合目标函数还会跟踪近期平均 batch size 与目标 batch size 的缺口，把“吞吐效率不足”显式纳入长 delay 决策。
- 可配置自动 Compaction：可按 WAL 累积字节阈值或无效 WAL 比例自动触发快照重写和 WAL 轮转。
- 启动恢复：先加载快照文件，再顺序重放 WAL，恢复到最近一次已提交状态。
- 显式 Compaction：将当前内存状态写成新快照，并原子替换旧快照，同时轮转为空 WAL。
- 并发读取：`Get` 通过读写锁直接读取内存态，不与普通读取互相阻塞。
- 运行指标：可通过 `GetMetrics()` 查看批次数、`fsync` 次数、WAL 写入字节数和当前队列深度。
- 后台线程指标：可观察 writer 的累计等待次数、累计等待时间、队列高水位和批量 WAL 字节数。
- 维护指标：可区分手动与自动 compaction 次数，并观察当前距离上次 compaction 的 WAL 累积字节数、有效字节数和无效字节数。
- 延迟统计：固定桶统计每个写请求从入队到完成的延迟分布，并给出近似 `p50/p95/p99`，便于观察尾延迟而不只看平均值。
- 明确错误模型：I/O、文件损坏和格式不兼容会抛出 `KVStoreError`，而不是静默打印错误。
- 确定性测试：覆盖持久化、WAL 恢复、顺序一致性、损坏检测、并发写入和 compaction。

## 架构概览

1. 快照文件保存最近一次 compact 后的完整键空间。
2. 公共写接口只负责入队和等待确认；后台 writer 线程串行消费请求，保证全局提交顺序。
3. writer 会尽量收集当前队列中的写请求，顺序写入 WAL，并对整个批次执行一次 `fsync`。
4. 当前批次会同时受“最大请求数”“最大 WAL 字节数”“最大等待时间”三种条件约束。
5. WAL 持久化完成后，writer 再把该批次更新应用到内存中的有序 `std::map<std::string, Value>`。
6. 每个写请求完成时都会更新延迟直方图，用于观测批量策略对尾延迟的影响。
7. `Get` 使用读写锁直接读取内存态，多个读线程可以并发执行。
8. writer 会跟踪“当前 WAL 中仍代表最新状态的记录字节数”，从而估算自上次 compaction 以来的无效 WAL 字节。
9. 当 WAL 自上次 compaction 以来的累计字节数超过阈值，或无效 WAL 比例超过阈值时，writer 会在同一串行路径上自动执行 compaction。
10. 重启时按“快照 -> WAL”顺序恢复，因此即使快照落后，最近已提交操作也不会丢失。

## API 摘要

```cpp
KVStore store("data.db");
KVStore tuned_store("data.db", KVStoreOptions{});

store.Put(42, Value(std::vector<uint8_t>{'o', 'k'}));
store.Put(std::string("user:42"), Value(std::vector<uint8_t>{'a', 'b'}));
auto value = store.Get(42);
auto string_value = store.Get(std::string("user:42"));
auto range = store.Scan("user:00", "user:99");
KVStoreMetrics metrics = store.GetMetrics();
```

当前公开 API 同时支持：

- `int` 键：保留兼容旧接口
- `std::string` 键：用于新的持久化格式与范围扫描

可调参数：

- `max_batch_size`：writer 一次最多合并多少个写请求。
- `max_batch_wal_bytes`：writer 一次最多合并多少字节的 WAL 记录，`0` 表示仅受条数限制。
- `max_batch_delay_us`：writer 在取到首个写请求后，最多等待多久来扩展当前批次。
- `adaptive_recent_window_batches`：近期批次窗口大小，用于估算近期队列峰值、近期平均 batch size 和近期 `fsync` 压力。
- `adaptive_recent_write_sample_limit`：近期写延迟样本窗口大小，用于估算近期 `p95`。
- `adaptive_objective_enabled`：是否启用组合目标函数策略；开启后，writer 会按压力 score 与成本 score 的差值统一决定是缩短还是放宽 batch delay。
- `adaptive_objective_*_weight`：分别控制队列、尾延迟、读占比、`fsync` 压力、compaction 压力和 WAL 增长信号在目标函数中的权重。
- `adaptive_objective_throughput_weight` / `adaptive_objective_target_batch_size`：控制“近期平均 batch size 低于目标值”时的吞吐效率缺口权重与目标 batch size。
- `adaptive_objective_short_delay_score_threshold` / `adaptive_objective_long_delay_score_threshold`：压力 score 或成本 score 需要领先多少，才会触发缩短或放宽 batch delay。
- `adaptive_objective_short_delay_divisor` / `adaptive_objective_long_delay_multiplier` / `adaptive_objective_max_batch_delay_us`：目标函数触发后，对 batch delay 的缩放方式和上限。
- `adaptive_read_heavy_read_per_1000_ops_threshold`：近期每 1000 个操作里读请求达到多少时，视为 read-heavy 负载，`0` 表示关闭。
- `adaptive_read_heavy_delay_divisor` / `adaptive_read_heavy_batch_size_divisor`：read-heavy 负载下，对 batch delay 和 batch size 收缩的除数。
- `adaptive_flush_enabled`：是否在队列压力升高时动态缩短批次等待时间。
- `adaptive_flush_queue_depth_threshold`：待处理队列达到多少条时开始缩短 batch delay。
- `adaptive_flush_delay_divisor`：每一级队列压力下，对 batch delay 缩短的除数。
- `adaptive_flush_min_batch_delay_us`：自适应 flush 允许缩短到的最小 batch delay。
- `adaptive_latency_target_p95_us`：当近期近似 `p95` 写延迟超过该目标时，writer 会进一步缩短 batch delay，`0` 表示关闭。
- `adaptive_fsync_pressure_per_1000_writes_threshold`：当观察到每 1000 次写入对应的 `fsync` 压力超过该阈值时，writer 会放宽 batch delay，`0` 表示关闭。
- `adaptive_fsync_pressure_delay_multiplier`：触发 `fsync` 压力调节时，对 batch delay 放大的倍数。
- `adaptive_fsync_pressure_max_batch_delay_us`：`fsync` 压力调节允许放大到的最大 batch delay，`0` 表示仅受乘数约束。
- `adaptive_compaction_pressure_obsolete_ratio_percent_threshold`：当当前无效 WAL 比例达到该阈值时，认为 compaction 压力升高，`0` 表示关闭。
- `adaptive_compaction_pressure_delay_multiplier`：触发 compaction 压力调节时，对 batch delay 放大的倍数。
- `adaptive_wal_growth_bytes_per_batch_threshold`：近期平均每批 WAL 字节达到该阈值时，认为 WAL 增长速率较高，`0` 表示关闭。
- `adaptive_wal_growth_delay_multiplier` / `adaptive_wal_growth_max_batch_delay_us`：WAL 增长速率调节下，对 batch delay 放大的倍数和上限。
- `adaptive_batching_enabled`：是否在队列积压时启用自适应批量。
- `adaptive_queue_depth_threshold`：待处理队列达到多少条时切换到自适应批量策略。
- `adaptive_batch_size_multiplier`：自适应模式下，对 `max_batch_size` 放大的倍数。
- `adaptive_batch_wal_bytes_multiplier`：自适应模式下，对 `max_batch_wal_bytes` 放大的倍数。
- `auto_compact_wal_bytes_threshold`：当 WAL 自上次 compaction 以来累计达到该字节数时自动 compact，`0` 表示关闭。
- `auto_compact_invalid_wal_ratio_percent`：当无效 WAL 字节占比达到该百分比时自动 compact，`0` 表示关闭。

常用指标：

- `wal_bytes_since_compaction`：当前 WAL 自上次 compaction 以来累计了多少字节。
- `live_wal_bytes_since_compaction` / `obsolete_wal_bytes_since_compaction`：当前 WAL 中仍代表最新状态的字节数，以及已经被覆盖/删除淘汰的字节数。
- `manual_compactions_completed` / `auto_compactions_completed`：手动和自动 compaction 的完成次数。
- `adaptive_batches_completed`：在队列压力下按自适应策略提交的批次数。
- `adaptive_flush_batches_completed`：以缩短后的 batch delay 提交的批次数。
- `adaptive_latency_target_batches_completed` / `adaptive_fsync_pressure_batches_completed`：分别统计由尾延迟目标和 `fsync` 压力驱动的自适应批次数。
- `adaptive_read_heavy_batches_completed` / `adaptive_compaction_pressure_batches_completed` / `adaptive_wal_growth_batches_completed`：分别统计 read-heavy、compaction 压力和 WAL 增长速率触发的批次数。
- `adaptive_objective_short_delay_batches_completed` / `adaptive_objective_long_delay_batches_completed`：分别统计组合目标函数偏向更短或更长 batch delay 的批次数。
- `adaptive_objective_throughput_batches_completed`：统计组合目标函数检测到吞吐效率缺口的批次数。
- `last_committed_batch_wal_bytes` / `max_committed_batch_wal_bytes`：最近一次批量提交和历史最大批量提交各写入了多少 WAL 字节。
- `pending_queue_depth` / `max_pending_queue_depth`：当前待处理队列深度和历史高水位。
- `writer_wait_events` / `writer_wait_time_us`：writer 等待新请求或等待批次扩展的次数，以及累计等待时间。
- `last_effective_batch_delay_us` / `min_effective_batch_delay_us` / `max_effective_batch_delay_us`：最近一次、历史最小和历史最大的实际 batch delay 预算。
- `observed_fsync_pressure_per_1000_writes`：基于近期批量大小估算的 `fsync` 压力，值越高表示越接近“每次写都要 fsync”。
- `last_objective_pressure_score` / `last_objective_cost_score` / `last_objective_throughput_score` / `last_objective_balance_score`：最近一次组合目标函数计算出的压力分、成本分、吞吐效率分和二者差值。
- `last_objective_mode`：最近一次 objective 决策模式，`1` 表示偏向更短 delay，`-1` 表示偏向更长 delay，`0` 表示保持中性。
- `recent_read_ratio_per_1000_ops` / `recent_avg_batch_wal_bytes` / `observed_obsolete_wal_ratio_percent`：近期读占比、近期平均每批 WAL 字节和当前无效 WAL 比例。
- `recent_batch_fill_per_1000`：近期平均 batch size 相对目标 batch size 的填充比例，`1000` 表示达到或超过目标吞吐填充。
- `total_snapshot_bytes_written` / `total_wal_bytes_reclaimed_by_compaction`：长期累计写入快照的字节数，以及被 compaction 回收的 WAL 字节数。
- `approx_write_latency_p50_us` / `approx_write_latency_p95_us` / `approx_write_latency_p99_us`：基于延迟直方图计算的近似分位数。
- `recent_observed_write_latency_p95_us` / `recent_peak_queue_depth` / `recent_avg_batch_size`：短窗口下观测到的近期 `p95`、近期队列峰值和近期平均 batch size。
- `write_latency_histogram`：12 个固定桶的写入延迟分布，桶上界依次为 `50us`、`100us`、`250us`、`500us`、`1ms`、`2.5ms`、`5ms`、`10ms`、`25ms`、`50ms`、`100ms`、`>100ms`。

## 构建与测试

### 依赖

- CMake >= 3.15
- 支持 C++17 的编译器

### 构建

```bash
cmake -S . -B build
cmake --build build
```

产物会输出到：

- `build/target/bin/kv_test`
- `build/target/lib/libkvstore.so`

### 运行测试

```bash
./build/target/bin/kv_test
cd build && ctest --output-on-failure
./target/bin/kv_test bench
```

`bench` 会打印基础吞吐、全局与近期的延迟/队列指标、读占比、WAL 增长/无效比例、`fsync` 压力、组合目标函数 score、吞吐填充比例、自适应策略命中次数、writer 等待指标、长期 compaction 字节统计和写入延迟直方图，便于快速观察瞬时突发和长期趋势。

仓库内还提供了几个标准化脚本：

- `bash scripts/ci-build.sh`：执行常规构建、测试和 `ctest`。
- `bash scripts/ci-sanitizers.sh`：分别执行 ASan 和 UBSan 构建与测试。
- `bash scripts/coverage.sh`：执行 coverage 构建与测试；若本地安装了 `gcovr`，会直接打印 coverage 摘要。
- `bash scripts/bench.sh`：构建并运行 benchmark。
- `bash scripts/soak.sh 10`：运行带 compaction 的混合读写 soak test，并在结束后重启校验数据一致性。
- `bash scripts/inspect-format.sh <db_path>`：检查快照和 WAL 的格式版本、记录数量、键类型分布，以及是否建议重写迁移。
- `bash scripts/rewrite-format.sh <db_path>`：加载数据库并执行一次 `Compact()`，把数据重写到当前格式。
- `bash scripts/verify-format.sh <db_path>`：检查一份数据是否已经处于当前受支持格式；若仍建议重写则返回非零状态。
- `bash scripts/profile.sh <balanced|write-heavy|read-heavy|low-latency>`：打印推荐配置模板的 JSON。

当前测试程序还内置了故障注入入口 `kv_test fault-inject <scenario> <db_path>`，用于在 WAL `fsync` 之后、快照 rename 前后、WAL 轮转后等关键持久化点模拟进程崩溃。常规测试会自动通过子进程调用这些场景来验证重启恢复。
若需要结构化指标，可调用 `kv_test bench-json` 或直接使用库函数 `MetricsToJson(store.GetMetrics())`。
若需要推荐配置模板，可调用 `kv_test profile-json <name>` 或 `RecommendedOptions(...)`。

磁盘格式说明见 [docs/file-format.md](/home/sakauma/code/lpue/docs/file-format.md)。当前程序会写入版本 `2` 的 snapshot / WAL，同时保留对版本 `1` 整型键格式的读取兼容。
运维建议和调参说明见 [docs/runbook.md](/home/sakauma/code/lpue/docs/runbook.md)。
持久化、可见性和 `WriteBatch` 原子性语义见 [docs/semantics.md](/home/sakauma/code/lpue/docs/semantics.md)。
事务与快照读的设计边界见 [docs/transaction-boundary.md](/home/sakauma/code/lpue/docs/transaction-boundary.md)。

### 可选 Sanitizer

```bash
cmake -S . -B build-asan -DKVSTORE_ENABLE_ASAN=ON
cmake --build build-asan
```

也可以启用 `-DKVSTORE_ENABLE_UBSAN=ON` 进行未定义行为检查。

### Coverage

```bash
cmake -S . -B build-coverage -DKVSTORE_ENABLE_COVERAGE=ON
cmake --build build-coverage
ctest --test-dir build-coverage --output-on-failure
```

## 后续改进

- 增加更完整的键类型迁移和跨类型比较策略
- 将多目标控制器继续扩展为带反馈学习的自调参策略
- 增加更完整的格式迁移与跨版本兼容策略
- 在批量原子写稳定后再评估事务支持与快照读语义
