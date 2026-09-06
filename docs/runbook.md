# 运维手册

## 平台与构建

目标平台是提供所需 `flock`、`pread/pwrite`、原子 rename 和目录同步语义的 Linux POSIX 文件系统。WSL 下数据库与测试临时目录必须位于 Linux 文件系统；`/mnt/*`、DrvFS、9p 和 fuseblk 未纳入持久化正确性支持范围，也不能用于性能或长时认证。源码可以位于 Windows 挂载路径，但 workload 数据不能。

```bash
cmake -S . -B build-release -DCMAKE_BUILD_TYPE=Release
cmake --build build-release --parallel
cd build-release && ctest --output-on-failure
```

Python/CUDA 不是运行依赖。以后若添加相关运维工具，统一在 WSL Miniconda 虚拟环境执行。

## 打开与关闭

- 每个主文件只能被一个 `KVStore` 实例独占打开。
- 第二个实例收到 `Database is already open` 时，不要绕过锁；先定位仍持有文件的进程。
- 需要明确持久化确认时，在关闭前调用 `Flush()`。
- 析构同步是 best-effort，不能替代可报告的 `Flush()`。
- 在线复制正在写入的单文件不保证得到一致备份。可靠备份应停止写入、`Flush()`、关闭实例，再复制主文件。

## Durability 选择

| 场景 | 建议 |
| --- | --- |
| 默认、不可丢已确认事务 | `kSync` |
| 可容忍最近一个周期丢失 | `kPeriodic`，默认 10 ms |
| 上层自己管理屏障、批量导入 | `kNoSync` + 明确 `Flush()` |

切换到较弱模式必须由业务明确接受崩溃窗口。Compaction 会同步新容器，因此可能提前持久化弱模式下尚未显式 Flush 的 frame，但调用方不能依赖这一副作用。

## 日常检查

```bash
bash scripts/verify-format.sh /data/store.db
bash scripts/inspect-format.sh /data/store.db
```

重点字段：

- `valid_superblocks=2`
- `truncated_tail=0`
- `last_lsn`
- `checkpoint_lsn`
- `journal_frames`
- `checkpoint_entries`

`verify-format` 非零时先保留原始文件和完整错误输出，不要直接覆盖。

## 指标

### 提交与同步

- `group_commit_calls` / `group_commit_requests` / `max_group_commit_requests`
- `wal_fsync_calls`
- `observed_fsync_pressure_per_1000_writes`：最后一个批次的 fsync 压力
- `recent_fsync_pressure_per_1000_writes`：最近批次窗口累计的 fsync 压力，供自适应策略使用
- `fdatasync_time_us` / `max_fdatasync_time_us`
- `approx_write_latency_p50_us/p95_us/p99_us`
- `pending_queue_depth` / `max_pending_queue_depth`
- `prepared_queue_depth`
- `inflight_request_count` / `max_inflight_request_count`

### Worker 与事务

- `worker_tasks_completed`
- `worker_busy_time_us`
- `active_workers` / `max_active_workers`
- `worker_utilization_per_1000`
- `transaction_commits` / `transaction_conflicts` / `transaction_rollbacks`

### Compaction 与容量

- `wal_bytes_since_compaction`
- `live_wal_bytes_since_compaction` / `obsolete_wal_bytes_since_compaction`
- `manual_compactions_completed` / `auto_compactions_completed`
- `auto_compaction_failures`
- `compaction_pause_time_us` / `max_compaction_pause_time_us`
- `total_snapshot_bytes_written`
- `total_wal_bytes_reclaimed_by_compaction`

### Cache 与恢复

- `value_cache_hits` / `value_cache_misses`
- `recovery_time_us`

Value cache 是按规范化 key + LSN 标识的分段 CLOCK，不提供精确 LRU 顺序保证。容量为零时关闭；非零容量按每至少 64 KiB 一个 segment 配置 1–64 个二次幂 segment，所有 segment charge 之和不会超过 `value_cache_bytes`。

## Compaction 调优

自动 compaction 默认关闭。启用时建议先只设置字节阈值，再依据真实 overwrite/delete 负载考虑无效比例阈值：

```cpp
options.auto_compact_wal_bytes_threshold = 256ULL * 1024 * 1024;
options.auto_compact_invalid_wal_ratio_percent = 0;
options.auto_compact_min_wal_bytes_for_ratio = 256ULL * 1024 * 1024;
```

两个触发条件为 OR。`auto_compact_min_wal_bytes_for_ratio` 只约束比例条件；设为 `0` 会允许很小的 journal 仅因比例达到阈值而压缩。建议让它至少等于业务可接受的最小压缩体积。观察：

- `auto_compactions_completed` 是否异常快速增长。
- `auto_compaction_failures` 是否增长；后台安全失败会退避至少 1 秒，并等待后续写入再次触发。
- `max_compaction_pause_time_us` 是否影响写 p99。
- checkpoint 写入带宽是否挤压 journal 同步。
- 数据集增长后 RSS 是否主要来自 key/index，而不是 value cache。

手动 `Compact()` 保证返回时 entry 迁移完成，但不等待已经取得旧代际快照的 reader。旧 inode 和对应空间在最后一个 reader 释放引用后自然回收，不能把 `Compact()` 返回解释为磁盘空间已立即全部释放。自动 compaction 在后台运行；最终提交暂停只包含 delta 复制、临时文件同步、rename、代际/accounting 发布和目录同步。逐 shard entry 迁移发生在恢复提交之后。观察 `max_compaction_pause_time_us` 时应把它解释为这段切换暂停，而不是完整 compaction 时长。

## Sticky fatal 处理

Journal/主代际 I/O、checksum、short write、sync、主状态不变量或 compaction rename 后错误会进入 sticky fatal：

1. 停止向该实例发送新请求。
2. 记录第一条错误；后续错误应与它相同。
3. 不要尝试在同一实例上“恢复运行”。
4. 关闭进程并保留数据库文件、所在目录和系统日志。
5. 使用只读 `verify-format` 检查。
6. 若只是已识别的不完整最终 frame，可备份后用运行库打开修复，再重新验证。
7. 完整 checksum、边界或中段损坏必须人工调查，不能自动跳过。

尚未 rename 的 compaction 临时文件创建、写入、同步或普通 failpoint 错误属于可恢复操作失败：调用会报错并只清理本次创建的临时文件，实例仍可继续服务。读取当前主代际 value/WAL delta 时的 EOF、checksum 或状态不变量错误即使发生在 rename 前也不可继续。遗留 `.compact.*` 文件不是恢复依赖；新 compaction 遇到文件名碰撞会换随机 nonce 重试，不会删除碰撞文件。

磁盘满或配额错误需要先释放其他空间。不要在空间仍不足时反复 compaction，因为临时容器需要接近 live 数据集大小的额外空间。

## 事务冲突

`KVStoreConflictError` 不是存储故障。调用方应丢弃旧事务、退避并从新事务重新读取。冲突长期偏高时：

- 检查热点 key 是否集中在少数 shard。
- 缩短事务生命周期。
- 减少事务访问的 shard 数。
- 不要在捕获冲突后复用已经失效的事务对象。

## Scan 影响

字符串 `Scan` 在持有全部 shard 共享锁时完成索引合并和 entry 快照，然后释放锁并读取 value。优先使用 `Scan(start, end, limit)` 控制单次快照大小；`limit == 0` 返回空，多次调用不提供跨页一致快照。若写 p99 突升时伴随长 Scan：

- 缩小范围或减少返回对象。
- 降低 Scan 频率。
- 把大规模导出放到停写维护窗口。

## 测试与认证

```bash
bash scripts/ci-build.sh
./build-release/target/bin/kv_unit_test --list
./build-release/target/bin/kv_test --list-groups
./build-release/target/bin/kv_test --group recovery-format
./build-release/target/bin/kv_test --filter "compaction"
bash scripts/ci-sanitizers.sh
bash scripts/concurrency-stress.sh 10 balanced
bash scripts/concurrency-stress.sh 10 compaction-heavy
qualification_root="$HOME/kvstore-qualification/$(date +%Y%m%d-%H%M%S)"
mkdir -p "$qualification_root"
bash scripts/qualification-benchmark.sh \
  "$qualification_root/benchmark" \
  /path/to/frozen-baseline.json
bash scripts/qualification-run.sh "$qualification_root/soak"
```

筛选不到用例会返回非零状态。排查失败时可以设置 `KVSTORE_KEEP_TEST_ARTIFACTS=1`；测试 runner 会继续执行其余独立命名用例，并把保留目录的绝对路径写到标准错误。

本地 WSL TSan 脚本默认使用 `/usr/bin/g++-10`；如需替换，设置 `KVSTORE_TSAN_CXX`。GitHub Actions 显式使用 runner 提供的 `/usr/bin/g++`。直接用 CMake/CTest 时，应在配置阶段通过 `-DKVSTORE_TSAN_OPTIONS=...` 传入扩展选项；配置完成后只修改进程环境中的 `TSAN_OPTIONS` 不会覆盖 CTest 已记录的属性。
GCC 10 deadlock detector 受 64 锁上限影响，而 `Scan` 需要同时持有 256 个 shard
锁，所以脚本设置 `detect_deadlocks=0`。这不会关闭数据竞争检测，任何竞态报告仍令
CTest 或并发压力测试失败。TSan 缺失是失败，不会标为 SKIP。

正式 qualification 开始前，输出所在 ext4 文件系统必须至少有 25 GiB 可用空间。命令、环境和结果必须一起保存在 WSL ext4 的仓库外目录，不提交或推送；短时 smoke 不能替代 12 小时认证。

## 常见故障

### 第二实例无法打开

预期的独占锁行为。使用 `lsof /data/store.db` 或 `/proc/<pid>/fd` 找到持有者，确认旧进程退出后再重试。

### 恢复报告 truncated tail

说明最终事务 frame 没有完整落盘。`inspect-format` 会报告，读写打开会截断到最后完整 frame。修复前应先复制现场以便调查。

### 完整 frame checksum mismatch

属于损坏，不是正常崩溃尾部。检查介质、文件被外部修改、错误备份流程和内存/硬件告警。

### Compaction 频繁

提高字节阈值或 `auto_compact_min_wal_bytes_for_ratio`，禁用/提高无效比例阈值，并确认没有把测试级几 KiB 阈值带到大数据集。

### 自动 Compaction 失败增长

查看 `auto_compaction_failures` 和首个操作错误。临时文件空间、权限或同步失败在 rename 前通常可恢复，修复环境后由后续写入重新调度；主代际读取/校验错误或 rename 后错误会进入 sticky fatal，按上面的保留现场流程处理。不要手工删除无法确认所有权的 `.compact.*` 文件。

### p99 高但 CPU 低

检查 `max_fdatasync_time_us`、group size 和文件系统。`kSync` 的尾延迟经常由同步延迟决定；盲目增加 worker 不能消除磁盘同步瓶颈。

更完整语义见[一致性与持久化语义](semantics.md)和[文件格式](file-format.md)。
