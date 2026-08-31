# 运维手册

## 平台与构建

目标平台是 Linux/WSL POSIX 文件系统。正式性能与长时验证必须使用 Release 和原生 Linux 文件系统，不能把 `/mnt/*`、9p、DrvFS 或 fuseblk 上的结果作为认证成绩。

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
- `fdatasync_time_us` / `max_fdatasync_time_us`
- `approx_write_latency_p50_us/p95_us/p99_us`
- `pending_queue_depth` / `max_pending_queue_depth`

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
- `compaction_pause_time_us` / `max_compaction_pause_time_us`
- `total_snapshot_bytes_written`
- `total_wal_bytes_reclaimed_by_compaction`

### Cache 与恢复

- `value_cache_hits` / `value_cache_misses`
- `recovery_time_us`

## Compaction 调优

自动 compaction 默认关闭。启用时建议先只设置字节阈值，再依据真实 overwrite/delete 负载考虑无效比例阈值：

```cpp
options.auto_compact_wal_bytes_threshold = 256ULL * 1024 * 1024;
options.auto_compact_invalid_wal_ratio_percent = 0;
```

无效比例阈值过低可能在小 journal 上频繁触发。观察：

- `auto_compactions_completed` 是否异常快速增长。
- `max_compaction_pause_time_us` 是否影响写 p99。
- checkpoint 写入带宽是否挤压 journal 同步。
- 数据集增长后 RSS 是否主要来自 key/index，而不是 value cache。

手动 `Compact()` 保证返回时 compaction 完成。自动 compaction 在后台运行，但最终 inode 切换仍需要短暂停止 commit。

## Sticky fatal 处理

一旦任意操作报告 I/O、checksum、short write、sync 或目录错误：

1. 停止向该实例发送新请求。
2. 记录第一条错误；后续错误应与它相同。
3. 不要尝试在同一实例上“恢复运行”。
4. 关闭进程并保留数据库文件、所在目录和系统日志。
5. 使用只读 `verify-format` 检查。
6. 若只是已识别的不完整最终 frame，可备份后用运行库打开修复，再重新验证。
7. 完整 checksum、边界或中段损坏必须人工调查，不能自动跳过。

磁盘满或配额错误需要先释放其他空间。不要在空间仍不足时反复 compaction，因为临时容器需要接近 live 数据集大小的额外空间。

## 事务冲突

`KVStoreConflictError` 不是存储故障。调用方应丢弃旧事务、退避并从新事务重新读取。冲突长期偏高时：

- 检查热点 key 是否集中在少数 shard。
- 缩短事务生命周期。
- 减少事务访问的 shard 数。
- 不要在捕获冲突后复用已经失效的事务对象。

## Scan 影响

字符串 `Scan` 持有所有 shard 的共享锁直到 k-way merge 完成。若写 p99 突升时伴随长 Scan：

- 缩小范围或减少返回对象。
- 降低 Scan 频率。
- 把大规模导出放到停写维护窗口。

## 测试与认证

```bash
bash scripts/ci-build.sh
bash scripts/ci-sanitizers.sh
bash scripts/tsan.sh
bash scripts/concurrency-stress.sh 10 balanced
bash scripts/concurrency-stress.sh 10 compaction-heavy
bash scripts/qualification-benchmark.sh <output_dir> <baseline_json>
bash scripts/qualification-run.sh <output_dir>
```

WSL TSan 默认使用 `/usr/bin/g++-10`；如需替换，设置 `KVSTORE_TSAN_CXX`。
GCC 10 deadlock detector 受 64 锁上限影响，而 `Scan` 需要同时持有 256 个 shard
锁，所以脚本设置 `detect_deadlocks=0`。这不会关闭数据竞争检测，任何竞态报告仍令
CTest 或并发压力测试失败。TSan 缺失是失败，不会标为 SKIP。

正式 qualification 命令、环境和结果必须一起归档；短时 smoke 不能替代 12 小时认证。

## 常见故障

### 第二实例无法打开

预期的独占锁行为。使用 `lsof /data/store.db` 或 `/proc/<pid>/fd` 找到持有者，确认旧进程退出后再重试。

### 恢复报告 truncated tail

说明最终事务 frame 没有完整落盘。`inspect-format` 会报告，读写打开会截断到最后完整 frame。修复前应先复制现场以便调查。

### 完整 frame checksum mismatch

属于损坏，不是正常崩溃尾部。检查介质、文件被外部修改、错误备份流程和内存/硬件告警。

### Compaction 频繁

提高字节阈值，禁用或提高无效比例阈值，并确认没有把测试级几 KiB 阈值带到大数据集。

### p99 高但 CPU 低

检查 `max_fdatasync_time_us`、group size 和文件系统。`kSync` 的尾延迟经常由同步延迟决定；盲目增加 worker 不能消除磁盘同步瓶颈。

更完整语义见[一致性与持久化语义](semantics.md)和[文件格式](file-format.md)。
