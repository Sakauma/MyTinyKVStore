# MyTinyKVStore

MyTinyKVStore 是一个面向 Linux/WSL POSIX 文件系统的 C++17 小型持久化键值存储引擎。当前版本使用单文件容器、分片内存索引、有界 worker pool、有序 group commit 和可串行化 OCC 事务。

当前代码已经具备高并发引擎的核心实现，但“高并发持久化 KV 引擎”的正式标签仍受 qualification gate 约束：必须在同一机器的 Release/ext4 环境中达到规定性能门槛，并完成 12 小时、1000 万唯一键的归档验证。没有 qualification artifact 时，不应把短时测试结果解释为生产认证。

## 核心能力

- 单一主数据库文件：双 CRC32C superblock、checkpoint index/object 区和内嵌 journal。
- 原子事务帧：`Put`、`Delete`、`WriteBatch` 和显式事务都进入同一提交路径；崩溃恢复不会公开半个批次。
- 256 个默认 shard：每个 shard 独立读写锁、点查哈希索引和字符串有序索引。
- 有界并发执行：默认 worker 数为 `min(32, hardware_concurrency())`，请求队列容量为 4096；队列满时调用方阻塞，不丢请求。
- 有序 group commit：调用线程完成轻量键编码，worker 并行校验并序列化，请求按提交序号由单 coordinator 聚合，通过 `pwritev` 和一次 `fdatasync` 提交。
- 显式事务：move-only `KVTransaction`，支持 read-your-writes、只读验证、rollback 和 shard-version OCC 冲突检测。
- 三种持久化模式：`kSync`、`kPeriodic` 和 `kNoSync`，并提供有序 `Flush()` 屏障。
- 低停顿 compaction：后台生成新容器，最终只在复制 journal delta、切换 inode 和重定位 offset 时短暂停止提交。
- offset 索引与有界缓存：内存条目保存 key、LSN、文件 offset、长度和 checksum；value 通过 `pread` 按需读取，默认 LRU 预算 256 MiB，并把每个缓存项的估算元数据开销计入预算。
- 一致字符串 `Scan`：按固定顺序持有全部 shard 的共享锁并进行 k-way merge。它是低频管理路径，长扫描会暂时阻塞写发布。
- 进程级独占锁：同一数据库文件不能被第二个实例同时打开。
- 共享格式验证器：运行时恢复、`inspect-format` 和 `verify-format` 使用同一个只读解析器。
- 单一活动格式：历史格式实现只作归档参考，不参与构建、安装、测试或运行时兼容。

运行库不依赖 Python 或 CUDA。以后若增加相关工具，仓库约定在 WSL Miniconda 虚拟环境中运行。

## 提交与恢复路径

1. 调用线程获得单调请求序号并进入有界队列。
2. worker pool 并行完成编码键校验和事务 payload 序列化。
3. ordered coordinator 按序验证事务 shard version，并为接受的请求分配单调 LSN。
4. coordinator 用 `pwritev` 写入一组完整事务帧；`kSync` 下执行一次 `fdatasync`。
5. 同步成功后，按 shard ID 排序加锁，原子发布每个请求的内存状态，再唤醒调用方。
6. 恢复时先验证 superblock 和 checkpoint，再按严格递增 LSN 重放完整 journal frame；不完整最终帧可截断，完整损坏或中段损坏拒绝打开。

单 coordinator 仍然定义全局持久化顺序，但 CPU 侧准备、不同 shard 的读取和请求生产是并行的。该结构的目标是用并行准备与 group commit 摊薄同步成本，而不是宣称磁盘提交本身可以无序执行。

## API 示例

```cpp
#include "kvstore.h"

KVStoreOptions options;
options.durability = DurabilityMode::kSync;
options.max_batch_size = 64;

KVStore store("data.db", options);
store.Put(42, Value(std::vector<uint8_t>{'o', 'k'}));
store.Put(std::string("user:42"), Value(std::vector<uint8_t>{1, 2, 3}));
store.Put(std::vector<uint8_t>{0x00, 0xFF}, Value(std::vector<uint8_t>{4, 5}));

store.WriteBatch({
    BatchWriteOperation::PutInt(7, Value(std::vector<uint8_t>{'v'})),
    BatchWriteOperation::Delete("obsolete"),
});

auto tx = store.BeginTransaction();
auto old_value = tx.Get(42);
tx.Put(42, Value(std::vector<uint8_t>{'n', 'e', 'w'}));
tx.Put(std::string("audit:42"), Value(std::vector<uint8_t>{'1'}));
tx.Commit();

store.Flush();
KVStoreMetrics metrics = store.GetMetrics();
```

事务对象不可复制、可以移动；未提交事务析构时自动 rollback。读事务也必须调用 `Commit()` 才能完成 OCC 验证。事务内暂不支持 `Scan`。

## 持久化模式

| 模式 | API 返回条件 | 崩溃窗口 |
| --- | --- | --- |
| `DurabilityMode::kSync` | journal 已 `fdatasync`，随后发布内存状态 | 成功返回的事务必须恢复 |
| `DurabilityMode::kPeriodic` | frame 已写入并发布 | 默认最多可能丢失最近 10 ms 周期内的提交 |
| `DurabilityMode::kNoSync` | frame 已写入并发布 | 在显式 `Flush()` 前不建立持久化承诺 |

`Flush()` 等待并同步它之前的全部提交。析构只做 best-effort 清理；需要可报告同步错误的调用方必须显式调用 `Flush()`。

## 关键默认配置

| 选项 | 默认值 | 说明 |
| --- | ---: | --- |
| `durability` | `kSync` | 强持久化默认值 |
| `periodic_sync_interval_ms` | 10 | periodic 同步周期 |
| `worker_threads` | 自动，最多 32 | payload 准备 worker 数 |
| `shard_count` | 256 | 必须为非零 2 的幂 |
| `request_queue_capacity` | 4096 | 有界生产者队列 |
| `value_cache_bytes` | 256 MiB | 按 shard 分段的 LRU 总预算 |
| `max_batch_size` | 64 | 一次 group commit 的最大请求数 |
| `max_batch_wal_bytes` | 0 | `0` 表示不额外限制字节数 |
| `max_batch_delay_us` | 1000 | 收集 group 的最长等待 |
| `auto_compact_*` | 0 | 自动 compaction 默认关闭 |
| `adaptive_*` | 关闭 | 保留源码兼容，默认不影响固定策略 |

## 构建与测试

建议在 WSL/Linux 中构建：

```bash
cmake -S . -B build-release -DCMAKE_BUILD_TYPE=Release
cmake --build build-release --parallel
cd build-release
ctest --output-on-failure
```

主要产物：

- `build-release/target/lib/libkvstore.so`
- `build-release/target/bin/kv_test`
- `build-release/target/bin/kv_unit_test`

常用入口：

```bash
./build-release/target/bin/kv_unit_test
./build-release/target/bin/kv_test
bash scripts/ci-sanitizers.sh
```

`ci-sanitizers.sh` 会运行 ASan、UBSan 和 TSan。WSL 中的 TSan 脚本默认使用
`g++-10`，也可通过 `KVSTORE_TSAN_CXX` 指定其他受支持的编译器。TSan 被要求时，
配置、链接或运行时缺失都属于失败，不会静默跳过。

当前 GCC 10 deadlock detector 最多跟踪 64 把同时持有的锁，而一致性 `Scan`
会按设计持有 256 个 shard 锁。因此 TSan 流程只关闭该 detector；数据竞争检测、
告警失败和 1 秒并发压力测试仍保持启用。

## 格式检查与重写

```bash
bash scripts/inspect-format.sh data.db
bash scripts/verify-format.sh data.db
bash scripts/rewrite-format.sh data.db
```

`inspect-format` 和 `verify-format` 只识别当前单文件格式，并复用运行时解析器。`rewrite-format` 对已经能够正常打开的当前数据库执行同步 compaction；它不是格式转换工具。历史格式与迁移实验已归档，不属于受支持的运行路径。

## 性能与正式认证

快速 benchmark 只能用于开发回归。正式门槛使用：

```bash
bash scripts/qualification-benchmark.sh <output_dir> <baseline_json>
bash scripts/qualification-matrix.sh <output_dir>
bash scripts/qualification-run.sh <output_dir>
```

标准 benchmark 固定为 Release、原生 Linux 文件系统、`kSync`、预填充 100 万整数键、16 writers、1000 万操作、80% Put / 10% Delete / 10% Get、256B value、均匀分布、自动 compaction 关闭，三轮取中位数。门禁要求：

- 中位写吞吐至少为冻结基线的 2 倍。
- 中位端到端写 p99 不得高于基线的 120%。

`qualification-benchmark.sh` 会把当前工作区复制到 `/tmp` 的原生 Linux 文件系统后用 Release 重新构建，并记录提交号、dirty 状态、CPU、内存、磁盘、文件系统、内核、编译器、配置和结构化结果。

长时认证脚本默认强制至少 43200 秒和 1000 万唯一整数键；两项条件满足后追加最多 5 分钟的覆盖写稳定窗口，将 RSS/FD 增长纳入 gate。随后同步 compact、重启并逐键校验。设置 `KVSTORE_ALLOW_SHORT_QUALIFICATION=1` 只能运行 smoke，结果会标记为 `smoke-only`，不能作为正式认证。

## 当前边界

当前不包含多进程共享写、网络协议、复制、TTL、加密、事务内范围扫描、MVCC 历史读和分布式事务。`Scan` 是全 shard 一致管理路径，不适合作为高频 OLTP 范围查询。

## 文档

- [文件格式](docs/file-format.md)
- [一致性与持久化语义](docs/semantics.md)
- [事务边界](docs/transaction-boundary.md)
- [运维手册](docs/runbook.md)
- [性能基线流程](docs/performance-baseline.md)
- [正式验收口径](docs/qualification-contract.md)
