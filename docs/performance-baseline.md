# 性能基线与认证流程

## 两类基准

开发回归和正式认证使用不同入口：

- `microbench` / `bench-baseline-json`：快速发现明显回归，不代表正式吞吐结论。
- `qualification-bench-json`：冻结工作负载、三轮中位数和正式 2×/p99 门禁。

所有性能结果必须来自 Release，并在 WSL/Linux 原生 ext4 文件系统运行，不能使用 `/mnt/*`、9p、DrvFS 或 fuseblk。执行 workload 的脚本会通过 `findmnt` 检查 `${KVSTORE_BENCHMARK_TMPDIR:-${TMPDIR:-/tmp}}`；不满足时直接失败。

## 标准 qualification benchmark

固定配置：

| 参数 | 值 |
| --- | ---: |
| Durability | `kSync` |
| 预填充 | 1,000,000 个整数键 |
| Writers | 16 |
| 总操作 | 10,000,000 |
| Put/Delete/Get | 80% / 10% / 10% |
| Value | 256 bytes |
| Key 分布 | uniform |
| 自动 compaction | off |
| 轮数 | 3，逐指标取中位数 |

运行：

```bash
qualification_root="$HOME/kvstore-qualification/$(date +%Y%m%d-%H%M%S)"
mkdir -p "$qualification_root"
bash scripts/qualification-benchmark.sh \
  "$qualification_root/canonical" \
  /path/to/frozen-baseline.json
```

开始前确认该 ext4 文件系统至少有 25 GiB 可用空间。Qualification 输出只保存在仓库外目录，不提交或推送。

脚本会：

1. 将当前工作区复制到 `/tmp` 原生 Linux 文件系统。
2. 以 `CMAKE_BUILD_TYPE=Release` 全量构建。
3. 运行标准三轮 workload。
4. 保存 candidate JSON、命令和环境。
5. 通过 `compare-qualification` 执行门禁。

门禁：

- `median_write_ops_per_s >= baseline * 2.0`
- `median_write_p99_us <= baseline * 1.2`

没有冻结 baseline 时，脚本保存 candidate 但把 gate 标记为 `not_evaluated`，不能宣称通过。

## 结构化结果

`qualification-bench-json` 顶层包含完整 workload 配置。每轮包含：

- duration
- total/write throughput
- Put/Delete/Get 精确计数
- 端到端写 p50/p95/p99
- 完整 `KVStoreMetrics`

顶层额外输出：

- `median_operations_per_s`
- `median_write_ops_per_s`
- `median_write_p50_us`
- `median_write_p95_us`
- `median_write_p99_us`

端到端写延迟从调用 `Put/Delete` 前开始，到 API 成功返回为止，包含排队、worker 准备、group wait、写入和 `fdatasync`。

## 补充矩阵

```bash
bash scripts/qualification-matrix.sh "$qualification_root/matrix"
```

默认矩阵覆盖：

- 8 / 32 writers
- 64B / 1KiB value
- 90% 请求命中 1% key 的 hotspot
- 自动 compaction 开启

矩阵结果用于解释结构性变化，不替代标准 16-writer gate。

## Qualification artifact 环境字段

正式 artifact 至少记录：

- 基线 commit、候选 commit，以及各自的 dirty 状态。
- Dirty 工作树采样时的 source digest、`git diff --stat` 和完整补丁保存路径；最终提交后必须用 digest 证明提交内容与被测源码一致。
- Harness/脚本路径与 digest、完整命令行、环境变量、开始/结束时间、时区、退出状态和重复轮数。
- CPU 型号、socket/core/thread 拓扑、频率 governor；内存总量与关键限制。
- 块设备、旋转属性、挂载点、文件系统类型、挂载选项与可用空间。
- Linux 内核、WSL 版本（如适用）及虚拟化边界。
- 编译器、CMake、生成器、`Release` 构建类型和实际编译/链接选项。
- 完整 workload/config、随机种子、auto-compaction 配置和 reference baseline 文件校验和。
- 每轮原始结果、逐指标中位数、门禁结果以及 stdout/stderr 日志路径。

同一 baseline/candidate 比较必须使用同一机器、磁盘、文件系统、内核策略和编译器配置。跨机器的比值没有认证意义。

本机一次或少量短时运行必须标记为 `development-sample`，只能用于发现明显回归；只有上述环境可比、固定工作负载完整运行且原始 artifact 齐全时，结果才可标记为 `qualification-candidate`。更新冻结门槛必须引用至少三轮可比测量，按逐指标中位数给出依据并保留保守裕量，不能为了让当前候选通过而降低门槛。

候选实现的 worker 负责 payload、payload/value CRC 和物理 WAL charge 准备；coordinator 负责 LSN、header/footer、ordered `pwritev` 与同步。热点读使用分段 CLOCK，compaction 使用文件代际并在提交暂停外迁移 entry。性能结果应同时保留 group size、worker utilization、cache hit rate 和 compaction pause 指标，以便判断吞吐变化来自哪条路径。

## 长时正确性认证

```bash
bash scripts/qualification-run.sh "$qualification_root/soak"
```

脚本默认拒绝低于 43200 秒或 1000 万唯一整数键的参数。工作负载在写完目标唯一键后继续对这些键更新，直到持续时间满足；compaction 开启。两项条件同时满足后，harness 再执行最多 300 秒的纯覆盖写稳定窗口；短时 smoke 的稳定窗口按持续时间同比缩短。结束时：

1. 执行同步 `Compact()` 和 `Flush()`。
2. 关闭并重新打开数据库。
3. 逐项校验全部目标唯一键和值。
4. 再运行共享 `verify-format`。
5. 记录初始、峰值、最终以及稳定窗口起止 RSS/FD 和恢复校验耗时。
6. 要求稳定窗口末端 RSS 不高于起点加 `max(64 MiB, 10%)`，FD 不增加超过 2；否则结果为 fail。

`KVSTORE_ALLOW_SHORT_QUALIFICATION=1` 允许开发 smoke，但 artifact 明确标记 `smoke-only`。

## 快速回归入口

```bash
bash scripts/microbench.sh
bash scripts/bench-baseline.sh
bash scripts/microbench-regression-check.sh benchmarks/reference/microbench-floor.json
bash scripts/bench-regression-check.sh benchmarks/reference/ci-floor.json
```

快速 gate 的阈值较宽，只用于阻止明显倒退。它们不能代替标准 2×/p99 gate。

仓库参考文件使用相对链接：

- [CI stressbench floor](../benchmarks/reference/ci-floor.json)
- [CI microbench floor](../benchmarks/reference/microbench-floor.json)
- [2026-09-06 开发验证样本](../benchmarks/reference/development-validation-2026-09-06.json)：保留历史 floor 的哈希、单次 ext4 Release 结果和来源信息；历史 floor 原始来源未知，正式 qualification 仍为 `not_evaluated`。

## 基线冻结规则

- 基线必须来自改进前冻结 commit，并使用同一个 qualification harness 或等价外部 load generator。
- 不允许拿 `/mnt/d` 的旧数字与 ext4 candidate 直接比较。
- 三轮原始结果全部保留，不能只保存挑选出的最好一轮。
- 机器发生内核、文件系统、编译器或硬件变化后，旧 baseline 只能作为历史记录，必须重新冻结可比基线。
