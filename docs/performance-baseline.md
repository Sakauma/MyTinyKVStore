# Performance Baseline Workflow

## Goal

把 benchmark 结果沉淀成可留档、可比较的 JSON 基线，而不是只看一次终端输出。

## Commands

- `./build/target/bin/kv_test bench`：打印面向人工阅读的单行 benchmark 摘要。
- `./build/target/bin/kv_test bench-baseline-json`：打印结构化 baseline JSON。
- `bash scripts/bench-baseline.sh`：运行 baseline benchmark，并把 JSON 落到 `benchmarks/baselines/<timestamp>.json`。
- `./build/target/bin/kv_test compare-baseline <baseline_json> <candidate_json> [min_write_ratio_pct min_read_ratio_pct max_latency_ratio_pct]`：比较两份 baseline。
- `./build/target/bin/kv_test trend-baselines <baseline_dir>`：汇总一组 baseline 的长期趋势。
- `bash scripts/bench-regression-check.sh <baseline_json>`：生成 candidate baseline，并按默认阈值执行回归检查。
- `bash scripts/ci-bench-regression.sh`：使用仓库提交的 `benchmarks/reference/ci-floor.json` 作为 CI floor。
- `bash scripts/bench-trend.sh [baseline_dir]`：对一组历史 baseline 输出趋势摘要。

## JSON Shape

baseline JSON 包含四部分：

- `label`：本次 benchmark 的标签。
- `workload`：writer 数、reader 数、时长和 key space。
- `summary`：吞吐、总写入/读取次数、平均写延迟。
- `options` / `metrics`：当前 benchmark 使用的选项和 KVStore 运行指标。

## Recommended Use

- 每次调整 writer 策略、compaction 策略或格式恢复逻辑后，都运行一次新的 baseline。
- 基线文件应与改动一起审阅，而不是只看口头描述。
- 如需对比历史结果，优先比较 `summary` 中的吞吐和延迟，再回看 `metrics` 中的 `fsync`、batch 和队列指标。

## Default Regression Thresholds

默认回归门槛如下：

- `write_ops_per_s` 不得低于参考基线的 `85%`
- `read_ops_per_s` 不得低于参考基线的 `85%`
- `avg_write_latency_us` 不得高于参考基线的 `125%`
- `approx_write_latency_p95_us` 不得高于参考基线的 `150%`
- `approx_write_latency_p99_us` 不得高于参考基线的 `175%`
- `observed_fsync_pressure_per_1000_writes` 不得高于参考基线的 `150%`
- `recent_batch_fill_per_1000` 不得低于参考基线的 `75%`

这些阈值目前是保守门槛，用来挡住明显退化，而不是替代长期性能分析。

## CI Floor

- 仓库当前提交了 [benchmarks/reference/ci-floor.json](/home/sakauma/code/lpue/benchmarks/reference/ci-floor.json) 作为 CI floor。
- 它不是“最佳成绩”，而是“明显退化不应低于的下限”。
- GitHub Actions 会运行 `scripts/ci-bench-regression.sh`，并把本次 candidate baseline 作为 artifact 上传。

## Trend Summary

`trend-baselines` / `bench-trend.sh` 当前会输出：

- `count`：纳入统计的 baseline 文件数
- `oldest_file` / `latest_file`：最早与最新的 baseline 文件
- `avg_*` / `min_*` / `max_*`：该组 baseline 的均值、最小值和最大值
- `latest_vs_oldest_*_ratio_pct`：最新结果相对最早结果的比例

这个趋势摘要适合观察一段时间内的大方向变化，但它还不是完整的时序分析或可视化系统。
