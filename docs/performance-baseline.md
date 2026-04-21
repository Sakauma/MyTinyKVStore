# Performance Baseline Workflow

## Goal

把 benchmark 结果沉淀成可留档、可比较的 JSON 基线，而不是只看一次终端输出。

## Commands

- `./build/target/bin/kv_test bench`：打印面向人工阅读的单行 benchmark 摘要。
- `./build/target/bin/kv_test bench-baseline-json`：打印结构化 baseline JSON。
- `bash scripts/bench-baseline.sh`：运行 baseline benchmark，并把 JSON 落到 `benchmarks/baselines/<timestamp>.json`。

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
