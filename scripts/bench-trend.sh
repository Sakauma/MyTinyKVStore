#!/usr/bin/env bash
set -euo pipefail

baseline_dir="${1:-benchmarks/baselines}"
recent_window="${2:-5}"

cmake -S . -B build
cmake --build build
./build/target/bin/kv_test trend-baselines "${baseline_dir}" "${recent_window}"
