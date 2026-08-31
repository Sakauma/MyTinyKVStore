#!/usr/bin/env bash
set -euo pipefail

baseline_dir="${1:-benchmarks/baselines}"
recent_window="${2:-5}"

cmake -S . -B build-release -DCMAKE_BUILD_TYPE=Release
cmake --build build-release --parallel
./build-release/target/bin/kv_test trend-microbench "${baseline_dir}" "${recent_window}"
