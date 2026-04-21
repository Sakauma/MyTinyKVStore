#!/usr/bin/env bash
set -euo pipefail

baseline_dir="${1:-benchmarks/baselines}"

cmake -S . -B build
cmake --build build
./build/target/bin/kv_test trend-baselines "${baseline_dir}"
