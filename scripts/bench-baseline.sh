#!/usr/bin/env bash
set -euo pipefail

out_dir="${1:-benchmarks/baselines}"
timestamp="$(date +%Y%m%dT%H%M%S)"
mkdir -p "${out_dir}"

cmake -S . -B build
cmake --build build

out_path="${out_dir}/${timestamp}.json"
./build/target/bin/kv_test bench-baseline-json | tee "${out_path}"
echo "baseline_file=${out_path}"
