#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 || $# -gt 5 ]]; then
  echo "Usage: bash scripts/bench-regression-check.sh <baseline_json> [output_dir] [min_write_ratio_pct] [min_read_ratio_pct] [max_latency_ratio_pct]" >&2
  exit 1
fi

baseline_path="$1"
out_dir="${2:-benchmarks/baselines}"
min_write_ratio_pct="${3:-85}"
min_read_ratio_pct="${4:-85}"
max_latency_ratio_pct="${5:-125}"
timestamp="$(date +%Y%m%dT%H%M%S)"
mkdir -p "${out_dir}"

cmake -S . -B build
cmake --build build

candidate_path="${out_dir}/${timestamp}.json"
./build/target/bin/kv_test bench-baseline-json > "${candidate_path}"
./build/target/bin/kv_test compare-baseline "${baseline_path}" "${candidate_path}" \
  "${min_write_ratio_pct}" "${min_read_ratio_pct}" "${max_latency_ratio_pct}"
echo "candidate_file=${candidate_path}"
