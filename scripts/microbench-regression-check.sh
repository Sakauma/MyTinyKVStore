#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 || $# -gt 6 ]]; then
  echo "Usage: bash scripts/microbench-regression-check.sh <baseline_json> [output_dir] [min_ops_ratio_pct] [min_compaction_ratio_pct] [min_rewrite_ratio_pct] [min_recovery_ratio_pct]" >&2
  exit 1
fi

baseline_path="$1"
out_dir="${2:-benchmarks/baselines}"
min_ops_ratio_pct="${3:-80}"
min_compaction_ratio_pct="${4:-75}"
min_rewrite_ratio_pct="${5:-75}"
min_recovery_ratio_pct="${6:-80}"
timestamp="$(date +%Y%m%dT%H%M%S)"
mkdir -p "${out_dir}"

cmake -S . -B build
cmake --build build

candidate_path="${out_dir}/microbench-${timestamp}.json"
./build/target/bin/kv_test microbench-json > "${candidate_path}"
./build/target/bin/kv_test compare-microbench "${baseline_path}" "${candidate_path}" \
  "${min_ops_ratio_pct}" "${min_compaction_ratio_pct}" "${min_rewrite_ratio_pct}" "${min_recovery_ratio_pct}"
echo "candidate_file=${candidate_path}"
