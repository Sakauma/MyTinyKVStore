#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
output_dir="${1:-$repo_root/artifacts/latest}"
baseline_dir="${2:-$repo_root/benchmarks/baselines}"
recent_window="${3:-5}"

mkdir -p "$output_dir"

cmake -S "$repo_root" -B "$repo_root/build"
cmake --build "$repo_root/build"

"$repo_root/build/target/bin/kv_test" microbench-json > "$output_dir/microbench.json"
"$repo_root/build/target/bin/kv_test" bench-baseline-json > "$output_dir/stressbench-baseline.json"
"$repo_root/build/target/bin/kv_test" trend-baselines-json "$baseline_dir" "$recent_window" > "$output_dir/trend-summary.json"
"$repo_root/build/target/bin/kv_test" compat-matrix > "$output_dir/compatibility-matrix.txt"
mkdir -p "$output_dir/stress-profiles"
bash "$repo_root/scripts/multi-profile-stress.sh" "$output_dir/stress-profiles" 1
