#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$repo_root/scripts/benchmark-env.sh"
kvstore_require_native_ext4
output_dir="${1:-$repo_root/artifacts/latest}"
baseline_dir="${2:-$repo_root/benchmarks/baselines}"
recent_window="${3:-5}"

mkdir -p "$output_dir"

cmake -S "$repo_root" -B "$repo_root/build-release" -DCMAKE_BUILD_TYPE=Release
cmake --build "$repo_root/build-release" --parallel

"$repo_root/build-release/target/bin/kv_test" microbench-json > "$output_dir/microbench.json"
"$repo_root/build-release/target/bin/kv_test" trend-microbench-json "$baseline_dir" "$recent_window" > "$output_dir/microbench-trend-summary.json"
"$repo_root/build-release/target/bin/kv_test" bench-baseline-json > "$output_dir/stressbench-baseline.json"
"$repo_root/build-release/target/bin/kv_test" trend-baselines-json "$baseline_dir" "$recent_window" > "$output_dir/trend-summary.json"
mkdir -p "$output_dir/stress-profiles"
bash "$repo_root/scripts/multi-profile-stress.sh" "$output_dir/stress-profiles" 1
