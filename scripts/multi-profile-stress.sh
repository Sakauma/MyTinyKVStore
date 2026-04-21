#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
output_dir="${1:-$repo_root/artifacts/stress-profiles}"
duration_seconds="${2:-1}"

mkdir -p "$output_dir"

cmake -S "$repo_root" -B "$repo_root/build"
cmake --build "$repo_root/build"

profiles=(balanced write-heavy compaction-heavy)
for profile in "${profiles[@]}"; do
  "$repo_root/build/target/bin/kv_test" concurrency-stress-json "$duration_seconds" "$profile" \
    > "$output_dir/$profile.json"
done
