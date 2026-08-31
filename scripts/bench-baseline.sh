#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/benchmark-env.sh"
kvstore_require_native_ext4

out_dir="${1:-benchmarks/baselines}"
timestamp="$(date +%Y%m%dT%H%M%S)"
mkdir -p "${out_dir}"

cmake -S . -B build-release -DCMAKE_BUILD_TYPE=Release
cmake --build build-release --parallel

out_path="${out_dir}/${timestamp}.json"
./build-release/target/bin/kv_test bench-baseline-json | tee "${out_path}"
echo "baseline_file=${out_path}"
