#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/benchmark-env.sh"
kvstore_require_native_ext4

if [[ $# -lt 1 || $# -gt 6 ]]; then
  echo "Usage: bash scripts/bench-regression-check.sh <baseline_json> [output_dir] [min_write_ratio_pct] [min_read_ratio_pct] [max_latency_ratio_pct] [sample_count]" >&2
  exit 1
fi

baseline_path="$1"
out_dir="${2:-benchmarks/baselines}"
min_write_ratio_pct="${3:-85}"
min_read_ratio_pct="${4:-85}"
max_latency_ratio_pct="${5:-125}"
sample_count="${6:-1}"
if [[ ! "${sample_count}" =~ ^[1-9][0-9]*$ ]]; then
  echo "sample_count must be a positive integer: ${sample_count}" >&2
  exit 1
fi

run_id="$(date +%Y%m%dT%H%M%S)-$$"
mkdir -p "${out_dir}"

cmake -S . -B build-release -DCMAKE_BUILD_TYPE=Release
cmake --build build-release --parallel

pass_count=0
gate_fail_count=0
failed_candidate_paths=()
for ((sample_index = 1; sample_index <= sample_count; ++sample_index)); do
  candidate_path="${out_dir}/${run_id}-sample-${sample_index}.json"
  set +e
  ./build-release/target/bin/kv_test bench-baseline-json > "${candidate_path}"
  benchmark_status=$?
  set -e
  if ((benchmark_status != 0)); then
    rm -f -- "${candidate_path}"
    echo "benchmark sample ${sample_index} generation failed with status ${benchmark_status}" >&2
    exit "${benchmark_status}"
  fi

  set +e
  ./build-release/target/bin/kv_test compare-baseline "${baseline_path}" "${candidate_path}" \
    "${min_write_ratio_pct}" "${min_read_ratio_pct}" "${max_latency_ratio_pct}"
  compare_status=$?
  set -e

  case "${compare_status}" in
    0)
      pass_count=$((pass_count + 1))
      sample_gate_status="pass"
      ;;
    2)
      gate_fail_count=$((gate_fail_count + 1))
      failed_candidate_paths+=("${candidate_path}")
      sample_gate_status="fail"
      ;;
    *)
      echo "benchmark sample ${sample_index} comparison failed with status ${compare_status}" >&2
      exit "${compare_status}"
      ;;
  esac
  echo "candidate_file=${candidate_path} sample_index=${sample_index} gate_status=${sample_gate_status}"
done

required_pass_count=$((sample_count / 2 + 1))
if ((pass_count < required_pass_count)); then
  echo "aggregate_samples=${sample_count} pass_count=${pass_count} gate_fail_count=${gate_fail_count} required_pass_count=${required_pass_count} status=fail" >&2
  exit 2
fi

if ((gate_fail_count != 0)); then
  echo "warning: aggregate benchmark gate passed with ${gate_fail_count} retained failing sample(s): ${failed_candidate_paths[*]}" >&2
fi
echo "aggregate_samples=${sample_count} pass_count=${pass_count} gate_fail_count=${gate_fail_count} required_pass_count=${required_pass_count} status=pass"
