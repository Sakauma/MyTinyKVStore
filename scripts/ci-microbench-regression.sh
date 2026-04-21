#!/usr/bin/env bash
set -euo pipefail

baseline_path="benchmarks/reference/microbench-floor.json"

if [[ ! -f "${baseline_path}" ]]; then
  echo "Missing CI microbench baseline: ${baseline_path}" >&2
  exit 1
fi

bash scripts/microbench-regression-check.sh "${baseline_path}"
