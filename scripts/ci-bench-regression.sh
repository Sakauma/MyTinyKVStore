#!/usr/bin/env bash
set -euo pipefail

baseline_path="benchmarks/reference/ci-floor.json"

if [[ ! -f "${baseline_path}" ]]; then
  echo "Missing CI benchmark baseline: ${baseline_path}" >&2
  exit 1
fi

bash scripts/bench-regression-check.sh "${baseline_path}"
