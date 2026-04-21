#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: bash scripts/profile.sh <balanced|write-heavy|read-heavy|low-latency>" >&2
  exit 1
fi

cmake -S . -B build
cmake --build build
./build/target/bin/kv_test profile-json "$1"
