#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: bash scripts/inspect-format.sh <db_path>" >&2
  exit 1
fi

cmake -S . -B build
cmake --build build
./build/target/bin/kv_test inspect-format "$1"
