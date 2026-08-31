#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "Usage: bash scripts/migrate-format.sh <legacy_db> <v3_db>" >&2
  exit 1
fi

cmake -S . -B build-release -DCMAKE_BUILD_TYPE=Release
cmake --build build-release --parallel --target kv_migrate
./build-release/target/bin/kv_migrate --input "$1" --output "$2"
