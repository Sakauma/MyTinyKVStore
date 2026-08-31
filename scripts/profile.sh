#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/benchmark-env.sh"
kvstore_require_native_ext4

if [[ $# -ne 1 ]]; then
  echo "Usage: bash scripts/profile.sh <balanced|write-heavy|read-heavy|low-latency>" >&2
  exit 1
fi

cmake -S . -B build-release -DCMAKE_BUILD_TYPE=Release
cmake --build build-release --parallel
./build-release/target/bin/kv_test profile-json "$1"
