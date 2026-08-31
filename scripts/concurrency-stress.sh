#!/usr/bin/env bash
set -euo pipefail

duration_seconds="${1:-10}"
profile="${2:-balanced}"

cmake -S . -B build-release -DCMAKE_BUILD_TYPE=Release
cmake --build build-release --parallel
./build-release/target/bin/kv_test concurrency-stress "${duration_seconds}" "${profile}"
