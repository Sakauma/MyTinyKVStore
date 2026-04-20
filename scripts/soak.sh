#!/usr/bin/env bash
set -euo pipefail

duration_seconds="${1:-10}"

cmake -S . -B build
cmake --build build
./build/target/bin/kv_test soak "${duration_seconds}"
