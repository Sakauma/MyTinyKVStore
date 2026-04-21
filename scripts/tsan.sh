#!/usr/bin/env bash
set -euo pipefail

if ! cmake -S . -B build-tsan -DKVSTORE_ENABLE_TSAN=ON; then
    echo "[SKIP] ThreadSanitizer runtime is not available in the current toolchain."
    exit 0
fi

cmake --build build-tsan
./build-tsan/target/bin/kv_test
./build-tsan/target/bin/kv_test concurrency-stress 1 balanced
