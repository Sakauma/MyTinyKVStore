#!/usr/bin/env bash
set -euo pipefail

tsan_cxx="${KVSTORE_TSAN_CXX:-}"
if [[ -z "$tsan_cxx" ]]; then
  if command -v g++-10 >/dev/null 2>&1; then
    tsan_cxx="$(command -v g++-10)"
  else
    echo "TSan requires GCC 10 in this WSL environment; set KVSTORE_TSAN_CXX to another supported compiler" >&2
    exit 2
  fi
fi

tsan_options="${TSAN_OPTIONS:+${TSAN_OPTIONS}:}detect_deadlocks=0:halt_on_error=1"
tsan_stress_seconds="${KVSTORE_TSAN_STRESS_SECONDS:-10}"
if ! [[ "$tsan_stress_seconds" =~ ^[1-9][0-9]*$ ]]; then
  echo "KVSTORE_TSAN_STRESS_SECONDS must be a positive integer" >&2
  exit 2
fi

echo "TSan uses RelWithDebInfo with race detection and halt_on_error enabled."
echo "GCC 10 deadlock detection is disabled because Scan can hold 256 shard locks; its detector tracks at most 64."

cmake -S . -B build-tsan \
  -DCMAKE_BUILD_TYPE=RelWithDebInfo \
  -DKVSTORE_ENABLE_TSAN=ON \
  -DCMAKE_CXX_COMPILER="$tsan_cxx"
cmake --build build-tsan --parallel
(cd build-tsan && TSAN_OPTIONS="$tsan_options" ctest --output-on-failure)
TSAN_OPTIONS="$tsan_options" ./build-tsan/target/bin/kv_test concurrency-stress "$tsan_stress_seconds" balanced
