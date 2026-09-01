#!/usr/bin/env bash
set -euo pipefail

sanitizer_build_type=RelWithDebInfo
asan_options="${ASAN_OPTIONS:+${ASAN_OPTIONS}:}detect_leaks=1:halt_on_error=1"
lsan_options="${LSAN_OPTIONS:+${LSAN_OPTIONS}:}detect_leaks=1"

cmake -S . -B build-asan -DCMAKE_BUILD_TYPE="$sanitizer_build_type" -DKVSTORE_ENABLE_ASAN=ON
cmake --build build-asan --parallel
(cd build-asan && ASAN_OPTIONS="$asan_options" LSAN_OPTIONS="$lsan_options" ctest --output-on-failure)

cmake -S . -B build-ubsan -DCMAKE_BUILD_TYPE="$sanitizer_build_type" -DKVSTORE_ENABLE_UBSAN=ON
cmake --build build-ubsan --parallel
(cd build-ubsan && ctest --output-on-failure)

bash scripts/tsan.sh
