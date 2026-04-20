#!/usr/bin/env bash
set -euo pipefail

cmake -S . -B build-asan -DKVSTORE_ENABLE_ASAN=ON
cmake --build build-asan
(cd build-asan && ASAN_OPTIONS=detect_leaks=0 LSAN_OPTIONS=detect_leaks=0 ctest --output-on-failure)

cmake -S . -B build-ubsan -DKVSTORE_ENABLE_UBSAN=ON
cmake --build build-ubsan
(cd build-ubsan && ctest --output-on-failure)
