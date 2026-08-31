#!/usr/bin/env bash
set -euo pipefail

cmake -S . -B build-asan -DCMAKE_BUILD_TYPE=RelWithDebInfo -DKVSTORE_ENABLE_ASAN=ON
cmake --build build-asan --parallel
(cd build-asan && ASAN_OPTIONS=detect_leaks=0 LSAN_OPTIONS=detect_leaks=0 ctest --output-on-failure)

cmake -S . -B build-ubsan -DCMAKE_BUILD_TYPE=RelWithDebInfo -DKVSTORE_ENABLE_UBSAN=ON
cmake --build build-ubsan --parallel
(cd build-ubsan && ctest --output-on-failure)

bash scripts/tsan.sh
