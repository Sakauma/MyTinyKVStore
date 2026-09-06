#!/usr/bin/env bash
set -euo pipefail

bash tests/scripts/bench_regression_check_test.sh

cmake -S . -B build-release -DCMAKE_BUILD_TYPE=Release
cmake --build build-release --parallel
(cd build-release && ctest --output-on-failure)
