#!/usr/bin/env bash
set -euo pipefail

cmake -S . -B build-release -DCMAKE_BUILD_TYPE=Release
cmake --build build-release --parallel
(cd build-release && ctest --output-on-failure)
