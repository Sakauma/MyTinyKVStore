#!/usr/bin/env bash
set -euo pipefail

cmake -S . -B build-coverage -DKVSTORE_ENABLE_COVERAGE=ON
cmake --build build-coverage
(cd build-coverage && ctest --output-on-failure)

if command -v gcovr >/dev/null 2>&1; then
  gcovr --root . --txt
else
  echo "gcovr not found; coverage artifacts were generated under build-coverage/"
fi
