#!/usr/bin/env bash
set -euo pipefail

cmake -S . -B build
cmake --build build
./build/target/bin/kv_test bench
