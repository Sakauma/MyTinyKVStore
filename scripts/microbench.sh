#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$repo_root/scripts/benchmark-env.sh"
kvstore_require_native_ext4

cmake -S "$repo_root" -B "$repo_root/build-release" -DCMAKE_BUILD_TYPE=Release
cmake --build "$repo_root/build-release" --parallel
"$repo_root/build-release/target/bin/kv_test" microbench
