#!/usr/bin/env bash

kvstore_format_tool_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"

kvstore_build_format_tool() {
  local build_dir="${KVSTORE_BUILD_DIR:-${kvstore_format_tool_root}/build-release}"
  cmake -S "${kvstore_format_tool_root}" -B "${build_dir}" -DCMAKE_BUILD_TYPE=Release
  cmake --build "${build_dir}" --target kv_test --parallel
  KVSTORE_FORMAT_TOOL="${build_dir}/target/bin/kv_test"
}
