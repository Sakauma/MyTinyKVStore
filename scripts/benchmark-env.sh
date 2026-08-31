#!/usr/bin/env bash

kvstore_require_native_ext4() {
  local probe_root="${KVSTORE_BENCHMARK_TMPDIR:-${TMPDIR:-/tmp}}"
  if [[ ! -d "$probe_root" ]]; then
    echo "benchmark temporary directory does not exist: $probe_root" >&2
    return 2
  fi
  probe_root="$(cd "$probe_root" && pwd -P)"
  case "$probe_root" in
    /mnt/*)
      echo "benchmarks must not use a /mnt/* filesystem: $probe_root" >&2
      return 2
      ;;
  esac
  if ! command -v findmnt >/dev/null 2>&1; then
    echo "findmnt is required to verify the benchmark filesystem" >&2
    return 2
  fi
  local filesystem_type
  filesystem_type="$(findmnt -T "$probe_root" -n -o FSTYPE)"
  if [[ "$filesystem_type" != "ext4" ]]; then
    echo "benchmarks require a native ext4 filesystem, got: $filesystem_type" >&2
    return 2
  fi
  export TMPDIR="$probe_root"
}
