#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
qualification_root="${KVSTORE_QUALIFICATION_ROOT:-${XDG_STATE_HOME:-$HOME/.local/state}/mytinykvstore/qualification}"
output_dir="${1:-$qualification_root/matrix-$(date +%Y%m%dT%H%M%S)}"
prefill_keys="${2:-1000000}"
operations="${3:-10000000}"
rounds="${4:-3}"
output_dir="$(realpath -m "$output_dir")"
case "$output_dir/" in
  "$repo_root/"*) echo "qualification output must be outside the repository: $output_dir" >&2; exit 2 ;;
esac
mkdir -p "$output_dir"
work_root="$(mktemp -d /tmp/mytinykv-matrix.XXXXXX)"
case "$work_root" in
  /tmp/mytinykv-matrix.*) ;;
  *) echo "unsafe temporary work path: $work_root" >&2; exit 1 ;;
esac
trap 'rm -rf -- "$work_root"' EXIT
filesystem_type="$(findmnt -T "$work_root" -n -o FSTYPE)"
if [[ "$filesystem_type" != "ext4" ]]; then
  echo "qualification matrix requires a native ext4 filesystem, got: $filesystem_type" >&2
  exit 2
fi
output_filesystem_type="$(findmnt -T "$output_dir" -n -o FSTYPE)"
if [[ "$output_filesystem_type" != "ext4" ]]; then
  echo "qualification matrix output requires a native ext4 filesystem, got: $output_filesystem_type" >&2
  exit 2
fi
minimum_free_bytes=$((25 * 1024 * 1024 * 1024))
available_bytes="$(df --output=avail -B1 "$work_root" | tail -n1 | tr -d ' ')"
if (( available_bytes < minimum_free_bytes )); then
  echo "qualification matrix requires at least 25 GiB free on the workload filesystem" >&2
  exit 2
fi
tar -C "$repo_root" --exclude=.git --exclude='build*' --exclude=target --exclude=artifacts -cf - . \
  | tar -C "$work_root" -xf -

cases=(
  "writers8-value256-uniform-off 8 256 uniform off"
  "writers32-value256-uniform-off 32 256 uniform off"
  "writers16-value64-uniform-off 16 64 uniform off"
  "writers16-value1024-uniform-off 16 1024 uniform off"
  "writers16-value256-hotspot-off 16 256 hotspot off"
  "writers16-value256-uniform-on 16 256 uniform on"
)

cmake -S "$work_root" -B "$work_root/build-release" -DCMAKE_BUILD_TYPE=Release
cmake --build "$work_root/build-release" --parallel
{
  echo "date=$(date -Iseconds)"
  echo "git_commit=$(git -C "$repo_root" rev-parse HEAD 2>/dev/null || echo unknown)"
  echo "git_dirty=$(if [[ -z "$(git -C "$repo_root" status --porcelain 2>/dev/null)" ]]; then echo false; else echo true; fi)"
  echo "kernel=$(uname -srvo)"
  echo "filesystem_type=$filesystem_type"
  echo "output_filesystem_type=$output_filesystem_type"
  echo "available_bytes_at_start=$available_bytes"
  echo "compiler=$(c++ --version | head -n1)"
  echo "cmake=$(cmake --version | head -n1)"
  echo "build_type=Release"
  echo "prefill_keys=$prefill_keys"
  echo "operations=$operations"
  echo "rounds=$rounds"
  lscpu
  free -h
  df -T "$work_root"
  lsblk -o NAME,TYPE,SIZE,ROTA,FSTYPE,MOUNTPOINTS 2>/dev/null || true
} > "$output_dir/environment.txt"
for case_spec in "${cases[@]}"; do
  read -r name writers value_bytes distribution compaction <<< "$case_spec"
  "$work_root/build-release/target/bin/kv_test" qualification-bench-json \
    "$prefill_keys" "$operations" "$writers" "$value_bytes" "$rounds" "$distribution" "$compaction" \
    > "$output_dir/$name.json"
done
