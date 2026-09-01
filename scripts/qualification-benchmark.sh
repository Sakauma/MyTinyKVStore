#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
qualification_root="${KVSTORE_QUALIFICATION_ROOT:-${XDG_STATE_HOME:-$HOME/.local/state}/mytinykvstore/qualification}"
output_dir="${1:-$qualification_root/benchmark-$(date +%Y%m%dT%H%M%S)}"
baseline_json="${2:-}"
prefill_keys="${3:-1000000}"
operations="${4:-10000000}"
writers="${5:-16}"
value_bytes="${6:-256}"
rounds="${7:-3}"

output_dir="$(realpath -m "$output_dir")"
case "$output_dir/" in
  "$repo_root/"*) echo "qualification output must be outside the repository: $output_dir" >&2; exit 2 ;;
esac
mkdir -p "$output_dir"
work_root="$(mktemp -d /tmp/mytinykv-qualification.XXXXXX)"
case "$work_root" in
  /tmp/mytinykv-qualification.*) ;;
  *) echo "unsafe temporary work path: $work_root" >&2; exit 1 ;;
esac
trap 'rm -rf -- "$work_root"' EXIT

filesystem_type="$(findmnt -T "$work_root" -n -o FSTYPE)"
if [[ "$filesystem_type" != "ext4" ]]; then
  echo "qualification requires a native ext4 filesystem, got: $filesystem_type" >&2
  exit 2
fi
output_filesystem_type="$(findmnt -T "$output_dir" -n -o FSTYPE)"
if [[ "$output_filesystem_type" != "ext4" ]]; then
  echo "qualification output requires a native ext4 filesystem, got: $output_filesystem_type" >&2
  exit 2
fi
minimum_free_bytes=$((25 * 1024 * 1024 * 1024))
available_bytes="$(df --output=avail -B1 "$work_root" | tail -n1 | tr -d ' ')"
if (( available_bytes < minimum_free_bytes )); then
  echo "qualification requires at least 25 GiB free on the workload filesystem" >&2
  exit 2
fi

tar -C "$repo_root" \
  --exclude=.git \
  --exclude='build*' \
  --exclude=target \
  --exclude=artifacts \
  -cf - . | tar -C "$work_root" -xf -

cmake -S "$work_root" -B "$work_root/build-release" -DCMAKE_BUILD_TYPE=Release
cmake --build "$work_root/build-release" --parallel

candidate_json="$output_dir/canonical-candidate.json"
environment_txt="$output_dir/environment.txt"
command_txt="$output_dir/command.txt"

{
  echo "date=$(date -Iseconds)"
  echo "git_commit=$(git -C "$repo_root" rev-parse HEAD 2>/dev/null || echo unknown)"
  echo "git_dirty=$(if [[ -z "$(git -C "$repo_root" status --porcelain 2>/dev/null)" ]]; then echo false; else echo true; fi)"
  echo "kernel=$(uname -srvo)"
  echo "filesystem_type=$filesystem_type"
  echo "output_filesystem_type=$output_filesystem_type"
  echo "available_bytes_at_start=$available_bytes"
  echo "compiler=$($work_root/build-release/target/bin/kv_test --version 2>/dev/null || c++ --version | head -n1)"
  echo "cmake=$(cmake --version | head -n1)"
  echo "build_type=Release"
  echo "prefill_keys=$prefill_keys"
  echo "operations=$operations"
  echo "writers=$writers"
  echo "value_bytes=$value_bytes"
  echo "rounds=$rounds"
  echo "distribution=uniform"
  echo "compaction=off"
  echo "durability=sync"
  lscpu
  free -h
  df -T "$work_root"
  lsblk -o NAME,TYPE,SIZE,ROTA,FSTYPE,MOUNTPOINTS 2>/dev/null || true
} > "$environment_txt"

echo "$work_root/build-release/target/bin/kv_test qualification-bench-json $prefill_keys $operations $writers $value_bytes $rounds uniform off" \
  > "$command_txt"
"$work_root/build-release/target/bin/kv_test" qualification-bench-json \
  "$prefill_keys" "$operations" "$writers" "$value_bytes" "$rounds" uniform off \
  | tee "$candidate_json"

if [[ -n "$baseline_json" ]]; then
  "$work_root/build-release/target/bin/kv_test" compare-qualification \
    "$baseline_json" "$candidate_json" 200 120 | tee "$output_dir/gate.txt"
else
  echo "status=not_evaluated reason=no_baseline_json" | tee "$output_dir/gate.txt"
fi
