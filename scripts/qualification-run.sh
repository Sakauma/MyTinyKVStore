#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
qualification_root="${KVSTORE_QUALIFICATION_ROOT:-${XDG_STATE_HOME:-$HOME/.local/state}/mytinykvstore/qualification}"
OUTPUT_DIR="${1:-$qualification_root/soak-$(date +%Y%m%dT%H%M%S)}"
DURATION_SECONDS="${2:-43200}"
REQUIRED_UNIQUE_KEYS="${3:-10000000}"
WRITERS="${4:-16}"
VALUE_BYTES="${5:-256}"
BIN_PATH="${6:-}"

if (( DURATION_SECONDS < 43200 || REQUIRED_UNIQUE_KEYS < 10000000 )); then
  if [[ "${KVSTORE_ALLOW_SHORT_QUALIFICATION:-0}" != "1" ]]; then
    echo "certification requires at least 43200 seconds and 10000000 unique keys" >&2
    exit 1
  fi
  certification_scope="smoke-only"
else
  certification_scope="full"
fi

OUTPUT_DIR="$(realpath -m "$OUTPUT_DIR")"
case "$OUTPUT_DIR/" in
  "$repo_root/"*) echo "qualification output must be outside the repository: $OUTPUT_DIR" >&2; exit 2 ;;
esac
mkdir -p "${OUTPUT_DIR}"
OUTPUT_FILESYSTEM_TYPE="$(findmnt -T "$OUTPUT_DIR" -n -o FSTYPE)"
if [[ "$OUTPUT_FILESYSTEM_TYPE" != "ext4" ]]; then
  echo "qualification output must be on native ext4, got $OUTPUT_FILESYSTEM_TYPE" >&2
  exit 2
fi

DB_ROOT="${KVSTORE_QUALIFICATION_DB_DIR:-/tmp/mytinykv-qualification-db-$(date +%Y%m%dT%H%M%S)}"
mkdir -p "$DB_ROOT"
DB_ROOT="$(cd "$DB_ROOT" && pwd)"
FILESYSTEM_TYPE="$(findmnt -T "$DB_ROOT" -n -o FSTYPE)"
if [[ "$FILESYSTEM_TYPE" != "ext4" ]]; then
  echo "qualification database must be on native ext4, got $FILESYSTEM_TYPE" >&2
  exit 2
fi
MINIMUM_FREE_BYTES=$((25 * 1024 * 1024 * 1024))
AVAILABLE_BYTES="$(df --output=avail -B1 "$DB_ROOT" | tail -n1 | tr -d ' ')"
if (( AVAILABLE_BYTES < MINIMUM_FREE_BYTES )); then
  echo "qualification requires at least 25 GiB free on the database filesystem" >&2
  exit 2
fi
DB_PATH="$DB_ROOT/qualification.dat"
if [[ -e "$DB_PATH" ]]; then
  echo "qualification database already exists: $DB_PATH" >&2
  exit 1
fi

BUILD_ROOT=""
if [[ -z "$BIN_PATH" ]]; then
  BUILD_ROOT="$(mktemp -d /tmp/mytinykv-soak-build.XXXXXX)"
  case "$BUILD_ROOT" in
    /tmp/mytinykv-soak-build.*) ;;
    *) echo "unsafe temporary build path: $BUILD_ROOT" >&2; exit 1 ;;
  esac
  trap 'rm -rf -- "$BUILD_ROOT"' EXIT
  tar -C "$repo_root" \
    --exclude=.git \
    --exclude='build*' \
    --exclude=target \
    --exclude=artifacts \
    -cf - . | tar -C "$BUILD_ROOT" -xf -
  cmake -S "$BUILD_ROOT" -B "$BUILD_ROOT/build-release" -DCMAKE_BUILD_TYPE=Release
  cmake --build "$BUILD_ROOT/build-release" --parallel
  BIN_PATH="$BUILD_ROOT/build-release/target/bin/kv_test"
fi

BIN_FILESYSTEM_TYPE="$(findmnt -T "$BIN_PATH" -n -o FSTYPE)"
if [[ "$BIN_FILESYSTEM_TYPE" != "ext4" ]]; then
  echo "qualification binary must be built on native ext4, got $BIN_FILESYSTEM_TYPE" >&2
  exit 2
fi

SUMMARY_JSON="${OUTPUT_DIR}/stress-summary.json"
ENV_TXT="${OUTPUT_DIR}/environment.txt"
COMMAND_TXT="${OUTPUT_DIR}/command.txt"
RESULT_TXT="${OUTPUT_DIR}/result.txt"

if [[ ! -x "${BIN_PATH}" ]]; then
  echo "kv_test binary not found or not executable: ${BIN_PATH}" >&2
  exit 1
fi

{
  echo "date=$(date -Iseconds)"
  echo "repo_root=${repo_root}"
  echo "kernel=$(uname -srvo)"
  echo "filesystem_type=${FILESYSTEM_TYPE}"
  echo "output_filesystem_type=${OUTPUT_FILESYSTEM_TYPE}"
  echo "binary_filesystem_type=${BIN_FILESYSTEM_TYPE}"
  echo "available_bytes_at_start=${AVAILABLE_BYTES}"
  echo "database_path=${DB_PATH}"
  echo "duration_seconds=${DURATION_SECONDS}"
  echo "required_unique_keys=${REQUIRED_UNIQUE_KEYS}"
  echo "writers=${WRITERS}"
  echo "value_bytes=${VALUE_BYTES}"
  echo "durability=sync"
  echo "compaction=on"
  echo "build_type=Release"
  echo "certification_scope=${certification_scope}"
  git -C "$repo_root" rev-parse HEAD 2>/dev/null | sed 's/^/git_commit=/'
  echo "git_dirty=$(if [[ -z "$(git -C "$repo_root" status --porcelain 2>/dev/null)" ]]; then echo false; else echo true; fi)"
  c++ --version | head -n1
  cmake --version | head -n1
  lscpu
  free -h
  df -T "$DB_ROOT"
  lsblk -o NAME,TYPE,SIZE,ROTA,FSTYPE,MOUNTPOINTS 2>/dev/null || true
} > "${ENV_TXT}"

echo "${BIN_PATH} qualification-soak-json ${DB_PATH} ${DURATION_SECONDS} ${REQUIRED_UNIQUE_KEYS} ${WRITERS} ${VALUE_BYTES}" \
  > "${COMMAND_TXT}"
"${BIN_PATH}" qualification-soak-json "$DB_PATH" "$DURATION_SECONDS" \
  "$REQUIRED_UNIQUE_KEYS" "$WRITERS" "$VALUE_BYTES" | tee "${SUMMARY_JSON}"
"${BIN_PATH}" verify-format "$DB_PATH" | tee "${OUTPUT_DIR}/verify-format.txt"

STATUS="$(grep -o '"status":"[^"]*"' "$SUMMARY_JSON" | head -n1 | cut -d'"' -f4)"
UNIQUE_KEYS="$(grep -o '"unique_keys_committed":[0-9]*' "$SUMMARY_JSON" | head -n1 | cut -d: -f2)"
VERIFIED_KEYS="$(grep -o '"verified_keys_after_restart":[0-9]*' "$SUMMARY_JSON" | head -n1 | cut -d: -f2)"

{
  echo "status=${STATUS}"
  echo "certification_scope=${certification_scope}"
  echo "unique_keys_committed=${UNIQUE_KEYS:-0}"
  echo "verified_keys_after_restart=${VERIFIED_KEYS:-0}"
  echo "required_unique_keys=${REQUIRED_UNIQUE_KEYS}"
  echo "duration_seconds=${DURATION_SECONDS}"
  echo "database_path=${DB_PATH}"
} > "${RESULT_TXT}"

cat "${RESULT_TXT}"

if [[ "${STATUS}" != "pass" ]]; then
  exit 2
fi
