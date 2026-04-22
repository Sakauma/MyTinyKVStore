#!/usr/bin/env bash
set -euo pipefail

OUTPUT_DIR="${1:-artifacts/qualification/$(date +%Y%m%dT%H%M%S)}"
DURATION_SECONDS="${2:-43200}"
REQUIRED_PUTS="${3:-10000000}"
PROFILE="${4:-write-heavy}"
BIN_PATH="${5:-./build/target/bin/kv_test}"

mkdir -p "${OUTPUT_DIR}"

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
  echo "pwd=$(pwd)"
  echo "uname=$(uname -a)"
  echo "duration_seconds=${DURATION_SECONDS}"
  echo "required_puts=${REQUIRED_PUTS}"
  echo "profile=${PROFILE}"
  git rev-parse HEAD 2>/dev/null | sed 's/^/git_commit=/'
} > "${ENV_TXT}"

echo "${BIN_PATH} concurrency-stress-json ${DURATION_SECONDS} ${PROFILE}" > "${COMMAND_TXT}"
"${BIN_PATH}" concurrency-stress-json "${DURATION_SECONDS}" "${PROFILE}" | tee "${SUMMARY_JSON}"

PUT_OPERATIONS="$(grep -o '"put_operations":[0-9]*' "${SUMMARY_JSON}" | head -n1 | cut -d: -f2)"
FINAL_LIVE_OBJECTS="$(grep -o '"final_live_objects":[0-9]*' "${SUMMARY_JSON}" | head -n1 | cut -d: -f2)"

if [[ -z "${PUT_OPERATIONS}" ]]; then
  echo "failed to parse put_operations from ${SUMMARY_JSON}" >&2
  exit 1
fi

STATUS="pass"
if (( PUT_OPERATIONS < REQUIRED_PUTS )); then
  STATUS="fail"
fi

{
  echo "status=${STATUS}"
  echo "put_operations=${PUT_OPERATIONS}"
  echo "required_puts=${REQUIRED_PUTS}"
  echo "final_live_objects=${FINAL_LIVE_OBJECTS:-0}"
  echo "duration_seconds=${DURATION_SECONDS}"
  echo "profile=${PROFILE}"
} > "${RESULT_TXT}"

cat "${RESULT_TXT}"

if [[ "${STATUS}" != "pass" ]]; then
  exit 2
fi
