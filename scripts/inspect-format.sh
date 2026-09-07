#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)/format-tool-common.sh"

if [[ $# -ne 1 ]]; then
  echo "Usage: bash scripts/inspect-format.sh <db_path>" >&2
  exit 1
fi

kvstore_build_format_tool
"${KVSTORE_FORMAT_TOOL}" inspect-format "$1"
