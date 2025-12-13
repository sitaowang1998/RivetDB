#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMMITS=(${COMMITS_LIST:-10 25 50 75 100})
TOTAL_OPS=1000

for c in "${COMMITS[@]}"; do
  LABEL="write_${TOTAL_OPS}_commit${c}"
  "$SCRIPT_DIR/run_case.sh" "$LABEL" 0 "$TOTAL_OPS" "$c" "$@"
done
