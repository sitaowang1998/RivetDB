#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RATIOS=(${RATIOS:-0 10 20 30 40 50 60 70 80 90 100})
TOTAL_OPS=1000
COMMITS=100

for r in "${RATIOS[@]}"; do
  READS=$((TOTAL_OPS * r / 100))
  WRITES=$((TOTAL_OPS - READS))
  LABEL="mixed_${TOTAL_OPS}_ops_${COMMITS}_commits_read${r}"
  SEED_FLAG=()
  if [ "$READS" -gt 0 ]; then
    SEED_FLAG=(--seed-data)
  fi
  "$SCRIPT_DIR/run_case.sh" "$LABEL" "$READS" "$WRITES" "$COMMITS" "${SEED_FLAG[@]}" "$@"
done
