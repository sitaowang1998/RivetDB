#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMMITS=100

for t in 1 2 3 4 5; do
  THREADS="$t" "$SCRIPT_DIR/run_case.sh" "threads_read_1000_c${COMMITS}_t${t}" 1000 0 "$COMMITS" --seed-data "$@"
  THREADS="$t" "$SCRIPT_DIR/run_case.sh" "threads_write_1000_c${COMMITS}_t${t}" 0 1000 "$COMMITS" "$@"
  THREADS="$t" "$SCRIPT_DIR/run_case.sh" "threads_mixed_500_500_c${COMMITS}_t${t}" 500 500 "$COMMITS" --seed-data "$@"
done
