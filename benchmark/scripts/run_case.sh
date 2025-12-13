#!/usr/bin/env bash
set -euo pipefail

# Usage: run_case.sh <label> <reads> <writes> <commits> [--seed-data] [--kill-at N --kill-target leader|<id> --restart-delay-ms MS] [extra flags...]

if [ $# -lt 4 ]; then
  echo "Usage: $0 <label> <reads> <writes> <commits> [extra flags...]" >&2
  exit 1
fi

LABEL="$1"; shift
READS="$1"; shift
WRITES="$1"; shift
COMMITS="$1"; shift

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CSV_DIR="${CSV_DIR:-$ROOT/reports/csv}"
RUNS="${RUNS:-7}"

# If you want to use an SSD mount, set STORAGE_ROOT=/mnt/ssd/benchmarks
STORAGE_ROOT_FLAG=()
if [ -n "${STORAGE_ROOT:-}" ]; then
  STORAGE_ROOT_FLAG=(--storage-root "$STORAGE_ROOT")
fi

cargo run --release --manifest-path "$ROOT/Cargo.toml" -- \
  --label "$LABEL" \
  --reads "$READS" \
  --writes "$WRITES" \
  --commits "$COMMITS" \
  --runs "$RUNS" \
  --csv-dir "$CSV_DIR" \
  "${STORAGE_ROOT_FLAG[@]}" \
  "$@"
