#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
"$SCRIPT_DIR/run_case.sh" "kill_restart_reads" 1000 0 100 --seed-data --kill-at 500 --kill-target leader --restart-delay-ms 250 "$@"
