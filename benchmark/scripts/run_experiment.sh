#!/usr/bin/env bash
set -euo pipefail

#!/usr/bin/env bash
set -euo pipefail
# Backward compatibility shim: delegates to per-case scripts by name.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [ $# -lt 1 ]; then
  echo "Usage: $0 <script-name> [args...]" >&2
  exit 1
fi
TARGET="$1"; shift
SCRIPT="$SCRIPT_DIR/${TARGET}.sh"
if [ ! -x "$SCRIPT" ]; then
  echo "Unknown experiment script: $TARGET" >&2
  exit 1
fi
exec "$SCRIPT" "$@"
