#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cargo run --release --manifest-path "$ROOT/Cargo.toml" -- "$@"
