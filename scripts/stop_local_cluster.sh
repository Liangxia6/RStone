#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

if [ -d build/pids ]; then
  for pid_file in build/pids/*.pid; do
    [ -e "$pid_file" ] || continue
    pid="$(cat "$pid_file")"
    if kill -0 "$pid" >/dev/null 2>&1; then
      kill "$pid" >/dev/null 2>&1 || true
    fi
    rm -f "$pid_file"
  done
fi

echo "RStone local cluster stopped"
