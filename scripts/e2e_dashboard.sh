#!/usr/bin/env bash
set -euo pipefail
set +m

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

cleanup() {
  scripts/stop_local_cluster.sh >/dev/null 2>&1 || true
}
trap cleanup EXIT

wait_for_log() {
  local file="$1"
  local pattern="$2"
  local name="$3"
  for _ in $(seq 1 100); do
    if [ -f "$file" ] && grep -q "$pattern" "$file"; then
      return 0
    fi
    sleep 0.1
  done
  echo "Timed out waiting for $name" >&2
  [ -f "$file" ] && cat "$file" >&2
  exit 1
}

rm -rf data
scripts/run_local_cluster.sh >/dev/null
wait_for_log build/logs/gateway.log "Dashboard HTTP service listening" "Dashboard"

health="$(curl -fsS http://127.0.0.1:9090/api/health)"
echo "$health" | grep -q '"ok":true'

status="$(curl -fsS http://127.0.0.1:9090/api/status)"
echo "$status" | grep -q '"store_count":"3"'
echo "$status" | grep -q '"region_count":"1"'
echo "$status" | grep -q '"runtime_'

html="$(curl -fsS http://127.0.0.1:9090/)"
echo "$html" | grep -q "RStone Dashboard"

echo "RStone dashboard e2e passed"
