#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

OPS="${1:-100}"
ENDPOINT="${2:-127.0.0.1:18080}"

cleanup() {
  scripts/stop_local_cluster.sh >/dev/null 2>&1 || true
}
trap cleanup EXIT

if ! [[ "$OPS" =~ ^[0-9]+$ ]] || [ "$OPS" -le 0 ]; then
  echo "Usage: $0 [ops] [endpoint]" >&2
  exit 2
fi

rm -rf data
scripts/run_local_cluster.sh >/dev/null

put_start_ns="$(date +%s%N)"
for i in $(seq 1 "$OPS"); do
  ./build/rstone-cli --endpoint "$ENDPOINT" put "bench:key:$i" "value-$i" >/dev/null
done
put_end_ns="$(date +%s%N)"

get_start_ns="$(date +%s%N)"
for i in $(seq 1 "$OPS"); do
  value="$(./build/rstone-cli --endpoint "$ENDPOINT" get "bench:key:$i")"
  expected="value-$i"
  if [ "$value" != "$expected" ]; then
    echo "unexpected value for bench:key:$i: expected=$expected actual=$value" >&2
    exit 1
  fi
done
get_end_ns="$(date +%s%N)"

put_ms=$(( (put_end_ns - put_start_ns) / 1000000 ))
get_ms=$(( (get_end_ns - get_start_ns) / 1000000 ))
total_ms=$(( put_ms + get_ms ))

awk -v endpoint="$ENDPOINT" -v ops="$OPS" -v put_ms="$put_ms" -v get_ms="$get_ms" -v total_ms="$total_ms" '
BEGIN {
  put_qps = put_ms > 0 ? ops * 1000 / put_ms : 0;
  get_qps = get_ms > 0 ? ops * 1000 / get_ms : 0;
  total_qps = total_ms > 0 ? ops * 2 * 1000 / total_ms : 0;
  printf("RStone benchmark\n");
  printf("endpoint=%s\n", endpoint);
  printf("ops_per_phase=%d\n", ops);
  printf("put_ms=%d\n", put_ms);
  printf("get_ms=%d\n", get_ms);
  printf("total_ms=%d\n", total_ms);
  printf("put_ops_per_sec=%.2f\n", put_qps);
  printf("get_ops_per_sec=%.2f\n", get_qps);
  printf("total_ops_per_sec=%.2f\n", total_qps);
}'
