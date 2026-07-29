#!/usr/bin/env bash
# Stage-4 verification harness (LAB-738): probe the live deployment for the
# epic's AC-1/AC-3/AC-4 evidence — reachability, X-Cache: HIT, payload
# freshness, hit-rate counters. Read-only; needs curl + python3, no secrets.
#
#   ./verify.sh              one full probe pass
#   ./verify.sh hitrate 3600 sample /api/stats for N seconds (AC-4 fallback;
#                            counters are PER-ISOLATE and reset on recycle —
#                            state that scope next to any number you quote)
set -euo pipefail

EDGE="${EDGE_URL:-https://skyline-edge.raywalker.workers.dev}"
INGESTER="${INGESTER_URL:-https://skyline-ingester.onrender.com}"
WINDOW="${WINDOW:-5m}"
# Locked contract: 5m TTL is 60 s, republished at TTL/2 — anything older than
# ~5 min means the pipeline stalled, not merely lagged.
MAX_AGE_SECONDS="${MAX_AGE_SECONDS:-300}"

hitrate() {
    local duration="${1:-3600}" interval=60 elapsed=0
    echo "sampling $EDGE/api/stats every ${interval}s for ${duration}s (per-isolate counters)"
    while [ "$elapsed" -le "$duration" ]; do
        printf '%s %s\n' "$(date -u +%FT%TZ)" "$(curl -fsS "$EDGE/api/stats")" || true
        sleep "$interval"
        elapsed=$((elapsed + interval))
    done
    exit 0
}
[ "${1:-}" = "hitrate" ] && hitrate "${2:-3600}"

fail=0

echo "== ingester /health (AC-1: alive, Jetstream connected)"
if health=$(curl -fsS --max-time 90 "$INGESTER/health"); then
    echo "$health" | python3 -m json.tool
else
    echo "FAIL: $INGESTER/health unreachable or non-200 (not deployed, spun down, or Jetstream disconnected)"
    fail=1
fi

echo "== edge serves real data (epic AC-1: 200 + X-Cache: HIT from a non-origin POP)"
headers=$(mktemp)
if body=$(curl -fsS -D "$headers" "$EDGE/api/posts_per_minute?window=$WINDOW"); then
    echo "$body"
    grep -i '^x-cache:' "$headers" || { echo "FAIL: no X-Cache header"; fail=1; }
    grep -iq '^x-cache: *hit' "$headers" || { echo "FAIL: expected X-Cache: HIT"; fail=1; }
    # cf-ray's trailing colo code is the serving POP — the request's own
    # evidence it was served outside the ingester's origin region (Oregon).
    grep -i '^cf-ray:' "$headers" || true
else
    echo "FAIL: $EDGE/api/posts_per_minute?window=$WINDOW did not return 200"
    fail=1
fi
rm -f "$headers"

echo "== freshness (epic AC-3: payload generated_at, not response timing)"
if [ -n "${body:-}" ]; then
    echo "$body" | python3 -c "
import json, sys, time
v = json.load(sys.stdin)['data']  # edge envelope: {operation, window, data}
age = time.time() - v['generated_at']
print(f'generated_at age: {age:.0f}s (limit ${MAX_AGE_SECONDS}s); total_posts={v[\"total_posts\"]}')
sys.exit(0 if age <= ${MAX_AGE_SECONDS} and v['total_posts'] > 0 else 1)
" || { echo "FAIL: stale or empty aggregate — a green pipeline serving nothing proves nothing"; fail=1; }
fi

echo "== hit/miss counters (epic AC-4 raw material; per-isolate scope)"
curl -fsS "$EDGE/api/stats" || { echo "FAIL: /api/stats unreachable"; fail=1; }
echo

[ "$fail" -eq 0 ] && echo "ALL CHECKS PASSED" || echo "CHECKS FAILED: $fail section(s)"
exit "$fail"
