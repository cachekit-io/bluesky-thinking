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
# Guessed name-based URL; if the first Render deploy lands suffixed, update it
# here AND in edge/wrangler.toml (INGESTER_HEALTH_URL).
INGESTER="${INGESTER_URL:-https://skyline-ingester.onrender.com}"
WINDOW="${WINDOW:-5m}"
# Locked contract: 5m TTL is 60 s, republished at TTL/2 — anything older than
# ~5 min means the pipeline stalled, not merely lagged.
MAX_AGE_SECONDS="${MAX_AGE_SECONDS:-300}"

hitrate() {
    local duration="${1:-3600}" interval=60 elapsed=0
    echo "sampling $EDGE/api/stats every ${interval}s for ${duration}s (per-isolate counters)"
    while [ "$elapsed" -le "$duration" ]; do
        printf '%s %s\n' "$(date -u +%FT%TZ)" "$(curl -fsS "$EDGE/api/stats")"
        sleep "$interval"
        elapsed=$((elapsed + interval))
    done
    exit 0
}
[ "${1:-}" = "hitrate" ] && hitrate "${2:-3600}"

fail=0

echo "== ingester /health (AC-1: alive, Jetstream connected)"
# No curl -f here: a 503 carries the JSON that says WHY (Jetstream down),
# which is exactly what separates "degraded" from "not deployed at all".
health_body=$(mktemp)
health_code=$(curl -sS --max-time 90 -o "$health_body" -w '%{http_code}' "$INGESTER/health" || echo 000)
if [ "$health_code" = "200" ]; then
    python3 -m json.tool "$health_body"
elif [ "$health_code" = "000" ]; then
    echo "FAIL: $INGESTER/health unreachable (not deployed, or spun down and still cold-starting)"
    fail=$((fail + 1))
else
    # 503 + JSON body = process up, Jetstream down; a Render "Not Found"
    # page = the service doesn't exist at this URL yet.
    echo "FAIL: /health returned $health_code:"
    cat "$health_body"; echo
    fail=$((fail + 1))
fi
rm -f "$health_body"

echo "== edge serves real data (epic AC-1: 200 + X-Cache: HIT from a non-origin POP)"
headers=$(mktemp)
if body=$(curl -fsS -D "$headers" "$EDGE/api/posts_per_minute?window=$WINDOW"); then
    echo "$body"
    grep -i '^x-cache:' "$headers" || { echo "FAIL: no X-Cache header"; fail=$((fail + 1)); }
    grep -iq '^x-cache: *hit' "$headers" || { echo "FAIL: expected X-Cache: HIT"; fail=$((fail + 1)); }
    # cf-ray's trailing colo code is the serving POP — the request's own
    # evidence it was served outside the ingester's origin region (Oregon).
    grep -i '^cf-ray:' "$headers" || true
else
    echo "FAIL: $EDGE/api/posts_per_minute?window=$WINDOW did not return 200"
    fail=$((fail + 1))
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
" || { echo "FAIL: stale or empty aggregate — a green pipeline serving nothing proves nothing"; fail=$((fail + 1)); }
fi

echo "== hit/miss counters (epic AC-4 raw material; per-isolate scope)"
curl -fsS "$EDGE/api/stats" || { echo "FAIL: /api/stats unreachable"; fail=$((fail + 1)); }
echo

if [ "$fail" -eq 0 ]; then
    echo "ALL CHECKS PASSED"
else
    echo "FAILED: $fail check(s)"
    exit 1
fi
