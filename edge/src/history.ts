/**
 * Skyline aggregate-snapshot history (LAB-1616, design: docs/history.md).
 *
 * Persists versioned, aggregate-only snapshots of the live interop values
 * into D1 so 7-day and 30-day charts and baselines exist after the rolling
 * windows move on. Capture runs on the edge cron — the Render ingester is
 * never involved, so history failure cannot touch current publishing (and
 * adds zero Render egress, the binding constraint since LAB-1894).
 *
 * Tiering is capture-time, not post-hoc: the hourly tier snapshots the `1h`
 * rolling window at each top of hour, the daily tier snapshots the `24h`
 * window at each UTC midnight. Each tier is exact for its own window at
 * capture time, which sidesteps merging non-additive top-N rankings
 * entirely. Missed captures are gaps; nothing backfills or zero-fills.
 *
 * Privacy: rows hold exactly what the public /api/{operation} endpoints
 * already serve — allowlisted, trimmed to top-20 — minus nothing and plus
 * nothing. No post text, no author DID, no record key, and never the
 * sentiment cache (zero-knowledge boundary, see snapshot() in
 * ingester windows.py).
 */
import { generateInteropKey, decodeInteropValue, type Backend } from '@cachekit-io/cachekit';
import { NAMESPACE, OPERATIONS, isOperation, json, jsonSafe, type Operation } from './handler.js';

/** Ranked lists are stored — and served — at this depth. */
export const STORED_TOP_N = 20;

interface TierSpec {
  periodSeconds: number;
  /** The rolling window this tier snapshots at each bucket boundary. */
  sourceWindow: '1h' | '24h';
  /**
   * Staleness guard: reject an aggregate generated more than this many
   * seconds before the bucket boundary. Matches the source window's locked
   * TTL (architecture.md), so anything older has in fact already expired —
   * this guards clock skew and republish races, and turns "stale" into a
   * gap instead of a mislabeled point.
   */
  maxAgeSeconds: number;
  retentionSeconds: number;
}

export const TIER_SPECS: Record<'hourly' | 'daily', TierSpec> = {
  hourly: {
    periodSeconds: 3600,
    sourceWindow: '1h',
    maxAgeSeconds: 300,
    retentionSeconds: 35 * 86400,
  },
  daily: {
    periodSeconds: 86400,
    sourceWindow: '24h',
    maxAgeSeconds: 900,
    retentionSeconds: 400 * 86400,
  },
};
export type Tier = keyof typeof TIER_SPECS;
/** Both tiers, typed at declaration so iteration needs no key-cast. */
const TIERS: readonly Tier[] = ['hourly', 'daily'];

/**
 * Range queries never include the bucket whose capture may still be in
 * flight: a request seconds after the boundary must not see a series
 * missing a point that lands moments later. Belt: responses are only
 * CacheKit-cached when the newest expected bucket is actually present.
 */
const CAPTURE_GRACE_SECONDS = 120;

/** Hard cap on a stored row's payload; a hostile aggregate becomes a gap. */
export const MAX_PAYLOAD_BYTES = 32_768;

/**
 * Per-field character cap for the allowlisted string fields of a ranked-list
 * element. Sized for the longest legitimate one — a link URI; tags, DNS
 * names and emoji sit far inside it. This bounds a single field, not the row
 * (MAX_PAYLOAD_BYTES does that), so a hostile value is truncated rather than
 * allowed to spend the whole row budget on one string.
 */
export const MAX_FIELD_CHARS = 512;

/**
 * Hard cap on a cached response — enforced on BOTH sides of the cache. The
 * backend is operator-writable, so cached bytes get the same distrust as
 * every other backend read: an oversized or unparseable value is treated as
 * a miss and recomputed from D1 (which also heals the poisoned key on
 * re-set). The write side enforces the same bound so an over-limit response
 * can never poison its own key into permanent reject-recompute-rewrite.
 */
export const MAX_RESPONSE_BYTES = 4 * 1024 * 1024;

/**
 * TTL for caching an INCOMPLETE series (newest expected bucket absent).
 * Long enough to bound D1 recompute while capture is late or down, short
 * enough that a point landing moments later is picked up within a minute.
 */
export const INCOMPLETE_RESPONSE_TTL_SECONDS = 60;

const RANGES: Record<'7d' | '30d', { seconds: number; tier: Tier }> = {
  '7d': { seconds: 7 * 86400, tier: 'hourly' },
  '30d': { seconds: 30 * 86400, tier: 'daily' },
};
type HistoryRange = keyof typeof RANGES;

/**
 * Structural slice of the D1 API (the repo doesn't use workers-types).
 * `run()` surfaces `meta.changes` so INSERT OR IGNORE can report
 * duplicate-vs-captured honestly.
 */
export interface D1PreparedStatement {
  bind(...values: unknown[]): D1PreparedStatement;
  all(): Promise<{ results: Record<string, unknown>[] }>;
  run(): Promise<{ meta?: { changes?: number } }>;
}
export interface D1Database {
  prepare(sql: string): D1PreparedStatement;
}

interface CaptureCounts {
  captured: number;
  duplicate: number;
  missing: number;
  stale: number;
  invalid: number;
  failed: number;
}

export interface CaptureReport {
  boundary: 'none' | 'hourly' | 'daily';
  hourly?: CaptureCounts;
  daily?: CaptureCounts;
  retention?: { hourlyDeleted: number; dailyDeleted: number };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === 'object' && !Array.isArray(value);
}

/** Coerce a decoded count-ish field (number | BigInt) to a finite number. */
function asFinite(value: unknown): number | null {
  if (typeof value === 'bigint') value = Number(value);
  return typeof value === 'number' && Number.isFinite(value) ? value : null;
}

/**
 * Rebuild a {name: count} record with values coerced to finite numbers and
 * key names capped at 64 chars; anything else is dropped. Counts-by-name is
 * these fields' contract, and enforcing it value-level at write time is what
 * makes the "no post text ever reaches storage" promise a property of the
 * code rather than trust in the operator-writable source cache (panel MAJ,
 * LAB-1935).
 */
function numericRecord(record: Record<string, unknown>): Record<string, number> {
  const out: Record<string, number> = {};
  for (const [key, raw] of Object.entries(record)) {
    const count = asFinite(raw);
    if (count !== null) out[key.slice(0, 64)] = count;
  }
  return out;
}

/**
 * Rebuild one ranked list from an allowlisted element shape: `label` (a
 * required string, truncated) + `count` (a required finite number) + any
 * `extra` decorative labels, copied only when present AND a string.
 *
 * The whole list is rejected — gapping the snapshot — when an element is not
 * a record or is missing its label or count, because a ranked list with
 * elements silently dropped would misstate the ranking it claims to be.
 * A malformed *decorative* field is dropped instead: it cannot change the
 * ranking, so it is not worth a gap.
 *
 * Rebuilding rather than slicing is the point. `list.slice()` copied elements
 * wholesale, so any extra key an operator-writable cache put on an element —
 * nested objects, post text under an unknown field — was persisted verbatim
 * under the byte cap. That made "no post text ever reaches storage" true of
 * the record-valued fields only, while the docs and this module claimed it of
 * the whole payload. Now it is uniform: unknown element keys cannot survive.
 */
function topList(
  list: unknown,
  label: string,
  extra: readonly string[] = [],
): Record<string, unknown>[] | null {
  if (!Array.isArray(list)) return null;
  const out: Record<string, unknown>[] = [];
  for (const element of list.slice(0, STORED_TOP_N)) {
    if (!isRecord(element)) return null;
    const name = element[label];
    const count = asFinite(element.count);
    if (typeof name !== 'string' || count === null) return null;
    const rebuilt: Record<string, unknown> = { [label]: name.slice(0, MAX_FIELD_CHARS), count };
    for (const field of extra) {
      const value = element[field];
      if (typeof value === 'string') rebuilt[field] = value.slice(0, MAX_FIELD_CHARS);
    }
    out.push(rebuilt);
  }
  return out;
}

/**
 * Allowlist + trim one live aggregate into its history payload. Returns
 * null when the operation's own required shape is absent — a malformed or
 * hostile cache entry becomes a counted gap, never a stored row. Only keys
 * named here can ever reach storage, and both record-valued fields and
 * ranked-list elements are rebuilt value-level, never copied wholesale.
 */
export function trimPayload(
  operation: Operation,
  value: Record<string, unknown>,
): Record<string, unknown> | null {
  const out: Record<string, unknown> = {};
  // `window` is compared strictly against the tier's source window before
  // capture ever calls this, so the stored value is one of two literals.
  if ('window' in value) out.window = value.window;
  for (const key of ['total_posts', 'total_events_considered', 'total_signal_candidates']) {
    const total = asFinite(value[key]);
    if (total !== null) out[key] = total;
  }
  if (isRecord(value.excluded_count_by_reason)) {
    out.excluded_count_by_reason = numericRecord(value.excluded_count_by_reason);
  }
  switch (operation) {
    case 'trending_hashtags': {
      // `display` is the tag's presentation casing (LAB-1613) — decorative,
      // so absence trims it rather than gapping the whole ranking.
      const hashtags = topList(value.hashtags, 'tag', ['display']);
      if (!hashtags) return null;
      out.hashtags = hashtags;
      return out;
    }
    case 'trending_links': {
      const links = topList(value.links, 'uri');
      const domains = topList(value.domains, 'domain');
      if (!links || !domains) return null;
      out.links = links;
      out.domains = domains;
      return out;
    }
    case 'lang_mix': {
      if (!isRecord(value.langs)) return null;
      out.langs = numericRecord(value.langs);
      return out;
    }
    case 'posts_per_minute': {
      const ppm = asFinite(value.ppm);
      if (ppm === null) return null;
      out.ppm = ppm;
      return out;
    }
    case 'top_emoji': {
      const emoji = topList(value.emoji, 'emoji');
      if (!emoji) return null;
      out.emoji = emoji;
      return out;
    }
  }
  // The switch is exhaustive over Operation — proven at compile time by the
  // `satisfies never` below — so this only fires for an untyped runtime
  // caller, and an unknown operation becomes a gap like any other bad input.
  operation satisfies never;
  return null;
}

const INSERT_SQL = `INSERT OR IGNORE INTO snapshots
  (operation, tier, bucket_ts, generated_at, normalization_version, payload, captured_at)
  VALUES (?, ?, ?, ?, ?, ?, ?)`;

async function captureTier(
  backend: Backend,
  db: D1Database,
  tier: Tier,
  nowSec: number,
): Promise<CaptureCounts> {
  const spec = TIER_SPECS[tier];
  const bucketTs = Math.floor(nowSec / spec.periodSeconds) * spec.periodSeconds;
  const counts: CaptureCounts = {
    captured: 0,
    duplicate: 0,
    missing: 0,
    stale: 0,
    invalid: 0,
    failed: 0,
  };

  for (const operation of OPERATIONS) {
    // Per-operation isolation: one bad aggregate must cost exactly one gap,
    // never the other four rows (the LAB-1613 round-4 lesson, applied here).
    try {
      const raw = await backend.get(generateInteropKey(NAMESPACE, operation, [spec.sourceWindow]));
      if (raw === null) {
        counts.missing += 1;
        console.log('history_gap', { tier, operation, bucketTs, reason: 'miss' });
        continue;
      }
      const decoded: unknown = decodeInteropValue(raw);
      const generatedAt = isRecord(decoded) ? asFinite(decoded.generated_at) : null;
      const normalizationVersion = isRecord(decoded) ? decoded.normalization_version : null;
      if (
        !isRecord(decoded) ||
        generatedAt === null ||
        typeof normalizationVersion !== 'string' ||
        normalizationVersion.length === 0 ||
        normalizationVersion.length > 64 ||
        // The key names the window, but the backend is operator-writable:
        // an entry whose own window claim disagrees would persist a point
        // at the wrong magnitude under the right label. Gap it instead.
        decoded.window !== spec.sourceWindow
      ) {
        counts.invalid += 1;
        console.error('history_gap', { tier, operation, bucketTs, reason: 'invalid_envelope' });
        continue;
      }
      // Symmetric bound: the bucket label tolerates skew of up to one
      // source TTL either side. Older means the entry should already have
      // expired (clock skew, republish race); newer means this tick was
      // delivered very late and the aggregate has rolled past the boundary.
      // The bound is symmetric ON PURPOSE — measuring "newer" against a
      // tighter constant would turn ordinary cron delivery lag into five
      // gaps on a perfectly healthy pipeline.
      const age = bucketTs - generatedAt;
      if (Math.abs(age) > spec.maxAgeSeconds) {
        counts.stale += 1;
        console.error('history_gap', { tier, operation, bucketTs, reason: 'stale', age });
        continue;
      }
      const trimmed = trimPayload(operation, decoded);
      if (trimmed === null) {
        counts.invalid += 1;
        console.error('history_gap', { tier, operation, bucketTs, reason: 'invalid_payload' });
        continue;
      }
      const payload = JSON.stringify(trimmed, jsonSafe);
      // Byte length, not string length: .length counts UTF-16 code units,
      // and emoji / non-Latin hashtags are multi-byte by construction, so
      // counting units would let the real ceiling drift to ~3× the
      // documented cap (panel MAJ, LAB-1935).
      if (new TextEncoder().encode(payload).length > MAX_PAYLOAD_BYTES) {
        counts.invalid += 1;
        console.error('history_gap', { tier, operation, bucketTs, reason: 'payload_too_large' });
        continue;
      }
      const result = await db
        .prepare(INSERT_SQL)
        .bind(
          operation,
          tier,
          bucketTs,
          Math.floor(generatedAt),
          normalizationVersion,
          payload,
          nowSec,
        )
        .run();
      if ((result.meta?.changes ?? 1) === 0) counts.duplicate += 1;
      else counts.captured += 1;
    } catch (err) {
      counts.failed += 1;
      console.error('history_capture_failed', { tier, operation, bucketTs, err: String(err) });
    }
  }
  return counts;
}

async function enforceRetention(
  db: D1Database,
  nowSec: number,
): Promise<{ hourlyDeleted: number; dailyDeleted: number }> {
  const sweep = async (tier: Tier): Promise<number> => {
    const result = await db
      .prepare('DELETE FROM snapshots WHERE tier = ? AND bucket_ts < ?')
      .bind(tier, nowSec - TIER_SPECS[tier].retentionSeconds)
      .run();
    return result.meta?.changes ?? 0;
  };
  return { hourlyDeleted: await sweep('hourly'), dailyDeleted: await sweep('daily') };
}

/**
 * One cron tick. Decides from the scheduled fire time which boundaries are
 * due: minute 0 → hourly capture; additionally hour 0 UTC → daily capture
 * plus the retention sweep. Any other fire is a no-op — the caller runs
 * this on the existing every-10-minutes keep-alive schedule (a new cron
 * expression would spend one of the account's five free-plan cron slots
 * for nothing).
 *
 * Errors are counted and logged per operation inside captureTier; this
 * function only throws if D1 itself fails the retention sweep, and the
 * caller isolates even that from the keep-alive.
 */
export async function captureTick(
  backend: Backend,
  db: D1Database,
  nowMs: number,
): Promise<CaptureReport> {
  const fire = new Date(nowMs);
  if (fire.getUTCMinutes() !== 0) return { boundary: 'none' };
  const nowSec = Math.floor(nowMs / 1000);
  const daily = fire.getUTCHours() === 0;
  const report: CaptureReport = { boundary: daily ? 'daily' : 'hourly' };
  report.hourly = await captureTier(backend, db, 'hourly', nowSec);
  if (daily) {
    report.daily = await captureTier(backend, db, 'daily', nowSec);
    report.retention = await enforceRetention(db, nowSec);
  }
  return report;
}

// ---------------------------------------------------------------------------
// Range-query API
// ---------------------------------------------------------------------------

export interface HistoryDeps {
  db: D1Database;
  /**
   * CacheKit response cache (dogfood). Optional and strictly an
   * optimization: any backend failure falls through to D1, because D1 is
   * the source of truth for history — the inverse of the live endpoints,
   * where CachekitIO IS the truth and there is no fallback.
   */
  backend: Backend | null;
  nowMs: number;
}

function isHistoryRange(value: string | null): value is HistoryRange {
  // hasOwn, not `in`: `in` walks the prototype chain, so ?range=toString
  // would pass the guard and crash on the RANGES lookup.
  return value !== null && Object.hasOwn(RANGES, value);
}

interface SnapshotRow {
  bucket_ts: number;
  generated_at: number;
  normalization_version: string;
  payload: string;
}

/**
 * Runtime guard for a D1 row (D1 types results as loose records). The schema
 * makes violations impossible in practice; a row that fails anyway is the
 * same corruption class as an unparseable payload and gets the same
 * treatment — a logged gap, never a crash or a fabricated point.
 */
function asSnapshotRow(row: Record<string, unknown>): SnapshotRow | null {
  const bucket_ts = asFinite(row.bucket_ts);
  const generated_at = asFinite(row.generated_at);
  if (
    bucket_ts === null ||
    generated_at === null ||
    typeof row.normalization_version !== 'string' ||
    typeof row.payload !== 'string'
  ) {
    return null;
  }
  return {
    bucket_ts,
    generated_at,
    normalization_version: row.normalization_version,
    payload: row.payload,
  };
}

/**
 * A cached response is relayed ONLY if it is the response this request would
 * have computed: same operation, range, tier and coverage bounds, points
 * strictly ascending inside (from, to], each structurally sound. The cache
 * key already encodes those dimensions, so a mismatch means the backend
 * value was tampered with (it is operator-writable) — a parseable forgery
 * must not reach the caller or the POP cache. Payload *contents* are not
 * re-validated here; capture's allowlist is the write-side gate for those.
 */
function isValidCachedEnvelope(
  value: unknown,
  operation: Operation,
  range: HistoryRange,
  fromBucket: number,
  toBucket: number,
  expected: number,
): boolean {
  if (!isRecord(value)) return false;
  const { tier } = RANGES[range];
  if (value.operation !== operation || value.range !== range || value.tier !== tier) return false;
  if (value.period_seconds !== TIER_SPECS[tier].periodSeconds) return false;
  const coverage = value.coverage;
  if (
    !isRecord(coverage) ||
    coverage.from !== fromBucket ||
    coverage.to !== toBucket ||
    coverage.expected_points !== expected
  ) {
    return false;
  }
  const points = value.points;
  if (!Array.isArray(points) || points.length > expected) return false;
  if (coverage.present_points !== points.length) return false;
  let previous = fromBucket;
  for (const point of points) {
    if (!isRecord(point) || !isRecord(point.data)) return false;
    if (typeof point.normalization_version !== 'string') return false;
    const ts = asFinite(point.bucket_ts);
    if (ts === null || ts <= previous || ts > toBucket) return false;
    previous = ts;
  }
  return true;
}

/**
 * GET /api/history/status — capture health, derived from the data itself
 * (no bookkeeping table to drift). `stale: true` on a tier means the
 * newest bucket is older than two periods: capture is failing even though
 * the live endpoints may be perfectly healthy — and vice versa.
 */
async function historyStatus(db: D1Database, nowMs: number): Promise<Response> {
  let results: Record<string, unknown>[];
  try {
    ({ results } = await db
      .prepare(
        `SELECT tier, COUNT(*) AS rows, MIN(bucket_ts) AS first_bucket, MAX(bucket_ts) AS latest_bucket
         FROM snapshots GROUP BY tier`,
      )
      .all());
  } catch (err) {
    // Same failure shape as the range path: the endpoint documented as
    // "capture health" must name its own outage, not fall through to the
    // Worker's generic edge_unhandled 500 (panel MAJ, LAB-1935).
    console.error('history_status_failed', { err: String(err) });
    return json(500, { error: 'history_unavailable', detail: 'history store query failed' });
  }
  const nowSec = Math.floor(nowMs / 1000);
  const tiers: Record<string, unknown> = {};
  let startedAt: number | null = null;
  for (const tier of TIERS) {
    const row = results.find((r) => r.tier === tier);
    const first = row ? asFinite(row.first_bucket) : null;
    const latest = row ? asFinite(row.latest_bucket) : null;
    if (first !== null && (startedAt === null || first < startedAt)) startedAt = first;
    tiers[tier] = {
      rows: row ? (asFinite(row.rows) ?? 0) : 0,
      first_bucket_ts: first,
      latest_bucket_ts: latest,
      latest_age_seconds: latest === null ? null : nowSec - latest,
      stale: latest === null ? true : nowSec - latest > 2 * TIER_SPECS[tier].periodSeconds,
    };
  }
  return json(200, { history_started_at: startedAt, tiers });
}

/**
 * Handle a GET under /api/history/. Routing:
 * - /api/history/status                    → capture health
 * - /api/history/{operation}?range=7d|30d  → snapshot series
 *
 * Validation mirrors handler.ts: 4xx before any read. Points come back in
 * stable bucket_ts ascending order; a bucket with no row is simply absent
 * (the coverage block says how many were expected). Results are bounded by
 * construction: ≤168 hourly points for 7d, ≤30 daily points for 30d,
 * ranked lists at their stored top-20 depth.
 */
export async function handleHistoryApi(url: URL, deps: HistoryDeps): Promise<Response> {
  const segment = url.pathname.slice('/api/history/'.length);
  if (segment === 'status') return historyStatus(deps.db, deps.nowMs);

  if (!isOperation(segment)) {
    return json(404, {
      error: 'unknown_operation',
      detail: `Unknown operation ${JSON.stringify(segment)}`,
      operations: OPERATIONS,
    });
  }
  const range = url.searchParams.get('range');
  if (!isHistoryRange(range)) {
    return json(400, {
      error: 'invalid_range',
      detail: 'The range parameter is required and must be one of: 7d, 30d',
      ranges: Object.keys(RANGES),
    });
  }

  const { seconds, tier } = RANGES[range];
  const period = TIER_SPECS[tier].periodSeconds;
  const nowSec = Math.floor(deps.nowMs / 1000);
  const toBucket = Math.floor((nowSec - CAPTURE_GRACE_SECONDS) / period) * period;
  const fromBucket = toBucket - seconds;
  const expected = seconds / period;

  // CacheKit-backed response cache. The key embeds every dimension plus the
  // current bucket, so a new bucket is a new key and the TTL retires the
  // old one; within a bucket the whole planet shares one D1 computation.
  // Key cardinality is closed: 5 operations × 2 ranges × one bucket.
  const cacheKey = `${NAMESPACE}:history_response:${segment}:${range}:${toBucket}`;
  // 'cachekit' is not a member: the hit branch returns early with a literal
  // header, so this variable only ever names the two D1-computed paths.
  let source: 'd1' | 'd1-fallback' = 'd1';
  if (deps.backend) {
    try {
      const cached = await deps.backend.get(cacheKey);
      // The backend is operator-writable, so a cached response earns the
      // same distrust as any other backend read (the live path integrity-
      // checks; capture allowlists and caps). Anything empty, oversized,
      // unparseable, or parseable-but-not-THIS-response (envelope mismatch)
      // is a MISS: we recompute from D1 — the source of truth — and the
      // re-set below overwrites the bad value instead of relaying it into
      // the POP cache.
      if (cached && cached.length > 0 && cached.length <= MAX_RESPONSE_BYTES) {
        try {
          const parsed: unknown = JSON.parse(
            new TextDecoder('utf-8', { fatal: true }).decode(cached),
          );
          if (isValidCachedEnvelope(parsed, segment, range, fromBucket, toBucket, expected)) {
            // Copy pins the generic to Uint8Array<ArrayBuffer>, which BodyInit
            // accepts (backend.get returns Uint8Array<ArrayBufferLike>).
            return new Response(new Uint8Array(cached), {
              status: 200,
              headers: {
                'content-type': 'application/json; charset=utf-8',
                'x-history-source': 'cachekit',
              },
            });
          }
          console.error('history_cache_invalid', {
            key: cacheKey,
            bytes: cached.length,
            reason: 'envelope_mismatch',
          });
        } catch {
          console.error('history_cache_invalid', {
            key: cacheKey,
            bytes: cached.length,
            reason: 'unparseable',
          });
        }
      } else if (cached) {
        console.error('history_cache_invalid', {
          key: cacheKey,
          bytes: cached.length,
          reason: 'size',
        });
      }
    } catch (err) {
      source = 'd1-fallback';
      console.error('history_cache_read_failed', { key: cacheKey, err: String(err) });
    }
  }

  let rows: SnapshotRow[];
  let startedAt: number | null;
  try {
    const listed = await deps.db
      .prepare(
        `SELECT bucket_ts, generated_at, normalization_version, payload FROM snapshots
         WHERE operation = ? AND tier = ? AND bucket_ts > ? AND bucket_ts <= ?
         ORDER BY bucket_ts ASC LIMIT ?`,
      )
      .bind(segment, tier, fromBucket, toBucket, expected)
      .all();
    rows = [];
    for (const raw of listed.results) {
      const row = asSnapshotRow(raw);
      if (row) rows.push(row);
      else console.error('history_row_invalid', { operation: segment, row_keys: Object.keys(raw) });
    }
    startedAt = await firstBucket(deps.db);
  } catch (err) {
    console.error('history_query_failed', { operation: segment, range, err: String(err) });
    return json(500, { error: 'history_unavailable', detail: 'history store query failed' });
  }

  const versions = new Set<string>();
  const points = [];
  for (const row of rows) {
    let data: unknown;
    try {
      data = JSON.parse(row.payload);
    } catch {
      // A row we wrote but can't parse is corruption; skip it as a gap
      // rather than failing the whole series, and say so in the log.
      console.error('history_row_unparseable', { operation: segment, bucket_ts: row.bucket_ts });
      continue;
    }
    versions.add(row.normalization_version);
    points.push({
      bucket_ts: row.bucket_ts,
      generated_at: row.generated_at,
      normalization_version: row.normalization_version,
      data,
    });
  }

  const body = {
    operation: segment,
    range,
    tier,
    period_seconds: period,
    // Comparisons across differing versions are the caller's decision to
    // make knowingly — surfaced here and per point, never silently mixed.
    normalization_versions: [...versions].sort(),
    coverage: {
      from: fromBucket,
      to: toBucket,
      expected_points: expected,
      present_points: points.length,
      history_started_at: startedAt,
    },
    points,
  };
  const encoded = JSON.stringify(body, jsonSafe);
  const encodedBytes = new TextEncoder().encode(encoded);

  // TTL depends on completeness. A series whose newest expected bucket is
  // present is exact for its bucket and cached for the full period. An
  // incomplete one (late tick, dead capture) must not be frozen that long —
  // the missing row may land moments later — but it must still be cached
  // BRIEFLY: never caching it means the response cache is defeated in
  // exactly the state where every request pays full D1, and capture-down is
  // when that recompute load is self-sustaining (panel CRIT, LAB-1935).
  // The write also honours the read path's size cap: storing a response the
  // read side would reject poisons the key into a permanent reject-
  // recompute-rewrite loop, so an over-limit response stays uncached.
  const newestPresent = points.at(-1)?.bucket_ts === toBucket;
  if (deps.backend && source === 'd1') {
    if (encodedBytes.length <= MAX_RESPONSE_BYTES) {
      try {
        await deps.backend.set(
          cacheKey,
          encodedBytes,
          newestPresent ? period : INCOMPLETE_RESPONSE_TTL_SECONDS,
        );
      } catch (err) {
        console.error('history_cache_write_failed', { key: cacheKey, err: String(err) });
      }
    } else {
      console.error('history_cache_write_skipped', {
        key: cacheKey,
        bytes: encodedBytes.length,
        reason: 'response_too_large',
      });
    }
  }
  return new Response(encoded, {
    status: 200,
    headers: { 'content-type': 'application/json; charset=utf-8', 'x-history-source': source },
  });
}

/**
 * History start = the earliest bucket across tiers, one indexed seek per
 * tier, both issued concurrently.
 *
 * A bare `MIN(bucket_ts)` over the table has no index with bucket_ts
 * leading (the PK starts at operation, the secondary index at tier), so it
 * walks every row — linear in table size and billed per row read, which at
 * steady state (~6,200 rows) turns each uncached range request into ~37×
 * its documented read budget (panel CRIT, LAB-1935). Binding tier lets
 * SQLite satisfy MIN straight off the (tier, bucket_ts) index.
 *
 * Folding the two seeks into one `GROUP BY tier` was measured and rejected:
 * on the real migration at 6,200 rows it plans as `SCAN … USING COVERING
 * INDEX` (0.32 ms) against `SEARCH … (tier=?)` (0.016 ms) for the bound
 * form, because SQLite's MIN/MAX index optimisation does not apply through
 * GROUP BY. That would reintroduce the full-index walk the panel's CRIT
 * finding removed, trading a bounded 2-query fan-out for an unbounded scan.
 * The seek count is fixed at TIERS.length, so it cannot grow with the data.
 */
async function firstBucket(db: D1Database): Promise<number | null> {
  const mins = await Promise.all(
    TIERS.map(async (tier) => {
      const { results } = await db
        .prepare('SELECT MIN(bucket_ts) AS first FROM snapshots WHERE tier = ?')
        .bind(tier)
        .all();
      return asFinite(results[0]?.first);
    }),
  );
  const present = mins.filter((value): value is number => value !== null);
  return present.length === 0 ? null : Math.min(...present);
}
