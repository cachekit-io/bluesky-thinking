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

export interface TierSpec {
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

/**
 * Range queries never include the bucket whose capture may still be in
 * flight: a request seconds after the boundary must not see a series
 * missing a point that lands moments later. Belt: responses are only
 * CacheKit-cached when the newest expected bucket is actually present.
 */
const CAPTURE_GRACE_SECONDS = 120;

/** Hard cap on a stored row's payload; a hostile aggregate becomes a gap. */
const MAX_PAYLOAD_BYTES = 32_768;

/**
 * Hard cap on a cached response we are willing to relay. The backend is
 * operator-writable, so cached bytes get the same distrust as any other
 * backend read — an oversized or unparseable value is treated as a miss
 * and recomputed from D1 (which also heals the poisoned key on re-set).
 */
const MAX_RESPONSE_BYTES = 4 * 1024 * 1024;

export const RANGES: Record<'7d' | '30d', { seconds: number; tier: Tier }> = {
  '7d': { seconds: 7 * 86400, tier: 'hourly' },
  '30d': { seconds: 30 * 86400, tier: 'daily' },
};
export type HistoryRange = keyof typeof RANGES;

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

export interface CaptureCounts {
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
 * Allowlist + trim one live aggregate into its history payload. Returns
 * null when the operation's own required shape is absent — a malformed or
 * hostile cache entry becomes a counted gap, never a stored row. Only keys
 * named here can ever reach storage.
 */
export function trimPayload(
  operation: Operation,
  value: Record<string, unknown>,
): Record<string, unknown> | null {
  const out: Record<string, unknown> = {};
  for (const key of [
    'window',
    'total_posts',
    'total_events_considered',
    'total_signal_candidates',
  ]) {
    if (key in value) out[key] = value[key];
  }
  if (isRecord(value.excluded_count_by_reason)) {
    out.excluded_count_by_reason = value.excluded_count_by_reason;
  }
  const top = (list: unknown): unknown[] | null =>
    Array.isArray(list) ? list.slice(0, STORED_TOP_N) : null;

  switch (operation) {
    case 'trending_hashtags': {
      const hashtags = top(value.hashtags);
      if (!hashtags) return null;
      out.hashtags = hashtags;
      return out;
    }
    case 'trending_links': {
      const links = top(value.links);
      const domains = top(value.domains);
      if (!links || !domains) return null;
      out.links = links;
      out.domains = domains;
      return out;
    }
    case 'lang_mix': {
      if (!isRecord(value.langs)) return null;
      out.langs = value.langs;
      return out;
    }
    case 'posts_per_minute': {
      const ppm = asFinite(value.ppm);
      if (ppm === null) return null;
      out.ppm = ppm;
      return out;
    }
    case 'top_emoji': {
      const emoji = top(value.emoji);
      if (!emoji) return null;
      out.emoji = emoji;
      return out;
    }
  }
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
      if (payload.length > MAX_PAYLOAD_BYTES) {
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
 * GET /api/history/status — capture health, derived from the data itself
 * (no bookkeeping table to drift). `stale: true` on a tier means the
 * newest bucket is older than two periods: capture is failing even though
 * the live endpoints may be perfectly healthy — and vice versa.
 */
async function historyStatus(db: D1Database, nowMs: number): Promise<Response> {
  const { results } = await db
    .prepare(
      `SELECT tier, COUNT(*) AS rows, MIN(bucket_ts) AS first_bucket, MAX(bucket_ts) AS latest_bucket
       FROM snapshots GROUP BY tier`,
    )
    .all();
  const nowSec = Math.floor(nowMs / 1000);
  const tiers: Record<string, unknown> = {};
  let startedAt: number | null = null;
  for (const tier of Object.keys(TIER_SPECS) as Tier[]) {
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
  let source: 'cachekit' | 'd1' | 'd1-fallback' = 'd1';
  if (deps.backend) {
    try {
      const cached = await deps.backend.get(cacheKey);
      // The backend is operator-writable, so a cached response earns the
      // same distrust as any other backend read (the live path integrity-
      // checks; capture allowlists and caps). Anything empty, oversized, or
      // unparseable is a MISS: we recompute from D1 — the source of truth —
      // and the re-set below overwrites the bad value instead of relaying
      // it into the POP cache.
      if (cached && cached.length > 0 && cached.length <= MAX_RESPONSE_BYTES) {
        try {
          JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(cached));
          // Copy pins the generic to Uint8Array<ArrayBuffer>, which BodyInit
          // accepts (backend.get returns Uint8Array<ArrayBufferLike>).
          return new Response(new Uint8Array(cached), {
            status: 200,
            headers: {
              'content-type': 'application/json; charset=utf-8',
              'x-history-source': 'cachekit',
            },
          });
        } catch {
          console.error('history_cache_invalid', { key: cacheKey, bytes: cached.length });
        }
      } else if (cached) {
        console.error('history_cache_invalid', { key: cacheKey, bytes: cached.length });
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
    rows = listed.results as unknown as SnapshotRow[];
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

  // Cache only a series whose newest expected bucket is present. If the
  // capture for toBucket hasn't landed yet (late cron tick, slow backend),
  // caching now would freeze the incomplete series for a full period —
  // hourly for 7d, a whole DAY for 30d — even though the row arrives
  // moments later. An incomplete series still serves; it just stays
  // recomputed (bounded by the 15 s POP cache) until the point exists.
  const newestPresent = points.length > 0 && points[points.length - 1]?.bucket_ts === toBucket;
  if (deps.backend && source === 'd1' && newestPresent) {
    try {
      await deps.backend.set(cacheKey, new TextEncoder().encode(encoded), period);
    } catch (err) {
      console.error('history_cache_write_failed', { key: cacheKey, err: String(err) });
    }
  }
  return new Response(encoded, {
    status: 200,
    headers: { 'content-type': 'application/json; charset=utf-8', 'x-history-source': source },
  });
}

async function firstBucket(db: D1Database): Promise<number | null> {
  const { results } = await db.prepare('SELECT MIN(bucket_ts) AS first FROM snapshots').all();
  return asFinite(results[0]?.first);
}
