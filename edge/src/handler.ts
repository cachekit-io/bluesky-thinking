/**
 * Skyline edge API — read-only interop/v1 consumer.
 *
 * Serves the five analytics aggregates written by the Python ingester from
 * the shared CachekitIO namespace. The edge never computes on miss: a miss
 * means the ingester hasn't written (or the TTL expired), and the honest
 * answer is 404 + X-Cache: MISS, not fabricated data.
 *
 * Contract: docs/architecture.md (locked, Stage 1). Keys are interop/v1 —
 * `bluesky-thinking:{operation}:{blake2b256(msgpack([window]))}` — byte-
 * identical across the Python, Rust and TS SDKs. The window argument is
 * always explicit (interop binding rules: no default parameters).
 */
import {
  generateInteropKey,
  decodeInteropValue,
  BackendError,
  TimeoutError,
  SerializationError,
  ValueTooLargeError,
  type Backend,
} from '@cachekit-io/cachekit';

export const NAMESPACE = 'bluesky-thinking';

export const OPERATIONS = [
  'trending_hashtags',
  'trending_links',
  'lang_mix',
  'posts_per_minute',
  'top_emoji',
] as const;
export type Operation = (typeof OPERATIONS)[number];

export const WINDOWS = ['5m', '1h', '24h'] as const;
export type Window = (typeof WINDOWS)[number];

/**
 * Per-isolate hit/miss counters — the raw material for the epic's HIT proof
 * (AC-1) and hit-rate metric (AC-4). Module state resets when Cloudflare
 * recycles the isolate; good enough for a live demo, exposed at /api/stats.
 */
export interface Stats {
  hits: number;
  misses: number;
  errors: number;
}
const stats: Stats = { hits: 0, misses: 0, errors: 0 };

/** Test hook: reset per-isolate counters. */
export function resetStats(): void {
  stats.hits = 0;
  stats.misses = 0;
  stats.errors = 0;
}

// Values a Python writer may exceed 2^53 on decode to BigInt; JSON.stringify
// rejects BigInt, so stringify those (safe-range ints are already number).
function jsonSafe(_key: string, value: unknown): unknown {
  return typeof value === 'bigint' ? value.toString() : value;
}

function json(status: number, body: unknown, headers: Record<string, string> = {}): Response {
  return new Response(JSON.stringify(body, jsonSafe), {
    status,
    headers: { 'content-type': 'application/json; charset=utf-8', ...headers },
  });
}

function isOperation(value: string): value is Operation {
  return (OPERATIONS as readonly string[]).includes(value);
}

function isWindow(value: string | null): value is Window {
  return value !== null && (WINDOWS as readonly string[]).includes(value);
}

/**
 * Handle a GET under /api/. Routing:
 * - /api/stats                     → per-isolate hit/miss counters
 * - /api/{operation}?window=5m|1h|24h → cached aggregate (interop/v1 read)
 *
 * Unknown operation → 404, missing/invalid window → 400 — both before any
 * cache read. Backend failures → 502, undecodable entries → 500; errors are
 * surfaced, never masked with fake data.
 */
export async function handleApi(url: URL, backend: Backend): Promise<Response> {
  const segment = url.pathname.slice('/api/'.length);

  if (segment === 'stats') {
    const reads = stats.hits + stats.misses;
    return json(200, {
      hits: stats.hits,
      misses: stats.misses,
      errors: stats.errors,
      hit_rate: reads === 0 ? null : stats.hits / reads,
    });
  }

  if (!isOperation(segment)) {
    return json(404, {
      error: 'unknown_operation',
      detail: `Unknown operation ${JSON.stringify(segment)}`,
      operations: OPERATIONS,
    });
  }

  const window = url.searchParams.get('window');
  if (!isWindow(window)) {
    return json(400, {
      error: 'invalid_window',
      detail: `The window parameter is required and must be one of: ${WINDOWS.join(', ')}`,
      windows: WINDOWS,
    });
  }

  const key = generateInteropKey(NAMESPACE, segment, [window]);

  let raw: Uint8Array | null;
  try {
    raw = await backend.get(key);
  } catch (error) {
    stats.errors += 1;
    const detail = error instanceof Error ? error.message : 'Unknown backend error';
    const status = error instanceof BackendError || error instanceof TimeoutError ? 502 : 500;
    return json(status, { error: 'backend_error', detail });
  }

  if (raw === null) {
    stats.misses += 1;
    return json(
      404,
      {
        error: 'miss',
        detail: `No cached ${segment} aggregate for window ${window} yet — the ingester has not written it or the entry expired`,
        operation: segment,
        window,
      },
      { 'x-cache': 'MISS' },
    );
  }

  let data: unknown;
  try {
    data = decodeInteropValue(raw);
  } catch (error) {
    stats.errors += 1;
    const detail =
      error instanceof SerializationError || error instanceof ValueTooLargeError
        ? error.message
        : 'Unknown decode error';
    return json(500, { error: 'decode_error', detail, key });
  }

  stats.hits += 1;
  return json(200, { operation: segment, window, data }, { 'x-cache': 'HIT' });
}
