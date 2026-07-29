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

/**
 * The Rust-WASM hot-path Worker, reached via a Cloudflare service binding
 * (wrangler [[services]]) — never a public URL. Structural type: the repo
 * doesn't depend on @cloudflare/workers-types, and a binding is just an
 * object with fetch().
 */
export interface HotpathBinding {
  fetch(input: string, init?: RequestInit): Promise<Response>;
}

type HotpathVerdict =
  | { state: 'verified'; xxh3: string }
  | { state: 'invalid'; xxh3: string; detail: string }
  | { state: 'unavailable'; detail: string };

/**
 * Integrity-check a fetched payload on the Rust-WASM hot path
 * (POST /v1/verify: xxHash3-64 + strict interop/v1 decode). Any transport or
 * hot-path failure degrades to 'unavailable' — the caller decides what that
 * means; this function never throws.
 */
async function verifyViaHotpath(hotpath: HotpathBinding, raw: Uint8Array): Promise<HotpathVerdict> {
  try {
    // The URL host is ignored by service bindings; only the path routes.
    // Copy pins the generic to Uint8Array<ArrayBuffer>, which BodyInit
    // accepts (backend.get returns Uint8Array<ArrayBufferLike>).
    const res = await hotpath.fetch('https://skyline-hotpath/v1/verify', {
      method: 'POST',
      body: new Uint8Array(raw),
    });
    if (!res.ok) {
      return { state: 'unavailable', detail: `hot path returned HTTP ${res.status}` };
    }
    const report = (await res.json()) as {
      xxh3_64?: string;
      valid_interop_value?: boolean;
      interop_error?: string | null;
    };
    if (typeof report.xxh3_64 !== 'string') {
      return { state: 'unavailable', detail: 'hot path returned no checksum' };
    }
    if (!report.valid_interop_value) {
      return {
        state: 'invalid',
        xxh3: report.xxh3_64,
        detail: report.interop_error ?? 'payload is not a valid interop/v1 document',
      };
    }
    return { state: 'verified', xxh3: report.xxh3_64 };
  } catch (error) {
    return {
      state: 'unavailable',
      detail: error instanceof Error ? error.message : 'hot path fetch failed',
    };
  }
}

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
export async function handleApi(
  url: URL,
  backend: Backend,
  hotpath?: HotpathBinding,
): Promise<Response> {
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

  // Stage 3 (AC-3): every payload served through the edge is integrity-checked
  // on the Rust-WASM hot path first. 'invalid' is a hard stop — a corrupt entry
  // must never be served; 'unavailable' degrades honestly: the aggregate is
  // real (it came from the backend), it just goes out unverified and says so.
  // A missing binding is a deploy-config fault, not a reason to go silent:
  // it degrades exactly like an unreachable hot path, header and all.
  const verdict: HotpathVerdict = hotpath
    ? await verifyViaHotpath(hotpath, raw)
    : { state: 'unavailable', detail: 'HOTPATH service binding is not configured' };
  console.log(
    `hotpath verify ${segment}/${window}: ${verdict.state}` +
      (verdict.state === 'unavailable' ? ` (${verdict.detail})` : ` xxh3=${verdict.xxh3}`),
  );
  if (verdict.state === 'invalid') {
    stats.errors += 1;
    return json(500, {
      error: 'integrity_check_failed',
      detail: verdict.detail,
      key,
      xxh3_64: verdict.xxh3,
    });
  }
  const hotpathHeaders: Record<string, string> = { 'x-hotpath': verdict.state };
  if (verdict.state === 'verified') {
    hotpathHeaders['x-hotpath-xxh3'] = verdict.xxh3;
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
  return json(200, { operation: segment, window, data }, { 'x-cache': 'HIT', ...hotpathHeaders });
}
