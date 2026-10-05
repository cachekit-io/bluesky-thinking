/**
 * Cloudflare Worker entrypoint. Static dashboard requests are served from
 * Workers Assets (wrangler [assets], ./public) before this fetch handler
 * runs; only unmatched paths — the /api/* routes — reach it.
 *
 * The backend is a lazy per-isolate singleton (per the SDK's Workers
 * guidance: create once, reuse across requests).
 */
import { cachekitio, type Backend } from '@cachekit-io/cachekit';
import { handleApi, type HotpathBinding } from './handler.js';
import { captureTick, handleHistoryApi, type D1Database } from './history.js';

interface Env {
  CACHEKIT_API_KEY?: string;
  /** Backend endpoint. Required; backend routes 503 while it is unset. */
  CACHEKIT_API_URL?: string;
  /** Service binding to the Rust-WASM hot-path Worker (wrangler [[services]]). */
  HOTPATH?: HotpathBinding;
  /** D1 snapshot-history store (wrangler [[d1_databases]]). */
  HISTORY?: D1Database;
}

let backend: Backend | null = null;

/**
 * Name of the first unset backend secret, or null when both are set. Both are
 * required: with no endpoint the SDK would fall back to its default production
 * host and send this tenant's key there, so the Worker fails closed instead.
 */
function missingSecret(env: Env): string | null {
  if (!env.CACHEKIT_API_KEY) return 'CACHEKIT_API_KEY';
  if (!env.CACHEKIT_API_URL) return 'CACHEKIT_API_URL';
  return null;
}

/** Lazy per-isolate backend singleton; requires both backend secrets. */
function ensureBackend(env: Env): Backend {
  // Callers gate on missingSecret; this throw is a belt their try/catch wears.
  if (!env.CACHEKIT_API_KEY || !env.CACHEKIT_API_URL) {
    throw new Error(`${missingSecret(env)} secret is not set`);
  }
  return (backend ??= cachekitio({
    apiKey: env.CACHEKIT_API_KEY,
    // The demo's endpoint is outside the SDK's SSRF allowlist; the value is a
    // deploy-time secret, so opting out is an operator decision, not a
    // request-time one.
    apiUrl: env.CACHEKIT_API_URL,
    allowCustomHost: true,
  }));
}

/** Structural slice of ExecutionContext (repo doesn't use workers-types). */
interface Ctx {
  waitUntil(promise: Promise<unknown>): void;
}

/** Structural slice of the Workers-only caches.default (not in lib.dom). */
interface EdgeCache {
  match(key: string): Promise<Response | undefined>;
  put(key: string, response: Response): Promise<void>;
}

/** POP-cache TTLs for the miss-minting guard below. */
const EDGE_CACHE_TTL_SECONDS = { hit: 15, negative: 10 } as const;

export default {
  async fetch(request: Request, env: Env, ctx?: Ctx): Promise<Response> {
    const url = new URL(request.url);

    if (!url.pathname.startsWith('/api/')) {
      return Response.json({ error: 'not_found' }, { status: 404 });
    }
    if (request.method !== 'GET') {
      return Response.json(
        { error: 'method_not_allowed' },
        { status: 405, headers: { allow: 'GET' } },
      );
    }
    const isHistory = url.pathname.startsWith('/api/history/');
    // The backend key and endpoint are deploy-time secrets
    // (docs/architecture.md#credentials); until both exist, fail loudly
    // instead of throwing from the backend constructor. History is exempt: D1
    // is its source of truth and the CachekitIO layer is only its response
    // cache, so a missing secret degrades history to uncached D1 reads
    // instead of a 503.
    const missing = missingSecret(env);
    if (missing && !isHistory) {
      return Response.json(
        { error: 'not_configured', detail: `${missing} secret is not set` },
        { status: 503 },
      );
    }

    // Miss-minting guard: these
    // URLs are public and the backend bills misses, so an unauthenticated
    // client must not be able to reach CachekitIO at will. Front every
    // aggregate read with the POP cache, 404s included (negative caching) —
    // repeat requests cost a Cloudflare cache hit, not a billable miss.
    // /api/stats stays uncached: it's per-isolate module state, no backend
    // call to protect, and caching it would blind the dashboard's counters.
    // Scope note: caches.default is per-POP, so this bounds minting to one
    // backend read per URL per POP per TTL rather than eliminating it.
    // absent under vitest / the node demo script
    const edgeCache = (globalThis as { caches?: { default?: EdgeCache } }).caches?.default;
    const cacheable = edgeCache !== undefined && url.pathname !== '/api/stats';
    if (cacheable) {
      const cached = await edgeCache.match(request.url);
      if (cached) return cached;
    }

    // handleApi maps its own failure modes to 502/500 responses; this outer
    // catch exists for what it can't — a throwing backend constructor or an
    // unforeseen bug — so the caller gets a JSON 500 instead of a Workers
    // 1101 (Kody review, PR #9).
    let response: Response;
    try {
      if (isHistory) {
        response = env.HISTORY
          ? await handleHistoryApi(url, {
              db: env.HISTORY,
              backend: missing ? null : ensureBackend(env),
              nowMs: Date.now(),
            })
          : Response.json(
              { error: 'not_configured', detail: 'HISTORY D1 binding is not set' },
              { status: 503 },
            );
      } else {
        response = await handleApi(url, ensureBackend(env), env.HOTPATH);
      }
    } catch (err) {
      console.error('edge_unhandled', { path: url.pathname, err: String(err) });
      return Response.json(
        { error: 'internal', detail: 'unhandled edge failure' },
        { status: 500 },
      );
    }

    if (cacheable && (response.status === 200 || response.status === 404)) {
      const ttl =
        response.status === 200 ? EDGE_CACHE_TTL_SECONDS.hit : EDGE_CACHE_TTL_SECONDS.negative;
      const copy = new Response(response.body, response); // mutable headers
      copy.headers.set('cache-control', `public, s-maxage=${ttl}`);
      const store = edgeCache.put(request.url, copy.clone());
      if (ctx) ctx.waitUntil(store);
      else await store;
      return copy;
    }
    return response;
  },

  /**
   * History capture, the cron's only job since the ingester left Render: the
   * keep-alive ping existed solely for Render's free-tier inbound-idle
   * spin-down, and the ingester now runs on self-hosted Kubernetes, which has
   * no such semantics (its restarts come from the Deployment's liveness
   * probe). The schedule is hourly (wrangler [triggers]) because captureTick
   * no-ops off minute 0 anyway — same set of effective fires as the old
   * every-10-minutes keep-alive schedule, minus the five no-ops an hour.
   */
  async scheduled(controller: unknown, env: Env): Promise<void> {
    if (!env.HISTORY || missingSecret(env)) {
      console.log('history: HISTORY binding or a backend secret not set, skipping capture');
      return;
    }
    try {
      const scheduledTime = (controller as { scheduledTime?: number } | null)?.scheduledTime;
      const report = await captureTick(
        ensureBackend(env),
        env.HISTORY,
        scheduledTime ?? Date.now(),
      );
      if (report.boundary !== 'none') console.log('history_capture', report);
    } catch (err) {
      console.error('history_capture_failed', { err: String(err) });
    }
  },
};
