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
  /** Override for the dev instance / tests; defaults to https://api.cachekit.io. */
  CACHEKIT_API_URL?: string;
  /** Service binding to the Rust-WASM hot-path Worker (wrangler [[services]]). */
  HOTPATH?: HotpathBinding;
  /** D1 snapshot-history store (wrangler [[d1_databases]], LAB-1616). */
  HISTORY?: D1Database;
}

let backend: Backend | null = null;

/** Lazy per-isolate backend singleton; requires env.CACHEKIT_API_KEY. */
function ensureBackend(env: Env): Backend {
  // Callers gate on the key; this throw is a belt their try/catch wears.
  if (!env.CACHEKIT_API_KEY) throw new Error('CACHEKIT_API_KEY secret is not set');
  return (backend ??= cachekitio({
    apiKey: env.CACHEKIT_API_KEY,
    // A non-default apiUrl (the dev instance) is outside the SDK's SSRF
    // allowlist; the value comes from wrangler config, so opting out is
    // an operator decision, not a request-time one.
    ...(env.CACHEKIT_API_URL ? { apiUrl: env.CACHEKIT_API_URL, allowCustomHost: true } : {}),
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
    // Live credentials are provisioned in Stage 3 (docs/architecture.md
    // runbook); until the secret exists, fail loudly instead of throwing
    // from the backend constructor. History is exempt: D1 is its source of
    // truth and the CachekitIO layer is only its response cache, so a
    // missing key degrades history to uncached D1 reads instead of a 503.
    if (!env.CACHEKIT_API_KEY && !isHistory) {
      return Response.json(
        { error: 'not_configured', detail: 'CACHEKIT_API_KEY secret is not set' },
        { status: 503 },
      );
    }

    // Miss-minting guard (Stage-3 panel finding, closed in LAB-738): these
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
              backend: env.CACHEKIT_API_KEY ? ensureBackend(env) : null,
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
   * History capture (LAB-1616), the cron's only job since LAB-2383: the
   * keep-alive ping existed solely for Render's free-tier inbound-idle
   * spin-down, and the ingester now runs on the lab k3s cluster, which has
   * no such semantics (its restarts come from the Deployment's liveness
   * probe). The schedule is hourly (wrangler [triggers]) because captureTick
   * no-ops off minute 0 anyway — same set of effective fires as the old
   * every-10-minutes keep-alive schedule, minus the five no-ops an hour.
   */
  async scheduled(controller: unknown, env: Env): Promise<void> {
    if (!env.HISTORY || !env.CACHEKIT_API_KEY) {
      console.log('history: HISTORY binding or CACHEKIT_API_KEY not set, skipping capture');
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
