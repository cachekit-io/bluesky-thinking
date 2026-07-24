/**
 * Cloudflare Worker entrypoint. Static dashboard requests are served from
 * Workers Assets (wrangler [assets], ./public) before this fetch handler
 * runs; only unmatched paths — the /api/* routes — reach it.
 *
 * The backend is a lazy per-isolate singleton (per the SDK's Workers
 * guidance: create once, reuse across requests).
 */
import { cachekitio, type Backend } from '@cachekit-io/cachekit';
import { handleApi } from './handler.js';

interface Env {
  CACHEKIT_API_KEY?: string;
  /** Override for tests/staging; defaults to https://api.cachekit.io. */
  CACHEKIT_API_URL?: string;
}

let backend: Backend | null = null;

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
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
    // Live credentials are provisioned in Stage 3 (docs/architecture.md
    // runbook); until the secret exists, fail loudly instead of throwing
    // from the backend constructor.
    if (!env.CACHEKIT_API_KEY) {
      return Response.json(
        { error: 'not_configured', detail: 'CACHEKIT_API_KEY secret is not set' },
        { status: 503 },
      );
    }

    backend ??= cachekitio({
      apiKey: env.CACHEKIT_API_KEY,
      ...(env.CACHEKIT_API_URL ? { apiUrl: env.CACHEKIT_API_URL } : {}),
    });

    return handleApi(url, backend);
  },
};
