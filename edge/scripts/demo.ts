/**
 * Credential-free local demo: serves the dashboard plus the real API handler
 * against an in-memory backend seeded with plausible aggregates (5m window
 * only — other windows render the MISS path). Verifies AC "dashboard renders
 * from the API responses in a plain browser" without network or
 * CACHEKIT_API_KEY.
 *
 *   npm run demo   →  http://localhost:8788
 */
import { createServer } from 'node:http';
import { readFileSync } from 'node:fs';
import { encodeInteropValue, generateInteropKey, type Backend } from '@cachekit-io/cachekit';
// Real .ts extension: this file runs under plain `node` (type stripping),
// which does not rewrite bundler-style .js specifiers.
import { handleApi, NAMESPACE } from '../src/handler.ts';

const computed_at = new Date().toISOString();
const seed: Record<string, unknown> = {
  trending_hashtags: {
    computed_at,
    counts: { cachekit: 412, bluesky: 300, rustlang: 187, typescript: 121, ai: 98, webdev: 55 },
  },
  trending_links: {
    computed_at,
    counts: { 'github.com': 231, 'youtube.com': 144, 'cachekit.io': 89, 'arxiv.org': 34 },
  },
  lang_mix: {
    computed_at,
    share: { en: 0.62, ja: 0.14, pt: 0.09, de: 0.06, es: 0.05, other: 0.04 },
  },
  posts_per_minute: { computed_at, value: 4211 },
  top_emoji: { computed_at, counts: { '😂': 902, '❤️': 671, '🔥': 402, '🦋': 217, '👀': 133 } },
};

const store = new Map(
  Object.entries(seed).map(([op, value]) => [
    generateInteropKey(NAMESPACE, op, ['5m']),
    encodeInteropValue(value),
  ]),
);

const backend: Backend = {
  get: async (key) => store.get(key) ?? null,
  set: async () => undefined,
  delete: async () => false,
  exists: async () => false,
  close: async () => undefined,
};

const html = readFileSync(new URL('../public/index.html', import.meta.url));

createServer(async (req, res) => {
  const url = new URL(req.url ?? '/', 'http://localhost');
  if (!url.pathname.startsWith('/api/')) {
    res.writeHead(200, { 'content-type': 'text/html; charset=utf-8' });
    res.end(html);
    return;
  }
  const response = await handleApi(url, backend);
  res.writeHead(response.status, Object.fromEntries(response.headers));
  res.end(Buffer.from(await response.arrayBuffer()));
}).listen(8788, () => console.log('Skyline demo: http://localhost:8788'));
