/**
 * Credential-free local demo: serves the real static dashboard assets plus the
 * real API handler against an in-memory backend seeded with current ingester
 * payloads. It returns 404 for unknown assets so a missing dashboard module is
 * visible during local verification.
 *
 *   npm run demo   →  http://localhost:8788
 */
import { createServer } from 'node:http';
import { readFileSync } from 'node:fs';
import { encodeInteropValue, generateInteropKey, type Backend } from '@cachekit-io/cachekit';
import { handleApi, NAMESPACE } from '../src/handler.ts';

const generated_at = Math.floor(Date.now() / 1000);
const base = { window: '5m', generated_at, total_posts: 1024 };
const seed: Record<string, unknown> = {
  trending_hashtags: {
    ...base,
    hashtags: [
      { tag: 'cachekit', count: 412 },
      { tag: 'bluesky', count: 300 },
      { tag: 'rustlang', count: 187 },
    ],
  },
  trending_links: {
    ...base,
    links: [
      { uri: 'https://github.com/cachekit-io', count: 231 },
      { uri: 'https://cachekit.io', count: 89 },
    ],
  },
  lang_mix: { ...base, langs: { en: 0.62, ja: 0.14, pt: 0.09, other: 0.15 } },
  posts_per_minute: { ...base, ppm: 204.8 },
  top_emoji: {
    ...base,
    emoji: [
      { emoji: '😂', count: 902 },
      { emoji: '❤️', count: 671 },
      { emoji: '🔥', count: 402 },
    ],
  },
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

const assets = new Map([
  [
    '/',
    {
      body: readFileSync(new URL('../public/index.html', import.meta.url)),
      type: 'text/html; charset=utf-8',
    },
  ],
  [
    '/dashboard.js',
    {
      body: readFileSync(new URL('../public/dashboard.js', import.meta.url)),
      type: 'text/javascript; charset=utf-8',
    },
  ],
]);

createServer(async (req, res) => {
  const url = new URL(req.url ?? '/', 'http://localhost');
  if (url.pathname.startsWith('/api/')) {
    const response = await handleApi(url, backend);
    res.writeHead(response.status, Object.fromEntries(response.headers));
    res.end(Buffer.from(await response.arrayBuffer()));
    return;
  }
  const asset = assets.get(url.pathname);
  if (!asset) {
    res.writeHead(404, { 'content-type': 'text/plain; charset=utf-8' });
    res.end('Not found');
    return;
  }
  res.writeHead(200, { 'content-type': asset.type });
  res.end(asset.body);
}).listen(8788, () => console.log('Skyline demo: http://localhost:8788'));
