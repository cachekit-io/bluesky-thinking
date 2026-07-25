/**
 * Handler tests against a mocked Backend — no network, no CACHEKIT_API_KEY
 * (Stage 2 AC-1; live integration is Stage 3). Payloads are produced with
 * the SDK's own interop value codec, so what the mock serves is what the
 * Python ingester writes.
 */
import { beforeEach, describe, expect, it } from 'vitest';
import {
  encodeInteropValue,
  generateInteropKey,
  BackendError,
  type Backend,
} from '@cachekit-io/cachekit';
import { handleApi, resetStats, NAMESPACE, OPERATIONS } from '../src/handler.js';

function mockBackend(get: Backend['get']): Backend {
  return {
    get,
    set: async () => undefined,
    delete: async () => false,
    exists: async () => false,
    close: async () => undefined,
  };
}

function storeOf(entries: Record<string, unknown>): Backend {
  const bytes = new Map(Object.entries(entries).map(([k, v]) => [k, encodeInteropValue(v)]));
  return mockBackend(async (key) => bytes.get(key) ?? null);
}

const api = (path: string) => new URL(`https://skyline.example${path}`);

beforeEach(resetStats);

describe('GET /api/{operation}', () => {
  it('serves a cached aggregate with X-Cache: HIT', async () => {
    const aggregate = { computed_at: '2026-07-24T09:00:00Z', tags: { cachekit: 42, bluesky: 7 } };
    const backend = storeOf({
      [generateInteropKey(NAMESPACE, 'trending_hashtags', ['5m'])]: aggregate,
    });

    const res = await handleApi(api('/api/trending_hashtags?window=5m'), backend);

    expect(res.status).toBe(200);
    expect(res.headers.get('x-cache')).toBe('HIT');
    expect(await res.json()).toEqual({
      operation: 'trending_hashtags',
      window: '5m',
      data: aggregate,
    });
  });

  it.each(OPERATIONS)('reads %s at its locked interop key', async (operation) => {
    const seen: string[] = [];
    const backend = mockBackend(async (key) => {
      seen.push(key);
      return null;
    });

    await handleApi(api(`/api/${operation}?window=1h`), backend);

    expect(seen).toEqual([generateInteropKey(NAMESPACE, operation, ['1h'])]);
  });

  it('returns 404 with X-Cache: MISS when the entry does not exist', async () => {
    const res = await handleApi(
      api('/api/top_emoji?window=24h'),
      mockBackend(async () => null),
    );

    expect(res.status).toBe(404);
    expect(res.headers.get('x-cache')).toBe('MISS');
    const body = (await res.json()) as { error: string };
    expect(body.error).toBe('miss');
  });

  it('serializes >2^53 integers from the ingester instead of throwing', async () => {
    const backend = storeOf({
      [generateInteropKey(NAMESPACE, 'posts_per_minute', ['5m'])]: { total: 2n ** 60n },
    });

    const res = await handleApi(api('/api/posts_per_minute?window=5m'), backend);

    expect(res.status).toBe(200);
    expect(await res.json()).toMatchObject({ data: { total: (2n ** 60n).toString() } });
  });
});

describe('input validation (4xx before any cache read)', () => {
  const explodingBackend = mockBackend(async () => {
    throw new Error('cache read attempted for invalid input');
  });

  it('unknown operation → 404, no cache read', async () => {
    const res = await handleApi(api('/api/sentiment?window=5m'), explodingBackend);
    expect(res.status).toBe(404);
    expect(((await res.json()) as { error: string }).error).toBe('unknown_operation');
  });

  it('missing window → 400, no cache read', async () => {
    const res = await handleApi(api('/api/lang_mix'), explodingBackend);
    expect(res.status).toBe(400);
    expect(((await res.json()) as { error: string }).error).toBe('invalid_window');
  });

  it('invalid window → 400, no cache read', async () => {
    const res = await handleApi(api('/api/lang_mix?window=7d'), explodingBackend);
    expect(res.status).toBe(400);
    expect(((await res.json()) as { error: string }).error).toBe('invalid_window');
  });
});

describe('failures surface as 5xx (no fake data)', () => {
  it('backend failure → 502 with the error detail', async () => {
    const backend = mockBackend(async () => {
      throw new BackendError('CachekitIO get failed: HTTP 500');
    });

    const res = await handleApi(api('/api/trending_links?window=5m'), backend);

    expect(res.status).toBe(502);
    expect(await res.json()).toMatchObject({
      error: 'backend_error',
      detail: 'CachekitIO get failed: HTTP 500',
    });
  });

  it('undecodable entry → 500 decode_error', async () => {
    // 0xc1 is the one permanently-unused msgpack byte — guaranteed invalid.
    const backend = mockBackend(async () => Uint8Array.of(0xc1));

    const res = await handleApi(api('/api/lang_mix?window=1h'), backend);

    expect(res.status).toBe(500);
    expect(((await res.json()) as { error: string }).error).toBe('decode_error');
  });
});

describe('GET /api/stats', () => {
  it('tracks hits, misses and errors across requests', async () => {
    const hitKey = generateInteropKey(NAMESPACE, 'lang_mix', ['5m']);
    const backend = mockBackend(async (key) => (key === hitKey ? encodeInteropValue({}) : null));

    await handleApi(api('/api/lang_mix?window=5m'), backend); // hit
    await handleApi(api('/api/lang_mix?window=1h'), backend); // miss
    await handleApi(api('/api/lang_mix?window=24h'), backend); // miss
    await handleApi(
      api('/api/top_emoji?window=5m'),
      mockBackend(async () => {
        throw new BackendError('boom');
      }),
    ); // error
    await handleApi(api('/api/nope?window=5m'), backend); // 404, not counted

    const res = await handleApi(api('/api/stats'), backend);

    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ hits: 1, misses: 2, errors: 1, hit_rate: 1 / 3 });
  });

  it('reports hit_rate null before any reads', async () => {
    const res = await handleApi(
      api('/api/stats'),
      mockBackend(async () => null),
    );
    expect(await res.json()).toEqual({ hits: 0, misses: 0, errors: 0, hit_rate: null });
  });
});
