/**
 * Scheduled-handler containment: the cron's only job is
 * history capture, and a capture failure must die inside the handler — a
 * scheduled() rejection would surface as a Worker error on every boundary
 * fire. Global fetch is stubbed — no network, no creds, same as every other
 * test.
 */
import { afterEach, describe, expect, it, vi } from 'vitest';
import worker from '../src/worker.js';
import type { D1Database } from '../src/history.js';

const explodingDb: D1Database = {
  prepare() {
    throw new Error('D1 is down');
  },
};

// Deliberately NOT credential-shaped (no ck_ prefix): the SDK only checks
// truthiness, global fetch is stubbed, and a real-looking literal would trip
// secret scanners for no test value.
const TEST_API_KEY = 'unit-test-api-key';
// RFC 2606 reserved name; global fetch is stubbed, so nothing resolves it.
const TEST_API_URL = 'https://cachekit.example.com';

function stubFetch(status = 200): ReturnType<typeof vi.fn> {
  const mock = vi.fn(async () => new Response('ok', { status }));
  vi.stubGlobal('fetch', mock);
  return mock;
}

afterEach(() => vi.unstubAllGlobals());

describe('scheduled: history capture is contained', () => {
  it('resolves even when the history store and backend both fail', async () => {
    stubFetch();
    await expect(
      worker.scheduled(
        { scheduledTime: Date.UTC(2026, 7, 14, 14, 0, 0) },
        {
          CACHEKIT_API_KEY: TEST_API_KEY,
          CACHEKIT_API_URL: TEST_API_URL,
          HISTORY: explodingDb,
        },
      ),
    ).resolves.toBeUndefined();
  });

  it('skips capture entirely when the history store is unconfigured', async () => {
    const fetchMock = stubFetch();
    await expect(
      worker.scheduled({ scheduledTime: Date.UTC(2026, 7, 14, 14, 0, 0) }, {}),
    ).resolves.toBeUndefined();
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it('no-ops off the hour boundary without any backend read', async () => {
    const fetchMock = stubFetch();
    await worker.scheduled(
      { scheduledTime: Date.UTC(2026, 7, 14, 14, 10, 0) },
      {
        CACHEKIT_API_KEY: TEST_API_KEY,
        CACHEKIT_API_URL: TEST_API_URL,
        HISTORY: explodingDb,
      },
    );
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it('skips capture without an endpoint instead of using the SDK default host', async () => {
    const fetchMock = stubFetch();
    await worker.scheduled(
      { scheduledTime: Date.UTC(2026, 7, 14, 14, 0, 0) },
      { CACHEKIT_API_KEY: TEST_API_KEY, HISTORY: explodingDb },
    );
    expect(fetchMock).not.toHaveBeenCalled();
  });
});

describe('fetch: backend routes fail closed', () => {
  it('503s naming CACHEKIT_API_URL when only the key is set, with no backend call', async () => {
    const fetchMock = stubFetch();
    const response = await worker.fetch(
      new Request('https://edge.test/api/posts_per_minute?window=5m'),
      { CACHEKIT_API_KEY: TEST_API_KEY },
    );
    expect(response.status).toBe(503);
    expect(await response.json()).toEqual({
      error: 'not_configured',
      detail: 'CACHEKIT_API_URL secret is not set',
    });
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it('serves history from D1 when only the key is set, with no backend call', async () => {
    const fetchMock = stubFetch();
    const emptyDb: D1Database = {
      prepare() {
        const statement = {
          bind: () => statement,
          all: async () => ({ results: [] }),
          run: async () => ({ meta: { changes: 0 } }),
        };
        return statement;
      },
    };
    const response = await worker.fetch(
      new Request('https://edge.test/api/history/posts_per_minute?range=7d'),
      { CACHEKIT_API_KEY: TEST_API_KEY, HISTORY: emptyDb },
    );
    // A failed D1 read answers 500, so a 200 labelled d1 proves the read ran.
    expect(response.status).toBe(200);
    expect(response.headers.get('x-history-source')).toBe('d1');
    expect(fetchMock).not.toHaveBeenCalled();
  });
});
