/**
 * Scheduled-handler containment (LAB-1616, LAB-2383): the cron's only job is
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
          CACHEKIT_API_KEY: 'ck_test_not_a_real_key',
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
        CACHEKIT_API_KEY: 'ck_test_not_a_real_key',
        HISTORY: explodingDb,
      },
    );
    expect(fetchMock).not.toHaveBeenCalled();
  });
});
