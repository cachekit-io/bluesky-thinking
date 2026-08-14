/**
 * Scheduled-handler isolation (LAB-1616): the keep-alive ping and history
 * capture share one cron, so each must survive the other's worst day.
 * Global fetch is stubbed — no network, no creds, same as every other test.
 */
import { afterEach, describe, expect, it, vi } from 'vitest';
import worker from '../src/worker.js';
import type { D1Database } from '../src/history.js';

const HEALTH_URL = 'https://ingester.example/health';

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

describe('scheduled: keep-alive and history capture are isolated', () => {
  it('keep-alive still pings when the history store and backend both fail', async () => {
    const fetchMock = stubFetch(500);
    await expect(
      worker.scheduled(
        { scheduledTime: Date.UTC(2026, 7, 14, 14, 0, 0) },
        {
          INGESTER_HEALTH_URL: HEALTH_URL,
          CACHEKIT_API_KEY: 'ck_test_not_a_real_key',
          HISTORY: explodingDb,
        },
      ),
    ).resolves.toBeUndefined();
    expect(fetchMock.mock.calls.map((call) => String(call[0]))).toContain(HEALTH_URL);
  });

  it('an unconfigured history store never breaks the keep-alive', async () => {
    const fetchMock = stubFetch();
    await expect(
      worker.scheduled(
        { scheduledTime: Date.UTC(2026, 7, 14, 14, 0, 0) },
        {
          INGESTER_HEALTH_URL: HEALTH_URL,
        },
      ),
    ).resolves.toBeUndefined();
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  it('a missing keep-alive URL never blocks history capture (no-op fire)', async () => {
    const fetchMock = stubFetch();
    // Off-boundary fire: captureTick must no-op without any backend read.
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
