/**
 * History capture + range-query tests (LAB-1616) — no network, no creds.
 *
 * D1 is SQLite, so the D1 binding is shimmed over node:sqlite (Node ≥22.13,
 * in core — no new dependency) running the REAL migration file: the SQL,
 * the PRIMARY KEY idempotency and the retention deletes are exercised for
 * real, not against a hand-faked query engine. Aggregate payloads are
 * produced with the SDK's own interop codec, same as handler.test.ts.
 */
import { readFileSync } from 'node:fs';
import { DatabaseSync } from 'node:sqlite';
import { describe, expect, it } from 'vitest';
import { encodeInteropValue, generateInteropKey, type Backend } from '@cachekit-io/cachekit';
import { NAMESPACE, OPERATIONS } from '../src/handler.js';
import {
  captureTick,
  handleHistoryApi,
  INCOMPLETE_RESPONSE_TTL_SECONDS,
  MAX_PAYLOAD_BYTES,
  MAX_RECORD_KEYS,
  MAX_RESPONSE_BYTES,
  STORED_TOP_N,
  TIER_SPECS,
  trimPayload,
  type D1Database,
  type D1PreparedStatement,
} from '../src/history.js';

const MIGRATION = readFileSync(
  new URL('../migrations/0001_history_snapshots.sql', import.meta.url),
  'utf8',
);

type SqlValue = null | number | bigint | string | Uint8Array;

/** Runtime-checked narrowing so the shim never smuggles an unbindable value. */
function toSqlValues(values: unknown[]): SqlValue[] {
  return values.map((value) => {
    if (
      value === null ||
      typeof value === 'number' ||
      typeof value === 'bigint' ||
      typeof value === 'string' ||
      value instanceof Uint8Array
    ) {
      return value;
    }
    throw new Error(`unbindable SQL value of type ${typeof value}`);
  });
}

/** Type guard, so narrowing needs no assertion (team rule: no unsafe `as`). */
function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === 'object' && !Array.isArray(value);
}

/** Runtime-checked JSON.parse for fixtures we expect to be objects. */
function asRecord(payload: unknown): Record<string, unknown> {
  const parsed: unknown = JSON.parse(String(payload));
  if (!isRecord(parsed)) throw new Error('fixture payload is not a JSON object');
  return parsed;
}

function d1Of(db: DatabaseSync): D1Database {
  return {
    prepare(sql: string) {
      const make = (args: SqlValue[]): D1PreparedStatement => ({
        bind: (...values: unknown[]) => make(toSqlValues(values)),
        all: async () => ({
          results: db
            .prepare(sql)
            .all(...args)
            .map((row) => ({ ...row })),
        }),
        run: async () => {
          const result = db.prepare(sql).run(...args);
          return { meta: { changes: Number(result.changes) } };
        },
      });
      return make([]);
    },
  };
}

function freshDb(): { db: D1Database; sqlite: DatabaseSync } {
  const sqlite = new DatabaseSync(':memory:');
  sqlite.exec(MIGRATION);
  return { db: d1Of(sqlite), sqlite };
}

const explodingDb: D1Database = {
  prepare() {
    throw new Error('history store read attempted for invalid input');
  },
};

function mockBackend(get: Backend['get'], set?: Backend['set']): Backend {
  return {
    get,
    set: set ?? (async () => undefined),
    delete: async () => false,
    exists: async () => false,
    close: async () => undefined,
  };
}

const NORMALIZATION = 'skyline-normalization-v1';

/** A full, realistic aggregate for one operation (ranked lists 30 deep). */
function aggregate(
  operation: string,
  window: string,
  generatedAt: number,
  overrides: Record<string, unknown> = {},
): Record<string, unknown> {
  const ranked = (name: string) =>
    Array.from({ length: 30 }, (_, i) => ({ [name]: `${name}-${i}`, count: 100 - i }));
  const base: Record<string, unknown> = {
    window,
    generated_at: generatedAt,
    total_posts: 1234,
    total_events_considered: 1234,
    total_signal_candidates: 2222,
    excluded_count_by_reason: { filtered_tag: 3 },
    normalization_version: NORMALIZATION,
  };
  switch (operation) {
    case 'trending_hashtags':
      base.hashtags = ranked('tag');
      break;
    case 'trending_links':
      // Real http(s) URLs, matching the publisher's normalize_link output —
      // the edge validates the `uri` scheme, so `uri-0` would be a gap.
      base.links = Array.from({ length: 30 }, (_, i) => ({
        uri: `https://example.com/${i}`,
        count: 100 - i,
      }));
      base.domains = ranked('domain');
      break;
    case 'lang_mix':
      base.langs = { en: 0.6, ja: 0.3, other: 0.1 };
      break;
    case 'posts_per_minute':
      base.ppm = 412.5;
      break;
    case 'top_emoji':
      base.emoji = ranked('emoji');
      break;
  }
  return { ...base, ...overrides };
}

/** Backend serving every operation for the given windows, fresh as of `at`. */
function liveBackend(at: number, windows: string[] = ['1h', '24h']): Backend {
  const bytes = new Map<string, Uint8Array>();
  for (const operation of OPERATIONS) {
    for (const window of windows) {
      bytes.set(
        generateInteropKey(NAMESPACE, operation, [window]),
        encodeInteropValue(aggregate(operation, window, at)),
      );
    }
  }
  return mockBackend(async (key) => bytes.get(key) ?? null);
}

function allRows(sqlite: DatabaseSync): Record<string, unknown>[] {
  return sqlite
    .prepare('SELECT * FROM snapshots ORDER BY tier, operation, bucket_ts')
    .all()
    .map((row) => ({ ...row }));
}

// 14:00:00 UTC — an hourly boundary that is not midnight.
const HOUR_MS = Date.UTC(2026, 7, 14, 14, 0, 0);
// 00:00:00 UTC — the daily boundary.
const MIDNIGHT_MS = Date.UTC(2026, 7, 14, 0, 0, 0);

describe('captureTick boundaries', () => {
  it('no-ops entirely off the hour boundary (defense against off-minute fires)', async () => {
    const { db, sqlite } = freshDb();
    const report = await captureTick(liveBackend(0), db, Date.UTC(2026, 7, 14, 14, 10, 0));
    expect(report).toEqual({ boundary: 'none' });
    expect(allRows(sqlite)).toHaveLength(0);
  });

  it('captures all five operations into the hourly tier at the top of the hour', async () => {
    const { db, sqlite } = freshDb();
    const nowSec = HOUR_MS / 1000;
    const report = await captureTick(liveBackend(nowSec - 30), db, HOUR_MS);

    expect(report.boundary).toBe('hourly');
    expect(report.hourly).toMatchObject({ captured: 5, duplicate: 0, missing: 0 });
    expect(report.daily).toBeUndefined();

    const rows = allRows(sqlite);
    expect(rows).toHaveLength(5);
    for (const row of rows) {
      expect(row.tier).toBe('hourly');
      expect(row.bucket_ts).toBe(nowSec);
      expect(row.normalization_version).toBe(NORMALIZATION);
    }
    const tags = asRecord(rows.find((r) => r.operation === 'trending_hashtags')?.payload);
    // Stored depth is bounded: top-20 of the live top-50, allowlisted keys only.
    expect((tags.hashtags as unknown[]).length).toBe(STORED_TOP_N);
    expect(tags.generated_at).toBeUndefined();
    expect(tags.normalization_version).toBeUndefined();
    expect(tags.total_posts).toBe(1234);
  });

  it('adds the daily tier and retention at midnight UTC', async () => {
    const { db, sqlite } = freshDb();
    const nowSec = MIDNIGHT_MS / 1000;
    const report = await captureTick(liveBackend(nowSec - 30), db, MIDNIGHT_MS);

    expect(report.boundary).toBe('daily');
    expect(report.hourly?.captured).toBe(5);
    expect(report.daily?.captured).toBe(5);
    expect(report.retention).toEqual({ hourlyDeleted: 0, dailyDeleted: 0 });
    expect(allRows(sqlite)).toHaveLength(10);
  });
});

describe('idempotency (AC: duplicate publishes cannot duplicate history)', () => {
  it('a re-fired capture for the same bucket is a counted no-op, first write wins', async () => {
    const { db, sqlite } = freshDb();
    const nowSec = HOUR_MS / 1000;
    await captureTick(liveBackend(nowSec - 30), db, HOUR_MS);

    // Restart/retry with DIFFERENT live values — the bucket must not change.
    const changed = liveBackend(nowSec - 10);
    const report = await captureTick(changed, db, HOUR_MS + 5_000);

    expect(report.hourly).toMatchObject({ captured: 0, duplicate: 5 });
    const rows = allRows(sqlite);
    expect(rows).toHaveLength(5);
    expect(rows.every((r) => r.generated_at === nowSec - 30)).toBe(true);
  });
});

describe('gaps stay gaps', () => {
  it('a missing aggregate is counted and skipped, the other four still land', async () => {
    const { db, sqlite } = freshDb();
    const nowSec = HOUR_MS / 1000;
    const full = liveBackend(nowSec - 30);
    const partial = mockBackend(async (key) =>
      key === generateInteropKey(NAMESPACE, 'top_emoji', ['1h']) ? null : full.get(key),
    );
    const report = await captureTick(partial, db, HOUR_MS);
    expect(report.hourly).toMatchObject({ captured: 4, missing: 1 });
    expect(allRows(sqlite).map((r) => r.operation)).not.toContain('top_emoji');
  });

  it('a stale aggregate (older than the source TTL) becomes a gap, not a mislabeled point', async () => {
    const { db, sqlite } = freshDb();
    const nowSec = HOUR_MS / 1000;
    // generated 20 minutes before the boundary — beyond the 1h window's 300s TTL guard
    const report = await captureTick(liveBackend(nowSec - 1200), db, HOUR_MS);
    expect(report.hourly).toMatchObject({ captured: 0, stale: 5 });
    expect(allRows(sqlite)).toHaveLength(0);
  });

  it('the skew bound is symmetric: a late-delivered tick within one TTL still captures', async () => {
    const { db, sqlite } = freshDb();
    const nowSec = HOUR_MS / 1000;
    // The tick arrives late; the ingester has already republished 200s past
    // the boundary. Within the 300s TTL either side → a point, not a gap.
    const report = await captureTick(liveBackend(nowSec + 200), db, HOUR_MS);
    expect(report.hourly).toMatchObject({ captured: 5, stale: 0 });
    expect(allRows(sqlite)).toHaveLength(5);

    // Beyond one TTL past the boundary → gap.
    const { db: db2, sqlite: sqlite2 } = freshDb();
    const late = await captureTick(liveBackend(nowSec + 400), db2, HOUR_MS);
    expect(late.hourly).toMatchObject({ captured: 0, stale: 5 });
    expect(allRows(sqlite2)).toHaveLength(0);
  });

  it('a malformed aggregate becomes a gap and one bad operation costs exactly one', async () => {
    const { db, sqlite } = freshDb();
    const nowSec = HOUR_MS / 1000;
    const full = liveBackend(nowSec - 30);
    const poisoned = mockBackend(async (key) =>
      key === generateInteropKey(NAMESPACE, 'trending_hashtags', ['1h'])
        ? encodeInteropValue({ window: '1h', generated_at: nowSec - 30, hashtags: 'nope' })
        : full.get(key),
    );
    const report = await captureTick(poisoned, db, HOUR_MS);
    expect(report.hourly).toMatchObject({ captured: 4, invalid: 1 });
    expect(allRows(sqlite).map((r) => r.operation)).not.toContain('trending_hashtags');
  });

  it('an aggregate whose trimmed payload exceeds the cap becomes a gap, not a row', async () => {
    const { db, sqlite } = freshDb();
    const nowSec = HOUR_MS / 1000;
    const full = liveBackend(nowSec - 30);
    // Per-label caps and MAX_RECORD_KEYS bound every other field tightly, so
    // the row cap's one remaining reachable path is a top-20 of maximal URIs:
    // the publisher permits 2,048-char URLs, and 20 of them is ~41 KB. This
    // is legitimate-but-pathological input — real URLs are ~100 chars — and
    // the honest outcome is a gap, not a silently over-budget row.
    const huge = aggregate('trending_links', '1h', nowSec - 30, {
      links: Array.from({ length: STORED_TOP_N }, (_, i) => ({
        uri: `https://example.com/${'p'.repeat(2000)}${i}`,
        count: 100 - i,
      })),
    });
    const oversized = mockBackend(async (key) =>
      key === generateInteropKey(NAMESPACE, 'trending_links', ['1h'])
        ? encodeInteropValue(huge)
        : full.get(key),
    );
    const report = await captureTick(oversized, db, HOUR_MS);
    expect(report.hourly).toMatchObject({ captured: 4, invalid: 1 });
    expect(allRows(sqlite).map((r) => r.operation)).not.toContain('trending_links');
  });

  it('caps stored payloads by UTF-8 bytes, not UTF-16 code units', async () => {
    const { db, sqlite } = freshDb();
    const nowSec = HOUR_MS / 1000;
    const full = liveBackend(nowSec - 30);
    // 20 URIs with a 1,000-char CJK path. Each is 1,020 UTF-16 units (inside
    // the publisher's 2,048 cap, so every element is legitimately KEPT — the
    // label cap cannot be what rejects this row) but ~3 bytes per char in
    // UTF-8: ~20.4k code units total, UNDER the 32 KiB cap, against ~61k
    // UTF-8 bytes, well OVER it. Counting units would store this at ~1.9× the
    // documented ceiling.
    const cjkHeavy = aggregate('trending_links', '1h', nowSec - 30, {
      links: Array.from({ length: STORED_TOP_N }, (_, i) => ({
        uri: `https://example.com/${'あ'.repeat(1000)}${i}`,
        count: 100 - i,
      })),
    });
    const backend = mockBackend(async (key) =>
      key === generateInteropKey(NAMESPACE, 'trending_links', ['1h'])
        ? encodeInteropValue(cjkHeavy)
        : full.get(key),
    );
    const report = await captureTick(backend, db, HOUR_MS);
    expect(report.hourly).toMatchObject({ captured: 4, invalid: 1 });
    expect(allRows(sqlite).map((r) => r.operation)).not.toContain('trending_links');
  });

  it('an entry whose own window claim disagrees with its key becomes a gap', async () => {
    const { db, sqlite } = freshDb();
    const nowSec = HOUR_MS / 1000;
    const full = liveBackend(nowSec - 30);
    const mislabeled = mockBackend(async (key) =>
      key === generateInteropKey(NAMESPACE, 'posts_per_minute', ['1h'])
        ? encodeInteropValue(aggregate('posts_per_minute', '5m', nowSec - 30))
        : full.get(key),
    );
    const report = await captureTick(mislabeled, db, HOUR_MS);
    expect(report.hourly).toMatchObject({ captured: 4, invalid: 1 });
    expect(allRows(sqlite).map((r) => r.operation)).not.toContain('posts_per_minute');
  });

  it('a failing backend read costs one gap, never the tick', async () => {
    const { db, sqlite } = freshDb();
    const nowSec = HOUR_MS / 1000;
    const full = liveBackend(nowSec - 30);
    const flaky = mockBackend(async (key) => {
      if (key === generateInteropKey(NAMESPACE, 'lang_mix', ['1h'])) throw new Error('boom');
      return full.get(key);
    });
    const report = await captureTick(flaky, db, HOUR_MS);
    expect(report.hourly).toMatchObject({ captured: 4, failed: 1 });
    expect(allRows(sqlite)).toHaveLength(4);
  });
});

describe('trimPayload value-level allowlist', () => {
  it('rebuilds record fields as bounded {name: count} — smuggled text is dropped, not stored', () => {
    const trimmed = trimPayload('lang_mix', {
      window: '1h',
      total_posts: 'thirty kilobytes of post text', // non-numeric total → dropped
      total_events_considered: 1234,
      excluded_count_by_reason: {
        filtered_tag: 3,
        smuggled: 'entire post body parked in a count field', // non-numeric → dropped
        '<img onerror=1>': 4, // not a reason name → dropped
        [`${'k'.repeat(200)}`]: 7, // over-long → dropped, NOT truncated
      },
      langs: { en: 0.6, evil: 'post text here', ja: 0.3 },
    });
    expect(trimmed).toEqual({
      window: '1h',
      total_events_considered: 1234,
      excluded_count_by_reason: { filtered_tag: 3 },
      langs: { en: 0.6, ja: 0.3 },
    });
  });

  it('drops record keys that are not their field vocabulary — the LAB-1613 round-4 class', () => {
    // A snapshot retains for up to 400 days what the live cache expired in an
    // hour, so an unvalidated key name is strictly worse here than upstream.
    const langs = trimPayload('lang_mix', {
      langs: {
        en: 0.5,
        'pt-BR': 0.2,
        other: 0.1,
        '<script>alert(1)</script>': 0.9,
        'not a language at all': 0.1,
      },
    });
    expect(langs).toEqual({ langs: { en: 0.5, 'pt-BR': 0.2, other: 0.1 } });

    // Key COUNT is bounded too — that was previously the byte cap's job alone.
    // 400 valid 3-letter codes, so the pattern cannot be what trims them.
    const codes = Array.from({ length: 400 }, (_, i) => [
      `a${String.fromCharCode(97 + Math.floor(i / 26))}${String.fromCharCode(97 + (i % 26))}`,
      0.001,
    ]);
    const many = trimPayload('lang_mix', { langs: Object.fromEntries(codes) });
    const kept = many && isRecord(many.langs) ? Object.keys(many.langs) : [];
    expect(kept).toHaveLength(MAX_RECORD_KEYS);
  });

  it('drops an over-long record key instead of truncating it into a collision', () => {
    // Truncation made `k…aaa` and `k…bbb` the same 64-char key, and the second
    // silently overwrote the first's count rather than summing it.
    const base = 'r'.repeat(62);
    const trimmed = trimPayload('lang_mix', {
      langs: { en: 1, [`${base}aaa`]: 1, [`${base}bbb`]: 2 },
    });
    expect(trimmed).toEqual({ langs: { en: 1 } });
  });

  it('rebuilds ranked-list elements from allowlisted fields — unknown element keys cannot survive', () => {
    const trimmed = trimPayload('trending_hashtags', {
      window: '1h',
      hashtags: [
        {
          tag: 'bluesky',
          display: 'BlueSky',
          count: 42,
          // Every one of these rode through the old `list.slice()` verbatim.
          note: 'the entire text of somebody‘s post',
          nested: { author_handle: 'alice.bsky.social', text: 'post body' },
          replies: [{ text: 'a reply' }],
        },
        { tag: 'nodisplay', count: 7 }, // decorative absent → trimmed, not a gap
        { tag: 'baddisplay', display: { text: 'post' }, count: 5 }, // non-string → dropped
        { tag: 'longdisplay', display: 'd'.repeat(900), count: 4 }, // over-cap → dropped
      ],
    });
    expect(trimmed).toEqual({
      window: '1h',
      hashtags: [
        { tag: 'bluesky', display: 'BlueSky', count: 42 },
        { tag: 'nodisplay', count: 7 },
        { tag: 'baddisplay', count: 5 },
        { tag: 'longdisplay', count: 4 },
      ],
    });
  });

  it('only stores `window` when it is one of the two literals', () => {
    // trimPayload is exported; its contract cannot rely on one caller's guard.
    expect(trimPayload('lang_mix', { window: { text: 'post body' }, langs: {} })).toEqual({
      langs: {},
    });
    expect(trimPayload('lang_mix', { window: '24h', langs: {} })).toEqual({
      window: '24h',
      langs: {},
    });
  });

  it('gaps the snapshot when a ranked element lacks or violates its identity field', () => {
    expect(trimPayload('trending_hashtags', { hashtags: [{ count: 5 }] })).toBeNull();
    expect(trimPayload('trending_hashtags', { hashtags: [{ tag: 'a' }] })).toBeNull();
    expect(trimPayload('trending_hashtags', { hashtags: ['just-a-string'] })).toBeNull();
    expect(trimPayload('trending_hashtags', { hashtags: [{ tag: '', count: 1 }] })).toBeNull();
    expect(trimPayload('top_emoji', { emoji: [{ emoji: '🔥', count: 'lots' }] })).toBeNull();
    // An over-cap identity field is a gap, NOT a truncated value: cutting it
    // would forge a different tag/URI and collapse two rankings into one.
    expect(
      trimPayload('trending_hashtags', { hashtags: [{ tag: 'x'.repeat(65), count: 1 }] }),
    ).toBeNull();
    // The legitimate shapes still pass, for each list-bearing operation.
    expect(
      trimPayload('trending_links', { links: [{ uri: 'https://a/b', count: 2 }], domains: [] }),
    ).toEqual({ links: [{ uri: 'https://a/b', count: 2 }], domains: [] });
    expect(trimPayload('top_emoji', { emoji: [{ emoji: '🔥', count: 9 }] })).toEqual({
      emoji: [{ emoji: '🔥', count: 9 }],
    });
    // A URI at the publisher's own 2048-char bound is legitimate and kept.
    const longUri = `https://example.com/${'p'.repeat(2000)}`;
    expect(
      trimPayload('trending_links', { links: [{ uri: longUri, count: 1 }], domains: [] }),
    ).toEqual({ links: [{ uri: longUri, count: 1 }], domains: [] });
  });

  it('gaps a link whose URI is not http(s) — no javascript:/data: reaches storage', () => {
    // /api/history/trending_links is public JSON; a third-party renderer must
    // not receive a scheme our own dashboard only avoids by checking at render.
    for (const uri of ['javascript:alert(1)', 'data:text/html,<script>1</script>', 'not a url']) {
      expect(trimPayload('trending_links', { links: [{ uri, count: 1 }], domains: [] })).toBeNull();
    }
  });
});

describe('capture never retains text-bearing fields (CodeRabbit, PR #19)', () => {
  it('a nested text field injected into a ranked element is absent from the D1 row', async () => {
    const { db, sqlite } = freshDb();
    const nowSec = HOUR_MS / 1000;
    const POST_TEXT = 'the-entire-body-of-a-post-that-must-never-be-persisted';

    // The source cache is operator-writable, so this is the threat model, not
    // a hypothetical: a hostile aggregate under a legitimate interop key.
    const hostile = encodeInteropValue(
      aggregate('trending_hashtags', '1h', nowSec - 30, {
        hashtags: [
          { tag: 'trending', display: 'Trending', count: 10, exfil: POST_TEXT },
          { tag: 'other', count: 9, nested: { text: POST_TEXT } },
        ],
      }),
    );
    const clean = liveBackend(nowSec - 30);
    const backend = mockBackend(async (key) =>
      key === generateInteropKey(NAMESPACE, 'trending_hashtags', ['1h']) ? hostile : clean.get(key),
    );

    const report = await captureTick(backend, db, HOUR_MS);
    expect(report.hourly).toMatchObject({ captured: 5, invalid: 0 });

    const row = allRows(sqlite).find((r) => r.operation === 'trending_hashtags');
    // The strongest assertion available: the text is nowhere in the stored row.
    expect(String(row?.payload)).not.toContain(POST_TEXT);
    expect(asRecord(row?.payload).hashtags).toEqual([
      { tag: 'trending', display: 'Trending', count: 10 },
      { tag: 'other', count: 9 },
    ]);
  });
});

describe('retention', () => {
  it('deletes only buckets past each tier horizon', async () => {
    const { db, sqlite } = freshDb();
    const nowSec = MIDNIGHT_MS / 1000;
    const insert = sqlite.prepare(
      `INSERT INTO snapshots (operation, tier, bucket_ts, generated_at, normalization_version,
        payload, captured_at) VALUES (?, ?, ?, ?, ?, ?, ?)`,
    );
    const seed = (tier: string, bucketTs: number) =>
      insert.run('posts_per_minute', tier, bucketTs, bucketTs, NORMALIZATION, '{}', bucketTs);

    seed('hourly', nowSec - TIER_SPECS.hourly.retentionSeconds - 3600); // past horizon
    seed('hourly', nowSec - TIER_SPECS.hourly.retentionSeconds + 3600); // inside
    seed('daily', nowSec - TIER_SPECS.daily.retentionSeconds - 86400); // past horizon
    seed('daily', nowSec - TIER_SPECS.daily.retentionSeconds + 86400); // inside

    const report = await captureTick(liveBackend(nowSec - 30), db, MIDNIGHT_MS);
    expect(report.retention).toEqual({ hourlyDeleted: 1, dailyDeleted: 1 });
    const kept = allRows(sqlite).map((r) => r.bucket_ts);
    expect(kept).toContain(nowSec - TIER_SPECS.hourly.retentionSeconds + 3600);
    expect(kept).toContain(nowSec - TIER_SPECS.daily.retentionSeconds + 86400);
    expect(kept).not.toContain(nowSec - TIER_SPECS.hourly.retentionSeconds - 3600);
    expect(kept).not.toContain(nowSec - TIER_SPECS.daily.retentionSeconds - 86400);
  });
});

// ---------------------------------------------------------------------------

const api = (path: string) => new URL(`https://skyline.example${path}`);

/** Seed hourly ppm rows for the `count` buckets ending at `lastBucket`. */
function seedHourly(
  sqlite: DatabaseSync,
  lastBucket: number,
  count: number,
  skip: Set<number> = new Set(),
  version = NORMALIZATION,
): void {
  const insert = sqlite.prepare(
    `INSERT INTO snapshots (operation, tier, bucket_ts, generated_at, normalization_version,
      payload, captured_at) VALUES (?, ?, ?, ?, ?, ?, ?)`,
  );
  for (let i = 0; i < count; i += 1) {
    const bucket = lastBucket - i * 3600;
    if (skip.has(bucket)) continue;
    insert.run(
      'posts_per_minute',
      'hourly',
      bucket,
      bucket - 20,
      version,
      JSON.stringify({ window: '1h', ppm: 100 + i, total_posts: 6000 + i }),
      bucket,
    );
  }
}

describe('GET /api/history/{operation}', () => {
  const nowMs = Date.UTC(2026, 7, 14, 14, 32, 0);
  const lastBucket = Date.UTC(2026, 7, 14, 14, 0, 0) / 1000;

  it('validates before any store read: unknown operation → 404', async () => {
    const res = await handleHistoryApi(api('/api/history/sentiment?range=7d'), {
      db: explodingDb,
      backend: null,
      nowMs,
    });
    expect(res.status).toBe(404);
    expect(((await res.json()) as { error: string }).error).toBe('unknown_operation');
  });

  it('validates before any store read: missing/invalid range → 400', async () => {
    for (const path of ['/api/history/lang_mix', '/api/history/lang_mix?range=90d']) {
      const res = await handleHistoryApi(api(path), { db: explodingDb, backend: null, nowMs });
      expect(res.status).toBe(400);
      expect(((await res.json()) as { error: string }).error).toBe('invalid_range');
    }
  });

  it('rejects prototype-chain range values (hasOwn, not `in`)', async () => {
    const res = await handleHistoryApi(api('/api/history/lang_mix?range=toString'), {
      db: explodingDb,
      backend: null,
      nowMs,
    });
    expect(res.status).toBe(400);
    expect(((await res.json()) as { error: string }).error).toBe('invalid_range');
  });

  it('returns a bounded, ascending series with honest coverage', async () => {
    const { db, sqlite } = freshDb();
    seedHourly(sqlite, lastBucket, 200); // more than a 7d range can return

    const res = await handleHistoryApi(api('/api/history/posts_per_minute?range=7d'), {
      db,
      backend: null,
      nowMs,
    });
    expect(res.status).toBe(200);
    expect(res.headers.get('x-history-source')).toBe('d1');
    const body = (await res.json()) as {
      tier: string;
      period_seconds: number;
      points: { bucket_ts: number; data: { ppm: number } }[];
      coverage: Record<string, number>;
      normalization_versions: string[];
    };
    expect(body.tier).toBe('hourly');
    expect(body.points).toHaveLength(168);
    const ts = body.points.map((p) => p.bucket_ts);
    expect(ts).toEqual([...ts].sort((a, b) => a - b));
    expect(ts[ts.length - 1]).toBe(lastBucket);
    expect(body.coverage).toMatchObject({
      expected_points: 168,
      present_points: 168,
      to: lastBucket,
    });
    expect(body.normalization_versions).toEqual([NORMALIZATION]);
  });

  it('represents missing buckets as absent points, not zeros', async () => {
    const { db, sqlite } = freshDb();
    const holes = new Set([lastBucket - 3600, lastBucket - 7200]);
    seedHourly(sqlite, lastBucket, 10, holes);

    const res = await handleHistoryApi(api('/api/history/posts_per_minute?range=7d'), {
      db,
      backend: null,
      nowMs,
    });
    const body = (await res.json()) as {
      points: { bucket_ts: number }[];
      coverage: { expected_points: number; present_points: number };
    };
    expect(body.coverage.expected_points).toBe(168);
    expect(body.coverage.present_points).toBe(8);
    for (const hole of holes) {
      expect(body.points.map((p) => p.bucket_ts)).not.toContain(hole);
    }
  });

  it('never includes the in-flight bucket (capture grace)', async () => {
    const { db, sqlite } = freshDb();
    seedHourly(sqlite, lastBucket, 5);
    // 60s past 14:00 — the 14:00 capture may still be in flight, so the
    // series must end at 13:00 and cache under that bucket.
    const res = await handleHistoryApi(api('/api/history/posts_per_minute?range=7d'), {
      db,
      backend: null,
      nowMs: Date.UTC(2026, 7, 14, 14, 1, 0),
    });
    const body = (await res.json()) as { coverage: { to: number } };
    expect(body.coverage.to).toBe(lastBucket - 3600);
  });

  it('surfaces mixed normalization versions instead of silently blending them', async () => {
    const { db, sqlite } = freshDb();
    seedHourly(sqlite, lastBucket, 3);
    seedHourly(sqlite, lastBucket - 3 * 3600, 3, new Set(), 'skyline-normalization-v2');
    const res = await handleHistoryApi(api('/api/history/posts_per_minute?range=7d'), {
      db,
      backend: null,
      nowMs,
    });
    const body = (await res.json()) as { normalization_versions: string[] };
    expect(body.normalization_versions).toEqual([
      'skyline-normalization-v1',
      'skyline-normalization-v2',
    ]);
  });
});

describe('CacheKit-backed response caching', () => {
  const nowMs = Date.UTC(2026, 7, 14, 14, 32, 0);
  const lastBucket = Date.UTC(2026, 7, 14, 14, 0, 0) / 1000;

  it('computes from D1 once, then serves the stored response without touching D1', async () => {
    const { db, sqlite } = freshDb();
    seedHourly(sqlite, lastBucket, 10);
    const stored = new Map<string, { value: Uint8Array; ttl?: number }>();
    const backend = mockBackend(
      async (key) => stored.get(key)?.value ?? null,
      async (key, value, ttl) => {
        stored.set(key, { value, ttl });
      },
    );

    const first = await handleHistoryApi(api('/api/history/posts_per_minute?range=7d'), {
      db,
      backend,
      nowMs,
    });
    expect(first.status).toBe(200);
    expect(first.headers.get('x-history-source')).toBe('d1');
    expect(stored.size).toBe(1);
    const [key, entry] = [...stored.entries()][0]!;
    expect(key).toBe(`bluesky-thinking:history_response:posts_per_minute:7d:${lastBucket}`);
    expect(entry.ttl).toBe(3600);

    // Same bucket, D1 gone: the CacheKit copy serves, byte-identical.
    const second = await handleHistoryApi(api('/api/history/posts_per_minute?range=7d'), {
      db: explodingDb,
      backend,
      nowMs: nowMs + 60_000,
    });
    expect(second.status).toBe(200);
    expect(second.headers.get('x-history-source')).toBe('cachekit');
    expect(await second.json()).toEqual(await first.json());
  });

  it('a poisoned cached response is treated as a miss, recomputed, and overwritten', async () => {
    const { db, sqlite } = freshDb();
    seedHourly(sqlite, lastBucket, 6);
    const cacheKey = `bluesky-thinking:history_response:posts_per_minute:7d:${lastBucket}`;
    const stored = new Map<string, Uint8Array>();
    // Hostile operator wrote non-JSON garbage under the exact response key.
    stored.set(cacheKey, new TextEncoder().encode('<script>not json</script>'));
    const backend = mockBackend(
      async (key) => stored.get(key) ?? null,
      async (key, value) => {
        stored.set(key, value);
      },
    );

    const res = await handleHistoryApi(api('/api/history/posts_per_minute?range=7d'), {
      db,
      backend,
      nowMs,
    });
    expect(res.status).toBe(200);
    expect(res.headers.get('x-history-source')).toBe('d1'); // never relayed
    const body = (await res.json()) as { coverage: { present_points: number } };
    expect(body.coverage.present_points).toBe(6);
    // The bad value was overwritten with the recomputed response (healed).
    expect(JSON.parse(new TextDecoder().decode(stored.get(cacheKey)))).toEqual(body);
  });

  it('pins the size limits and TTLs the cache contract depends on', () => {
    expect(MAX_PAYLOAD_BYTES).toBe(32_768);
    expect(MAX_RESPONSE_BYTES).toBe(4 * 1024 * 1024);
    expect(INCOMPLETE_RESPONSE_TTL_SECONDS).toBe(60);
  });

  it('a structurally valid envelope carrying post text in point.data is a miss', async () => {
    // The response cache lives in the same operator-writable store the capture
    // allowlist defends against, and it is the only short-circuit past that
    // allowlist. A forgery that satisfies every envelope check but smuggles
    // post text through points[].data must not be relayed, or the write-side
    // gate is decorative.
    const { db, sqlite } = freshDb();
    seedHourly(sqlite, lastBucket, 6);
    const cacheKey = `bluesky-thinking:history_response:posts_per_minute:7d:${lastBucket}`;
    const stored = new Map<string, Uint8Array>();

    // Build the REAL response, then poison one point's data in place so every
    // coverage/ordering/structure check still passes.
    const genuine = await handleHistoryApi(api('/api/history/posts_per_minute?range=7d'), {
      db,
      backend: null,
      nowMs,
    });
    const forged = (await genuine.json()) as {
      points: { data: Record<string, unknown> }[];
    };
    const poisoned = forged.points[0];
    if (!poisoned) throw new Error('fixture: expected a seeded point to poison');
    poisoned.data.exfil = 'the entire body of a post, plus did:plc:victim';
    stored.set(cacheKey, new TextEncoder().encode(JSON.stringify(forged)));

    const backend = mockBackend(
      async (key) => stored.get(key) ?? null,
      async (key, value) => {
        stored.set(key, value);
      },
    );
    const res = await handleHistoryApi(api('/api/history/posts_per_minute?range=7d'), {
      db,
      backend,
      nowMs,
    });
    expect(res.status).toBe(200);
    expect(res.headers.get('x-history-source')).toBe('d1'); // recomputed, not relayed
    const served = await res.text();
    expect(served).not.toContain('exfil');
    expect(served).not.toContain('did:plc:victim');
    // ...and the poisoned key was healed with the clean recomputation.
    expect(new TextDecoder().decode(stored.get(cacheKey))).not.toContain('exfil');
  });

  it('an oversized cached value is a miss, recomputed from D1, and healed', async () => {
    const { db, sqlite } = freshDb();
    seedHourly(sqlite, lastBucket, 3);
    const cacheKey = `bluesky-thinking:history_response:posts_per_minute:7d:${lastBucket}`;
    const stored = new Map<string, Uint8Array>();
    stored.set(cacheKey, new Uint8Array(MAX_RESPONSE_BYTES + 1));
    const backend = mockBackend(
      async (key) => stored.get(key) ?? null,
      async (key, value) => {
        stored.set(key, value);
      },
    );
    const res = await handleHistoryApi(api('/api/history/posts_per_minute?range=7d'), {
      db,
      backend,
      nowMs,
    });
    expect(res.status).toBe(200);
    expect(res.headers.get('x-history-source')).toBe('d1'); // never relayed
    const body = (await res.json()) as { coverage: { present_points: number } };
    expect(body.coverage.present_points).toBe(3);
    expect(JSON.parse(new TextDecoder().decode(stored.get(cacheKey)))).toEqual(body);
  });

  it('a parseable but mismatched cached envelope is a miss, recomputed, and healed', async () => {
    const { db, sqlite } = freshDb();
    seedHourly(sqlite, lastBucket, 4);
    const cacheKey = `bluesky-thinking:history_response:posts_per_minute:7d:${lastBucket}`;
    // A structurally plausible response — for the WRONG operation with empty
    // coverage — planted under the ppm key by a hostile operator. It parses,
    // it is size-bounded, and it must still never be relayed.
    const forged = {
      operation: 'top_emoji',
      range: '7d',
      tier: 'hourly',
      period_seconds: 3600,
      normalization_versions: [],
      coverage: {
        from: lastBucket - 7 * 86400,
        to: lastBucket,
        expected_points: 168,
        present_points: 0,
        history_started_at: null,
      },
      points: [],
    };
    const stored = new Map<string, Uint8Array>();
    stored.set(cacheKey, new TextEncoder().encode(JSON.stringify(forged)));
    const backend = mockBackend(
      async (key) => stored.get(key) ?? null,
      async (key, value) => {
        stored.set(key, value);
      },
    );
    const res = await handleHistoryApi(api('/api/history/posts_per_minute?range=7d'), {
      db,
      backend,
      nowMs,
    });
    expect(res.status).toBe(200);
    expect(res.headers.get('x-history-source')).toBe('d1'); // never relayed
    const body = (await res.json()) as { operation: string; coverage: { present_points: number } };
    expect(body.operation).toBe('posts_per_minute');
    expect(body.coverage.present_points).toBe(4);
    expect(JSON.parse(new TextDecoder().decode(stored.get(cacheKey)))).toEqual(body);
  });

  it('never writes a response the read side would reject (cap enforced on both ends)', async () => {
    const { db, sqlite } = freshDb();
    // 168 legitimate rows, each just under the per-row payload cap — the
    // assembled 7d response (~5.2 MB) exceeds MAX_RESPONSE_BYTES.
    const insert = sqlite.prepare(
      `INSERT INTO snapshots (operation, tier, bucket_ts, generated_at, normalization_version,
        payload, captured_at) VALUES (?, ?, ?, ?, ?, ?, ?)`,
    );
    const blob = 'x'.repeat(31_000);
    for (let i = 0; i < 168; i += 1) {
      const bucket = lastBucket - i * 3600;
      insert.run(
        'posts_per_minute',
        'hourly',
        bucket,
        bucket - 20,
        NORMALIZATION,
        JSON.stringify({ window: '1h', ppm: i, blob }),
        bucket,
      );
    }
    const sets: string[] = [];
    const backend = mockBackend(
      async () => null,
      async (key) => {
        sets.push(key);
      },
    );
    const res = await handleHistoryApi(api('/api/history/posts_per_minute?range=7d'), {
      db,
      backend,
      nowMs,
    });
    expect(res.status).toBe(200); // still served, just never frozen
    const body = (await res.json()) as { coverage: { present_points: number } };
    expect(body.coverage.present_points).toBe(168);
    expect(sets).toEqual([]);
  });

  it('an empty cached value is a miss, not an empty 200', async () => {
    const { db, sqlite } = freshDb();
    seedHourly(sqlite, lastBucket, 2);
    const backend = mockBackend(async () => new Uint8Array(0));
    const res = await handleHistoryApi(api('/api/history/posts_per_minute?range=7d'), {
      db,
      backend,
      nowMs,
    });
    expect(res.status).toBe(200);
    expect(res.headers.get('x-history-source')).toBe('d1');
    expect(
      ((await res.json()) as { coverage: { present_points: number } }).coverage.present_points,
    ).toBe(2);
  });

  it('caches an incomplete series briefly (60 s), never frozen for a full period', async () => {
    const { db, sqlite } = freshDb();
    // Rows exist, but NOT for lastBucket — the capture is late or lost. This
    // is exactly the state where every request would otherwise pay full D1,
    // so the response IS cached — with a short TTL, not the tier period, so
    // a point landing moments later shows within a minute.
    seedHourly(sqlite, lastBucket - 3600, 5);
    const stored = new Map<string, { value: Uint8Array; ttl?: number }>();
    const backend = mockBackend(
      async () => null,
      async (key, value, ttl) => {
        stored.set(key, { value, ttl });
      },
    );
    const res = await handleHistoryApi(api('/api/history/posts_per_minute?range=7d'), {
      db,
      backend,
      nowMs,
    });
    expect(res.status).toBe(200);
    const body = (await res.json()) as { coverage: { to: number; present_points: number } };
    expect(body.coverage.to).toBe(lastBucket);
    expect(body.coverage.present_points).toBe(5);
    const entry = stored.get(`bluesky-thinking:history_response:posts_per_minute:7d:${lastBucket}`);
    expect(entry?.ttl).toBe(INCOMPLETE_RESPONSE_TTL_SECONDS);
  });

  it('a failing cache backend degrades to D1, never to an error', async () => {
    const { db, sqlite } = freshDb();
    seedHourly(sqlite, lastBucket, 4);
    const backend = mockBackend(
      async () => {
        throw new Error('cachekit down');
      },
      async () => {
        throw new Error('cachekit down');
      },
    );
    const res = await handleHistoryApi(api('/api/history/posts_per_minute?range=7d'), {
      db,
      backend,
      nowMs,
    });
    expect(res.status).toBe(200);
    expect(res.headers.get('x-history-source')).toBe('d1-fallback');
    const body = (await res.json()) as { coverage: { present_points: number } };
    expect(body.coverage.present_points).toBe(4);
  });

  it('a failing D1 store surfaces as 500, never fake data', async () => {
    const res = await handleHistoryApi(api('/api/history/posts_per_minute?range=7d'), {
      db: explodingDb,
      backend: null,
      nowMs,
    });
    expect(res.status).toBe(500);
    expect(((await res.json()) as { error: string }).error).toBe('history_unavailable');
  });
});

describe('GET /api/history/status', () => {
  it('reports null start and stale tiers before any capture', async () => {
    const { db } = freshDb();
    const res = await handleHistoryApi(api('/api/history/status'), {
      db,
      backend: null,
      nowMs: HOUR_MS,
    });
    const body = (await res.json()) as {
      history_started_at: number | null;
      tiers: Record<string, { rows: number; stale: boolean }>;
    };
    expect(body.history_started_at).toBeNull();
    expect(body.tiers.hourly).toMatchObject({ rows: 0, stale: true });
    expect(body.tiers.daily).toMatchObject({ rows: 0, stale: true });
  });

  it('a failing D1 store surfaces as history_unavailable, not a generic 500', async () => {
    const res = await handleHistoryApi(api('/api/history/status'), {
      db: explodingDb,
      backend: null,
      nowMs: HOUR_MS,
    });
    expect(res.status).toBe(500);
    expect(((await res.json()) as { error: string }).error).toBe('history_unavailable');
  });

  it('distinguishes healthy capture from a silently-stopped one', async () => {
    const { db, sqlite } = freshDb();
    const nowSec = HOUR_MS / 1000;
    seedHourly(sqlite, nowSec - 3600, 3); // newest hourly bucket is 1h old — healthy
    const res = await handleHistoryApi(api('/api/history/status'), {
      db,
      backend: null,
      nowMs: HOUR_MS,
    });
    const body = (await res.json()) as {
      history_started_at: number;
      tiers: Record<string, { rows: number; stale: boolean; latest_age_seconds: number }>;
    };
    expect(body.tiers.hourly).toMatchObject({ rows: 3, stale: false, latest_age_seconds: 3600 });
    expect(body.tiers.daily?.stale).toBe(true); // daily never captured → visibly unhealthy
    expect(body.history_started_at).toBe(nowSec - 3 * 3600);
  });
});
