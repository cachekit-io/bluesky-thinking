import { afterEach, describe, expect, it, vi } from 'vitest';
import {
  FRESHNESS_SECONDS,
  formatGeneratedAt,
  renderCardMarkup,
  renderOperation,
  windowFromSearch,
} from '../public/dashboard.js';

const generated_at = 1_754_000_000;

describe('Skyline dashboard payload renderers', () => {
  it('renders the real hashtag payload as ranked tags, never generated_at or total_posts', () => {
    const markup = renderOperation('trending_hashtags', {
      window: '5m',
      generated_at,
      total_posts: 912,
      hashtags: [
        { tag: 'cachekit', count: 42 },
        { tag: 'bluesky', count: 7 },
      ],
    });
    expect(markup).toContain('<ol class="rows">');
    expect(markup).toContain('<li class="row">');
    expect(markup).toContain('cachekit');
    expect(markup).not.toContain('1.75');
    expect(markup).not.toContain('912');
  });

  it.each([
    [
      'posts_per_minute',
      { window: '5m', generated_at, total_posts: 60, ppm: 12.4 },
      'posts/minute',
    ],
    [
      'trending_links',
      {
        window: '5m',
        generated_at,
        total_posts: 10,
        links: [{ uri: 'https://example.com/a', count: 3 }],
      },
      'example.com/a',
    ],
    [
      'lang_mix',
      { window: '5m', generated_at, total_posts: 10, langs: { en: 0.75, ja: 0.25 } },
      '75.0%',
    ],
    [
      'top_emoji',
      { window: '5m', generated_at, total_posts: 10, emoji: [{ emoji: '🔥', count: 3 }] },
      '🔥',
    ],
  ])('renders the real %s payload shape', (operation, payload, expected) => {
    expect(renderOperation(operation, payload)).toContain(expected);
  });

  it('rejects missing or wrong-shape rankings instead of calling them empty', () => {
    expect(renderOperation('trending_hashtags', { hashtags: {} })).toContain(
      'unexpected payload shape',
    );
    expect(renderOperation('trending_links', {})).toContain('unexpected payload shape');
    expect(renderOperation('lang_mix', { langs: [] })).toContain('unexpected payload shape');
    expect(renderOperation('top_emoji', { emoji: '🔥' })).toContain('unexpected payload shape');
  });

  it('skips null or non-object ranking elements instead of throwing into the network-error path', () => {
    const markup = renderOperation('trending_hashtags', {
      hashtags: [null, 'junk', 42, { tag: 'cachekit', count: 3 }],
    });
    expect(markup).toContain('cachekit');
    expect(markup).not.toContain('junk');
  });

  it('escapes labels and only makes http(s) links outbound links', () => {
    const markup = renderOperation('trending_links', {
      links: [
        { uri: 'javascript:alert(1)', count: 2 },
        { uri: 'https://example.com/?q="<tag>', count: 1 },
      ],
    });
    expect(markup).not.toContain('href="javascript:');
    expect(markup).toContain('rel="noopener noreferrer"');
    expect(markup).toContain('%3Ctag%3E');
  });

  it('uses the per-window freshness cadence instead of one global stale threshold', () => {
    expect(
      formatGeneratedAt(
        generated_at,
        '5m',
        (generated_at + (FRESHNESS_SECONDS['5m'] ?? 60) + 1) * 1000,
      ),
    ).toContain('older than 1 minute');
    expect(formatGeneratedAt(generated_at, '24h', (generated_at + 450) * 1000)).not.toContain(
      'Stale',
    );
    expect(
      formatGeneratedAt(
        generated_at,
        '24h',
        (generated_at + (FRESHNESS_SECONDS['24h'] ?? 900) + 1) * 1000,
      ),
    ).toContain('older than 15 minutes');
  });

  it('keeps failure states actionable and fails closed on unverified cache evidence', () => {
    expect(
      renderCardMarkup(
        'Links',
        { cache: 'ERR', operation: 'trending_links', error: 'decode_error' },
        '5m',
      ),
    ).toContain('could not be decoded');
    expect(
      renderCardMarkup(
        'Links',
        { cache: 'ERR', operation: 'trending_links', error: 'network_error' },
        '5m',
      ),
    ).toContain('Check your connection');
    expect(
      renderCardMarkup(
        'Links',
        { cache: 'HIT', operation: 'trending_links', data: { links: [] } },
        '5m',
      ),
    ).toContain('unverified');
    expect(
      renderCardMarkup('Links', { cache: 'MISS', operation: 'trending_links' }, '5m'),
    ).not.toContain('unverified');
    expect(
      renderCardMarkup(
        'Links',
        { cache: 'ERR', operation: 'trending_links', error: 'backend_error' },
        '5m',
      ),
    ).not.toContain('unverified');
  });

  it('reads only the supported window from the URL', () => {
    expect(windowFromSearch('?window=24h')).toBe('24h');
    expect(windowFromSearch('?window=7d')).toBe('5m');
  });
});

class ElementStub {
  dataset: Record<string, string> = {};
  children: unknown[] = [];
  content = '';
  set innerHTML(value: string) {
    this.content = value;
    this.children = value.includes('card-') ? [{}] : [];
  }
  get innerHTML() {
    return this.content;
  }
  addEventListener() {}
  setAttribute() {}
}

describe('dashboard bootstrap smoke test', () => {
  afterEach(() => vi.unstubAllGlobals());

  it('boots from the module tag and renders live aggregate cards', async () => {
    const grid = new ElementStub();
    const tiles = new ElementStub();
    const cards = new Map<string, ElementStub>();
    Object.defineProperty(grid, 'innerHTML', {
      set(value: string) {
        grid.content = value;
        grid.children = [{}];
        for (const operation of [
          'posts_per_minute',
          'trending_hashtags',
          'trending_links',
          'lang_mix',
          'top_emoji',
        ])
          cards.set(`card-${operation}`, new ElementStub());
      },
      get: () => grid.content,
    });
    const documentStub = {
      getElementById: (id: string) => ({ grid, tiles, ...Object.fromEntries(cards) })[id] ?? null,
      querySelector: () => new ElementStub(),
      querySelectorAll: () => [new ElementStub(), new ElementStub(), new ElementStub()],
    };
    vi.stubGlobal('document', documentStub);
    vi.stubGlobal('window', {
      location: { href: 'https://skyline.example/?window=5m', search: '?window=5m' },
      history: { replaceState: vi.fn() },
      setInterval: vi.fn(),
    });
    vi.stubGlobal('fetch', async (input: string) => {
      if (input === '/api/stats')
        return Response.json({ hits: 1, misses: 0, errors: 0, hit_rate: 1 });
      const operation = input.match(/\/api\/([^?]+)/)?.[1];
      const data = {
        posts_per_minute: { generated_at, total_posts: 3, ppm: 0.6 },
        trending_hashtags: {
          generated_at,
          total_posts: 3,
          hashtags: [{ tag: 'cachekit', count: 3 }],
        },
        trending_links: {
          generated_at,
          total_posts: 3,
          links: [{ uri: 'https://example.com', count: 3 }],
        },
        lang_mix: { generated_at, total_posts: 3, langs: { en: 1 } },
        top_emoji: { generated_at, total_posts: 3, emoji: [{ emoji: '🔥', count: 3 }] },
      }[operation ?? 'posts_per_minute'];
      return Response.json({ data }, { headers: { 'x-cache': 'HIT', 'x-hotpath': 'verified' } });
    });

    await import('../public/dashboard.js?smoke');
    await vi.waitFor(() =>
      expect(cards.get('card-trending_hashtags')?.innerHTML).toContain('cachekit'),
    );
    expect(grid.innerHTML).toContain('card-trending_hashtags');
    expect(tiles.innerHTML).toContain('Key availability (this isolate)');
  });
});
