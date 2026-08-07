import { describe, expect, it } from 'vitest';
import {
  STALE_AFTER_SECONDS,
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

    expect(markup).toContain('cachekit');
    expect(markup).toContain('Rank 1');
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

  it('shows localized time and a stale warning after the documented five-minute threshold', () => {
    const now = (generated_at + STALE_AFTER_SECONDS + 1) * 1000;
    const markup = formatGeneratedAt(generated_at, now);

    expect(markup).toContain('datetime="2025-07-');
    expect(markup).toContain('Stale — older than 5 minutes.');
  });

  it('keeps error states actionable and distinguishes unavailable verification', () => {
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
        { cache: 'HIT', operation: 'trending_links', data: { links: [] }, hotpath: 'unavailable' },
        '5m',
      ),
    ).toContain('unverified');
  });

  it('reads only the supported window from the URL', () => {
    expect(windowFromSearch('?window=24h')).toBe('24h');
    expect(windowFromSearch('?window=7d')).toBe('5m');
  });
});
