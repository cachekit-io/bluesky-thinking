/** History panel renderer (LAB-1616): gaps stay visible, coverage stays honest. */
import { describe, expect, it } from 'vitest';
import { renderHistoryMarkup } from '../public/dashboard.js';

const FROM = 1_786_100_400; // arbitrary hour boundary
const PERIOD = 3600;

function response(pointCount: number, expected = 168, overrides: Record<string, unknown> = {}) {
  const points = Array.from({ length: pointCount }, (_, i) => ({
    bucket_ts: FROM + (i + 1) * PERIOD,
    generated_at: FROM + (i + 1) * PERIOD - 20,
    normalization_version: 'skyline-normalization-v1',
    data: { window: '1h', ppm: 100 + i },
  }));
  return {
    operation: 'posts_per_minute',
    range: '7d',
    tier: 'hourly',
    period_seconds: PERIOD,
    normalization_versions: ['skyline-normalization-v1'],
    coverage: {
      from: FROM,
      to: FROM + expected * PERIOD,
      expected_points: expected,
      present_points: pointCount,
      history_started_at: FROM + PERIOD,
    },
    points,
    ...overrides,
  };
}

describe('renderHistoryMarkup', () => {
  it('renders one bar per present point — a missing bucket is a hole, never a zero', () => {
    const markup = renderHistoryMarkup(response(42), '7d');
    expect(markup.match(/<rect /g)).toHaveLength(42);
    expect(markup).toContain('42 of 168 hourly points in range');
    expect(markup).toContain('missing points are gaps in collection, not zero activity');
  });

  it('drops the incompleteness warning when coverage is full', () => {
    const markup = renderHistoryMarkup(response(168), '7d');
    expect(markup).toContain('168 of 168 hourly points in range');
    expect(markup).not.toContain('missing points are gaps');
  });

  it('states when history began, forward-only', () => {
    const markup = renderHistoryMarkup(response(3), '7d');
    expect(markup).toContain('History since');
    expect(markup).toContain('forward-only, nothing is backfilled');
  });

  it('says so plainly when no history exists yet', () => {
    const markup = renderHistoryMarkup(
      response(0, 168, {
        coverage: {
          from: FROM,
          to: FROM,
          expected_points: 168,
          present_points: 0,
          history_started_at: null,
        },
      }),
      '7d',
    );
    expect(markup).toContain('No history captured yet');
    expect(markup).not.toContain('<svg');
  });

  it('flags a range that spans normalization versions instead of blending silently', () => {
    const markup = renderHistoryMarkup(
      response(4, 168, {
        normalization_versions: ['skyline-normalization-v1', 'skyline-normalization-v2'],
      }),
      '7d',
    );
    expect(markup).toContain('spans 2 normalization versions');
  });

  it('renders an error state for a failed request, never fake bars', () => {
    const markup = renderHistoryMarkup(null, '7d');
    expect(markup).toContain('history request failed');
    expect(markup).not.toContain('<rect');
  });

  it('ships a data table alongside the chart', () => {
    const markup = renderHistoryMarkup(response(2), '7d');
    expect(markup).toContain('<details class="history-table">');
    expect(markup.match(/<tr><td>/g)).toHaveLength(2);
  });
});
