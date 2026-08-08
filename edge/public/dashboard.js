// @ts-check

/** @type {[string, string][]} */
export const OPERATIONS = [
  ['posts_per_minute', 'Posts per minute'],
  ['trending_hashtags', 'Trending hashtags'],
  ['trending_links', 'Trending links'],
  ['lang_mix', 'Language mix'],
  ['top_emoji', 'Top emoji'],
];

/** @type {Record<string, number>} */
export const FRESHNESS_SECONDS = { '5m': 60, '1h': 300, '24h': 900 };
export const WINDOWS = new Set(['5m', '1h', '24h']);

/** @typedef {Record<string, unknown>} AggregatePayload */
/** @typedef {'HIT' | 'MISS' | 'ERR'} CacheStatus */
/** @typedef {{ cache: CacheStatus, operation: string, data?: AggregatePayload, hotpath?: string | null, error?: string }} CardState */

/** @param {unknown} value */
function esc(value) {
  return String(value).replace(
    /[&<>"]/g,
    (character) =>
      ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' })[character] ?? character,
  );
}

/** @param {number} value */
function fmt(value) {
  return Intl.NumberFormat('en', {
    notation: value >= 10000 ? 'compact' : 'standard',
    maximumFractionDigits: 2,
  }).format(value);
}

/** @param {unknown} value @returns {value is number} */
function isNumber(value) {
  return typeof value === 'number' && Number.isFinite(value);
}

/** @param {unknown} value @returns {value is AggregatePayload} */
function isAggregatePayload(value) {
  return Boolean(value) && typeof value === 'object' && !Array.isArray(value);
}

/** @param {number} seconds */
function freshnessLabel(seconds) {
  const minutes = seconds / 60;
  return `${minutes} minute${minutes === 1 ? '' : 's'}`;
}

/** @param {unknown[]} items @param {string} nameKey @param {string} valueKey @param {{ percent?: boolean, links?: boolean }} [options] */
function rankedRows(items, nameKey, valueKey, { percent = false, links = false } = {}) {
  const rows = items
    .flatMap((item) => {
      if (!isAggregatePayload(item)) return [];
      const name = item[nameKey];
      const value = item[valueKey];
      return typeof name === 'string' && isNumber(value) ? [{ name, value }] : [];
    })
    .sort((a, b) => b.value - a.value)
    .slice(0, 10);
  const firstRow = rows[0];
  if (!firstRow) return null;

  const max = Math.max(firstRow.value, 1);
  return `<ol class="rows">${rows
    .map((item) => {
      const { name } = item;
      const value = Math.max(item.value, 0);
      const label = percent ? `${(value * 100).toFixed(1)}%` : fmt(value);
      const display = links
        ? externalLink(name)
        : `<span class="name" title="${esc(name)}">${esc(name)}</span>`;
      return `<li class="row">
        ${display}
        <span class="bar-track" aria-hidden="true"><span class="bar" style="width:${Math.max(1, Math.min(100, (value / max) * 100))}%"></span></span>
        <span class="num">${label}</span>
      </li>`;
    })
    .join('')}</ol>`;
}

/** @param {AggregatePayload} langs */
function languageRows(langs) {
  return rankedRows(
    Object.entries(langs).map(([lang, share]) => ({
      lang: lang === 'other' ? 'Other languages' : lang,
      share,
    })),
    'lang',
    'share',
    { percent: true },
  );
}

/** @param {string} uri */
function externalLink(uri) {
  try {
    const url = new URL(uri);
    if (url.protocol !== 'https:' && url.protocol !== 'http:') throw new Error('unsafe protocol');
    const escaped = esc(url.href);
    return `<a class="name" href="${escaped}" target="_blank" rel="noopener noreferrer" title="${escaped}">${escaped}</a>`;
  } catch {
    return `<span class="name" title="${esc(uri)}">${esc(uri)}</span>`;
  }
}

/** Render only the documented live payload shapes; never inspect metadata as data. */
/** @param {string} operation @param {unknown} data */
export function renderOperation(operation, data) {
  if (!isAggregatePayload(data)) return renderMalformed();

  switch (operation) {
    case 'posts_per_minute':
      if (!isNumber(data.ppm)) return renderMalformed();
      return `<p class="hero">${fmt(data.ppm)} <small>posts/minute</small></p>${sampleSize(data)}`;
    case 'trending_hashtags':
      if (!Array.isArray(data.hashtags)) return renderMalformed();
      return rankingOrEmpty(
        rankedRows(data.hashtags, 'tag', 'count'),
        'No hashtags in this window yet.',
      );
    case 'trending_links':
      if (!Array.isArray(data.links)) return renderMalformed();
      return rankingOrEmpty(
        rankedRows(data.links, 'uri', 'count', { links: true }),
        'No external links in this window yet.',
      );
    case 'lang_mix':
      if (!isAggregatePayload(data.langs)) return renderMalformed();
      return rankingOrEmpty(
        languageRows(data.langs),
        'No language mix is available for this window yet.',
      );
    case 'top_emoji':
      if (!Array.isArray(data.emoji)) return renderMalformed();
      return rankingOrEmpty(
        rankedRows(data.emoji, 'emoji', 'count'),
        'No emoji in this window yet.',
      );
    default:
      return renderMalformed();
  }
}

/** @param {AggregatePayload} data */
function sampleSize(data) {
  return isNumber(data.total_posts)
    ? `<p class="meta">${fmt(data.total_posts)} posts sampled</p>`
    : '';
}

/** @param {string | null} rows @param {string} emptyMessage */
function rankingOrEmpty(rows, emptyMessage) {
  return rows ?? `<p class="empty">${emptyMessage}</p>`;
}

function renderMalformed() {
  return '<p class="error">This aggregate has an unexpected payload shape. Try refreshing; if it persists, the ingester needs attention.</p>';
}

/** @param {unknown} value @param {string} selectedWindow @param {number} [now] */
export function formatGeneratedAt(value, selectedWindow, now = Date.now()) {
  if (!isNumber(value)) return '';
  const date = new Date(value * 1000);
  if (Number.isNaN(date.valueOf())) return '';
  const absolute = date.toISOString();
  const local = new Intl.DateTimeFormat(undefined, {
    dateStyle: 'medium',
    timeStyle: 'medium',
  }).format(date);
  const freshness = FRESHNESS_SECONDS[selectedWindow] ?? FRESHNESS_SECONDS['5m'] ?? 60;
  const stale = now - date.valueOf() > freshness * 1000;
  const warning = stale
    ? ` <span class="stale">Stale — older than ${freshnessLabel(freshness)}.</span>`
    : '';
  return `<time datetime="${absolute}" title="${absolute}">as of ${esc(local)}</time>${warning}`;
}

/** @param {string} error */
function errorMessage(error) {
  switch (error) {
    case 'decode_error':
      return 'The cached aggregate could not be decoded. The ingester should republish it.';
    case 'backend_error':
      return 'The cache backend could not be reached. Please try again shortly.';
    case 'integrity_check_failed':
      return 'The integrity check rejected this aggregate. It will not be displayed.';
    case 'network_error':
      return 'The network request failed. Check your connection and try again.';
    case 'not_configured':
      return 'The Skyline API is not configured yet. Please try again later.';
    case 'invalid_window':
      return 'The selected time window is invalid. Choose 5m, 1h, or 24h.';
    case 'cache_status_unknown':
      return 'The server did not confirm the cache status, so this aggregate is not displayed.';
    default:
      return 'The aggregate request failed. Please try again shortly.';
  }
}

/** @param {string} title @param {CardState} state @param {string} selectedWindow */
export function renderCardMarkup(title, state, selectedWindow) {
  /** @type {Record<string, string>} */
  const badgeClasses = { HIT: 'hit', MISS: 'miss', ERR: 'err' };
  const badge = badgeClasses[state.cache] ?? 'err';
  const timestamp = formatGeneratedAt(state.data?.generated_at, selectedWindow);
  const meta = `window ${esc(selectedWindow)}${timestamp ? ` · ${timestamp}` : ''}`;
  let content;
  if (state.error) content = `<p class="error">${esc(errorMessage(state.error))}</p>`;
  else if (state.cache === 'MISS')
    content =
      '<p class="empty">No cached aggregate for this window yet — the ingester is still collecting posts.</p>';
  else content = renderOperation(state.operation, state.data);
  const hotpath =
    !state.error && state.cache === 'HIT' && state.hotpath !== 'verified'
      ? '<p class="warning">Integrity verification is temporarily unavailable; this is live cache data, but it is unverified.</p>'
      : '';
  return `<h2>${esc(title)} <span class="badge ${badge}">${esc(state.cache)}</span></h2><p class="meta">${meta}</p>${hotpath}${content}`;
}

/** @param {string} search */
export function windowFromSearch(search) {
  const selected = new URLSearchParams(search).get('window');
  return selected && WINDOWS.has(selected) ? selected : '5m';
}

/** @param {string} selectedWindow */
function setWindowInUrl(selectedWindow) {
  const url = new URL(window.location.href);
  url.searchParams.set('window', selectedWindow);
  window.history.replaceState({}, '', url);
}

function initDashboard() {
  let currentWindow = windowFromSearch(window.location.search);
  let refreshVersion = 0;
  const grid = document.getElementById('grid');
  const tiles = document.getElementById('tiles');
  const windowControl = document.querySelector('.windows');
  if (!grid || !tiles || !windowControl) return;
  const dashboardGrid = grid;
  const dashboardTiles = tiles;
  /** @type {HTMLButtonElement[]} */
  const buttons = /** @type {HTMLButtonElement[]} */ (
    Array.from(document.querySelectorAll('.windows button'))
  );

  /** @param {string} next */
  function selectWindow(next) {
    currentWindow = next;
    setWindowInUrl(next);
    for (const button of buttons)
      button.setAttribute('aria-pressed', String(button.dataset.window === next));
  }

  /** @param {HTMLElement} element @param {string} operation @param {string} title @param {string} selectedWindow @param {number} version */
  async function loadOperation(element, operation, title, selectedWindow, version) {
    try {
      const response = await fetch(
        `/api/${operation}?window=${encodeURIComponent(selectedWindow)}`,
      );
      const body = /** @type {{ data?: unknown, error?: unknown, detail?: unknown }} */ (
        await response.json()
      );
      if (version !== refreshVersion) return;
      if (response.ok) {
        const cache = response.headers.get('x-cache');
        element.innerHTML = renderCardMarkup(
          title,
          cache === 'HIT'
            ? {
                cache: 'HIT',
                operation,
                data: isAggregatePayload(body.data) ? body.data : undefined,
                hotpath: response.headers.get('x-hotpath'),
              }
            : { cache: 'ERR', operation, error: 'cache_status_unknown' },
          selectedWindow,
        );
        return;
      }
      if (response.status === 404 && body.error === 'miss') {
        element.innerHTML = renderCardMarkup(title, { cache: 'MISS', operation }, selectedWindow);
        return;
      }
      element.innerHTML = renderCardMarkup(
        title,
        {
          cache: 'ERR',
          operation,
          error:
            typeof body.error === 'string'
              ? body.error
              : typeof body.detail === 'string'
                ? body.detail
                : 'internal',
        },
        selectedWindow,
      );
    } catch (error) {
      console.error(`skyline dashboard: ${operation} failed`, error);
      if (version !== refreshVersion) return;
      element.innerHTML = renderCardMarkup(
        title,
        { cache: 'ERR', operation, error: 'network_error' },
        selectedWindow,
      );
    }
  }

  /** @param {number} version */
  async function loadStats(version) {
    try {
      const response = await fetch('/api/stats');
      const stats = await response.json();
      if (
        !response.ok ||
        !isNumber(stats.hits) ||
        !isNumber(stats.misses) ||
        !isNumber(stats.errors)
      )
        throw new Error('stats unavailable');
      if (version !== refreshVersion) return;
      const rate = stats.hit_rate === null ? '—' : `${(stats.hit_rate * 100).toFixed(1)}%`;
      dashboardTiles.innerHTML = [
        ['Cache hit rate', rate],
        ['Hits', fmt(stats.hits)],
        ['Misses', fmt(stats.misses)],
        ['Errors', fmt(stats.errors)],
      ]
        .map(
          ([label, item]) =>
            `<div class="tile"><div class="label">${label}</div><div class="value">${item}</div></div>`,
        )
        .join('');
    } catch (error) {
      console.error('skyline dashboard: stats failed', error);
      if (version === refreshVersion)
        dashboardTiles.innerHTML =
          '<div class="tile"><div class="label">Stats unavailable</div><div class="value">—</div></div>';
    }
  }

  function refresh() {
    const version = ++refreshVersion;
    const selectedWindow = currentWindow;
    if (!dashboardGrid.children.length)
      dashboardGrid.innerHTML = OPERATIONS.map(
        ([operation]) => `<section class="card" id="card-${operation}"></section>`,
      ).join('');
    loadStats(version);
    for (const [operation, title] of OPERATIONS) {
      const card = document.getElementById(`card-${operation}`);
      if (card) loadOperation(card, operation, title, selectedWindow, version);
    }
  }

  windowControl.addEventListener('click', (event) => {
    if (!(event.target instanceof Element)) return;
    const button = event.target.closest('button[data-window]');
    const next = button instanceof HTMLButtonElement ? button.dataset.window : undefined;
    if (!next || !WINDOWS.has(next)) return;
    selectWindow(next);
    refresh();
  });

  selectWindow(currentWindow);
  refresh();
  window.setInterval(refresh, 30_000);
}

if (typeof document !== 'undefined') initDashboard();
