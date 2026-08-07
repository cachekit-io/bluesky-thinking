export const OPERATIONS = [
  ['posts_per_minute', 'Posts per minute'],
  ['trending_hashtags', 'Trending hashtags'],
  ['trending_links', 'Trending links'],
  ['lang_mix', 'Language mix'],
  ['top_emoji', 'Top emoji'],
];

// This matches the live verification check in stage4/verify.sh. The ingester
// refreshes often enough that five minutes old is a useful, visible warning.
export const STALE_AFTER_SECONDS = 300;
export const WINDOWS = new Set(['5m', '1h', '24h']);

const esc = (value) =>
  String(value).replace(
    /[&<>\"]/g,
    (character) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' })[character],
  );

const fmt = (value) =>
  Intl.NumberFormat('en', {
    notation: value >= 10000 ? 'compact' : 'standard',
    maximumFractionDigits: 2,
  }).format(value);

const isNumber = (value) => typeof value === 'number' && Number.isFinite(value);

function rankedRows(items, nameKey, valueKey, { percent = false, links = false } = {}) {
  const rows = items
    .filter((item) => item && typeof item[nameKey] === 'string' && isNumber(item[valueKey]))
    .sort((a, b) => b[valueKey] - a[valueKey])
    .slice(0, 10);
  if (!rows.length) return null;

  const max = Math.max(rows[0][valueKey], 1);
  return `<div class="rows" aria-label="Ranked results">${rows
    .map((item, index) => {
      const name = item[nameKey];
      const value = Math.max(item[valueKey], 0);
      const label = percent ? `${(value * 100).toFixed(1)}%` : fmt(value);
      const display = links
        ? externalLink(name)
        : `<span class="name" title="${esc(name)}">${esc(name)}</span>`;
      return `<div class="row">
        <span class="rank" aria-label="Rank ${index + 1}">${index + 1}</span>
        ${display}
        <span class="bar-track" aria-hidden="true"><span class="bar" style="width:${Math.max(1, Math.min(100, (value / max) * 100))}%"></span></span>
        <span class="num">${label}</span>
      </div>`;
    })
    .join('')}</div>`;
}

function languageRows(langs) {
  if (!langs || typeof langs !== 'object' || Array.isArray(langs)) return null;
  return rankedRows(
    Object.entries(langs).map(([lang, share]) => ({ lang, share })),
    'lang',
    'share',
    { percent: true },
  );
}

function externalLink(uri) {
  try {
    const url = new URL(uri);
    if (url.protocol !== 'https:' && url.protocol !== 'http:') throw new Error('unsafe protocol');
    const escaped = esc(url.href);
    return `<a class="name" href="${escaped}" target="_blank" rel="noopener noreferrer" title="${escaped}" aria-label="${escaped} (opens in a new tab)">${escaped}</a>`;
  } catch {
    return `<span class="name" title="${esc(uri)}">${esc(uri)}</span>`;
  }
}

/** Render only the documented live payload shapes; never inspect metadata as data. */
export function renderOperation(operation, data) {
  if (!data || typeof data !== 'object' || Array.isArray(data)) return renderMalformed();

  switch (operation) {
    case 'posts_per_minute':
      if (!isNumber(data.ppm)) return renderMalformed();
      return `<p class="hero">${fmt(data.ppm)} <small>posts/minute</small></p>${sampleSize(data)}`;
    case 'trending_hashtags':
      return rankingOrEmpty(
        rankedRows(data.hashtags ?? [], 'tag', 'count'),
        'No hashtags in this window yet.',
      );
    case 'trending_links':
      return rankingOrEmpty(
        rankedRows(data.links ?? [], 'uri', 'count', { links: true }),
        'No external links in this window yet.',
      );
    case 'lang_mix':
      return rankingOrEmpty(
        languageRows(data.langs),
        'No language mix is available for this window yet.',
      );
    case 'top_emoji':
      return rankingOrEmpty(
        rankedRows(data.emoji ?? [], 'emoji', 'count'),
        'No emoji in this window yet.',
      );
    default:
      return renderMalformed();
  }
}

function sampleSize(data) {
  return isNumber(data.total_posts)
    ? `<p class="meta sample-size">${fmt(data.total_posts)} posts sampled</p>`
    : '';
}

function rankingOrEmpty(rows, emptyMessage) {
  return rows ?? `<p class="empty">${emptyMessage}</p>`;
}

function renderMalformed() {
  return '<p class="error">This aggregate has an unexpected payload shape. Try refreshing; if it persists, the ingester needs attention.</p>';
}

export function formatGeneratedAt(value, now = Date.now()) {
  if (!isNumber(value)) return '';
  const date = new Date(value * 1000);
  if (Number.isNaN(date.valueOf())) return '';
  const absolute = date.toISOString();
  const local = new Intl.DateTimeFormat(undefined, {
    dateStyle: 'medium',
    timeStyle: 'medium',
  }).format(date);
  const stale = now - date.valueOf() > STALE_AFTER_SECONDS * 1000;
  return `<time datetime="${absolute}" title="${absolute}">as of ${esc(local)}</time>${stale ? ' <span class="stale" role="status">Stale — older than 5 minutes.</span>' : ''}`;
}

function errorMessage(error) {
  switch (error) {
    case 'decode_error':
      return 'The cached aggregate could not be decoded. The ingester should republish it.';
    case 'backend_error':
      return 'The cache backend could not be reached. Please try again shortly.';
    case 'integrity_check_failed':
      return 'The integrity check rejected this aggregate. It will not be displayed.';
    default:
      return error || 'The aggregate request failed. Please try again shortly.';
  }
}

export function renderCardMarkup(title, state, selectedWindow, now = Date.now()) {
  const badge = { HIT: 'hit', MISS: 'miss', ERR: 'err' }[state.cache] ?? 'err';
  const timestamp = formatGeneratedAt(state.data?.generated_at, now);
  const meta = `window ${esc(selectedWindow)}${timestamp ? ` · ${timestamp}` : ''}`;
  let content;
  if (state.error) content = `<p class="error">${esc(errorMessage(state.error))}</p>`;
  else if (state.cache === 'MISS')
    content =
      '<p class="empty">No cached aggregate for this window yet — the ingester is still collecting posts.</p>';
  else content = renderOperation(state.operation, state.data);
  const hotpath =
    state.hotpath === 'unavailable'
      ? '<p class="warning">Integrity verification is temporarily unavailable; this is live cache data, but it is unverified.</p>'
      : '';
  return `<h2>${esc(title)} <span class="badge ${badge}">${esc(state.cache)}</span></h2>
    <p class="meta">${meta}</p>${hotpath}${content}`;
}

export function windowFromSearch(search) {
  const selected = new URLSearchParams(search).get('window');
  return WINDOWS.has(selected) ? selected : '5m';
}

function setWindowInUrl(browserWindow, selectedWindow) {
  const url = new URL(browserWindow.location.href);
  url.searchParams.set('window', selectedWindow);
  browserWindow.history.replaceState({}, '', url);
}

export function initDashboard(document, browserWindow) {
  let currentWindow = windowFromSearch(browserWindow.location.search);
  let refreshVersion = 0;
  const grid = document.getElementById('grid');
  const tiles = document.getElementById('tiles');
  const buttons = [...document.querySelectorAll('.windows button')];

  function selectWindow(next) {
    currentWindow = next;
    setWindowInUrl(browserWindow, next);
    for (const button of buttons)
      button.setAttribute('aria-pressed', String(button.dataset.window === next));
  }

  async function loadOperation(element, operation, title, selectedWindow, version) {
    try {
      const response = await fetch(
        `/api/${operation}?window=${encodeURIComponent(selectedWindow)}`,
      );
      const body = await response.json();
      if (version !== refreshVersion) return;
      if (response.ok) {
        element.innerHTML = renderCardMarkup(
          title,
          {
            cache: response.headers.get('x-cache') ?? 'HIT',
            operation,
            data: body.data,
            hotpath: response.headers.get('x-hotpath'),
          },
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
        { cache: 'ERR', operation, error: body.error || body.detail || `HTTP ${response.status}` },
        selectedWindow,
      );
    } catch {
      if (version !== refreshVersion) return;
      element.innerHTML = renderCardMarkup(
        title,
        { cache: 'ERR', operation, error: 'network_error' },
        selectedWindow,
      );
    }
  }

  async function loadStats(version) {
    try {
      const response = await fetch('/api/stats');
      const stats = await response.json();
      if (version !== refreshVersion) return;
      const rate = stats.hit_rate === null ? '—' : `${(stats.hit_rate * 100).toFixed(1)}%`;
      tiles.innerHTML = [
        ['Cache hit rate', rate],
        ['Hits', fmt(stats.hits)],
        ['Misses', fmt(stats.misses)],
        ['Errors', fmt(stats.errors)],
      ]
        .map(
          ([label, value]) =>
            `<div class="tile"><div class="label">${label}</div><div class="value">${value}</div></div>`,
        )
        .join('');
    } catch {
      if (version === refreshVersion)
        tiles.innerHTML =
          '<div class="tile"><div class="label">Stats unavailable</div><div class="value">—</div></div>';
    }
  }

  function refresh() {
    const version = ++refreshVersion;
    const selectedWindow = currentWindow;
    if (!grid.children.length)
      grid.innerHTML = OPERATIONS.map(
        ([operation]) => `<section class="card" id="card-${operation}"></section>`,
      ).join('');
    loadStats(version);
    for (const [operation, title] of OPERATIONS)
      loadOperation(
        document.getElementById(`card-${operation}`),
        operation,
        title,
        selectedWindow,
        version,
      );
  }

  document.querySelector('.windows').addEventListener('click', (event) => {
    const button = event.target.closest('button[data-window]');
    if (!button || !WINDOWS.has(button.dataset.window)) return;
    selectWindow(button.dataset.window);
    refresh();
  });

  selectWindow(currentWindow);
  refresh();
  browserWindow.setInterval(refresh, 30_000);
}
