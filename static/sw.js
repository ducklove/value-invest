/* Value Compass service worker — PWA v1: installability only (no web push).
 *
 * CACHING STRATEGY — deliberately conservative. Read before changing.
 *
 * The server injects ?v=<commit-hash> into asset URLs at HTML serve time
 * (core/static_routes.py) and serves HTML with Cache-Control: no-cache.
 * A service worker that cached HTML, or stale-while-revalidated assets,
 * would keep serving old ?v= references after a deploy and break cache
 * busting. Therefore:
 *
 * - HTML (navigation requests): network-first, NO HTML caching — ever.
 *   On total network failure we return a tiny inline offline page; there
 *   is intentionally no offline app shell.
 * - /api/*: network-only — the SW never intercepts or caches API responses.
 * - ?v=-stamped static assets: cache-first. These URLs are immutable by
 *   construction (the hash changes on deploy, so a new URL is fetched).
 * - manifest + icons: cache-first (small, safe to refresh via new cache
 *   name when this file changes).
 * - Everything else: passed through to the network untouched.
 *
 * Versioning: /sw.js itself is served with Cache-Control: no-cache, so the
 * browser revalidates it and any byte change triggers the SW update flow.
 * Bump CACHE_NAME when the precache list or strategy changes; activate()
 * deletes old caches.
 */
'use strict';

const CACHE_NAME = 'vc-static-v1';

// Small, stable shell extras worth precaching for the install prompt.
const PRECACHE_URLS = [
  '/manifest.webmanifest',
  '/favicon.svg',
  '/static/icon-640.jpg',
];

const OFFLINE_HTML = '<!DOCTYPE html><html lang="ko"><head><meta charset="UTF-8">'
  + '<meta name="viewport" content="width=device-width, initial-scale=1.0">'
  + '<title>오프라인 — Value Compass</title></head>'
  + '<body style="font-family:-apple-system,BlinkMacSystemFont,\'Segoe UI\',sans-serif;'
  + 'background:#f5f5f5;color:#1a1a1a;display:flex;align-items:center;justify-content:center;'
  + 'min-height:100vh;margin:0;text-align:center;">'
  + '<div><h1 style="font-size:20px;margin-bottom:8px;">오프라인 상태입니다</h1>'
  + '<p style="color:#666;font-size:14px;">네트워크 연결을 확인한 뒤 다시 시도해 주세요.</p></div>'
  + '</body></html>';

self.addEventListener('install', (event) => {
  event.waitUntil(
    caches.open(CACHE_NAME)
      .then((cache) => cache.addAll(PRECACHE_URLS))
      .catch(() => { /* precache is best-effort; never block install */ })
  );
  self.skipWaiting();
});

self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches.keys()
      .then((keys) => Promise.all(
        keys.filter((key) => key !== CACHE_NAME).map((key) => caches.delete(key))
      ))
      .then(() => self.clients.claim())
  );
});

// ?v=<commit-hash> URLs are immutable by construction — safe to cache-first.
function isVersionStampedAsset(url) {
  return url.origin === self.location.origin && url.searchParams.has('v');
}

function isPrecachedShellExtra(url) {
  return url.origin === self.location.origin && PRECACHE_URLS.includes(url.pathname);
}

async function cacheFirst(request) {
  const cached = await caches.match(request);
  if (cached) return cached;
  const response = await fetch(request);
  if (response && response.ok) {
    const cache = await caches.open(CACHE_NAME);
    await cache.put(request, response.clone());
  }
  return response;
}

self.addEventListener('fetch', (event) => {
  const request = event.request;
  if (request.method !== 'GET') return;

  const url = new URL(request.url);

  // /api/*: network-only — never intercepted, never cached.
  if (url.origin === self.location.origin && url.pathname.startsWith('/api/')) return;

  // Version-stamped assets + manifest/icons: cache-first (immutable URLs).
  if (isVersionStampedAsset(url) || isPrecachedShellExtra(url)) {
    event.respondWith(cacheFirst(request));
    return;
  }

  // Navigations (HTML): network-first, NO HTML caching — stale HTML would
  // point at old ?v= assets after a deploy. Offline gets a minimal fallback.
  if (request.mode === 'navigate') {
    event.respondWith(
      fetch(request).catch(() => new Response(OFFLINE_HTML, {
        status: 503,
        headers: { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store' },
      }))
    );
    return;
  }

  // Everything else falls through to the network untouched.
});
