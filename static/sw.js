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
 *   배포마다 URL 이 통째로 바뀌므로 캐시는 최근 MAX_CACHED_ASSETS 개만 남기고,
 *   저장 실패(용량 초과)는 응답에 영향을 주지 않는다 — 아래 cacheFirst 참고.
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

const CACHE_NAME = 'vc-static-v2';

// ?v= URL 은 배포마다 통째로 바뀐다 — 캐시에 계속 쌓기만 하면(배포 1회 = 자산 55개,
// 약 1.1MB) 오리진 저장 용량이 작은 모바일에서 결국 한도를 넘고, 그때부터 cache.put
// 이 실패한다. 그 실패가 응답까지 깨뜨리면 렌더 차단 CSS/JS 가 네트워크 오류로
// 떨어져 흰 화면이 된다. 그래서 (1) 저장 실패는 삼키고 (2) 최근 N개만 남긴다.
const MAX_CACHED_ASSETS = 120; // ≈ 최근 배포 2회분

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

// 오래된 항목부터 버려 캐시 크기를 묶어 둔다(cache.keys() 는 삽입 순서).
async function trimCache(cache) {
  const keys = await cache.keys();
  const excess = keys.length - MAX_CACHED_ASSETS;
  if (excess <= 0) return;
  await Promise.all(keys.slice(0, excess).map((key) => cache.delete(key)));
}

async function cacheFirst(request) {
  const cached = await caches.match(request);
  if (cached) return cached;
  const response = await fetch(request);
  // 캐싱은 부가 기능이다 — 용량 초과 등으로 실패해도 응답은 그대로 돌려준다.
  // (여기서 예외가 새면 respondWith 가 거부되면서 그 자산이 통째로 로드 실패한다.)
  if (response && response.ok) {
    try {
      const cache = await caches.open(CACHE_NAME);
      await cache.put(request, response.clone());
      await trimCache(cache);
    } catch (e) { /* 캐시 저장 실패는 무시 */ }
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
