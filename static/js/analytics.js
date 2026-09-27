/* Shared GA4 bootstrap. Source: value-invest/static/js/analytics.js.
 * Sync copies with: node scripts/sync-analytics.mjs --write
 */
(function () {
  'use strict';
  const id = 'G-KE611DTCFZ';
  const project = document.currentScript?.dataset.project;
  const projects = ['value-invest', 'spac-hunter', 'holding_value',
    'common_preferred_spread', 'gold_gap', 'nps-tracker', 'eiayn',
    'buybacks', 'bond-mate', 'index-popup', 'all-about-gold'];
  // Keep existing app event callers safe on local, embedded and redirect pages.
  window.trackEvent = window.trackEvent || function () {};
  const location = window.location;
  const pages = location.hostname === 'ducklove.github.io'
    && location.pathname.split('/')[1] === project;
  const server = location.hostname === 'ducklove.duckdns.org'
    && ((project === 'value-invest' && location.port === '3691')
      || (project === 'index-popup' && location.port === '3358'));
  if (!projects.includes(project) || location.protocol !== 'https:'
    || !(pages || server) || window.self !== window.top
    || window['ga-disable-' + id] || window.__valueAnalyticsInitialized
    || (typeof SHOULD_REDIRECT_TO_APP_SERVER !== 'undefined'
      && SHOULD_REDIRECT_TO_APP_SERVER)) return;
  window.__valueAnalyticsInitialized = true;
  window.dataLayer = window.dataLayer || [];
  window.gtag = function () { window.dataLayer.push(arguments); };
  const gtag = window.gtag;
  function cleanUrl(value) {
    try {
      const url = new URL(value);
      return /^https?:$/.test(url.protocol) ? url.origin + url.pathname : '';
    } catch { return ''; }
  }
  let previous = cleanUrl(document.referrer);
  let lastPath;
  function pageView() {
    if (location.pathname === lastPath) return;
    lastPath = location.pathname;
    const url = cleanUrl(location.href);
    const params = { page_location: url, page_referrer: previous,
      page_title: project, content_group: project };
    // Also sanitize context inherited by subsequent events (queries can contain
    // search terms or auth redirects). History measurement is disabled in GA4.
    gtag('set', params);
    gtag('event', 'page_view', params);
    previous = url;
  }
  gtag('set', 'linker', { domains: ['ducklove.github.io', 'ducklove.duckdns.org'] });
  gtag('set', { page_location: cleanUrl(location.href), page_referrer: previous,
    page_title: project, content_group: project });
  gtag('js', new Date());
  gtag('config', id, { send_page_view: false, content_group: project,
    allow_google_signals: false,
    allow_ad_personalization_signals: false });
  window.trackEvent = function (name, params = {}) {
    gtag('event', name, { ...params, content_group: project });
  };
  pageView();
  for (const method of ['pushState', 'replaceState']) {
    const original = window.history[method];
    window.history[method] = function (...args) {
      const result = original.apply(this, args);
      pageView();
      return result;
    };
  }
  window.addEventListener('popstate', pageView);
  const script = document.createElement('script');
  script.async = true;
  script.src = 'https://www.googletagmanager.com/gtag/js?id=' + id;
  document.head.appendChild(script);
}());
