import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { JSDOM } from 'jsdom';

const source = readFileSync(new URL('../../static/js/analytics.js', import.meta.url), 'utf8');
const projects = JSON.parse(readFileSync(new URL('../../config/analytics-projects.json', import.meta.url)));
function setup(project, url, { redirect = false, embedded = false } = {}) {
  const dom = new JSDOM('<!doctype html><head></head>', {
    url, referrer: 'https://example.org/source?private=secret#token', runScripts: 'outside-only',
  });
  const { window } = dom;
  const script = window.document.createElement('script');
  script.dataset.project = project;
  Object.defineProperty(window.document, 'currentScript', { value: script });
  if (embedded) Object.defineProperty(window, 'self', { value: {} });
  window.SHOULD_REDIRECT_TO_APP_SERVER = redirect;
  window.eval(source);
  return { window, close: () => window.close(), commands: () => Array.from(window.dataLayer || [], a => Array.from(a)) };
}

test('all projects use one stream and their own content group, without URL queries', () => {
  for (const { project } of projects) {
    const s = setup(project, `https://ducklove.github.io/${project}/?account=secret#token`);
    try {
      const config = s.commands().filter(c => c[0] === 'config');
      const views = s.commands().filter(c => c[1] === 'page_view');
      assert.equal(config.length, 1);
      assert.equal(config[0][1], 'G-KE611DTCFZ');
      assert.equal(config[0][2].send_page_view, false);
      assert.equal(views.length, 1);
      assert.equal(views[0][2].content_group, project);
      assert.equal(views[0][2].page_location, `https://ducklove.github.io/${project}/`);
      assert.equal(views[0][2].page_referrer, 'https://example.org/source');
      assert.equal(JSON.stringify(s.commands()).includes('secret'), false);
      assert.equal(s.window.document.querySelectorAll('script[src*="googletagmanager"]').length, 1);
      s.window.eval(source);
      assert.equal(s.commands().filter(c => c[1] === 'page_view').length, 1);
    } finally { s.close(); }
  }
});

test('local, preview, mismatched project, embedded and redirect pages do not collect', () => {
  for (const [url, options] of [
    ['http://localhost:5173/value-invest/', {}],
    ['https://preview.example/value-invest/', {}],
    ['https://ducklove.github.io/spac-hunter/', {}],
    ['https://ducklove.github.io/value-invest/', { redirect: true }],
    ['https://ducklove.duckdns.org:3691/', { embedded: true }],
  ]) {
    const s = setup('value-invest', url, options);
    try {
      s.window.trackEvent('app_ready');
      assert.equal(s.commands().length, 0);
      assert.equal(s.window.document.querySelectorAll('script[src]').length, 0);
    } finally { s.close(); }
  }
});

test('SPA routes count once while filter changes do not create page views', () => {
  const s = setup('value-invest', 'https://ducklove.duckdns.org:3691/');
  try {
    s.window.history.replaceState({}, '', '/?filter=a');
    s.window.history.pushState({}, '', '/portfolio?account=secret');
    s.window.history.replaceState({}, '', '/portfolio#tab');
    s.window.dispatchEvent(new s.window.PopStateEvent('popstate'));
    s.window.history.pushState({}, '', '/quant');
    const views = s.commands().filter(c => c[1] === 'page_view');
    assert.equal(views.length, 3);
    assert.equal(views[1][2].page_referrer, 'https://ducklove.duckdns.org:3691/');
    assert.equal(views[2][2].page_location, 'https://ducklove.duckdns.org:3691/quant');
    s.window.trackEvent('theme_toggle', { theme: 'dark' });
    assert.equal(s.commands().at(-1)[2].content_group, 'value-invest');
    assert.equal(s.commands().at(-1)[2].theme, 'dark');
    assert.equal(JSON.stringify(s.commands()).includes('secret'), false);
  } finally { s.close(); }
});

test('standalone market widget is collected on its production port', () => {
  const s = setup('index-popup', 'https://ducklove.duckdns.org:3358/');
  try { assert.equal(s.commands().filter(c => c[1] === 'page_view').length, 1); }
  finally { s.close(); }
});
