// Tests for describeChart() — 차트 대체 텍스트 접근성 헬퍼 (ST-05).
// describeChart 는 utils.js 에 정의된 전역 함수로, jsdom 환경에서
// utils.js 를 로드해 직접 호출한다.

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { JSDOM } from 'jsdom';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

const __dirname = dirname(fileURLToPath(import.meta.url));
const utilsSrc = readFileSync(join(__dirname, '..', '..', 'static', 'js', 'utils.js'), 'utf-8');

function setupDom() {
  const dom = new JSDOM('<!DOCTYPE html><html><body></body></html>', {
    url: 'http://localhost/',
    pretendToBeVisual: true,
  });
  const { window } = dom;
  globalThis.window = window;
  globalThis.document = window.document;
  globalThis.navigator = window.navigator;
  // utils.js 내부가 참조할 수 있는 전역들
  for (const k of ['localStorage', 'getComputedStyle']) {
    if (window[k] !== undefined) globalThis[k] = window[k];
  }
  // utils.js 를 window 컨텍스트에서 평가해 전역으로 노출.
  window.eval(utilsSrc);
  return window;
}

test('describeChart: 컨테이너에 role=img 와 aria-label 을 설정한다', () => {
  const window = setupDom();
  const container = window.document.createElement('div');
  // echarts canvas 가 있다고 가정하고 가짜 canvas 추가.
  const canvas = window.document.createElement('canvas');
  container.appendChild(canvas);
  describeChart(container, '포트폴리오 NAV 추이: 최근 30일 +5.2%');
  assert.equal(container.getAttribute('role'), 'img');
  assert.equal(container.getAttribute('aria-label'), '포트폴리오 NAV 추이: 최근 30일 +5.2%');
  // canvas 는 보조기기가 무시하도록 aria-hidden.
  assert.equal(canvas.getAttribute('aria-hidden'), 'true');
});

test('describeChart: rows 옵션으로 숨김 데이터 표를 추가한다 (sr-only)', () => {
  const window = setupDom();
  const container = window.document.createElement('div');
  window.document.body.appendChild(container);
  describeChart(container, '연간 재무 지표', {
    caption: '최근 3년 주가/PER',
    headers: ['연도', '주가', 'PER'],
    rows: [
      ['2024', '50000', '12.3'],
      ['2025', '55000', '11.8'],
    ],
  });
  const table = container.querySelector('.sr-only-chart-table');
  assert.ok(table, '숨김 표가 추가되어야 함');
  assert.equal(table.querySelector('caption').textContent, '최근 3년 주가/PER');
  const ths = [...table.querySelectorAll('th')].map((t) => t.textContent);
  assert.deepEqual(ths, ['연도', '주가', 'PER']);
  const cells = [...table.querySelectorAll('tbody td')].map((t) => t.textContent);
  assert.deepEqual(cells, ['2024', '50000', '12.3', '2025', '55000', '11.8']);
});

test('describeChart: rows=null 이면 기존 숨김 표를 제거한다', () => {
  const window = setupDom();
  const container = window.document.createElement('div');
  window.document.body.appendChild(container);
  describeChart(container, '라벨', {
    headers: ['x'],
    rows: [['1']],
  });
  assert.ok(container.querySelector('.sr-only-chart-table'));
  describeChart(container, '라벨');  // rows 없음
  assert.equal(container.querySelector('.sr-only-chart-table'), null);
});

test('describeChart: 다시 호출하면 기존 숨김 표를 갱신(교체)한다', () => {
  const window = setupDom();
  const container = window.document.createElement('div');
  window.document.body.appendChild(container);
  describeChart(container, '라벨', {
    headers: ['연도'],
    rows: [['2024']],
  });
  describeChart(container, '라벨', {
    headers: ['연도'],
    rows: [['2025'], ['2026']],
  });
  const rows = [...container.querySelectorAll('.sr-only-chart-table tbody td')].map((t) => t.textContent);
  assert.deepEqual(rows, ['2025', '2026']);
});

test('describeChart: 컨테이너가 null 이면 no-op (에러 없음)', () => {
  setupDom();
  assert.doesNotThrow(() => describeChart(null, '라벨'));
});

test('describeChart: XSS 방지 — 셀 값은 textContent 로 주입(HTML 해석 안 됨)', () => {
  const window = setupDom();
  const container = window.document.createElement('div');
  window.document.body.appendChild(container);
  describeChart(container, '라벨', {
    headers: ['x'],
    rows: [['<img src=x onerror=alert(1)>']],
  });
  const table = container.querySelector('.sr-only-chart-table');
  // 스크립트 태그가 DOM 으로 해석되지 않고 텍스트로 들어가야 함.
  assert.equal(table.querySelector('img'), null);
  assert.equal(table.querySelector('tbody td').textContent, '<img src=x onerror=alert(1)>');
});
