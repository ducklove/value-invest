// 생태계 연결 — 허브 쪽 레지스트리 헬퍼 (window.APP_CONFIG.ecosystem).
//
// 정본은 config/ecosystem.json 이고, 서버가 공개 항목만 /app-config.js 의
// APP_CONFIG.ecosystem 으로 내려준다(core/ecosystem.public_projection). 이 파일은
// 그 투영에서 형제 대시보드 링크(stockLink/viewLink/embed 템플릿), 도구 카드,
// iframe 메시지 브리지를 만든다. 레지스트리가 없으면(옛 app-config, 테스트 하니스)
// 모든 함수가 null/빈 값을 돌려주고 호출부는 기존 상수로 폴백한다.
//
// 로드 순서: utils.js(escapeHtml·buildApiUrl·portfolioIntegrationHref) 다음,
// search.js(테마 토글이 ecoRefreshLinks·프레임 동기화를 부른다) 앞 — index.html 계약.
// 다른 파일은 이 파일의 함수를 항상 `typeof fn === 'function'` 으로 확인하고 쓴다.

'use strict';

const ECO_HUB_ID = 'value-invest';
const ECO_FRAME_HEIGHT_MIN = 200;
const ECO_FRAME_HEIGHT_MAX = 20000;
const ECO_CODE_RE = /^[0-9A-Z]{6}$/;   // 허브 분석이 받는 종목코드 형식

function ecoRegistry() {
  const cfg = (typeof window !== 'undefined' && window.APP_CONFIG) || {};
  const eco = cfg.ecosystem;
  return eco && Array.isArray(eco.tools) ? eco : null;
}

function ecoTool(id) {
  const reg = ecoRegistry();
  if (!reg || !id) return null;
  return reg.tools.find((t) => t && t.id === id) || null;
}

function ecoTheme() {
  return document.documentElement.getAttribute('data-theme') === 'dark' ? 'dark' : 'light';
}

// 형제 대시보드(허브 자신과 허브 내부 화면 제외) — labs '연결 대시보드' 카드 목록.
function ecoSiblingTools() {
  const reg = ecoRegistry();
  if (!reg) return [];
  return reg.tools.filter((t) => t && t.id !== ECO_HUB_ID && t.deploy !== 'hub' && /^https?:\/\//.test(String(t.url || '')));
}

function _ecoAccepts(link, value) {
  if (!link || typeof link.template !== 'string' || !value) return false;
  try { return new RegExp(link.accepts).test(value); } catch (e) { return false; }
}

function _ecoFill(template, name, value) {
  return template.split('{' + name + '}').join(encodeURIComponent(value));
}

/**
 * 도구의 딥링크 조각(예: '?code=005930', '?stock=005930', '#gold-history').
 * null  = 레지스트리/도구가 없다(호출부가 기존 규칙으로 폴백).
 * ''    = 도구는 있지만 이 값을 받지 않는다(도구 홈으로).
 */
function ecoDeepPath(id, field, value) {
  const tool = ecoTool(id);
  if (!tool) return null;
  const name = field === 'stockLink' ? 'code' : field === 'viewLink' ? 'view' : 'asset';
  const v = field === 'stockLink' ? String(value || '').trim().toUpperCase() : String(value || '');
  const link = tool[field];
  return _ecoAccepts(link, v) ? _ecoFill(link.template, name, v) : '';
}

function ecoStockPath(id, code) {
  return ecoDeepPath(id, 'stockLink', code);
}

function _ecoHomeUrl(tool) {
  const url = String(tool.url || '');
  return tool.deploy === 'github-pages' && !/\/$/.test(url) ? url + '/' : url;
}

/** 도구로 가는 절대 URL — 딥링크(맞을 때만) + ?theme + ?from=value-invest. http(s) 밖은 null. */
function ecoToolUrl(id, vars = {}, { from = ECO_HUB_ID } = {}) {
  const tool = ecoTool(id);
  if (!tool) return null;
  const base = _ecoHomeUrl(tool);
  const path = (vars.code && ecoDeepPath(id, 'stockLink', vars.code))
    || (vars.view && ecoDeepPath(id, 'viewLink', vars.view))
    || (vars.asset && ecoDeepPath(id, 'assetLink', vars.asset))
    || '';
  let u;
  try { u = new URL(path || base, base); } catch (e) { return null; }
  if (u.protocol !== 'https:' && u.protocol !== 'http:') return null;
  if (tool.themeParam !== false) u.searchParams.set('theme', ecoTheme());
  if (from && tool.id !== from) u.searchParams.set('from', from);
  return u.href;
}

/** 새 탭 핸드오프 링크 — 허브 /go/{id} 가 레지스트리로 303 한다(보유 스냅샷은 handoff 도구만). */
function ecoGoHref(id, vars = {}) {
  const query = new URLSearchParams({ theme: ecoTheme() });
  for (const key of ['code', 'view', 'asset']) {
    if (vars[key]) query.set(key, String(vars[key]));
  }
  const path = `/go/${encodeURIComponent(id)}?${query}`;
  return typeof buildApiUrl === 'function' ? buildApiUrl(path) : path;
}

/** 이 종목코드를 받는 도구(허브 제외, stockLink.accepts 기준). */
function ecoStockTools(code) {
  const reg = ecoRegistry();
  const c = String(code || '').trim().toUpperCase();
  if (!reg || !c) return [];
  return reg.tools.filter((t) => t && t.id !== ECO_HUB_ID && _ecoAccepts(t.stockLink, c));
}

/**
 * ?from=<도구 id> 로 들어온 경우 그 도구(공개·허브 아님). URL 의 ?code 가 지금 종목과
 * 같을 때만 돌려준다 — 다른 종목으로 옮겨 가면 '돌아가기'는 더 이상 맞지 않는다.
 */
function ecoArrivalTool(code) {
  let params;
  try { params = new URLSearchParams(window.location.search); } catch (e) { return null; }
  const from = params.get('from');
  const urlCode = String(params.get('code') || '').trim().toUpperCase();
  if (!from || from === ECO_HUB_ID || !urlCode || urlCode !== String(code || '').trim().toUpperCase()) return null;
  const tool = ecoTool(from);
  return tool && tool.deploy !== 'hub' ? tool : null;
}

/** 한국어 조사 '(으)로' — 받침 없음·ㄹ 받침이면 '로'. 한글이 아니면 '로'. */
function ecoJosaRo(word) {
  const s = String(word || '').trim();
  const last = s.charCodeAt(s.length - 1);
  if (!(last >= 0xac00 && last <= 0xd7a3)) return '로';
  const jong = (last - 0xac00) % 28;
  return jong === 0 || jong === 8 ? '로' : '으로';
}

// data-vc-tool 링크는 렌더 후 테마가 바뀌면 href 를 다시 계산한다(?theme 동기화).
//   data-vc-link="go"    → ecoGoHref (새 탭 핸드오프)
//   data-vc-link="tool"  → ecoToolUrl → portfolioIntegrationHref (handoff 도구는 보유 스냅샷 경유)
function ecoLinkHref(kind, id, vars) {
  if (kind === 'go') return ecoGoHref(id, vars);
  const url = ecoToolUrl(id, vars);
  if (!url) return null;
  return typeof portfolioIntegrationHref === 'function' ? portfolioIntegrationHref(url) : url;
}

function ecoRefreshLinks(root) {
  const scope = root || document;
  scope.querySelectorAll('a[data-vc-tool][data-vc-link]').forEach((a) => {
    const vars = { code: a.dataset.vcCode || '', view: a.dataset.vcView || '' };
    const href = ecoLinkHref(a.dataset.vcLink, a.dataset.vcTool, vars);
    if (href) a.setAttribute('href', href);
  });
}

function _ecoIcon(tool) {
  if (typeof VCShell !== 'undefined' && VCShell && typeof VCShell.icon === 'function') return VCShell.icon(tool.icon);
  const initial = escapeHtml(String(tool.name || tool.id || '?').trim().charAt(0));
  return `<span class="lab-eco-mono" aria-hidden="true">${initial}</span>`;
}

// --- 도구(labs) 화면의 '연결 대시보드' — 레지스트리의 공개 형제 도구 전부 ---
// /api/external/insights 데이터 유무와 무관하게 항상 보인다(요약 카드가 사라져도 도구는 남는다).
function renderLabEcosystem() {
  const section = document.getElementById('labEcosystem');
  const grid = document.getElementById('labEcoGrid');
  if (!section || !grid) return;
  const reg = ecoRegistry();
  const tools = ecoSiblingTools();
  if (!tools.length) {
    section.hidden = true;
    grid.innerHTML = '';
    return;
  }
  const labels = {};
  ((reg && reg.categories) || []).forEach((c) => { if (c && c.id) labels[c.id] = c.label; });
  grid.innerHTML = tools.map((t) => {
    const accent = /^#[0-9a-fA-F]{6}$/.test(String(t.accent || '')) ? t.accent : '';
    return `<a class="lab-eco-card" data-vc-tool="${escapeHtml(t.id)}" data-vc-link="go"`
      + ` href="${escapeHtml(ecoGoHref(t.id))}" target="_blank" rel="noopener"`
      + (accent ? ` style="--lab-eco-accent:${accent}"` : '') + '>'
      + `<span class="lab-eco-icon">${_ecoIcon(t)}</span>`
      + '<span class="lab-eco-text">'
      + `<span class="lab-eco-cat">${escapeHtml(labels[t.category] || '')}</span>`
      + `<strong class="lab-eco-name">${escapeHtml(t.name)}</strong>`
      + `<span class="lab-eco-desc">${escapeHtml(t.description || '')}</span>`
      + '</span><span class="lab-eco-open" aria-hidden="true">↗</span></a>';
  }).join('');
  section.hidden = false;
}

// --- iframe 메시지 브리지 (허브 ↔ nps-tracker·bond-mate·index-popup) ---
// 자식 → 부모: {source:'vc', type:'vc:ready'|'vc:height'|'vc:open-stock'} (+ bond-mate 구 형식
// {source:'bond-mate', type:'height'}). 부모 → 자식: {source:'vc', type:'vc:theme', theme}.
// vc:ready 를 보낸 자식에게만 테마를 postMessage 로 밀고, 나머지는 기존 src 리로드를 유지한다.
// 메시지는 (1) 등록된 iframe(data-vc-tool)의 contentWindow 에서 왔고 (2) origin 이 그 도구의
// 레지스트리 URL origin 과 같을 때만 받는다(nps·bond-mate 는 같은 github.io origin 이라
// origin 만으로는 구분할 수 없다).
function _ecoOrigin(url) {
  try { return new URL(url).origin; } catch (e) { return ''; }
}

function ecoFrameOrigin(frame) {
  const tool = ecoTool(frame && frame.dataset ? frame.dataset.vcTool : '');
  if (tool) return _ecoOrigin(tool.url);
  // 레지스트리가 없으면(옛 app-config) iframe 이 실제로 연 주소의 origin 만 믿는다.
  return _ecoOrigin(frame && frame.getAttribute ? frame.getAttribute('src') : '');
}

function ecoFrameReady(frame) {
  return !!(frame && frame.dataset && frame.dataset.vcReady === '1');
}

/** vc:ready 를 보낸 자식이면 테마를 postMessage 로 보내고 true. 아니면 false(호출부가 src 리로드). */
function ecoPostFrameTheme(frame, theme) {
  if (!ecoFrameReady(frame) || !frame.contentWindow) return false;
  const origin = ecoFrameOrigin(frame);
  if (!origin) return false;
  try {
    frame.contentWindow.postMessage({ source: 'vc', type: 'vc:theme', theme: theme || ecoTheme() }, origin);
    return true;
  } catch (e) {
    return false;
  }
}

function ecoApplyFrameHeight(frame, height) {
  const h = Number(height);
  if (!frame || !Number.isFinite(h) || h <= 0) return;
  const px = Math.round(Math.min(ECO_FRAME_HEIGHT_MAX, Math.max(ECO_FRAME_HEIGHT_MIN, h)));
  frame.style.height = px + 'px';
  frame.classList.add('vc-auto-height');
}

function _ecoFrameForSource(source) {
  if (!source) return null;
  const frames = document.querySelectorAll('iframe[data-vc-tool]');
  for (const frame of frames) {
    if (frame.contentWindow === source) return frame;
  }
  return null;
}

function ecoHandleFrameMessage(event) {
  const d = event && event.data;
  if (!d || typeof d !== 'object') return;
  const frame = _ecoFrameForSource(event.source);
  if (!frame) return;
  const origin = ecoFrameOrigin(frame);
  if (!origin || event.origin !== origin) return;
  const toolId = frame.dataset.vcTool;
  if (d.source === 'bond-mate' && d.type === 'height' && toolId === 'bond-mate') {
    ecoApplyFrameHeight(frame, d.height);   // 구 형식 — bond-mate 가 vc:height 를 병행 송신할 때까지 유지
    return;
  }
  if (d.source !== 'vc') return;
  if (d.type === 'vc:ready') {
    frame.dataset.vcReady = '1';
    // 준비되기 전에 테마가 바뀌었을 수 있으니 지금 테마를 한 번 맞춰 준다.
    ecoPostFrameTheme(frame);
  } else if (d.type === 'vc:height') {
    ecoApplyFrameHeight(frame, d.height);
  } else if (d.type === 'vc:open-stock') {
    const code = String(d.code || '').trim().toUpperCase();
    if (!ECO_CODE_RE.test(code) || typeof analyzeStock !== 'function') return;
    if (typeof switchView === 'function') switchView('analysis');
    analyzeStock(code);
  }
}

if (typeof window !== 'undefined') {
  window.addEventListener('message', ecoHandleFrameMessage);
  // 테마가 다른 경로(다른 탭의 storage 이벤트·OS 설정·vc-shell)로 바뀌어도 링크의 ?theme 을 맞춘다.
  document.addEventListener('vc:themechange', () => ecoRefreshLinks());
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', renderLabEcosystem);
  } else {
    renderLabEcosystem();
  }
}
