// 컬러 모드 (heat mode) — 보유종목 리스트에서 "오늘 얼마나 움직였나"를
// 색·크기·애니메이션 강도로 증폭해 한눈에 잡히게 한다.
//
// 설계 원칙
//  - 잔잔한 날은 평소 화면 그대로: |등락률| < 1% 는 레벨 0 이라 아무 장식도 없다.
//  - 강도는 절대 등락률(레벨 0~5)로, 게이지 길이는 "현재 보이는 목록 안에서의
//    상대 크기"로 준다. 둘을 섞으면 5% 하나뿐인 날에도 화면이 벌겋게 되거나
//    반대로 전 종목 20% 인 날 아무 차이가 안 보인다.
//  - 상/하한가는 별도 등급: 정확한 KRX 가격제한폭(호가단위 정렬)에 실제로 닿았을
//    때만 붙이고, 이때는 표현을 과장한다(솔리드 배지 + 스윕 + 행 글로우).
//    단 "상한가" 라벨은 등락률 옆에 덧붙이지 않고 같은 자리에서 교대로 띄운다 —
//    칸 폭을 한 글자도 더 먹지 않게. 대신 도달 순간엔 전면 이펙트로 크게 알린다.
//  - 색은 한국 시장 관행(상승=빨강 / 하락=파랑) 고정. base.css 계약과 동일.
//
// 렌더 경로가 둘이라 상태 계산(pfHeatRowState)을 한 곳에 모았다.
//  1) renderPortfolio() 의 tbody 전체 재생성 → pfChangeCellHtml / pfHeatRowAttrs
//  2) WS tick 의 셀 단위 in-place 갱신 → pfChangeCellHtml / pfHeatApplyRow

const PF_HEAT_MODE_KEY = 'pf_heat_mode';
// |등락률| 경계 → 레벨. 1%/2%/4%/7%/15% 를 넘을 때마다 한 단계씩 올라간다.
const PF_HEAT_THRESHOLDS = [1, 2, 4, 7, 15];
// "급등/급락" 요약 카운트 기준 레벨 (= 4% 이상).
const PF_HEAT_SPIKE_LEVEL = 3;

// 현재 보이는 목록의 최대 |등락률|. 게이지 길이의 분모.
let _pfHeatScale = 0;

try { PfStore.prefs.heatMode = localStorage.getItem(PF_HEAT_MODE_KEY) !== '0'; } catch (e) {}

function pfHeatEnabled() {
  return !!(typeof PfStore !== 'undefined' && PfStore.prefs && PfStore.prefs.heatMode);
}

function pfHeatLevel(pct) {
  const abs = Math.abs(Number(pct));
  if (!Number.isFinite(abs)) return 0;
  let level = 0;
  for (const threshold of PF_HEAT_THRESHOLDS) {
    if (abs >= threshold) level += 1;
  }
  return level;
}

// --- KRX 가격제한폭 ---------------------------------------------------------
// services/krx_limits.py 의 포팅. ±30% 자체가 아니라 "그 가격대 호가단위에
// 정렬된 값"이 상/하한가이므로, 근사 ±30% 비교로는 실제 도달 여부를 못 가른다.
// (호가단위 표: 2023-01-25 KOSPI/KOSDAQ 통합 기준.)
const PF_KRX_TICK_BANDS = [[2000, 1], [5000, 5], [20000, 10], [50000, 50], [200000, 100], [500000, 500]];
const PF_KRX_TICK_TOP = 1000;

function pfKrxTickSize(price) {
  for (const [ceiling, tick] of PF_KRX_TICK_BANDS) {
    if (price < ceiling) return tick;
  }
  return PF_KRX_TICK_TOP;
}

function pfKrxUpperLimit(basePrice) {
  const base = Number(basePrice);
  if (!Number.isFinite(base) || base <= 0) return null;
  const raw = base * 1.30;
  const tick = pfKrxTickSize(raw);
  return Math.floor(Number(raw.toFixed(4)) / tick) * tick;
}

function pfKrxLowerLimit(basePrice) {
  const base = Number(basePrice);
  if (!Number.isFinite(base) || base <= 0) return null;
  const raw = base * 0.70;
  const tick = pfKrxTickSize(raw);
  return Math.ceil(Number(raw.toFixed(4)) / tick) * tick;
}

// 국내 주식 코드만 대상. 현금/금/크립토/해외 티커는 가격제한폭 개념이 없고,
// KONEX(±15%)·상장일·정리매매 종목은 밴드가 달라 여기선 "미도달"로 빠진다.
function pfHeatLimitState(row) {
  if (!row) return null;
  const code = String(row.stock_code || '').toUpperCase();
  if (!/^[0-9][0-9A-Z]{5}$/.test(code)) return null;
  const price = Number(row.price);
  const base = Number(row.quote && row.quote.previous_close);
  if (!Number.isFinite(price) || price <= 0 || !Number.isFinite(base) || base <= 0) return null;
  const upper = pfKrxUpperLimit(base);
  const lower = pfKrxLowerLimit(base);
  if (upper !== null && price >= upper) return 'up';
  if (lower !== null && price <= lower) return 'down';
  return null;
}

// --- 행 상태 ---------------------------------------------------------------
function pfHeatRowState(row) {
  if (!row) return null;
  const pct = Number(row.changePct);
  if (row.changePct === null || row.changePct === undefined || !Number.isFinite(pct)) return null;
  const abs = Math.abs(pct);
  // tick 경로에선 전체 재렌더 없이 신고점이 나올 수 있어 스케일을 키워 준다.
  if (abs > _pfHeatScale) _pfHeatScale = abs;
  return {
    pct,
    dir: pct > 0 ? 'up' : pct < 0 ? 'down' : 'flat',
    level: pfHeatLevel(pct),
    limit: pfHeatLimitState(row),
    ratio: _pfHeatScale > 0 ? Math.min(1, abs / _pfHeatScale) : 0,
  };
}

// renderPortfolio() 가 보이는 행 전체를 넘겨 게이지 분모를 다시 잡는다.
function pfHeatSetScale(rows) {
  let max = 0;
  for (const row of rows || []) {
    const abs = Math.abs(Number(row && row.changePct));
    if (Number.isFinite(abs) && abs > max) max = abs;
  }
  _pfHeatScale = max;
  return max;
}

function pfHeatRowAttrs(row) {
  if (!pfHeatEnabled()) return '';
  const state = pfHeatRowState(row);
  if (!state || (!state.level && !state.limit)) return '';
  let attrs = ` data-heat-dir="${state.dir}" data-heat-level="${state.level}"`;
  if (state.limit) attrs += ` data-heat-limit="${state.limit}"`;
  return attrs;
}

// WS tick 경로: tbody 재생성 없이 <tr> 의 히트 속성만 맞춘다.
function pfHeatApplyRow(tr, row) {
  if (!tr) return;
  const state = pfHeatEnabled() ? pfHeatRowState(row) : null;
  if (state) pfHeatTrackLimit(row, state);
  if (!state || (!state.level && !state.limit)) {
    tr.removeAttribute('data-heat-dir');
    tr.removeAttribute('data-heat-level');
    tr.removeAttribute('data-heat-limit');
    return;
  }
  tr.setAttribute('data-heat-dir', state.dir);
  tr.setAttribute('data-heat-level', String(state.level));
  if (state.limit) tr.setAttribute('data-heat-limit', state.limit);
  else tr.removeAttribute('data-heat-limit');
}

function _pfHeatIcon(state) {
  if (state.limit === 'up') return '🚀';    // 🚀 상한가
  if (state.limit === 'down') return '🧊';  // 🧊 하한가
  if (state.level >= 4) return state.dir === 'up' ? '🔥' : '❄️'; // 🔥 / ❄️
  return '';
}

function pfHeatChangeCell(row) {
  const state = pfHeatRowState(row);
  if (!state) return '-';
  const icon = _pfHeatIcon(state);
  const iconHtml = icon ? `<span class="pf-heat-icon" aria-hidden="true">${icon}</span>` : '';
  const pct = fmtPct(state.pct);
  // 상/하한 라벨은 등락률 옆에 덧붙이지 않고 같은 자리에서 교대로 보여 준다.
  // 두 글자를 같은 그리드 셀에 겹쳐 두므로 칸 폭은 둘 중 넓은 쪽(=등락률)
  // 그대로고, 라벨 때문에 등락률 칸이 넓어지지 않는다.
  const valueHtml = state.limit
    ? '<span class="pf-heat-swap">'
      + `<span class="pf-heat-swap-pct">${pct}</span>`
      + `<span class="pf-heat-swap-tag" aria-hidden="true">${state.limit === 'up' ? '상한가' : '하한가'}</span>`
      + '</span>'
    : pct;
  const limitAttr = state.limit ? ` data-heat-limit="${state.limit}"` : '';
  const srLabel = state.limit ? ` title="${state.limit === 'up' ? '상한가 도달' : '하한가 도달'}"` : '';
  return `<span class="pf-heat-cell" data-heat-dir="${state.dir}" data-heat-level="${state.level}"${limitAttr}${srLabel}`
    + ` style="--pf-heat-ratio:${state.ratio.toFixed(3)}">`
    + '<span class="pf-heat-gauge" aria-hidden="true"></span>'
    + `<span class="pf-heat-val">${iconHtml}${valueHtml}</span>`
    + '</span>';
}

// 등락률 셀의 단일 진입점 — 컬러 모드가 꺼져 있으면 종전 표시 그대로.
function pfChangeCellHtml(row) {
  if (!pfHeatEnabled()) return fmtChangePct(row.changePct, row.change);
  return pfHeatChangeCell(row);
}

// --- 전면 이펙트 (상/하한가 도달 순간) --------------------------------------
// 배지 하나로는 "오늘 상한가"라는 사건의 크기가 안 산다. 새로 닿은 순간에만
// 화면 전체에 꽃잎(상한) / 눈보라(하한)를 한 번 뿌린다.
//  - 엣지 트리거: 같은 종목이 상한가에 머무는 동안엔 다시 뿌리지 않고, 풀렸다가
//    다시 닿으면 그때는 새 사건으로 본다.
//  - 같은 렌더에서 여러 종목이 걸리면 한 번으로 합친다(화면 겹침 방지).
//  - prefers-reduced-motion 이면 아예 만들지 않는다.
const PF_FX_PARTICLES = 42;      // 오버레이 1회당 파티클 수 — DOM 부담 상한
const PF_FX_LIFETIME_MS = 6200;  // 가장 늦게 출발하는 파티클이 화면을 벗어날 때까지 + 여유
const PF_FX_COALESCE_MS = 90;    // 이 시간 안에 걸린 종목들은 오버레이 하나로
const PF_FX_NAME_LIMIT = 3;      // 배너에 이름을 몇 개까지 적나
const PF_FX_GLYPHS = {
  up: ['🌸', '🌸', '🌷', '🎉', '✨', '💮'],
  down: ['❄️', '❄️', '🧊', '💧'],
};

const _pfFxLimitSeen = new Map();  // stock_code → 'up' | 'down' | null
let _pfFxPending = null;           // { up: [이름…], down: [이름…] }
let _pfFxTimer = null;

function pfFxAllowed() {
  if (typeof document === 'undefined' || !document.body) return false;
  try {
    if (window.matchMedia && window.matchMedia('(prefers-reduced-motion: reduce)').matches) return false;
  } catch (e) {}
  return true;
}

// 두 렌더 경로(전체 재렌더 / WS tick)가 모두 여기로 상태를 흘려보내 엣지를 잡는다.
function pfHeatTrackLimit(row, state) {
  if (!pfHeatEnabled() || !row) return;
  const code = String(row.stock_code || '');
  if (!code) return;
  const limit = (state && state.limit) || null;
  if (limit === (_pfFxLimitSeen.get(code) || null)) return;
  _pfFxLimitSeen.set(code, limit);
  if (limit) _pfFxQueue(limit, row.stock_name || code);
}

function _pfFxQueue(dir, name) {
  if (!_pfFxPending) _pfFxPending = { up: [], down: [] };
  if (!_pfFxPending[dir].includes(name)) _pfFxPending[dir].push(name);
  if (_pfFxTimer) return;
  _pfFxTimer = setTimeout(() => {
    const pending = _pfFxPending;
    _pfFxPending = null;
    _pfFxTimer = null;
    if (!pending) return;
    if (pending.up.length) pfHeatCelebrate('up', pending.up);
    if (pending.down.length) pfHeatCelebrate('down', pending.down);
  }, PF_FX_COALESCE_MS);
}

function _pfFxBannerNames(names) {
  if (names.length <= PF_FX_NAME_LIMIT) return names.join(' · ');
  return `${names.slice(0, PF_FX_NAME_LIMIT).join(' · ')} 외 ${names.length - PF_FX_NAME_LIMIT}`;
}

// 파티클은 목록 전체에 고르게 깔고(칸마다 하나) 칸 안에서만 흔든다 — 순수 난수는
// 뭉쳐서 화면 한쪽만 비는 일이 잦다.
function _pfFxParticleHtml(dir) {
  const glyphs = PF_FX_GLYPHS[dir] || PF_FX_GLYPHS.up;
  const slot = 100 / PF_FX_PARTICLES;
  let html = '';
  for (let i = 0; i < PF_FX_PARTICLES; i += 1) {
    const left = (i * slot + Math.random() * slot).toFixed(2);
    const size = (dir === 'down' ? 11 : 13) + Math.random() * 12;
    const style = `--pf-fx-x:${left}%;`
      + `--pf-fx-size:${size.toFixed(1)}px;`
      + `--pf-fx-drift:${(Math.random() * 140 - 70).toFixed(0)}px;`
      + `--pf-fx-spin:${(Math.random() * 900 - 450).toFixed(0)}deg;`
      + `--pf-fx-dur:${(3.4 + Math.random() * 2.4).toFixed(2)}s;`
      + `--pf-fx-delay:${(Math.random() * 1.6).toFixed(2)}s;`;
    html += `<span class="pf-fx-p" style="${style}">${glyphs[i % glyphs.length]}</span>`;
  }
  return html;
}

// 전역에 열어 둔다 — 이벤트 경로 없이도 호출해 볼 수 있게(테스트/데모).
function pfHeatCelebrate(dir, names) {
  if (!pfFxAllowed()) return null;
  const direction = dir === 'down' ? 'down' : 'up';
  const list = (Array.isArray(names) ? names : [names]).filter(Boolean).map(String);
  const layer = document.createElement('div');
  layer.className = 'pf-fx';
  layer.setAttribute('data-fx', direction);
  layer.setAttribute('aria-hidden', 'true');
  layer.innerHTML = '<span class="pf-fx-veil"></span>'
    + _pfFxParticleHtml(direction)
    + '<span class="pf-fx-banner"><span class="pf-fx-title"></span><span class="pf-fx-names"></span></span>';
  // 종목명은 사용자 입력이라 textContent 로만 넣는다.
  layer.querySelector('.pf-fx-title').textContent = direction === 'up' ? '🚀 상한가' : '🧊 하한가';
  layer.querySelector('.pf-fx-names').textContent = _pfFxBannerNames(list);
  document.body.appendChild(layer);
  setTimeout(() => layer.remove(), PF_FX_LIFETIME_MS);
  return layer;
}

// --- 요약 스트립 -----------------------------------------------------------
// 목록을 훑기 전에 "오늘 뭔가 터졌나"를 먼저 알려 준다. 컬러 모드 토글 옆.
function pfHeatUpdateSummary(rows) {
  const el = document.getElementById('pfHeatSummary');
  if (!el) return;
  if (!pfHeatEnabled()) { el.textContent = ''; el.hidden = true; return; }
  let limitUp = 0, limitDown = 0, spikeUp = 0, spikeDown = 0;
  for (const row of rows || []) {
    const state = pfHeatRowState(row);
    if (!state) continue;
    pfHeatTrackLimit(row, state);
    if (state.limit === 'up') { limitUp += 1; continue; }
    if (state.limit === 'down') { limitDown += 1; continue; }
    if (state.level < PF_HEAT_SPIKE_LEVEL) continue;
    if (state.dir === 'up') spikeUp += 1;
    else if (state.dir === 'down') spikeDown += 1;
  }
  const parts = [];
  if (limitUp) parts.push(`<b class="up">🚀 상한가 ${limitUp}</b>`);
  if (limitDown) parts.push(`<b class="down">🧊 하한가 ${limitDown}</b>`);
  if (spikeUp) parts.push(`<b class="up">🔥 급등 ${spikeUp}</b>`);
  if (spikeDown) parts.push(`<b class="down">❄️ 급락 ${spikeDown}</b>`);
  el.hidden = false;
  el.innerHTML = parts.length ? parts.join('<span class="pf-heat-summary-sep">·</span>') : '<span class="pf-heat-calm">오늘은 잔잔합니다</span>';
}

// --- 토글 ------------------------------------------------------------------
function _pfApplyHeatMode(enabled) {
  PfStore.prefs.heatMode = !!enabled;
  document.body.classList.toggle('pf-heat-mode', PfStore.prefs.heatMode);
  const btn = document.getElementById('pfHeatToggle');
  if (btn) {
    btn.classList.toggle('active', PfStore.prefs.heatMode);
    btn.setAttribute('aria-pressed', PfStore.prefs.heatMode ? 'true' : 'false');
  }
}

function pfToggleHeatMode() {
  _pfApplyHeatMode(!PfStore.prefs.heatMode);
  try { localStorage.setItem(PF_HEAT_MODE_KEY, PfStore.prefs.heatMode ? '1' : '0'); } catch (e) {}
  if (typeof renderPortfolio === 'function') renderPortfolio();
}

(function initPfHeatMode() {
  const apply = () => _pfApplyHeatMode(PfStore.prefs.heatMode);
  if (typeof document === 'undefined') return;
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', apply);
  } else {
    apply();
  }
})();
