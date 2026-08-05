// 컬러 모드 (heat mode) — 보유종목 리스트에서 "오늘 얼마나 움직였나"를
// 색·크기·애니메이션 강도로 증폭해 한눈에 잡히게 한다.
//
// 설계 원칙
//  - 잔잔한 날은 평소 화면 그대로: |등락률| < 1% 는 레벨 0 이라 아무 장식도 없다.
//  - 실제 보유분(수량 > 0)에만 건다. 관심종목처럼 수량 0 인 행은 종전 표시.
//  - 강도는 절대 등락률(레벨 0~5)로, 게이지 길이는 "현재 보이는 목록 안에서의
//    상대 크기"로 준다. 둘을 섞으면 5% 하나뿐인 날에도 화면이 벌겋게 되거나
//    반대로 전 종목 20% 인 날 아무 차이가 안 보인다.
//  - 상/하한가는 별도 등급: 오늘(KST) 시세로 정확한 KRX 가격제한폭(호가단위
//    정렬)에 실제로 닿았을 때만 붙이고, 이때는 표현을 과장한다
//    (솔리드 배지 + 스윕 + 행 글로우). 지난 거래일 스냅샷이면 판정 자체를 버린다.
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

// 컬러 모드는 "실제 보유분"에만 건다. 수량 0(관심종목만 담아 둔 행)이나 음수
// 포지션까지 벌겋게 물들면 정작 내 돈이 움직인 행이 묻힌다. 행 틴트·게이지·
// 요약 집계·전면 이펙트가 모두 이 판정을 공유한다.
// 렌더 경로마다 필드가 달라(전체 재렌더는 qty, tick 은 item 의 quantity) 둘 다 본다.
function pfHeatQty(row) {
  if (!row) return 0;
  const qty = Number(row.quantity !== undefined && row.quantity !== null ? row.quantity : row.qty);
  return Number.isFinite(qty) ? qty : 0;
}

function pfHeatAppliesTo(row) {
  return pfHeatEnabled() && pfHeatQty(row) > 0;
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

// 상/하한가는 "오늘 일어난 사건"이다. 그런데 시세 스냅샷은 며칠 지나도
// price 와 previous_close 가 함께 멈춰 있어 가격 비교만으로는 판정이 그대로
// 성립한다 — 며칠 만에 접속해도 지난 상한가가 살아 있는 것처럼 보이고
// 축포까지 터진다. 그래서 스냅샷 거래일이 오늘(KST)이 아니면 판정을 버린다.
const PF_KST_DATE_FMT = (() => {
  try {
    return new Intl.DateTimeFormat('en-CA', {
      timeZone: 'Asia/Seoul', year: 'numeric', month: '2-digit', day: '2-digit',
    });
  } catch (e) { return null; }
})();

// 렌더 한 번에 행 수만큼 불리므로 값을 잠깐 캐시한다(자정을 넘겨도 1분 안에 따라감).
let _pfKstToday = { at: 0, value: null };

function pfKstTodayValue() {
  const now = Date.now();
  if (_pfKstToday.value !== null && now - _pfKstToday.at < 60_000) return _pfKstToday.value;
  // quoteSnapshotDateValue 와 같은 기준(YYYY-MM-DD → UTC 자정)으로 맞춘다.
  const ymd = PF_KST_DATE_FMT ? PF_KST_DATE_FMT.format(new Date()) : new Date().toISOString().slice(0, 10);
  const value = Date.parse(ymd);
  _pfKstToday = { at: now, value: Number.isFinite(value) ? value : null };
  return _pfKstToday.value;
}

// 날짜를 모르는 스냅샷은 "오늘 것"으로 치지 않는다 — 지난 상한가로 축포가
// 터지는 쪽이, 오늘 상한가를 한 번 못 알리는 쪽보다 나쁘다.
function pfHeatQuoteIsToday(row) {
  const snapshot = quoteSnapshotDateValue(row && row.quote);
  const today = pfKstTodayValue();
  return snapshot !== null && today !== null && snapshot === today;
}

// 국내 주식 코드만 대상. 현금/금/크립토/해외 티커는 가격제한폭 개념이 없고,
// KONEX(±15%)·상장일·정리매매 종목은 밴드가 달라 여기선 "미도달"로 빠진다.
function pfHeatLimitState(row) {
  if (!row) return null;
  const code = String(row.stock_code || '').toUpperCase();
  if (!/^[0-9][0-9A-Z]{5}$/.test(code)) return null;
  if (!pfHeatQuoteIsToday(row)) return null;
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
// 게이지가 안 붙는 행(수량 0)은 분모에서도 빼야 "보이는 막대들"의 상대 비교가
// 어긋나지 않는다.
function pfHeatSetScale(rows) {
  let max = 0;
  for (const row of rows || []) {
    if (pfHeatQty(row) <= 0) continue;
    const abs = Math.abs(Number(row && row.changePct));
    if (Number.isFinite(abs) && abs > max) max = abs;
  }
  _pfHeatScale = max;
  return max;
}

function pfHeatRowAttrs(row) {
  if (!pfHeatAppliesTo(row)) return '';
  const state = pfHeatRowState(row);
  if (!state || (!state.level && !state.limit)) return '';
  let attrs = ` data-heat-dir="${state.dir}" data-heat-level="${state.level}"`;
  if (state.limit) attrs += ` data-heat-limit="${state.limit}"`;
  return attrs;
}

// WS tick 경로: tbody 재생성 없이 <tr> 의 히트 속성만 맞춘다.
function pfHeatApplyRow(tr, row) {
  if (!tr) return;
  const state = pfHeatAppliesTo(row) ? pfHeatRowState(row) : null;
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

// 상/하한가는 부호를 화살표로 바꿔 적는다(+29.91% → ↑29.91%). 색·배지·애니메이션이
// 없는 곳(컬러 모드 off, 수량 0 행, 색각 이상·흑백)에서도 글자만으로 도달 여부가
// 구분되게 — 국내 HTS 관행과 같은 표기(상승 +, 상한 ↑).
function pfLimitPctText(pct, limit) {
  const text = fmtPct(pct);
  if (limit === 'up') return text.replace(/^\+?/, '↑');
  if (limit === 'down') return text.replace(/^-?/, '↓');
  return text;
}

function pfHeatChangeCell(row) {
  const state = pfHeatRowState(row);
  if (!state) return '-';
  const icon = _pfHeatIcon(state);
  const iconHtml = icon ? `<span class="pf-heat-icon" aria-hidden="true">${icon}</span>` : '';
  const pct = pfLimitPctText(state.pct, state.limit);
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

// 컬러 모드가 안 걸리는 행(모드 off / 수량 0)의 종전 표시. 상/하한가만은
// 여기서도 화살표로 남긴다 — 컬러 모드는 강조 수단이지 유일한 전달 수단이 아니다.
function pfPlainChangeCell(row) {
  const limit = pfHeatLimitState(row);
  if (!limit) return fmtChangePct(row.changePct, row.change);
  const title = limit === 'up' ? '상한가 도달' : '하한가 도달';
  return `<span class="pf-return ${limit === 'up' ? 'positive' : 'negative'} pf-limit-mark"`
    + ` title="${title}">${pfLimitPctText(row.changePct, limit)}</span>`;
}

// 등락률 셀의 단일 진입점 — 컬러 모드가 꺼져 있으면 종전 표시 그대로.
function pfChangeCellHtml(row) {
  if (!pfHeatAppliesTo(row)) return pfPlainChangeCell(row);
  return pfHeatChangeCell(row);
}

// --- 전면 이펙트 (상/하한가로 전이하는 순간) --------------------------------
// 배지 하나로는 "지금 막 상한가"라는 사건의 크기가 안 산다. 보고 있는 동안
// 상한가로 넘어가는 순간에만 화면 전체에 꽃잎(상한) / 눈보라(하한)를 뿌린다.
//  - 축포는 "지금 일어난 일"에만 붙는다. 첫 관찰(페이지 로드·화면 진입 시점에
//    이미 상한가)은 기록만 하고 넘어간다 — 이미 상한가로 마감한 종목이나 지난
//    거래일 스냅샷으로 축포가 터지면, 축포가 사건이 아니라 소음이 된다.
//  - 상한가에 머무는 동안엔 다시 뿌리지 않고, 풀렸다가 다시 닿으면 새 사건이다.
//  - 이벤트를 쌓아 뒀다 나중에 재생하지 않는다. 전이하는 그 순간에 못 띄우면
//    (안 보는 탭 / 이미 이펙트가 떠 있음 / 동작 줄이기) 그냥 버린다.
const PF_FX_PARTICLES = 42;      // 오버레이 1회당 파티클 수 — DOM 부담 상한
const PF_FX_LIFETIME_MS = 6200;  // 가장 늦게 출발하는 파티클이 화면을 벗어날 때까지 + 여유
const PF_FX_NAME_LIMIT = 3;      // 배너에 이름을 몇 개까지 적나
const PF_FX_GLYPHS = {
  up: ['🌸', '🌸', '🌷', '🎉', '✨', '💮'],
  down: ['❄️', '❄️', '🧊', '💧'],
};

const _pfFxLimitSeen = new Map();  // stock_code → 'up' | 'down' | null

function pfFxAllowed() {
  if (typeof document === 'undefined' || !document.body) return false;
  if (document.hidden) return false;                       // 안 보는 화면엔 띄우지 않는다
  if (document.querySelector('.pf-fx')) return false;      // 이미 떠 있으면 겹치지 않게 버린다
  try {
    if (window.matchMedia && window.matchMedia('(prefers-reduced-motion: reduce)').matches) return false;
  } catch (e) {}
  return true;
}

// 두 렌더 경로(전체 재렌더 / WS tick)가 모두 여기로 상태를 흘려보내 전이를 잡는다.
function pfHeatTrackLimit(row, state) {
  if (!pfHeatAppliesTo(row)) return;
  const code = String(row.stock_code || '');
  if (!code) return;
  const limit = (state && state.limit) || null;
  const known = _pfFxLimitSeen.has(code);
  if (known && limit === _pfFxLimitSeen.get(code)) return;
  _pfFxLimitSeen.set(code, limit);
  // 첫 관찰은 "지금 닿았다"는 근거가 없다 — 상태만 기억하고 축포는 생략.
  if (!known || !limit) return;
  pfHeatCelebrate(limit, row.stock_name || code);
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
    // 수량 0 행은 컬러 모드 자체가 안 걸리므로 집계·이펙트에서도 뺀다.
    // (게이지 분모를 키우지 않도록 상태 계산 전에 거른다.)
    if (pfHeatQty(row) <= 0) continue;
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
