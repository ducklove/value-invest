// Portfolio risk panel — 성과 탭의 '리스크' 카드.
//
// GET /api/portfolio/risk?window=… (routes/portfolio_risk.py) 를 소비해
// 누적/연환산 수익률, 변동성, MDD(기간), 현재 낙폭, 샤프, 베타·상관
// (vs 벤치마크), 최고/최악 일간 타일을 렌더링한다.
//
// - lazy: 성과 탭이 처음 보일 때 pfSwitchTab(portfolio-performance.js)이
//   pfLoadRiskPanel() 을 호출한다. 앱 시작 시에는 아무것도 조회하지 않음.
// - 윈도(1M/3M/6M/1Y/YTD/ALL) 전환 시 재조회하되 윈도별 인메모리 메모를
//   둔다(서버도 사용자·윈도별 10분 캐시라 단순 메모로 충분).
// - 백그라운드 로드이므로 오류는 reportApiError silent 로 콘솔에만 남기고
//   패널 안에 안내 문구를 그린다.
// 포맷터(fmtPct/returnClass/escapeHtml)는 portfolio-render.js / utils.js
// 의 공용 헬퍼를 재사용한다 — 여기서 중복 정의하지 않는다.

const PF_RISK_WINDOWS = ['1M', '3M', '6M', '1Y', 'YTD', 'ALL'];
let _pfRiskWindow = '1Y';
let _pfRiskLoadSeq = 0;
// window -> 서버 응답 페이로드. 탭 재진입/윈도 재선택 시 재조회 없이 재사용.
const _pfRiskCache = {};

function _pfRiskContentEl() { return document.getElementById('pfRiskContent'); }

function _pfRiskMsg(message) {
  const el = _pfRiskContentEl();
  if (el) el.innerHTML = `<div class="pf-risk-empty">${escapeHtml(message)}</div>`;
}

function _pfRenderRiskWindowButtons() {
  const wrap = document.getElementById('pfRiskWindowBtns');
  if (!wrap) return;
  wrap.innerHTML = PF_RISK_WINDOWS.map(w =>
    `<button type="button" class="vp-btn${w === _pfRiskWindow ? ' active' : ''}" data-window="${w}" onclick="pfRiskSetWindow('${w}')">${w}</button>`
  ).join('');
}

function pfRiskSetWindow(win) {
  if (!PF_RISK_WINDOWS.includes(win) || win === _pfRiskWindow) return Promise.resolve();
  _pfRiskWindow = win;
  _pfRenderRiskWindowButtons();
  return pfLoadRiskPanel();
}

// 샤프/베타/상관 같은 무차원 지표용 — fmtPct 와 달리 % 없이 소수 2자리.
function _pfRiskNum(v, digits = 2) {
  return (v === null || v === undefined) ? '-' : Number(v).toFixed(digits);
}

function _pfRiskTile(label, value, cls = '', sub = '') {
  const subHtml = sub ? `<div class="pf-risk-sub">${sub}</div>` : '';
  return `<div class="pf-nav-ret-card"><div class="pf-nav-ret-label">${label}</div><div class="pf-nav-ret-value ${cls}">${value}</div>${subHtml}</div>`;
}

function _pfRenderRiskPanel(data) {
  const el = _pfRiskContentEl();
  if (!el) return;
  if (!data || data.insufficient || !data.metrics) {
    _pfRiskMsg('데이터가 부족합니다 — NAV 스냅샷이 2일 이상 쌓이면 리스크 지표가 표시됩니다.');
    return;
  }
  const m = data.metrics;
  const bench = data.benchmark || null;
  const benchName = bench ? (bench.name || bench.code || '') : '';

  const pct = (v, signed = true) => (v === null || v === undefined) ? '-' : fmtPct(v, signed);

  const mddSub = (m.max_drawdown_peak_date && m.max_drawdown_trough_date)
    ? `${escapeHtml(m.max_drawdown_peak_date)} → ${escapeHtml(m.max_drawdown_trough_date)}`
    : '';
  const best = m.best_day || null;
  const worst = m.worst_day || null;
  const benchSub = benchName ? `vs ${escapeHtml(benchName)}` : '';

  const tiles = [
    _pfRiskTile('누적 수익률', pct(m.cumulative_return_pct), returnClass(m.cumulative_return_pct)),
    _pfRiskTile('연환산 수익률', pct(m.annualized_return_pct), returnClass(m.annualized_return_pct)),
    _pfRiskTile('변동성 (연환산)', pct(m.annualized_volatility_pct, false)),
    _pfRiskTile('최대 낙폭 (MDD)', pct(m.max_drawdown_pct), returnClass(m.max_drawdown_pct), mddSub),
    _pfRiskTile('현재 낙폭', pct(m.current_drawdown_pct), returnClass(m.current_drawdown_pct)),
    _pfRiskTile('샤프 지수', _pfRiskNum(m.sharpe_ratio)),
    _pfRiskTile('베타', _pfRiskNum(bench?.beta), '', benchSub),
    _pfRiskTile('상관계수', _pfRiskNum(bench?.correlation), '', benchSub),
    _pfRiskTile('최고 일간', best ? pct(best.return_pct) : '-', returnClass(best?.return_pct), best?.date ? escapeHtml(best.date) : ''),
    _pfRiskTile('최악 일간', worst ? pct(worst.return_pct) : '-', returnClass(worst?.return_pct), worst?.date ? escapeHtml(worst.date) : ''),
  ];

  const rangeText = (data.start_date && data.end_date)
    ? `기간 <strong>${escapeHtml(data.start_date)} ~ ${escapeHtml(data.end_date)}</strong> · 스냅샷 ${Number(data.points || 0).toLocaleString()}개`
    : '';
  const benchText = bench
    ? ` · 벤치마크 ${escapeHtml(benchName)}${bench.overlap_returns ? ` (표본 ${Number(bench.overlap_returns).toLocaleString()}일)` : ''}`
    : '';

  el.innerHTML = `<div class="pf-risk-grid">${tiles.join('')}</div>`
    + (rangeText ? `<div class="pf-chart-range">${rangeText}${benchText}</div>` : '');
}

// 성과 탭이 보일 때 호출(lazy). 같은 윈도는 메모를 그대로 그린다.
async function pfLoadRiskPanel({ force = false } = {}) {
  const el = _pfRiskContentEl();
  if (!el) return;
  const win = _pfRiskWindow;
  _pfRenderRiskWindowButtons();
  if (!force && _pfRiskCache[win]) {
    _pfRenderRiskPanel(_pfRiskCache[win]);
    return;
  }
  const seq = ++_pfRiskLoadSeq;
  _pfRiskMsg('리스크 지표를 불러오는 중입니다...');
  try {
    const resp = await apiFetch(`/api/portfolio/risk?window=${encodeURIComponent(win)}`);
    if (resp.status === 401) {
      if (seq === _pfRiskLoadSeq && win === _pfRiskWindow) _pfRiskMsg('로그인 후 이용할 수 있습니다.');
      return;
    }
    if (!resp.ok) throw new Error(`리스크 지표 요청 실패 (${resp.status})`);
    const data = await resp.json();
    _pfRiskCache[win] = data;
    // 윈도를 빠르게 전환했을 때 늦게 도착한 응답은 그리지 않는다(메모만 갱신).
    if (seq !== _pfRiskLoadSeq || win !== _pfRiskWindow) return;
    _pfRenderRiskPanel(data);
  } catch (e) {
    // 백그라운드 로드 — 토스트 없이 콘솔 기록만 남기고 패널 안에 안내.
    reportApiError(e, '리스크 지표', { silent: true });
    if (seq === _pfRiskLoadSeq && win === _pfRiskWindow) {
      _pfRiskMsg('리스크 지표를 불러오지 못했습니다. 잠시 후 다시 시도해 주세요.');
    }
  }
}
