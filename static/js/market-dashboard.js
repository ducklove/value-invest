// 투자정보 (Investing dashboard): public market overview.
//
// Built entirely from already-public endpoints — /api/market-indicators
// (catalog: code -> {label, category}) and /api/market-summary?codes=...
// (values: code -> {value, change, change_pct, direction}). Renders the
// indicators grouped by category into #marketDashboard. No auth required.
//
// Phase 1 covers the indicator grid (지수·해외증시·원자재·환율·금리·KOSPI 선물).
// Crypto / news / 수급·시총·업종 widgets are layered on in later steps.

let _mdCatalog = null; // {code: {label, category}}
let _mdLoadedOnce = false;
let _mdInFlight = null;

// Display order for category groups; unknown categories fall to the end.
const MD_CATEGORY_ORDER = ['국내 지수', '해외 지수', '국채', '원자재', '환율', '야간선물', '바이낸스'];
const MD_KOSPI_FUTURES_FRAME_URL = 'https://cantabile.tplinkdns.com:3358/?index=kospi-night-futures&theme=light&period=1D';

// 국채(yield curve·국가비교) 렌더링 상수/상태.
const BOND_COUNTRY_NAMES = { KR: '한국', US: '미국', JP: '일본', CN: '중국', DE: '독일', FR: '프랑스', GB: '영국', AU: '호주', IT: '이탈리아', CA: '캐나다', IN: '인도', BR: '브라질' };
const BOND_CURVE_COLORS = { KR: '#2563eb', US: '#e11d48', JP: '#16a34a' };
let _bondCharts = [];  // [{ec, ro}] — 재렌더 시 dispose

function _mdChange(d) {
  // Mirror the market-bar contract: direction up/down/flat, change_pct like "1.23%".
  const rawPct = String(d.change_pct || '').replace(/[-+%]/g, '');
  const isDown = d.direction === 'down';
  const isUp = d.direction === 'up';
  const cls = isDown ? 'md-down' : (isUp ? 'md-up' : 'md-flat');
  const sign = isDown ? '-' : (isUp ? '+' : '');
  const chgVal = d.change ? `${sign}${String(d.change)}` : '';
  const chgPct = rawPct ? `(${sign}${rawPct}%)` : '';
  // abs/pct 분리: 좁은 화면(모바일)에서는 절대값을 숨기고 등락%만 노출한다.
  return { cls, text: [chgVal, chgPct].filter(Boolean).join(' '), abs: chgVal, pct: chgPct };
}

// 투자정보 대시보드 표시에서만 제외할 코드 (데이터 수집·금일시황 등 다른 경로는 유지).
// KOSPI200 은 국내 지수 카드에서 자리가 어색하고 중요도가 낮아 숨긴다.
const MD_HIDDEN_CODES = new Set(['KOSPI200']);

function _mdGroupByCategory(catalog) {
  const groups = {};
  for (const [code, meta] of Object.entries(catalog || {})) {
    if (MD_HIDDEN_CODES.has(code)) continue;
    const cat = (meta && meta.category) || '기타';
    (groups[cat] = groups[cat] || []).push(code);
  }
  const cats = Object.keys(groups).sort((a, b) => {
    const ia = MD_CATEGORY_ORDER.indexOf(a);
    const ib = MD_CATEGORY_ORDER.indexOf(b);
    return (ia === -1 ? 99 : ia) - (ib === -1 ? 99 : ib);
  });
  return cats.map((cat) => ({ category: cat, codes: groups[cat] }));
}

// Naver-style information architecture: prominent 주요 지수 hero + 국채 in the
// main column; the lighter indicator strips (해외 지수·환율·원자재·KOSPI 선물) and
// 시장 랭킹 in the right rail so the two columns stay balanced in height.
const MD_HERO_CATEGORIES = ['국내 지수'];
// Categories forced into the main column (besides hero/국채). Empty = only the
// hero + 국채 live in main; everything else goes to the rail.
const MD_MAIN_CATEGORIES = [];

// 국내 지수 hero 카드 우상단에 띄울 네이버 일간 미니 차트(코드=네이버 심볼).
// down 변형 파일은 없고 _end_up_tablet.png 만 존재하며, 이미지 내용 자체가
// 당일 등락을 반영한다. 캐시버스팅을 5분 단위로 둬 최신 차트를 받되 과도한
// 재요청은 피한다.
const MD_MINI_CHART = new Set(['KOSPI', 'KOSDAQ']);

function _miniChartHtml(code) {
  if (!MD_MINI_CHART.has(code)) return '';
  const bust = Math.floor(Date.now() / 300000);
  const url = `https://ssl.pstatic.net/imgfinance/chart/mobile/mini/${code}_end_up_tablet.png?${bust}`;
  return `<img class="md-hero-chart" src="${escapeHtml(url)}" alt="${escapeHtml(code)} 일간 추이"`
    + ` loading="lazy" onerror="this.style.display='none'">`;
}

function _mdCardHtml(code, catalog, dataMap, variant) {
  const meta = catalog[code] || {};
  const label = meta.label || code;
  const d = dataMap ? dataMap[code] : null;
  let valHtml = '-';
  let chgHtml = '';      // hero: 절대값+%(full)
  // list: 전일대비를 못 구한 항목도 자리(정렬)는 지키고 '-' placeholder 로 표시.
  let rowChgHtml = '<span class="md-chg md-flat">-</span>';
  if (d && d.value) {
    const c = _mdChange(d);
    valHtml = escapeHtml(String(d.value));
    chgHtml = c.text ? `<span class="md-chg ${c.cls}">${escapeHtml(c.text)}</span>` : '';
    if (c.text) {
      rowChgHtml = `<span class="md-chg ${c.cls}">`
        + (c.abs ? `<span class="md-chg-abs">${escapeHtml(c.abs)} </span>` : '')
        + `<span class="md-chg-pct">${escapeHtml(c.pct)}</span></span>`;
    }
  }
  if (variant === 'hero') {
    // 카드 안에 해당 시장 수급 슬롯을 둔다. 캐시값이 있으면 즉시 채우고,
    // loadInvestorFlows()가 최신값으로 갱신한다(없으면 빈 슬롯).
    const flow = _mdFlows ? _mdFlows[String(code).toLowerCase()] : null;
    const flowSlot = `<div class="md-card-flow" data-flow-code="${escapeHtml(String(code))}">${_cardFlowHtml(flow)}</div>`;
    return `<div class="md-hero-card">`
      + _miniChartHtml(code)
      + `<div class="md-hero-label">${escapeHtml(label)}</div>`
      + `<div class="md-hero-val">${valHtml}</div>${chgHtml}`
      + flowSlot + `</div>`;
  }
  return `<div class="md-row">`
    + `<span class="md-row-label">${escapeHtml(label)}</span>`
    + `<span class="md-row-val">${valHtml}</span>${rowChgHtml}</div>`;
}

// 최근 투자자별 순매수(개인/외국인/기관). 각 국내 지수 카드(코스피/코스닥)
// 안에 해당 시장 수급을 넣는다. {kospi, kosdaq} 형태이며 코드를 소문자화해
// 매칭한다(KOSPI→kospi).
let _mdFlows = null;
let _flowsInFlight = false;

function _cardFlowHtml(flow) {
  if (!flow) return '';
  const actors = [['individual', '개인'], ['foreign', '외국인'], ['institution', '기관']];
  const rows = actors.map(([k, name]) => {
    const d = flow[k] || {};
    const cls = d.direction === 'up' ? 'md-up' : (d.direction === 'down' ? 'md-down' : 'md-flat');
    return `<div class="cf-row"><span class="cf-actor">${name}</span>`
      + `<span class="cf-val ${cls}">${escapeHtml(String(d.value || '-'))}</span></div>`;
  }).join('');
  return `<div class="cf-head">순매수<span class="cf-date">${escapeHtml(String(flow.date || ''))} · 억</span></div>${rows}`;
}

async function loadInvestorFlows() {
  if (_flowsInFlight) return;
  _flowsInFlight = true;
  try {
    const r = await apiFetch('/api/market/investor-flows');
    const data = r.ok ? await r.json() : {};
    _mdFlows = data.flows || null;
    document.querySelectorAll('.md-card-flow[data-flow-code]').forEach((el) => {
      const f = _mdFlows ? _mdFlows[String(el.dataset.flowCode).toLowerCase()] : null;
      el.innerHTML = _cardFlowHtml(f);
    });
  } catch (e) {
    console.warn('investor flows load failed', e);
  } finally {
    _flowsInFlight = false;
  }
}

function _mdSectionHtml(category, codes, catalog, dataMap, variant) {
  const body = variant === 'hero'
    ? `<div class="md-hero">${codes.map((c) => _mdCardHtml(c, catalog, dataMap, 'hero')).join('')}</div>`
    : `<div class="md-rows">${codes.map((c) => _mdCardHtml(c, catalog, dataMap, 'list')).join('')}</div>`;
  return `<section class="md-section${variant === 'hero' ? ' md-hero-section' : ''}" data-md-cat="${escapeHtml(category)}">`
    + `<h3 class="md-section-title">${escapeHtml(category)}</h3>${body}</section>`;
}

function _mdKospiFuturesSectionHtml() {
  return '<section class="md-section md-kospi-futures-section" data-md-cat="야간선물">'
    + '<h3 class="md-section-title">KOSPI 선물</h3>'
    + '<div class="md-kospi-futures-frame-wrap">'
    + `<iframe class="md-kospi-futures-frame" src="${escapeHtml(MD_KOSPI_FUTURES_FRAME_URL)}" `
    + 'title="KOSPI 선물 실시간 그래프" loading="eager" referrerpolicy="no-referrer"></iframe>'
    + '</div></section>';
}

// --- 바이낸스 섹션: USDT↔원화 토글(기본 원화) ---
// 값은 바이낸스 USDT 선물가. 원화 모드면 USD_KRW 환율로 환산해 표시한다.
// (등락%·방향은 통화와 무관하므로 그대로 둔다.)
let _bnbCcy = null;  // 'KRW' | 'USDT' — lazy init(localStorage)

function _bnbCurrentCcy() {
  if (_bnbCcy == null) {
    _bnbCcy = 'KRW';
    try { if (localStorage.getItem('bnbCcy') === 'USDT') _bnbCcy = 'USDT'; } catch (e) { /* noop */ }
  }
  return _bnbCcy;
}

function _bnbParseNum(s) {
  if (s == null) return null;
  const n = Number(String(s).replace(/,/g, ''));
  return isFinite(n) ? n : null;
}

function _bnbUsdKrwRate(dataMap) {
  return _bnbParseNum(dataMap && dataMap.USD_KRW ? dataMap.USD_KRW.value : null);
}

function _bnbFmtKrw(n) {
  return Math.round(n).toLocaleString('en-US');
}

function _bnbRowsHtml(codes, catalog, dataMap) {
  const useKrw = _bnbCurrentCcy() === 'KRW';
  const rate = _bnbUsdKrwRate(dataMap);
  // 원화 모드 + 환율이 있으면 value/change 를 환산한 view 로 기존 행 렌더러 재사용.
  const view = {};
  for (const c of codes) {
    const d = dataMap && dataMap[c] ? dataMap[c] : {};
    if (useKrw && rate) {
      const v = _bnbParseNum(d.value);
      const ch = _bnbParseNum(d.change);
      view[c] = Object.assign({}, d, {
        value: v != null ? _bnbFmtKrw(v * rate) : d.value,
        change: ch != null ? _bnbFmtKrw(ch * rate) : d.change,
      });
    } else {
      view[c] = d;
    }
  }
  return codes.map((c) => _mdCardHtml(c, catalog, view, 'list')).join('');
}

function _mdBinanceSectionHtml(codes, catalog, dataMap) {
  const ccy = _bnbCurrentCcy();
  const btn = (key, label) =>
    `<button class="mv-mkt${ccy === key ? ' active' : ''}" data-bnb-ccy="${key}">${label}</button>`;
  return '<section class="md-section bnb-section" id="mdBinanceSection">'
    + '<div class="mv-head"><h3 class="md-section-title">바이낸스</h3>'
    + `<div class="mv-mkts">${btn('KRW', '원화')}${btn('USDT', 'USDT')}</div></div>`
    + `<div class="md-rows">${_bnbRowsHtml(codes, catalog, dataMap)}</div></section>`;
}

function _mdWireBinanceToggle(catalog, dataMap) {
  const sec = document.getElementById('mdBinanceSection');
  if (!sec) return;
  const codes = Object.keys(catalog || {}).filter(
    (c) => (catalog[c] || {}).category === '바이낸스'
  );
  sec.querySelectorAll('[data-bnb-ccy]').forEach((b) =>
    b.addEventListener('click', () => {
      const ccy = b.dataset.bnbCcy === 'USDT' ? 'USDT' : 'KRW';
      if (ccy === _bnbCurrentCcy()) return;
      _bnbCcy = ccy;
      try { localStorage.setItem('bnbCcy', ccy); } catch (e) { /* noop */ }
      // 버튼은 유지(리스너 보존)하고 active 표시 + 행만 다시 그린다.
      sec.querySelectorAll('[data-bnb-ccy]').forEach((x) =>
        x.classList.toggle('active', x.dataset.bnbCcy === ccy));
      const rowsEl = sec.querySelector('.md-rows');
      if (rowsEl) rowsEl.innerHTML = _bnbRowsHtml(codes, catalog, dataMap);
    })
  );
}

// --- 박스 단위 라이브 갱신: 바이낸스만 10초마다(가볍게) ---
// 전용 엔드포인트(/api/market/live, 서버 8초 캐시)로 필요한 코드만 받아 해당
// 섹션 행만 교체한다(전체 재렌더·차트 재init 없음). 숨겨진 뷰/백그라운드 탭은 스킵.
const MD_LIVE_CODES = ['BNB_EWY', 'BNB_SAMSUNG', 'BNB_SKHYNIX', 'BNB_HYUNDAI'];
const MD_LIVE_INTERVAL_MS = 10000;
let _mdLastDataMap = null;
let _mdLiveTimer = null;

function _mdLiveActive() {
  if (typeof document === 'undefined' || document.hidden) return false;
  const view = document.getElementById('investingView');
  if (view && view.offsetParent === null) return false;  // 숨겨진 투자정보 뷰면 스킵
  return !!document.getElementById('mdIndRail');
}

async function _mdLiveRefresh() {
  if (!_mdCatalog || !_mdLastDataMap || !_mdLiveActive()) return;
  let live;
  try {
    const r = await apiFetch('/api/market/live?codes=' + encodeURIComponent(MD_LIVE_CODES.join(',')));
    if (!r.ok) return;
    live = await r.json();
  } catch (e) {
    return;
  }
  if (!live || typeof live !== 'object') return;
  // 최신값만 머지(USD_KRW 등 환산 기준은 그대로 유지).
  Object.assign(_mdLastDataMap, live);
  // 바이낸스: 통화 토글 상태 반영해 행만 교체(토글 버튼/리스너는 유지).
  const bnb = document.getElementById('mdBinanceSection');
  const bnbRows = bnb && bnb.querySelector('.md-rows');
  if (bnbRows) {
    const codes = Object.keys(_mdCatalog).filter((c) => (_mdCatalog[c] || {}).category === '바이낸스');
    bnbRows.innerHTML = _bnbRowsHtml(codes, _mdCatalog, _mdLastDataMap);
  }
}

function _mdStartLiveRefresh() {
  if (_mdLiveTimer || typeof setInterval === 'undefined') return;
  _mdLiveTimer = setInterval(_mdLiveRefresh, MD_LIVE_INTERVAL_MS);
}

// --- 국채 (yield curve + 국가별 10년물 비교) ---

function _bondVal(d) {
  if (!d || d.value == null || d.value === '') return null;
  const n = Number(String(d.value).replace(/,/g, ''));
  return isFinite(n) ? n : null;
}

// 전일대비 금리 변동(%p, 부호는 direction). 값이 없으면 null.
function _bondChg(d) {
  if (!d || d.change == null || d.change === '') return null;
  const n = Number(String(d.change).replace(/[,+%]/g, ''));
  if (!isFinite(n)) return null;
  return d.direction === 'down' ? -Math.abs(n) : Math.abs(n);
}

function _bondMatLabel(m) {
  if (m === 0) return '1D';  // 익일물(overnight): 한국=KOFR, 미국=SOFR
  if (m < 1) return Math.round(m * 12) + 'M';  // 0.25 → 3M
  return m + 'Y';
}

// 한국·미국·일본 곡선을 공통 만기축에 맞춰 정렬. {labels, kr[], us[], jp[]} (없는 만기는 null).
function _mdBondCurve(codes, catalog, dataMap) {
  const pick = (country) => codes
    .filter((c) => (catalog[c] || {}).country === country && (catalog[c] || {}).maturity != null)
    .map((c) => ({
      m: catalog[c].maturity,
      v: _bondVal(dataMap ? dataMap[c] : null),
      chg: _bondChg(dataMap ? dataMap[c] : null),
    }))
    .filter((x) => x.v != null)
    .sort((a, b) => a.m - b.m);
  const kr = pick('KR');
  const us = pick('US');
  const jp = pick('JP');
  const mats = [...new Set([...kr, ...us, ...jp].map((x) => x.m))].sort((a, b) => a - b);
  const vMap = (arr) => new Map(arr.map((x) => [x.m, x.v]));
  const cMap = (arr) => new Map(arr.map((x) => [x.m, x.chg]));
  const krV = vMap(kr); const usV = vMap(us); const jpV = vMap(jp);
  const krC = cMap(kr); const usC = cMap(us); const jpC = cMap(jp);
  const at = (map, m) => (map.has(m) ? map.get(m) : null);
  return {
    labels: mats.map(_bondMatLabel),
    kr: mats.map((m) => at(krV, m)),
    us: mats.map((m) => at(usV, m)),
    jp: mats.map((m) => at(jpV, m)),
    krChg: mats.map((m) => at(krC, m)),
    usChg: mats.map((m) => at(usC, m)),
    jpChg: mats.map((m) => at(jpC, m)),
  };
}

// 국가별 10년물(maturity===10) 비교, 금리 내림차순.
function _mdBondCountries(codes, catalog, dataMap) {
  return codes
    .filter((c) => (catalog[c] || {}).maturity === 10)
    .map((c) => ({
      country: catalog[c].country,
      name: BOND_COUNTRY_NAMES[catalog[c].country] || catalog[c].country,
      value: _bondVal(dataMap ? dataMap[c] : null),
    }))
    .filter((x) => x.value != null)
    .sort((a, b) => b.value - a.value);
}

function _bondCurveTableHtml(curve) {
  if (!curve.labels.length) return '';
  // 금리(값)와 전일대비 변동(%p)을 국가별 별도 컬럼으로 분리해 자리를 맞춘다.
  const valCell = (v, grp) => `<td class="bt-val${grp ? ' bt-grp' : ''}">${v == null ? '-' : v.toFixed(2)}</td>`;
  const chgCell = (c) => {
    if (c == null || !isFinite(c)) return '<td class="bt-chg">-</td>';
    const cls = c > 0 ? 'md-up' : (c < 0 ? 'md-down' : 'md-flat');
    return `<td class="bt-chg ${cls}">${c > 0 ? '+' : ''}${c.toFixed(2)}</td>`;
  };
  const rows = curve.labels.map((lab, i) =>
    `<tr><td class="bt-mat">${escapeHtml(lab)}</td>`
    + valCell(curve.kr[i], false) + chgCell(curve.krChg[i])
    + valCell(curve.us[i], true) + chgCell(curve.usChg[i])
    + valCell(curve.jp[i], true) + chgCell(curve.jpChg[i])
    + '</tr>'
  ).join('');
  return '<table class="bond-tbl bond-tbl-split">'
    + '<thead>'
    + '<tr><th rowspan="2">만기</th>'
    + '<th colspan="2">한국</th><th colspan="2" class="bt-grp">미국</th><th colspan="2" class="bt-grp">일본</th></tr>'
    + '<tr><th class="bt-sub">금리</th><th class="bt-sub">변동</th>'
    + '<th class="bt-sub bt-grp">금리</th><th class="bt-sub">변동</th>'
    + '<th class="bt-sub bt-grp">금리</th><th class="bt-sub">변동</th></tr>'
    + '</thead>'
    + `<tbody>${rows}</tbody></table>`;
}

function _mdBondSectionHtml() {
  // 국가별 10년물은 비교 그래프(bondCountryCompare)가 모든 국가를 막대로 보여주므로
  // 별도 수치 표는 생략한다. 기간별 금리는 곡선이 한·미·일이라 표를 함께 둔다.
  return '<section class="md-section md-bond-section">'
    + '<h3 class="md-section-title">국채</h3>'
    + '<div class="md-bond-sub">기간별 금리 (Yield Curve · 변동=전일대비 %p)</div>'
    + '<div class="md-bond-chart" id="bondYieldCurve"></div>'
    + '<div class="md-bond-table" id="bondCurveTable"></div>'
    + '<div class="md-bond-sub">국가별 10년물</div>'
    + '<div class="md-bond-chart md-bond-chart-sm" id="bondCountryCompare"></div>'
    + '</section>';
}

function _disposeBondCharts() {
  _bondCharts.forEach(({ ec, ro }) => {
    try { if (ro) ro.disconnect(); } catch (e) { /* noop */ }
    try { if (ec) ec.dispose(); } catch (e) { /* noop */ }
  });
  _bondCharts = [];
}

function _bondChartTheme() {
  const cs = getComputedStyle(document.documentElement);
  return {
    text: cs.getPropertyValue('--text-secondary').trim() || '#888',
    grid: cs.getPropertyValue('--border').trim() || '#333',
  };
}

function _bondTrackChart(el, ec) {
  let ro = null;
  if (typeof ResizeObserver !== 'undefined') {
    ro = new ResizeObserver(() => { try { ec.resize(); } catch (e) { /* noop */ } });
    ro.observe(el);
  }
  _bondCharts.push({ ec, ro });
}

function _drawBondCurveChart(curve) {
  const el = document.getElementById('bondYieldCurve');
  if (!el || !window.echarts || !curve.labels.length) return;
  const t = _bondChartTheme();
  const ec = echarts.init(el);
  const mkSeries = (name, data, color) => ({
    name, type: 'line', data: data.map((v) => (v == null ? '-' : v)),
    smooth: 0.2, symbol: 'circle', symbolSize: 5, connectNulls: true,
    lineStyle: { color, width: 2 }, itemStyle: { color },
  });
  ec.setOption({
    grid: { left: 46, right: 14, top: 28, bottom: 24 },
    legend: { data: ['한국', '미국', '일본'], top: 0, right: 0, textStyle: { color: t.text, fontSize: 11 }, itemWidth: 18, itemHeight: 2 },
    xAxis: { type: 'category', data: curve.labels, axisLine: { lineStyle: { color: t.grid } }, axisLabel: { color: t.text, fontSize: 10 }, splitLine: { show: false } },
    yAxis: { type: 'value', min: 0, axisLine: { show: false }, axisLabel: { color: t.text, fontSize: 10, formatter: (v) => v.toFixed(1) + '%' }, splitLine: { lineStyle: { color: t.grid, width: 0.5 } } },
    tooltip: {
      trigger: 'axis',
      formatter(ps) {
        let h = ps[0] ? ps[0].axisValue : '';
        for (const p of ps) {
          if (p.value == null || p.value === '-') continue;
          h += `<br/><span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:${p.color};margin-right:4px;"></span>${p.seriesName}: ${Number(p.value).toFixed(2)}%`;
        }
        return h;
      },
    },
    series: [
      mkSeries('한국', curve.kr, BOND_CURVE_COLORS.KR),
      mkSeries('미국', curve.us, BOND_CURVE_COLORS.US),
      mkSeries('일본', curve.jp, BOND_CURVE_COLORS.JP),
    ],
  });
  _bondTrackChart(el, ec);
}

function _drawBondCountryChart(countries) {
  const el = document.getElementById('bondCountryCompare');
  if (!el || !window.echarts || !countries.length) return;
  const t = _bondChartTheme();
  const ec = echarts.init(el);
  // 가로 막대: 금리 높은 국가가 위로 오도록 역순(echarts y-category는 아래부터).
  const ordered = countries.slice().reverse();
  const names = ordered.map((c) => c.name);
  ec.setOption({
    grid: { left: 48, right: 44, top: 8, bottom: 8 },
    xAxis: { type: 'value', scale: true, axisLine: { show: false }, axisLabel: { color: t.text, fontSize: 10, formatter: (v) => v.toFixed(1) }, splitLine: { lineStyle: { color: t.grid, width: 0.5 } } },
    yAxis: { type: 'category', data: names, axisLine: { lineStyle: { color: t.grid } }, axisLabel: { color: t.text, fontSize: 11 } },
    tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' }, formatter: (ps) => `${ps[0].name}: ${Number(ps[0].value).toFixed(2)}%` },
    series: [{
      type: 'bar', barWidth: '55%',
      data: ordered.map((c) => ({
        value: c.value,
        itemStyle: { color: (c.country === 'KR' || c.country === 'US') ? '#2563eb' : '#94a3b8', borderRadius: [0, 3, 3, 0] },
      })),
      label: { show: true, position: 'right', color: t.text, fontSize: 10, formatter: (p) => Number(p.value).toFixed(2) },
    }],
  });
  _bondTrackChart(el, ec);
}

function _mdRenderBonds(codes, catalog, dataMap) {
  const curve = _mdBondCurve(codes, catalog, dataMap);
  const countries = _mdBondCountries(codes, catalog, dataMap);
  const curveTbl = document.getElementById('bondCurveTable');
  if (curveTbl) curveTbl.innerHTML = _bondCurveTableHtml(curve);
  // 다중 시리즈/막대라 echarts 로 통일(모바일 포함, 목표가 차트와 동일 전략).
  if (typeof loadEcharts === 'function') {
    loadEcharts().then(() => {
      _drawBondCurveChart(curve);
      _drawBondCountryChart(countries);
    }).catch((e) => console.warn('bond charts load failed', e));
  }
}

function _mdRenderDashboard(catalog, dataMap) {
  // The two-column shell is stable HTML in index.html; we only fill the
  // indicator slots so sibling widgets (movers, 수급, 뉴스) aren't disturbed.
  const mainEl = document.getElementById('mdIndMain');
  const railEl = document.getElementById('mdIndRail');
  if (!mainEl || !railEl) return;
  _mdLastDataMap = dataMap;  // 라이브 갱신이 머지·환산에 쓸 최신 dataMap
  _disposeBondCharts();  // 재렌더 전 이전 차트 정리(누수 방지)
  const groups = _mdGroupByCategory(catalog);
  if (!groups.length) {
    mainEl.innerHTML = '<div class="md-loading">표시할 지표가 없습니다.</div>';
    return;
  }
  const main = [];
  const rail = [];
  let bondCodes = null;
  for (const { category, codes } of groups) {
    if (category === '야간선물') {
      rail.push(_mdKospiFuturesSectionHtml());
      continue;
    }
    if (category === '국채') {
      bondCodes = codes;
      main.push(_mdBondSectionHtml());  // 차트 컨테이너 + 수치 리스트 자리
      continue;
    }
    if (category === '바이낸스') {
      // 우측 rail 에 통화 토글(원화/USDT) 포함 섹션으로 렌더.
      rail.push(_mdBinanceSectionHtml(codes, catalog, dataMap));
      continue;
    }
    const isHero = MD_HERO_CATEGORIES.includes(category);
    const html = _mdSectionHtml(category, codes, catalog, dataMap, isHero ? 'hero' : 'list');
    (isHero || MD_MAIN_CATEGORIES.includes(category) ? main : rail).push(html);
  }
  mainEl.innerHTML = main.join('');
  railEl.innerHTML = rail.join('');
  _mdWireBinanceToggle(catalog, dataMap);
  if (bondCodes) _mdRenderBonds(bondCodes, catalog, dataMap);
}

async function loadInvestingDashboard(refresh = false) {
  // Sibling widgets load independently so a slow/failed indicator fetch never
  // blocks them (and vice versa).
  if (typeof loadMarketMovers === 'function') loadMarketMovers();
  if (typeof loadSectors === 'function') loadSectors();
  if (typeof loadMarketNews === 'function') loadMarketNews();
  if (typeof loadExternalInsights === 'function') loadExternalInsights();
  if (typeof loadEconomicCalendar === 'function') loadEconomicCalendar();
  if (_mdInFlight) return _mdInFlight;
  _mdInFlight = (async () => {
    try {
      if (!_mdCatalog || refresh) {
        const cr = await apiFetch('/api/market-indicators');
        if (cr.ok) _mdCatalog = await cr.json();
      }
      const catalog = _mdCatalog || {};
      const codes = Object.keys(catalog);
      if (!codes.length) return;
      const sr = await apiFetch('/api/market-summary?codes=' + encodeURIComponent(codes.join(',')));
      const dataMap = sr.ok ? await sr.json() : {};
      _mdRenderDashboard(catalog, dataMap);
      _mdLoadedOnce = true;
      _mdStartLiveRefresh();  // 바이낸스 10초 라이브 갱신 시작(1회만)
      // 수급 슬롯은 hero 섹션과 함께 생성되므로 렌더 직후 채운다.
      if (typeof loadInvestorFlows === 'function') loadInvestorFlows();
    } catch (e) {
      console.warn('investing dashboard load failed', e);
      const mainEl = document.getElementById('mdIndMain');
      if (mainEl && !_mdLoadedOnce) {
        mainEl.innerHTML = '<div class="md-loading">시장 지표를 불러오지 못했습니다.</div>';
      }
    } finally {
      _mdInFlight = null;
    }
  })();
  return _mdInFlight;
}


// --- 시장 랭킹 (market movers): 시총상위 / 거래상위 / 급상승 / 급하락 ---
const MV_TABS = [
  { kind: 'market_cap', label: '시총상위' },
  { kind: 'volume', label: '거래상위' },
  { kind: 'rising', label: '급상승' },
  { kind: 'falling', label: '급하락' },
];
let _mvKind = 'market_cap';
let _mvMarket = 'kospi';
let _mvInFlight = false;

function _mvRenderShell(root) {
  const tabs = MV_TABS.map((t) =>
    `<button class="mv-tab${t.kind === _mvKind ? ' active' : ''}" data-kind="${t.kind}">${escapeHtml(t.label)}</button>`
  ).join('');
  const markets = [['kospi', '코스피'], ['kosdaq', '코스닥']].map(([m, label]) =>
    `<button class="mv-mkt${m === _mvMarket ? ' active' : ''}" data-market="${m}">${label}</button>`
  ).join('');
  root.innerHTML = '<section class="md-section mv-section">'
    + '<div class="mv-head"><h3 class="md-section-title">시장 랭킹</h3>'
    + `<div class="mv-mkts">${markets}</div></div>`
    + `<div class="mv-tabs">${tabs}</div>`
    + '<div class="mv-body"><div class="md-loading">불러오는 중...</div></div>'
    + '</section>';
  root.querySelectorAll('.mv-tab').forEach((b) =>
    b.addEventListener('click', () => { _mvKind = b.dataset.kind; loadMarketMovers(); }));
  root.querySelectorAll('.mv-mkt').forEach((b) =>
    b.addEventListener('click', () => { _mvMarket = b.dataset.market; loadMarketMovers(); }));
}

function _mvRenderRows(root, items) {
  const body = root.querySelector('.mv-body');
  if (!body) return;
  if (!items.length) {
    body.innerHTML = '<div class="md-loading">표시할 종목이 없습니다.</div>';
    return;
  }
  const showMetric = _mvKind === 'market_cap' || _mvKind === 'volume';
  body.innerHTML = items.map((it) => {
    const dirCls = it.direction === 'up' ? 'md-up' : (it.direction === 'down' ? 'md-down' : 'md-flat');
    const metric = showMetric && it.metric
      ? `<span class="mv-metric">${escapeHtml(String(it.metric))}</span>` : '';
    return `<button class="mv-row" data-code="${escapeHtml(String(it.code || ''))}">`
      + `<span class="mv-rank">${escapeHtml(String(it.rank || ''))}</span>`
      + `<span class="mv-name">${escapeHtml(String(it.name || ''))}</span>`
      + `<span class="mv-price">${escapeHtml(String(it.price || '-'))}</span>`
      + `<span class="mv-chg ${dirCls}">${escapeHtml(String(it.change_pct || ''))}</span>`
      + `${metric}</button>`;
  }).join('');
  body.querySelectorAll('.mv-row').forEach((b) =>
    b.addEventListener('click', () => {
      const code = b.dataset.code;
      if (!code) return;
      if (typeof switchView === 'function') switchView('analysis');
      if (typeof analyzeStock === 'function') analyzeStock(code);
    }));
}

// --- 업종별 등락 (sector performance) — rail widget ---
let _secInFlight = false;

function _secRenderRows(root, items) {
  if (!items.length) {
    root.innerHTML = '<section class="md-section"><h3 class="md-section-title">업종별 등락</h3>'
      + '<div class="md-loading">표시할 업종이 없습니다.</div></section>';
    return;
  }
  const rows = items.map((it) => {
    const dirCls = it.direction === 'up' ? 'md-up' : (it.direction === 'down' ? 'md-down' : 'md-flat');
    return `<div class="sec-row">`
      + `<span class="sec-name">${escapeHtml(String(it.name || ''))}</span>`
      + `<span class="sec-chg ${dirCls}">${escapeHtml(String(it.change_pct || ''))}</span></div>`;
  }).join('');
  root.innerHTML = '<section class="md-section"><h3 class="md-section-title">업종별 등락</h3>'
    + `<div class="sec-rows">${rows}</div></section>`;
}

async function loadSectors() {
  const root = document.getElementById('marketSectors');
  if (!root || _secInFlight) return;
  _secInFlight = true;
  try {
    const r = await apiFetch('/api/market/sectors?limit=12');
    const data = r.ok ? await r.json() : { sectors: [] };
    _secRenderRows(root, data.sectors || []);
  } catch (e) {
    console.warn('sectors load failed', e);
  } finally {
    _secInFlight = false;
  }
}

// --- 분석 도구 (external insights) — 외부 GitHub Pages 도구 요약 허브 ---
// 지주사 NAV 디스카운트 / 우선주 괴리율 / 스팩 / 김치프리미엄 / 국민연금.
// public JSON 요약을 한 섹션에 카드로 묶고, 항목 클릭 시 해당 도구로(새 탭,
// 가능하면 deep-link).
let _extInFlight = false;

function _extSafeUrl(url) {
  return /^https?:\/\//.test(String(url || '')) ? String(url) : '#';
}

// 외부 도구는 새 탭(deep-link)으로 열린다. 현재 앱 테마를 ?theme= 로 넘겨
// 열린 대시보드가 같은 라이트/다크로 뜨게 한다.
function _withTheme(url) {
  const u = String(url || '');
  if (!/^https?:\/\//.test(u)) return u;
  const theme = document.documentElement.getAttribute('data-theme') === 'dark' ? 'dark' : 'light';
  return u + (u.includes('?') ? '&' : '?') + 'theme=' + theme;
}

function _extHref(url) {
  return _extSafeUrl(_withTheme(url));
}

function _extPct(v, signed) {
  if (v === null || v === undefined || v === '') return '-';
  const n = Number(v);
  if (!isFinite(n)) return escapeHtml(String(v));
  return (signed && n > 0 ? '+' : '') + n.toFixed(signed ? 2 : 1) + '%';
}

function _extLinkRows(rows, valKey, baseUrl, useCode) {
  return (rows || []).map((r) => {
    // holding 도구만 ?code= deep-link 지원. 그 외엔 도구 홈으로.
    const href = useCode && r.code ? `${baseUrl}?code=${encodeURIComponent(r.code)}` : baseUrl;
    return `<a class="ext-row" href="${escapeHtml(_extHref(href))}" target="_blank" rel="noopener noreferrer">`
      + `<span class="ext-name">${escapeHtml(String(r.name || r.code || ''))}</span>`
      + `<span class="ext-val">${escapeHtml(_extPct(r[valKey]))}</span></a>`;
  }).join('');
}

function _extSpacRows(rows, baseUrl) {
  // 스팩은 현재가(원)를 그대로 보여준다(저가순). spac-hunter 는 ?code= deep-link 지원.
  return (rows || []).map((r) => {
    const href = r.code ? `${baseUrl}?code=${encodeURIComponent(r.code)}` : baseUrl;
    const n = Number(r.currentPrice);
    const price = (r.currentPrice != null && isFinite(n)) ? n.toLocaleString() : '-';
    return `<a class="ext-row" href="${escapeHtml(_extHref(href))}" target="_blank" rel="noopener noreferrer">`
      + `<span class="ext-name">${escapeHtml(String(r.name || r.code || ''))}</span>`
      + `<span class="ext-val">${escapeHtml(price)}</span></a>`;
  }).join('');
}

// 국민연금 중기 자산배분 목표 비중(%) — current.json 의 allocation 에는 추정
// 현재 비중(pct)만 있어, 공표된 목표치를 key 기준 상수로 둔다. 데이터 소스가
// class 별 target 을 제공하면 그 값을 우선한다.
const NPS_TARGET_ALLOC = {
  domestic_stock: 20.8,
  domestic_bond: 23.1,
  foreign_stock: 34.7,
  foreign_bond: 7.4,
  alternative: 14.0,
};

function _extAllocRows(classes, url) {
  // 자산배분 행은 종목 deep-link이 아니라 도구 홈으로(테마 포함).
  // 값 = 추정 현재 비중, 괄호 = 목표 비중.
  const href = escapeHtml(_extHref(url));
  return (classes || []).map((c) => {
    const target = c.target != null ? c.target : NPS_TARGET_ALLOC[c.key];
    const valTxt = _extPct(c.pct) + (target != null ? ` (${_extPct(target)})` : '');
    return `<a class="ext-row" href="${href}" target="_blank" rel="noopener noreferrer">`
      + `<span class="ext-name">${escapeHtml(String(c.label || c.key || ''))}</span>`
      + `<span class="ext-val">${escapeHtml(valTxt)}</span></a>`;
  }).join('');
}

function _extCard(title, url, subText, bodyHtml) {
  return '<div class="ext-card">'
    + `<div class="ext-head"><span>${escapeHtml(title)}</span>`
    + `<a href="${escapeHtml(_extHref(url))}" target="_blank" rel="noopener noreferrer" class="ext-more" title="도구 열기">↗</a></div>`
    + (subText ? `<div class="ext-sub">${escapeHtml(subText)}</div>` : '')
    + `<div class="ext-rows">${bodyHtml}</div></div>`;
}

function _extRender(root, data) {
  const cards = [];
  const h = data && data.holding;
  if (h && (h.top || []).length) {
    const sub = h.averageRatio != null ? `평균 ${_extPct(h.averageRatio)} · 보유가치/시총` : '보유가치/시총';
    cards.push(_extCard('지주사 저평가', h.url, sub, _extLinkRows(h.top, 'ratio', h.url, true)));
  }
  const s = data && data.spread;
  if (s && (s.top || []).length) {
    const sub = s.averageSpread != null ? `평균 괴리율 ${_extPct(s.averageSpread)}` : '우선주 괴리율';
    cards.push(_extCard('우선주 괴리율', s.url, sub, _extLinkRows(s.top, 'spread', s.url, false)));
  }
  const p = data && data.spac;
  if (p && (p.top || []).length) {
    // 현재가가 낮은(공모가 대비 할인 큰) 순. spac-hunter 는 ?code= deep-link 지원.
    cards.push(_extCard('스팩 저가순', p.url, '현재가 낮은 순', _extSpacRows(p.top, p.url)));
  }
  const g = data && data.goldGap;
  if (g && (g.assets || []).length) {
    const rows = g.assets.map((a) => {
      const n = Number(a.gap);
      const cls = isFinite(n) ? (n > 0 ? 'md-up' : (n < 0 ? 'md-down' : 'md-flat')) : 'md-flat';
      return `<a class="ext-row" href="${escapeHtml(_extHref(a.link || g.url))}" target="_blank" rel="noopener noreferrer">`
        + `<span class="ext-name">${escapeHtml(String(a.label || a.key || ''))}</span>`
        + `<span class="ext-val ${cls}">${escapeHtml(_extPct(a.gap, true))}</span></a>`;
    }).join('');
    cards.push(_extCard('김치프리미엄', g.url, '국내가 vs 국제가', rows));
  }
  const ep = data && data.etfPicks;
  if (ep && (ep.top || []).length) {
    // AIYN 점수 TOP 100 중 날마다(서버, KST 날짜 시드) 5개 추첨.
    // 값 = 일간 등락률(서버가 실시간 시세로 별도 조회) — 방향 색상 포함.
    const rows = ep.top.map((r) => {
      const n = Number(r.changePct);
      const has = r.changePct != null && isFinite(n);
      const cls = has ? (n > 0 ? 'md-up' : (n < 0 ? 'md-down' : 'md-flat')) : 'md-flat';
      return `<a class="ext-row" href="${escapeHtml(_extHref(r.link || ep.url))}" target="_blank" rel="noopener noreferrer">`
        + `<span class="ext-name">${escapeHtml(String(r.name || r.code || ''))}</span>`
        + `<span class="ext-val ${cls}">${escapeHtml(_extPct(r.changePct, true))}</span></a>`;
    }).join('');
    cards.push(_extCard('오늘의 추천 ETF', ep.url, 'AIYN TOP 100 중 오늘의 5선', rows));
  }
  const nps = data && data.nps;
  const npsAlloc = nps && nps.allocation;
  if (npsAlloc && (npsAlloc.classes || []).length) {
    // 기금 자산배분(국내주식/해외주식/국내채권/해외채권/대체투자): 추정 현재
    // 비중과 괄호 안 목표 비중을 함께 표시한다.
    cards.push(_extCard('국민연금 자산 비중', nps.url, '자산배분 (목표치)', _extAllocRows(npsAlloc.classes, nps.url)));
  } else if (nps && (nps.top || []).length) {
    const sub = nps.nav != null
      ? `NAV ${Number(nps.nav).toFixed(1)} · 비중 상위`
      : '포트폴리오 비중 상위';
    cards.push(_extCard('국민연금', nps.url, sub, _extLinkRows(nps.top, 'weight', nps.url, false)));
  }
  if (!cards.length) {
    root.innerHTML = '';
    return;
  }
  root.innerHTML = '<section class="md-section ext-section">'
    + '<h3 class="md-section-title">분석 도구</h3>'
    + `<div class="ext-grid">${cards.join('')}</div></section>`;
}

async function loadExternalInsights() {
  const root = document.getElementById('externalTools');
  if (!root || _extInFlight) return;
  _extInFlight = true;
  try {
    const r = await apiFetch('/api/external/insights');
    const data = r.ok ? await r.json() : {};
    _extRender(root, data);
  } catch (e) {
    console.warn('external insights load failed', e);
  } finally {
    _extInFlight = false;
  }
}

// --- 주요 뉴스 (market news) — main-column widget ---
let _newsInFlight = false;

function _newsRender(root, items) {
  if (!items.length) {
    root.innerHTML = '<section class="md-section"><h3 class="md-section-title">주요 뉴스</h3>'
      + '<div class="md-loading">표시할 뉴스가 없습니다.</div></section>';
    return;
  }
  const rows = items.map((it) => {
    const meta = [it.source, it.date].filter(Boolean).map((s) => escapeHtml(String(s))).join(' · ');
    const summ = it.summary
      ? `<div class="news-summary">${escapeHtml(String(it.summary))}</div>` : '';
    const url = String(it.url || '');
    const safeUrl = /^https?:\/\//.test(url) ? url : '#';
    return `<a class="news-item" href="${escapeHtml(safeUrl)}" target="_blank" rel="noopener noreferrer">`
      + `<div class="news-title">${escapeHtml(String(it.title || ''))}</div>`
      + summ
      + (meta ? `<div class="news-meta">${meta}</div>` : '')
      + '</a>';
  }).join('');
  root.innerHTML = '<section class="md-section"><h3 class="md-section-title">주요 뉴스</h3>'
    + `<div class="news-list">${rows}</div></section>`;
}

async function loadMarketNews() {
  const root = document.getElementById('marketNews');
  if (!root || _newsInFlight) return;
  _newsInFlight = true;
  try {
    const r = await apiFetch('/api/market/news?limit=8');
    const data = r.ok ? await r.json() : { news: [] };
    _newsRender(root, data.news || []);
  } catch (e) {
    console.warn('market news load failed', e);
  } finally {
    _newsInFlight = false;
  }
}

async function loadMarketMovers() {
  const root = document.getElementById('marketMovers');
  if (!root || _mvInFlight) return;
  _mvInFlight = true;
  _mvRenderShell(root);
  try {
    const r = await apiFetch(`/api/market/movers?kind=${encodeURIComponent(_mvKind)}&market=${encodeURIComponent(_mvMarket)}&limit=10`);
    const data = r.ok ? await r.json() : { items: [] };
    _mvRenderRows(root, data.items || []);
  } catch (e) {
    console.warn('market movers load failed', e);
    const body = root.querySelector('.mv-body');
    if (body) body.innerHTML = '<div class="md-loading">불러오지 못했습니다.</div>';
  } finally {
    _mvInFlight = false;
  }
}
