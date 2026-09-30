// 종목분석 밸류에이션 요약 카드 그리드(#coverageNote).
// PER/PBR/ROE/배당수익률/시가총액/목표가/베타 카드 + 외부 도구(우선주 괴리율,
// 지주사, ETF)·해외 DR 카드. analysis.js(SSE 오케스트레이션 본체)에서 분리 —
// index.html 로드 순서: analysis-charts → analysis-filings → analysis-valuation
// → analysis. 전역 의존: activeIndicators/activeQuoteSnapshot/activeStockCode
// (analysis.js), _lastWeeklyIndicators·_dateDaysAgo(analysis-charts.js),
// allReports(analysis-filings.js), escapeHtml·apiFetchJson(utils.js). 생태계 링크는
// ecosystem-links.js(ecoStockTools·ecoLinkHref …)를 typeof 로 확인하고 쓴다(없으면 기존 url).

function getLatestIndicatorValue(series) {
  const entries = (series || []).filter(item => item && item.value !== null && item.value !== undefined && Number.isFinite(Number(item.value)));
  if (entries.length === 0) return null;
  return Number(entries[entries.length - 1].value);
}

// Walks backward through a series and returns the latest entry whose
// value is strictly positive. Used for trailing dividend so the card
// doesn't read 0% just because the current year hasn't had a payout
// recorded yet (e.g., it's January and last year's dividend was
// declared but no distribution yet).
function getLatestPositiveValue(series) {
  const arr = series || [];
  for (let i = arr.length - 1; i >= 0; i -= 1) {
    const v = Number(arr[i]?.value);
    if (Number.isFinite(v) && v > 0) return v;
  }
  return null;
}

function formatMetricNumber(value, suffix = '') {
  return value === null || value === undefined || !Number.isFinite(value)
    ? 'N/A'
    : `${value.toLocaleString('ko-KR', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}${suffix}`;
}

function getLatestDerivedBps(indicators) {
  const priceByYear = new Map((indicators['주가 (원)'] || []).map(item => [item.year, Number(item.value)]));
  const pbrSeries = indicators['PBR'] || [];

  for (let index = pbrSeries.length - 1; index >= 0; index -= 1) {
    const item = pbrSeries[index];
    const pbr = Number(item?.value);
    const price = priceByYear.get(item?.year);
    if (Number.isFinite(price) && Number.isFinite(pbr) && pbr > 0) {
      return price / pbr;
    }
  }

  return null;
}

// 주간 시계열에서 최신 BPS 를 역산한다 — 같은 주의 (주가 ÷ PBR). 주간 지표는
// 분석 때마다 KIS 재무비율로 새로 계산되므로, 여러 소스가 누적되는 연간
// 캐시(market_data)와 달리 아래 PBR 그래프와 항상 정합한다.
function getLatestWeeklyBps(weeklyIndicators) {
  const priceByDate = new Map((weeklyIndicators?.['주가'] || []).map(item => [item.date, Number(item.value)]));
  const pbrSeries = weeklyIndicators?.['PBR'] || [];

  for (let index = pbrSeries.length - 1; index >= 0; index -= 1) {
    const item = pbrSeries[index];
    const pbr = Number(item?.value);
    const price = priceByDate.get(item?.date);
    if (Number.isFinite(price) && Number.isFinite(pbr) && pbr > 0 && price > 0) {
      return price / pbr;
    }
  }

  return null;
}

function getCurrentValuationMetrics(indicators, weeklyIndicators, quoteSnapshot) {
  const currentPrice = Number(quoteSnapshot?.price);
  if (!Number.isFinite(currentPrice) || currentPrice <= 0) {
    return { per: null, pbr: null, roe: null, dividendYield: null, marketCap: null };
  }

  // 카드의 분모(EPS/BPS/DPS/ROE)는 주간 시계열 최신값을 우선한다 — 아래
  // 그래프와 같은 소스라 카드-그래프 불일치가 구조적으로 사라지고, 연간
  // market_data 캐시 오염(교차연도 close_price 등)이 카드로 새지 않는다.
  // 주간이 비어 있을 때만 연간 시계열로 폴백.
  const weekly = weeklyIndicators || {};
  const latestEps = getLatestIndicatorValue(weekly['EPS (원)'])
    ?? getLatestIndicatorValue(indicators['EPS (원)']);
  const latestBps = getLatestWeeklyBps(weekly) ?? getLatestDerivedBps(indicators);

  // 주간 주당배당금은 시점별 TTM(최근 365일 합)이라 그대로 연 수익률의
  // 분자가 된다. 연간 시계열의 현재 연도 값은 연초 이후 분기배당 부분합이라
  // 수익률이 실제의 1/2~1/4 로 찍히는 문제가 있었다. TTM 0 은 "최근 1년
  // 무배당"이라는 실측값이므로 0.00% 로 그대로 보여준다. 주간 배당 데이터가
  // 아예 없을 때만 기존 규칙(연간 최신 양수 DPS)으로 폴백.
  const ttmDps = getLatestIndicatorValue(weekly['주당배당금 (원)']);
  const trailingDps = ttmDps !== null ? ttmDps : getLatestPositiveValue(indicators['주당배당금 (원)']);

  const roe = getLatestIndicatorValue(weekly['ROE (%)'])
    ?? getLatestIndicatorValue(indicators['ROE (%)']);

  return {
    per: latestEps && latestEps > 0 ? currentPrice / latestEps : null,
    pbr: latestBps && latestBps > 0 ? currentPrice / latestBps : null,
    roe,
    dividendYield: trailingDps !== null ? (trailingDps / currentPrice) * 100 : null,
    marketCap: getCurrentMarketCap(indicators, currentPrice),
  };
}

// 현재 시총 = 최신 연간 (시가총액/주가)로 추정한 상장주식수 × 현재가.
// 둘 중 하나라도 없으면 최신 연간 시총을 그대로 쓴다.
function getCurrentMarketCap(indicators, currentPrice) {
  const mcapSeries = (indicators['시가총액'] && indicators['시가총액'].length)
    ? indicators['시가총액']
    : (indicators['시가총액 (억원)'] || []).map(d => ({ ...d, value: d.value != null ? d.value * 1e8 : null }));
  const latestMcap = getLatestIndicatorValue(mcapSeries);
  const latestAnnualPrice = getLatestIndicatorValue(indicators['주가 (원)']);
  if (latestMcap && latestAnnualPrice && latestAnnualPrice > 0 && currentPrice > 0) {
    return (latestMcap / latestAnnualPrice) * currentPrice;
  }
  return latestMcap;
}

// 베타는 별도 엔드포인트에서 비동기로 받아오며, 처음 렌더 시에는 '…' 로
// 플레이스홀더를 그렸다가 loadBeta 가 완료되면 해당 카드만 덮어쓴다.
let _currentBeta = null;   // {beta, sample_size, benchmark} 또는 null
let _currentStockLinks = null;  // {preferred?, holding?} 또는 null — 외부 분석 도구
let _currentDr = null;  // [{label, exchange, ticker, change_pct, converted_price}] 또는 null — 해외 DR

// 시총을 조/억 단위로 압축 표기.
function _fmtMarketCap(v) {
  const n = Number(v);
  if (!Number.isFinite(n) || n <= 0) return 'N/A';
  if (n >= 1e12) return (n / 1e12).toFixed(n >= 1e13 ? 1 : 2) + '조';
  if (n >= 1e8) return Math.round(n / 1e8).toLocaleString('ko-KR') + '억';
  return Math.round(n).toLocaleString('ko-KR');
}

// 증권사 목표가: 발표 3개월 이내 리포트의 목표주가 중 최근 5개 평균(allReports 기준).
function _computeBrokerTargetAvg() {
  if (!Array.isArray(allReports) || !allReports.length) return null;
  const cutoff = _dateDaysAgo(90);  // 3개월(=90일) 초과분 제외
  const valid = [];
  for (const r of allReports) {
    const tp = r && r.target_price ? Number(String(r.target_price).replace(/,/g, '')) : null;
    const d = r && r.date ? String(r.date).slice(0, 10) : '';
    if (tp && tp > 0 && d && d >= cutoff) valid.push({ date: d, price: tp });
  }
  if (!valid.length) return null;
  valid.sort((a, b) => b.date.localeCompare(a.date));  // 최신 발표순
  const recent = valid.slice(0, 5);
  return Math.round(recent.reduce((acc, x) => acc + x.price, 0) / recent.length);
}

function _fmtTargetAvg() {
  const avg = _computeBrokerTargetAvg();
  return avg ? avg.toLocaleString('ko-KR') + '원' : 'N/A';
}

function renderCurrentValuationSummary(indicators, weeklyIndicators, quoteSnapshot) {
  const metrics = getCurrentValuationMetrics(indicators, weeklyIndicators, quoteSnapshot);
  const betaVal = _currentBeta && _currentBeta.beta !== null && _currentBeta.beta !== undefined
    ? Number(_currentBeta.beta).toFixed(2)
    : (_currentBeta === null ? '…' : 'N/A');
  return [
    { label: 'PER', value: formatMetricNumber(metrics.per) },
    { label: 'PBR', value: formatMetricNumber(metrics.pbr) },
    { label: 'ROE', value: formatMetricNumber(metrics.roe, '%') },
    { label: '배당수익률', value: formatMetricNumber(metrics.dividendYield, '%') },
    { label: '시가총액', value: _fmtMarketCap(metrics.marketCap) },
    { label: '목표가', value: _fmtTargetAvg() },
    { label: '베타 (1Y)', value: betaVal, attr: 'data-beta="1"' },
  ].map(item => (
    `<div class="valuation-card" ${item.attr || ''}><span class="valuation-label">${item.label}</span><span class="valuation-value">${item.value}</span></div>`
  )).join('') + _externalValuationCards(_currentStockLinks).join('') + _drValuationCards(_currentDr).join('');
}

// 외부 분석 도구 카드 — 이 종목이 우선주 쌍/지주사면 밸류에이션 그리드에 같은
// .valuation-card 로 합류시킨다(별도 위젯이 아니라 PER/PBR/베타와 한 그리드).
function _sxlSafeUrl(url) {
  return /^https?:\/\//.test(String(url || '')) ? portfolioIntegrationHref(String(url)) : '#';
}

function _sxlNum(v) {
  const n = Number(v);
  return isFinite(n) ? n.toLocaleString('ko-KR') : '-';
}

function _sxlPct(v) {
  const n = Number(v);
  return isFinite(n) ? n.toFixed(1) + '%' : '-';
}

// 카드 링크 — 생태계 레지스트리(APP_CONFIG.ecosystem)가 있으면 그 도구의 stockLink 로
// 지금 종목·테마를 실어 보낸다(보유 스냅샷 도구는 /api/portfolio/open 경유). data-vc-* 는
// 테마 전환 때 ecoRefreshLinks 가 href 를 다시 계산하는 표식. 레지스트리가 없거나 도구가
// 이 코드를 받지 않으면 서버가 준 url 을 그대로 쓴다.
function _sxlToolLink(toolId, fallbackUrl) {
  const code = typeof activeStockCode !== 'undefined' && activeStockCode ? String(activeStockCode) : '';
  const href = code && toolId && typeof ecoLinkHref === 'function' && typeof ecoStockPath === 'function'
    && ecoStockPath(toolId, code) ? ecoLinkHref('tool', toolId, { code }) : null;
  if (!href) return { href: _sxlSafeUrl(fallbackUrl), attrs: '' };
  return {
    href,
    attrs: ` data-vc-tool="${escapeHtml(toolId)}" data-vc-link="tool" data-vc-code="${escapeHtml(code)}"`,
  };
}

// links = {preferred?, holding?} → valuation-card(링크) HTML 배열.
function _externalValuationCards(links) {
  if (!links) return [];
  const card = (label, value, sub, url, toolId) => {
    const link = _sxlToolLink(toolId, url);
    return `<a class="valuation-card is-link" href="${escapeHtml(link.href)}"${link.attrs} target="_blank" rel="noopener noreferrer" title="외부 분석 도구로 이동">`
    + `<span class="valuation-label">${escapeHtml(label)}</span>`
    + `<span class="valuation-value">${escapeHtml(value)}</span>`
    + (sub ? `<span class="valuation-sub">${sub}</span>` : '')
    + '</a>';
  };
  const cards = [];
  const p = links.preferred;
  if (p) {
    const sub = `${escapeHtml(String(p.name || ''))} ${_sxlNum(p.commonPrice)}`
      + ` · ${escapeHtml(String(p.preferredName || '우선주'))} ${_sxlNum(p.preferredPrice)}`;
    cards.push(card('우선주 괴리율', _sxlPct(p.spread), sub, p.url, 'common_preferred_spread'));
  }
  const h = links.holding;
  if (h) {
    const sub = `보유 ${_sxlNum(h.holdingValue)} · 시총 ${_sxlNum(h.marketCap)} (억)`;
    cards.push(card('지주사 보유가치/시총', _sxlPct(h.ratio), sub, h.url, 'holding_value'));
  }
  const e = links.etf;
  if (e && e.url) {
    cards.push(card('ETF 상세', 'eiayn ↗', '국내·해외 ETF 분석', e.url));
  }
  return cards;
}

// 해외 DR 카드 — 교환비율·환율로 환산한 '원주 1주 환산가(원)' + DR 일간상승률을
// 같은 .valuation-card 그리드에 합류시킨다. 환산가는 외국인 디스카운트/시차로
// 원주와 다소 차이날 수 있다(저유동성 거래소는 일간상승률이 비거나 튈 수 있음).
function _drValuationCards(drs) {
  if (!Array.isArray(drs) || !drs.length) return [];
  const ordPrice = Number(activeQuoteSnapshot && activeQuoteSnapshot.price) || 0;
  return drs.map(d => {
    const conv = Number(d.converted_price);
    // 환산가가 원주가 대비 크게 어긋나면(저유동성 DR 의 yfinance 시세 오류 —
    // 예: SMSN.L stale 가격) 카드를 감춘다. 틀린 환산가를 보여주느니 빼는 게 낫다.
    if (ordPrice > 0 && conv > 0 && (conv / ordPrice < 0.6 || conv / ordPrice > 1.6)) return '';
    const pct = (d.change_pct != null && isFinite(Number(d.change_pct))) ? _sxlPct(d.change_pct) : '—';
    const sub = `${escapeHtml(String(d.ticker || ''))} · 일간 ${pct}`;
    return `<div class="valuation-card">`
      + `<span class="valuation-label">${escapeHtml(String(d.label || ''))} (${escapeHtml(String(d.exchange || ''))})</span>`
      + `<span class="valuation-value">${_sxlNum(d.converted_price)}원</span>`
      + `<span class="valuation-sub">${sub}</span></div>`;
  }).filter(Boolean);
}

// coverageNote(밸류에이션 그리드) 재렌더 + 카드 수에 맞춰 열 수 조정용 data-count.
// PER/PBR/배당/베타(4) + 외부카드(0~2). 4→4열, 5·6→3열로 외톨이를 없앤다.
function _renderCoverage() {
  const el = document.getElementById('coverageNote');
  if (!el) return;
  el.innerHTML = renderCurrentValuationSummary(activeIndicators || {}, _lastWeeklyIndicators || {}, activeQuoteSnapshot || {});
  // 실제 렌더된 카드 수로 열 수를 정한다 — DR sanity 가드로 일부가 숨겨질 수
  // 있어 _currentDr.length 가 아니라 DOM 의 실제 .valuation-card 수를 센다.
  el.dataset.count = String(el.querySelectorAll('.valuation-card').length);
  // 외부 도구 신호(_currentStockLinks)가 바뀌면 헤더의 '연결 도구' 칩도 같이 맞춘다.
  renderAnalysisToolLinks();
}

// --- 분석 헤더 생태계 칩: '← 도구로 돌아가기' + '연결 도구' (#analysisEcoLinks) ---
// 레지스트리 stockLink.accepts 가 이 코드를 받는 도구 중에서 고른다. 관련성 신호가 있는
// 도구는 위 밸류에이션 카드가 대표하므로 칩으로 겹쳐 띄우지 않는다(지주사 → holding_value,
// 우선주 쌍 → common_preferred_spread, ETF → eiayn: 신호가 없으면 관련 없는 종목이다).
// 신호가 이름뿐인 도구(스팩)는 이름으로 거르고, 나머지(자사주 등)는 일반 칩으로 둔다.
const ANALYSIS_TOOL_CARD_SIGNALS = {
  holding_value: 'holding',
  common_preferred_spread: 'preferred',
  eiayn: 'etf',
};
const ANALYSIS_TOOL_NAME_SIGNALS = {
  'spac-hunter': /스팩|SPAC/i,
};

function analysisRelatedTools(code, corpName) {
  if (typeof ecoStockTools !== 'function') return [];
  return ecoStockTools(code).filter((tool) => {
    if (tool.deploy === 'hub') return false;
    if (ANALYSIS_TOOL_CARD_SIGNALS[tool.id]) return false;  // 카드(신호 있을 때만)가 담당
    const nameSignal = ANALYSIS_TOOL_NAME_SIGNALS[tool.id];
    return nameSignal ? nameSignal.test(String(corpName || '')) : true;
  });
}

function _analysisEcoChip(tool, code, { back = false } = {}) {
  const href = ecoLinkHref('tool', tool.id, { code });
  if (!href) return '';
  const label = back ? `← ${tool.name}${ecoJosaRo(tool.name)} 돌아가기` : tool.name;
  const accent = /^#[0-9a-fA-F]{6}$/.test(String(tool.accent || '')) ? ` style="--eco-chip-accent:${tool.accent}"` : '';
  return `<a class="eco-chip${back ? ' eco-chip-back' : ''}" href="${escapeHtml(href)}"`
    + ` data-vc-tool="${escapeHtml(tool.id)}" data-vc-link="tool" data-vc-code="${escapeHtml(code)}"${accent}`
    + (back ? '' : ' target="_blank" rel="noopener"')
    + ` title="${escapeHtml(back ? `${tool.name}에서 이 종목 다시 보기` : String(tool.description || tool.name))}">`
    + `${escapeHtml(label)}</a>`;
}

function renderAnalysisToolLinks() {
  const el = document.getElementById('analysisEcoLinks');
  if (!el) return;
  const code = typeof activeStockCode !== 'undefined' && activeStockCode ? String(activeStockCode) : '';
  if (!code || typeof ecoStockTools !== 'function' || typeof ecoLinkHref !== 'function') {
    el.hidden = true;
    el.innerHTML = '';
    return;
  }
  const data = typeof _lastAnalysisData !== 'undefined' ? _lastAnalysisData : null;
  const corpName = data && data.stock_code === code ? data.corp_name : '';
  const parts = [];
  const back = typeof ecoArrivalTool === 'function' ? ecoArrivalTool(code) : null;
  if (back) parts.push(_analysisEcoChip(back, code, { back: true }));
  const chips = analysisRelatedTools(code, corpName)
    .filter((tool) => !back || tool.id !== back.id)
    .map((tool) => _analysisEcoChip(tool, code))
    .filter(Boolean);
  if (chips.length) {
    parts.push('<span class="eco-chip-row" role="group" aria-label="연결 도구">'
      + `<span class="eco-chip-label">연결 도구</span>${chips.join('')}</span>`);
  }
  const html = parts.filter(Boolean).join('');
  el.innerHTML = html;
  el.hidden = !html;
}

async function loadStockExternalLinks(stockCode) {
  try {
    const data = await apiFetchJson(`/api/external/stock/${encodeURIComponent(stockCode)}`, { fallback: null });
    if (!data) return;
    if (activeStockCode !== stockCode) return;  // 종목이 바뀌었으면 무시
    _currentStockLinks = (data && (data.preferred || data.holding || data.etf)) ? data : null;
    _renderCoverage();
  } catch (e) {
    console.warn('stock external links failed', e);
  }
}

async function loadBeta(stockCode) {
  // 새 분석이 시작되면 이전 베타는 날려 플레이스홀더 '…' 로 표시되게.
  _currentBeta = null;
  try {
    _currentBeta = await apiFetchJson(`/api/analyze/${encodeURIComponent(stockCode)}/beta`, {
      errorMessage: 'beta fetch failed',
    });
  } catch (e) {
    _currentBeta = { beta: null, sample_size: 0, benchmark: 'KOSPI' };
  }
  // 분석 종목이 바뀌지 않았다면 valuation card 만 재렌더.
  if (activeStockCode === stockCode) {
    _renderCoverage();
  }
}

async function loadStockDr(stockCode) {
  _currentDr = null;
  try {
    const data = await apiFetchJson(`/api/analyze/${encodeURIComponent(stockCode)}/dr`, { fallback: null });
    if (!data) return;
    if (activeStockCode !== stockCode) return;  // 종목이 바뀌었으면 무시
    _currentDr = (data && Array.isArray(data.drs) && data.drs.length) ? data.drs : null;
    _renderCoverage();
  } catch (e) {
    console.warn('stock DR load failed', e);
  }
}
