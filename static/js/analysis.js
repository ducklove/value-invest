// Analyze
const STEP_LABELS = {
  start: '분석 시작',
  financial_start: '재무제표 수집',
  financial_done: '재무제표 수집 완료',
  financial_error: '재무제표 조회 실패',
  market_start: '시장 데이터 계산',
  market_done: '시장 데이터 수집 완료',
  market_error: '시장 데이터 조회 실패',
  saving: '캐시 저장',
  analyzing: '지표 계산',
};

function getSeriesCoverage(series) {
  const years = (series || [])
    .filter(item => item && item.value !== null && item.value !== undefined)
    .map(item => item.year)
    .filter(year => Number.isFinite(year));
  if (years.length === 0) return '';
  const startYear = Math.min(...years);
  const endYear = Math.max(...years);
  return startYear === endYear ? String(startYear) : `${startYear}-${endYear}`;
}

function getWeeklyDateCoverage(series) {
  const dates = (series || [])
    .filter(item => item && item.value !== null && item.value !== undefined && item.date)
    .map(item => item.date)
    .sort();
  if (dates.length === 0) return null;
  return {
    startDate: dates[0],
    endDate: dates[dates.length - 1],
  };
}

function formatWeeklySectionTitle(weeklyIndicators) {
  const coverage = getWeeklyDateCoverage(weeklyIndicators?.['주가'] || weeklyIndicators?.['PER'] || []);
  if (!coverage) return '밸류에이션';
  return `밸류에이션 (${coverage.startDate} ~ ${coverage.endDate})`;
}

function getLatestIndicatorValue(series) {
  const entries = (series || []).filter(item => item && item.value !== null && item.value !== undefined && Number.isFinite(Number(item.value)));
  if (entries.length === 0) return null;
  return Number(entries[entries.length - 1].value);
}

function _formatDailyMarketCost(costUsd) {
  const cost = Number(costUsd || 0);
  if (!Number.isFinite(cost) || cost <= 0) return '';
  // analysis.js loads before portfolio-store.js — guard the whole namespace.
  if (typeof PfStore !== 'undefined' && PfStore.currency.fxRate) {
    return `${Math.max(1, Math.round(cost * PfStore.currency.fxRate)).toLocaleString('ko-KR')}원`;
  }
  return `$${cost.toFixed(5)}`;
}

async function loadDailyMarketBrief(refresh = false) {
  const section = document.getElementById('dailyMarketSection');
  if (!section) return;
  const status = document.getElementById('dailyMarketStatus');
  const body = document.getElementById('dailyMarketBody');
  const meta = document.getElementById('dailyMarketMeta');
  const btn = document.getElementById('dailyMarketRefreshBtn');
  if (btn) btn.disabled = true;
  if (status) {
    status.classList.remove('error');
    status.textContent = refresh ? '최신 시황을 다시 생성하는 중입니다...' : '시황을 불러오는 중입니다...';
  }
  if (body && refresh) body.textContent = '';
  if (meta && refresh) meta.textContent = '';

  try {
    const url = `/api/market/daily-brief${refresh ? '?refresh=true' : ''}`;
    const resp = await apiFetch(url);
    if (!resp.ok) {
      const err = await resp.json().catch(() => ({}));
      throw new Error(err.detail || `HTTP ${resp.status}`);
    }
    const data = await resp.json();
    const markdown = data.markdown || '';
    if (body) {
      body.innerHTML = markdown
        ? _renderSafeMarkdown(markdown)
        : '<p>표시할 시황 본문이 없습니다.</p>';
    }
    const payload = data.payload || {};
    const notableCount = (payload.moves || []).filter(row => row && row.is_notable).length;
    const disclosureCount = (payload.disclosures || []).length;
    const materialCount = (payload.disclosures || []).filter(row => row && row.is_material).length;
    const tokenText = `입력 ${(data.tokens_in ?? 0).toLocaleString('ko-KR')} / 출력 ${(data.tokens_out ?? 0).toLocaleString('ko-KR')} 토큰`;
    const costText = _formatDailyMarketCost(data.cost_usd);
    const cacheText = data.cached ? '캐시' : '새 생성';
    const generatedAt = data.updated_at ? new Date(data.updated_at).toLocaleString('ko-KR') : '';
    if (status) {
      status.textContent = [
        `${payload.brief_date || data.brief_date || ''} 기준`,
        `관심목록 ${payload.interest_count || 0}개`,
        `급등/급락 ${notableCount}개`,
        `공시 ${disclosureCount}건${materialCount ? ` (중요 후보 ${materialCount}건)` : ''}`,
      ].filter(Boolean).join(' · ');
    }
    if (meta) {
      meta.textContent = [
        cacheText,
        data.model,
        tokenText,
        costText,
        generatedAt ? `생성 ${generatedAt}` : '',
      ].filter(Boolean).join(' · ');
    }
  } catch (err) {
    if (status) {
      status.classList.add('error');
      status.textContent = '금일 시황을 불러오지 못했습니다: ' + (err.message || err);
    }
  } finally {
    if (btn) btn.disabled = false;
  }
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
    : `${value.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}${suffix}`;
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

function getCurrentValuationMetrics(indicators, quoteSnapshot) {
  const currentPrice = Number(quoteSnapshot?.price);
  if (!Number.isFinite(currentPrice) || currentPrice <= 0) {
    return { per: null, pbr: null, roe: null, dividendYield: null, marketCap: null };
  }

  const latestEps = getLatestIndicatorValue(indicators['EPS (원)']);
  // Use the most recent *positive* DPS so companies that pay dividends
  // don't read as 0% when the current year's payout hasn't been
  // recorded yet. Legitimate non-dividend-payers return null here
  // (all-zero series) and the card renders 'N/A'.
  const trailingDps = getLatestPositiveValue(indicators['주당배당금 (원)']);
  const latestBps = getLatestDerivedBps(indicators);

  return {
    per: latestEps && latestEps > 0 ? currentPrice / latestEps : null,
    pbr: latestBps && latestBps > 0 ? currentPrice / latestBps : null,
    roe: getLatestIndicatorValue(indicators['ROE (%)']),
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

function renderCurrentValuationSummary(indicators, quoteSnapshot) {
  const metrics = getCurrentValuationMetrics(indicators, quoteSnapshot);
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
  return /^https?:\/\//.test(String(url || '')) ? String(url) : '#';
}

function _sxlNum(v) {
  const n = Number(v);
  return isFinite(n) ? n.toLocaleString('ko-KR') : '-';
}

function _sxlPct(v) {
  const n = Number(v);
  return isFinite(n) ? n.toFixed(1) + '%' : '-';
}

// links = {preferred?, holding?} → valuation-card(링크) HTML 배열.
function _externalValuationCards(links) {
  if (!links) return [];
  const card = (label, value, sub, url) => (
    `<a class="valuation-card is-link" href="${escapeHtml(_sxlSafeUrl(url))}" target="_blank" rel="noopener noreferrer" title="외부 분석 도구로 이동">`
    + `<span class="valuation-label">${escapeHtml(label)}</span>`
    + `<span class="valuation-value">${escapeHtml(value)}</span>`
    + (sub ? `<span class="valuation-sub">${sub}</span>` : '')
    + '</a>'
  );
  const cards = [];
  const p = links.preferred;
  if (p) {
    const sub = `${escapeHtml(String(p.name || ''))} ${_sxlNum(p.commonPrice)}`
      + ` · ${escapeHtml(String(p.preferredName || '우선주'))} ${_sxlNum(p.preferredPrice)}`;
    cards.push(card('우선주 괴리율', _sxlPct(p.spread), sub, p.url));
  }
  const h = links.holding;
  if (h) {
    const sub = `보유 ${_sxlNum(h.holdingValue)} · 시총 ${_sxlNum(h.marketCap)} (억)`;
    cards.push(card('지주사 보유가치/시총', _sxlPct(h.ratio), sub, h.url));
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
  el.innerHTML = renderCurrentValuationSummary(activeIndicators || {}, activeQuoteSnapshot || {});
  // 실제 렌더된 카드 수로 열 수를 정한다 — DR sanity 가드로 일부가 숨겨질 수
  // 있어 _currentDr.length 가 아니라 DOM 의 실제 .valuation-card 수를 센다.
  el.dataset.count = String(el.querySelectorAll('.valuation-card').length);
}

async function loadStockExternalLinks(stockCode) {
  try {
    const resp = await apiFetch(`/api/external/stock/${encodeURIComponent(stockCode)}`);
    if (!resp.ok) return;
    const data = await resp.json();
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
    const resp = await apiFetch(`/api/analyze/${encodeURIComponent(stockCode)}/beta`);
    if (!resp.ok) throw new Error('beta fetch failed');
    _currentBeta = await resp.json();
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
    const resp = await apiFetch(`/api/analyze/${encodeURIComponent(stockCode)}/dr`);
    if (!resp.ok) return;
    const data = await resp.json();
    if (activeStockCode !== stockCode) return;  // 종목이 바뀌었으면 무시
    _currentDr = (data && Array.isArray(data.drs) && data.drs.length) ? data.drs : null;
    _renderCoverage();
  } catch (e) {
    console.warn('stock DR load failed', e);
  }
}

function renderQuoteSnapshot(quoteSnapshot, indicators = activeIndicators) {
  const quoteSummary = document.getElementById('quoteSummary');
  const quotePrice = document.getElementById('quotePrice');
  const quoteChange = document.getElementById('quoteChange');
  const quoteDate = document.getElementById('quoteDate');
  const coverageNote = document.getElementById('coverageNote');
  const quote = quoteSnapshot || {};

  if (quote.price !== null && quote.price !== undefined) {
    quoteSummary.style.display = 'flex';
    quotePrice.textContent = `${Number(quote.price).toLocaleString()}원`;
    const change = Number(quote.change || 0);
    const changePct = quote.change_pct;
    const changePrefix = change > 0 ? '+' : '';
    quoteChange.textContent = changePct !== null && changePct !== undefined
      ? `${changePrefix}${change.toLocaleString()}원 (${changePrefix}${Number(changePct).toLocaleString()}%)`
      : '변동 정보 없음';
    quoteChange.className = 'quote-change';
    quoteChange.classList.add(change > 0 ? 'up' : change < 0 ? 'down' : 'flat');
    quoteDate.textContent = quote.date ? `${quote.date} 기준` : '';
    // WS live dot next to date
    let dateDot = quoteDate.querySelector('.ws-live-dot');
    if (activeStockCode && QuoteManager.isLive(activeStockCode)) {
      if (!dateDot) { dateDot = document.createElement('span'); dateDot.className = 'ws-live-dot'; dateDot.title = '실시간'; quoteDate.appendChild(dateDot); }
    } else if (dateDot) { dateDot.remove(); }
  } else {
    quoteSummary.style.display = 'none';
    quotePrice.textContent = '';
    quoteChange.textContent = '';
    quoteDate.textContent = '';
  }

  _renderCoverage();
}

function resetProgress() {
  document.getElementById('progressBar').style.width = '0%';
  document.getElementById('progressSteps').innerHTML = '';
  document.getElementById('loadingDetail').textContent = '';
}

function addStep(text, cls) {
  const steps = document.getElementById('progressSteps');
  const div = document.createElement('div');
  div.className = cls || '';
  div.textContent = text;
  steps.appendChild(div);
  steps.scrollTop = steps.scrollHeight;
}

function markLastStepDone() {
  const steps = document.getElementById('progressSteps');
  const active = steps.querySelector('.active:last-child');
  if (active) { active.classList.remove('active'); active.classList.add('done'); active.textContent = '\u2713 ' + active.textContent; }
}

function cancelAnalysis() {
  if (currentAbortController) {
    currentAbortController.abort();
    currentAbortController = null;
  }
}

async function analyzeStock(stockCode) {
  try {
    requireApiConfiguration();
  } catch (error) {
    showToast(error.message);
    return;
  }

  // 이전 분석 진행중이면 취소
  if (currentAbortController) currentAbortController.abort();
  currentAbortController = new AbortController();
  const signal = currentAbortController.signal;

  const overlay = document.getElementById('loadingOverlay');
  const loadingText = document.getElementById('loadingText');
  const loadingDetail = document.getElementById('loadingDetail');
  const progressBar = document.getElementById('progressBar');
  const cancelBtn = document.getElementById('cancelBtn');

  overlay.classList.add('show');
  cancelBtn.style.display = 'inline-block';
  resetProgress();
  loadingText.textContent = '데이터를 분석하고 있습니다...';

  try {
    trackEvent('analysis_start', { stock_code: stockCode });
    // SSE 스트리밍 응답 — apiFetch 기본 타임아웃 제외(stream: true).
    const resp = await apiFetch(`/api/analyze/${stockCode}`, { signal, stream: true });
    const contentType = resp.headers.get('content-type') || '';

    // 캐시 히트: 일반 JSON 응답
    if (contentType.includes('application/json')) {
      if (!resp.ok) {
        const err = await resp.json();
        showToast(err.detail || '분석 실패');
        return;
      }
      const data = await resp.json();
      renderResult(data);
      if (!currentUser) saveGuestRecent(data.stock_code, data.corp_name);
      if (activeTab === 'starred' && currentUser && !data.user_preference?.is_starred) {
        await autoStarCurrentStock();
      }
      loadRecentList();
      return;
    }

    // SSE 스트리밍 응답
    const reader = resp.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';
    let resultData = null;
    let lastDartStep = '';
    let eventType = '';

    // signal로 취소 시 reader도 정리
    signal.addEventListener('abort', () => { reader.cancel(); });

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });

      const lines = buffer.split('\n');
      buffer = lines.pop();

      for (const line of lines) {
        if (line.startsWith('event: ')) {
          eventType = line.slice(7).trim();
        } else if (line.startsWith('data: ')) {
          const payload = JSON.parse(line.slice(6));

          if (eventType === 'progress') {
            const step = payload.step;
            loadingText.textContent = payload.message;

            if (step === 'financial_start') {
              markLastStepDone();
              progressBar.style.width = '30%';
              addStep(payload.message, 'active');
            } else if (step === 'financial_done' || step === 'financial_error') {
              markLastStepDone();
              progressBar.style.width = '60%';
              addStep(payload.message, step === 'financial_done' ? 'done' : '');
            } else if (step === 'market_start') {
              markLastStepDone();
              progressBar.style.width = '65%';
              addStep(payload.message, 'active');
            } else if (step === 'market_done' || step === 'market_error') {
              markLastStepDone();
              progressBar.style.width = '85%';
              addStep(payload.message, step === 'market_done' ? 'done' : '');
            } else if (step === 'saving') {
              progressBar.style.width = '90%';
              addStep(payload.message, 'active');
            } else if (step === 'analyzing') {
              markLastStepDone();
              progressBar.style.width = '95%';
              addStep(payload.message, 'active');
            } else if (step === 'start') {
              addStep(payload.message, 'active');
            }
          } else if (eventType === 'result') {
            progressBar.style.width = '100%';
            markLastStepDone();
            addStep('분석 완료!', 'done');
            resultData = payload;
          } else if (eventType === 'error') {
            showToast(payload.message || '분석 실패');
          }
          eventType = '';
        }
      }
    }

    // 스트림 종료 후 버퍼에 남은 데이터 처리
    if (buffer.trim()) {
      const remainingLines = buffer.split('\n');
      for (const line of remainingLines) {
        if (line.startsWith('event: ')) {
          eventType = line.slice(7).trim();
        } else if (line.startsWith('data: ')) {
          const payload = JSON.parse(line.slice(6));
          if (eventType === 'result') {
            resultData = payload;
          }
          eventType = '';
        }
      }
    }

    if (resultData) {
      await new Promise(r => setTimeout(r, 300));
      renderResult(resultData);
      if (!currentUser) saveGuestRecent(resultData.stock_code, resultData.corp_name);
      if (activeTab === 'starred' && currentUser && !resultData.user_preference?.is_starred) {
        await autoStarCurrentStock();
      }
      loadRecentList();
    }
  } catch (e) {
    if (e.name === 'AbortError') {
      // 사용자가 취소함 - 무시
    } else {
      showToast('서버 오류: ' + e.message);
    }
  } finally {
    currentAbortController = null;
    overlay.classList.remove('show');
  }
}

async function renderResult(data) {
  _lastAnalysisData = data;
  _currentStockLinks = null;  // 종목 전환 — 이전 외부 카드 제거(loadStockExternalLinks가 다시 채움)
  _currentDr = null;  // 종목 전환 — 이전 DR 카드 제거(loadStockDr가 다시 채움)
  allReports = [];    // 이전 종목 리포트 잔상 제거 — loadReports 가 다시 채우고 목표가 카드 갱신
  _currentPeriod = 'all';
  _dailyCache = {};
  document.querySelectorAll('.vp-btn').forEach(b => b.classList.toggle('active', b.dataset.period === 'all'));

  // Company info
  const infoEl = document.getElementById('companyInfo');
  infoEl.style.display = 'block';
  activeStockCode = data.stock_code;
  activeIndicators = data.indicators || {};
  activeQuoteSnapshot = data.quote_snapshot || {};
  currentUserPreference = normalizeUserPreference(data.user_preference);
  document.getElementById('companyName').textContent = `${data.corp_name} (${data.stock_code})`;
  const cachedText = data.cached ? `캐시됨 (${new Date(data.analyzed_at).toLocaleDateString('ko-KR')})` : '신규 분석 완료';
  document.getElementById('companyMeta').textContent = cachedText;
  renderUserPreference();
  renderQuoteSnapshot(data.quote_snapshot || {}, activeIndicators);
  trackEvent('analysis_complete', { stock_code: data.stock_code, cached: String(Boolean(data.cached)) });

  // Hide empty state, show charts
  document.getElementById('emptyState').style.display = 'none';
  const weeklyTitle = document.getElementById('weeklySectionTitle');
  const weeklyGrid = document.getElementById('weeklyChartsGrid');
  const annualTitle = document.getElementById('annualSectionTitle');
  const grid = document.getElementById('chartsGrid');
  const hasWeeklyCharts = WEEKLY_CHART_KEYS.some(key => (data.weekly_indicators?.[key] || []).length > 0);
  _lastWeeklyIndicators = data.weekly_indicators || null;
  weeklyTitle.textContent = formatWeeklySectionTitle(data.weekly_indicators || {});
  const sectionRow = document.getElementById('weeklySectionRow');
  if (sectionRow) sectionRow.style.display = hasWeeklyCharts ? 'flex' : 'none';
  weeklyGrid.style.display = hasWeeklyCharts ? 'grid' : 'none';
  annualTitle.style.display = hasWeeklyCharts ? 'none' : 'block';
  grid.style.display = hasWeeklyCharts ? 'none' : 'grid';
  weeklyGrid.innerHTML = '';
  grid.innerHTML = '';

  // Destroy existing charts
  Object.values(charts).forEach(c => c.dispose());
  charts = {};

  const isDark = document.documentElement.getAttribute('data-theme') === 'dark';
  const gridColor = isDark ? 'rgba(148,163,184,0.15)' : 'rgba(0,0,0,0.06)';
  const tickColor = isDark ? '#94a3b8' : '#666';

  if (hasWeeklyCharts) {
    await renderChartGrid(weeklyGrid, WEEKLY_CHART_KEYS, data.weekly_indicators || {}, gridColor, tickColor, 'weekly');
  } else {
    await renderChartGrid(grid, ANNUAL_CHART_KEYS, data.indicators || {}, gridColor, tickColor, 'annual');
  }

  // Load reports after charts are rendered (needs grid visible for target price chart)
  loadReports(data.stock_code);
  // Wiki is lazy — fire-and-forget, renders under the report table.
  loadWiki(data.stock_code);
  // DART filing review is cached-only on the user path. Background
  // ingestion pre-generates missing reviews.
  loadFilingReview(data.stock_code);
  // 베타 — 1Y 일별 수익률 vs KOSPI. 비동기로 받아 valuation card 갱신.
  loadBeta(data.stock_code);
  // 외부 분석 도구(우선주 괴리율/지주사 NAV) deep-link 카드 — 해당 시 표시.
  loadStockExternalLinks(data.stock_code);
  // 해외 DR(예탁증서) 환산가 — 해당 종목에 DR 이 매핑돼 있으면 카드로 표시.
  loadStockDr(data.stock_code);
  _updateQuoteSubscriptions();
}

// Recent list
async function loadRecentList() {
  updateSidebarTabs();
  if (!hasApiConfiguration()) {
    document.getElementById('recentList').innerHTML = '<div style="color:var(--text-secondary);font-size:13px;">GitHub Pages에서는 API 서버 연결 후 최근 분석 목록을 불러옵니다.</div>';
    return;
  }

  if (recentListLoading) return;
  recentListLoading = true;

  try {
    const container = document.getElementById('recentList');
    const tab = currentUser ? activeTab : 'recent';

    if (currentUser) {
      const resp = await apiFetch(`/api/cache/list?include_quotes=true&tab=${tab}`);
      const data = await resp.json();
      recentListItems = Array.isArray(data) ? data.slice() : [];
    } else {
      recentListItems = getGuestRecent();
    }
    if (recentListItems.length === 0) {
      const emptyMsg = currentUser
        ? (tab === 'starred' ? '관심종목이 없습니다. 분석 화면에서 관심종목을 추가하세요.' : '최근 검색한 종목이 없습니다.')
        : '아직 분석한 종목이 없습니다.';
      container.innerHTML = `<div style="color:var(--text-secondary);font-size:13px;">${emptyMsg}</div>`;
      return;
    }
    container.innerHTML = '';
    recentListItems.forEach((item, index) => {
      const wrapper = document.createElement('div');
      wrapper.className = 'sidebar-item';
      wrapper.dataset.index = index;
      wrapper.dataset.code = item.stock_code;

      if (currentUser) {
        wrapper.draggable = true;
        wrapper.addEventListener('dragstart', (e) => {
          wrapper.classList.add('dragging');
          e.dataTransfer.effectAllowed = 'move';
          e.dataTransfer.setData('text/plain', String(index));
        });
        wrapper.addEventListener('dragend', () => {
          wrapper.classList.remove('dragging');
          container.querySelectorAll('.drag-over').forEach(el => el.classList.remove('drag-over'));
        });
        wrapper.addEventListener('dragover', (e) => {
          e.preventDefault();
          e.dataTransfer.dropEffect = 'move';
          const dragging = container.querySelector('.dragging');
          if (dragging !== wrapper) wrapper.classList.add('drag-over');
        });
        wrapper.addEventListener('dragleave', () => {
          wrapper.classList.remove('drag-over');
        });
        wrapper.addEventListener('drop', (e) => {
          e.preventDefault();
          wrapper.classList.remove('drag-over');
          const fromIndex = parseInt(e.dataTransfer.getData('text/plain'), 10);
          const toIndex = parseInt(wrapper.dataset.index, 10);
          if (fromIndex !== toIndex && !isNaN(fromIndex) && !isNaN(toIndex)) {
            dropRecentItem(fromIndex, toIndex);
          }
        });
      }

      const info = document.createElement('div');
      info.className = 'info';
      info.addEventListener('click', () => { switchView('analysis'); analyzeStock(item.stock_code); });

      const name = document.createElement('div');
      name.className = 'name';
      name.textContent = item.corp_name;
      if (QuoteManager.isLive(item.stock_code)) {
        const dot = document.createElement('span');
        dot.className = 'ws-live-dot';
        dot.title = '실시간';
        name.appendChild(dot);
      }
      const nameRow = document.createElement('div');
      nameRow.className = 'name-row';
      nameRow.appendChild(name);

      const badges = document.createElement('div');
      badges.className = 'badges';
      if (item.is_starred && activeTab !== 'starred') {
        const badge = document.createElement('span');
        badge.className = 'sidebar-badge star';
        badge.textContent = '관심';
        badges.appendChild(badge);
      }
      if (item.note) {
        const badge = document.createElement('span');
        badge.className = 'sidebar-badge note';
        badge.textContent = '메모';
        badges.appendChild(badge);
      }
      if (badges.childElementCount > 0) {
        nameRow.appendChild(badges);
      }

      const quote = item.quote_snapshot || {};
      const quotePrice = document.createElement('div');
      quotePrice.className = 'quote-price';
      const quoteChange = document.createElement('div');
      quoteChange.className = 'quote-change';

      if (quote.price !== null && quote.price !== undefined) {
        quotePrice.textContent = Number(quote.price).toLocaleString();
        const change = Number(quote.change || 0);
        const changePct = quote.change_pct;
        const changeClass = change > 0 ? 'up' : change < 0 ? 'down' : 'flat';
        quoteChange.classList.add(changeClass);
        if (changePct !== null && changePct !== undefined) {
          quoteChange.textContent = `${change > 0 ? '+' : ''}${Number(changePct).toFixed(2)}%`;
        }
      }

      info.append(nameRow, quotePrice, quoteChange);
      wrapper.appendChild(info);

      const button = document.createElement('button');
      button.className = 'delete-btn';
      button.title = activeTab === 'starred' ? '관심 해제' : '삭제';
      button.innerHTML = '&times;';
      button.addEventListener('click', (event) => {
        event.stopPropagation();
        if (currentUser) {
          deleteCache(item.stock_code);
        } else {
          removeGuestRecent(item.stock_code);
          loadRecentList();
        }
      });
      wrapper.appendChild(button);
      container.appendChild(wrapper);
    });
  } catch (e) { console.warn(e); } finally {
    recentListLoading = false;
    _updateQuoteSubscriptions();
  }
}

function refreshRecentList() {
  loadRecentList();
}

async function deleteCache(stockCode) {
  try {
    const resp = await apiFetch(`/api/cache/${stockCode}?tab=${activeTab}`, { method: 'DELETE' });
    if (!resp.ok) {
      const data = await resp.json().catch(() => ({}));
      throw new Error(data.detail || '삭제하지 못했습니다.');
    }
    loadRecentList();
  } catch (e) {
    showToast(e.message || '삭제하지 못했습니다.');
  }
}

async function saveRecentOrder(stockCodes) {
  const resp = await apiFetch('/api/cache/order', {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ stock_codes: stockCodes, tab: activeTab }),
  });
  const data = await resp.json().catch(() => ({}));
  if (!resp.ok) {
    throw new Error(data.detail || '순서를 저장하지 못했습니다.');
  }
}

async function moveRecentItem(index, delta) {
  if (!currentUser) return;
  const nextIndex = index + delta;
  if (nextIndex < 0 || nextIndex >= recentListItems.length) return;

  const nextItems = recentListItems.slice();
  const [moved] = nextItems.splice(index, 1);
  nextItems.splice(nextIndex, 0, moved);
  recentListItems = nextItems;

  try {
    await saveRecentOrder(nextItems.map(item => item.stock_code));
    await loadRecentList();
  } catch (error) {
    showToast(error.message || '순서를 저장하지 못했습니다.');
    await loadRecentList();
  }
}

async function dropRecentItem(fromIndex, toIndex) {
  if (!currentUser) return;
  const nextItems = recentListItems.slice();
  const [moved] = nextItems.splice(fromIndex, 1);
  nextItems.splice(toIndex, 0, moved);
  recentListItems = nextItems;

  try {
    await saveRecentOrder(nextItems.map(item => item.stock_code));
    await loadRecentList();
  } catch (error) {
    showToast(error.message || '순서를 저장하지 못했습니다.');
    await loadRecentList();
  }
}

let _wikiQaStockCode = null;

function _setupWikiQa(stockCode) {
  _wikiQaStockCode = stockCode;
  const btn = document.getElementById('wikiQaSubmit');
  if (btn && !btn.dataset.bound) {
    btn.addEventListener('click', () => askWikiQuestion());
    btn.dataset.bound = '1';
  }
  const input = document.getElementById('wikiQaInput');
  if (input && !input.dataset.bound) {
    input.addEventListener('keydown', e => {
      // Ctrl+Enter submits; plain Enter inserts newline.
      if (e.key === 'Enter' && (e.ctrlKey || e.metaKey)) {
        e.preventDefault();
        askWikiQuestion();
      }
    });
    input.dataset.bound = '1';
  }
  // Gate input on auth state.
  const hint = document.getElementById('wikiQaHint');
  const isLoggedIn = !!(typeof currentUser !== 'undefined' && currentUser);
  if (input) input.disabled = !isLoggedIn;
  if (btn) btn.disabled = !isLoggedIn;
  if (hint) {
    hint.textContent = isLoggedIn
      ? 'DART 공시 리뷰, 증권사 리포트, 재무/시세 정보를 함께 근거로 답변합니다.'
      : '로그인하면 DART 공시 리뷰, 증권사 리포트, 재무/시세 정보를 근거로 LLM이 답변합니다.';
    hint.style.display = '';
  }
}

async function askWikiQuestion() {
  const stockCode = _wikiQaStockCode;
  if (!stockCode) return;
  const input = document.getElementById('wikiQaInput');
  const btn = document.getElementById('wikiQaSubmit');
  const status = document.getElementById('wikiQaStatus');
  const answerEl = document.getElementById('wikiQaAnswer');
  const metaEl = document.getElementById('wikiQaMeta');
  if (!input || !btn) return;
  const q = (input.value || '').trim();
  if (!q) { showToast && showToast('질문을 입력해 주세요.'); return; }

  btn.disabled = true;
  btn.textContent = '생성 중...';
  status.textContent = '';
  answerEl.style.display = 'block';
  answerEl.textContent = '';
  metaEl.textContent = '';

  let mdText = '';
  try {
    const resp = await apiFetch(`/api/analysis/${encodeURIComponent(stockCode)}/ask`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ question: q }),
      stream: true, // SSE 스트리밍 — 기본 타임아웃 제외
    });
    if (!resp.ok) {
      const err = await resp.json().catch(() => ({}));
      throw new Error(err.detail || `HTTP ${resp.status}`);
    }
    const reader = resp.body.getReader();
    const decoder = new TextDecoder();
    let buf = '';
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buf += decoder.decode(value, { stream: true });
      const lines = buf.split(/\r?\n/);
      buf = lines.pop();
      for (const line of lines) {
        if (!line.startsWith('data: ')) continue;
        try {
          const d = JSON.parse(line.slice(6));
          if (d.content) {
            mdText += d.content;
            if (typeof _renderSafeMarkdown === 'function') {
              answerEl.innerHTML = _renderSafeMarkdown(mdText);
            } else {
              answerEl.textContent = mdText;
            }
          }
          if (d.done) {
            if (!mdText) {
              mdText = 'AI 모델이 최종 답변 본문을 반환하지 않았습니다. 잠시 후 다시 질문해 주세요.';
              if (typeof _renderSafeMarkdown === 'function') {
                answerEl.innerHTML = _renderSafeMarkdown(mdText);
              } else {
                answerEl.textContent = mdText;
              }
            }
            const srcN = (d.sources || []).length;
            const modelS = d.model ? ` · ${d.model}` : '';
            const costUsd = Number(d.cost || 0);
            const costS = costUsd ? ` · $${costUsd.toFixed(6)}` : '';
            metaEl.textContent = `참조 요약 ${srcN}건 · 입력 ${d.input_tokens || '?'} / 출력 ${d.output_tokens || '?'} 토큰${costS}${modelS}`;
          }
        } catch {}
      }
    }
    if (typeof _renderSafeMarkdown === 'function' && mdText) {
      answerEl.innerHTML = _renderSafeMarkdown(mdText);
    }
  } catch (e) {
    answerEl.textContent = '질문 처리 실패: ' + (e.message || e);
  }
  btn.disabled = false;
  btn.textContent = '질문';
}
