// 종목 360° 수치 패널(#stock360Panel): 이 종목에 걸린 형제 도구 신호의 수치와 기준일.
//
// 서버(/api/external/stock/{code}/360, services/ecosystem/opportunities.py)가 도구별로 독립 조회하므로
// 한 도구가 실패해도 나머지가 나온다. 해당 없는 종목이면 패널을 숨긴다. 입력은 없다.
//
// index.html 로드 순서: analysis-valuation → analysis-360 → analysis. 전역 의존(utils.js): activeStockCode,
// apiFetchJson, escapeHtml, fmtSignalMetric, fmtSignalLiquidity, signalToolLink.

const STOCK360_BASIS_LABELS = { history3y: '자기 3년 분포', crossSection: '종목군 내 순위' };

async function loadStock360(stockCode) {
  const panel = document.getElementById('stock360Panel');
  if (!panel) return;
  panel.hidden = true;  // 종목 전환 — 이전 종목의 수치를 남기지 않는다
  panel.innerHTML = '';
  let data = null;
  try {
    data = await apiFetchJson(`/api/external/stock/${encodeURIComponent(stockCode)}/360`, { fallback: null, timeoutMs: 35_000 });
  } catch (e) {
    console.warn('stock 360 failed', e);
  }
  if (activeStockCode !== stockCode) return;  // 그 사이 종목이 바뀌었으면 무시
  renderStock360(data);
}

function renderStock360(data) {
  const panel = document.getElementById('stock360Panel');
  if (!panel) return;
  const signals = (data && Array.isArray(data.signals)) ? data.signals : [];
  const failed = (data && Array.isArray(data.failedTools)) ? data.failedTools : [];
  if (!signals.length && !failed.length) {
    panel.hidden = true;
    panel.innerHTML = '';
    return;
  }
  const liquidity = data.liquidity || {};
  const asOf = liquidity.asOf ? ` · ${liquidity.asOf}` : '';
  panel.innerHTML = '<div class="stock360-head">'
    + '<h3 id="stock360Title">종목 360°</h3>'
    + `<span class="stock360-liquidity${liquidity.low ? ' is-low' : ''}">${escapeHtml(fmtSignalLiquidity(liquidity) + asOf)}</span>`
    + '</div>'
    + (signals.length ? `<div class="stock360-grid">${signals.map((signal) => _stock360CardHtml(signal, data.code)).join('')}</div>` : '')
    + (failed.length
      ? `<p class="stock360-failed">응답 없는 도구: ${escapeHtml(failed.map((tool) => tool.label).join(', '))}. 나머지 도구만 표시합니다.</p>`
      : '')
    + '<p class="stock360-note">백분위와 순위는 과거 분포나 종목군 안의 위치일 뿐 수익을 뜻하지 않습니다. '
    + '거래비용을 추정할 수 없어 비용을 뺀 수치는 표시하지 않습니다.</p>';
  panel.hidden = false;
}

function _stock360CardHtml(signal, code) {
  const link = signalToolLink(signal.tool, signal.code, signal.url);
  const subject = signal.name || signal.code;
  // 우선주를 보면 보통주의 신호가, 보통주를 보면 우선주 쌍이 함께 온다. 어느 코드 기준인지 밝힌다.
  const related = signal.code !== code ? ` <small>연결 종목 ${escapeHtml(signal.code)} 기준</small>` : '';
  const score = signal.score !== null && signal.score !== undefined
    ? `<p class="stock360-score" title="${escapeHtml(signal.scoreNote || '')}"><strong>${escapeHtml(Math.round(Number(signal.score)))}</strong>`
      + `<span>${escapeHtml(STOCK360_BASIS_LABELS[signal.scoreBasis] || '')}</span></p>`
    : '';
  const metrics = (signal.metrics || []).map((item) => (
    `<div><dt>${escapeHtml(item.label)}</dt><dd>${escapeHtml(fmtSignalMetric(item))}</dd></div>`
  )).join('');
  const asOfParts = [`기준일 ${signal.asOf || '확인 불가'}`];
  if (signal.distributionEnd) asOfParts.push(`분포 ${signal.distributionEnd}까지`);
  const warnings = (signal.warnings || []).map((text) => `<p class="stock360-warning">${escapeHtml(text)}</p>`).join('');
  return `<article class="stock360-card${signal.stale ? ' is-stale' : ''}">`
    + '<header>'
    + `<span class="stock360-label">${escapeHtml(signal.label)}</span>`
    + `<a href="${escapeHtml(link.href)}"${link.attrs} target="_blank" rel="noopener noreferrer">도구 열기 ↗</a>`
    + '</header>'
    + `<p class="stock360-subject">${escapeHtml(subject)}${related}</p>`
    + score
    + (metrics ? `<dl class="stock360-metrics">${metrics}</dl>` : '')
    + `<p class="stock360-asof">${escapeHtml(asOfParts.join(' · '))}</p>`
    + warnings
    + '</article>';
}
