// 유령 포트폴리오("가만히 있었다면") + 매도 후 5·20거래일·현재가 추적 카드.
// 입력 없이 일별 정산 스냅샷·NH 실현손익·수동 매매 기록으로 자동 계산한다.
let _pfGhostSeq = 0;
let _pfGhostWindow = 'ytd';
let _pfGhostCache = {};
let _pfGhostChartInstance = null;
let _pfGhostResizeObserver = null;

const _PF_GHOST_WINDOWS = [
  { key: 'ytd', label: '올해' },
  { key: '6m', label: '6개월', days: 183 },
  { key: '3m', label: '3개월', days: 92 },
];

function _pfGhostDisposeChart() {
  if (_pfGhostChartInstance) {
    try { _pfGhostChartInstance.dispose(); } catch (_) { /* ignore */ }
    _pfGhostChartInstance = null;
  }
  if (_pfGhostResizeObserver) {
    _pfGhostResizeObserver.disconnect();
    _pfGhostResizeObserver = null;
  }
}

function _pfGhostSinceForWindow(windowKey, asOfIso) {
  const found = _PF_GHOST_WINDOWS.find(w => w.key === windowKey);
  if (!found || !found.days) return null;
  const base = asOfIso && /^\d{4}-\d{2}-\d{2}$/.test(asOfIso)
    ? new Date(`${asOfIso}T00:00:00Z`)
    : new Date();
  const shifted = new Date(base.getTime() - found.days * 86400000);
  return shifted.toISOString().slice(0, 10);
}

function pfGhostFmtSummaryKrw(value) {
  const num = Number(value);
  if (value === null || value === undefined || !Number.isFinite(num)) return '미계산';
  if (Math.abs(num) < 0.5) return '0원';
  const sign = num > 0 ? '+' : '-';
  const abs = Math.abs(num);
  if (abs >= 1e8) return `${sign}${(abs / 1e8).toFixed(2)}억원`;
  if (abs >= 1e4) return `${sign}${Math.round(abs / 1e4).toLocaleString('ko-KR')}만원`;
  return `${sign}${Math.round(abs).toLocaleString('ko-KR')}원`;
}

function pfGhostFmtExactKrw(value, signed = false) {
  const num = Number(value);
  if (value === null || value === undefined || !Number.isFinite(num)) return '—';
  const rounded = Math.round(num);
  const prefix = signed && rounded > 0 ? '+' : '';
  return `${prefix}${rounded.toLocaleString('ko-KR')}원`;
}

function pfGhostFmtPctPoint(value) {
  const num = Number(value);
  if (value === null || value === undefined || !Number.isFinite(num)) return '미계산';
  const prefix = num > 0 ? '+' : '';
  return `${prefix}${num.toFixed(2)}%p`;
}

function pfGhostFmtPrice(value, currency) {
  const num = Number(value);
  if (value === null || value === undefined || !Number.isFinite(num)) return '—';
  const cur = String(currency || 'KRW').toUpperCase();
  if (cur === 'KRW') return `${Math.round(num).toLocaleString('ko-KR')}원`;
  const digits = num >= 100 ? 2 : 4;
  return `${num.toLocaleString('ko-KR', { minimumFractionDigits: 2, maximumFractionDigits: digits })} ${escapeHtml(cur)}`;
}

function pfGhostFmtDiffAmount(value, currency) {
  const num = Number(value);
  if (value === null || value === undefined || !Number.isFinite(num)) return '';
  const cur = String(currency || 'KRW').toUpperCase();
  if (cur === 'KRW') return pfGhostFmtSummaryKrw(num);
  const sign = num > 0 ? '+' : '';
  return `${sign}${num.toLocaleString('ko-KR', { maximumFractionDigits: 2 })} ${escapeHtml(cur)}`;
}

function _pfGhostToneClass(value) {
  const num = Number(value);
  if (!Number.isFinite(num) || num === 0) return '';
  if (typeof returnClass === 'function') return returnClass(num) || '';
  return num > 0 ? 'positive' : 'negative';
}

function _pfGhostRenderWindowBtns() {
  const wrap = document.getElementById('pfGhostWindowBtns');
  if (!wrap) return;
  wrap.innerHTML = _PF_GHOST_WINDOWS.map(w => {
    const active = w.key === _pfGhostWindow ? ' active' : '';
    const pressed = w.key === _pfGhostWindow ? 'true' : 'false';
    return `<button type="button" class="vp-btn${active}" data-ghost-window="${escapeHtml(w.key)}" aria-pressed="${pressed}" onclick="pfGhostSetWindow('${escapeHtml(w.key)}')">${escapeHtml(w.label)}</button>`;
  }).join('');
}

function _pfGhostHeadlineHtml(data, ghost) {
  const periodLabel = data.requested_since ? `${escapeHtml(data.baseline_date || data.requested_since)} 이후` : '올해';
  if (!ghost || !ghost.available) {
    return `<div class="pf-ghost-headline is-muted">${escapeHtml(ghost?.reason || '방치 평가액을 비교할 정산 데이터가 부족합니다.')}</div>`;
  }
  if (ghost.scope === 'uncalculated') {
    return `<div class="pf-ghost-headline is-muted"><strong>${periodLabel} 매매 효과: 미계산</strong><span>${escapeHtml(ghost.reason || '기준일 보유 종목 가격·수량 미확인')}</span></div>`;
  }
  const pctStr = pfGhostFmtPctPoint(ghost.effect_pct_p);
  const krwStr = pfGhostFmtSummaryKrw(ghost.effect_krw);
  const tone = _pfGhostToneClass(ghost.effect_krw);
  const scopeBadge = ghost.scope === 'partial'
    ? `<span class="pf-ghost-badge is-partial">일부 미계산 포함 · 계산 범위 ${Number(ghost.coverage?.calculable_weight_pct ?? 0).toFixed(1)}%</span>`
    : `<span class="pf-ghost-badge">기준일 ${escapeHtml(data.baseline_date || '')} ~ ${escapeHtml(data.ending_date || '')}</span>`;
  return `<div class="pf-ghost-headline">
    <div class="pf-ghost-headline-main">
      <span class="pf-ghost-kicker">${periodLabel} 매매 효과</span>
      <strong class="pf-ghost-effect ${tone}">${escapeHtml(pctStr)} (${escapeHtml(krwStr)})</strong>
    </div>
    ${scopeBadge}
  </div>`;
}

function _pfGhostSummaryTilesHtml(ghost) {
  if (!ghost || !ghost.available) return '';
  const cov = ghost.coverage || {};
  const actualRet = ghost.actual_return_pct !== null && ghost.actual_return_pct !== undefined
    ? fmtPct(Number(ghost.actual_return_pct))
    : '미계산';
  const ghostRet = ghost.ghost_return_pct !== null && ghost.ghost_return_pct !== undefined
    ? fmtPct(Number(ghost.ghost_return_pct))
    : '미계산';
  const uncalcCnt = Number(cov.uncalculated_holdings || 0);
  const scopeSub = uncalcCnt > 0
    ? `계산 ${Number(cov.calculable_weight_pct || 0).toFixed(1)}% · 미계산 ${uncalcCnt}종목(${Number(cov.uncalculated_weight_pct || 0).toFixed(1)}%)`
    : `전체 보유 ${Number(cov.total_holdings || 0)}종목 + 현금 ${Number(cov.cash_weight_pct || 0).toFixed(1)}%`;

  return `<div class="pf-insight-summary pf-ghost-tiles">
    <div>실제 평가액<strong>${escapeHtml(pfGhostFmtExactKrw(ghost.actual_end_value))}</strong><small>수익률 ${escapeHtml(actualRet)}</small></div>
    <div>방치 평가액 (가만히 있었다면)<strong>${escapeHtml(ghost.ghost_end_value !== null && ghost.ghost_end_value !== undefined ? pfGhostFmtExactKrw(ghost.ghost_end_value) : '미계산')}</strong><small>수익률 ${escapeHtml(ghostRet)}</small></div>
    <div>매매 효과 (실제 − 방치)<strong class="${_pfGhostToneClass(ghost.effect_krw)}">${escapeHtml(ghost.effect_krw !== null && ghost.effect_krw !== undefined ? pfGhostFmtExactKrw(ghost.effect_krw, true) : '미계산')}</strong><small>${escapeHtml(pfGhostFmtPctPoint(ghost.effect_pct_p))}</small></div>
    <div>기간 순입출금 · 계산 범위<strong>${escapeHtml(pfGhostFmtExactKrw(ghost.net_cashflow_krw, true))}</strong><small>${escapeHtml(scopeSub)}</small></div>
  </div>`;
}

function _pfGhostUncalculatedHtml(ghost) {
  const uncalc = ghost?.uncalculated || [];
  if (!uncalc.length) return '';
  const rows = uncalc.map(item => {
    const code = item.stock_code === '_UNTRACKED' ? '' : item.stock_code;
    const nameHtml = code
      ? stockLinkHtml(code, escapeHtml(item.stock_name || code))
      : escapeHtml(item.stock_name || '미분류');
    const qtyText = item.quantity !== null && item.quantity !== undefined
      ? `${Number(item.quantity).toLocaleString('ko-KR')}주`
      : '수량 미기록';
    return `<tr>
      <th scope="row">${nameHtml} <small>${escapeHtml(qtyText)}</small></th>
      <td class="num">${escapeHtml(pfGhostFmtExactKrw(item.baseline_value))}</td>
      <td class="num">${Number(item.baseline_weight_pct || 0).toFixed(2)}%</td>
      <td><span class="pf-ghost-uncalc-tag">미계산</span> ${escapeHtml(item.reason || '가격 미확보')}</td>
    </tr>`;
  }).join('');
  return `<div class="pf-ghost-uncalc-box" role="note">
    <div class="pf-ghost-uncalc-title">미계산 종목 범위 안내 (${uncalc.length}건 · 기준일 비중 합계 ${Number(ghost.coverage?.uncalculated_weight_pct || 0).toFixed(2)}%)</div>
    <div class="pf-insight-table-scroll">
      <table class="pf-insight-table pf-ghost-table">
        <thead><tr><th>종목</th><th>기준일 평가액</th><th>기준일 비중</th><th>상태 · 사유</th></tr></thead>
        <tbody>${rows}</tbody>
      </table>
    </div>
  </div>`;
}

function _pfGhostHorizonCellHtml(horizon, currency) {
  if (!horizon) return '<td class="pf-ghost-cell-muted">미계산</td>';
  if (horizon.status === 'pending') {
    return `<td class="pf-ghost-cell-muted"><span class="pf-ghost-pending-tag">미도래</span><small>${escapeHtml(horizon.reason || '')}</small></td>`;
  }
  if (horizon.status !== 'ok') {
    return `<td class="pf-ghost-cell-muted"><span class="pf-ghost-uncalc-tag">미계산</span><small>${escapeHtml(horizon.reason || '확인 전')}</small></td>`;
  }
  const pct = Number(horizon.change_pct);
  const pctText = typeof fmtPct === 'function' ? fmtPct(pct) : `${pct > 0 ? '+' : ''}${pct.toFixed(2)}%`;
  const tone = _pfGhostToneClass(pct);
  const diffStr = pfGhostFmtDiffAmount(horizon.diff_amount, currency);
  return `<td class="num">
    <div>${pfGhostFmtPrice(horizon.price, currency)}</div>
    <small class="${tone}">${escapeHtml(pctText)}${diffStr ? ` (${diffStr})` : ''}</small>
  </td>`;
}

function _pfGhostOutcomeCellHtml(item) {
  if (item.status === 'uncalculated' && item.day20?.status === 'uncalculated') {
    return `<td><span class="pf-ghost-uncalc-tag">미계산</span> <small>${escapeHtml(item.day20.reason || item.reason || '')}</small></td>`;
  }
  const d20 = item.day20 || {};
  if (d20.status === 'pending') {
    return `<td class="pf-ghost-cell-muted">20거래일 진행 중</td>`;
  }
  if (d20.status === 'ok') {
    const cls = d20.outcome === 'avoided_drop' ? 'is-avoided' : (d20.outcome === 'rose_after' ? 'is-rose' : '');
    return `<td><span class="pf-ghost-outcome ${cls}">${escapeHtml(d20.outcome_label || '확인')}</span></td>`;
  }
  return `<td><span class="pf-ghost-uncalc-tag">미계산</span> <small>${escapeHtml(d20.reason || '')}</small></td>`;
}

function _pfGhostPostSaleHtml(postSale) {
  const ps = postSale || {};
  const sum = ps.summary || {};
  const items = ps.items || [];
  const total = Number(sum.total_count || 0);
  const matured20 = Number(sum.matured_20d_count || 0);
  const calc20 = Number(sum.calculable_20d_count || 0);
  const uncalc20 = Number(sum.uncalculated_20d_count || 0);
  const pending20 = Number(sum.pending_20d_count || 0);
  const uncalcSale = Number(sum.uncalculated_sale_count || 0);

  const countParts = [
    `전체 매도 ${total}건`,
    `계산 가능 ${Number(sum.calculable_any_count || 0)}건`,
    `20거래일 만기 ${matured20}건(계산 가능 ${calc20}건${uncalc20 ? ` · 미계산 ${uncalc20}건` : ''})`,
  ];
  if (pending20 > 0) countParts.push(`미도래 ${pending20}건`);
  if (uncalcSale > 0) countParts.push(`매도원장 미계산 ${uncalcSale}건`);

  let outcomeLine = '';
  if (calc20 > 0) {
    const outcomeParts = [
      `하락 회피 ${Number(sum.avoided_drop_20d_count || 0)}건`,
      `매도 후 상승 ${Number(sum.rose_after_20d_count || 0)}건`,
    ];
    if (Number(sum.flat_20d_count || 0) > 0) outcomeParts.push(`보합 ${Number(sum.flat_20d_count)}건`);
    outcomeLine = ` · 20거래일 기준: ${outcomeParts.join(' / ')}`;
  }

  if (!items.length) {
    return `<div class="pf-ghost-postsale">
      <div class="pf-ghost-section-head">
        <h4>매도 후 추적 (5 · 20거래일 · 현재가)</h4>
        <span class="pf-chart-note">${escapeHtml(countParts.join(' · '))}</span>
      </div>
      <p class="pf-risk-empty">선택 기간(${escapeHtml(ps.since || '')} 이후)에 확인된 매도 내역이 없습니다.</p>
    </div>`;
  }

  const rows = items.map(item => {
    const nameText = escapeHtml(item.stock_name || item.stock_code || '종목 미상');
    const linkHtml = item.stock_code ? stockLinkHtml(item.stock_code, nameText) : nameText;
    const qtyText = item.quantity !== null && item.quantity !== undefined
      ? `${Number(item.quantity).toLocaleString('ko-KR', { maximumFractionDigits: 4 })}주`
      : '';
    const basisSub = item.price_basis === 'close_estimate' ? '<small class="pf-ghost-basis-note">종가 기준</small>' : '';
    const sellCell = item.sell_price !== null && item.sell_price !== undefined
      ? `<td class="num"><div>${pfGhostFmtPrice(item.sell_price, item.currency)}</div>${basisSub}</td>`
      : `<td class="pf-ghost-cell-muted"><span class="pf-ghost-uncalc-tag">미계산</span><small>${escapeHtml(item.reason || '')}</small></td>`;

    return `<tr>
      <td><div>${escapeHtml(item.trade_date || '')}</div><small>${escapeHtml(item.sale_type_label || '매도')}</small></td>
      <th scope="row"><div>${linkHtml}</div>${qtyText ? `<small>${escapeHtml(qtyText)}</small>` : ''}</th>
      <td><span class="pf-ghost-source-tag">${escapeHtml(item.source_label || item.source || '')}</span></td>
      ${sellCell}
      ${_pfGhostHorizonCellHtml(item.day5, item.currency)}
      ${_pfGhostHorizonCellHtml(item.day20, item.currency)}
      ${_pfGhostHorizonCellHtml(item.current, item.currency)}
      ${_pfGhostOutcomeCellHtml(item)}
    </tr>`;
  }).join('');

  const notesHtml = (ps.notes || []).map(n => `<p>${escapeHtml(n)}</p>`).join('');

  return `<div class="pf-ghost-postsale">
    <div class="pf-ghost-section-head">
      <h4>매도 후 추적 (5 · 20거래일 · 현재가)</h4>
      <span class="pf-chart-note">${escapeHtml(countParts.join(' · ') + outcomeLine)}</span>
    </div>
    <div class="pf-insight-table-scroll">
      <table class="pf-insight-table pf-ghost-table pf-ghost-postsale-table">
        <thead>
          <tr>
            <th>매도일</th>
            <th>종목</th>
            <th>출처</th>
            <th>매도가</th>
            <th>+5거래일</th>
            <th>+20거래일</th>
            <th>현재가</th>
            <th>20거래일 결과</th>
          </tr>
        </thead>
        <tbody>${rows}</tbody>
      </table>
    </div>
    ${notesHtml ? `<details class="pf-insight-method"><summary>매도 후 추적 기준과 미계산 처리</summary>${notesHtml}</details>` : ''}
  </div>`;
}

function pfGhostHtml(data) {
  if (!data?.available) {
    return `<p class="pf-risk-empty">${escapeHtml(data?.message || '일별 정산 스냅샷이나 매도 기록이 쌓이면 자동으로 계산합니다.')}</p>`;
  }
  const ghost = data.ghost || {};
  const canDrawChart = ghost.available && ghost.scope !== 'uncalculated' && Array.isArray(ghost.series) && ghost.series.length >= 2;
  const baselineNotice = data.baseline_note
    ? `<p class="pf-insight-notice" role="note">${escapeHtml(data.baseline_note)}</p>`
    : '';
  const assumptionNotice = `<p class="pf-insight-notice pf-ghost-assumption" role="note"><strong>비교 가정:</strong> 기간 중 순입출금은 방치 계좌에도 같은 정산일·같은 금액의 원화 현금으로 반영했으며(현금 비례 가정), 기준일 보유 수량과 원화 현금은 그대로 유지했습니다. 배당 재투자·매매 비용은 별도 가산하지 않은 세전 가격 기준입니다.</p>`;
  const chartBlock = canDrawChart
    ? `<div class="pf-nav-chart-container pf-nav-short pf-ghost-chart-wrap"><div id="pfGhostChart" style="width:100%;height:100%;"></div></div>`
    : '';
  const assumptionsDetail = (ghost.assumptions || []).length
    ? `<details class="pf-insight-method"><summary>유령 포트폴리오 계산 가정 · 범위</summary>${ghost.assumptions.map(t => `<p>${escapeHtml(t)}</p>`).join('')}</details>`
    : '';

  return `${baselineNotice}
    ${_pfGhostHeadlineHtml(data, ghost)}
    ${_pfGhostSummaryTilesHtml(ghost)}
    ${chartBlock}
    ${assumptionNotice}
    ${_pfGhostUncalculatedHtml(ghost)}
    ${assumptionsDetail}
    ${_pfGhostPostSaleHtml(data.post_sale)}`;
}

async function _pfRenderGhostChart(ghost) {
  _pfGhostDisposeChart();
  const container = document.getElementById('pfGhostChart');
  if (!container || !ghost?.available || ghost.scope === 'uncalculated') return;
  const series = Array.isArray(ghost.series) ? ghost.series : [];
  if (series.length < 2) return;
  if (typeof PortfolioTrendChart === 'undefined') {
    container.innerHTML = '<div class="pf-chart-message">차트 렌더러를 불러오지 못했습니다.</div>';
    return;
  }

  if (typeof _waitForChartContainer === 'function') {
    const ready = await _waitForChartContainer(container);
    if (!ready && container.offsetParent === null) return;
  }

  const isUsd = typeof PfStore !== 'undefined' && PfStore.currency?.unit === 'USD';
  const toDisplay = (krw, fx) => {
    const v = Number(krw);
    if (!Number.isFinite(v)) return null;
    if (isUsd && fx && fx > 0) return v / fx;
    if (isUsd && typeof pfFx === 'function') return pfFx(v);
    return Math.round(v);
  };

  const labels = series.map(pt => pt.date);
  const actualVals = series.map(pt => toDisplay(pt.actual_value, pt.fx_usdkrw));
  const ghostVals = series.map(pt => toDisplay(pt.ghost_value, pt.fx_usdkrw));

  const actualColor = typeof cssToken === 'function' ? cssToken('--primary', '#2563eb') : '#2563eb';
  const ghostColor = typeof cssToken === 'function' ? cssToken('--text-secondary', '#64748b') : '#64748b';
  const div = typeof pfFxDivisor === 'function' ? pfFxDivisor() : 10000;
  const unit = typeof pfFxUnit === 'function' ? pfFxUnit() : '만';
  const sym = typeof pfFxSymbol === 'function' ? pfFxSymbol() : '';
  const mobileMode = typeof _isMobileChartMode === 'function' ? _isMobileChartMode() : false;

  const ec = PortfolioTrendChart.create(container, {
    legend: {
      data: ['실제 평가액', '방치 평가액'],
    },
    grid: { left: 68, right: 14, top: 28, bottom: mobileMode ? 24 : 52 },
    dataZoom: mobileMode ? [] : [{ start: 0, end: 100 }],
    xAxis: { type: 'category', data: labels },
    yAxis: {
      type: 'value',
      axisLabel: {
        formatter: v => `${sym}${(v / div).toFixed(isUsd ? 2 : 0)}${unit}`,
      },
    },
    series: [
      {
        name: '실제 평가액',
        type: 'line',
        data: actualVals.map(v => (v === null ? '-' : v)),
        symbol: 'none',
        lineStyle: { color: actualColor, width: 2.2 },
        itemStyle: { color: actualColor },
        areaStyle: {},
        tooltipSuffix: isUsd ? ' USD' : '원',
      },
      {
        name: '방치 평가액',
        type: 'line',
        data: ghostVals.map(v => (v === null ? '-' : v)),
        symbol: 'none',
        lineStyle: { color: ghostColor, width: 1.9, type: 'dashed' },
        itemStyle: { color: ghostColor },
        connectNulls: true,
        tooltipSuffix: isUsd ? ' USD' : '원',
        tooltipDetail: idx => {
          const diff = series[idx]?.diff_krw;
          return Number.isFinite(Number(diff)) ? `매매 효과: ${pfGhostFmtSummaryKrw(diff)}` : '';
        },
      },
    ],
  });

  if (typeof _applyVisibleYAxis === 'function') {
    _applyVisibleYAxis(ec, [actualVals, ghostVals], 0, labels.length - 1, false);
    ec.on('datazoom', () => {
      const win = typeof _chartWindowFromInstance === 'function'
        ? _chartWindowFromInstance(ec, labels.length)
        : { startIdx: 0, endIdx: labels.length - 1 };
      _applyVisibleYAxis(ec, [actualVals, ghostVals], win.startIdx, win.endIdx, false);
    });
  }

  _pfGhostChartInstance = ec;
  if (typeof _kickChartResize === 'function') _kickChartResize(ec);
  if (typeof ResizeObserver !== 'undefined') {
    const ro = new ResizeObserver(() => {
      if (_pfGhostChartInstance) _pfGhostChartInstance.resize();
    });
    ro.observe(container);
    _pfGhostResizeObserver = ro;
  }
}

function pfGhostSetWindow(windowKey) {
  if (!_PF_GHOST_WINDOWS.some(w => w.key === windowKey)) return;
  if (_pfGhostWindow === windowKey && _pfGhostCache[windowKey]) return;
  _pfGhostWindow = windowKey;
  _pfGhostRenderWindowBtns();
  void pfLoadGhostPanel();
}

async function pfLoadGhostPanel({ force = false } = {}) {
  const content = document.getElementById('pfGhostContent');
  if (!content) return;
  _pfGhostRenderWindowBtns();
  const winKey = _pfGhostWindow;
  if (force) _pfGhostCache = {};
  if (!force && _pfGhostCache[winKey]) {
    content.innerHTML = pfGhostHtml(_pfGhostCache[winKey]);
    await _pfRenderGhostChart(_pfGhostCache[winKey]?.ghost);
    return;
  }

  const seq = ++_pfGhostSeq;
  const knownAsOf = _pfGhostCache.ytd?.as_of || null;
  const sinceParam = _pfGhostSinceForWindow(winKey, knownAsOf);
  const url = sinceParam
    ? `/api/portfolio/ghost?since=${encodeURIComponent(sinceParam)}`
    : '/api/portfolio/ghost';

  try {
    const data = await apiFetchJson(url, { errorMessage: '유령 포트폴리오 데이터를 불러오지 못했습니다.' });
    if (seq !== _pfGhostSeq) return;
    _pfGhostCache[winKey] = data;
    content.innerHTML = pfGhostHtml(data);
    await _pfRenderGhostChart(data?.ghost);
  } catch (error) {
    if (seq !== _pfGhostSeq) return;
    _pfGhostDisposeChart();
    if (error?.status !== 401 && typeof reportApiError === 'function') {
      reportApiError(error, '유령 포트폴리오', { silent: true });
    }
    content.textContent = error?.status === 401
      ? '로그인 후 이용할 수 있습니다.'
      : '유령 포트폴리오·매도 후 추적을 불러오지 못했습니다. 잠시 후 다시 열어 주세요.';
  }
}
