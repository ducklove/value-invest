// 관심종목 universe 가치투자 백테스트
let _btChart = null;

// renderResult 호환용 (분석 화면에서 호출되던 잔재) — no-op
function resetBacktestUI() {}

const SCORE_LABELS = {
  pbr_low: 'PBR 낮은 순',
  per_low: 'PER 낮은 순',
  dy_high: '배당수익률 높은 순',
  roe_high: 'ROE 높은 순',
  blend: 'PBR↓ × ROE↑',
};

async function runBacktest() {
  const errEl = document.getElementById('btError');
  const statsEl = document.getElementById('btStats');
  const picksEl = document.getElementById('btPicks');
  const univInfoEl = document.getElementById('btUniverseInfo');
  errEl.textContent = '';
  picksEl.innerHTML = '';
  univInfoEl.innerHTML = '';

  const body = {
    universe: document.getElementById('btUniverse').value,
    score: document.getElementById('btScore').value,
    top_k: Number(document.getElementById('btTopK').value) || 5,
    rebalance: document.getElementById('btRebalance').value,
    years: Number(document.getElementById('btYears').value) || 5,
  };
  const univLabel = {watchlist:'관심종목',portfolio:'포트폴리오',nps:'국민연금'}[body.universe] || body.universe;
  statsEl.innerHTML = `<span class="bt-loading">백테스트 실행 중... (${univLabel} 데이터를 가져오는 데 시간이 걸릴 수 있습니다)</span>`;

  try {
    const res = await apiFetch('/api/backtest/watchlist', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    if (!res.ok) {
      const detail = await res.json().catch(() => ({}));
      throw new Error(detail.detail || `HTTP ${res.status}`);
    }
    const data = await res.json();
    await _renderBacktest(data);
    if (typeof trackEvent === 'function') {
      trackEvent('backtest_watchlist_run', { score: body.score, top_k: String(body.top_k) });
    }
  } catch (e) {
    statsEl.innerHTML = '';
    errEl.textContent = '실행 실패: ' + e.message;
  }
}

function _fmtPct(v) {
  if (v == null || !Number.isFinite(v)) return '-';
  return (v >= 0 ? '+' : '') + v.toFixed(2) + '%';
}

function _fmtNum(v, digits = 2) {
  if (v == null || !Number.isFinite(v)) return '-';
  return Number(v).toFixed(digits);
}

async function _renderBacktest(data) {
  const s = data.stats || {};
  const stratColor = (s.strategy_total_return_pct ?? 0) >= (s.benchmark_total_return_pct ?? 0)
    ? 'var(--positive,#16a34a)' : 'var(--negative,#dc2626)';

  document.getElementById('btStats').innerHTML = `
    <div class="bt-stat-grid">
      <div class="bt-stat"><label>전략 총수익률</label><strong style="color:${stratColor}">${_fmtPct(s.strategy_total_return_pct)}</strong></div>
      <div class="bt-stat"><label>벤치마크 총수익률</label><strong>${_fmtPct(s.benchmark_total_return_pct)}</strong></div>
      <div class="bt-stat"><label>전략 CAGR</label><strong>${_fmtPct(s.strategy_cagr_pct)}</strong></div>
      <div class="bt-stat"><label>벤치마크 CAGR</label><strong>${_fmtPct(s.benchmark_cagr_pct)}</strong></div>
      <div class="bt-stat"><label>리밸런싱 횟수</label><strong>${s.rebalances ?? 0}회</strong></div>
      <div class="bt-stat"><label>universe 종목 수</label><strong>${data.universe_size ?? 0}개</strong></div>
    </div>`;

  if (typeof loadChartLib === 'function') {
    try { await loadChartLib(); } catch {}
  }
  const chartEl = document.getElementById('btChart');
  chartEl.style.display = 'block';
  if (_btChart) { try { _btChart.dispose(); } catch {} }
  _btChart = echarts.init(chartEl);

  const isDark = document.documentElement.getAttribute('data-theme') === 'dark';
  const gridColor = isDark ? 'rgba(148,163,184,0.15)' : 'rgba(0,0,0,0.06)';
  const tickColor = isDark ? '#94a3b8' : '#666';

  _btChart.setOption({
    tooltip: { trigger: 'axis', valueFormatter: (v) => v == null ? '-' : ((v - 1) * 100).toFixed(2) + '%' },
    legend: { data: ['전략', '벤치마크 (전체 동일가중)'], textStyle: { color: tickColor } },
    grid: { left: 56, right: 16, top: 36, bottom: 32 },
    xAxis: {
      type: 'category', data: data.dates,
      axisLine: { lineStyle: { color: gridColor } },
      axisLabel: { color: tickColor },
    },
    yAxis: {
      type: 'value',
      axisLabel: { color: tickColor, formatter: (v) => ((v - 1) * 100).toFixed(0) + '%' },
      splitLine: { lineStyle: { color: gridColor } },
    },
    series: [
      { name: '전략', type: 'line', data: data.strategy_equity, showSymbol: false, lineStyle: { width: 2.2, color: '#2563eb' } },
      { name: '벤치마크 (전체 동일가중)', type: 'line', data: data.benchmark_equity, showSymbol: false, lineStyle: { width: 2, color: '#94a3b8' } },
    ],
  });

  // 최근 리밸런싱 picks
  const picks = data.current_picks || [];
  if (picks.length) {
    const rows = picks.map(p => `
      <tr>
        <td>${escapeHtml(p.corp_name)}</td>
        <td class="num">${_fmtNum(p.metrics?.per)}</td>
        <td class="num">${_fmtNum(p.metrics?.pbr)}</td>
        <td class="num">${_fmtNum(p.metrics?.roe)}</td>
        <td class="num">${_fmtNum(p.metrics?.dividend_yield)}</td>
      </tr>`).join('');
    document.getElementById('btPicks').innerHTML = `
      <h3>최근 리밸런싱 종목 (${SCORE_LABELS[data.score] || data.score})</h3>
      <table class="bt-table">
        <thead><tr><th>종목</th><th class="num">PER</th><th class="num">PBR</th><th class="num">ROE %</th><th class="num">배당 %</th></tr></thead>
        <tbody>${rows}</tbody>
      </table>`;
  }

  // Universe / 누락 종목
  const missing = data.missing || [];
  if (missing.length) {
    document.getElementById('btUniverseInfo').innerHTML = `
      <div class="bt-missing">데이터를 불러오지 못한 종목 (제외됨): ${missing.map(m => escapeHtml(m.corp_name)).join(', ')}</div>`;
  }
}
