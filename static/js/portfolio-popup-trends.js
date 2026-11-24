// Daily evaluation and weight charts shared by the portfolio holding popups.
const _pfPopupTrendDisposers = new WeakMap();

function _pfRenderPopupTrendShell(valueTitle, basis) {
  return `
    <section data-pf-popup-trends>
      <p class="pf-popup-trend-basis">${escapeHtml(basis)}<span data-trend-weight-status></span></p>
      <div class="pf-tag-summary-trends">
        <div class="pf-tag-summary-trend-panel">
          <div class="pf-tag-summary-trend-head">
            <strong>${escapeHtml(valueTitle)} 추세</strong>
            <span data-trend-range></span>
          </div>
          <div class="pf-tag-summary-chart" data-trend-chart></div>
        </div>
        <div class="pf-tag-summary-trend-panel">
          <div class="pf-tag-summary-trend-head">
            <strong>포트폴리오 비중 추세</strong>
            <span data-trend-range></span>
          </div>
          <div class="pf-tag-summary-chart" data-trend-chart></div>
        </div>
      </div>
    </section>`;
}

function _pfDisposePopupTrends(root) {
  _pfPopupTrendDisposers.get(root)?.();
  _pfPopupTrendDisposers.delete(root);
}

function _pfSetPopupTrendMessage(container, message) {
  container.innerHTML = `<div class="pf-chart-message">${escapeHtml(message)}</div>`;
}

function _pfCreatePopupTrendChart(container, range, dates, values, {
  title, color, format, suffix, left, tooltipDetail,
}) {
  const mobile = _isMobileChartMode();
  const chart = PortfolioTrendChart.create(container, {
    grid: { left, right: 24, top: 12, bottom: mobile ? 24 : 52 },
    dataZoom: mobile || dates.length < 2 ? [] : [{ start: 0, end: 100 }],
    xAxis: { type: 'category', data: dates, axisLabel: { formatter: date => date.slice(5) } },
    yAxis: { type: 'value', axisLabel: { formatter: format } },
    series: [{
      name: title,
      type: 'line',
      data: values,
      symbol: 'circle',
      lineStyle: { color, width: 2 },
      areaStyle: values.every(value => Number.isFinite(value) && value >= 0) ? {} : undefined,
      tooltipSuffix: suffix,
      tooltipDetail,
    }],
  });
  function updateWindow() {
    const { start, end } = chart.getOption().dataZoom[0];
    const { startIdx, endIdx } = _chartZoomWindow(dates.length, start, end);
    const hasNegative = _visibleChartValues([values], startIdx, endIdx).some(value => value < 0);
    const axis = _axisRangeForVisibleSeries([values], startIdx, endIdx, !hasNegative);
    if (hasNegative) axis.max = Math.max(0, axis.max);
    chart.setOption({ yAxis: axis });
    const first = dates[startIdx];
    const last = dates[endIdx];
    const days = Math.round((Date.parse(last) - Date.parse(first)) / 86400000) + 1;
    range.textContent = `${first} ~ ${last} · ${days.toLocaleString()}일`;
  }
  updateWindow();
  chart.on('datazoom', updateWindow);
  const observer = typeof ResizeObserver === 'undefined' ? null : new ResizeObserver(() => chart.resize());
  observer?.observe(container);
  return () => {
    observer?.disconnect();
    chart.dispose();
    range.textContent = '';
  };
}

async function _pfLoadPopupTrends(root, path, {
  valueTitle, scopeLabel, valueKey = 'market_value', selectRows = rows => rows,
}) {
  _pfDisposePopupTrends(root);
  const containers = root.querySelectorAll('[data-trend-chart]');
  const ranges = root.querySelectorAll('[data-trend-range]');
  const weightStatus = root.querySelector('[data-trend-weight-status]');
  weightStatus.textContent = '';
  const cleanups = [];
  const dispose = () => cleanups.forEach(cleanup => cleanup());
  const isCurrent = () => root.isConnected && _pfPopupTrendDisposers.get(root) === dispose;
  _pfPopupTrendDisposers.set(root, dispose);
  const showMessage = message => containers.forEach(container => _pfSetPopupTrendMessage(container, message));
  showMessage(`${scopeLabel} 추세 데이터를 불러오는 중입니다...`);
  try {
    const payload = await apiFetchJson(path, { errorMessage: '추세 데이터를 불러오지 못했습니다.' });
    if (!isCurrent()) return;
    const rows = selectRows(Array.isArray(payload) ? payload : []).filter(row => row?.date);
    if (!rows.length) {
      showMessage('일간 스냅샷 데이터가 없습니다.');
      return;
    }
    if (typeof PortfolioTrendChart === 'undefined') {
      showMessage('차트 렌더러를 불러오지 못했습니다.');
      return;
    }
    if (!await _waitForChartContainer(containers[0]) || !isCurrent()) return;

    const usd = PfStore.currency.unit === 'USD';
    const dates = rows.map(row => row.date);
    const values = rows.map(row => {
      const value = row[valueKey];
      if (!Number.isFinite(value)) return null;
      if (!usd || value === 0) return value;
      const fx = row.fx_usdkrw;
      return Number.isFinite(fx) && fx > 0 ? value / fx : null;
    });
    // A constituent-only sum can disagree with the saved portfolio total on legacy dates.
    // Weight requires the numerator and the portfolio total to use the same basis.
    const weights = rows.map(row => {
      const { total_value: total, snapshot_total_value: snapshotTotal } = row;
      const value = row[valueKey];
      if (!Number.isFinite(value) || !Number.isFinite(total) || !Number.isFinite(snapshotTotal) || snapshotTotal <= 0
          || Math.abs(total - snapshotTotal) > 0.01) return null;
      return value / snapshotTotal * 100;
    });
    const unavailable = weights.filter(value => !Number.isFinite(value)).length;
    if (unavailable) weightStatus.textContent = ` · 비중 산출 불가 ${unavailable.toLocaleString()}일`;
    const amount = new Intl.NumberFormat(usd ? 'en-US' : 'ko-KR', {
      notation: 'compact', maximumSignificantDigits: 3,
      ...(usd ? { style: 'currency', currency: 'USD' } : {}),
    });
    const percent = new Intl.NumberFormat('ko-KR', { maximumSignificantDigits: 3 });
    const charts = [
      {
        title: valueTitle, series: values, color: '#2563eb', left: 70,
        format: value => `${amount.format(value)}${usd ? '' : '원'}`,
        suffix: usd ? ' USD' : '원',
        empty: usd && rows.some(row => Number.isFinite(row[valueKey]))
          ? '저장된 일별 환율이 없습니다.' : '평가금액 데이터가 없습니다.',
      },
      {
        title: '포트폴리오 비중', series: weights, color: '#059669', left: 62,
        format: value => `${percent.format(value)}%`, suffix: '%',
        tooltipDetail: index => `전체 평가액 ${Math.round(rows[index].snapshot_total_value).toLocaleString('ko-KR')}원`,
        empty: '포트폴리오 비중 데이터가 없습니다.',
      },
    ];
    charts.forEach(({ series, empty, ...options }, index) => {
      const container = containers[index];
      if (series.some(Number.isFinite)) {
        cleanups.push(_pfCreatePopupTrendChart(container, ranges[index], dates, series, options));
      } else {
        _pfSetPopupTrendMessage(container, empty);
      }
    });
  } catch (error) {
    if (!isCurrent()) return;
    _pfDisposePopupTrends(root);
    showMessage(error?.message || '추세 데이터를 불러오지 못했습니다.');
    if (error?.status !== 401) console.warn(error);
  }
}
