// Analyze
const STEP_LABELS = {
  start: '분석 시작',
  dart_start: 'DART 재무제표 수집',
  dart_fetch: 'DART 재무제표 조회',
  dart_done: 'DART 수집 완료',
  dart_error: 'DART 조회 실패',
  market_start: '시장 데이터 및 파생 지표 계산',
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
  const coverage = getWeeklyDateCoverage(weeklyIndicators?.['주간 주가'] || weeklyIndicators?.['주간 PER'] || []);
  if (!coverage) return '주간 밸류에이션';
  return `주간 밸류에이션 (${coverage.startDate} ~ ${coverage.endDate})`;
}

function getLatestIndicatorValue(series) {
  const entries = (series || []).filter(item => item && item.value !== null && item.value !== undefined && Number.isFinite(Number(item.value)));
  if (entries.length === 0) return null;
  return Number(entries[entries.length - 1].value);
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
    return { per: null, pbr: null, dividendYield: null };
  }

  const latestEps = getLatestIndicatorValue(indicators['EPS (원)']);
  const latestDps = getLatestIndicatorValue(indicators['주당배당금 (원)']);
  const latestBps = getLatestDerivedBps(indicators);

  return {
    per: latestEps && latestEps > 0 ? currentPrice / latestEps : null,
    pbr: latestBps && latestBps > 0 ? currentPrice / latestBps : null,
    dividendYield: latestDps !== null ? (latestDps / currentPrice) * 100 : null,
  };
}

function renderCurrentValuationSummary(indicators, quoteSnapshot) {
  const metrics = getCurrentValuationMetrics(indicators, quoteSnapshot);
  return [
    { label: 'PER', value: formatMetricNumber(metrics.per) },
    { label: 'PBR', value: formatMetricNumber(metrics.pbr) },
    { label: '배당수익률', value: formatMetricNumber(metrics.dividendYield, '%') },
  ].map(item => (
    `<div class="valuation-card"><span class="valuation-label">${item.label}</span><span class="valuation-value">${item.value}</span></div>`
  )).join('');
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
  } else {
    quoteSummary.style.display = 'none';
    quotePrice.textContent = '';
    quoteChange.textContent = '';
    quoteDate.textContent = '';
  }

  coverageNote.innerHTML = renderCurrentValuationSummary(indicators || {}, quote);
}

function isPerChart(key) {
  return key === 'PER' || key === '주간 PER';
}

function normalizeChartValue(value) {
  return value === null || value === undefined ? null : Number(value);
}

function buildChartSeries(key, series) {
  const labelField = Object.prototype.hasOwnProperty.call(series[0], 'date') ? 'date' : 'year';
  const labels = series.map(item => item[labelField]);
  const rawValues = series.map(item => normalizeChartValue(item.value));
  let values = rawValues.slice();
  let note = '';
  let spanGaps = true;

  if (isPerChart(key)) {
    values = rawValues.map(value => {
      if (value === null || !Number.isFinite(value)) return null;
      if (value <= 0 || Math.abs(value) > PER_DISPLAY_MAX) return null;
      return value;
    });
    note = `표시 기준: 음수 PER와 ${PER_DISPLAY_MAX}배 초과 구간은 추세 왜곡을 막기 위해 제외합니다.`;
    spanGaps = false;
  }

  return { labelField, labels, values, rawValues, note, spanGaps };
}

function shouldUseZeroBaseline(values) {
  const validValues = (values || []).filter(value => value !== null && Number.isFinite(value));
  return validValues.length > 0 && validValues.every(value => value >= 0);
}

function formatWeeklyTickLabel(value) {
  if (typeof value !== 'string') return value;
  const match = value.match(/^(\d{4})-(\d{2})-\d{2}$/);
  if (!match) return value;
  return `${match[1].slice(-2)}.${match[2]}`;
}

function _hexToRgba(hex, alpha) {
  const r = parseInt(hex.slice(1,3), 16), g = parseInt(hex.slice(3,5), 16), b = parseInt(hex.slice(5,7), 16);
  return `rgba(${r},${g},${b},${alpha})`;
}

async function renderChartGrid(container, chartKeys, indicatorMap, gridColor, tickColor, prefix) {
  container.innerHTML = '';
  await loadECharts();

  chartKeys.forEach((key, i) => {
    const series = indicatorMap[key] || [];
    if (series.length === 0) return;

    const { labelField, labels, values, rawValues, note, spanGaps } = buildChartSeries(key, series);
    const isWeeklyChart = labelField === 'date';
    const zeroBaseline = shouldUseZeroBaseline(values);
    const color = CHART_COLORS[i % CHART_COLORS.length];

    const card = document.createElement('div');
    card.className = 'chart-card';
    const chartDiv = document.createElement('div');
    chartDiv.className = 'chart-canvas-wrap';
    const echartsDiv = document.createElement('div');
    echartsDiv.id = `${prefix}-chart-${i}`;
    echartsDiv.style.cssText = 'width:100%;height:100%;';
    chartDiv.appendChild(echartsDiv);
    card.innerHTML = `<h3>${key}</h3>`;
    card.appendChild(chartDiv);
    if (note) {
      const noteEl = document.createElement('div');
      noteEl.className = 'chart-note';
      noteEl.textContent = note;
      card.appendChild(noteEl);
    }
    container.appendChild(card);

    const ec = echarts.init(echartsDiv);
    charts[key] = ec;

    const displayLabels = isWeeklyChart ? labels.map(formatWeeklyTickLabel) : labels;

    ec.setOption({
      grid: { left: 45, right: 12, top: 10, bottom: 24 },
      xAxis: {
        type: 'category',
        data: displayLabels,
        axisLine: { lineStyle: { color: gridColor } },
        axisLabel: { color: tickColor, fontSize: 10, interval: isWeeklyChart ? 'auto' : 0, rotate: labels.length > 8 ? 30 : 0 },
        splitLine: { show: false },
      },
      yAxis: {
        type: 'value',
        min: zeroBaseline ? 0 : undefined,
        axisLine: { show: false },
        axisLabel: { color: tickColor, fontSize: 10 },
        splitLine: { lineStyle: { color: gridColor, width: 0.5 } },
      },
      tooltip: {
        trigger: 'axis',
        formatter(params) {
          const p = params[0];
          const idx = p.dataIndex;
          const raw = rawValues[idx];
          const label = labels[idx];
          if (p.value == null || p.value === '-') {
            if (isPerChart(key) && raw !== null && Number.isFinite(raw)) {
              return `${label}<br/>${key}: 표시 제외 (${raw.toLocaleString()})`;
            }
            return `${label}<br/>N/A`;
          }
          return `${label}<br/>${key}: ${Number(p.value).toLocaleString()}`;
        },
      },
      series: [{
        type: 'line',
        data: values.map(v => v === null ? '-' : v),
        smooth: 0.3,
        symbol: isWeeklyChart ? 'none' : 'circle',
        symbolSize: isWeeklyChart ? 0 : 6,
        lineStyle: { color, width: 2.5 },
        itemStyle: { color },
        areaStyle: {
          color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
            { offset: 0, color: _hexToRgba(color, 0.25) },
            { offset: 1, color: _hexToRgba(color, 0.0) },
          ]),
        },
        connectNulls: spanGaps,
      }],
    });
  });
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
    const resp = await apiFetch(`/api/analyze/${stockCode}`, { signal });
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

            if (step === 'dart_fetch') {
              const pct = Math.round((payload.current / payload.total) * 60);
              progressBar.style.width = pct + '%';
              loadingDetail.textContent = `${payload.current} / ${payload.total}`;
              if (lastDartStep !== 'dart_fetch') {
                markLastStepDone();
                addStep('DART 재무제표 연도별 조회 중...', 'active');
              }
              lastDartStep = 'dart_fetch';
            } else if (step === 'dart_done' || step === 'dart_error') {
              markLastStepDone();
              progressBar.style.width = '60%';
              loadingDetail.textContent = '';
              addStep(payload.message, step === 'dart_done' ? 'done' : '');
              lastDartStep = step;
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

function renderResult(data) {
  // Company info
  const infoEl = document.getElementById('companyInfo');
  infoEl.style.display = 'block';
  activeStockCode = data.stock_code;
  activeIndicators = data.indicators || {};
  currentUserPreference = normalizeUserPreference(data.user_preference);
  document.getElementById('companyName').textContent = `${data.corp_name} (${data.stock_code})`;
  const cachedText = data.cached ? `캐시됨 (${new Date(data.analyzed_at).toLocaleDateString('ko-KR')})` : '신규 분석 완료';
  document.getElementById('companyMeta').textContent = cachedText;
  renderUserPreference();
  renderQuoteSnapshot(data.quote_snapshot || {}, activeIndicators);
  trackEvent('analysis_complete', { stock_code: data.stock_code, cached: String(Boolean(data.cached)) });

  // Load reports asynchronously
  loadReports(data.stock_code);

  // Hide empty state, show charts
  document.getElementById('emptyState').style.display = 'none';
  const weeklyTitle = document.getElementById('weeklySectionTitle');
  const weeklyGrid = document.getElementById('weeklyChartsGrid');
  const annualTitle = document.getElementById('annualSectionTitle');
  const grid = document.getElementById('chartsGrid');
  const hasWeeklyCharts = WEEKLY_CHART_KEYS.some(key => (data.weekly_indicators?.[key] || []).length > 0);
  weeklyTitle.textContent = formatWeeklySectionTitle(data.weekly_indicators || {});
  weeklyTitle.style.display = hasWeeklyCharts ? 'block' : 'none';
  weeklyGrid.style.display = hasWeeklyCharts ? 'grid' : 'none';
  annualTitle.style.display = 'block';
  grid.style.display = 'grid';
  weeklyGrid.innerHTML = '';
  grid.innerHTML = '';

  // Destroy existing charts
  Object.values(charts).forEach(c => c.dispose());
  charts = {};

  const isDark = document.documentElement.getAttribute('data-theme') === 'dark';
  const gridColor = isDark ? 'rgba(148,163,184,0.15)' : 'rgba(0,0,0,0.06)';
  const tickColor = isDark ? '#94a3b8' : '#666';

  if (hasWeeklyCharts) {
    renderChartGrid(weeklyGrid, WEEKLY_CHART_KEYS, data.weekly_indicators || {}, gridColor, tickColor, 'weekly');
  }
  renderChartGrid(grid, ANNUAL_CHART_KEYS, data.indicators || {}, gridColor, tickColor, 'annual');
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
          quoteChange.textContent = `${change > 0 ? '+' : ''}${Number(changePct).toLocaleString()}%`;
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

// Reports
let allReports = [];
let reportDisplayCount = 20;
let reportsRequestId = 0;

function getRecommBadge(recomm) {
  if (!recomm) return '';
  const r = recomm.toUpperCase();
  if (r.includes('BUY') || r.includes('매수') || r.includes('강력')) return `<span class="badge badge-buy">${recomm}</span>`;
  if (r.includes('SELL') || r.includes('매도') || r.includes('비중축소')) return `<span class="badge badge-sell">${recomm}</span>`;
  return `<span class="badge badge-hold">${recomm}</span>`;
}

function renderReportsTable(reports, limit) {
  const tbody = document.getElementById('reportsBody');
  const slice = reports.slice(0, limit);
  tbody.innerHTML = slice.map(r => {
    const safeTitle = escapeHtml(r.title);
    const safeSummary = r.summary ? `<div class="report-summary">${escapeHtml(r.summary)}</div>` : '';
    const safeAnalyst = escapeHtml(r.analyst);
    const safeFirm = escapeHtml(r.firm_short || r.firm);
    const safeDate = escapeHtml(r.date);
    const safeHref = safeExternalUrl(buildReportPdfUrl(r.pdf_url) || r.source_url);
    const titleLink = safeHref
      ? `<a href="${safeHref}" target="_blank" rel="noopener noreferrer">${safeTitle}</a>`
      : safeTitle;
    const safeReadHref = safeExternalUrl(r.source_url);
    const linkNote = safeReadHref
      ? `<div class="report-link-note"><a href="${safeReadHref}" target="_blank" rel="noopener noreferrer">네이버 리서치 본문 페이지</a></div>`
      : '';
    const targetPrc = r.target_price ? Number(r.target_price.replace(/,/g, '')).toLocaleString() + '원' : '-';
    return `<tr>
      <td class="report-date">${safeDate}</td>
      <td class="report-firm">${safeFirm}</td>
      <td class="report-title">${titleLink}${safeSummary}${linkNote}<div style="font-size:11px;color:var(--text-secondary);margin-top:2px;">${safeAnalyst}</div></td>
      <td>${getRecommBadge(escapeHtml(r.recommendation))}</td>
      <td class="report-target">${escapeHtml(targetPrc)}</td>
    </tr>`;
  }).join('');

  const moreBtn = document.getElementById('reportsMore');
  moreBtn.style.display = reports.length > limit ? 'block' : 'none';
  moreBtn.textContent = `더 보기 (${limit}/${reports.length}건)`;
}

function showMoreReports() {
  reportDisplayCount += 20;
  renderReportsTable(allReports, reportDisplayCount);
}

async function loadReports(stockCode) {
  const section = document.getElementById('reportsSection');
  const loading = document.getElementById('reportsLoading');
  const table = document.getElementById('reportsTable');
  const countEl = document.getElementById('reportCount');
  const requestId = ++reportsRequestId;

  section.style.display = 'block';
  loading.style.display = 'block';
  loading.textContent = '최신 리포트를 불러오는 중...';
  table.style.display = 'none';
  countEl.textContent = '';
  allReports = [];
  reportDisplayCount = 20;

  if (!hasApiConfiguration()) {
    loading.style.display = 'block';
    loading.textContent = 'GitHub Pages에서는 API 서버 연결 후 리포트를 불러올 수 있습니다.';
    return;
  }

  const localCache = loadReportCache(stockCode);
  let latestReport = localCache?.latestReport || null;
  let renderedFromCache = false;

  if (Array.isArray(localCache?.reports) && localCache.reports.length > 0) {
    allReports = localCache.reports;
    countEl.textContent = `(최근 3년, ${allReports.length}건 · 캐시)`;
    table.style.display = 'table';
    loading.style.display = 'none';
    renderReportsTable(allReports, reportDisplayCount);
    renderedFromCache = true;
  } else if (latestReport) {
    allReports = [latestReport];
    countEl.textContent = '(최신 1건 · 캐시)';
    table.style.display = 'table';
    loading.style.display = 'none';
    renderReportsTable(allReports, 1);
    renderedFromCache = true;
  }

  try {
    const latestResp = await apiFetch(`/api/reports/${stockCode}/latest?refresh=1`);
    const latestData = await latestResp.json();
    if (requestId !== reportsRequestId) return;
    const networkLatest = latestData.report || null;
    const latestChanged = latestData.changed ?? !sameReport(latestReport, networkLatest);

    if (networkLatest) {
      latestReport = networkLatest;
      saveReportCache(stockCode, {
        latestReport,
        reports: Array.isArray(localCache?.reports) ? localCache.reports : [],
      });
    }

    if (networkLatest && (!renderedFromCache || latestChanged)) {
      allReports = [latestReport];
      loading.style.display = 'none';
      countEl.textContent = latestChanged ? '(최신 1건 갱신됨)' : '(최신 1건 확인됨)';
      table.style.display = 'table';
      renderReportsTable(allReports, 1);
    }
  } catch (e) { console.warn(e); }

  try {
    const shouldRefreshFullReports =
      !Array.isArray(localCache?.reports) ||
      localCache.reports.length === 0 ||
      !sameReport(localCache.latestReport, latestReport);

    if (!shouldRefreshFullReports) {
      return;
    }

    const resp = await apiFetch(`/api/reports/${stockCode}`);
    const data = await resp.json();
    if (requestId !== reportsRequestId) return;

    allReports = data.reports || [];
    loading.style.display = 'none';

    if (allReports.length === 0) {
      if (latestReport) {
        countEl.textContent = '(최신 1건만 표시)';
        table.style.display = 'table';
        renderReportsTable([latestReport], 1);
      } else {
        loading.style.display = 'block';
        loading.textContent = '증권사 리포트가 없습니다.';
      }
      return;
    }

    countEl.textContent = `(최근 3년, ${allReports.length}건)`;
    table.style.display = 'table';
    renderReportsTable(allReports, reportDisplayCount);
    saveReportCache(stockCode, { latestReport: allReports[0] || latestReport, reports: allReports });
  } catch (e) {
    if (requestId !== reportsRequestId) return;
    if (latestReport) {
      loading.style.display = 'none';
      countEl.textContent = '(최신 1건만 표시)';
      table.style.display = 'table';
      renderReportsTable([latestReport], 1);
    } else {
      loading.textContent = '리포트를 불러오지 못했습니다.';
    }
  }
}
