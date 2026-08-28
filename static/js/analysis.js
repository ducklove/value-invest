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
    const data = await apiFetchJson(url, { errorMessage: '금일 시황을 불러오지 못했습니다.' });
    const markdown = data.markdown || '';
    if (body) {
      body.innerHTML = markdown
        ? _renderSafeMarkdown(markdown)
        : '<p>표시할 시황 본문이 없습니다.</p>';
    }
    const payload = data.payload || {};
    // 시장 전체 기준 브리프: 이슈 종목 = 상/하한가·급등·급락 movers.
    const issueCount = (payload.movers || [])
      .filter(row => row && ['상한가', '하한가', '급등', '급락'].includes(row.bucket)).length;
    const disclosureCount = (payload.disclosures || []).length;
    const materialCount = (payload.disclosures || []).filter(row => row && row.is_material).length;
    const tokenText = `입력 ${(data.tokens_in ?? 0).toLocaleString('ko-KR')} / 출력 ${(data.tokens_out ?? 0).toLocaleString('ko-KR')} 토큰`;
    const costText = _formatDailyMarketCost(data.cost_usd);
    const cacheText = data.cached ? '캐시' : '새 생성';
    const generatedAt = data.updated_at ? new Date(data.updated_at).toLocaleString('ko-KR') : '';
    // 기준 시각 = 증거 수집 시점(payload.generated_at, 서버 KST). 문자열에서
    // HH:MM만 뽑아 타임존 재해석 없이 그대로 표기한다(없으면 날짜만).
    const baseTimeMatch = String(payload.generated_at || data.updated_at || '').match(/[T ](\d{2}):(\d{2})/);
    const baseTime = baseTimeMatch ? ` ${baseTimeMatch[1]}:${baseTimeMatch[2]}` : '';
    if (status) {
      status.textContent = [
        `${payload.brief_date || data.brief_date || ''}${baseTime} 기준`,
        '시장 전체',
        `급등/급락 ${issueCount}개`,
        `공시 ${disclosureCount}건${materialCount ? ` (중요 후보 ${materialCount}건)` : ''}`,
        data.llm_ok === false ? '생성 실패 — 새로고침 시 재시도' : '',
      ].filter(Boolean).join(' · ');
      status.classList.toggle('error', data.llm_ok === false);
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

function renderQuoteSnapshot(quoteSnapshot, indicators = activeIndicators) {
  const quoteSummary = document.getElementById('quoteSummary');
  const quotePrice = document.getElementById('quotePrice');
  const quoteChange = document.getElementById('quoteChange');
  const quoteDate = document.getElementById('quoteDate');
  const coverageNote = document.getElementById('coverageNote');
  const quote = quoteSnapshot || {};

  if (quote.price !== null && quote.price !== undefined) {
    quoteSummary.style.display = 'flex';
    quotePrice.textContent = `${Number(quote.price).toLocaleString('ko-KR')}원`;
    const change = Number(quote.change || 0);
    const changePct = quote.change_pct;
    const changePrefix = change > 0 ? '+' : '';
    quoteChange.textContent = changePct !== null && changePct !== undefined
      ? `${changePrefix}${change.toLocaleString('ko-KR')}원 (${changePrefix}${Number(changePct).toLocaleString('ko-KR')}%)`
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
  setAnalysisProgress(0);
  document.getElementById('progressSteps').innerHTML = '';
  document.getElementById('loadingDetail').textContent = '';
}

function setAnalysisProgress(percent) {
  const value = Math.max(0, Math.min(100, Number(percent) || 0));
  const progressBar = document.getElementById('progressBar');
  if (!progressBar) return;
  progressBar.style.width = `${value}%`;
  progressBar.setAttribute('aria-valuenow', String(value));
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
  const cancelBtn = document.getElementById('cancelBtn');

  overlay.classList.add('show');
  overlay.setAttribute('aria-busy', 'true');
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
              setAnalysisProgress(30);
              addStep(payload.message, 'active');
            } else if (step === 'financial_done' || step === 'financial_error') {
              markLastStepDone();
              setAnalysisProgress(60);
              addStep(payload.message, step === 'financial_done' ? 'done' : '');
            } else if (step === 'market_start') {
              markLastStepDone();
              setAnalysisProgress(65);
              addStep(payload.message, 'active');
            } else if (step === 'market_done' || step === 'market_error') {
              markLastStepDone();
              setAnalysisProgress(85);
              addStep(payload.message, step === 'market_done' ? 'done' : '');
            } else if (step === 'saving') {
              setAnalysisProgress(90);
              addStep(payload.message, 'active');
            } else if (step === 'analyzing') {
              markLastStepDone();
              setAnalysisProgress(95);
              addStep(payload.message, 'active');
            } else if (step === 'start') {
              addStep(payload.message, 'active');
            }
          } else if (eventType === 'result') {
            setAnalysisProgress(100);
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
    overlay.setAttribute('aria-busy', 'false');
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
  // renderQuoteSnapshot → _renderCoverage 가 주간 시계열로 카드를 계산하므로,
  // 이전 종목의 주간 데이터가 섞이지 않게 카드 첫 렌더 전에 갱신해 둔다.
  _lastWeeklyIndicators = data.weekly_indicators || null;
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
  // 투자 일지 — 이 종목의 판단 기록 폼/타임라인(portfolio-journal.js).
  if (typeof loadStockJournal === 'function') loadStockJournal(data.stock_code);
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
      const data = await apiFetchJson(`/api/cache/list?include_quotes=true&tab=${tab}`, { fallback: [] });
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
        quotePrice.textContent = Number(quote.price).toLocaleString('ko-KR');
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
    await apiFetchJson(`/api/cache/${stockCode}?tab=${activeTab}`, {
      method: 'DELETE',
      errorMessage: '삭제하지 못했습니다.',
    });
    loadRecentList();
  } catch (e) {
    showToast(e.message || '삭제하지 못했습니다.');
  }
}

async function saveRecentOrder(stockCodes) {
  await apiFetchJson('/api/cache/order', {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ stock_codes: stockCodes, tab: activeTab }),
    errorMessage: '순서를 저장하지 못했습니다.',
  });
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
