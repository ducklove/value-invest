// --- Quote subscription management ---
function _updateQuoteSubscriptions() {
  const requested = { portfolio: [], benchmark: [], sidebar: [], analysis: [] };
  PfStore.items.forEach(item => {
    requested.portfolio.push(item.stock_code);
    if (item.benchmark_code) requested.benchmark.push(item.benchmark_code);
  });
  recentListItems.forEach(item => requested.sidebar.push(item.stock_code));
  if (activeStockCode) requested.analysis.push(activeStockCode);
  QuoteManager.updateSubscriptions(requested);
}

let _pfQuotePaintQueued = false;
let _pfDeferredRenderTimer = null;
let _pfSummaryRenderQueued = false;
const _pfQuoteQueuedCodes = new Set();
const _pfBenchmarkQueuedCodes = new Set();
const _pfFlashQueuedCodes = new Set();

function _schedulePortfolioDeferredRender(delay = 1200) {
  if (PfStore.edit.code) return;
  if (_pfDeferredRenderTimer) return;
  _pfDeferredRenderTimer = setTimeout(() => {
    _pfDeferredRenderTimer = null;
    if (typeof _pfIsPointerInteractionActive === 'function' && _pfIsPointerInteractionActive()) {
      _schedulePortfolioDeferredRender(350);
      return;
    }
    renderPortfolio();
  }, delay);
}

function _queuePortfolioSummaryRender() {
  if (_pfSummaryRenderQueued) return;
  _pfSummaryRenderQueued = true;
  requestAnimationFrame(() => {
    _pfSummaryRenderQueued = false;
    if (PfStore.activeView === 'portfolio' && typeof renderPortfolio === 'function') {
      renderPortfolio({ summaryOnly: true });
    }
  });
}

function _paintPortfolioQuoteUpdates() {
  _pfQuotePaintQueued = false;
  const rowCodes = [..._pfQuoteQueuedCodes];
  const benchmarkCodes = [..._pfBenchmarkQueuedCodes];
  _pfQuoteQueuedCodes.clear();
  _pfBenchmarkQueuedCodes.clear();

  rowCodes.forEach(rowCode => {
    updatePortfolioRowQuote(rowCode, _pfFlashQueuedCodes.has(rowCode));
  });
  benchmarkCodes.forEach(updatePortfolioBenchmarkCells);
  _pfFlashQueuedCodes.clear();
  _queuePortfolioSummaryRender();
  _schedulePortfolioDeferredRender();
}

function _queuePortfolioQuotePaint() {
  if (PfStore.edit.code || _pfQuotePaintQueued) return;
  _pfQuotePaintQueued = true;
  requestAnimationFrame(_paintPortfolioQuoteUpdates);
}

QuoteManager.onQuote = function(code, q) {
  const usableQuote = quoteIsUsable(q);
  // 1) 분석 뷰 활성 종목
  if (code === activeStockCode && usableQuote) {
    const nextQuote = mergeQuoteSnapshot(activeQuoteSnapshot, q);
    const accepted = quoteSnapshotDisplayChanged(activeQuoteSnapshot, nextQuote);
    activeQuoteSnapshot = nextQuote;
    if (accepted) {
      renderQuoteSnapshot({
        date: activeQuoteSnapshot.date, price: activeQuoteSnapshot.price, previous_close: activeQuoteSnapshot.previous_close,
        change: activeQuoteSnapshot.change, change_pct: activeQuoteSnapshot.change_pct,
      }, activeIndicators);
      flashEl(document.getElementById('quoteSummary'));
    }
  }
  // 2) 포트폴리오 종목. 가격이 실제 변경된 경우에만 flash 대상에 추가 —
  //    tick 수신 자체로 flash 하면 거래 활발한 종목이 계속 번쩍거려 거슬림.
  const pfItem = PfStore.items.find(i => i.stock_code === code);
  let pfQuoteAccepted = false;
  if (pfItem && usableQuote) {
    const prevPrice = quotePriceOrNull(pfItem.quote);
    const nextQuote = mergeQuoteSnapshot(pfItem.quote, q);
    pfQuoteAccepted = quoteSnapshotDisplayChanged(pfItem.quote, nextQuote);
    if (pfQuoteAccepted) {
      pfItem.quote = nextQuote;
      if (prevPrice !== quotePriceOrNull(pfItem.quote)) _pfFlashQueuedCodes.add(code);
      _pfQuoteQueuedCodes.add(code);
    }
  }
  const isBenchmark = PfStore.items.some(i => i.benchmark_code === code);
  if (isBenchmark && q.change_pct != null && q._stale !== true) {
    PfStore.benchmarkQuotes[code] = { ...(PfStore.benchmarkQuotes[code] || {}), change_pct: q.change_pct };
    _pfBenchmarkQueuedCodes.add(code);
  }
  if (pfQuoteAccepted || (isBenchmark && q._stale !== true)) {
    _queuePortfolioQuotePaint();
  }
  // 3) 사이드바
  const sbItem = recentListItems.find(i => i.stock_code === code);
  if (sbItem && usableQuote) {
    const nextQuote = mergeQuoteSnapshot(sbItem.quote_snapshot, {
      date: q.date, price: q.price, previous_close: q.previous_close,
      change: q.change, change_pct: q.change_pct, source: q.source,
      ts: q.ts, fetched_at: q.fetched_at,
    });
    if (quoteSnapshotDisplayChanged(sbItem.quote_snapshot, nextQuote)) {
      sbItem.quote_snapshot = nextQuote;
      const wrapper = document.querySelector(`#recentList .sidebar-item[data-code="${code}"]`);
      if (wrapper) {
        const sq = sbItem.quote_snapshot || {};
        const priceEl = wrapper.querySelector('.quote-price');
        const changeEl = wrapper.querySelector('.quote-change');
        if (priceEl) priceEl.textContent = Number(sq.price).toLocaleString();
        if (changeEl) {
          const change = Number(sq.change || 0);
          changeEl.classList.remove('up', 'down', 'flat');
          changeEl.classList.add(change > 0 ? 'up' : change < 0 ? 'down' : 'flat');
          if (sq.change_pct != null) {
            changeEl.textContent = `${change > 0 ? '+' : ''}${Number(sq.change_pct).toFixed(2)}%`;
          }
        }
        // live dot
        const nameEl = wrapper.querySelector('.name');
        if (nameEl) {
          const dot = nameEl.querySelector('.ws-live-dot');
          if (QuoteManager.isLive(code) && !dot) {
            const d = document.createElement('span');
            d.className = 'ws-live-dot'; d.title = '실시간';
            nameEl.appendChild(d);
          } else if (!QuoteManager.isLive(code) && dot) { dot.remove(); }
        }
      }
    }
  }
};

// Init
async function initApp() {
  // 브라우저의 스크롤 위치 복원을 끈다 — 모바일에서 새로고침/재방문 시 이전 스크롤
  // 위치(아래로 내려간 상태)로 복원돼 포트폴리오가 중간부터 보이는 문제를 막는다.
  if ('scrollRestoration' in history) {
    try { history.scrollRestoration = 'manual'; } catch (e) {}
  }
  await initAuth();
  await loadRecentList();
  await _mbLoadCatalog();
  await _mbLoadCodes();
  loadMarketSummary();
  loadMarketTape();
  if (typeof loadInvestingDashboard === 'function') loadInvestingDashboard();
  loadDailyMarketBrief();
  loadWikiStats();
  setInterval(loadMarketSummary, 60_000);
  setInterval(() => loadMarketTape(false), 45_000);
  setInterval(_pollBenchmarkQuotes, 60_000);
  setInterval(_refreshActivePortfolioTodayState, 5 * 60_000);
  // Refresh wiki stats every 5 minutes so the badge reflects ongoing
  // background ingestion without needing a reload.
  setInterval(loadWikiStats, 5 * 60_000);
  QuoteManager.connect();
  _updateQuoteSubscriptions();
  trackEvent('app_ready', { auth_state: currentUser ? 'logged_in' : 'guest' });

  // 외부 사이트에서 특정 탭·종목으로 바로 연결 가능하도록 URL 을 해석.
  //   /analysis?code=005930  → 분석 탭 + 005930 자동 분석
  //   /portfolio             → 포트폴리오 탭
  //   /nps                   → 국민연금 탭
  //   /labs                  → 실험실 허브 (메인 탭에서는 숨김)
  //   /insights              → 실험실 인사이트 보드 (직접 URL)
  //   /screener              → 실험실 밸류 스크리너 (직접 URL)
  //   /?code=005930          → (기존 호환) 분석 탭 + 자동 분석
  // 서버가 이 path 들을 모두 index.html 로 서빙하므로 SPA 진입 후
  // pathname 만 보고 탭을 정하면 됨.
  const params = new URLSearchParams(window.location.search);
  const code = params.get('code');
  const path = window.location.pathname.replace(/\/+$/, '') || '/';
  const PATH_TO_VIEW = {
    '/investing': 'investing',
    '/analysis': 'analysis',
    '/portfolio': 'portfolio',
    '/nps': 'nps',
    '/labs': 'labs',
    '/insights': 'insights',
    '/screener': 'screener',
  };
  const viewFromPath = PATH_TO_VIEW[path];
  if (viewFromPath) {
    switchView(viewFromPath);
  }
  if (code) {
    // code 가 있으면 분석 대상. path 와 무관하게 분석 탭으로 이동해
    // 종목을 로드 — '/portfolio?code=...' 같은 조합도 자연스럽게 동작.
    switchView('analysis');
    analyzeStock(code.trim());
  } else if (!viewFromPath && currentUser && window.innerWidth <= 900) {
    // Mobile + logged in → default to portfolio (경로가 명시된 경우
    // 이 기본값은 덮지 않음).
    switchView('portfolio');
    // 포트폴리오 데이터가 비동기로 채워진 뒤에도 화면을 최상단에서 시작하도록 보정.
    requestAnimationFrame(() => window.scrollTo(0, 0));
  }
}

initApp();

window.addEventListener('pageshow', () => {
  syncAuthState({ refreshRecentList: true, refreshPreference: true });
  _refreshActivePortfolioTodayState();
});

window.addEventListener('focus', () => {
  syncAuthState({ refreshRecentList: true, refreshPreference: true });
  _refreshActivePortfolioTodayState();
});

document.addEventListener('visibilitychange', () => {
  if (!document.hidden) {
    syncAuthState({ refreshRecentList: true, refreshPreference: true });
    _refreshActivePortfolioTodayState();
  }
});

function _refreshActivePortfolioTodayState() {
  if (PfStore.activeView !== 'portfolio' || !currentUser || typeof pfRefreshTodayState !== 'function') return;
  // 탭 복귀 시 백그라운드 갱신 — 실패해도 토스트 없이 로그만.
  pfRefreshTodayState({ force: true }).catch(e => reportApiError(e, '오늘 수익 갱신', { silent: true }));
}

function _resizeAllCharts() {
  Object.values(charts).forEach(c => { if (c && c.resize) c.resize(); });
  if (_treemapInstance && _treemapInstance.resize) _treemapInstance.resize();
  if (_navChartInstance && _navChartInstance.resize) _navChartInstance.resize();
  if (_valueChartInstance && _valueChartInstance.resize) _valueChartInstance.resize();
}

window.addEventListener('resize', () => {
  if (typeof updateMobileAuthChrome === 'function') updateMobileAuthChrome();
  if (typeof pfSyncSimpleModeForViewport === 'function') pfSyncSimpleModeForViewport();
  if (typeof pfSyncMobileFixedView === 'function') pfSyncMobileFixedView();
  _resizeAllCharts();
});

// 모바일 회전 대응. iOS Safari 는 orientationchange 직후 resize 이벤트가 레이아웃
// 갱신 전에 발생해 차트가 옛 폭으로 남는다. 레이아웃이 안정될 시간을 두고 여러 시점에
// 다시 리사이즈해 가로↔세로 전환 시 그래프가 새 폭에 맞춰지도록 한다.
window.addEventListener('orientationchange', () => {
  [120, 350, 650].forEach(delay => setTimeout(_resizeAllCharts, delay));
});

// --- Wiki stats badge in the top header --------------------------------
async function loadWikiStats() {
  const el = document.getElementById('wikiStats');
  if (!el) return;
  try {
    const d = await apiFetchJson('/api/wiki/stats', { fallback: null });
    if (!d) return;
    const stocks = Number(d.stocks_covered || 0);
    const entries = Number(d.total_entries || 0);
    if (stocks === 0 && entries === 0) {
      el.innerHTML = '';
      return;
    }
    // Keep it compact — `<num> 리포트 · <num> 종목`.
    el.innerHTML = `<span class="ws-num">${entries.toLocaleString()}</span> 리포트`
      + `<span class="ws-sep">·</span>`
      + `<span class="ws-num">${stocks.toLocaleString()}</span> 종목`;
  } catch {
    // Silent — the badge is optional UX; no error toast.
  }
}

// --- PWA: service worker registration (installability v1) ---------------
// Feature-detected and non-fatal — the app behaves identically without it.
// sw.js is conservative by contract: network-first for HTML and /api/*,
// cache-first only for ?v=-stamped (immutable) assets and manifest/icons.
if ('serviceWorker' in navigator) {
  window.addEventListener('load', () => {
    navigator.serviceWorker.register('/sw.js').catch(() => {
      // Silent — installability is a progressive enhancement.
    });
  });
}
