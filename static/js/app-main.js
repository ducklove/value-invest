// --- Quote subscription management ---
function _updateQuoteSubscriptions() {
  const requested = { portfolio: [], benchmark: [], sidebar: [], analysis: [] };
  portfolioItems.forEach(item => {
    requested.portfolio.push(item.stock_code);
    if (item.benchmark_code) requested.benchmark.push(item.benchmark_code);
  });
  recentListItems.forEach(item => requested.sidebar.push(item.stock_code));
  if (activeStockCode) requested.analysis.push(activeStockCode);
  QuoteManager.updateSubscriptions(requested);
}

QuoteManager.onQuote = function(code, q) {
  // 'WS tick 왔는데 UI 가 갱신 안 되는 것' 을 확인하기 위한 debug 로그.
  // 기본 off. 브라우저 콘솔에서 `QuoteManager.debug = true` 로 켠 뒤
  // 수 초 관찰하면 tick 수신/매칭/UI 업데이트 호출 여부가 모두 찍힘.
  if (QuoteManager.debug) {
    const pfMatched = portfolioItems.find(i => i.stock_code === code);
    const benchMatched = portfolioItems.some(i => i.benchmark_code === code);
    console.log(
      `[WS] tick code=${code} price=${q.price} change_pct=${q.change_pct}`,
      { pfMatch: !!pfMatched, benchMatch: benchMatched, activeView, editing: pfEditingCode }
    );
  }
  // 1) 분석 뷰 활성 종목
  if (code === activeStockCode && q.price != null) {
    renderQuoteSnapshot({
      date: q.date, price: q.price, previous_close: q.previous_close,
      change: q.change, change_pct: q.change_pct,
    }, activeIndicators);
    flashEl(document.getElementById('quoteSummary'));
  }
  // 2) 포트폴리오 종목 — 이전에는 tbody.innerHTML 를 통째로 재생성해서
  // 마우스 아래 tr 이 매 tick 마다 재생성 → hover 깜빡임 + 갱신된 행에
  // flash 효과가 안 붙는 문제가 있었다. 이제 영향 받는 셀만 in-place
  // 업데이트하고, 해당 행에 flash-update 클래스를 붙여 번쩍인다.
  const pfItem = portfolioItems.find(i => i.stock_code === code);
  if (pfItem && q.price != null) {
    pfItem.quote = { ...(pfItem.quote || {}), ...q };
    if (!pfEditingCode && activeView === 'portfolio'
        && typeof updatePortfolioRowQuote === 'function') {
      updatePortfolioRowQuote(code);
      if (QuoteManager.debug) console.log(`[WS] → updatePortfolioRowQuote(${code}) called`);
    } else if (QuoteManager.debug) {
      console.log(`[WS] SKIP UI update code=${code} reason=`,
        { editing: pfEditingCode, view: activeView,
          fnAvail: typeof updatePortfolioRowQuote === 'function' });
    }
  }
  // 2-1) 벤치마크 실시간 갱신 — 벤치마크 셀들만 in-place 업데이트.
  const isBenchmark = portfolioItems.some(i => i.benchmark_code === code);
  if (isBenchmark && q.change_pct != null) {
    pfBenchmarkQuotes[code] = { ...(pfBenchmarkQuotes[code] || {}), change_pct: q.change_pct };
    if (!pfEditingCode && activeView === 'portfolio'
        && typeof updatePortfolioBenchmarkCells === 'function') {
      updatePortfolioBenchmarkCells(code);
    }
  }
  // 3) 사이드바
  const sbItem = recentListItems.find(i => i.stock_code === code);
  if (sbItem && q.price != null) {
    sbItem.quote_snapshot = { price: q.price, change: q.change, change_pct: q.change_pct };
    const wrapper = document.querySelector(`#recentList .sidebar-item[data-code="${code}"]`);
    if (wrapper) {
      const priceEl = wrapper.querySelector('.quote-price');
      const changeEl = wrapper.querySelector('.quote-change');
      if (priceEl) priceEl.textContent = Number(q.price).toLocaleString();
      if (changeEl) {
        const change = Number(q.change || 0);
        changeEl.classList.remove('up', 'down', 'flat');
        changeEl.classList.add(change > 0 ? 'up' : change < 0 ? 'down' : 'flat');
        if (q.change_pct != null) {
          changeEl.textContent = `${change > 0 ? '+' : ''}${Number(q.change_pct).toFixed(2)}%`;
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
};

// Init
async function initApp() {
  await initAuth();
  await loadRecentList();
  await _mbLoadCatalog();
  await _mbLoadCodes();
  loadMarketSummary();
  loadWikiStats();
  setInterval(loadMarketSummary, 60_000);
  setInterval(_pollBenchmarkQuotes, 60_000);
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
  //   /backtest              → 백테스트 탭
  //   /?code=005930          → (기존 호환) 분석 탭 + 자동 분석
  // 서버가 이 네 개 path 를 모두 index.html 로 서빙하므로 SPA 진입 후
  // pathname 만 보고 탭을 정하면 됨.
  const params = new URLSearchParams(window.location.search);
  const code = params.get('code');
  const path = window.location.pathname.replace(/\/+$/, '') || '/';
  const PATH_TO_VIEW = {
    '/analysis': 'analysis',
    '/portfolio': 'portfolio',
    '/nps': 'nps',
    '/backtest': 'backtest',
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
  }
}

initApp();

window.addEventListener('pageshow', () => {
  syncAuthState({ refreshRecentList: true, refreshPreference: true });
});

window.addEventListener('focus', () => {
  syncAuthState({ refreshRecentList: true, refreshPreference: true });
});

document.addEventListener('visibilitychange', () => {
  if (!document.hidden) {
    syncAuthState({ refreshRecentList: true, refreshPreference: true });
  }
});

window.addEventListener('resize', () => {
  Object.values(charts).forEach(c => { if (c && c.resize) c.resize(); });
  if (_treemapInstance && _treemapInstance.resize) _treemapInstance.resize();
  if (_navChartInstance && _navChartInstance.resize) _navChartInstance.resize();
  if (_valueChartInstance && _valueChartInstance.resize) _valueChartInstance.resize();
});

// Esc closes any visible modal. Iterates in display order so the top-most
// one closes first; after that a second Esc press can close the one behind.
document.addEventListener('keydown', (e) => {
  if (e.key !== 'Escape') return;
  const chartModal = document.getElementById('chartModal');
  if (chartModal && chartModal.style.display !== 'none') {
    if (typeof closeChartModal === 'function') closeChartModal();
    return;
  }
  const groupModal = document.getElementById('pfGroupModal');
  if (groupModal && groupModal.style.display !== 'none') {
    if (typeof closeGroupModal === 'function') closeGroupModal();
    return;
  }
});

// --- Wiki stats badge in the top header --------------------------------
async function loadWikiStats() {
  const el = document.getElementById('wikiStats');
  if (!el) return;
  try {
    const resp = await apiFetch('/api/wiki/stats');
    if (!resp.ok) return;
    const d = await resp.json();
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
