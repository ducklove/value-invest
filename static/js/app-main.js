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

let _pfRenderQueued = false;
QuoteManager.onQuote = function(code, q) {
  // 1) 분석 뷰 활성 종목
  if (code === activeStockCode && q.price != null) {
    renderQuoteSnapshot({
      date: q.date, price: q.price, previous_close: q.previous_close,
      change: q.change, change_pct: q.change_pct,
    }, activeIndicators);
    flashEl(document.getElementById('quoteSummary'));
  }
  // 2) 포트폴리오 종목
  const pfItem = portfolioItems.find(i => i.stock_code === code);
  if (pfItem && q.price != null) {
    pfItem.quote = { price: q.price, change: q.change, change_pct: q.change_pct, previous_close: q.previous_close, date: q.date };
  }
  // 2-1) 벤치마크 실시간 갱신
  const isBenchmark = portfolioItems.some(i => i.benchmark_code === code);
  if (isBenchmark && q.change_pct != null) {
    pfBenchmarkQuotes[code] = { ...(pfBenchmarkQuotes[code] || {}), change_pct: q.change_pct };
  }
  if ((pfItem || isBenchmark) && q.price != null) {
    if (!pfEditingCode && activeView === 'portfolio' && !_pfRenderQueued) {
      _pfRenderQueued = true;
      requestAnimationFrame(() => { _pfRenderQueued = false; renderPortfolio(); });
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
  setInterval(loadMarketSummary, 60_000);
  setInterval(_pollBenchmarkQuotes, 60_000);
  QuoteManager.connect();
  _updateQuoteSubscriptions();
  trackEvent('app_ready', { auth_state: currentUser ? 'logged_in' : 'guest' });
  // Mobile + logged in → default to portfolio view
  const params = new URLSearchParams(window.location.search);
  const code = params.get('code');
  if (code) {
    switchView('analysis');
    analyzeStock(code.trim());
  } else if (currentUser && window.innerWidth <= 900) {
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
