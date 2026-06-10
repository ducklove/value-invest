// Portfolio front-end state store.
//
// The roadmap's "전역 변수 기반을 줄이고 portfolio-store.js 같은 상태 모듈을
// 둔다" step: a single namespace that owns the portfolio view state that is
// shared across the split portfolio-*.js files, instead of bare top-level
// globals scattered around. File-local UI plumbing (debounce timers, style
// element refs, pointer guards) intentionally stays in its owning file.
//
// Loaded as a plain (non-module) script before the other portfolio scripts, so
// `PfStore` is a shared global by the time they run.
const PfStore = {
  // benchmark_code -> { change_pct, name }
  benchmarkQuotes: {},
  // [{date, nav, total_value, total_invested, total_units}, ...]
  navHistory: [],
  // portfolio holdings: [{stock_code, quantity, avg_price, quote, ...}, ...]
  items: [],
  // Which top-level view is visible: 'analysis' | 'portfolio' | 'nps' | ...
  activeView: 'analysis',
  // loadPortfolio() re-entrancy guard.
  loading: false,
  // [{group_name, sort_order, is_default}, ...]
  groups: [],
  // Holdings table sort. groupSort is the independent group-order toggle.
  sort: { key: null, asc: false, groupSort: false },
  // group: null = all selected, Set of group_names = filtered.
  filters: { group: null, searchText: '' },
  // Inline row edit: code being edited / code whose save is in flight.
  edit: { code: null, savingCode: null },
  // Drag&drop manual order save pipeline (see portfolio-order.js).
  manualOrder: { pendingCodes: null, revision: 0, saveInFlight: false },
  snapshots: {
    // {total_value, nav, fx_usdkrw, ...} at end of previous month
    monthEnd: null,
    // stock_code -> market_value at month end
    monthEndStockValues: {},
    // {date, total_value, fx_usdkrw, ...} for first snapshot of this year
    yearStart: null,
    // stock_code -> market_value at year start
    yearStartStockValues: {},
    // {total_value, fx_usdkrw, stock_values, today_net_cashflow}
    prevDay: null,
    // [{ts, total_value}, ...]
    intraday: [],
  },
  // unit: 'KRW' or 'USD'; fxRate: USD/KRW rate.
  currency: { unit: 'KRW', fxRate: null },
  // localStorage-backed view preferences (persisted by portfolio-shell.js).
  prefs: { simpleMode: false, compactRows: false },
};

// Expose explicitly for tests and any late-bound access.
if (typeof window !== 'undefined') {
  window.PfStore = PfStore;
}
