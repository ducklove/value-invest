// Portfolio front-end state store.
//
// First step of the roadmap's "전역 변수 기반을 줄이고 portfolio-store.js 같은
// 상태 모듈을 둔다": a single namespace that owns portfolio view state instead
// of letting it live in bare top-level globals scattered across the split
// files. Benchmark quotes, NAV history and the holdings list now live here;
// remaining view-state globals (snapshots, intraday, etc.) migrate in later.
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
};

// Expose explicitly for tests and any late-bound access.
if (typeof window !== 'undefined') {
  window.PfStore = PfStore;
}
