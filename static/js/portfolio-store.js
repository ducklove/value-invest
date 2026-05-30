// Portfolio front-end state store.
//
// First step of the roadmap's "전역 변수 기반을 줄이고 portfolio-store.js 같은
// 상태 모듈을 둔다": a single namespace that owns portfolio view state instead
// of letting it live in bare top-level globals scattered across the split
// files. State is migrated in incrementally — benchmark quotes move first;
// pfNavHistory / portfolioItems follow once each is verified.
//
// Loaded as a plain (non-module) script before the other portfolio scripts, so
// `PfStore` is a shared global by the time they run.
const PfStore = {
  // benchmark_code -> { change_pct, name }
  benchmarkQuotes: {},
};

// Expose explicitly for tests and any late-bound access.
if (typeof window !== 'undefined') {
  window.PfStore = PfStore;
}
