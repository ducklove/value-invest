// 종목분석 "내 포지션" 바 — 보유 중인 종목을 분석하면 수량·평단·평가손익·
// 목표가·보유 메모를 입력 없이 보여주고, 포트폴리오 행·투자 인사이트·메모
// 편집으로 바로 이어 준다. 종목분석과 포트폴리오가 서로를 모르던 단절을 메운다.
// 보유 목록은 포트폴리오를 이미 열었으면 PfStore.items 를, 아니면 시세 조회가
// 없는 GET /api/portfolio(합산 계좌)를 1분 캐시해 쓴다.

const ANALYSIS_POSITION_TTL_MS = 60_000;
let _analysisPositionCache = { at: 0, items: null, pending: null, user: null };

function _analysisPositionUserKey() {
  return typeof currentUser !== 'undefined' && currentUser ? String(currentUser.google_sub || currentUser.email || 'user') : null;
}
let _analysisPositionSeq = 0;

function _analysisPositionStoreItems() {
  if (typeof PfStore === 'undefined' || !Array.isArray(PfStore.items) || !PfStore.items.length) return null;
  // 개별 계좌를 보고 있으면 그 계좌 수량만 들어 있으므로 합산 목록을 따로 받는다.
  if (PfStore.accountId) return null;
  return PfStore.items;
}

async function _analysisPositionItems() {
  const fromStore = _analysisPositionStoreItems();
  if (fromStore) return fromStore;
  const userKey = _analysisPositionUserKey();
  if (_analysisPositionCache.user !== userKey) analysisPositionInvalidate();
  const cache = _analysisPositionCache;
  cache.user = userKey;
  if (cache.items && Date.now() - cache.at < ANALYSIS_POSITION_TTL_MS) return cache.items;
  if (!cache.pending) {
    cache.pending = apiFetchJson('/api/portfolio', { errorMessage: '보유 종목을 불러오지 못했습니다.' })
      .then((items) => {
        // 요청 중 로그아웃·사용자 전환이 있었으면 버린다.
        if (_analysisPositionCache !== cache || _analysisPositionUserKey() !== userKey) return [];
        cache.items = Array.isArray(items) ? items : [];
        cache.at = Date.now();
        return cache.items;
      })
      .finally(() => { cache.pending = null; });
  }
  return cache.pending;
}

function analysisPositionInvalidate() {
  _analysisPositionCache = { at: 0, items: null, pending: null, user: null };
}

// 인사이트 모달 메모 섹션의 폴백 — 포트폴리오를 안 열었을 때 분석 화면이 받은 보유 행.
function analysisPositionCachedItem(code) {
  return _analysisPositionFind(_analysisPositionCache.items, code);
}

function _analysisPositionClear(host) {
  host.hidden = true;
  host.innerHTML = '';
  host.dataset.code = '';
  host._apItem = null;
}

function _analysisPositionFind(items, code) {
  const c = String(code || '').toUpperCase();
  return (items || []).find(it => String(it.stock_code || '').toUpperCase() === c && Number(it.quantity)) || null;
}

function _analysisPositionPrice() {
  const q = typeof activeQuoteSnapshot !== 'undefined' ? activeQuoteSnapshot : null;
  const p = q && q.price !== null && q.price !== undefined ? Number(q.price) : NaN;
  return Number.isFinite(p) && p > 0 ? p : null;
}

// 보유 표와 같은 목표가(수식·우선주·지주사·자동)를 쓴다 — portfolio-actions.js _computeTargetPrice.
function _analysisPositionTarget(item, items) {
  if (typeof _computeTargetPrice === 'function') {
    try {
      const v = _computeTargetPrice(item, items || [item]);
      return v !== null && Number.isFinite(Number(v)) && Number(v) > 0 ? Number(v) : null;
    } catch (e) { /* 수식 변수 부족 등은 저장값으로 폴백 */ }
  }
  const t = Number(item.target_price);
  return !item.target_price_disabled && Number.isFinite(t) && t > 0 ? t : null;
}

function analysisPositionHtml(item, price, items = null) {
  const qty = Number(item.quantity);
  const avg = Number.isFinite(Number(item.avg_price_krw)) && Number(item.avg_price_krw) > 0
    ? Number(item.avg_price_krw) : Number(item.avg_price);
  const isShort = qty < 0;
  const hasPrice = price !== null && Number.isFinite(avg) && avg > 0;
  const pnl = hasPrice ? (price - avg) * qty : null;
  const ret = hasPrice ? ((price / avg) - 1) * 100 * (isShort ? -1 : 1) : null;
  const value = price !== null ? price * Math.abs(qty) : null;
  const target = _analysisPositionTarget(item, items);
  const hasTarget = target !== null;
  const upside = hasTarget && price !== null ? ((target / price) - 1) * 100 : null;
  const cls = (v) => (v === null ? '' : v > 0 ? ' up' : v < 0 ? ' down' : '');
  const stat = (label, val, extra = '') => `<div class="ap-stat"><span>${label}</span><strong class="${extra}">${val}</strong></div>`;
  const memo = String(item.memo || '');
  return `<div class="ap-head">
      <span class="ap-kicker">내 포지션${isShort ? ' · 숏' : ''}</span>
      ${item.group_name ? `<span class="ap-group">${escapeHtml(item.group_name)}</span>` : ''}
    </div>
    <div class="ap-stats">
      ${stat('수량', fmtNum(Math.abs(qty)))}
      ${stat('평단', Number.isFinite(avg) && avg > 0 ? `${fmtNum(Math.round(avg))}원` : '-')}
      ${stat('평가액', value !== null ? `${fmtNum(Math.round(value))}원` : '-')}
      ${stat('평가손익', pnl !== null ? `${pnl > 0 ? '+' : ''}${fmtNum(Math.round(pnl))}원` : '-', cls(pnl))}
      ${stat('수익률', ret !== null ? fmtPct(ret) : '-', cls(ret))}
      ${stat('목표가', hasTarget ? `${fmtNum(Math.round(target))}원${upside !== null ? ` <small>(${fmtPct(upside, true, 1)})</small>` : ''}` : '-')}
    </div>
    <button type="button" class="ap-memo js-ap-memo" title="보유 메모 편집">
      <span class="ap-memo-label">📝 보유 메모</span>
      <span class="ap-memo-text${memo ? '' : ' is-empty'}">${memo ? escapeHtml(memo) : '메모 추가 — 매수 이유, 확인할 것'}</span>
    </button>
    <div class="ap-actions">
      <button type="button" class="ap-btn js-ap-focus">포트폴리오에서 보기</button>
      <button type="button" class="ap-btn js-ap-insight">투자 인사이트</button>
    </div>`;
}

async function renderAnalysisPosition(code = (typeof activeStockCode !== 'undefined' ? activeStockCode : null)) {
  const host = document.getElementById('analysisPositionStrip');
  if (!host) return;
  const seq = ++_analysisPositionSeq;
  const userKey = _analysisPositionUserKey();
  // 종목이 바뀌면 이전 종목 포지션을 즉시 지운다 — 응답을 기다리는 동안 섞여 보이지 않게.
  if (host.dataset.code && host.dataset.code !== String(code || '').toUpperCase()) _analysisPositionClear(host);
  if (!code || !userKey) {
    _analysisPositionClear(host);
    return;
  }
  let items;
  try {
    items = await _analysisPositionItems();
  } catch (e) {
    items = null;
  }
  if (seq !== _analysisPositionSeq || _analysisPositionUserKey() !== userKey) return;
  const item = _analysisPositionFind(items, code);
  if (!item) {
    _analysisPositionClear(host);
    return;
  }
  host.dataset.code = item.stock_code;
  host._apItem = item;
  host._apItems = items;
  host.innerHTML = analysisPositionHtml(item, _analysisPositionPrice(), items);
  host.hidden = false;
}

// 로그인·로그아웃·사용자 전환(auth.js renderAuthState) — 캐시를 버리고 다시 그린다.
let _analysisPositionLastAuthKey;
function analysisPositionAuthChanged() {
  const key = _analysisPositionUserKey();
  if (key === _analysisPositionLastAuthKey) return;
  _analysisPositionLastAuthKey = key;
  analysisPositionInvalidate();
  renderAnalysisPosition();
}

// 실시간 시세 tick — 보유 목록을 다시 받지 않고 가격만 바꿔 다시 그린다.
function analysisPositionRefreshPrice() {
  const host = document.getElementById('analysisPositionStrip');
  if (!host || host.hidden || !host._apItem) return;
  // 버튼에 포커스가 있으면(키보드 사용 중) tick 으로 DOM 을 갈아엎지 않는다.
  if (host.contains(document.activeElement)) return;
  host.innerHTML = analysisPositionHtml(host._apItem, _analysisPositionPrice(), host._apItems);
}

(function initAnalysisPosition() {
  document.addEventListener('click', (e) => {
    const host = e.target.closest?.('#analysisPositionStrip');
    if (!host) return;
    const code = host.dataset.code;
    if (!code) return;
    if (e.target.closest('.js-ap-focus')) {
      e.preventDefault();
      switchView('portfolio');
      if (typeof pfFocusHolding === 'function') pfFocusHolding(code);
    } else if (e.target.closest('.js-ap-insight')) {
      e.preventDefault();
      if (typeof pfOpenAssetInsight === 'function') pfOpenAssetInsight(code);
    } else if (e.target.closest('.js-ap-memo')) {
      e.preventDefault();
      if (typeof pfOpenMemoEditor === 'function') pfOpenMemoEditor(code, e.target.closest('.js-ap-memo'), host._apItem);
    }
  });
  // 포트폴리오 쪽에서 메모를 고쳐도 분석 화면이 같은 값을 보이게 한다.
  document.addEventListener('pf:memo-saved', (e) => {
    const { code, memo } = e.detail || {};
    for (const it of _analysisPositionCache.items || []) if (it.stock_code === code) it.memo = memo;
    const host = document.getElementById('analysisPositionStrip');
    if (host && host.dataset.code === code) {
      if (host._apItem) host._apItem.memo = memo;
      renderAnalysisPosition(code);
    }
  });
})();
