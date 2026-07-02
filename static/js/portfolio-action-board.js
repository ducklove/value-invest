// Portfolio action board: today's review queue + linked-project signals.
// Data source: GET /api/portfolio/action-board.
const PfActionBoard = {
  data: null,
  signalsByCode: {},
  loadingSeq: 0,
  enabled: false,
};
const PF_ACTION_BOARD_ENABLED_KEY = 'valueInvest.portfolio.actionBoard.enabled';

if (typeof window !== 'undefined') window.PfActionBoard = PfActionBoard;

function _pfActionBoardEl() { return document.getElementById('pfActionBoard'); }
function _pfActionBoardContentEl() { return document.getElementById('pfActionBoardContent'); }
function _pfActionBoardToggleEl() { return document.getElementById('pfActionBoardToggle'); }

function pfActionBoardIsEnabled() {
  return !!PfActionBoard.enabled;
}

function _pfActionBoardReadEnabled() {
  try {
    return localStorage.getItem(PF_ACTION_BOARD_ENABLED_KEY) === '1';
  } catch (e) {
    console.warn(e);
    return false;
  }
}

function _pfActionBoardPersistEnabled(enabled) {
  try {
    localStorage.setItem(PF_ACTION_BOARD_ENABLED_KEY, enabled ? '1' : '0');
  } catch (e) {
    console.warn(e);
  }
}

function _pfActionBoardSetToggleState(enabled) {
  const toggle = _pfActionBoardToggleEl();
  if (!toggle) return;
  toggle.setAttribute('aria-pressed', enabled ? 'true' : 'false');
  toggle.classList.toggle('active', enabled);
  toggle.title = enabled ? '오늘의 투자 액션 보드 숨기기' : '오늘의 투자 액션 보드 표시';
}

function _pfActionBoardClear() {
  const board = _pfActionBoardEl();
  const content = _pfActionBoardContentEl();
  const meta = document.getElementById('pfActionBoardMeta');
  PfActionBoard.data = null;
  PfActionBoard.signalsByCode = {};
  if (board) board.hidden = true;
  if (meta) meta.textContent = '액션 버튼을 누르면 확인할 항목을 불러옵니다.';
  if (content) content.innerHTML = '<div class="pf-risk-empty">액션 보드를 불러오는 중입니다.</div>';
}

function _pfActionBoardApplyEnabled(enabled, { persist = true, load = true } = {}) {
  PfActionBoard.enabled = !!enabled;
  _pfActionBoardSetToggleState(PfActionBoard.enabled);
  if (persist) _pfActionBoardPersistEnabled(PfActionBoard.enabled);
  if (!PfActionBoard.enabled) {
    PfActionBoard.loadingSeq += 1;
    _pfActionBoardClear();
    if (typeof renderPortfolio === 'function' && PfStore.items.length) renderPortfolio();
    return;
  }
  if (load) void pfLoadActionBoard({ force: false });
}

function pfToggleActionBoard(enabled) {
  const nextEnabled = typeof enabled === 'boolean' ? enabled : !PfActionBoard.enabled;
  _pfActionBoardApplyEnabled(nextEnabled);
}

function _pfActionSeverityLabel(severity) {
  if (severity === 'high') return '중요';
  if (severity === 'watch') return '확인';
  return '정보';
}

function _pfActionStatusLabel(status) {
  if (status === 'done') return '처리완료';
  if (status === 'dismissed') return '무시';
  return '검토';
}

function _pfActionMsg(message) {
  if (!pfActionBoardIsEnabled()) return;
  const board = _pfActionBoardEl();
  const el = _pfActionBoardContentEl();
  if (board) board.hidden = false;
  if (el) el.innerHTML = `<div class="pf-risk-empty">${escapeHtml(message)}</div>`;
}

function _pfActionBuildSignalsByCode(data) {
  const byCode = {};
  (data?.signals || []).forEach((signal) => {
    const code = String(signal.stock_code || '').trim().toUpperCase();
    if (!code) return;
    if (!byCode[code]) byCode[code] = [];
    byCode[code].push(signal);
  });
  PfActionBoard.signalsByCode = byCode;
}

function pfActionBoardBadgesForCode(code) {
  if (!pfActionBoardIsEnabled()) return '';
  const signals = PfActionBoard.signalsByCode[String(code || '').trim().toUpperCase()] || [];
  if (!signals.length) return '';
  const shown = signals.slice(0, 3);
  const badges = shown.map((signal) => {
    const label = signal.short_label || signal.kind || '신호';
    const title = [signal.title, signal.detail].filter(Boolean).join(' · ');
    const href = signal.url ? ` href="${escapeHtml(signal.url)}" target="_blank" rel="noopener noreferrer"` : '';
    return `<a class="pf-linked-signal-badge ${escapeHtml(signal.severity || 'info')}"${href} title="${escapeHtml(title)}">${escapeHtml(label)}</a>`;
  }).join('');
  const more = signals.length > shown.length
    ? `<span class="pf-linked-signal-badge more" title="추가 신호 ${signals.length - shown.length}건">+${signals.length - shown.length}</span>`
    : '';
  return `<span class="pf-linked-signal-badges">${badges}${more}</span>`;
}

function _pfActionCardHtml(item) {
  const key = escapeHtml(item.key || '');
  const severity = escapeHtml(item.severity || 'info');
  const status = item.status || 'open';
  const source = item.source ? `<span class="pf-action-source">${escapeHtml(item.source)}</span>` : '';
  const stock = item.stock_name || item.stock_code
    ? `<span class="pf-action-stock">${escapeHtml(item.stock_name || item.stock_code)}</span>`
    : '';
  const href = item.url || '';
  const openBtn = href
    ? `<a class="pf-action-btn" href="${escapeHtml(href)}" target="${href.startsWith('#') ? '_self' : '_blank'}" rel="noopener noreferrer" data-pf-action-open="1">열기</a>`
    : '';
  const reviewBtns = status === 'open'
    ? `<button class="pf-action-btn primary" type="button" data-pf-action-key="${key}" data-pf-action-status="done">완료</button>
       <button class="pf-action-btn" type="button" data-pf-action-key="${key}" data-pf-action-status="dismissed">무시</button>`
    : `<button class="pf-action-btn" type="button" data-pf-action-key="${key}" data-pf-action-status="open">다시 검토</button>`;
  return `<article class="pf-action-card ${severity} ${escapeHtml(status)}" data-action-key="${key}">
    <div class="pf-action-card-main">
      <div class="pf-action-card-top">
        <span class="pf-action-severity">${_pfActionSeverityLabel(item.severity)}</span>
        ${source}${stock}
        <span class="pf-action-status">${_pfActionStatusLabel(status)}</span>
      </div>
      <h4>${escapeHtml(item.title || '확인할 항목')}</h4>
      <p>${escapeHtml(item.detail || '')}</p>
    </div>
    <div class="pf-action-card-actions">${openBtn}${reviewBtns}</div>
  </article>`;
}

function _pfRenderActionBoard(data) {
  if (!pfActionBoardIsEnabled()) return;
  const board = _pfActionBoardEl();
  const content = _pfActionBoardContentEl();
  const meta = document.getElementById('pfActionBoardMeta');
  if (!board || !content) return;
  board.hidden = false;

  const summary = data?.summary || {};
  const actions = Array.isArray(data?.actions) ? data.actions : [];
  const queue = Array.isArray(data?.queue) ? data.queue : [];
  const resolved = actions.filter((item) => item.status !== 'open').slice(0, 4);
  const metaParts = [
    `검토 ${Number(summary.open_count || 0)}건`,
    `리밸런싱 ${Number(summary.rebalance_breaches || 0)}건`,
    `연결 신호 ${Number(summary.signal_count || 0)}건`,
  ];
  if (data?.as_of) metaParts.push(`스냅샷 ${data.as_of}`);
  if (meta) meta.textContent = metaParts.join(' · ');

  if (!actions.length) {
    content.innerHTML = '<div class="pf-risk-empty">오늘 확인할 투자 액션이 없습니다.</div>';
    return;
  }

  const openHtml = queue.length
    ? queue.map(_pfActionCardHtml).join('')
    : '<div class="pf-risk-empty">검토 큐가 비어 있습니다. 처리한 항목은 아래에 남습니다.</div>';
  const resolvedHtml = resolved.length
    ? `<details class="pf-action-resolved"><summary>처리한 항목 ${Number(summary.resolved_count || resolved.length)}건</summary>${resolved.map(_pfActionCardHtml).join('')}</details>`
    : '';
  content.innerHTML = `<div class="pf-action-summary">
      <span>전체 ${Number(summary.action_count || actions.length)}건</span>
      <span>검토 ${Number(summary.open_count || queue.length)}건</span>
      <span>연결 신호 ${Number(summary.signal_count || 0)}건</span>
    </div>
    <div class="pf-action-list">${openHtml}</div>
    ${resolvedHtml}`;
}

async function pfLoadActionBoard({ force = false } = {}) {
  const board = _pfActionBoardEl();
  if (!board) return;
  if (!pfActionBoardIsEnabled()) {
    _pfActionBoardClear();
    return;
  }
  if (!force && PfActionBoard.data) {
    _pfRenderActionBoard(PfActionBoard.data);
    return;
  }
  const seq = ++PfActionBoard.loadingSeq;
  _pfActionMsg('오늘의 투자 액션을 불러오는 중입니다...');
  try {
    const data = await apiFetchJson('/api/portfolio/action-board', {
      cache: 'no-store',
      errorMessage: '액션 보드 요청 실패',
    });
    if (seq !== PfActionBoard.loadingSeq) return;
    PfActionBoard.data = data;
    _pfActionBuildSignalsByCode(data);
    _pfRenderActionBoard(data);
    if (typeof renderPortfolio === 'function' && PfStore.items.length) renderPortfolio();
  } catch (e) {
    if (e?.status === 401) {
      board.hidden = true;
      return;
    }
    reportApiError(e, '액션 보드', { silent: true });
    if (seq === PfActionBoard.loadingSeq) {
      _pfActionMsg('액션 보드를 불러오지 못했습니다. 잠시 후 다시 시도해 주세요.');
    }
  }
}

async function pfActionBoardSetStatus(actionKey, status) {
  if (!pfActionBoardIsEnabled()) return;
  try {
    const data = await apiFetchJson(`/api/portfolio/action-board/queue/${encodeURIComponent(actionKey)}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ status }),
      errorMessage: '액션 검토 상태를 변경하지 못했습니다.',
    });
    if (PfActionBoard.data && Array.isArray(PfActionBoard.data.actions)) {
      PfActionBoard.data.actions = PfActionBoard.data.actions.map((item) => (
        item.key === actionKey ? { ...item, status, review: data.review || item.review } : item
      ));
      PfActionBoard.data.queue = PfActionBoard.data.actions.filter((item) => item.status === 'open');
      PfActionBoard.data.summary = {
        ...(PfActionBoard.data.summary || {}),
        open_count: PfActionBoard.data.queue.length,
        resolved_count: PfActionBoard.data.actions.length - PfActionBoard.data.queue.length,
      };
      _pfRenderActionBoard(PfActionBoard.data);
    } else {
      await pfLoadActionBoard({ force: true });
    }
  } catch (e) {
    reportApiError(e, '액션 검토 상태 변경');
  }
}

(function initPfActionBoard() {
  const onReady = () => {
    _pfActionBoardApplyEnabled(_pfActionBoardReadEnabled(), { persist: false, load: false });
    const refresh = document.getElementById('pfActionBoardRefresh');
    if (refresh) refresh.addEventListener('click', () => pfLoadActionBoard({ force: true }));
    const hide = document.getElementById('pfActionBoardHide');
    if (hide) hide.addEventListener('click', () => pfToggleActionBoard(false));
    document.addEventListener('click', (e) => {
      const badge = e.target.closest && e.target.closest('.pf-linked-signal-badge');
      if (badge) e.stopPropagation();
      const open = e.target.closest && e.target.closest('[data-pf-action-open]');
      if (open && open.getAttribute('href') === '#pfRebalanceWrap') {
        e.preventDefault();
        if (typeof pfSwitchTab === 'function') pfSwitchTab('performance');
        document.getElementById('pfRebalanceWrap')?.scrollIntoView({ behavior: 'smooth', block: 'start' });
        return;
      }
      const btn = e.target.closest && e.target.closest('[data-pf-action-key][data-pf-action-status]');
      if (!btn) return;
      e.preventDefault();
      pfActionBoardSetStatus(btn.dataset.pfActionKey || '', btn.dataset.pfActionStatus || 'open');
    });
  };
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', onReady);
  } else {
    onReady();
  }
})();

if (typeof window !== 'undefined') {
  Object.assign(window, {
    pfLoadActionBoard,
    pfActionBoardSetStatus,
    pfActionBoardBadgesForCode,
    pfActionBoardIsEnabled,
    pfToggleActionBoard,
  });
}
