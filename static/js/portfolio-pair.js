// 롱숏 페어 액션: 페어 설정/해제 API 호출과 합산 등락률 팝오버.
// 순수 데이터 헬퍼(pfPairLongCode/pfPairStats/pfPairShortsForLong)는
// portfolio-data.js 에 있다. 이 파일은 portfolio-actions.js 의 유지보수 상한
// (1,000줄)을 지키기 위해 분리된 페어 전용 액션 홈.
async function pfChangePair(stockCode, longCode) {
  const item = PfStore.items.find(i => i.stock_code === stockCode);
  if (!item) return;
  try {
    const data = await apiFetchJson(`/api/portfolio/${encodeURIComponent(stockCode)}/pair`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ long_code: longCode || null }),
      errorMessage: '롱숏 페어 설정에 실패했습니다.',
    });
    item.pair_long_code = data.pair_long_code || null;
    if (data.group_name) item.group_name = data.group_name;
    // 페어 설정 시 서버가 숏의 태그를 제거한다 — 로컬 상태도 맞춘다.
    if (item.pair_long_code) item.tags = [];
    renderPortfolio();
    showToast(item.pair_long_code ? '롱숏 페어를 설정했습니다.' : '롱숏 페어를 해제했습니다.', 'success');
  } catch (e) { reportApiError(e, '롱숏 페어'); }
}

let _pfPairSummaryAnchor = null;

function pfClosePairSummary(restoreFocus = false) {
  document.getElementById('pfPairSummary')?.remove();
  if (restoreFocus && _pfPairSummaryAnchor) {
    const row = document.querySelector(`#pfBody tr[data-code="${CSS.escape(_pfPairSummaryAnchor)}"]`);
    row?.querySelector('.js-pf-open-pair-summary')?.focus({ preventScroll: true });
  }
  _pfPairSummaryAnchor = null;
}

function pfRefreshPairSummary() {
  const menu = document.getElementById('pfPairSummary');
  if (!menu) return;
  const longItem = PfStore.items.find(i => i.stock_code === menu.dataset.longCode);
  const shorts = pfPairShortsForLong(menu.dataset.longCode);
  if (PfStore.accountId || !longItem || !shorts.length) { pfClosePairSummary(); return; }
  const stats = pfPairStats(longItem, shorts);
  const pnl = stats.dailyPnl;
  const pnlText = pnl === null ? '-' : `${pnl > 0 ? '+' : pnl < 0 ? '-' : ''}${pfFmtPortfolioValue(Math.abs(pnl))}`;
  const basis = stats.netPreviousValue === null ? '전일 시세 확인 중'
    : Math.abs(stats.netPreviousValue) <= 1e-8 ? '전일 순평가액이 0이라 등락률 계산 불가'
    : '전일 순평가액 대비';
  menu.innerHTML = `
    <div class="pf-pair-title">${stats.legs.map(leg => escapeHtml(leg.name || leg.code)).join(' + ')}</div>
    <div class="pf-pair-label">합산 등락률</div>
    <div class="pf-pair-value ${returnClass(stats.dailyChangePct)}">${stats.dailyChangePct === null ? '-' : fmtPct(stats.dailyChangePct)}</div>
    <div class="pf-pair-pnl ${returnClass(pnl)}">당일 합산 손익 ${pnlText}</div>
    <div class="pf-pair-basis" title="합산 손익 ÷ 전일 순평가액의 절댓값 × 100">${basis}</div>`;
}

// 기존 등락률 클릭 → 롱과 연결된 숏 전체의 일간 합산 성과.
function pfShowPairSummary(longCode, e) {
  if (PfStore.accountId) return;
  const longItem = PfStore.items.find(i => i.stock_code === longCode);
  const shorts = pfPairShortsForLong(longCode);
  if (!longItem || !shorts.length) return;
  pfClosePairSummary();
  document.querySelectorAll('.pf-pref-menu').forEach(el => el.remove());
  const menu = document.createElement('div');
  menu.id = 'pfPairSummary';
  menu.className = 'pf-pref-menu pf-pair-menu';
  menu.dataset.longCode = longCode;
  menu.setAttribute('role', 'dialog');
  menu.setAttribute('aria-label', '롱·숏 합산 등락률');
  menu.tabIndex = -1;
  _pfPairSummaryAnchor = e?.target?.closest('tr[data-code]')?.dataset.code || longCode;
  document.body.appendChild(menu);
  pfRefreshPairSummary();
  _positionPortfolioPopupMenu(menu, e);
  menu.focus({ preventScroll: true });
}

document.addEventListener('click', e => {
  if (!e.target.closest('#pfPairSummary, .js-pf-open-pair-summary')) pfClosePairSummary();
});
document.addEventListener('keydown', e => {
  if (e.key === 'Escape' && document.getElementById('pfPairSummary')) {
    e.preventDefault();
    pfClosePairSummary(true);
  }
});
