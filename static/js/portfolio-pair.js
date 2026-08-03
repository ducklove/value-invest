// 롱숏 페어 액션: 페어 설정/해제 API 호출과 순투자액 요약 팝오버.
// 순수 데이터 헬퍼(pfPairLongCode/pfPairStats/pfPairShortsForLong/칩 렌더러)는
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

// 페어 칩 클릭 → 순투자액 요약 팝오버. 롱 다리 + 그 롱을 가리키는 숏 다리
// 전부의 투자액/평가액과 순투자액·순평가액·합산 손익을 보여준다.
function pfShowPairSummary(longCode, e) {
  const longItem = PfStore.items.find(i => i.stock_code === longCode);
  const shorts = pfPairShortsForLong(longCode);
  if (!longItem || !shorts.length) return;
  const stats = pfPairStats(longItem, shorts);
  document.querySelectorAll('.pf-pref-menu').forEach(el => el.remove());
  const fmtVal = v => (v === null || v === undefined) ? '-' : pfFmtPortfolioValue(v);
  const menu = document.createElement('div');
  menu.className = 'pf-pref-menu pf-pair-menu';
  const legRows = stats.legs.map(leg => `
    <div class="pf-pair-row">
      <span class="pf-pair-name">${escapeHtml(leg.name || leg.code)} <em>${leg.qty < 0 ? '숏' : '롱'}</em></span>
      <span class="pf-pair-nums">투자 ${fmtVal(leg.invested)} · 평가 ${fmtVal(leg.marketValue)}</span>
    </div>`).join('');
  menu.innerHTML = `
    <div class="pf-pair-title">롱숏 페어</div>
    ${legRows}
    <div class="pf-pair-total">
      <div><span>순투자액</span><strong>${fmtVal(stats.netInvested)}</strong></div>
      <div><span>순평가액</span><strong>${fmtVal(stats.netMarketValue)}</strong></div>
      <div><span>합산 손익</span><strong class="${returnClass(stats.totalPnl)}">${stats.totalPnl === null ? '-' : fmtSignedKrw(stats.totalPnl)}</strong></div>
    </div>`;
  document.body.appendChild(menu);
  _positionPortfolioPopupMenu(menu, e);
  setTimeout(() => {
    document.addEventListener('click', function close(ev) {
      if (!menu.contains(ev.target)) { menu.remove(); document.removeEventListener('click', close); }
    });
  }, 0);
}
