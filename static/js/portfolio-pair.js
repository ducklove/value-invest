// 롱숏 설정/해제와 모든 보유 행의 성과 툴팁.
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

let _pfPerformanceTooltipState = null;

function pfClosePerformanceTooltip() {
  _pfPerformanceTooltipState?.trigger?.removeAttribute('aria-describedby');
  _pfPerformanceTooltipState = null;
  document.getElementById('pfPerformanceTooltip')?.remove();
}

function pfPerformanceTooltipHtml(item, metric) {
  const anchor = PfStore.accountId ? item.stock_code : pfPairAnchorCode(item);
  const longItem = PfStore.items.find(i => i.stock_code === anchor);
  const shorts = PfStore.accountId ? [] : pfPairShortsForLong(anchor);
  const paired = longItem && shorts.length;
  const stats = pfPairStats(paired ? longItem : item, paired ? shorts : []);
  const cumulative = metric === 'returnPct';
  const pct = cumulative
    ? stats.allPriced && stats.netInvested !== 0 ? stats.totalPnl / Math.abs(stats.netInvested) * 100 : null
    : paired ? stats.dailyChangePct : stats.netPreviousValue === null ? null : item.quote?.change_pct ?? null;
  const pnl = cumulative ? stats.totalPnl : stats.dailyPnl;
  const money = value => pfFmtSignedPortfolioValue(value) + (value !== null && PfStore.currency.unit !== 'USD' ? '원' : '');
  const line = (label, value, text) => `<div class="pf-tooltip-line"><span>${label}</span><strong class="${returnClass(value)}">${text}</strong></div>`;
  let content = line(paired ? `합산 ${cumulative ? '수익률' : '등락률'}` : cumulative ? '수익률' : '등락률', pct, pct === null ? '-' : fmtPct(pct));
  if (!paired && !cumulative) {
    const change = pfHoldingDailyStats(item).change;
    content += line('등락액', change, money(change));
  }
  content += line(cumulative ? '평가손익' : '당일손익', pnl, money(pnl));
  const basis = cumulative ? '매입금액 대비' : paired ? '전일 순평가액 대비' : '전일 종가 대비 · 보유 수량 반영';
  const missing = !cumulative && stats.netPreviousValue === null;
  const zero = !cumulative && paired && Math.abs(stats.netPreviousValue) <= 1e-8;
  return `<div class="pf-tooltip-title">${stats.legs.map(leg => escapeHtml(leg.name || leg.code)).join(' + ')}</div>${content}
    <div class="pf-tooltip-note">${missing ? '전일 시세 확인 중' : zero ? '전일 순평가액이 0이라 등락률 계산 불가' : basis}</div>`;
}

function pfRefreshPerformanceTooltip() {
  const state = _pfPerformanceTooltipState;
  if (!state) return;
  if (state.accountId !== PfStore.accountId) { pfClosePerformanceTooltip(); return; }
  const item = PfStore.items.find(i => i.stock_code === state.code);
  const row = document.querySelector(`#pfBody tr[data-code="${CSS.escape(state.code)}"]`);
  const trigger = row?.querySelector(`.js-pf-performance-tooltip[data-metric="${state.metric}"]`);
  if (!item || !trigger || !trigger.getClientRects().length) { pfClosePerformanceTooltip(); return; }
  state.trigger = trigger;
  let tooltip = document.getElementById('pfPerformanceTooltip');
  if (!tooltip) {
    tooltip = document.createElement('div');
    tooltip.id = 'pfPerformanceTooltip';
    tooltip.className = 'pf-performance-tooltip';
    tooltip.setAttribute('role', 'tooltip');
    document.body.appendChild(tooltip);
  }
  trigger.setAttribute('aria-describedby', tooltip.id);
  tooltip.innerHTML = pfPerformanceTooltipHtml(item, state.metric);
  const rect = trigger.getBoundingClientRect();
  const width = tooltip.offsetWidth;
  const height = tooltip.offsetHeight;
  tooltip.style.left = `${Math.max(8, Math.min(rect.right - width, window.innerWidth - width - 8))}px`;
  const top = rect.bottom + height + 8 <= window.innerHeight ? rect.bottom + 6 : rect.top - height - 6;
  tooltip.style.top = `${Math.max(8, top)}px`;
}

function pfPreviewPerformanceTooltip(event, entering) {
  const trigger = event.target.closest?.('.js-pf-performance-tooltip');
  if (!trigger || trigger.contains(event.relatedTarget)) return;
  if (entering) {
    if (_pfPerformanceTooltipState?.trigger !== trigger) pfClosePerformanceTooltip();
    _pfPerformanceTooltipState = { code: trigger.dataset.code, metric: trigger.dataset.metric, trigger, accountId: PfStore.accountId };
    pfRefreshPerformanceTooltip();
  } else if (_pfPerformanceTooltipState?.trigger === trigger && document.activeElement !== trigger) {
    pfClosePerformanceTooltip();
  }
}

window.addEventListener('scroll', pfClosePerformanceTooltip, true);
window.addEventListener('resize', pfClosePerformanceTooltip);
