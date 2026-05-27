// Portfolio manual row ordering: drag placement, optimistic order, and save queue.
function _pfSameOrderCodes(a, b) {
  if (!Array.isArray(a) || !Array.isArray(b) || a.length !== b.length) return false;
  return a.every((code, index) => String(code || '') === String(b[index] || ''));
}

function _pfSetPortfolioSortOrder(orderCodes) {
  if (!Array.isArray(orderCodes) || !orderCodes.length) return;
  const rank = new Map();
  orderCodes.forEach((code, index) => rank.set(String(code || '').trim(), index));
  portfolioItems.forEach(item => {
    if (rank.has(item.stock_code)) item.sort_order = rank.get(item.stock_code);
  });
}

function _pfNextOrderAfterDrop(items, fromCode, toCode, dropPosition = 'before') {
  const fromIdx = items.findIndex(i => i.stock_code === fromCode);
  if (fromIdx < 0 || !toCode || fromCode === toCode) return null;
  const next = items.slice();
  const [moved] = next.splice(fromIdx, 1);
  const targetIdx = next.findIndex(i => i.stock_code === toCode);
  if (targetIdx < 0) return null;
  const insertIdx = dropPosition === 'after' ? targetIdx + 1 : targetIdx;
  next.splice(insertIdx, 0, moved);
  return next;
}

function _pfClearPortfolioDragOver(root = document) {
  root.querySelectorAll('.drag-over-before, .drag-over-after').forEach(el => {
    el.classList.remove('drag-over-before', 'drag-over-after');
  });
}

function _pfDropPositionForEvent(e, row) {
  const rect = row.getBoundingClientRect();
  return e.clientY > rect.top + rect.height / 2 ? 'after' : 'before';
}

async function pfDropRow(fromCode, toCode, dropPosition = 'before') {
  const next = _pfNextOrderAfterDrop(portfolioItems, fromCode, toCode, dropPosition);
  if (!next) return;
  const orderCodes = next.map(i => i.stock_code);
  pfPendingManualOrderCodes = orderCodes;
  pfManualOrderRevision += 1;
  if (pfManualOrderKeepTimer) {
    clearTimeout(pfManualOrderKeepTimer);
    pfManualOrderKeepTimer = null;
  }
  portfolioItems = pfApplyManualOrder(next, orderCodes);
  _pfSetPortfolioSortOrder(orderCodes);
  _savePortfolioSnapshot(portfolioItems);
  renderPortfolio();
  void pfFlushManualOrderSave();
}

async function pfFlushManualOrderSave() {
  if (pfManualOrderSaveInFlight) return;
  pfManualOrderSaveInFlight = true;
  try {
    while (pfPendingManualOrderCodes && pfPendingManualOrderCodes.length) {
      const orderCodes = pfPendingManualOrderCodes.slice();
      const orderRevision = pfManualOrderRevision;
      try {
        const resp = await apiFetch('/api/portfolio/order', {
          method: 'PUT',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ stock_codes: orderCodes }),
        });
        if (!resp.ok) {
          const data = await resp.json().catch(() => ({}));
          throw new Error(data.detail || 'Portfolio order save failed');
        }
      } catch (e) {
        if (_pfSameOrderCodes(pfPendingManualOrderCodes, orderCodes) && pfManualOrderRevision === orderRevision) {
          pfPendingManualOrderCodes = null;
          if (pfManualOrderKeepTimer) {
            clearTimeout(pfManualOrderKeepTimer);
            pfManualOrderKeepTimer = null;
          }
          showToast(e.message || 'Portfolio order save failed');
          await loadPortfolio({ force: true });
          return;
        }
        continue;
      }
      if (!_pfSameOrderCodes(pfPendingManualOrderCodes, orderCodes) || pfManualOrderRevision !== orderRevision) {
        continue;
      }
      portfolioItems = pfApplyManualOrder(portfolioItems, orderCodes);
      _pfSetPortfolioSortOrder(orderCodes);
      _savePortfolioSnapshot(portfolioItems);
      renderPortfolio();
      const savedCodes = orderCodes.slice();
      pfManualOrderKeepTimer = setTimeout(() => {
        if (_pfSameOrderCodes(pfPendingManualOrderCodes, savedCodes)) pfPendingManualOrderCodes = null;
        pfManualOrderKeepTimer = null;
      }, 5000);
      return;
    }
  } finally {
    pfManualOrderSaveInFlight = false;
  }
}
