// Portfolio manual row ordering: drag placement, optimistic order, and save queue.
// File-local: timer that keeps an optimistic manual order alive briefly after
// a failed save (only used here; cross-file order state is PfStore.manualOrder).
let pfManualOrderKeepTimer = null;

function _pfSameOrderCodes(a, b) {
  if (!Array.isArray(a) || !Array.isArray(b) || a.length !== b.length) return false;
  return a.every((code, index) => String(code || '') === String(b[index] || ''));
}

function _pfSetPortfolioSortOrder(orderCodes) {
  if (!Array.isArray(orderCodes) || !orderCodes.length) return;
  const rank = new Map();
  orderCodes.forEach((code, index) => rank.set(String(code || '').trim(), index));
  PfStore.items.forEach(item => {
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
  const next = _pfNextOrderAfterDrop(PfStore.items, fromCode, toCode, dropPosition);
  if (!next) return;
  const orderCodes = next.map(i => i.stock_code);
  PfStore.manualOrder.pendingCodes = orderCodes;
  PfStore.manualOrder.revision += 1;
  if (pfManualOrderKeepTimer) {
    clearTimeout(pfManualOrderKeepTimer);
    pfManualOrderKeepTimer = null;
  }
  PfStore.items = pfApplyManualOrder(next, orderCodes);
  _pfSetPortfolioSortOrder(orderCodes);
  _savePortfolioSnapshot(PfStore.items);
  renderPortfolio();
  void pfFlushManualOrderSave();
}

async function pfFlushManualOrderSave() {
  if (PfStore.manualOrder.saveInFlight) return;
  PfStore.manualOrder.saveInFlight = true;
  try {
    while (PfStore.manualOrder.pendingCodes && PfStore.manualOrder.pendingCodes.length) {
      const orderCodes = PfStore.manualOrder.pendingCodes.slice();
      const orderRevision = PfStore.manualOrder.revision;
      try {
        await apiFetchJson('/api/portfolio/order', {
          method: 'PUT',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ stock_codes: orderCodes }),
          errorMessage: 'Portfolio order save failed',
        });
      } catch (e) {
        if (_pfSameOrderCodes(PfStore.manualOrder.pendingCodes, orderCodes) && PfStore.manualOrder.revision === orderRevision) {
          PfStore.manualOrder.pendingCodes = null;
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
      if (!_pfSameOrderCodes(PfStore.manualOrder.pendingCodes, orderCodes) || PfStore.manualOrder.revision !== orderRevision) {
        continue;
      }
      PfStore.items = pfApplyManualOrder(PfStore.items, orderCodes);
      _pfSetPortfolioSortOrder(orderCodes);
      _savePortfolioSnapshot(PfStore.items);
      renderPortfolio();
      const savedCodes = orderCodes.slice();
      pfManualOrderKeepTimer = setTimeout(() => {
        if (_pfSameOrderCodes(PfStore.manualOrder.pendingCodes, savedCodes)) PfStore.manualOrder.pendingCodes = null;
        pfManualOrderKeepTimer = null;
      }, 5000);
      return;
    }
  } finally {
    PfStore.manualOrder.saveInFlight = false;
  }
}
