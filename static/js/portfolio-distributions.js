// 분배금 출금은 좌수를 줄이지 않는다. 미리보기·중복 방지·응답 유실 복구를 제공한다.
const _pfDistribution = { balances: [], preview: null, payload: null, pending: null, busy: false, generation: 0, dirty: false };
const _pfDistributionEl = id => document.getElementById(`pfDistribution${id}`);
const _pfDistributionFmt = value => Number(value).toLocaleString('ko-KR', { maximumFractionDigits: 8 });

function _pfDistributionKey() {
  const user = typeof currentUser === 'undefined' ? null : currentUser;
  return `value-invest:pending-distribution:${user?.google_sub || user?.email || 'session'}`;
}

function _pfDistributionPersist(value) {
  _pfDistribution.pending = value;
  try {
    if (value) sessionStorage.setItem(_pfDistributionKey(), JSON.stringify(value));
    else sessionStorage.removeItem(_pfDistributionKey());
  } catch { /* 현재 화면에서는 요청 번호를 유지한다. */ }
}

function _pfDistributionInvalidate() {
  if (_pfDistribution.pending || _pfDistribution.busy) return;
  _pfDistribution.preview = null;
  _pfDistribution.payload = null;
  _pfDistributionEl('Preview').textContent = '';
  _pfDistributionEl('Status').textContent = '';
  _pfDistributionEl('Save').disabled = true;
  _pfDistributionEl('Save').textContent = '분배금 출금 저장';
}

function _pfDistributionCurrency(fill = true) {
  const currency = _pfDistributionEl('Currency').value;
  const row = _pfDistribution.balances.find(r => r.currency === currency);
  const cash = PfStore.items.find(r => r.stock_code === `CASH_${currency}`);
  _pfDistributionEl('Balance').textContent = `수취 ${_pfDistributionFmt(row?.net_amount || 0)} / 분배 ${_pfDistributionFmt(row?.distributed_amount || 0)} / 미분배 ${_pfDistributionFmt(row?.available_amount || 0)} ${currency} · 현재 현금 ${_pfDistributionFmt(cash?.quantity || 0)} ${currency}`;
  _pfDistributionEl('FxLabel').hidden = currency === 'KRW';
  _pfDistributionEl('Fx').required = currency !== 'KRW';
  _pfDistributionEl('FxText').textContent = `분배 환율: 1 ${currency} = ? KRW`;
  if (fill) {
    _pfDistributionEl('Amount').value = row?.available_amount > 0 ? row.available_amount : '';
    _pfDistributionEl('Fx').value = currency === 'USD' && PfStore.currency?.fxRate > 0 ? PfStore.currency.fxRate : '';
  }
}

function _pfDistributionRead() {
  return { currency: _pfDistributionEl('Currency').value, amount: _pfDistributionEl('Amount').value,
    fx_rate: _pfDistributionEl('FxLabel').hidden ? null : _pfDistributionEl('Fx').value || null,
    memo: _pfDistributionEl('Memo').value.trim() };
}

function _pfDistributionPreview(result) {
  const fmt = _pfDistributionFmt;
  _pfDistributionEl('Preview').innerHTML = `<strong>분배금 ${fmt(result.amount)} ${escapeHtml(result.currency)} 출금</strong><dl>
    <dt>현금 잔고</dt><dd>${fmt(result.cash_before)} → ${fmt(result.cash_after)} ${escapeHtml(result.currency)}</dd>
    <dt>미분배 배당금</dt><dd>${fmt(result.available_before)} → ${fmt(result.available_after)} ${escapeHtml(result.currency)}</dd>
    <dt>좌수 변동</dt><dd>0 · 기존 좌수 유지</dd></dl><p class="pf-trade-help">오늘 현금에서 차감합니다. NAV 정산 시 좌당 분배금을 기록하고, 차트와 수익률은 분배금을 재투자한 기준으로 계산합니다.</p>`;
}

async function pfPreviewDistribution(event) {
  event?.preventDefault();
  if (_pfDistribution.busy || _pfDistribution.pending) return;
  _pfDistributionInvalidate();
  _pfDistribution.dirty = true;
  try {
    const payload = _pfDistributionRead();
    _pfDistribution.busy = true;
    _pfDistributionEl('Fields').disabled = true;
    const result = await apiFetchJson('/api/portfolio/distributions/preview', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload), errorMessage: '분배 내용을 확인하지 못했습니다.' });
    _pfDistribution.preview = result;
    _pfDistribution.payload = payload;
    _pfDistributionPreview(result);
    _pfDistributionEl('Save').disabled = false;
  } catch (error) { _pfDistributionEl('Status').textContent = error.message; }
  finally { _pfDistribution.busy = false; _pfDistributionEl('Fields').disabled = false; }
}

async function pfLoadDistributions() {
  try {
    const rows = await apiFetchJson('/api/portfolio/distributions?limit=20', { errorMessage: '분배 내역을 불러오지 못했습니다.' });
    _pfDistributionEl('History').innerHTML = rows.length ? rows.map(row => `<article><strong>${escapeHtml(row.date)} · ${_pfDistributionFmt(row.amount)} ${escapeHtml(row.currency)}</strong><p>좌수 유지 · 미분배 잔액 ${_pfDistributionFmt(row.available_after)} ${escapeHtml(row.currency)}</p><small>${escapeHtml(row.memo || '')}</small></article>`).join('') : '<p>아직 분배금 출금 내역이 없습니다.</p>';
  } catch (error) { _pfDistributionEl('History').textContent = error.message; }
}

async function pfSaveDistribution() {
  if (_pfDistribution.busy || (!_pfDistribution.preview && !_pfDistribution.pending)) return;
  if (!_pfDistribution.pending) {
    if (JSON.stringify(_pfDistributionRead()) !== JSON.stringify(_pfDistribution.payload)) { _pfDistributionInvalidate(); return; }
    _pfDistributionPersist({ ..._pfDistribution.payload, request_id: crypto.randomUUID(), expected_revision: _pfDistribution.preview.revision });
  }
  _pfDistribution.busy = true;
  _pfDistributionEl('Fields').disabled = true;
  _pfDistributionEl('Save').disabled = true;
  _pfDistributionEl('Status').textContent = '분배금 출금을 저장하고 있습니다…';
  let saved;
  try {
    saved = await apiFetchJson('/api/portfolio/distributions', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(_pfDistribution.pending), errorMessage: '저장 결과를 확인하지 못했습니다.' });
    if (!saved || !Number.isFinite(saved.cash_after) || !Number.isFinite(saved.available_after)) throw new Error('저장 응답을 확인하지 못했습니다.');
  } catch (error) {
    saved = null;
    if ([400, 401, 403, 409, 422].includes(error.status)) {
      _pfDistributionPersist(null);
      _pfDistribution.preview = null;
      _pfDistributionEl('Fields').disabled = false;
      _pfDistributionEl('Preview').textContent = '';
      _pfDistributionEl('Status').textContent = `${error.message} 분배 내용을 다시 확인해 주세요.`;
    } else {
      _pfDistributionEl('Status').textContent = '응답을 확인하지 못했습니다. 같은 요청으로 저장 결과를 확인하면 중복 출금되지 않습니다.';
      _pfDistributionEl('Save').disabled = false;
      _pfDistributionEl('Save').textContent = '저장 결과 확인';
    }
  } finally { _pfDistribution.busy = false; }
  if (!saved) return;
  _pfDistributionPersist(null);
  _pfDistribution.preview = null;
  _pfDistribution.payload = null;
  _pfDistributionEl('Fields').disabled = false;
  _pfDistributionEl('Amount').value = '';
  _pfDistributionEl('Save').textContent = '분배금 출금 저장';
  _pfDistributionPreview(saved);
  _pfDistributionEl('Status').textContent = '분배금 출금을 저장했습니다. 좌수는 유지됩니다.';
  const row = _pfDistribution.balances.find(r => r.currency === saved.currency);
  if (row) { row.available_amount = saved.available_after; row.distributed_amount += saved.amount; }
  await Promise.allSettled([loadPortfolio({ force: true }), pfLoadDistributions()]);
  _pfDistributionCurrency(false);
  if (typeof refreshPortfolioAfterCashflowMutation === 'function') await refreshPortfolioAfterCashflowMutation();
  if (typeof pfLoadDividendReceipts === 'function') await pfLoadDividendReceipts();
}

async function pfOpenDistribution() {
  if (_pfDistribution.busy) return;
  const generation = ++_pfDistribution.generation;
  _pfDistributionEl('Form').reset();
  _pfDistribution.dirty = false;
  _pfDistribution.preview = null;
  _pfDistribution.payload = null;
  _pfDistributionEl('Preview').textContent = '';
  _pfDistributionEl('Fields').disabled = false;
  try { _pfDistribution.pending = JSON.parse(sessionStorage.getItem(_pfDistributionKey()) || 'null'); } catch { /* 현재 화면의 요청 유지 */ }
  if (_pfDistribution.pending) {
    const p = _pfDistribution.pending;
    for (const [id, key] of [['Currency', 'currency'], ['Amount', 'amount'], ['Fx', 'fx_rate'], ['Memo', 'memo']]) _pfDistributionEl(id).value = p[key] ?? '';
    _pfDistributionEl('Fields').disabled = true;
    _pfDistributionEl('Save').disabled = false;
    _pfDistributionEl('Save').textContent = '저장 결과 확인';
    _pfDistributionEl('Status').textContent = '확인 중인 분배금 출금이 있습니다. 저장 결과를 먼저 확인해 주세요.';
  } else _pfDistributionInvalidate();
  _pfDistributionCurrency(false);
  document.getElementById('pfDividendDialog')?.close();
  if (!_pfDistributionEl('Dialog').open) _pfDistributionEl('Dialog').showModal();
  pfLoadDistributions();
  try {
    const balances = await apiFetchJson('/api/portfolio/distributions/balances', { errorMessage: '미분배 배당금을 불러오지 못했습니다.' });
    if (generation !== _pfDistribution.generation) return;
    _pfDistribution.balances = balances;
    if (!_pfDistribution.dirty && !_pfDistribution.pending && !_pfDistribution.busy) {
      _pfDistributionEl('Currency').value = balances.find(r => r.available_amount > 0)?.currency || 'KRW';
      _pfDistributionCurrency();
    }
  } catch (error) { if (generation === _pfDistribution.generation) _pfDistributionEl('Balance').textContent = error.message; }
}

_pfDistributionEl('Form').addEventListener('submit', pfPreviewDistribution);
_pfDistributionEl('Form').addEventListener('input', () => { _pfDistribution.dirty = true; _pfDistributionInvalidate(); });
_pfDistributionEl('Currency').addEventListener('change', () => { _pfDistribution.dirty = true; _pfDistributionInvalidate(); _pfDistributionCurrency(); });
_pfDistributionEl('Save').addEventListener('click', pfSaveDistribution);
_pfDistributionEl('Close').addEventListener('click', () => _pfDistributionEl('Dialog').close());
