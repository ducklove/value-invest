// 일별 자산 증감과 투자 손익의 원인 분해. 저장 리포트도 같은 렌더러를 쓴다.
let _pfAttributionSeq = 0;
let _pfIncomeBusy = false;

function pfAttributionMoney(value) {
  if (value === null || value === undefined || !Number.isFinite(Number(value))) return '—';
  return `${Number(value) > 0 ? '+' : ''}${Number(value).toLocaleString('ko-KR', { maximumFractionDigits: 0 })}원`;
}

function pfAttributionHtml(data) {
  if (!data?.available) return `<p class="pf-risk-empty">${escapeHtml(data?.message || '수익 분해 기록이 없습니다.')}</p>`;
  const parts = data.components || [];
  const max = Math.max(1, ...parts.map(row => Math.abs(Number(row.amount))));
  const bars = parts.map(row => `<div class="pf-attribution-row">
    <span>${escapeHtml(row.label)}</span><div class="pf-attribution-track" aria-hidden="true"><i class="${Number(row.amount) < 0 ? 'negative' : 'positive'}" style="width:${Math.max(0, Math.min(100, Math.abs(Number(row.amount)) / max * 100))}%"></i></div>
    <strong>${pfAttributionMoney(row.amount)}</strong></div>`).join('');
  const stocks = (data.stocks || []).map(row => `<tr><th scope="row">${escapeHtml(row.stock_name || row.stock_code)} <small>${escapeHtml(row.stock_code)}</small></th><td>${pfAttributionMoney(row.price)}</td><td>${pfAttributionMoney(row.fx)}</td><td>${pfAttributionMoney(row.combined)}</td><td>${pfAttributionMoney(row.total)}</td></tr>`).join('');
  const issues = Object.entries(data.coverage?.issues || {}).map(([label, count]) => `${escapeHtml(label)} ${Number(count)}구간`).join(' · ');
  return `<p class="pf-chart-note">실제 기준일 ${escapeHtml(data.baseline_date)} → ${escapeHtml(data.ending_date)}${data.baseline_mode === 'first_in_period' ? ' · 기간 내 첫 기록부터 계산' : ''}</p>
    <div class="pf-insight-summary"><div>자산 증감<strong>${pfAttributionMoney(data.value_change)}</strong></div><div>순입출금 제외 손익<strong>${pfAttributionMoney(data.investment_pnl)}</strong></div></div>
    <div class="pf-attribution-bars" aria-label="자산 증감 구성">${bars}</div>
    <p class="pf-chart-note">합계 검산 차이 ${pfAttributionMoney(data.reconciliation_error)} · 분석 가능한 종목 구간 ${Number(data.coverage?.eligible_intervals || 0)}/${Number(data.coverage?.examined_intervals || 0)}</p>
    ${issues ? `<p class="pf-insight-notice">${issues}</p>` : ''}
    <details><summary>종목별 기여분 (${(data.stocks || []).length}개)</summary><div class="pf-insight-table-scroll"><table class="pf-insight-table"><thead><tr><th>종목</th><th>가격</th><th>환율</th><th>미분리</th><th>합계</th></tr></thead><tbody>${stocks || '<tr><td colspan="5">분류 가능한 종목 구간이 없습니다.</td></tr>'}</tbody></table></div></details>
    <details class="pf-insight-method"><summary>계산 방식과 데이터 한계</summary>${(data.notes || []).map(note => `<p>${escapeHtml(note)}</p>`).join('')}</details>`;
}

function _pfIncomeHtml(events) {
  return (events || []).map(row => `<div class="pf-income-row"><span>${escapeHtml(row.date)} · ${row.kind === 'dividend' ? '배당' : '비용'} · ${escapeHtml(row.stock_code || '포트폴리오')} · ${pfAttributionMoney(row.amount_krw)} ${escapeHtml(row.memo || '')}</span><button type="button" class="pf-mini-btn" onclick="pfDeleteIncome(${Number(row.id)})" aria-label="${escapeHtml(row.date)} 분류 내역 삭제">삭제</button></div>`).join('') || '<p class="pf-chart-note">선택 기간에 기록한 배당·비용이 없습니다.</p>';
}

async function pfLoadAttribution() {
  const content = document.getElementById('pfAttributionContent');
  const start = document.getElementById('pfAttributionStart');
  const end = document.getElementById('pfAttributionEnd');
  if (!content || !start || !end) return;
  if (!end.value) end.value = new Date().toLocaleDateString('sv-SE', { timeZone: 'Asia/Seoul' });
  if (!start.value) start.value = end.value.slice(0, 7) + '-01';
  const seq = ++_pfAttributionSeq;
  content.textContent = '수익 원인을 계산하고 있습니다…';
  try {
    const data = await apiFetchJson(`/api/portfolio/attribution?start=${encodeURIComponent(start.value)}&end=${encodeURIComponent(end.value)}`, { errorMessage: '수익 분해를 불러오지 못했습니다.' });
    if (seq !== _pfAttributionSeq) return;
    content.innerHTML = pfAttributionHtml(data);
    const events = document.getElementById('pfIncomeEvents');
    if (events) events.innerHTML = _pfIncomeHtml(data.income_events);
  } catch (error) {
    if (seq === _pfAttributionSeq) content.textContent = error?.status === 401 ? '로그인 후 이용할 수 있습니다.' : '수익 분해를 불러오지 못했습니다. 기간을 확인하고 다시 조회해 주세요.';
  }
}

async function pfAddIncome(event) {
  event.preventDefault();
  if (_pfIncomeBusy) return;
  const form = event.target;
  const payload = Object.fromEntries(new FormData(form));
  payload.stock_code = payload.stock_code.trim().toUpperCase();
  payload.amount_krw = Number(payload.amount_krw);
  const button = form.querySelector('button[type="submit"]');
  _pfIncomeBusy = true;
  button.disabled = true;
  try {
    await apiFetchJson('/api/portfolio/income-events', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload), errorMessage: '배당·비용 분류를 저장하지 못했습니다.' });
    form.reset();
    await pfLoadAttribution();
  } catch (error) { reportApiError(error, '배당·비용 분류'); }
  finally { _pfIncomeBusy = false; button.disabled = false; }
}

async function pfDeleteIncome(id) {
  if (!confirm('이 분류 내역을 삭제할까요? 실제 잔고는 변경되지 않습니다.')) return;
  try {
    await apiFetchJson(`/api/portfolio/income-events/${id}`, { method: 'DELETE', errorMessage: '분류 내역 삭제 실패' });
    await pfLoadAttribution();
  } catch (error) { reportApiError(error, '분류 내역 삭제'); }
}
