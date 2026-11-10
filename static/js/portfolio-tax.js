// 실현손익·세금 추정 카드. 입력 없이 서버 원장만 읽는다(이 요청은 NH를 호출하지 않는다).
// 세액은 신고용이 아닌 추정이다. fields_verified가 false면 NH 필드 의미를 확인하기 전이므로
// 해외 손익에서 나온 금액을 어림값으로만 보여 준다. 계산 계약은 docs/realized-tax.md.
let _pfTaxSeq = 0;
const _PF_TAX_INCOME_STATUS = { ok: '여유', near: '근접', over: '초과 예상' };
const _PF_TAX_EXCLUDED_LABEL = { not_nh: 'NH 외', product: 'NH 주식 외 상품', account_type: '일반 외 유형' };
const _PF_TAX_MARKET_LABEL = { domestic: '국내', overseas: '해외', income: '이자·배당' };

function pfTaxExact(value) {
  if (value === null || value === undefined || !Number.isFinite(Number(value))) return '—';
  return `${Number(value).toLocaleString('ko-KR', { maximumFractionDigits: 0 })}원`;
}

// 서버의 domain.realized_tax.approx_krw와 같은 규칙. 정확한 숫자로 읽히지 않게 만원 단위로 줄인다.
function pfTaxApprox(value) {
  if (value === null || value === undefined || !Number.isFinite(Number(value))) return '—';
  const amount = Math.abs(Number(value));
  if (amount === 0) return '0원';
  const sign = Number(value) < 0 ? '-' : '';
  if (amount >= 10000) return `약 ${sign}${Math.round(amount / 10000).toLocaleString('ko-KR')}만원`;
  return `약 ${sign}${(Math.round(amount / 100) * 100).toLocaleString('ko-KR')}원`;
}

function pfTaxMoney(value, verified) {
  return verified ? pfTaxExact(value) : pfTaxApprox(value);
}

function _pfTaxNative(value, currency) {
  if (value === null || value === undefined || !Number.isFinite(Number(value))) return '—';
  return `${Number(value).toLocaleString('ko-KR', { maximumFractionDigits: 2 })} ${escapeHtml(currency || '')}`.trim();
}

function _pfTaxTile(label, value, note) {
  return `<div>${escapeHtml(label)}<strong>${escapeHtml(value)}</strong>${note ? `<small>${escapeHtml(note)}</small>` : ''}</div>`;
}

function _pfTaxTaxNote(year, verified) {
  if (year.tax_status === 'unavailable') return { failed: 'NH 조회 실패', no_account: 'NH 주식 계좌 없음' }[year.overseas?.status] || 'NH 조회 전';
  if (year.tax_status === 'incomplete') return verified ? '일부 누락 추정' : '일부 누락 · 미검증 추정';
  return verified ? '추정' : '미검증 추정';
}

function _pfTaxTiles(data, verified) {
  const current = data.current || {};
  const tax = current.tax;
  const harvest = data.loss_harvest || {};
  const realized = tax ? pfTaxMoney(current.overseas?.realized_krw, verified) : '—';
  const rate = tax ? `연 ${pfTaxExact(tax.deduction_krw)} 공제 후 ${Number(tax.rate_pct)}%` : '';
  return `<div class="pf-insight-summary pf-tax-tiles">
    ${_pfTaxTile(`${Number(data.year)}년 해외 실현손익`, realized, 'NH 해외주식 · 원화 기준')}
    ${_pfTaxTile('기본공제 잔여', tax ? pfTaxMoney(tax.deduction_remaining_krw, verified) : '—', tax ? `연 ${pfTaxExact(tax.deduction_krw)}` : '')}
    ${_pfTaxTile('추정 세액', tax ? pfTaxMoney(tax.estimated_tax_krw, verified) : '—', [rate, _pfTaxTaxNote(current, verified)].filter(Boolean).join(' · '))}
    ${_pfTaxTile('손실 매도 시 최대 감소', harvest.available ? pfTaxMoney(harvest.max_saving_krw, verified) : '—', harvest.available ? `평가손실 ${(harvest.candidates || []).length}종목 기준` : '')}
  </div>`;
}

function _pfTaxYearNotices(data, verified) {
  const current = data.current || {};
  const overseas = current.overseas || {};
  const notices = [];
  if (data.unlisted_currencies?.length) notices.push(`${data.unlisted_currencies.join('·')} 보유 종목의 매매는 NH 손익 명세의 국가 목록에 없어 누락될 수 있습니다. NH 앱의 손익과 대조하세요.`);
  if (overseas.status === 'pending') notices.push('NH 해외 손익을 아직 불러오지 못했습니다. 자동으로 수집하므로 입력할 것은 없습니다.');
  if (overseas.status === 'partial') notices.push('일부 계좌만 반영했거나 최근 조회가 실패했습니다. 아래 수집 상태를 확인하세요.');
  if (overseas.excluded_rows) notices.push(`원화 손익을 확인하지 못한 ${Number(overseas.excluded_rows)}건은 세액 합계에서 뺐습니다.`);
  if (overseas.review_rows) notices.push(`NH 산식 검산이 맞지 않거나 거래통화 응답이 없는 ${Number(overseas.review_rows)}건이 있습니다.`);
  if (overseas.total_gap_krw) notices.push(`원장 합계와 NH 기간 합계가 ${pfTaxMoney(Math.abs(overseas.total_gap_krw), verified)} 차이 납니다.`);
  if (overseas.pending_rows) notices.push(`종목별 상세 ${Number(overseas.pending_rows)}건을 불러오는 중입니다. 연도 합계에는 이미 반영했습니다.`);
  if (current.rules_stale) notices.push(`${Number(current.year)}년 세법은 아직 확인하지 않아 마지막으로 확인한 세율·공제액을 적용했습니다.`);
  return notices.map(text => `<p class="pf-insight-notice">${escapeHtml(text)}</p>`).join('');
}

function _pfTaxIncome(income) {
  if (!income) return '';
  const threshold = Number(income.threshold_krw);
  const received = Number(income.received_krw);
  const upcoming = Number(income.upcoming_krw);
  const scale = Math.max(threshold, received + upcoming) || 1;
  const width = value => `${Math.round(Math.max(0, Math.min(100, value / scale * 100)) * 100) / 100}%`;
  const status = _PF_TAX_INCOME_STATUS[income.status] ? income.status : 'ok';
  const plan = income.upcoming || {};
  const label = _PF_TAX_INCOME_STATUS[status];
  const notes = [];
  if (!income.interest_complete) notes.push(income.interest_from ? `이자는 ${income.interest_from} 이후 확인된 금액만 포함했습니다. 연초분은 자동으로 보강합니다.` : '이자 내역을 아직 연초부터 불러오지 못했습니다. 자동으로 보강합니다.');
  if (income.review_rows) notes.push(`확인 필요로 표시된 NH 배당 ${Number(income.review_rows)}건은 합계에서 뺐습니다.`);
  if (income.unverified_rows) notes.push(`세전 원화 금액을 확인하지 못한 NH 배당 ${Number(income.unverified_rows)}건은 합계에서 뺐습니다.`);
  if (plan.available === false) notes.push('배당 일정을 불러오지 못해 남은 지급 예정을 넣지 않았습니다.');
  if (plan.unscheduled) notes.push(`지급일을 모르는 일정 ${Number(plan.unscheduled)}건(${pfTaxApprox(plan.unscheduled_krw)})은 합계에 넣지 않고 근접 판단에만 썼습니다.`);
  if (plan.other_accounts_krw) notes.push(`NH 외 계좌 몫의 예정 배당 ${pfTaxApprox(plan.other_accounts_krw)}은 포함하지 않았습니다.`);
  if (income.rules_stale) notes.push(`${Number(income.year)}년 세법은 아직 확인하지 않아 마지막으로 확인한 기준을 적용했습니다.`);
  const row = (swatch, name, value) => `<div><dt><i class="pf-tax-swatch ${swatch}" aria-hidden="true"></i>${escapeHtml(name)}</dt><dd>${escapeHtml(value)}</dd></div>`;
  return `<div class="pf-tax-income ${status}">
    <div class="pf-tax-income-head"><h4>금융소득 ${pfTaxExact(threshold)} 대비</h4><span class="pf-tax-status">${escapeHtml(label)}</span></div>
    <div class="pf-tax-meter" role="img" aria-label="금융소득 기준 ${escapeHtml(pfTaxExact(threshold))} 대비 받은 금액 ${escapeHtml(pfTaxExact(received))}, 남은 지급 예정 ${escapeHtml(pfTaxApprox(upcoming))}, 합계의 ${Number(income.ratio_pct)}%, ${escapeHtml(label)}">
      <i class="received" style="width:${width(received)}"></i><i class="upcoming" style="width:${width(upcoming)}"></i>
      <b class="pf-tax-meter-limit" style="left:${width(threshold)}"></b>
    </div>
    <dl class="pf-tax-income-list">
      ${row('received', '받은 배당 (세전)', pfTaxExact(income.dividends_krw))}
      ${row('received', '확인된 이자 (세전)', pfTaxExact(income.interest_krw))}
      ${row('upcoming', '남은 지급 예정', pfTaxApprox(upcoming))}
      <div class="pf-tax-income-total"><dt>합계 (${Number(income.ratio_pct)}%)</dt><dd>${escapeHtml(pfTaxApprox(income.projected_krw))}</dd></div>
    </dl>
    ${notes.map(text => `<p class="pf-chart-note pf-tax-note">${escapeHtml(text)}</p>`).join('')}
  </div>`;
}

function _pfTaxYears(data, verified) {
  const rows = (data.years || []).map(year => {
    const tax = year.tax;
    const overseasMoney = tax || year.overseas?.rows ? pfTaxMoney(year.overseas?.realized_krw, verified) : '—';
    // 받은 적이 없는 시장을 0원으로 보이지 않는다.
    const domestic = !['ok', 'partial'].includes(year.domestic?.status) && !year.domestic?.rows ? '—' : pfTaxExact(year.domestic?.realized_krw);
    return `<tr><th scope="row">${Number(year.year)}년</th><td>${escapeHtml(domestic)}</td><td>${escapeHtml(overseasMoney)}</td>
      <td>${escapeHtml(tax ? pfTaxMoney(tax.deduction_used_krw, verified) : '—')}</td>
      <td>${escapeHtml(tax ? pfTaxMoney(tax.estimated_tax_krw, verified) : '—')}</td></tr>`;
  }).join('');
  const currencies = (data.current?.overseas?.by_currency || []).map(row => _pfTaxNative(row.realized_native, row.currency)).join(' · ');
  return `<h4 class="pf-tax-heading">연도별 실현손익</h4>
    <div class="pf-insight-table-scroll"><table class="pf-insight-table pf-tax-table"><thead><tr><th>연도</th><th>국내</th><th>해외</th><th>공제 사용</th><th>추정 세액</th></tr></thead><tbody>${rows}</tbody></table></div>
    <p class="pf-chart-note pf-tax-note">국내 금액은 NH 응답값이며 세액을 계산하지 않습니다.${currencies ? ` 해외 거래통화 합계: ${currencies}` : ''}</p>`;
}

function _pfTaxHarvest(data, verified) {
  const harvest = data.loss_harvest || {};
  if (!harvest.available) return '';
  const candidates = harvest.candidates || [];
  const rows = candidates.map(row => `<tr><th scope="row">${escapeHtml(row.stock_name || row.stock_code)} <small>${escapeHtml(row.account_name || '')}</small></th><td>${escapeHtml(pfTaxMoney(row.unrealized_krw, verified))}</td><td>${escapeHtml(pfTaxMoney(row.tax_saving_krw, verified))}</td></tr>`).join('');
  const notes = [];
  if (harvest.as_of) notes.push(`${harvest.as_of} 정산 기준`);
  if (harvest.unpriced_count) notes.push(`평가 기준이 없는 ${Number(harvest.unpriced_count)}종목 제외`);
  const summary = !harvest.current_tax_krw
    ? '현재 추정 세액이 없어 손실을 실현해도 줄어드는 세액이 없습니다.'
    : `추정 세액을 없애려면 ${pfTaxMoney(harvest.loss_to_zero_tax_krw, verified)}의 손실이 필요합니다. 평가손실을 모두 실현하면 ${pfTaxMoney(harvest.max_saving_krw, verified)} 줄어듭니다.`;
  return `<details class="pf-insight-method"><summary>손실 종목을 매도할 때 줄어드는 세액 (${candidates.length}종목)</summary>
    <p class="pf-chart-note pf-tax-note">${escapeHtml(summary)}${notes.length ? ` ${escapeHtml(notes.join(' · '))}` : ''}</p>
    <div class="pf-insight-table-scroll"><table class="pf-insight-table pf-tax-table"><thead><tr><th>종목</th><th>평가손익</th><th>그 종목만 매도 시 감소</th></tr></thead><tbody>${rows || '<tr><td colspan="3">평가손실 종목이 없습니다.</td></tr>'}</tbody></table></div>
  </details>`;
}

function _pfTaxStocks(data, verified) {
  const stocks = data.current?.stocks || [];
  if (!stocks.length) return '';
  const rows = stocks.map(stock => {
    const name = stock.stock_name || stock.stock_code || '일자 합계 (종목 상세 없음)';
    return `<tr><th scope="row">${escapeHtml(name)} <small>${escapeHtml(_PF_TAX_MARKET_LABEL[stock.market] || '')}</small></th><td>${stock.market === 'overseas' ? escapeHtml(pfTaxMoney(stock.realized_krw, verified)) : escapeHtml(pfTaxExact(stock.realized_krw))}</td><td>${stock.market === 'overseas' ? _pfTaxNative(stock.realized_native, stock.currency) : '—'}</td></tr>`;
  }).join('');
  return `<details class="pf-insight-method"><summary>${Number(data.year)}년 종목별 실현손익 (${stocks.length}종목)</summary>
    <div class="pf-insight-table-scroll"><table class="pf-insight-table pf-tax-table"><thead><tr><th>종목</th><th>원화</th><th>거래통화</th></tr></thead><tbody>${rows}</tbody></table></div>
  </details>`;
}

function _pfTaxCoverage(coverage) {
  const data = coverage || {};
  const accounts = data.nh_accounts || [];
  const excluded = data.excluded_accounts || [];
  const lines = [`포함: NH 주식 계좌 ${accounts.length}개${accounts.length ? ` (${accounts.map(name => escapeHtml(name)).join(', ')})` : ''}`];
  const notNh = excluded.filter(row => row.reason === 'not_nh');
  lines.push(notNh.length ? `미포함: NH 외 계좌 ${notNh.length}개 (${notNh.map(row => escapeHtml(row.name)).join(', ')})` : '미포함: NH 외 계좌의 손익·배당은 계산하지 않습니다');
  excluded.filter(row => row.reason !== 'not_nh').forEach(row => lines.push(`미포함: ${escapeHtml(_PF_TAX_EXCLUDED_LABEL[row.reason] || '')} 계좌 ${escapeHtml(row.name)}`));
  if (data.last_success_at) lines.push(`마지막 수집 ${escapeHtml(String(data.last_success_at).slice(0, 16).replace('T', ' '))}`);
  const errors = (data.errors || []).map(row => `<p class="pf-insight-notice">${escapeHtml(row.account_name)} ${Number(row.year)}년 ${escapeHtml(_PF_TAX_MARKET_LABEL[row.market] || '')} 조회 보류: ${escapeHtml(row.message)}${row.last_success_at ? ' 마지막으로 받은 내역을 유지합니다.' : ''}</p>`).join('');
  return `<p class="pf-chart-note pf-tax-note pf-tax-coverage">${lines.join(' · ')}</p>${errors}`;
}

function pfTaxHtml(data) {
  if (!data?.available) {
    return `<p class="pf-risk-empty">${escapeHtml(data?.message || '실현손익 기록이 없습니다.')}</p>${_pfTaxCoverage(data?.coverage)}`;
  }
  const verified = !!data.fields_verified;
  const banner = verified
    ? '<strong>신고용이 아닌 추정입니다.</strong> 실제 세액은 증권사 양도세 자료로 확인하세요.'
    : '<strong>신고용이 아닌 미검증 추정입니다.</strong> NH 손익 응답의 필드 의미를 실제 응답으로 확인하기 전이라 해외 금액은 어림값이고 세액은 확정값이 아닙니다.';
  return `<p class="pf-insight-notice pf-tax-banner" role="note">${banner} ${verified ? 'NH 원화 손익에는 취득 시 환율이 반영되지만, 원가 산정이 세법의 선입선출·기준환율과 다를 수 있습니다.' : 'NH 손익 산식과 세법의 선입선출·취득 시 환율 기준은 다를 수 있습니다.'}</p>
    ${_pfTaxTiles(data, verified)}
    ${_pfTaxYearNotices(data, verified)}
    ${_pfTaxIncome(data.financial_income)}
    ${_pfTaxYears(data, verified)}
    ${_pfTaxCoverage(data.coverage)}
    ${_pfTaxHarvest(data, verified)}
    ${_pfTaxStocks(data, verified)}
    <details class="pf-insight-method"><summary>계산 방식과 데이터 한계</summary>${(data.notes || []).map(note => `<p>${escapeHtml(note)}</p>`).join('')}</details>`;
}

async function pfLoadTaxPanel() {
  const content = document.getElementById('pfTaxContent');
  if (!content) return;
  const seq = ++_pfTaxSeq;
  try {
    const data = await apiFetchJson('/api/portfolio/realized-tax', { errorMessage: '세금 추정을 불러오지 못했습니다.' });
    if (seq === _pfTaxSeq) content.innerHTML = pfTaxHtml(data);
  } catch (error) {
    if (seq === _pfTaxSeq) content.textContent = error?.status === 401 ? '로그인 후 이용할 수 있습니다.' : '세금 추정을 불러오지 못했습니다. 잠시 후 다시 열어 주세요.';
  }
}
