// Portfolio X-Ray — 심층 분석 탭의 실제 노출 카드.
//
// GET /api/portfolio/exposure[?account_id=…] (routes/portfolio_exposure.py) 를 소비한다.
// - 입력 없음. 심층 분석 탭이 보일 때 pfSwitchTab(portfolio-performance.js)이 lazy 로드한다.
// - 계좌 선택(PfStore.accountId)을 따르고, 계좌별 메모를 둔다(서버도 10분 캐시).
// - 모르는 값은 0 이 아니라 '-' 로 그린다. 데이터 기준일과 커버리지는 항상 보여 준다.
// 포맷터(fmtKrw/fmtPct/escapeHtml)는 utils.js 의 공용 헬퍼를 쓴다.

let _pfExposureSeq = 0;
const _pfExposureCache = {};
const PF_XRAY_TOP_ROWS = 10;
const PF_XRAY_SECTOR_ROWS = 10;
const PF_XRAY_ETF_STATUS = {
  covered: '구성 공개분 합산',
  asset_line: '채권·원자재형 — 한 줄로 유지',
  no_weights: '구성 비중 미공개',
  no_holdings: '구성 자료 없음',
  not_in_universe: 'EIAYN 미수록',
  no_source: 'ETF 자료 수신 실패',
};
const PF_XRAY_RISK_REASON = { no_series: '일봉 없음', no_rows: '일봉 없음', stale_rows: '일봉 지연', short_history: '표본 부족' };

function _pfExposureEl() { return document.getElementById('pfExposureContent'); }

function _pfExposureMsg(message) {
  const el = _pfExposureEl();
  if (el) el.innerHTML = `<div class="pf-risk-empty">${escapeHtml(message)}</div>`;
}

function _pfXrayPct(value, { signed = false, decimals = 1 } = {}) {
  return value === null || value === undefined ? '-' : fmtPct(Number(value), signed, decimals);
}

// 표 안에서 단위가 섞여 크기를 잘못 읽지 않도록 억·만 단위만 쓴다.
function _pfXrayMoney(value) {
  if (value === null || value === undefined) return '-';
  const number = Number(value);
  const size = Math.abs(number);
  if (size >= 1e8) return `${(number / 1e8).toLocaleString('ko-KR', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}억`;
  if (size >= 1e4) return `${Math.round(number / 1e4).toLocaleString('ko-KR')}만`;
  return Math.round(number).toLocaleString('ko-KR');
}

// 긴 표는 앞부분만 펼쳐 두고 나머지는 접는다.
function _pfXrayTable(id, head, rows, rowHtml, { total = rows.length } = {}) {
  const tableHead = `<thead><tr>${head}</tr></thead>`;
  const top = rows.slice(0, PF_XRAY_TOP_ROWS).map(rowHtml).join('');
  const restRows = rows.slice(PF_XRAY_TOP_ROWS);
  const rest = restRows.map(rowHtml).join('');
  // 응답은 행 수를 제한한다. 접힌 행이 전체 나머지가 아니면 그 사실을 그대로 적는다.
  const partial = rows.length < Number(total);
  const summary = partial ? `다음 ${restRows.length}개 보기 (전체 ${Number(total)}개)` : `나머지 ${restRows.length}개 보기`;
  return `<div class="pf-insight-table-scroll"><table class="pf-insight-table" id="${id}">${tableHead}<tbody>${top}</tbody></table></div>`
    + (rest ? `<details class="pf-insight-method"><summary>${summary}</summary><div class="pf-insight-table-scroll"><table class="pf-insight-table">${tableHead}<tbody>${rest}</tbody></table></div></details>` : '');
}

function _pfXrayNum(value, digits = 2) {
  return value === null || value === undefined ? '-' : Number(value).toFixed(digits);
}

function _pfXrayHhi(value) {
  return value === null || value === undefined ? '-' : Math.round(Number(value)).toLocaleString('ko-KR');
}

function _pfXrayTile(label, value, sub = '') {
  return `<div class="pf-nav-ret-card"><div class="pf-nav-ret-label">${escapeHtml(label)}</div><div class="pf-nav-ret-value">${escapeHtml(value)}</div>${sub ? `<div class="pf-risk-sub">${escapeHtml(sub)}</div>` : ''}</div>`;
}

function _pfXrayAccountLabel(data) {
  if (!data.account_id) return '전체 계좌 합산';
  const rows = typeof PfAccounts !== 'undefined' ? (PfAccounts.rows || []) : [];
  return rows.find(row => row.account_id === data.account_id)?.name || '선택 계좌';
}

function _pfXraySources(data) {
  const sources = data.sources || [];
  const chips = sources.map(source => {
    const state = !source.available ? '받지 못함'
      : source.as_of ? `${source.as_of}${source.age_days ? ` (${source.age_days}일 전)` : ''}` : '기준일 미상';
    const flagged = !source.available || source.stale;
    return `<span class="pf-xray-source${flagged ? ' is-stale' : ''}" data-source="${escapeHtml(source.id)}">${escapeHtml(source.label)} ${escapeHtml(state)}</span>`;
  }).join('');
  const warnings = sources.filter(source => !source.available || source.stale).map(source => (
    source.available
      ? `${source.label} 자료가 ${source.as_of} 기준입니다. 그 뒤의 변화는 반영되지 않았습니다.`
      : `${source.label} 자료를 받지 못해 해당 항목은 계산하지 못했습니다.`
  ));
  return `<div class="pf-xray-sources" aria-label="데이터 기준일">${chips}</div>`
    + (warnings.length ? `<p class="pf-insight-notice">${warnings.map(escapeHtml).join('<br>')}</p>` : '');
}

function _pfXrayTiles(data) {
  const totals = data.totals || {};
  const look = data.concentration?.look_through || {};
  const direct = data.concentration?.direct || {};
  const risk = data.risk || {};
  return `<div class="pf-risk-grid">${[
    _pfXrayTile('투자자산', _pfXrayMoney(totals.invested_value), '현금 제외'),
    _pfXrayTile('현금 (별도)', _pfXrayMoney(totals.cash_value), `총자산의 ${_pfXrayPct(totals.cash_pct)}`),
    _pfXrayTile('ETF 커버리지', _pfXrayPct(data.etfs?.coverage_pct), '주식·혼합형 ETF 평가액 기준'),
    _pfXrayTile('상위 5 비중', _pfXrayPct(look.top5_pct), `풀기 전 ${_pfXrayPct(direct.top5_pct)}`),
    _pfXrayTile('HHI (풀어 본 값)', _pfXrayHhi(look.hhi), `풀기 전 ${_pfXrayHhi(direct.hhi)} · 유효 ${_pfXrayNum(look.effective_n, 1)}개 · 하한`),
    _pfXrayTile('변동성 (모델)', _pfXrayPct(risk.portfolio_volatility_pct), `평가액 ${_pfXrayPct(risk.coverage_pct)} 반영`),
  ].join('')}</div>`;
}

function _pfXrayExposureTable(data) {
  const exposures = data.exposures || {};
  const rows = exposures.rows || [];
  if (!rows.length) return '<p class="pf-chart-note">풀어 볼 종목이 없습니다.</p>';
  const members = item => (item.members || []).map(member => `${member.name} ${_pfXrayMoney(member.value)}${member.via === 'holdco' ? ' (지주 환산)' : member.via === 'etf' ? ' (ETF)' : member.via === 'derivative' ? ' (선물)' : ''}`).join('\n');
  const row = item => `<tr><th scope="row" title="${escapeHtml(members(item))}">${escapeHtml(item.name)} <small>${escapeHtml(item.code)}</small></th><td><strong>${_pfXrayPct(item.net_pct)}</strong></td><td>${_pfXrayMoney(item.direct_value + item.derivative_value)}</td><td>${_pfXrayMoney(item.etf_value)}</td><td>${_pfXrayPct(item.holdco_pct)}</td></tr>`;
  return _pfXrayTable('pfXrayExposureTable', '<th>발행사·자산</th><th>순노출</th><th>직접·선물</th><th>ETF 경유</th><th>지주 환산(추정)</th>', rows, row, { total: exposures.row_count || rows.length })
    + `<p class="pf-chart-note">우선주는 보통주 발행사로 합쳤습니다. ETF 미공개 구성 ${_pfXrayPct(exposures.etf_unknown_pct)}는 어느 발행사에도 넣지 않았습니다. 지주 환산은 설정값으로 구한 별도 추정치라 순노출·합계·HHI에 더하지 않습니다.${exposures.leveraged_etf ? ' 레버리지·인버스 ETF가 있어 줄 합이 평가액과 다를 수 있습니다.' : ''}</p>`;
}

function _pfXrayBarScale(rows) {
  return Math.max(1e-9, ...rows.map(item => Math.abs(Number(item.pct) || 0)));
}

function _pfXrayBars(rows, scale = _pfXrayBarScale(rows)) {
  return rows.map(item => {
    const pct = Number(item.pct) || 0;
    const muted = item.is_cash || (item.kind && item.kind !== 'sector');
    const cash = Number(item.cash_value) ? `현금 ${_pfXrayMoney(item.cash_value)}` : '';
    return `<div class="pf-xray-bar${muted ? ' is-muted' : ''}${pct < 0 ? ' is-negative' : ''}" title="${escapeHtml(`${item.label} ${_pfXrayPct(item.pct)}`)}"><span>${escapeHtml(item.label)}</span><div class="pf-xray-track" aria-hidden="true"><i style="width:${(Math.abs(pct) / scale * 100).toFixed(1)}%"></i></div><strong>${_pfXrayPct(item.pct)}${cash ? `<small>${escapeHtml(cash)}</small>` : ''}</strong></div>`;
  }).join('');
}

function _pfXrayBreakdowns(data) {
  const sector = data.breakdowns?.sector || { rows: [] };
  const isSector = row => row.kind === 'sector';
  const named = sector.rows.filter(isSector);
  const buckets = sector.rows.filter(row => !isSector(row));
  const shown = named.slice(0, PF_XRAY_SECTOR_ROWS);
  const hidden = named.slice(PF_XRAY_SECTOR_ROWS);
  // 펼친 행·접힌 행·분류 밖 항목이 한 차트이므로 막대 축척을 하나로 맞춘다.
  const scale = _pfXrayBarScale(sector.rows);
  return `<div class="pf-xray-split">
    <div><h4>통화 <small>총자산 대비 · 거래 통화</small></h4>${_pfXrayBars(data.breakdowns?.currency || [])}</div>
    <div><h4>자산군 <small>총자산 대비 · 현금 별도</small></h4>${_pfXrayBars(data.breakdowns?.asset_class || [])}</div>
  </div>
  <h4>업종 <small>투자자산 대비 · 분류 ${_pfXrayPct(sector.classified_pct)}</small></h4>
  <div id="pfXraySector">${_pfXrayBars(shown, scale)}${hidden.length ? `<details class="pf-insight-method"><summary>나머지 업종 ${hidden.length}개 보기</summary>${_pfXrayBars(hidden, scale)}</details>` : ''}${_pfXrayBars(buckets, scale)}</div>`;
}

function _pfXrayEtfTable(data) {
  const rows = data.etfs?.rows || [];
  if (!rows.length) return '<p class="pf-chart-note">보유 ETF가 없습니다.</p>';
  const row = item => {
    const top = (item.top || []).slice(0, 3).map(part => `${escapeHtml(part.name)} ${_pfXrayNum(part.weight_pct, 1)}`).join(' · ');
    const status = PF_XRAY_ETF_STATUS[item.status] || item.status;
    return `<tr data-etf="${escapeHtml(item.code)}"><th scope="row">${escapeHtml(item.name)} <small>${escapeHtml(item.code)}</small></th><td>${_pfXrayPct(item.weight_pct)}</td><td><strong>${_pfXrayPct(item.coverage_pct)}</strong></td><td class="pf-xray-text">${escapeHtml(status)}${item.leveraged ? ' · 레버리지·인버스' : ''}${top ? `<br><small>${top}</small>` : ''}</td></tr>`;
  };
  return _pfXrayTable('pfXrayEtfTable', '<th>ETF</th><th>비중</th><th>커버리지</th><th class="pf-xray-text">상태·상위 구성</th>', rows, row);
}

function _pfXrayHoldcos(data) {
  const holdcos = data.holdcos || {};
  const rows = holdcos.rows || [];
  if (!rows.length) return '<p class="pf-chart-note">보유 지주사가 없습니다.</p>';
  const basis = holdcos.as_of ? ` (지분 파일 ${escapeHtml(holdcos.as_of)})` : '';
  const multiple = item => (item.multiple === null || item.multiple === undefined ? (item.hedged ? '헤지 포함' : '-') : `${_pfXrayNum(item.multiple)}배`);
  const row = item => `<tr><th scope="row">${escapeHtml(item.name)} <small>${escapeHtml(item.code)}</small></th><td>${_pfXrayMoney(item.value)}</td><td>${_pfXrayMoney(item.look_through_value)}</td><td>${multiple(item)}</td><td class="pf-xray-text">${(item.subsidiaries || []).slice(0, 3).map(sub => `${escapeHtml(sub.name)} ${_pfXrayMoney(sub.value)}`).join(' · ')}${item.missing_prices ? ` · 시세 없음 ${Number(item.missing_prices)}개` : ''}</td></tr>`;
  const subsidiaryRow = item => `<tr><th scope="row">${escapeHtml(item.name)} <small>${escapeHtml(item.code)}</small></th><td>${_pfXrayMoney(item.value)}</td><td>${_pfXrayPct(item.pct)}</td><td>${_pfXrayPct(item.net_pct)}</td><td class="pf-xray-text">${(item.via || []).map(escapeHtml).join(' · ')}</td></tr>`;
  const subsidiaries = holdcos.subsidiary_rows || [];
  return `<p class="pf-insight-notice" id="pfXrayHoldcoBasis">추정치입니다. 주식수는 지주사 지분가치 도구의 설정값${basis}이라 공시와 다를 수 있으니 정본으로 쓰지 마세요. 환산 합계 ${_pfXrayMoney(holdcos.total_look_through_value)}는 지주사 평가액과 별개의 참고치이며 총액에 더하지 않았습니다.</p>
    ${_pfXrayTable('pfXrayHoldcoTable', '<th>지주사</th><th>평가액</th><th>자회사 환산</th><th>배수</th><th class="pf-xray-text">주요 자회사</th>', rows, row)}
    ${subsidiaries.length ? `<h4>자회사별 환산 합계 <small>추정 · 투자자산 대비</small></h4>${_pfXrayTable('pfXraySubsidiaryTable', '<th>자회사</th><th>환산액</th><th>환산 비중</th><th>직접·ETF 순노출</th><th class="pf-xray-text">경유 지주사</th>', subsidiaries, subsidiaryRow)}` : ''}`;
}

function _pfXrayHeatmap(correlation) {
  if (!correlation?.codes?.length) return '';
  const names = correlation.names || correlation.codes;
  const cell = (value, a, b) => {
    if (value === null || value === undefined) return `<td title="${escapeHtml(`${a} × ${b}: 계산 불가`)}">-</td>`;
    const strength = Math.min(40, Math.abs(value) * 40).toFixed(0);
    const tone = value >= 0 ? 'var(--up)' : 'var(--down)';
    return `<td style="background:color-mix(in srgb, ${tone} ${strength}%, var(--surface))" title="${escapeHtml(`${a} × ${b}: ${Number(value).toFixed(2)}`)}">${Number(value).toFixed(2)}</td>`;
  };
  const body = correlation.matrix.map((row, i) => `<tr><th scope="row">${i + 1}. ${escapeHtml(names[i])}</th>${row.map((value, j) => cell(value, names[i], names[j])).join('')}</tr>`).join('');
  return `<h4>종목 간 상관 <small>위험 기여 상위 ${correlation.codes.length}개</small></h4><div class="pf-insight-table-scroll"><table class="pf-xray-heatmap" id="pfXrayHeatmap"><thead><tr><th></th>${names.map((name, i) => `<th scope="col" title="${escapeHtml(name)}">${i + 1}</th>`).join('')}</tr></thead><tbody>${body}</tbody></table></div>`;
}

function _pfXrayRisk(data) {
  const risk = data.risk || {};
  if (!risk.available) {
    return '<p class="pf-chart-note">일봉 표본이 부족하거나 일봉을 받지 못해 위험 기여를 계산하지 못했습니다.</p>';
  }
  const row = item => `<tr><th scope="row">${escapeHtml(item.name)} <small>${escapeHtml(item.code)}</small></th><td><strong>${_pfXrayPct(item.risk_contribution_pct, { signed: true })}</strong></td><td>${_pfXrayPct(item.weight_pct)}</td><td>${_pfXrayPct(item.volatility_pct)}</td></tr>`;
  const reasons = {};
  (risk.excluded || []).forEach(item => {
    const label = PF_XRAY_RISK_REASON[item.reason] || item.reason;
    reasons[label] = (reasons[label] || 0) + 1;
  });
  const excluded = Object.entries(reasons).map(([label, count]) => `${escapeHtml(label)} ${count}개`).join(' · ');
  const foreignCount = Number(risk.foreign_count) || 0;
  const foreignNote = foreignCount
    ? `<p class="pf-chart-note" id="pfXrayRiskForeign">해외 ${foreignCount}종목은 원화로 환산했고, 국내 장 마감 전에 끝난 마지막 종가를 그날 값으로 씁니다(미국·유럽은 전날 종가).</p>`
    : '';
  return `<p class="pf-chart-note">${escapeHtml(risk.start_date)} ~ ${escapeHtml(risk.end_date)} · 수익률 표본 ${Number(risk.sample_returns)}일 · 분산 비율 ${_pfXrayNum(risk.diversification_ratio)} · 가중 평균 상관 ${_pfXrayNum(risk.implied_avg_correlation)}${excluded ? ` · 제외 ${excluded}` : ''}</p>${foreignNote}
    ${_pfXrayTable('pfXrayRiskTable', '<th>포지션</th><th>위험 기여</th><th>모델 비중</th><th>변동성</th>', risk.rows || [], row, { total: risk.modeled_count || (risk.rows || []).length })}${_pfXrayHeatmap(risk.correlation)}`;
}

function _pfRenderExposure(data) {
  const el = _pfExposureEl();
  if (!el) return;
  if (!data?.available) {
    _pfExposureMsg('평가할 보유 종목이 없습니다.');
    return;
  }
  const totals = data.totals || {};
  const quoteNotes = [];
  if (totals.unpriced_count) quoteNotes.push(`시세가 없는 ${Number(totals.unpriced_count)}종목은 합계에서 뺐습니다.`);
  if (totals.stale_quotes) quoteNotes.push(`${Number(totals.stale_quotes)}종목은 직전 시세·스냅샷 값입니다.`);
  el.innerHTML = `<p class="pf-chart-note" id="pfXrayScope">${escapeHtml(_pfXrayAccountLabel(data))}${data.computed_at ? ` · ${escapeHtml(String(data.computed_at).slice(0, 16).replace('T', ' '))} 계산` : ''}</p>
    ${_pfXraySources(data)}
    ${_pfXrayTiles(data)}
    ${quoteNotes.length ? `<p class="pf-insight-notice">${escapeHtml(quoteNotes.join(' '))}</p>` : ''}
    <h4>실제 노출 <small>투자자산 대비</small></h4>${_pfXrayExposureTable(data)}
    ${_pfXrayBreakdowns(data)}
    <h4>ETF 풀어 보기 <small>전체 커버리지 ${_pfXrayPct(data.etfs?.coverage_pct)}</small></h4>${_pfXrayEtfTable(data)}
    <h4>지주사 풀어 보기 <small>추정</small></h4>${_pfXrayHoldcos(data)}
    <h4>위험 기여</h4>${_pfXrayRisk(data)}
    <details class="pf-insight-method"><summary>계산 방식과 데이터 한계</summary>${(data.notes || []).map(note => `<p>${escapeHtml(note)}</p>`).join('')}</details>`;
}

async function pfLoadExposurePanel({ force = false } = {}) {
  const el = _pfExposureEl();
  if (!el) return;
  const account = (typeof PfStore !== 'undefined' && PfStore.accountId) || '';
  if (!force && _pfExposureCache[account]) {
    _pfExposureSeq += 1;
    _pfRenderExposure(_pfExposureCache[account]);
    return;
  }
  const seq = ++_pfExposureSeq;
  _pfExposureMsg('실제 노출을 계산하는 중입니다...');
  try {
    const query = account ? `?account_id=${encodeURIComponent(account)}` : '';
    const data = await apiFetchJson(`/api/portfolio/exposure${query}`, { errorMessage: 'X-Ray 요청 실패' });
    _pfExposureCache[account] = data;
    // 계좌를 빠르게 바꿨을 때 늦게 도착한 응답은 그리지 않는다(메모만 갱신).
    if (seq === _pfExposureSeq) _pfRenderExposure(data);
  } catch (e) {
    if (seq !== _pfExposureSeq) return;
    if (e.status === 401) {
      _pfExposureMsg('로그인 후 이용할 수 있습니다.');
      return;
    }
    reportApiError(e, 'X-Ray', { silent: true });
    _pfExposureMsg('실제 노출을 불러오지 못했습니다. 잠시 후 다시 시도해 주세요.');
  }
}

function pfResetExposurePanel() {
  _pfExposureSeq += 1;
  Object.keys(_pfExposureCache).forEach(key => delete _pfExposureCache[key]);
  _pfExposureMsg('심층 분석을 열면 실제 노출을 계산합니다.');
}
