// 현선물 연구 화면. 가격·손익 계산은 finance-pi의 고정 버전 엔진을 사용한다.
let basisResult = null;
let basisImported = null;
let basisDetailVersion = 0;
const BASIS_STATUS = {no_trade:'진입 안 함',open:'보유',closed:'가상 청산',take_profit:'목표 이익 청산',stop_loss:'손실 한도 청산',margin_liquidation:'증거금 부족 청산',borrow_recall:'대차 회수 청산',unresolved:'청산 미확인'};
const BASIS_FIELDS = [
  ['capital','기준 투자금 (원)',100000000,10000,10000000000,10000],
  ['spot','현물 가격 가정 (원)',70000,1,10000000,0.01],
  ['future','선물 가격 가정 (원·포인트)',72000,1,10000000,0.01],
  ['underlying','기초주식 가격·지수 가정',70000,1,10000000,0.01],
  ['multiplier','거래승수',10,1,1000000,1],
  ['expected_dividend','기간 중 주당 세후 배당 가정 (원)',0,0,100000,0.01],
];
const BASIS_ADVANCED = [
  ['spot_fee_bps','현물 편도 수수료 (bp)',2,0,100,0.1],
  ['futures_fee_bps','선물 편도 수수료 (bp)',0.5,0,100,0.1],
  ['sell_tax_bps','현물 매도세 가정 (bp)',20,0,100,0.1],
  ['slippage_bps','편도 슬리피지 (bp)',2,0,100,0.1],
  ['funding_pct','연 금융·기회비용 (%)',4,0,40,0.1],
  ['borrow_pct','연 대차료 (%)',5,0,100,0.1],
  ['initial_margin_pct','개시증거금 가정 (%)',20,1,100,0.1],
  ['maintenance_margin_pct','유지증거금 가정 (%)',15,1,100,0.1],
  ['reserve_pct','유동성 예비금 (%)',20,0,80,1],
  ['min_edge_bps','비용 차감 최소 우위 (bp)',10,0,1000,1],
  ['leg_risk_bps','한쪽 미체결 위험 공제 (bp)',10,0,1000,1],
  ['tracking_risk_bps','ETF 추적 위험 공제 (bp)',30,0,2000,1],
  ['spread_bps','양방향 호가 차이 가정 (bp)',5,0,100,0.1],
  ['spot_depth','현물 최우선 호가 수량 가정 (주)',100000,1,100000000,1],
  ['future_depth','선물 최우선 호가 수량 가정 (계약)',10000,1,1000000,1],
  ['borrow_shares','확보한 대차 수량 가정 (주)',0,0,10000000,1],
  ['max_contracts','최대 계약 수',1000,1,10000,1],
  ['participation','호가 수량 참여율 (비율)',0.1,0.001,1,0.001],
  ['take_profit_fraction','진입 예상 순익 대비 청산 목표 (배)',1,0.1,2,0.1],
  ['max_loss_pct','투자금 대비 손실 한도 (%)',2,0.1,20,0.1],
];

function basisInputs(fields) {
  return fields.map(([key,label,value,min,max,step])=>`<label>${label}<input name="${key}" type="number" value="${value}" min="${min}" max="${max}" step="${step}" required></label>`).join('');
}

function basisInit() {
  const root = document.getElementById('quantBasis');
  if (!root) return;
  if (!root.dataset.bound) {
    root.dataset.bound = '1';
    root.innerHTML = `<header class="quant-report-head"><div><span class="lab-eyebrow">현물 × 선물</span><h3>현선물 차익 연구</h3></div><span class="quant-mode">가정 시뮬레이션 · 주문 0건</span></header>
      <p>개별주식·선물과 ETF·지수선물의 비용 차감 우위를 비교합니다. 아래 가격은 <strong>실제 시세가 아닌 예시</strong>입니다. 확보한 동시 호가 이력은 JSON으로 재생할 수 있습니다.</p>
      <details open><summary>가정과 비용 설정</summary><form id="basisForm">
        <div class="basis-grid"><label>조합<select name="kind"><option value="single_stock">개별주식 + 개별주식선물</option><option value="index_etf">ETF + KOSPI200 선물</option></select></label>
        <label>전략<select name="direction"><option value="cash_carry">현물 매수 + 선물 매도</option><option value="reverse_carry">현물 공매도 + 선물 매수</option></select></label>
        <label>현물 코드<input name="spot_code" value="005930" pattern="[0-9A-Z]{6}" required></label>
        <label>계약 식별자<input name="contract" value="SYNTHETIC-SSF" maxlength="40" required></label>
        <label>가정 시작일<input name="start" type="date" value="2026-09-16" required></label>
        <label>가정 만기일<input name="expiry" type="date" value="2026-12-10" required></label>
        ${basisInputs(BASIS_FIELDS)}</div>
        <p class="quant-muted">예시 증거금·비용은 NH 계좌의 실제 조건이 아닙니다. ETF는 지수와 정확히 같은 자산이 아니며 정수 수량과 추적 차이로 손익이 남습니다.</p>
        <details><summary>비용·증거금·체결·대차 가정</summary><div class="basis-grid">${basisInputs(BASIS_ADVANCED)}</div>
        <label class="basis-check"><input name="borrow_confirmed" type="checkbox"> 공매도용 대차 확보를 가정함 · 실제 대차 주문은 하지 않음</label></details>
        <button type="submit" id="basisSubmit" class="quant-primary">가정별 손익 시뮬레이션</button>
      </form></details>
      <details><summary>동시 호가 JSON 재생</summary><p>보고서에서 예시 JSON을 내려받을 수 있습니다. 파일에는 계약·비용 설정, 시간대가 있는 호가 시각, 매수·매도 호가, 수량, 기초가격, 배당 현금흐름이 필요합니다. 입력은 500KB·1,000개 관측값 이하입니다. 파일의 설정으로 실행하며 위 가정 입력란은 적용하지 않습니다.</p>
        <label>호가 이력 파일<input id="basisFile" type="file" accept="application/json,.json"></label>
        <p id="basisImportNote"></p><button type="button" id="basisReplay" disabled>파일의 설정·호가로 재생</button></details>
      <p id="basisMessage" role="status" aria-live="polite"></p>
      <div id="basisReport"><p class="quant-muted">실제 동시 호가 이력은 아직 연결되지 않았습니다. 가정 결과를 실제 아비트리지 수익률로 해석하지 마세요.</p></div>
      <details><summary>저장한 현선물 연구</summary><div id="basisRuns"></div></details>`;
    document.getElementById('basisForm').addEventListener('submit', basisSubmit);
    document.getElementById('basisForm').elements.kind.addEventListener('change', basisPreset);
    document.getElementById('basisFile').addEventListener('change', basisImport);
    document.getElementById('basisReplay').addEventListener('click', ()=>basisRun(basisImported));
    root.addEventListener('click', basisClick);
  }
  basisHistory();
}

function basisPreset() {
  const f = document.getElementById('basisForm');
  const etf = f.elements.kind.value === 'index_etf';
  const values = etf ? {spot:40000,future:408,underlying:400,multiplier:50000,spot_code:'069500',contract:'SYNTHETIC-MINI-KOSPI200',sell_tax_bps:0} : {spot:70000,future:72000,underlying:70000,multiplier:10,spot_code:'005930',contract:'SYNTHETIC-SSF',sell_tax_bps:20};
  for (const [key,value] of Object.entries(values)) f.elements[key].value = value;
  document.getElementById('basisMessage').textContent = '조합에 맞는 가정 예시를 적용했습니다. 실시간 가격이 아닙니다.';
}

function basisSubmit(event) {
  event.preventDefault();
  const f = event.currentTarget;
  const config = {};
  const scenario = {start:f.elements.start.value};
  const scenarioKeys = new Set(['spot','future','underlying','spread_bps','spot_depth','future_depth']);
  for (const [key] of [...BASIS_FIELDS,...BASIS_ADVANCED]) (scenarioKeys.has(key) ? scenario : config)[key] = Number(f.elements[key].value);
  for (const key of ['kind','direction','spot_code','contract','expiry']) config[key] = f.elements[key].value;
  config.borrow_confirmed = f.elements.borrow_confirmed.checked;
  basisRun({mode:'scenario',config,scenario,source_note:'화면에서 입력한 가정 · 실제 시세 아님'});
}

async function basisImport(event) {
  basisImported = null;
  const button = document.getElementById('basisReplay'); button.disabled = true;
  const note = document.getElementById('basisImportNote');
  try {
    const file = event.target.files[0];
    if (!file) { note.textContent = ''; return; }
    if (file.size > 500000) throw new Error('파일은 500KB 이하여야 합니다.');
    const data = JSON.parse(await file.text());
    if (data.mode !== 'replay' || !Array.isArray(data.quotes) || !data.config || data.quotes.length > 1000) throw new Error('호가 재생 JSON 형식을 확인하세요.');
    basisImported = data;
    note.textContent = `${data.config.contract} · 관측값 ${data.quotes.length}개 · ${data.source_note || '출처 미지정'}`;
    button.disabled = false;
  } catch (error) { note.textContent = error.message; }
}

async function basisRun(input) {
  if (!input) return;
  const button = document.getElementById('basisSubmit');
  if (button.disabled) return;
  const f = document.getElementById('basisForm');
  const signature = JSON.stringify(input);
  if (f.dataset.signature !== signature) { f.dataset.signature = signature; f.dataset.requestKey = crypto.randomUUID(); }
  button.disabled = true;
  const message = document.getElementById('basisMessage');
  message.textContent = '비용·체결·증거금 경로를 계산하는 중입니다.';
  const version = ++basisDetailVersion;
  try {
    const row = await apiFetchJson('/api/quant/basis/runs', {method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({request_key:f.dataset.requestKey,input})});
    if (version === basisDetailVersion) { basisRender(row); message.textContent = '입력과 보고서를 저장했습니다. 새로고침 후에도 다시 열 수 있습니다.'; }
    await basisHistory();
  } catch (error) { message.textContent = error.message; }
  finally { button.disabled = false; }
}

async function basisHistory() {
  try {
    const data = await apiFetchJson('/api/quant/basis/runs');
    document.getElementById('basisRuns').innerHTML = data.runs.length ? data.runs.map(r=>`<button type="button" class="quant-run" data-basis-open="${escapeHtml(r.id)}">${escapeHtml(r.contract)} · ${r.mode === 'scenario' ? '가정' : '호가 재생'} · ${escapeHtml(new Date(r.created_at*1000).toLocaleString('ko-KR'))}</button>`).join('') : '<p>저장한 현선물 연구가 없습니다.</p>';
  } catch (error) { document.getElementById('basisRuns').textContent = error.message; }
}

function basisRender(row) {
  basisResult = row;
  const r = row.result, c = r.config, n = quantNumber;
  const table = r.scenarios.map(s=>`<tr><th>${escapeHtml(s.name)}<small>${BASIS_STATUS[s.status] || '확인 필요'}</small></th><td>${n(s.pnl,0)}원<small>${n(s.return_on_capital_pct)}%</small></td><td>${n(s.contracts,0)}계약 / ${n(s.shares,0)}주</td><td>${n(s.max_drawdown_pct)}%</td><td>${n(s.additional_capital,0)}원</td></tr>`).join('');
  document.getElementById('basisReport').innerHTML = `<h4>${r.mode === 'scenario' ? '가정별 손익 · 과거 실적 아님' : '사용자 호가 재생 · 원천 검증 전'}</h4>
    <p>${escapeHtml(c.contract)} · 기준 투자금 ${n(c.capital,0)}원 · ${c.direction === 'cash_carry' ? '현물 매수 / 선물 매도' : '현물 공매도 / 선물 매수'}</p>
    <div class="quant-table-wrap"><table class="quant-table"><thead><tr><th>시나리오·종료 상태</th><th>가상 순손익 / 투자금 수익률</th><th>맞춘 수량</th><th>최대 낙폭</th><th>추가 필요 자금</th></tr></thead><tbody>${table}</tbody></table></div>
    ${basisChart(r.scenarios)}
    <p class="quant-muted">한쪽만 체결된 뒤 가격이 1% 불리하게 움직일 때의 가격 손실 규모: ${n(r.scenarios[0]?.unhedged_1pct_move_loss,0)}원. 추가 청산 비용 전이며 발생 확률을 뜻하지 않습니다.</p>
    <p class="quant-muted">수익률 분모는 증거금만이 아닌 기준 투자금 전체입니다. 미진입·청산 미확인의 손익은 —로 표시합니다. 추가 필요 자금은 중간에 개시증거금을 복원하는 기준이며, 실제 계좌 요건은 다를 수 있습니다.</p>
    ${r.scenarios.map(s=>{const last=s.ledger.at(-1);return `<details><summary>${escapeHtml(s.name)} · 판단과 손익 분해</summary><p>${escapeHtml(s.plan.blocked || '비용 차감 우위 통과')} · 진입 예상 우위 ${n(s.plan.edge_bps)}bp · 계약당 위험 공제 후 ${n(s.plan.net_per_contract,0)}원</p><p>순수량으로 남은 지수 노출 ${n(s.delta_residual_won,0)}원 · 최대 필요 자금 ${n(s.peak_capital_required,0)}원</p>${last ? `<p>마지막 평가 ${escapeHtml(last.at)}<br>현물 ${n(last.spot_pnl,0)}원 + 선물 ${n(last.future_pnl,0)}원 + 배당 ${n(last.dividends,0)}원 − 비용 ${n(last.costs,0)}원</p>` : ''}<ul>${s.events.map(e=>`<li>${escapeHtml(e.at)} · ${escapeHtml(e.reason || ({paired_entry:'양쪽 가상 체결',...BASIS_STATUS}[e.type]) || e.type)}</li>`).join('')}</ul></details>`;}).join('')}
    <div class="basis-actions"><button type="button" data-basis-export="report">입력·보고서 저장</button><button type="button" data-basis-export="quotes">호가 재생 JSON 저장</button></div>
    <details><summary>가정·검증 한계·재현 정보</summary><ul>${r.limitations.map(x=>`<li>${escapeHtml(x)}</li>`).join('')}</ul><p class="quant-hash">엔진 ${escapeHtml(r.engine_version)}<br>입력 ${escapeHtml(r.input_hash)}<br>결과 ${escapeHtml(r.result_hash)}</p></details>`;
}

function basisChart(scenarios) {
  const rows = scenarios.flatMap(s=>s.ledger);
  if (rows.length < 2) return '';
  const times = rows.map(r=>Date.parse(r.at));
  const start = Math.min(...times), end = Math.max(...times);
  const lo = Math.min(0,...rows.map(r=>r.pnl)), hi = Math.max(1,...rows.map(r=>r.pnl));
  const colors = ['#2563eb','#a855f7','#d97706','#dc2626','#059669'];
  const x = t=>55+650*(Date.parse(t)-start)/Math.max(1,end-start);
  const y = v=>185-155*(v-lo)/(hi-lo);
  return `<figure class="basis-chart"><figcaption>관측 시점별 가상 청산 손익 · 원</figcaption><svg viewBox="0 0 750 225" role="img" aria-label="시나리오별 가상 손익 경로"><line x1="55" x2="705" y1="${y(0)}" y2="${y(0)}" stroke="currentColor" opacity="0.3"/><text x="2" y="25" fill="currentColor" font-size="12">${escapeHtml(quantNumber(hi,0))}</text><text x="2" y="195" fill="currentColor" font-size="12">${escapeHtml(quantNumber(lo,0))}</text>${scenarios.map((s,i)=>`<polyline fill="none" stroke="${colors[i%colors.length]}" stroke-width="2" points="${s.ledger.map(r=>`${x(r.at).toFixed(2)},${y(r.pnl).toFixed(2)}`).join(' ')}"><title>${escapeHtml(s.name)}</title></polyline>`).join('')}<text x="55" y="217" fill="currentColor" font-size="12">${escapeHtml(rows[0].at.slice(0,10))}</text><text x="625" y="217" fill="currentColor" font-size="12">${escapeHtml(new Date(end).toISOString().slice(0,10))}</text></svg><div>${scenarios.map((s,i)=>`<span style="color:${colors[i%colors.length]}">● ${escapeHtml(s.name)}</span>`).join(' ')}</div></figure>`;
}

async function basisClick(event) {
  const open = event.target.closest('[data-basis-open]');
  if (open) {
    const version = ++basisDetailVersion;
    try { const row = await apiFetchJson('/api/quant/basis/runs/'+encodeURIComponent(open.dataset.basisOpen)); if (version === basisDetailVersion) basisRender(row); }
    catch (error) { document.getElementById('basisMessage').textContent = error.message; }
  }
  const download = event.target.closest('[data-basis-export]');
  if (!download || !basisResult) return;
  const data = download.dataset.basisExport === 'quotes' ? basisResult.result.replay_input : basisResult;
  const url = URL.createObjectURL(new Blob([JSON.stringify(data,null,2)],{type:'application/json'}));
  const link = document.createElement('a'); link.href = url; link.download = `현선물-${download.dataset.basisExport}-${basisResult.id}.json`; link.click(); setTimeout(()=>URL.revokeObjectURL(url),1000);
}
