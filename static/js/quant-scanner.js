// 전체 순회와 연속 호가 감시를 구분한다. 주문 API 호출은 없다.
let scannerData = null, scannerTimer = null, scannerLoading = false;
let scannerVersion = 0;
const SCANNER_FIELDS = [
  ['interval_minutes','전체 순회 목표 (분)',60,5,240,1],
  ['watch_bps','실시간 편입 순우위 (bp)',30,0,5000,1],
  ['signal_bps','기회 기록 순우위 (bp)',50,0,5000,1],
  ['max_pairs','최대 집중 감시 쌍',10,1,15,1],
  ['hold_minutes','최소 감시 유지 (분)',5,1,60,1],
  ['spot_fee_bps','현물 편도 수수료 (bp)',1,0,100,0.1],
  ['future_fee_bps','선물 편도 수수료 (bp)',0.6,0,100,0.1],
  ['sell_tax_bps','현물 매도세 가정 (bp)',20,0,100,0.1],
  ['slippage_bps','편도 슬리피지 여유 (bp)',2,0,100,0.1],
  ['funding_pct','연 금융비용 가정 (%)',4,0,40,0.1],
  ['margin_pct','증거금 가정 (%)',20,1,100,1],
  ['leg_risk_bps','한쪽 미체결 위험 공제 (bp)',10,0,100,1],
];
const SCANNER_STATES = {scanning:'전체 순회 중',market_closed:'장중 순회 대기',degraded:'연결·데이터 확인 필요',waiting:'후보 대기',connecting:'호가 연결 중',subscribed:'구독 승인',receiving:'호가 수신 중'};
const scannerTime = v => v ? new Date(v*1000).toLocaleString('ko-KR') : '—';

function scannerInit() {
  const root = document.getElementById('quantScanner');
  if (!root) return;
  if (!root.dataset.bound) {
    root.dataset.bound = '1';
    root.innerHTML = `<header class="quant-report-head"><div><span class="lab-eyebrow">전체 순회 → 후보 집중 감시</span><h3>현선물 시장 감시</h3></div><span class="quant-mode">관찰 · 실제 주문 0건</span></header>
      <p>종목마다 근월물 하나와 대응 현물을 순회합니다. 최종거래일 2거래일 전 장 시작부터 다음 상장 월물로 전환하며, 주말·휴장일을 제외합니다. 모의 계좌도 실제 시장 시세를 조회합니다.</p>
      <p class="quant-muted">전환은 감시 대상 변경입니다. 보유 포지션 청산·차월물 주문은 아직 실행하지 않습니다.</p>
      <div id="scannerSummary" class="scanner-summary" aria-live="polite"></div>
      <p id="scannerNotice" class="quant-notice">나무 연결 상태를 확인하는 중입니다.</p>
      <details id="scannerSettings"><summary>계좌·순회·감시 기준</summary><form id="scannerForm">
        <label>시세 조회에 사용할 연결 계좌<select name="account_id" required></select></label>
        <div class="basis-grid">${SCANNER_FIELDS.map(([k,n,v,min,max,step])=>`<label>${n}<input name="${k}" type="number" value="${v}" min="${min}" max="${max}" step="${step}" required></label>`).join('')}</div>
        <p class="quant-muted">1bp = 0.01%. 현물 매수·선물 매도 기준이며 배당 수입은 0으로 추정합니다. 만기까지 자금 유지·추정 증거금 비용을 차감합니다. 실제 결제·권리·조정승수와 주문 체결 검증 전이므로 거래 신호가 아닙니다.</p>
        <button type="submit" class="quant-primary" id="scannerStart">설정 저장·감시 시작</button>
        <button type="button" id="scannerStop">감시 중지</button>
      </form></details><p id="scannerMessage" role="status"></p>
      <div class="scanner-toolbar"><label>종목·계약 검색<input id="scannerSearch" type="search" placeholder="종목명, 현물 코드, 선물 계약"></label><label>표시 범위<select id="scannerFilter"><option value="all">감시 대상 전체</option><option value="watch">실시간 감시 중</option><option value="candidate">편입 기준 이상</option><option value="error">관측 실패</option></select></label><button type="button" id="scannerRefresh">갱신</button></div>
      <p id="scannerCoverage" class="quant-muted"></p><details id="scannerExcluded"><summary>월물 선택 제외 사유</summary><div></div></details><div id="scannerRows" class="basis-table-wrap"></div>
      <h3>실시간 기회 기록</h3><p class="quant-muted">신선한 양쪽 호가에서 재확인한 추정 순우위입니다. 주문·체결 기록이 아닙니다. 호가 관측은 최대 3,000건 보관합니다.</p><div id="scannerEvents" class="basis-table-wrap"></div>`;
    document.getElementById('scannerForm').addEventListener('submit',e=>{e.preventDefault();scannerSave(true);});
    document.getElementById('scannerStop').addEventListener('click',()=>scannerSave(false));
    document.getElementById('scannerRefresh').addEventListener('click',scannerRefresh);
    for (const id of ['scannerSearch','scannerFilter']) document.getElementById(id).addEventListener('input',scannerTable);
  }
  scannerRefresh();
}

async function scannerRefresh() {
  clearTimeout(scannerTimer);
  if (scannerLoading) return;
  scannerLoading = true;
  const version=scannerVersion;
  try {const data = await apiFetchJson('/api/quant/scanner'); if(version===scannerVersion){scannerData=data;scannerRender();}}
  catch (error) {document.getElementById('scannerMessage').textContent = error.message;}
  finally {
    scannerLoading = false;
    scannerTimer = setTimeout(()=>{if(document.getElementById('quantView')?.style.display !== 'none' && !document.hidden) scannerRefresh();},10000);
  }
}

async function scannerSave(enabled) {
  const f = document.getElementById('scannerForm'), msg = document.getElementById('scannerMessage');
  if (f.dataset.saving || enabled && !f.reportValidity()) return;
  const input = {enabled, account_id:f.elements.account_id.value};
  for (const [key] of SCANNER_FIELDS) input[key] = Number(f.elements[key].value);
  if (enabled && input.signal_bps < input.watch_bps) {msg.textContent='기회 기준은 감시 편입 기준 이상이어야 합니다.';return;}
  f.dataset.saving = '1'; f.querySelectorAll('button').forEach(b=>b.disabled=true);
  scannerVersion++;
  try {
    scannerData = enabled ? await apiFetchJson('/api/quant/scanner',{method:'PUT',headers:{'Content-Type':'application/json'},body:JSON.stringify(input)})
      : await apiFetchJson('/api/quant/scanner/stop',{method:'POST'});
    scannerRender();
    msg.textContent = enabled ? '감시 설정을 저장했습니다. 장중에 종목별 근월물 순회를 시작합니다.' : '감시 중지를 요청했습니다. 진행 중인 연결은 곧 정리됩니다.';
  } catch(error) {msg.textContent=error.message;}
  finally {delete f.dataset.saving; document.getElementById('scannerStart').disabled=!(scannerData?.accounts?.length);document.getElementById('scannerStop').disabled=!scannerData?.config?.enabled;}
}

function scannerRender() {
  const d=scannerData, p=d.progress||{}, rt=d.runtime||{}, f=document.getElementById('scannerForm');
  const signature=JSON.stringify(d.accounts||[]);
  if (f.dataset.accounts!==signature) {
    const selected=f.elements.account_id.value || d.config?.account_id || '';
    f.elements.account_id.innerHTML = '<option value="">연결 계좌 선택</option>'+(d.accounts||[]).map(a=>`<option value="${escapeHtml(a.account_id)}">${escapeHtml(a.name||'나무 계좌')} · ${a.environment==='mock'?'모의':'실계좌 시세'}</option>`).join('');
    f.elements.account_id.value=selected;f.dataset.accounts=signature;
  }
  if (!f.dataset.loaded) {
    if (d.config) for (const key of ['account_id',...SCANNER_FIELDS.map(x=>x[0])]) f.elements[key].value=d.config[key];
    f.dataset.loaded='1';
  }
  document.getElementById('scannerStart').disabled=!(d.accounts?.length)||!!f.dataset.saving;
  document.getElementById('scannerStop').disabled=!d.config?.enabled||!!f.dataset.saving;
  document.getElementById('scannerSummary').innerHTML=[
    ['운용 상태',d.config?.enabled ? (SCANNER_STATES[p.state]||'시작 대기'):'중지'],
    ['감시 대상',`${p.total??0}계약`],['이번 순회',`${p.cursor||0} / ${p.total??0}`],
    ['집중 감시',`${(rt.watched||[]).length}쌍`],['호가 구독',`${rt.approved||0} / ${rt.requested||0}`],
    ['마지막 순회 소요',p.last_cycle_seconds?`${(p.last_cycle_seconds/60).toFixed(1)}분`:'—'],
  ].map(([n,v])=>`<div><span>${n}</span><strong>${escapeHtml(String(v))}</strong></div>`).join('');
  document.getElementById('scannerNotice').textContent = !d.accounts?.length
    ? '나무 연결 계좌가 없습니다. 포트폴리오의 계좌 관리에서 연결한 뒤 감시를 시작하세요.'
    : `${p.message||'감시 계좌와 기준을 설정하세요.'} 기존 보유종목 시세 1세션 + 후보 호가 1세션을 사용합니다. 실주문과 자동 가상 체결은 아직 비활성입니다.`;
  if(p.catalog_error)document.getElementById('scannerNotice').textContent += ` 계약 목록 갱신 실패: ${p.catalog_error}`;
  const excluded=d.excluded||[], detail=document.getElementById('scannerExcluded');
  detail.hidden=!excluded.length;
  detail.querySelector('div').innerHTML=excluded.map(r=>`<p>${escapeHtml(r.name)} · ${escapeHtml(r.spot_code)}: ${escapeHtml(r.reason)}</p>`).join('');
  scannerTable();
  const events=(d.events||[]).filter(e=>e.type==='opportunity').slice(0,30);
  document.getElementById('scannerEvents').innerHTML=events.length?`<table><thead><tr><th>시각</th><th>종목·월물</th><th>추정 순우위</th><th>현물 매수 비용</th><th>선물 매도 가격</th></tr></thead><tbody>${events.map(e=>`<tr><td>${escapeHtml(scannerTime(e.observed_at))}</td><td>${escapeHtml(e.name)}<br>${escapeHtml(e.contract)}</td><td>${quantNumber(e.net_bps)}bp</td><td>${quantNumber(e.spot?.ask)}원</td><td>${quantNumber(e.future?.bid)}원</td></tr>`).join('')}</tbody></table>`:'<p>실시간 양쪽 호가에서 확인한 기회 기록이 없습니다.</p>';
}

function scannerTable() {
  if(!scannerData) return;
  const q=document.getElementById('scannerSearch').value.trim().toLowerCase(), filter=document.getElementById('scannerFilter').value;
  const watched=new Set(scannerData.runtime?.watched||[]);
  const rows=(scannerData.rows||[]).filter(r=>(!q||`${r.name} ${r.spot_code} ${r.contract} ${r.contract_name}`.toLowerCase().includes(q))
    && (filter==='all'||filter==='watch'&&watched.has(r.contract)||filter==='candidate'&&r.net_bps!=null&&r.net_bps>=(scannerData.config?.watch_bps??30)||filter==='error'&&r.error));
  document.getElementById('scannerCoverage').textContent=`감시 대상 ${scannerData.progress?.total||0}계약 / 원본 목록 ${scannerData.progress?.source_contracts||0}계약 · 관측 ${(scannerData.rows||[]).filter(r=>r.observed_at).length}계약 · 월물 선택 제외 ${scannerData.excluded?.length||0}종목 · 검색 결과 ${rows.length}계약 중 상위 100개 표시 · 비구독 구간은 실시간 감시하지 않습니다. 마지막 순회 관측 ${scannerTime(scannerData.progress?.last_scan_at)}. 목표 주기는 조회 지연·호출 한도에 따라 늘어날 수 있습니다.`;
  document.getElementById('scannerRows').innerHTML=rows.length?`<table><thead><tr><th>종목 / 감시 월물</th><th>최종거래일 / 전환일</th><th>관측 시각</th><th>현물 매수 비용</th><th>선물 매도 가격</th><th>가격 차이</th><th>비용 후 순우위</th><th>상태</th></tr></thead><tbody>${rows.slice(0,100).map(r=>`<tr><td>${escapeHtml(r.name)} · ${escapeHtml(r.spot_code)}<br>${escapeHtml(r.contract_name||r.contract)}<br>${r.contract_role==='next'?'차월물 전환':'근월물'}</td><td>${escapeHtml(r.expiry||'—')}<br>전환 ${escapeHtml(r.roll_on||'—')}<br>${r.expiry_verified?'최종거래일 확인':'예정 · 시세 응답 대조 전'}</td><td>${escapeHtml(scannerTime(r.observed_at))}</td><td>${quantNumber(r.spot?.ask)}원</td><td>${quantNumber(r.future?.bid)}원</td><td>${quantNumber(r.gross_bps)}bp</td><td>${quantNumber(r.net_bps)}bp</td><td>${escapeHtml(r.error||(watched.has(r.contract)?'실시간 감시':'순회 관측'))}</td></tr>`).join('')}</tbody></table>`:'<p>표시할 감시 대상이 없습니다. 월물 선택 제외 사유나 계약 목록 갱신 상태를 확인하세요.</p>';
}
