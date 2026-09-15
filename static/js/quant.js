// 퀀트 연구 UI. 신호·수량 계산은 버전이 고정된 서버 결과만 사용한다.
let quantSelected = null;
let quantResult = null;
let quantTimer = null;
let quantDetailVersion = 0;
let quantLoadVersion = 0;
const QUANT_NAMES = { switch: '상대가치 교체', common: '보통주 보유', preferred: '우선주 보유', mixed: '고정 혼합' };
const QUANT_STATUS = { queued: '대기', running: '연구 중', succeeded: '완료', failed: '실패', cancelled: '취소' };
const quantNumber = (v, n = 2) => Number.isFinite(v) ? v.toLocaleString('ko-KR', { maximumFractionDigits: n }) : '—';
const quantNames = config => config?.strategy === 'etf_switch'
  ? { ...QUANT_NAMES, common: 'ETF A 보유', preferred: 'ETF B 보유' } : QUANT_NAMES;

function quantFactorInputs(audit) {
  if (!audit || audit.status === 'unavailable') return '<p>복합 팩터 입력 검사 결과를 받지 못했습니다. 기존 상대가치 연구는 별도로 사용할 수 있습니다.</p>';
  if (audit.status === 'missing_data' || audit.status === 'missing_fields') return '<p>복합 팩터용 시점별 재무 자료 또는 필수 필드가 부족합니다.</p>';
  const reasons = {as_of_mismatch:'평가일 불일치',unknown_or_future_publication:'공개일 불명·평가일 이후 공개',invalid_receipt:'접수번호·접수일 불일치',historical_or_unknown_provenance:'역사적 일괄 수집·출처 불명',unknown_or_foreign_units:'통화·단위 미확인 또는 외화',unknown_amount_basis:'금액 기준 미확인',invalid_amount:'유효하지 않은 금액',stale_or_future_period:'오래되거나 미래의 결산',unknown_scope:'연결·별도 범위 미확인'};
  return `<details><summary>복합 팩터 입력 점검 · 재무 ${escapeHtml(audit.as_of)} · 추가 검증 필요</summary><p>가격 기준일 ${escapeHtml(audit.latest_price_date || '미확인')}${audit.aligned_with_latest_prices ? '' : ' · 재무 기준일과 다름'}</p><p>검사 ${quantNumber(audit.securities,0)}종목 중 행 단위 통과 ${quantNumber(audit.eligible_securities,0)}종목, 동일 보고서·범위에서 이익·자산·자본이 갖춰진 후보 ${quantNumber(audit.complete_securities,0)}종목.</p><ul>${Object.entries(audit.exclusions || {}).filter(([,count])=>count>0).map(([key,count])=>`<li>${escapeHtml(reasons[key] || key)}: ${quantNumber(count,0)}행</li>`).join('')}</ul><p>${escapeHtml(audit.note || '')}</p></details>`;
}

function quantPairChanged() {
  const form = document.getElementById('quantForm');
  const strategy = form.elements.pair.selectedOptions[0]?.dataset.strategy || 'preferred_switch';
  if (form.dataset.strategy && form.dataset.strategy !== strategy) {
    form.elements.sell_tax_bps.value = strategy === 'etf_switch' ? '0' : '20';
    form.querySelector('details').open = true;
  } else if (!form.dataset.strategy && strategy === 'etf_switch') form.elements.sell_tax_bps.value = '0';
  form.dataset.strategy = strategy;
  document.getElementById('quantPairHint').textContent = strategy === 'etf_switch'
    ? 'ETF A / B 순서입니다. 과거 로그 가격비를 비교하고 왕복 교체 비용 이하의 진입은 보류합니다. 분배금·iNAV는 미반영이며 매도세 연구 기본값은 0bp입니다. 실제 비용을 확인하세요.'
    : '보통주 / 우선주 순서입니다. 과거 할인율 분포의 확대와 복귀를 비교합니다.';
}

async function loadQuant() {
  const generation = ++quantLoadVersion;
  const form = document.getElementById('quantForm');
  if (!form) return;
  if (!form.dataset.bound) {
    form.dataset.bound = '1';
    const yesterday = new Date(Date.now() + 9 * 3600000 - 86400000).toISOString().slice(0, 10);
    form.elements.end.value = yesterday;
    form.elements.end.max = yesterday;
    form.elements.start.value = `${Number(yesterday.slice(0, 4)) - 2}${yesterday.slice(4)}`;
    form.addEventListener('submit', quantSubmit);
    form.elements.pair.addEventListener('change', quantPairChanged);
    document.getElementById('quantView').addEventListener('click', quantClick);
  }
  try {
    const cap = await apiFetchJson('/api/quant/capabilities');
    if (generation !== quantLoadVersion) return;
    const health = document.getElementById('quantHealth');
    const ready = cap.readiness?.status === 'ready';
    health.textContent = cap.error || `${ready ? '수집 완료' : '수집 상태 확인 필요'} · 최근 가격 ${cap.readiness?.checks?.latest_price_date || '미확인'} · 과거 연구는 지정 기간의 자료를 별도로 검증합니다.`;
    if (!cap.error && cap.research_readiness) health.textContent += cap.research_readiness.status === 'ready'
      ? ' 일봉 관찰 가능 · 종목별 검증을 추가 수행합니다.' : ' 일봉 관찰 보류 · 가격 수집 상태를 확인하세요.';
    health.classList.toggle('quant-warning', !ready);
    let inputs = document.getElementById('quantFactorInputs');
    if (!inputs) {
      inputs = document.createElement('div'); inputs.id = 'quantFactorInputs';
      inputs.className = 'quant-notice'; health.after(inputs);
    }
    inputs.innerHTML = quantFactorInputs(cap.factor_inputs);
    const select = document.getElementById('quantPair');
    const previous = select.value;
    const options = (rows, strategy) => rows.map(p => `<option data-strategy="${strategy}" value="${escapeHtml(p.common + ':' + p.preferred)}">${escapeHtml(p.name)} · ${escapeHtml(p.common)} / ${escapeHtml(p.preferred)}</option>`).join('');
    select.innerHTML = '<option value="">종목 쌍을 선택하세요</option>' +
      `<optgroup label="보통주·우선주">${options(cap.pairs || [], 'preferred_switch')}</optgroup>` +
      `<optgroup label="동일 지수 ETF 연구 후보">${options(cap.etf_pairs || [], 'etf_switch')}</optgroup>`;
    if ([...select.options].some(o => o.value === previous)) select.value = previous;
    quantPairChanged();
    await quantRefresh();
  } catch (error) {
    document.getElementById('quantHealth').textContent = error.message;
  }
}

async function quantSubmit(event) {
  event.preventDefault();
  const form = event.currentTarget;
  const button = document.getElementById('quantSubmit');
  const message = document.getElementById('quantMessage');
  if (button.disabled) return;
  const [common, preferred] = form.elements.pair.value.split(':');
  const config = { strategy: form.elements.pair.selectedOptions[0]?.dataset.strategy || 'preferred_switch', common, preferred,
    start: form.elements.start.value, end: form.elements.end.value };
  for (const key of ['capital', 'window', 'max_holding', 'entry_z', 'exit_z', 'commission_bps', 'sell_tax_bps', 'slippage_bps', 'participation']) {
    config[key] = Number(form.elements[key].value);
  }
  config.participation /= 100;
  const signature = JSON.stringify(config);
  if (form.dataset.signature !== signature) {
    form.dataset.signature = signature;
    form.dataset.requestKey = crypto.randomUUID();
  }
  button.disabled = true;
  message.textContent = '연구를 대기열에 등록하는 중입니다.';
  try {
    const row = await apiFetchJson('/api/quant/runs', { method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ request_key: form.dataset.requestKey, config }) });
    quantSelected = row.id;
    message.textContent = '등록됐습니다. 화면을 닫아도 서버가 연구를 계속합니다.';
    delete form.dataset.signature;
    await quantRefresh();
  } catch (error) { message.textContent = error.message; }
  finally { button.disabled = false; }
}

async function quantRefresh() {
  clearTimeout(quantTimer);
  try {
    const [data, obs] = await Promise.all([apiFetchJson('/api/quant/runs'), apiFetchJson('/api/quant/observations')]);
    document.getElementById('quantRuns').innerHTML = data.runs.length ? data.runs.map(r =>
      `<button type="button" class="quant-run" data-quant-action="open" data-id="${escapeHtml(r.id)}"><strong>${escapeHtml(r.config.common)} / ${escapeHtml(r.config.preferred)}</strong><span>${escapeHtml(r.config.start)} ~ ${escapeHtml(r.config.end)} · ${QUANT_STATUS[r.status] || '확인 필요'}</span></button>`).join('') : '<p class="quant-muted">아직 저장한 실험이 없습니다.</p>';
    document.getElementById('quantObservations').innerHTML = obs.watches.length ?
      obs.watches.map(w => `<div class="quant-watch"><strong>${escapeHtml(w.run_id.slice(0, 8))} · ${w.enabled ? '관찰 중' : '관찰 중지'}</strong><span>${escapeHtml(w.error || '새로운 완료 일봉을 기다립니다.')}</span><button type="button" data-quant-action="${w.enabled ? 'stop-watch' : 'watch'}" data-id="${escapeHtml(w.run_id)}">${w.enabled ? '관찰 중지' : '다시 관찰'}</button></div>`).join('') +
      obs.observations.map(o => `<p>${escapeHtml(o.market_date)} · ${escapeHtml(o.run_id.slice(0, 8))} · ${quantNames(o.payload.config)[o.payload.signal.target] || '확인 필요'} · z ${quantNumber(o.payload.signal.z)} · ${escapeHtml(o.payload.signal.reason)}<small class="quant-muted"> 기록 ${escapeHtml(new Date(o.observed_at * 1000).toLocaleString('ko-KR'))}</small></p>`).join('') : '<p>보고서에서 일별 신호 관찰을 시작하세요.</p>';
    if (quantSelected) await quantOpen(quantSelected);
    if (data.runs.some(r => ['queued', 'running'].includes(r.status))) {
      quantTimer = setTimeout(() => {
        if (document.getElementById('quantView').style.display !== 'none' && !document.hidden) quantRefresh();
      }, 5000);
    }
  } catch (error) { document.getElementById('quantMessage').textContent = error.message; }
}

async function quantOpen(id) {
  const version = ++quantDetailVersion;
  quantSelected = id;
  const row = await apiFetchJson('/api/quant/runs/' + encodeURIComponent(id));
  if (version !== quantDetailVersion) return;
  const report = document.getElementById('quantReport');
  if (!row.result) {
    quantResult = null;
    report.innerHTML = `<h3>${QUANT_STATUS[row.status] || '확인 필요'}</h3><p>${escapeHtml(row.error || (row.status === 'cancelled' ? '연구가 취소됐습니다.' : '서버에서 데이터를 확인하고 연구를 실행합니다.'))}</p>` +
      (['queued', 'running'].includes(row.status) ? `<button type="button" data-quant-action="cancel" data-id="${escapeHtml(id)}">연구 취소</button>` : '');
    return;
  }
  quantResult = row.result;
  const r = row.result, c = r.config, s = r.latest_signal;
  const names = quantNames(c), isEtf = c.strategy === 'etf_switch';
  const currentEngine = r.engine_version === (isEtf ? 'etf-switch-2' : 'preferred-switch-2');
  report.innerHTML = `<div class="quant-report-head"><div><span class="quant-muted">검증 보고서 · ${escapeHtml(id.slice(0, 8))}</span><h3>${escapeHtml(c.common)} / ${escapeHtml(c.preferred)}</h3></div><button type="button" data-quant-action="export">원본·결과 저장</button></div>
    <p>${escapeHtml(c.start)} ~ ${escapeHtml(c.end)} · 가상 배정 ${quantNumber(c.capital, 0)}원 · 추정창 ${c.window}일 · 진입 ${c.entry_z} / 복귀 ${c.exit_z}</p>
    <div class="quant-notice">수정주가 기준 탐색 · 실거래 전환 불가 · 가격 수익과 비용을 비교하며 현금배당은 별도 미반영</div>
    <p class="quant-muted">유동성 추정치를 사용한 관측일: A ${quantNumber(r.liquidity_coverage?.common?.adjusted_close_times_volume_proxy, 0)}일 / B ${quantNumber(r.liquidity_coverage?.preferred?.adjusted_close_times_volume_proxy, 0)}일. 거래대금 관측값과 구분합니다.</p>
    ${isEtf ? '<p class="quant-muted">동일 지수 연구 후보 · ETF A / ETF B · 분배·복제 정책의 역사적 일치와 iNAV 검증 전</p>' : ''}
    ${quantChart(r.scenarios, c.capital, names)}
    <div class="quant-table-wrap"><table class="quant-table"><thead><tr><th>전략</th><th>비용 후 수익</th><th>최대 낙폭</th><th>추정 비용</th><th>체결 건수</th></tr></thead><tbody>${r.scenarios.map(x => `<tr><th>${names[x.mode]}</th><td>${quantNumber(x.return_pct)}%</td><td>${quantNumber(x.max_drawdown_pct)}%</td><td>${quantNumber(x.cost, 0)}원</td><td>${x.trade_count}</td></tr>`).join('')}</tbody></table></div>
    <div class="quant-metrics"><div><span>비용 2배 시 교체 수익</span><strong>${quantNumber(r.stress.return_pct)}%</strong></div><div><span>${isEtf ? "과거 평균 대비 상대가격 이탈" : "최근 할인율"} · ${escapeHtml(s.date)}</span><strong>${quantNumber(isEtf ? s.relative_deviation_bps / 100 : s.discount * 100)}%</strong></div><div><span>최근 분포 이탈 정도</span><strong>z ${quantNumber(s.z)}</strong></div></div>
    ${quantValidation(r)}
    ${quantLiquidity(r)}
    ${isEtf ? `<p class="quant-muted">왕복 교체 비용 기준 ${quantNumber(s.round_trip_cost_bps)}bp · 상대가격의 평균 복귀를 가정한 비교이며 기대수익 보장이 아닙니다.</p>` : ""}
    <h4>최근 판단</h4><p>${escapeHtml(s.reason)} · ${names[s.target]} 방향. 다음 관측일에만 집행하는 가정입니다.</p>
    ${currentEngine ? `<button type="button" class="quant-primary" data-quant-action="watch" data-id="${escapeHtml(id)}">이 설정으로 일별 신호 관찰</button>` : '<p class="quant-notice">이전 엔진 결과입니다. 새 연구를 실행한 후 관찰해 주세요.</p>'}
    ${quantForward(row, currentEngine)}
    <details><summary>검증 한계·재현 정보</summary><ul>${r.limitations.map(x => `<li>${escapeHtml(x)}</li>`).join('')}</ul><p class="quant-hash">입력 ${escapeHtml(r.snapshot.snapshot_id)}<br>엔진 ${escapeHtml(r.engine_version)}<br>설정 ${escapeHtml(r.config_hash)}</p></details>
    <details><summary>최근 가상 체결 30건 · 수정주가 기준 단위</summary><div class="quant-table-wrap"><table class="quant-table"><thead><tr><th>체결일</th><th>신호일</th><th>자산</th><th>구분</th><th>가상수량</th><th>가격</th></tr></thead><tbody>${r.scenarios[0].trades.slice(-30).reverse().map(t => `<tr><td>${escapeHtml(t.date)}</td><td>${escapeHtml(t.signal_date)}</td><td>${t.leg === 'common' ? (isEtf ? 'ETF A' : '보통주') : (isEtf ? 'ETF B' : '우선주')}</td><td>${t.side === 'buy' ? '매수' : '매도'}</td><td>${quantNumber(t.quantity, 0)}</td><td>${quantNumber(t.price)}</td></tr>`).join('')}</tbody></table></div></details>`;
}

function quantLiquidity(result) {
  const stress = result.liquidity_stress;
  if (!stress) return '';
  return `<h4>비용·유동성 스트레스</h4><p class="quant-muted">${escapeHtml(stress.note)}</p>
    <div class="quant-table-wrap"><table class="quant-table"><thead><tr><th>참여율 한도</th><th>교체 수익</th><th>혼합 대비 차이</th><th>최대 낙폭</th><th>종료 현금</th></tr></thead><tbody>${stress.scenarios.map(s => `<tr><th>기준의 ${quantNumber(s.participation_multiplier * 100, 0)}%</th><td>${quantNumber(s.return_pct)}%</td><td>${quantNumber(s.excess_return_pct)}%p</td><td>${quantNumber(s.max_drawdown_pct)}%</td><td>${quantNumber(s.ending_cash, 0)}원</td></tr>`).join('')}</tbody></table></div>`;
}

function quantForward(row, currentEngine) {
  const f = row.forward, id = escapeHtml(row.id);
  const intro = '<h4>사전 고정 전진 평가</h4><p class="quant-muted">다음 한국 날짜부터 설정과 시작일을 고정하고 새 일봉을 가상 원장으로 재생합니다. 첫 일봉은 신호만 생성하며 다음 일봉부터 집행합니다. 실제 호가 체결과 다르며 중단 기간도 재생합니다. 중지 후 재개할 수 없고 기록은 보존됩니다.</p>';
  if (!f) return currentEngine ? `${intro}<button type="button" data-quant-action="forward" data-id="${id}">이 설정으로 전진 평가 시작</button>` : '';
  const ledger = f.payload?.ledger;
  return `${intro}<p>${f.status === 'active' ? '평가 중' : '평가 중지'} · 시작일 ${escapeHtml(f.start_date)} · 최근 확인 ${f.checked_at ? escapeHtml(new Date(f.checked_at * 1000).toLocaleString('ko-KR')) : '대기'}</p>
    ${f.error ? `<p class="quant-notice">${escapeHtml(f.error)}</p>` : ''}
    ${ledger?.status === 'available' ? `<div class="quant-table-wrap"><table class="quant-table"><thead><tr><th>전진 평가</th><th>비용 후 수익</th><th>최대 낙폭</th><th>가상 체결</th><th>현금</th></tr></thead><tbody>${ledger.scenarios.map(s => `<tr><th>${quantNames(row.config)[s.mode]}</th><td>${quantNumber(s.return_pct)}%</td><td>${quantNumber(s.max_drawdown_pct)}%</td><td>${s.trade_count}건</td><td>${quantNumber(s.ending_cash, 0)}원</td></tr>`).join('')}</tbody></table></div><p class="quant-muted">평가 기준일 ${escapeHtml(ledger.scenarios[0].nav.at(-1).date)} · 수정주가 단위 · 실제 주문 0건</p><button type="button" data-quant-action="export-forward" data-id="${id}">전진 원장·입력 저장</button>` : `<p class="quant-muted">${f.status === 'active' ? '시작일 이후 완료된 일봉을 기다립니다. 수익률은 아직 없습니다.' : '평가 자료가 쌓이기 전에 중지했습니다. 기록된 수익률은 없습니다.'}</p>`}
    ${f.status === 'active' ? `<button type="button" data-quant-action="stop-forward" data-id="${id}">전진 평가 중지</button>` : ''}`;
}

function quantValidation(result) {
  const v = result.validation;
  if (!v) return '<p class="quant-muted">이전 버전의 결과입니다. 새 연구를 실행하면 기간 분할 진단을 볼 수 있습니다.</p>';
  if (v.status !== 'available') return '<h4>기간 분할 진단</h4><p class="quant-muted">구간당 63개, 전체 189개 이상의 관측일이 필요합니다.</p>';
  return `<h4>기간 분할 진단 · 혼합 보유 대비 우위 ${v.positive_excess_periods}/3</h4><p class="quant-muted">${escapeHtml(v.note)}</p>
    <div class="quant-table-wrap"><table class="quant-table"><thead><tr><th>평가 구간</th><th>교체 수익</th><th>혼합 보유</th><th>차이</th><th>최대 낙폭</th></tr></thead><tbody>${v.periods.map(p => `<tr><th>${escapeHtml(p.start)}<br>~ ${escapeHtml(p.end)}</th><td>${quantNumber(p.return_pct)}%</td><td>${quantNumber(p.benchmark_return_pct)}%</td><td>${quantNumber(p.excess_return_pct)}%p</td><td>${quantNumber(p.max_drawdown_pct)}%</td></tr>`).join('')}</tbody></table></div>
    <p class="quant-muted">비용을 두 배로 높인 경우의 혼합 보유 대비 수익 차이: ${quantNumber(result.stress.excess_return_pct)}%p. 두 전략에 같은 비용 배수를 적용했습니다.</p>`;
}

function quantChart(scenarios, capital, names = QUANT_NAMES) {
  const colors = ['#0d9488', '#64748b', '#b45309', '#7c3aed'];
  const values = scenarios.flatMap(s => s.nav.map(p => (p.nav / capital - 1) * 100));
  if (!values.length) return '';
  const low = Math.min(0, ...values), high = Math.max(1, ...values), span = high - low;
  const lines = scenarios.map((s, k) => {
    const points = s.nav.map((p, i) => `${50 + i / Math.max(1, s.nav.length - 1) * 680},${205 - ((p.nav / capital - 1) * 100 - low) / span * 170}`).join(' ');
    return `<polyline fill="none" stroke="${colors[k]}" stroke-width="${k ? 1.5 : 3}" points="${points}"/>`;
  }).join('');
  return `<figure class="quant-chart"><svg viewBox="0 0 760 245" role="img" aria-label="같은 초기 자금 기준 네 전략의 비용 후 누적 수익률"><text x="4" y="38">${quantNumber(high, 1)}%</text><text x="4" y="208">${quantNumber(low, 1)}%</text>${lines}<text x="50" y="235">${escapeHtml(scenarios[0].nav[0].date)}</text><text x="640" y="235">${escapeHtml(scenarios[0].nav.at(-1).date)}</text></svg><figcaption>${scenarios.map((s, i) => `<span style="color:${colors[i]}">● ${names[s.mode]}</span>`).join(' ')}</figcaption></figure>`;
}

async function quantClick(event) {
  const button = event.target.closest('[data-quant-action]');
  if (!button || button.disabled) return;
  const action = button.dataset.quantAction, id = button.dataset.id;
  try {
    if (action === 'open') { await quantOpen(id); return; }
    if (action === 'export-forward') {
      const row = await apiFetchJson('/api/quant/runs/' + encodeURIComponent(id));
      const blob = new Blob([JSON.stringify(row.forward, null, 2)], {type:'application/json'});
      const url = URL.createObjectURL(blob), a = document.createElement('a');
      a.href = url; a.download = `quant-forward-${id}.json`; a.click();
      setTimeout(() => URL.revokeObjectURL(url), 1000); return;
    }
    if (action === 'export' && quantResult) {
      const blob = new Blob([JSON.stringify(quantResult, null, 2)], { type: 'application/json' });
      const url = URL.createObjectURL(blob), a = document.createElement('a');
      a.href = url; a.download = `quant-${quantSelected}.json`; a.click();
      setTimeout(() => URL.revokeObjectURL(url), 1000); return;
    }
    button.disabled = true;
    if (action === 'cancel') await apiFetchJson(`/api/quant/runs/${encodeURIComponent(id)}/cancel`, { method: 'POST' });
    if (action === 'forward' || action === 'stop-forward') {
      await apiFetchJson(`/api/quant/runs/${encodeURIComponent(id)}/forward`, {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({enabled:action === 'forward'})});
      document.getElementById('quantMessage').textContent = action === 'forward' ? '다음 한국 날짜부터 전진 평가가 고정됐습니다. 최초 체결까지 최소 두 개의 새 일봉이 필요합니다.' : '전진 평가를 중지했습니다. 기존 원장은 보존됩니다.';
    }
    if (action === 'watch' || action === 'stop-watch') {
      await apiFetchJson(`/api/quant/runs/${encodeURIComponent(id)}/watch`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ enabled: action === 'watch' }) });
      document.getElementById('quantMessage').textContent = action === 'watch' ? '신호 관찰을 시작했습니다. 실제 주문은 발생하지 않습니다.' : '신호 관찰을 중지했습니다.';
    }
    await quantRefresh();
  } catch (error) { document.getElementById('quantMessage').textContent = error.message; }
  finally { button.disabled = false; }
}
