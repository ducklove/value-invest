// 퀀트 연구 UI. 신호·수량 계산은 버전이 고정된 서버 결과만 사용한다.
let quantSelected = null;
let quantResult = null;
let quantTimer = null;
let quantDetailVersion = 0;
let quantLoadVersion = 0;
const QUANT_NAMES = { switch: '상대가치 교체', common: '보통주 보유', preferred: '우선주 보유', mixed: '고정 혼합' };
const QUANT_STATUS = { queued: '대기', running: '연구 중', succeeded: '완료', failed: '실패', cancelled: '취소' };
const quantNumber = (v, n = 2) => Number.isFinite(v) ? v.toLocaleString('ko-KR', { maximumFractionDigits: n }) : '—';

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
    document.getElementById('quantView').addEventListener('click', quantClick);
  }
  try {
    const cap = await apiFetchJson('/api/quant/capabilities');
    if (generation !== quantLoadVersion) return;
    const health = document.getElementById('quantHealth');
    const ready = cap.readiness?.status === 'ready';
    health.textContent = cap.error || `${ready ? '수집 완료' : '수집 상태 확인 필요'} · 최근 가격 ${cap.readiness?.checks?.latest_price_date || '미확인'} · 과거 연구는 지정 기간의 자료를 별도로 검증합니다.`;
    health.classList.toggle('quant-warning', !ready);
    const select = document.getElementById('quantPair');
    const previous = select.value;
    select.innerHTML = '<option value="">종목 쌍을 선택하세요</option>' + (cap.pairs || []).map(p =>
      `<option value="${escapeHtml(p.common + ':' + p.preferred)}">${escapeHtml(p.name)} · ${escapeHtml(p.common)} / ${escapeHtml(p.preferred)}</option>`).join('');
    if ([...select.options].some(o => o.value === previous)) select.value = previous;
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
  const config = { strategy: 'preferred_switch', common, preferred,
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
      obs.observations.map(o => `<p>${escapeHtml(o.market_date)} · ${escapeHtml(o.run_id.slice(0, 8))} · ${QUANT_NAMES[o.payload.signal.target] || '확인 필요'} · z ${quantNumber(o.payload.signal.z)} · ${escapeHtml(o.payload.signal.reason)}<small class="quant-muted"> 기록 ${escapeHtml(new Date(o.observed_at * 1000).toLocaleString('ko-KR'))}</small></p>`).join('') : '<p>보고서에서 일별 신호 관찰을 시작하세요.</p>';
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
  report.innerHTML = `<div class="quant-report-head"><div><span class="quant-muted">검증 보고서 · ${escapeHtml(id.slice(0, 8))}</span><h3>${escapeHtml(c.common)} / ${escapeHtml(c.preferred)}</h3></div><button type="button" data-quant-action="export">원본·결과 저장</button></div>
    <p>${escapeHtml(c.start)} ~ ${escapeHtml(c.end)} · 가상 배정 ${quantNumber(c.capital, 0)}원 · 추정창 ${c.window}일 · 진입 ${c.entry_z} / 복귀 ${c.exit_z}</p>
    <div class="quant-notice">수정주가 기준 탐색 · 실거래 전환 불가 · 가격 수익과 비용을 비교하며 현금배당은 별도 미반영</div>
    ${quantChart(r.scenarios, c.capital)}
    <div class="quant-table-wrap"><table class="quant-table"><thead><tr><th>전략</th><th>비용 후 수익</th><th>최대 낙폭</th><th>추정 비용</th><th>체결 건수</th></tr></thead><tbody>${r.scenarios.map(x => `<tr><th>${QUANT_NAMES[x.mode]}</th><td>${quantNumber(x.return_pct)}%</td><td>${quantNumber(x.max_drawdown_pct)}%</td><td>${quantNumber(x.cost, 0)}원</td><td>${x.trade_count}</td></tr>`).join('')}</tbody></table></div>
    <div class="quant-metrics"><div><span>비용 2배 시 교체 수익</span><strong>${quantNumber(r.stress.return_pct)}%</strong></div><div><span>최근 할인율 · ${escapeHtml(s.date)}</span><strong>${quantNumber(s.discount * 100)}%</strong></div><div><span>최근 분포 이탈 정도</span><strong>z ${quantNumber(s.z)}</strong></div></div>
    <h4>최근 판단</h4><p>${escapeHtml(s.reason)} · ${QUANT_NAMES[s.target]} 방향. 다음 관측일에만 집행하는 가정입니다.</p>
    <button type="button" class="quant-primary" data-quant-action="watch" data-id="${escapeHtml(id)}">이 설정으로 일별 신호 관찰</button>
    <details><summary>검증 한계·재현 정보</summary><ul>${r.limitations.map(x => `<li>${escapeHtml(x)}</li>`).join('')}</ul><p class="quant-hash">입력 ${escapeHtml(r.snapshot.snapshot_id)}<br>엔진 ${escapeHtml(r.engine_version)}<br>설정 ${escapeHtml(r.config_hash)}</p></details>
    <details><summary>최근 가상 체결 30건 · 수정주가 기준 단위</summary><div class="quant-table-wrap"><table class="quant-table"><thead><tr><th>체결일</th><th>신호일</th><th>자산</th><th>구분</th><th>가상수량</th><th>가격</th></tr></thead><tbody>${r.scenarios[0].trades.slice(-30).reverse().map(t => `<tr><td>${escapeHtml(t.date)}</td><td>${escapeHtml(t.signal_date)}</td><td>${t.leg === 'common' ? '보통주' : '우선주'}</td><td>${t.side === 'buy' ? '매수' : '매도'}</td><td>${quantNumber(t.quantity, 0)}</td><td>${quantNumber(t.price)}</td></tr>`).join('')}</tbody></table></div></details>`;
}

function quantChart(scenarios, capital) {
  const colors = ['#0d9488', '#64748b', '#b45309', '#7c3aed'];
  const values = scenarios.flatMap(s => s.nav.map(p => (p.nav / capital - 1) * 100));
  if (!values.length) return '';
  const low = Math.min(0, ...values), high = Math.max(1, ...values), span = high - low;
  const lines = scenarios.map((s, k) => {
    const points = s.nav.map((p, i) => `${50 + i / Math.max(1, s.nav.length - 1) * 680},${205 - ((p.nav / capital - 1) * 100 - low) / span * 170}`).join(' ');
    return `<polyline fill="none" stroke="${colors[k]}" stroke-width="${k ? 1.5 : 3}" points="${points}"/>`;
  }).join('');
  return `<figure class="quant-chart"><svg viewBox="0 0 760 245" role="img" aria-label="같은 초기 자금 기준 네 전략의 비용 후 누적 수익률"><text x="4" y="38">${quantNumber(high, 1)}%</text><text x="4" y="208">${quantNumber(low, 1)}%</text>${lines}<text x="50" y="235">${escapeHtml(scenarios[0].nav[0].date)}</text><text x="640" y="235">${escapeHtml(scenarios[0].nav.at(-1).date)}</text></svg><figcaption>${scenarios.map((s, i) => `<span style="color:${colors[i]}">● ${QUANT_NAMES[s.mode]}</span>`).join(' ')}</figcaption></figure>`;
}

async function quantClick(event) {
  const button = event.target.closest('[data-quant-action]');
  if (!button || button.disabled) return;
  const action = button.dataset.quantAction, id = button.dataset.id;
  try {
    if (action === 'open') { await quantOpen(id); return; }
    if (action === 'export' && quantResult) {
      const blob = new Blob([JSON.stringify(quantResult, null, 2)], { type: 'application/json' });
      const url = URL.createObjectURL(blob), a = document.createElement('a');
      a.href = url; a.download = `quant-${quantSelected}.json`; a.click();
      setTimeout(() => URL.revokeObjectURL(url), 1000); return;
    }
    button.disabled = true;
    if (action === 'cancel') await apiFetchJson(`/api/quant/runs/${encodeURIComponent(id)}/cancel`, { method: 'POST' });
    if (action === 'watch' || action === 'stop-watch') {
      await apiFetchJson(`/api/quant/runs/${encodeURIComponent(id)}/watch`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ enabled: action === 'watch' }) });
      document.getElementById('quantMessage').textContent = action === 'watch' ? '신호 관찰을 시작했습니다. 실제 주문은 발생하지 않습니다.' : '신호 관찰을 중지했습니다.';
    }
    await quantRefresh();
  } catch (error) { document.getElementById('quantMessage').textContent = error.message; }
  finally { button.disabled = false; }
}
