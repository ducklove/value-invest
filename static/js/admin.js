/* Admin dashboard */

let _adminLoaded = false;
let _liveInterval = null;
let _linkedProjectConfigs = [];
let _aiAdminConfig = null;
let _preferredConfigFilter = '';
let _preferredDividendFilter = '';
let _preferredDividendRows = [];

async function loadAdminView() {
  const container = document.getElementById('adminContent');
  if (!container) return;
  if (!_adminLoaded) {
    container.innerHTML = '<div style="color:var(--text-secondary);text-align:center;padding:40px;">로딩 중...</div>';
  }
  try {
    // deploy-status answers the "did my push land" question at a glance,
    // so it sits right next to the other meta-status calls and renders
    // at the top of the page.
    const [deployRes, batchRes, serverRes, dbRes, usersRes, summaryRes, eventsRes, linkedConfigsRes, aiConfigRes] = await Promise.all([
      apiFetch('/api/admin/deploy-status'),
      apiFetch('/api/admin/batch-status'),
      apiFetch('/api/admin/server-stats'),
      apiFetch('/api/admin/db-stats'),
      apiFetch('/api/admin/users'),
      apiFetch('/api/admin/event-summary?hours=24'),
      apiFetch('/api/admin/events?limit=50'),
      apiFetch('/api/admin/linked-project-configs'),
      apiFetch('/api/admin/ai-config'),
    ]);
    // deploy-status may 404 on servers that haven't picked up this build
    // yet — which is, ironically, exactly the state this card is built
    // to warn about. Fall back to an empty object so the rest of the
    // dashboard still renders.
    const deploy = deployRes.ok ? await deployRes.json() : null;
    const batch = await batchRes.json();
    const server = await serverRes.json();
    const db = await dbRes.json();
    const users = await usersRes.json();
    const summary = summaryRes.ok ? await summaryRes.json() : {by_source: {}, latest: {}};
    const events = eventsRes.ok ? await eventsRes.json() : [];
    _linkedProjectConfigs = linkedConfigsRes.ok ? await linkedConfigsRes.json() : [];
    _aiAdminConfig = aiConfigRes.ok ? await aiConfigRes.json() : null;
    container.innerHTML = _renderAdmin(deploy, batch, server, db, users, summary, events, _linkedProjectConfigs, _aiAdminConfig);
    _adminLoaded = true;
    _startLiveUpdates();
    // 해외 배당 목록은 섹션 HTML 삽입 후에만 컨테이너가 존재 — 별도
    // 비동기 로드로 가져와 표시. 실패해도 페이지 나머지엔 영향 없음.
    loadPreferredDividendsList();
    loadForeignDividendsList();
  } catch (e) {
    container.innerHTML = `<div style="color:var(--text-secondary);padding:40px;">어드민 데이터를 불러오지 못했습니다.</div>`;
  }
}

function _renderAdmin(deploy, batch, server, db, users, summary, events, linkedConfigs, aiConfig) {
  return `
    <div class="admin-dashboard">
      <h2 class="admin-title">시스템 관리</h2>
      ${_renderDeployCard(deploy)}
      <div id="adminLiveSection">${_renderServerCard(server)}</div>
      ${_renderBatchSection(batch)}
      ${_renderSubsystemSummary(summary)}
      ${_renderDataSyncSection()}
      ${_renderAiConfigSection(aiConfig)}
      ${_renderLinkedProjectConfigSection(linkedConfigs)}
      ${_renderDiagSection()}
      ${_renderEventsSection(events)}
      ${_renderUsersSection(users)}
      ${_renderDbSection(db)}
    </div>
  `;
}

// --- Data sync (manual refresh triggers) --------------------------------
//
// 시트 관리자가 Google Sheet 값을 방금 고쳤을 때 12시간 자동 루프를
// 기다리지 않고 즉시 반영할 수 있는 버튼. POST /api/admin/refresh-
// preferred-dividends 엔드포인트를 호출하고 응답 요약을 보여준다.

function _renderDataSyncSection() {
  return `
    <div class="admin-section">
      <h3>외부 데이터 동기화 <span class="admin-sub">관리자 수동 반영</span></h3>
      <div style="display:flex;gap:10px;align-items:center;margin-bottom:8px;flex-wrap:wrap;">
        <button class="admin-btn" id="refreshPrefDivBtn" onclick="refreshPreferredDividends()">우선주 배당 시트 새로고침</button>
        <span class="admin-sub">Google Sheet Data!AI 컬럼</span>
      </div>
      <div id="prefDivResult"></div>
      <div id="prefDivCoverageSection" style="margin-top:16px;"></div>
      <div style="display:flex;gap:10px;align-items:center;margin:16px 0 8px;flex-wrap:wrap;">
        <button class="admin-btn" id="refreshFgnDivBtn" onclick="refreshForeignDividends()">해외 배당 yfinance 새로고침</button>
        <span class="admin-sub">trailingAnnualDividendRate → KRW 환산. 수동 입력(아래) 은 덮어쓰지 않음.</span>
      </div>
      <div id="fgnDivResult"></div>
      <div id="fgnDivManualSection" style="margin-top:16px;"></div>
    </div>
  `;
}

// --- 해외 배당 yfinance 새로고침 버튼 --------------------------------

async function refreshForeignDividends() {
  const btn = document.getElementById('refreshFgnDivBtn');
  const result = document.getElementById('fgnDivResult');
  if (!result) return;
  if (btn) btn.disabled = true;
  result.innerHTML = '<div style="color:var(--text-secondary);padding:6px 0;">yfinance 호출 중... (종목당 ~1초)</div>';
  try {
    const res = await apiFetch('/api/admin/refresh-foreign-dividends', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
    });
    if (!res.ok) {
      const data = await res.json().catch(() => ({}));
      result.innerHTML = `<div style="color:var(--color-danger)">실패 (HTTP ${res.status}): ${_esc(data.detail || res.statusText)}</div>`;
      return;
    }
    const data = await res.json();
    if (!data.ok) {
      result.innerHTML = `<div style="color:var(--color-danger)">실패: ${_esc(data.error || '알 수 없음')}</div>`;
      return;
    }
    const failCount = (data.failures || []).length;
    const failNote = failCount ? ` · 실패 ${failCount}건` : '';
    result.innerHTML = `
      <div class="admin-cards" style="margin-top:4px;">
        <div class="admin-card">
          <div class="admin-card-label">쓰여진 행</div>
          <div class="admin-card-value">${data.rows_written}${failNote}</div>
        </div>
        <div class="admin-card">
          <div class="admin-card-label">시도한 종목</div>
          <div class="admin-card-value">${data.total_attempted}</div>
        </div>
        <div class="admin-card">
          <div class="admin-card-label">DB 총 캐시</div>
          <div class="admin-card-value">${data.total_cached ?? '-'}</div>
        </div>
        <div class="admin-card">
          <div class="admin-card-label">소요 시간</div>
          <div class="admin-card-value">${data.elapsed_seconds ?? '-'}s</div>
        </div>
      </div>
    `;
    // 새 값 반영된 목록 다시 로드
    await loadForeignDividendsList();
  } catch (e) {
    result.innerHTML = `<div style="color:var(--color-danger)">요청 실패: ${_esc(e.name + ': ' + e.message)}</div>`;
  } finally {
    if (btn) btn.disabled = false;
  }
}

// --- 해외 배당 수동 CRUD ---------------------------------------------

async function loadForeignDividendsList() {
  const root = document.getElementById('fgnDivManualSection');
  if (!root) return;
  try {
    const res = await apiFetch('/api/admin/foreign-dividends');
    if (!res.ok) {
      root.innerHTML = `<div class="admin-sub" style="color:var(--color-danger)">목록 조회 실패 (HTTP ${res.status})</div>`;
      return;
    }
    const rows = await res.json();
    root.innerHTML = _renderForeignDivTable(rows);
  } catch (e) {
    root.innerHTML = `<div class="admin-sub" style="color:var(--color-danger)">목록 조회 에러: ${_esc(e.message)}</div>`;
  }
}

function _renderForeignDivTable(rows) {
  const manualRows = rows.filter(r => r.source === 'manual');
  const autoRows = rows.filter(r => r.source !== 'manual');
  const rowHtml = (r) => {
    const badge = r.source === 'manual'
      ? '<span class="admin-event-kv admin-status-fail">수동</span>'
      : '<span class="admin-event-kv">yfinance</span>';
    const native = r.dps_native !== null && r.dps_native !== undefined
      ? `${Number(r.dps_native).toFixed(4)} ${_esc(r.currency || '')}`
      : '-';
    const note = r.manual_note ? _esc(r.manual_note) : '';
    return `
      <tr>
        <td><code>${_esc(r.stock_code)}</code> ${badge}</td>
        <td class="admin-num">${r.dps_krw !== null && r.dps_krw !== undefined ? Number(r.dps_krw).toLocaleString() : '-'}</td>
        <td class="admin-sub">${native}</td>
        <td class="admin-sub">${note}</td>
        <td class="admin-sub">${_esc((r.fetched_at || '').slice(0, 16).replace('T', ' '))}</td>
        <td><button class="admin-btn admin-btn-secondary" onclick="deleteForeignDividend('${_esc(r.stock_code)}')">삭제</button></td>
      </tr>
    `;
  };
  const body = [...manualRows, ...autoRows].map(rowHtml).join('')
    || '<tr><td colspan="6" style="text-align:center;color:var(--text-secondary);padding:12px;">등록된 항목이 없습니다. 위 yfinance 새로고침을 실행하거나 아래에서 수동 입력하세요.</td></tr>';
  return `
    <h4 style="margin:0 0 8px;font-size:14px;">해외 배당 수동 입력 / 조회</h4>
    <form id="fgnDivManualForm" onsubmit="event.preventDefault(); submitForeignDividend();"
          style="display:flex;gap:8px;align-items:center;flex-wrap:wrap;margin-bottom:10px;">
      <input type="text" id="fgnDivCode" placeholder="종목코드 (예: AAPL)"
             style="padding:6px 10px;border:1px solid var(--border);border-radius:4px;background:var(--surface);color:var(--text-primary);font-family:monospace;width:140px;">
      <input type="number" id="fgnDivDps" placeholder="연 배당 (원)" step="0.01" min="0"
             style="padding:6px 10px;border:1px solid var(--border);border-radius:4px;background:var(--surface);color:var(--text-primary);width:140px;">
      <input type="text" id="fgnDivNote" placeholder="메모 (선택)"
             style="padding:6px 10px;border:1px solid var(--border);border-radius:4px;background:var(--surface);color:var(--text-primary);width:240px;">
      <button class="admin-btn" type="submit">저장 (수동)</button>
      <span class="admin-sub">수동 저장은 yfinance 새로고침이 덮어쓰지 않습니다.</span>
    </form>
    <div id="fgnDivManualResult" class="admin-sub" style="margin-bottom:6px;"></div>
    <table class="admin-table admin-table-compact">
      <thead><tr><th>종목</th><th>KRW 주당배당</th><th>원본</th><th>메모</th><th>갱신</th><th></th></tr></thead>
      <tbody>${body}</tbody>
    </table>
  `;
}

async function submitForeignDividend() {
  const code = (document.getElementById('fgnDivCode')?.value || '').trim();
  const dpsStr = (document.getElementById('fgnDivDps')?.value || '').trim();
  const note = (document.getElementById('fgnDivNote')?.value || '').trim();
  const result = document.getElementById('fgnDivManualResult');
  if (!code || !dpsStr) {
    if (result) result.innerHTML = '<span style="color:var(--color-danger)">종목코드 + 배당 모두 필수</span>';
    return;
  }
  try {
    const res = await apiFetch('/api/admin/foreign-dividend', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ stock_code: code, dps_krw: parseFloat(dpsStr), note: note || null }),
    });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) {
      if (result) result.innerHTML = `<span style="color:var(--color-danger)">실패 (HTTP ${res.status}): ${_esc(data.detail || res.statusText)}</span>`;
      return;
    }
    if (result) result.innerHTML = `<span style="color:var(--color-success)">${_esc(data.stock_code)} 저장됨 (${data.dps_krw} 원)</span>`;
    document.getElementById('fgnDivCode').value = '';
    document.getElementById('fgnDivDps').value = '';
    document.getElementById('fgnDivNote').value = '';
    await loadForeignDividendsList();
  } catch (e) {
    if (result) result.innerHTML = `<span style="color:var(--color-danger)">에러: ${_esc(e.message)}</span>`;
  }
}

async function deleteForeignDividend(code) {
  if (!confirm(`"${code}" 배당 항목을 삭제할까요?\n(자동 refresh 가 다시 채울 수 있음)`)) return;
  try {
    const res = await apiFetch(`/api/admin/foreign-dividend/${encodeURIComponent(code)}`, { method: 'DELETE' });
    if (!res.ok) {
      const data = await res.json().catch(() => ({}));
      alert(`삭제 실패: ${data.detail || res.statusText}`);
      return;
    }
    await loadForeignDividendsList();
  } catch (e) {
    alert(`삭제 에러: ${e.message}`);
  }
}

async function refreshPreferredDividends() {
  const btn = document.getElementById('refreshPrefDivBtn');
  const result = document.getElementById('prefDivResult');
  if (!result) return;
  if (btn) btn.disabled = true;
  result.innerHTML = '<div style="color:var(--text-secondary);padding:6px 0;">새로고침 중... (시트 다운로드 + 파싱 + upsert)</div>';
  try {
    const res = await apiFetch('/api/admin/refresh-preferred-dividends', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
    });
    if (!res.ok) {
      const data = await res.json().catch(() => ({}));
      result.innerHTML = `<div style="color:var(--color-danger)">실패 (HTTP ${res.status}): ${_esc(data.detail || res.statusText)}</div>`;
      return;
    }
    const data = await res.json();
    if (!data.ok) {
      result.innerHTML = `<div style="color:var(--color-danger)">실패: ${_esc(data.error || '알 수 없음')}</div>`;
      return;
    }
    result.innerHTML = `
      <div class="admin-cards" style="margin-top:4px;">
        <div class="admin-card">
          <div class="admin-card-label">쓰여진 행</div>
          <div class="admin-card-value">${data.rows_written}</div>
        </div>
        <div class="admin-card">
          <div class="admin-card-label">시트 연도</div>
          <div class="admin-card-value">${data.sheet_year ?? '-'}</div>
        </div>
        <div class="admin-card">
          <div class="admin-card-label">DB 총 캐시</div>
          <div class="admin-card-value">${data.total_cached ?? '-'}</div>
        </div>
        <div class="admin-card">
          <div class="admin-card-label">소요 시간</div>
          <div class="admin-card-value">${data.elapsed_seconds ?? '-'}s</div>
        </div>
      </div>
    `;
    await loadPreferredDividendsList();
  } catch (e) {
    result.innerHTML = `<div style="color:var(--color-danger)">요청 실패: ${_esc(e.name + ': ' + e.message)}</div>`;
  } finally {
    if (btn) btn.disabled = false;
  }
}

async function loadPreferredDividendsList() {
  const root = document.getElementById('prefDivCoverageSection');
  if (!root) return;
  try {
    const res = await apiFetch('/api/admin/preferred-dividends');
    if (!res.ok) {
      root.innerHTML = `<div class="admin-sub" style="color:var(--color-danger)">우선주 배당 목록 조회 실패 (HTTP ${res.status})</div>`;
      return;
    }
    _preferredDividendRows = await res.json();
    root.innerHTML = _renderPreferredDividendCoverage(_preferredDividendRows);
  } catch (e) {
    root.innerHTML = `<div class="admin-sub" style="color:var(--color-danger)">우선주 배당 목록 조회 에러: ${_esc(e.message)}</div>`;
  }
}

function _renderPreferredDividendCoverage(rows) {
  const pairMap = _preferredPairMap();
  const mappedCount = rows.filter(r => pairMap[_tickerCode(r.stock_code)]).length;
  const missingCount = Math.max(0, rows.length - mappedCount);
  const rendered = _preferredDividendTableRows(rows);
  return `
    <h4 style="margin:0 0 8px;font-size:14px;">우선주 배당 시트 / pair 연결 상태</h4>
    <div class="admin-cards" style="margin-top:4px;">
      <div class="admin-card">
        <div class="admin-card-label">시트 캐시</div>
        <div class="admin-card-value">${rows.length}</div>
      </div>
      <div class="admin-card">
        <div class="admin-card-label">pair 연결됨</div>
        <div class="admin-card-value">${mappedCount}</div>
      </div>
      <div class="admin-card">
        <div class="admin-card-label">pair 누락 후보</div>
        <div class="admin-card-value">${missingCount}</div>
      </div>
    </div>
    <div class="admin-sub" style="margin:8px 0;">
      배당 시트에는 있는데 pair config 에 없으면 배당액은 알 수 있어도 본주 연결·스프레드 분석 메뉴가 비게 됩니다.
      누락 후보는 아래 버튼으로 pair 입력 폼에 바로 채울 수 있습니다.
    </div>
    <div style="display:flex;gap:8px;align-items:center;margin:8px 0;flex-wrap:wrap;">
      <input id="prefDivSearch" placeholder="시트 우선주 검색: 대덕전자우, 35320K, 본주코드"
             value="${_esc(_preferredDividendFilter)}"
             oninput="filterPreferredDividendList(this.value)"
             style="${_adminInputStyle()}min-width:280px;">
      <span class="admin-sub" id="prefDivVisibleCount">${rendered.visibleCount}/${rows.length}개 표시</span>
    </div>
    <table class="admin-table admin-table-compact">
      <thead><tr><th>우선주</th><th>보통주</th><th>배당</th><th>pair 상태</th><th>갱신</th><th></th></tr></thead>
      <tbody id="prefDivTableBody">${rendered.body}</tbody>
    </table>
  `;
}

function _preferredDividendTableRows(rows) {
  const pairMap = _preferredPairMap();
  const visible = rows.filter(_preferredDividendMatches);
  const body = visible.map(row => {
    const code = _tickerCode(row.stock_code);
    const commonCode = _tickerCode(row.common_code);
    const pair = pairMap[code];
    const status = pair
      ? `<span class="admin-status-ok">연결됨</span><div class="admin-sub">${_esc(pair.commonName || pair.commonTicker || '')}</div>`
      : '<span class="admin-status-fail">pair 누락</span>';
    const action = pair
      ? ''
      : `<button class="admin-btn admin-btn-secondary" onclick="prefillPreferredConfigFromDividend('${_urlArg(code)}','${_urlArg(row.source_name)}','${_urlArg(commonCode)}')">pair 폼에 채우기</button>`;
    const dps = row.dividend_per_share === null || row.dividend_per_share === undefined
      ? '-'
      : Number(row.dividend_per_share).toLocaleString();
    return `
      <tr>
        <td><code>${_esc(code)}</code><div class="admin-sub">${_esc(row.source_name || '')}</div></td>
        <td><code>${_esc(commonCode || '-')}</code></td>
        <td>${dps}<div class="admin-sub">${row.sheet_year ? _esc(String(row.sheet_year)) : ''}</div></td>
        <td>${status}</td>
        <td class="admin-sub">${_esc((row.fetched_at || '').slice(0, 16).replace('T', ' '))}</td>
        <td>${action}</td>
      </tr>
    `;
  }).join('');
  return {
    visibleCount: visible.length,
    body: body || '<tr><td colspan="6" style="text-align:center;color:var(--text-secondary);padding:12px;">표시할 우선주 배당 시트 행이 없습니다.</td></tr>',
  };
}

function filterPreferredDividendList(value) {
  _preferredDividendFilter = value || '';
  const rendered = _preferredDividendTableRows(_preferredDividendRows || []);
  const body = document.getElementById('prefDivTableBody');
  const count = document.getElementById('prefDivVisibleCount');
  if (body) body.innerHTML = rendered.body;
  if (count) count.textContent = `${rendered.visibleCount}/${(_preferredDividendRows || []).length}개 표시`;
}

function _preferredDividendMatches(row) {
  const filter = _normalizePreferredSearch(_preferredDividendFilter);
  if (!filter) return true;
  return _normalizePreferredSearch([
    row.stock_code,
    row.source_name,
    row.common_code,
    _preferredPairMap()[_tickerCode(row.stock_code)] ? '연결됨' : '누락',
  ].join(' ')).includes(filter);
}

function _preferredPairMap() {
  const map = {};
  _currentPreferredRows().forEach(row => {
    const code = _tickerCode(row.preferredTicker || row.preferredCode);
    if (code) map[code] = row;
  });
  return map;
}

function prefillPreferredConfigFromDividend(encodedCode, encodedName, encodedCommonCode) {
  const code = decodeURIComponent(encodedCode || '');
  const preferredName = decodeURIComponent(encodedName || '');
  const commonCode = decodeURIComponent(encodedCommonCode || '');
  const commonName = _guessCommonNameFromPreferredName(preferredName) || commonCode;
  const id = `pref_${code}`.toLowerCase();
  const values = {
    prefCfgIndex: '',
    prefCfgId: id,
    prefCfgName: commonName,
    prefCfgCommonTicker: _ksTicker(commonCode),
    prefCfgPreferredTicker: _ksTicker(code),
    prefCfgCommonName: commonName,
    prefCfgPreferredName: preferredName || code,
  };
  if (_preferredConfigFilter) {
    _preferredConfigFilter = '';
    const search = document.getElementById('prefCfgSearch');
    if (search) search.value = '';
    filterPreferredConfigList('');
  }
  Object.entries(values).forEach(([id, value]) => {
    const el = document.getElementById(id);
    if (el) el.value = value;
  });
  document.getElementById('prefCfgId')?.scrollIntoView({behavior: 'smooth', block: 'center'});
  _showLinkedConfigMessage(`${preferredName || code} pair 입력값을 채웠습니다. 본주명을 확인한 뒤 추가/수정을 누르세요.`, false);
}

function _guessCommonNameFromPreferredName(name) {
  return String(name || '')
    .replace(/\s+/g, '')
    .replace(/[0-9]*우$/, '')
    .replace(/우선주$/, '')
    .trim();
}

function _ksTicker(code) {
  const normalized = _tickerCode(code);
  return normalized ? `${normalized}.KS` : '';
}

function _tickerCode(value) {
  return String(value || '').split('.', 1)[0].trim().toUpperCase();
}

function _urlArg(value) {
  return encodeURIComponent(String(value || ''));
}

// --- AI operations ------------------------------------------------------

function _renderAiConfigSection(config) {
  if (!config) {
    return `
      <div class="admin-section">
        <h3>AI 운영 관리</h3>
        <div class="admin-sub admin-status-fail">AI 설정을 불러오지 못했습니다.</div>
      </div>
    `;
  }
  const key = config.openrouter || {};
  const keyStatus = key.configured
    ? `<span class="admin-status-ok">설정됨</span> <code>${_esc(key.masked || '')}</code> <span class="admin-sub">${_esc(key.source || '')}</span>`
    : '<span class="admin-status-fail">미설정</span>';
  const featureRows = (config.features || []).map(f => `
    <tr>
      <td>${_esc(f.label)}<div class="admin-sub"><code>${_esc(f.key)}</code> · ${_esc(f.source)}</div></td>
      <td>
        <input data-ai-feature="${_esc(f.key)}" value="${_esc(f.model)}" style="${_adminInputStyle()}width:100%;font-family:monospace;">
      </td>
    </tr>
  `).join('');
  const usageRows = (config.usage?.by_feature || []).map(row => `
    <tr>
      <td>${_esc(row.feature)}<div class="admin-sub">${_esc(row.model_profile || '')}</div></td>
      <td><code>${_esc(row.model)}</code></td>
      <td class="admin-num">${Number(row.calls || 0).toLocaleString()}</td>
      <td class="admin-num">${Number(row.input_tokens || 0).toLocaleString()} / ${Number(row.output_tokens || 0).toLocaleString()}</td>
      <td class="admin-num">$${Number(row.cost_usd || 0).toFixed(4)}</td>
      <td class="admin-num">${row.avg_latency_ms ? Math.round(row.avg_latency_ms).toLocaleString() + 'ms' : '-'}</td>
      <td class="${Number(row.errors || 0) > 0 ? 'admin-status-fail' : 'admin-status-ok'}">${Number(row.errors || 0)}</td>
    </tr>
  `).join('');
  return `
    <div class="admin-section" id="aiConfigSection">
      <h3>AI 운영 관리 <span class="admin-sub">키·기능별 모델·사용량</span></h3>
      <div id="aiConfigResult" class="admin-sub" style="margin-bottom:8px;"></div>
      <div class="admin-cards">
        <div class="admin-card">
          <div class="admin-card-label">OpenRouter API Key</div>
          <div class="admin-card-value">${keyStatus}</div>
          <div class="admin-sub">${_esc(key.updated_at || '')} ${_esc(key.updated_by || '')}</div>
        </div>
        <div class="admin-card">
          <div class="admin-card-label">최근 ${config.usage?.days || 30}일 AI 비용</div>
          <div class="admin-card-value">$${_sumAiCost(config).toFixed(4)}</div>
        </div>
      </div>
      <div style="display:flex;gap:8px;align-items:center;margin:12px 0;flex-wrap:wrap;">
        <input id="aiOpenRouterKey" type="password" placeholder="새 OpenRouter API key"
               style="${_adminInputStyle()}min-width:320px;">
        <button class="admin-btn" onclick="saveAiKey()">키 저장/교체</button>
        <button class="admin-btn admin-btn-secondary" onclick="deleteAiKey()">DB 저장 키 삭제</button>
        <span class="admin-sub">화면에는 마스킹만 표시됩니다. env/keys.txt 키는 삭제하지 않습니다.</span>
      </div>
      <table class="admin-table admin-table-compact">
        <thead><tr><th>기능</th><th>사용 모델</th></tr></thead>
        <tbody>${featureRows}</tbody>
      </table>
      <div style="margin-top:8px;">
        <button class="admin-btn" onclick="saveAiModels()">기능별 모델 저장</button>
      </div>
      <div style="margin-top:16px;">
        <strong>사용량</strong>
        <table class="admin-table admin-table-compact" style="margin-top:4px;">
          <thead><tr><th>기능</th><th>모델</th><th>호출</th><th>입력/출력 토큰</th><th>비용</th><th>평균 지연</th><th>오류</th></tr></thead>
          <tbody>${usageRows || '<tr><td colspan="7" style="text-align:center;color:var(--text-secondary)">아직 기록된 AI 사용량이 없습니다.</td></tr>'}</tbody>
        </table>
      </div>
    </div>
  `;
}

function _sumAiCost(config) {
  return (config?.usage?.by_feature || []).reduce((sum, row) => sum + Number(row.cost_usd || 0), 0);
}

function _showAiConfigMessage(message, isError) {
  const el = document.getElementById('aiConfigResult');
  if (el) {
    el.innerHTML = `<span class="${isError ? 'admin-status-fail' : 'admin-status-ok'}">${_esc(message)}</span>`;
  }
}

async function _refreshAiConfigSection() {
  const res = await apiFetch('/api/admin/ai-config');
  const data = await res.json().catch(() => null);
  if (!res.ok || !data) throw new Error(data?.detail || res.statusText || 'AI 설정 갱신 실패');
  _aiAdminConfig = data;
  const section = document.getElementById('aiConfigSection');
  if (section) section.outerHTML = _renderAiConfigSection(_aiAdminConfig);
}

async function saveAiKey() {
  const key = (document.getElementById('aiOpenRouterKey')?.value || '').trim();
  if (!key) {
    _showAiConfigMessage('새 API key를 입력하세요.', true);
    return;
  }
  try {
    const res = await apiFetch('/api/admin/ai-config/key', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ openrouter_api_key: key }),
    });
    const data = await res.json().catch(() => null);
    if (!res.ok) throw new Error(data?.detail || res.statusText || '키 저장 실패');
    _aiAdminConfig = data;
    document.getElementById('aiConfigSection').outerHTML = _renderAiConfigSection(_aiAdminConfig);
    _showAiConfigMessage('AI API key를 저장했습니다.', false);
  } catch (e) {
    _showAiConfigMessage(e.message, true);
  }
}

async function deleteAiKey() {
  if (!confirm('DB에 저장된 OpenRouter key를 삭제할까요? env/keys.txt 값은 그대로 둡니다.')) return;
  try {
    const res = await apiFetch('/api/admin/ai-config/key', { method: 'DELETE' });
    const data = await res.json().catch(() => null);
    if (!res.ok) throw new Error(data?.detail || res.statusText || '키 삭제 실패');
    _aiAdminConfig = data;
    document.getElementById('aiConfigSection').outerHTML = _renderAiConfigSection(_aiAdminConfig);
    _showAiConfigMessage('DB 저장 key를 삭제했습니다.', false);
  } catch (e) {
    _showAiConfigMessage(e.message, true);
  }
}

async function saveAiModels() {
  const models = {};
  document.querySelectorAll('[data-ai-feature]').forEach(input => {
    models[input.getAttribute('data-ai-feature')] = input.value.trim();
  });
  try {
    const res = await apiFetch('/api/admin/ai-config/models', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ models }),
    });
    const data = await res.json().catch(() => null);
    if (!res.ok) throw new Error(data?.detail || res.statusText || '모델 저장 실패');
    _aiAdminConfig = data;
    document.getElementById('aiConfigSection').outerHTML = _renderAiConfigSection(_aiAdminConfig);
    _showAiConfigMessage('기능별 모델을 저장했습니다.', false);
  } catch (e) {
    _showAiConfigMessage(e.message, true);
  }
}

// --- Linked project config management ----------------------------------

function _linkedProjectConfigMap(configs) {
  const map = {};
  (configs || []).forEach(p => { map[p.key] = p; });
  return map;
}

function _projectConfigRows(project) {
  return Array.isArray(project?.config) ? project.config : [];
}

function _projectConfigSourceLabel(project) {
  if (!project?.configLoaded) return '<span class="admin-status-fail">config 없음</span>';
  if (project.source === 'public') return '<span class="admin-status-ok">공개 config fallback</span>';
  if (project.source === 'merged') return '<span class="admin-status-ok">로컬+공개 병합</span>';
  return '<span class="admin-status-ok">로컬 config 로드됨</span>';
}

function _renderProjectConfigBanner(project) {
  if (!project) {
    return '<div class="admin-sub admin-status-fail">설정을 불러오지 못했습니다.</div>';
  }
  const status = _projectConfigSourceLabel(project);
  const writable = project.writable
    ? '<span class="admin-status-ok">저장 가능</span>'
    : '<span class="admin-status-fail">저장 불가</span>';
  const diag = project.diagnostics || {};
  const countNote = project.publicConfigUrl
    ? `<br>로컬 ${diag.localCount ?? 0}개 · 공개 ${diag.publicCount ?? 0}개 · 표시 ${diag.effectiveCount ?? project.summary?.count ?? 0}개`
    : '';
  const driftNote = _renderProjectConfigDrift(project);
  const remoteNote = diag.remoteError
    ? `<br><span class="admin-status-fail">공개 config 확인 실패: ${_esc(diag.remoteError)}</span>`
    : '';
  return `
    <div class="admin-sub" style="margin:4px 0 10px;">
      ${status} · ${writable} · ${_esc(project.repo || '')}
      ${project.configPath ? `<br><code>${_esc(project.configPath)}</code>` : ''}
      ${project.publicConfigUrl ? `<br><code>${_esc(project.publicConfigUrl)}</code>` : ''}
      ${countNote}
      ${remoteNote}
      ${driftNote}
    </div>
  `;
}

function _renderProjectConfigDrift(project) {
  const diag = project?.diagnostics || {};
  const missingLocal = Number(diag.missingLocallyCount || 0);
  const missingPublic = Number(diag.missingPubliclyCount || 0);
  const notes = [];
  if (missingLocal) notes.push(`공개에는 있고 로컬에는 없는 항목 ${missingLocal}개`);
  if (missingPublic) notes.push(`로컬에는 있고 공개에는 없는 항목 ${missingPublic}개`);
  return notes.length
    ? `<br><span class="admin-status-fail">${_esc(notes.join(' · '))}</span>`
    : '';
}

function _renderLinkedProjectConfigSection(configs) {
  const map = _linkedProjectConfigMap(configs);
  return `
    <div class="admin-section" id="linkedProjectConfigSection">
      <h3>서브프로젝트 설정 관리 <span class="admin-sub">config.json 원천 목록 편집</span></h3>
      <div id="linkedConfigResult" class="admin-sub" style="margin-bottom:8px;"></div>
      ${_renderPreferredConfigManager(map.preferredSpread)}
      ${_renderHoldingConfigManager(map.holdingValue)}
      ${_renderGoldConfigManager(map.goldGap)}
    </div>
  `;
}

function _renderPreferredConfigManager(project) {
  const rows = _projectConfigRows(project);
  const rendered = _preferredConfigTableRows(rows);
  return `
    <details open style="margin-top:12px;">
      <summary style="cursor:pointer;font-weight:600;">우선주 pair 목록 <span class="admin-sub" id="prefCfgVisibleCount">${rendered.visibleCount}/${rows.length}개</span></summary>
      ${_renderProjectConfigBanner(project)}
      <div style="display:flex;gap:8px;align-items:center;margin:8px 0;flex-wrap:wrap;">
        <input id="prefCfgSearch" placeholder="pair 검색: 대덕전자우, 대덕전자1우, 35320K"
               value="${_esc(_preferredConfigFilter)}"
               oninput="filterPreferredConfigList(this.value)"
               style="${_adminInputStyle()}min-width:300px;">
        <span class="admin-sub">대덕전자우처럼 1우 표기를 빼고 검색해도 찾습니다.</span>
      </div>
      <form onsubmit="event.preventDefault(); savePreferredConfigItem();" style="display:grid;grid-template-columns:repeat(6,minmax(110px,1fr));gap:8px;margin:10px 0;">
        <input type="hidden" id="prefCfgIndex">
        <input id="prefCfgId" placeholder="id" style="${_adminInputStyle()}">
        <input id="prefCfgName" placeholder="표시명" style="${_adminInputStyle()}">
        <input id="prefCfgCommonTicker" placeholder="본주 ticker" style="${_adminInputStyle()}">
        <input id="prefCfgPreferredTicker" placeholder="우선주 ticker" style="${_adminInputStyle()}">
        <input id="prefCfgCommonName" placeholder="본주명" style="${_adminInputStyle()}">
        <input id="prefCfgPreferredName" placeholder="우선주명" style="${_adminInputStyle()}">
        <button class="admin-btn" type="submit">추가/수정</button>
        <button class="admin-btn admin-btn-secondary" type="button" onclick="resetPreferredConfigForm()">초기화</button>
      </form>
      <table class="admin-table admin-table-compact">
        <thead><tr><th>우선주</th><th>본주</th><th>이름</th><th>작업</th></tr></thead>
        <tbody id="prefCfgTableBody">${rendered.body}</tbody>
      </table>
    </details>
  `;
}

function _preferredConfigTableRows(rows) {
  const visible = rows
    .map((row, idx) => ({row, idx}))
    .filter(({row}) => _preferredConfigMatches(row));
  const body = visible.map(({row, idx}) => `
    <tr>
      <td>
        <code>${_esc(row.preferredTicker)}</code> ${_preferredSourceBadge(row)}
        <div class="admin-sub">${_esc(row.preferredName)}</div>
      </td>
      <td><code>${_esc(row.commonTicker)}</code><div class="admin-sub">${_esc(row.commonName)}</div></td>
      <td>${_esc(row.name)}</td>
      <td>
        <button class="admin-btn admin-btn-secondary" onclick="editPreferredConfigItem(${idx})">수정</button>
        <button class="admin-btn admin-btn-secondary" onclick="deletePreferredConfigItem(${idx})">삭제</button>
      </td>
    </tr>
  `).join('');
  return {
    visibleCount: visible.length,
    body: body || '<tr><td colspan="4" style="text-align:center;color:var(--text-secondary)">목록 없음</td></tr>',
  };
}

function filterPreferredConfigList(value) {
  _preferredConfigFilter = value || '';
  const rows = _currentPreferredRows();
  const rendered = _preferredConfigTableRows(rows);
  const body = document.getElementById('prefCfgTableBody');
  const count = document.getElementById('prefCfgVisibleCount');
  if (body) body.innerHTML = rendered.body;
  if (count) count.textContent = `${rendered.visibleCount}/${rows.length}개`;
}

function _preferredConfigMatches(row) {
  const filter = _normalizePreferredSearch(_preferredConfigFilter);
  if (!filter) return true;
  return _normalizePreferredSearch([
    row.id,
    row.name,
    row.commonTicker,
    row.preferredTicker,
    row.commonName,
    row.preferredName,
  ].join(' ')).includes(filter);
}

function _preferredSourceBadge(row) {
  if (row._configSource === 'public-only') return '<span class="admin-event-kv admin-status-fail">공개만</span>';
  if (row._configSource === 'local-only') return '<span class="admin-event-kv">로컬만</span>';
  if (row._configSource === 'public') return '<span class="admin-event-kv">공개</span>';
  return '';
}

function _normalizePreferredSearch(value) {
  return String(value || '')
    .toUpperCase()
    .replace(/\.KS/g, '')
    .replace(/\s+/g, '')
    .replace(/[._-]/g, '')
    .replace(/[0-9]+우/g, '우');
}

function _renderHoldingConfigManager(project) {
  const rows = _projectConfigRows(project);
  const body = rows.map((row, idx) => {
    const subs = (row.subsidiaries || []).map(s => `${s.name || s.ticker} ${Number(s.sharesHeld || 0).toLocaleString()}주`).join(', ');
    return `
      <tr>
        <td><code>${_esc(row.holdingTicker)}</code><div class="admin-sub">${_esc(row.holdingName)}</div></td>
        <td>${Number(row.holdingTotalShares || 0).toLocaleString()}</td>
        <td>${Number(row.holdingTreasuryShares || 0).toLocaleString()}</td>
        <td class="admin-sub">${_esc(subs)}</td>
        <td>
          <button class="admin-btn admin-btn-secondary" onclick="editHoldingConfigItem(${idx})">수정</button>
          <button class="admin-btn admin-btn-secondary" onclick="deleteHoldingConfigItem(${idx})">삭제</button>
        </td>
      </tr>
    `;
  }).join('');
  return `
    <details style="margin-top:18px;">
      <summary style="cursor:pointer;font-weight:600;">지주사 목록 <span class="admin-sub">${rows.length}개</span></summary>
      ${_renderProjectConfigBanner(project)}
      <form onsubmit="event.preventDefault(); saveHoldingConfigItem();" style="display:grid;grid-template-columns:repeat(3,minmax(120px,1fr));gap:8px;margin:10px 0;">
        <input type="hidden" id="holdingCfgIndex">
        <input id="holdingCfgId" placeholder="id" style="${_adminInputStyle()}">
        <input id="holdingCfgName" placeholder="표시명" style="${_adminInputStyle()}">
        <input id="holdingCfgHoldingName" placeholder="지주사명" style="${_adminInputStyle()}">
        <input id="holdingCfgTicker" placeholder="지주사 ticker" style="${_adminInputStyle()}">
        <input id="holdingCfgTotalShares" type="number" min="0" placeholder="총 발행주식" style="${_adminInputStyle()}">
        <input id="holdingCfgTreasuryShares" type="number" min="0" placeholder="자사주" style="${_adminInputStyle()}">
        <textarea id="holdingCfgSubsidiaries" placeholder='[{"name":"자회사","ticker":"005930.KS","sharesHeld":1000}]' style="${_adminInputStyle()}grid-column:1/-1;min-height:72px;font-family:monospace;"></textarea>
        <button class="admin-btn" type="submit">추가/수정</button>
        <button class="admin-btn admin-btn-secondary" type="button" onclick="resetHoldingConfigForm()">초기화</button>
      </form>
      <table class="admin-table admin-table-compact">
        <thead><tr><th>지주사</th><th>총 발행주식</th><th>자사주</th><th>자회사</th><th>작업</th></tr></thead>
        <tbody>${body || '<tr><td colspan="5" style="text-align:center;color:var(--text-secondary)">목록 없음</td></tr>'}</tbody>
      </table>
    </details>
  `;
}

function _renderGoldConfigManager(project) {
  const jsonText = JSON.stringify(project?.config || {}, null, 2);
  return `
    <details style="margin-top:18px;">
      <summary style="cursor:pointer;font-weight:600;">Gold Gap asset 설정 <span class="admin-sub">${project?.summary?.count || 0}개</span></summary>
      ${_renderProjectConfigBanner(project)}
      <textarea id="goldGapConfigJson" style="${_adminInputStyle()}width:100%;min-height:210px;font-family:monospace;">${_esc(jsonText)}</textarea>
      <div style="margin-top:8px;">
        <button class="admin-btn" onclick="saveGoldGapConfig()">저장</button>
      </div>
    </details>
  `;
}

function _adminInputStyle() {
  return 'padding:6px 10px;border:1px solid var(--border);border-radius:4px;background:var(--surface);color:var(--text-primary);';
}

async function saveLinkedProjectConfig(projectKey, config) {
  const res = await apiFetch(`/api/admin/linked-project-configs/${encodeURIComponent(projectKey)}`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ config }),
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    throw new Error(data.detail || res.statusText || '저장 실패');
  }
  const idx = _linkedProjectConfigs.findIndex(p => p.key === projectKey);
  if (idx >= 0) _linkedProjectConfigs[idx] = data;
  else _linkedProjectConfigs.push(data);
  const section = document.getElementById('linkedProjectConfigSection');
  if (section) section.outerHTML = _renderLinkedProjectConfigSection(_linkedProjectConfigs);
  if (projectKey === 'preferredSpread') {
    const prefDivRoot = document.getElementById('prefDivCoverageSection');
    if (prefDivRoot) prefDivRoot.innerHTML = _renderPreferredDividendCoverage(_preferredDividendRows || []);
  }
  return data;
}

function _showLinkedConfigMessage(message, isError) {
  const el = document.getElementById('linkedConfigResult');
  if (el) {
    el.innerHTML = `<span class="${isError ? 'admin-status-fail' : 'admin-status-ok'}">${_esc(message)}</span>`;
  }
}

function _currentPreferredRows() {
  return _projectConfigRows(_linkedProjectConfigMap(_linkedProjectConfigs).preferredSpread);
}

function editPreferredConfigItem(index) {
  const row = _currentPreferredRows()[index];
  if (!row) return;
  document.getElementById('prefCfgIndex').value = String(index);
  document.getElementById('prefCfgId').value = row.id || '';
  document.getElementById('prefCfgName').value = row.name || '';
  document.getElementById('prefCfgCommonTicker').value = row.commonTicker || '';
  document.getElementById('prefCfgPreferredTicker').value = row.preferredTicker || '';
  document.getElementById('prefCfgCommonName').value = row.commonName || '';
  document.getElementById('prefCfgPreferredName').value = row.preferredName || '';
}

function resetPreferredConfigForm() {
  ['prefCfgIndex', 'prefCfgId', 'prefCfgName', 'prefCfgCommonTicker', 'prefCfgPreferredTicker', 'prefCfgCommonName', 'prefCfgPreferredName']
    .forEach(id => { const el = document.getElementById(id); if (el) el.value = ''; });
}

async function savePreferredConfigItem() {
  const rows = [..._currentPreferredRows()];
  const row = {
    id: document.getElementById('prefCfgId')?.value.trim(),
    name: document.getElementById('prefCfgName')?.value.trim(),
    commonTicker: document.getElementById('prefCfgCommonTicker')?.value.trim(),
    preferredTicker: document.getElementById('prefCfgPreferredTicker')?.value.trim(),
    commonName: document.getElementById('prefCfgCommonName')?.value.trim(),
    preferredName: document.getElementById('prefCfgPreferredName')?.value.trim(),
  };
  const index = parseInt(document.getElementById('prefCfgIndex')?.value || '-1', 10);
  if (Object.values(row).some(v => !v)) {
    _showLinkedConfigMessage('우선주 항목의 모든 필드를 입력하세요.', true);
    return;
  }
  if (index >= 0 && index < rows.length) rows[index] = row;
  else rows.push(row);
  try {
    await saveLinkedProjectConfig('preferredSpread', rows);
    _showLinkedConfigMessage('우선주 목록을 저장했습니다.', false);
  } catch (e) {
    _showLinkedConfigMessage(e.message, true);
  }
}

async function deletePreferredConfigItem(index) {
  const rows = [..._currentPreferredRows()];
  const row = rows[index];
  if (!row || !confirm(`${row.preferredName || row.preferredTicker} 항목을 삭제할까요?`)) return;
  rows.splice(index, 1);
  try {
    await saveLinkedProjectConfig('preferredSpread', rows);
    _showLinkedConfigMessage('우선주 항목을 삭제했습니다.', false);
  } catch (e) {
    _showLinkedConfigMessage(e.message, true);
  }
}

function _currentHoldingRows() {
  return _projectConfigRows(_linkedProjectConfigMap(_linkedProjectConfigs).holdingValue);
}

function editHoldingConfigItem(index) {
  const row = _currentHoldingRows()[index];
  if (!row) return;
  document.getElementById('holdingCfgIndex').value = String(index);
  document.getElementById('holdingCfgId').value = row.id || '';
  document.getElementById('holdingCfgName').value = row.name || '';
  document.getElementById('holdingCfgHoldingName').value = row.holdingName || '';
  document.getElementById('holdingCfgTicker').value = row.holdingTicker || '';
  document.getElementById('holdingCfgTotalShares').value = row.holdingTotalShares || 0;
  document.getElementById('holdingCfgTreasuryShares').value = row.holdingTreasuryShares || 0;
  document.getElementById('holdingCfgSubsidiaries').value = JSON.stringify(row.subsidiaries || [], null, 2);
}

function resetHoldingConfigForm() {
  ['holdingCfgIndex', 'holdingCfgId', 'holdingCfgName', 'holdingCfgHoldingName', 'holdingCfgTicker', 'holdingCfgTotalShares', 'holdingCfgTreasuryShares', 'holdingCfgSubsidiaries']
    .forEach(id => { const el = document.getElementById(id); if (el) el.value = ''; });
}

async function saveHoldingConfigItem() {
  const rows = [..._currentHoldingRows()];
  let subsidiaries;
  try {
    subsidiaries = JSON.parse(document.getElementById('holdingCfgSubsidiaries')?.value || '[]');
  } catch (e) {
    _showLinkedConfigMessage('자회사 JSON 형식이 올바르지 않습니다.', true);
    return;
  }
  const row = {
    id: document.getElementById('holdingCfgId')?.value.trim(),
    name: document.getElementById('holdingCfgName')?.value.trim(),
    holdingName: document.getElementById('holdingCfgHoldingName')?.value.trim(),
    holdingTicker: document.getElementById('holdingCfgTicker')?.value.trim(),
    holdingTotalShares: Number(document.getElementById('holdingCfgTotalShares')?.value || 0),
    holdingTreasuryShares: Number(document.getElementById('holdingCfgTreasuryShares')?.value || 0),
    subsidiaries,
  };
  const index = parseInt(document.getElementById('holdingCfgIndex')?.value || '-1', 10);
  if (!row.id || !row.name || !row.holdingName || !row.holdingTicker) {
    _showLinkedConfigMessage('지주사 항목의 기본 필드를 입력하세요.', true);
    return;
  }
  if (index >= 0 && index < rows.length) rows[index] = row;
  else rows.push(row);
  try {
    await saveLinkedProjectConfig('holdingValue', rows);
    _showLinkedConfigMessage('지주사 목록을 저장했습니다.', false);
  } catch (e) {
    _showLinkedConfigMessage(e.message, true);
  }
}

async function deleteHoldingConfigItem(index) {
  const rows = [..._currentHoldingRows()];
  const row = rows[index];
  if (!row || !confirm(`${row.holdingName || row.holdingTicker} 항목을 삭제할까요?`)) return;
  rows.splice(index, 1);
  try {
    await saveLinkedProjectConfig('holdingValue', rows);
    _showLinkedConfigMessage('지주사 항목을 삭제했습니다.', false);
  } catch (e) {
    _showLinkedConfigMessage(e.message, true);
  }
}

async function saveGoldGapConfig() {
  let config;
  try {
    config = JSON.parse(document.getElementById('goldGapConfigJson')?.value || '{}');
  } catch (e) {
    _showLinkedConfigMessage('Gold Gap JSON 형식이 올바르지 않습니다.', true);
    return;
  }
  try {
    await saveLinkedProjectConfig('goldGap', config);
    _showLinkedConfigMessage('Gold Gap 설정을 저장했습니다.', false);
  } catch (e) {
    _showLinkedConfigMessage(e.message, true);
  }
}

// --- Deploy status ------------------------------------------------------
//
// Shows the commit SHA currently running in this process, when the
// service started, and whether the auto-deploy runner is alive. The
// whole reason this exists is so that "did my push reach prod?" is
// answerable without SSH'ing in.

function _renderDeployCard(d) {
  if (!d) {
    // Endpoint 404 — either the deploy hasn't reached this release yet
    // (old binary), or the route is genuinely missing. Either way the
    // operator learns something useful: this card IS the canary.
    return `
      <div class="admin-section">
        <h3>배포 상태 <span class="admin-sub admin-status-fail">엔드포인트 없음 — 자동 배포 미반영 또는 브라우저 캐시</span></h3>
        <div class="admin-sub" style="padding:8px 0;">
          최신 배포가 반영됐다면 이 카드에 커밋 SHA 가 보여야 합니다.
          Ctrl+Shift+R 로 강제 새로고침 후에도 안 보이면 자동 배포 파이프라인 점검이 필요합니다.
        </div>
      </div>
    `;
  }
  const b = d.build || {};
  const runner = d.actions_runner || {};
  const runnerCls = runner.active ? 'admin-status-ok' : 'admin-status-fail';
  const runnerIcon = runner.active ? '✓' : '✗';
  return `
    <div class="admin-section">
      <h3>배포 상태</h3>
      <div class="admin-cards">
        <div class="admin-card">
          <div class="admin-card-label">현재 커밋</div>
          <div class="admin-card-value"><code>${_esc(b.short_sha || '-')}</code></div>
          <div class="admin-sub" title="${_esc(b.sha || '')}">${_esc(b.subject || '')}</div>
        </div>
        <div class="admin-card">
          <div class="admin-card-label">커밋 시각</div>
          <div class="admin-card-value">${_esc(_fmtBuildTime(b.committed_at))}</div>
        </div>
        <div class="admin-card">
          <div class="admin-card-label">서비스 시작</div>
          <div class="admin-card-value">${_esc(_fmtBuildTime(d.service_started))}</div>
        </div>
        <div class="admin-card">
          <div class="admin-card-label">자동 배포 러너</div>
          <div class="admin-card-value ${runnerCls}">${runnerIcon} ${runner.active ? '동작 중' : '중지됨'}</div>
          <div class="admin-sub">${_esc(runner.name || '')}</div>
        </div>
      </div>
    </div>
  `;
}

function _fmtBuildTime(s) {
  if (!s) return '-';
  try {
    const d = new Date(s);
    if (isNaN(d.getTime())) return s;  // raw pass-through if not ISO
    const yyyy = d.getFullYear();
    const mm = String(d.getMonth() + 1).padStart(2, '0');
    const dd = String(d.getDate()).padStart(2, '0');
    const hh = String(d.getHours()).padStart(2, '0');
    const mn = String(d.getMinutes()).padStart(2, '0');
    return `${yyyy}-${mm}-${dd} ${hh}:${mn}`;
  } catch (_) { return s; }
}

function _renderServerCard(s) {
  const memTotal = s.memory?.MemTotal || 0;
  const memAvail = s.memory?.MemAvailable || 0;
  const memUsed = memTotal - memAvail;
  const memPct = memTotal ? Math.round(memUsed / memTotal * 100) : 0;
  const diskTotal = s.disk?.total || 0;
  const diskUsed = s.disk?.used || 0;
  const diskPct = diskTotal ? Math.round(diskUsed / diskTotal * 100) : 0;

  // True CPU utilization (server samples /proc/stat); fall back to loadavg
  // estimate only if cpu_pct is missing.
  let loadPct;
  if (typeof s.cpu_pct === 'number') {
    loadPct = Math.round(s.cpu_pct);
  } else {
    const loadParts = (s.load_avg || '').split(' ');
    const load1m = parseFloat(loadParts[0]) || 0;
    loadPct = Math.round(load1m / 4 * 100);
  }

  return `
    <div class="admin-section">
      <h3>서버 상태</h3>
      <div class="admin-cards">
        <div class="admin-card">
          <div class="admin-card-label">가동 시간</div>
          <div class="admin-card-value">${s.uptime || '-'}</div>
        </div>
        <div class="admin-card" id="adminCpuTemp">
          <div class="admin-card-label">CPU Temp</div>
          <div class="admin-card-value">${s.cpu_temp != null ? s.cpu_temp.toFixed(1) + '°C' : '-'}</div>
          ${s.cpu_temp != null ? _progressBar(Math.round(s.cpu_temp / 85 * 100)) : ''}
        </div>
        <div class="admin-card" id="adminCpuLoad">
          <div class="admin-card-label">CPU 사용률</div>
          <div class="admin-card-value">${loadPct}%</div>
          ${_progressBar(loadPct)}
        </div>
        <div class="admin-card" id="adminMemory">
          <div class="admin-card-label">메모리</div>
          <div class="admin-card-value">${_fmtBytes(memUsed)} / ${_fmtBytes(memTotal)} (${memPct}%)</div>
          ${_progressBar(memPct)}
        </div>
        <div class="admin-card">
          <div class="admin-card-label">디스크</div>
          <div class="admin-card-value">${_fmtBytes(diskUsed)} / ${_fmtBytes(diskTotal)}</div>
          ${_progressBar(diskPct)}
        </div>
      </div>
    </div>
  `;
}

// --- Live updates every 5s for CPU temp, load, memory ---

function _startLiveUpdates() {
  if (_liveInterval) clearInterval(_liveInterval);
  _liveInterval = setInterval(_updateLiveStats, 5000);
}

async function _updateLiveStats() {
  try {
    const res = await apiFetch('/api/admin/server-stats');
    const s = await res.json();

    // CPU Temp
    const tempEl = document.getElementById('adminCpuTemp');
    if (tempEl && s.cpu_temp != null) {
      const tempPct = Math.round(s.cpu_temp / 85 * 100);
      tempEl.querySelector('.admin-card-value').textContent = s.cpu_temp.toFixed(1) + '°C';
      tempEl.querySelector('.admin-progress-fill').style.width = tempPct + '%';
      tempEl.querySelector('.admin-progress-fill').style.background = _progressColor(tempPct);
    }

    // CPU Load (true utilization from /proc/stat)
    const loadEl = document.getElementById('adminCpuLoad');
    if (loadEl) {
      let loadPct;
      if (typeof s.cpu_pct === 'number') {
        loadPct = Math.round(s.cpu_pct);
      } else {
        const loadParts = (s.load_avg || '').split(' ');
        const load1m = parseFloat(loadParts[0]) || 0;
        loadPct = Math.round(load1m / 4 * 100);
      }
      loadEl.querySelector('.admin-card-value').textContent = loadPct + '%';
      loadEl.querySelector('.admin-progress-fill').style.width = loadPct + '%';
      loadEl.querySelector('.admin-progress-fill').style.background = _progressColor(loadPct);
    }

    // Memory
    const memEl = document.getElementById('adminMemory');
    if (memEl) {
      const memTotal = s.memory?.MemTotal || 0;
      const memAvail = s.memory?.MemAvailable || 0;
      const memUsed = memTotal - memAvail;
      const memPct = memTotal ? Math.round(memUsed / memTotal * 100) : 0;
      memEl.querySelector('.admin-card-value').textContent = `${_fmtBytes(memUsed)} / ${_fmtBytes(memTotal)} (${memPct}%)`;
      memEl.querySelector('.admin-progress-fill').style.width = memPct + '%';
      memEl.querySelector('.admin-progress-fill').style.background = _progressColor(memPct);
    }
  } catch (e) {
    // Silently ignore live update failures
  }
}

function _progressColor(pct) {
  return pct > 85 ? 'var(--color-danger, #e74c3c)' : pct > 60 ? 'var(--color-warning, #f39c12)' : 'var(--color-success, #27ae60)';
}

// --- Batch section ---

function _renderBatchSection(jobs) {
  const rows = jobs.map(j => {
    // Execution status = "did systemd exit 0". Kept for visibility into
    // the process layer (did the script even run?), but it no longer
    // stands alone — see the 최신 데이터 column below for "did the
    // script actually write anything useful?".
    const statusIcon = j.status === 'success' ? '✓' : j.status === 'failed' ? '✗' : j.status === 'running' ? '⟳' : '—';
    const statusClass = j.status === 'success' ? 'admin-status-ok' : j.status === 'failed' ? 'admin-status-fail' : j.status === 'running' ? 'admin-status-run' : '';
    const lastRun = j.last_start ? _fmtTimestamp(j.last_start) : '-';
    const nextRun = j.next_run ? _fmtTimestamp(j.next_run) : '-';

    // 최신 데이터 — the column that would have made 4/17 NPS miss
    // obvious from the dashboard instead of a "success" check mark.
    const stale = j.staleness || {};
    let dataCell;
    if (stale.level === 'missing') {
      dataCell = `<span class="admin-status-fail">✗ ${_esc(stale.note || '데이터 없음')}</span>`;
    } else if (stale.level === 'stale') {
      dataCell = `<span class="admin-status-fail">⚠ ${_esc(stale.note || '지연')}</span>`;
    } else {
      dataCell = `<span class="admin-status-ok">${_esc(stale.note || j.latest_data_date || '-')}</span>`;
    }

    return `
      <tr>
        <td><strong>${j.label}</strong><div class="admin-sub">${j.schedule}</div></td>
        <td class="${statusClass}">${statusIcon} ${_statusLabel(j.status)}</td>
        <td>${dataCell}</td>
        <td>${lastRun}</td>
        <td>${nextRun}</td>
        <td>
          <button class="admin-btn" onclick="triggerJob('${j.name}')" ${j.status === 'running' ? 'disabled' : ''}>실행</button>
          <button class="admin-btn admin-btn-secondary" onclick="triggerJobWithDate('${j.name}')">날짜 지정</button>
        </td>
      </tr>
    `;
  }).join('');

  return `
    <div class="admin-section">
      <h3>배치 작업 <span class="admin-sub">실행 상태 ≠ 데이터 상태 — 둘 다 확인</span></h3>
      <table class="admin-table">
        <thead><tr>
          <th>작업</th>
          <th>실행 상태</th>
          <th>최신 데이터</th>
          <th>최근 실행</th>
          <th>다음 실행</th>
          <th>수동 실행</th>
        </tr></thead>
        <tbody>${rows}</tbody>
      </table>
    </div>
  `;
}

// --- Users section ---

function _renderUsersSection(users) {
  const rows = users.map(u => `
    <tr>
      <td>${_esc(u.name)}</td>
      <td>${_esc(u.email)}</td>
      <td>${u.is_admin ? '관리자' : '일반'}</td>
      <td>${u.last_login_at ? u.last_login_at.slice(0, 16).replace('T', ' ') : '-'}</td>
      <td>${u.created_at ? u.created_at.slice(0, 10) : '-'}</td>
    </tr>
  `).join('');
  return `
    <div class="admin-section">
      <h3>사용자 <span class="admin-sub">${users.length}명</span></h3>
      <table class="admin-table">
        <thead><tr><th>이름</th><th>이메일</th><th>역할</th><th>최근 로그인</th><th>가입일</th></tr></thead>
        <tbody>${rows}</tbody>
      </table>
    </div>
  `;
}

// --- DB section ---

function _renderDbSection(db) {
  const tables = Object.entries(db.tables || {}).sort((a, b) => b[1] - a[1]);
  const rows = tables.map(([name, count]) => `<tr><td>${name}</td><td class="admin-num">${count.toLocaleString()}</td></tr>`).join('');
  return `
    <div class="admin-section">
      <h3>데이터베이스 <span class="admin-sub">${_fmtBytes(db.db_size_bytes || 0)}</span></h3>
      <table class="admin-table admin-table-compact">
        <thead><tr><th>테이블</th><th>행 수</th></tr></thead>
        <tbody>${rows}</tbody>
      </table>
    </div>
  `;
}

// --- Actions ---

async function triggerJob(jobName) {
  try {
    const res = await apiFetch(`/api/admin/trigger/${jobName}`, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({}),
    });
    const data = await res.json();
    if (!res.ok) {
      alert(data.detail || '실행 실패');
      return;
    }
    alert(data.message || '실행 시작');
    setTimeout(loadAdminView, 2000);
  } catch (e) {
    alert('실행 요청 실패: ' + e.message);
  }
}

async function triggerJobWithDate(jobName) {
  const dateStr = prompt('실행할 날짜를 입력하세요 (YYYY-MM-DD):', new Date().toISOString().slice(0, 10));
  if (!dateStr) return;
  if (!/^\d{4}-\d{2}-\d{2}$/.test(dateStr)) {
    alert('올바른 날짜 형식이 아닙니다.');
    return;
  }
  try {
    const res = await apiFetch(`/api/admin/trigger/${jobName}`, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({date: dateStr}),
    });
    const data = await res.json();
    if (!res.ok) {
      alert(data.detail || '실행 실패');
      return;
    }
    alert(`${data.message} (${dateStr})`);
    setTimeout(loadAdminView, 2000);
  } catch (e) {
    alert('실행 요청 실패: ' + e.message);
  }
}

// --- Helpers ---

function _fmtBytes(bytes) {
  if (bytes >= 1073741824) return (bytes / 1073741824).toFixed(1) + ' GB';
  if (bytes >= 1048576) return (bytes / 1048576).toFixed(1) + ' MB';
  if (bytes >= 1024) return (bytes / 1024).toFixed(1) + ' KB';
  return bytes + ' B';
}

function _fmtTimestamp(ts) {
  if (!ts || ts === 'n/a') return '-';
  const m = ts.match(/(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2})/);
  if (m) return `${m[1]} ${m[2]}`;
  return ts.slice(0, 16);
}

function _statusLabel(s) {
  const labels = {success: '성공', failed: '실패', running: '실행 중', idle: '대기', error: '오류'};
  return labels[s] || s;
}

function _progressBar(pct) {
  const color = _progressColor(pct);
  return `<div class="admin-progress"><div class="admin-progress-fill" style="width:${pct}%;background:${color}"></div></div>`;
}

function _esc(str) {
  const el = document.createElement('span');
  el.textContent = str || '';
  return el.innerHTML;
}

// --- Subsystem summary card strip ---------------------------------------
//
// One tile per known subsystem showing "last event time + outcome + counts
// over the last 24h". Catches the "nothing has happened for 3 days" case
// without having to scroll the event list.

// Anomaly detector for subsystem details payloads. Returns an array of
// short strings describing things that should NEVER be >0 in a healthy
// run. These surface as warning chips on the tile so the operator spots
// them without opening the raw JSON.
//
// Add rules here per subsystem as new silent-failure modes are found.
function _detectAnomalies(source, detailsObj) {
  if (!detailsObj || typeof detailsObj !== 'object') return [];
  const flags = [];
  if (source === 'wiki_ingestion') {
    const sbr = detailsObj.skipped_by_reason || {};
    // Whitelist rejection means we found a PDF we COULD have summarized
    // but refused the host — config bug, not a dataset limit. Always 0
    // in a healthy state.
    if ((sbr.rejected_by_whitelist || 0) > 0) {
      flags.push(`화이트리스트 탈락 ${sbr.rejected_by_whitelist}건`);
    }
    if ((detailsObj.failed || 0) > 0) {
      flags.push(`실패 ${detailsObj.failed}건`);
    }
  }
  if (source === 'snapshot_nav' || source === 'snapshot_intraday') {
    const failed = detailsObj.users_failed;
    if (Array.isArray(failed) && failed.length > 0) {
      flags.push(`사용자 ${failed.length}명 실패`);
    }
  }
  return flags;
}

function _renderSubsystemSummary(summary) {
  const bySource = summary.by_source || {};
  const latest = summary.latest || {};
  const labels = {
    snapshot_nav:       '포트폴리오 NAV 스냅샷',
    snapshot_intraday:  '장중 스냅샷',
    snapshot_nps:       '국민연금 스냅샷',
    wiki_ingestion:     '위키 인제션 루프',
    benchmark_history:  '벤치마크 일별 증분',
  };
  const tiles = Object.entries(labels).map(([src, label]) => {
    const row = latest[src];
    const counts = bySource[src] || {};
    const info = counts.info || 0;
    const warn = counts.warning || 0;
    const err = counts.error || 0;
    const anomalies = row ? _detectAnomalies(src, row.details_obj) : [];
    // Anomalies force a visible warning even when level='info' —
    // because wiki_ingestion tick WAS logged as info while silently
    // dropping 51 reports to whitelist rejection.
    const hasIssue = err > 0 || anomalies.length > 0;
    const hasWarn = warn > 0;
    const barCls = err > 0 ? 'admin-status-fail' : (hasIssue || hasWarn) ? 'admin-status-run' : 'admin-status-ok';
    const icon = err > 0 ? '✗' : (hasIssue || hasWarn) ? '⚠' : '✓';
    const lastLabel = row ? `${_esc(row.kind)} · ${_fmtRelTime(row.ts)}` : '<span style="color:var(--text-secondary)">이벤트 없음</span>';
    const anomalyChips = anomalies.length
      ? `<div class="admin-anomaly-chips">${anomalies.map(f => `<span class="admin-event-kv admin-status-fail">${_esc(f)}</span>`).join(' ')}</div>`
      : '';
    return `
      <div class="admin-card">
        <div class="admin-card-label">${_esc(label)}</div>
        <div class="admin-card-value ${barCls}">${icon} ${lastLabel}</div>
        <div class="admin-sub">24h: info ${info} · warn ${warn} · error ${err}</div>
        ${anomalyChips}
      </div>
    `;
  }).join('');
  return `
    <div class="admin-section">
      <h3>서브시스템 상태 <span class="admin-sub">최근 24시간</span></h3>
      <div class="admin-cards">${tiles}</div>
    </div>
  `;
}

// --- Event feed ---------------------------------------------------------

function _renderEventsSection(events) {
  const rows = events.map(e => {
    const anomalies = _detectAnomalies(e.source, e.details_obj);
    // Level taxonomy (error/warning/info) comes from the writer, but the
    // event table also amplifies anomalies so that e.g. a wiki_ingestion
    // tick with rejected_by_whitelist>0 shows up red even if the writer
    // tagged it info for backwards compat. Anomalies trump level.
    const effectiveLevel = anomalies.length > 0
      ? 'warning'
      : e.level;
    const lvlCls = effectiveLevel === 'error' ? 'admin-status-fail'
                 : effectiveLevel === 'warning' ? 'admin-status-run'
                 : 'admin-status-ok';
    const icon = effectiveLevel === 'error' ? '✗' : effectiveLevel === 'warning' ? '⚠' : '·';
    let detailsPreview = '';
    if (e.details_obj) {
      try {
        const keys = Object.keys(e.details_obj);
        detailsPreview = keys.slice(0, 4).map(k => {
          let v = e.details_obj[k];
          if (typeof v === 'object' && v !== null) v = JSON.stringify(v);
          const s = String(v);
          // Highlight key-value pairs that pattern-match an anomaly —
          // e.g. {rejected_by_whitelist: 51} should pop even inside the
          // condensed preview row.
          const isAnomaly = k === 'skipped_by_reason' && /rejected_by_whitelist"?\s*:\s*[1-9]/.test(s);
          const extraCls = isAnomaly ? ' admin-status-fail' : '';
          return `<span class="admin-event-kv${extraCls}">${_esc(k)}=${_esc(s.length > 40 ? s.slice(0,40)+'…' : s)}</span>`;
        }).join(' ');
      } catch (_) { detailsPreview = ''; }
    }
    return `
      <tr>
        <td><span class="admin-sub">${_fmtEventTs(e.ts)}</span></td>
        <td class="${lvlCls}">${icon} ${_esc(effectiveLevel)}</td>
        <td><strong>${_esc(e.source)}</strong></td>
        <td>${_esc(e.kind)}</td>
        <td>${e.stock_code ? `<code>${_esc(e.stock_code)}</code>` : ''}</td>
        <td class="admin-event-details">${detailsPreview}</td>
      </tr>
    `;
  }).join('');
  return `
    <div class="admin-section">
      <h3>최근 이벤트 <span class="admin-sub">${events.length}건</span>
        <button class="admin-btn admin-btn-secondary" onclick="loadAdminView()" style="float:right;">새로고침</button>
      </h3>
      <table class="admin-table admin-table-compact">
        <thead><tr><th>시각</th><th>레벨</th><th>소스</th><th>종류</th><th>종목</th><th>상세</th></tr></thead>
        <tbody>${rows || '<tr><td colspan="6" style="text-align:center;color:var(--text-secondary)">이벤트 없음</td></tr>'}</tbody>
      </table>
    </div>
  `;
}

// --- Wiki diagnostic form ----------------------------------------------
//
// "이 종목 위키 왜 안 늘어" 를 한 번에 조회. code 입력 → /api/admin/diag/wiki
// 호출 → 결과 패널에 depositional Naver funnel + DB 상태 + verdict 출력.

function _renderDiagSection() {
  return `
    <div class="admin-section">
      <h3>위키 진단 <span class="admin-sub">종목별 파이프라인 funnel</span></h3>
      <form id="diagWikiForm" onsubmit="event.preventDefault(); runWikiDiag();" style="display:flex;gap:8px;align-items:center;margin-bottom:12px;">
        <input id="diagWikiCode" type="text" placeholder="종목코드 (예: 051910)" style="padding:6px 10px;border:1px solid var(--border);border-radius:4px;background:var(--surface);color:var(--text-primary);font-family:monospace;width:140px;">
        <button class="admin-btn" type="submit">진단 실행</button>
        <span class="admin-sub">Naver 응답·화이트리스트·DB 상태를 한 번에 확인합니다.</span>
      </form>
      <div id="diagWikiResult"></div>
    </div>
  `;
}

async function runWikiDiag() {
  const code = (document.getElementById('diagWikiCode')?.value || '').trim();
  const result = document.getElementById('diagWikiResult');
  if (!result) return;
  if (!code) {
    result.innerHTML = '<div style="color:var(--color-danger)">종목 코드를 입력하세요.</div>';
    return;
  }
  result.innerHTML = '<div style="color:var(--text-secondary);padding:8px;">진단 중... (Naver 스크랩 포함하여 최대 15초 정도 소요)</div>';
  try {
    const res = await apiFetch(`/api/admin/diag/wiki?code=${encodeURIComponent(code)}`);
    if (!res.ok) {
      const data = await res.json().catch(() => ({}));
      const detail = data.detail || res.statusText || '';
      // Surface the HTTP status too — "진단 실패" 단독으론 원인 파악이
      // 어려웠음. 401/403 = 세션 문제, 404 = 엔드포인트 누락 (옛 배포),
      // 500/502/504 = 서버 내부/프록시 문제 등 상태코드 하나로 분류 가능.
      result.innerHTML = `<div style="color:var(--color-danger)">진단 실패 (HTTP ${res.status}): ${_esc(detail)}</div>`;
      return;
    }
    const data = await res.json();
    result.innerHTML = _renderWikiDiagResult(data);
  } catch (e) {
    result.innerHTML = `<div style="color:var(--color-danger)">진단 요청 실패: ${_esc(e.name + ': ' + e.message)}</div>`;
  }
}

function _renderWikiDiagResult(d) {
  const n = d.naver || {};
  const db = d.db || {};
  const byStatus = db.pdf_cache_by_status || {};
  const statusChips = Object.entries(byStatus).map(([k, v]) => {
    const cls = k === 'parsed' ? 'admin-status-ok' : 'admin-status-fail';
    return `<span class="admin-event-kv ${cls}">${_esc(k)}: ${v}</span>`;
  }).join(' ') || '<span class="admin-sub">없음</span>';

  const sampleRows = (n.samples || []).map(s => {
    const statusCls = s.status === 'ok' ? 'admin-status-ok' : 'admin-status-fail';
    return `
      <tr>
        <td>${_esc(s.date || '-')}</td>
        <td>${_esc(s.firm || '-')}</td>
        <td>${_esc(s.title || '-')}</td>
        <td class="${statusCls}">${_esc(s.status)}</td>
      </tr>
    `;
  }).join('');

  const failuresRows = (db.recent_failures || []).map(f => `
    <tr>
      <td class="admin-status-fail">${_esc(f.status)}</td>
      <td style="word-break:break-all">${_esc(f.error)}</td>
    </tr>
  `).join('');

  return `
    <div class="admin-diag-verdict"><strong>${_esc(d.stock_code)}</strong> — ${_esc(d.verdict)}</div>
    <div class="admin-cards" style="margin-top:12px;">
      <div class="admin-card">
        <div class="admin-card-label">Naver total</div>
        <div class="admin-card-value">${n.total ?? 0}</div>
      </div>
      <div class="admin-card">
        <div class="admin-card-label">pdf_url 보유</div>
        <div class="admin-card-value">${n.has_pdf ?? 0}</div>
      </div>
      <div class="admin-card">
        <div class="admin-card-label">화이트리스트 통과</div>
        <div class="admin-card-value">${n.passes_whitelist ?? 0}</div>
      </div>
      <div class="admin-card">
        <div class="admin-card-label">DB 위키 엔트리</div>
        <div class="admin-card-value">${db.wiki_entries ?? 0}</div>
      </div>
    </div>
    <div style="margin-top:12px;">
      <strong>PDF 캐시 상태:</strong> ${statusChips}
    </div>
    <div style="margin-top:16px;">
      <strong>Naver 샘플 (상위 10건)</strong>
      <table class="admin-table admin-table-compact" style="margin-top:4px;">
        <thead><tr><th>날짜</th><th>증권사</th><th>제목</th><th>상태</th></tr></thead>
        <tbody>${sampleRows || '<tr><td colspan="4" style="text-align:center;color:var(--text-secondary)">없음</td></tr>'}</tbody>
      </table>
    </div>
    ${failuresRows ? `
      <div style="margin-top:12px;">
        <strong>최근 실패</strong>
        <table class="admin-table admin-table-compact">
          <thead><tr><th>상태</th><th>에러</th></tr></thead>
          <tbody>${failuresRows}</tbody>
        </table>
      </div>` : ''}
  `;
}

function _fmtRelTime(iso) {
  if (!iso) return '-';
  try {
    const d = new Date(iso);
    const diff = (Date.now() - d.getTime()) / 1000;
    if (diff < 60) return '방금 전';
    if (diff < 3600) return `${Math.round(diff/60)}분 전`;
    if (diff < 86400) return `${Math.round(diff/3600)}시간 전`;
    return `${Math.round(diff/86400)}일 전`;
  } catch (_) { return iso; }
}

function _fmtEventTs(iso) {
  if (!iso) return '-';
  // Compact: "04-18 18:42" for today/recent, "2026-04-10" for older
  try {
    const d = new Date(iso);
    const now = new Date();
    if (d.toDateString() === now.toDateString()) {
      return d.toTimeString().slice(0, 5);
    }
    const mm = String(d.getMonth() + 1).padStart(2, '0');
    const dd = String(d.getDate()).padStart(2, '0');
    const hh = String(d.getHours()).padStart(2, '0');
    const mn = String(d.getMinutes()).padStart(2, '0');
    return `${mm}-${dd} ${hh}:${mn}`;
  } catch (_) { return iso; }
}
