// Admin linked-project config (preferredSpread/holdingValue/goldGap) and
// preferred/foreign dividend administration: sheet/yfinance refresh, manual
// CRUD, and pair coverage. Split from static/js/admin.js.

let _linkedProjectConfigs = [];
let _preferredConfigFilter = '';
let _preferredDividendFilter = '';
let _preferredDividendRows = [];

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
      showToast(`삭제 실패: ${data.detail || res.statusText}`);
      return;
    }
    await loadForeignDividendsList();
  } catch (e) {
    reportApiError(e, '삭제');
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
  const syncNote = _renderProjectConfigSync(project);
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
      ${syncNote}
      ${remoteNote}
      ${driftNote}
    </div>
  `;
}

function _renderProjectConfigSync(project) {
  const sync = project?.diagnostics?.sync || {};
  if (sync.updated) {
    return `<br><span class="admin-status-ok">공개 config 기준으로 로컬 파일 갱신: ${Number(sync.addedFromPublicCount || 0)}개 추가</span>`;
  }
  if (sync.error) {
    return `<br><span class="admin-status-fail">로컬 config 자동 갱신 실패: ${_esc(sync.error)}</span>`;
  }
  return '';
}

function _renderProjectConfigDrift(project) {
  const diag = project?.diagnostics || {};
  const missingLocal = Number(diag.missingLocallyCount || 0);
  const missingPublic = Number(diag.missingPubliclyCount || 0);
  const notes = [];
  if (missingLocal) notes.push(`로컬 config 갱신 필요 ${missingLocal}개`);
  if (missingPublic) notes.push(`공개 배포 대기 ${missingPublic}개`);
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
        <code>${_esc(row.preferredTicker)}</code>
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
