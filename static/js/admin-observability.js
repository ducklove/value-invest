// Admin observability dashboard: batch/users/db/events/HTTP panels, the 5s
// live refresh loop, manual job triggers, and the wiki diagnostic form.
// Split from static/js/admin.js to keep panels maintainable. (배포/서버
// 카드는 admin.js 의 운영 콘솔 KPI + admin-charts.js 타임라인으로 대체됨.)

let _liveInterval = null;

// --- HTTP performance (latency observer) --------------------------------
//
// 요청 계측 미들웨어가 /api/* 의 느린 요청(>= SLOW_REQUEST_MS)과 5xx 만
// system_events(source="http") 에 남긴다. 여기서는 endpoint 별로 묶어
// "어디가 느리고 어디가 실패하는지"를 한눈에 본다. 정상·빠른 요청은
// 기록되지 않으므로 평균 응답시간이 아니라 "문제 구간"으로 읽어야 한다.

function _renderHttpMetricsSection(httpMetrics) {
  const endpoints = (httpMetrics && httpMetrics.endpoints) || [];
  const hours = (httpMetrics && httpMetrics.hours) || 24;
  const rows = endpoints.map(e => {
    const errCls = (e.errors || 0) > 0 ? 'admin-status-fail' : '';
    const maxCls = (e.max_ms != null && e.max_ms >= 3000) ? 'admin-status-fail'
                 : (e.max_ms != null && e.max_ms >= 1000) ? 'admin-status-run'
                 : '';
    const fmt = (v) => (v == null ? '-' : Math.round(v).toLocaleString());
    // 수치 정렬은 admin-num 클래스(inline style 은 모바일 카드 변환의
    // text-align 재정의를 막아서 금지), data-label 은 카드 라벨용.
    return `
      <tr>
        <td class="admin-cell-full"><code>${_esc(e.path || '(unknown)')}</code></td>
        <td data-label="건수" class="admin-num">${e.count || 0}</td>
        <td data-label="에러" class="admin-num ${errCls}">${e.errors || 0}</td>
        <td data-label="평균 (ms)" class="admin-num">${fmt(e.avg_ms)}</td>
        <td data-label="최대 (ms)" class="admin-num ${maxCls}">${fmt(e.max_ms)}</td>
        <td data-label="최근"><span class="admin-sub">${e.last_ts ? _fmtRelTime(e.last_ts) : '-'}</span></td>
      </tr>`;
  }).join('');
  return `
    <details class="admin-section admin-collapsible" id="httpMetricsSection">
      ${_adminCollapsibleSummary('HTTP 성능', `느린 요청·에러 / 최근 ${hours}시간 (ms)`, `${endpoints.length}개 경로`)}
      <div class="admin-collapsible-body">
        <div style="display:flex;align-items:center;justify-content:space-between;gap:8px;margin-bottom:8px;flex-wrap:wrap;">
          <p class="admin-sub" style="margin:0;">느린 요청(≥SLOW_REQUEST_MS)과 5xx 만 기록됩니다. 정상·빠른 요청은 집계에 없습니다.</p>
          <button class="admin-btn admin-btn-secondary" onclick="loadAdminView()" type="button">새로고침</button>
        </div>
        <div class="admin-table-wrap">
          <table class="admin-table admin-table-compact admin-table-cards">
            <thead><tr><th>경로</th><th style="text-align:right;">건수</th><th style="text-align:right;">에러</th><th style="text-align:right;">평균</th><th style="text-align:right;">최대</th><th>최근</th></tr></thead>
            <tbody>${rows || '<tr><td colspan="6" style="text-align:center;color:var(--text-secondary)">기록된 느린 요청/에러 없음</td></tr>'}</tbody>
          </table>
        </div>
      </div>
    </details>
  `;
}

// --- Live updates every 5s for CPU temp, load, memory ---

function _startLiveUpdates() {
  if (_liveInterval) clearInterval(_liveInterval);
  _liveInterval = setInterval(_updateLiveStats, 5000);
}

async function _updateLiveStats() {
  try {
    const s = await apiFetchJson('/api/admin/server-stats');
    _pushServerSample(s);
    _renderServerTimeline();

    // CPU Temp
    const tempEl = document.getElementById('adminCpuTemp');
    if (tempEl && s.cpu_temp != null) {
      const tempPct = Math.round(s.cpu_temp / 85 * 100);
      const valueEl = tempEl.querySelector('.admin-card-value, .admin-kpi-value');
      if (valueEl) valueEl.textContent = s.cpu_temp.toFixed(1) + '°C';
      const fill = tempEl.querySelector('.admin-progress-fill');
      if (fill) {
        fill.style.width = tempPct + '%';
        fill.style.background = _progressColor(tempPct);
      }
    }

    // CPU Load (true utilization from /proc/stat)
    const loadEl = document.getElementById('adminCpuLoad');
    if (loadEl) {
      const loadPct = _serverCpuPct(s);
      const valueEl = loadEl.querySelector('.admin-card-value, .admin-kpi-value');
      if (valueEl) valueEl.textContent = loadPct + '%';
      const noteEl = loadEl.querySelector('.admin-kpi-note');
      if (noteEl && s.cpu_temp != null) noteEl.textContent = `CPU ${s.cpu_temp.toFixed(1)}°C`;
      const fill = loadEl.querySelector('.admin-progress-fill');
      if (fill) {
        fill.style.width = loadPct + '%';
        fill.style.background = _progressColor(loadPct);
      }
    }

    // Memory
    const memEl = document.getElementById('adminMemory');
    if (memEl) {
      const memTotal = s.memory?.MemTotal || 0;
      const memAvail = s.memory?.MemAvailable || 0;
      const memUsed = memTotal - memAvail;
      const memPct = memTotal ? Math.round(memUsed / memTotal * 100) : 0;
      const valueEl = memEl.querySelector('.admin-card-value, .admin-kpi-value');
      if (valueEl) valueEl.textContent = memPct + '%';
      const noteEl = memEl.querySelector('.admin-kpi-note');
      if (noteEl) noteEl.textContent = `${_fmtBytes(memUsed)} / ${_fmtBytes(memTotal)}`;
      const fill = memEl.querySelector('.admin-progress-fill');
      if (fill) {
        fill.style.width = memPct + '%';
        fill.style.background = _progressColor(memPct);
      }
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
  if (!Array.isArray(jobs)) {
    return _adminPanelError('배치 작업', '배치 상태를 불러오지 못했습니다. 새로고침으로 다시 시도하세요.');
  }
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
    const slo = j.slo || {};
    const sloClass = slo.level === 'breach' ? 'admin-status-fail'
                   : slo.level === 'watch' ? 'admin-status-run'
                   : 'admin-status-ok';
    const sloLabel = slo.level === 'breach' ? '위반'
                   : slo.level === 'watch' ? '주의'
                   : '정상';

    // data-label 은 모바일(≤720px) 카드 변환 시 CSS ::before 라벨로 쓰인다
    // (admin.html .admin-table-cards). 작업명·버튼 셀은 라벨 없이 전체 폭.
    return `
      <tr>
        <td class="admin-cell-full"><strong>${j.label}</strong><div class="admin-sub">${j.schedule}</div></td>
        <td data-label="실행 상태" class="${statusClass}">${statusIcon} ${_statusLabel(j.status)}</td>
        <td data-label="최신 데이터">${dataCell}</td>
        <td data-label="SLO" class="${sloClass}">${sloLabel}<div class="admin-sub">${_esc(slo.note || '')}</div></td>
        <td data-label="최근 실행">${lastRun}</td>
        <td data-label="다음 실행">${nextRun}</td>
        <td class="admin-cell-full">
          <button class="admin-btn" onclick="triggerJob('${j.name}')" ${j.status === 'running' ? 'disabled' : ''}>실행</button>
          <button class="admin-btn admin-btn-secondary" onclick="triggerJobWithDate('${j.name}')">날짜 지정</button>
        </td>
      </tr>
    `;
  }).join('');

  return `
    <div class="admin-section">
      <h3>배치 작업 <span class="admin-sub">실행 상태·데이터 상태·SLO를 함께 확인</span></h3>
      <table class="admin-table admin-table-cards">
        <thead><tr>
          <th>작업</th>
          <th>실행 상태</th>
          <th>최신 데이터</th>
          <th>SLO</th>
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
  if (!Array.isArray(users)) {
    return _adminPanelError('사용자 관리', '사용자 목록을 불러오지 못했습니다. 새로고침으로 다시 시도하세요.');
  }
  const rows = _renderAdminUserRows(users);
  return `
    <div class="admin-section" id="adminUsersSection">
      <h3>사용자 관리 <span class="admin-sub">${users.length}명 · 삭제/롤 변경/프로필 수정</span></h3>
      <div class="admin-user-toolbar">
        <div class="admin-toolbar-left">
          <input class="admin-search-input" id="adminUserSearch" placeholder="사용자 검색: 이름, 이메일, google_sub"
                 oninput="filterAdminUsers()" style="min-width:300px;">
          <select class="admin-select" id="adminRoleFilter" onchange="filterAdminUsers()">
            <option value="all">전체 역할</option>
            <option value="admin">관리자</option>
            <option value="user">일반</option>
          </select>
        </div>
        <div class="admin-toolbar-right">
          <input class="admin-search-input" id="adminPortfolioSearch" placeholder="포트폴리오 검색: 종목명, 코드, 그룹"
                 onkeydown="if(event.key==='Enter')searchAdminPortfolios()" style="min-width:300px;">
          <button class="admin-btn admin-btn-primary" onclick="searchAdminPortfolios()" type="button">검색</button>
        </div>
      </div>
      <div id="adminUserMessage" class="admin-sub" style="margin-bottom:8px;"></div>
      <div id="adminUserEditor"></div>
      <div id="adminPortfolioSearchResult"></div>
      <div class="admin-table-wrap">
        <table class="admin-table admin-table-cards">
          <thead><tr><th>사용자</th><th>역할</th><th>포트폴리오</th><th>최근 로그인</th><th>가입일</th><th>작업</th></tr></thead>
          <tbody id="adminUsersBody">${rows}</tbody>
        </table>
      </div>
    </div>
  `;
}

function _renderAdminUserRows(users) {
  return (users || []).map(u => {
    const subArg = encodeURIComponent(u.google_sub || '');
    const pic = u.picture
      ? `<img class="admin-avatar" src="${_esc(u.picture)}" alt="">`
      : `<div class="admin-avatar-fallback">${_esc(_userInitial(u))}</div>`;
    const portfolioCount = Number(u.portfolio_count || 0);
    const portfolioUrl = _adminPortfolioUrl(subArg);
    // data-label = 모바일 카드 변환용(admin.html .admin-table-cards).
    // 사용자 셀·작업 버튼 셀은 라벨 없이 카드 전체 폭을 쓴다.
    return `
    <tr>
      <td class="admin-cell-full">
        <div class="admin-user-cell">
          ${pic}
          <div class="admin-user-main">
            <strong>${_esc(u.name)}</strong>
            <span>${_esc(u.email)}</span>
            <span><code>${_esc(u.google_sub)}</code></span>
          </div>
        </div>
      </td>
      <td data-label="역할">
        <select class="admin-select" onchange="changeAdminUserRole('${subArg}', this.value === 'admin')">
          <option value="user" ${u.is_admin ? '' : 'selected'}>일반</option>
          <option value="admin" ${u.is_admin ? 'selected' : ''}>관리자</option>
        </select>
      </td>
      <td data-label="포트폴리오">
        <span class="admin-badge">${portfolioCount.toLocaleString()}종목</span>
        <a class="admin-link-button" href="${portfolioUrl}" target="_blank" rel="noopener" style="margin-left:6px;">포트폴리오</a>
      </td>
      <td data-label="최근 로그인">${u.last_login_at ? _esc(u.last_login_at.slice(0, 16).replace('T', ' ')) : '-'}</td>
      <td data-label="가입일">${u.created_at ? _esc(u.created_at.slice(0, 10)) : '-'}</td>
      <td class="admin-cell-full">
        <button class="admin-btn admin-btn-secondary" onclick="editAdminUser('${subArg}')" type="button">프로필 수정</button>
        <button class="admin-btn admin-btn-danger" onclick="deleteAdminUser('${subArg}')" type="button">삭제</button>
      </td>
    </tr>
    `;
  }).join('') || '<tr><td class="admin-muted-row" colspan="6">표시할 사용자가 없습니다.</td></tr>';
}

function _userInitial(u) {
  return String(u?.name || u?.email || '?').trim().slice(0, 1).toUpperCase() || '?';
}

function _adminPortfolioUrl(encodedSub) {
  return `https://192.168.68.67:3691/api/admin/users/${encodedSub}/portfolio.html`;
}

function filterAdminUsers() {
  const q = String(document.getElementById('adminUserSearch')?.value || '').trim().toLowerCase();
  const role = document.getElementById('adminRoleFilter')?.value || 'all';
  const filtered = (_adminUsers || []).filter(u => {
    const haystack = [u.name, u.email, u.google_sub].join(' ').toLowerCase();
    const roleOk = role === 'all' || (role === 'admin' ? !!u.is_admin : !u.is_admin);
    return roleOk && (!q || haystack.includes(q));
  });
  const body = document.getElementById('adminUsersBody');
  if (body) body.innerHTML = _renderAdminUserRows(filtered);
}

function _showAdminUserMessage(message, isError = false) {
  const el = document.getElementById('adminUserMessage');
  if (el) el.innerHTML = `<span class="${isError ? 'admin-status-fail' : 'admin-status-ok'}">${_esc(message)}</span>`;
}

function editAdminUser(encodedSub) {
  const googleSub = decodeURIComponent(encodedSub || '');
  const u = (_adminUsers || []).find(row => row.google_sub === googleSub);
  const root = document.getElementById('adminUserEditor');
  if (!u || !root) return;
  root.innerHTML = `
    <form class="admin-section" style="box-shadow:none;margin:0 0 12px;padding:14px;background:color-mix(in srgb,var(--surface) 94%,var(--bg));"
          onsubmit="event.preventDefault(); saveAdminUserProfile('${encodedSub}');">
      <h3 style="margin-bottom:10px;">프로필 수정 <span class="admin-sub">${_esc(u.email)}</span></h3>
      <div style="display:grid;grid-template-columns:repeat(4,minmax(140px,1fr));gap:8px;align-items:center;">
        <input class="admin-field" id="adminEditName" value="${_esc(u.name)}" placeholder="이름">
        <input class="admin-field" id="adminEditEmail" value="${_esc(u.email)}" placeholder="이메일">
        <input class="admin-field" id="adminEditPicture" value="${_esc(u.picture || '')}" placeholder="프로필 이미지 URL">
        <label class="admin-sub" style="display:flex;gap:6px;align-items:center;">
          <input id="adminEditVerified" type="checkbox" ${u.email_verified ? 'checked' : ''}> 이메일 확인됨
        </label>
      </div>
      <div style="display:flex;gap:8px;margin-top:10px;">
        <button class="admin-btn admin-btn-primary" type="submit">저장</button>
        <button class="admin-btn admin-btn-secondary" type="button" onclick="document.getElementById('adminUserEditor').innerHTML=''">닫기</button>
      </div>
    </form>
  `;
  root.scrollIntoView({behavior: 'smooth', block: 'nearest'});
}

async function saveAdminUserProfile(encodedSub) {
  const googleSub = decodeURIComponent(encodedSub || '');
  try {
    await apiFetchJson(`/api/admin/users/${encodeURIComponent(googleSub)}`, {
      method: 'PATCH',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({
        name: document.getElementById('adminEditName')?.value || '',
        email: document.getElementById('adminEditEmail')?.value || '',
        picture: document.getElementById('adminEditPicture')?.value || '',
        email_verified: !!document.getElementById('adminEditVerified')?.checked,
      }),
      errorMessage: '프로필 저장 실패',
    });
    _showAdminUserMessage('프로필을 저장했습니다.');
    await refreshAdminUsers();
    document.getElementById('adminUserEditor').innerHTML = '';
  } catch (e) {
    _showAdminUserMessage(e.message, true);
  }
}

async function changeAdminUserRole(encodedSub, isAdmin) {
  const googleSub = decodeURIComponent(encodedSub || '');
  try {
    await apiFetchJson(`/api/admin/users/${encodeURIComponent(googleSub)}/role`, {
      method: 'PUT',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({is_admin: isAdmin}),
      errorMessage: '역할 변경 실패',
    });
    _showAdminUserMessage('역할을 변경했습니다.');
    await refreshAdminUsers();
  } catch (e) {
    _showAdminUserMessage(e.message, true);
    await refreshAdminUsers();
  }
}

async function deleteAdminUser(encodedSub) {
  const googleSub = decodeURIComponent(encodedSub || '');
  const u = (_adminUsers || []).find(row => row.google_sub === googleSub);
  if (!u) return;
  if (!(await adminConfirm(`${u.name || u.email} 사용자를 삭제할까요?\n포트폴리오와 세션 데이터도 함께 삭제됩니다.`))) return;
  try {
    await apiFetchJson(`/api/admin/users/${encodeURIComponent(googleSub)}`, {
      method: 'DELETE',
      headers: {'Content-Type': 'application/json'},
      errorMessage: '삭제 실패',
    });
    _showAdminUserMessage('사용자를 삭제했습니다.');
    await refreshAdminUsers();
  } catch (e) {
    _showAdminUserMessage(e.message, true);
  }
}

async function refreshAdminUsers() {
  _adminUsers = await apiFetchJson('/api/admin/users', {
    errorMessage: '사용자 목록 갱신 실패',
  });
  filterAdminUsers();
}

async function searchAdminPortfolios() {
  const q = String(document.getElementById('adminPortfolioSearch')?.value || '').trim();
  const root = document.getElementById('adminPortfolioSearchResult');
  if (!root) return;
  if (!q) {
    root.innerHTML = '<div class="admin-sub" style="margin-bottom:10px;">검색어를 입력하세요.</div>';
    return;
  }
  root.innerHTML = '<div class="admin-sub" style="margin-bottom:10px;">포트폴리오 검색 중...</div>';
  try {
    const data = await apiFetchJson(`/api/admin/portfolio-search?q=${encodeURIComponent(q)}&limit=80`, {
      errorMessage: '포트폴리오 검색 실패',
    });
    root.innerHTML = _renderPortfolioSearchResult(data.rows || [], q);
  } catch (e) {
    root.innerHTML = `<div class="admin-sub admin-status-fail" style="margin-bottom:10px;">${_esc(e.message)}</div>`;
  }
}

function _renderPortfolioSearchResult(rows, query) {
  const body = rows.map(r => `
    <tr>
      <td>
        <strong>${_esc(r.stock_name || r.stock_code)}</strong>
        <div class="admin-sub"><code>${_esc(r.stock_code)}</code> · ${_esc(r.group_name || '-')}</div>
      </td>
      <td>${_esc(r.name)}<div class="admin-sub">${_esc(r.email)}</div></td>
      <td class="admin-num">${Number(r.quantity || 0).toLocaleString()}</td>
      <td class="admin-num">${Number(r.avg_price || 0).toLocaleString()} ${_esc(r.currency || '')}</td>
      <td><a class="admin-link-button" href="${_esc(r.portfolio_url || '#')}" target="_blank" rel="noopener">포트폴리오</a></td>
    </tr>
  `).join('');
  return `
    <div style="margin:0 0 12px;">
      <div class="admin-sub" style="margin:0 0 8px;">포트폴리오 검색 "${_esc(query)}" · ${rows.length}건</div>
      <div class="admin-table-wrap">
        <table class="admin-table admin-table-compact">
          <thead><tr><th>종목</th><th>사용자</th><th class="admin-num">수량</th><th class="admin-num">평균단가</th><th>링크</th></tr></thead>
          <tbody>${body || '<tr><td class="admin-muted-row" colspan="5">검색 결과가 없습니다.</td></tr>'}</tbody>
        </table>
      </div>
    </div>
  `;
}

// --- DB section ---

function _renderDbSection(db) {
  if (!db) {
    return _adminPanelError('데이터베이스', 'DB 상태를 불러오지 못했습니다. 새로고침으로 다시 시도하세요.');
  }
  const tables = Object.entries(db.tables || {}).sort((a, b) => b[1] - a[1]);
  const rows = tables.map(([name, count]) => `<tr><td>${name}</td><td class="admin-num">${count.toLocaleString()}</td></tr>`).join('');
  return `
    <details class="admin-section admin-collapsible" id="dbStatsSection">
      ${_adminCollapsibleSummary('데이터베이스', _fmtBytes(db.db_size_bytes || 0), `${tables.length}개 테이블`)}
      <div class="admin-collapsible-body">
        <div class="admin-table-wrap">
          <table class="admin-table admin-table-compact">
            <thead><tr><th>테이블</th><th>행 수</th></tr></thead>
            <tbody>${rows}</tbody>
          </table>
        </div>
      </div>
    </details>
  `;
}

// --- Actions ---

async function triggerJob(jobName) {
  try {
    const data = await apiFetchJson(`/api/admin/trigger/${jobName}`, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({}),
      errorMessage: '실행 실패',
    });
    showToast(data.message || '실행 시작', 'success');
    setTimeout(loadAdminView, 2000);
  } catch (e) {
    reportApiError(e, '실행 요청');
  }
}

async function triggerJobWithDate(jobName) {
  const dateStr = await adminPromptDate('실행할 날짜를 입력하세요 (YYYY-MM-DD)');
  if (!dateStr) return;
  if (!/^\d{4}-\d{2}-\d{2}$/.test(dateStr)) {
    showToast('올바른 날짜 형식이 아닙니다.', 'warning');
    return;
  }
  try {
    const data = await apiFetchJson(`/api/admin/trigger/${jobName}`, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({date: dateStr}),
      errorMessage: '실행 실패',
    });
    showToast(`${data.message} (${dateStr})`, 'success');
    setTimeout(loadAdminView, 2000);
  } catch (e) {
    reportApiError(e, '실행 요청');
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

// --- 데이터 품질 점검 -----------------------------------------------------
//
// data-quality.timer 가 매일 20:30(KST) 돌리는 정기 점검의 최신 결과.
// /api/admin/event-summary 가 내려주는 check_summary 이벤트 하나
// (source='data_quality') 의 details(counts + results)로 전체를 그린다.
// 비정상(warn/error) 점검만 표로 노출 — 전부 정상이면 카운트 한 줄.

const _DQ_CHECK_LABELS = {
  nav_snapshot_freshness: 'NAV 스냅샷 신선도',
  intraday_points: '장중 스냅샷 (금일)',
  system_events_error_rate: '시스템 이벤트 에러율 (24h)',
};

function _dqCheckLabel(check) {
  if (_DQ_CHECK_LABELS[check]) return _DQ_CHECK_LABELS[check];
  if (check && check.indexOf('benchmark_freshness_') === 0) {
    return `벤치마크 신선도 (${check.slice('benchmark_freshness_'.length)})`;
  }
  return check || '(unknown)';
}

function _renderDataQualitySection(dq) {
  if (!dq) {
    return `
      <div class="admin-section">
        <h3>데이터 품질 <span class="admin-sub">매일 20:30 자동 점검</span></h3>
        <div class="admin-sub" style="padding:8px 0;">아직 점검 기록이 없습니다 — data-quality.timer 첫 실행 후 표시됩니다.</div>
      </div>
    `;
  }
  const d = dq.details_obj || {};
  const counts = d.counts || {};
  const results = Array.isArray(d.results) ? d.results : [];
  const failing = results.filter(r => r && r.status !== 'ok');
  const err = counts.error || 0;
  const warn = counts.warn || 0;
  const okCount = counts.ok || 0;
  const headCls = err > 0 ? 'admin-status-fail' : warn > 0 ? 'admin-status-run' : 'admin-status-ok';
  const headIcon = err > 0 ? '✗' : warn > 0 ? '⚠' : '✓';
  const failRows = failing.map(r => {
    const cls = r.status === 'error' ? 'admin-status-fail' : 'admin-status-run';
    const icon = r.status === 'error' ? '✗' : '⚠';
    return `
      <tr>
        <td>${_esc(_dqCheckLabel(r.check))}<div class="admin-sub"><code>${_esc(r.check)}</code></div></td>
        <td class="${cls}">${icon} ${_esc(r.status)}</td>
        <td>${_esc(r.detail || '')}</td>
        <td class="admin-num">${r.value != null ? _esc(String(r.value)) : '-'}</td>
      </tr>`;
  }).join('');
  return `
    <div class="admin-section">
      <h3>데이터 품질 <span class="admin-sub">최근 점검 ${_fmtRelTime(dq.ts)} · 매일 20:30 자동</span></h3>
      <div class="admin-card-value ${headCls}" style="margin-bottom:8px;">${headIcon} 정상 ${okCount} · 주의 ${warn} · 오류 ${err}</div>
      ${failing.length ? `
      <table class="admin-table admin-table-compact">
        <thead><tr><th>점검</th><th>상태</th><th>상세</th><th style="text-align:right;">값</th></tr></thead>
        <tbody>${failRows}</tbody>
      </table>` : '<div class="admin-sub">모든 점검 통과 — 비정상 항목 없음</div>'}
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
    <details class="admin-section admin-collapsible" id="eventsSection">
      ${_adminCollapsibleSummary('최근 이벤트', '시스템 이벤트 피드', `${events.length}건`)}
      <div class="admin-collapsible-body">
        <div style="display:flex;justify-content:flex-end;margin-bottom:8px;">
          <button class="admin-btn admin-btn-secondary" onclick="loadAdminView()" type="button">새로고침</button>
        </div>
        <div class="admin-table-wrap">
          <table class="admin-table admin-table-compact">
            <thead><tr><th>시각</th><th>레벨</th><th>소스</th><th>종류</th><th>종목</th><th>상세</th></tr></thead>
            <tbody>${rows || '<tr><td colspan="6" style="text-align:center;color:var(--text-secondary)">이벤트 없음</td></tr>'}</tbody>
          </table>
        </div>
      </div>
    </details>
  `;
}

// --- Wiki diagnostic form ----------------------------------------------
//
// "이 종목 위키 왜 안 늘어" 를 한 번에 조회. code 입력 → /api/admin/diag/wiki
// 호출 → 결과 패널에 depositional Naver funnel + DB 상태 + verdict 출력.

function _renderDiagSection() {
  return `
    <details class="admin-section admin-collapsible" id="diagWikiSection">
      ${_adminCollapsibleSummary('위키 진단', '종목별 파이프라인 funnel')}
      <div class="admin-collapsible-body">
        <form id="diagWikiForm" onsubmit="event.preventDefault(); runWikiDiag();" style="display:flex;gap:8px;align-items:center;margin-bottom:12px;flex-wrap:wrap;">
          <input id="diagWikiCode" type="text" placeholder="종목코드 (예: 051910)" style="padding:6px 10px;border:1px solid var(--border);border-radius:4px;background:var(--surface);color:var(--text-primary);font-family:monospace;width:140px;">
          <button class="admin-btn" type="submit">진단 실행</button>
          <span class="admin-sub">Naver 응답·화이트리스트·DB 상태를 한 번에 확인합니다.</span>
        </form>
        <div id="diagWikiResult"></div>
      </div>
    </details>
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
    // Naver 스크랩 포함 최대 15초+ 걸릴 수 있어 기본 20초 타임아웃을 늘린다.
    const data = await apiFetchJson(`/api/admin/diag/wiki?code=${encodeURIComponent(code)}`, {
      timeoutMs: 60000,
      errorMessage: '진단 실패',
    });
    result.innerHTML = _renderWikiDiagResult(data);
  } catch (e) {
    const status = e?.status ? `HTTP ${e.status}: ` : '';
    result.innerHTML = `<div style="color:var(--color-danger)">진단 요청 실패: ${_esc(status + e.name + ': ' + e.message)}</div>`;
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
