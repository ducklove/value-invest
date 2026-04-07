/* Admin dashboard */

let _adminLoaded = false;
let _liveInterval = null;

async function loadAdminView() {
  const container = document.getElementById('adminContent');
  if (!container) return;
  if (!_adminLoaded) {
    container.innerHTML = '<div style="color:var(--text-secondary);text-align:center;padding:40px;">로딩 중...</div>';
  }
  try {
    const [batchRes, serverRes, dbRes, usersRes] = await Promise.all([
      apiFetch('/api/admin/batch-status'),
      apiFetch('/api/admin/server-stats'),
      apiFetch('/api/admin/db-stats'),
      apiFetch('/api/admin/users'),
    ]);
    const batch = await batchRes.json();
    const server = await serverRes.json();
    const db = await dbRes.json();
    const users = await usersRes.json();
    container.innerHTML = _renderAdmin(batch, server, db, users);
    _adminLoaded = true;
    _startLiveUpdates();
  } catch (e) {
    container.innerHTML = `<div style="color:var(--text-secondary);padding:40px;">어드민 데이터를 불러오지 못했습니다.</div>`;
  }
}

function _renderAdmin(batch, server, db, users) {
  return `
    <div class="admin-dashboard">
      <h2 class="admin-title">시스템 관리</h2>
      <div id="adminLiveSection">${_renderServerCard(server)}</div>
      ${_renderBatchSection(batch)}
      ${_renderUsersSection(users)}
      ${_renderDbSection(db)}
    </div>
  `;
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
    const statusIcon = j.status === 'success' ? '✓' : j.status === 'failed' ? '✗' : j.status === 'running' ? '⟳' : '—';
    const statusClass = j.status === 'success' ? 'admin-status-ok' : j.status === 'failed' ? 'admin-status-fail' : j.status === 'running' ? 'admin-status-run' : '';
    const lastRun = j.last_start ? _fmtTimestamp(j.last_start) : '-';
    const nextRun = j.next_run ? _fmtTimestamp(j.next_run) : '-';
    return `
      <tr>
        <td><strong>${j.label}</strong><div class="admin-sub">${j.schedule}</div></td>
        <td class="${statusClass}">${statusIcon} ${_statusLabel(j.status)}</td>
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
      <h3>배치 작업</h3>
      <table class="admin-table">
        <thead><tr><th>작업</th><th>상태</th><th>최근 실행</th><th>다음 실행</th><th>수동 실행</th></tr></thead>
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
