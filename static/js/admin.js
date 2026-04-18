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
    // Fetch the usual cards + the new observability cards in one round.
    // event-summary is a tiny aggregate; events is the newest 50 rows.
    const [batchRes, serverRes, dbRes, usersRes, summaryRes, eventsRes] = await Promise.all([
      apiFetch('/api/admin/batch-status'),
      apiFetch('/api/admin/server-stats'),
      apiFetch('/api/admin/db-stats'),
      apiFetch('/api/admin/users'),
      apiFetch('/api/admin/event-summary?hours=24'),
      apiFetch('/api/admin/events?limit=50'),
    ]);
    const batch = await batchRes.json();
    const server = await serverRes.json();
    const db = await dbRes.json();
    const users = await usersRes.json();
    const summary = summaryRes.ok ? await summaryRes.json() : {by_source: {}, latest: {}};
    const events = eventsRes.ok ? await eventsRes.json() : [];
    container.innerHTML = _renderAdmin(batch, server, db, users, summary, events);
    _adminLoaded = true;
    _startLiveUpdates();
  } catch (e) {
    container.innerHTML = `<div style="color:var(--text-secondary);padding:40px;">어드민 데이터를 불러오지 못했습니다.</div>`;
  }
}

function _renderAdmin(batch, server, db, users, summary, events) {
  return `
    <div class="admin-dashboard">
      <h2 class="admin-title">시스템 관리</h2>
      <div id="adminLiveSection">${_renderServerCard(server)}</div>
      ${_renderBatchSection(batch)}
      ${_renderSubsystemSummary(summary)}
      ${_renderDiagSection()}
      ${_renderEventsSection(events)}
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

// --- Subsystem summary card strip ---------------------------------------
//
// One tile per known subsystem showing "last event time + outcome + counts
// over the last 24h". Catches the "nothing has happened for 3 days" case
// without having to scroll the event list.

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
    const barCls = err ? 'admin-status-fail' : warn ? 'admin-status-run' : 'admin-status-ok';
    const icon = err ? '✗' : warn ? '⚠' : '✓';
    const lastLabel = row ? `${_esc(row.kind)} · ${_fmtRelTime(row.ts)}` : '<span style="color:var(--text-secondary)">이벤트 없음</span>';
    return `
      <div class="admin-card">
        <div class="admin-card-label">${_esc(label)}</div>
        <div class="admin-card-value ${barCls}">${icon} ${lastLabel}</div>
        <div class="admin-sub">24h: info ${info} · warn ${warn} · error ${err}</div>
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
    const lvlCls = e.level === 'error' ? 'admin-status-fail' : e.level === 'warning' ? 'admin-status-run' : 'admin-status-ok';
    const icon = e.level === 'error' ? '✗' : e.level === 'warning' ? '⚠' : '·';
    let detailsPreview = '';
    if (e.details_obj) {
      try {
        const keys = Object.keys(e.details_obj);
        detailsPreview = keys.slice(0, 4).map(k => {
          let v = e.details_obj[k];
          if (typeof v === 'object' && v !== null) v = JSON.stringify(v);
          const s = String(v);
          return `<span class="admin-event-kv">${_esc(k)}=${_esc(s.length > 40 ? s.slice(0,40)+'…' : s)}</span>`;
        }).join(' ');
      } catch (_) { detailsPreview = ''; }
    }
    return `
      <tr>
        <td><span class="admin-sub">${_fmtEventTs(e.ts)}</span></td>
        <td class="${lvlCls}">${icon} ${_esc(e.level)}</td>
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
  result.innerHTML = '<div style="color:var(--text-secondary);padding:8px;">진단 중... (Naver 스크랩 포함하여 최대 10초 정도 소요)</div>';
  try {
    const res = await apiFetch(`/api/admin/diag/wiki?code=${encodeURIComponent(code)}`);
    if (!res.ok) {
      const data = await res.json().catch(() => ({}));
      result.innerHTML = `<div style="color:var(--color-danger)">진단 실패: ${_esc(data.detail || res.statusText)}</div>`;
      return;
    }
    const data = await res.json();
    result.innerHTML = _renderWikiDiagResult(data);
  } catch (e) {
    result.innerHTML = `<div style="color:var(--color-danger)">진단 요청 실패: ${_esc(e.message)}</div>`;
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
