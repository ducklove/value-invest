/* Admin dashboard — bootstrap, AI ops config, and shared helpers.
   Observability panels live in admin-observability.js; linked-project
   config + dividend admin live in admin-linked-projects.js. */

let _adminLoaded = false;
let _aiAdminConfig = null;
let _adminUsers = [];

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
    // 모든 호출에 fallback 을 줘서 패널 하나의 HTTP 실패가 페이지 전체를
    // 무너뜨리지 않게 한다. null/빈 fallback 을 받은 렌더러는 해당 패널에
    // 인라인 오류 문구를 그리고 나머지 패널은 정상 렌더된다. 바깥 catch 는
    // 네트워크 단절처럼 전부 실패한 경우만 담당한다.
    const [deploy, batch, server, db, users, summary, events, httpMetrics, timeline, linkedConfigs, aiConfig] = await Promise.all([
      apiFetchJson('/api/admin/deploy-status', { fallback: null }),
      apiFetchJson('/api/admin/batch-status', { fallback: null }),
      apiFetchJson('/api/admin/server-stats', { fallback: null }),
      apiFetchJson('/api/admin/db-stats', { fallback: null }),
      apiFetchJson('/api/admin/users', { fallback: null }),
      apiFetchJson('/api/admin/event-summary?hours=24', { fallback: {by_source: {}, latest: {}} }),
      apiFetchJson('/api/admin/events?limit=50', { fallback: [] }),
      apiFetchJson('/api/admin/http-metrics?hours=24', { fallback: {endpoints: []} }),
      apiFetchJson('/api/admin/timeseries?hours=24', { fallback: {hours: 24, events: [], http: []} }),
      apiFetchJson('/api/admin/linked-project-configs', { fallback: [] }),
      apiFetchJson('/api/admin/ai-config', { fallback: null }),
    ]);
    _adminUsers = Array.isArray(users) ? users : [];
    _linkedProjectConfigs = Array.isArray(linkedConfigs) ? linkedConfigs : [];
    _aiAdminConfig = aiConfig;
    // users 는 실패(null)와 빈 목록([])을 구분해야 하므로 fetch 결과를
    // 그대로 넘긴다. _adminUsers 는 검색/필터 등 후속 동작용 배열 사본.
    container.innerHTML = _renderAdmin(deploy, batch, server, db, users, summary, events, httpMetrics, timeline, _linkedProjectConfigs, _aiAdminConfig);
    _adminLoaded = true;
    if (server && typeof _seedServerSeries === 'function') _seedServerSeries(server);
    if (typeof _renderServerTimeline === 'function') _renderServerTimeline();
    _startLiveUpdates();
    // 해외 배당 목록은 섹션 HTML 삽입 후에만 컨테이너가 존재 — 별도
    // 비동기 로드로 가져와 표시. 실패해도 페이지 나머지엔 영향 없음.
    loadPreferredDividendsList();
    loadForeignDividendsList();
  } catch (e) {
    container.innerHTML = `<div style="color:var(--text-secondary);padding:40px;">어드민 데이터를 불러오지 못했습니다.</div>`;
  }
}

function _renderAdmin(deploy, batch, server, db, users, summary, events, httpMetrics, timeline, linkedConfigs, aiConfig) {
  return `
    <div class="admin-shell">
      ${_renderAdminTopbar()}
      ${_renderOperationsOverview(deploy, server, db, users, summary, httpMetrics)}
      ${_renderTimelineSection(server, timeline)}
      <div class="admin-grid-two">
        ${_renderBatchSection(batch)}
        ${_renderSubsystemSummary(summary)}
      </div>
      ${_renderUsersSection(users)}
      ${_renderDataQualitySection(summary.data_quality)}
      ${_renderHttpMetricsSection(httpMetrics)}
      ${_renderDbSection(db)}
      ${_renderEventsSection(events)}
      ${_renderDataSyncSection()}
      ${_renderAiConfigSection(aiConfig)}
      ${_renderLinkedProjectConfigSection(linkedConfigs)}
      ${_renderDiagSection()}
    </div>
  `;
}

function _renderAdminTopbar() {
  return `
    <header class="admin-topbar">
      <div class="admin-brand">
        <div class="admin-brand-mark">VI</div>
        <div>
          <h1>Value Invest Admin</h1>
          <p>운영 콘솔 · 배포, 시스템, 사용자, 데이터 파이프라인</p>
        </div>
      </div>
      <div class="admin-actions">
        <button class="admin-btn" onclick="loadAdminView()" type="button">새로고침</button>
        <button class="theme-toggle" onclick="toggleAdminTheme()" type="button" aria-label="테마 전환">◐</button>
      </div>
    </header>
  `;
}

function _renderOperationsOverview(deploy, server, db, users, summary, httpMetrics) {
  const memTotal = server?.memory?.MemTotal || 0;
  const memAvail = server?.memory?.MemAvailable || 0;
  const memUsed = Math.max(0, memTotal - memAvail);
  const memPct = memTotal ? Math.round(memUsed / memTotal * 100) : 0;
  const diskTotal = server?.disk?.total || 0;
  const diskUsed = server?.disk?.used || 0;
  const diskPct = diskTotal ? Math.round(diskUsed / diskTotal * 100) : 0;
  const cpuPct = _serverCpuPct(server);
  const eventCounts = Object.values(summary?.by_source || {}).reduce((acc, levels) => {
    acc.error += Number(levels.error || 0);
    acc.warning += Number(levels.warning || 0);
    acc.info += Number(levels.info || 0);
    return acc;
  }, {error: 0, warning: 0, info: 0});
  const httpErrors = (httpMetrics?.endpoints || []).reduce((sum, row) => sum + Number(row.errors || 0), 0);
  const commit = deploy?.build?.short_sha || '-';
  const runner = deploy?.actions_runner?.active ? '동작 중' : '중지';
  const runnerCls = deploy?.actions_runner?.active ? 'admin-status-ok' : 'admin-status-fail';
  // server/users 가 null(fallback) 이면 0% 같은 가짜 수치 대신 '-' 와
  // 조회 실패 문구를 보여준다 — 아래 개별 패널의 인라인 오류와 짝.
  const serverOk = !!server;
  const usersOk = Array.isArray(users);
  const portfolioUsers = (users || []).filter(u => Number(u.portfolio_count || 0) > 0).length;
  return `
    <section class="admin-hero">
      <div class="admin-status-panel">
        <div class="admin-panel-head">
          <div>
            <h2 class="admin-panel-title">운영 콘솔</h2>
            <div class="admin-panel-sub">최근 24시간 기준 상태를 한 화면에서 확인합니다.</div>
          </div>
          <span class="admin-badge admin-badge-internal">내부망 전용 링크 사용</span>
        </div>
        <div class="admin-kpi-grid">
          ${_renderKpi('현재 커밋', `<code>${_esc(commit)}</code>`, _esc(deploy?.build?.subject || runner), '')}
          ${_renderKpi('CPU 사용률', serverOk ? `${cpuPct}%` : '-', serverOk ? (server?.cpu_temp != null ? `CPU ${server.cpu_temp.toFixed(1)}°C` : '실시간 샘플') : '서버 상태 조회 실패', serverOk ? _progressBar(cpuPct) : '', 'adminCpuLoad')}
          ${_renderKpi('메모리', serverOk ? `${memPct}%` : '-', serverOk ? `${_fmtBytes(memUsed)} / ${_fmtBytes(memTotal)}` : '서버 상태 조회 실패', serverOk ? _progressBar(memPct) : '', 'adminMemory')}
          ${_renderKpi('디스크', serverOk ? `${diskPct}%` : '-', serverOk ? `${_fmtBytes(diskUsed)} / ${_fmtBytes(diskTotal)}` : '서버 상태 조회 실패', serverOk ? _progressBar(diskPct) : '')}
          ${_renderKpi('사용자', usersOk ? `${users.length.toLocaleString()}명` : '-', usersOk ? `포트폴리오 ${portfolioUsers.toLocaleString()}명` : '사용자 조회 실패', '')}
        </div>
      </div>
      <div class="admin-status-panel">
        <div class="admin-panel-head">
          <div>
            <h2 class="admin-panel-title">위험 신호</h2>
            <div class="admin-panel-sub">오류와 지연을 먼저 봅니다.</div>
          </div>
          <span class="${runnerCls}">${runner}</span>
        </div>
        <div class="admin-cards">
          <div class="admin-card">
            <div class="admin-card-label">이벤트 오류</div>
            <div class="admin-card-value ${eventCounts.error ? 'admin-status-fail' : 'admin-status-ok'}">${eventCounts.error.toLocaleString()}</div>
            <div class="admin-sub">warning ${eventCounts.warning.toLocaleString()} · info ${eventCounts.info.toLocaleString()}</div>
          </div>
          <div class="admin-card">
            <div class="admin-card-label">HTTP 오류</div>
            <div class="admin-card-value ${httpErrors ? 'admin-status-fail' : 'admin-status-ok'}">${httpErrors.toLocaleString()}</div>
            <div class="admin-sub">느린/5xx 요청 tail 기준</div>
          </div>
          <div class="admin-card">
            <div class="admin-card-label">DB 크기</div>
            <div class="admin-card-value">${db ? _fmtBytes(db.db_size_bytes || 0) : '-'}</div>
            <div class="admin-sub">${db ? `${Object.keys(db.tables || {}).length} tables` : 'DB 상태 조회 실패'}</div>
          </div>
        </div>
      </div>
    </section>
  `;
}

function _renderKpi(label, value, note, progress, id) {
  return `
    <div class="admin-kpi" ${id ? `id="${id}"` : ''}>
      <div class="admin-kpi-label">${label}</div>
      <div class="admin-kpi-value">${value}</div>
      ${note ? `<div class="admin-kpi-note">${note}</div>` : ''}
      ${progress || ''}
    </div>
  `;
}

function _adminCollapsibleSummary(title, subtitle = '', count = '') {
  return `
    <summary class="admin-collapsible-summary">
      <span>
        <span class="admin-collapsible-title">${_esc(title)}</span>
        ${subtitle ? `<span class="admin-sub">${_esc(subtitle)}</span>` : ''}
      </span>
      <span class="admin-collapsible-meta">
        ${count ? `<span class="admin-badge">${_esc(count)}</span>` : ''}
        <span class="admin-collapsible-icon" aria-hidden="true"></span>
      </span>
    </summary>
  `;
}

// 테마는 생태계 공통 'theme' 키를 쓴다(index.html 의 search.js 와 동일).
// 과거 admin 전용 'valueInvestAdminTheme' 키는 읽기 폴백 후 'theme' 으로
// 이전(마이그레이션)하고 제거한다.
const ADMIN_THEME_KEY = 'theme';
const ADMIN_LEGACY_THEME_KEY = 'valueInvestAdminTheme';

function toggleAdminTheme() {
  const next = document.documentElement.getAttribute('data-theme') === 'dark' ? 'light' : 'dark';
  document.documentElement.setAttribute('data-theme', next);
  try { localStorage.setItem(ADMIN_THEME_KEY, next); } catch (_) {}
  // SVG 차트는 렌더 시점의 CSS 토큰 색을 인라인으로 굽는다 — 테마가 바뀌면
  // 세 차트(서버/이벤트/HTTP)를 다시 그려 새 토큰 색을 반영한다(admin-charts.js).
  if (typeof _renderAdminCharts === 'function') _renderAdminCharts();
}

try {
  let savedAdminTheme = localStorage.getItem(ADMIN_THEME_KEY);
  if (!savedAdminTheme) {
    const legacyTheme = localStorage.getItem(ADMIN_LEGACY_THEME_KEY);
    if (legacyTheme) {
      savedAdminTheme = legacyTheme;
      localStorage.setItem(ADMIN_THEME_KEY, legacyTheme);
    }
  }
  localStorage.removeItem(ADMIN_LEGACY_THEME_KEY);
  if (savedAdminTheme) document.documentElement.setAttribute('data-theme', savedAdminTheme);
} catch (_) {}

// --- AI operations ------------------------------------------------------

function _renderAiConfigSection(config) {
  if (!config) {
    return `
      <details class="admin-section admin-collapsible" id="aiConfigSection">
        ${_adminCollapsibleSummary('AI 운영 관리', '키·기능별 모델·사용량')}
        <div class="admin-collapsible-body">
          <div class="admin-sub admin-status-fail">AI 설정을 불러오지 못했습니다.</div>
        </div>
      </details>
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
    <details class="admin-section admin-collapsible" id="aiConfigSection">
      ${_adminCollapsibleSummary('AI 운영 관리', '키·기능별 모델·사용량', `${config.features?.length || 0}개 기능`)}
      <div class="admin-collapsible-body">
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
        <div class="admin-table-wrap">
          <table class="admin-table admin-table-compact">
            <thead><tr><th>기능</th><th>사용 모델</th></tr></thead>
            <tbody>${featureRows}</tbody>
          </table>
        </div>
        <div style="margin-top:8px;">
          <button class="admin-btn" onclick="saveAiModels()">기능별 모델 저장</button>
        </div>
        <div style="margin-top:16px;">
          <strong>사용량</strong>
          <div class="admin-table-wrap" style="margin-top:4px;">
            <table class="admin-table admin-table-compact">
              <thead><tr><th>기능</th><th>모델</th><th>호출</th><th>입력/출력 토큰</th><th>비용</th><th>평균 지연</th><th>오류</th></tr></thead>
              <tbody>${usageRows || '<tr><td colspan="7" style="text-align:center;color:var(--text-secondary)">아직 기록된 AI 사용량이 없습니다.</td></tr>'}</tbody>
            </table>
          </div>
        </div>
      </div>
    </details>
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
  const data = await apiFetchJson('/api/admin/ai-config', {
    errorMessage: 'AI 설정 갱신 실패',
  });
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
    const data = await apiFetchJson('/api/admin/ai-config/key', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ openrouter_api_key: key }),
      errorMessage: '키 저장 실패',
    });
    _aiAdminConfig = data;
    document.getElementById('aiConfigSection').outerHTML = _renderAiConfigSection(_aiAdminConfig);
    _showAiConfigMessage('AI API key를 저장했습니다.', false);
  } catch (e) {
    _showAiConfigMessage(e.message, true);
  }
}

async function deleteAiKey() {
  if (!(await adminConfirm('DB에 저장된 OpenRouter key를 삭제할까요?\nenv/keys.txt 값은 그대로 둡니다.'))) return;
  try {
    const data = await apiFetchJson('/api/admin/ai-config/key', {
      method: 'DELETE',
      errorMessage: '키 삭제 실패',
    });
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
    const data = await apiFetchJson('/api/admin/ai-config/models', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ models }),
      errorMessage: '모델 저장 실패',
    });
    _aiAdminConfig = data;
    document.getElementById('aiConfigSection').outerHTML = _renderAiConfigSection(_aiAdminConfig);
    _showAiConfigMessage('기능별 모델을 저장했습니다.', false);
  } catch (e) {
    _showAiConfigMessage(e.message, true);
  }
}

// --- Shared helpers (used by all admin-* modules) ---

// 부트 부분 실패용 패널 대체 — 해당 패널만 인라인 오류 문구로 그리고
// 나머지 패널은 정상 렌더된다(loadAdminView 의 fallback: null 과 짝).
function _adminPanelError(title, message) {
  return `
    <div class="admin-section">
      <h3>${_esc(title)}</h3>
      <div class="admin-sub admin-status-fail" style="padding:8px 0;">${_esc(message)}</div>
    </div>
  `;
}

// --- 관리형 다이얼로그 (window.confirm/prompt 대체) ----------------------
//
// utils.js 의 openManagedModal/closeManagedModal 이 포커스 트랩·Escape·
// 스크롤 잠금·포커스 복원을 처리한다. DOM 은 호출 시 동적 생성하고 닫힐 때
// 제거한다. 스타일은 admin.html 의 .admin-dialog-* 참고.

function _adminDialog({ message, input = null, confirmLabel = '확인', cancelLabel = '취소', danger = false }) {
  return new Promise(resolve => {
    const overlay = document.createElement('div');
    overlay.className = 'admin-dialog-overlay';
    const box = document.createElement('div');
    box.className = 'admin-dialog';
    box.setAttribute('role', input ? 'dialog' : 'alertdialog');
    box.setAttribute('aria-modal', 'true');
    const msg = document.createElement('div');
    msg.className = 'admin-dialog-message';
    msg.textContent = message;
    box.appendChild(msg);
    let inputEl = null;
    if (input) {
      const row = document.createElement('div');
      row.className = 'admin-dialog-input-row';
      inputEl = document.createElement('input');
      inputEl.className = 'admin-field';
      inputEl.type = input.type || 'text';
      if (input.placeholder) inputEl.placeholder = input.placeholder;
      inputEl.value = input.value || '';
      row.appendChild(inputEl);
      box.appendChild(row);
    }
    const actions = document.createElement('div');
    actions.className = 'admin-dialog-actions';
    const cancelBtn = document.createElement('button');
    cancelBtn.type = 'button';
    cancelBtn.className = 'admin-btn admin-btn-secondary';
    cancelBtn.setAttribute('data-admin-dialog-cancel', '');
    cancelBtn.textContent = cancelLabel;
    const confirmBtn = document.createElement('button');
    confirmBtn.type = 'button';
    confirmBtn.className = `admin-btn ${danger ? 'admin-btn-danger' : 'admin-btn-primary'}`;
    confirmBtn.setAttribute('data-admin-dialog-confirm', '');
    confirmBtn.textContent = confirmLabel;
    actions.appendChild(cancelBtn);
    actions.appendChild(confirmBtn);
    box.appendChild(actions);
    overlay.appendChild(box);
    document.body.appendChild(overlay);

    let settled = false;
    const close = (confirmed) => {
      if (settled) return;
      settled = true;
      const value = inputEl ? inputEl.value : null;
      closeManagedModal(overlay, { remove: true });
      resolve({ confirmed, value });
    };
    cancelBtn.addEventListener('click', () => close(false));
    confirmBtn.addEventListener('click', () => close(true));
    if (inputEl) {
      inputEl.addEventListener('keydown', (event) => {
        if (event.key === 'Enter') { event.preventDefault(); close(true); }
      });
    }
    overlay.addEventListener('mousedown', (event) => {
      if (event.target === overlay) close(false);
    });
    openManagedModal(overlay, {
      onEscape: () => close(false),
      initialFocus: inputEl || confirmBtn,
    });
  });
}

// window.confirm 대체. 확인이면 true, 취소/Escape/바깥 클릭이면 false.
function adminConfirm(message, options = {}) {
  return _adminDialog({
    message,
    confirmLabel: options.confirmLabel || '확인',
    cancelLabel: options.cancelLabel || '취소',
    danger: options.danger === undefined ? true : !!options.danger,
  }).then(result => result.confirmed);
}

// window.prompt 대체 — 날짜(YYYY-MM-DD) 하나를 입력받는다. 취소하면 null.
function adminPromptDate(message, defaultValue = '') {
  const initial = defaultValue || new Date().toISOString().slice(0, 10);
  return _adminDialog({
    message,
    input: { type: 'date', value: initial, placeholder: 'YYYY-MM-DD' },
    danger: false,
  }).then(result => (result.confirmed ? String(result.value || '').trim() : null));
}

function _adminInputStyle() {
  return 'min-height:34px;padding:0 10px;border:1px solid var(--border);border-radius:7px;background:var(--surface);color:var(--text-primary);font:inherit;font-size:13px;';
}

function _esc(str) {
  const el = document.createElement('span');
  el.textContent = str || '';
  return el.innerHTML;
}
