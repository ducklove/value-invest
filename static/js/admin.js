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
    const [deploy, batch, server, db, users, summary, events, httpMetrics, timeline, linkedConfigs, aiConfig] = await Promise.all([
      apiFetchJson('/api/admin/deploy-status', { fallback: null }),
      apiFetchJson('/api/admin/batch-status'),
      apiFetchJson('/api/admin/server-stats'),
      apiFetchJson('/api/admin/db-stats'),
      apiFetchJson('/api/admin/users'),
      apiFetchJson('/api/admin/event-summary?hours=24', { fallback: {by_source: {}, latest: {}} }),
      apiFetchJson('/api/admin/events?limit=50', { fallback: [] }),
      apiFetchJson('/api/admin/http-metrics?hours=24', { fallback: {endpoints: []} }),
      apiFetchJson('/api/admin/timeseries?hours=24', { fallback: {hours: 24, events: [], http: []} }),
      apiFetchJson('/api/admin/linked-project-configs', { fallback: [] }),
      apiFetchJson('/api/admin/ai-config', { fallback: null }),
    ]);
    _adminUsers = Array.isArray(users) ? users : [];
    container.innerHTML = _renderAdmin(deploy, batch, server, db, _adminUsers, summary, events, httpMetrics, timeline, _linkedProjectConfigs, _aiAdminConfig);
    _adminLoaded = true;
    if (typeof _seedServerSeries === 'function') _seedServerSeries(server);
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
          ${_renderKpi('CPU 사용률', `${cpuPct}%`, server?.cpu_temp != null ? `CPU ${server.cpu_temp.toFixed(1)}°C` : '실시간 샘플', _progressBar(cpuPct), 'adminCpuLoad')}
          ${_renderKpi('메모리', `${memPct}%`, `${_fmtBytes(memUsed)} / ${_fmtBytes(memTotal)}`, _progressBar(memPct), 'adminMemory')}
          ${_renderKpi('디스크', `${diskPct}%`, `${_fmtBytes(diskUsed)} / ${_fmtBytes(diskTotal)}`, _progressBar(diskPct))}
          ${_renderKpi('사용자', `${(users || []).length.toLocaleString()}명`, `포트폴리오 ${portfolioUsers.toLocaleString()}명`, '')}
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
            <div class="admin-card-value">${_fmtBytes(db?.db_size_bytes || 0)}</div>
            <div class="admin-sub">${Object.keys(db?.tables || {}).length} tables</div>
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

function toggleAdminTheme() {
  const next = document.documentElement.getAttribute('data-theme') === 'dark' ? 'light' : 'dark';
  document.documentElement.setAttribute('data-theme', next);
  try { localStorage.setItem('valueInvestAdminTheme', next); } catch (_) {}
  if (typeof _renderServerTimeline === 'function') _renderServerTimeline();
}

try {
  const savedAdminTheme = localStorage.getItem('valueInvestAdminTheme');
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
  if (!confirm('DB에 저장된 OpenRouter key를 삭제할까요? env/keys.txt 값은 그대로 둡니다.')) return;
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

function _adminInputStyle() {
  return 'min-height:34px;padding:0 10px;border:1px solid var(--border);border-radius:7px;background:var(--surface);color:var(--text-primary);font:inherit;font-size:13px;';
}

function _esc(str) {
  const el = document.createElement('span');
  el.textContent = str || '';
  return el.innerHTML;
}
