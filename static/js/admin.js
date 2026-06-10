/* Admin dashboard — bootstrap, AI ops config, and shared helpers.
   Observability panels live in admin-observability.js; linked-project
   config + dividend admin live in admin-linked-projects.js. */

let _adminLoaded = false;
let _aiAdminConfig = null;

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
    const [deployRes, batchRes, serverRes, dbRes, usersRes, summaryRes, eventsRes, httpRes, linkedConfigsRes, aiConfigRes] = await Promise.all([
      apiFetch('/api/admin/deploy-status'),
      apiFetch('/api/admin/batch-status'),
      apiFetch('/api/admin/server-stats'),
      apiFetch('/api/admin/db-stats'),
      apiFetch('/api/admin/users'),
      apiFetch('/api/admin/event-summary?hours=24'),
      apiFetch('/api/admin/events?limit=50'),
      apiFetch('/api/admin/http-metrics?hours=24'),
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
    const httpMetrics = httpRes.ok ? await httpRes.json() : {endpoints: []};
    _linkedProjectConfigs = linkedConfigsRes.ok ? await linkedConfigsRes.json() : [];
    _aiAdminConfig = aiConfigRes.ok ? await aiConfigRes.json() : null;
    container.innerHTML = _renderAdmin(deploy, batch, server, db, users, summary, events, httpMetrics, _linkedProjectConfigs, _aiAdminConfig);
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

function _renderAdmin(deploy, batch, server, db, users, summary, events, httpMetrics, linkedConfigs, aiConfig) {
  return `
    <div class="admin-dashboard">
      <h2 class="admin-title">시스템 관리</h2>
      ${_renderDeployCard(deploy)}
      <div id="adminLiveSection">${_renderServerCard(server)}</div>
      ${_renderBatchSection(batch)}
      ${_renderSubsystemSummary(summary)}
      ${_renderHttpMetricsSection(httpMetrics)}
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

// --- Shared helpers (used by all admin-* modules) ---

function _adminInputStyle() {
  return 'padding:6px 10px;border:1px solid var(--border);border-radius:4px;background:var(--surface);color:var(--text-primary);';
}

function _esc(str) {
  const el = document.createElement('span');
  el.textContent = str || '';
  return el.innerHTML;
}
