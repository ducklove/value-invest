// Portfolio condition alerts: Telegram linking + alert rule CRUD.
//
// Opens from the 🔔 알림 button in the portfolio toolbar. v1 channel is
// Telegram (bot auto-link via deep link + server-side getUpdates poller);
// rule types are 지정가(price) / 총평가액(nav) / 일간등락률(daily change).
// Kakao can be added later as another channel without touching this rule UI.

const PfAlerts = {
  channel: null,
  alerts: [],
  linkTimer: null,
  linkDeadline: 0,
  category: 'price', // price | nav | daily
};

if (typeof window !== 'undefined') window.PfAlerts = PfAlerts;

function pfAlertsApi(path, options = {}) {
  const init = { ...options };
  if (options.body !== undefined) {
    init.headers = { 'Content-Type': 'application/json', ...(options.headers || {}) };
  }
  return apiFetch(`/api/notifications${path}`, init);
}

function pfFmtNum(value) {
  if (value === null || value === undefined || value === '') return '-';
  const n = Number(value);
  if (!isFinite(n)) return '-';
  return Math.abs(n - Math.round(n)) < 1e-9
    ? Math.round(n).toLocaleString()
    : n.toLocaleString(undefined, { maximumFractionDigits: 2 });
}

// --- Modal open/close -------------------------------------------------------

function pfOpenAlerts() {
  const modal = document.getElementById('pfAlertsModal');
  if (!modal) return;
  modal.style.display = 'flex';
  pfAlertsRenderForm();
  pfAlertsLoadChannel();
  pfAlertsLoadList();
}

function pfCloseAlerts() {
  const modal = document.getElementById('pfAlertsModal');
  if (modal) modal.style.display = 'none';
  pfAlertsStopLinkPoll();
}

document.addEventListener('keydown', (e) => {
  if (e.key !== 'Escape') return;
  const modal = document.getElementById('pfAlertsModal');
  if (modal && modal.style.display !== 'none') pfCloseAlerts();
});

// --- Channel (Telegram) -----------------------------------------------------

async function pfAlertsLoadChannel() {
  const statusEl = document.getElementById('pfAlertChannelStatus');
  const actionsEl = document.getElementById('pfAlertChannelActions');
  if (!statusEl || !actionsEl) return;
  try {
    const resp = await pfAlertsApi('/channels');
    if (!resp.ok) throw new Error('load failed');
    PfAlerts.channel = await resp.json();
  } catch (e) {
    statusEl.textContent = '채널 정보를 불러오지 못했습니다.';
    actionsEl.innerHTML = '';
    return;
  }
  pfAlertsRenderChannel();
}

function pfAlertsRenderChannel() {
  const statusEl = document.getElementById('pfAlertChannelStatus');
  const actionsEl = document.getElementById('pfAlertChannelActions');
  const ch = PfAlerts.channel || {};
  const tg = ch.telegram || {};
  if (!ch.bot_configured) {
    statusEl.innerHTML = '<span class="pf-alert-badge off">서버에 텔레그램 봇이 설정되지 않았습니다.</span>'
      + '<div class="pf-alert-hint">관리자가 <code>TELEGRAM_BOT_TOKEN</code> 을 설정하면 사용할 수 있습니다.</div>';
    actionsEl.innerHTML = '';
    return;
  }
  if (tg.connected) {
    const who = tg.username ? ` (@${escapeHtml(tg.username)})` : '';
    const onoff = tg.enabled
      ? '<span class="pf-alert-badge on">연결됨 · 알림 켜짐</span>'
      : '<span class="pf-alert-badge off">연결됨 · 알림 꺼짐</span>';
    statusEl.innerHTML = onoff + `<span class="pf-alert-who">${who}</span>`;
    actionsEl.innerHTML = `
      <button class="pf-alert-btn" type="button" onclick="pfAlertsTest()">테스트 전송</button>
      <button class="pf-alert-btn" type="button" onclick="pfAlertsToggleChannel(${tg.enabled ? 'false' : 'true'})">${tg.enabled ? '알림 끄기' : '알림 켜기'}</button>
      <button class="pf-alert-btn danger" type="button" onclick="pfAlertsUnlink()">연결 해제</button>`;
  } else {
    statusEl.innerHTML = '<span class="pf-alert-badge off">연결 안 됨</span>';
    actionsEl.innerHTML = `
      <button class="pf-alert-btn primary" type="button" onclick="pfAlertsConnect()">텔레그램 연결</button>
      <span class="pf-alert-hint" id="pfAlertLinkHint"></span>`;
  }
}

async function pfAlertsConnect() {
  const hint = document.getElementById('pfAlertLinkHint');
  try {
    const resp = await pfAlertsApi('/telegram/link', { method: 'POST', body: '{}' });
    const data = await resp.json();
    if (!resp.ok) throw new Error(data.detail || '연결 코드 생성 실패');
    window.open(data.deep_link, '_blank', 'noopener');
    if (hint) hint.textContent = '텔레그램에서 [시작]을 누르면 자동으로 연결됩니다… (대기 중)';
    pfAlertsStartLinkPoll(data.expires_in_minutes || 10);
  } catch (e) {
    if (hint) hint.textContent = (e && e.message) || '연결 코드 생성 실패';
  }
}

function pfAlertsStartLinkPoll(ttlMinutes) {
  pfAlertsStopLinkPoll();
  PfAlerts.linkDeadline = Date.now() + ttlMinutes * 60 * 1000;
  PfAlerts.linkTimer = setInterval(async () => {
    if (Date.now() > PfAlerts.linkDeadline) {
      pfAlertsStopLinkPoll();
      const hint = document.getElementById('pfAlertLinkHint');
      if (hint) hint.textContent = '연결 시간이 만료되었습니다. 다시 시도해주세요.';
      return;
    }
    try {
      const resp = await pfAlertsApi('/telegram/link-status');
      const data = await resp.json();
      if (data.connected) {
        pfAlertsStopLinkPoll();
        pfAlertsLoadChannel();
      }
    } catch (e) { /* keep polling */ }
  }, 2500);
}

function pfAlertsStopLinkPoll() {
  if (PfAlerts.linkTimer) {
    clearInterval(PfAlerts.linkTimer);
    PfAlerts.linkTimer = null;
  }
}

async function pfAlertsTest() {
  try {
    const resp = await pfAlertsApi('/channels/telegram/test', { method: 'POST', body: '{}' });
    const data = await resp.json().catch(() => ({}));
    if (!resp.ok) throw new Error(data.detail || '전송 실패');
    alert('테스트 메시지를 보냈습니다. 텔레그램을 확인하세요.');
  } catch (e) {
    alert((e && e.message) || '테스트 전송에 실패했습니다.');
  }
}

async function pfAlertsToggleChannel(enabled) {
  try {
    await pfAlertsApi('/channels/telegram', {
      method: 'PUT',
      body: JSON.stringify({ enabled }),
    });
  } finally {
    pfAlertsLoadChannel();
  }
}

async function pfAlertsUnlink() {
  if (!confirm('텔레그램 연결을 해제할까요? 알림이 더 이상 전송되지 않습니다.')) return;
  try {
    await pfAlertsApi('/telegram', { method: 'DELETE' });
  } finally {
    pfAlertsLoadChannel();
  }
}

// --- Rule form --------------------------------------------------------------

function pfAlertsRenderForm() {
  const form = document.getElementById('pfAlertForm');
  if (!form) return;
  const cat = PfAlerts.category;
  const holdings = (window.PfStore && PfStore.items) || [];
  const stockOptions = holdings
    .map((it) => {
      const price = it.quote && it.quote.price != null ? ` · 현재가 ${pfFmtNum(it.quote.price)}` : '';
      return `<option value="${escapeHtml(it.stock_code)}">${escapeHtml(it.stock_name || it.stock_code)}${price}</option>`;
    })
    .join('');

  let valueField = '';
  if (cat === 'price') {
    valueField = `
      <select class="pf-modal-input pf-alert-stock" id="pfAlertStock">${stockOptions || '<option value="">보유 종목 없음</option>'}</select>
      <select class="pf-modal-input pf-alert-dir" id="pfAlertDir">
        <option value="above">이상</option><option value="below">이하</option>
      </select>
      <input class="pf-modal-input pf-alert-threshold" id="pfAlertThreshold" type="number" step="any" placeholder="지정가">`;
  } else if (cat === 'nav') {
    valueField = `
      <select class="pf-modal-input pf-alert-dir" id="pfAlertDir">
        <option value="above">이상</option><option value="below">이하</option>
      </select>
      <input class="pf-modal-input pf-alert-threshold" id="pfAlertThreshold" type="number" step="any" placeholder="총평가액(원)">`;
  } else {
    valueField = `
      <select class="pf-modal-input pf-alert-dir" id="pfAlertDir">
        <option value="above">이상</option><option value="below">이하</option>
      </select>
      <input class="pf-modal-input pf-alert-threshold" id="pfAlertThreshold" type="number" step="any" placeholder="일간 등락률(%)">`;
  }

  form.innerHTML = `
    <div class="pf-alert-form-row">
      <select class="pf-modal-input pf-alert-cat" id="pfAlertCat" onchange="pfAlertsSetCategory(this.value)">
        <option value="price"${cat === 'price' ? ' selected' : ''}>종목 지정가</option>
        <option value="nav"${cat === 'nav' ? ' selected' : ''}>포트폴리오 총평가액</option>
        <option value="daily"${cat === 'daily' ? ' selected' : ''}>일간 등락률</option>
      </select>
      ${valueField}
    </div>
    <div class="pf-alert-form-row">
      <input class="pf-modal-input pf-alert-note" id="pfAlertNote" type="text" maxlength="200" placeholder="메모(선택)">
      <button class="pf-modal-add-btn" type="button" onclick="pfAlertsSubmit()">규칙 추가</button>
    </div>`;
}

function pfAlertsSetCategory(cat) {
  PfAlerts.category = cat;
  pfAlertsRenderForm();
}

function pfAlertsBuildType(cat, dir) {
  if (cat === 'price') return dir === 'below' ? 'price_below' : 'price_above';
  if (cat === 'nav') return dir === 'below' ? 'nav_below' : 'nav_above';
  return dir === 'below' ? 'daily_change_below' : 'daily_change_above';
}

async function pfAlertsSubmit() {
  const cat = PfAlerts.category;
  const dir = (document.getElementById('pfAlertDir') || {}).value || 'above';
  const thresholdRaw = (document.getElementById('pfAlertThreshold') || {}).value;
  const note = (document.getElementById('pfAlertNote') || {}).value || '';
  const threshold = Number(thresholdRaw);
  if (!isFinite(threshold) || thresholdRaw === '') {
    alert('임계값을 입력해주세요.');
    return;
  }
  const payload = { alert_type: pfAlertsBuildType(cat, dir), threshold, note };
  if (cat === 'price') {
    const code = (document.getElementById('pfAlertStock') || {}).value;
    if (!code) { alert('종목을 선택해주세요.'); return; }
    payload.stock_code = code;
  }
  try {
    const resp = await pfAlertsApi('/alerts', { method: 'POST', body: JSON.stringify(payload) });
    const data = await resp.json().catch(() => ({}));
    if (!resp.ok) throw new Error(data.detail || '규칙 추가 실패');
    const noteEl = document.getElementById('pfAlertNote');
    const thrEl = document.getElementById('pfAlertThreshold');
    if (noteEl) noteEl.value = '';
    if (thrEl) thrEl.value = '';
    pfAlertsLoadList();
  } catch (e) {
    alert((e && e.message) || '규칙 추가에 실패했습니다.');
  }
}

// --- Rule list --------------------------------------------------------------

async function pfAlertsLoadList() {
  const listEl = document.getElementById('pfAlertList');
  if (!listEl) return;
  try {
    const resp = await pfAlertsApi('/alerts');
    PfAlerts.alerts = await resp.json();
  } catch (e) {
    listEl.innerHTML = '<div class="pf-alert-empty">규칙을 불러오지 못했습니다.</div>';
    return;
  }
  pfAlertsRenderList();
}

function pfAlertsLabel(rule) {
  const t = rule.alert_type;
  const held = (window.PfStore && PfStore.items) || [];
  if (t === 'price_above' || t === 'price_below') {
    const item = held.find((it) => it.stock_code === rule.stock_code);
    const name = item ? (item.stock_name || rule.stock_code) : rule.stock_code;
    return `${escapeHtml(name)} 현재가 ${pfFmtNum(rule.threshold)} ${t.endsWith('above') ? '이상' : '이하'}`;
  }
  if (t === 'nav_above' || t === 'nav_below') {
    return `총평가액 ${pfFmtNum(rule.threshold)}원 ${t.endsWith('above') ? '이상' : '이하'}`;
  }
  return `일간 등락률 ${Number(rule.threshold).toFixed(2)}% ${t.endsWith('above') ? '이상' : '이하'}`;
}

function pfAlertsRenderList() {
  const listEl = document.getElementById('pfAlertList');
  const rules = PfAlerts.alerts || [];
  if (!rules.length) {
    listEl.innerHTML = '<div class="pf-alert-empty">등록된 알림 규칙이 없습니다.</div>';
    return;
  }
  listEl.innerHTML = rules.map((rule) => {
    const armed = rule.armed
      ? '<span class="pf-alert-state armed" title="조건 충족 시 발송 대기">대기</span>'
      : '<span class="pf-alert-state fired" title="이미 발송됨. 조건이 풀리면 다시 무장">발송됨</span>';
    const note = (rule.note || '').trim();
    const noteHtml = note ? `<span class="pf-alert-rule-note">📝 ${escapeHtml(note)}</span>` : '';
    return `
      <div class="pf-alert-rule ${rule.enabled ? '' : 'disabled'}">
        <div class="pf-alert-rule-main">
          <span class="pf-alert-rule-label">${pfAlertsLabel(rule)}</span>
          ${noteHtml}
        </div>
        <div class="pf-alert-rule-actions">
          ${rule.enabled ? armed : '<span class="pf-alert-state off">꺼짐</span>'}
          <button class="pf-alert-btn" type="button" onclick="pfAlertsToggle(${rule.id}, ${rule.enabled ? 'false' : 'true'})">${rule.enabled ? '끄기' : '켜기'}</button>
          <button class="pf-alert-btn danger" type="button" onclick="pfAlertsDelete(${rule.id})">삭제</button>
        </div>
      </div>`;
  }).join('');
}

async function pfAlertsToggle(id, enabled) {
  try {
    await pfAlertsApi(`/alerts/${id}`, { method: 'PUT', body: JSON.stringify({ enabled }) });
  } finally {
    pfAlertsLoadList();
  }
}

async function pfAlertsDelete(id) {
  if (!confirm('이 알림 규칙을 삭제할까요?')) return;
  try {
    await pfAlertsApi(`/alerts/${id}`, { method: 'DELETE' });
  } finally {
    pfAlertsLoadList();
  }
}

if (typeof window !== 'undefined') {
  Object.assign(window, {
    pfOpenAlerts, pfCloseAlerts,
    pfAlertsConnect, pfAlertsTest, pfAlertsToggleChannel, pfAlertsUnlink,
    pfAlertsSetCategory, pfAlertsSubmit, pfAlertsToggle, pfAlertsDelete,
  });
}
