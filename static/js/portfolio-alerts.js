// Portfolio condition alerts: notification channels (텔레그램 + 카카오톡) + rules.
//
// Opens from the 🔔 알림 button in the portfolio toolbar.
// 채널: 텔레그램(봇 딥링크 + getUpdates 폴링) / 카카오톡(OAuth "나에게 보내기").
// 규칙: 종목 지정가 / 종목 목표가 달성 / 종목 일간등락률 /
//       포트폴리오 총평가액 / 포트폴리오 일간등락률. 엣지 트리거(서버측).

const PfAlerts = {
  channels: null,
  alerts: [],
  pollTimer: null,
  pollDeadline: 0,
  pollKind: null, // 'telegram' | 'kakao'
  category: 'price', // price | target | stockDaily | nav | daily
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
  pfAlertsLoadChannels();
  pfAlertsLoadList();
}

function pfCloseAlerts() {
  const modal = document.getElementById('pfAlertsModal');
  if (modal) modal.style.display = 'none';
  pfAlertsStopPoll();
}

document.addEventListener('keydown', (e) => {
  if (e.key !== 'Escape') return;
  const modal = document.getElementById('pfAlertsModal');
  if (modal && modal.style.display !== 'none') pfCloseAlerts();
});

function pfAlertsToggleHelp() {
  const help = document.getElementById('pfAlertHelp');
  const btn = document.querySelector('.pf-alert-help-toggle');
  if (!help) return;
  const show = help.hasAttribute('hidden');
  if (show) help.removeAttribute('hidden');
  else help.setAttribute('hidden', '');
  if (btn) {
    btn.setAttribute('aria-expanded', show ? 'true' : 'false');
    btn.textContent = show ? '연결 방법 ▴' : '연결 방법 ▾';
  }
}

// --- Channels (telegram + kakao) -------------------------------------------

async function pfAlertsLoadChannels() {
  const el = document.getElementById('pfAlertChannels');
  if (!el) return;
  try {
    const resp = await pfAlertsApi('/channels');
    if (!resp.ok) throw new Error('load failed');
    PfAlerts.channels = await resp.json();
  } catch (e) {
    el.textContent = '채널 정보를 불러오지 못했습니다.';
    return;
  }
  pfAlertsRenderChannels();
}

function pfAlertsChannelRow(opts) {
  // opts: {key, name, ch, connectFn, badgeWho}
  const { key, name, ch, connectLabel, badgeWho } = opts;
  if (!ch.configured) {
    return `<div class="pf-alert-channel">
      <span class="pf-alert-channel-name">${name}</span>
      <span class="pf-alert-badge off">서버 미설정</span>
    </div>`;
  }
  if (ch.connected) {
    const who = badgeWho ? `<span class="pf-alert-who">${badgeWho}</span>` : '';
    const onoff = ch.enabled
      ? '<span class="pf-alert-badge on">알림 켜짐</span>'
      : '<span class="pf-alert-badge off">알림 꺼짐</span>';
    return `<div class="pf-alert-channel">
      <span class="pf-alert-channel-name">${name}</span>
      ${onoff}${who}
      <span class="pf-alert-channel-actions">
        <button class="pf-alert-btn" type="button" onclick="pfAlertsTest('${key}')">테스트</button>
        <button class="pf-alert-btn" type="button" onclick="pfAlertsToggleChannel('${key}', ${ch.enabled ? 'false' : 'true'})">${ch.enabled ? '끄기' : '켜기'}</button>
        <button class="pf-alert-btn danger" type="button" onclick="pfAlertsUnlink('${key}')">해제</button>
      </span>
    </div>`;
  }
  return `<div class="pf-alert-channel">
    <span class="pf-alert-channel-name">${name}</span>
    <span class="pf-alert-badge off">연결 안 됨</span>
    <span class="pf-alert-channel-actions">
      <button class="pf-alert-btn primary" type="button" onclick="pfAlertsConnect('${key}')">${connectLabel}</button>
    </span>
  </div>`;
}

function pfAlertsRenderChannels() {
  const el = document.getElementById('pfAlertChannels');
  const data = PfAlerts.channels || {};
  const tg = data.telegram || { configured: false };
  const kk = data.kakao || { configured: false };
  let html = '';
  html += pfAlertsChannelRow({
    key: 'telegram', name: '텔레그램', ch: tg, connectLabel: '연결',
    badgeWho: tg.username ? `@${escapeHtml(tg.username)}` : '',
  });
  html += pfAlertsChannelRow({
    key: 'kakao', name: '카카오톡', ch: kk, connectLabel: '카카오 로그인',
    badgeWho: kk.nickname ? escapeHtml(kk.nickname) : '',
  });
  if (!tg.configured && !kk.configured) {
    html += '<div class="pf-alert-hint">서버에 <code>TELEGRAM_BOT_TOKEN</code> 또는 <code>KAKAO_REST_API_KEY</code> 가 설정되면 연결할 수 있습니다.</div>';
  }
  if (kk.configured) {
    html += '<div class="pf-alert-hint">카카오는 앱에 등록된 <b>팀원·테스트 사용자</b>만 바로 연결됩니다. 일반 사용자에게 열려면 카카오 <b>권한 검수</b>가 필요합니다. 텔레그램은 누구나 즉시 연결됩니다.</div>';
  }
  html += '<div class="pf-alert-hint" id="pfAlertConnectHint"></div>';
  el.innerHTML = html;
}

async function pfAlertsConnect(key) {
  const hint = document.getElementById('pfAlertConnectHint');
  try {
    if (key === 'telegram') {
      const resp = await pfAlertsApi('/telegram/link', { method: 'POST', body: '{}' });
      const data = await resp.json();
      if (!resp.ok) throw new Error(data.detail || '연결 코드 생성 실패');
      window.open(data.deep_link, '_blank', 'noopener');
      if (hint) hint.textContent = '텔레그램에서 [시작]을 누르면 자동으로 연결됩니다… (대기 중)';
      pfAlertsStartPoll('telegram', data.expires_in_minutes || 10);
    } else {
      const resp = await pfAlertsApi('/kakao/connect');
      const data = await resp.json();
      if (!resp.ok) throw new Error(data.detail || '카카오 연결 시작 실패');
      window.open(data.authorize_url, '_blank', 'width=480,height=720');
      if (hint) hint.textContent = '카카오 로그인 후 동의하면 연결됩니다… (대기 중)';
      pfAlertsStartPoll('kakao', data.expires_in_minutes || 10);
    }
  } catch (e) {
    if (hint) hint.textContent = (e && e.message) || '연결을 시작하지 못했습니다.';
  }
}

function pfAlertsStartPoll(kind, ttlMinutes) {
  pfAlertsStopPoll();
  PfAlerts.pollKind = kind;
  PfAlerts.pollDeadline = Date.now() + ttlMinutes * 60 * 1000;
  PfAlerts.pollTimer = setInterval(async () => {
    if (Date.now() > PfAlerts.pollDeadline) {
      pfAlertsStopPoll();
      const hint = document.getElementById('pfAlertConnectHint');
      if (hint) hint.textContent = '연결 시간이 만료되었습니다. 다시 시도해주세요.';
      return;
    }
    try {
      const resp = await pfAlertsApi('/channels');
      const data = await resp.json();
      const ch = data[kind] || {};
      if (ch.connected) {
        PfAlerts.channels = data;
        pfAlertsStopPoll();
        pfAlertsRenderChannels();
      }
    } catch (e) { /* keep polling */ }
  }, 2500);
}

function pfAlertsStopPoll() {
  if (PfAlerts.pollTimer) {
    clearInterval(PfAlerts.pollTimer);
    PfAlerts.pollTimer = null;
  }
}

async function pfAlertsTest(key) {
  try {
    const resp = await pfAlertsApi(`/channels/${key}/test`, { method: 'POST', body: '{}' });
    const data = await resp.json().catch(() => ({}));
    if (!resp.ok) throw new Error(data.detail || '전송 실패');
    alert('테스트 메시지를 보냈습니다. 메신저를 확인하세요.');
  } catch (e) {
    alert((e && e.message) || '테스트 전송에 실패했습니다.');
  }
}

async function pfAlertsToggleChannel(key, enabled) {
  try {
    await pfAlertsApi(`/channels/${key}`, { method: 'PUT', body: JSON.stringify({ enabled }) });
  } finally {
    pfAlertsLoadChannels();
  }
}

async function pfAlertsUnlink(key) {
  const label = key === 'kakao' ? '카카오톡' : '텔레그램';
  if (!confirm(`${label} 연결을 해제할까요? 해당 채널로 알림이 전송되지 않습니다.`)) return;
  try {
    await pfAlertsApi(`/${key}`, { method: 'DELETE' });
  } finally {
    pfAlertsLoadChannels();
  }
}

// --- Rule form --------------------------------------------------------------

function pfAlertsStockSelect() {
  const holdings = (window.PfStore && PfStore.items) || [];
  const options = holdings
    .map((it) => {
      const price = it.quote && it.quote.price != null ? ` · 현재가 ${pfFmtNum(it.quote.price)}` : '';
      return `<option value="${escapeHtml(it.stock_code)}">${escapeHtml(it.stock_name || it.stock_code)}${price}</option>`;
    })
    .join('');
  return `<select class="pf-modal-input pf-alert-stock" id="pfAlertStock">${options || '<option value="">보유 종목 없음</option>'}</select>`;
}

function pfAlertsDirSelect() {
  return `<select class="pf-modal-input pf-alert-dir" id="pfAlertDir">
    <option value="above">이상</option><option value="below">이하</option>
  </select>`;
}

function pfAlertsRenderForm() {
  const form = document.getElementById('pfAlertForm');
  if (!form) return;
  const cat = PfAlerts.category;
  let valueField = '';
  if (cat === 'price') {
    valueField = pfAlertsStockSelect() + pfAlertsDirSelect()
      + '<input class="pf-modal-input pf-alert-threshold" id="pfAlertThreshold" type="number" step="any" placeholder="지정가">';
  } else if (cat === 'target') {
    valueField = pfAlertsStockSelect()
      + '<span class="pf-alert-form-note">설정한 목표가에 도달하면 알림</span>';
  } else if (cat === 'stockDaily') {
    valueField = pfAlertsStockSelect() + pfAlertsDirSelect()
      + '<input class="pf-modal-input pf-alert-threshold" id="pfAlertThreshold" type="number" step="any" placeholder="등락률(%)">';
  } else if (cat === 'nav') {
    valueField = pfAlertsDirSelect()
      + '<input class="pf-modal-input pf-alert-threshold" id="pfAlertThreshold" type="number" step="any" placeholder="총평가액(원)">';
  } else { // daily
    valueField = pfAlertsDirSelect()
      + '<input class="pf-modal-input pf-alert-threshold" id="pfAlertThreshold" type="number" step="any" placeholder="등락률(%)">';
  }

  form.innerHTML = `
    <div class="pf-alert-form-row">
      <select class="pf-modal-input pf-alert-cat" id="pfAlertCat" onchange="pfAlertsSetCategory(this.value)">
        <option value="price"${cat === 'price' ? ' selected' : ''}>종목 지정가</option>
        <option value="target"${cat === 'target' ? ' selected' : ''}>종목 목표가 달성</option>
        <option value="stockDaily"${cat === 'stockDaily' ? ' selected' : ''}>종목 일간 등락률</option>
        <option value="nav"${cat === 'nav' ? ' selected' : ''}>포트폴리오 총평가액</option>
        <option value="daily"${cat === 'daily' ? ' selected' : ''}>포트폴리오 일간 등락률</option>
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

// 카테고리 -> {alert_type, scope} 매핑. 지정가/목표가는 서버가 stock 스코프를
// 유추하므로 scope 생략; 일간등락률은 stock/portfolio 가 같은 타입이라 명시.
function pfAlertsBuildType(cat, dir) {
  if (cat === 'price') return { alert_type: dir === 'below' ? 'price_below' : 'price_above' };
  if (cat === 'target') return { alert_type: 'target_reached' };
  if (cat === 'stockDaily') return { alert_type: dir === 'below' ? 'daily_change_below' : 'daily_change_above', scope: 'stock' };
  if (cat === 'nav') return { alert_type: dir === 'below' ? 'nav_below' : 'nav_above' };
  return { alert_type: dir === 'below' ? 'daily_change_below' : 'daily_change_above', scope: 'portfolio' };
}

async function pfAlertsSubmit() {
  const cat = PfAlerts.category;
  const dir = (document.getElementById('pfAlertDir') || {}).value || 'above';
  const note = (document.getElementById('pfAlertNote') || {}).value || '';
  const payload = { ...pfAlertsBuildType(cat, dir), note };

  const needsStock = cat === 'price' || cat === 'target' || cat === 'stockDaily';
  if (needsStock) {
    const code = (document.getElementById('pfAlertStock') || {}).value;
    if (!code) { alert('종목을 선택해주세요.'); return; }
    payload.stock_code = code;
  }
  if (cat !== 'target') {
    const thresholdRaw = (document.getElementById('pfAlertThreshold') || {}).value;
    const threshold = Number(thresholdRaw);
    if (!isFinite(threshold) || thresholdRaw === '') { alert('임계값을 입력해주세요.'); return; }
    payload.threshold = threshold;
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

function pfAlertsStockName(code) {
  const held = (window.PfStore && PfStore.items) || [];
  const item = held.find((it) => it.stock_code === code);
  return item ? (item.stock_name || code) : code;
}

function pfAlertsLabel(rule) {
  const t = rule.alert_type;
  const dir = t.endsWith('above') ? '이상' : '이하';
  if (t === 'target_reached') {
    return `${escapeHtml(pfAlertsStockName(rule.stock_code))} 목표가 달성 시`;
  }
  if (t === 'price_above' || t === 'price_below') {
    return `${escapeHtml(pfAlertsStockName(rule.stock_code))} 현재가 ${pfFmtNum(rule.threshold)} ${dir}`;
  }
  if (t === 'nav_above' || t === 'nav_below') {
    return `총평가액 ${pfFmtNum(rule.threshold)}원 ${dir}`;
  }
  // daily_change_*
  const pct = `${Number(rule.threshold).toFixed(2)}% ${dir}`;
  return rule.scope === 'stock'
    ? `${escapeHtml(pfAlertsStockName(rule.stock_code))} 일간 등락률 ${pct}`
    : `포트폴리오 일간 등락률 ${pct}`;
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
    pfOpenAlerts, pfCloseAlerts, pfAlertsToggleHelp,
    pfAlertsConnect, pfAlertsTest, pfAlertsToggleChannel, pfAlertsUnlink,
    pfAlertsSetCategory, pfAlertsSubmit, pfAlertsToggle, pfAlertsDelete,
  });
}
