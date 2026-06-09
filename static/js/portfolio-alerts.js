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
  category: 'price', // price | target | dailyAbs | nav | daily
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

function pfAlertsChannelActions(key, enabled) {
  return `<span class="pf-alert-channel-actions">
    <button class="pf-alert-btn" type="button" onclick="pfAlertsTest('${key}')">테스트</button>
    <button class="pf-alert-btn" type="button" onclick="pfAlertsToggleChannel('${key}', ${enabled ? 'false' : 'true'})">${enabled ? '끄기' : '켜기'}</button>
    <button class="pf-alert-btn danger" type="button" onclick="pfAlertsUnlink('${key}')">해제</button>
  </span>`;
}

function pfAlertsConnectedRow(name, enabled, who) {
  const onoff = enabled
    ? '<span class="pf-alert-badge on">알림 켜짐</span>'
    : '<span class="pf-alert-badge off">알림 꺼짐</span>';
  const whoHtml = who ? `<span class="pf-alert-who">${who}</span>` : '';
  const key = name === '카카오톡' ? 'kakao' : 'telegram';
  return `<div class="pf-alert-channel">
    <span class="pf-alert-channel-name">${name}</span>${onoff}${whoHtml}
    ${pfAlertsChannelActions(key, enabled)}
  </div>`;
}

function pfAlertsTelegramBlock(tg) {
  if (tg.connected) {
    const who = tg.username ? `@${escapeHtml(tg.username)}` : (tg.chat_id ? `chat ${escapeHtml(String(tg.chat_id))}` : '');
    return pfAlertsConnectedRow('텔레그램', tg.enabled, who);
  }
  return `<div class="pf-alert-channel-reg">
    <div class="pf-alert-channel-reg-head"><span class="pf-alert-channel-name">텔레그램</span><span class="pf-alert-badge off">연결 안 됨</span></div>
    <div class="pf-alert-reg-form">
      <input class="pf-modal-input" id="pfTgToken" type="text" placeholder="봇 토큰 (BotFather에서 발급)" autocomplete="off">
      <input class="pf-modal-input" id="pfTgChat" type="text" placeholder="chat_id (선택 · 비우면 자동 감지)" autocomplete="off">
      <button class="pf-alert-btn primary" type="button" onclick="pfAlertsTelegramRegister()">연결</button>
    </div>
    <div class="pf-alert-hint" id="pfTgHint"></div>
  </div>`;
}

function pfAlertsKakaoBlock(kk) {
  if (kk.connected) {
    const who = kk.nickname ? escapeHtml(kk.nickname) : '';
    return pfAlertsConnectedRow('카카오톡', kk.enabled, who);
  }
  const redirect = kk.redirect_uri || '';
  const redirectHtml = redirect
    ? `<div class="pf-alert-hint">카카오 앱의 <b>Redirect URI</b>에 아래 주소를 등록하세요:<br><code class="pf-alert-redirect">${escapeHtml(redirect)}</code></div>`
    : '<div class="pf-alert-hint">서버 주소를 확인할 수 없어 Redirect URI를 표시하지 못했습니다.</div>';
  return `<div class="pf-alert-channel-reg">
    <div class="pf-alert-channel-reg-head"><span class="pf-alert-channel-name">카카오톡</span><span class="pf-alert-badge off">연결 안 됨</span></div>
    <div class="pf-alert-reg-form">
      <input class="pf-modal-input" id="pfKkKey" type="text" placeholder="카카오 REST API 키" autocomplete="off">
      <button class="pf-alert-btn primary" type="button" onclick="pfAlertsKakaoConnect()">카카오 연결</button>
    </div>
    ${redirectHtml}
    <div class="pf-alert-hint" id="pfKkHint"></div>
  </div>`;
}

function pfAlertsRenderChannels() {
  const el = document.getElementById('pfAlertChannels');
  const data = PfAlerts.channels || {};
  el.innerHTML =
    pfAlertsTelegramBlock(data.telegram || {})
    + pfAlertsKakaoBlock(data.kakao || {});
}

async function pfAlertsTelegramRegister() {
  const hint = document.getElementById('pfTgHint');
  const token = ((document.getElementById('pfTgToken') || {}).value || '').trim();
  const chat = ((document.getElementById('pfTgChat') || {}).value || '').trim();
  if (!token) { if (hint) hint.textContent = '봇 토큰을 입력하세요.'; return; }
  if (hint) hint.textContent = '확인 중…';
  try {
    const resp = await pfAlertsApi('/telegram/register', {
      method: 'POST', body: JSON.stringify({ bot_token: token, chat_id: chat }),
    });
    const data = await resp.json().catch(() => ({}));
    if (!resp.ok) throw new Error(data.detail || '연결 실패');
    if (data.connected) pfAlertsLoadChannels();
    else if (hint) hint.textContent = data.detail || '봇에게 메시지를 보낸 뒤 다시 [연결]을 누르세요.';
  } catch (e) {
    if (hint) hint.textContent = (e && e.message) || '연결에 실패했습니다.';
  }
}

async function pfAlertsKakaoConnect() {
  const hint = document.getElementById('pfKkHint');
  const key = ((document.getElementById('pfKkKey') || {}).value || '').trim();
  if (!key) { if (hint) hint.textContent = '카카오 REST API 키를 입력하세요.'; return; }
  try {
    const resp = await pfAlertsApi('/kakao/connect', { method: 'POST', body: JSON.stringify({ rest_key: key }) });
    const data = await resp.json().catch(() => ({}));
    if (!resp.ok) throw new Error(data.detail || '연결 실패');
    window.open(data.authorize_url, '_blank', 'width=480,height=720');
    if (hint) hint.textContent = '카카오 로그인 후 동의하면 연결됩니다… (대기 중)';
    pfAlertsStartPoll('kakao', data.expires_in_minutes || 10);
  } catch (e) {
    if (hint) hint.textContent = (e && e.message) || '연결에 실패했습니다.';
  }
}

function pfAlertsStartPoll(kind, ttlMinutes) {
  pfAlertsStopPoll();
  PfAlerts.pollKind = kind;
  PfAlerts.pollDeadline = Date.now() + ttlMinutes * 60 * 1000;
  PfAlerts.pollTimer = setInterval(async () => {
    if (Date.now() > PfAlerts.pollDeadline) {
      pfAlertsStopPoll();
      const hint = document.getElementById(kind === 'kakao' ? 'pfKkHint' : 'pfTgHint');
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
    valueField = '<span class="pf-alert-form-note">보유 전 종목의 목표가 도달 시 알림 (켜고 끄기만)</span>';
  } else if (cat === 'limit') {
    valueField = '<span class="pf-alert-form-note">보유 전 종목 중 상한가·하한가 도달 시 알림 (켜고 끄기만)</span>';
  } else if (cat === 'dailyAbs') {
    valueField = '<span class="pf-alert-form-note">전 종목, 하루</span>'
      + '<input class="pf-modal-input pf-alert-threshold" id="pfAlertThreshold" type="number" step="any" min="0" placeholder="±% (예: 5)">'
      + '<span class="pf-alert-form-note">이상 등락 시</span>';
  } else if (cat === 'nav') {
    valueField = pfAlertsDirSelect()
      + '<input class="pf-modal-input pf-alert-threshold" id="pfAlertThreshold" type="number" step="any" placeholder="총평가액(원)">';
  } else { // daily (포트폴리오 전체)
    valueField = pfAlertsDirSelect()
      + '<input class="pf-modal-input pf-alert-threshold" id="pfAlertThreshold" type="number" step="any" placeholder="등락률(%)">';
  }

  form.innerHTML = `
    <div class="pf-alert-form-row">
      <select class="pf-modal-input pf-alert-cat" id="pfAlertCat" onchange="pfAlertsSetCategory(this.value)">
        <option value="price"${cat === 'price' ? ' selected' : ''}>종목 지정가</option>
        <option value="target"${cat === 'target' ? ' selected' : ''}>목표가 도달 (전체)</option>
        <option value="limit"${cat === 'limit' ? ' selected' : ''}>상한가·하한가 도달 (전체)</option>
        <option value="dailyAbs"${cat === 'dailyAbs' ? ' selected' : ''}>종목 일간 등락률 (전체)</option>
        <option value="nav"${cat === 'nav' ? ' selected' : ''}>포트폴리오 총평가액</option>
        <option value="daily"${cat === 'daily' ? ' selected' : ''}>포트폴리오 일간 등락률</option>
      </select>
      ${valueField}
    </div>
    <div class="pf-alert-form-row">
      <input class="pf-modal-input pf-alert-note" id="pfAlertNote" type="text" maxlength="200" placeholder="메모(선택)">
      <label class="pf-alert-important-check" title="발송 시 강조 헤더로 더 눈에 띄게 보냅니다">
        <input type="checkbox" id="pfAlertImportant"> 🚨 중요
      </label>
      <button class="pf-modal-add-btn" type="button" onclick="pfAlertsSubmit()">${cat === 'target' ? '목표가 알림 켜기' : cat === 'limit' ? '상하한가 알림 켜기' : '규칙 추가'}</button>
    </div>`;
}

function pfAlertsSetCategory(cat) {
  PfAlerts.category = cat;
  pfAlertsRenderForm();
}

// 카테고리 -> {alert_type} 매핑. 서버가 alert_type 으로 scope 를 유추한다.
function pfAlertsBuildType(cat, dir) {
  if (cat === 'price') return { alert_type: dir === 'below' ? 'price_below' : 'price_above' };
  if (cat === 'target') return { alert_type: 'target_reached' };
  if (cat === 'limit') return { alert_type: 'limit_reached' };
  if (cat === 'dailyAbs') return { alert_type: 'daily_change_abs' };
  if (cat === 'nav') return { alert_type: dir === 'below' ? 'nav_below' : 'nav_above' };
  return { alert_type: dir === 'below' ? 'daily_change_below' : 'daily_change_above' };
}

async function pfAlertsSubmit() {
  const cat = PfAlerts.category;
  const dir = (document.getElementById('pfAlertDir') || {}).value || 'above';
  const note = (document.getElementById('pfAlertNote') || {}).value || '';
  const important = !!((document.getElementById('pfAlertImportant') || {}).checked);
  const payload = { ...pfAlertsBuildType(cat, dir), note, important };

  if (cat === 'price') {
    const code = (document.getElementById('pfAlertStock') || {}).value;
    if (!code) { alert('종목을 선택해주세요.'); return; }
    payload.stock_code = code;
  }
  if (cat !== 'target' && cat !== 'limit') {
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
    const impEl = document.getElementById('pfAlertImportant');
    if (noteEl) noteEl.value = '';
    if (thrEl) thrEl.value = '';
    if (impEl) impEl.checked = false;
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
    return '보유 전 종목 — 목표가 도달 시';
  }
  if (t === 'limit_reached') {
    return '보유 전 종목 — 상한가·하한가 도달 시';
  }
  if (t === 'daily_change_abs') {
    return `보유 전 종목 — 일간 등락률 ±${pfFmtNum(rule.threshold)}% 이상`;
  }
  if (t === 'price_above' || t === 'price_below') {
    return `${escapeHtml(pfAlertsStockName(rule.stock_code))} 현재가 ${pfFmtNum(rule.threshold)} ${dir}`;
  }
  if (t === 'nav_above' || t === 'nav_below') {
    return `총평가액 ${pfFmtNum(rule.threshold)}원 ${dir}`;
  }
  // daily_change_above / below (포트폴리오 전체)
  return `포트폴리오 일간 등락률 ${Number(rule.threshold).toFixed(2)}% ${dir}`;
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
    const important = !!rule.important;
    const flagHtml = important ? '<span class="pf-alert-flag" title="중요 알림 — 강조해서 발송">🚨 중요</span>' : '';
    const starBtn = `<button class="pf-alert-btn pf-alert-star${important ? ' on' : ''}" type="button" title="${important ? '중요 표시 해제' : '중요 알림으로 표시'}" onclick="pfAlertsToggleImportant(${rule.id}, ${important ? 'false' : 'true'})">${important ? '★' : '☆'}</button>`;
    return `
      <div class="pf-alert-rule ${rule.enabled ? '' : 'disabled'}${important ? ' important' : ''}">
        <div class="pf-alert-rule-main">
          <span class="pf-alert-rule-label">${flagHtml}${pfAlertsLabel(rule)}</span>
          ${noteHtml}
        </div>
        <div class="pf-alert-rule-actions">
          ${rule.enabled ? armed : '<span class="pf-alert-state off">꺼짐</span>'}
          ${starBtn}
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

async function pfAlertsToggleImportant(id, important) {
  try {
    await pfAlertsApi(`/alerts/${id}`, { method: 'PUT', body: JSON.stringify({ important }) });
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
    pfAlertsTelegramRegister, pfAlertsKakaoConnect,
    pfAlertsTest, pfAlertsToggleChannel, pfAlertsUnlink,
    pfAlertsSetCategory, pfAlertsSubmit, pfAlertsToggle, pfAlertsToggleImportant, pfAlertsDelete,
  });
}
