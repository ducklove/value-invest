// 종목 분석 화면 — 개별 종목 알림(가격 도달 / 일간 등락률 / 신규 공시 / 신규 리포트).
//
// 로그인 사용자만 사용. 기존 /api/notifications/alerts 를 source:'analysis' 로 호출해
// 보유하지 않은 종목에도 건다(포트폴리오 조건 알림과 같은 엔진·채널·엣지트리거 공유).
// - 가격 도달: 한 종목에 여러 개(이상/이하 × 가격).
// - 일간 등락률(±%) / 신규 공시 / 신규 리포트: 종목당 1개(서버가 upsert).

const StockAlerts = { code: null, name: '', alerts: [] };
if (typeof window !== 'undefined') window.StockAlerts = StockAlerts;

function saApi(path, options = {}) {
  const init = { ...options };
  if (options.body !== undefined) {
    init.headers = { 'Content-Type': 'application/json', ...(options.headers || {}) };
  }
  return apiFetch(`/api/notifications${path}`, init);
}

function saFmtNum(value) {
  if (value === null || value === undefined || value === '') return '-';
  const n = Number(value);
  if (!isFinite(n)) return '-';
  return Math.abs(n - Math.round(n)) < 1e-9
    ? Math.round(n).toLocaleString()
    : n.toLocaleString(undefined, { maximumFractionDigits: 2 });
}

function saFindAlert(type) {
  return (StockAlerts.alerts || []).find((a) => a.alert_type === type);
}

function saPriceAlerts() {
  return (StockAlerts.alerts || []).filter(
    (a) => a.alert_type === 'price_above' || a.alert_type === 'price_below',
  );
}

function saLoggedIn() {
  return typeof currentUser !== 'undefined' && !!currentUser;
}

// --- Modal open/close -------------------------------------------------------

function openStockAlerts() {
  if (!saLoggedIn()) {
    if (typeof setPreferenceStatus === 'function') {
      setPreferenceStatus('로그인하면 이 종목의 가격·등락률·공시·리포트 알림을 설정할 수 있습니다.', 'warning');
    } else {
      alert('로그인하면 종목 알림을 설정할 수 있습니다.');
    }
    return;
  }
  const code = (typeof activeStockCode !== 'undefined' && activeStockCode) ? activeStockCode : null;
  if (!code) { alert('먼저 종목을 분석해주세요.'); return; }
  StockAlerts.code = code;
  const nameEl = document.getElementById('companyName');
  StockAlerts.name = (nameEl && nameEl.textContent) ? nameEl.textContent.trim() : code;
  const modal = document.getElementById('stockAlertModal');
  if (!modal) return;
  modal.style.display = 'flex';
  StockAlerts.alerts = [];
  saRender();        // 즉시 빈 폼 표시
  saLoadList();      // 서버 상태로 갱신
}

function closeStockAlerts() {
  const modal = document.getElementById('stockAlertModal');
  if (modal) modal.style.display = 'none';
}

document.addEventListener('keydown', (e) => {
  if (e.key !== 'Escape') return;
  const modal = document.getElementById('stockAlertModal');
  if (modal && modal.style.display !== 'none') closeStockAlerts();
});

// --- Load + render ----------------------------------------------------------

async function saLoadList() {
  try {
    const resp = await saApi(`/alerts?stock_code=${encodeURIComponent(StockAlerts.code)}`);
    StockAlerts.alerts = await resp.json();
  } catch (e) {
    StockAlerts.alerts = [];
  }
  saRender();
}

function saRender() {
  const nameEl = document.getElementById('stockAlertStockName');
  if (nameEl) nameEl.textContent = StockAlerts.name || StockAlerts.code || '종목';
  saRenderForm();
  saRenderList();
}

function saToggleBtn(type, rule) {
  const on = !!(rule && rule.enabled);
  return `<button class="pf-alert-btn sa-toggle${on ? ' on' : ''}" type="button" onclick="saToggleFeed('${type}', ${on ? 'false' : 'true'})">${on ? '켜짐' : '꺼짐'}</button>`;
}

function saRenderForm() {
  const form = document.getElementById('stockAlertForm');
  if (!form) return;
  const daily = saFindAlert('stock_daily_abs');
  const disc = saFindAlert('disclosure_new');
  const rep = saFindAlert('report_new');
  const dailyVal = daily ? saFmtNum(daily.threshold) : '';
  form.innerHTML = `
    <div class="pf-alert-form-row">
      <span class="sa-field-label">가격 도달</span>
      <select class="pf-modal-input sa-price-dir" id="saPriceDir">
        <option value="above">이상</option><option value="below">이하</option>
      </select>
      <input class="pf-modal-input sa-price-val" id="saPriceVal" type="number" step="any" placeholder="가격">
      <button class="pf-modal-add-btn" type="button" onclick="saAddPrice()">추가</button>
    </div>
    <div class="pf-alert-form-row">
      <span class="sa-field-label">일간 등락률</span>
      <input class="pf-modal-input sa-daily-val" id="saDailyVal" type="number" step="any" min="0" placeholder="±% (예: 5)" value="${dailyVal}">
      <button class="pf-modal-add-btn" type="button" onclick="saSetDaily()">${daily ? '변경' : '설정'}</button>
      ${daily ? `<button class="pf-alert-btn danger" type="button" onclick="saDelete(${daily.id})">해제</button>` : ''}
    </div>
    <div class="pf-alert-form-row sa-toggle-row">
      <span class="sa-field-label">신규 공시</span>
      <span class="sa-note">증권발행실적보고서 등 발행 공시는 제외</span>
      ${saToggleBtn('disclosure_new', disc)}
    </div>
    <div class="pf-alert-form-row sa-toggle-row">
      <span class="sa-field-label">신규 리포트</span>
      <span class="sa-note">증권사 리포트가 새로 올라오면 알림</span>
      ${saToggleBtn('report_new', rep)}
    </div>`;
}

function saRenderList() {
  const listEl = document.getElementById('stockAlertList');
  if (!listEl) return;
  const prices = saPriceAlerts();
  if (!prices.length) {
    listEl.innerHTML = '<div class="pf-alert-empty">설정된 가격 알림이 없습니다.</div>';
    return;
  }
  listEl.innerHTML = prices.map((rule) => {
    const dir = rule.alert_type.endsWith('above') ? '이상' : '이하';
    const armed = rule.armed
      ? '<span class="pf-alert-state armed" title="조건 충족 시 발송 대기">대기</span>'
      : '<span class="pf-alert-state fired" title="이미 발송됨. 조건이 풀리면 다시 무장">발송됨</span>';
    return `
      <div class="pf-alert-rule ${rule.enabled ? '' : 'disabled'}">
        <div class="pf-alert-rule-main">
          <span class="pf-alert-rule-label">현재가 ${saFmtNum(rule.threshold)} ${dir}</span>
        </div>
        <div class="pf-alert-rule-actions">
          ${rule.enabled ? armed : '<span class="pf-alert-state off">꺼짐</span>'}
          <button class="pf-alert-btn" type="button" onclick="saToggle(${rule.id}, ${rule.enabled ? 'false' : 'true'})">${rule.enabled ? '끄기' : '켜기'}</button>
          <button class="pf-alert-btn danger" type="button" onclick="saDelete(${rule.id})">삭제</button>
        </div>
      </div>`;
  }).join('');
}

// --- Mutations --------------------------------------------------------------

async function saCreate(extra) {
  const payload = { stock_code: StockAlerts.code, source: 'analysis', ...extra };
  try {
    const resp = await saApi('/alerts', { method: 'POST', body: JSON.stringify(payload) });
    const data = await resp.json().catch(() => ({}));
    if (!resp.ok) throw new Error(data.detail || '설정 실패');
    const v = document.getElementById('saPriceVal'); if (v) v.value = '';
    saLoadList();
  } catch (e) {
    alert((e && e.message) || '설정에 실패했습니다.');
  }
}

async function saAddPrice() {
  const dir = (document.getElementById('saPriceDir') || {}).value || 'above';
  const raw = (document.getElementById('saPriceVal') || {}).value;
  const val = Number(raw);
  if (!isFinite(val) || raw === '' || val <= 0) { alert('가격을 입력해주세요.'); return; }
  await saCreate({ alert_type: dir === 'below' ? 'price_below' : 'price_above', threshold: val });
}

async function saSetDaily() {
  const raw = (document.getElementById('saDailyVal') || {}).value;
  const val = Number(raw);
  if (!isFinite(val) || raw === '' || val <= 0) { alert('등락률(%)을 입력해주세요.'); return; }
  await saCreate({ alert_type: 'stock_daily_abs', threshold: val });
}

async function saToggleFeed(type, on) {
  const turnOn = (on === true || on === 'true');
  const existing = saFindAlert(type);
  if (turnOn) {
    if (existing) { await saToggle(existing.id, true); return; }
    await saCreate({ alert_type: type });
  } else if (existing) {
    await saToggle(existing.id, false);
  }
}

async function saToggle(id, enabled) {
  try {
    await saApi(`/alerts/${id}`, { method: 'PUT', body: JSON.stringify({ enabled }) });
  } finally {
    saLoadList();
  }
}

async function saDelete(id) {
  try {
    await saApi(`/alerts/${id}`, { method: 'DELETE' });
  } finally {
    saLoadList();
  }
}

if (typeof window !== 'undefined') {
  Object.assign(window, {
    openStockAlerts, closeStockAlerts, saRender,
    saAddPrice, saSetDaily, saToggleFeed, saToggle, saDelete,
  });
}
