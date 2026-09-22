// 경제 캘린더 (Economic Calendar): 투자정보 뷰의 풀폭 섹션.
//
// /api/market/economic-calendar (zeroin/한경 피드) 를 국가·중요도·기간으로
// 필터해 받아, 날짜별로 그룹핑한 일정 표를 #econCalContent 에 렌더한다.
// 공개(무인증). 필터 바는 한 번만 그리고, 이후 필터 변경 시 본문만 갱신한다.

// 주요국 칩(국내 가치투자자 관점). 전체 국가 코드는 백엔드 COUNTRY_META 참고.
const EC_COUNTRY_CHIPS = [
  { code: 'kr', name: '한국', flag: '🇰🇷' },
  { code: 'us', name: '미국', flag: '🇺🇸' },
  { code: 'cn', name: '중국', flag: '🇨🇳' },
  { code: 'eu', name: '유럽', flag: '🇪🇺' },
  { code: 'jp', name: '일본', flag: '🇯🇵' },
  { code: 'gb', name: '영국', flag: '🇬🇧' },
  { code: 'de', name: '독일', flag: '🇩🇪' },
  { code: 'hk', name: '홍콩', flag: '🇭🇰' },
];
const EC_IMPORTANCE = [
  { level: 'high', label: '상' },
  { level: 'mid', label: '중' },
  { level: 'low', label: '하' },
];
const EC_WEEKDAYS = ['일', '월', '화', '수', '목', '금', '토'];

// 기간(시작/종료일, 기본=이번 주) + 중요도별 국가 선택.
const EC_LS_KEY = 'econCalLevelCountries';
let _ecStart = '';
let _ecEnd = '';
// _ecLevels[level] = 'all'(모든 국가) | Set(국가코드). 빈 Set = 해당 중요도 숨김.
// 기본: 상=모든 국가, 중·하=한국만.
let _ecLevels = { high: 'all', mid: new Set(['kr']), low: new Set(['kr']) };
let _ecInFlight = null;
let _ecShellReady = false;

// 결과 알림 구독 상태(로그인 시). _ecSubs=구독한 event_id 집합,
// _ecEventById=토글 시 메타 조회용, _ecSubsLoaded=세션당 1회만 서버 조회.
let _ecSubs = new Set();
let _ecEventById = {};
let _ecSubsLoaded = false;
let _ecAutoSubs = new Set();
let _ecRules = [];
let _ecRuleDraft = [];
let _ecRuleCountries = [];
let _ecUserKey = '';
let _ecLastData = { events: [] };

function _ecCurrentUserKey() {
  return currentUser ? String(currentUser.google_sub || currentUser.id || currentUser.email || '') : '';
}

function _ecResetUserState() {
  const key = _ecCurrentUserKey();
  if (key === _ecUserKey) return;
  _ecUserKey = key;
  _ecSubsLoaded = false;
  _ecSubs = new Set();
  _ecAutoSubs = new Set();
  _ecRules = [];
  _ecRuleDraft = [];
  const panel = document.getElementById('econCalAlertSettings');
  if (panel) panel.hidden = true;
  document.getElementById('econCalAlertsToggle')?.setAttribute('aria-expanded', 'false');
}

function _ecRuleMatches(ev) {
  const eid = String(ev.index_id || '');
  return _ecRules.some(rule => {
    if (rule.country !== ev.country) return false;
    if (rule.min_importance !== 'all' && ev.importance !== 'high'
        && !(rule.min_importance === 'mid' && ev.importance === 'mid')) return false;
    if (_ecAutoSubs.has(eid)) return true;
    const since = new Date(rule.starts_at);
    const raw = String(ev.datetime || '').replace(' ', 'T');
    const scheduled = raw ? new Date(/(?:Z|[+-]\d\d:\d\d)$/.test(raw) ? raw : raw + '+09:00') : null;
    if (scheduled) return scheduled >= since;
    const day = ev.date || '';
    return day > rule.starts_at.slice(0, 10)
      || (day === rule.starts_at.slice(0, 10) && !String(ev.actual || '').trim());
  });
}

function _ecFmtDate(d) {
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, '0');
  const day = String(d.getDate()).padStart(2, '0');
  return `${y}-${m}-${day}`;
}

// 이번 주(월요일~일요일, 브라우저 로컬=KST 기준).
function _ecThisWeek() {
  const now = new Date();
  const today = new Date(now.getFullYear(), now.getMonth(), now.getDate());
  const dow = (today.getDay() + 6) % 7; // 월=0 ... 일=6
  const monday = new Date(today);
  monday.setDate(today.getDate() - dow);
  const sunday = new Date(monday);
  sunday.setDate(monday.getDate() + 6);
  return { start: _ecFmtDate(monday), end: _ecFmtDate(sunday) };
}

// 중요도별 국가 선택을 localStorage 에 보존(브라우저 기준, 로그인 불필요).
function _ecLoadLevels() {
  try {
    const raw = localStorage.getItem(EC_LS_KEY);
    if (raw) {
      const o = JSON.parse(raw);
      const norm = (v) => (v === 'all' ? 'all' : new Set(Array.isArray(v) ? v : []));
      _ecLevels = { high: norm(o.high), mid: norm(o.mid), low: norm(o.low) };
    }
  } catch (e) { /* 기본값 유지 */ }
}

function _ecSaveLevels() {
  try {
    const o = {};
    for (const lvl of ['high', 'mid', 'low']) {
      o[lvl] = _ecLevels[lvl] === 'all' ? 'all' : [..._ecLevels[lvl]];
    }
    localStorage.setItem(EC_LS_KEY, JSON.stringify(o));
  } catch (e) { /* noop */ }
}

// 한 중요도의 선택을 API 파라미터 문자열로. 'all' | 'kr,us' | '' (비활성).
function _ecLevelParam(lvl) {
  const v = _ecLevels[lvl];
  if (v === 'all') return 'all';
  return v && v.size ? [...v].join(',') : '';
}

// "실제 vs 예상" 방향(같은 단위 비교). 한국 색관례(빨강=높음/상승, 파랑=낮음).
function _ecNum(s) {
  const m = String(s == null ? '' : s).replace(/[^0-9.\-]/g, '');
  if (m === '' || m === '-' || m === '.') return null;
  const n = Number(m);
  return isFinite(n) ? n : null;
}

function _ecActualClass(actual, forecast) {
  const a = _ecNum(actual);
  const f = _ecNum(forecast);
  if (a == null || f == null) return 'ec-flat';
  return a > f ? 'ec-up' : (a < f ? 'ec-down' : 'ec-flat');
}

function _ecValCell(label, value, extraCls) {
  const v = (value == null || String(value).trim() === '') ? '-' : escapeHtml(String(value));
  return `<span class="ec-val ${extraCls || ''}"><span class="ec-val-k">${label}</span>`
    + `<span class="ec-val-v">${v}</span></span>`;
}

// 이벤트 예정 시각(브라우저 로컬=KST). datetime 없으면 날짜의 그날 끝으로 본다
// (날짜만 있는 항목을 당일 동안은 미래로 취급).
function _ecEventDate(ev) {
  const m = String(ev.datetime || '').match(/^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2})/);
  if (m) return new Date(+m[1], +m[2] - 1, +m[3], +m[4], +m[5]);
  const dm = String(ev.date || '').match(/^(\d{4})-(\d{2})-(\d{2})/);
  if (dm) return new Date(+dm[1], +dm[2] - 1, +dm[3], 23, 59);
  return null;
}

function _ecIsPast(ev) {
  const d = _ecEventDate(ev);
  return d ? d.getTime() < Date.now() : false;
}

// 결과 미발표(actual 없음) + 아직 지나지 않은 이벤트에만 🔔 구독 체크박스.
// 발표됐거나 이미 지난 일정(결과가 더 안 나옴)은 빈 칸으로 정렬만 유지.
// event_id(zeroin index_id)가 없으면 추적 불가라 체크박스 생략.
function _ecBellCell(ev) {
  const hasActual = ev.actual && String(ev.actual).trim() !== '';
  const eid = String(ev.index_id || '').trim();
  if (eid && _ecRuleMatches(ev)) {
    return '<span class="ec-bell-cell"><span class="ec-rule-badge" role="img" aria-label="조건 알림 대상" title="국가·중요도 조건 알림 대상입니다. 알림 조건에서 변경하세요."><span>🔔</span><span>조건</span></span></span>';
  }
  // 구독했던 이벤트의 결과가 나오면 🔔 마커를 남겨(행 배경 강조와 함께) 눈에 띄게 한다.
  if (hasActual) {
    if (eid && _ecSubs.has(eid)) {
      // title 은 hover 전용이라 role=img + aria-label 을 병행(터치·스크린리더).
      return '<span class="ec-bell-cell"><span class="ec-bell-done" role="img" title="구독한 일정의 결과가 발표됨" aria-label="구독한 일정의 결과가 발표됨">🔔</span></span>';
    }
    return '<span class="ec-bell-cell"></span>';
  }
  if (!eid || _ecIsPast(ev)) return '<span class="ec-bell-cell"></span>';
  const checked = _ecSubs.has(eid) ? ' checked' : '';
  return `<span class="ec-bell-cell"><label class="ec-bell" title="결과 발표 시 알림 받기">`
    + `<input type="checkbox" class="ec-bell-cb" aria-label="결과 발표 시 알림 받기" data-eid="${escapeHtml(eid)}"${checked}>`
    + `<span class="ec-bell-ico" aria-hidden="true">🔔</span></label></span>`;
}

function _ecRowHtml(ev) {
  const impCls = ev.importance ? `ec-imp-${ev.importance}` : 'ec-imp-low';
  const impLabel = ev.importance_label || '';
  const hasActual = ev.actual && String(ev.actual).trim() !== '';
  const actualCls = hasActual ? _ecActualClass(ev.actual, ev.forecast) : 'ec-flat';
  // 내가 알림 구독한 일정의 결과가 나왔으면 행 배경으로 강조.
  const eid = String(ev.index_id || '').trim();
  const alerted = hasActual && eid && (_ecSubs.has(eid) || _ecRuleMatches(ev));
  return `<div class="ec-row${alerted ? ' ec-row-alerted' : ''}">`
    + `<span class="ec-time">${escapeHtml(String(ev.time || '').trim() || '-')}</span>`
    + `<span class="ec-country" title="${escapeHtml(String(ev.country_name || ''))}">`
    + `<span class="ec-flag">${escapeHtml(String(ev.flag || ''))}</span>`
    + `<span class="ec-cname">${escapeHtml(String(ev.country_name || ev.country || ''))}</span></span>`
    + `<span class="ec-imp ${impCls}" title="중요도 ${escapeHtml(impLabel)}"><i></i>${escapeHtml(impLabel)}</span>`
    + `<span class="ec-event">${escapeHtml(String(ev.event || ''))}</span>`
    + `<span class="ec-vals">`
    + _ecValCell('실제', ev.actual, `ec-actual ${actualCls}`)
    + _ecValCell('예상', ev.forecast, '')
    + _ecValCell('이전', ev.previous, '')
    + `</span>`
    + _ecBellCell(ev)
    + `</div>`;
}

function _ecGroupByDate(events) {
  const groups = new Map();
  for (const ev of events || []) {
    const key = ev.date || (ev.datetime || '').split(' ')[0];
    if (!key) continue;
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(ev);
  }
  return [...groups.entries()].sort((a, b) => (a[0] < b[0] ? -1 : 1));
}

function _ecDateHeading(iso) {
  // "2026-06-06" → "6월 6일 (토)"
  const parts = String(iso).split('-').map(Number);
  if (parts.length !== 3 || parts.some((n) => !isFinite(n))) return escapeHtml(iso);
  const d = new Date(parts[0], parts[1] - 1, parts[2]);
  const wd = EC_WEEKDAYS[d.getDay()] || '';
  const todayIso = _ecFmtDate(new Date());
  const isToday = iso === todayIso;
  return `${parts[1]}월 ${parts[2]}일 <span class="ec-dow">(${wd})</span>`
    + (isToday ? '<span class="ec-today-badge">오늘</span>' : '');
}

// 현재 시각 표시선(파란 라인). 오늘 그룹에서 시간순 정렬된 행 사이,
// 처음으로 현재 시각을 지나지 않은 일정 앞에 끼워 "지금 여기"를 보여준다.
function _ecNowLineHtml() {
  const d = new Date();
  const hm = `${String(d.getHours()).padStart(2, '0')}:${String(d.getMinutes()).padStart(2, '0')}`;
  return `<div class="ec-now-line" role="separator" aria-label="현재 시각 ${hm}"><span>${hm}</span></div>`;
}

function _ecRowsWithNowLine(sortedRows) {
  const now = new Date();
  const nowMin = now.getHours() * 60 + now.getMinutes();
  const toMin = (t) => {
    const m = String(t == null ? '' : t).match(/(\d{1,2}):(\d{2})/);
    return m ? (+m[1]) * 60 + (+m[2]) : null;  // 시각 불명(전일/미정 등)은 위치에서 제외
  };
  let html = '';
  let inserted = false;
  for (const ev of sortedRows) {
    const mins = toMin(ev.time);
    if (!inserted && mins != null && mins >= nowMin) {
      html += _ecNowLineHtml();
      inserted = true;
    }
    html += _ecRowHtml(ev);
  }
  if (!inserted) html += _ecNowLineHtml();  // 남은 일정이 모두 과거면 맨 아래
  return html;
}

function _ecRenderBody(data) {
  const body = document.getElementById('econCalBody');
  if (!body) return;
  _ecLastData = data || { events: [] };
  const events = (data && data.events) || [];
  // 토글 시 구독 메타(날짜·국가·예상치 등) 조회용 인덱스.
  _ecEventById = {};
  for (const ev of events) {
    const eid = String(ev.index_id || '').trim();
    if (eid) _ecEventById[eid] = ev;
  }
  if (!events.length) {
    body.innerHTML = '<div class="md-loading">해당 조건의 일정이 없습니다.</div>';
    return;
  }
  const groups = _ecGroupByDate(events);
  const todayIso = _ecFmtDate(new Date());
  body.innerHTML = groups.map(([iso, rows]) => {
    const sorted = rows.slice().sort((a, b) => (String(a.time) < String(b.time) ? -1 : 1));
    // 오늘 그룹에만 현재 시각 표시선을 끼운다.
    const rowsHtml = iso === todayIso
      ? _ecRowsWithNowLine(sorted)
      : sorted.map(_ecRowHtml).join('');
    return `<div class="ec-daygroup"><div class="ec-dayhead">${_ecDateHeading(iso)}</div>${rowsHtml}</div>`;
  }).join('');
}

// 설정 패널: 중요도(상/중/하)별로 국가를 선택. '전체'는 모든 국가.
function _ecRenderSettings() {
  const el = document.getElementById('econCalSettings');
  if (!el) return;
  el.innerHTML = EC_IMPORTANCE.map((m) => {
    const lvl = m.level;
    const isAll = _ecLevels[lvl] === 'all';
    const set = isAll ? null : _ecLevels[lvl];
    const allChip = `<button class="ec-chip ec-all-chip${isAll ? ' active' : ''}" data-lvl="${lvl}" data-all="1">전체</button>`;
    const chips = EC_COUNTRY_CHIPS.map((c) => {
      const active = !isAll && set.has(c.code);
      return `<button class="ec-chip${active ? ' active' : ''}${isAll ? ' dim' : ''}" data-lvl="${lvl}" data-country="${c.code}">${c.flag} ${escapeHtml(c.name)}</button>`;
    }).join('');
    return `<div class="ec-set-row"><span class="ec-set-label ec-imp-${lvl}"><i></i>${m.label}</span>`
      + `<span class="ec-set-chips">${allChip}${chips}</span></div>`;
  }).join('');

  el.querySelectorAll('.ec-chip[data-all]').forEach((b) => b.addEventListener('click', () => {
    const lvl = b.dataset.lvl;
    _ecLevels[lvl] = _ecLevels[lvl] === 'all' ? new Set() : 'all';
    _ecSaveLevels();
    _ecRenderSettings();
    loadEconomicCalendar();
  }));
  el.querySelectorAll('.ec-chip[data-country]').forEach((b) => b.addEventListener('click', () => {
    const lvl = b.dataset.lvl;
    const code = b.dataset.country;
    if (_ecLevels[lvl] === 'all') _ecLevels[lvl] = new Set();  // '전체' → 특정 국가 모드로 전환
    const set = _ecLevels[lvl];
    if (set.has(code)) set.delete(code); else set.add(code);
    _ecSaveLevels();
    _ecRenderSettings();
    loadEconomicCalendar();
  }));
}

function _ecRenderShell() {
  const root = document.getElementById('econCalContent');
  if (!root) return;
  const wk = _ecThisWeek();
  if (!_ecStart) _ecStart = wk.start;
  if (!_ecEnd) _ecEnd = wk.end;

  root.innerHTML = '<div class="ec-filters">'
    + '<div class="ec-filter-row ec-daterow">'
    + `<input type="date" class="ec-date" id="econCalStart" value="${_ecStart}" aria-label="시작일">`
    + '<span class="ec-date-sep">~</span>'
    + `<input type="date" class="ec-date" id="econCalEnd" value="${_ecEnd}" aria-label="종료일">`
    + '<button class="ec-settings-toggle" id="econCalSettingsToggle" type="button" aria-expanded="false">⚙ 표시 설정</button>'
    + '<button class="ec-settings-toggle" id="econCalAlertsToggle" type="button" aria-expanded="false" aria-controls="econCalAlertSettings">🔔 알림 조건</button>'
    + '</div>'
    + '<div class="ec-settings" id="econCalSettings" hidden></div>'
    + '<div class="ec-alert-settings" id="econCalAlertSettings" hidden></div>'
    + '</div>'
    + '<div class="ec-body" id="econCalBody"><div class="md-loading">경제 일정을 불러오는 중입니다...</div></div>';

  const start = document.getElementById('econCalStart');
  const end = document.getElementById('econCalEnd');
  const onDate = () => {
    if (!start.value || !end.value) return;
    _ecStart = start.value;
    _ecEnd = end.value;
    loadEconomicCalendar();
  };
  if (start) start.addEventListener('change', onDate);
  if (end) end.addEventListener('change', onDate);

  const toggle = document.getElementById('econCalSettingsToggle');
  if (toggle) toggle.addEventListener('click', () => {
    const panel = document.getElementById('econCalSettings');
    if (!panel) return;
    const show = panel.hasAttribute('hidden');
    if (show) panel.removeAttribute('hidden'); else panel.setAttribute('hidden', '');
    toggle.setAttribute('aria-expanded', show ? 'true' : 'false');
    toggle.classList.toggle('active', show);
  });

  document.getElementById('econCalAlertsToggle')?.addEventListener('click', async () => {
    _ecResetUserState();
    const panel = document.getElementById('econCalAlertSettings');
    panel.hidden = !panel.hidden;
    document.getElementById('econCalAlertsToggle').setAttribute('aria-expanded', String(!panel.hidden));
    if (!panel.hidden) await _ecLoadRules();
  });

  // 🔔 체크박스는 본문이 매 렌더마다 다시 그려지므로 위임 리스너로 처리.
  const body = document.getElementById('econCalBody');
  if (body) {
    body.addEventListener('change', (e) => {
      const cb = e.target.closest && e.target.closest('.ec-bell-cb');
      if (cb) _ecToggleSubscription(cb);
    });
  }

  _ecRenderSettings();
  _ecShellReady = true;
}

// 구독 목록을 세션당 1회 로드(로그인 시). 필터 변경 시엔 메모리 _ecSubs를 재사용.
async function _ecLoadSubs() {
  _ecResetUserState();
  if (_ecSubsLoaded || !currentUser) return;
  const userKey = _ecCurrentUserKey();
  try {
    const d = await apiFetchJson('/api/notifications/calendar', { errorMessage: '알림 설정을 불러오지 못했습니다.' });
    if (_ecCurrentUserKey() !== userKey) return;
    _ecSubs = new Set(d.event_ids || []);
    _ecAutoSubs = new Set(d.automatic_event_ids || []);
    _ecRules = d.rules || [];
    _ecSubsLoaded = true;
  } catch (e) {
    console.warn('calendar subscriptions load failed', e);
  }
}

async function _ecLoadRules() {
  const panel = document.getElementById('econCalAlertSettings');
  if (!currentUser) {
    panel.innerHTML = '<p>조건 알림은 로그인 후 이용할 수 있습니다.</p><button type="button" class="ec-settings-toggle" onclick="_ecPromptLogin()">로그인</button>';
    return;
  }
  const userKey = _ecCurrentUserKey();
  panel.textContent = '알림 조건을 불러오는 중…';
  try {
    const data = await apiFetchJson('/api/notifications/calendar/rules', { errorMessage: '알림 조건을 불러오지 못했습니다.' });
    if (_ecCurrentUserKey() !== userKey) return;
    _ecRules = data.rules || [];
    _ecRuleDraft = _ecRules.map(rule => ({ country: rule.country, min_importance: rule.min_importance }));
    _ecRuleCountries = data.countries || [];
    _ecRenderRulePanel();
  } catch (e) {
    panel.innerHTML = '<p role="alert">알림 조건을 불러오지 못했습니다.</p><button type="button" class="ec-settings-toggle" onclick="_ecLoadRules()">다시 시도</button>';
  }
}

function _ecRenderRulePanel() {
  const panel = document.getElementById('econCalAlertSettings');
  const rows = _ecRuleDraft.map((rule, i) => {
    const countries = _ecRuleCountries.map(c => `<option value="${escapeHtml(c.code)}"${c.code === rule.country ? ' selected' : ''}${_ecRuleDraft.some((r, j) => j !== i && r.country === c.code) ? ' disabled' : ''}>${escapeHtml(c.flag)} ${escapeHtml(c.name)}</option>`).join('');
    return `<div class="ec-rule-row" data-rule-index="${i}">
      <label>국가 <select data-field="country" aria-label="${i + 1}번째 알림 국가">${countries}</select></label>
      <label>중요도 <select data-field="min_importance" aria-label="${i + 1}번째 알림 중요도">
        <option value="all"${rule.min_importance === 'all' ? ' selected' : ''}>전체</option>
        <option value="mid"${rule.min_importance === 'mid' ? ' selected' : ''}>중 이상</option>
        <option value="high"${rule.min_importance === 'high' ? ' selected' : ''}>상</option>
      </select></label>
      <button type="button" class="ec-settings-toggle" data-remove="${i}" aria-label="${escapeHtml(_ecRuleCountries.find(c => c.code === rule.country)?.name || rule.country)} 알림 조건 삭제">삭제</button>
    </div>`;
  }).join('');
  panel.innerHTML = `<form id="econCalRuleForm">
    <h3>국가·중요도별 결과 알림</h3>
    <p>설정 이후의 일정에 자동 적용됩니다. 화면을 닫아도 결과 발표 후 텔레그램·카카오톡으로 알려드립니다.</p>
    <p class="ec-rule-hint">예: 한국 — 전체, 미국 — 중 이상, 일본 — 상. 표시 설정과 별도로 저장되며, 개별 알림과 겹쳐도 한 번만 보냅니다.</p>
    <fieldset id="econCalRuleFields"><legend class="sr-only">알림 조건</legend>
      <div id="econCalRuleRows">${rows || '<p class="ec-rule-empty">설정한 조건이 없습니다. 알림을 받을 국가를 추가하세요.</p>'}</div>
      <div class="ec-rule-actions">
        <button type="button" id="econCalRuleAdd" class="ec-settings-toggle"${_ecRuleDraft.length >= _ecRuleCountries.length ? ' disabled' : ''}>+ 국가 추가</button>
        <button type="submit" class="ec-settings-toggle" id="econCalRuleSave">알림 조건 저장</button>
      </div>
    </fieldset>
    <p id="econCalRuleStatus" role="status" aria-live="polite"></p>
    <p class="ec-rule-hint">조건을 삭제하고 저장하면 해당 조건의 대기 알림도 취소됩니다. 개별로 신청한 알림은 유지됩니다.</p>
  </form>`;
  panel.querySelector('form').addEventListener('submit', e => { e.preventDefault(); _ecSaveRules(); });
  panel.querySelector('#econCalRuleAdd').addEventListener('click', () => {
    const next = _ecRuleCountries.find(c => !_ecRuleDraft.some(r => r.country === c.code));
    if (next) _ecRuleDraft.push({ country: next.code, min_importance: 'all' });
    _ecRenderRulePanel();
  });
  panel.querySelectorAll('[data-remove]').forEach(button => button.addEventListener('click', () => {
    _ecRuleDraft.splice(Number(button.dataset.remove), 1);
    _ecRenderRulePanel();
  }));
  panel.querySelectorAll('[data-field]').forEach(select => select.addEventListener('change', () => {
    const index = Number(select.closest('[data-rule-index]').dataset.ruleIndex);
    _ecRuleDraft[index][select.dataset.field] = select.value;
    // Disable already chosen countries without replacing the focused select.
    panel.querySelectorAll('select[data-field="country"]').forEach((countrySelect, i) => {
      [...countrySelect.options].forEach(option => {
        option.disabled = _ecRuleDraft.some((r, j) => i !== j && r.country === option.value);
      });
    });
    document.getElementById('econCalRuleStatus').textContent = '';
  }));
}

async function _ecSaveRules() {
  const fields = document.getElementById('econCalRuleFields');
  const status = document.getElementById('econCalRuleStatus');
  if (!fields || fields.disabled) return;
  const userKey = _ecCurrentUserKey();
  fields.disabled = true;
  status.textContent = '저장 중…';
  try {
    const data = await apiFetchJson('/api/notifications/calendar/rules', {
      method: 'PUT', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ rules: _ecRuleDraft }), errorMessage: '알림 조건을 저장하지 못했습니다.',
    });
    if (_ecCurrentUserKey() !== userKey) return;
    _ecRules = data.rules || [];
    status.textContent = '저장했습니다.';
    _ecRenderBody(_ecLastData);
  } catch (e) {
    status.textContent = e.message || '저장하지 못했습니다. 다시 시도하세요.';
    if (e?.status === 409) _ecPromptChannel();
  } finally {
    fields.disabled = false;
  }
}

async function _ecPromptLogin() {
  if (await confirmModal('경제지표 결과 알림은 로그인 후 이용할 수 있습니다. 로그인 페이지로 이동할까요?')) {
    if (typeof buildLoginPageUrl === 'function') window.location.href = buildLoginPageUrl();
  }
}

async function _ecPromptChannel() {
  if (await confirmModal('알림을 받으려면 텔레그램 또는 카카오톡 연결이 필요합니다. 포트폴리오 > 알림 설정으로 이동할까요?')) {
    if (typeof switchView === 'function') switchView('portfolio');
    if (typeof pfOpenAlerts === 'function') setTimeout(pfOpenAlerts, 80);
  }
}

async function _ecToggleSubscription(cb) {
  const eid = cb.dataset.eid;
  const wantOn = cb.checked;
  if (!currentUser) { cb.checked = false; _ecPromptLogin(); return; }

  if (wantOn) {
    // 채널 보유 여부는 서버가 단일 진실원: 구독을 시도하고 409(채널 없음)면 안내.
    // 별도 클라이언트 사전체크는 오판(이미 연결됐는데 연결하라는 팝업) 위험이 있어 제거.
    const ev = _ecEventById[eid] || {};
    try {
      await apiFetchJson('/api/notifications/calendar', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          event_id: eid,
          event_date: ev.date || '',
          event_datetime: ev.datetime || '',
          country: ev.country || '',
          country_name: ev.country_name || '',
          event: ev.event || '',
          importance: ev.importance || '',
          forecast: ev.forecast || '',
          previous: ev.previous || '',
        }),
        errorMessage: 'subscribe failed',
      });
      _ecSubs.add(eid);
    } catch (e) {
      if (e?.status === 409) { cb.checked = false; _ecPromptChannel(); return; }
      cb.checked = false;
      console.warn('calendar subscribe failed', e);
    }
  } else {
    try {
      await apiFetch('/api/notifications/calendar/' + encodeURIComponent(eid), { method: 'DELETE' });
    } catch (e) {
      console.warn('calendar unsubscribe failed', e);
    }
    _ecSubs.delete(eid);
  }
}

async function loadEconomicCalendar() {
  const root = document.getElementById('econCalContent');
  if (!root) return;
  _ecResetUserState();
  if (!_ecShellReady) {
    _ecLoadLevels();   // 저장된 중요도별 국가 선택 복원(셸 렌더 전에)
    _ecRenderShell();
  }
  if (currentUser && !_ecSubsLoaded) await _ecLoadSubs();
  const params = new URLSearchParams({ start: _ecStart, end: _ecEnd });
  // 중요도별 국가 선택은 항상 명시적으로 전달('' = 그 중요도 숨김).
  params.set('high', _ecLevelParam('high'));
  params.set('mid', _ecLevelParam('mid'));
  params.set('low', _ecLevelParam('low'));
  const reqKey = params.toString();
  _ecInFlight = reqKey;
  try {
    const data = await apiFetchJson('/api/market/economic-calendar?' + reqKey, { fallback: { events: [] } });
    if (_ecInFlight !== reqKey) return; // 더 최신 요청이 진행 중이면 폐기
    _ecRenderBody(data);
  } catch (e) {
    console.warn('economic calendar load failed', e);
    const body = document.getElementById('econCalBody');
    if (body) body.innerHTML = '<div class="md-loading">경제 일정을 불러오지 못했습니다.</div>';
  }
}
