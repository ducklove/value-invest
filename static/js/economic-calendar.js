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
  if (hasActual || !eid || _ecIsPast(ev)) return '<span class="ec-bell-cell"></span>';
  const checked = _ecSubs.has(eid) ? ' checked' : '';
  return `<span class="ec-bell-cell"><label class="ec-bell" title="결과 발표 시 알림 받기">`
    + `<input type="checkbox" class="ec-bell-cb" data-eid="${escapeHtml(eid)}"${checked}>`
    + `<span class="ec-bell-ico" aria-hidden="true">🔔</span></label></span>`;
}

function _ecRowHtml(ev) {
  const impCls = ev.importance ? `ec-imp-${ev.importance}` : 'ec-imp-low';
  const impLabel = ev.importance_label || '';
  const hasActual = ev.actual && String(ev.actual).trim() !== '';
  const actualCls = hasActual ? _ecActualClass(ev.actual, ev.forecast) : 'ec-flat';
  return `<div class="ec-row">`
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

function _ecRenderBody(data) {
  const body = document.getElementById('econCalBody');
  if (!body) return;
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
  body.innerHTML = groups.map(([iso, rows]) => {
    const rowsHtml = rows
      .slice()
      .sort((a, b) => (String(a.time) < String(b.time) ? -1 : 1))
      .map(_ecRowHtml)
      .join('');
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
    + '<button class="ec-settings-toggle" id="econCalSettingsToggle" type="button" aria-expanded="false">⚙ 설정</button>'
    + '</div>'
    + '<div class="ec-settings" id="econCalSettings" hidden></div>'
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
  if (_ecSubsLoaded || !currentUser) return;
  try {
    const r = await apiFetch('/api/notifications/calendar');
    if (r.ok) {
      const d = await r.json();
      _ecSubs = new Set(d.event_ids || []);
    }
  } catch (e) {
    console.warn('calendar subscriptions load failed', e);
  } finally {
    _ecSubsLoaded = true;
  }
}

function _ecPromptLogin() {
  if (confirm('경제지표 결과 알림은 로그인 후 이용할 수 있습니다. 로그인 페이지로 이동할까요?')) {
    if (typeof buildLoginPageUrl === 'function') window.location.href = buildLoginPageUrl();
  }
}

function _ecPromptChannel() {
  if (confirm('알림을 받으려면 텔레그램 또는 카카오톡 연결이 필요합니다. 포트폴리오 > 알림 설정으로 이동할까요?')) {
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
      const r = await apiFetch('/api/notifications/calendar', {
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
      });
      if (!r.ok) {
        if (r.status === 409) { cb.checked = false; _ecPromptChannel(); return; }
        throw new Error('subscribe failed');
      }
      _ecSubs.add(eid);
    } catch (e) {
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
    const r = await apiFetch('/api/market/economic-calendar?' + reqKey);
    if (_ecInFlight !== reqKey) return; // 더 최신 요청이 진행 중이면 폐기
    const data = r.ok ? await r.json() : { events: [] };
    _ecRenderBody(data);
  } catch (e) {
    console.warn('economic calendar load failed', e);
    const body = document.getElementById('econCalBody');
    if (body) body.innerHTML = '<div class="md-loading">경제 일정을 불러오지 못했습니다.</div>';
  }
}
