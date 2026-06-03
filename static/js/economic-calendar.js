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

// 필터 상태(기본: 주요국 / 상·중 / 이번 주). 하(low) 는 잡음이 많아 기본 제외.
let _ecCountries = new Set(['kr', 'us', 'cn', 'eu', 'jp']);
let _ecImportance = new Set(['high', 'mid']);
let _ecRange = 'week'; // 'today' | 'week' | 'next' | 'custom'
let _ecCustom = { start: '', end: '' };
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

// 선택된 기간 → {start, end} ISO 문자열(브라우저 로컬=KST 기준).
function _ecRangeDates() {
  const now = new Date();
  const today = new Date(now.getFullYear(), now.getMonth(), now.getDate());
  if (_ecRange === 'today') {
    const s = _ecFmtDate(today);
    return { start: s, end: s };
  }
  if (_ecRange === 'custom') {
    const s = _ecCustom.start || _ecFmtDate(today);
    const e = _ecCustom.end || s;
    return { start: s, end: e };
  }
  // week / next: 월요일 시작 ~ 일요일 끝
  const dow = (today.getDay() + 6) % 7; // 월=0 ... 일=6
  const monday = new Date(today);
  monday.setDate(today.getDate() - dow + (_ecRange === 'next' ? 7 : 0));
  const sunday = new Date(monday);
  sunday.setDate(monday.getDate() + 6);
  return { start: _ecFmtDate(monday), end: _ecFmtDate(sunday) };
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

// 결과 미발표(actual 없음) 이벤트에만 🔔 구독 체크박스. 발표된 행은 빈 칸으로
// 정렬만 유지. event_id(zeroin index_id)가 없으면 추적 불가라 체크박스 생략.
function _ecBellCell(ev) {
  const hasActual = ev.actual && String(ev.actual).trim() !== '';
  const eid = String(ev.index_id || '').trim();
  if (hasActual || !eid) return '<span class="ec-bell-cell"></span>';
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

function _ecSyncFilterActive() {
  document.querySelectorAll('#econCalContent .ec-chip[data-country]').forEach((b) => {
    b.classList.toggle('active', _ecCountries.has(b.dataset.country));
  });
  document.querySelectorAll('#econCalContent .ec-chip[data-imp]').forEach((b) => {
    b.classList.toggle('active', _ecImportance.has(b.dataset.imp));
  });
  document.querySelectorAll('#econCalContent .ec-range-btn[data-range]').forEach((b) => {
    b.classList.toggle('active', b.dataset.range === _ecRange);
  });
  const custom = document.getElementById('econCalCustom');
  if (custom) custom.style.display = _ecRange === 'custom' ? 'flex' : 'none';
}

function _ecRenderShell() {
  const root = document.getElementById('econCalContent');
  if (!root) return;
  const ranges = [['today', '오늘'], ['week', '이번 주'], ['next', '다음 주'], ['custom', '직접선택']]
    .map(([r, label]) => `<button class="ec-range-btn" data-range="${r}">${label}</button>`).join('');
  const chips = EC_COUNTRY_CHIPS
    .map((c) => `<button class="ec-chip" data-country="${c.code}">${c.flag} ${escapeHtml(c.name)}</button>`).join('');
  const imps = EC_IMPORTANCE
    .map((m) => `<button class="ec-chip ec-imp-chip ec-imp-${m.level}" data-imp="${m.level}"><i></i>${m.label}</button>`).join('');
  root.innerHTML = '<div class="ec-filters">'
    + `<div class="ec-filter-row ec-ranges">${ranges}`
    + '<span class="ec-custom" id="econCalCustom">'
    + '<input type="date" class="ec-date" id="econCalStart"> ~ '
    + '<input type="date" class="ec-date" id="econCalEnd"></span></div>'
    + `<div class="ec-filter-row ec-countries">${chips}</div>`
    + `<div class="ec-filter-row ec-imps"><span class="ec-flabel">중요도</span>${imps}</div>`
    + '</div>'
    + '<div class="ec-body" id="econCalBody"><div class="md-loading">경제 일정을 불러오는 중입니다...</div></div>';

  root.querySelectorAll('.ec-range-btn[data-range]').forEach((b) => b.addEventListener('click', () => {
    _ecRange = b.dataset.range;
    _ecSyncFilterActive();
    if (_ecRange !== 'custom') loadEconomicCalendar();
  }));
  root.querySelectorAll('.ec-chip[data-country]').forEach((b) => b.addEventListener('click', () => {
    const c = b.dataset.country;
    if (_ecCountries.has(c)) _ecCountries.delete(c); else _ecCountries.add(c);
    _ecSyncFilterActive();
    loadEconomicCalendar();
  }));
  root.querySelectorAll('.ec-chip[data-imp]').forEach((b) => b.addEventListener('click', () => {
    const l = b.dataset.imp;
    if (_ecImportance.has(l)) _ecImportance.delete(l); else _ecImportance.add(l);
    _ecSyncFilterActive();
    loadEconomicCalendar();
  }));
  const start = document.getElementById('econCalStart');
  const end = document.getElementById('econCalEnd');
  const onCustom = () => {
    _ecCustom = { start: start.value, end: end.value };
    if (start.value && end.value) loadEconomicCalendar();
  };
  if (start) start.addEventListener('change', onCustom);
  if (end) end.addEventListener('change', onCustom);

  // 🔔 체크박스는 본문이 매 렌더마다 다시 그려지므로 위임 리스너로 처리.
  const body = document.getElementById('econCalBody');
  if (body) {
    body.addEventListener('change', (e) => {
      const cb = e.target.closest && e.target.closest('.ec-bell-cb');
      if (cb) _ecToggleSubscription(cb);
    });
  }

  _ecShellReady = true;
  _ecSyncFilterActive();
}

// 구독 목록을 세션당 1회 로드(로그인 시). 필터 변경 시엔 메모리 _ecSubs를 재사용.
async function _ecLoadSubs() {
  if (_ecSubsLoaded || !window.currentUser) return;
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

// 활성 알림 채널(텔레그램/카카오) 보유 여부. 켤 때만 조회하므로 매번 신선하게.
async function _ecHasActiveChannel() {
  try {
    const r = await apiFetch('/api/notifications/channels');
    if (!r.ok) return false;
    const d = await r.json();
    const tg = d.telegram || {};
    const kk = d.kakao || {};
    return Boolean((tg.connected && tg.enabled) || (kk.connected && kk.enabled));
  } catch (e) {
    return false;
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
  if (!window.currentUser) { cb.checked = false; _ecPromptLogin(); return; }

  if (wantOn) {
    if (!(await _ecHasActiveChannel())) { cb.checked = false; _ecPromptChannel(); return; }
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
  if (!_ecShellReady) _ecRenderShell();
  if (window.currentUser && !_ecSubsLoaded) await _ecLoadSubs();
  const { start, end } = _ecRangeDates();
  const params = new URLSearchParams({ start, end });
  if (_ecCountries.size) params.set('countries', [..._ecCountries].join(','));
  if (_ecImportance.size) params.set('importance', [..._ecImportance].join(','));
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
