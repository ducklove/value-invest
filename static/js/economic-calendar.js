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
    + `</span></div>`;
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

  _ecShellReady = true;
  _ecSyncFilterActive();
}

async function loadEconomicCalendar() {
  const root = document.getElementById('econCalContent');
  if (!root) return;
  if (!_ecShellReady) _ecRenderShell();
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
