// Low-level sparkline canvas-drawing primitives, split out of
// static/js/portfolio-render.js to keep that file under the maintenance ceiling.
//
// _drawSparkline / _drawSparklinePoints 는 canvas id + 계산된 시리즈만 받는
// 순수 렌더 헬퍼다. 데이터 준비 쪽(_renderSummarySparklines + _spark* 결산축
// 헬퍼)은 PfStore.snapshots.prevDay / .intraday 에 붙어 있어 한동안
// portfolio-render.js 에 남아 있었지만, 그 파일이 유지보수 상한(1,000줄)에
// 닿아 2026-08-14 여기로 함께 옮겼다. 요약 카드 스파크라인 관련 코드는 이제
// 이 파일 하나에 모인다.
//
// _drawSparkline      — evenly-spaced values across fixed slots (right/left aligned).
// _drawSparklinePoints — explicit {x,y} points on a 0..xMax axis.
// Both draw a faint 0% baseline placed naturally at the data range's zero edge.
function _drawSparkline(canvasId, values, color, maxSlots, align) {
  const canvas = document.getElementById(canvasId);
  if (!canvas) return;
  const ctx = canvas.getContext('2d');
  const dpr = window.devicePixelRatio || 1;
  const w = canvas.clientWidth;
  const h = canvas.clientHeight;
  canvas.width = w * dpr;
  canvas.height = h * dpr;
  ctx.scale(dpr, dpr);
  ctx.clearRect(0, 0, w, h);

  const slots = maxSlots || Math.max(values.length, 1);
  const offset = align === 'left' ? 0 : slots - values.length;
  const min = values.length ? Math.min(...values) : 0;
  const max = values.length ? Math.max(...values) : 0;
  const pad = 2;

  // 0% 을 데이터 범위의 경계에 자연스럽게 배치. 전부 양수면 0% 이 맨
  // 아래, 전부 음수면 맨 위.
  const minZ = Math.min(min, 0);
  const maxZ = Math.max(max, 0);
  const rangeZ = maxZ - minZ || 1;

  // v → y 좌표 변환. 기준선·데이터 라인이 동일 수식 사용.
  const yFor = (v) => pad + (1 - (v - minZ) / rangeZ) * (h - pad * 2);
  const zeroY = yFor(0);

  // 0% 기준선 — 연한 점선.
  ctx.save();
  ctx.beginPath();
  ctx.strokeStyle = '#64748b';
  ctx.lineWidth = 1;
  ctx.setLineDash([3, 3]);
  ctx.globalAlpha = 0.5;
  ctx.moveTo(0, zeroY);
  ctx.lineTo(w, zeroY);
  ctx.stroke();
  ctx.restore();

  if (values.length > 1) {
    ctx.beginPath();
    ctx.strokeStyle = color;
    ctx.lineWidth = 1.5;
    ctx.lineJoin = 'round';
    values.forEach((v, i) => {
      const x = ((i + offset) / (slots - 1)) * w;
      const y = yFor(v);
      if (i === 0) ctx.moveTo(x, y);
      else ctx.lineTo(x, y);
    });
    ctx.stroke();
  }
}

function _drawSparklinePoints(canvasId, points, color, xMax) {
  const canvas = document.getElementById(canvasId);
  if (!canvas) return;
  const ctx = canvas.getContext('2d');
  const dpr = window.devicePixelRatio || 1;
  const w = canvas.clientWidth;
  const h = canvas.clientHeight;
  canvas.width = w * dpr;
  canvas.height = h * dpr;
  ctx.scale(dpr, dpr);
  ctx.clearRect(0, 0, w, h);

  const clean = (points || [])
    .map(p => ({ x: Number(p.x), y: Number(p.y) }))
    .filter(p => Number.isFinite(p.x) && Number.isFinite(p.y))
    .sort((a, b) => a.x - b.x);
  const ys = clean.map(p => p.y);
  const min = ys.length ? Math.min(...ys) : 0;
  const max = ys.length ? Math.max(...ys) : 0;
  const pad = 2;
  const minZ = Math.min(min, 0);
  const maxZ = Math.max(max, 0);
  const rangeZ = maxZ - minZ || 1;
  const axisMax = xMax || Math.max(clean[clean.length - 1]?.x || 1, 1);
  const yFor = (v) => pad + (1 - (v - minZ) / rangeZ) * (h - pad * 2);
  const xFor = (x) => (Math.max(0, Math.min(axisMax, x)) / axisMax) * w;
  const zeroY = yFor(0);

  ctx.save();
  ctx.beginPath();
  ctx.strokeStyle = '#64748b';
  ctx.lineWidth = 1;
  ctx.setLineDash([3, 3]);
  ctx.globalAlpha = 0.5;
  ctx.moveTo(0, zeroY);
  ctx.lineTo(w, zeroY);
  ctx.stroke();
  ctx.restore();

  if (clean.length > 1) {
    ctx.beginPath();
    ctx.strokeStyle = color;
    ctx.lineWidth = 1.5;
    ctx.lineJoin = 'round';
    clean.forEach((p, i) => {
      const x = xFor(p.x);
      const y = yFor(p.y);
      if (i === 0) ctx.moveTo(x, y);
      else ctx.lineTo(x, y);
    });
    ctx.stroke();
  }
}

function _sparkLocalMinuteValue(ts) {
  const m = String(ts || '').match(/^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})(?::(\d+(?:\.\d+)?))?/);
  if (!m) return null;
  const seconds = Number(m[6] || 0);
  return Date.UTC(
    Number(m[1]),
    Number(m[2]) - 1,
    Number(m[3]),
    Number(m[4]),
    Number(m[5]),
    Math.floor(seconds),
    Math.round((seconds % 1) * 1000),
  ) / 60000;
}

// TODAY sparkline 은 세션일 08:00~20:00(KST) 고정 축으로 그린다. 결산창(직전 20:00→
// 다음 20:00)의 야간 빈 구간을 잘라 장전·장중·장후 활성 시간대만 보여준다.
const SPARK_DAILY_START_HOUR = 8;
const SPARK_DAILY_END_HOUR = 20;
// 세션일 = intraday 최신 점의 날짜(주말·공휴일에도 장중 점이 몰리지 않음). 없으면 현재 KST.
function _sparkDailyAxis() {
  const ints = Array.isArray(PfStore.snapshots.intraday) ? PfStore.snapshots.intraday : [];
  let maxTs = null;
  for (const d of ints) {
    if (d && d.ts && (maxTs === null || d.ts > maxTs)) maxTs = d.ts;
  }
  const m = maxTs && /^(\d{4})-(\d{2})-(\d{2})T/.exec(maxTs);
  const ymd = m ? `${m[1]}-${m[2]}-${m[3]}` : _sparkNowKstIsoMinute().slice(0, 10);
  const pad = (n) => String(n).padStart(2, '0');
  return { start: `${ymd}T${pad(SPARK_DAILY_START_HOUR)}:00`, end: `${ymd}T${pad(SPARK_DAILY_END_HOUR)}:00` };
}

function _sparkNowKstIsoMinute() {
  const parts = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'Asia/Seoul',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    hourCycle: 'h23',
  }).formatToParts(new Date()).reduce((acc, part) => {
    if (part.type !== 'literal') acc[part.type] = part.value;
    return acc;
  }, {});
  return `${parts.year}-${parts.month}-${parts.day}T${parts.hour}:${parts.minute}`;
}

function _sparkAxisHoursFromTs(ts, axisStartTs, axisEndTs) {
  const start = _sparkLocalMinuteValue(axisStartTs);
  const end = _sparkLocalMinuteValue(axisEndTs);
  const value = _sparkLocalMinuteValue(ts);
  if (start === null || end === null || value === null || end <= start) return null;
  const hours = (value - start) / 60;
  const maxHours = (end - start) / 60;
  return Math.max(0, Math.min(maxHours, hours));
}

function _sparkTodayCashflowThroughTs(ts) {
  const target = _sparkLocalMinuteValue(ts);
  if (target === null) return 0;
  const cashflows = Array.isArray(PfStore.snapshots.prevDay?.today_cashflows)
    ? PfStore.snapshots.prevDay.today_cashflows
    : [];
  let total = 0;
  for (const cf of cashflows) {
    const cfTime = _sparkLocalMinuteValue(cf?.created_at);
    if (cfTime === null || cfTime > target) continue;
    if (cf.signed_amount !== undefined && cf.signed_amount !== null) {
      total += Number(cf.signed_amount || 0);
    } else if (cf.type === 'deposit') {
      total += Number(cf.amount || 0);
    } else if (cf.type === 'withdrawal') {
      total -= Number(cf.amount || 0);
    }
  }
  return total;
}

function _formatLocalYmd(date) {
  return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}-${String(date.getDate()).padStart(2, '0')}`;
}

function _parseLocalYmd(ymd) {
  const m = String(ymd || '').match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!m) return null;
  return new Date(Number(m[1]), Number(m[2]) - 1, Number(m[3]));
}

function _diffLocalDays(start, end) {
  return Math.round((end.getTime() - start.getTime()) / 86400000);
}

// 스파크라인 상승/하락 색 — 하드코딩 대신 CSS 토큰(--up/--down)에서 읽어
// 다크모드에서도 테마 팔레트와 톤이 맞게 한다(토큰이 없으면 종전 색 폴백).
function _sparkTrendColor(isUp) {
  const value = getComputedStyle(document.documentElement)
    .getPropertyValue(isUp ? '--up' : '--down').trim();
  return value || (isUp ? '#dc2626' : '#2563eb');
}

function _renderSummarySparklines(currentTotalValue) {
  // 총 수익률 — 52주 (약 252 거래일) 누적 수익률 추이
  if (PfStore.navHistory.length > 1) {
    const last365 = PfStore.navHistory.slice(-365);
    const returnPcts = last365.map(d => d.total_invested > 0 ? ((d.total_value - d.total_invested) / d.total_invested * 100) : 0);
    const lastReturn = returnPcts[returnPcts.length - 1] || 0;
    _drawSparkline('sparkTotalReturn', returnPcts, _sparkTrendColor(lastReturn >= 0), 252, 'right');
  } else {
    _drawSparkline('sparkTotalReturn', [], _sparkTrendColor(true), 252, 'right');
  }

  // MTD sparkline: fixed previous-month-end -> current-month-end axis.
  // The first point is always previous month-end at 0%, so the shape is
  // stable even before the first daily snapshot of the month exists.
  if (PfStore.snapshots.monthEnd?.total_value && PfStore.snapshots.monthEnd?.total_value > 0) {
    const now = new Date();
    const prevMonthEnd = new Date(now.getFullYear(), now.getMonth(), 0);
    const thisMonthStart = new Date(now.getFullYear(), now.getMonth(), 1);
    const thisMonthEnd = new Date(now.getFullYear(), now.getMonth() + 1, 0);
    const monthStartYmd = _formatLocalYmd(thisMonthStart);
    const monthEndYmd = _formatLocalYmd(thisMonthEnd);
    const axisDays = Math.max(1, _diffLocalDays(prevMonthEnd, thisMonthEnd));
    const monthPoints = [{ x: 0, y: 0 }];
    const monthData = PfStore.navHistory.filter(d => d.date >= monthStartYmd && d.date <= monthEndYmd);
    for (const d of monthData) {
      if (!d || !d.total_value) continue;
      const dt = _parseLocalYmd(d.date);
      if (!dt) continue;
      monthPoints.push({
        x: Math.max(0, Math.min(axisDays, _diffLocalDays(prevMonthEnd, dt))),
        y: ((d.total_value / PfStore.snapshots.monthEnd.total_value) - 1) * 100,
      });
    }
    if (currentTotalValue) {
      monthPoints.push({
        x: Math.max(0, Math.min(axisDays, _diffLocalDays(prevMonthEnd, now))),
        y: ((currentTotalValue / PfStore.snapshots.monthEnd.total_value) - 1) * 100,
      });
    }
    const lastPct = monthPoints.length ? monthPoints[monthPoints.length - 1].y : 0;
    _drawSparklinePoints('sparkMonthly', monthPoints, _sparkTrendColor(lastPct >= 0), axisDays);
  } else {
    _drawSparklinePoints('sparkMonthly', [], _sparkTrendColor(true), 31);
  }

  // TODAY sparkline 은 세션일 08:00~20:00(KST) 고정 축. y 는 직전 20:00 결산(prevClose)
  // 대비 등락%. 축은 _sparkDailyAxis() 가 세션일 기준으로 만든다(now 까지 그려지고
  // 우측 빈 구간은 미래 시간).
  const _prevClose = (PfStore.snapshots.prevDay && PfStore.snapshots.prevDay.total_value > 0)
    ? PfStore.snapshots.prevDay.total_value
    : null;
  const _dailyAxis = _sparkDailyAxis();
  const axisStartTs = _dailyAxis.start;
  const axisEndTs = _dailyAxis.end;
  const _dailyAxisHours = SPARK_DAILY_END_HOUR - SPARK_DAILY_START_HOUR;
  if (!_prevClose) {
    _drawSparklinePoints('sparkDaily', [], _sparkTrendColor(true), _dailyAxisHours);
  } else {
    const raw = [{ x: 0, y: 0 }];
    for (const d of PfStore.snapshots.intraday) {
      if (!d || !d.total_value) continue;
      const x = _sparkAxisHoursFromTs(d.ts, axisStartTs, axisEndTs);
      if (x === null) continue;
      const adjustedTotal = Number(d.total_value) - _sparkTodayCashflowThroughTs(d.ts);
      raw.push({ x, y: (adjustedTotal / _prevClose - 1) * 100 });
    }
    if (currentTotalValue) {
      const x = _sparkAxisHoursFromTs(_sparkNowKstIsoMinute(), axisStartTs, axisEndTs);
      if (x !== null) {
        raw.push({ x, y: (currentTotalValue / _prevClose - 1) * 100 });
      }
    }
    const lastPct = raw.length ? raw[raw.length - 1].y : 0;
    _drawSparklinePoints('sparkDaily', raw, _sparkTrendColor(lastPct >= 0), _dailyAxisHours);
  }
}
