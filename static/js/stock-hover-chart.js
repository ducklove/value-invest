// 종목 hover 당일 일중(1일) 그래프 툴팁 — 데스크톱 전용 기본 동작.
//
// document 레벨 이벤트 위임 하나로 종목이 나열되는 표면 전체를 커버한다:
//   - 포트폴리오 보유종목 테이블 (#pfBody tr[data-code] 이름 셀)
//   - 종목분석 최근/관심 목록 (#recentList .sidebar-item[data-code])
//   - 투자정보 시장 랭킹 (.mv-row[data-code])
//   - 신규 표면은 아무 요소에나 data-candle-code="005930" 만 붙이면 된다.
//
// 터치 기기는 hover 가 없고 탭은 이미 다른 행동(분석 이동/인사이트)에 쓰여
// (hover: hover) and (pointer: fine) 환경에서만 동작한다.
// 데이터는 GET /api/stocks/{code}/intraday — 당일 분 단위 가격 + 전일종가.
// 특수자산(현금/금/암호화폐)은 supported=false 로 내려오며 툴팁을 띄우지
// 않는다(gold_gap 일중 API 연결 전까지).

const SCHC_SHOW_DELAY_MS = (typeof window !== 'undefined' && window.STOCK_CANDLE_TIP_DELAY_MS) ?? 300;
const SCHC_CACHE_TTL_MS = 60 * 1000; // 일중 데이터라 짧게 캐시
const SCHC_FAIL_TTL_MS = 60 * 1000;

const SCHC_HOVER_SELECTOR = [
  '[data-candle-code]',
  '#pfBody tr[data-code] .pf-stock-cell',
  '#recentList .sidebar-item[data-code]',
  '.mv-row[data-code]',
].join(', ');

// 특수자산은 서버도 supported=false 를 주지만, 요청 자체를 아끼려 프론트에서도 거른다.
const SCHC_SKIP_CODE_RE = /^(CASH_|KRX_GOLD$|CRYPTO_)/;

const _schcCache = new Map();     // code -> { ts, ttl, data|null(실패) }
const _schcInflight = new Map();  // code -> Promise
let _schcHoverEl = null;
let _schcShowTimer = null;
let _schcSeq = 0;

function _schcHoverCapable() {
  try {
    return typeof window.matchMedia === 'function'
      && window.matchMedia('(hover: hover) and (pointer: fine)').matches;
  } catch (_) {
    return false;
  }
}

function _schcTargetInfo(el) {
  const explicit = el.closest('[data-candle-code]');
  if (explicit) {
    return { code: explicit.dataset.candleCode || '', name: explicit.dataset.candleName || '' };
  }
  const host = el.closest('[data-code]');
  if (!host) return null;
  const nameEl = host.querySelector('.pf-stock-link strong, .name, .mv-name');
  return { code: host.dataset.code || '', name: nameEl ? nameEl.textContent.trim() : '' };
}

async function _schcData(code) {
  const cached = _schcCache.get(code);
  if (cached && Date.now() - cached.ts < cached.ttl) return cached.data;
  if (_schcInflight.has(code)) return _schcInflight.get(code);
  const p = apiFetchJson(
    `/api/stocks/${encodeURIComponent(code)}/intraday`,
    { fallback: null },
  ).catch(() => null).then(data => {
    const usable = data && data.supported && Array.isArray(data.points) && data.points.length >= 2 ? data : null;
    _schcCache.set(code, { ts: Date.now(), ttl: usable ? SCHC_CACHE_TTL_MS : SCHC_FAIL_TTL_MS, data: usable });
    _schcInflight.delete(code);
    return usable;
  });
  _schcInflight.set(code, p);
  return p;
}

function _schcEnsureTip() {
  let tip = document.getElementById('stockHoverTip');
  if (!tip) {
    tip = document.createElement('div');
    tip.id = 'stockHoverTip';
    tip.className = 'stock-hover-tip';
    tip.setAttribute('aria-hidden', 'true');
    document.body.appendChild(tip);
  }
  return tip;
}

function _schcFmtPrice(v, currency) {
  if (v === null || v === undefined || !Number.isFinite(Number(v))) return '-';
  const n = Number(v);
  if (currency === 'KRW') return Math.round(n).toLocaleString();
  return n.toLocaleString(undefined, { maximumFractionDigits: 2 });
}

function _schcCssColor(name, fallback) {
  try {
    const v = getComputedStyle(document.documentElement).getPropertyValue(name).trim();
    return v || fallback;
  } catch (_) {
    return fallback;
  }
}

// 전일종가 파싱 — null/undefined 는 null 유지 (Number(null)===0 함정 방지).
function _schcPrevClose(data) {
  if (data.prevClose === null || data.prevClose === undefined) return null;
  const n = Number(data.prevClose);
  return Number.isFinite(n) && n > 0 ? n : null;
}

// "HH:MM" → 분. 형식이 아니면 null.
function _schcMinutes(t) {
  const m = /^(\d{1,2}):(\d{2})$/.exec(t || '');
  if (!m) return null;
  return parseInt(m[1], 10) * 60 + parseInt(m[2], 10);
}

function _schcDrawChart(canvas, data) {
  let ctx = null;
  try {
    ctx = typeof canvas.getContext === 'function' ? canvas.getContext('2d') : null;
  } catch (_) {
    return; // canvas 미지원 환경(jsdom 등) — 텍스트 정보만 노출.
  }
  if (!ctx) return;
  const dpr = window.devicePixelRatio || 1;
  const w = canvas.clientWidth || 264;
  const h = canvas.clientHeight || 120;
  canvas.width = w * dpr;
  canvas.height = h * dpr;
  ctx.scale(dpr, dpr);
  ctx.clearRect(0, 0, w, h);

  const points = data.points;
  const prevClose = _schcPrevClose(data);
  const pad = 3;
  let min = Infinity;
  let max = -Infinity;
  points.forEach(p => {
    if (p.p < min) min = p.p;
    if (p.p > max) max = p.p;
  });
  // 전일종가 기준선이 항상 보이도록 스케일에 포함.
  if (prevClose !== null) {
    if (prevClose < min) min = prevClose;
    if (prevClose > max) max = prevClose;
  }
  if (!(max > min)) max = min + 1;
  const yFor = v => pad + (1 - (v - min) / (max - min)) * (h - pad * 2);

  // x 축은 정규장 시간에 비례 — 장중엔 그래프가 진행률만큼만 차오른다.
  const sessStart = _schcMinutes(data.session && data.session.start);
  const sessEnd = _schcMinutes(data.session && data.session.end);
  const useSession = sessStart !== null && sessEnd !== null && sessEnd > sessStart;
  const xFor = (p, i) => {
    if (useSession) {
      const t = _schcMinutes(p.t);
      if (t !== null) {
        return Math.max(0, Math.min(1, (t - sessStart) / (sessEnd - sessStart))) * w;
      }
    }
    return points.length > 1 ? (i / (points.length - 1)) * w : w / 2;
  };

  if (prevClose !== null) {
    const y = yFor(prevClose);
    ctx.save();
    ctx.beginPath();
    ctx.strokeStyle = _schcCssColor('--text-secondary', '#666');
    ctx.globalAlpha = 0.5;
    ctx.lineWidth = 1;
    ctx.setLineDash([3, 3]);
    ctx.moveTo(0, y);
    ctx.lineTo(w, y);
    ctx.stroke();
    ctx.restore();
  }

  const last = points[points.length - 1].p;
  const lineColor = prevClose === null
    ? _schcCssColor('--chart-line', '#2563eb')
    : last >= prevClose ? _schcCssColor('--up', '#b91c1c') : _schcCssColor('--down', '#1d4ed8');
  ctx.beginPath();
  ctx.strokeStyle = lineColor;
  ctx.lineWidth = 1.5;
  ctx.lineJoin = 'round';
  points.forEach((p, i) => {
    const x = xFor(p, i);
    const y = yFor(p.p);
    if (i === 0) ctx.moveTo(x, y);
    else ctx.lineTo(x, y);
  });
  ctx.stroke();
}

function _schcRender(tip, info, data) {
  const points = data.points;
  const last = points[points.length - 1];
  const prevClose = _schcPrevClose(data);
  const chgPct = prevClose ? (last.p / prevClose - 1) * 100 : null;
  const chgCls = chgPct === null || chgPct === 0 ? '' : chgPct > 0 ? 'sct-up' : 'sct-down';
  const chgText = chgPct === null ? '' : `${chgPct > 0 ? '+' : ''}${chgPct.toFixed(2)}%`;
  const footParts = [`${data.date || ''} 일중`, `${last.t} 기준`];
  if (prevClose !== null) footParts.push(`전일종가 ${_schcFmtPrice(prevClose, data.currency)}`);

  tip.innerHTML = `
    <div class="sct-head">
      <span class="sct-name">${escapeHtml(info.name || info.code)}</span>
      <span class="sct-code">${escapeHtml(info.code)}</span>
    </div>
    <div class="sct-price">${escapeHtml(_schcFmtPrice(last.p, data.currency))}${data.currency && data.currency !== 'KRW' ? ` <span class="sct-code">${escapeHtml(data.currency)}</span>` : ''}
      <span class="sct-chg ${chgCls}">${escapeHtml(chgText)}</span>
    </div>
    <canvas class="sct-canvas" width="264" height="120"></canvas>
    <div class="sct-foot">${escapeHtml(footParts.join(' · '))}</div>`;
  _schcDrawChart(tip.querySelector('canvas'), data);
}

function _schcPosition(tip, anchorRect) {
  const margin = 10;
  const vw = window.innerWidth;
  const vh = window.innerHeight;
  const tw = tip.offsetWidth || 280;
  const th = tip.offsetHeight || 190;
  let left = anchorRect.left + 16;
  let top = anchorRect.bottom + 8;
  if (left + tw + margin > vw) left = Math.max(margin, vw - tw - margin);
  if (top + th + margin > vh) top = anchorRect.top - th - 8;
  if (top < margin) top = margin;
  tip.style.left = `${left}px`;
  tip.style.top = `${top}px`;
}

function _schcHide() {
  _schcHoverEl = null;
  _schcSeq++;
  if (_schcShowTimer) {
    clearTimeout(_schcShowTimer);
    _schcShowTimer = null;
  }
  const tip = document.getElementById('stockHoverTip');
  if (tip) tip.classList.remove('visible');
}

async function _schcShow(target, info, seq) {
  const data = await _schcData(info.code);
  if (seq !== _schcSeq || _schcHoverEl !== target) return;
  if (!data) return;
  const tip = _schcEnsureTip();
  _schcRender(tip, info, data);
  tip.classList.add('visible');
  _schcPosition(tip, target.getBoundingClientRect());
}

function _schcOnMouseOver(e) {
  if (!_schcHoverCapable()) return;
  const target = e.target instanceof Element ? e.target.closest(SCHC_HOVER_SELECTOR) : null;
  if (!target) return;
  if (target === _schcHoverEl) return;
  const info = _schcTargetInfo(e.target);
  if (!info || !info.code || SCHC_SKIP_CODE_RE.test(info.code)) return;
  _schcHide();
  _schcHoverEl = target;
  const seq = _schcSeq;
  _schcData(info.code); // 지연과 병행해 미리 받아둔다.
  _schcShowTimer = setTimeout(() => {
    _schcShowTimer = null;
    _schcShow(target, info, seq);
  }, SCHC_SHOW_DELAY_MS);
}

function _schcOnMouseOut(e) {
  if (!_schcHoverEl) return;
  const to = e.relatedTarget;
  if (to instanceof Element && _schcHoverEl.contains(to)) return;
  _schcHide();
}

document.addEventListener('mouseover', _schcOnMouseOver);
document.addEventListener('mouseout', _schcOnMouseOut);
// 스크롤·클릭 시 앵커가 이동/재렌더될 수 있어 즉시 닫는다.
document.addEventListener('scroll', () => { if (_schcHoverEl) _schcHide(); }, true);
document.addEventListener('click', () => { if (_schcHoverEl) _schcHide(); }, true);
