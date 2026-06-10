// Low-level sparkline canvas-drawing primitives, split out of
// static/js/portfolio-render.js to keep that file under the maintenance ceiling.
//
// These are pure rendering helpers — they take a canvas id + pre-computed
// series and draw, touching no portfolio state. The data-preparation side
// (_renderSummarySparklines + the _spark* settlement-axis helpers) stays in
// portfolio-render.js because it is tightly coupled to PfStore.snapshots.prevDay /
// PfStore.snapshots.intraday and is asserted there by tests/test_frontend_structure.py.
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
