// Admin time-series charts: live server samples and SVG renderers for
// server health, event volume, and HTTP observer tails.

let _adminServerSeries = [];
const _ADMIN_SERVER_SERIES_LIMIT = 120;

function _serverCpuPct(s) {
  if (!s) return 0;
  if (typeof s.cpu_pct === 'number') return Math.max(0, Math.min(100, Math.round(s.cpu_pct)));
  const loadParts = (s.load_avg || '').split(' ');
  const load1m = parseFloat(loadParts[0]) || 0;
  return Math.max(0, Math.min(100, Math.round(load1m / 4 * 100)));
}

function _serverMemoryPct(s) {
  const memTotal = s?.memory?.MemTotal || 0;
  const memAvail = s?.memory?.MemAvailable || 0;
  return memTotal ? Math.round((memTotal - memAvail) / memTotal * 100) : 0;
}

function _serverDiskPct(s) {
  const diskTotal = s?.disk?.total || 0;
  const diskUsed = s?.disk?.used || 0;
  return diskTotal ? Math.round(diskUsed / diskTotal * 100) : 0;
}

function _serverSample(s) {
  return {
    ts: Date.now(),
    cpu: _serverCpuPct(s),
    memory: _serverMemoryPct(s),
    disk: _serverDiskPct(s),
    temp: s?.cpu_temp == null ? null : Number(s.cpu_temp),
  };
}

function _seedServerSeries(s) {
  _adminServerSeries = [_serverSample(s)];
}

function _pushServerSample(s) {
  _adminServerSeries.push(_serverSample(s));
  if (_adminServerSeries.length > _ADMIN_SERVER_SERIES_LIMIT) {
    _adminServerSeries = _adminServerSeries.slice(-_ADMIN_SERVER_SERIES_LIMIT);
  }
}

function _adminChartTextColor() {
  return getComputedStyle(document.documentElement).getPropertyValue('--text-secondary').trim() || '#64748b';
}

function _adminChartGridColor() {
  return getComputedStyle(document.documentElement).getPropertyValue('--border').trim() || '#dbe3ee';
}

function _renderTimelineSection(server, timeline) {
  const seeded = _adminServerSeries.length ? _adminServerSeries : [_serverSample(server)];
  return `
    <section class="admin-chart-grid">
      <div class="admin-chart-panel">
        <div class="admin-chart-head">
          <div>
            <h2 class="admin-chart-title">서버 상태</h2>
            <div class="admin-chart-sub">5초 라이브 샘플 · CPU/메모리/디스크</div>
          </div>
          <span class="admin-badge">live</span>
        </div>
        <div class="admin-chart" id="adminServerChart">${_renderServerChartSvg(seeded)}</div>
      </div>
      <div class="admin-chart-panel">
        <div class="admin-chart-head">
          <div>
            <h2 class="admin-chart-title">이벤트 발생</h2>
            <div class="admin-chart-sub">최근 ${timeline?.hours || 24}시간 · info/warn/error</div>
          </div>
        </div>
        <div class="admin-chart">${_renderEventChartSvg(timeline?.events || [])}</div>
      </div>
      <div class="admin-chart-panel">
        <div class="admin-chart-head">
          <div>
            <h2 class="admin-chart-title">접속 통계</h2>
            <div class="admin-chart-sub">느린 요청/5xx tail · HTTP 관측</div>
          </div>
        </div>
        <div class="admin-chart">${_renderHttpChartSvg(timeline?.http || [])}</div>
      </div>
    </section>
  `;
}

function _renderServerTimeline() {
  const el = document.getElementById('adminServerChart');
  if (el) el.innerHTML = _renderServerChartSvg(_adminServerSeries);
}

function _chartBaseSvg(inner, {maxLabel = '100%', minLabel = '0'} = {}) {
  const text = _adminChartTextColor();
  const grid = _adminChartGridColor();
  return `
    <svg viewBox="0 0 560 218" role="img" aria-label="시계열 그래프" preserveAspectRatio="none">
      <line x1="44" y1="20" x2="44" y2="176" stroke="${grid}" stroke-width="1"/>
      <line x1="44" y1="176" x2="540" y2="176" stroke="${grid}" stroke-width="1"/>
      <line x1="44" y1="98" x2="540" y2="98" stroke="${grid}" stroke-width="1" stroke-dasharray="3 5"/>
      <text x="10" y="25" fill="${text}" font-size="11">${maxLabel}</text>
      <text x="18" y="179" fill="${text}" font-size="11">${minLabel}</text>
      ${inner}
    </svg>
  `;
}

function _polyline(points, key, color, maxValue = 100) {
  if (!points.length) return '';
  const width = 496;
  const height = 156;
  const x0 = 44;
  const y0 = 176;
  const denom = Math.max(1, points.length - 1);
  const coords = points.map((p, idx) => {
    const v = Math.max(0, Math.min(maxValue, Number(p[key] || 0)));
    const x = x0 + (idx / denom) * width;
    const y = y0 - (v / maxValue) * height;
    return `${x.toFixed(1)},${y.toFixed(1)}`;
  }).join(' ');
  return `<polyline points="${coords}" fill="none" stroke="${color}" stroke-width="3" stroke-linecap="round" stroke-linejoin="round"/>`;
}

function _renderServerChartSvg(points) {
  const data = (points || []).filter(Boolean);
  if (!data.length) return '<div class="admin-chart-empty">서버 샘플 대기 중</div>';
  const last = data[data.length - 1] || {};
  const legend = `
    <g transform="translate(44,198)" font-size="11">
      <circle cx="0" cy="-4" r="4" fill="#2563eb"/><text x="10" y="0" fill="${_adminChartTextColor()}">CPU ${Math.round(last.cpu || 0)}%</text>
      <circle cx="92" cy="-4" r="4" fill="#0f766e"/><text x="102" y="0" fill="${_adminChartTextColor()}">Memory ${Math.round(last.memory || 0)}%</text>
      <circle cx="214" cy="-4" r="4" fill="#d97706"/><text x="224" y="0" fill="${_adminChartTextColor()}">Disk ${Math.round(last.disk || 0)}%</text>
    </g>
  `;
  return _chartBaseSvg(`
    ${_polyline(data, 'cpu', '#2563eb')}
    ${_polyline(data, 'memory', '#0f766e')}
    ${_polyline(data, 'disk', '#d97706')}
    ${legend}
  `);
}

function _bucketLabel(bucket) {
  if (!bucket) return '';
  const d = new Date(bucket);
  if (Number.isNaN(d.getTime())) return String(bucket).slice(11, 16);
  return `${String(d.getHours()).padStart(2, '0')}:00`;
}

function _renderEventChartSvg(rows) {
  const data = (rows || []).filter(r => r && r.bucket);
  if (!data.length) return '<div class="admin-chart-empty">이벤트 기록 없음</div>';
  const max = Math.max(1, ...data.map(r => Number(r.total || 0)));
  const barW = Math.max(4, Math.min(28, 452 / data.length));
  const gap = data.length > 1 ? Math.max(2, (496 - data.length * barW) / (data.length - 1)) : 0;
  const bars = data.map((r, idx) => {
    const x = 44 + idx * (barW + gap);
    let y = 176;
    const parts = [
      ['info', '#0f766e'],
      ['warning', '#d97706'],
      ['error', '#dc2626'],
    ];
    const rects = parts.map(([key, color]) => {
      const h = (Number(r[key] || 0) / max) * 156;
      y -= h;
      return h > 0 ? `<rect x="${x.toFixed(1)}" y="${y.toFixed(1)}" width="${barW.toFixed(1)}" height="${h.toFixed(1)}" fill="${color}" rx="2"/>` : '';
    }).join('');
    const label = idx === 0 || idx === data.length - 1 || idx % Math.ceil(data.length / 4) === 0
      ? `<text x="${x.toFixed(1)}" y="192" fill="${_adminChartTextColor()}" font-size="10">${_bucketLabel(r.bucket)}</text>`
      : '';
    return rects + label;
  }).join('');
  const legend = `<text x="410" y="18" fill="${_adminChartTextColor()}" font-size="11">최대 ${max.toLocaleString()}건/h</text>`;
  return _chartBaseSvg(`${legend}${bars}`, {maxLabel: `${max}`, minLabel: '0'});
}

function _renderHttpChartSvg(rows) {
  const data = (rows || []).filter(r => r && r.bucket);
  if (!data.length) return '<div class="admin-chart-empty">느린 요청/5xx 기록 없음</div>';
  const max = Math.max(1, ...data.map(r => Number(r.count || 0)));
  const barW = Math.max(4, Math.min(28, 452 / data.length));
  const gap = data.length > 1 ? Math.max(2, (496 - data.length * barW) / (data.length - 1)) : 0;
  const bars = data.map((r, idx) => {
    const x = 44 + idx * (barW + gap);
    const totalH = (Number(r.count || 0) / max) * 156;
    const errH = (Number(r.errors || 0) / max) * 156;
    const y = 176 - totalH;
    const errY = 176 - errH;
    const label = idx === 0 || idx === data.length - 1 || idx % Math.ceil(data.length / 4) === 0
      ? `<text x="${x.toFixed(1)}" y="192" fill="${_adminChartTextColor()}" font-size="10">${_bucketLabel(r.bucket)}</text>`
      : '';
    return `
      <rect x="${x.toFixed(1)}" y="${y.toFixed(1)}" width="${barW.toFixed(1)}" height="${totalH.toFixed(1)}" fill="#2563eb" opacity="0.78" rx="2"/>
      ${errH > 0 ? `<rect x="${x.toFixed(1)}" y="${errY.toFixed(1)}" width="${barW.toFixed(1)}" height="${errH.toFixed(1)}" fill="#dc2626" rx="2"/>` : ''}
      ${label}
    `;
  }).join('');
  const maxLatency = Math.max(0, ...data.map(r => Number(r.max_ms || 0)));
  const legend = `<text x="356" y="18" fill="${_adminChartTextColor()}" font-size="11">max ${Math.round(maxLatency).toLocaleString()}ms</text>`;
  return _chartBaseSvg(`${legend}${bars}`, {maxLabel: `${max}`, minLabel: '0'});
}
