// Lightweight canvas chart for portfolio performance views.
// It intentionally avoids ECharts so the NAV/value trend appears immediately
// without downloading and initializing a 1MB charting bundle.
(function () {
  const DEFAULT_GRID = { left: 56, right: 14, top: 16, bottom: 52 };

  function toNumber(value) {
    if (value === null || value === undefined || value === '' || value === '-') return null;
    const n = Number(value);
    return Number.isFinite(n) ? n : null;
  }

  function colorWithAlpha(color, alpha) {
    if (!color) return `rgba(239,68,68,${alpha})`;
    if (color.startsWith('#') && (color.length === 7 || color.length === 4)) {
      let hex = color.slice(1);
      if (hex.length === 3) hex = hex.split('').map(ch => ch + ch).join('');
      const r = parseInt(hex.slice(0, 2), 16);
      const g = parseInt(hex.slice(2, 4), 16);
      const b = parseInt(hex.slice(4, 6), 16);
      return `rgba(${r},${g},${b},${alpha})`;
    }
    if (color.startsWith('rgb(')) return color.replace('rgb(', 'rgba(').replace(')', `,${alpha})`);
    return color;
  }

  function formatDateLabel(label) {
    if (!label || typeof label !== 'string') return '';
    const parts = label.split('-');
    if (parts.length === 3) return `${parts[1]}-${parts[2]}`;
    return label;
  }

  function resolveThemeColor(name, fallback) {
    const value = getComputedStyle(document.documentElement).getPropertyValue(name).trim();
    return value || fallback;
  }

  class PortfolioCanvasChart {
    constructor(container, option = {}) {
      this.container = container;
      this.option = {};
      this.zoom = { start: 0, end: 100 };
      this.handlers = { datazoom: [] };
      this.hoverIndex = null;
      this.disposed = false;

      container.innerHTML = '';
      container.classList.add('pf-lite-chart');
      container.style.position = container.style.position || 'relative';

      this.canvas = document.createElement('canvas');
      this.canvas.className = 'pf-lite-canvas';
      this.tooltip = document.createElement('div');
      this.tooltip.className = 'pf-lite-tooltip';
      this.rangeEl = document.createElement('div');
      this.rangeEl.className = 'pf-lite-zoom';
      this.startInput = document.createElement('input');
      this.endInput = document.createElement('input');
      [this.startInput, this.endInput].forEach(input => {
        input.type = 'range';
        input.min = '0';
        input.max = '100';
        input.step = '0.1';
        input.className = 'pf-lite-zoom-range';
      });
      this.startInput.value = '0';
      this.endInput.value = '100';
      this.rangeEl.append(this.startInput, this.endInput);
      container.append(this.canvas, this.tooltip, this.rangeEl);

      this.ctx = this.canvas.getContext('2d');
      this._bindEvents();
      this.setOption(option);
    }

    _bindEvents() {
      this._onPointerMove = event => {
        const labels = this._labels();
        if (!labels.length) return;
        const rect = this.canvas.getBoundingClientRect();
        const x = event.clientX - rect.left;
        const plot = this._plotArea();
        const { startIdx, endIdx } = this._window();
        if (x < plot.left || x > plot.right || event.clientY - rect.top < plot.top || event.clientY - rect.top > plot.bottom) {
          this.hoverIndex = null;
          this.tooltip.style.display = 'none';
          this.draw();
          return;
        }
        const pct = (x - plot.left) / Math.max(1, plot.right - plot.left);
        this.hoverIndex = Math.max(startIdx, Math.min(endIdx, Math.round(startIdx + pct * Math.max(1, endIdx - startIdx))));
        this._showTooltip(event);
        this.draw();
      };
      this._onPointerLeave = () => {
        this.hoverIndex = null;
        this.tooltip.style.display = 'none';
        this.draw();
      };
      this._onRangeInput = () => {
        let start = Number(this.startInput.value);
        let end = Number(this.endInput.value);
        if (start > end - 1) {
          if (document.activeElement === this.startInput) start = end - 1;
          else end = start + 1;
        }
        start = Math.max(0, Math.min(99, start));
        end = Math.max(start + 1, Math.min(100, end));
        this.zoom = { start, end };
        this.startInput.value = String(start);
        this.endInput.value = String(end);
        this.draw();
        this._emit('datazoom');
      };
      this.canvas.addEventListener('pointermove', this._onPointerMove);
      this.canvas.addEventListener('pointerleave', this._onPointerLeave);
      this.startInput.addEventListener('input', this._onRangeInput);
      this.endInput.addEventListener('input', this._onRangeInput);
    }

    dispose() {
      this.disposed = true;
      this.canvas.removeEventListener('pointermove', this._onPointerMove);
      this.canvas.removeEventListener('pointerleave', this._onPointerLeave);
      this.startInput.removeEventListener('input', this._onRangeInput);
      this.endInput.removeEventListener('input', this._onRangeInput);
      this.container.innerHTML = '';
      this.container.classList.remove('pf-lite-chart');
    }

    resize() {
      this.draw();
    }

    on(event, handler) {
      if (!this.handlers[event]) this.handlers[event] = [];
      this.handlers[event].push(handler);
    }

    _emit(event) {
      (this.handlers[event] || []).forEach(handler => {
        try { handler(); } catch (err) { console.warn(err); }
      });
    }

    dispatchAction(action = {}) {
      if (action.type !== 'dataZoom') return;
      const start = Number.isFinite(Number(action.start)) ? Number(action.start) : this.zoom.start;
      const end = Number.isFinite(Number(action.end)) ? Number(action.end) : this.zoom.end;
      this.zoom = {
        start: Math.max(0, Math.min(99, start)),
        end: Math.max(Math.max(0, Math.min(99, start)) + 1, Math.min(100, end)),
      };
      this.startInput.value = String(this.zoom.start);
      this.endInput.value = String(this.zoom.end);
      this.draw();
      this._emit('datazoom');
    }

    getOption() {
      return {
        ...this.option,
        dataZoom: [{ start: this.zoom.start, end: this.zoom.end }],
      };
    }

    setOption(next = {}) {
      if (next.xAxis) this.option.xAxis = next.xAxis;
      if (next.legend !== undefined) this.option.legend = next.legend;
      if (next.grid) this.option.grid = { ...(this.option.grid || {}), ...next.grid };
      if (next.tooltip) this.option.tooltip = next.tooltip;
      if (next.yAxis) this.option.yAxis = { ...(this.option.yAxis || {}), ...next.yAxis };
      if (next.dataZoom !== undefined) {
        this.option.dataZoom = next.dataZoom;
        const dz = Array.isArray(next.dataZoom) ? next.dataZoom[0] : null;
        if (dz) {
          this.zoom.start = Number.isFinite(Number(dz.start)) ? Number(dz.start) : this.zoom.start;
          this.zoom.end = Number.isFinite(Number(dz.end)) ? Number(dz.end) : this.zoom.end;
        }
      }
      if (next.series) {
        if (!this.option.series) {
          this.option.series = next.series;
        } else {
          const maxLen = Math.max(this.option.series.length, next.series.length);
          this.option.series = Array.from({ length: maxLen }, (_, idx) => ({
            ...(this.option.series[idx] || {}),
            ...(next.series[idx] || {}),
          })).filter(series => series && series.data);
        }
      }
      this._syncRangeVisibility();
      this.draw();
    }

    _syncRangeVisibility() {
      const enabled = Array.isArray(this.option.dataZoom) && this.option.dataZoom.length > 0;
      this.rangeEl.style.display = enabled ? 'block' : 'none';
      this.startInput.value = String(this.zoom.start);
      this.endInput.value = String(this.zoom.end);
    }

    _labels() {
      return this.option?.xAxis?.data || [];
    }

    _series() {
      return (this.option.series || []).map(series => ({
        ...series,
        values: (series.data || []).map(toNumber),
        color: series.lineStyle?.color || series.itemStyle?.color || '#ef4444',
        dashed: series.lineStyle?.type === 'dashed',
      }));
    }

    _window() {
      const labels = this._labels();
      const last = Math.max(0, labels.length - 1);
      return {
        startIdx: Math.max(0, Math.min(last, Math.round(this.zoom.start / 100 * last))),
        endIdx: Math.max(0, Math.min(last, Math.round(this.zoom.end / 100 * last))),
      };
    }

    _plotArea() {
      const width = this.canvas.clientWidth || this.container.clientWidth || 640;
      const height = this.canvas.clientHeight || this.container.clientHeight || 320;
      const grid = { ...DEFAULT_GRID, ...(this.option.grid || {}) };
      return {
        left: grid.left,
        right: width - grid.right,
        top: grid.top,
        bottom: height - grid.bottom,
        width,
        height,
      };
    }

    _axisRange(seriesList, startIdx, endIdx) {
      const yAxis = this.option.yAxis || {};
      const values = [];
      seriesList.forEach(series => {
        for (let i = startIdx; i <= endIdx; i++) {
          const value = series.values[i];
          if (Number.isFinite(value)) values.push(value);
        }
      });
      if (!values.length) return { min: 0, max: 1 };
      let min = Number.isFinite(Number(yAxis.min)) ? Number(yAxis.min) : Math.min(...values);
      let max = Number.isFinite(Number(yAxis.max)) ? Number(yAxis.max) : Math.max(...values);
      if (yAxis.min === 0) min = 0;
      if (min === max) {
        const pad = Math.max(Math.abs(max) * 0.02, 1);
        min -= pad;
        max += pad;
      }
      return { min, max };
    }

    _xFor(index, plot, startIdx, endIdx) {
      const span = Math.max(1, endIdx - startIdx);
      return plot.left + ((index - startIdx) / span) * (plot.right - plot.left);
    }

    _yFor(value, plot, min, max) {
      const span = max - min || 1;
      return plot.top + (1 - (value - min) / span) * (plot.bottom - plot.top);
    }

    _showTooltip(event) {
      const labels = this._labels();
      const seriesList = this._series();
      const idx = this.hoverIndex;
      if (idx == null || !labels[idx]) return;
      const rows = seriesList
        .map(series => {
          const value = series.values[idx];
          if (!Number.isFinite(value)) return '';
          return `<div><span style="background:${series.color}"></span>${series.name || ''}: ${value.toLocaleString(undefined, { maximumFractionDigits: 2 })}</div>`;
        })
        .filter(Boolean)
        .join('');
      if (!rows) {
        this.tooltip.style.display = 'none';
        return;
      }
      const rect = this.container.getBoundingClientRect();
      this.tooltip.innerHTML = `<strong>${labels[idx]}</strong>${rows}`;
      this.tooltip.style.display = 'block';
      const x = Math.min(rect.width - 180, Math.max(8, event.clientX - rect.left + 14));
      const y = Math.min(rect.height - 80, Math.max(8, event.clientY - rect.top - 36));
      this.tooltip.style.left = `${x}px`;
      this.tooltip.style.top = `${y}px`;
    }

    draw() {
      if (this.disposed || !this.ctx) return;
      const rect = this.container.getBoundingClientRect();
      const cssWidth = Math.max(1, Math.round(rect.width || this.container.clientWidth || 640));
      const cssHeight = Math.max(1, Math.round(rect.height || this.container.clientHeight || 320));
      const dpr = Math.max(1, Math.min(2, window.devicePixelRatio || 1));
      if (this.canvas.width !== Math.round(cssWidth * dpr) || this.canvas.height !== Math.round(cssHeight * dpr)) {
        this.canvas.width = Math.round(cssWidth * dpr);
        this.canvas.height = Math.round(cssHeight * dpr);
        this.canvas.style.width = `${cssWidth}px`;
        this.canvas.style.height = `${cssHeight}px`;
      }
      const ctx = this.ctx;
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
      ctx.clearRect(0, 0, cssWidth, cssHeight);

      const labels = this._labels();
      const seriesList = this._series();
      if (!labels.length || !seriesList.length) return;

      const plot = this._plotArea();
      const { startIdx, endIdx } = this._window();
      const { min, max } = this._axisRange(seriesList, startIdx, endIdx);
      const textColor = resolveThemeColor('--text-secondary', '#64748b');
      const gridColor = resolveThemeColor('--border', '#e5e7eb');

      ctx.save();
      ctx.font = '11px sans-serif';
      ctx.textBaseline = 'middle';
      ctx.strokeStyle = gridColor;
      ctx.fillStyle = textColor;
      ctx.lineWidth = 1;
      for (let i = 0; i <= 4; i++) {
        const y = plot.top + (i / 4) * (plot.bottom - plot.top);
        const value = max - (i / 4) * (max - min);
        ctx.globalAlpha = 0.85;
        ctx.beginPath();
        ctx.moveTo(plot.left, y);
        ctx.lineTo(plot.right, y);
        ctx.stroke();
        ctx.globalAlpha = 1;
        ctx.textAlign = 'right';
        const formatter = this.option.yAxis?.axisLabel?.formatter;
        const label = typeof formatter === 'function' ? formatter(value) : Math.round(value).toLocaleString();
        ctx.fillText(label, plot.left - 8, y);
      }

      const tickCount = Math.min(6, Math.max(2, endIdx - startIdx + 1));
      ctx.textAlign = 'center';
      ctx.textBaseline = 'top';
      for (let i = 0; i < tickCount; i++) {
        const idx = Math.round(startIdx + (i / Math.max(1, tickCount - 1)) * (endIdx - startIdx));
        const x = this._xFor(idx, plot, startIdx, endIdx);
        ctx.fillText(formatDateLabel(labels[idx]), x, plot.bottom + 8);
      }

      seriesList.forEach((series, seriesIdx) => {
        const points = [];
        for (let i = startIdx; i <= endIdx; i++) {
          const value = series.values[i];
          if (Number.isFinite(value)) {
            points.push({ x: this._xFor(i, plot, startIdx, endIdx), y: this._yFor(value, plot, min, max), i });
          } else if (!series.connectNulls) {
            points.push(null);
          }
        }
        if (!points.some(Boolean)) return;
        if (seriesIdx === 0 && series.areaStyle !== undefined) {
          const clean = points.filter(Boolean);
          if (clean.length > 1) {
            const gradient = ctx.createLinearGradient(0, plot.top, 0, plot.bottom);
            gradient.addColorStop(0, colorWithAlpha(series.color, 0.22));
            gradient.addColorStop(1, colorWithAlpha(series.color, 0));
            ctx.beginPath();
            clean.forEach((point, idx) => {
              if (idx === 0) ctx.moveTo(point.x, point.y);
              else ctx.lineTo(point.x, point.y);
            });
            ctx.lineTo(clean[clean.length - 1].x, plot.bottom);
            ctx.lineTo(clean[0].x, plot.bottom);
            ctx.closePath();
            ctx.fillStyle = gradient;
            ctx.fill();
          }
        }
        ctx.beginPath();
        ctx.strokeStyle = series.color;
        ctx.lineWidth = series.lineStyle?.width || (seriesIdx === 0 ? 2.4 : 1.7);
        ctx.setLineDash(series.dashed ? [6, 4] : []);
        let started = false;
        points.forEach(point => {
          if (!point) {
            started = false;
            return;
          }
          if (!started) {
            ctx.moveTo(point.x, point.y);
            started = true;
          } else {
            ctx.lineTo(point.x, point.y);
          }
        });
        ctx.stroke();
        ctx.setLineDash([]);
      });

      if (this.hoverIndex != null && this.hoverIndex >= startIdx && this.hoverIndex <= endIdx) {
        const x = this._xFor(this.hoverIndex, plot, startIdx, endIdx);
        ctx.strokeStyle = colorWithAlpha(textColor, 0.45);
        ctx.setLineDash([3, 3]);
        ctx.beginPath();
        ctx.moveTo(x, plot.top);
        ctx.lineTo(x, plot.bottom);
        ctx.stroke();
        ctx.setLineDash([]);
      }

      const legend = this.option.legend?.data || [];
      if (legend.length) {
        let x = plot.right;
        ctx.font = '11px sans-serif';
        ctx.textAlign = 'right';
        legend.slice().reverse().forEach(name => {
          const series = seriesList.find(item => item.name === name);
          const width = ctx.measureText(name).width + 24;
          ctx.fillStyle = textColor;
          ctx.fillText(name, x, 12);
          ctx.strokeStyle = series?.color || textColor;
          ctx.lineWidth = 2;
          ctx.setLineDash(series?.dashed ? [5, 3] : []);
          ctx.beginPath();
          ctx.moveTo(x - width + 4, 12);
          ctx.lineTo(x - width + 18, 12);
          ctx.stroke();
          ctx.setLineDash([]);
          x -= width + 10;
        });
      }
      ctx.restore();
    }
  }

  window.PortfolioTrendChart = {
    create(container, option) {
      return new PortfolioCanvasChart(container, option);
    },
  };
})();
