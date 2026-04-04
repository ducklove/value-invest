"""Daily NPS (국민연금공단) portfolio snapshot. Run via systemd timer at 22:05 KST."""

import asyncio
import html
import json
import logging
from datetime import date, datetime

import cache
from nps_scraper import fetch_nps_holdings, resolve_stock_codes

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BASE_NAV = 1000.0


def _esc(text: str) -> str:
    return html.escape(str(text or ""), quote=True)


def _fmt_krw(val: float) -> str:
    """Format KRW value with comma-separated thousands (no suffix)."""
    return f"{round(val):,}"


def _fmt_pct(val: float | None) -> str:
    if val is None:
        return "-"
    sign = "+" if val > 0 else ""
    return f"{sign}{val:.2f}%"


def _pct_color_class(val: float | None) -> str:
    """Korean convention: red=up, blue=down."""
    if val is None or val == 0:
        return "nps-neutral"
    return "nps-up" if val > 0 else "nps-down"


async def _fetch_quotes_for_holdings(holdings: list[dict]) -> list[dict]:
    """Fetch closing price + change_pct for each holding that has a stock_code."""
    from routes.portfolio import _fetch_quote

    enriched = []
    for h in holdings:
        code = h.get("stock_code", "")
        if not code:
            h["price"] = None
            h["change_pct"] = None
            h["market_value"] = None
            enriched.append(h)
            continue
        try:
            q = await _fetch_quote(code)
            price = q.get("price") if q else None
            change_pct = q.get("change_pct") if q else None
        except Exception as e:
            logger.warning("Quote fetch failed for %s: %s", code, e)
            price = None
            change_pct = None
        shares = h.get("shares", 0)
        market_value = round(price * shares) if price is not None else None
        h["price"] = price
        h["change_pct"] = change_pct
        h["market_value"] = market_value
        enriched.append(h)
        await asyncio.sleep(0.25)  # rate limit
    return enriched


def _compute_nav(snapshots: list[dict], total_value: float) -> tuple[float, float]:
    """Return (nav, total_units). No cashflows for NPS."""
    if not snapshots:
        # First snapshot ever
        nav = BASE_NAV
        total_units = total_value / BASE_NAV if total_value > 0 else 0.0
    else:
        # Reuse total_units from most recent snapshot
        latest = snapshots[-1]
        total_units = latest.get("total_units") or (latest["total_value"] / BASE_NAV)
        nav = total_value / total_units if total_units > 0 else BASE_NAV
    return nav, total_units


def _find_snapshot_for_date(snapshots: list[dict], target_date: str) -> dict | None:
    """Binary-ish scan: find latest snapshot <= target_date."""
    match = None
    for s in snapshots:
        if s["date"] <= target_date:
            match = s
        else:
            break
    return match


def _mtd_pct(snapshots: list[dict], snap_date: str) -> float | None:
    """MTD: compare today vs last day of previous month."""
    today = date.fromisoformat(snap_date)
    if today.month == 1:
        prev_month_last = date(today.year - 1, 12, 31).isoformat()
    else:
        import calendar
        prev_month = today.month - 1
        last_day = calendar.monthrange(today.year, prev_month)[1]
        prev_month_last = date(today.year, prev_month, last_day).isoformat()

    ref = _find_snapshot_for_date(snapshots, prev_month_last)
    if ref is None or ref["nav"] == 0:
        return None
    latest_nav = snapshots[-1]["nav"] if snapshots else None
    if latest_nav is None:
        return None
    return (latest_nav / ref["nav"] - 1) * 100


def _ytd_pct(snapshots: list[dict], snap_date: str) -> float | None:
    """YTD: compare today vs first snapshot of this year."""
    year = snap_date[:4]
    year_start = f"{year}-01-01"
    # Find snapshot just before this year (or first available)
    ref = None
    for s in snapshots[:-1]:  # exclude today
        if s["date"] < year_start:
            ref = s
        elif ref is None and s["date"] >= year_start:
            ref = s
            break
    if ref is None or ref["nav"] == 0:
        return None
    latest_nav = snapshots[-1]["nav"] if snapshots else None
    if latest_nav is None:
        return None
    return (latest_nav / ref["nav"] - 1) * 100


def _today_change_pct(holdings: list[dict]) -> float | None:
    """Weighted average daily change_pct across holdings."""
    total_mv = 0.0
    weighted_sum = 0.0
    for h in holdings:
        mv = h.get("market_value")
        cp = h.get("change_pct")
        if mv is not None and cp is not None:
            weighted_sum += cp * mv
            total_mv += mv
    if total_mv == 0:
        return None
    return weighted_sum / total_mv


def _pct_to_treemap_color(pct: float | None, is_dark: bool = False) -> str:
    """Blue(-) → gray(0) → red(+), clamped ±5%."""
    if pct is None:
        return "#475569" if is_dark else "#9ca3af"
    clamped = max(-5.0, min(5.0, pct))
    t = (clamped + 5.0) / 10.0  # 0..1
    if t < 0.5:
        s = t / 0.5
        r = round(37 + (148 - 37) * s)
        g = round(99 + (163 - 99) * s)
        b = round(235 + (184 - 235) * s)
    else:
        s = (t - 0.5) / 0.5
        r = round(148 + (220 - 148) * s)
        g = round(163 + (38 - 163) * s)
        b = round(184 + (38 - 184) * s)
    return f"rgb({r},{g},{b})"


def _fetch_kospi_history(dates: list[str]) -> list[dict]:
    """Fetch KOSPI index values for given dates using yfinance."""
    try:
        import yfinance as yf
        if not dates:
            return []
        ticker = yf.Ticker("^KS11")
        hist = ticker.history(start=dates[0], end="2099-12-31", interval="1d")
        by_date = {d.strftime("%Y-%m-%d"): row["Close"] for d, row in hist.iterrows()}
        result = []
        for d in dates:
            val = by_date.get(d)
            if val:
                result.append({"date": d, "value": round(val, 2)})
        return result
    except Exception:
        return []


def _build_treemap_data(holdings: list[dict]) -> list[dict]:
    """Build ECharts treemap series data (flat, one level)."""
    items = []
    for h in holdings:
        mv = h.get("market_value")
        if not mv or mv <= 0:
            continue
        cp = h.get("change_pct")
        items.append({
            "name": h.get("stock_name") or h.get("name", ""),
            "value": mv,
            "changePct": cp,
            "itemStyle": {"color": _pct_to_treemap_color(cp)},
        })
    return items


def _return_to_color(pct: float | None, range_: float = 20.0) -> str:
    """Map return % to blue-red hex color (same as JS returnToColor)."""
    if pct is None:
        return "#9ca3af"
    t = max(-1.0, min(1.0, pct / range_))
    abs_t = abs(t)
    gray = (156, 163, 175)
    blue = (37, 99, 235)
    red = (220, 38, 38)
    target = blue if t < 0 else red
    r = round(gray[0] + (target[0] - gray[0]) * abs_t)
    g = round(gray[1] + (target[1] - gray[1]) * abs_t)
    b = round(gray[2] + (target[2] - gray[2]) * abs_t)
    return f"#{r:02x}{g:02x}{b:02x}"


def _build_html(
    snap_date: str,
    holdings: list[dict],
    total_value: float,
    nav: float,
    today_pct: float | None,
    mtd: float | None,
    ytd: float | None,
    nav_history: list[dict],
    value_history: list[dict],
    kospi_history: list[dict] | None = None,
) -> str:
    today_cls = _pct_color_class(today_pct)
    mtd_cls = _pct_color_class(mtd)
    ytd_cls = _pct_color_class(ytd)

    # Summary cards
    cards_html = f"""
<div class="pf-summary" style="margin-bottom:20px;">
  <div class="pf-summary-card">
    <div class="pf-summary-text">
      <div class="pf-summary-label">총 평가금액</div>
      <div class="pf-summary-value">{_esc(_fmt_krw(total_value))}</div>
      <div class="pf-summary-sub">NAV {nav:.2f}</div>
    </div>
  </div>
  <div class="pf-summary-card">
    <div class="pf-summary-text">
      <div class="pf-summary-label">오늘</div>
      <div class="pf-summary-value {today_cls}">{_esc(_fmt_pct(today_pct))}</div>
      <div class="pf-summary-sub">가중평균 등락률</div>
    </div>
  </div>
  <div class="pf-summary-card">
    <div class="pf-summary-text">
      <div class="pf-summary-label">MTD</div>
      <div class="pf-summary-value {mtd_cls}">{_esc(_fmt_pct(mtd))}</div>
      <div class="pf-summary-sub">전월말 대비</div>
    </div>
  </div>
  <div class="pf-summary-card">
    <div class="pf-summary-text">
      <div class="pf-summary-label">YTD</div>
      <div class="pf-summary-value {ytd_cls}">{_esc(_fmt_pct(ytd))}</div>
      <div class="pf-summary-sub">연초 대비</div>
    </div>
  </div>
</div>
"""

    # Holdings table
    rows_html = ""
    total_displayed = sum(h.get("market_value") or 0 for h in holdings if h.get("market_value"))
    for i, h in enumerate(holdings):
        name = h.get("stock_name") or h.get("name", "")
        cp = h.get("change_pct")
        price = h.get("price")
        shares = h.get("shares", 0)
        mv = h.get("market_value")
        weight = (mv / total_displayed * 100) if (mv and total_displayed > 0) else None
        own_pct = h.get("ownership_pct")

        cp_cls = _pct_color_class(cp)
        rows_html += f"""<tr>
  <td class="pf-col-name">{_esc(name)}</td>
  <td class="pf-col-num {cp_cls}">{_esc(_fmt_pct(cp))}</td>
  <td class="pf-col-num">{_esc(f"{round(price):,}") if price else "-"}</td>
  <td class="pf-col-num">{_esc(f"{shares:,}")}</td>
  <td class="pf-col-num">{_esc(_fmt_krw(mv)) if mv else "-"}</td>
  <td class="pf-col-num">{_esc(f"{weight:.1f}%") if weight is not None else "-"}</td>
  <td class="pf-col-num">{_esc(f"{own_pct:.2f}%") if own_pct is not None else "-"}</td>
</tr>
"""

    table_html = f"""
<div class="pf-table-wrap" style="margin-bottom:24px;">
  <table class="pf-table">
    <thead>
      <tr>
        <th class="pf-col-name">종목명</th>
        <th class="pf-col-num">등락률</th>
        <th class="pf-col-num">현재가</th>
        <th class="pf-col-num">수량</th>
        <th class="pf-col-num">평가금액</th>
        <th class="pf-col-num">비중</th>
        <th class="pf-col-num">지분율</th>
      </tr>
    </thead>
    <tbody>
{rows_html}
    </tbody>
  </table>
</div>
"""

    # Treemap container
    treemap_html = """
<div class="pf-nav-chart-wrap" style="margin-bottom:24px;">
  <div class="pf-nav-header">
    <h3 style="font-size:16px;margin:0;">포트폴리오 구성</h3>
  </div>
  <div id="npsTreemap" style="height:640px;border:1px solid var(--border);border-radius:8px;background:var(--surface);"></div>
</div>
"""

    # NAV chart container
    nav_chart_html = """
<div class="pf-nav-chart-wrap" style="margin-bottom:24px;">
  <div class="pf-nav-header">
    <h3 style="font-size:16px;margin:0;">NAV 추이</h3>
  </div>
  <div class="pf-nav-chart-container">
    <div id="npsNavChart" style="width:100%;height:100%;border:1px solid var(--border);border-radius:8px;background:var(--surface);"></div>
  </div>
</div>
"""

    # Value chart container
    value_chart_html = """
<div class="pf-nav-chart-wrap" style="margin-bottom:24px;">
  <div class="pf-nav-header">
    <h3 style="font-size:16px;margin:0;">총 평가금액 추이</h3>
  </div>
  <div class="pf-nav-chart-container">
    <div id="npsValueChart" style="width:100%;height:100%;border:1px solid var(--border);border-radius:8px;background:var(--surface);"></div>
  </div>
</div>
"""

    # Inline JSON data
    treemap_data = _build_treemap_data(holdings)
    nav_chart_data = [{"date": s["date"], "nav": s["nav"]} for s in nav_history]
    value_chart_data = [{"date": s["date"], "total_value": s["total_value"]} for s in value_history]

    # Compute line color for NAV chart (YoY-based)
    nav_color = "#9ca3af"
    if len(nav_history) > 1:
        last_365 = nav_history[-365:]
        if len(last_365) > 1:
            yoy = (nav_history[-1]["nav"] / last_365[0]["nav"] - 1) * 100
            nav_color = _return_to_color(yoy)

    # Compute line color for value chart (MoM-based)
    val_color = "#9ca3af"
    if len(value_history) > 1:
        last_30 = value_history[-30:]
        if len(last_30) > 1:
            mom = (value_history[-1]["total_value"] / last_30[0]["total_value"] - 1) * 100
            val_color = _return_to_color(mom, 10)

    script_html = f"""
<style>
.nps-up   {{ color: #dc2626; }}
.nps-down {{ color: #2563eb; }}
.nps-neutral {{ color: var(--text-secondary); }}
[data-theme="dark"] .nps-up   {{ color: #fca5a5; }}
[data-theme="dark"] .nps-down {{ color: #93c5fd; }}
</style>
<script>
(function() {{
  const NPS_TREEMAP_DATA = {json.dumps(treemap_data, ensure_ascii=False)};
  const NPS_NAV_DATA     = {json.dumps(nav_chart_data, ensure_ascii=False)};
  const NPS_KOSPI_DATA   = {json.dumps(kospi_history or [], ensure_ascii=False)};
  const NPS_VALUE_DATA   = {json.dumps(value_chart_data, ensure_ascii=False)};
  const NPS_NAV_COLOR    = {json.dumps(nav_color)};
  const NPS_VAL_COLOR    = {json.dumps(val_color)};

  function _loadScript(src) {{
    return new Promise((resolve, reject) => {{
      if (document.querySelector('script[src="' + src + '"]')) {{ resolve(); return; }}
      const s = document.createElement('script');
      s.src = src;
      s.onload = resolve;
      s.onerror = reject;
      document.head.appendChild(s);
    }});
  }}

  async function _ensureECharts() {{
    if (typeof echarts !== 'undefined') return;
    await _loadScript('https://cdn.jsdelivr.net/npm/echarts@5.6.0/dist/echarts.min.js');
  }}

  function _textColor() {{
    return getComputedStyle(document.documentElement).getPropertyValue('--text-secondary').trim() || '#888';
  }}
  function _gridColor() {{
    return getComputedStyle(document.documentElement).getPropertyValue('--border').trim() || '#ccc';
  }}
  function _isDark() {{
    return document.documentElement.getAttribute('data-theme') === 'dark';
  }}

  function _fmtPct(v) {{
    if (v == null) return '-';
    return (v > 0 ? '+' : '') + v.toFixed(2) + '%';
  }}

  function _renderTreemap() {{
    const container = document.getElementById('npsTreemap');
    if (!container || !NPS_TREEMAP_DATA.length) return;
    const ec = echarts.init(container);
    const isDark = _isDark();
    ec.setOption({{
      tooltip: {{
        formatter(info) {{
          const d = info.data;
          const cpStr = _fmtPct(d.changePct);
          const val = Number(info.value).toLocaleString();
          return '<strong>' + info.name + '</strong><br/>평가: ' + val + '<br/>일간: ' + cpStr;
        }},
      }},
      series: [{{
        type: 'treemap',
        left: 0, right: 0, top: 0, bottom: 0,
        roam: false,
        nodeClick: false,
        breadcrumb: {{ show: false }},
        itemStyle: {{
          borderColor: isDark ? '#334155' : '#e5e7eb',
          borderWidth: 1,
        }},
        label: {{
          show: true,
          formatter(params) {{
            const cp = params.data.changePct;
            const cpStr = cp != null ? (cp > 0 ? '+' : '') + cp.toFixed(2) + '%' : '';
            return '{{name|' + params.name + '}}\\n{{pct|' + cpStr + '}}';
          }},
          rich: {{
            name: {{ fontSize: 11, fontWeight: 600, color: '#fff', lineHeight: 16 }},
            pct: {{ fontSize: 10, color: 'rgba(255,255,255,0.8)', lineHeight: 14 }},
          }},
        }},
        data: NPS_TREEMAP_DATA,
      }}],
    }});
  }}

  function _renderLineChart(containerId, chartData, keyField, color, tooltipPrefix, yFormatter) {{
    const container = document.getElementById(containerId);
    if (!container || !chartData.length) return;
    const labels = chartData.map(d => d.date.slice(5));
    const values = chartData.map(d => d[keyField]);
    const textColor = _textColor();
    const gridColor = _gridColor();
    const ec = echarts.init(container);
    ec.setOption({{
      grid: {{ left: 60, right: 12, top: 10, bottom: 24 }},
      xAxis: {{
        type: 'category',
        data: labels,
        axisLine: {{ lineStyle: {{ color: gridColor }} }},
        axisLabel: {{ color: textColor, fontSize: 10 }},
        splitLine: {{ show: false }},
      }},
      yAxis: {{
        type: 'value',
        axisLine: {{ show: false }},
        axisLabel: {{ color: textColor, fontSize: 10, formatter: yFormatter || null }},
        splitLine: {{ lineStyle: {{ color: gridColor, width: 0.5 }} }},
      }},
      tooltip: {{
        trigger: 'axis',
        formatter(params) {{
          const p = params[0];
          const val = yFormatter ? yFormatter(p.value) : p.value.toLocaleString();
          return p.axisValue + '<br/>' + (tooltipPrefix || '') + val;
        }},
      }},
      series: [{{
        type: 'line',
        data: values,
        lineStyle: {{ color: color, width: 2 }},
        itemStyle: {{ color: color }},
        symbol: 'none',
        smooth: false,
        areaStyle: {{
          color: {{
            type: 'linear', x: 0, y: 0, x2: 0, y2: 1,
            colorStops: [
              {{ offset: 0, color: color + '33' }},
              {{ offset: 1, color: color + '00' }},
            ],
          }},
        }},
      }}],
    }});
  }}

  function _renderNavWithKospi() {{
    const container = document.getElementById('npsNavChart');
    if (!container || !NPS_NAV_DATA.length) return;
    const labels = NPS_NAV_DATA.map(d => d.date.slice(5));
    const navValues = NPS_NAV_DATA.map(d => d.nav);
    const textColor = _textColor();
    const gridColor = _gridColor();
    const navColor = NPS_NAV_COLOR;

    // Normalize KOSPI to start at 1000 (same as NAV base)
    const kospiRaw = NPS_KOSPI_DATA.map(d => d.value);
    let kospiNorm = [];
    if (kospiRaw.length > 0 && kospiRaw[0] > 0) {{
      const base = kospiRaw[0];
      kospiNorm = kospiRaw.map(v => +(v / base * 1000).toFixed(2));
    }}

    const series = [{{
      name: '국민연금',
      type: 'line',
      data: navValues,
      lineStyle: {{ color: navColor, width: 2 }},
      itemStyle: {{ color: navColor }},
      symbol: 'none',
      smooth: false,
      areaStyle: {{
        color: {{
          type: 'linear', x: 0, y: 0, x2: 0, y2: 1,
          colorStops: [
            {{ offset: 0, color: navColor + '33' }},
            {{ offset: 1, color: navColor + '00' }},
          ],
        }},
      }},
    }}];

    if (kospiNorm.length > 0) {{
      series.push({{
        name: 'KOSPI',
        type: 'line',
        data: kospiNorm,
        lineStyle: {{ color: '#94a3b8', width: 1.5, type: 'dashed' }},
        itemStyle: {{ color: '#94a3b8' }},
        symbol: 'none',
        smooth: false,
      }});
    }}

    const ec = echarts.init(container);
    ec.setOption({{
      grid: {{ left: 60, right: 12, top: 24, bottom: 24 }},
      legend: {{
        show: kospiNorm.length > 0,
        top: 0,
        right: 0,
        textStyle: {{ color: textColor, fontSize: 11 }},
      }},
      xAxis: {{
        type: 'category',
        data: labels,
        axisLine: {{ lineStyle: {{ color: gridColor }} }},
        axisLabel: {{ color: textColor, fontSize: 10 }},
        splitLine: {{ show: false }},
      }},
      yAxis: {{
        type: 'value',
        axisLine: {{ show: false }},
        axisLabel: {{ color: textColor, fontSize: 10 }},
        splitLine: {{ lineStyle: {{ color: gridColor, width: 0.5 }} }},
      }},
      tooltip: {{
        trigger: 'axis',
        formatter(params) {{
          let s = params[0].axisValue;
          params.forEach(p => {{
            const pct = ((p.value / 1000 - 1) * 100).toFixed(2);
            s += '<br/>' + p.marker + ' ' + p.seriesName + ': ' + p.value.toLocaleString() + ' (' + (pct > 0 ? '+' : '') + pct + '%)';
          }});
          return s;
        }},
      }},
      series: series,
    }});
  }}

  async function _init() {{
    await _ensureECharts();
    _renderTreemap();
    _renderNavWithKospi();
    _renderLineChart('npsValueChart', NPS_VALUE_DATA, 'total_value', NPS_VAL_COLOR, '', function(v) {{
      return (v / 1e12).toFixed(0) + '조';
    }});
  }}

  if (document.readyState === 'loading') {{
    document.addEventListener('DOMContentLoaded', _init);
  }} else {{
    _init();
  }}
}})();
</script>
"""

    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M")
    header_html = f"""<div style="color:var(--text-secondary);font-size:12px;margin-bottom:16px;">
  국민연금공단 주식 보유 현황 · 기준일 {_esc(snap_date)} · 생성 {_esc(generated_at)}
</div>
"""

    return header_html + cards_html + treemap_html + nav_chart_html + value_chart_html + table_html + script_html


async def run_nps_snapshot(snap_date: str | None = None):
    """Main entry point: scrape → enrich → compute NAV → save → generate HTML."""
    await cache.init_db()

    if snap_date is None:
        snap_date = date.today().isoformat()

    logger.info("NPS snapshot starting for date=%s", snap_date)

    # 1. Scrape holdings from FnGuide
    raw_holdings = fetch_nps_holdings()
    if not raw_holdings:
        logger.error("NPS: no holdings scraped from FnGuide")
        return
    logger.info("NPS: scraped %d holdings", len(raw_holdings))

    # 2. Resolve stock codes
    holdings = await resolve_stock_codes(raw_holdings)

    # 3. Fetch closing prices
    holdings = await _fetch_quotes_for_holdings(holdings)

    # Filter to those with valid market values; keep unknowns too but mark them
    valid = [h for h in holdings if h.get("market_value") is not None]
    total_value = sum(h["market_value"] for h in valid)
    logger.info("NPS: total_value=%.0f from %d/%d holdings", total_value, len(valid), len(holdings))

    if total_value == 0:
        logger.error("NPS: total value is 0, aborting")
        return

    # 4. Load history, compute NAV
    existing_snapshots = await cache.get_nps_snapshots()

    # If today already exists, remove from list to recompute
    history = [s for s in existing_snapshots if s["date"] < snap_date]

    # Re-derive total_units from latest history entry
    if not history:
        nav = BASE_NAV
        total_units = total_value / BASE_NAV
    else:
        latest = history[-1]
        # total_units is not stored in nps_snapshots, re-derive from nav/total_value chain
        # We'll store total_units in the nav field slot and total_value accurately
        # Actually nps_snapshots only stores (date, total_value, nav, total_count, generated_html)
        # We'll re-derive total_units from the first snapshot: total_units = first_total_value / BASE_NAV
        first = history[0]
        first_units = first["total_value"] / BASE_NAV
        # Walk forward applying nav changes to recompute units
        # Since there are no cashflows, total_units is constant after first snapshot
        # nav = total_value / total_units at each step
        # So: total_units = first_total_value / BASE_NAV (constant)
        total_units = first_units
        nav = total_value / total_units

    # 5. Compute return metrics
    # Build a synthetic "today" snapshot appended to history for mtd/ytd calculations
    synthetic = history + [{"date": snap_date, "nav": nav, "total_value": total_value}]

    today_pct = _today_change_pct(holdings)
    mtd = _mtd_pct(synthetic, snap_date)
    ytd = _ytd_pct(synthetic, snap_date)

    # Build chart histories
    nav_history = [{"date": s["date"], "nav": s["nav"]} for s in synthetic]
    value_history = [{"date": s["date"], "total_value": s["total_value"]} for s in synthetic]

    # 6. Prepare items for DB save
    db_items = []
    for h in holdings:
        code = h.get("stock_code")
        if not code or len(code) != 6 or not code.isdigit():
            continue  # Skip non-KOSPI/KOSDAQ (must be 6 digits)
        db_items.append({
            "stock_code": code,
            "stock_name": h.get("name", ""),
            "shares": h.get("shares", 0),
            "ownership_pct": h.get("ownership_pct", 0.0),
            "price": h.get("price"),
            "market_value": h.get("market_value"),
            "change_pct": h.get("change_pct"),
        })

    await cache.save_nps_holdings(snap_date, db_items)
    logger.info("NPS: holdings saved (%d rows)", len(db_items))

    # 7. Generate HTML (use filtered db_items which have valid stock codes)
    generated_html = _build_html(
        snap_date=snap_date,
        holdings=db_items,
        total_value=total_value,
        nav=nav,
        today_pct=today_pct,
        mtd=mtd,
        ytd=ytd,
        nav_history=nav_history,
        value_history=value_history,
    )

    # 8. Save snapshot
    await cache.save_nps_snapshot(
        date=snap_date,
        total_value=total_value,
        nav=nav,
        count=len(holdings),
        html=generated_html,
    )
    logger.info(
        "NPS snapshot saved: date=%s total_value=%.0f nav=%.2f count=%d",
        snap_date, total_value, nav, len(holdings),
    )


if __name__ == "__main__":
    asyncio.run(run_nps_snapshot())
