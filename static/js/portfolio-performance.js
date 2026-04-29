// Portfolio performance tab shell, treemap, and performance data loading.
// Split from static/js/portfolio.js to keep portfolio features maintainable.
// --- Portfolio Performance Tab ---
let pfActiveTab = 'holdings';

function pfSwitchTab(tab) {
  pfActiveTab = tab;
  document.querySelectorAll('.pf-tab').forEach(btn => btn.classList.toggle('active', btn.dataset.tab === tab));
  const holdingsTab = document.getElementById('pfHoldingsTab');
  const performanceTab = document.getElementById('pfPerformanceTab');
  holdingsTab.style.display = tab === 'holdings' ? '' : 'none';
  performanceTab.style.display = tab === 'performance' ? '' : 'none';
  const activeEl = tab === 'holdings' ? holdingsTab : performanceTab;
  activeEl.classList.remove('fade-in');
  void activeEl.offsetWidth;
  activeEl.classList.add('fade-in');
  if (tab === 'performance') { loadPerformanceData(); _loadAiModels(); }
}

// 영역지도 팝업 — 기존에는 보유종목 탭 안에서 테이블/영역지도 토글 뷰로
// 존재했으나, 영역지도만 단독으로 보면 썰렁해 보이고 테이블과 전환하며
// 보는 니즈도 낮아 모달로 전환. ESC / backdrop / ✕ 모두 닫기 지원.
function pfOpenTreemap() {
  const modal = document.getElementById('pfTreemapModal');
  if (!modal) return;
  modal.style.display = 'flex';
  // ECharts 는 컨테이너가 화면에 보여진 뒤 init/resize 해야 크기를 맞게
  // 측정함. display 전환 → 레이아웃 확정 → 측정 순서 보장 위해 두 번째
  // rAF 에서 렌더 (첫 rAF 는 style flush, 두 번째에 실제 크기가 확정).
  requestAnimationFrame(() => {
    requestAnimationFrame(() => {
      if (!USE_UPLOT) renderTreemap();
    });
  });
  document.addEventListener('keydown', _pfTreemapEscHandler);
}

function pfCloseTreemap() {
  const modal = document.getElementById('pfTreemapModal');
  if (!modal) return;
  modal.style.display = 'none';
  document.removeEventListener('keydown', _pfTreemapEscHandler);
  // ECharts 인스턴스 해제 — 다음 open 에서 다시 그림. 메모리 관리 겸
  // 닫은 뒤 브라우저 창 리사이즈 때 hidden 컨테이너에 대고 resize 가
  // 호출되지 않도록. ResizeObserver 도 함께 해제.
  if (_treemapInstance) { _treemapInstance.dispose(); _treemapInstance = null; }
  if (_treemapResizeObserver) { _treemapResizeObserver.disconnect(); _treemapResizeObserver = null; }
}

function _pfTreemapEscHandler(e) {
  if (e.key === 'Escape') pfCloseTreemap();
}

async function loadPerformanceData() {
  const dateInput = document.getElementById('pfCfDate');
  if (dateInput && !dateInput.value) {
    const now = new Date();
    dateInput.value = `${now.getFullYear()}-${String(now.getMonth()+1).padStart(2,'0')}-${String(now.getDate()).padStart(2,'0')}`;
  }
  const showChartError = message => {
    ['pfNavChart', 'pfValueChart'].forEach(id => {
      const el = document.getElementById(id);
      if (!el) return;
      el.innerHTML = `<div style="display:flex;align-items:center;justify-content:center;height:100%;padding:16px;text-align:center;color:var(--text-secondary);font-size:14px;">${escapeHtml(message)}</div>`;
    });
  };
  try {
    const [navResp, cfResp] = await Promise.all([
      apiFetch('/api/portfolio/nav-history'),
      apiFetch('/api/portfolio/cashflows'),
    ]);
    if (!navResp.ok) {
      throw new Error(navResp.status === 401 ? '로그인이 필요합니다.' : `NAV 데이터를 불러오지 못했습니다. (${navResp.status})`);
    }
    const navData = navResp.ok ? await navResp.json() : [];
    const cfData = cfResp.ok ? await cfResp.json() : [];
    renderNavReturns(navData);
    await Promise.all([
      renderNavChart(navData),
      renderValueChart(navData),
    ]);
    renderCashflows(cfData, navData);
  } catch (e) {
    console.warn(e);
    const message = e?.message || '추이 그래프를 불러오지 못했습니다.';
    showChartError(message);
    if (typeof showToast === 'function') showToast(message, 'error');
  }
}

let _treemapInstance = null;
// 모달 open 시점에 flex 레이아웃이 아직 확정 안 돼 container height 가
// 0 에 가까운 상태로 echarts.init 이 불리면 treemap 이 상단 일부에만
// 그려지는 증상이 있음. ResizeObserver 로 container 크기 변화를 잡아
// 자동 ec.resize() 해주면 레이아웃 확정 순간 바로 교정된다.
let _treemapResizeObserver = null;

async function renderTreemap() {
  const container = document.getElementById('pfTreemap');
  if (!container) return;
  if (_treemapInstance) { _treemapInstance.dispose(); _treemapInstance = null; }
  if (_treemapResizeObserver) { _treemapResizeObserver.disconnect(); _treemapResizeObserver = null; }

  // ECharts required for treemap
  if (typeof echarts === 'undefined') {
    await new Promise((resolve, reject) => {
      const s = document.createElement('script');
      s.src = 'https://cdn.jsdelivr.net/npm/echarts@5.6.0/dist/echarts.min.js';
      s.onload = resolve;
      s.onerror = reject;
      document.head.appendChild(s);
    });
  }

  // Wait one frame for container layout
  await new Promise(r => requestAnimationFrame(r));

  if (!portfolioItems.length) {
    container.innerHTML = '<div style="display:flex;align-items:center;justify-content:center;height:100%;color:var(--text-secondary);font-size:14px;">포트폴리오가 비어 있습니다.</div>';
    return;
  }

  const isDark = document.documentElement.getAttribute('data-theme') === 'dark';

  // Build treemap data grouped by pfGetGroup (exclude negative qty)
  const groups = {};
  portfolioItems.forEach(item => {
    if (item.quantity <= 0) return;
    const gn = pfGetGroup(item);
    if (!groups[gn]) groups[gn] = [];
    const q = item.quote || {};
    const price = q.price ?? null;
    const qty = item.quantity;
    const mv = price !== null ? qty * price : qty * item.avg_price;
    if (mv <= 0) return;
    const changePct = q.change_pct ?? null;
    groups[gn].push({
      name: item.stock_name,
      value: mv,
      changePct,
      code: item.stock_code,
    });
  });

  // Map changePct to color: blue(-) → gray(0) → red(+)
  function _pctToColor(pct) {
    if (pct === null || pct === undefined) return isDark ? '#475569' : '#9ca3af';
    const clamped = Math.max(-5, Math.min(5, pct));
    const t = (clamped + 5) / 10; // 0~1
    let r, g, b;
    if (t < 0.5) {
      const s = t / 0.5;
      r = Math.round(37 + (148 - 37) * s);
      g = Math.round(99 + (163 - 99) * s);
      b = Math.round(235 + (184 - 235) * s);
    } else {
      const s = (t - 0.5) / 0.5;
      r = Math.round(148 + (220 - 148) * s);
      g = Math.round(163 + (38 - 163) * s);
      b = Math.round(184 + (38 - 184) * s);
    }
    return `rgb(${r},${g},${b})`;
  }

  // Compute grand total for weight %
  let grandTotal = 0;
  Object.values(groups).forEach(items => items.forEach(it => { grandTotal += it.value; }));

  // Muted color for group header based on changePct
  function _pctToHeaderColor(pct) {
    if (pct === null || pct === undefined) return isDark ? '#1e293b' : '#f3f4f6';
    const clamped = Math.max(-3, Math.min(3, pct));
    const abs = Math.abs(clamped) / 3; // 0~1
    if (clamped < 0) {
      // Blue tint
      return isDark
        ? `rgba(37,99,235,${0.08 + abs * 0.2})`
        : `rgba(37,99,235,${0.04 + abs * 0.12})`;
    } else {
      // Red tint
      return isDark
        ? `rgba(220,38,38,${0.08 + abs * 0.2})`
        : `rgba(220,38,38,${0.04 + abs * 0.12})`;
    }
  }

  function _pctToHeaderTextColor(pct) {
    if (pct === null || pct === undefined) return isDark ? '#94a3b8' : '#6b7280';
    if (pct < 0) return isDark ? '#93c5fd' : '#2563eb';
    if (pct > 0) return isDark ? '#fca5a5' : '#dc2626';
    return isDark ? '#94a3b8' : '#6b7280';
  }

  const treeData = Object.entries(groups).map(([gn, items]) => {
    // Group-level weighted daily change
    const grpTotal = items.reduce((s, it) => s + it.value, 0);
    let grpChangePct = null;
    const withPct = items.filter(it => it.changePct !== null);
    if (withPct.length > 0 && grpTotal > 0) {
      grpChangePct = withPct.reduce((s, it) => s + (it.changePct * it.value), 0) / grpTotal;
    }
    const grpWeight = grandTotal > 0 ? (grpTotal / grandTotal * 100) : 0;

    return {
      name: gn,
      changePct: grpChangePct,
      weight: grpWeight,
      itemStyle: { color: _pctToHeaderColor(grpChangePct), borderColor: 'transparent' },
      upperLabel: {
        color: _pctToHeaderTextColor(grpChangePct),
        backgroundColor: _pctToHeaderColor(grpChangePct),
      },
      children: items.map(it => ({
        name: it.name,
        value: it.value,
        changePct: it.changePct,
        weight: grandTotal > 0 ? (it.value / grandTotal * 100) : 0,
        code: it.code,
        itemStyle: { color: _pctToColor(it.changePct) },
      })),
    };
  });

  const ec = echarts.init(container);
  _treemapInstance = ec;

  // Container 가 flex 레이아웃 완료 전이라 처음 init 크기가 잘못
  // 잡혔더라도, ResizeObserver 가 실제 확정 크기를 감지해 즉시 resize.
  // 모달 open 시 '위쪽 반만 그려지는' 증상의 근본 대책.
  if (typeof ResizeObserver !== 'undefined') {
    const ro = new ResizeObserver(() => {
      if (_treemapInstance) _treemapInstance.resize();
    });
    ro.observe(container);
    _treemapResizeObserver = ro;
  }

  const _fmtPct = v => v !== null && v !== undefined ? (v > 0 ? '+' : '') + v.toFixed(2) + '%' : '-';

  ec.setOption({
    tooltip: {
      formatter(info) {
        const d = info.data;
        const cpStr = _fmtPct(d.changePct);
        const wStr = d.weight !== undefined ? d.weight.toFixed(1) + '%' : '';
        const val = Number(info.value).toLocaleString();
        return `<strong>${escapeHtml(info.name)}</strong><br/>평가: ${val}<br/>비중: ${wStr}<br/>일간: ${cpStr}`;
      },
    },
    series: [{
      type: 'treemap',
      left: 0, right: 0, top: 0, bottom: 0,
      roam: false,
      nodeClick: false,
      breadcrumb: { show: false },
      itemStyle: {
        borderColor: isDark ? '#334155' : '#e5e7eb',
        borderWidth: 1,
      },
      upperLabel: { show: false },
      levels: [
        {
          // Level 0: root — hide upperLabel
          upperLabel: { show: false },
          itemStyle: { borderWidth: 0 },
        },
        {
          // Level 1: group — header tinted by performance
          itemStyle: {
            borderColor: isDark ? '#475569' : '#d1d5db',
            borderWidth: 2,
          },
          upperLabel: {
            show: true,
            height: 22,
            fontSize: 11,
            fontWeight: 600,
            padding: [2, 8],
            formatter(params) {
              const d = params.data;
              const wStr = d.weight !== undefined ? ` (${d.weight.toFixed(1)}%)` : '';
              if (d.changePct === null || d.changePct === undefined) return params.name + wStr;
              return `${params.name}  ${_fmtPct(d.changePct)}${wStr}`;
            },
          },
        },
        {
          // Stock level
          itemStyle: {
            borderColor: isDark ? 'rgba(255,255,255,0.08)' : 'rgba(255,255,255,0.6)',
            borderWidth: 1,
          },
          label: {
            show: true,
            formatter(params) {
              const cp = params.data.changePct;
              const cpStr = cp !== null && cp !== undefined ? (cp > 0 ? '+' : '') + cp.toFixed(2) + '%' : '';
              return `{name|${params.name}}\n{pct|${cpStr}}`;
            },
            rich: {
              name: { fontSize: 11, fontWeight: 600, color: '#fff', lineHeight: 16 },
              pct: { fontSize: 10, color: 'rgba(255,255,255,0.8)', lineHeight: 14 },
            },
          },
        },
      ],
      data: treeData,
    }],
  });
}
