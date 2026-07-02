// Monthly/annual portfolio period report panel.
// The saved server artifact is a versioned JSON snapshot; this file only
// selects a period, triggers generation, and renders that stable payload.
let _pfPeriodReportOptions = { monthly: [], annual: [], defaults: {}, saved: [] };
let _pfPeriodReportType = 'monthly';
let _pfPeriodReportKey = '';
let _pfPeriodReportLoaded = false;
let _pfPeriodReportBusy = false;

function _pfPeriodReportStatus(text, state = 'idle') {
  const el = document.getElementById('pfPeriodReportStatus');
  if (!el) return;
  el.textContent = text;
  el.dataset.state = state;
}

function _pfPeriodReportContent(html) {
  const el = document.getElementById('pfPeriodReportContent');
  if (el) el.innerHTML = html;
}

function _pfPeriodTypeLabel(type) {
  return type === 'annual' ? '연간' : '월간';
}

function _pfPeriodOptionLabel(p) {
  const suffix = p.is_complete ? '' : ' · 진행 중';
  return `${p.key}${suffix}`;
}

function _pfSavedPeriod(type, key) {
  return (_pfPeriodReportOptions.saved || []).find(r => r.period_type === type && r.period_key === key) || null;
}

function _pfReportNum(v, digits = 2) {
  const n = Number(v);
  if (!Number.isFinite(n)) return '-';
  return n.toLocaleString(undefined, { maximumFractionDigits: digits });
}

function _pfReportKrw(v) {
  return typeof fmtKrw === 'function' ? fmtKrw(v) : _pfReportNum(v, 0);
}

function _pfReportPct(v, signed = true) {
  return typeof fmtPct === 'function' ? fmtPct(v, signed) : _pfReportNum(v, 2) + '%';
}

function _pfReportQty(v) {
  const n = Number(v);
  if (!Number.isFinite(n)) return '-';
  return n.toLocaleString(undefined, { maximumFractionDigits: 6 });
}

function _pfCompositionActivityLabel(activity) {
  const labels = {
    new_position: '신규 매수',
    closed_position: '전량 매도',
    increased_position: '추가 매수',
    reduced_position: '부분 매도',
    futures_short: '선물 매도',
    unchanged_position: '수량 유지',
    value_only_increase: '평가액 증가',
    value_only_decrease: '평가액 감소',
  };
  return labels[activity] || activity || '-';
}

function _pfCompositionConfidenceLabel(confidence) {
  const labels = {
    quantity_delta: '수량 기준',
    position_boundary: '편입/제거 기준',
    value_only: '평가액 기준',
  };
  return labels[confidence] || confidence || '-';
}

function _pfReportCard(label, value, sub = '', cls = '') {
  return `<div class="pf-nav-ret-card">
    <div class="pf-nav-ret-label">${escapeHtml(label)}</div>
    <div class="pf-nav-ret-value ${cls}">${escapeHtml(value)}</div>
    ${sub ? `<div class="pf-risk-sub">${escapeHtml(sub)}</div>` : ''}
  </div>`;
}

function _pfRenderPeriodSelects() {
  const typeSel = document.getElementById('pfPeriodReportType');
  const keySel = document.getElementById('pfPeriodReportKey');
  if (!typeSel || !keySel) return;
  typeSel.value = _pfPeriodReportType;
  const periods = _pfPeriodReportOptions[_pfPeriodReportType] || [];
  if (!_pfPeriodReportKey && periods.length) {
    _pfPeriodReportKey = _pfPeriodReportOptions.defaults?.[_pfPeriodReportType] || periods[0].key;
  }
  if (!periods.some(p => p.key === _pfPeriodReportKey) && periods.length) {
    _pfPeriodReportKey = periods[0].key;
  }
  keySel.innerHTML = periods.map(p =>
    `<option value="${escapeHtml(p.key)}"${p.key === _pfPeriodReportKey ? ' selected' : ''}>${escapeHtml(_pfPeriodOptionLabel(p))}</option>`
  ).join('');
  keySel.disabled = !periods.length;
}

function _pfPeriodReportEmpty() {
  const label = `${_pfPeriodTypeLabel(_pfPeriodReportType)} ${_pfPeriodReportKey || ''}`.trim();
  _pfPeriodReportContent(`<div class="pf-risk-empty">${escapeHtml(label)} 보고서가 아직 저장되지 않았습니다. 생성/갱신을 눌러 데이터 스냅샷을 쌓으세요.</div>`);
}

function _pfRenderReportTable(rows, columns) {
  if (!Array.isArray(rows) || !rows.length) return '<div class="pf-risk-empty">표시할 행이 없습니다.</div>';
  return `<div class="pf-period-report-table-wrap"><table class="pf-period-report-table">
    <thead><tr>${columns.map(c => `<th>${escapeHtml(c.label)}</th>`).join('')}</tr></thead>
    <tbody>${rows.map(row => `<tr>${columns.map(c => `<td>${c.render(row)}</td>`).join('')}</tr>`).join('')}</tbody>
  </table></div>`;
}

function _pfRenderPeriodReport(saved) {
  const report = saved?.report || {};
  const period = report.period || {};
  const summary = report.summary || {};
  const cash = report.cashflows || {};
  const composition = report.composition_changes || {};
  const compSummary = composition.summary || {};
  const risk = report.risk || {};
  const allocation = report.allocation || {};
  const holdings = report.holdings || {};
  const changes = holdings.changes || {};
  const counts = changes.counts || {};
  const quality = report.data_quality || {};
  const notes = report.review_notes || [];
  const navCls = typeof returnClass === 'function' ? returnClass(summary.nav_return_pct) : '';
  const valueCls = typeof returnClass === 'function' ? returnClass(summary.value_change_pct) : '';
  const warnHtml = (quality.warnings || []).length
    ? `<div class="pf-period-warnings">${quality.warnings.map(w => `<span>${escapeHtml(w)}</span>`).join('')}</div>`
    : '';
  const cards = [
    _pfReportCard('NAV 수익률', _pfReportPct(summary.nav_return_pct), `${summary.baseline_date || '-'} → ${summary.ending_date || '-'}`, navCls),
    _pfReportCard('평가금액 변화', _pfReportKrw(summary.value_change), `${_pfReportKrw(summary.starting_value)} → ${_pfReportKrw(summary.ending_value)}`, valueCls),
    _pfReportCard('순입출금', _pfReportKrw(cash.net_cashflow), `입금 ${_pfReportKrw(cash.total_deposit)} · 출금 ${_pfReportKrw(cash.total_withdrawal)}`),
    _pfReportCard('순 구성 변화', _pfReportKrw(compSummary.net_trade_value_estimate), `매수/증가 ${compSummary.buy_like_count || 0} · 매도/축소 ${compSummary.sell_like_count || 0}`),
    _pfReportCard('종목 변동', `${(counts.added || 0) + (counts.removed || 0) + (counts.increased || 0) + (counts.decreased || 0)}개`, `추가 ${counts.added || 0} · 제거 ${counts.removed || 0} · 증가 ${counts.increased || 0} · 감소 ${counts.decreased || 0}`),
    _pfReportCard('MDD', _pfReportPct(risk.max_drawdown_pct, false), risk.max_drawdown_trough_date || ''),
    _pfReportCard('집중도 Top5', _pfReportPct(allocation.concentration?.end?.top5_weight_pct, false), `HHI ${_pfReportNum(allocation.concentration?.end?.hhi, 4)}`),
  ];
  const buyRows = (composition.top_buys || []).slice(0, 8);
  const sellRows = (composition.top_sells || []).slice(0, 8);
  const incRows = (changes.top_increases || []).slice(0, 6);
  const decRows = (changes.top_decreases || []).slice(0, 6);
  const groupRows = (allocation.groups || []).slice(0, 8);
  const compositionCols = [
    { label: '종목', render: r => `${escapeHtml(r.stock_name || r.stock_code)}<div class="pf-risk-sub">${escapeHtml(r.stock_code || '')}</div>` },
    { label: '구분', render: r => `${escapeHtml(_pfCompositionActivityLabel(r.activity))}<div class="pf-risk-sub">${escapeHtml(_pfCompositionConfidenceLabel(r.confidence))}</div>` },
    { label: '수량 변화', render: r => escapeHtml(_pfReportQty(r.quantity_change)) },
    { label: '거래 추정', render: r => `<span class="${returnClass(r.trade_value_estimate)}">${escapeHtml(_pfReportKrw(r.trade_value_estimate))}</span>` },
    { label: '비중', render: r => `${escapeHtml(_pfReportPct(r.start_weight_pct, false))} → ${escapeHtml(_pfReportPct(r.end_weight_pct, false))}<div class="pf-risk-sub">${escapeHtml(_pfReportPct(r.weight_change_ppt, true))}p</div>` },
  ];
  const holdingCols = [
    { label: '종목', render: r => `${escapeHtml(r.stock_name || r.stock_code)}<div class="pf-risk-sub">${escapeHtml(r.stock_code || '')}</div>` },
    { label: '상태', render: r => escapeHtml(r.status || '') },
    { label: '변화', render: r => `<span class="${returnClass(r.change_value)}">${escapeHtml(_pfReportKrw(r.change_value))}</span>` },
    { label: '종료 비중', render: r => escapeHtml(_pfReportPct(r.end_weight_pct, false)) },
  ];
  const groupCols = [
    { label: '그룹', render: r => escapeHtml(r.group_name || '') },
    { label: '비중', render: r => `${escapeHtml(_pfReportPct(r.start_weight_pct, false))} → ${escapeHtml(_pfReportPct(r.end_weight_pct, false))}` },
    { label: '변화', render: r => `<span class="${returnClass(r.weight_change_ppt)}">${escapeHtml(_pfReportPct(r.weight_change_ppt, true))}p</span>` },
    { label: '평가액 변화', render: r => escapeHtml(_pfReportKrw(r.change_value)) },
  ];
  const mdBtn = saved.report_md
    ? `<button class="pf-mini-btn" type="button" onclick="pfDownloadPeriodReportMarkdown()">Markdown</button>`
    : '';
  _pfPeriodReportContent(`
    <div class="pf-period-report-meta">
      <span>${escapeHtml(period.label || '기간 보고서')}</span>
      <span>${escapeHtml(period.start_date || '')} ~ ${escapeHtml(period.end_date || '')}</span>
      <span>schema v${escapeHtml(report.schema_version || saved.schema_version || 1)}</span>
      <span title="${escapeHtml(saved.source_hash || '')}">hash ${escapeHtml(String(saved.source_hash || '').slice(0, 8))}</span>
      ${mdBtn}
    </div>
    ${warnHtml}
    <div class="pf-risk-grid pf-period-report-grid">${cards.join('')}</div>
    <div class="pf-period-report-sections">
      <section>
        <h4>매수·편입 구성 변화</h4>
        ${_pfRenderReportTable(buyRows, compositionCols)}
      </section>
      <section>
        <h4>매도·축소 구성 변화</h4>
        ${_pfRenderReportTable(sellRows, compositionCols)}
      </section>
      <section>
        <h4>평가액 증가 상위</h4>
        ${_pfRenderReportTable(incRows, holdingCols)}
      </section>
      <section>
        <h4>평가액 감소 상위</h4>
        ${_pfRenderReportTable(decRows, holdingCols)}
      </section>
      <section>
        <h4>그룹 비중 변화</h4>
        ${_pfRenderReportTable(groupRows, groupCols)}
      </section>
      <section>
        <h4>검토 메모</h4>
        <div class="pf-period-notes">${notes.length ? notes.map(n => `<div class="pf-period-note ${escapeHtml(n.level || '')}"><strong>${escapeHtml(n.category || '')}</strong><span>${escapeHtml(n.message || '')}</span></div>`).join('') : '<div class="pf-risk-empty">자동 검토 메모가 없습니다.</div>'}</div>
      </section>
    </div>
  `);
  window._pfCurrentPeriodReportMarkdown = saved.report_md || '';
}

async function _pfLoadSavedPeriodReport() {
  if (!_pfPeriodReportKey) {
    _pfPeriodReportEmpty();
    return;
  }
  const saved = _pfSavedPeriod(_pfPeriodReportType, _pfPeriodReportKey);
  if (!saved) {
    _pfPeriodReportEmpty();
    _pfPeriodReportStatus('저장된 보고서 없음', 'idle');
    return;
  }
  _pfPeriodReportStatus('저장된 보고서를 불러오는 중입니다...', 'loading');
  try {
    const data = await apiFetchJson(`/api/portfolio/period-reports/${encodeURIComponent(_pfPeriodReportType)}/${encodeURIComponent(_pfPeriodReportKey)}`, {
      errorMessage: '저장된 보고서를 불러오지 못했습니다.',
    });
    _pfRenderPeriodReport(data);
    _pfPeriodReportStatus(`마지막 생성 ${data.generated_at || data.updated_at || ''}`, 'done');
  } catch (e) {
    reportApiError(e, '기간 투자 보고서', { silent: true });
    _pfPeriodReportEmpty();
    _pfPeriodReportStatus('저장된 보고서를 불러오지 못했습니다.', 'error');
  }
}

async function pfLoadPeriodReportsPanel({ force = false } = {}) {
  if (_pfPeriodReportLoaded && !force) return;
  _pfPeriodReportLoaded = true;
  _pfPeriodReportStatus('기간 목록을 불러오는 중입니다...', 'loading');
  try {
    _pfPeriodReportOptions = await apiFetchJson('/api/portfolio/period-reports/periods', {
      errorMessage: '기간 목록을 불러오지 못했습니다.',
    });
    if (!_pfPeriodReportKey) {
      _pfPeriodReportKey = _pfPeriodReportOptions.defaults?.[_pfPeriodReportType] || '';
    }
    _pfRenderPeriodSelects();
    await _pfLoadSavedPeriodReport();
  } catch (e) {
    if (e?.status === 401) {
      _pfPeriodReportStatus('로그인 후 이용할 수 있습니다.', 'error');
      _pfPeriodReportContent('<div class="pf-risk-empty">로그인 후 기간 보고서를 생성할 수 있습니다.</div>');
      return;
    }
    reportApiError(e, '기간 투자 보고서', { silent: true });
    _pfPeriodReportStatus('기간 목록을 불러오지 못했습니다.', 'error');
  }
}

async function pfPeriodReportTypeChanged(type) {
  _pfPeriodReportType = type === 'annual' ? 'annual' : 'monthly';
  _pfPeriodReportKey = _pfPeriodReportOptions.defaults?.[_pfPeriodReportType] || '';
  _pfRenderPeriodSelects();
  await _pfLoadSavedPeriodReport();
}

async function pfPeriodReportKeyChanged(key) {
  _pfPeriodReportKey = String(key || '');
  await _pfLoadSavedPeriodReport();
}

async function pfGeneratePeriodReport() {
  if (_pfPeriodReportBusy || !_pfPeriodReportKey) return;
  _pfPeriodReportBusy = true;
  const btn = document.getElementById('pfPeriodReportGenerateBtn');
  if (btn) {
    btn.disabled = true;
    btn.textContent = '생성 중...';
  }
  _pfPeriodReportStatus('보고서 데이터를 생성하고 저장하는 중입니다...', 'loading');
  try {
    const data = await apiFetchJson('/api/portfolio/period-reports/generate', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ period_type: _pfPeriodReportType, period_key: _pfPeriodReportKey }),
      timeoutMs: 60000,
      errorMessage: '보고서 생성에 실패했습니다.',
    });
    _pfRenderPeriodReport(data);
    _pfPeriodReportStatus(`저장 완료 ${data.generated_at || data.updated_at || ''}`, 'done');
    if (typeof showToast === 'function') showToast('기간 투자 보고서를 저장했습니다.', 'success');
    await pfLoadPeriodReportsPanel({ force: true });
  } catch (e) {
    reportApiError(e, '기간 투자 보고서 생성');
    _pfPeriodReportStatus('보고서 생성 실패', 'error');
  } finally {
    _pfPeriodReportBusy = false;
    if (btn) {
      btn.disabled = false;
      btn.textContent = '생성/갱신';
    }
  }
}

function pfDownloadPeriodReportMarkdown() {
  const text = window._pfCurrentPeriodReportMarkdown || '';
  if (!text) return;
  const blob = new Blob([text], { type: 'text/markdown;charset=utf-8' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = `portfolio-${_pfPeriodReportType}-${_pfPeriodReportKey}.md`;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}
