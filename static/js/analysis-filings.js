// Analysis filings: DART filing AI review, broker reports table, and
// per-stock LLM wiki summaries rendering.
// Split from static/js/analysis.js to keep analysis features maintainable.

// DART filing AI review
let _filingReviewLoadId = 0;
let _filingReviewStockCode = '';
let _filingReviewActionBusy = false;
let _filingReviewButtonState = { status: '', canGenerate: false };
const FILING_REVIEW_STATUS_TIMEOUT_MS = 60000;
const FILING_REVIEW_GENERATE_TIMEOUT_MS = 10 * 60 * 1000;

function _isAdminUser() {
  return !!(typeof currentUser !== 'undefined' && currentUser && currentUser.is_admin);
}

function _setFilingReviewAdminAction(stockCode, state = {}) {
  const btn = document.getElementById('filingReviewGenerateBtn');
  if (!btn) return;
  _filingReviewStockCode = stockCode || _filingReviewStockCode || activeStockCode || '';
  _filingReviewButtonState = {
    status: state.status || _filingReviewButtonState.status || '',
    canGenerate: state.canGenerate !== undefined ? !!state.canGenerate : !!_filingReviewButtonState.canGenerate,
  };

  if (!_isAdminUser() || !_filingReviewStockCode) {
    btn.style.display = 'none';
    btn.onclick = null;
    return;
  }

  const status = _filingReviewButtonState.status;
  const canGenerate = _filingReviewButtonState.canGenerate && status !== 'loading';
  btn.style.display = 'inline-flex';
  btn.disabled = _filingReviewActionBusy || !canGenerate;
  btn.textContent = _filingReviewActionBusy
    ? 'AI 리뷰 생성 중...'
    : (status === 'ready' ? 'AI 리뷰 재생성' : 'AI 리뷰 생성');
  btn.title = canGenerate
    ? '관리자 권한으로 최신 DART 정기보고서 AI 리뷰를 생성합니다.'
    : '생성 가능한 DART 정기보고서가 확인되면 활성화됩니다.';
  btn.onclick = () => generateFilingReview(_filingReviewStockCode, { force: status === 'ready' });
}

function _filingReviewToneClass(tone) {
  const value = String(tone || 'neutral').toLowerCase();
  if (['good', 'watch', 'bad'].includes(value)) return value;
  return 'neutral';
}

// 구버전 캐시 리뷰는 summary_md 안에 유니코드 블록 막대(█▇▆…, U+2580–U+259F)가
// 박혀 있어 폰트에서 깨져 보인다. 마크다운 렌더 전에 그 문자들을 제거해 텍스트만
// 남긴다(숫자/설명은 보존). 신규 리뷰는 metric_trends 차트로 대체된다.
function _stripBlockBars(md) {
  return String(md || '').replace(/[▀-▟]+/g, '').replace(/[ \t]{2,}/g, ' ');
}

function _mtFmt(value, unit) {
  if (value === null || value === undefined || !isFinite(Number(value))) return '-';
  return Number(value).toLocaleString('ko-KR', { maximumFractionDigits: 2 }) + (unit || '');
}

// metric_trends → before/after 비교 가로 막대 차트. 같은 지표의 두 값을
// max 기준으로 정규화해 길이를 잡는다(음수는 0폭, 절댓값 기준).
function _renderMetricTrends(trends) {
  if (!Array.isArray(trends) || !trends.length) return '';
  const items = trends.map((t) => {
    const unit = t.unit || '';
    const bv = t.before && t.before.value;
    const av = t.after && t.after.value;
    const nb = Number(bv);
    const na = Number(av);
    const scale = Math.max(Math.abs(isFinite(nb) ? nb : 0), Math.abs(isFinite(na) ? na : 0)) || 1;
    const width = (v) => (isFinite(Number(v)) ? Math.max(2, Math.min(100, (Math.abs(Number(v)) / scale) * 100)) : 0);
    const bar = (cls, whenLabel, v) => (
      '<div class="mt-bar-row">'
      + `<span class="mt-when">${escapeHtml(String(whenLabel || ''))}</span>`
      + `<span class="mt-track"><span class="mt-fill ${cls}" style="width:${width(v)}%"></span></span>`
      + `<span class="mt-val">${escapeHtml(_mtFmt(v, unit))}</span></div>`
    );
    return '<div class="mt-item"><div class="mt-head">'
      + `<span class="mt-label">${escapeHtml(String(t.label || ''))}</span>`
      + (t.note ? `<span class="mt-note">${escapeHtml(String(t.note))}</span>` : '')
      + '</div>'
      + bar('mt-before', t.before && t.before.label, bv)
      + bar('mt-after', t.after && t.after.label, av)
      + '</div>';
  }).join('');
  return `<div class="mt-wrap"><div class="mt-title">핵심 지표 변화</div>${items}</div>`;
}

function _renderFilingReviewCards(cards) {
  const container = document.getElementById('filingReviewCards');
  if (!container) return;
  const safeCards = Array.isArray(cards) ? cards.slice(0, 8) : [];
  container.innerHTML = safeCards.map(card => {
    const tone = _filingReviewToneClass(card.tone);
    return `<div class="filing-review-card ${tone}">
      <span>${escapeHtml(card.label || '체크')}</span>
      <strong>${escapeHtml(card.value || '-')}</strong>
      <p>${escapeHtml(card.detail || '')}</p>
    </div>`;
  }).join('');
}

function _renderFilingReviewDetails(review) {
  const details = document.getElementById('filingReviewDetails');
  const body = document.getElementById('filingReviewDetailsBody');
  if (!details || !body) return;
  const comparisons = review.comparison_reports || [];
  const comparisonRows = comparisons.length
    ? comparisons.map(item => {
      const href = safeExternalUrl(item.viewer_url || '');
      const link = href
        ? `<a href="${href}" target="_blank" rel="noopener noreferrer">${escapeHtml(item.report_name || item.rcept_no || '')}</a>`
        : escapeHtml(item.report_name || item.rcept_no || '');
      const suffix = item.error ? ` · ${escapeHtml(item.error)}` : '';
      return `<li>${link} <span>${escapeHtml(item.report_date || '')}${suffix}</span></li>`;
    }).join('')
    : '<li>비교 보고서 없음</li>';
  const model = review.model ? `모델 ${escapeHtml(review.model)}` : '';
  const tokenText = review.tokens_in || review.tokens_out
    ? `입력 ${Number(review.tokens_in || 0).toLocaleString()} / 출력 ${Number(review.tokens_out || 0).toLocaleString()} 토큰`
    : '';
  const sourceLimits = review.review?.source_limits
    ? `<p>${escapeHtml(review.review.source_limits)}</p>`
    : '';
  body.innerHTML = `
    <ul>${comparisonRows}</ul>
    ${sourceLimits}
    <div class="filing-review-runtime">${[model, tokenText, review.created_at ? `생성 ${new Date(review.created_at).toLocaleString('ko-KR')}` : ''].filter(Boolean).join(' · ')}</div>
  `;
  details.style.display = 'block';
}

function renderFilingReview(review, { cached = false } = {}) {
  const section = document.getElementById('filingReviewSection');
  const status = document.getElementById('filingReviewStatus');
  const meta = document.getElementById('filingReviewMeta');
  const body = document.getElementById('filingReviewBody');
  const source = document.getElementById('filingReviewSource');
  if (!section || !status || !body) return;

  section.style.display = 'block';
  _setFilingReviewAdminAction(review.stock_code || _filingReviewStockCode, { status: 'ready', canGenerate: true });
  const title = review.report_name || '최근 정기보고서';
  const date = review.report_date || '';
  if (meta) meta.textContent = `${title}${date ? ` · ${date}` : ''}${cached ? ' · 캐시' : ''}`;
  if (status) status.textContent = '';
  if (source) {
    const href = safeExternalUrl(review.review?.viewer_url || review.viewer_url || `https://dart.fss.or.kr/dsaf001/main.do?rcpNo=${review.rcept_no || ''}`);
    source.href = href || '#';
    source.style.display = href ? 'inline-flex' : 'none';
  }
  _renderFilingReviewCards(review.review?.cards || []);
  const md = review.review_md || review.review?.summary_md || '';
  const cleanMd = _stripBlockBars(md);
  body.innerHTML = cleanMd
    ? (typeof _renderSafeMarkdown === 'function' ? _renderSafeMarkdown(cleanMd) : escapeHtml(cleanMd))
    : '<p>생성된 리뷰 본문이 없습니다.</p>';
  const trendsEl = document.getElementById('filingReviewTrends');
  if (trendsEl) trendsEl.innerHTML = _renderMetricTrends(review.review?.metric_trends || review.metric_trends || []);
  _renderFilingReviewDetails(review);
}

function renderFilingReviewMissing(data) {
  const section = document.getElementById('filingReviewSection');
  const status = document.getElementById('filingReviewStatus');
  const meta = document.getElementById('filingReviewMeta');
  const body = document.getElementById('filingReviewBody');
  const cards = document.getElementById('filingReviewCards');
  const source = document.getElementById('filingReviewSource');
  const details = document.getElementById('filingReviewDetails');
  if (!section || !status || !body) return;

  section.style.display = 'block';
  _setFilingReviewAdminAction(data.stock_code || _filingReviewStockCode, {
    status: data.status || 'missing',
    canGenerate: data.can_generate !== false && data.status !== 'no_report',
  });
  const report = data.latest_report || {};
  if (meta) meta.textContent = report.report_name ? `${report.report_name}${report.report_date ? ` · ${report.report_date}` : ''}` : '';
  if (source) {
    const href = safeExternalUrl(report.viewer_url || '');
    source.href = href || '#';
    source.style.display = href ? 'inline-flex' : 'none';
  }
  if (cards) cards.innerHTML = '';
  { const t = document.getElementById('filingReviewTrends'); if (t) t.innerHTML = ''; }
  if (details) details.style.display = 'none';
  if (data.status === 'no_report') {
    status.textContent = '최근 DART 정기보고서를 찾지 못했습니다.';
    body.innerHTML = '';
    return;
  }
  status.textContent = '최근 공시 AI 리뷰는 백그라운드에서 자동 생성됩니다. 아직 준비되지 않았다면 잠시 후 다시 확인해 주세요.';
  body.innerHTML = '';
}

async function loadFilingReview(stockCode) {
  const section = document.getElementById('filingReviewSection');
  const status = document.getElementById('filingReviewStatus');
  const body = document.getElementById('filingReviewBody');
  const cards = document.getElementById('filingReviewCards');
  const details = document.getElementById('filingReviewDetails');
  const loadId = ++_filingReviewLoadId;
  if (!section || !status) return;

  _filingReviewStockCode = stockCode;
  _setFilingReviewAdminAction(stockCode, { status: 'loading', canGenerate: false });
  section.style.display = 'block';
  status.textContent = '최근 DART 공시를 확인하는 중...';
  if (body) body.innerHTML = '';
  if (cards) cards.innerHTML = '';
  { const t = document.getElementById('filingReviewTrends'); if (t) t.innerHTML = ''; }
  if (details) details.style.display = 'none';

  if (!hasApiConfiguration()) {
    status.textContent = 'GitHub Pages에서는 API 서버 연결 후 DART 리뷰를 불러올 수 있습니다.';
    return;
  }

  try {
    const resp = await apiFetch(`/api/analysis/${encodeURIComponent(stockCode)}/filing-review`, {
      timeoutMs: FILING_REVIEW_STATUS_TIMEOUT_MS,
    });
    if (loadId !== _filingReviewLoadId) return;
    const data = await resp.json().catch(() => ({}));
    if (!resp.ok) throw new Error(data.detail || `HTTP ${resp.status}`);
    if (loadId !== _filingReviewLoadId) return;
    if (data.status === 'ready' && data.review) {
      renderFilingReview(data.review, { cached: true });
    } else {
      renderFilingReviewMissing(data);
    }
  } catch (err) {
    if (loadId !== _filingReviewLoadId) return;
    status.textContent = 'DART 리뷰 상태를 불러오지 못했습니다: ' + (err.message || err);
    _setFilingReviewAdminAction(stockCode, { status: 'error', canGenerate: true });
  }
}

async function generateFilingReview(stockCode, { force = true } = {}) {
  if (!_isAdminUser() || _filingReviewActionBusy) return;
  const code = stockCode || _filingReviewStockCode || activeStockCode;
  if (!code) return;

  const status = document.getElementById('filingReviewStatus');
  _filingReviewActionBusy = true;
  _setFilingReviewAdminAction(code, _filingReviewButtonState);
  if (status) {
    status.textContent = force
      ? 'DART AI 리뷰를 재생성하는 중입니다. 원문 수집과 LLM 호출 때문에 시간이 걸릴 수 있습니다...'
      : 'DART AI 리뷰를 생성하는 중입니다. 원문 수집과 LLM 호출 때문에 시간이 걸릴 수 있습니다...';
  }

  try {
    const resp = await apiFetch(`/api/analysis/${encodeURIComponent(code)}/filing-review`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ force }),
      timeoutMs: FILING_REVIEW_GENERATE_TIMEOUT_MS,
    });
    const data = await resp.json().catch(() => ({}));
    if (!resp.ok) throw new Error(data.detail || `HTTP ${resp.status}`);
    if (data.status === 'ready' && data.review) {
      renderFilingReview(data.review, { cached: !data.generated });
      showToast(data.generated ? '공시 AI 리뷰를 생성했습니다.' : '캐시된 공시 AI 리뷰를 불러왔습니다.', 'success');
    } else {
      renderFilingReviewMissing(data);
      showToast(data.message || '공시 AI 리뷰 상태를 확인했습니다.');
    }
  } catch (err) {
    if (status) status.textContent = 'DART AI 리뷰 생성에 실패했습니다: ' + (err.message || err);
    showToast(err.message || 'DART AI 리뷰 생성에 실패했습니다.');
  } finally {
    _filingReviewActionBusy = false;
    _setFilingReviewAdminAction(code, _filingReviewButtonState);
  }
}

// Reports
let allReports = [];
let reportDisplayCount = 20;
let reportsRequestId = 0;

function getRecommBadge(recomm) {
  if (!recomm) return '';
  const r = recomm.toUpperCase();
  if (r.includes('BUY') || r.includes('매수') || r.includes('강력')) return `<span class="badge badge-buy">${recomm}</span>`;
  if (r.includes('SELL') || r.includes('매도') || r.includes('비중축소')) return `<span class="badge badge-sell">${recomm}</span>`;
  return `<span class="badge badge-hold">${recomm}</span>`;
}

function renderReportsTable(reports, limit) {
  const tbody = document.getElementById('reportsBody');
  const slice = reports.slice(0, limit);
  tbody.innerHTML = slice.map((r, idx) => {
    const safeTitle = escapeHtml(r.title);
    const safeSummary = r.summary ? `<div class="report-summary">${escapeHtml(r.summary)}</div>` : '';
    const safeAnalyst = escapeHtml(r.analyst);
    const safeFirm = escapeHtml(r.firm_short || r.firm);
    const safeDate = escapeHtml(r.date);
    const safeHref = safeExternalUrl(buildReportPdfUrl(r.pdf_url) || r.source_url);
    const titleLink = safeHref
      ? `<a href="${safeHref}" target="_blank" rel="noopener noreferrer">${safeTitle}</a>`
      : safeTitle;
    const safeReadHref = safeExternalUrl(r.source_url);
    // If we have an LLM summary for this exact PDF, offer a toggle just
    // before the 본문 페이지 link. Match is by pdf_url so the frontend
    // doesn't need to know about sha1.
    const wikiEntry = r.pdf_url ? _wikiByPdfUrl.get(r.pdf_url) : null;
    const summaryToggle = wikiEntry
      ? `<a href="#" class="report-summary-toggle" data-report-idx="${idx}">요약 내용 보기</a>`
      : '';
    const linkParts = [];
    if (summaryToggle) linkParts.push(summaryToggle);
    if (safeReadHref) linkParts.push(`<a href="${safeReadHref}" target="_blank" rel="noopener noreferrer">네이버 리서치 본문 페이지</a>`);
    const linkNote = linkParts.length
      ? `<div class="report-link-note">${linkParts.join('<span class="report-link-sep"> · </span>')}</div>`
      : '';
    const targetPrc = r.target_price ? Number(r.target_price.replace(/,/g, '')).toLocaleString() + '원' : '-';
    const mainRow = `<tr class="report-row" data-report-idx="${idx}">
      <td class="report-date">${safeDate}</td>
      <td class="report-firm">${safeFirm}</td>
      <td class="report-title">${titleLink}${safeSummary}${linkNote}<div style="font-size:11px;color:var(--text-secondary);margin-top:2px;">${safeAnalyst}</div></td>
      <td>${getRecommBadge(escapeHtml(r.recommendation))}</td>
      <td class="report-target">${escapeHtml(targetPrc)}</td>
    </tr>`;
    // Hidden companion row — populated on first click. Span all 5 cols.
    const summaryRow = wikiEntry
      ? `<tr class="report-summary-row" data-report-idx="${idx}" style="display:none;"><td colspan="5"><div class="report-summary-body" data-report-idx="${idx}"></div></td></tr>`
      : '';
    return mainRow + summaryRow;
  }).join('');

  // Wire up the toggles (event delegation would also work; direct binding
  // here is scoped to the rows we just rendered).
  tbody.querySelectorAll('.report-summary-toggle').forEach(a => {
    a.addEventListener('click', (e) => {
      e.preventDefault();
      const idx = a.dataset.reportIdx;
      const row = tbody.querySelector(`tr.report-summary-row[data-report-idx="${idx}"]`);
      if (!row) return;
      const body = row.querySelector('.report-summary-body');
      const report = slice[Number(idx)];
      const entry = report && report.pdf_url ? _wikiByPdfUrl.get(report.pdf_url) : null;
      if (!entry) return;
      const visible = row.style.display !== 'none';
      if (visible) {
        row.style.display = 'none';
        a.textContent = '요약 내용 보기';
      } else {
        // Render markdown lazily on first open; re-renders are cheap
        // but skipping avoids an allocation spike on bulk expand.
        if (!body.dataset.rendered) {
          const md = entry.summary_md || '';
          body.innerHTML = typeof _renderSafeMarkdown === 'function'
            ? _renderSafeMarkdown(md)
            : escapeHtml(md);
          body.dataset.rendered = '1';
        }
        row.style.display = '';
        a.textContent = '요약 접기';
      }
    });
  });

  const moreBtn = document.getElementById('reportsMore');
  moreBtn.style.display = reports.length > limit ? 'block' : 'none';
  moreBtn.textContent = `더 보기 (${limit}/${reports.length}건)`;
}

function showMoreReports() {
  reportDisplayCount += 20;
  renderReportsTable(allReports, reportDisplayCount);
}

async function loadReports(stockCode) {
  const section = document.getElementById('reportsSection');
  const loading = document.getElementById('reportsLoading');
  const table = document.getElementById('reportsTable');
  const countEl = document.getElementById('reportCount');
  const requestId = ++reportsRequestId;

  section.style.display = 'block';
  loading.style.display = 'block';
  loading.textContent = '최신 리포트를 불러오는 중...';
  table.style.display = 'none';
  countEl.textContent = '';
  allReports = [];
  reportDisplayCount = 20;

  if (!hasApiConfiguration()) {
    loading.style.display = 'block';
    loading.textContent = 'GitHub Pages에서는 API 서버 연결 후 리포트를 불러올 수 있습니다.';
    return;
  }

  const localCache = loadReportCache(stockCode);
  let latestReport = localCache?.latestReport || null;
  let renderedFromCache = false;

  if (Array.isArray(localCache?.reports) && localCache.reports.length > 0) {
    allReports = localCache.reports;
    countEl.textContent = `(최근 3년, ${allReports.length}건 · 캐시)`;
    table.style.display = 'table';
    loading.style.display = 'none';
    renderReportsTable(allReports, reportDisplayCount);
    _overlayTargetPrices(allReports);
    renderedFromCache = true;
  } else if (latestReport) {
    allReports = [latestReport];
    countEl.textContent = '(최신 1건 · 캐시)';
    table.style.display = 'table';
    loading.style.display = 'none';
    renderReportsTable(allReports, 1);
    renderedFromCache = true;
  }

  try {
    const latestResp = await apiFetch(`/api/reports/${stockCode}/latest`);
    const latestData = await latestResp.json();
    if (requestId !== reportsRequestId) return;
    const networkLatest = latestData.report || null;
    const latestChanged = latestData.changed ?? !sameReport(latestReport, networkLatest);

    if (networkLatest) {
      latestReport = networkLatest;
      saveReportCache(stockCode, {
        latestReport,
        reports: Array.isArray(localCache?.reports) ? localCache.reports : [],
      });
    }

    if (networkLatest && (!renderedFromCache || latestChanged)) {
      allReports = [latestReport];
      loading.style.display = 'none';
      countEl.textContent = latestChanged ? '(최신 1건 갱신됨)' : '(최신 1건 확인됨)';
      table.style.display = 'table';
      renderReportsTable(allReports, 1);
    }
  } catch (e) { console.warn(e); }

  try {
    const shouldRefreshFullReports =
      !Array.isArray(localCache?.reports) ||
      localCache.reports.length === 0 ||
      !sameReport(localCache.latestReport, latestReport);

    if (!shouldRefreshFullReports) {
      return;
    }

    const resp = await apiFetch(`/api/reports/${stockCode}`);
    const data = await resp.json();
    if (requestId !== reportsRequestId) return;

    allReports = data.reports || [];
    loading.style.display = 'none';

    if (allReports.length === 0) {
      if (latestReport) {
        countEl.textContent = '(최신 1건만 표시)';
        table.style.display = 'table';
        renderReportsTable([latestReport], 1);
      } else {
        loading.style.display = 'block';
        loading.textContent = '증권사 리포트가 없습니다.';
      }
      return;
    }

    countEl.textContent = `(최근 3년, ${allReports.length}건)`;
    table.style.display = 'table';
    renderReportsTable(allReports, reportDisplayCount);
    saveReportCache(stockCode, { latestReport: allReports[0] || latestReport, reports: allReports });
    _overlayTargetPrices(allReports);
  } catch (e) {
    if (requestId !== reportsRequestId) return;
    if (latestReport) {
      loading.style.display = 'none';
      countEl.textContent = '(최신 1건만 표시)';
      table.style.display = 'table';
      renderReportsTable([latestReport], 1);
    } else {
      loading.textContent = '리포트를 불러오지 못했습니다.';
    }
  }
}

// --- LLM wiki (per-stock report summaries) --------------------------------

let _wikiLoadId = 0;
// pdf_url → wiki entry, populated by loadWiki so renderReportsTable can
// attach '요약 내용 보기' inline to matching rows.
let _wikiByPdfUrl = new Map();

async function loadWiki(stockCode) {
  const countEl = document.getElementById('wikiCount');

  // Tag each in-flight load; a newer analyzeStock() call invalidates older
  // ones so we don't flicker stale wiki into a different stock's view.
  const myId = ++_wikiLoadId;

  _wikiByPdfUrl = new Map();
  if (countEl) countEl.textContent = '';
  // Set up Q&A binding for this stock; safe to call repeatedly.
  _setupWikiQa(stockCode);
  // Clear previous answer.
  const prevAns = document.getElementById('wikiQaAnswer');
  const prevMeta = document.getElementById('wikiQaMeta');
  if (prevAns) { prevAns.style.display = 'none'; prevAns.innerHTML = ''; }
  if (prevMeta) prevMeta.textContent = '';

  try {
    const resp = await apiFetch(`/api/analysis/${encodeURIComponent(stockCode)}/wiki?limit=100`);
    if (myId !== _wikiLoadId) return;
    if (!resp.ok) return;
    const data = await resp.json();
    if (myId !== _wikiLoadId) return;
    const entries = data.entries || [];
    entries.forEach(e => {
      if (e.pdf_url) _wikiByPdfUrl.set(e.pdf_url, e);
    });
    if (countEl) countEl.textContent = entries.length ? `· 요약 ${entries.length}건` : '';
    // If the reports table is already rendered, re-render so the toggles
    // attach to rows that just got summaries.
    if (typeof allReports !== 'undefined' && allReports.length) {
      renderReportsTable(allReports, reportDisplayCount);
    }
  } catch (err) {
    if (myId !== _wikiLoadId) return;
    console.warn('wiki load failed', err);
  }
}
