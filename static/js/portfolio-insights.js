// Portfolio asset insight modal, tags, linked dashboard actions, holding/preferred helpers.
// Split from static/js/portfolio.js to keep portfolio features maintainable.
let pfAssetInsightCode = null;
let pfAssetInsightData = null;
let pfAssetInsightActions = [];

function pfCloseAssetInsight() {
  const modal = document.getElementById('pfAssetInsightModal');
  if (modal) modal.style.display = 'none';
  pfAssetInsightCode = null;
  pfAssetInsightData = null;
  pfAssetInsightActions = [];
}

async function pfOpenAssetInsight(stockCode) {
  const modal = document.getElementById('pfAssetInsightModal');
  const title = document.getElementById('pfAssetInsightTitle');
  const body = document.getElementById('pfAssetInsightBody');
  if (!modal || !body || !stockCode) return;
  pfAssetInsightCode = stockCode;
  pfAssetInsightData = null;
  pfAssetInsightActions = [];
  modal.style.display = 'flex';
  if (title) title.textContent = '투자 인사이트';
  body.innerHTML = '<div class="pf-insight-loading">자산 데이터를 불러오는 중입니다...</div>';

  const slowNoticeId = setTimeout(() => {
    if (pfAssetInsightCode !== stockCode) return;
    body.innerHTML = '<div class="pf-insight-loading">첫 조회라 가격/벤치마크 데이터를 조금 더 모으는 중입니다. 완료되면 자동으로 표시됩니다...</div>';
  }, 12000);
  const verySlowNoticeId = setTimeout(() => {
    if (pfAssetInsightCode !== stockCode) return;
    body.innerHTML = '<div class="pf-insight-loading">외부 데이터 응답이 늦습니다. 요청은 끊지 않고 계속 기다리는 중입니다...</div>';
  }, 30000);
  try {
    const resp = await apiFetch(`/api/portfolio/asset-insight/${encodeURIComponent(stockCode)}`);
    if (!resp.ok) {
      let detail = '';
      try {
        const err = await resp.json();
        detail = err.detail || err.message || '';
      } catch (e) {}
      throw new Error(detail || `인사이트 로드 실패 (${resp.status})`);
    }
    const data = await resp.json();
    if (pfAssetInsightCode !== stockCode) return;
    pfAssetInsightData = data;
    const profile = data.profile || {};
    if (title) title.textContent = `${profile.name || stockCode} 투자 인사이트`;
    body.innerHTML = _renderAssetInsight(data);
  } catch (e) {
    if (pfAssetInsightCode !== stockCode) return;
    const message = e.message || '인사이트를 불러오지 못했습니다.';
    body.innerHTML = `<div class="pf-insight-error">${escapeHtml(message)}</div>`;
  } finally {
    clearTimeout(slowNoticeId);
    clearTimeout(verySlowNoticeId);
  }
}

function _insightNum(value) {
  if (value === null || value === undefined || value === '') return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function _fmtInsightPct(value, signed = true) {
  const n = _insightNum(value);
  return n === null ? '-' : fmtPct(n, signed);
}

function _fmtInsightSignedAmount(value, currency = '') {
  const n = _insightNum(value);
  if (n === null) return '-';
  const prefix = n > 0 ? '+' : '';
  return `${prefix}${_fmtInsightAmount(n, currency)}`;
}

function _fmtInsightAmount(value, currency = '') {
  const n = _insightNum(value);
  if (n === null) return '-';
  const cur = String(currency || '').toUpperCase();
  if (!cur || cur === 'KRW') return fmtKrw(Math.round(n));
  const digits = Math.abs(n) >= 100 ? 2 : 4;
  return `${cur} ${n.toLocaleString(undefined, { maximumFractionDigits: digits })}`;
}

function _fmtInsightPrice(value, currency = '') {
  const n = _insightNum(value);
  if (n === null) return '-';
  const cur = String(currency || '').toUpperCase();
  if (!cur || cur === 'KRW') return n.toLocaleString(undefined, { maximumFractionDigits: 2 });
  return `${cur} ${n.toLocaleString(undefined, { maximumFractionDigits: 4 })}`;
}

function _insightClass(value) {
  const n = _insightNum(value);
  if (n === null) return '';
  return n > 0 ? 'positive' : n < 0 ? 'negative' : '';
}

function _renderInsightCard(label, value, sub = '', cls = '') {
  return `<div class="pf-insight-card">
    <div class="pf-insight-card-label">${escapeHtml(label)}</div>
    <div class="pf-insight-card-value ${cls}">${escapeHtml(value)}</div>
    ${sub ? `<div class="pf-insight-card-sub">${escapeHtml(sub)}</div>` : ''}
  </div>`;
}

function _normalizeInsightTags(tags) {
  const raw = Array.isArray(tags)
    ? tags
    : (typeof tags === 'string' ? tags.split(/[,#\n]+/) : []);
  const normalized = [];
  const seen = new Set();
  for (const value of raw) {
    const tag = String(value ?? '')
      .trim()
      .replace(/^#+/, '')
      .replace(/\s+/g, ' ')
      .slice(0, 30);
    if (!tag) continue;
    const key = tag.toLocaleLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    normalized.push(tag);
    if (normalized.length >= 12) break;
  }
  return normalized;
}

function _insightTagListId(code) {
  const safe = String(code || 'asset').replace(/[^A-Za-z0-9_-]/g, '_');
  return `pfInsightTagList_${safe}`;
}

function _renderInsightActionLinks(code, goldGap) {
  if (!code) {
    pfAssetInsightActions = [];
    return '';
  }
  pfAssetInsightActions = _portfolioLinkActions(code, {
    includeInsight: false,
    includeGoldGap: !goldGap,
  });
  if (!pfAssetInsightActions.length) return '';
  return `<div class="pf-insight-link-actions" aria-label="연결 메뉴">
    ${pfAssetInsightActions.map((action, idx) => `
      <button type="button" class="pf-insight-link-action js-pf-insight-action" data-action-idx="${idx}">
        <strong>${escapeHtml(action.label)}</strong>
        <span>${escapeHtml(action.hint || '')}</span>
      </button>
    `).join('')}
  </div>`;
}

function _renderInsightTags(code, tags, suggestions) {
  const normalizedTags = _normalizeInsightTags(tags);
  const selectedKeys = new Set(normalizedTags.map(tag => tag.toLocaleLowerCase()));
  const suggestedTags = _normalizeInsightTags(suggestions)
    .filter(tag => !selectedKeys.has(tag.toLocaleLowerCase()))
    .slice(0, 12);
  const safeCode = escapeHtml(code);
  const listId = _insightTagListId(code);
  const tagChips = normalizedTags.length
    ? normalizedTags.map(tag => `
      <button type="button" class="pf-insight-tag-chip js-pf-tag-remove" data-code="${safeCode}" data-tag="${escapeHtml(tag)}" title="태그 제거">
        <span>#${escapeHtml(tag)}</span>
        <b aria-hidden="true">×</b>
      </button>
    `).join('')
    : '<span class="pf-insight-tag-empty">아직 태그가 없습니다. 투자 아이디어를 짧게 붙여두세요.</span>';
  const suggestionButtons = suggestedTags.length
    ? `<div class="pf-insight-tag-suggestions">
        ${suggestedTags.map(tag => `<button type="button" class="pf-insight-tag-suggestion js-pf-tag-suggest" data-code="${safeCode}" data-tag="${escapeHtml(tag)}">#${escapeHtml(tag)}</button>`).join('')}
      </div>`
    : '';
  const dataListOptions = suggestedTags
    .map(tag => `<option value="${escapeHtml(tag)}"></option>`)
    .join('');

  return `<section class="pf-insight-tag-panel" data-code="${safeCode}">
    <div class="pf-insight-tag-head">
      <div>
        <span class="pf-insight-section-kicker">Investment Tags</span>
        <strong>투자 아이디어 태그</strong>
      </div>
      <span>${normalizedTags.length}/12</span>
    </div>
    <div class="pf-insight-tag-list">${tagChips}</div>
    <div class="pf-insight-tag-add">
      <input id="pfInsightTagInput" class="pf-insight-tag-input" list="${escapeHtml(listId)}" type="text" maxlength="30" placeholder="예: 자산주, 턴어라운드, AI관련주">
      <datalist id="${escapeHtml(listId)}">${dataListOptions}</datalist>
      <button type="button" class="pf-insight-tag-add-btn js-pf-tag-add" data-code="${safeCode}">추가</button>
    </div>
    ${suggestionButtons}
  </section>`;
}

function _updateAssetInsightModalBody() {
  if (!pfAssetInsightData) return;
  const body = document.getElementById('pfAssetInsightBody');
  if (body) body.innerHTML = _renderAssetInsight(pfAssetInsightData);
}

async function pfSaveAssetTags(stockCode, tags) {
  const normalizedTags = _normalizeInsightTags(tags);
  const resp = await apiFetch(`/api/portfolio/${encodeURIComponent(stockCode)}/tags`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ tags: normalizedTags }),
  });
  if (!resp.ok) {
    const err = await resp.json().catch(() => ({}));
    throw new Error(err.detail || err.message || '태그 저장에 실패했습니다.');
  }
  const data = await resp.json();
  const savedTags = _normalizeInsightTags(data.tags || normalizedTags);
  const item = portfolioItems.find(i => i.stock_code === stockCode);
  if (item) item.tags = savedTags;
  if (pfAssetInsightCode === stockCode && pfAssetInsightData) {
    pfAssetInsightData = {
      ...pfAssetInsightData,
      tags: savedTags,
      tagSuggestions: data.tagSuggestions || pfAssetInsightData.tagSuggestions || [],
    };
    _updateAssetInsightModalBody();
  }
  renderPortfolio();
  showToast('태그를 저장했습니다.', 'success');
}

async function pfAddAssetTag(stockCode, rawTag) {
  const [tag] = _normalizeInsightTags([rawTag]);
  if (!stockCode || !tag) {
    showToast('추가할 태그를 입력해 주세요.');
    return;
  }
  const current = _normalizeInsightTags(pfAssetInsightData?.tags || []);
  const next = _normalizeInsightTags([...current, tag]);
  await pfSaveAssetTags(stockCode, next);
}

async function pfRemoveAssetTag(stockCode, tag) {
  if (!stockCode || !tag) return;
  const key = String(tag).toLocaleLowerCase();
  const current = _normalizeInsightTags(pfAssetInsightData?.tags || []);
  const next = current.filter(existing => existing.toLocaleLowerCase() !== key);
  await pfSaveAssetTags(stockCode, next);
}

function _renderAssetInsight(data) {
  const profile = data.profile || {};
  const position = data.position || {};
  const metrics = data.metrics || {};
  const returns = metrics.returns || {};
  const volatility = metrics.volatility || {};
  const benchmark = data.benchmark || {};
  const benchmarkReturns = benchmark.returns || {};
  const relativeReturns = benchmark.relativeReturns || {};
  const quality = data.dataQuality || {};
  const code = profile.code || '';
  const historyCurrency = (
    profile.currency ||
    quality.historyCurrency ||
    (code === 'KRX_GOLD' || String(code).startsWith('CASH_') ? 'KRW' : '')
  );
  const positionCurrency = 'KRW';
  const goldGap = data.goldGap;
  const actionLinks = _renderInsightActionLinks(code, goldGap);
  const tagPanel = _renderInsightTags(code, data.tags || [], data.tagSuggestions || []);

  const cards = [
    _renderInsightCard('현재가', _fmtInsightPrice(position.currentPrice, positionCurrency), benchmark.dayChangePct !== null && benchmark.dayChangePct !== undefined ? `벤치마크 오늘 ${_fmtInsightPct(benchmark.dayChangePct)}` : ''),
    _renderInsightCard('평가금액', _fmtInsightAmount(position.marketValue, positionCurrency), `투입 ${_fmtInsightAmount(position.invested, positionCurrency)}`),
    _renderInsightCard('보유 수익률', _fmtInsightPct(position.returnPct), _fmtInsightSignedAmount(position.pnl, positionCurrency), _insightClass(position.returnPct)),
    _renderInsightCard('오늘 손익', _fmtInsightPct(position.dailyChangePct), _fmtInsightSignedAmount(position.dailyPnl, positionCurrency), _insightClass(position.dailyPnl)),
    _renderInsightCard('최근 3개월', _fmtInsightPct(returns['3m']), `벤치마크 대비 ${_fmtInsightPct(relativeReturns['3m'])}`, _insightClass(returns['3m'])),
    _renderInsightCard('60일 변동성', _fmtInsightPct(volatility['60d'], false), '연율화 기준'),
    _renderInsightCard('최대 낙폭', _fmtInsightPct(metrics.maxDrawdownPct), '최근 1년 가격 기준', _insightClass(metrics.maxDrawdownPct)),
    _renderInsightCard('52주 고점 대비', _fmtInsightPct(metrics.fromHigh52Pct), `저점 대비 ${_fmtInsightPct(metrics.fromLow52Pct)}`, _insightClass(metrics.fromHigh52Pct)),
  ].join('');

  const windows = [
    ['1m', '1개월'],
    ['3m', '3개월'],
    ['6m', '6개월'],
    ['1y', '1년'],
  ];
  const returnRows = windows.map(([key, label]) => `
    <tr>
      <td>${label}</td>
      <td class="${_insightClass(returns[key])}">${_fmtInsightPct(returns[key])}</td>
      <td class="${_insightClass(benchmarkReturns[key])}">${_fmtInsightPct(benchmarkReturns[key])}</td>
      <td class="${_insightClass(relativeReturns[key])}">${_fmtInsightPct(relativeReturns[key])}</td>
    </tr>
  `).join('');

  const signals = Array.isArray(data.signals) && data.signals.length ? data.signals : [{
    level: 'neutral',
    title: '추가 경고 신호 없음',
    body: '현재 확보된 가격/벤치마크 데이터 기준으로는 큰 이상 신호가 보이지 않습니다.',
  }];
  const signalHtml = signals.map(s => `
    <div class="pf-insight-signal ${escapeHtml(s.level || 'neutral')}">
      <strong>${escapeHtml(s.title || '')}</strong>
      <span>${escapeHtml(s.body || '')}</span>
    </div>
  `).join('');

  const macro = Array.isArray(data.macro) ? data.macro : [];
  const macroHtml = macro.length ? macro.map(m => `
    <div class="pf-insight-macro-item">
      <span>${escapeHtml(m.label || m.code || '')}</span>
      <strong>${escapeHtml(m.value || '-')}</strong>
      <em class="${m.direction === 'up' ? 'positive' : m.direction === 'down' ? 'negative' : ''}">${escapeHtml(m.changePct || m.change || '')}</em>
    </div>
  `).join('') : '<div class="pf-insight-empty">연동된 시장 지표가 없습니다.</div>';

  const goldGapHtml = goldGap ? `
    <div class="pf-insight-gold-gap">
      <div>
        <span class="pf-insight-section-kicker">Gold Gap</span>
        <strong>${escapeHtml(goldGap.label || goldGap.asset || '')}</strong>
        <p>최근 괴리율 ${_fmtInsightPct(goldGap.latestGapPct)}${goldGap.latestDate ? ` · ${escapeHtml(goldGap.latestDate)}` : ''}</p>
      </div>
      <button class="pf-insight-link js-pf-gold-gap" data-gap-asset="${escapeHtml(goldGap.asset || '')}">대시보드 열기</button>
    </div>
  ` : '';

  return `
    <div class="pf-insight-hero">
      <div>
        <div class="pf-insight-kicker">${escapeHtml(code)}</div>
        <h4>${escapeHtml(profile.name || code)}</h4>
        <p>${escapeHtml(profile.assetClassLabel || '기타 자산')} 자산을 가격 추세, 벤치마크, 매크로 지표 기준으로 빠르게 점검합니다.</p>
        ${actionLinks}
      </div>
      <div class="pf-insight-chips">
        <span>${escapeHtml(profile.assetClassLabel || '자산')}</span>
        <span>${escapeHtml(historyCurrency || '통화 미확인')}</span>
        <span>BM ${escapeHtml(benchmark.name || profile.benchmarkName || '-')}</span>
        <span>${Number(quality.historyPoints || 0).toLocaleString()} pts</span>
      </div>
    </div>
    ${tagPanel}
    <div class="pf-insight-grid">${cards}</div>
    <div class="pf-insight-two-col">
      <section class="pf-insight-section">
        <div class="pf-insight-section-title">수익률 비교</div>
        <table class="pf-insight-table">
          <thead><tr><th>기간</th><th>자산</th><th>벤치마크</th><th>초과</th></tr></thead>
          <tbody>${returnRows}</tbody>
        </table>
      </section>
      <section class="pf-insight-section">
        <div class="pf-insight-section-title">체크 포인트</div>
        <div class="pf-insight-signals">${signalHtml}</div>
      </section>
    </div>
    <section class="pf-insight-section">
      <div class="pf-insight-section-title">시장 배경</div>
      <div class="pf-insight-macro">${macroHtml}</div>
    </section>
    ${goldGapHtml}
  `;
}

let _HOLDING_CODES = new Set([
  '000670','000880','002790','003380','004360','004700','004800',
  '005810','006120','024800','028260','030530','032830',
  '036710','051910','058650','402340',
]);
// 지주사 코드 → 메타 (자회사 + 발행주식수). NAV per share 계산에 사용.
// holdings.json 의 단순 코드 Set 외에 subsidiaries / totalShares /
// treasuryShares 도 캐시. localStorage 키도 holdingCodes (Set 호환) 와
// 새 holdingMeta 두 가지 병행 — 옛 키만 있는 사용자도 호환되도록 폴백.
let _HOLDING_META = {};

function _applyHoldingPayload(data, persist) {
  const codes = (data.items || []).map(i => i.holdingCode).filter(Boolean);
  const meta = {};
  for (const it of (data.items || [])) {
    if (!it.holdingCode) continue;
    meta[it.holdingCode] = {
      totalShares: it.holdingTotalShares || 0,
      treasuryShares: it.holdingTreasuryShares || 0,
      subsidiaries: (it.subsidiaries || [])
        .filter(s => s.code && s.sharesHeld != null)
        .map(s => ({ code: s.code, sharesHeld: s.sharesHeld })),
    };
  }
  if (!codes.length) return false;
  _HOLDING_CODES = new Set(codes);
  _HOLDING_META = meta;
  if (persist) {
    localStorage.setItem('holdingCodes', JSON.stringify({ codes, ts: Date.now() }));
    localStorage.setItem('holdingMeta', JSON.stringify({ meta, ts: Date.now() }));
  }
  return true;
}

function _applyHoldingIntegrationConfig() {
  const config = getIntegrationConfig('holdingValue');
  if (Array.isArray(config.items) && _applyHoldingPayload({ items: config.items }, false)) return true;
  let applied = false;
  if (Array.isArray(config.codes) && config.codes.length) {
    _HOLDING_CODES = new Set(config.codes);
    applied = true;
  }
  if (config.meta && typeof config.meta === 'object') {
    _HOLDING_META = config.meta;
    applied = true;
  }
  return applied;
}

(function _refreshHoldingCodes() {
  const hasConfig = _applyHoldingIntegrationConfig();
  try {
    const codeCache = JSON.parse(localStorage.getItem('holdingCodes') || '{}');
    const metaCache = JSON.parse(localStorage.getItem('holdingMeta') || '{}');
    if (!hasConfig) {
      if (codeCache.codes) _HOLDING_CODES = new Set(codeCache.codes);
      if (metaCache.meta) _HOLDING_META = metaCache.meta;
    }
    if (hasConfig || (codeCache.ts && metaCache.ts && Date.now() - Math.min(codeCache.ts, metaCache.ts) < 86400000)) return;
  } catch (e) { console.warn(e); }
  const holdingsUrl = getIntegrationEndpoint('holdingValue', 'holdingsUrl', 'api/holdings.json');
  if (!holdingsUrl) return;
  fetch(holdingsUrl)
    .then(r => r.json())
    .then(data => { _applyHoldingPayload(data, true); })
    .catch(() => {});
})();

function pfGoAnalyze(stockCode, e) {
  if (e) {
    e.preventDefault();
    e.stopPropagation();
  }
  document.querySelectorAll('.pf-pref-menu').forEach(el => el.remove());
  pfOpenAssetInsight(stockCode);
}

function _showPrefMenu(prefCode, commonCode, e) {
  // Remove any existing menu
  document.querySelectorAll('.pf-pref-menu').forEach(el => el.remove());
  const menu = document.createElement('div');
  menu.className = 'pf-pref-menu';
  menu.innerHTML = `
    <div class="pf-pref-item" data-action="common">본주 분석 (${commonCode})</div>
    <div class="pf-pref-item" data-action="spread">우선주 괴리율 대시보드</div>
  `;
  document.body.appendChild(menu);
  _positionPortfolioPopupMenu(menu, e);

  menu.querySelector('[data-action="common"]').addEventListener('click', () => {
    menu.remove();
    switchView('analysis');
    analyzeStock(commonCode);
  });
  menu.querySelector('[data-action="spread"]').addEventListener('click', () => {
    menu.remove();
    openIntegration('preferredSpread', '', { code: prefCode });
  });
  // Close on outside click
  setTimeout(() => {
    document.addEventListener('click', function close(ev) {
      if (!menu.contains(ev.target)) { menu.remove(); document.removeEventListener('click', close); }
    });
  }, 0);
}

function _showHoldingMenu(stockCode, e) {
  document.querySelectorAll('.pf-pref-menu').forEach(el => el.remove());
  const menu = document.createElement('div');
  menu.className = 'pf-pref-menu';
  menu.innerHTML = `
    <div class="pf-pref-item" data-action="analysis">본주 분석</div>
    <div class="pf-pref-item" data-action="holding">자회사 비율 추이</div>
  `;
  document.body.appendChild(menu);
  _positionPortfolioPopupMenu(menu, e);
  menu.querySelector('[data-action="analysis"]').addEventListener('click', () => {
    menu.remove();
    switchView('analysis');
    analyzeStock(stockCode);
  });
  menu.querySelector('[data-action="holding"]').addEventListener('click', () => {
    menu.remove();
    openIntegration('holdingValue', '', { code: stockCode });
  });
  setTimeout(() => {
    document.addEventListener('click', function close(ev) {
      if (!menu.contains(ev.target)) { menu.remove(); document.removeEventListener('click', close); }
    });
  }, 0);
}
