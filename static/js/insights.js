let _insightsLoaded = false;
let _insightPosts = [];

async function loadInsightsBoard({ force = false } = {}) {
  const compose = document.getElementById('insightCompose');
  const list = document.getElementById('insightPostList');
  if (!compose || !list) return;
  renderInsightComposer();
  if (_insightsLoaded && !force) return;
  list.innerHTML = '<div class="insight-board-empty">인사이트를 불러오는 중...</div>';
  try {
    const resp = await apiFetch('/api/insights?limit=50');
    if (!resp.ok) throw new Error('인사이트 목록을 불러오지 못했습니다.');
    _insightPosts = await resp.json();
    _insightsLoaded = true;
    renderInsightPosts();
  } catch (err) {
    list.innerHTML = `<div class="insight-board-empty error">${escapeHtml(err.message)}</div>`;
  }
}

function renderInsightComposer() {
  const root = document.getElementById('insightCompose');
  if (!root) return;
  if (!currentUser) {
    root.innerHTML = `
      <div class="insight-login-card">
        <strong>로그인하면 인사이트를 등록할 수 있습니다.</strong>
        <span>공개 글은 로그인하지 않아도 읽을 수 있습니다.</span>
      </div>
    `;
    return;
  }
  root.innerHTML = `
    <form class="insight-form" onsubmit="event.preventDefault(); submitInsightPost();">
      <div class="insight-form-row">
        <input id="insightTitle" class="insight-input" maxlength="120" placeholder="제목: 예) 소형 가치주 3년 리밸런싱 결과">
        <select id="insightSourceType" class="insight-input">
          <option value="backtest">백테스트</option>
          <option value="valuation">밸류에이션</option>
          <option value="portfolio">포트폴리오</option>
          <option value="manual">수동 검증</option>
          <option value="memo">메모</option>
        </select>
        <select id="insightVisibility" class="insight-input">
          <option value="public">공개</option>
          <option value="private">나만 보기</option>
        </select>
      </div>
      <div class="insight-result-grid">
        <input id="insightStrategy" class="insight-input" placeholder="전략/조건">
        <input id="insightPeriod" class="insight-input" placeholder="기간: 예) 2021-2026">
        <input id="insightReturn" class="insight-input" type="number" step="0.01" placeholder="전략 수익률 %">
        <input id="insightBenchmark" class="insight-input" type="number" step="0.01" placeholder="벤치마크 %">
        <input id="insightCagr" class="insight-input" type="number" step="0.01" placeholder="CAGR %">
        <input id="insightMdd" class="insight-input" type="number" step="0.01" placeholder="MDD %">
      </div>
      <textarea id="insightBody" class="insight-textarea" maxlength="8000"
        placeholder="핵심 인사이트를 적어주세요. 예: 초과수익은 좋지만 거래비용 반영 전이고, 특정 구간에 drawdown이 집중됨. 다음에는 리밸런싱 주기와 universe 필터를 바꿔 재검증."></textarea>
      <details class="insight-raw">
        <summary>원본 결과 JSON 붙여넣기 (선택)</summary>
        <textarea id="insightRawJson" class="insight-textarea raw" placeholder='{"trades": [], "equity": []}'></textarea>
      </details>
      <div class="insight-form-row bottom">
        <input id="insightTags" class="insight-input" placeholder="태그: value, small-cap, monthly">
        <button class="insight-submit" type="submit">인사이트 등록</button>
      </div>
      <div class="insight-form-message" id="insightFormMessage"></div>
    </form>
  `;
}

function _numOrNull(id) {
  const raw = document.getElementById(id)?.value;
  if (raw === '' || raw == null) return null;
  const n = Number(raw);
  return Number.isFinite(n) ? n : null;
}

function _buildInsightSummary() {
  const summary = {};
  const strategy = (document.getElementById('insightStrategy')?.value || '').trim();
  const period = (document.getElementById('insightPeriod')?.value || '').trim();
  if (strategy) summary.strategy = strategy;
  if (period) summary.period = period;
  const metrics = {
    return_pct: _numOrNull('insightReturn'),
    benchmark_return_pct: _numOrNull('insightBenchmark'),
    cagr_pct: _numOrNull('insightCagr'),
    max_drawdown_pct: _numOrNull('insightMdd'),
  };
  Object.entries(metrics).forEach(([key, value]) => {
    if (value !== null) summary[key] = value;
  });
  return summary;
}

function _parseOptionalRawJson() {
  const raw = (document.getElementById('insightRawJson')?.value || '').trim();
  if (!raw) return null;
  return JSON.parse(raw);
}

async function submitInsightPost() {
  const msg = document.getElementById('insightFormMessage');
  if (msg) msg.textContent = '';
  let resultPayload = null;
  try {
    resultPayload = _parseOptionalRawJson();
  } catch (_) {
    if (msg) {
      msg.textContent = '원본 결과 JSON 형식이 올바르지 않습니다.';
      msg.className = 'insight-form-message error';
    }
    return;
  }
  const payload = {
    title: (document.getElementById('insightTitle')?.value || '').trim(),
    source_type: document.getElementById('insightSourceType')?.value || 'manual',
    visibility: document.getElementById('insightVisibility')?.value || 'public',
    insight_md: (document.getElementById('insightBody')?.value || '').trim(),
    result_summary: _buildInsightSummary(),
    result_payload: resultPayload,
    tags: (document.getElementById('insightTags')?.value || '').split(',').map(s => s.trim()).filter(Boolean),
  };
  try {
    const resp = await apiFetch('/api/insights', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    const data = await resp.json().catch(() => ({}));
    if (!resp.ok) throw new Error(data.detail || '등록 실패');
    if (msg) {
      msg.textContent = '등록되었습니다.';
      msg.className = 'insight-form-message ok';
    }
    _clearInsightForm();
    await loadInsightsBoard({ force: true });
  } catch (err) {
    if (msg) {
      msg.textContent = err.message;
      msg.className = 'insight-form-message error';
    }
  }
}

function _clearInsightForm() {
  [
    'insightTitle', 'insightStrategy', 'insightPeriod', 'insightReturn',
    'insightBenchmark', 'insightCagr', 'insightMdd', 'insightBody',
    'insightRawJson', 'insightTags',
  ].forEach(id => {
    const el = document.getElementById(id);
    if (el) el.value = '';
  });
}

function renderInsightPosts() {
  const list = document.getElementById('insightPostList');
  if (!list) return;
  if (!_insightPosts.length) {
    list.innerHTML = '<div class="insight-board-empty">아직 등록된 인사이트가 없습니다.</div>';
    return;
  }
  list.innerHTML = _insightPosts.map(renderInsightPostCard).join('');
}

function _fmtInsightMetric(value, suffix = '%') {
  if (value == null || !Number.isFinite(Number(value))) return null;
  const n = Number(value);
  return `${n >= 0 ? '+' : ''}${n.toFixed(2)}${suffix}`;
}

function _summaryChips(summary) {
  const chips = [];
  if (summary.strategy) chips.push(['전략', summary.strategy]);
  if (summary.period) chips.push(['기간', summary.period]);
  const metrics = [
    ['전략', _fmtInsightMetric(summary.return_pct)],
    ['BM', _fmtInsightMetric(summary.benchmark_return_pct)],
    ['CAGR', _fmtInsightMetric(summary.cagr_pct)],
    ['MDD', _fmtInsightMetric(summary.max_drawdown_pct)],
  ];
  metrics.forEach(([label, value]) => { if (value) chips.push([label, value]); });
  return chips.map(([label, value]) => `<span><b>${escapeHtml(label)}</b>${escapeHtml(value)}</span>`).join('');
}

function renderInsightPostCard(post) {
  const summary = post.result_summary || {};
  const chips = _summaryChips(summary);
  const tags = (post.tags || []).map(t => `<span class="insight-tag">${escapeHtml(t)}</span>`).join('');
  const body = typeof _renderSafeMarkdown === 'function'
    ? _renderSafeMarkdown(post.insight_md || '')
    : escapeHtml(post.insight_md || '');
  const created = (post.created_at || '').slice(0, 16).replace('T', ' ');
  const privateBadge = post.visibility === 'private' ? '<span class="insight-private">나만 보기</span>' : '';
  return `
    <article class="insight-post-card">
      <header>
        <div>
          <div class="insight-post-meta">${escapeHtml(post.source_type || 'manual')} · ${escapeHtml(post.author_name || '')} · ${escapeHtml(created)} ${privateBadge}</div>
          <h3>${escapeHtml(post.title)}</h3>
        </div>
        ${post.can_delete ? `<button class="insight-delete" onclick="deleteInsightPost(${post.id})">삭제</button>` : ''}
      </header>
      ${chips ? `<div class="insight-summary-chips">${chips}</div>` : ''}
      <div class="insight-post-body">${body}</div>
      ${tags ? `<div class="insight-tags">${tags}</div>` : ''}
    </article>
  `;
}

async function deleteInsightPost(id) {
  if (!confirm('이 인사이트 글을 삭제할까요?')) return;
  try {
    const resp = await apiFetch(`/api/insights/${encodeURIComponent(id)}`, { method: 'DELETE' });
    if (!resp.ok) {
      const data = await resp.json().catch(() => ({}));
      throw new Error(data.detail || '삭제 실패');
    }
    await loadInsightsBoard({ force: true });
  } catch (err) {
    alert(err.message);
  }
}
