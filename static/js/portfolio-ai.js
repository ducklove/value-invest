// Portfolio AI analysis request flow.
// Split from static/js/portfolio.js to keep portfolio features maintainable.
// --- AI Analysis ---
let _aiModelsLoaded = false;

function _setPfAiStatus(text, state = 'idle') {
  const status = document.getElementById('pfAiStatus');
  if (!status) return;
  status.textContent = text;
  status.dataset.state = state;
}

function _pfAiSectionKind(text) {
  const title = String(text || '').replace(/^#+\s*/, '').trim();
  if (title.includes('핵심') || title.includes('판단')) return title.includes('근거') ? 'rationale' : 'summary';
  if (title.includes('점검') || title.includes('구성')) return 'portfolio';
  if (title.includes('근거')) return 'rationale';
  if (title.includes('리스크') || title.includes('촉매') || title.includes('시나리오')) return 'risk';
  if (title.includes('실행') || title.includes('우선순위') || title.includes('리밸런싱')) return 'action';
  if (title.includes('추가') || title.includes('데이터')) return 'data';
  return 'default';
}

function _decoratePfAiResult(container) {
  if (!container || !container.children.length) return;
  const fragment = document.createDocumentFragment();
  let section = null;
  Array.from(container.children).forEach(node => {
    if (/^H[1-4]$/.test(node.tagName)) {
      section = document.createElement('section');
      section.className = 'pf-ai-section';
      section.dataset.kind = _pfAiSectionKind(node.textContent);
      const heading = document.createElement('div');
      heading.className = 'pf-ai-section-title';
      heading.appendChild(node);
      section.appendChild(heading);
      fragment.appendChild(section);
      return;
    }
    if (!section) {
      section = document.createElement('section');
      section.className = 'pf-ai-section pf-ai-section-lead';
      fragment.appendChild(section);
    }
    section.appendChild(node);
  });
  container.replaceChildren(fragment);
}

function _renderPfAiMarkdown(target, mdText, options = {}) {
  if (!target) return;
  const text = (mdText || '').trim();
  target.classList.toggle('pf-ai-empty', !text);
  if (!text) {
    target.textContent = options.emptyText || '분석 실행 후 결과가 여기에 표시됩니다.';
    return;
  }
  if (typeof _renderSafeMarkdown === 'function') {
    target.innerHTML = _renderSafeMarkdown(mdText);
    if (options.decorate) _decoratePfAiResult(target);
  } else {
    target.textContent = mdText;
  }
}

async function _loadAiModels() {
  if (_aiModelsLoaded) return;
  // Show model picker only for admin
  if (typeof currentUser === 'undefined' || !currentUser || !currentUser.is_admin) return;
  const picker = document.getElementById('pfAiModelPicker');
  if (!picker) return;
  picker.style.display = '';
  try {
    const resp = await apiFetch('/api/portfolio/ai-models');
    if (!resp.ok) return;
    const data = await resp.json();
    const input = document.getElementById('pfAiModelInput');
    const datalist = document.getElementById('pfAiModelList');
    input.value = data.default || '';
    datalist.innerHTML = data.models.map(m =>
      `<option value="${m.id}">${m.name} ($${m.prompt_price.toFixed(2)}/$${m.completion_price.toFixed(2)} per 1M)</option>`
    ).join('');
    _aiModelsLoaded = true;
  } catch {}
}

async function runAiAnalysis() {
  _loadAiModels();
  _ensureFxRate();
  const btn = document.getElementById('pfAiBtn');
  const result = document.getElementById('pfAiResult');
  const tokens = document.getElementById('pfAiTokens');
  const output = document.getElementById('pfAiOutput');
  btn.disabled = true;
  btn.textContent = '분석 중...';
  if (output) output.classList.add('is-loading');
  _setPfAiStatus('분석 중', 'loading');
  _renderPfAiMarkdown(result, '', { emptyText: '분석 결과를 생성하고 있습니다...' });
  if (tokens) tokens.textContent = '';

  const modelInput = document.getElementById('pfAiModelInput');
  const selectedModel = modelInput ? modelInput.value.trim() : '';
  const queryInput = document.getElementById('pfAiQuery');
  const userQuery = queryInput ? queryInput.value.trim() : '';
  const payload = {};
  if (selectedModel) payload.model = selectedModel;
  if (userQuery) payload.query = userQuery;
  const body = JSON.stringify(payload);

  let mdText = '';
  try {
    const resp = await apiFetch('/api/portfolio/ai-analysis', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body,
    });
    if (!resp.ok) {
      const err = await resp.json().catch(() => ({}));
      throw new Error(err.detail || `HTTP ${resp.status}`);
    }
    const reader = resp.body.getReader();
    const decoder = new TextDecoder();
    let buf = '';
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buf += decoder.decode(value, { stream: true });
      const lines = buf.split('\n');
      buf = lines.pop();
      for (const line of lines) {
        if (!line.startsWith('data: ')) continue;
        try {
          const d = JSON.parse(line.slice(6));
          if (d.content) {
            mdText += d.content;
            _renderPfAiMarkdown(result, mdText);
          }
          if (d.done) {
            const model = d.model ? ` · ${d.model}` : '';
            const costUsd = Number(d.cost || 0);
            const costKrw = costUsd && pfFxRate ? Math.round(costUsd * pfFxRate) : null;
            const cost = costUsd ? ` · ${costKrw !== null ? costKrw.toLocaleString() + '원' : '$' + costUsd.toFixed(6)}` : '';
            const wikiN = Number(d.wiki_used || 0);
            const wikiTag = wikiN > 0 ? ` · 리포트 ${wikiN}건 참조` : '';
            const reasoning = d.reasoning_effort ? ` · 추론 ${d.reasoning_effort}` : '';
            const contextN = Number(d.context_holdings || 0);
            const reportsN = Number(d.context_reports_per_holding || 0);
            const context = contextN ? ` · 컨텍스트 상위 ${contextN}종목${reportsN ? `×${reportsN}리포트` : ''}` : '';
            if (tokens) tokens.textContent = `입력 ${d.input_tokens?.toLocaleString() || '?'} / 출력 ${d.output_tokens?.toLocaleString() || '?'} 토큰${cost}${model}${wikiTag}${reasoning}${context}`;
            _setPfAiStatus('완료', 'done');
          }
        } catch {}
      }
    }
    // Final render
    _renderPfAiMarkdown(result, mdText, { decorate: true, emptyText: '분석 결과가 비어 있습니다.' });
  } catch (e) {
    if (result) {
      result.classList.add('pf-ai-empty');
      result.textContent = '분석 실패: ' + e.message;
    }
    _setPfAiStatus('오류', 'error');
  }
  if (output) output.classList.remove('is-loading');
  btn.disabled = false;
  btn.textContent = '분석 실행';
}
