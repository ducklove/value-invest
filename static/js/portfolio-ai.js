// Portfolio AI analysis request flow.
// Split from static/js/portfolio.js to keep portfolio features maintainable.
// --- AI Analysis ---
let _aiModelsLoaded = false;
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
  btn.disabled = true;
  btn.textContent = '분석 중...';
  result.textContent = '';
  tokens.textContent = '';

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
            // Live preview: render markdown as it streams.
            // Sanitize with DOMPurify — prompt injection via portfolio
            // names could otherwise cause the model to echo raw HTML.
            if (typeof marked !== 'undefined') {
              result.innerHTML = _renderSafeMarkdown(mdText);
            } else {
              result.textContent = mdText;
            }
          }
          if (d.done) {
            const model = d.model ? ` · ${d.model}` : '';
            const costUsd = Number(d.cost || 0);
            const costKrw = costUsd && pfFxRate ? Math.round(costUsd * pfFxRate) : null;
            const cost = costUsd ? ` · ${costKrw !== null ? costKrw.toLocaleString() + '원' : '$' + costUsd.toFixed(6)}` : '';
            const wikiN = Number(d.wiki_used || 0);
            const wikiTag = wikiN > 0 ? ` · 리포트 ${wikiN}건 참조` : '';
            tokens.textContent = `입력 ${d.input_tokens?.toLocaleString() || '?'} / 출력 ${d.output_tokens?.toLocaleString() || '?'} 토큰${cost}${model}${wikiTag}`;
          }
        } catch {}
      }
    }
    // Final render
    if (typeof marked !== 'undefined' && mdText) result.innerHTML = _renderSafeMarkdown(mdText);
  } catch (e) {
    result.textContent = '분석 실패: ' + e.message;
  }
  btn.disabled = false;
  btn.textContent = '분석 실행';
}
