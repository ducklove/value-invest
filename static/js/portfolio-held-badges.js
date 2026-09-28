// Shared by linked dashboards. Only explicit stock labels opt in; no names are
// guessed. A hub link hands off a snapshot in the fragment, which is consumed
// immediately; direct visits can still use the credentialed API when available.
(() => {
  const endpoint = new URL('/api/portfolio/held-codes', document.currentScript.src).href;
  const selector = '[data-portfolio-code]';
  let codes = new Set();
  let quantities = new Map();
  let pending;
  const normalize = value => String(value || '').trim().toUpperCase().replace(/\.(KS|KQ)$/, '');
  const fragment = new URLSearchParams(location.hash.slice(1));
  const hasSnapshot = fragment.has('vc-held');
  if (hasSnapshot) {
    for (const entry of (fragment.get('vc-held') || '').split(',')) {
      const [code, quantity] = entry.split(':');
      if (!/^[0-9][0-9A-Z]{5}$/.test(code)) continue;
      // Older links contain codes only. Keep their badges without inventing quantities.
      codes.add(code);
      const value = Number(quantity);
      if (Number.isFinite(value) && value > 0) quantities.set(code, value);
    }
    fragment.delete('vc-held');
    const rest = fragment.toString();
    history.replaceState(history.state, '', location.pathname + location.search + (rest ? '#' + rest : ''));
  }
  const style = document.createElement('style');
  style.textContent = `
    .portfolio-held-badge {
      display:inline-block; margin-inline-start:5px; padding:1px 5px;
      border:1px solid #86bfa3; border-radius:4px; background:#e5f5eb;
      color:#17613b; font-size:11px; font-weight:700; line-height:1.5;
      vertical-align:middle; white-space:nowrap; cursor:help;
    }
    [data-theme="dark"] .portfolio-held-badge {
      background:#163b2b; color:#a2ebbd; border-color:#397855;
    }`;
  document.head.appendChild(style);

  function tooltip(label) {
    const quantity = quantities.get(normalize(label.dataset.portfolioCode));
    const price = Number(label.dataset.portfolioPrice);
    const value = quantity * price;
    const quantityText = quantity == null ? '확인 불가' : `${quantity.toLocaleString('ko-KR', { maximumFractionDigits: 20 })}주`;
    const valueText = quantity != null && Number.isFinite(price) && price > 0 && Number.isFinite(value)
      ? `${value.toLocaleString('ko-KR', { maximumFractionDigits: 0 })}원` : '확인 불가';
    return `보유수량: ${quantityText}\n평가액: ${valueText}\n화면 현재가 기준${hasSnapshot ? ' · 수량은 링크를 연 시점 기준' : ''}`;
  }

  const observer = new MutationObserver(render);
  function render() {
    observer.disconnect();
    document.querySelectorAll(selector).forEach(label => {
      let badge = Array.from(label.children).find(child => child.classList.contains('portfolio-held-badge'));
      const held = codes.has(normalize(label.dataset.portfolioCode));
      if (!held) {
        badge?.remove();
      } else {
        if (!badge) {
          badge = document.createElement('span');
          badge.className = 'portfolio-held-badge';
          badge.textContent = '보유';
          label.appendChild(badge);
        }
        badge.title = tooltip(label);
        badge.setAttribute('aria-label', `보유 · ${badge.title}`);
      }
    });
    observer.observe(document.body, {
      childList: true, subtree: true, attributes: true,
      attributeFilter: ['data-portfolio-code', 'data-portfolio-price'],
    });
  }

  async function refresh() {
    if (hasSnapshot) { render(); return; }
    pending?.abort();
    const controller = new AbortController();
    pending = controller;
    codes = new Set();
    quantities = new Map();
    render();
    const timeout = setTimeout(() => controller.abort(), 8000);
    try {
      const response = await fetch(endpoint, {
        credentials: 'include', cache: 'no-store', signal: controller.signal,
      });
      if (!response.ok) return;
      const data = await response.json();
      if (controller !== pending || controller.signal.aborted) return;
      codes = new Set((Array.isArray(data.codes) ? data.codes : [])
        .filter(code => typeof code === 'string' && code.trim()).map(normalize));
      quantities = new Map(Object.entries(data.quantities || {})
        .filter(([, value]) => Number.isFinite(value) && value > 0)
        .map(([code, value]) => [normalize(code), value]));
      render();
    } catch (_) {
      // Optional personalization: unavailable sessions/network leave the dashboard usable.
    } finally {
      clearTimeout(timeout);
    }
  }
  window.addEventListener('focus', refresh);
  window.addEventListener('pagehide', () => {
    pending?.abort();
    observer.disconnect();
  });
  window.addEventListener('pageshow', event => { if (event.persisted) refresh(); });
  document.addEventListener('visibilitychange', () => { if (!document.hidden) refresh(); });
  refresh();
})();
