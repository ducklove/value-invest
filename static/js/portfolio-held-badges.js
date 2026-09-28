// Shared by linked dashboards. Only explicit stock labels opt in; no names are
// guessed. A hub link hands off a snapshot in the fragment, which is consumed
// immediately; direct visits can still use the credentialed API when available.
(() => {
  const endpoint = new URL('/api/portfolio/held-codes', document.currentScript.src).href;
  const selector = '[data-portfolio-code]';
  let codes = new Set();
  let pending;
  const normalize = value => String(value || '').trim().toUpperCase().replace(/\.(KS|KQ)$/, '');
  const fragment = new URLSearchParams(location.hash.slice(1));
  const hasSnapshot = fragment.has('vc-held');
  if (hasSnapshot) {
    codes = new Set((fragment.get('vc-held') || '').split(',').filter(code => /^[0-9][0-9A-Z]{5}$/.test(code)));
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
      vertical-align:middle; white-space:nowrap;
    }
    [data-theme="dark"] .portfolio-held-badge {
      background:#163b2b; color:#a2ebbd; border-color:#397855;
    }`;
  document.head.appendChild(style);

  const observer = new MutationObserver(render);
  function render() {
    observer.disconnect();
    document.querySelectorAll(selector).forEach(label => {
      const badge = Array.from(label.children).find(child => child.classList.contains('portfolio-held-badge'));
      const held = codes.has(normalize(label.dataset.portfolioCode));
      if (!held) {
        badge?.remove();
      } else if (!badge) {
        const element = document.createElement('span');
        element.className = 'portfolio-held-badge';
        element.textContent = '보유';
        element.title = hasSnapshot ? '링크를 연 시점에 내 포트폴리오에 보유 중인 종목' : '내 포트폴리오에 보유 중인 종목';
        label.appendChild(element);
      }
    });
    observer.observe(document.body, {
      childList: true, subtree: true, attributes: true,
      attributeFilter: ['data-portfolio-code'],
    });
  }

  async function refresh() {
    if (hasSnapshot) { render(); return; }
    pending?.abort();
    const controller = new AbortController();
    pending = controller;
    codes = new Set();
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
