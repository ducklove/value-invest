/* vc-shell.js v1.1.0 — Value Compass ecosystem bar (<vc-shell> + window.VCShell).
 * Canonical copy: value-invest/static/ecosystem/vc-shell.js. The registry between the
 * vc:registry markers is generated from config/ecosystem.json by scripts/sync-ecosystem.mjs
 * (public tools only). Vendored byte-identical into sibling repos — do not edit copies.
 * Classic script, no dependencies, no network requests.
 * <vc-shell tool="<id>" variant="menu"> renders only the ▾ tool switcher + popover (no bar), for
 * a host header that already has its own brand/theme controls (the hub header uses it). */
(function () {
  'use strict';
  if (window.VCShell) return; // double-load guard

  var REGISTRY = /* vc:registry:start */ {"version":1,"hub":"https://ducklove.duckdns.org:3691","categories":[{"id":"hub","label":"Value Compass"},{"id":"equity","label":"종목 밸류에이션"},{"id":"etf","label":"ETF·자산배분"},{"id":"real-assets","label":"실물자산·크립토"},{"id":"macro","label":"거시·채권·지수"},{"id":"flows","label":"기관·수급"},{"id":"hub-tools","label":"허브 도구"}],"tools":[{"id":"value-invest","name":"Value Compass","description":"가치투자 포트폴리오·종목분석 허브","category":"hub","icon":"compass","accent":"#2563eb","url":"https://ducklove.duckdns.org:3691","deploy":"self-hosted","stockLink":{"template":"/analysis?code={code}","accepts":"^[0-9A-Z]{6}$"},"hubView":"/","themeParam":true,"handoff":false,"heldBadges":false},{"id":"holding_value","integrationKey":"holdingValue","name":"지주사 지분가치","description":"지주사 보유지분가치 대비 시가총액 비율과 할인율 추이","category":"equity","icon":"building","accent":"#2d66d6","url":"https://ducklove.github.io/holding_value","deploy":"github-pages","stockLink":{"template":"?code={code}","accepts":"^[0-9A-Z]{6}$"},"embed":{"template":"?embed=1"},"themeParam":true,"handoff":true,"heldBadges":true},{"id":"common_preferred_spread","integrationKey":"preferredSpread","name":"우선주 괴리율","description":"보통주·우선주 괴리율과 백분위, 전환 전략","category":"equity","icon":"split","accent":"#315bdb","url":"https://ducklove.github.io/common_preferred_spread","deploy":"github-pages","stockLink":{"template":"?code={code}","accepts":"^[0-9A-Z]{6}$"},"embed":{"template":"?embed=1"},"themeParam":true,"handoff":true,"heldBadges":true},{"id":"spac-hunter","integrationKey":"spacHunter","name":"스팩 헌터","description":"SPAC 청산가치·합병 일정과 하방이 막힌 수익률","category":"equity","icon":"rocket","accent":"#0b7285","url":"https://ducklove.github.io/spac-hunter","deploy":"github-pages","stockLink":{"template":"?code={code}","accepts":"^[0-9A-Z]{6}$"},"embed":{"template":"?embed=1"},"themeParam":true,"handoff":true,"heldBadges":true},{"id":"buybacks","integrationKey":"buybacks","name":"자사주 분석","description":"자사주 매입·처분·소각 공시와 보유 비율 추이","category":"equity","icon":"buyback","accent":"#007f78","url":"https://ducklove.github.io/buybacks","deploy":"github-pages","stockLink":{"template":"?stock={code}","accepts":"^[0-9A-Z]{6}$"},"embed":{"template":"?embed=1"},"themeParam":true,"handoff":true,"heldBadges":true},{"id":"eiayn","integrationKey":"eiayn","name":"ETF 평가 (EIAYN)","description":"국내외 ETF 비용·추적·위험 평가와 랭킹","category":"etf","icon":"etf","accent":"#009b7d","url":"https://ducklove.github.io/eiayn","deploy":"github-pages","stockLink":{"template":"?code={code}","accepts":"^[A-Z0-9][A-Z0-9.-]{0,29}$"},"viewLink":{"template":"?view={view}","accepts":"^(list|ranking|analysis|compare)$"},"embed":{"template":"?embed=1"},"themeParam":true,"handoff":true,"heldBadges":true},{"id":"gold_gap","integrationKey":"goldGap","name":"김치프리미엄","description":"금·비트코인·USDT의 국내외 가격 괴리","category":"real-assets","icon":"coin","accent":"#2d66d6","url":"https://ducklove.github.io/gold_gap","deploy":"github-pages","assetLink":{"template":"?asset={asset}","accepts":"^[a-z][a-z0-9_]{0,31}$"},"embed":{"template":"?embed=1"},"themeParam":true,"handoff":false,"heldBadges":false},{"id":"all-about-gold","integrationKey":"allAboutGold","name":"All About Gold","description":"금 가격·수급·ETF·세제를 한곳에 모은 금 투자 리서치","category":"real-assets","icon":"gold","accent":"#b7791f","url":"https://ducklove.github.io/all-about-gold","deploy":"github-pages","viewLink":{"template":"#{view}","accepts":"^[a-z0-9][a-z0-9-]{0,39}$"},"embed":{"template":"?embed=1"},"themeParam":true,"handoff":false,"heldBadges":false},{"id":"nps-tracker","integrationKey":"npsTracker","name":"국민연금 포트폴리오","description":"국민연금 국내주식 보유 종목과 비중 변화 추적","category":"flows","icon":"pension","accent":"#2563eb","url":"https://ducklove.github.io/nps-tracker","deploy":"github-pages","stockLink":{"template":"?code={code}","accepts":"^[0-9A-Z]{6}$"},"hubView":"/nps","embed":{"template":"?embed=1"},"themeParam":true,"handoff":false,"heldBadges":false},{"id":"bond-mate","integrationKey":"bondMate","name":"채권·금리","description":"세계 국채 커브·정책금리·환율·회사채 스프레드","category":"macro","icon":"bond","accent":"#2563eb","url":"https://ducklove.github.io/bond-mate","deploy":"github-pages","viewLink":{"template":"?tab={view}","accepts":"^(overview|government|policy|fx|credit|issuance)$","labels":{"overview":"개요","government":"국채","policy":"기준금리","fx":"환율","credit":"신용","issuance":"발행"}},"hubView":"/bonds","embed":{"template":"?embed={view}"},"themeParam":true,"handoff":false,"heldBadges":false},{"id":"index-popup","name":"지수 위젯","description":"KOSPI·KOSDAQ 등 실시간 지수 미니 차트","category":"macro","icon":"chart","accent":"#2563eb","url":"https://ducklove.duckdns.org:3358","deploy":"self-hosted","viewLink":{"template":"?index={view}","accepts":"^[a-z0-9_-]{1,32}$"},"embed":{"template":"?headless=1"},"themeParam":true,"handoff":false,"heldBadges":false},{"id":"hub:screener","name":"밸류 스크리너","description":"P/E·P/B·ROE·배당수익률 조건으로 KOSPI/KOSDAQ 전체 스캔","category":"hub-tools","icon":"grid","accent":"#2563eb","url":"https://ducklove.duckdns.org:3691/screener","deploy":"hub","hubView":"/screener","themeParam":true,"handoff":false,"heldBadges":false},{"id":"hub:quant","name":"퀀트 운용실","description":"보통주·우선주 교체 전략 검증과 일별 신호 관찰","category":"hub-tools","icon":"gauge","accent":"#2563eb","url":"https://ducklove.duckdns.org:3691/quant","deploy":"hub","hubView":"/quant","themeParam":true,"handoff":false,"heldBadges":false},{"id":"hub:insights","name":"인사이트 보드","description":"외부 실험 결과와 수동 메모를 남기는 기록장","category":"hub-tools","icon":"chart","accent":"#2563eb","url":"https://ducklove.duckdns.org:3691/insights","deploy":"hub","hubView":"/insights","themeParam":true,"handoff":false,"heldBadges":false},{"id":"hub:masters","name":"투자 대가의 전략","description":"대표 투자 철학 비교와 참고용 자산배분 시뮬레이션","category":"hub-tools","icon":"compass","accent":"#2563eb","url":"https://ducklove.duckdns.org:3691/masters","deploy":"hub","hubView":"/masters","themeParam":true,"handoff":false,"heldBadges":false}]} /* vc:registry:end */;
  var VERSION = '1.1.0';
  var KEY = 'theme';
  var HUB_CODE = /^[0-9A-Z]{6}$/;
  var root = document.documentElement;
  var mq = window.matchMedia ? window.matchMedia('(prefers-color-scheme: dark)') : null;

  // ---- theme -------------------------------------------------------------
  function params() {
    try { return new URLSearchParams(window.location.search); } catch (_) { return new URLSearchParams(''); }
  }
  function valid(t) { return t === 'light' || t === 'dark' ? t : null; }
  function urlTheme() { return valid(params().get('theme')); }
  function stored() {
    try { return valid(window.localStorage.getItem(KEY)); } catch (_) { return null; }
  }
  function effective() { return urlTheme() || stored() || (mq && mq.matches ? 'dark' : 'light'); }
  function pageTheme() { return valid(root.getAttribute('data-theme')) || effective(); }
  function emit(theme) {
    document.dispatchEvent(new CustomEvent('vc:themechange', { detail: { theme: theme } }));
  }
  function applyTheme(theme) {
    var t = valid(theme) || effective();
    if (root.getAttribute('data-theme') !== t) root.setAttribute('data-theme', t);
    emit(t);
    return t;
  }
  function setTheme(t) {
    try {
      if (t === 'auto') window.localStorage.removeItem(KEY);
      else if (valid(t)) window.localStorage.setItem(KEY, t);
    } catch (_) { /* storage blocked: still apply for this page */ }
    var p = params();
    if (p.has('theme') && window.history && window.history.replaceState) {
      p.delete('theme');
      var q = p.toString();
      window.history.replaceState(window.history.state, '', window.location.pathname + (q ? '?' + q : '') + window.location.hash);
    }
    return applyTheme(valid(t));
  }
  window.addEventListener('storage', function (e) { if (e.key === KEY || e.key === null) applyTheme(); });
  if (mq) {
    var onScheme = function () { if (!urlTheme() && !stored()) applyTheme(); };
    if (mq.addEventListener) mq.addEventListener('change', onScheme);
    else if (mq.addListener) mq.addListener(onScheme);
  }
  window.addEventListener('message', function (e) {
    var d = e.data;
    if (!d || d.source !== 'vc' || d.type !== 'vc:theme' || !valid(d.theme)) return;
    try { if (e.origin !== new URL(REGISTRY.hub).origin) return; } catch (_) { return; }
    applyTheme(d.theme);
  });
  // Hosts without the pre-paint boot snippet may have no explicit theme yet.
  if (!valid(root.getAttribute('data-theme'))) root.setAttribute('data-theme', effective());

  // ---- registry & links ---------------------------------------------------
  function embedded() {
    var p = params(), em = p.get('embed'), framed;
    try { framed = window.top !== window.self; } catch (_) { framed = true; }
    return (em !== null && em !== '0' && em !== 'false') || p.get('headless') === '1' || p.get('vc-shell') === '0' || framed;
  }
  function toolById(id) {
    for (var i = 0; i < REGISTRY.tools.length; i++) if (REGISTRY.tools[i].id === id) return REGISTRY.tools[i];
    return null;
  }
  function currentToolId() {
    var el = document.querySelector('vc-shell[tool]');
    return el ? el.getAttribute('tool') : '';
  }
  function homeUrl(t) {
    return t.deploy === 'github-pages' && !/\/$/.test(t.url) ? t.url + '/' : t.url;
  }
  function fill(template, name, value) {
    return template.split('{' + name + '}').join(encodeURIComponent(value));
  }
  function deepLink(t, vars) {
    var pairs = [['stockLink', 'code', vars.code ? String(vars.code).trim().toUpperCase() : ''],
      ['viewLink', 'view', vars.view || ''], ['assetLink', 'asset', vars.asset || '']];
    for (var i = 0; i < pairs.length; i++) {
      var link = t[pairs[i][0]], value = pairs[i][2];
      if (!link || !value) continue;
      try { if (new RegExp(link.accepts).test(value)) return fill(link.template, pairs[i][1], value); } catch (_) { /* bad regex */ }
    }
    return '';
  }
  function linkTo(id, vars, from) {
    var t = toolById(id);
    if (!t) return null;
    var base = homeUrl(t), u;
    try { u = new URL(deepLink(t, vars || {}) || base, base); } catch (_) { return null; }
    if (u.protocol !== 'https:' && u.protocol !== 'http:') return null;
    if (t.themeParam !== false) u.searchParams.set('theme', pageTheme());
    var source = from === undefined ? currentToolId() : from;
    if (source === 'value-invest' && u.origin === hubOrigin()) {
      // Inside the hub, its own pages stay on the current origin (dev/staging hosts) and carry no from.
      var here = window.location;
      if ((here.protocol === 'https:' || here.protocol === 'http:') && here.origin !== u.origin) {
        u = new URL(u.pathname + u.search + u.hash, here.origin);
      }
      return u.href;
    }
    if (source) u.searchParams.set('from', source);
    return u.href;
  }
  function hubOrigin() {
    try { return new URL(REGISTRY.hub).origin; } catch (_) { return ''; }
  }
  function hubAnalysisUrl(code, from) {
    var c = String(code || '').trim().toUpperCase();
    return HUB_CODE.test(c) ? linkTo('value-invest', { code: c }, from) : null;
  }

  // ---- icons (single-colour inline sprite; currentColor) --------------------
  var ICONS = {
    building: 'M4 21V6l8-3 8 3v15M3 21h18M9 21v-4h6v4M8.5 9h.01M12 9h.01M15.5 9h.01M8.5 13h.01M12 13h.01M15.5 13h.01',
    split: 'M12 3v6M12 9l-6 6v6M12 9l6 6v6',
    rocket: 'M14 4c3.5 0 6 2.5 6 6l-7 7-6-6zM15 9h.01M7 17l-3 3M9.5 18.5L8 21M5.5 14.5L3 16',
    buyback: 'M4 12a8 8 0 0 1 14-5.3M19 3v4h-4M20 12a8 8 0 0 1-14 5.3M5 21v-4h4',
    etf: 'M12 3v9h9M20.5 15.5A9 9 0 1 1 8.5 3.7',
    gold: 'M3 20h18M5 20l2-6h10l2 6M8.5 14l1.5-5h4l1.5 5',
    coin: 'M12 3a9 9 0 1 0 0 18a9 9 0 1 0 0-18zM9.5 8h3.5a2 2 0 0 1 0 4H9.5h4a2 2 0 0 1 0 4H9.5zM11 6.5V8M11 16v1.5',
    pension: 'M12 3l8 3v6c0 4.5-3.4 8-8 9-4.6-1-8-4.5-8-9V6zM9 12l2 2 4-4',
    bond: 'M6 3h9l3 3v15H6zM9 16l6-6M9.5 10.5h.01M14.5 15.5h.01',
    chart: 'M4 4v16h16M7 15l4-4 3 3 5-6',
    compass: 'M12 3a9 9 0 1 0 0 18a9 9 0 1 0 0-18zM15.5 8.5l-2 5-5 2 2-5z',
    grid: 'M4 4h7v7H4zM13 4h7v7h-7zM4 13h7v7H4zM13 13h7v7h-7z',
    gauge: 'M4 17a8 8 0 1 1 16 0M12 17l4-5M3 20h18',
    server: 'M4 4h16v6H4zM4 14h16v6H4zM8 7h.01M8 17h.01'
  };
  function icon(name) {
    return '<svg class="i" viewBox="0 0 24 24" aria-hidden="true" focusable="false"><path d="' +
      (ICONS[name] || ICONS.grid) + '"/></svg>';
  }
  function esc(s) {
    return String(s == null ? '' : s).replace(/[&<>"']/g, function (c) {
      return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c];
    });
  }

  var CSS = [
    ':host{display:block;min-height:var(--vc-bar-h,40px);position:relative;z-index:40;',
    'font-family:var(--vc-font-sans,-apple-system,BlinkMacSystemFont,"Segoe UI","Apple SD Gothic Neo","Malgun Gothic",sans-serif);',
    '--_bg:var(--vc-surface,#fff);--_text:var(--vc-text,#1a1a1a);--_muted:var(--vc-text-muted,#666);',
    '--_border:var(--vc-border,#e0e0e0);--_brand:var(--vc-brand,#2563eb);--_hover:var(--vc-surface-muted,rgba(148,163,184,.10));',
    '--_ring:var(--vc-focus-ring,0 0 0 3px rgba(37,99,235,.15));--_shadow:var(--vc-shadow-md,0 4px 16px rgba(15,23,42,.08));',
    '--_overlay:var(--vc-overlay-bg,rgba(15,23,42,.48));color-scheme:light}',
    ':host([data-theme="dark"]){color-scheme:dark;--_bg:var(--vc-surface,#1e293b);--_text:var(--vc-text,#e2e8f0);',
    '--_muted:var(--vc-text-muted,#94a3b8);--_border:var(--vc-border,#334155);--_brand:var(--vc-brand,#3b82f6);',
    '--_hover:var(--vc-surface-muted,rgba(148,163,184,.14));--_ring:var(--vc-focus-ring,0 0 0 3px rgba(96,165,250,.28));',
    '--_shadow:var(--vc-shadow-md,0 4px 16px rgba(0,0,0,.40));--_overlay:var(--vc-overlay-bg,rgba(0,0,0,.55))}',
    ':host([hidden]){display:none}',
    '*{box-sizing:border-box}',
    '.bar{position:relative;display:flex;align-items:center;gap:6px;height:var(--vc-bar-h,40px);padding:0 12px;min-width:0;',
    'background:var(--_bg);color:var(--_text);border-bottom:1px solid var(--_border);font-size:13px;line-height:1}',
    'a,button{color:inherit;font:inherit;text-decoration:none;border-radius:6px}',
    'a:focus-visible,button:focus-visible{outline:none;box-shadow:var(--_ring)}',
    '.brand{display:inline-flex;align-items:center;gap:6px;padding:4px 6px;font-weight:700;white-space:nowrap;flex:none}',
    '.mark{display:inline-grid;place-items:center;width:20px;height:20px;border-radius:5px;background:var(--_brand);color:#fff;font-size:12px;font-weight:800}',
    '.sep{color:var(--_muted);flex:none}',
    '.switch{min-width:0}',
    'button{background:none;border:0;cursor:pointer;padding:0}',
    '.current{display:inline-flex;align-items:center;gap:6px;max-width:100%;min-width:0;padding:6px 8px;font-weight:600}',
    '.current:hover,.brand:hover,.stock:hover,.theme:hover{background:var(--_hover)}',
    '.name{overflow:hidden;text-overflow:ellipsis;white-space:nowrap;min-width:0}',
    '.caret{color:var(--_muted);font-size:10px;flex:none}',
    '.i{width:16px;height:16px;flex:none;fill:none;stroke:currentColor;stroke-width:1.8;stroke-linecap:round;stroke-linejoin:round}',
    '.spacer{flex:1 1 0;min-width:0}',
    '.stock{display:inline-flex;align-items:center;gap:6px;padding:5px 10px;border:1px solid var(--_border);border-radius:999px;',
    'white-space:nowrap;min-width:0;max-width:50%;overflow:hidden;text-overflow:ellipsis;color:var(--_brand);font-weight:600}',
    '.stock-name{color:var(--_text);overflow:hidden;text-overflow:ellipsis}',
    '.short{display:none}',
    '.theme{display:inline-grid;place-items:center;width:32px;height:32px;flex:none}',
    '.menu{position:absolute;top:calc(100% + 4px);left:12px;width:min(560px,calc(100vw - 24px));max-height:min(75vh,640px);overflow:auto;',
    'padding:8px;background:var(--_bg);color:var(--_text);border:1px solid var(--_border);border-radius:12px;box-shadow:var(--_shadow);',
    'display:block}',
    '.cols{column-count:2;column-gap:12px}',
    '.menu[hidden]{display:none}',
    '.group{display:flex;flex-direction:column;gap:2px;min-width:0;break-inside:avoid;padding-bottom:4px}',
    '.label{padding:8px 8px 4px;font-size:11px;font-weight:700;color:var(--_muted);letter-spacing:.02em}',
    '.item{display:flex;align-items:flex-start;gap:8px;padding:8px;min-height:40px;border-radius:8px}',
    '.item:hover,.item:focus{background:var(--_hover);outline:none}',
    '.item[aria-current]{background:var(--_hover);box-shadow:inset 3px 0 0 var(--_accent,var(--_brand))}',
    '.item .i{margin-top:1px;color:var(--_accent,var(--_brand))}',
    '.t{display:flex;flex-direction:column;gap:3px;min-width:0}',
    '.n{font-weight:600;font-size:13px;line-height:1.2}',
    '.d{font-size:11.5px;line-height:1.35;color:var(--_muted)}',
    '.scrim{display:none}',
    // variant="menu": only the switch button + the same popover (e.g. inside a host header).
    ':host([variant="menu"]){display:inline-block;min-height:0;vertical-align:middle}',
    '.compact{display:inline-flex;align-items:center;gap:4px;min-height:36px;padding:0 10px;background:var(--_bg);',
    'color:var(--_text);border:1px solid var(--_border);border-radius:8px}',
    '.compact:hover{background:var(--_bg);border-color:var(--_brand)}',
    ':host([variant="menu"]) .menu{left:auto;right:0}',
    '@media (max-width:600px){.bar{padding:0 8px;gap:4px}.brand-text,.stock-long{display:none}.short{display:inline}',
    '.stock{max-width:none;padding:5px 8px}',
    '.menu{position:fixed;top:auto;left:0!important;right:0;bottom:0;width:auto;max-height:75vh;border-radius:14px 14px 0 0;',
    'padding:8px 8px calc(8px + env(safe-area-inset-bottom,0px));z-index:2}.cols{column-count:1}',
    '.item{min-height:44px;align-items:center}.current,.theme,.brand{min-height:44px}',
    '.scrim:not([hidden]){display:block;position:fixed;inset:0;background:var(--_overlay);z-index:1}}',
    '@media print{:host{display:none!important}}'
  ].join('');

  var instances = [];
  function refreshAll() { for (var i = 0; i < instances.length; i++) instances[i].refresh(); }
  new MutationObserver(refreshAll).observe(root, { attributes: true, attributeFilter: ['data-theme'] });

  // ---- <vc-shell> ------------------------------------------------------------
  var VCShellElement = function () {};
  if (window.HTMLElement && window.customElements) {
    VCShellElement = class extends HTMLElement {
      static get observedAttributes() { return ['tool', 'variant', 'stock', 'stock-name', 'theme-toggle']; }
      connectedCallback() {
        if (embedded()) { this.hidden = true; return; }
        if (!this.shadowRoot) {
          this.attachShadow({ mode: 'open' });
          this.shadowRoot.addEventListener('click', this._onClick.bind(this));
          this.shadowRoot.addEventListener('keydown', this._onKey.bind(this));
        }
        this._outside = this._outside || this._onOutside.bind(this);
        document.addEventListener('click', this._outside);
        if (instances.indexOf(this) < 0) instances.push(this);
        this.render();
      }
      disconnectedCallback() {
        document.removeEventListener('click', this._outside);
        var i = instances.indexOf(this);
        if (i >= 0) instances.splice(i, 1);
      }
      attributeChangedCallback() { if (this.shadowRoot && !this.hidden) this.render(); }
      _vars() { return { code: this.getAttribute('stock') || '' }; }
      _href(id) {
        return linkTo(id, id === 'value-invest' ? {} : this._vars(), this.getAttribute('tool') || '');
      }
      render() {
        var self = this, toolId = this.getAttribute('tool') || '', current = toolById(toolId);
        var stock = this.getAttribute('stock') || '', stockName = this.getAttribute('stock-name') || '';
        var analysis = hubAnalysisUrl(stock, toolId);
        var groups = REGISTRY.categories.map(function (cat) {
          var items = REGISTRY.tools.filter(function (t) { return t.category === cat.id; }).map(function (t) {
            return '<a class="item" role="menuitem" tabindex="-1" data-tool="' + esc(t.id) + '" href="' + esc(self._href(t.id)) + '"' +
              (t.id === toolId ? ' aria-current="page"' : '') + ' style="--_accent:' + esc(t.accent) + '">' + icon(t.icon) +
              '<span class="t"><span class="n">' + esc(t.name) + '</span><span class="d">' + esc(t.description) + '</span></span></a>';
          }).join('');
          return items ? '<div class="group" role="group" aria-label="' + esc(cat.label) + '"><div class="label" aria-hidden="true">' +
            esc(cat.label) + '</div>' + items + '</div>' : '';
        }).join('');
        this.setAttribute('data-theme', pageTheme());
        var popover = '<div class="scrim" hidden></div><div class="menu" id="vc-menu" role="menu" aria-label="도구 전환" hidden>' +
          '<div class="cols">' + groups + '</div></div>';
        if (this.getAttribute('variant') === 'menu') {
          this.shadowRoot.innerHTML = '<style>' + CSS + '</style>' +
            '<div class="switch"><button class="current compact" type="button" aria-haspopup="menu" aria-expanded="false"' +
            ' aria-controls="vc-menu" aria-label="Value Compass 도구 전환" title="도구 전환">' + icon('grid') +
            '<span class="caret" aria-hidden="true">▾</span></button>' + popover + '</div>';
          this.refresh();
          return;
        }
        this.shadowRoot.innerHTML = '<style>' + CSS + '</style>' +
          '<nav class="bar" aria-label="Value Compass 도구 이동">' +
          '<a class="brand" data-tool="value-invest" href="' + esc(this._href('value-invest')) + '" aria-label="Value Compass 허브">' +
          '<span class="mark" aria-hidden="true">V</span><span class="brand-text">Value Compass</span></a>' +
          '<span class="sep" aria-hidden="true">›</span>' +
          '<div class="switch"><button class="current" type="button" aria-haspopup="menu" aria-expanded="false" aria-controls="vc-menu">' +
          icon(current ? current.icon : 'grid') + '<span class="name">' + esc(current ? current.name : '도구') + '</span>' +
          '<span class="caret" aria-hidden="true">▾</span></button>' +
          popover + '</div>' +
          '<span class="spacer"></span>' +
          (analysis ? '<a class="stock" data-stock-link href="' + esc(analysis) + '" title="허브 종목분석에서 열기">' +
            '<span class="stock-long">' + (stockName ? '<span class="stock-name">' + esc(stockName) + '</span> → ' : '') +
            '허브에서 분석 ↗</span><span class="short">분석 ↗</span></a>' : '') +
          (this.hasAttribute('theme-toggle') ? '<button class="theme" type="button" data-theme-toggle></button>' : '') +
          '</nav>';
        this.refresh();
      }
      refresh() {
        var sr = this.shadowRoot;
        if (!sr || this.hidden) return;
        var theme = pageTheme(), self = this;
        this.setAttribute('data-theme', theme);
        Array.prototype.forEach.call(sr.querySelectorAll('a[data-tool]'), function (a) {
          var href = self._href(a.getAttribute('data-tool'));
          if (href) a.setAttribute('href', href);
        });
        var stock = sr.querySelector('a[data-stock-link]');
        var analysis = stock && hubAnalysisUrl(this.getAttribute('stock'), this.getAttribute('tool') || '');
        if (analysis) stock.setAttribute('href', analysis);
        var btn = sr.querySelector('[data-theme-toggle]');
        if (btn) {
          var label = theme === 'dark' ? '라이트 모드로 전환' : '다크 모드로 전환';
          btn.setAttribute('aria-label', label);
          btn.setAttribute('title', label);
          btn.innerHTML = '<svg class="i" viewBox="0 0 24 24" aria-hidden="true" focusable="false"><circle cx="12" cy="12" r="8.5"/>' +
            '<path d="M12 3.5a8.5 8.5 0 0 1 0 17z" fill="currentColor"/></svg>';
        }
      }
      _parts() {
        var sr = this.shadowRoot;
        return { button: sr.querySelector('.current'), menu: sr.querySelector('.menu'), scrim: sr.querySelector('.scrim') };
      }
      _items() { return Array.prototype.slice.call(this.shadowRoot.querySelectorAll('[role="menuitem"]')); }
      isOpen() { var p = this._parts(); return !!(p.menu && !p.menu.hidden); }
      open(focus) {
        var p = this._parts(), items;
        if (!p.menu) return;
        this.refresh();
        p.menu.hidden = false;
        this._place(p);
        p.scrim.hidden = false;
        p.button.setAttribute('aria-expanded', 'true');
        this.setAttribute('open', '');
        items = this._items();
        var start = items.filter(function (a) { return a.hasAttribute('aria-current'); })[0] || items[0];
        if (focus === 'last') start = items[items.length - 1];
        if (start) start.focus();
      }
      _place(p) {
        // Desktop: align under the trigger but keep the popover inside the viewport.
        // (The mobile bottom sheet is position:fixed and ignores this offset.)
        var barEl = this.shadowRoot.querySelector('.bar');
        if (!barEl) { p.menu.style.left = ''; return; } // variant="menu": CSS anchors it to the button's right edge
        var bar = barEl.getBoundingClientRect();
        var btn = p.button.getBoundingClientRect(), vw = window.innerWidth || 0;
        var width = p.menu.offsetWidth || 0, left = btn.left - bar.left;
        if (vw && width) left = Math.min(left, vw - bar.left - width - 12);
        p.menu.style.left = Math.max(12, left) + 'px';
      }
      close(returnFocus) {
        var p = this._parts();
        if (!p.menu || p.menu.hidden) return;
        p.menu.hidden = true;
        p.scrim.hidden = true;
        p.button.setAttribute('aria-expanded', 'false');
        this.removeAttribute('open');
        if (returnFocus) p.button.focus();
      }
      _onOutside(e) {
        var path = e.composedPath ? e.composedPath() : [];
        if (this.isOpen() && path.indexOf(this) < 0) this.close(false);
      }
      _onClick(e) {
        var target = e.target.closest ? e.target.closest('a,button,.scrim') : null;
        if (!target) return;
        if (target.classList.contains('scrim')) { this.close(true); return; }
        if (target.classList.contains('current')) { if (this.isOpen()) this.close(true); else this.open(); return; }
        if (target.hasAttribute('data-theme-toggle')) { setTheme(pageTheme() === 'dark' ? 'light' : 'dark'); return; }
        var href = target.hasAttribute('data-tool') ? this._href(target.getAttribute('data-tool'))
          : target.hasAttribute('data-stock-link') ? hubAnalysisUrl(this.getAttribute('stock'), this.getAttribute('tool') || '') : null;
        if (href) target.setAttribute('href', href);
        if (target.getAttribute('role') === 'menuitem') this.close(false);
      }
      _onKey(e) {
        var key = e.key, active = this.shadowRoot.activeElement, items, i;
        if (active && active.classList.contains('current') && (key === 'ArrowDown' || key === 'ArrowUp')) {
          e.preventDefault();
          this.open(key === 'ArrowUp' ? 'last' : 'first');
          return;
        }
        if (!this.isOpen()) return;
        if (key === 'Escape') { e.preventDefault(); this.close(true); return; }
        if (key === 'Tab') { this.close(false); return; }
        items = this._items();
        i = items.indexOf(active);
        if (key === 'ArrowDown' || key === 'ArrowRight') i = (i + 1) % items.length;
        else if (key === 'ArrowUp' || key === 'ArrowLeft') i = (i - 1 + items.length) % items.length;
        else if (key === 'Home') i = 0;
        else if (key === 'End') i = items.length - 1;
        else return;
        e.preventDefault();
        if (items[i]) items[i].focus();
      }
    };
    if (!window.customElements.get('vc-shell')) window.customElements.define('vc-shell', VCShellElement);
  }

  function setStock(code, name) {
    Array.prototype.forEach.call(document.querySelectorAll('vc-shell'), function (el) {
      if (code) el.setAttribute('stock', String(code)); else el.removeAttribute('stock');
      if (code && name) el.setAttribute('stock-name', String(name)); else el.removeAttribute('stock-name');
    });
  }

  window.VCShell = {
    version: VERSION,
    registry: REGISTRY,
    tools: REGISTRY.tools,
    getTheme: effective,
    setTheme: setTheme,
    setStock: setStock,
    linkTo: function (id, vars) { return linkTo(id, vars); },
    icon: function (name) { return icon(name); },
    hubAnalysisUrl: function (code) { return hubAnalysisUrl(code); }
  };
})();
