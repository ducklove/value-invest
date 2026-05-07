// --- WebSocket Quote Manager ---
const QUOTE_MANAGER_STALE_WS_MS = 90_000;
const QUOTE_MANAGER_GENERAL_POLL_MS = 60_000;
const QUOTE_MANAGER_OVERFLOW_POLL_MS = 30_000;
const QUOTE_MANAGER_RETRY_MS = 5_000;

const QuoteManager = {
  ws: null,
  connected: false,
  reconnectTimer: null,
  subscriptions: {},
  wsCodes: new Set(),
  overflowCodes: [],
  lastQuoteAt: {},
  overflowTimer: null,
  generalPollTimer: null,
  wsActive: false,      // true when this session owns the active WS slot
  onQuote: null,

  connect() {
    if (this.ws) return;
    const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
    const url = `${proto}//${location.host}/ws/quotes`;
    try { this.ws = new WebSocket(url); } catch { this._scheduleReconnect(); return; }
    this.ws.onopen = () => {
      this.connected = true;
      this.ws.send(JSON.stringify({ action: 'takeover' }));
    };
    this.ws.onmessage = (event) => {
      try {
        const msg = JSON.parse(event.data);
        if (msg.type === 'quote' && msg.code) {
          if (msg.price != null) this._markQuoteFresh(msg.code);
          if (this.onQuote) this.onQuote(msg.code, msg);
        } else if (msg.type === 'subscriptions') {
          this.wsCodes = new Set(msg.ws || []);
          this.overflowCodes = msg.rest || [];
          const allCodes = [...(msg.ws || []), ...(msg.rest || [])];
          this._fetchInitialQuotes(allCodes);
          this._startOverflowPolling();
        } else if (msg.type === 'ws_status') {
          if (msg.active) {
            this.wsActive = true;
            this._sendSubscriptions();
          } else if (msg.occupied && this.connected && this.ws) {
            this.ws.send(JSON.stringify({ action: 'takeover' }));
          }
        } else if (msg.type === 'ws_taken_over') {
          this.wsActive = false;
          this._showTakenOverBanner();
        }
      } catch (e) { console.warn(e); }
    };
    this.ws.onclose = (ev) => {
      this.connected = false;
      this.wsActive = false;
      this.ws = null;
      if (ev.code === 4001) return;
      this._scheduleReconnect();
    };
    this.ws.onerror = () => {};
    this._startGeneralPolling();
  },

  disconnect() {
    if (this.reconnectTimer) { clearTimeout(this.reconnectTimer); this.reconnectTimer = null; }
    if (this.overflowTimer) { clearInterval(this.overflowTimer); this.overflowTimer = null; }
    if (this.generalPollTimer) { clearInterval(this.generalPollTimer); this.generalPollTimer = null; }
    if (this.ws) { this.ws.close(); this.ws = null; }
    this.connected = false;
    this.wsActive = false;
    this.wsCodes = new Set();
    this.overflowCodes = [];
    this.lastQuoteAt = {};
  },

  _scheduleReconnect() {
    if (this.reconnectTimer) return;
    this.reconnectTimer = setTimeout(() => { this.reconnectTimer = null; this.connect(); }, 5000);
  },

  _showTakenOverBanner() {
    const banner = document.createElement('div');
    banner.textContent = '다른 세션이 실시간 시세 연결을 가져갔습니다. 1분 간격 폴링으로 전환합니다.';
    banner.style.cssText = 'position:fixed;top:0;left:0;right:0;padding:10px;background:#e67e22;color:white;text-align:center;z-index:9999;font-size:13px;';
    document.body.appendChild(banner);
    setTimeout(() => banner.remove(), 5000);
  },

  isLive(code) { return this.wsActive && this.wsCodes.has(code); },

  updateSubscriptions(requested) {
    this.subscriptions = requested;
    this._sendSubscriptions();
  },

  _sendSubscriptions() {
    if (!this.connected || !this.ws || !this.wsActive) return;
    this.ws.send(JSON.stringify({ action: 'subscribe', requested: this.subscriptions }));
  },

  _retryTimer: null,

  _markQuoteFresh(code) {
    if (!code) return;
    this.lastQuoteAt[code] = Date.now();
  },

  _getStaleWsCodes() {
    const now = Date.now();
    return [...this.wsCodes].filter(code =>
      now - (this.lastQuoteAt[code] || 0) >= QUOTE_MANAGER_STALE_WS_MS
    );
  },

  async _fetchQuotes(codes) {
    const uniqueCodes = [...new Set((codes || []).filter(Boolean))];
    if (!uniqueCodes.length) return;
    await Promise.all(uniqueCodes.map(code =>
      apiFetch(`/api/asset-quote/${encodeURIComponent(code)}`)
        .then(async resp => {
          if (!resp.ok) return;
          const q = await resp.json();
          if (q && q.price != null) {
            this._markQuoteFresh(code);
            if (this.onQuote) this.onQuote(code, { code, ...q });
          }
        })
        .catch(() => { /* Keep one bad quote from blocking the rest. */ })
    ));
    this._scheduleRetry();
  },

  _getMissingCodes() {
    const missing = new Set();
    for (const i of portfolioItems) {
      if (!i.quote || i.quote.price == null) missing.add(i.stock_code);
    }
    if (typeof recentListItems !== 'undefined' && Array.isArray(recentListItems)) {
      for (const i of recentListItems) {
        if (!i.quote || i.quote.price == null) missing.add(i.stock_code);
      }
    }
    return [...missing];
  },

  _scheduleRetry() {
    if (this._retryTimer) return;
    const missing = this._getMissingCodes();
    if (!missing.length) return;
    this._retryTimer = setTimeout(async () => {
      this._retryTimer = null;
      const still = this._getMissingCodes();
      if (still.length) await this._fetchQuotes(still);
    }, QUOTE_MANAGER_RETRY_MS);
  },

  async _fetchInitialQuotes(wsCodes) {
    await this._fetchQuotes(wsCodes);
  },

  async _pollOverflow() {
    await this._fetchQuotes(this.overflowCodes);
  },

  _startOverflowPolling() {
    if (this.overflowTimer) clearInterval(this.overflowTimer);
    if (!this.overflowCodes.length) return;
    this._pollOverflow();
    this.overflowTimer = setInterval(() => this._pollOverflow(), QUOTE_MANAGER_OVERFLOW_POLL_MS);
  },

  _startGeneralPolling() {
    if (this.generalPollTimer) clearInterval(this.generalPollTimer);
    this.generalPollTimer = setInterval(() => this._pollAll(), QUOTE_MANAGER_GENERAL_POLL_MS);
  },

  async _pollAll() {
    const allCodes = new Set();
    if (this.wsActive) {
      this.overflowCodes.forEach(c => allCodes.add(c));
      this._getStaleWsCodes().forEach(c => allCodes.add(c));
    } else {
      for (const codes of Object.values(this.subscriptions)) {
        for (const c of codes) allCodes.add(c);
      }
    }
    if (allCodes.size) await this._fetchQuotes([...allCodes]);
  },
};
