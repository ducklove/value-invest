// --- WebSocket Quote Manager ---
const QuoteManager = {
  ws: null,
  connected: false,
  reconnectTimer: null,
  subscriptions: {},
  overflowCodes: [],
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
      // Request takeover immediately (server will check if occupied)
      this.ws.send(JSON.stringify({ action: 'takeover' }));
    };
    this.ws.onmessage = (event) => {
      try {
        const msg = JSON.parse(event.data);
        if (msg.type === 'quote' && msg.code && this.onQuote) this.onQuote(msg.code, msg);
        else if (msg.type === 'subscriptions') {
          this.overflowCodes = msg.rest || [];
          this._fetchInitialQuotes(msg.ws || []);
          this._startOverflowPolling();
        }
        else if (msg.type === 'ws_status') {
          if (msg.occupied && !msg.active) {
            // Another session has the WS — ask user
            this._promptTakeover();
          } else if (msg.active) {
            // We are now the active subscriber
            this.wsActive = true;
            this._sendSubscriptions();
          }
        }
        else if (msg.type === 'ws_taken_over') {
          // Another session took over — fall back to polling
          this.wsActive = false;
          this._showTakenOverBanner();
        }
      } catch (e) { console.warn(e); }
    };
    this.ws.onclose = (ev) => {
      this.connected = false;
      this.wsActive = false;
      this.ws = null;
      // Don't reconnect if we were kicked by takeover
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
  },

  _scheduleReconnect() {
    if (this.reconnectTimer) return;
    this.reconnectTimer = setTimeout(() => { this.reconnectTimer = null; this.connect(); }, 5000);
  },

  _promptTakeover() {
    if (confirm('다른 세션에서 실시간 시세를 사용 중입니다.\n이 세션에서 실시간 시세를 사용하시겠습니까?\n\n(취소 시 1분 간격 폴링으로 동작합니다)')) {
      if (this.connected && this.ws) {
        this.ws.send(JSON.stringify({ action: 'takeover' }));
      }
    }
    // If cancelled, keep polling-only mode (generalPollTimer handles it)
  },

  _showTakenOverBanner() {
    // Brief visual notification
    const banner = document.createElement('div');
    banner.textContent = '다른 세션이 실시간 시세를 가져갔습니다. 1분 간격 폴링으로 전환됩니다.';
    banner.style.cssText = 'position:fixed;top:0;left:0;right:0;padding:10px;background:#e67e22;color:white;text-align:center;z-index:9999;font-size:13px;';
    document.body.appendChild(banner);
    setTimeout(() => banner.remove(), 5000);
  },

  updateSubscriptions(requested) {
    this.subscriptions = requested;
    this._sendSubscriptions();
  },

  _sendSubscriptions() {
    if (!this.connected || !this.ws || !this.wsActive) return;
    this.ws.send(JSON.stringify({ action: 'subscribe', requested: this.subscriptions }));
  },

  _retryTimer: null,

  async _fetchQuotes(codes) {
    if (!codes.length) return;
    try {
      const resp = await apiFetch('/api/asset-quotes', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ codes }),
      });
      if (!resp.ok) return;
      const results = await resp.json();
      for (const [code, q] of Object.entries(results)) {
        if (q && q.price != null && this.onQuote) {
          this.onQuote(code, { code, price: q.price, change: q.change, change_pct: q.change_pct, previous_close: q.previous_close, date: q.date });
        }
      }
    } catch (e) { console.warn(e); }
    // Schedule fast retry for any still-missing quotes
    this._scheduleRetry();
  },

  _getMissingCodes() {
    // Check portfolio + sidebar (recent search) items, not benchmark/index codes
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
    // Retry missing codes in 5 seconds
    this._retryTimer = setTimeout(async () => {
      this._retryTimer = null;
      const still = this._getMissingCodes();
      if (still.length) await this._fetchQuotes(still);
    }, 5000);
  },

  async _fetchInitialQuotes(wsCodes) {
    const needsFetch = wsCodes.filter(code => {
      const pf = portfolioItems.find(i => i.stock_code === code);
      if (pf && pf.quote && pf.quote.price != null) return false;
      return true;
    });
    await this._fetchQuotes(needsFetch);
  },

  async _pollOverflow() {
    await this._fetchQuotes(this.overflowCodes);
  },

  _startOverflowPolling() {
    if (this.overflowTimer) clearInterval(this.overflowTimer);
    if (!this.overflowCodes.length) return;
    this._pollOverflow();
    this.overflowTimer = setInterval(() => this._pollOverflow(), 30_000);
  },

  // 60초 간격 전체 폴링 — WS 활성 여부와 무관하게 항상 동작
  _startGeneralPolling() {
    if (this.generalPollTimer) clearInterval(this.generalPollTimer);
    this.generalPollTimer = setInterval(() => this._pollAll(), 60_000);
  },

  async _pollAll() {
    const allCodes = new Set();
    if (this.wsActive) {
      // WS 활성: overflow 코드만
      this.overflowCodes.forEach(c => allCodes.add(c));
    } else {
      // WS 비활성: 모든 구독 코드 (benchmark 포함)
      for (const codes of Object.values(this.subscriptions)) {
        for (const c of codes) allCodes.add(c);
      }
    }
    if (allCodes.size) await this._fetchQuotes([...allCodes]);
  },
};
