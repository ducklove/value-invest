// --- WebSocket Quote Manager ---
const QuoteManager = {
  ws: null,
  connected: false,
  reconnectTimer: null,
  subscriptions: {},
  wsCodes: new Set(),
  overflowCodes: [],
  overflowTimer: null,
  generalPollTimer: null,
  wsActive: false,      // true when this session owns the active WS slot
  kisConnected: false,  // true only when the server has an upstream KIS WS
  onQuote: null,
  onStatus: null,

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
          this.wsCodes = new Set(msg.ws || []);
          this.overflowCodes = msg.rest || [];
          this._notifyStatus(msg);
          // Refresh ALL codes immediately (ws + overflow)
          const allCodes = [...(msg.ws || []), ...(msg.rest || [])];
          this._fetchInitialQuotes(allCodes);
          this._startOverflowPolling();
        }
        else if (msg.type === 'ws_status') {
          const hadActive = Object.prototype.hasOwnProperty.call(msg, 'active');
          const hadKisStatus = Object.prototype.hasOwnProperty.call(msg, 'kis_connected');
          const wasStreaming = this.isStreaming();
          if (hadActive) this.wsActive = Boolean(msg.active);
          if (hadKisStatus) this.kisConnected = Boolean(msg.kis_connected);
          const streamingChanged = wasStreaming !== this.isStreaming();
          this._notifyStatus(msg);
          if (streamingChanged && !this.isStreaming()) this._pollAll();
          if (msg.active) {
            // We are now the active subscriber
            this._sendSubscriptions();
          } else if (msg.occupied) {
            // Slot busy — request takeover unconditionally; server kicks the oldest session
            if (this.connected && this.ws) {
              this.ws.send(JSON.stringify({ action: 'takeover' }));
            }
          }
        }
        else if (msg.type === 'ws_taken_over') {
          // Another session took over — fall back to polling
          this.wsActive = false;
          this.kisConnected = false;
          this._notifyStatus(msg);
          this._showTakenOverBanner();
        }
      } catch (e) { console.warn(e); }
    };
    this.ws.onclose = (ev) => {
      this.connected = false;
      this.wsActive = false;
      this.kisConnected = false;
      this.ws = null;
      this._notifyStatus({ type: 'ws_closed', code: ev.code });
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
    this.kisConnected = false;
    this._notifyStatus({ type: 'ws_disconnected' });
  },

  _scheduleReconnect() {
    if (this.reconnectTimer) return;
    this.reconnectTimer = setTimeout(() => { this.reconnectTimer = null; this.connect(); }, 5000);
  },

  _showTakenOverBanner() {
    // Brief visual notification
    const banner = document.createElement('div');
    banner.textContent = '다른 세션이 실시간 시세를 가져갔습니다. 1분 간격 폴링으로 전환됩니다.';
    banner.style.cssText = 'position:fixed;top:0;left:0;right:0;padding:10px;background:#e67e22;color:white;text-align:center;z-index:9999;font-size:13px;';
    document.body.appendChild(banner);
    setTimeout(() => banner.remove(), 5000);
  },

  isStreaming() { return this.wsActive && this.kisConnected; },

  isLive(code) { return this.isStreaming() && this.wsCodes.has(code); },

  _notifyStatus(message = {}) {
    if (!this.onStatus) return;
    this.onStatus({
      ...message,
      browserConnected: this.connected,
      wsActive: this.wsActive,
      kisConnected: this.kisConnected,
      streaming: this.isStreaming(),
    });
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
    // 전 종목 병렬 요청. 각 요청은 개별 HTTP 로 독립적이라 응답 도착
    // 순서대로 onQuote → 해당 행만 즉시 UI 반영 (progressive loading).
    // 한꺼번에 발사하지만 upstream 자체 세마포어 (_YF_SEM, _NAVER_SEM
    // 등) 가 안쪽에서 동시성 제어.
    await Promise.all(codes.map(code =>
      apiFetch(`/api/asset-quote/${encodeURIComponent(code)}`)
        .then(async resp => {
          if (!resp.ok) return;
          const q = await resp.json();
          if (q && q.price != null && this.onQuote) {
            this.onQuote(code, { code, ...q });
          }
        })
        .catch(() => { /* per-code 실패 무시, 다른 종목 영향 없음 */ })
    ));
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
    // Always refresh ALL codes in the background — initial load may
    // have stale last-known prices that need updating.
    await this._fetchQuotes(wsCodes);
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
    if (this.isStreaming()) {
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
