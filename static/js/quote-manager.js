// --- WebSocket Quote Manager ---
const QUOTE_MANAGER_STALE_WS_MS = 55_000;
const QUOTE_MANAGER_GENERAL_POLL_MS = 60_000;
const QUOTE_MANAGER_OVERFLOW_POLL_MS = 30_000;
const QUOTE_MANAGER_RETRY_MS = 5_000;
// The backend /api/asset-quotes pulls all domestic (KRX) codes in one bulk
// upstream call, so larger client batches mean fewer round-trips (≈ one
// request for a typical portfolio) instead of one request per 4 codes.
const QUOTE_MANAGER_BATCH_SIZE = 30;
const QUOTE_MANAGER_BATCH_PARALLEL = 1;
const QUOTE_MANAGER_PRIORITY_CODES = new Set(['A200', 'A200.AX', 'EUN2', 'EUN2.DE']);
const QUOTE_MANAGER_MANUAL_WS_KEY = 'quote_manager_manual_ws_enabled';

const QuoteManager = {
  ws: null,
  connected: false,
  reconnectTimer: null,
  subscriptions: {},
  wsCodes: new Set(),
  overflowCodes: [],
  lastWsQuoteAt: {},
  overflowTimer: null,
  generalPollTimer: null,
  wsActive: false,      // true when this session owns the active WS slot
  desiredActive: false,
  manualControlAllowed: false,
  serverCanTakeover: null,
  lastStatus: 'offline',
  lastSlotMeta: null,
  onQuote: null,
  inflightCodes: new Set(),

  _loadDesiredActive() {
    try { return sessionStorage.getItem(QUOTE_MANAGER_MANUAL_WS_KEY) === '1'; } catch { return false; }
  },

  _saveDesiredActive() {
    try {
      if (this.desiredActive) sessionStorage.setItem(QUOTE_MANAGER_MANUAL_WS_KEY, '1');
      else sessionStorage.removeItem(QUOTE_MANAGER_MANUAL_WS_KEY);
    } catch (e) {}
  },

  setManualControlAllowed(allowed) {
    const nextAllowed = !!allowed;
    this.manualControlAllowed = nextAllowed;
    if (nextAllowed) {
      this.desiredActive = this._loadDesiredActive();
    } else {
      const shouldRelease = this.wsActive || this.desiredActive;
      this.desiredActive = false;
      this._saveDesiredActive();
      if (shouldRelease && this.connected && this.ws) {
        try { this.ws.send(JSON.stringify({ action: 'release' })); } catch (e) {}
      }
      this._deactivateWsSlot();
    }
    this._syncControlUi();
  },

  connect() {
    if (this.manualControlAllowed && this._loadDesiredActive()) {
      this.desiredActive = true;
    }
    if (this.ws) {
      this._syncControlUi();
      return;
    }
    const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
    const url = `${proto}//${location.host}/ws/quotes`;
    try { this.ws = new WebSocket(url); } catch { this._scheduleReconnect(); return; }
    this.lastStatus = this.desiredActive ? 'connecting' : 'polling';
    this._syncControlUi();
    this.ws.onopen = () => {
      this.connected = true;
      this.lastStatus = this.desiredActive ? 'connecting' : 'polling';
      this._syncControlUi();
      if (this.desiredActive) this._requestActiveSlot();
    };
    this.ws.onmessage = (event) => {
      try {
        const msg = JSON.parse(event.data);
        if (msg.type === 'quote' && msg.code) {
          if (msg.price != null) this._markWsQuoteFresh(msg.code);
          if (this.onQuote) this.onQuote(msg.code, msg);
        } else if (msg.type === 'subscriptions') {
          this.wsCodes = new Set(msg.ws || []);
          this.overflowCodes = msg.rest || [];
          const allCodes = [...(msg.ws || []), ...(msg.rest || [])];
          this._fetchInitialQuotes(allCodes);
          this._startOverflowPolling();
        } else if (msg.type === 'ws_status') {
          this.lastSlotMeta = msg;
          this.serverCanTakeover = msg.can_takeover !== false;
          if (msg.active) {
            this.wsActive = true;
            this.lastStatus = 'active';
            if (this.manualControlAllowed) {
              this.desiredActive = true;
              this._saveDesiredActive();
            }
            this._sendSubscriptions();
          } else {
            this._deactivateWsSlot();
            if (msg.released || msg.forbidden) {
              this.desiredActive = false;
              this._saveDesiredActive();
            }
            this.lastStatus = msg.forbidden ? 'forbidden'
              : this.desiredActive && msg.occupied ? 'occupied'
              : this.connected ? 'polling'
              : 'offline';
          }
          this._syncControlUi();
        } else if (msg.type === 'ws_taken_over') {
          this.desiredActive = false;
          this._saveDesiredActive();
          this._deactivateWsSlot();
          this.lastStatus = 'taken_over';
          this._syncControlUi();
          this._showTakenOverBanner();
        }
      } catch (e) { console.warn(e); }
    };
    this.ws.onclose = (ev) => {
      this.connected = false;
      this._deactivateWsSlot();
      this.ws = null;
      this.serverCanTakeover = null;
      if (ev.code === 4001) {
        this.desiredActive = false;
        this._saveDesiredActive();
        this.lastStatus = 'taken_over';
        this._syncControlUi();
        return;
      }
      this.lastStatus = 'reconnecting';
      this._syncControlUi();
      this._scheduleReconnect();
    };
    this.ws.onerror = () => {};
    this._startGeneralPolling();
  },

  disconnect() {
    if (this.reconnectTimer) { clearTimeout(this.reconnectTimer); this.reconnectTimer = null; }
    if (this.overflowTimer) { clearInterval(this.overflowTimer); this.overflowTimer = null; }
    if (this.generalPollTimer) { clearInterval(this.generalPollTimer); this.generalPollTimer = null; }
    if (this.ws) {
      // close 이벤트가 비동기로 도착해 onclose의 재접속 경로를 되살리지
      // 않도록, 명시적 해제에서는 핸들러를 먼저 뗀다.
      this.ws.onclose = null;
      this.ws.close();
      this.ws = null;
    }
    this.desiredActive = false;
    this._saveDesiredActive();
    this.connected = false;
    this._deactivateWsSlot();
    this.serverCanTakeover = null;
    this.lastStatus = 'offline';
    this.inflightCodes.clear();
    this._syncControlUi();
  },

  _scheduleReconnect() {
    if (this.reconnectTimer) return;
    this.reconnectTimer = setTimeout(() => { this.reconnectTimer = null; this.connect(); }, 5000);
    this._syncControlUi();
  },

  _showTakenOverBanner() {
    const banner = document.createElement('div');
    banner.textContent = '다른 세션이 실시간 시세 연결을 가져갔습니다. 1분 간격 폴링으로 전환합니다.';
    banner.style.cssText = 'position:fixed;top:0;left:0;right:0;padding:10px;background:#e67e22;color:white;text-align:center;z-index:9999;font-size:13px;';
    document.body.appendChild(banner);
    setTimeout(() => banner.remove(), 5000);
  },

  isLive(code) { return this.wsActive && this.wsCodes.has(code); },

  requestActive() {
    if (!this.manualControlAllowed) {
      this.desiredActive = false;
      this._saveDesiredActive();
      this.lastStatus = 'forbidden';
      this._syncControlUi();
      return;
    }
    this.desiredActive = true;
    this._saveDesiredActive();
    this.lastStatus = this.wsActive ? 'active' : 'connecting';
    if (this.ws && this.serverCanTakeover === false) {
      this.ws.onclose = null;
      try { this.ws.close(); } catch (e) {}
      this.ws = null;
      this.connected = false;
      this.serverCanTakeover = null;
    }
    this.connect();
    if (this.connected) this._requestActiveSlot();
    this._syncControlUi();
  },

  releaseActive() {
    this.desiredActive = false;
    this._saveDesiredActive();
    if (this.connected && this.ws) {
      try { this.ws.send(JSON.stringify({ action: 'release' })); } catch (e) {}
    }
    this._deactivateWsSlot();
    this.lastStatus = this.connected ? 'polling' : 'offline';
    this._syncControlUi();
    this._pollAll();
  },

  toggleActive() {
    if (this.wsActive || this.desiredActive) this.releaseActive();
    else this.requestActive();
  },

  updateSubscriptions(requested) {
    this.subscriptions = requested;
    this._sendSubscriptions();
  },

  _requestActiveSlot() {
    if (!this.connected || !this.ws || !this.desiredActive || !this.manualControlAllowed) return;
    try {
      this.ws.send(JSON.stringify({ action: 'takeover' }));
      this.lastStatus = this.wsActive ? 'active' : 'connecting';
    } catch (e) {
      this.lastStatus = 'reconnecting';
    }
    this._syncControlUi();
  },

  _sendSubscriptions() {
    if (!this.connected || !this.ws || !this.wsActive) return;
    this.ws.send(JSON.stringify({ action: 'subscribe', requested: this.subscriptions }));
  },

  _deactivateWsSlot() {
    this.wsActive = false;
    this.wsCodes = new Set();
    this.overflowCodes = [];
    this.lastWsQuoteAt = {};
    if (this.overflowTimer) {
      clearInterval(this.overflowTimer);
      this.overflowTimer = null;
    }
  },

  _controlStatusText() {
    if (this.wsActive) {
      const slots = this.lastSlotMeta?.slots_active;
      return slots ? `실시간 ${slots}슬롯` : '실시간 연결됨';
    }
    if (this.lastStatus === 'connecting') return '연결 중';
    if (this.lastStatus === 'reconnecting') return '재연결 중';
    if (this.lastStatus === 'occupied') return '다른 세션 사용 중';
    if (this.lastStatus === 'forbidden') return '관리자 전용';
    if (this.lastStatus === 'taken_over') return '폴링 전환됨';
    if (this.connected) return '폴링';
    return '오프라인';
  },

  _syncControlUi() {
    const button = document.getElementById('pfWsToggle');
    const status = document.getElementById('pfWsStatus');
    const visible = !!this.manualControlAllowed;
    if (button) {
      button.hidden = !visible;
      if (visible) {
        const activeOrPending = this.wsActive || this.desiredActive;
        button.textContent = activeOrPending ? '웹소켓 해제' : '웹소켓 연결';
        button.classList.toggle('active', activeOrPending);
        button.setAttribute('aria-pressed', activeOrPending ? 'true' : 'false');
        button.disabled = this.desiredActive && !this.connected && !!this.reconnectTimer;
        button.title = activeOrPending ? '실시간 웹소켓 연결 해제' : '실시간 웹소켓 연결';
      }
    }
    if (status) {
      status.hidden = !visible;
      if (visible) {
        status.textContent = this._controlStatusText();
        status.dataset.state = this.wsActive ? 'active'
          : this.lastStatus === 'forbidden' || this.lastStatus === 'occupied' ? 'warning'
          : this.lastStatus === 'reconnecting' || this.lastStatus === 'connecting' ? 'pending'
          : 'polling';
      }
    }
  },

  _retryTimer: null,

  _markWsQuoteFresh(code) {
    if (!code) return;
    this.lastWsQuoteAt[code] = Date.now();
  },

  _getStaleWsCodes() {
    const now = Date.now();
    return [...this.wsCodes].filter(code =>
      now - (this.lastWsQuoteAt[code] || 0) >= QUOTE_MANAGER_STALE_WS_MS
    );
  },

  _quotePriority(code) {
    if (QUOTE_MANAGER_PRIORITY_CODES.has(String(code || '').toUpperCase())) return 0;
    return /^\d/.test(String(code || '')) ? 2 : 1;
  },

  async _fetchQuoteBatch(codes, { fresh = true } = {}) {
    codes.forEach(code => this.inflightCodes.add(code));
    try {
      const data = await apiFetchJson('/api/asset-quotes', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ codes, fresh }),
        fallback: null,
      });
      if (!data) return;
      for (const [code, q] of Object.entries(data || {})) {
        if (q && q.price != null) {
          if (this.onQuote) this.onQuote(code, { code, ...q });
        }
      }
    } catch {
      /* Keep quote polling best-effort; portfolio rendering must not wait. */
    } finally {
      codes.forEach(code => this.inflightCodes.delete(code));
    }
  },

  async _fetchQuotes(codes, { fresh = true, scheduleRetry = true } = {}) {
    const uniqueCodes = [...new Set((codes || []).filter(Boolean))]
      .filter(code => !this.inflightCodes.has(code))
      .sort((a, b) => this._quotePriority(a) - this._quotePriority(b));
    if (!uniqueCodes.length) return;
    const batches = [];
    for (let i = 0; i < uniqueCodes.length; i += QUOTE_MANAGER_BATCH_SIZE) {
      batches.push(uniqueCodes.slice(i, i + QUOTE_MANAGER_BATCH_SIZE));
    }
    let nextBatch = 0;
    const workerCount = Math.min(QUOTE_MANAGER_BATCH_PARALLEL, batches.length);
    const workers = Array.from({ length: workerCount }, async () => {
      while (nextBatch < batches.length) {
        const batch = batches[nextBatch++];
        await this._fetchQuoteBatch(batch, { fresh });
      }
    });
    await Promise.all(workers);
    if (scheduleRetry) this._scheduleRetry();
  },

  _getMissingCodes() {
    const missing = new Set();
    for (const i of PfStore.items) {
      if (!quoteIsUsable(i.quote)) missing.add(i.stock_code);
    }
    if (typeof recentListItems !== 'undefined' && Array.isArray(recentListItems)) {
      for (const i of recentListItems) {
        if (!quoteIsUsable(i.quote)) missing.add(i.stock_code);
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
    await this._fetchQuotes(wsCodes, { fresh: false, scheduleRetry: false });
    const missing = this._getMissingCodes();
    if (missing.length) {
      await this._fetchQuotes(missing, { fresh: true });
    } else {
      this._scheduleRetry();
    }
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

function toggleQuoteWebSocket() {
  QuoteManager.toggleActive();
}
