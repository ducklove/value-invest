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
  onQuote: null,
  // 브라우저 콘솔에서 `QuoteManager.debug = true` 로 켜면 tick 수신과
  // 포트폴리오 UI 업데이트 경로가 상세히 찍힘. 'WS tick 오는데 UI
  // 갱신 안 되는 것 아닌가' 의심 진단용.
  debug: false,

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
          // Refresh ALL codes immediately (ws + overflow)
          const allCodes = [...(msg.ws || []), ...(msg.rest || [])];
          this._fetchInitialQuotes(allCodes);
          this._startOverflowPolling();
        }
        else if (msg.type === 'ws_status') {
          if (msg.active) {
            // We are now the active subscriber
            this.wsActive = true;
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

  _showTakenOverBanner() {
    // Brief visual notification
    const banner = document.createElement('div');
    banner.textContent = '다른 세션이 실시간 시세를 가져갔습니다. 1분 간격 폴링으로 전환됩니다.';
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

  async _fetchQuotes(codes) {
    if (!codes.length) return;
    // Server caps at 100 codes per request — chunk to stay under the limit.
    const CHUNK = 100;
    for (let i = 0; i < codes.length; i += CHUNK) {
      const slice = codes.slice(i, i + CHUNK);
      try {
        const resp = await apiFetch('/api/asset-quotes', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ codes: slice }),
        });
        if (!resp.ok) continue;
        const results = await resp.json();
        for (const [code, q] of Object.entries(results)) {
          if (q && q.price != null && this.onQuote) {
            // 서버가 내려주는 quote dict 를 **통째로** 전달한다. 이전엔
            // price/change/previous_close/date 만 추려냈는데 서버가 돌려
            // 주는 trade_value / volume / 기타 필드가 모두 잘림.
            // 결과: 초기 로드엔 제대로 떴던 '거래대금' 같은 값이 이 polling
            // 이 덮어쓰면서 '-' 로 바뀌는 증상. onQuote 핸들러가 spread
            // merge 를 쓰도록 app-main.js 쪽도 함께 고쳤다.
            this.onQuote(code, { code, ...q });
          }
        }
      } catch (e) { console.warn(e); }
    }
    // Schedule fast retry for any still-missing quotes
    this._scheduleRetry();
  },

  _getMissingCodes() {
    // Check portfolio + sidebar (recent search) items, not benchmark/index codes.
    // stale:true 도 missing 으로 취급 — 서버가 last_known 으로 즉시 응답한
    // 경우 fresh 받을 때까지 5초 retry 로 빠르게 교체.
    const missing = new Set();
    for (const i of portfolioItems) {
      if (!i.quote || i.quote.price == null || i.quote.stale) missing.add(i.stock_code);
    }
    if (typeof recentListItems !== 'undefined' && Array.isArray(recentListItems)) {
      for (const i of recentListItems) {
        if (!i.quote || i.quote.price == null || i.quote.stale) missing.add(i.stock_code);
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

  // 15초 간격 폴링 — WS 활성 여부와 무관하게 항상 동작. KIS WS 는 40
  // 슬롯 제한이라 포트폴리오 종목 수가 40을 넘으면 나머지는 아예 tick
  // 을 받지 못함 (사용자 관찰: 삼성전자우 같은 대형주가 1 분 동안 변동
  // 없음 = WS 미도달, polling 대기). WS 종목도 포함해 15초마다 한 번
  // REST 로 확인 → tick 이 온 종목은 서버 cache hit 으로 즉응답, tick
  // 못 받은 종목만 실제 KIS REST 경유. 체감 실시간성 크게 개선.
  _startGeneralPolling() {
    if (this.generalPollTimer) clearInterval(this.generalPollTimer);
    this.generalPollTimer = setInterval(() => this._pollAll(), 15_000);
  },

  async _pollAll() {
    const allCodes = new Set();
    if (this.wsActive) {
      this.overflowCodes.forEach(c => allCodes.add(c));
      // WS 활성이어도 portfolio 전체는 15초마다 REST 보강. 40 슬롯
      // 초과로 WS 못 받는 종목 + WS 슬롯에 있지만 tick 이 드문 종목 전부.
      (this.subscriptions.portfolio || []).forEach(c => allCodes.add(c));
    } else {
      for (const codes of Object.values(this.subscriptions)) {
        for (const c of codes) allCodes.add(c);
      }
    }
    if (allCodes.size) await this._fetchQuotes([...allCodes]);
  },
};
