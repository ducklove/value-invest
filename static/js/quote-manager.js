// --- WebSocket Quote Manager ---
const QUOTE_MANAGER_STALE_WS_MS = 55_000;
const QUOTE_MANAGER_GENERAL_POLL_MS = 60_000;
const QUOTE_MANAGER_OVERFLOW_POLL_MS = 30_000;
const QUOTE_MANAGER_RETRY_MS = 5_000;
const QUOTE_MANAGER_PING_TIMEOUT_MS = 4_000;
const QUOTE_MANAGER_CONNECTION_CHECK_MS = 10_000;
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
  _pingTimer: null,
  connectionPollTimer: null,
  connectStartedAt: 0,
  streamState: 'offline',
  namuhLinked: false,
  namuhQuotes: {},
  namuhStartedAt: 0,
  namuhLastAcquire: 0,
  namuhLastPlan: '',
  namuhSuspended: false,
  namuhAutoSlot: false,
  namuhKisPaused: false,
  sharedMode: false,

  setNamuhLinked(linked) {
    if (this.namuhLinked === !!linked) return;
    this.namuhLinked = !!linked;
    this.namuhStartedAt = Date.now();
    this.namuhKisPaused = false;
    this.namuhLastPlan = '';
    if (!linked) {
      this.namuhUnavailable();
      if (this.namuhAutoSlot) { this.namuhAutoSlot = false; this.releaseActive({manual:false}); }
    }
    this._syncControlUi();
  },

  onNamuhQuote(code, quote) {
    if (this.sharedMode) return;
    if (!this.namuhLinked || !isNamuhLiveQuote(quote)) return;
    if (!shouldAcceptQuoteSnapshot(this.namuhQuotes[code], quote)) return;
    this.namuhQuotes[code] = quote;
    this.onQuote?.(code, quote);
    this._syncNamuhFallback();
  },

  namuhUnavailable() {
    if (this.sharedMode) return;
    this.namuhQuotes = {};
    this.namuhStartedAt = 0;
    for (const item of (typeof PfStore !== 'undefined' ? PfStore.items : [])) {
      if (item.quote?.source === 'namuh_ws') item.quote = {...item.quote, _stale: true};
    }
    this._syncNamuhFallback();
  },

  onNamuhStatus(message) {
    if (this.sharedMode) return;
    for (const [market, state] of [['domestic', message.domestic], ['foreign', message.foreign]]) {
      if (!state || !['degraded', 'waiting'].includes(state.state) || state.reason === 'subscription_rejected') continue;
      for (const code of Object.keys(this.namuhQuotes)) {
        const domestic = /^[0-9][0-9A-Z]{5}$/.test(code) || code === 'KRX_GOLD';
        if (domestic === (market === 'domestic')) {
          delete this.namuhQuotes[code];
          for (const item of (typeof PfStore !== 'undefined' ? PfStore.items : [])) {
            if (item.stock_code === code && item.quote?.source === 'namuh_ws') item.quote = {...item.quote, _stale: true};
          }
        }
      }
    }
    this._syncNamuhFallback();
  },

  _hasNamuhQuote(code) { return !this.sharedMode && this.namuhLinked && isNamuhLiveQuote(this.namuhQuotes[code]); },

  _fallbackSubscriptions() {
    return Object.fromEntries(Object.entries(this.subscriptions).map(([group, codes]) =>
      [group, codes.filter(code => !this._hasNamuhQuote(code))]));
  },

  _syncNamuhFallback() {
    if (this.sharedMode) return;
    if (!this.namuhLinked) return;
    const requested = this._fallbackSubscriptions();
    const needsKis = Object.values(requested).flat().some(code => /^[0-9][0-9A-Z]{5}$/.test(code));
    if (!needsKis) {
      if (this.wsActive && this.connected && this.ws) {
        this.namuhSuspended = true;
        this.ws.send(JSON.stringify({action: 'release'}));
        this._deactivateWsSlot();
      }
    } else if (this.wsActive) {
      if (this.namuhLastPlan !== JSON.stringify(requested)) this._sendSubscriptions();
    } else if (!this.namuhKisPaused && this.connected && this.ws && Date.now() - this.namuhLastAcquire >= 10_000
        && (Object.keys(this.namuhQuotes).length || Date.now() - this.namuhStartedAt >= 30_000)) {
      // NH 연결 사용자만 빈 KIS 슬롯을 보조로 사용한다. 다른 세션을 빼앗지 않는다.
      this.namuhLastAcquire = Date.now();
      this.namuhAutoSlot = !this.desiredActive;
      this.ws.send(JSON.stringify({action: 'acquire'}));
    }
    this._syncControlUi();
  },

  _loadDesiredActive() {
    return safeStorageGet(QUOTE_MANAGER_MANUAL_WS_KEY, null, 'session') === '1';
  },

  _saveDesiredActive() {
    if (this.desiredActive) safeStorageSet(QUOTE_MANAGER_MANUAL_WS_KEY, '1', 'session');
    else safeStorageRemove(QUOTE_MANAGER_MANUAL_WS_KEY, 'session');
  },

  setManualControlAllowed(allowed) {
    if (this.sharedMode) { this.manualControlAllowed = false; this._syncControlUi(); return; }
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
    const socket = this.ws;
    this.connectStartedAt = Date.now();
    if (!this.connectionPollTimer) {
      this.connectionPollTimer = schedulePoll('quotes.connection', () => {
        this.verifyConnection();
        this._syncControlUi();
      }, QUOTE_MANAGER_CONNECTION_CHECK_MS);
    }
    this.lastStatus = this.desiredActive ? 'connecting' : 'polling';
    this._syncControlUi();
    this.ws.onopen = () => {
      if (this.ws !== socket) return;
      this.connected = true;
      this.lastStatus = this.desiredActive ? 'connecting' : 'polling';
      this._syncControlUi();
      if (this.desiredActive && !this.namuhLinked) this._requestActiveSlot();
      this._syncNamuhFallback();
    };
    this.ws.onmessage = (event) => {
      if (this.ws !== socket) return;
      try {
        const msg = JSON.parse(event.data);
        if (msg.type === 'quote' && msg.code) {
          this._markWsQuoteFresh(msg.code, msg);
          if (this.onQuote) this.onQuote(msg.code, msg);
          this._syncControlUi();
        } else if (msg.type === 'subscriptions') {
          this.wsCodes = new Set(msg.ws || []);
          this.overflowCodes = msg.rest || [];
          const allCodes = [...(msg.ws || []), ...(msg.rest || [])];
          this._fetchInitialQuotes(allCodes);
          this._startOverflowPolling();
        } else if (msg.type === 'ws_status') {
          if (msg.shared) {
            this.sharedMode = true;
            this.manualControlAllowed = false;
            this.desiredActive = false;
            this._saveDesiredActive();
            this.wsActive = !!msg.active;
            this.streamState = msg.stream_state || 'connecting';
            this.lastSlotMeta = msg;
            this.lastStatus = msg.active ? 'active' : 'forbidden';
            this._sendSubscriptions();
            this._syncControlUi();
            return;
          }
          this.lastSlotMeta = msg;
          this.serverCanTakeover = msg.can_takeover !== false;
          if (msg.active) {
            this.wsActive = true;
            this.streamState = msg.stream_state || 'connecting';
            this.lastStatus = 'active';
            if (this.manualControlAllowed && !this.namuhAutoSlot) {
              this.desiredActive = true;
              this._saveDesiredActive();
            }
            this._sendSubscriptions();
          } else {
            this._deactivateWsSlot();
            if ((msg.released && !this.namuhSuspended) || msg.forbidden) {
              this.desiredActive = false;
              this._saveDesiredActive();
            }
            this.lastStatus = msg.forbidden ? 'forbidden'
              : this.desiredActive && msg.occupied ? 'occupied'
              : this.connected ? 'polling'
              : 'offline';
            this.namuhSuspended = false;
          }
          this._syncControlUi();
        } else if (msg.type === 'stream_status') {
          this._updateStreamState(msg);
        } else if (msg.type === 'stream_unavailable') {
          if (this.namuhLinked) this.namuhLastAcquire = Date.now() + 50_000;
          this.releaseActive({manual:false});
        } else if (msg.type === 'pong') {
          this._clearPingTimer();
          this._updateStreamState(msg);
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
      if (this.ws !== socket) return;
      this._clearPingTimer();
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
    this.namuhLinked = false;
    this.namuhQuotes = {};
    this.namuhAutoSlot = false;
    this._clearPingTimer();
    if (this.reconnectTimer) { clearTimeout(this.reconnectTimer); this.reconnectTimer = null; }
    this._stopOverflowPolling();
    if (this.generalPollTimer) { this.generalPollTimer.cancel(); this.generalPollTimer = null; }
    if (this.connectionPollTimer) { this.connectionPollTimer.cancel(); this.connectionPollTimer = null; }
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

  // --- 포그라운드 복귀 시 연결 생사 검증 ---------------------------------
  // 백그라운드에서 OS/브라우저가 소켓을 끊으면 close 이벤트가 전달되지 않아
  // (half-open) readyState 가 OPEN 인 채로 '실시간 연결됨' 표시가 남는다.
  // 복귀 시점에 실제 상태를 확인해 죽은 연결이면 즉시 재접속한다 —
  // desiredActive 의도는 유지되므로 활성 슬롯도 자동으로 재청구된다.
  verifyConnection() {
    if (!this.ws) {
      // 재접속 대기 중이었다면 5초 백오프를 기다리지 않고 즉시 시도.
      // 타이머도 없이 ws 가 없는 상태(초기화 전·명시적 disconnect·taken_over)는
      // 의도된 상태이므로 여기서 되살리지 않는다.
      if (this.reconnectTimer) {
        clearTimeout(this.reconnectTimer);
        this.reconnectTimer = null;
        this.connect();
      }
      return;
    }
    const state = this.ws.readyState;
    if (state === 0 /* CONNECTING */) {
      if (Date.now() - this.connectStartedAt >= 15_000) this._forceReconnect();
      return;
    }
    if (state !== 1 /* OPEN */) { this._forceReconnect(); return; }
    this._sendPing();
  },

  _sendPing() {
    if (this._pingTimer) return; // 이미 검증 진행 중
    try {
      this.ws.send(JSON.stringify({ action: 'ping' }));
    } catch (e) {
      this._forceReconnect();
      return;
    }
    this._pingTimer = setTimeout(() => {
      this._pingTimer = null;
      this._forceReconnect();
    }, QUOTE_MANAGER_PING_TIMEOUT_MS);
  },

  _clearPingTimer() {
    if (this._pingTimer) { clearTimeout(this._pingTimer); this._pingTimer = null; }
  },

  _forceReconnect() {
    this._clearPingTimer();
    if (this.ws) {
      // 죽은 소켓의 뒤늦은 close 이벤트가 새 연결의 재접속 경로를 건드리지
      // 않도록 핸들러를 떼고 닫는다 (disconnect 와 같은 이유).
      this.ws.onclose = null;
      try { this.ws.close(); } catch (e) {}
      this.ws = null;
    }
    this.connected = false;
    this._deactivateWsSlot();
    this.serverCanTakeover = null;
    if (this.reconnectTimer) { clearTimeout(this.reconnectTimer); this.reconnectTimer = null; }
    this.lastStatus = 'reconnecting';
    this._syncControlUi();
    this.connect();
  },

  _showTakenOverBanner() {
    const banner = document.createElement('div');
    banner.textContent = '다른 세션이 실시간 시세 연결을 가져갔습니다. 1분 간격 폴링으로 전환합니다.';
    banner.style.cssText = 'position:fixed;top:0;left:0;right:0;padding:10px;background:#e67e22;color:white;text-align:center;z-index:9999;font-size:13px;';
    document.body.appendChild(banner);
    setTimeout(() => banner.remove(), 5000);
  },

  _hasKisQuote(code) {
    const at = this.lastWsQuoteAt[code];
    return this.connected && this.ws?.readyState === 1 && this.wsActive && this.streamState === 'connected' && this.wsCodes.has(code)
      && Number.isFinite(at) && Date.now() - at >= 0 && Date.now() - at < QUOTE_MANAGER_STALE_WS_MS;
  },

  isLive(code) { return this._hasNamuhQuote(code) || this._hasKisQuote(code); },

  _updateStreamState(message) {
    if (!this.wsActive || !message.stream_state) return;
    this.streamState = message.stream_state;
    this.lastSlotMeta = {...this.lastSlotMeta, ...message};
    if (message.slots_connected === 0) this.lastWsQuoteAt = {};
    for (const code of message.disconnected_codes || []) delete this.lastWsQuoteAt[code];
    this._syncControlUi();
  },

  requestActive() {
    if (!this.manualControlAllowed) {
      this.desiredActive = false;
      this._saveDesiredActive();
      this.lastStatus = 'forbidden';
      this._syncControlUi();
      return;
    }
    this.namuhKisPaused = false;
    this.desiredActive = true;
    this._saveDesiredActive();
    this.lastStatus = this.wsActive ? 'active' : 'connecting';
    if (this.ws && this.serverCanTakeover === false) {
      this._clearPingTimer();
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

  releaseActive({manual = true} = {}) {
    if (manual && this.namuhLinked) this.namuhKisPaused = true;
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
    this._syncNamuhFallback();
  },

  _requestActiveSlot() {
    if (!this.connected || !this.ws || !this.desiredActive || !this.manualControlAllowed) return;
    if (this.namuhLinked) { this._syncNamuhFallback(); return; }
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
    const requested = this.sharedMode ? this.subscriptions : this.namuhLinked ? this._fallbackSubscriptions() : this.subscriptions;
    this.namuhLastPlan = JSON.stringify(requested);
    this.ws.send(JSON.stringify({ action: 'subscribe', requested }));
  },

  _deactivateWsSlot() {
    this.wsActive = false;
    this.streamState = 'offline';
    this.wsCodes = new Set();
    this.overflowCodes = [];
    this.lastWsQuoteAt = {};
    this._stopOverflowPolling();
  },

  _controlStatusText() {
    if (this.sharedMode) {
      if (!this.connected) return '실시간 시세 재연결 중';
      if (!this.wsActive) return '실시간 시세 · 로그인 필요';
      const meta = this.lastSlotMeta || {};
      if (meta.subscribed) return `실시간 ${meta.subscribed}종목 · ${meta.receiving || 0}종목 수신${meta.fallback ? ` · 조회 ${meta.fallback}종목` : ''}`;
      return meta.requested ? '실시간 시세 연결 중 · 조회 시세 보완' : '실시간 시세 대기';
    }
    const kisLive = [...this.wsCodes].some(code => this._hasKisQuote(code));
    if (this.wsActive) {
      if (this.streamState === 'reconnecting') return 'KIS 재연결 중';
      if (this.streamState === 'connecting') return 'KIS 연결 중';
      if (this.streamState !== 'connected') return 'KIS 미연결';
      const partial = this.lastSlotMeta?.slots_connected < this.lastSlotMeta?.slots_active;
      return `${partial ? 'KIS 일부 연결' : 'KIS 연결됨'} · ${kisLive ? '시세 수신 중' : '체결 대기'}`;
    }
    if (this.desiredActive && ['connecting', 'reconnecting'].includes(this.lastStatus)) return 'KIS 연결 중';
    if (this.lastStatus === 'occupied') return 'KIS 미연결 · 다른 세션 사용 중';
    return 'KIS 미연결';
  },

  _controlStatusDetail() {
    if (this.sharedMode) {
      const names = {kis: 'KIS', toss: '토스', namuh: 'NH'};
      const states = {idle: '대기', connecting: '연결 중', connected: '구독 확인 중', subscribed: '체결 대기', live: '수신 중', reconnecting: '재연결 중', degraded: '일부 제한', waiting: '대기', offline: '미연결'};
      const sources = (this.lastSlotMeta?.sources || []).filter(source => source.requested || source.reserved);
      return sources.map(source => `${names[source.provider] || source.provider} ${source.subscribed || 0}/${source.requested || 0} · ${states[source.state] || '상태 확인 중'}`).join(' | ') || '서버에서 연결을 자동 관리합니다';
    }
    if (!this.wsActive || this.streamState !== 'connected') return '조회 시세로 갱신 중';
    const latest = Math.max(0, ...[...this.wsCodes].filter(code => this._hasKisQuote(code)).map(code => this.lastWsQuoteAt[code]));
    const connections = this.lastSlotMeta?.slots_connected;
    const count = connections ? `연결 ${connections}개 · ` : '';
    return count + (latest ? `최근 체결 ${new Date(latest).toLocaleTimeString('ko-KR', {hour12: false})}` : '새 체결 수신을 기다립니다');
  },

  _syncControlUi() {
    for (const dot of document.querySelectorAll('.ws-live-dot')) {
      const code = dot.closest('[data-code]')?.dataset.code
        || (dot.parentElement?.id === 'quoteDate' && typeof activeStockCode !== 'undefined' ? activeStockCode : null);
      if (code && !this.isLive(code)) dot.remove();
    }
    const button = document.getElementById('pfWsToggle');
    const status = document.getElementById('pfWsStatus');
    const visible = !!this.manualControlAllowed && !this.sharedMode;
    if (button) {
      button.hidden = !visible;
      if (visible) {
        const activeOrPending = this.wsActive || this.desiredActive;
        button.textContent = activeOrPending ? 'KIS 해제' : 'KIS 연결';
        button.classList.toggle('active', activeOrPending);
        button.setAttribute('aria-pressed', activeOrPending ? 'true' : 'false');
        button.disabled = this.desiredActive && !this.connected && !!this.reconnectTimer;
        button.title = activeOrPending ? '한국투자증권 웹소켓 연결 해제' : '한국투자증권 웹소켓 연결';
      }
    }
    if (status) {
      status.hidden = false;
      status.textContent = this._controlStatusText();
      status.title = this.sharedMode ? '서버 공통 실시간 시세의 구독 승인과 체결 수신 상태' : '한국투자증권 웹소켓의 실제 연결 상태';
      status.dataset.state = this.wsActive && this.streamState === 'connected' ? 'active'
        : this.lastStatus === 'occupied' ? 'warning'
        : this.wsActive || this.desiredActive ? 'pending' : 'polling';
    }
    const detail = document.getElementById('pfWsDetail');
    if (detail) detail.textContent = this._controlStatusDetail();
  },

  _retryTimer: null,

  _markWsQuoteFresh(code, quote) {
    const at = Date.parse(quote.as_of || '');
    if (!code || !quoteIsUsable(quote) || !['ws', 'kis_ws', 'toss_ws', 'namuh_ws'].includes(quote.source)
        || !Number.isFinite(at) || Date.now() - at < 0 || Date.now() - at >= QUOTE_MANAGER_STALE_WS_MS) return;
    this.lastWsQuoteAt[code] = Math.max(this.lastWsQuoteAt[code] || 0, at);
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
      .filter(code => !this.inflightCodes.has(code) && !this._hasNamuhQuote(code))
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

  // 폴링은 가시성 인지 헬퍼(utils.js schedulePoll)로 돈다: 숨은 탭에서는 멈추고,
  // 다시 보이면 마지막 폴링이 주기보다 오래됐을 때만 즉시 한 번 갱신한 뒤 재개한다.
  // 이름이 고정이라 재호출해도 타이머가 겹치지 않는다.
  _stopOverflowPolling() {
    if (this.overflowTimer) { this.overflowTimer.cancel(); this.overflowTimer = null; }
  },

  _startOverflowPolling() {
    this._stopOverflowPolling();
    if (!this.overflowCodes.length) return;
    this._pollOverflow();
    this.overflowTimer = schedulePoll('quotes.overflow', () => this._pollOverflow(), QUOTE_MANAGER_OVERFLOW_POLL_MS);
  },

  _startGeneralPolling() {
    if (this.generalPollTimer) this.generalPollTimer.cancel();
    // 연결 직후엔 초기 조회가 따로 돌므로 '방금 폴링함'으로 시작한다(runNow 없음).
    this.generalPollTimer = schedulePoll('quotes.general', () => this._pollAll(), QUOTE_MANAGER_GENERAL_POLL_MS);
  },

  async _pollAll() {
    this._syncNamuhFallback();
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
