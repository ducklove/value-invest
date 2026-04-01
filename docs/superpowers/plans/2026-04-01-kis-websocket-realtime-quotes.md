# KIS WebSocket 실시간 시세 구현 계획

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** KIS 프록시 대신 KIS WebSocket에 직접 연결하여 실시간 시세를 수신하고, 서버 자체 WebSocket으로 브라우저에 중계한다.

**Architecture:** 서버가 KIS WebSocket(`ws://ops.koreainvestment.com:21000`)에 단일 연결을 유지하며, `H0STCNT0`(실시간 체결가)를 구독한다. 브라우저는 서버의 `wss://` WebSocket 엔드포인트에 연결하여 실시간 시세를 수신한다. 세션당 41종목 제한 내에서 포트폴리오 > 벤치마크 > 사이드바 우선순위로 구독하고, 초과분은 REST API로 polling한다.

**Tech Stack:** Python `websockets` 16.0, FastAPI WebSocket, vanilla JS WebSocket API

---

## 파일 구조

| 파일 | 상태 | 역할 |
|------|------|------|
| `kis_ws_manager.py` | **신규** | KIS WebSocket 연결, 구독 관리, 데이터 파싱, 인메모리 캐시 |
| `routes/ws_quotes.py` | **신규** | 브라우저-facing WebSocket 엔드포인트 `/ws/quotes` |
| `main.py` | 수정 | `.kis.env` 로드, WS 매니저 시작/종료, 라우터 추가 |
| `routes/__init__.py` | 수정 | ws_quotes 라우터 export |
| `stock_price.py` | 수정 | `fetch_quote_snapshot`에서 WS 캐시 우선 사용 |
| `routes/portfolio.py` | 수정 | `_enrich_with_cached_quotes`에서 WS 캐시 병합 |
| `static/app.js` | 수정 | 클라이언트 QuoteManager 구현, 기존 polling 제거 |
| `static/app-config.js` | 수정 | `kisProxyBaseUrl` 제거 |

---

### Task 1: KIS WebSocket 매니저 — 코어 모듈

**Files:**
- Create: `kis_ws_manager.py`

- [ ] **Step 1: 파일 생성 — approval key + WebSocket 연결**

```python
"""KIS WebSocket manager for real-time stock quotes (H0STCNT0)."""
from __future__ import annotations

import asyncio
import json
import logging
import os
from pathlib import Path

import httpx
import websockets

logger = logging.getLogger(__name__)

# --- 설정 ---
_APP_KEY = ""
_APP_SECRET = ""
_KIS_BASE_URL = "https://openapi.koreainvestment.com:9443"
_KIS_WS_URL = "ws://ops.koreainvestment.com:21000"
_MAX_SUBS = 40  # 41 한도에서 1개 버퍼

# --- 상태 ---
_approval_key: str | None = None
_ws: websockets.ClientConnection | None = None
_connected = False
_ws_task: asyncio.Task | None = None

_subscriptions: set[str] = set()          # 현재 구독 중인 종목 코드
_quotes: dict[str, dict] = {}             # code -> 최신 시세
_listeners: list[asyncio.Queue] = []      # 브라우저 중계 큐


def load_credentials():
    """Load KIS credentials from environment (call after dotenv loading)."""
    global _APP_KEY, _APP_SECRET, _KIS_BASE_URL
    _APP_KEY = os.getenv("KIS_APP_KEY", "")
    _APP_SECRET = os.getenv("KIS_APP_SECRET", "")
    _KIS_BASE_URL = os.getenv("KIS_BASE_URL", _KIS_BASE_URL)
    if not _APP_KEY or not _APP_SECRET:
        logger.warning("KIS_APP_KEY / KIS_APP_SECRET not set — WebSocket disabled")


async def _get_approval_key() -> str:
    global _approval_key
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.post(
            f"{_KIS_BASE_URL}/oauth2/Approval",
            json={
                "grant_type": "client_credentials",
                "appkey": _APP_KEY,
                "secretkey": _APP_SECRET,
            },
        )
        resp.raise_for_status()
        _approval_key = resp.json()["approval_key"]
    logger.info("KIS WebSocket approval key obtained")
    return _approval_key


def _sub_msg(tr_key: str, tr_type: str = "1") -> str:
    return json.dumps({
        "header": {
            "approval_key": _approval_key,
            "custtype": "P",
            "tr_type": tr_type,
            "content-type": "utf-8",
        },
        "body": {"input": {"tr_id": "H0STCNT0", "tr_key": tr_key}},
    })


async def _connect():
    global _ws, _connected
    if not _APP_KEY:
        return
    if not _approval_key:
        await _get_approval_key()
    _ws = await websockets.connect(_KIS_WS_URL, ping_interval=60)
    _connected = True
    logger.info("KIS WebSocket connected")
```

- [ ] **Step 2: H0STCNT0 파싱 함수**

아래 코드를 같은 파일 하단에 추가:

```python
def _parse_h0stcnt0(raw: str) -> dict | None:
    """Parse H0STCNT0 real-time trade data.

    Wire format: 0|H0STCNT0|001|005930^153025^67500^2^500^0.75^...
    """
    if not raw or raw[0] not in ("0", "1"):
        return None
    parts = raw.split("|")
    if len(parts) < 4 or parts[1] != "H0STCNT0":
        return None
    fields = parts[3].split("^")
    if len(fields) < 34:
        return None

    code = fields[0]
    price = int(fields[2]) if fields[2] else None
    sign = fields[3]  # 1=상한 2=상승 3=보합 4=하락 5=하한
    change_abs = int(fields[4]) if fields[4] else 0
    change_pct_abs = float(fields[5]) if fields[5] else 0.0
    volume = int(fields[13]) if fields[13] else 0
    time_str = fields[1]   # HHMMSS
    biz_date = fields[33]  # YYYYMMDD

    negative = sign in ("4", "5")
    change = -change_abs if negative else change_abs
    change_pct = -change_pct_abs if negative else change_pct_abs
    previous_close = (price - change) if price is not None else None

    date_iso = ""
    if len(biz_date) == 8:
        date_iso = f"{biz_date[:4]}-{biz_date[4:6]}-{biz_date[6:]}"

    return {
        "code": code,
        "price": price,
        "change": change,
        "change_pct": round(change_pct, 2),
        "previous_close": previous_close,
        "volume": volume,
        "time": time_str,
        "date": date_iso,
    }
```

- [ ] **Step 3: 수신 루프 + 자동 재연결**

```python
async def _recv_loop():
    global _connected
    while _connected and _ws:
        try:
            data = await _ws.recv()
            if isinstance(data, bytes):
                data = data.decode("utf-8")

            # JSON 제어 메시지 (PINGPONG, 구독 확인)
            if data[0] not in ("0", "1"):
                try:
                    msg = json.loads(data)
                    if msg.get("header", {}).get("tr_id") == "PINGPONG":
                        await _ws.send(data)
                except (json.JSONDecodeError, KeyError):
                    pass
                continue

            quote = _parse_h0stcnt0(data)
            if not quote:
                continue
            code = quote["code"]
            _quotes[code] = quote
            # 리스너에게 전달
            for q in _listeners:
                try:
                    q.put_nowait(quote)
                except asyncio.QueueFull:
                    pass
        except websockets.ConnectionClosed:
            logger.warning("KIS WebSocket connection closed")
            _connected = False
        except Exception as exc:
            logger.error("KIS WS recv error: %s", exc)
            _connected = False


async def _run_forever():
    """Connect and run with auto-reconnect."""
    global _connected
    while True:
        try:
            if not _connected:
                await _connect()
                # 기존 구독 복원
                for code in list(_subscriptions):
                    if _ws:
                        await _ws.send(_sub_msg(code, "1"))
                        await asyncio.sleep(0.05)
            await _recv_loop()
        except Exception as exc:
            logger.error("KIS WS loop error: %s", exc)
            _connected = False
        await asyncio.sleep(5)
```

- [ ] **Step 4: 구독 관리 + 공개 API**

```python
async def subscribe(code: str):
    if code in _subscriptions or not _ws or not _connected:
        return
    if len(_subscriptions) >= _MAX_SUBS:
        return
    await _ws.send(_sub_msg(code, "1"))
    _subscriptions.add(code)


async def unsubscribe(code: str):
    if code not in _subscriptions or not _ws or not _connected:
        return
    await _ws.send(_sub_msg(code, "2"))
    _subscriptions.discard(code)


def is_korean_stock(code: str) -> bool:
    """6자리 숫자 또는 6자리+K 형태의 한국 종목코드인지 판별."""
    return bool(code) and len(code) == 6 and code[:5].isdigit()


async def update_subscriptions(requested: dict[str, list[str]]) -> dict:
    """우선순위에 따라 구독 갱신.

    requested: {"portfolio": [...], "benchmark": [...], "sidebar": [...]}
    Returns: {"ws": [구독된 코드], "rest": [REST fallback 코드]}
    """
    # 한국 주식만 필터링하고 우선순위 순으로 병합 (중복 제거)
    ordered: list[str] = []
    seen: set[str] = set()
    for priority in ("portfolio", "benchmark", "sidebar", "analysis"):
        for code in requested.get(priority, []):
            if code not in seen and is_korean_stock(code):
                ordered.append(code)
                seen.add(code)

    ws_codes = set(ordered[:_MAX_SUBS])
    rest_codes = ordered[_MAX_SUBS:]

    # 제거할 구독
    to_unsub = _subscriptions - ws_codes
    for code in to_unsub:
        await unsubscribe(code)
        await asyncio.sleep(0.02)

    # 추가할 구독
    to_sub = ws_codes - _subscriptions
    for code in to_sub:
        await subscribe(code)
        await asyncio.sleep(0.02)

    return {"ws": sorted(ws_codes), "rest": rest_codes}


def get_cached_quote(code: str) -> dict | None:
    """인메모리 캐시에서 최신 시세 반환."""
    return _quotes.get(code)


def get_all_cached_quotes() -> dict[str, dict]:
    """모든 캐시된 시세 반환."""
    return dict(_quotes)


def add_listener() -> asyncio.Queue:
    q: asyncio.Queue = asyncio.Queue(maxsize=500)
    _listeners.append(q)
    return q


def remove_listener(q: asyncio.Queue):
    try:
        _listeners.remove(q)
    except ValueError:
        pass


async def start():
    global _ws_task
    if not _APP_KEY:
        logger.info("KIS credentials not configured — WebSocket disabled")
        return
    _ws_task = asyncio.create_task(_run_forever())
    logger.info("KIS WebSocket manager started")


async def stop():
    global _connected, _ws_task, _ws
    _connected = False
    if _ws_task:
        _ws_task.cancel()
        try:
            await _ws_task
        except asyncio.CancelledError:
            pass
        _ws_task = None
    if _ws:
        await _ws.close()
        _ws = None
    logger.info("KIS WebSocket manager stopped")
```

- [ ] **Step 5: 커밋**

```bash
git add kis_ws_manager.py
git commit -m "feat: add KIS WebSocket manager for real-time quotes"
```

---

### Task 2: 브라우저 WebSocket 엔드포인트

**Files:**
- Create: `routes/ws_quotes.py`
- Modify: `routes/__init__.py`

- [ ] **Step 1: WebSocket 라우트 생성**

`routes/ws_quotes.py`:

```python
"""Browser-facing WebSocket endpoint for real-time stock quotes."""
from __future__ import annotations

import asyncio
import json
import logging

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

import kis_ws_manager

logger = logging.getLogger(__name__)
router = APIRouter()


@router.websocket("/ws/quotes")
async def ws_quotes(ws: WebSocket):
    await ws.accept()
    listener = kis_ws_manager.add_listener()
    send_task: asyncio.Task | None = None

    async def _relay():
        """KIS 시세를 브라우저에 전달."""
        try:
            while True:
                quote = await listener.get()
                await ws.send_json({"type": "quote", **quote})
        except (WebSocketDisconnect, asyncio.CancelledError):
            pass

    try:
        send_task = asyncio.create_task(_relay())
        # 초기 캐시된 시세 전송
        for code, q in kis_ws_manager.get_all_cached_quotes().items():
            await ws.send_json({"type": "quote", **q})

        while True:
            raw = await ws.receive_text()
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                continue

            action = msg.get("action")
            if action == "subscribe":
                requested = msg.get("requested", {})
                result = await kis_ws_manager.update_subscriptions(requested)
                await ws.send_json({"type": "subscriptions", **result})
                # 구독된 종목의 캐시된 시세 즉시 전송
                for code in result["ws"]:
                    cached = kis_ws_manager.get_cached_quote(code)
                    if cached:
                        await ws.send_json({"type": "quote", **cached})
            elif action == "ping":
                await ws.send_json({"type": "pong"})
    except WebSocketDisconnect:
        pass
    except Exception as exc:
        logger.debug("WS client error: %s", exc)
    finally:
        kis_ws_manager.remove_listener(listener)
        if send_task:
            send_task.cancel()
            try:
                await send_task
            except asyncio.CancelledError:
                pass
```

- [ ] **Step 2: `routes/__init__.py`에 라우터 등록**

기존 파일 끝에 추가:

```python
from .ws_quotes import router as ws_quotes_router
```

`__all__` 리스트에 `"ws_quotes_router"` 추가.

- [ ] **Step 3: 커밋**

```bash
git add routes/ws_quotes.py routes/__init__.py
git commit -m "feat: add browser WebSocket endpoint for quote relay"
```

---

### Task 3: 서버 시작/종료 통합

**Files:**
- Modify: `main.py`

- [ ] **Step 1: `.kis.env` 로드 및 WS 매니저 연결**

`main.py` 상단 import 영역에 추가:

```python
from dotenv import load_dotenv
import kis_ws_manager
```

`lifespan` 함수 수정 — `yield` 전에 WS 시작, 후에 종료:

```python
@asynccontextmanager
async def lifespan(app: FastAPI):
    # .kis.env 로드
    env_path = Path(__file__).parent / ".kis.env"
    if env_path.exists():
        load_dotenv(env_path, override=True)
    kis_ws_manager.load_credentials()

    await kis_proxy_client.init_client()
    await kis_ws_manager.start()
    await cache.init_db()
    # ... (기존 corp_codes 로직 그대로)
    yield
    await kis_ws_manager.stop()
    await kis_proxy_client.close_client()
```

- [ ] **Step 2: ws_quotes 라우터 추가**

`main.py`의 라우터 import 줄에 `ws_quotes_router` 추가:

```python
from routes import auth_router, analysis_router, reports_router, stocks_router, cache_router, portfolio_router, ws_quotes_router
```

라우터 등록:

```python
app.include_router(ws_quotes_router)
```

- [ ] **Step 3: 커밋**

```bash
git add main.py
git commit -m "feat: wire KIS WebSocket manager into server lifecycle"
```

---

### Task 4: 서버 시세 캐시 통합

**Files:**
- Modify: `stock_price.py` (fetch_quote_snapshot 함수)
- Modify: `routes/portfolio.py` (_enrich_with_cached_quotes 함수)

- [ ] **Step 1: `stock_price.fetch_quote_snapshot`에서 WS 캐시 우선 사용**

`stock_price.py`의 `fetch_quote_snapshot` 함수 시작 부분에 WS 캐시 확인 로직 추가:

```python
import kis_ws_manager

async def fetch_quote_snapshot(stock_code: str) -> dict:
    # WebSocket 캐시 먼저 확인
    ws_quote = kis_ws_manager.get_cached_quote(stock_code)
    if ws_quote and ws_quote.get("price") is not None:
        return {
            "date": ws_quote.get("date", date.today().isoformat()),
            "price": ws_quote["price"],
            "previous_close": ws_quote.get("previous_close"),
            "change": ws_quote.get("change"),
            "change_pct": ws_quote.get("change_pct"),
        }

    # 기존 REST API 로직 (kis_proxy_client 사용)
    end_date = date.today()
    # ... (나머지 기존 코드 그대로)
```

- [ ] **Step 2: `routes/portfolio._enrich_with_cached_quotes`에서 WS 캐시 병합**

`routes/portfolio.py`의 `_enrich_with_cached_quotes` 함수 수정:

```python
import kis_ws_manager

async def _enrich_with_cached_quotes(items: list[dict]) -> list[dict]:
    """Attach cached quotes — WebSocket cache preferred, then polling cache."""
    import time as _time_mod
    now = _time_mod.monotonic()
    result = []
    for item in items:
        enriched = dict(item)
        code = item["stock_code"]
        # WS 캐시 우선
        ws_q = kis_ws_manager.get_cached_quote(code)
        if ws_q and ws_q.get("price") is not None:
            enriched["quote"] = {
                "date": ws_q.get("date", ""),
                "price": ws_q["price"],
                "previous_close": ws_q.get("previous_close"),
                "change": ws_q.get("change"),
                "change_pct": ws_q.get("change_pct"),
            }
        else:
            # 기존 polling 캐시 fallback
            cached = _quote_cache.get(code)
            enriched["quote"] = cached[1] if cached and (now - cached[0]) < _QUOTE_CACHE_TTL else {}
        result.append(enriched)
    return result
```

- [ ] **Step 3: 커밋**

```bash
git add stock_price.py routes/portfolio.py
git commit -m "feat: prefer WebSocket quote cache in server-side quote fetching"
```

---

### Task 5: 클라이언트 QuoteManager 구현

**Files:**
- Modify: `static/app.js`

- [ ] **Step 1: QuoteManager 클래스 추가**

`app.js` 상단 (상수 선언 후, `fetchQuoteSnapshot` 함수 전)에 삽입:

```javascript
// --- WebSocket Quote Manager ---
const QuoteManager = {
  ws: null,
  connected: false,
  reconnectTimer: null,
  subscriptions: {},  // {portfolio: [...], benchmark: [...], sidebar: [...]}
  overflowCodes: [],  // REST fallback 대상
  overflowTimer: null,
  onQuote: null,      // callback(code, quote)

  connect() {
    if (this.ws) return;
    const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
    const url = `${proto}//${location.host}/ws/quotes`;
    try {
      this.ws = new WebSocket(url);
    } catch (e) {
      this._scheduleReconnect();
      return;
    }

    this.ws.onopen = () => {
      this.connected = true;
      this._sendSubscriptions();
    };

    this.ws.onmessage = (event) => {
      try {
        const msg = JSON.parse(event.data);
        if (msg.type === 'quote' && msg.code && this.onQuote) {
          this.onQuote(msg.code, msg);
        } else if (msg.type === 'subscriptions') {
          this.overflowCodes = msg.rest || [];
          this._startOverflowPolling();
        }
      } catch {}
    };

    this.ws.onclose = () => {
      this.connected = false;
      this.ws = null;
      this._scheduleReconnect();
    };

    this.ws.onerror = () => {
      // onclose will fire after this
    };
  },

  disconnect() {
    if (this.reconnectTimer) { clearTimeout(this.reconnectTimer); this.reconnectTimer = null; }
    if (this.overflowTimer) { clearInterval(this.overflowTimer); this.overflowTimer = null; }
    if (this.ws) { this.ws.close(); this.ws = null; }
    this.connected = false;
  },

  _scheduleReconnect() {
    if (this.reconnectTimer) return;
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      this.connect();
    }, 5000);
  },

  updateSubscriptions(requested) {
    // requested: {portfolio: [...], benchmark: [...], sidebar: [...]}
    this.subscriptions = requested;
    this._sendSubscriptions();
  },

  _sendSubscriptions() {
    if (!this.connected || !this.ws) return;
    this.ws.send(JSON.stringify({
      action: 'subscribe',
      requested: this.subscriptions,
    }));
  },

  async _pollOverflow() {
    if (!this.overflowCodes.length) return;
    for (const code of this.overflowCodes) {
      try {
        const resp = await apiFetch(`/api/quote/${code}`);
        if (resp.ok) {
          const q = await resp.json();
          if (this.onQuote) this.onQuote(code, {
            code, price: q.price, change: q.change,
            change_pct: q.change_pct, previous_close: q.previous_close, date: q.date,
          });
        }
      } catch {}
    }
  },

  _startOverflowPolling() {
    if (this.overflowTimer) clearInterval(this.overflowTimer);
    if (!this.overflowCodes.length) return;
    this._pollOverflow();
    this.overflowTimer = setInterval(() => this._pollOverflow(), 30_000);
  },
};
```

- [ ] **Step 2: QuoteManager 시세 콜백 — UI 업데이트**

`app.js`에 시세 수신 시 UI를 갱신하는 콜백 함수 추가 (QuoteManager 정의 뒤):

```javascript
QuoteManager.onQuote = function(code, q) {
  // 1) 분석 뷰 활성 종목
  if (code === activeStockCode && q.price != null) {
    renderQuoteSnapshot({
      date: q.date, price: q.price, previous_close: q.previous_close,
      change: q.change, change_pct: q.change_pct,
    }, activeIndicators);
    flashEl(document.getElementById('quoteSummary'));
  }

  // 2) 포트폴리오 종목
  const pfItem = portfolioItems.find(i => i.stock_code === code);
  if (pfItem && q.price != null) {
    pfItem.quote = {
      price: q.price, change: q.change, change_pct: q.change_pct,
      previous_close: q.previous_close, date: q.date,
    };
    if (!pfEditingCode && activeView === 'portfolio') renderPortfolio();
  }

  // 3) 사이드바
  const sidebarItem = recentListItems.find(i => i.stock_code === code);
  if (sidebarItem && q.price != null) {
    sidebarItem.quote_snapshot = {
      price: q.price, change: q.change, change_pct: q.change_pct,
    };
    refreshRecentList();
  }
};
```

- [ ] **Step 3: 커밋**

```bash
git add static/app.js
git commit -m "feat: add client-side QuoteManager with WebSocket + REST overflow"
```

---

### Task 6: 기존 polling 제거 및 QuoteManager 통합

**Files:**
- Modify: `static/app.js`
- Modify: `static/app-config.js`

- [ ] **Step 1: `fetchQuoteSnapshot` 및 KIS_PROXY 관련 코드 제거**

`app.js`에서 다음을 삭제/수정:

1. `const KIS_PROXY_BASE_URL = ...` 줄 삭제
2. `fetchQuoteSnapshot` 함수 전체 삭제
3. `QUOTE_REFRESH_INTERVAL_MS` 상수 삭제
4. `quoteRefreshTimer` 변수 삭제
5. `activeQuoteLoading` 변수 삭제
6. `refreshActiveQuote` 함수 삭제
7. `ensureQuoteRefreshTimer` 함수 삭제

- [ ] **Step 2: `initApp`에서 QuoteManager 시작**

`initApp()` 함수 수정:

```javascript
async function initApp() {
  await initAuth();
  await loadRecentList();
  loadMarketSummary();
  setInterval(loadMarketSummary, 60_000);
  QuoteManager.connect();
  _updateQuoteSubscriptions();
  trackEvent('app_ready', { auth_state: currentUser ? 'logged_in' : 'guest' });
  const params = new URLSearchParams(window.location.search);
  const code = params.get('code');
  if (code) {
    switchView('analysis');
    analyzeStock(code.trim());
  }
}
```

- [ ] **Step 3: 구독 갱신 함수 추가**

뷰 전환, 분석 완료, 포트폴리오 로드 등의 시점에 호출될 함수:

```javascript
function _updateQuoteSubscriptions() {
  const requested = { portfolio: [], benchmark: [], sidebar: [], analysis: [] };

  // 포트폴리오 종목
  portfolioItems.forEach(item => {
    requested.portfolio.push(item.stock_code);
    if (item.benchmark_code) requested.benchmark.push(item.benchmark_code);
  });

  // 사이드바 종목
  recentListItems.forEach(item => {
    requested.sidebar.push(item.stock_code);
  });

  // 분석 뷰 활성 종목
  if (activeStockCode) requested.analysis.push(activeStockCode);

  QuoteManager.updateSubscriptions(requested);
}
```

- [ ] **Step 4: 호출 지점 연결**

다음 함수들에 `_updateQuoteSubscriptions()` 호출 추가:

1. `renderResult()` 끝에 — 분석 완료 시
2. `loadPortfolio()` — `renderPortfolio()` 호출 뒤
3. `loadRecentList()` — `refreshRecentList()` 호출 뒤
4. `switchView()` — 뷰 전환 시

- [ ] **Step 5: 포트폴리오 quote refresh 정리**

`refreshPfQuotes` 함수 전체 삭제 (WebSocket이 대체함).
`schedulePfQuoteRefresh` 함수 삭제.
`pfQuoteRefreshing`, `pfQuoteTimer` 변수 삭제.
`loadPortfolio()`에서 `schedulePfQuoteRefresh()`, `refreshPfQuotes()` 호출 제거.

사이드바의 KIS_PROXY 직접 호출도 제거:
`loadRecentList()`에서 `if (KIS_PROXY_BASE_URL && recentListItems.length > 0)` 블록 삭제.

- [ ] **Step 6: `app-config.js` 정리**

```javascript
window.APP_CONFIG = {
  apiBaseUrl: "https://cantabile.tplinkdns.com:3691"
};
```

`kisProxyBaseUrl` 줄 제거.

- [ ] **Step 7: 포트폴리오 SSE 스트리밍 엔드포인트 유지 (REST fallback 용)**

`routes/portfolio.py`의 `/api/portfolio/quotes` SSE 엔드포인트는 그대로 유지.
overflow 종목의 REST polling이 `/api/quote/{code}` 개별 엔드포인트를 사용하므로 SSE는 레거시 호환용으로 남겨둠.

- [ ] **Step 8: 커밋**

```bash
git add static/app.js static/app-config.js
git commit -m "feat: replace KIS proxy polling with WebSocket quote manager"
```

---

### Task 7: 통합 테스트 및 배포

- [ ] **Step 1: 서버 구동 테스트**

```bash
cd /home/cantabile/Works/value_invest
python -c "
import asyncio
from dotenv import load_dotenv
from pathlib import Path
load_dotenv(Path('.kis.env'), override=True)
import kis_ws_manager
kis_ws_manager.load_credentials()
async def test():
    key = await kis_ws_manager._get_approval_key()
    print(f'Approval key obtained: {key[:20]}...')
asyncio.run(test())
"
```

Expected: `Approval key obtained: ...` 출력.

- [ ] **Step 2: 배포**

```bash
sudo systemctl restart value-invest
```

- [ ] **Step 3: 브라우저 동작 확인**

1. 포트폴리오 뷰로 전환 → 현재가가 실시간으로 표시되는지 확인
2. 분석 뷰에서 종목 검색 → 현재가 자동 갱신 확인
3. 사이드바 최근 검색 → 시세 표시 확인
4. 장 마감 시간에는 마지막 종가가 캐시에서 제공되는지 확인

- [ ] **Step 4: 최종 커밋**

```bash
git add -A
git commit -m "chore: finalize KIS WebSocket real-time quote integration"
```
