# 국민연금공단 포트폴리오 탭 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 국민연금공단 보유종목을 매일 22시에 스크래핑하여 정적 HTML로 생성하고, SPA의 세 번째 탭으로 제공

**Architecture:** FnGuide 스크래핑 → DB 저장 → 정적 HTML 생성 (카드+테이블+차트 데이터 인라인) → SPA 탭에서 fetch하여 삽입. 모든 데이터는 장마감가 기준 정적. 백필은 현재 지분율 고정 + KIS/Naver 과거 시세로 2026-01-01부터 소급.

**Tech Stack:** Python (httpx, BeautifulSoup), SQLite, ECharts (인라인 데이터), Jinja2-style 문자열 템플릿

---

### Task 1: FnGuide 스크래퍼

**Files:**
- Create: `nps_scraper.py`

- [ ] **Step 1:** FnGuide Inst_Data.asp 호출하여 국민연금 보유종목 파싱

```python
# nps_scraper.py
"""Scrape NPS (국민연금공단) holdings from FnGuide."""
import logging
import re
import subprocess
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

FNGUIDE_URL = "https://comp.fnguide.com/SVO/WooriRenewal/Inst_Data.asp?strInstCD=49530"


def fetch_nps_holdings() -> list[dict]:
    """Fetch current NPS holdings from FnGuide. Returns list of dicts."""
    result = subprocess.run(
        ["curl", "-s", FNGUIDE_URL],
        capture_output=True, timeout=30,
    )
    soup = BeautifulSoup(result.stdout, "html.parser", from_encoding="euc-kr")
    rows = soup.find_all("tr")
    holdings = []
    for tr in rows:
        tds = tr.find_all("td")
        if len(tds) < 7:
            continue
        texts = [td.get_text(strip=True) for td in tds]
        holdings.append({
            "rank": int(texts[0]),
            "name": texts[1],
            "shares": int(texts[2].replace(",", "")),
            "shares_change": int(texts[3].replace(",", "")) if texts[3].replace(",", "").lstrip("-").isdigit() else 0,
            "ownership_pct": float(texts[5]),
            "total_ownership_pct": float(texts[6]),
            "report_date": texts[7].replace(".", "-") if len(texts) > 7 and texts[7] else "",
        })
    return holdings
```

- [ ] **Step 2:** 종목명 → 종목코드 매칭 (corp_codes DB 활용)

```python
# nps_scraper.py에 추가
import cache

async def resolve_stock_codes(holdings: list[dict]) -> list[dict]:
    """Match holding names to stock codes via corp_codes DB."""
    for h in holdings:
        results = await cache.search_corp(h["name"])
        if results:
            # Exact name match first
            exact = [r for r in results if r["corp_name"] == h["name"]]
            h["stock_code"] = exact[0]["stock_code"] if exact else results[0]["stock_code"]
        else:
            h["stock_code"] = ""
            logger.warning("NPS: no match for %s", h["name"])
    return holdings
```

- [ ] **Step 3:** 테스트 실행

```bash
python3 -c "
from nps_scraper import fetch_nps_holdings
h = fetch_nps_holdings()
print(f'{len(h)} holdings')
for x in h[:5]: print(x)
"
```

- [ ] **Step 4:** Commit

```bash
git add nps_scraper.py
git commit -m "feat: add NPS holdings scraper from FnGuide"
```

---

### Task 2: DB 테이블 + CRUD

**Files:**
- Modify: `cache.py`

- [ ] **Step 1:** nps_holdings, nps_snapshots 테이블 추가

```sql
-- cache.py init_db에 추가
CREATE TABLE IF NOT EXISTS nps_holdings (
    date TEXT NOT NULL,
    stock_code TEXT NOT NULL,
    stock_name TEXT NOT NULL,
    shares INTEGER NOT NULL,
    ownership_pct REAL NOT NULL DEFAULT 0,
    price REAL,
    market_value REAL,
    change_pct REAL,
    PRIMARY KEY (date, stock_code)
);

CREATE TABLE IF NOT EXISTS nps_snapshots (
    date TEXT NOT NULL PRIMARY KEY,
    total_value REAL NOT NULL DEFAULT 0,
    nav REAL NOT NULL DEFAULT 1000,
    total_count INTEGER NOT NULL DEFAULT 0,
    generated_html TEXT
);

CREATE INDEX IF NOT EXISTS idx_nps_holdings_date ON nps_holdings(date);
```

- [ ] **Step 2:** CRUD 함수 추가

```python
async def save_nps_holdings(date: str, items: list[dict]):
    db = await get_db()
    await db.executemany(
        """INSERT OR REPLACE INTO nps_holdings
        (date, stock_code, stock_name, shares, ownership_pct, price, market_value, change_pct)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        [(date, it["stock_code"], it["stock_name"], it["shares"],
          it["ownership_pct"], it.get("price"), it.get("market_value"), it.get("change_pct"))
         for it in items],
    )
    await db.commit()

async def get_nps_holdings(date: str) -> list[dict]:
    ...

async def save_nps_snapshot(date: str, total_value: float, nav: float, count: int, html: str | None = None):
    ...

async def get_nps_snapshots() -> list[dict]:
    ...

async def get_latest_nps_html() -> str | None:
    ...
```

- [ ] **Step 3:** Commit

---

### Task 3: 일일 스냅샷 + HTML 생성 스크립트

**Files:**
- Create: `snapshot_nps.py`
- Create: `nps-snapshot.service`
- Create: `nps-snapshot.timer`

- [ ] **Step 1:** 스크립트 구현 — 스크래핑 → 시세 조회 → NAV 계산 → HTML 생성 → DB 저장

```python
# snapshot_nps.py
"""Daily NPS portfolio snapshot + static HTML generation. Run at 22:00."""
import asyncio, logging
from datetime import date
import cache
from nps_scraper import fetch_nps_holdings, resolve_stock_codes

async def run():
    await cache.init_db()
    snap_date = date.today().isoformat()
    
    # 1. Scrape
    holdings = fetch_nps_holdings()
    holdings = await resolve_stock_codes(holdings)
    
    # 2. Fetch closing prices
    from routes.portfolio import _fetch_quote
    for h in holdings:
        if not h["stock_code"]: continue
        q = await _fetch_quote(h["stock_code"])
        h["price"] = q.get("price") if q else None
        h["market_value"] = h["shares"] * h["price"] if h["price"] else None
        h["change_pct"] = q.get("change_pct") if q else None
        await asyncio.sleep(0.15)
    
    # 3. Calculate NAV
    total_value = sum(h["market_value"] for h in holdings if h.get("market_value"))
    prev = await cache.get_nps_snapshots()  # latest
    ...NAV calculation...
    
    # 4. Save holdings
    await cache.save_nps_holdings(snap_date, [h for h in holdings if h["stock_code"]])
    
    # 5. Generate HTML
    html = generate_nps_html(snap_date, holdings, nav_history)
    await cache.save_nps_snapshot(snap_date, total_value, nav, len(holdings), html)

def generate_nps_html(date, holdings, nav_history):
    """Generate complete HTML fragment for NPS tab."""
    # Cards (Total, Today, MTD, YTD — no invested amount)
    # Table (종목명, 등락률, 현재가, 수량, 평가금액, 비중, 지분율)
    # Treemap data (inline JSON)
    # NAV chart data (inline JSON)
    # Value chart data (inline JSON)
    ...return html string...
```

- [ ] **Step 2:** systemd timer (22:05, after main snapshot at 22:00)

- [ ] **Step 3:** Commit

---

### Task 4: API 엔드포인트

**Files:**
- Create: `routes/nps.py`
- Modify: `main.py`

- [ ] **Step 1:** NPS HTML 서빙 엔드포인트

```python
@router.get("/api/nps/html")
async def get_nps_html():
    html = await cache.get_latest_nps_html()
    if not html:
        return Response(content="<div>데이터 준비 중...</div>", media_type="text/html")
    return Response(content=html, media_type="text/html")
```

- [ ] **Step 2:** main.py에 라우터 등록

- [ ] **Step 3:** Commit

---

### Task 5: 프론트엔드 탭 통합

**Files:**
- Modify: `static/index.html`
- Modify: `static/js/app-main.js`
- Modify: `static/styles.css`

- [ ] **Step 1:** 탭 버튼에 국민연금공단 추가

```html
<nav class="main-nav">
  <button class="nav-btn active" data-view="analysis" onclick="switchView('analysis')">분석</button>
  <button class="nav-btn" data-view="portfolio" onclick="switchView('portfolio')">포트폴리오</button>
  <button class="nav-btn" data-view="nps" onclick="switchView('nps')">국민연금</button>
</nav>
```

- [ ] **Step 2:** NPS 뷰 컨테이너 추가

```html
<div id="npsView" style="display:none;">
  <div id="npsContent">로딩 중...</div>
</div>
```

- [ ] **Step 3:** switchView에 nps 처리 추가 — fetch('/api/nps/html') → innerHTML

- [ ] **Step 4:** NPS HTML 내의 차트를 ECharts로 렌더 (인라인 데이터 사용)

- [ ] **Step 5:** Commit

---

### Task 6: 백필 (2026-01-01 ~ 현재)

**Files:**
- Create: `backfill_nps.py`

- [ ] **Step 1:** 현재 보유종목 + 현재 지분율 고정, 과거 시세로 일별 스냅샷 생성

```python
# backfill_nps.py
"""Backfill NPS snapshots from 2026-01-01 using current holdings + historical prices."""
# For each trading day from 2026-01-01 to yesterday:
#   1. Use current holdings (shares, ownership_pct)
#   2. Fetch historical close price for that date (yfinance)
#   3. Calculate market_value, total_value, NAV
#   4. Save to nps_holdings + nps_snapshots
#   5. Generate HTML for that date
```

- [ ] **Step 2:** 실행 + 검증

- [ ] **Step 3:** Commit

---

### Task 7: HTML 생성 내 차트 렌더링

- [ ] **Step 1:** NPS HTML에 인라인 script로 ECharts 차트 렌더링

```html
<!-- generate_nps_html이 생성하는 HTML 내부 -->
<script>
  const NPS_DATA = { /* nav_history, holdings, etc */ };
  // 페이지 로드 시 ECharts로 treemap, nav chart, value chart 렌더
</script>
```

- [ ] **Step 2:** 트리맵 (종목별 비중 + 일간 등락률)
- [ ] **Step 3:** NAV 기준가 추이 + 평가금액 추이 (그래디언트 area chart)
- [ ] **Step 4:** Commit
