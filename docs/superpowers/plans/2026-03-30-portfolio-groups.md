# Portfolio Groups Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add user-defined group classification to portfolio items with default auto-assignment, group CRUD, table column with sorting, and dynamic filter bar.

**Architecture:** Add `portfolio_groups` table and `group_name` column to `user_portfolio`. Backend provides group CRUD endpoints + migration logic. Frontend replaces hardcoded filter buttons with dynamic group-based toggles, adds group column to table, and provides a management modal.

**Tech Stack:** Python/FastAPI, SQLite/aiosqlite, vanilla JS

---

### Task 1: Database Schema & Migration (cache.py)

**Files:**
- Modify: `/home/cantabile/Works/value_invest/cache.py:113-137` (init_db, add table + column + migration)

- [ ] **Step 1: Add portfolio_groups table to init_db**

In `cache.py`, inside `init_db()`, after the `user_portfolio` CREATE TABLE block (line 124), add:

```python
            CREATE TABLE IF NOT EXISTS portfolio_groups (
                google_sub TEXT NOT NULL,
                group_name TEXT NOT NULL,
                sort_order INTEGER DEFAULT 0,
                is_default INTEGER DEFAULT 0,
                PRIMARY KEY (google_sub, group_name),
                FOREIGN KEY (google_sub) REFERENCES users(google_sub) ON DELETE CASCADE
            );
```

- [ ] **Step 2: Add group_name column migration**

After the existing `_ensure_column` calls (around line 137), add:

```python
        await _ensure_column(db, "user_portfolio", "group_name", "TEXT")
```

- [ ] **Step 3: Add default groups migration logic**

After the `_ensure_column` calls, still inside `init_db()` before `await db.commit()`, add migration to create default groups for existing users and assign group_name to existing items:

```python
        # Migrate: ensure default groups exist for all users with portfolio items
        cursor = await db.execute("SELECT DISTINCT google_sub FROM user_portfolio")
        subs = [row["google_sub"] for row in await cursor.fetchall()]
        for sub in subs:
            await _ensure_default_groups(db, sub)
            # Assign group_name to items that don't have one
            await db.execute("""
                UPDATE user_portfolio SET group_name = '기타'
                WHERE google_sub = ? AND group_name IS NULL AND stock_code IN ('KRX_GOLD', 'CRYPTO_BTC', 'CRYPTO_ETH')
            """, (sub,))
            await db.execute("""
                UPDATE user_portfolio SET group_name = '한국주식'
                WHERE google_sub = ? AND group_name IS NULL AND length(stock_code) = 6 AND substr(stock_code, 1, 5) GLOB '[0-9][0-9][0-9][0-9][0-9]'
            """, (sub,))
            await db.execute("""
                UPDATE user_portfolio SET group_name = '해외주식'
                WHERE google_sub = ? AND group_name IS NULL
            """, (sub,))
```

- [ ] **Step 4: Add _ensure_default_groups helper**

Add this helper function right after `_ensure_column` (around line 149):

```python
_DEFAULT_GROUPS = [
    ("한국주식", 0, 1),
    ("해외주식", 1, 1),
    ("기타", 2, 1),
]

async def _ensure_default_groups(db: aiosqlite.Connection, google_sub: str):
    for name, order, is_default in _DEFAULT_GROUPS:
        await db.execute(
            "INSERT OR IGNORE INTO portfolio_groups (google_sub, group_name, sort_order, is_default) VALUES (?, ?, ?, ?)",
            (google_sub, name, order, is_default),
        )
```

- [ ] **Step 5: Verify migration runs**

Run: `python -c "import asyncio; from cache import init_db; asyncio.run(init_db())"`
Expected: No errors, DB has new table and column.

- [ ] **Step 6: Commit**

```bash
git add cache.py
git commit -m "Add portfolio_groups table and group_name column with migration"
```

---

### Task 2: Backend Group CRUD Functions (cache.py)

**Files:**
- Modify: `/home/cantabile/Works/value_invest/cache.py` (add functions after `save_portfolio_order`)

- [ ] **Step 1: Add get_portfolio_groups function**

After `save_portfolio_order` (line 899), add:

```python
async def get_portfolio_groups(google_sub: str) -> list[dict]:
    db = await get_db()
    try:
        await _ensure_default_groups(db, google_sub)
        await db.commit()
        cursor = await db.execute(
            "SELECT group_name, sort_order, is_default FROM portfolio_groups WHERE google_sub = ? ORDER BY sort_order ASC",
            (google_sub,),
        )
        return [dict(row) for row in await cursor.fetchall()]
    finally:
        await db.close()
```

- [ ] **Step 2: Add add_portfolio_group function**

```python
async def add_portfolio_group(google_sub: str, group_name: str) -> dict:
    db = await get_db()
    try:
        # Get next sort_order
        cursor = await db.execute(
            "SELECT MAX(sort_order) AS mx FROM portfolio_groups WHERE google_sub = ?",
            (google_sub,),
        )
        row = await cursor.fetchone()
        next_order = (row["mx"] or 0) + 1
        await db.execute(
            "INSERT INTO portfolio_groups (google_sub, group_name, sort_order, is_default) VALUES (?, ?, ?, 0)",
            (google_sub, group_name, next_order),
        )
        await db.commit()
        return {"group_name": group_name, "sort_order": next_order, "is_default": 0}
    finally:
        await db.close()
```

- [ ] **Step 3: Add rename_portfolio_group function**

```python
async def rename_portfolio_group(google_sub: str, old_name: str, new_name: str):
    db = await get_db()
    try:
        await db.execute(
            "UPDATE portfolio_groups SET group_name = ? WHERE google_sub = ? AND group_name = ?",
            (new_name, google_sub, old_name),
        )
        await db.execute(
            "UPDATE user_portfolio SET group_name = ? WHERE google_sub = ? AND group_name = ?",
            (new_name, google_sub, old_name),
        )
        await db.commit()
    finally:
        await db.close()
```

- [ ] **Step 4: Add delete_portfolio_group function**

```python
_SPECIAL_ASSETS = {"KRX_GOLD", "CRYPTO_BTC", "CRYPTO_ETH"}

def _default_group_for_code(stock_code: str) -> str:
    if stock_code in _SPECIAL_ASSETS:
        return "기타"
    if len(stock_code) == 6 and stock_code[:5].isdigit():
        return "한국주식"
    return "해외주식"

async def delete_portfolio_group(google_sub: str, group_name: str):
    db = await get_db()
    try:
        # Revert items to their default group
        cursor = await db.execute(
            "SELECT stock_code FROM user_portfolio WHERE google_sub = ? AND group_name = ?",
            (google_sub, group_name),
        )
        items = await cursor.fetchall()
        for item in items:
            default_grp = _default_group_for_code(item["stock_code"])
            await db.execute(
                "UPDATE user_portfolio SET group_name = ? WHERE google_sub = ? AND stock_code = ?",
                (default_grp, google_sub, item["stock_code"]),
            )
        await db.execute(
            "DELETE FROM portfolio_groups WHERE google_sub = ? AND group_name = ?",
            (google_sub, group_name),
        )
        await db.commit()
    finally:
        await db.close()
```

- [ ] **Step 5: Update get_portfolio to include group_name**

Modify `get_portfolio` (line 807-821) — update the SELECT:

```python
async def get_portfolio(google_sub: str) -> list[dict]:
    db = await get_db()
    try:
        cursor = await db.execute(
            """
            SELECT stock_code, stock_name, quantity, avg_price, sort_order,
                   COALESCE(currency, 'KRW') AS currency, group_name
            FROM user_portfolio
            WHERE google_sub = ?
            ORDER BY CASE WHEN sort_order IS NULL THEN 1 ELSE 0 END, sort_order ASC, created_at ASC
            """,
            (google_sub,),
        )
        return [dict(row) for row in await cursor.fetchall()]
    finally:
        await db.close()
```

- [ ] **Step 6: Update save_portfolio_item to accept group_name**

Modify `save_portfolio_item` signature and SQL (line 824-863):

```python
async def save_portfolio_item(
    google_sub: str, stock_code: str, stock_name: str, quantity: float, avg_price: float,
    currency: str = "KRW", group_name: str | None = None,
) -> dict:
    db = await get_db()
    try:
        now = datetime.now().isoformat()
        cursor = await db.execute(
            "SELECT sort_order, group_name FROM user_portfolio WHERE google_sub = ? AND stock_code = ?",
            (google_sub, stock_code),
        )
        existing = await cursor.fetchone()
        sort_order = existing["sort_order"] if existing else None
        # Keep existing group_name if not provided, auto-assign for new items
        if group_name is None:
            if existing:
                group_name = existing["group_name"]
            else:
                group_name = _default_group_for_code(stock_code)

        if sort_order is None and not existing:
            cursor = await db.execute(
                "SELECT MIN(sort_order) AS mn FROM user_portfolio WHERE google_sub = ? AND sort_order IS NOT NULL",
                (google_sub,),
            )
            row = await cursor.fetchone()
            min_order = row["mn"] if row and row["mn"] is not None else 0
            sort_order = min_order - 1

        await db.execute(
            """
            INSERT INTO user_portfolio (google_sub, stock_code, stock_name, quantity, avg_price, sort_order, currency, group_name, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(google_sub, stock_code) DO UPDATE SET
                stock_name = excluded.stock_name,
                quantity = excluded.quantity,
                avg_price = excluded.avg_price,
                currency = excluded.currency,
                group_name = excluded.group_name,
                updated_at = excluded.updated_at
            """,
            (google_sub, stock_code, stock_name, quantity, avg_price, sort_order, currency, group_name, now, now),
        )
        await db.commit()
        return {"stock_code": stock_code, "stock_name": stock_name, "quantity": quantity, "avg_price": avg_price, "currency": currency, "group_name": group_name}
    finally:
        await db.close()
```

- [ ] **Step 7: Commit**

```bash
git add cache.py
git commit -m "Add group CRUD functions and update portfolio queries for group_name"
```

---

### Task 3: Backend API Endpoints (routes/portfolio.py)

**Files:**
- Modify: `/home/cantabile/Works/value_invest/routes/portfolio.py`

- [ ] **Step 1: Add _default_group_for_code helper to routes**

At the top of the file (after `_is_korean_stock`, around line 25), add:

```python
def _default_group_for_code(code: str) -> str:
    if _is_special_asset(code):
        return "기타"
    if _is_korean_stock(code):
        return "한국주식"
    return "해외주식"
```

- [ ] **Step 2: Add GET /api/portfolio/groups endpoint**

Before the existing `@router.get("/api/portfolio/quotes")` (line 391), add:

```python
@router.get("/api/portfolio/groups")
async def get_groups(request: Request):
    user = _require_user(await get_current_user(request))
    return await cache.get_portfolio_groups(user["google_sub"])


@router.post("/api/portfolio/groups")
async def add_group(request: Request, payload: dict = Body(...)):
    user = _require_user(await get_current_user(request))
    name = str(payload.get("name") or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="그룹명을 입력해 주세요.")
    groups = await cache.get_portfolio_groups(user["google_sub"])
    if any(g["group_name"] == name for g in groups):
        raise HTTPException(status_code=400, detail="이미 존재하는 그룹명입니다.")
    result = await cache.add_portfolio_group(user["google_sub"], name)
    return {"ok": True, **result}


@router.put("/api/portfolio/groups/{group_name}")
async def rename_group(group_name: str, request: Request, payload: dict = Body(...)):
    user = _require_user(await get_current_user(request))
    new_name = str(payload.get("new_name") or "").strip()
    if not new_name:
        raise HTTPException(status_code=400, detail="새 그룹명을 입력해 주세요.")
    groups = await cache.get_portfolio_groups(user["google_sub"])
    target = next((g for g in groups if g["group_name"] == group_name), None)
    if not target:
        raise HTTPException(status_code=404, detail="그룹을 찾을 수 없습니다.")
    if any(g["group_name"] == new_name for g in groups):
        raise HTTPException(status_code=400, detail="이미 존재하는 그룹명입니다.")
    await cache.rename_portfolio_group(user["google_sub"], group_name, new_name)
    return {"ok": True}


@router.delete("/api/portfolio/groups/{group_name}")
async def delete_group(group_name: str, request: Request):
    user = _require_user(await get_current_user(request))
    groups = await cache.get_portfolio_groups(user["google_sub"])
    target = next((g for g in groups if g["group_name"] == group_name), None)
    if not target:
        raise HTTPException(status_code=404, detail="그룹을 찾을 수 없습니다.")
    if target["is_default"]:
        raise HTTPException(status_code=400, detail="기본 그룹은 삭제할 수 없습니다.")
    await cache.delete_portfolio_group(user["google_sub"], group_name)
    return {"ok": True}
```

- [ ] **Step 3: Update save_portfolio_item endpoint to pass group_name**

In the existing `save_portfolio_item` route (line 430-465), add `group_name` extraction and pass it through:

After `currency` extraction (line 463), add:

```python
    group_name = str(payload.get("group_name") or "").strip() or None
```

And update the `cache.save_portfolio_item` call to include `group_name`:

```python
    result = await cache.save_portfolio_item(user["google_sub"], stock_code, stock_name, quantity, avg_price, currency, group_name)
```

- [ ] **Step 4: Ensure default groups are created on first portfolio load**

In the `get_portfolio` endpoint (line 423-427), ensure groups are initialized:

```python
@router.get("/api/portfolio")
async def get_portfolio(request: Request):
    user = _require_user(await get_current_user(request))
    # Ensure default groups exist (idempotent)
    await cache.get_portfolio_groups(user["google_sub"])
    items = await cache.get_portfolio(user["google_sub"])
    return await _enrich_with_cached_quotes(items)
```

- [ ] **Step 5: Commit**

```bash
git add routes/portfolio.py
git commit -m "Add group CRUD API endpoints and group_name support in save item"
```

---

### Task 4: Frontend — Dynamic Filter Bar & Group State (app.js)

**Files:**
- Modify: `/home/cantabile/Works/value_invest/static/app.js:1587-1595` (state variables)
- Modify: `/home/cantabile/Works/value_invest/static/app.js:1716-1733` (pfMarketType, pfToggleFilter)
- Modify: `/home/cantabile/Works/value_invest/static/index.html:152-156` (filter bar HTML)

- [ ] **Step 1: Replace state variables**

Replace lines 1595 (the pfMarketFilter line):

```javascript
let pfMarketFilter = new Set(['kr', 'foreign', 'etc']);
```

with:

```javascript
let pfGroups = [];        // [{group_name, sort_order, is_default}, ...]
let pfGroupFilter = null; // null = all selected, Set of group_names = filtered
```

- [ ] **Step 2: Add group loading to loadPortfolio**

In `loadPortfolio()` (line 1610), after `const freshItems = await resp.json();` (line 1624), add group loading:

```javascript
    // Load groups
    try {
      const gResp = await apiFetch('/api/portfolio/groups');
      if (gResp.ok) pfGroups = await gResp.json();
    } catch {}
```

- [ ] **Step 3: Replace pfMarketType and pfToggleFilter**

Replace lines 1716-1733 (pfMarketType + pfToggleFilter functions) with:

```javascript
function pfGetGroup(item) {
  return item.group_name || '기타';
}

function pfToggleGroupFilter(groupName) {
  if (pfGroupFilter === null) {
    // All were selected, now select only this one
    pfGroupFilter = new Set([groupName]);
  } else if (pfGroupFilter.has(groupName)) {
    pfGroupFilter.delete(groupName);
    if (pfGroupFilter.size === 0) pfGroupFilter = null; // none selected = all selected
  } else {
    pfGroupFilter.add(groupName);
    // If all groups selected, reset to null
    if (pfGroups.length && pfGroupFilter.size === pfGroups.length) pfGroupFilter = null;
  }
  renderPortfolio();
}
```

- [ ] **Step 4: Update filter bar HTML**

In `index.html`, replace lines 152-156:

```html
    <div class="pf-filter-bar" id="pfFilterBar">
      <button class="pf-filter-btn active" data-filter="kr" onclick="pfToggleFilter('kr')">한국</button>
      <button class="pf-filter-btn active" data-filter="foreign" onclick="pfToggleFilter('foreign')">해외</button>
      <button class="pf-filter-btn active" data-filter="etc" onclick="pfToggleFilter('etc')">기타</button>
    </div>
```

with:

```html
    <div class="pf-filter-bar" id="pfFilterBar"></div>
```

The buttons are now rendered dynamically in `renderPortfolio()`.

- [ ] **Step 5: Commit**

```bash
git add static/app.js static/index.html
git commit -m "Replace hardcoded market filter with dynamic group-based filter state"
```

---

### Task 5: Frontend — Update renderPortfolio for Groups (app.js)

**Files:**
- Modify: `/home/cantabile/Works/value_invest/static/app.js:1735-1900` (renderPortfolio function)

- [ ] **Step 1: Replace filter bar rendering**

Replace lines 1743-1753 (the filterBar block inside renderPortfolio):

```javascript
  const filterBar = document.getElementById('pfFilterBar');
  if (filterBar) {
    filterBar.style.display = portfolioItems.length ? 'flex' : 'none';
    if (portfolioItems.length) {
      const counts = { kr: 0, foreign: 0, etc: 0 };
      portfolioItems.forEach(i => counts[pfMarketType(i.stock_code)]++);
      filterBar.querySelector('[data-filter="kr"]').textContent = `한국 (${counts.kr})`;
      filterBar.querySelector('[data-filter="foreign"]').textContent = `해외 (${counts.foreign})`;
      filterBar.querySelector('[data-filter="etc"]').textContent = `기타 (${counts.etc})`;
    }
  }
```

with:

```javascript
  const filterBar = document.getElementById('pfFilterBar');
  if (filterBar) {
    filterBar.style.display = portfolioItems.length ? 'flex' : 'none';
    if (portfolioItems.length && pfGroups.length) {
      const counts = {};
      pfGroups.forEach(g => counts[g.group_name] = 0);
      portfolioItems.forEach(i => {
        const gn = pfGetGroup(i);
        if (counts[gn] !== undefined) counts[gn]++;
        else counts[gn] = 1;
      });
      filterBar.innerHTML = pfGroups.map(g => {
        const active = pfGroupFilter === null || pfGroupFilter.has(g.group_name);
        return `<button class="pf-filter-btn${active ? ' active' : ''}" onclick="pfToggleGroupFilter('${escapeHtml(g.group_name)}')">${escapeHtml(g.group_name)} (${counts[g.group_name] || 0})</button>`;
      }).join('') + `<button class="pf-filter-btn pf-group-manage-btn" onclick="openGroupModal()" title="그룹 관리">\u2699</button>`;
    }
  }
```

- [ ] **Step 2: Replace market filter logic in row filtering**

Replace lines 1784-1785:

```javascript
  const allSelected = pfMarketFilter.size === 3;
  const rows = allSelected ? allRows : allRows.filter(r => pfMarketFilter.has(pfMarketType(r.stock_code)));
```

with:

```javascript
  const rows = pfGroupFilter === null ? allRows : allRows.filter(r => pfGroupFilter.has(pfGetGroup(r)));
```

- [ ] **Step 3: Add group_name to row data mapping**

In the `allRows` mapping (line 1763-1777), the spread `...item` already includes `group_name` from the server response. No change needed.

- [ ] **Step 4: Add group sort support**

In the sorting block (lines 1805-1816), add group sorting support. Replace:

```javascript
  if (pfSortKey) {
    rows.sort((a, b) => {
      let va, vb;
      if (pfSortKey === 'name') {
        va = a.stock_name; vb = b.stock_name;
        return pfSortAsc ? va.localeCompare(vb) : vb.localeCompare(va);
      }
      va = a[pfSortKey] ?? -Infinity;
      vb = b[pfSortKey] ?? -Infinity;
      return pfSortAsc ? va - vb : vb - va;
    });
  }
```

with:

```javascript
  if (pfSortKey) {
    rows.sort((a, b) => {
      let va, vb;
      if (pfSortKey === 'name' || pfSortKey === 'group') {
        va = pfSortKey === 'group' ? pfGetGroup(a) : a.stock_name;
        vb = pfSortKey === 'group' ? pfGetGroup(b) : b.stock_name;
        return pfSortAsc ? va.localeCompare(vb) : vb.localeCompare(va);
      }
      va = a[pfSortKey] ?? -Infinity;
      vb = b[pfSortKey] ?? -Infinity;
      return pfSortAsc ? va - vb : vb - va;
    });
  }
```

- [ ] **Step 5: Commit**

```bash
git add static/app.js
git commit -m "Update renderPortfolio for dynamic group filter and group sorting"
```

---

### Task 6: Frontend — Group Column in Table (app.js + index.html)

**Files:**
- Modify: `/home/cantabile/Works/value_invest/static/index.html:174-185` (table header)
- Modify: `/home/cantabile/Works/value_invest/static/app.js:1860-1908` (table row + footer rendering)

- [ ] **Step 1: Add group column header in HTML**

In `index.html`, after the 종목명 `<th>` (line 176), add:

```html
            <th class="pf-col-group pf-sortable" data-sort="group" onclick="pfSort('group')">그룹</th>
```

- [ ] **Step 2: Add group cell to normal row template**

In `renderPortfolio()`, in the non-editing row template (around line 1886-1899), after the first `<td>` (stock name cell), add a group cell with a dropdown:

Replace the non-editing row return (line 1886-1899):

```javascript
    return `<tr draggable="true" data-code="${r.stock_code}">
      <td><a href="#" class="pf-stock-link" onclick="pfGoAnalyze('${r.stock_code}');return false;"><strong>${escapeHtml(r.stock_name)}</strong></a> <span class="pf-stock-code">${r.stock_code}</span>${curTag}</td>
      <td class="pf-col-group"><select class="pf-group-select" onchange="pfChangeGroup('${r.stock_code}', this.value)">${pfGroups.map(g => `<option value="${escapeHtml(g.group_name)}"${g.group_name === pfGetGroup(r) ? ' selected' : ''}>${escapeHtml(g.group_name)}</option>`).join('')}</select></td>
      <td class="pf-col-num">${fmtChangePct(r.changePct, r.change)}</td>
      <td class="pf-col-num">${fmtNum(r.avgPrice)}</td>
      <td class="pf-col-num">${r.price !== null ? fmtNum(r.price) : '-'}</td>
      <td class="pf-col-num">${fmtQty(r.qty)}</td>
      <td class="pf-col-num"><span class="pf-return ${returnClass(r.returnPct)}">${r.returnPct !== null ? fmtPct(r.returnPct) : '-'}</span></td>
      <td class="pf-col-num">${r.marketValue !== null ? fmtNum(r.marketValue) : '-'}</td>
      <td class="pf-col-num">${fmtPct(weight)}</td>
      <td class="pf-col-act"><div class="pf-row-actions">
        <button class="pf-row-btn" onclick="startPortfolioEdit('${r.stock_code}')" title="편집">E</button>
        <button class="pf-row-btn delete" onclick="deletePortfolioItem('${r.stock_code}')" title="삭제">X</button>
      </div></td>
    </tr>`;
```

- [ ] **Step 3: Add group cell to editing row template**

Similarly, in the editing row template (line 1871-1884), after the first `<td>`, add:

```javascript
        <td class="pf-col-group"><select class="pf-group-select" onchange="pfChangeGroup('${r.stock_code}', this.value)">${pfGroups.map(g => `<option value="${escapeHtml(g.group_name)}"${g.group_name === pfGetGroup(r) ? ' selected' : ''}>${escapeHtml(g.group_name)}</option>`).join('')}</select></td>
```

- [ ] **Step 4: Update footer colspan**

In the footer (line 1903), update colspan from 6 to 7:

```javascript
  tfoot.innerHTML = `<tr>
    <td colspan="7">합계</td>
    <td class="pf-col-num">${fmtNum(totalMarketValue)}</td>
    <td class="pf-col-num">${fmtPct(grandTotalMarketValue > 0 ? totalMarketValue / grandTotalMarketValue * 100 : 0)}</td>
    <td></td>
  </tr>`;
```

- [ ] **Step 5: Add pfChangeGroup function**

After `pfDropRow` function (around line 1980), add:

```javascript
async function pfChangeGroup(stockCode, groupName) {
  const item = portfolioItems.find(i => i.stock_code === stockCode);
  if (!item) return;
  try {
    const resp = await apiFetch(`/api/portfolio/${stockCode}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        stock_name: item.stock_name,
        quantity: item.quantity,
        avg_price: item.avg_price,
        group_name: groupName,
      }),
    });
    if (!resp.ok) throw new Error('그룹 변경 실패');
    item.group_name = groupName;
    renderPortfolio();
  } catch (e) { alert(e.message); }
}
```

- [ ] **Step 6: Commit**

```bash
git add static/app.js static/index.html
git commit -m "Add group column to portfolio table with inline dropdown and sorting"
```

---

### Task 7: Frontend — Group Management Modal (app.js + index.html + styles.css)

**Files:**
- Modify: `/home/cantabile/Works/value_invest/static/index.html` (add modal HTML)
- Modify: `/home/cantabile/Works/value_invest/static/app.js` (add modal logic)
- Modify: `/home/cantabile/Works/value_invest/static/styles.css` (add modal styles)

- [ ] **Step 1: Add modal HTML**

In `index.html`, before the closing `</body>` tag, add:

```html
  <div class="pf-modal-overlay" id="pfGroupModal" style="display:none;" onclick="if(event.target===this)closeGroupModal()">
    <div class="pf-modal">
      <div class="pf-modal-header">
        <h3>그룹 관리</h3>
        <button class="pf-modal-close" onclick="closeGroupModal()">&times;</button>
      </div>
      <div class="pf-modal-body" id="pfGroupModalBody"></div>
      <div class="pf-modal-footer">
        <input class="pf-modal-input" id="pfNewGroupInput" placeholder="새 그룹명">
        <button class="pf-modal-add-btn" onclick="addNewGroup()">추가</button>
      </div>
    </div>
  </div>
```

- [ ] **Step 2: Add modal JavaScript functions**

In `app.js`, after `pfChangeGroup`, add:

```javascript
function openGroupModal() {
  const modal = document.getElementById('pfGroupModal');
  modal.style.display = 'flex';
  renderGroupModalBody();
}

function closeGroupModal() {
  document.getElementById('pfGroupModal').style.display = 'none';
}

function renderGroupModalBody() {
  const body = document.getElementById('pfGroupModalBody');
  const counts = {};
  portfolioItems.forEach(i => {
    const g = pfGetGroup(i);
    counts[g] = (counts[g] || 0) + 1;
  });
  body.innerHTML = pfGroups.map(g => {
    const cnt = counts[g.group_name] || 0;
    const delBtn = g.is_default
      ? ''
      : `<button class="pf-grp-del" onclick="deleteGroup('${escapeHtml(g.group_name)}')" title="삭제">&times;</button>`;
    return `<div class="pf-grp-row">
      <input class="pf-grp-name" value="${escapeHtml(g.group_name)}" data-orig="${escapeHtml(g.group_name)}" onblur="renameGroup(this)">
      <span class="pf-grp-cnt">${cnt}종목</span>
      ${delBtn}
    </div>`;
  }).join('');
}

async function addNewGroup() {
  const input = document.getElementById('pfNewGroupInput');
  const name = input.value.trim();
  if (!name) return;
  try {
    const resp = await apiFetch('/api/portfolio/groups', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name }),
    });
    if (!resp.ok) {
      const d = await resp.json().catch(() => ({}));
      throw new Error(d.detail || '추가 실패');
    }
    const result = await resp.json();
    pfGroups.push(result);
    input.value = '';
    renderGroupModalBody();
    renderPortfolio();
  } catch (e) { alert(e.message); }
}

async function renameGroup(inputEl) {
  const orig = inputEl.dataset.orig;
  const newName = inputEl.value.trim();
  if (!newName || newName === orig) {
    inputEl.value = orig;
    return;
  }
  try {
    const resp = await apiFetch(`/api/portfolio/groups/${encodeURIComponent(orig)}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ new_name: newName }),
    });
    if (!resp.ok) {
      const d = await resp.json().catch(() => ({}));
      throw new Error(d.detail || '변경 실패');
    }
    // Update local state
    const g = pfGroups.find(g => g.group_name === orig);
    if (g) g.group_name = newName;
    portfolioItems.forEach(i => { if (i.group_name === orig) i.group_name = newName; });
    // Update pfGroupFilter if needed
    if (pfGroupFilter && pfGroupFilter.has(orig)) {
      pfGroupFilter.delete(orig);
      pfGroupFilter.add(newName);
    }
    inputEl.dataset.orig = newName;
    renderPortfolio();
  } catch (e) {
    alert(e.message);
    inputEl.value = orig;
  }
}

async function deleteGroup(groupName) {
  const counts = {};
  portfolioItems.forEach(i => {
    const g = pfGetGroup(i);
    counts[g] = (counts[g] || 0) + 1;
  });
  const cnt = counts[groupName] || 0;
  if (cnt > 0 && !confirm(`"${groupName}" 그룹에 ${cnt}개 종목이 있습니다. 삭제하면 기본 그룹으로 이동합니다. 삭제할까요?`)) return;
  try {
    const resp = await apiFetch(`/api/portfolio/groups/${encodeURIComponent(groupName)}`, { method: 'DELETE' });
    if (!resp.ok) {
      const d = await resp.json().catch(() => ({}));
      throw new Error(d.detail || '삭제 실패');
    }
    pfGroups = pfGroups.filter(g => g.group_name !== groupName);
    if (pfGroupFilter) pfGroupFilter.delete(groupName);
    // Reload portfolio to get updated group_name assignments
    await loadPortfolio();
    renderGroupModalBody();
  } catch (e) { alert(e.message); }
}
```

- [ ] **Step 3: Add modal CSS**

In `styles.css`, at the end, add:

```css
/* Group management modal */
.pf-modal-overlay {
  position: fixed; top: 0; left: 0; right: 0; bottom: 0;
  background: rgba(0,0,0,0.4); z-index: 1000;
  display: flex; align-items: center; justify-content: center;
}
.pf-modal {
  background: var(--surface); border-radius: 12px; padding: 0;
  min-width: 340px; max-width: 420px; width: 90%; box-shadow: 0 8px 32px rgba(0,0,0,0.2);
}
.pf-modal-header {
  display: flex; align-items: center; justify-content: space-between;
  padding: 16px 20px; border-bottom: 1px solid var(--border);
}
.pf-modal-header h3 { margin: 0; font-size: 16px; }
.pf-modal-close {
  background: none; border: none; font-size: 22px; cursor: pointer;
  color: var(--text-secondary); line-height: 1;
}
.pf-modal-body { padding: 12px 20px; max-height: 320px; overflow-y: auto; }
.pf-modal-footer {
  display: flex; gap: 8px; padding: 12px 20px; border-top: 1px solid var(--border);
}
.pf-modal-input {
  flex: 1; padding: 8px 12px; border: 1px solid var(--border); border-radius: 8px;
  background: var(--bg); color: var(--text); font-size: 13px;
}
.pf-modal-add-btn {
  padding: 8px 16px; border: none; border-radius: 8px;
  background: var(--primary); color: white; font-size: 13px; font-weight: 600; cursor: pointer;
}
.pf-grp-row {
  display: flex; align-items: center; gap: 8px; padding: 8px 0;
  border-bottom: 1px solid var(--border);
}
.pf-grp-row:last-child { border-bottom: none; }
.pf-grp-name {
  flex: 1; padding: 6px 8px; border: 1px solid transparent; border-radius: 6px;
  background: transparent; color: var(--text); font-size: 14px;
}
.pf-grp-name:focus { border-color: var(--primary); background: var(--bg); outline: none; }
.pf-grp-cnt { font-size: 12px; color: var(--text-secondary); white-space: nowrap; }
.pf-grp-del {
  background: none; border: none; color: var(--text-secondary); font-size: 18px;
  cursor: pointer; line-height: 1; padding: 2px 6px;
}
.pf-grp-del:hover { color: #e74c3c; }
.pf-group-manage-btn { font-size: 16px !important; padding: 6px 10px !important; }
/* Group column */
.pf-col-group { min-width: 80px; }
.pf-group-select {
  padding: 3px 6px; border: 1px solid var(--border); border-radius: 6px;
  background: var(--surface); color: var(--text); font-size: 12px; cursor: pointer;
}
```

- [ ] **Step 4: Commit**

```bash
git add static/app.js static/index.html static/styles.css
git commit -m "Add group management modal with add/rename/delete functionality"
```

---

### Task 8: Fix Bulk Import for Group Assignment (routes/portfolio.py)

**Files:**
- Modify: `/home/cantabile/Works/value_invest/routes/portfolio.py:486-533` (bulk_import)

- [ ] **Step 1: Update bulk import to auto-assign groups**

The `bulk_import` endpoint already calls `cache.save_portfolio_item` which now auto-assigns `group_name` when `None` is passed. No change needed — the default parameter in `save_portfolio_item` handles this automatically.

Verify by reading the code path: `bulk_import` → `cache.save_portfolio_item(... currency)` — `group_name` defaults to `None` → `_default_group_for_code()` assigns the correct group.

- [ ] **Step 2: Commit** (skip if no changes)

---

### Task 9: End-to-End Verification

- [ ] **Step 1: Restart the server and verify**

```bash
sudo systemctl restart value_invest
```

- [ ] **Step 2: Manual verification checklist**

1. Open portfolio page → filter bar shows 한국주식/해외주식/기타 buttons dynamically
2. Table has "그룹" column after 종목명
3. Click group header → sorts by group name
4. Click group dropdown on a row → changes the group
5. Click gear icon → group management modal opens
6. Add a custom group in modal → filter bar gets new button
7. Rename a group → all items update
8. Delete a custom group with items → items revert to defaults
9. Cannot delete 한국주식/해외주식/기타
10. Add new stock via search → auto-assigned to correct group
11. Bulk CSV import → items auto-assigned to correct groups

- [ ] **Step 3: Commit any fixes**
