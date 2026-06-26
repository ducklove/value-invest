# Linked Projects

`value-invest` is the portfolio and analysis hub. The related repositories stay
as independent deployables, and this app integrates them through stable public
URLs or server-side environment variables instead of copying their code.

## Project Map

| Integration key | Repository | Local directory | How value-invest uses it |
| --- | --- | --- | --- |
| `holdingValue` | `https://github.com/ducklove/holding_value` | `../hodling-value` or `../holding_value` | Loads `api/holdings.json` and links holding-company rows to the dashboard. |
| `preferredSpread` | `https://github.com/ducklove/common_preferred_spread` | `../common_preferred_spread` | Links preferred-stock rows to the spread dashboard. |
| `spacHunter` | `https://github.com/ducklove/spac-hunter` | `../spac-hunter` | Links SPAC portfolio rows to the spac dashboard via `?code=`. Exposes `baseUrl` only (no local `config.json`). |
| `buybacks` | `https://github.com/ducklove/buybacks` | — | Links the analysis-tools card to the buybacks dashboard and summarizes published holding snapshots by treasury-stock holding ratio. Exposes `baseUrl` only. |
| `goldGap` | `https://github.com/ducklove/gold_gap` | `../gold_gap` | Links `KRX_GOLD` and `CRYPTO_BTC` portfolio rows to the gold/bitcoin gap dashboard. |
| `npsTracker` | `https://github.com/ducklove/nps-tracker` | — | Embeds the NPS domestic-equity portfolio dashboard in the NPS tab via iframe and summarizes `current.json` for the 투자정보 insight card. Exposes `baseUrl` only. |
| `kisProxy` | `https://github.com/ducklove/kis-proxy` | `../kis-proxy` | Used server-side by `kis_proxy_client.py` through `KIS_PROXY_BASE_URL`. |

> `finance-pi` (`../finance-pi`, Raspberry Pi 데이터레이크 `:8400`)는 위 integration
> registry에 속하지 않는 인프라 백엔드다. `value-invest`는 이를 `CLOSE_PRICE_API_BASE_URL`
> 종가 백업 소스로만 쓴다(아래 Local Config Discovery 참고).

## Operating Model

- Keep each project deployable on its own. `value-invest` should compose data and
  navigation, not vendor sibling project code.
- Use `value-invest` `/admin.html` as the central operating console for linked
  project config files. The admin API validates and writes each sibling
  project's `config.json` locally:
  - `holdingValue`: holding-company list and subsidiary share counts.
  - `preferredSpread`: common/preferred pair list.
  - `goldGap`: asset labels, portfolio code mapping, and gap thresholds.
- Keep browser-facing integration URLs and public metadata in
  `window.APP_CONFIG.integrations`. The FastAPI `/app-config.js` route reads
  sibling project settings when they are present locally, then falls back to
  public GitHub Pages URLs.
- Use `/api/integrations` to inspect the normalized integration config that the
  running `value-invest` server is currently exposing.
- Keep KIS proxy access server-side. The proxy is public, but browser calls from
  the HTTPS app can hit mixed-content/CORS constraints and should not become the
  default path.
- Use `scripts/sync-linked-projects.ps1` to clone missing sibling repos and fetch
  their latest remote state without touching dirty worktrees.

## Current Frontend Links

- Preferred stocks open `preferredSpread` with `?code=<preferred-code>`.
- Holding-company stocks open `holdingValue` with `?code=<stock-code>`.
- SPAC stocks open `spacHunter` with `?code=<spac-code>`.
- `KRX_GOLD` opens `goldGap` with `?asset=gold`.
- `CRYPTO_BTC` opens `goldGap` with `?asset=bitcoin`.

## Server-side External Insights

Separate from the browser deep-links above, `external_tools.py`
(`fetch_external_insights`) pulls each dashboard's published JSON from
`raw.githubusercontent`, summarizes it, and feeds the AI portfolio-insight layer.
Results are cached ~15 minutes and each fetch fails independently:

- `holdingValue`: `current.json` — largest NAV discounts.
- `preferredSpread`: `current.json` — widest common/preferred spreads.
- `goldGap`: `data.json` — gap per asset.
- `spacHunter`: `current.json` (branch `main`) — deepest discount-to-offer SPACs.
- `npsTracker`: `current.json` (branch `main`) — National Pension Service domestic-equity holdings by portfolio weight (top), NAV, and total value.
- `buybacks`: `data/buybacks/holding_snapshots.json` — latest common-stock treasury holding ratios (top).

This path needs neither local config nor the `/admin.html` config writer; it only
needs each dashboard's public GitHub Pages / raw content to be reachable.

## Shared Notification API (공용 알림)

서브프로젝트는 텔레그램 봇 토큰이나 카카오 OAuth 토큰을 **직접 들고 있지
않는다**. 알림 채널 설정·토큰·카카오 refresh 갱신은 전부 이 허브 한 곳에
있고, 다른 프로젝트는 HTTP 한 번으로 발송을 위임한다. 카카오 refresh
token 은 갱신 시 회전하므로 여러 프로세스가 같은 토큰을 공유하면 서로를
무효화한다 — 발송 주체를 허브로 단일화해야 하는 구조적 이유이기도 하다.

```
POST /api/internal/notify
{
  "text":   "금 시세 괴리 5% 초과",   // 필수
  "title":  "골드갭 알림",            // 선택 — 첫 줄에 📌 표기
  "source": "gold_gap",              // 선택 — 마지막 줄 "— gold_gap"
  "google_sub": "..."                // 선택 — 생략 시 활성 채널 보유 전체 사용자
}
→ {"ok": true, "sent": 2, "users": 1}
```

인증은 다른 `/api/internal/*` 와 동일: 같은 호스트는 loopback 으로 충분하고,
다른 호스트(finance-pi 등)는 `.env.production` 의 `INTERNAL_API_TOKEN` 값을
`X-Internal-Token` 헤더로 보낸다.

```bash
# 같은 Pi 의 다른 프로젝트 (loopback)
curl -s -X POST http://127.0.0.1:3691/api/internal/notify \
  -H 'Content-Type: application/json' \
  -d '{"text":"백테스트 완료","source":"nps-tracker"}'
```

```python
# 다른 호스트의 프로젝트 (예: finance-pi) — 의존성은 httpx 뿐
import httpx

def notify(text: str, *, title: str = "", source: str = "finance-pi") -> None:
    httpx.post(
        "https://cantabile.tplinkdns.com:3691/api/internal/notify",
        json={"text": text, "title": title, "source": source},
        headers={"X-Internal-Token": INTERNAL_API_TOKEN},
        timeout=10,
    )
```

메시지는 텔레그램 한도 아래(3,800자)로 잘리고, 채널 단위 실패는 허브가
삼키므로 호출자는 fire-and-forget 으로 쓰면 된다. 시세가 필요한
서브프로젝트는 같은 원리로 `POST /api/asset-quotes` (국내·해외·금·암호화폐
공통, 인증 불요)를 호출한다 — 시세 수집기/토큰을 복제하지 않는다.

## Local Config Discovery

By default `value-invest` looks one directory above the repo root for sibling
projects. Override this with `LINKED_PROJECTS_ROOT` or per-project directories:

- `HOLDING_VALUE_DIR`
- `PREFERRED_SPREAD_DIR`
- `GOLD_GAP_DIR`

Public base URLs can be overridden with:

- `HOLDING_VALUE_BASE_URL`
- `PREFERRED_SPREAD_BASE_URL`
- `SPAC_HUNTER_BASE_URL`
- `GOLD_GAP_BASE_URL`
- `NPS_TRACKER_BASE_URL`
- `KIS_PROXY_BASE_URL`
- `KIS_PROXY_TOKEN` (optional, sent as `X-KIS-Proxy-Token` when the proxy is
  configured with `KIS_PROXY_PUBLIC_TOKENS`)
- `CLOSE_PRICE_API_BASE_URL` (optional, defaults to `http://192.168.68.84`; this
  is the `finance-pi` data-lake internal API, used as a backup source for daily
  adjusted close history when KIS history fails or is empty)
- `CLOSE_PRICE_API_ENABLED` (set to `0` to disable the internal close-price
  shortcut and always use the KIS proxy)

## AI Admin Operations

The main admin console also owns runtime AI configuration:

- OpenRouter key: stored server-side only when saved from admin, shown masked in
  the UI. If no DB value exists, the app falls back to `OPENROUTER_API_KEY` from
  process env or `keys.txt`.
- Feature model registry:
  - `portfolio_fast`
  - `portfolio_balanced`
  - `portfolio_premium`
  - `wiki_qa`
  - `wiki_ingestion`
- Usage ledger: portfolio insight, wiki Q&A, and wiki ingestion calls write
  token/cost/latency rows to `ai_usage_events`, summarized in admin.
