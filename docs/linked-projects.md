# Linked Projects

`value-invest` is the portfolio and analysis hub. The related repositories stay
as independent deployables, and this app integrates them through stable public
URLs or server-side environment variables instead of copying their code.

## Project Map

| Integration key | Repository | Local directory | How value-invest uses it |
| --- | --- | --- | --- |
| `holdingValue` | `https://github.com/ducklove/holding_value` | `../hodling-value` or `../holding_value` | Loads `api/holdings.json` and links holding-company rows to the dashboard. |
| `preferredSpread` | `https://github.com/ducklove/common_preferred_spread` | `../common_preferred_spread` | Links preferred-stock rows to the spread dashboard. |
| `goldGap` | `https://github.com/ducklove/gold_gap` | `../gold_gap` | Links `KRX_GOLD` and `CRYPTO_BTC` portfolio rows to the gold/bitcoin gap dashboard. |
| `kisProxy` | `https://github.com/ducklove/kis-proxy` | `../kis-proxy` | Used server-side by `kis_proxy_client.py` through `KIS_PROXY_BASE_URL`. |

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
- `KRX_GOLD` opens `goldGap` with `?asset=gold`.
- `CRYPTO_BTC` opens `goldGap` with `?asset=bitcoin`.

## Local Config Discovery

By default `value-invest` looks one directory above the repo root for sibling
projects. Override this with `LINKED_PROJECTS_ROOT` or per-project directories:

- `HOLDING_VALUE_DIR`
- `PREFERRED_SPREAD_DIR`
- `GOLD_GAP_DIR`

Public base URLs can be overridden with:

- `HOLDING_VALUE_BASE_URL`
- `PREFERRED_SPREAD_BASE_URL`
- `GOLD_GAP_BASE_URL`
- `KIS_PROXY_BASE_URL`
- `KIS_PROXY_TOKEN` (optional, sent as `X-KIS-Proxy-Token` when the proxy is
  configured with `KIS_PROXY_PUBLIC_TOKENS`)

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
