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
- Keep browser-facing integration URLs in `window.APP_CONFIG.integrations`.
  The FastAPI `/app-config.js` route mirrors the same keys for the server-hosted
  app.
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
