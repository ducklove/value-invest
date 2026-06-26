import json
import os
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_WORKSPACE_ROOT = PROJECT_ROOT.parent

DEFAULT_BASE_URLS = {
    "holdingValue": "https://ducklove.github.io/holding_value",
    "preferredSpread": "https://ducklove.github.io/common_preferred_spread",
    "spacHunter": "https://ducklove.github.io/spac-hunter",
    "buybacks": "https://ducklove.github.io/buybacks",
    "goldGap": "https://ducklove.github.io/gold_gap",
    "npsTracker": "https://ducklove.github.io/nps-tracker",
    "kisProxy": "http://cantabile.tplinkdns.com:3288",
}

DEFAULT_GOLD_GAP_ASSETS = {
    "gold": {
        "label": "Gold",
        "portfolioCodes": ["KRX_GOLD"],
        "thresholdPct": 5.0,
    },
    "bitcoin": {
        "label": "Bitcoin",
        "portfolioCodes": ["CRYPTO_BTC"],
        "thresholdPct": 5.0,
    },
    "usdt": {
        "label": "USDT",
        "portfolioCodes": [],
        "thresholdPct": 3.0,
    },
}


def build_app_config(api_base_url: str = "", workspace_root: Path | None = None) -> dict[str, Any]:
    return {
        "apiBaseUrl": api_base_url,
        "integrations": build_public_integrations(workspace_root=workspace_root),
    }


def build_public_integrations(workspace_root: Path | None = None) -> dict[str, Any]:
    root = _workspace_root(workspace_root)
    return {
        "holdingValue": _holding_value_config(root),
        "preferredSpread": _preferred_spread_config(root),
        "spacHunter": _spac_hunter_config(),
        "buybacks": _buybacks_config(),
        "goldGap": _gold_gap_config(root),
        "npsTracker": _nps_tracker_config(),
        "kisProxy": _kis_proxy_config(),
    }


def _workspace_root(workspace_root: Path | None) -> Path:
    if workspace_root is not None:
        return Path(workspace_root)
    return Path(os.getenv("LINKED_PROJECTS_ROOT", str(DEFAULT_WORKSPACE_ROOT)))


def _base_url(key: str, env_name: str) -> str:
    return os.getenv(env_name, DEFAULT_BASE_URLS[key]).rstrip("/")


def _project_dir(root: Path, env_name: str, candidates: list[str]) -> Path | None:
    override = os.getenv(env_name)
    if override:
        path = Path(override)
        return path if path.exists() else None
    for name in candidates:
        path = root / name
        if path.exists():
            return path
    return None


def _read_json(path: Path | None) -> Any:
    if not path or not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _read_js_object(path: Path | None, const_name: str) -> Any:
    if not path or not path.exists():
        return None
    try:
        text = path.read_text(encoding="utf-8")
        marker_idx = text.find(f"const {const_name}")
        if marker_idx < 0:
            return None
        start = text.find("{", marker_idx)
        end = text.rfind("};")
        if start < 0:
            return None
        if end < start:
            end = text.rfind("}")
        if end < start:
            return None
        return json.loads(text[start : end + 1])
    except (OSError, json.JSONDecodeError):
        return None


def _ticker_code(ticker: Any) -> str:
    return str(ticker or "").split(".", 1)[0].strip().upper()


def _public_status(project_dir: Path | None, config_loaded: bool) -> dict[str, Any]:
    if config_loaded:
        source = "local"
    elif project_dir:
        source = "local-unreadable"
    else:
        source = "remote-fallback"
    return {"source": source, "available": bool(config_loaded)}


def _holding_value_config(root: Path) -> dict[str, Any]:
    base_url = _base_url("holdingValue", "HOLDING_VALUE_BASE_URL")
    project_dir = _project_dir(root, "HOLDING_VALUE_DIR", ["hodling-value", "holding_value"])
    raw_entries = _read_json(project_dir / "config.json" if project_dir else None)
    entries = raw_entries if isinstance(raw_entries, list) else []
    items = [_build_holding_item(entry) for entry in entries if isinstance(entry, dict)]
    items = [item for item in items if item]
    current = _holding_value_current(project_dir)
    for item in items:
        snapshot = _build_holding_current_snapshot(item, current)
        if snapshot:
            item["current"] = snapshot
    codes = [item["holdingCode"] for item in items]
    meta = {
        item["holdingCode"]: {
            "totalShares": item["holdingTotalShares"],
            "treasuryShares": item["holdingTreasuryShares"],
            "holdingValuePerShare": (item.get("current") or {}).get("holdingValuePerShare"),
            "holdingValueUpdatedAt": (item.get("current") or {}).get("updatedAt"),
            "subsidiaries": [
                {"code": sub["code"], "sharesHeld": sub["sharesHeld"]}
                for sub in item["subsidiaries"]
            ],
        }
        for item in items
    }
    return {
        "baseUrl": base_url,
        "configUrl": f"{base_url}/config.json",
        "holdingsUrl": f"{base_url}/api/holdings.json",
        "settings": _public_status(project_dir, bool(items)),
        "count": len(items),
        "codes": codes,
        "meta": meta,
        "items": items,
    }


def _holding_value_current(project_dir: Path | None) -> dict[str, Any]:
    data = _read_js_object(project_dir / "current.js" if project_dir else None, "CURRENT_DATA")
    if not isinstance(data, dict):
        return {"updatedAt": None, "pairs": {}}
    pairs = data.get("pairs") if isinstance(data.get("pairs"), list) else []
    return {
        "updatedAt": data.get("lastUpdated") or data.get("generatedAt"),
        "pairs": {
            str(pair.get("id") or ""): pair
            for pair in pairs
            if isinstance(pair, dict) and pair.get("id")
        },
    }


def _as_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number == number else None


def _build_holding_current_snapshot(item: dict[str, Any], current: dict[str, Any]) -> dict[str, Any] | None:
    pair = (current.get("pairs") or {}).get(item.get("id"))
    if not isinstance(pair, dict):
        return None
    holding_value = _as_float(pair.get("holdingValue"))
    total_shares = _as_float(item.get("holdingTotalShares")) or 0
    treasury_shares = _as_float(item.get("holdingTreasuryShares")) or 0
    adjusted_shares = total_shares - treasury_shares
    if holding_value is None or holding_value <= 0 or adjusted_shares <= 0:
        return None
    return {
        "updatedAt": current.get("updatedAt"),
        "holdingValue": holding_value,
        "holdingValueUnit": "억원",
        "holdingValuePerShare": round(holding_value * 100_000_000 / adjusted_shares, 4),
        "quoteSource": pair.get("quoteSource"),
    }


def _build_holding_item(entry: dict[str, Any]) -> dict[str, Any] | None:
    holding_ticker = entry.get("holdingTicker")
    holding_code = _ticker_code(holding_ticker)
    if not holding_code:
        return None
    subsidiaries = []
    for sub in entry.get("subsidiaries") or []:
        if not isinstance(sub, dict):
            continue
        sub_code = _ticker_code(sub.get("ticker"))
        if not sub_code:
            continue
        subsidiaries.append(
            {
                "name": sub.get("name") or sub_code,
                "ticker": sub.get("ticker") or sub_code,
                "code": sub_code,
                "sharesHeld": sub.get("sharesHeld") or 0,
            }
        )
    return {
        "id": entry.get("id") or holding_code,
        "name": entry.get("name") or entry.get("holdingName") or holding_code,
        "holdingName": entry.get("holdingName") or holding_code,
        "holdingTicker": holding_ticker,
        "holdingCode": holding_code,
        "holdingTotalShares": entry.get("holdingTotalShares") or 0,
        "holdingTreasuryShares": entry.get("holdingTreasuryShares") or 0,
        "subsidiaryCount": len(subsidiaries),
        "subsidiaries": subsidiaries,
    }


def _preferred_spread_config(root: Path) -> dict[str, Any]:
    base_url = _base_url("preferredSpread", "PREFERRED_SPREAD_BASE_URL")
    project_dir = _project_dir(root, "PREFERRED_SPREAD_DIR", ["common_preferred_spread"])
    raw_entries = _read_json(project_dir / "config.json" if project_dir else None)
    entries = raw_entries if isinstance(raw_entries, list) else []
    pairs = [_build_preferred_pair(entry) for entry in entries if isinstance(entry, dict)]
    pairs = [pair for pair in pairs if pair]
    by_preferred_code = {pair["preferredCode"]: pair for pair in pairs}
    return {
        "baseUrl": base_url,
        "configUrl": f"{base_url}/config.json",
        "dataUrl": f"{base_url}/data.js",
        "currentUrl": f"{base_url}/current.json",
        "settings": _public_status(project_dir, bool(pairs)),
        "count": len(pairs),
        "pairs": pairs,
        "pairsByPreferredCode": by_preferred_code,
    }


def _build_preferred_pair(entry: dict[str, Any]) -> dict[str, Any] | None:
    common_code = _ticker_code(entry.get("commonTicker"))
    preferred_code = _ticker_code(entry.get("preferredTicker"))
    if not common_code or not preferred_code:
        return None
    return {
        "id": entry.get("id") or preferred_code,
        "name": entry.get("name") or preferred_code,
        "commonTicker": entry.get("commonTicker") or common_code,
        "preferredTicker": entry.get("preferredTicker") or preferred_code,
        "commonCode": common_code,
        "preferredCode": preferred_code,
        "commonName": entry.get("commonName") or common_code,
        "preferredName": entry.get("preferredName") or preferred_code,
    }


def _spac_hunter_config() -> dict[str, Any]:
    # spac-hunter 는 별도 서브 프로젝트(SPA)로, 종목 코드를 ?code= 쿼리로만
    # 받는다. 로컬 config 를 읽을 필요가 없어 baseUrl 만 노출한다.
    return {"baseUrl": _base_url("spacHunter", "SPAC_HUNTER_BASE_URL")}


def _buybacks_config() -> dict[str, Any]:
    # buybacks 는 자사주 매입·처분·소각 분석용 정적 SPA다. 분석 도구 요약은
    # external_tools 가 published JSON 을 읽고, 브라우저에는 baseUrl 만 노출한다.
    return {"baseUrl": _base_url("buybacks", "BUYBACKS_BASE_URL")}


def _nps_tracker_config() -> dict[str, Any]:
    # nps-tracker 는 국민연금 국내주식 포트폴리오 대시보드(별도 정적 SPA).
    # 허브 NPS 탭은 이를 iframe 으로 임베드하고, 인사이트 요약은 external_tools
    # 가 current.json 을 직접 읽는다. 여기선 임베드용 baseUrl 만 노출한다.
    return {"baseUrl": _base_url("npsTracker", "NPS_TRACKER_BASE_URL")}


def _gold_gap_config(root: Path) -> dict[str, Any]:
    base_url = _base_url("goldGap", "GOLD_GAP_BASE_URL")
    project_dir = _project_dir(root, "GOLD_GAP_DIR", ["gold_gap"])
    raw_config = _read_json(project_dir / "config.json" if project_dir else None)
    raw_data = _read_json(project_dir / "data.json" if project_dir else None)

    assets = _merge_gold_gap_assets(raw_config)
    if isinstance(raw_data, dict):
        for asset_key, asset_config in assets.items():
            asset_data = raw_data.get(asset_key)
            if isinstance(asset_data, dict):
                latest_gap = _last_number(asset_data.get("gap_pct"))
                if latest_gap is not None:
                    asset_config["latestGapPct"] = latest_gap
                latest_date = _last_value(asset_data.get("dates"))
                if latest_date:
                    asset_config["latestDate"] = latest_date

    asset_by_portfolio_code: dict[str, str] = {}
    for asset_key, asset_config in assets.items():
        for code in asset_config.get("portfolioCodes") or []:
            asset_by_portfolio_code[str(code)] = asset_key

    return {
        "baseUrl": base_url,
        "configUrl": f"{base_url}/config.json",
        "dataUrl": f"{base_url}/data.json",
        "settings": _public_status(project_dir, bool(raw_config or raw_data)),
        "updatedAt": raw_data.get("updated_at") if isinstance(raw_data, dict) else None,
        "assets": assets,
        "assetByPortfolioCode": asset_by_portfolio_code,
    }


def _merge_gold_gap_assets(raw_config: Any) -> dict[str, dict[str, Any]]:
    assets = {key: dict(value) for key, value in DEFAULT_GOLD_GAP_ASSETS.items()}
    if isinstance(raw_config, dict) and isinstance(raw_config.get("assets"), dict):
        for key, value in raw_config["assets"].items():
            if isinstance(value, dict):
                assets.setdefault(key, {}).update(value)
    return assets


def _last_number(values: Any) -> float | None:
    value = _last_value(values)
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _last_value(values: Any) -> Any:
    if isinstance(values, list) and values:
        return values[-1]
    return None


def _kis_proxy_config() -> dict[str, Any]:
    return {
        "baseUrl": _base_url("kisProxy", "KIS_PROXY_BASE_URL"),
        "role": "server-side",
        "settings": {"source": "environment", "available": True},
    }
