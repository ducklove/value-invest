import json
import os
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_WORKSPACE_ROOT = PROJECT_ROOT.parent

DEFAULT_BASE_URLS = {
    "holdingValue": "https://ducklove.github.io/holding_value",
    "preferredSpread": "https://ducklove.github.io/common_preferred_spread",
    "goldGap": "https://ducklove.github.io/gold_gap",
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
        "goldGap": _gold_gap_config(root),
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
    codes = [item["holdingCode"] for item in items]
    meta = {
        item["holdingCode"]: {
            "totalShares": item["holdingTotalShares"],
            "treasuryShares": item["holdingTreasuryShares"],
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
