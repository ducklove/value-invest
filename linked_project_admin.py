"""Admin helpers for editing linked-project configuration files.

The linked projects remain separate deployables, but their source-of-truth
lists are small `config.json` files. This module gives the value-invest admin
surface a safe, validated way to read/write those configs without importing
the sibling apps themselves.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import integrations


PROJECT_SPECS: dict[str, dict[str, Any]] = {
    "holdingValue": {
        "label": "지주사 목록",
        "repo": "ducklove/holding_value",
        "env_dir": "HOLDING_VALUE_DIR",
        "candidates": ["hodling-value", "holding_value"],
        "config_kind": "holding",
    },
    "preferredSpread": {
        "label": "우선주 pair 목록",
        "repo": "ducklove/common_preferred_spread",
        "env_dir": "PREFERRED_SPREAD_DIR",
        "candidates": ["common_preferred_spread"],
        "config_kind": "preferred",
    },
    "goldGap": {
        "label": "금/비트코인 gap 설정",
        "repo": "ducklove/gold_gap",
        "env_dir": "GOLD_GAP_DIR",
        "candidates": ["gold_gap"],
        "config_kind": "gold",
    },
}


class LinkedProjectConfigError(ValueError):
    """Raised when a linked-project config payload is invalid."""


def project_keys() -> list[str]:
    return list(PROJECT_SPECS)


def list_project_configs(workspace_root: Path | None = None) -> list[dict[str, Any]]:
    return [get_project_config(key, workspace_root=workspace_root) for key in project_keys()]


def get_project_config(project_key: str, workspace_root: Path | None = None) -> dict[str, Any]:
    spec = _spec(project_key)
    project_dir = _project_dir(spec, workspace_root)
    config_path = project_dir / "config.json" if project_dir else None
    raw_config = _read_json(config_path)
    config_loaded = raw_config is not None
    summary = _summarize_config(spec["config_kind"], raw_config)
    return {
        "key": project_key,
        "label": spec["label"],
        "repo": spec["repo"],
        "configKind": spec["config_kind"],
        "localAvailable": bool(project_dir),
        "configLoaded": config_loaded,
        "writable": bool(config_path and (config_path.exists() or config_path.parent.exists())),
        "source": "local" if config_loaded else "missing",
        "configPath": str(config_path) if config_path else "",
        "summary": summary,
        "config": raw_config,
    }


def save_project_config(project_key: str, config: Any, workspace_root: Path | None = None) -> dict[str, Any]:
    spec = _spec(project_key)
    project_dir = _project_dir(spec, workspace_root)
    if not project_dir:
        raise LinkedProjectConfigError(f"{project_key} local project directory is not available.")
    config_path = project_dir / "config.json"
    normalized = validate_config(spec["config_kind"], config)
    _atomic_write_json(config_path, normalized)
    result = get_project_config(project_key, workspace_root=workspace_root)
    result["saved"] = True
    return result


def validate_config(config_kind: str, config: Any) -> Any:
    if config_kind == "preferred":
        return _validate_preferred_config(config)
    if config_kind == "holding":
        return _validate_holding_config(config)
    if config_kind == "gold":
        return _validate_gold_config(config)
    raise LinkedProjectConfigError(f"Unsupported config kind: {config_kind}")


def _spec(project_key: str) -> dict[str, Any]:
    try:
        return PROJECT_SPECS[project_key]
    except KeyError as exc:
        raise LinkedProjectConfigError(f"Unknown linked project: {project_key}") from exc


def _workspace_root(workspace_root: Path | None) -> Path:
    if workspace_root is not None:
        return Path(workspace_root)
    return Path(os.getenv("LINKED_PROJECTS_ROOT", str(integrations.DEFAULT_WORKSPACE_ROOT)))


def _project_dir(spec: dict[str, Any], workspace_root: Path | None) -> Path | None:
    override = os.getenv(spec["env_dir"])
    if override:
        path = Path(override)
        return path if path.exists() else None
    root = _workspace_root(workspace_root)
    for name in spec["candidates"]:
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


def _atomic_write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    tmp.replace(path)


def _summarize_config(config_kind: str, config: Any) -> dict[str, Any]:
    if config_kind in {"preferred", "holding"} and isinstance(config, list):
        return {"count": len(config)}
    if config_kind == "gold" and isinstance(config, dict):
        assets = config.get("assets") if isinstance(config.get("assets"), dict) else {}
        return {"count": len(assets), "assets": sorted(assets)}
    return {"count": 0}


def _validate_preferred_config(config: Any) -> list[dict[str, Any]]:
    if not isinstance(config, list):
        raise LinkedProjectConfigError("preferred config must be a list.")
    seen_ids: set[str] = set()
    seen_pref: set[str] = set()
    rows: list[dict[str, Any]] = []
    for idx, raw in enumerate(config, start=1):
        if not isinstance(raw, dict):
            raise LinkedProjectConfigError(f"preferred row #{idx} must be an object.")
        row = {
            "id": _required_text(raw, "id", idx),
            "name": _required_text(raw, "name", idx),
            "commonTicker": _required_text(raw, "commonTicker", idx),
            "preferredTicker": _required_text(raw, "preferredTicker", idx),
            "commonName": _required_text(raw, "commonName", idx),
            "preferredName": _required_text(raw, "preferredName", idx),
        }
        _ensure_unique(seen_ids, row["id"], f"duplicate preferred id: {row['id']}")
        _ensure_unique(
            seen_pref,
            row["preferredTicker"].upper(),
            f"duplicate preferredTicker: {row['preferredTicker']}",
        )
        rows.append(row)
    return rows


def _validate_holding_config(config: Any) -> list[dict[str, Any]]:
    if not isinstance(config, list):
        raise LinkedProjectConfigError("holding config must be a list.")
    seen_ids: set[str] = set()
    seen_holding: set[str] = set()
    rows: list[dict[str, Any]] = []
    for idx, raw in enumerate(config, start=1):
        if not isinstance(raw, dict):
            raise LinkedProjectConfigError(f"holding row #{idx} must be an object.")
        subsidiaries_raw = raw.get("subsidiaries")
        if not isinstance(subsidiaries_raw, list) or not subsidiaries_raw:
            raise LinkedProjectConfigError(f"holding row #{idx} needs at least one subsidiary.")
        subsidiaries = []
        for sub_idx, sub in enumerate(subsidiaries_raw, start=1):
            if not isinstance(sub, dict):
                raise LinkedProjectConfigError(f"holding row #{idx} subsidiary #{sub_idx} must be an object.")
            subsidiaries.append(
                {
                    "name": _required_text(sub, "name", sub_idx),
                    "ticker": _required_text(sub, "ticker", sub_idx),
                    "sharesHeld": _non_negative_number(sub.get("sharesHeld"), f"subsidiary #{sub_idx} sharesHeld"),
                }
            )
        row = {
            "id": _required_text(raw, "id", idx),
            "name": _required_text(raw, "name", idx),
            "holdingName": _required_text(raw, "holdingName", idx),
            "holdingTicker": _required_text(raw, "holdingTicker", idx),
            "holdingTotalShares": _non_negative_number(raw.get("holdingTotalShares"), "holdingTotalShares"),
            "holdingTreasuryShares": _non_negative_number(raw.get("holdingTreasuryShares"), "holdingTreasuryShares"),
            "subsidiaries": subsidiaries,
        }
        _ensure_unique(seen_ids, row["id"], f"duplicate holding id: {row['id']}")
        _ensure_unique(
            seen_holding,
            row["holdingTicker"].upper(),
            f"duplicate holdingTicker: {row['holdingTicker']}",
        )
        rows.append(row)
    return rows


def _validate_gold_config(config: Any) -> dict[str, Any]:
    if not isinstance(config, dict):
        raise LinkedProjectConfigError("gold config must be an object.")
    assets = config.get("assets")
    if not isinstance(assets, dict) or not assets:
        raise LinkedProjectConfigError("gold config needs assets object.")
    normalized = dict(config)
    normalized_assets = {}
    for key, raw in assets.items():
        if not isinstance(raw, dict):
            raise LinkedProjectConfigError(f"gold asset {key} must be an object.")
        portfolio_codes = raw.get("portfolioCodes") or []
        if not isinstance(portfolio_codes, list):
            raise LinkedProjectConfigError(f"gold asset {key} portfolioCodes must be a list.")
        asset = dict(raw)
        asset["label"] = _required_text(asset, "label", key)
        asset["portfolioCodes"] = [str(code).strip() for code in portfolio_codes if str(code).strip()]
        asset["thresholdPct"] = float(asset.get("thresholdPct", 0) or 0)
        normalized_assets[str(key)] = asset
    normalized["assets"] = normalized_assets
    return normalized


def _required_text(raw: dict[str, Any], field: str, row_label: Any) -> str:
    value = str(raw.get(field) or "").strip()
    if not value:
        raise LinkedProjectConfigError(f"row {row_label}: {field} is required.")
    return value


def _non_negative_number(value: Any, label: str) -> int | float:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise LinkedProjectConfigError(f"{label} must be a number.") from exc
    if number < 0:
        raise LinkedProjectConfigError(f"{label} must be >= 0.")
    return int(number) if number.is_integer() else number


def _ensure_unique(seen: set[str], value: str, message: str) -> None:
    key = value.upper()
    if key in seen:
        raise LinkedProjectConfigError(message)
    seen.add(key)
