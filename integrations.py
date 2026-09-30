import copy
import json
import os
import threading
from pathlib import Path
from typing import Any

from core import ecosystem

PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_WORKSPACE_ROOT = PROJECT_ROOT.parent

# 연결 도구 기본 주소는 config/ecosystem.json(생태계 레지스트리)에서 파생한다.
# integrationKey 가 있는 항목만 — kisProxy 처럼 visibility "internal" 인 서버 전용
# 항목도 여기엔 포함되지만, 브라우저용 APP_CONFIG.ecosystem 투영에는 빠진다.
# 각 항목의 envOverride(HOLDING_VALUE_BASE_URL 등)는 _base_url 이 그대로 존중한다.
DEFAULT_BASE_URLS = ecosystem.default_base_urls()

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
        "ecosystem": ecosystem.public_projection(),
    }


def handoff_integration_keys() -> frozenset[str]:
    """보유 스냅샷(#vc-held) handoff 를 받는 연결 도구 키 — 레지스트리 ``handoff: true``."""
    return ecosystem.handoff_keys()


# 로컬 형제 파일 파싱 결과 메모: 경로 → ((mtime_ns, 크기), 파싱값). 요청마다 SD 카드에서
# 수십 KB 를 다시 읽고 파싱하지 않는다 — stat 한 번으로 바뀌었는지만 본다. 메모한 값은
# 공유 객체이므로 빌더는 절대 변경하지 않고 새 dict 를 만든다(_merge_gold_gap_assets 참고).
_file_memo_lock = threading.Lock()
_file_memo: dict[tuple[str, str], tuple[tuple[int, int], Any]] = {}


def build_public_integrations(workspace_root: Path | None = None) -> dict[str, Any]:
    """브라우저(/app-config.js, /api/integrations)로 나가는 연결 도구 설정.

    kisProxy 같은 서버 전용 항목은 여기 없다(:func:`build_server_integrations`).
    """
    root = _workspace_root(workspace_root)
    result = _build_public_integrations(root)
    _apply_gold_gap_latest(result["goldGap"])
    return result


def build_server_integrations() -> dict[str, Any]:
    """서버 전용 연결 설정(브라우저로 내보내지 않는다) — 지금은 KIS 프록시뿐."""
    return {"kisProxy": _kis_proxy_config()}


def clear_file_memo() -> None:
    """테스트용 — 다음 호출이 파일을 다시 읽는다."""
    with _file_memo_lock:
        _file_memo.clear()


def _memoized_file(path: Path | None, kind: str, parse) -> Any:
    """``parse(text)`` 결과를 (경로, mtime_ns, 크기)로 메모한다. 파일이 없으면 None."""
    if not path:
        return None
    try:
        stat = path.stat()
    except OSError:
        return None
    key = (str(path), kind)
    signature = (stat.st_mtime_ns, stat.st_size)
    with _file_memo_lock:
        hit = _file_memo.get(key)
    if hit is not None and hit[0] == signature:
        return hit[1]
    try:
        value = parse(path.read_text(encoding="utf-8"))
    except OSError:
        return None
    with _file_memo_lock:
        _file_memo[key] = (signature, value)
    return value


def _build_public_integrations(root: Path) -> dict[str, Any]:
    return {
        "holdingValue": _holding_value_config(root),
        "preferredSpread": _preferred_spread_config(root),
        "spacHunter": _spac_hunter_config(),
        "buybacks": _buybacks_config(),
        "eiayn": {"baseUrl": _base_url("eiayn", "EIAYN_BASE_URL")},
        "goldGap": _gold_gap_config(root),
        "allAboutGold": _all_about_gold_config(),
        "npsTracker": _nps_tracker_config(),
        "bondMate": _bond_mate_config(),
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
    def parse(text: str) -> Any:
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return None

    return _memoized_file(path, "json", parse)


def _read_js_object(path: Path | None, const_name: str) -> Any:
    def parse(text: str) -> Any:
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
        try:
            return json.loads(text[start : end + 1])
        except json.JSONDecodeError:
            return None

    return _memoized_file(path, f"js:{const_name}", parse)


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
    # hodling-value 는 현재 스냅샷을 current.json 으로 낸다(구버전은 current.js
    # 의 CURRENT_DATA). json 을 먼저 보고 없으면 js 로 폴백 — 둘 다 없으면 빈
    # 스냅샷이라 보유지분 목표가는 자회사 라이브 시세로만 계산된다.
    data = _read_json(project_dir / "current.json" if project_dir else None)
    if not isinstance(data, dict):
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


def _all_about_gold_config() -> dict[str, Any]:
    base_url = _base_url("allAboutGold", "ALL_ABOUT_GOLD_BASE_URL")
    return {"baseUrl": base_url, "dataUrl": f"{base_url}/data/current.json"}


def _buybacks_config() -> dict[str, Any]:
    # buybacks 는 자사주 매입·처분·소각 분석용 정적 SPA다. 분석 도구 요약은
    # external_tools 가 published JSON 을 읽고, 브라우저에는 baseUrl 만 노출한다.
    return {"baseUrl": _base_url("buybacks", "BUYBACKS_BASE_URL")}


def _nps_tracker_config() -> dict[str, Any]:
    # nps-tracker 는 국민연금 국내주식 포트폴리오 대시보드(별도 정적 SPA).
    # 허브 NPS 탭은 이를 iframe 으로 임베드하고, 인사이트 요약은 external_tools
    # 가 current.json 을 직접 읽는다. 여기선 임베드용 baseUrl 만 노출한다.
    return {"baseUrl": _base_url("npsTracker", "NPS_TRACKER_BASE_URL")}


def _bond_mate_config() -> dict[str, Any]:
    # bond-mate 는 전 세계 금리·환율·국채·회사채 대시보드(별도 정적 SPA)이고,
    # 투자정보 탭의 국채·환율 패널이 쓰는 데이터의 source of record 다.
    # 브라우저는 published JSON 을 직접 읽고(dataUrl) 필요하면 화면을 통째로
    # iframe 임베드한다(?embed=<탭>) — 로컬 config 는 필요 없다.
    base_url = _base_url("bondMate", "BOND_MATE_BASE_URL")
    return {
        "baseUrl": base_url,
        "dataUrl": f"{base_url}/data/current.json",
        "embedUrl": f"{base_url}/?embed=",
        # 임베드·딥링크에 쓰는 화면 키. bond-mate 쪽 계약이라 임의로 바꾸지 않는다.
        "views": ["overview", "government", "policy", "fx", "credit", "issuance"],
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


def _apply_gold_gap_latest(config: dict[str, Any]) -> None:
    """최신 갭은 external_tools 의 gold_gap 캐시(Pages data.json/summary.json)가 있으면 그걸 쓴다.

    로컬 ``gold_gap/data.json`` 은 orphan ``data`` 브랜치에만 있어 오래되기 쉽다 — 캐시가
    비어 있으면(아직 한 번도 안 받았으면) 로컬 파일 값을 그대로 둔다. 네트워크는 타지 않는다.
    """
    import external_tools

    latest = external_tools.peek_gold_latest()
    if not isinstance(latest, dict):
        return
    applied = False
    for asset_key, asset_config in (config.get("assets") or {}).items():
        asset_data = latest.get(asset_key)
        if not isinstance(asset_data, dict):
            continue
        latest_gap = _last_number(asset_data.get("gap_pct"))
        if latest_gap is None:
            continue
        asset_config["latestGapPct"] = latest_gap
        latest_date = _last_value(asset_data.get("dates"))
        if latest_date:
            asset_config["latestDate"] = latest_date
        applied = True
    if applied and latest.get("updated_at"):
        config["updatedAt"] = latest["updated_at"]


def _merge_gold_gap_assets(raw_config: Any) -> dict[str, dict[str, Any]]:
    # 기본값·메모된 파일 값은 공유 객체다 — 깊은 복사로 새 dict 를 만든다.
    assets = copy.deepcopy(DEFAULT_GOLD_GAP_ASSETS)
    if isinstance(raw_config, dict) and isinstance(raw_config.get("assets"), dict):
        for key, value in raw_config["assets"].items():
            if isinstance(value, dict):
                assets.setdefault(key, {}).update(copy.deepcopy(value))
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
