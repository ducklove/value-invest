from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[1]

DEFAULT_CORS_ORIGINS = (
    "http://localhost",
    "http://127.0.0.1",
    "http://localhost:8000",
    "http://127.0.0.1:8000",
    "http://localhost:8010",
    "http://127.0.0.1:8010",
    "https://ducklove.github.io",
    "https://cantabile.tplinkdns.com:3691",
)

_LOADED_ROOT: Path | None = None
_SETTINGS: "AppSettings | None" = None


def _normalize_env(value: str | None) -> str:
    env = (value or "production").strip().lower()
    aliases = {
        "dev": "development",
        "local": "development",
        "prod": "production",
        "stage": "staging",
    }
    return aliases.get(env, env or "production")


def _split_csv(value: str | None) -> list[str]:
    if not value:
        return []
    return [part.strip() for part in value.split(",") if part.strip()]


def _bool_env(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() not in {"0", "false", "no", "off", ""}


def _load_keys_file(path: Path, *, override: bool = False) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if key and (override or key not in os.environ):
            os.environ[key] = value


def load_environment(project_root: Path | None = None, *, force: bool = False) -> str:
    """Load env files before modules freeze import-time settings.

    Order:
    1. `.env` for shared local defaults.
    2. `.env.<environment>` for profile-specific values.
    3. `.kis.env` as the legacy production file, with override preserved.
    4. `keys.txt` as a legacy secret fallback that never overrides env vars.

    The default profile is production so current deployments keep their
    behavior unless `VALUE_INVEST_ENV` / `APP_ENV` is set explicitly.
    """
    global _LOADED_ROOT, _SETTINGS

    root = Path(project_root or PROJECT_ROOT)
    if _LOADED_ROOT == root and not force:
        return _normalize_env(os.getenv("VALUE_INVEST_ENV") or os.getenv("APP_ENV") or os.getenv("ENVIRONMENT"))

    load_dotenv(root / ".env", override=False)
    env = _normalize_env(os.getenv("VALUE_INVEST_ENV") or os.getenv("APP_ENV") or os.getenv("ENVIRONMENT"))
    os.environ.setdefault("VALUE_INVEST_ENV", env)

    env_file = root / f".env.{env}"
    if env_file.exists():
        load_dotenv(env_file, override=True)

    legacy_env = root / ".kis.env"
    if legacy_env.exists():
        load_dotenv(legacy_env, override=True)

    _load_keys_file(root / "keys.txt", override=False)
    _LOADED_ROOT = root
    _SETTINGS = None
    return _normalize_env(os.getenv("VALUE_INVEST_ENV") or os.getenv("APP_ENV") or os.getenv("ENVIRONMENT"))


@dataclass(frozen=True)
class AppSettings:
    environment: str
    project_root: Path
    app_title: str
    public_api_base_url: str
    cors_allowed_origins: tuple[str, ...]
    enable_docs: bool

    @property
    def is_development(self) -> bool:
        return self.environment == "development"

    @property
    def is_production(self) -> bool:
        return self.environment == "production"


def get_settings(*, force: bool = False, project_root: Path | None = None) -> AppSettings:
    global _SETTINGS

    if _SETTINGS is not None and not force and project_root is None:
        return _SETTINGS

    root = Path(project_root or PROJECT_ROOT)
    env = load_environment(root, force=force)
    cors = tuple(_split_csv(os.getenv("CORS_ALLOWED_ORIGINS"))) or DEFAULT_CORS_ORIGINS
    settings = AppSettings(
        environment=env,
        project_root=root,
        app_title=os.getenv("APP_TITLE", "Value Compass"),
        public_api_base_url=os.getenv("PUBLIC_API_BASE_URL", "").rstrip("/"),
        cors_allowed_origins=tuple(cors),
        enable_docs=_bool_env("ENABLE_API_DOCS", default=env != "production"),
    )
    if project_root is None:
        _SETTINGS = settings
    return settings
