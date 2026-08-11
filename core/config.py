from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[1]

DEFAULT_CORS_ORIGINS = (
    "http://localhost",
    "http://127.0.0.1",
    "http://localhost:8000",
    "http://127.0.0.1:8000",
    "http://localhost:8010",
    "http://127.0.0.1:8010",
    # 로컬 개발 프리뷰(.claude/launch.json)의 uvicorn 포트 — 없으면 /ws/quotes
    # 핸드셰이크가 오리진 검사에서 1008로 거부된다.
    "http://localhost:8021",
    "http://127.0.0.1:8021",
    "https://ducklove.github.io",
    "https://cantabile.tplinkdns.com:3691",
)

# 단일화 이전에 쓰이던 설정 파일들. 더 이상 읽지 않으며, 남아 있으면 값이
# 조용히 무시되는 상태이므로 시작 시 한 번 경고한다.
RETIRED_CONFIG_FILES = (".env.development", ".env.production", ".env.local", ".kis.env", "keys.txt")

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


def _current_env() -> str:
    return _normalize_env(os.getenv("VALUE_INVEST_ENV") or os.getenv("APP_ENV") or os.getenv("ENVIRONMENT"))


def _warn_retired_config_files(root: Path) -> None:
    """Name any leftover pre-consolidation config file — they are not read."""
    leftovers = [name for name in RETIRED_CONFIG_FILES if (root / name).exists()]
    if leftovers:
        logger.warning(
            "retired config file(s) present but ignored: %s — 값을 .env 로 옮기고 파일은 삭제하세요.",
            ", ".join(leftovers),
        )


def load_environment(project_root: Path | None = None, *, force: bool = False) -> str:
    """Load `.env` before modules freeze import-time settings.

    설정과 시크릿의 단일 소스는 프로젝트 루트의 `.env` 하나다. 이미 프로세스
    환경변수로 들어와 있는 값(systemd `Environment=`, 쉘 export, 테스트의
    `patch.dict`)이 우선하고, `.env`는 비어 있는 키만 채운다.

    프로필은 파일이 아니라 `.env` 안의 `VALUE_INVEST_ENV` 값으로 구분한다.
    기본값은 `production`이라 env를 명시하지 않은 배포는 동작이 바뀌지 않는다.
    """
    global _LOADED_ROOT, _SETTINGS

    root = Path(project_root or PROJECT_ROOT)
    if _LOADED_ROOT == root and not force:
        return _current_env()

    load_dotenv(root / ".env", override=False)
    os.environ.setdefault("VALUE_INVEST_ENV", _current_env())
    _warn_retired_config_files(root)

    _LOADED_ROOT = root
    _SETTINGS = None
    return _current_env()


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
