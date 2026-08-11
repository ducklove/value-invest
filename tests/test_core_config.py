import logging
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from core import config


class CoreConfigTests(unittest.TestCase):
    def test_loads_env_without_overriding_process_environment(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".env").write_text(
                "VALUE_INVEST_ENV=development\n"
                "APP_TITLE=From dotenv\n"
                "CORS_ALLOWED_ORIGINS=http://dev.local\n"
                "OPENROUTER_API_KEY=from-file\n",
                encoding="utf-8",
            )

            with patch.dict(os.environ, {"OPENROUTER_API_KEY": "from-env"}, clear=True):
                env = config.load_environment(root, force=True)
                settings = config.get_settings(force=True, project_root=root)
                # 프로세스 환경변수가 .env 보다 우선한다.
                self.assertEqual(os.environ["OPENROUTER_API_KEY"], "from-env")

            self.assertEqual(env, "development")
            self.assertEqual(settings.app_title, "From dotenv")
            self.assertEqual(settings.cors_allowed_origins, ("http://dev.local",))
            self.assertTrue(settings.is_development)

    def test_default_environment_is_production_for_compatibility(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            with patch.dict(os.environ, {}, clear=True):
                settings = config.get_settings(force=True, project_root=root)

            self.assertEqual(settings.environment, "production")
            self.assertTrue(settings.is_production)
            self.assertFalse(settings.is_development)


class RetiredConfigFileTests(unittest.TestCase):
    """설정 단일화: 예전 파일들은 더 이상 읽지 않고, 남아 있으면 경고한다."""

    @staticmethod
    def _capture_warnings(root: Path) -> list[logging.LogRecord]:
        records: list[logging.LogRecord] = []

        class _Handler(logging.Handler):
            def emit(self, record):
                records.append(record)

        handler = _Handler(level=logging.WARNING)
        logger = logging.getLogger("core.config")
        logger.addHandler(handler)
        try:
            config.load_environment(root, force=True)
        finally:
            logger.removeHandler(handler)
        return records

    def test_retired_files_are_not_loaded(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".env").write_text("APP_TITLE=Only dotenv\n", encoding="utf-8")
            (root / ".env.production").write_text("APP_TITLE=Profile\n", encoding="utf-8")
            (root / ".kis.env").write_text("KIS_PROXY_BASE_URL=http://legacy.local\n", encoding="utf-8")
            (root / "keys.txt").write_text("SESSION_SECRET=file-secret\n", encoding="utf-8")

            with patch.dict(os.environ, {}, clear=True):
                config.load_environment(root, force=True)
                settings = config.get_settings(force=True, project_root=root)
                self.assertNotIn("KIS_PROXY_BASE_URL", os.environ)
                self.assertNotIn("SESSION_SECRET", os.environ)

            self.assertEqual(settings.app_title, "Only dotenv")

    def test_leftover_retired_files_are_named_in_a_warning(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".env").write_text("APP_TITLE=X\n", encoding="utf-8")
            (root / ".kis.env").write_text("KIS_PROXY_TOKEN=tok\n", encoding="utf-8")
            (root / "keys.txt").write_text("SESSION_SECRET=ss\n", encoding="utf-8")

            with patch.dict(os.environ, {}, clear=True):
                records = self._capture_warnings(root)

            joined = "\n".join(r.getMessage() for r in records)
            self.assertIn(".kis.env", joined)
            self.assertIn("keys.txt", joined)
            # 값은 절대 로그에 찍히지 않는다.
            self.assertNotIn("tok", joined)
            self.assertNotIn("ss", joined)

    def test_no_warning_when_only_dotenv_is_present(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".env").write_text("APP_TITLE=X\n", encoding="utf-8")

            with patch.dict(os.environ, {}, clear=True):
                records = self._capture_warnings(root)

            self.assertEqual([r for r in records if "retired config" in r.getMessage()], [])
