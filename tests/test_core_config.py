import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from core import config


class CoreConfigTests(unittest.TestCase):
    def test_loads_profile_env_and_legacy_keys_without_overriding_env(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".env").write_text("VALUE_INVEST_ENV=development\nAPP_TITLE=Base\n", encoding="utf-8")
            (root / ".env.development").write_text("APP_TITLE=Dev\nCORS_ALLOWED_ORIGINS=http://dev.local\n", encoding="utf-8")
            (root / ".kis.env").write_text("KIS_PROXY_BASE_URL=http://legacy.local\n", encoding="utf-8")
            (root / "keys.txt").write_text("OPENROUTER_API_KEY=from-file\nSESSION_SECRET=file-secret\n", encoding="utf-8")

            with patch.dict(os.environ, {"OPENROUTER_API_KEY": "from-env"}, clear=True):
                env = config.load_environment(root, force=True)
                settings = config.get_settings(force=True, project_root=root)
                self.assertEqual(os.environ["KIS_PROXY_BASE_URL"], "http://legacy.local")
                self.assertEqual(os.environ["OPENROUTER_API_KEY"], "from-env")
                self.assertEqual(os.environ["SESSION_SECRET"], "file-secret")

            self.assertEqual(env, "development")
            self.assertEqual(settings.app_title, "Dev")
            self.assertEqual(settings.cors_allowed_origins, ("http://dev.local",))

    def test_default_environment_is_production_for_compatibility(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            with patch.dict(os.environ, {}, clear=True):
                settings = config.get_settings(force=True, project_root=root)

            self.assertEqual(settings.environment, "production")
            self.assertTrue(settings.is_production)
            self.assertFalse(settings.is_development)


class LegacyConfigDeprecationTests(unittest.TestCase):
    """ST-09: legacy .kis.env / keys.txt 가 어느 키를 적용했는지 경고한다."""

    def test_kis_env_deprecation_warns_naming_contributed_keys(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".env").write_text("VALUE_INVEST_ENV=production\n", encoding="utf-8")
            (root / ".kis.env").write_text("KIS_PROXY_BASE_URL=http://legacy.local\nKIS_PROXY_TOKEN=tok\n", encoding="utf-8")

            with patch.dict(os.environ, {}, clear=True):
                with self.assertLogs("core.config", level="WARNING") as logs:
                    config.load_environment(root, force=True)
            # 적용된 키 이름이 로그에 나타난다(값은 노출되지 않음).
            joined = "\n".join(logs.output)
            self.assertIn("KIS_PROXY_BASE_URL", joined)
            self.assertIn("KIS_PROXY_TOKEN", joined)
            # 비밀값 자체는 로그에 찍히지 않는다.
            self.assertNotIn("tok", joined)
            self.assertIn("legacy config", joined)

    def test_keys_txt_deprecation_warns_naming_applied_keys(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".env").write_text("VALUE_INVEST_ENV=production\n", encoding="utf-8")
            (root / "keys.txt").write_text("OPENROUTER_API_KEY=secret-value\nSESSION_SECRET=ss\n", encoding="utf-8")

            with patch.dict(os.environ, {}, clear=True):
                with self.assertLogs("core.config", level="WARNING") as logs:
                    config.load_environment(root, force=True)
            joined = "\n".join(logs.output)
            self.assertIn("OPENROUTER_API_KEY", joined)
            self.assertIn("SESSION_SECRET", joined)
            self.assertIn("keys.txt", joined)
            # 비밀값은 로그에 찍히지 않는다.
            self.assertNotIn("secret-value", joined)

    def test_silence_flag_suppresses_deprecation_warnings(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".env").write_text("VALUE_INVEST_ENV=production\n", encoding="utf-8")
            (root / ".kis.env").write_text("KIS_PROXY_BASE_URL=http://legacy.local\n", encoding="utf-8")
            (root / "keys.txt").write_text("OPENROUTER_API_KEY=from-file\n", encoding="utf-8")

            with patch.dict(os.environ, {"SILENCE_LEGACY_CONFIG_WARNINGS": "1"}, clear=True):
                # 경고가 전혀 발생하지 않아야 한다 — assertLogs 는 최소 1건을 요구하므로
                # 로거를 직접 잡고 레코드가 비었는지 확인한다.
                import logging
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
            legacy_warnings = [r for r in records if "legacy config" in r.getMessage()]
            self.assertEqual(legacy_warnings, [])

    def test_no_warning_when_legacy_files_absent(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".env").write_text("VALUE_INVEST_ENV=production\n", encoding="utf-8")
            # .kis.env / keys.txt 모두 없음.

            with patch.dict(os.environ, {}, clear=True):
                import logging
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
            legacy_warnings = [r for r in records if "legacy config" in r.getMessage()]
            self.assertEqual(legacy_warnings, [])

