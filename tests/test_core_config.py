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
