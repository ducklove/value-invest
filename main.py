"""ASGI entrypoint.

The application is assembled in `core.app_factory` so tests, local dev, and
production can create the same app with explicit settings.
"""

import logging

from core.app_factory import create_app
from core.config import get_settings, load_environment


logging.basicConfig(level=logging.INFO)

load_environment()
SETTINGS = get_settings()
app = create_app(SETTINGS)
ASSET_VERSION = app.state.asset_version


# Compatibility helpers for older tests/imports. Runtime routes are registered
# by `create_app`; new code should exercise the app instead of importing these.
async def spa_pages():
    return await app.state.static_handlers["spa_pages"]()
