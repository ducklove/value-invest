from __future__ import annotations

import os

from core import runtime


def test_asset_version_uses_commit_hash_when_static_is_clean(tmp_path, monkeypatch):
    def fake_check_output(args, **kwargs):
        if args[:3] == ["git", "rev-parse", "--short"]:
            return b"abc123\n"
        if args[:3] == ["git", "status", "--porcelain"]:
            return b""
        raise AssertionError(args)

    monkeypatch.setattr(runtime.subprocess, "check_output", fake_check_output)

    assert runtime.get_asset_version(tmp_path) == "abc123"


def test_asset_version_changes_for_dirty_static_assets(tmp_path, monkeypatch):
    static_js = tmp_path / "static" / "js"
    static_js.mkdir(parents=True)
    asset = static_js / "app.js"
    asset.write_text("console.log('one');", encoding="utf-8")

    def fake_check_output(args, **kwargs):
        if args[:3] == ["git", "rev-parse", "--short"]:
            return b"abc123\n"
        if args[:3] == ["git", "status", "--porcelain"]:
            return b" M static/js/app.js\n"
        raise AssertionError(args)

    monkeypatch.setattr(runtime.subprocess, "check_output", fake_check_output)
    os.utime(asset, ns=(1_000_000_000, 1_000_000_000))
    first = runtime.get_asset_version(tmp_path)

    os.utime(asset, ns=(2_000_000_000, 2_000_000_000))
    second = runtime.get_asset_version(tmp_path)

    assert first.startswith("abc123-")
    assert second.startswith("abc123-")
    assert first != second
