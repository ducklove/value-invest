"""routes.internal._require_loopback — (valid token) OR (direct loopback) matrix."""

import itertools
import unittest
from unittest.mock import AsyncMock, patch

from fastapi import HTTPException, Request

from routes import internal

SECRET = "s3cret-token"


def _request(headers: dict[str, str] | None = None, client_host: str | None = "127.0.0.1") -> Request:
    scope = {
        "type": "http",
        "method": "POST",
        "path": "/api/internal/snapshot/nav",
        "headers": [(k.lower().encode("latin-1"), v.encode("latin-1")) for k, v in (headers or {}).items()],
        "query_string": b"",
        "client": (client_host, 40000) if client_host is not None else None,
        "server": ("127.0.0.1", 3691),
        "scheme": "https",
    }
    return Request(scope)


def _allowed(headers, client_host, env) -> bool:
    with patch.dict("os.environ", env, clear=True):
        try:
            internal._require_loopback(_request(headers, client_host))
        except HTTPException as exc:
            assert exc.status_code == 403, exc.status_code
            return False
    return True


PEERS = {"loopback4": "127.0.0.1", "loopback6": "::1", "lan": "192.168.68.84", "public": "203.0.113.10"}
PROXY = {
    "none": {},
    "xff": {"X-Forwarded-For": "203.0.113.10"},
    "real_ip": {"X-Real-IP": "203.0.113.10"},
    "forwarded": {"Forwarded": "for=203.0.113.10;proto=https"},
}
TOKENS = {
    "none": {},
    "valid": {"X-Internal-Token": SECRET},
    "valid_alt_header": {"X-Value-Invest-Internal-Token": SECRET},
    "invalid": {"X-Internal-Token": "wrong"},
    "empty": {"X-Internal-Token": ""},
}
CONFIG = {"configured": {"INTERNAL_API_TOKEN": SECRET}, "unset": {}, "blank": {"INTERNAL_API_TOKEN": "   "}}


class RequireLoopbackMatrixTests(unittest.TestCase):
    def test_full_matrix(self):
        for (cfg, env), (peer, host), (proxy, proxy_headers), (tok, token_headers) in itertools.product(
            CONFIG.items(), PEERS.items(), PROXY.items(), TOKENS.items()
        ):
            token_ok = cfg == "configured" and tok in {"valid", "valid_alt_header"}
            direct_loopback = peer.startswith("loopback") and proxy == "none"
            expected = token_ok or direct_loopback
            with self.subTest(config=cfg, peer=peer, proxy=proxy, token=tok):
                self.assertEqual(_allowed({**proxy_headers, **token_headers}, host, env), expected)

    def test_token_configured_plain_loopback_timer_is_allowed(self):
        # systemd timers curl 127.0.0.1 without a token header.
        self.assertTrue(_allowed({}, "127.0.0.1", {"INTERNAL_API_TOKEN": SECRET}))

    def test_token_configured_loopback_with_xff_and_no_token_is_rejected(self):
        self.assertFalse(_allowed({"X-Forwarded-For": "203.0.113.10"}, "127.0.0.1", {"INTERNAL_API_TOKEN": SECRET}))

    def test_remote_caller_with_token_is_allowed(self):
        self.assertTrue(_allowed({"X-Internal-Token": SECRET}, "192.168.68.84", {"INTERNAL_API_TOKEN": SECRET}))

    def test_remote_caller_without_token_is_rejected_even_when_unset(self):
        self.assertFalse(_allowed({}, "192.168.68.84", {}))

    def test_empty_token_never_matches_unset_config(self):
        self.assertFalse(_allowed({"X-Internal-Token": ""}, "203.0.113.10", {"INTERNAL_API_TOKEN": ""}))

    def test_missing_client_is_rejected(self):
        self.assertFalse(_allowed({}, None, {}))

    def test_token_whitespace_is_trimmed(self):
        self.assertTrue(_allowed({"X-Internal-Token": f"  {SECRET} "}, "203.0.113.10",
                                 {"INTERNAL_API_TOKEN": f" {SECRET}\n"}))

    def test_non_ascii_token_header_is_rejected_not_crashing(self):
        request = _request(client_host="203.0.113.10")
        request.scope["headers"] = [(b"x-internal-token", "토큰".encode("utf-8"))]
        with patch.dict("os.environ", {"INTERNAL_API_TOKEN": SECRET}, clear=True):
            with self.assertRaises(HTTPException) as ctx:
                internal._require_loopback(request)
        self.assertEqual(ctx.exception.status_code, 403)

    def test_uses_constant_time_compare(self):
        with patch.object(internal.hmac, "compare_digest", wraps=internal.hmac.compare_digest) as spy, \
             patch.dict("os.environ", {"INTERNAL_API_TOKEN": SECRET}, clear=True):
            internal._require_loopback(_request({"X-Internal-Token": SECRET}, "203.0.113.10"))
        spy.assert_called_once()

    def test_rejection_log_does_not_contain_token(self):
        with patch.dict("os.environ", {"INTERNAL_API_TOKEN": SECRET}, clear=True), \
             self.assertLogs("routes.internal", level="WARNING") as logs:
            with self.assertRaises(HTTPException):
                internal._require_loopback(_request({"X-Internal-Token": "wrong-guess"}, "203.0.113.10"))
        joined = "\n".join(logs.output)
        self.assertNotIn("wrong-guess", joined)
        self.assertNotIn(SECRET, joined)


class InternalRouteAccessTests(unittest.IsolatedAsyncioTestCase):
    async def test_nav_snapshot_route_allows_timer_when_token_configured(self):
        run = AsyncMock()
        with patch.dict("os.environ", {"INTERNAL_API_TOKEN": SECRET}, clear=True), \
             patch("services.portfolio.nav_snapshot.run_all_snapshots", new=run):
            result = await internal.run_nav_snapshot(_request())
        self.assertEqual(result, {"ok": True, "kind": "nav"})
        run.assert_awaited_once_with(manage_db=False, only_missing=True)

    async def test_nav_snapshot_route_rejects_proxied_call_without_token(self):
        run = AsyncMock()
        with patch.dict("os.environ", {"INTERNAL_API_TOKEN": SECRET}, clear=True), \
             patch("services.portfolio.nav_snapshot.run_all_snapshots", new=run):
            with self.assertRaises(HTTPException) as ctx:
                await internal.run_nav_snapshot(_request({"X-Real-IP": "203.0.113.10"}))
        self.assertEqual(ctx.exception.status_code, 403)
        run.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
