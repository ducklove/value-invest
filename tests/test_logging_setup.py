"""core.logging_setup — secret redaction on real LogRecords + quiet httpx."""

import io
import logging
import unittest

import httpx

from core import logging_setup
from core.logging_setup import (
    REDACTED,
    SecretRedactingFilter,
    install_secret_redaction,
    quiet_http_client_loggers,
    redact_secrets,
)

DART_KEY = "abcdef0123456789abcdef0123456789abcdef01"
ECOS_KEY = "ECOSKEY1234567890"
TG_TOKEN = "123456789:AAH-secret_Token-xyz"


def _record(msg, args=(), *, name="httpx", exc_info=None, level=logging.INFO):
    return logging.LogRecord(name, level, __file__, 1, msg, args, exc_info)


def _formatted(record: logging.LogRecord) -> str:
    SecretRedactingFilter().filter(record)
    return logging.Formatter("%(message)s").format(record)


class RedactSecretsTextTests(unittest.TestCase):
    def test_masks_each_secret_query_param(self):
        for name in logging_setup.SECRET_PARAM_NAMES:
            with self.subTest(name=name):
                url = f"https://api.example.com/x?a=1&{name}=S3CR3T&b=2"
                out = redact_secrets(url)
                self.assertNotIn("S3CR3T", out)
                self.assertIn(f"{name}={REDACTED}", out)
                self.assertIn("a=1", out)
                self.assertIn("b=2", out)

    def test_param_names_are_case_insensitive(self):
        out = redact_secrets("https://apis.data.go.kr/x?SERVICEKEY=abc%2Bdef&numOfRows=10")
        self.assertNotIn("abc%2Bdef", out)
        self.assertIn("numOfRows=10", out)

    def test_masks_first_param_after_question_mark(self):
        out = redact_secrets(f"https://opendart.fss.or.kr/api/list.json?crtfc_key={DART_KEY}&corp_code=00126380")
        self.assertNotIn(DART_KEY, out)
        self.assertIn("corp_code=00126380", out)

    def test_does_not_touch_similar_param_names(self):
        url = "https://x.test/?monkey=1&keyword=samsung&tokens_used=3"
        self.assertEqual(redact_secrets(url), url)

    def test_leaves_free_text_key_value_alone(self):
        text = "cache miss key=005930 ttl=60"
        self.assertEqual(redact_secrets(text), text)

    def test_masks_telegram_bot_token_path(self):
        url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
        out = redact_secrets(url)
        self.assertNotIn(TG_TOKEN, out)
        self.assertNotIn("AAH-secret", out)
        self.assertEqual(out, f"https://api.telegram.org/bot{REDACTED}/sendMessage")
        file_url = f"https://api.telegram.org/file/bot{TG_TOKEN}/photos/a.jpg"
        self.assertNotIn(TG_TOKEN, redact_secrets(file_url))

    def test_masks_ecos_key_path_segment(self):
        url = f"https://ecos.bok.or.kr/api/StatisticSearch/{ECOS_KEY}/json/kr/1/700/817Y002/D/20260101/20260930"
        out = redact_secrets(url)
        self.assertNotIn(ECOS_KEY, out)
        self.assertIn(f"/api/StatisticSearch/{REDACTED}/json/kr/1/700/817Y002", out)

    def test_plain_text_unchanged(self):
        self.assertEqual(redact_secrets("NAV snapshot done for 3 users"), "NAV snapshot done for 3 users")
        self.assertEqual(redact_secrets(""), "")


class SecretRedactingFilterRecordTests(unittest.TestCase):
    def test_httpx_request_line_with_url_object_arg(self):
        url = httpx.URL(f"https://opendart.fss.or.kr/api/list.json?crtfc_key={DART_KEY}&page_no=1")
        record = _record('HTTP Request: %s %s "%s %d %s"', ("GET", url, "HTTP/1.1", 200, "OK"))
        text = _formatted(record)
        self.assertNotIn(DART_KEY, text)
        self.assertIn(f"crtfc_key={REDACTED}", text)
        self.assertIn('"HTTP/1.1 200 OK"', text)

    def test_secret_in_msg_itself(self):
        record = _record(f"GET https://api.telegram.org/bot{TG_TOKEN}/getUpdates failed")
        self.assertNotIn(TG_TOKEN, _formatted(record))

    def test_dict_args(self):
        record = _record("fetch %(url)s", None)
        record.args = {"url": "https://api.stlouisfed.org/fred/series?api_key=FREDKEY&series_id=DGS10"}
        text = _formatted(record)
        self.assertNotIn("FREDKEY", text)
        self.assertIn("series_id=DGS10", text)

    def test_numeric_args_keep_their_type(self):
        record = _record("%d rows in %.1fs", (3, 1.25))
        self.assertEqual(_formatted(record), "3 rows in 1.2s")

    def test_exception_object_arg_is_redacted(self):
        request = httpx.Request("GET", f"https://opendart.fss.or.kr/api/document.xml?crtfc_key={DART_KEY}&rcept_no=1")
        response = httpx.Response(403, request=request)
        exc = httpx.HTTPStatusError("Client error '403 Forbidden' for url '%s'" % request.url,
                                    request=request, response=response)
        record = _record("comparison DART report skipped %s: %s", ("2026", exc), name="dart_report_review")
        text = _formatted(record)
        self.assertNotIn(DART_KEY, text)
        self.assertIn("rcept_no=1", text)

    def test_exception_traceback_is_redacted(self):
        try:
            raise RuntimeError(f"boom https://x.test/a?token={DART_KEY}")
        except RuntimeError:
            import sys
            record = _record("job failed", exc_info=sys.exc_info(), level=logging.ERROR)
        text = _formatted(record)
        self.assertIn("RuntimeError", text)
        self.assertNotIn(DART_KEY, text)

    def test_filter_never_drops_records(self):
        self.assertTrue(SecretRedactingFilter().filter(_record("hello")))


class InstallTests(unittest.TestCase):
    def setUp(self):
        self.root = logging.getLogger()
        self.stream = io.StringIO()
        self.handler = logging.StreamHandler(self.stream)
        self.handler.setFormatter(logging.Formatter("%(name)s %(message)s"))
        self.root.addHandler(self.handler)
        self.saved_levels = {n: logging.getLogger(n).level for n in ("httpx", "httpcore")}
        self.saved_filters = {n: list(logging.getLogger(n).filters) for n in ("httpx", "httpcore")}

    def tearDown(self):
        self.root.removeHandler(self.handler)
        for name, level in self.saved_levels.items():
            logger = logging.getLogger(name)
            logger.setLevel(level)
            logger.filters[:] = self.saved_filters[name]

    def test_handler_filter_redacts_records_from_any_logger(self):
        install_secret_redaction()
        logger = logging.getLogger("tests.logging_setup.child")
        logger.setLevel(logging.INFO)
        logger.info("url %s", "https://apis.data.go.kr/x?serviceKey=DATAGOKEY&pageNo=1")
        output = self.stream.getvalue()
        self.assertIn("pageNo=1", output)
        self.assertNotIn("DATAGOKEY", output)

    def test_install_is_idempotent(self):
        install_secret_redaction()
        install_secret_redaction()
        count = sum(isinstance(f, SecretRedactingFilter) for f in self.handler.filters)
        self.assertEqual(count, 1)
        httpx_count = sum(isinstance(f, SecretRedactingFilter) for f in logging.getLogger("httpx").filters)
        self.assertEqual(httpx_count, 1)

    def test_httpx_loggers_quieted_to_warning(self):
        logging.getLogger("httpx").setLevel(logging.NOTSET)
        logging.getLogger("httpcore").setLevel(logging.DEBUG)
        quiet_http_client_loggers()
        self.assertGreaterEqual(logging.getLogger("httpx").getEffectiveLevel(), logging.WARNING)
        self.assertGreaterEqual(logging.getLogger("httpcore").getEffectiveLevel(), logging.WARNING)
        self.assertFalse(logging.getLogger("httpx").isEnabledFor(logging.INFO))

    def test_main_module_installs_logging(self):
        import main  # noqa: F401 — importing runs install_logging()

        self.assertGreaterEqual(logging.getLogger("httpx").getEffectiveLevel(), logging.WARNING)
        self.assertTrue(any(isinstance(f, SecretRedactingFilter) for f in logging.getLogger("httpx").filters))

    def test_httpx_warning_record_through_logger_is_redacted(self):
        install_secret_redaction()
        quiet_http_client_loggers()
        logging.getLogger("httpx").warning(
            "HTTP Request: %s %s", "GET", httpx.URL(f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"))
        output = self.stream.getvalue()
        self.assertIn(f"/bot{REDACTED}/sendMessage", output)
        self.assertNotIn(TG_TOKEN, output)


if __name__ == "__main__":
    unittest.main()
