import asyncio
import io
import unittest
import zipfile
from unittest.mock import AsyncMock, patch

import httpx

from repositories.broker_secrets import BrokerError
from services.brokers import namuh_listing as listing


def nh_row(code, market="1", stopped=False):
    row = bytearray(b" " * 236 + b"\n")
    row[:7] = (code + market).encode("ascii")
    row[161:162] = b"Y" if stopped else b"N"
    return bytes(row)


def konex_zip(codes):
    data = b"".join((code.ljust(9) + "KR7" + code + "000").encode("ascii") + b" " * 224 + b"\r\n" for code in codes)
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w") as archive:
        archive.writestr("konex_code.mst", data)
    return output.getvalue()


class NamuhListingTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        listing._cache.clear()
        self.addCleanup(listing._cache.clear)
        lock = patch.object(listing, "_lock", asyncio.Lock())
        lock.start()
        self.addCleanup(lock.stop)

    def test_parser_keeps_stopped_preferred_etf_etn_and_alphanumeric_codes(self):
        data = (nh_row("005930", stopped=True) + nh_row("000660", "4") + nh_row("02826K")
                + nh_row("069500") + nh_row("500023", "A") + nh_row("0001A0", "4"))
        self.assertEqual(listing.parse_nh_master(data), {"005930", "000660", "02826K", "069500", "500023", "0001A0"})
        self.assertEqual(listing.parse_konex_master(konex_zip(["0203K0", "0070X0"])), {"0203K0", "0070X0"})

    def test_invalid_and_incomplete_masters_are_not_an_empty_listing(self):
        valid = nh_row("005930") + nh_row("000660", "4") + nh_row("500023", "A")
        for data in (b"", valid[:-1], valid.replace(b"\n", b"\r"), nh_row("005930"),
                     valid + nh_row("005930"), valid + nh_row("072610", "?"), valid + nh_row("ABCDEF")):
            with self.subTest(data=data[:10]), self.assertRaises(ValueError):
                listing.parse_nh_master(data)
        for data in (konex_zip([]), konex_zip(["ABCDEF"]), konex_zip(["0203K0", "0203K0"])):
            with self.subTest(size=len(data)), self.assertRaises(ValueError):
                listing.parse_konex_master(data)

    async def test_download_is_shared_cached_and_includes_konex(self):
        calls = []
        nh = b"".join(nh_row(str(100000 + i), ("1", "4", "A")[i % 3]) for i in range(1000))
        konex = konex_zip([str(200000 + i) for i in range(10)])

        async def respond(request):
            calls.append(str(request.url))
            await asyncio.sleep(0)
            return httpx.Response(200, content=nh if str(request.url) == listing.NH_MASTER_URL else konex)

        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            with patch.object(listing, "get_http_client", AsyncMock(return_value=client)):
                first, second = await asyncio.gather(listing.listed_codes(), listing.listed_codes())
                self.assertEqual(await listing.listed_codes(), first)
        self.assertEqual(first, second)
        self.assertEqual(len(first), 1010)
        self.assertIn("200000", first)
        self.assertNotIn("072610", first)
        self.assertEqual(calls, [listing.NH_MASTER_URL, listing.KONEX_MASTER_URL])

    async def test_failed_download_or_short_master_does_not_cache_exclusions(self):
        valid = nh_row("005930") + nh_row("000660", "4") + nh_row("500023", "A")
        for status, body in ((503, b""), (200, b""), (200, b"<html>error</html>"), (200, valid)):
            with self.subTest(status=status, length=len(body)):
                async with httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(
                    status, content=body if str(request.url) == listing.NH_MASTER_URL else konex_zip(["0203K0"]),
                ))) as client:
                    with patch.object(listing, "get_http_client", AsyncMock(return_value=client)):
                        with self.assertRaisesRegex(BrokerError, "기존 잔고를 유지"):
                            await listing.listed_codes()
                self.assertIsNone(listing._cache.get("codes"))
