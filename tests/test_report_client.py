"""Tests for report_client pure helpers (no network).

Scope is intentionally narrow — the network-dependent parts (fetch_reports
over HTTP, HTML parsing of specific Naver layouts) are exercised manually
and would be flaky in CI. _dedupe_reports is pure and the bug it fixes
(058650 returning 25 rows for ~13 unique reports) was user-visible.
"""
import unittest

from report_client import _dedupe_reports


class DedupeReportsTests(unittest.TestCase):
    def test_keeps_unique_rows(self):
        rows = [
            {"date": "2026-03-06", "firm": "신한투자증권", "title": "A", "pdf_url": None},
            {"date": "2026-02-10", "firm": "신한투자증권", "title": "B", "pdf_url": None},
            {"date": "2025-12-24", "firm": "한국IR협의회", "title": "C", "pdf_url": "https://x"},
        ]
        out = _dedupe_reports(rows)
        self.assertEqual(len(out), 3)
        self.assertEqual([r["title"] for r in out], ["A", "B", "C"])

    def test_drops_exact_duplicates(self):
        # Naver pagination frequently emits the same row on consecutive pages.
        rows = [
            {"date": "2026-03-06", "firm": "신한투자증권", "title": "리포트 1"},
            {"date": "2025-12-24", "firm": "신한투자증권", "title": "리포트 2"},
            {"date": "2026-03-06", "firm": "신한투자증권", "title": "리포트 1"},  # dup of 1
            {"date": "2025-12-24", "firm": "신한투자증권", "title": "리포트 2"},  # dup of 2
        ]
        out = _dedupe_reports(rows)
        self.assertEqual(len(out), 2)

    def test_first_occurrence_wins(self):
        # Second occurrence has pdf_url filled in — but the first survives.
        # That's OK: in practice enrichment happens before dedup, so the
        # first row is already the "richer" one when duplicates exist.
        rows = [
            {"date": "2025-08-08", "firm": "A", "title": "T", "pdf_url": "https://first"},
            {"date": "2025-08-08", "firm": "A", "title": "T", "pdf_url": "https://second"},
        ]
        out = _dedupe_reports(rows)
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["pdf_url"], "https://first")

    def test_distinguishes_same_title_different_firm(self):
        # Two firms releasing reports with the same headline on the same day
        # are NOT duplicates — the analyst perspective differs.
        rows = [
            {"date": "2026-03-06", "firm": "신한투자증권", "title": "반도체 수급 전망"},
            {"date": "2026-03-06", "firm": "미래에셋증권", "title": "반도체 수급 전망"},
        ]
        out = _dedupe_reports(rows)
        self.assertEqual(len(out), 2)

    def test_distinguishes_same_title_different_date(self):
        rows = [
            {"date": "2026-03-06", "firm": "X", "title": "연간 전망"},
            {"date": "2025-03-06", "firm": "X", "title": "연간 전망"},  # same firm, prior year
        ]
        out = _dedupe_reports(rows)
        self.assertEqual(len(out), 2)

    def test_handles_missing_fields(self):
        # Defensive — rows from unusual HTML layouts might miss keys. Should
        # not raise, and two rows with the same (None, None, None) collapse
        # to one rather than accumulating infinitely.
        rows = [
            {},
            {"date": None},
            {"firm": ""},
            {"date": "", "firm": "", "title": ""},
        ]
        out = _dedupe_reports(rows)
        self.assertEqual(len(out), 1)

    def test_058650_regression(self):
        # Shape of the actual 058650 response captured during triage:
        # Naver returned each unique row twice. 25 rows total, ~13 unique.
        pairs = [
            ("2026-03-06", "신한투자증권", "R1"),
            ("2025-12-24", "신한투자증권", "R2"),
            ("2025-08-27", "신한투자증권", "R3"),
            ("2025-08-08", "한국IR협의회", "R4"),
            ("2025-06-30", "신한투자증권", "R5"),
        ]
        rows = [{"date": d, "firm": f, "title": t} for d, f, t in pairs * 2]
        out = _dedupe_reports(rows)
        self.assertEqual(len(out), 5)


if __name__ == "__main__":
    unittest.main()
