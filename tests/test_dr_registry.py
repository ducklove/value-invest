import unittest

import dr_registry


class DrRegistryTests(unittest.TestCase):
    def test_lookup(self):
        self.assertTrue(dr_registry.drs_for("005930"))   # 삼성전자
        self.assertTrue(dr_registry.drs_for("005490"))   # POSCO홀딩스
        self.assertEqual(dr_registry.drs_for("999999"), [])
        self.assertEqual(dr_registry.drs_for(""), [])
        self.assertEqual(dr_registry.drs_for("  005930  "), dr_registry.drs_for("005930"))

    def test_entries_well_formed(self):
        for code, drs in dr_registry.DR_REGISTRY.items():
            self.assertRegex(code, r"^\d{6}$", f"종목코드 형식 오류: {code}")
            self.assertTrue(drs)
            for d in drs:
                for key in ("ticker", "exchange", "currency", "shares_per_dr", "label"):
                    self.assertIn(key, d, f"{code} {key} 누락")
                self.assertGreater(d["shares_per_dr"], 0)
                self.assertTrue(d["ticker"])

    def test_known_ratios(self):
        """환산가/원주가 실측으로 확정한 핵심 교환비율 회귀 방지."""
        def spd(code):
            return dr_registry.drs_for(code)[0]["shares_per_dr"]
        self.assertEqual(spd("005930"), 25)    # 삼성전자 GDR (SMSN.L)
        self.assertEqual(spd("005935"), 25)    # 삼성전자우 GDS (SMSD.L)
        self.assertEqual(spd("000660"), 1)     # SK하이닉스 GDR (HY9H.F)
        self.assertEqual(spd("005490"), 0.25)  # POSCO홀딩스 ADR (PKX)
        self.assertEqual(spd("316140"), 3)     # 우리금융 ADR (WF)
        self.assertEqual(spd("015760"), 0.5)   # 한국전력 ADR (KEP)
        self.assertEqual(spd("030200"), 0.5)   # KT ADR
        # SK텔레콤은 1/9 가 아니라 5/9 (2021 분할/분사). 1/9 로 회귀하면 5배 오차.
        self.assertAlmostEqual(spd("017670"), 5 / 9)


if __name__ == "__main__":
    unittest.main()
