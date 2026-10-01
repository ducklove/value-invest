"""Price freshness, ranking, provider isolation and request budgets for tool cards."""

import asyncio
import copy
import unittest
from datetime import datetime
from unittest.mock import AsyncMock, patch

import httpx

from domain.timeutil import KST
from services import stock_quotes
from services.ecosystem import external_tools as ext
from services.ecosystem import live_cards as live


class LiveCardTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        ext._cache.clear()
        live._config_cache.clear()
        live._quotes_cache.clear()

    def tearDown(self):
        self.setUp()

    async def test_whole_candidate_lists_refresh_before_top_five_selection(self):
        holding = ({'pairs': [{'id': 'h', 'ratio': 500}], 'summary': {}, 'lastUpdated': '2026-09-29'},
                   [{'id': 'h', 'holdingTicker': '000001.KS', 'name': 'Holding'}])
        cfg = [{'id': 'h', 'holdingTicker': '000001.KS', 'holdingAdjustedShares': 80,
                'subsidiaries': [{'ticker': '000002.KS', 'sharesHeld': 40}]}]
        spread = ({'prices': {'p': {'spread': 10}}},
                  [{'id': 'p', 'name': 'Preferred', 'commonTicker': '000003.KS', 'preferredTicker': '000004.KS'}])
        spac = {'lastUpdated': '2026-09-29', 'prices': {
            f'{n:06d}': {'name': str(n), 'currentPrice': 1900 + n} for n in range(10, 16)}}
        quotes = {f'{n:06d}': {'price': p} for n, p in ((1, 100), (2, 300), (3, 1000), (4, 100), (15, 1800))}
        out = {'holding': {}, 'spread': {}, 'spac': {}}
        with patch.object(ext, '_load_pair', new=AsyncMock(side_effect=[holding, spread])), \
             patch.object(ext, '_spac_current', new=AsyncMock(return_value=spac)), \
             patch.object(live, 'holding_config', new=AsyncMock(return_value=cfg)), \
             patch.object(live, 'domestic_quotes', new=AsyncMock(return_value=quotes)) as bulk:
            await live.refresh(out)
        self.assertEqual(out['holding']['top'][0]['ratio'], 150)
        self.assertEqual(out['holding']['averageRatio'], 150)
        self.assertEqual(out['spread']['top'][0]['spread'], 90)
        self.assertEqual(out['spac']['top'][0]['code'], '000015')  # originally outside TOP 5
        self.assertEqual(out['spac']['top'][0]['currentPrice'], 1800)
        self.assertTrue(out['spac']['partialQuotes'])
        self.assertEqual(out['holding']['lastUpdated'], '2026-09-29')
        self.assertIn('quoteCheckedAt', out['holding'])
        self.assertEqual(len(bulk.await_args.args[0]), 10)

    async def test_missing_or_stale_leg_keeps_snapshot_without_new_timestamp(self):
        cur = {'pairs': [{'id': 'h', 'ratio': 456}], 'summary': {'averageRatio': 456}}
        cfg = [{'id': 'h', 'holdingTicker': '000001', 'holdingTotalShares': 100, 'holdingTreasuryShares': 20,
                'subsidiaries': [{'ticker': '000002', 'sharesHeld': 50}]}]
        self.assertEqual(live.refresh_holding(cur, cfg, {'000001': {'price': 100}}), 0)
        self.assertEqual(live.refresh_holding(cur, cfg, {'000001': {'price': 100},
                                                      '000002': {'price': 300, '_stale': True}}), 0)
        self.assertEqual(cur['pairs'][0]['ratio'], 456)
        self.assertEqual(live.refresh_holding(cur, cfg, {'000001': {'price': 100}, '000002': {'price': 300}}), 1)
        self.assertEqual(cur['pairs'][0]['ratio'], 187.5)  # treasury deduction
        self.assertIsNone(live.price({'price': float('inf')}))

    async def test_spread_updates_all_pairs_then_deduplicates_common_stock(self):
        cfg = [{'id': 'a', 'name': 'A', 'commonTicker': '000001', 'preferredTicker': '000002'},
               {'id': 'b', 'name': 'B', 'commonTicker': '000001', 'preferredTicker': '000003'}]
        cur = {'prices': {}}
        self.assertEqual(live.refresh_spread(cur, cfg, {c: {'price': p} for c, p in
                                                      [('000001', 100), ('000002', 50), ('000003', 20)]}), 2)
        card = ext._summarize_spread(cur, cfg)
        self.assertEqual(card['averageSpread'], 80)
        self.assertEqual(card['top'], [{'name': 'B', 'code': '000001', 'spread': 80, 'spreadChange': None}])

    async def test_holding_representative_value_keeps_sibling_median_definition(self):
        cur = {'pairs': [{'id': 'a', 'ratio': 10}, {'id': 'b', 'ratio': 100},
                         {'id': 'c', 'ratio': 1000}, {'id': '_average', 'ratio': 300}]}
        cfg = [{'id': key, 'holdingTicker': '000001', 'holdingAdjustedShares': 100,
                'subsidiaries': [{'ticker': '000002', 'sharesHeld': shares}]}
               for key, shares in [('a', 10), ('b', 100), ('c', 1000)]]
        live.refresh_holding(cur, cfg, {'000001': {'price': 100}, '000002': {'price': 100}})
        self.assertEqual(cur['summary']['averageRatio'], 100)

    async def test_live_failure_preserves_base_cards(self):
        out = {'holding': {'top': [{'ratio': 123}]}, 'spac': {'top': [{'currentPrice': 1900}]}}
        original = copy.deepcopy(out)
        with patch.object(live, '_refresh', new=AsyncMock(side_effect=httpx.ConnectError('offline'))):
            await live.refresh(out)
        self.assertEqual(out, original)

    async def test_slow_gold_source_does_not_block_bond_update(self):
        out = {'goldGap': {'assets': []}, 'bondMate': {'usCurveSpreadBp': 42}}
        market = {'US10Y': {'value': 3.8}, 'US2Y': {'value': 4.1}}
        with patch.object(live.indicators, 'fetch_indicators', new=AsyncMock(return_value=market)), \
             patch.object(live, 'refresh_gold', new=AsyncMock(side_effect=httpx.ReadTimeout('gold offline'))):
            await live.refresh(out)
        self.assertEqual(out['bondMate']['usCurveSpreadBp'], -30)
        self.assertIn('quoteCheckedAt', out['bondMate'])

    async def test_gold_uses_matching_units_and_bithumb_usdt_and_isolates_failure(self):
        card = {'assets': [{'key': k, 'gap': -99} for k in ['gold', 'bitcoin', 'eth', 'usdt']]}
        domestic = {'KRX_GOLD': {'price': 120000}, 'CRYPTO_BTC': {'price': 132000000}, 'CRYPTO_ETH': {}}
        intl = {'BTC-USD': {'price': 100000}, 'ETH-USD': {'price': 3000}, 'USDT-USD': {'price': 0.99}}
        with patch.object(stock_quotes, 'get_quote_snapshot', new=AsyncMock(side_effect=lambda c: domestic[c])), \
             patch.object(live.premium_quotes, 'international_crypto', new=AsyncMock(side_effect=lambda c: intl[c])), \
             patch.object(live.premium_quotes, 'usdt_krw', new=AsyncMock(return_value={'price': 1320})) as usdt:
            await live.refresh_gold(card, {'USD_KRW': {'value': '1,200'}, 'CMDT_GC': {'value': '3,110.35'}})
        self.assertEqual([r['gap'] for r in card['assets']], [0, 10, -99, 11.11])
        usdt.assert_awaited_once()
        self.assertTrue(card['partialQuotes'])
        self.assertNotIn('quoteCheckedAt', card['assets'][2])
        # Missing gold future must not be mistaken for a $1/g reference.
        original = {'assets': [{'key': 'gold', 'gap': 3}]}
        with patch.object(stock_quotes, 'get_quote_snapshot', new=AsyncMock(return_value={'price': 120000})):
            await live.refresh_gold(original, {'USD_KRW': {'value': 1200}})
        self.assertEqual(original, {'assets': [{'key': 'gold', 'gap': 3}]})

    async def test_bond_spreads_require_two_usable_legs_and_preserve_credit(self):
        card = {'usCurveSpreadBp': 42, 'krCurveSpreadBp': 43, 'creditSpreadBp': {'BBB': 102}}
        live.refresh_bonds(card, {'US_BASE': {'value': '4.00'}, 'US10Y': {'value': '3.80'},
                                 'US2Y': {'value': '4.10'}, 'KR10Y': {'value': '3.00'},
                                 'KR3Y': {'value': '2.00', '_stale': True}})
        self.assertEqual(card['usCurveSpreadBp'], -30)
        self.assertTrue(card['usCurveInverted'])
        self.assertEqual(card['krCurveSpreadBp'], 43)
        self.assertEqual(card['creditSpreadBp'], {'BBB': 102})

    async def test_equity_cache_changes_at_open_and_relaxes_when_closed(self):
        codes = ['000001']
        with patch.object(stock_quotes, 'get_bulk_quote_snapshots', new=AsyncMock(return_value={'000001': {'price': 100}})) as bulk:
            with patch.object(live, 'now_kst', return_value=datetime(2026, 10, 1, 7, tzinfo=KST)):
                await live.domestic_quotes(codes)
                await live.domestic_quotes(codes)
                self.assertEqual(bulk.await_count, 1)
                self.assertEqual(live._quotes_cache.get_entry('closed:000001').ttl_seconds, 900)
            with patch.object(live, 'now_kst', return_value=datetime(2026, 10, 1, 9, tzinfo=KST)):
                await live.domestic_quotes(codes)
                self.assertEqual(bulk.await_count, 2)
                self.assertEqual(live._quotes_cache.get_entry('active:000001').ttl_seconds, 120)

    async def test_mixed_etfs_use_domestic_bulk_and_foreign_single_quotes(self):
        picks = [{'code': '069500'}, {'code': 'vgt'}, {'code': 'BAD'}, {'code': '000001'}]
        with patch.object(stock_quotes, 'get_bulk_quote_snapshots', new=AsyncMock(return_value={
            '069500': {'change_pct': 1.2}, '000001': {'change_pct': 9, '_stale': True}})) as bulk, \
             patch.object(stock_quotes, 'get_quote_snapshot', new=AsyncMock(side_effect=[
                 {'change_pct': -2.3, 'as_of': '2026-10-01'}, httpx.ConnectError('offline')])) as single:
            await ext._fill_etf_changes(picks)
        self.assertEqual(bulk.await_args.args[0], ['069500', '000001'])
        self.assertEqual([c.args[0] for c in single.await_args_list], ['VGT', 'BAD'])
        self.assertEqual(picks[0]['changePct'], 1.2)
        self.assertEqual(picks[1]['changePct'], -2.3)
        self.assertNotIn('changePct', picks[2])
        self.assertNotIn('changePct', picks[3])

    async def test_insights_single_flight_and_two_minute_expiry(self):
        gate = asyncio.Event()
        async def load():
            await gate.wait()
            return {'spac': {'top': [{'currentPrice': 1800}]}}
        with patch.object(ext, '_load_external_insights', new=AsyncMock(side_effect=load)) as loader:
            tasks = [asyncio.create_task(ext.fetch_external_insights()) for _ in range(5)]
            await asyncio.sleep(0)
            gate.set()
            results = await asyncio.gather(*tasks)
            self.assertEqual(loader.await_count, 1)
            results[0]['spac']['top'][0]['currentPrice'] = 1
            self.assertEqual((await ext.fetch_external_insights())['spac']['top'][0]['currentPrice'], 1800)
            key = next(iter(ext._cache._data))
            entry = ext._cache.get_entry(key)
            self.assertEqual(entry.ttl_seconds, 120)
            ext._cache.set(key, copy.deepcopy(entry.value), ttl_seconds=-1)
            await ext.fetch_external_insights()
            self.assertEqual(loader.await_count, 2)

    async def test_new_kst_day_does_not_reuse_yesterdays_etf_picks(self):
        with patch.object(ext, '_load_external_insights', new=AsyncMock(return_value={'etfPicks': {}})) as loader:
            with patch.object(ext, 'datetime') as clock:
                clock.now.return_value = datetime(2026, 10, 1, 23, 59, tzinfo=KST)
                await ext.fetch_external_insights()
                clock.now.return_value = datetime(2026, 10, 2, 0, 0, tzinfo=KST)
                await ext.fetch_external_insights()
            self.assertEqual(loader.await_count, 2)
