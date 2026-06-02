"""작업1(외부도구 구조)·작업2(CNBC 신규 채권) 검증. 작업 후 삭제."""
import asyncio, json
import httpx

UA = {"User-Agent": "Mozilla/5.0", "Accept": "application/json,*/*"}
RAW = "https://raw.githubusercontent.com/ducklove"


async def probe_spread():
    print("\n========== 우선주 괴리율 (common_preferred_spread) ==========")
    async with httpx.AsyncClient(timeout=10, follow_redirects=True) as c:
        cfg = (await c.get(f"{RAW}/common_preferred_spread/master/config.json", headers=UA)).json()
        cur = (await c.get(f"{RAW}/common_preferred_spread/master/current.json", headers=UA)).json()
    print(f"config 항목 수: {len(cfg)}")
    print("config 첫 항목 키:", list(cfg[0].keys()) if cfg else None)
    print("config 첫 항목:", json.dumps(cfg[0], ensure_ascii=False) if cfg else None)
    # 두산퓨얼셀 / 같은 보통주에 우선주 여러 개인 케이스 찾기
    from collections import defaultdict
    by_common = defaultdict(list)
    for item in cfg:
        ct = (item.get("commonTicker") or "").split(".")[0]
        by_common[ct].append(item)
    multi = {k: v for k, v in by_common.items() if len(v) > 1}
    print(f"\n같은 보통주에 우선주 2개 이상인 그룹: {len(multi)}개")
    for ct, items in list(multi.items())[:6]:
        print(f"  보통주 {ct}:")
        for it in items:
            print(f"    id={it.get('id')} name={it.get('name')} "
                  f"commonName={it.get('commonName')} preferredName={it.get('preferredName')} "
                  f"prefTicker={it.get('preferredTicker')}")
    # prices 한 항목 구조
    prices = cur.get("prices") or {}
    k0 = next(iter(prices))
    print(f"\nprices['{k0}'] 키:", list(prices[k0].keys()))


async def probe_spac():
    print("\n\n========== 스팩 (spac-hunter) ==========")
    async with httpx.AsyncClient(timeout=10, follow_redirects=True) as c:
        cur = (await c.get(f"{RAW}/spac-hunter/main/current.json", headers=UA)).json()
    prices = cur.get("prices") or {}
    print(f"종목 수: {len(prices)}")
    for i, (code, v) in enumerate(prices.items()):
        if i >= 3:
            break
        print(f"  {code}: {json.dumps(v, ensure_ascii=False)}")


async def probe_cnbc():
    print("\n\n========== CNBC 신규 채권 심볼 ==========")
    candidates = [
        # 미국
        "US3M", "US1Y", "US3Y", "US20Y", "US6M",
        # 한국 국채 만기
        "KR1Y-KR", "KR2Y-KR", "KR20Y-KR", "KR30Y-KR",
        # KOFR 후보
        "KOFR", "KOFR-KR", "KR-KOFR", "KORIBOR", "KORIBOR-KR",
    ]
    url = "https://quote.cnbc.com/quote-html-webservice/restQuote/symbolType/symbol"
    async with httpx.AsyncClient(timeout=12, follow_redirects=True) as c:
        r = await c.get(url, params={
            "symbols": "|".join(candidates), "requestMethod": "itv", "noform": "1",
            "partnerId": "2", "fund": "1", "exthrs": "1", "output": "json",
        }, headers=UA)
        d = json.loads(r.text)
        qr = d.get("FormattedQuoteResult", {}).get("FormattedQuote", []) or []
        got = {q.get("symbol"): q for q in qr}
        for sym in candidates:
            q = got.get(sym)
            if q and q.get("last") not in (None, ""):
                print(f"  {sym:12} OK  last={q.get('last')!r} chg={q.get('change')!r} "
                      f"name={q.get('name')!r}")
            elif q:
                print(f"  {sym:12} 빈값 name={q.get('name')!r}")
            else:
                print(f"  {sym:12} 없음")


async def main():
    await probe_spread()
    await probe_spac()
    await probe_cnbc()

asyncio.run(main())
