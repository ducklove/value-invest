"""한국 국채(1/2/20/30년)·KOFR 무료 소스 탐색. 작업 후 삭제."""
import asyncio, json, re
import httpx

UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
      "Accept": "application/json,text/html,*/*"}


async def probe_naver_main():
    print("\n========== Naver marketindex 메인 금리 테이블 항목 ==========")
    async with httpx.AsyncClient(timeout=8) as c:
        r = await c.get("https://finance.naver.com/marketindex/", headers=UA)
        html = r.content.decode("euc-kr", errors="ignore")
    # 금리 섹션 marketindexCd 와 라벨
    irr = re.findall(r'marketindexCd=(IRR_\w+)"[^>]*>([^<]*)', html)
    print("IRR 항목:", irr)


async def probe_naver_mobile():
    print("\n========== Naver 모바일 채권 API 시도 ==========")
    urls = [
        "https://m.stock.naver.com/api/bond/majorBond",
        "https://m.stock.naver.com/api/marketindex/interest",
        "https://api.stock.naver.com/marketindex/interest",
        "https://m.stock.naver.com/api/home/marketIndex",
    ]
    async with httpx.AsyncClient(timeout=8, follow_redirects=True) as c:
        for u in urls:
            try:
                r = await c.get(u, headers=UA)
                body = r.text[:200]
                print(f"  [{r.status_code}] {u}\n      {body!r}")
            except Exception as e:
                print(f"  [ERR] {u}: {type(e).__name__}")


async def probe_ecos():
    print("\n========== 한국은행 ECOS (sample 키) 817Y002 시장금리(일별) ==========")
    # 항목 리스트 먼저
    async with httpx.AsyncClient(timeout=10, follow_redirects=True) as c:
        # 통계항목 목록
        try:
            r = await c.get("https://ecos.bok.or.kr/api/StatisticItemList/sample/json/kr/1/100/817Y002",
                            headers=UA)
            d = json.loads(r.text)
            rows = d.get("StatisticItemList", {}).get("row", [])
            print(f"  항목 수: {len(rows)}")
            for it in rows:
                nm = it.get("ITEM_NAME", "")
                if any(k in nm for k in ["국고채", "통안", "KOFR", "콜", "CD"]):
                    print(f"    {it.get('ITEM_CODE')}  {nm}")
        except Exception as e:
            print(f"  항목목록 ERR: {type(e).__name__} {str(e)[:100]}")
        # 최근값 조회 (국고채3년 010200000 알려진 코드로 sample 테스트)
        try:
            r = await c.get("https://ecos.bok.or.kr/api/StatisticSearch/sample/json/kr/1/5/817Y002/D/20260501/20260602/010200000",
                            headers=UA)
            print("  국고채3년 최근:", r.text[:300])
        except Exception as e:
            print(f"  값조회 ERR: {type(e).__name__}")


async def probe_kofr():
    print("\n========== KOFR (kofr.kr) ==========")
    urls = [
        "https://www.kofr.kr/main.do",
        "https://www.kofr.kr/api/kofr",
    ]
    async with httpx.AsyncClient(timeout=8, follow_redirects=True) as c:
        for u in urls:
            try:
                r = await c.get(u, headers=UA)
                txt = r.text
                # KOFR 값 패턴
                m = re.findall(r'(\d\.\d{2,4})\s*%?', txt[:5000])
                print(f"  [{r.status_code}] {u} len={len(txt)} 숫자샘플={m[:6]}")
            except Exception as e:
                print(f"  [ERR] {u}: {type(e).__name__}")


async def main():
    await probe_naver_main()
    await probe_naver_mobile()
    await probe_ecos()
    await probe_kofr()

asyncio.run(main())
