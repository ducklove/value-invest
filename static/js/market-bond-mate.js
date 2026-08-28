// 투자정보 탭의 국채·환율 데이터 소스 — bond-mate(형제 프로젝트) 연동.
//
// 왜 이렇게 나눴나
//   bond-mate 는 전 세계 금리·환율·회사채의 **source of record** 다. 국가·만기
//   커버리지(16개국, 한국 국고채 전 만기)와 히스토리를 그쪽이 관리하고, 여기서는
//   그 카탈로그와 값을 받아 기존 렌더러에 그대로 먹인다.
//
//   다만 bond-mate 는 정적 사이트라 30분마다 갱신된다. 이 앱의
//   /api/market-summary 는 60초 TTL 라이브 스크래핑이라 **같은 코드라면 로컬이
//   더 신선하다**. 그래서 bond-mate 로 판을 깔고 로컬 값을 위에 덮는다:
//
//       카탈로그·커버리지 → bond-mate (넓다)
//       값               → 로컬 라이브가 있으면 로컬, 없으면 bond-mate
//
//   bond-mate 에 닿지 못하면 병합을 통째로 건너뛴다 — 화면은 이 파일이 없던
//   때와 정확히 같게 동작한다.
//
// 로드 순서: utils.js 다음, market-dashboard.js 앞 (index.html 계약).

'use strict';

// bond-mate 시리즈 ID → 이 앱의 지표 코드. 대부분 같은 이름이라 다른 것만 적는다.
const BM_CODE_ALIASES = {
  US_ON: 'US_SOFR',       // 미국 익일물: bond-mate 는 SOFR 를 US_ON 으로 부른다
  KR_ON: 'KOFR',          // 한국 익일물
  JP_ON: 'JP_TONA',       // 일본 익일물
  KR3M: 'KR_CD91',        // 한국 3개월: CD(91일)
  KR6M: 'KR_KORIBOR6M',   // 한국 6개월: KORIBOR (국고채 6M 이 없어 대용)
};

// 이 앱 카탈로그에 없던 코드를 bond-mate 가 들고 올 때 붙일 라벨.
// (한국 국고채 1년·미국 1개월/7년·일본 7·15년·멕시코 10년 등)
const BM_COUNTRY_LABELS = {
  KR: '한국', US: '미국', JP: '일본', CN: '중국', DE: '독일', FR: '프랑스',
  GB: '영국', AU: '호주', IT: '이탈리아', ES: '스페인', CH: '스위스',
  CA: '캐나다', RU: '러시아', IN: '인도', ID: '인도네시아', BR: '브라질', MX: '멕시코',
};

const BM_TTL_MS = 5 * 60 * 1000;   // 5분 — 원본이 30분 주기라 더 자주 받을 이유가 없다
let _bmCache = null;               // {at, snapshot}
let _bmInFlight = null;

function _bmConfig() {
  const integrations = (typeof window !== 'undefined' && window.APP_CONFIG && window.APP_CONFIG.integrations) || {};
  return integrations.bondMate || null;
}

function bondMateBaseUrl() {
  const cfg = _bmConfig();
  return cfg && cfg.baseUrl ? String(cfg.baseUrl).replace(/\/+$/, '') : '';
}

/** 특정 화면으로 바로 가는 링크. 히스토리는 bond-mate 쪽에서 본다. */
function bondMateLink(view) {
  const base = bondMateBaseUrl();
  return base ? `${base}/?tab=${encodeURIComponent(view || 'overview')}` : '';
}

/** 스냅샷을 받아온다. 실패하면 null — 호출부는 병합을 건너뛴다. */
async function loadBondMateSnapshot(force) {
  if (!force && _bmCache && Date.now() - _bmCache.at < BM_TTL_MS) return _bmCache.snapshot;
  if (_bmInFlight) return _bmInFlight;

  const cfg = _bmConfig();
  const url = (cfg && cfg.dataUrl) || (bondMateBaseUrl() ? bondMateBaseUrl() + '/data/current.json' : '');
  if (!url) return null;

  _bmInFlight = (async () => {
    try {
      // GitHub Pages 는 Access-Control-Allow-Origin: * 를 주므로 브라우저에서
      // 직접 받는다(서버 프록시를 거치지 않아 파이 부하가 없다).
      const response = await fetch(url, { cache: 'no-cache' });
      if (!response.ok) throw new Error('HTTP ' + response.status);
      const snapshot = await response.json();
      _bmCache = { at: Date.now(), snapshot };
      return snapshot;
    } catch (e) {
      console.warn('bond-mate 스냅샷을 불러오지 못했습니다 — 로컬 지표만 사용합니다', e);
      // 예전 스냅샷이라도 있으면 그걸 쓴다(빈 화면보다 낫다).
      return _bmCache ? _bmCache.snapshot : null;
    } finally {
      _bmInFlight = null;
    }
  })();
  return _bmInFlight;
}

function _bmLocalCode(seriesId) {
  return BM_CODE_ALIASES[seriesId] || seriesId;
}

/** bond-mate 의 수치 견적을 이 앱 dataMap 의 문자열 형태로 바꾼다. */
function _bmQuote(quote, decimals) {
  if (!quote || quote.value == null) return null;
  const digits = decimals == null ? 2 : decimals;
  const change = quote.change;
  const hasChange = change != null && isFinite(change) && Math.abs(change) >= Math.pow(10, -digits) / 2;
  return {
    value: Number(quote.value).toFixed(digits),
    change: hasChange ? Math.abs(change).toFixed(digits) : '',
    change_pct: hasChange && quote.change_pct != null ? Math.abs(quote.change_pct).toFixed(2) + '%' : '',
    direction: hasChange ? (change > 0 ? 'up' : 'down') : '',
  };
}

/** 환율은 통화쌍마다 읽는 자릿수가 다르다(달러/원 2자리, 유로/달러 4자리). */
function _bmFxDigits(pair) {
  if (pair === 'EUR_USD' || pair === 'GBP_USD' || pair === 'USD_CNY'
      || pair === 'USD_BRL' || pair === 'USD_MXN') return 4;
  if (pair === 'USD_INR') return 3;
  return 2;
}

function _bmRateLabel(seriesId, meta) {
  const country = BM_COUNTRY_LABELS[meta.country] || meta.country || '';
  if (meta.maturity === -1) return `${country} 기준금리`;
  if (meta.maturity === 0) return `${country} 익일물`;
  return `${country}${meta.tenor || ''}`.trim();
}

/**
 * bond-mate 스냅샷을 카탈로그·dataMap 에 병합한다.
 *
 * 반환 객체는 새로 만든 것이라 호출부의 원본은 건드리지 않는다.
 * bond-mate 가 없거나 비어 있으면 입력을 그대로 돌려준다.
 */
function mergeBondMate(catalog, dataMap, snapshot) {
  if (!snapshot || (!snapshot.rates && !snapshot.fx)) return { catalog, dataMap, merged: false };

  const nextCatalog = Object.assign({}, catalog);
  const nextData = Object.assign({}, dataMap);

  // 같은 (국가, 만기) 자리를 서로 다른 코드가 차지하는 경우가 있다 — 한국 1년은
  // 이 앱이 통안채(KR_MSB1Y)를, bond-mate 는 국고채(KR1Y)를 쓴다. 국고채 곡선에는
  // 국고채가 맞고, 무엇보다 둘을 함께 두면 커브가 어느 쪽을 그릴지 순서에 좌우된다.
  // 그래서 bond-mate 가 값을 주는 자리는 그쪽 코드로 통일하고 로컬 코드는 뺀다.
  const bondMateSlots = new Set();
  Object.entries(snapshot.rates || {}).forEach(([seriesId, meta]) => {
    if (meta && meta.value != null && meta.country && meta.maturity != null) {
      bondMateSlots.add(`${meta.country}:${meta.maturity}:${_bmLocalCode(seriesId)}`);
    }
  });
  const bondMateSlotKeys = new Set(
    [...bondMateSlots].map((slot) => slot.split(':').slice(0, 2).join(':'))
  );
  Object.keys(nextCatalog).forEach((code) => {
    const meta = nextCatalog[code];
    if (!meta || meta.category !== '국채' || meta.maturity == null || !meta.country) return;
    const slotKey = `${meta.country}:${meta.maturity}`;
    if (!bondMateSlotKeys.has(slotKey)) return;
    if (bondMateSlots.has(`${slotKey}:${code}`)) return;   // 같은 코드면 그대로
    delete nextCatalog[code];
    delete nextData[code];
  });

  // --- 국채·정책금리 ---
  Object.entries(snapshot.rates || {}).forEach(([seriesId, meta]) => {
    if (!meta || meta.maturity == null || !meta.country) return;
    const code = _bmLocalCode(seriesId);

    // 카탈로그: 이 앱이 모르던 만기·국가를 bond-mate 가 채운다.
    if (!nextCatalog[code]) {
      nextCatalog[code] = {
        label: _bmRateLabel(seriesId, meta),
        category: '국채',
        country: meta.country,
        maturity: meta.maturity,
      };
    }

    // 값: 로컬 라이브가 이미 채웠으면 그대로 둔다(그쪽이 60초로 더 신선하다).
    const local = nextData[code];
    if (local && local.value != null && local.value !== '') return;
    const quote = _bmQuote(meta, 2);
    if (quote) nextData[code] = quote;
  });

  // --- 환율 ---
  Object.entries(snapshot.fx || {}).forEach(([pair, meta]) => {
    if (!meta || meta.value == null) return;
    if (!nextCatalog[pair]) {
      nextCatalog[pair] = { label: meta.label || pair, category: '환율' };
    }
    const local = nextData[pair];
    if (local && local.value != null && local.value !== '') return;
    const quote = _bmQuote(meta, _bmFxDigits(pair));
    if (quote) nextData[pair] = quote;
  });

  return { catalog: nextCatalog, dataMap: nextData, merged: true };
}

// jsdom 테스트에서 모듈 단위로 부르기 위해 export (브라우저에서는 무시된다).
if (typeof module !== 'undefined' && module.exports) {
  module.exports = {
    BM_CODE_ALIASES,
    bondMateBaseUrl,
    bondMateLink,
    loadBondMateSnapshot,
    mergeBondMate,
    _bmQuote,
    _bmFxDigits,
  };
}
