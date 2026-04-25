window.APP_CONFIG = {
  // GitHub Pages에서 프런트만 배포할 때 FastAPI 서버 URL을 설정합니다.
  apiBaseUrl: "https://cantabile.tplinkdns.com:3691",
  integrations: {
    holdingValue: {
      baseUrl: "https://ducklove.github.io/holding_value",
      configUrl: "https://ducklove.github.io/holding_value/config.json",
      holdingsUrl: "https://ducklove.github.io/holding_value/api/holdings.json"
    },
    preferredSpread: {
      baseUrl: "https://ducklove.github.io/common_preferred_spread",
      configUrl: "https://ducklove.github.io/common_preferred_spread/config.json",
      dataUrl: "https://ducklove.github.io/common_preferred_spread/data.js",
      currentUrl: "https://ducklove.github.io/common_preferred_spread/current.json"
    },
    goldGap: {
      baseUrl: "https://ducklove.github.io/gold_gap",
      configUrl: "https://ducklove.github.io/gold_gap/config.json",
      dataUrl: "https://ducklove.github.io/gold_gap/data.json",
      assets: {
        gold: { label: "Gold", portfolioCodes: ["KRX_GOLD"], thresholdPct: 5 },
        bitcoin: { label: "Bitcoin", portfolioCodes: ["CRYPTO_BTC"], thresholdPct: 5 },
        usdt: { label: "USDT", portfolioCodes: [], thresholdPct: 3 }
      },
      assetByPortfolioCode: {
        KRX_GOLD: "gold",
        CRYPTO_BTC: "bitcoin"
      }
    },
    kisProxy: {
      baseUrl: "http://cantabile.tplinkdns.com:3288",
      role: "server-side"
    }
  }
};
