window.APP_CONFIG = {
  // GitHub Pages에서 프런트만 배포할 때 FastAPI 서버 URL을 설정합니다.
  apiBaseUrl: "https://cantabile.tplinkdns.com:3691",
  integrations: {
    holdingValue: {
      baseUrl: "https://ducklove.github.io/holding_value",
      holdingsUrl: "https://ducklove.github.io/holding_value/api/holdings.json"
    },
    preferredSpread: {
      baseUrl: "https://ducklove.github.io/common_preferred_spread"
    },
    goldGap: {
      baseUrl: "https://ducklove.github.io/gold_gap",
      dataUrl: "https://ducklove.github.io/gold_gap/data.json"
    },
    kisProxy: {
      baseUrl: "http://cantabile.tplinkdns.com:3288",
      role: "server-side"
    }
  }
};
