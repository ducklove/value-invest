// Compatibility loader for older cached HTML that still requests /js/portfolio.js.
// The actual implementation is split across portfolio-*.js files and loaded
// directly by index.html. If this legacy entrypoint is requested, load and
// evaluate the split files synchronously so deferred app-main.js still sees
// the same global functions before it runs.
(function loadPortfolioSplitCompat() {
  if (window.__VALUE_INVEST_PORTFOLIO_SPLIT_LOADED__) return;
  var files = [
    'portfolio-shell.js',
    'portfolio-data.js',
    'portfolio-render.js',
    'portfolio-actions.js',
    'portfolio-insights.js',
    'portfolio-groups-market.js',
    'portfolio-ai.js',
    'portfolio-performance.js',
    'portfolio-trends.js',
    'portfolio-cashflows.js',
    'portfolio-events.js',
  ];
  var base = (document.currentScript && document.currentScript.src)
    ? document.currentScript.src.replace(/portfolio\.js(?:\?.*)?$/, '')
    : './js/';
  var source = '';
  for (var i = 0; i < files.length; i += 1) {
    var xhr = new XMLHttpRequest();
    xhr.open('GET', base + files[i], false);
    xhr.send(null);
    if (xhr.status !== 0 && (xhr.status < 200 || xhr.status >= 300)) {
      throw new Error('Failed to load ' + files[i] + ' (' + xhr.status + ')');
    }
    source += '\n;/* ' + files[i] + ' */\n' + xhr.responseText + '\n';
  }
  (0, eval)(source + '\n//# sourceURL=portfolio-split-compat.js');
})();
