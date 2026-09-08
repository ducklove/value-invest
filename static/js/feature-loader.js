// classic script의 전역 계약과 HTML 순서를 유지하는 화면별 로더.
const _featureScriptPromises = new Map();
const _featureReadyGroups = new Set();

function featureScriptsReady(group) {
  return _featureReadyGroups.has(group)
    || !document.querySelector(`script[data-feature="${group}"]`);
}

async function loadFeatureScripts(group) {
  const entries = document.querySelectorAll(`script[data-feature="${group}"]`);
  for (const entry of entries) {
    const src = entry.dataset.src;
    if (!_featureScriptPromises.has(src)) {
      const pending = new Promise((resolve, reject) => {
        const script = document.createElement('script');
        script.src = src;
        script.async = false;
        script.onload = () => resolve();
        script.onerror = () => {
          script.remove();
          _featureScriptPromises.delete(src);
          reject(new Error('화면 파일을 불러오지 못했습니다. 다시 눌러 주세요.'));
        };
        entry.after(script);
      });
      _featureScriptPromises.set(src, pending);
    }
    await _featureScriptPromises.get(src);
  }
  _featureReadyGroups.add(group);
}
