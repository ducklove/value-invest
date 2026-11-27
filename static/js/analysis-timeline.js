// 종목 타임라인 카드 — 종목을 열면 입력 없이 최근 뉴스와 DART 공시를 날짜순으로
// 보여 준다(GET /api/stocks/{code}/timeline). 소스별 상태를 구분해 "조회 실패"를
// "새 소식 없음"으로 보이지 않게 하고, 늦게 도착한 이전 종목 응답은 버린다.

let _stockTimelineSeq = 0;
let _stockTimelineData = null;
let _stockTimelineFilter = 'all';

const STOCK_TIMELINE_TYPE_LABEL = { news: '뉴스', disclosure: '공시' };

function _stockTimelineSourceNote(name, src) {
  const label = STOCK_TIMELINE_TYPE_LABEL[name] || name;
  if (!src) return '';
  if (src.status === 'error') return `${label} 조회 실패`;
  if (src.status === 'quota') return `${label}: 알림용 조회 한도 보호 중`;
  if (src.status === 'unsupported') return '';
  if (src.stale) return `${label}: 지난 목록 (최신 조회 실패)`;
  return '';
}

function stockTimelineHtml(data, filter = 'all') {
  const sources = data?.sources || {};
  const all = Array.isArray(data?.items) ? data.items : [];
  // '전체'는 최근 뉴스 10건 + 최근 공시 4건을 날짜순으로 섞는다 — 뉴스가 많은 날에도
  // 공시가 묻히지 않게. 필터를 고르면 그 종류를 전부 보여 준다.
  const items = filter === 'all'
    ? [...all.filter(r => r.type === 'news').slice(0, 10), ...all.filter(r => r.type === 'disclosure').slice(0, 4)]
      .sort((a, b) => `${b.date || ''} ${b.time || ''}`.localeCompare(`${a.date || ''} ${a.time || ''}`))
    : all.filter(row => row.type === filter);
  const notes = ['news', 'disclosure'].map(n => _stockTimelineSourceNote(n, sources[n])).filter(Boolean);
  const allFailed = ['news', 'disclosure'].every(n => ['error', 'quota', 'unsupported'].includes(sources[n]?.status));
  const chip = (key, label) => `<button type="button" class="stl-chip js-stl-filter${filter === key ? ' active' : ''}" data-filter="${key}" aria-pressed="${filter === key}">${label}</button>`;
  let body;
  if (items.length) {
    let lastDate = '';
    body = `<ol class="stl-list">${items.map(row => {
      const dateHead = row.date && row.date !== lastDate ? `<span class="stl-date">${escapeHtml(row.date.slice(5).replace('-', '.'))}</span>` : '<span class="stl-date"></span>';
      lastDate = row.date || lastDate;
      const safeUrl = /^https:\/\//.test(row.url || '') ? row.url : '';
      const title = safeUrl
        ? `<a href="${escapeHtml(safeUrl)}" target="_blank" rel="noopener noreferrer">${escapeHtml(row.title)}</a>`
        : escapeHtml(row.title);
      const meta = [row.time, row.source].filter(Boolean).map(escapeHtml).join(' · ');
      return `<li class="stl-row stl-${escapeHtml(row.type)}">${dateHead}
        <span class="stl-type">${escapeHtml(STOCK_TIMELINE_TYPE_LABEL[row.type] || row.type)}</span>
        <span class="stl-main">${title}${meta ? `<small>${meta}</small>` : ''}</span></li>`;
    }).join('')}</ol>`;
  } else if (allFailed) {
    body = '<p class="stl-empty">지금은 뉴스와 공시를 불러올 수 없습니다.</p>';
  } else {
    body = `<p class="stl-empty">${filter === 'disclosure' ? '최근 90일 공시가 없습니다.' : filter === 'news' ? '최근 기사가 없습니다.' : '최근 뉴스와 공시가 없습니다.'}</p>`;
  }
  const name = typeof _lastAnalysisData !== 'undefined' && _lastAnalysisData?.corp_name ? _lastAnalysisData.corp_name : (data?.stock_code || '');
  const searchUrl = `https://search.naver.com/search.naver?where=news&sort=1&query=${encodeURIComponent(name)}`;
  return `<div class="stl-head">
      <h3 id="stockTimelineTitle">최근 소식</h3>
      <div class="stl-chips" role="group" aria-label="종류">${chip('all', '전체')}${chip('news', '뉴스')}${chip('disclosure', '공시')}</div>
    </div>
    ${notes.length ? `<p class="stl-note" role="status">${notes.map(escapeHtml).join(' · ')}</p>` : ''}
    ${body}
    <a class="stl-more" href="${escapeHtml(searchUrl)}" target="_blank" rel="noopener noreferrer">${escapeHtml(name)} 뉴스 더 찾기 →</a>`;
}

async function loadStockTimeline(code) {
  const host = document.getElementById('stockTimelinePanel');
  if (!host) return;
  const seq = ++_stockTimelineSeq;
  _stockTimelineData = null;
  _stockTimelineFilter = 'all';
  if (!code) { host.hidden = true; host.innerHTML = ''; return; }
  host.hidden = false;
  host.innerHTML = '<div class="stl-head"><h3 id="stockTimelineTitle">최근 소식</h3></div><p class="stl-empty">불러오는 중…</p>';
  let data;
  try {
    data = await apiFetchJson(`/api/stocks/${encodeURIComponent(code)}/timeline?limit=20`, { errorMessage: '최근 소식을 불러오지 못했습니다.' });
  } catch (e) {
    if (seq !== _stockTimelineSeq) return;
    host.innerHTML = '<div class="stl-head"><h3 id="stockTimelineTitle">최근 소식</h3></div><p class="stl-empty">최근 소식을 불러오지 못했습니다.</p>';
    return;
  }
  if (seq !== _stockTimelineSeq) return;  // 그사이 다른 종목으로 바뀜
  const unsupported = ['news', 'disclosure'].every(n => data?.sources?.[n]?.status === 'unsupported');
  if (unsupported) { host.hidden = true; host.innerHTML = ''; return; }
  _stockTimelineData = data;
  host.innerHTML = stockTimelineHtml(data, _stockTimelineFilter);
}

(function initStockTimeline() {
  document.addEventListener('click', (e) => {
    const chip = e.target.closest?.('#stockTimelinePanel .js-stl-filter');
    if (!chip || !_stockTimelineData) return;
    _stockTimelineFilter = chip.dataset.filter || 'all';
    document.getElementById('stockTimelinePanel').innerHTML = stockTimelineHtml(_stockTimelineData, _stockTimelineFilter);
    document.querySelector(`#stockTimelinePanel .js-stl-filter[data-filter="${_stockTimelineFilter}"]`)?.focus();
  });
})();
