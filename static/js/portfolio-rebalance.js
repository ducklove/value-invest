// Portfolio rebalancing helper — 성과 탭의 '리밸런싱' 카드 (#pfRiskWrap 다음).
//
// GET  /api/portfolio/rebalance         (routes/rebalance.py) 를 소비해
//      목표 비중 대비 드리프트 보고서(현재/목표 비중, 이탈, 매수·매도 제안)를
//      테이블로 렌더링한다. as_of = 최근 일별 스냅샷 날짜.
// PUT  /api/portfolio/rebalance/targets  목표 전체 목록 교체 — '목표 설정'
//      토글로 여는 인라인 리스트 에디터(모달 아님)에서 저장한다.
// 알림: '이탈 시 알림' 체크박스가 알림 API(routes/notifications.py)의
//      rebalance_drift 규칙(사용자당 singleton, 임계값 없음)을 켜고 끈다.
//      켜기 = POST(재생성 시 기존 규칙 갱신), 끄기 = PUT {enabled:false}.
//
// - lazy: 성과 탭이 처음 보일 때 pfSwitchTab(portfolio-performance.js)이
//   pfLoadRebalancePanel() 을 호출한다. 보고서는 인메모리 메모(_pfRebalData)
//   — 목표 저장 시 force 재조회.
// - 백그라운드 로드 오류는 reportApiError silent + 패널 내 안내,
//   사용자 조작(저장/알림 토글) 오류는 토스트(서버 detail 전달).
// 포맷터(fmtPct/fmtKrw/returnClass/escapeHtml)는 portfolio-render.js /
// utils.js 공용 헬퍼를 재사용한다 — 여기서 중복 정의하지 않는다.

let _pfRebalData = null; // 마지막 GET /api/portfolio/rebalance 페이로드 (메모)
let _pfRebalLoadSeq = 0;
let _pfRebalAlertRule = null; // rebalance_drift 알림 규칙 (없으면 null)

function _pfRebalContentEl() { return document.getElementById('pfRebalanceContent'); }

function _pfRebalMsg(message) {
  const el = _pfRebalContentEl();
  if (el) el.innerHTML = `<div class="pf-risk-empty">${escapeHtml(message)}</div>`;
}

// --- Drift report -----------------------------------------------------------

function _pfRebalScopeBadge(scope) {
  const isGroup = scope === 'group';
  return `<span class="pf-rebal-badge${isGroup ? ' group' : ''}">${isGroup ? '그룹' : '종목'}</span>`;
}

// 절대 퍼센트(현재/목표 비중)는 부호 없이, 이탈은 부호 포함으로 그린다.
function _pfRebalRowHtml(item) {
  const current = item.current_weight_pct === null || item.current_weight_pct === undefined
    ? '-'
    : fmtPct(item.current_weight_pct, false);
  const currentSub = item.current_value === null || item.current_value === undefined
    ? ''
    : `<div class="pf-rebal-sub">${fmtKrw(item.current_value)}원</div>`;
  const target = `${fmtPct(item.target_weight_pct, false)} <span class="pf-rebal-sub">±${Number(item.tolerance_pct)}%p</span>`;
  const driftCls = `pf-rebal-drift ${returnClass(item.drift_pct)}${item.breached ? ' breached' : ''}`;
  const drift = item.drift_pct === null || item.drift_pct === undefined
    ? '-'
    : `${item.breached ? '⚠ ' : ''}${fmtPct(item.drift_pct)}`;
  let action = '-';
  if (item.action && item.action_amount) {
    const shares = item.approx_shares ? ` <span class="pf-rebal-sub">(~${Number(item.approx_shares).toLocaleString()}주)</span>` : '';
    action = `${escapeHtml(item.action)} ${fmtKrw(item.action_amount)}원${shares}`;
  }
  return `<tr class="${item.breached ? 'pf-rebal-row-breached' : ''}" data-scope="${escapeHtml(item.scope)}" data-key="${escapeHtml(item.key)}">
    <td class="pf-rebal-label">${_pfRebalScopeBadge(item.scope)} ${escapeHtml(item.label)}</td>
    <td class="pf-rebal-num">${current}${currentSub}</td>
    <td class="pf-rebal-num">${target}</td>
    <td class="pf-rebal-num ${driftCls}">${drift}</td>
    <td class="pf-rebal-num">${action}</td>
  </tr>`;
}

function _pfRenderRebalanceReport(data) {
  const el = _pfRebalContentEl();
  if (!el) return;
  const items = (data && Array.isArray(data.items)) ? data.items : [];
  if (!items.length) {
    _pfRebalMsg('목표 비중을 설정하면 이탈 현황이 표시됩니다.');
    return;
  }
  const breached = Number(data.breached_count || 0);
  const breachedHtml = breached > 0
    ? `<strong class="pf-rebal-breached-count">이탈 ${breached}건</strong>`
    : '이탈 없음';
  const asOf = data.as_of ? `기준일 <strong>${escapeHtml(data.as_of)}</strong>` : '';
  const total = (data.total_value !== null && data.total_value !== undefined)
    ? `총평가액 ${fmtKrw(data.total_value)}원` : '';
  el.innerHTML = `<div class="pf-rebal-table-wrap"><table class="pf-rebal-table">
    <thead><tr><th>항목</th><th>현재 비중</th><th>목표 비중</th><th>이탈</th><th>제안</th></tr></thead>
    <tbody>${items.map(_pfRebalRowHtml).join('')}</tbody>
  </table></div>
  <div class="pf-chart-range">${[asOf, total, breachedHtml].filter(Boolean).join(' · ')}</div>`;
}

// 성과 탭이 보일 때 호출(lazy). 메모가 있으면 그대로 그린다.
async function pfLoadRebalancePanel({ force = false } = {}) {
  const el = _pfRebalContentEl();
  if (!el) return;
  void _pfRebalSyncAlertToggle(); // 알림 토글 상태는 백그라운드 동기화
  if (!force && _pfRebalData) {
    _pfRenderRebalanceReport(_pfRebalData);
    return;
  }
  const seq = ++_pfRebalLoadSeq;
  _pfRebalMsg('리밸런싱 현황을 불러오는 중입니다...');
  try {
    const data = await apiFetchJson('/api/portfolio/rebalance', {
      errorMessage: '리밸런싱 현황 요청 실패',
    });
    _pfRebalData = data;
    if (seq !== _pfRebalLoadSeq) return;
    _pfRenderRebalanceReport(data);
  } catch (e) {
    if (e?.status === 401) {
      if (seq === _pfRebalLoadSeq) _pfRebalMsg('로그인 후 이용할 수 있습니다.');
      return;
    }
    // 백그라운드 로드 — 토스트 없이 콘솔 기록만 남기고 패널 안에 안내.
    reportApiError(e, '리밸런싱 현황', { silent: true });
    if (seq === _pfRebalLoadSeq) {
      _pfRebalMsg('리밸런싱 현황을 불러오지 못했습니다. 잠시 후 다시 시도해 주세요.');
    }
  }
}

// --- Target editor ('목표 설정' 토글 → 인라인 리스트 에디터) -----------------

function _pfRebalKeyOptionsHtml(scope, selectedKey = '') {
  let opts;
  if (scope === 'group') {
    opts = ((window.PfStore && PfStore.groups) || [])
      .map((g) => ({ value: g.group_name, label: g.group_name }));
  } else {
    opts = ((window.PfStore && PfStore.items) || [])
      .map((it) => ({ value: it.stock_code, label: `${it.stock_name || it.stock_code} (${it.stock_code})` }));
  }
  // 보유 청산/그룹 삭제 등으로 목록에 없는 기존 목표 키도 선택 상태를 보존.
  if (selectedKey && !opts.some((o) => o.value === selectedKey)) {
    opts.unshift({ value: selectedKey, label: selectedKey });
  }
  if (!opts.length) return '<option value="">항목 없음</option>';
  return opts.map((o) =>
    `<option value="${escapeHtml(o.value)}"${o.value === selectedKey ? ' selected' : ''}>${escapeHtml(o.label)}</option>`
  ).join('');
}

function _pfRebalEditorRowHtml(target = {}) {
  const scope = target.scope === 'group' ? 'group' : 'stock';
  const pct = target.target_weight_pct ?? '';
  const tol = target.tolerance_pct ?? '';
  return `<div class="pf-rebal-editor-row">
    <select class="pf-modal-input pf-rebal-scope" onchange="pfRebalanceScopeChanged(this)" aria-label="구분">
      <option value="stock"${scope === 'stock' ? ' selected' : ''}>종목</option>
      <option value="group"${scope === 'group' ? ' selected' : ''}>그룹</option>
    </select>
    <select class="pf-modal-input pf-rebal-key" aria-label="대상">${_pfRebalKeyOptionsHtml(scope, target.key || '')}</select>
    <input class="pf-modal-input pf-rebal-target" type="number" step="any" min="0" max="100" placeholder="목표 %" value="${pct === '' ? '' : Number(pct)}">
    <input class="pf-modal-input pf-rebal-tol" type="number" step="any" min="0" max="100" placeholder="±%p (기본 5)" value="${tol === '' ? '' : Number(tol)}">
    <button class="pf-alert-btn danger" type="button" title="이 목표 삭제" onclick="this.closest('.pf-rebal-editor-row').remove()">✕</button>
  </div>`;
}

// scope(종목/그룹) 변경 시 같은 행의 대상 select 옵션만 다시 채운다.
function pfRebalanceScopeChanged(scopeSelect) {
  const row = scopeSelect.closest('.pf-rebal-editor-row');
  const keySelect = row && row.querySelector('.pf-rebal-key');
  if (keySelect) keySelect.innerHTML = _pfRebalKeyOptionsHtml(scopeSelect.value, '');
}

function pfRebalanceEditorAddRow() {
  const rows = document.getElementById('pfRebalanceEditorRows');
  if (rows) rows.insertAdjacentHTML('beforeend', _pfRebalEditorRowHtml());
}

function pfRebalanceToggleEditor() {
  const editor = document.getElementById('pfRebalanceEditor');
  if (!editor) return;
  if (editor.style.display !== 'none') {
    editor.style.display = 'none';
    return;
  }
  const existing = (_pfRebalData && Array.isArray(_pfRebalData.items)) ? _pfRebalData.items : [];
  const rowsHtml = (existing.length ? existing : [{}]).map(_pfRebalEditorRowHtml).join('');
  editor.innerHTML = `
    <div class="pf-rebal-editor-head">목표 비중 설정 — 종목·그룹별 목표 %와 허용 오차(%p). 저장 시 목록 전체가 교체됩니다.</div>
    <div id="pfRebalanceEditorRows">${rowsHtml}</div>
    <div class="pf-rebal-editor-actions">
      <button class="pf-alert-btn" type="button" onclick="pfRebalanceEditorAddRow()">+ 행 추가</button>
      <button class="pf-alert-btn primary" type="button" onclick="pfRebalanceSave()">저장</button>
      <button class="pf-alert-btn" type="button" onclick="pfRebalanceToggleEditor()">닫기</button>
    </div>`;
  editor.style.display = '';
}

async function pfRebalanceSave() {
  const rows = [...document.querySelectorAll('#pfRebalanceEditorRows .pf-rebal-editor-row')];
  const targets = [];
  for (const row of rows) {
    const scope = row.querySelector('.pf-rebal-scope').value;
    const key = (row.querySelector('.pf-rebal-key').value || '').trim();
    const targetRaw = row.querySelector('.pf-rebal-target').value;
    const tolRaw = row.querySelector('.pf-rebal-tol').value;
    if (!key) { showToast('대상(종목/그룹)을 선택해주세요.', 'warning'); return; }
    if (targetRaw === '') { showToast('목표 비중(%)을 입력해주세요.', 'warning'); return; }
    const t = { scope, key, target_weight_pct: Number(targetRaw) };
    if (tolRaw !== '') t.tolerance_pct = Number(tolRaw); // 비우면 서버 기본값(5%p)
    targets.push(t);
  }
  try {
    await apiFetchJson('/api/portfolio/rebalance/targets', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ targets }),
      errorMessage: '리밸런싱 목표 저장에 실패했습니다.',
    });
    const editor = document.getElementById('pfRebalanceEditor');
    if (editor) editor.style.display = 'none';
    await pfLoadRebalancePanel({ force: true });
  } catch (e) {
    reportApiError(e, '리밸런싱 목표 저장');
  }
}

// --- '이탈 시 알림' 토글 (rebalance_drift 규칙) -------------------------------
// portfolio-alerts.js 의 조건 알림과 같은 API 를 쓰지만, 이 카드에서 바로
// 켜고 끌 수 있도록 최소한의 로컬 호출만 둔다(규칙 목록 UI 는 알림 모달 몫).

async function _pfRebalFetchAlertRule() {
  const rules = await apiFetchJson('/api/notifications/alerts', { fallback: [] });
  return (Array.isArray(rules) ? rules : []).find((r) => r.alert_type === 'rebalance_drift') || null;
}

async function _pfRebalSyncAlertToggle() {
  const cb = document.getElementById('pfRebalanceAlertCb');
  if (!cb) return;
  try {
    _pfRebalAlertRule = await _pfRebalFetchAlertRule();
    cb.checked = !!(_pfRebalAlertRule && _pfRebalAlertRule.enabled);
  } catch (e) {
    reportApiError(e, '리밸런싱 알림 상태', { silent: true });
  }
}

async function pfRebalanceToggleAlert(checked) {
  const cb = document.getElementById('pfRebalanceAlertCb');
  try {
    if (checked) {
      // 사용자당 singleton — 재POST 는 기존 규칙을 갱신(재활성화)한다.
      const data = await apiFetchJson('/api/notifications/alerts', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ alert_type: 'rebalance_drift' }),
        errorMessage: '리밸런싱 이탈 알림 설정에 실패했습니다.',
      });
      _pfRebalAlertRule = data;
    } else {
      if (!_pfRebalAlertRule) _pfRebalAlertRule = await _pfRebalFetchAlertRule();
      if (_pfRebalAlertRule && _pfRebalAlertRule.id != null) {
        await apiFetchJson(`/api/notifications/alerts/${_pfRebalAlertRule.id}`, {
          method: 'PUT',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ enabled: false }),
          errorMessage: '리밸런싱 이탈 알림 해제에 실패했습니다.',
        });
        _pfRebalAlertRule = { ..._pfRebalAlertRule, enabled: false };
      }
    }
  } catch (e) {
    reportApiError(e, '리밸런싱 이탈 알림');
    if (cb) cb.checked = !checked; // 실패 시 토글 원복
  }
}

if (typeof window !== 'undefined') {
  Object.assign(window, {
    pfLoadRebalancePanel, pfRebalanceToggleEditor, pfRebalanceEditorAddRow,
    pfRebalanceScopeChanged, pfRebalanceSave, pfRebalanceToggleAlert,
  });
}
