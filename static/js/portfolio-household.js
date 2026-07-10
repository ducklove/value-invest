// Household asset register, national net-worth position, and retirement plan.
// Official reference values are supplied by /api/household-assets so their
// survey date/source stays explicit and can be updated server-side.

const PfHousehold = {
  loaded: false,
  loading: false,
  saving: false,
  dirty: false,
  items: [],
  retirement: null,
  categories: {},
  owners: {},
  distribution: null,
  retirementReference: null,
  portfolioValue: 0,
  updatedAt: null,
};

const PF_HH_COLORS = {
  portfolio: '#4f46e5',
  real_estate: '#0f766e',
  cash: '#0891b2',
  pension: '#7c3aed',
  insurance: '#2563eb',
  business: '#d97706',
  vehicle: '#64748b',
  other: '#94a3b8',
  liability: '#e11d48',
};

function _pfHouseholdMoney(value) {
  const amount = Number(value || 0);
  if (typeof fmtKrw === 'function') return `${fmtKrw(amount, 2)}원`;
  return `${Math.round(amount).toLocaleString('ko-KR')}원`;
}

function _pfHouseholdPortfolioValue() {
  if (typeof PfStore === 'undefined' || !Array.isArray(PfStore.items)) return 0;
  return PfStore.items.reduce((total, item) => {
    const price = typeof quotePriceOrNull === 'function'
      ? quotePriceOrNull(item.quote || {})
      : Number(item?.quote?.price);
    const quantity = Number(item.quantity || 0);
    return Number.isFinite(price) ? total + price * quantity : total;
  }, 0);
}

function pfHouseholdPortfolioValueChanged(value = null) {
  const next = value === null ? _pfHouseholdPortfolioValue() : Number(value || 0);
  if (Math.abs(next - PfHousehold.portfolioValue) < 1) return;
  PfHousehold.portfolioValue = next;
  if (PfHousehold.loaded && typeof pfActiveTab !== 'undefined' && pfActiveTab === 'household') {
    _pfHouseholdRenderInsights();
  }
}

function _pfHouseholdSummaryData() {
  let assets = Math.max(0, Number(PfHousehold.portfolioValue || 0));
  let liabilities = 0;
  let retirementCapital = Number(PfHousehold.portfolioValue || 0);
  const byCategory = { portfolio: Math.max(0, Number(PfHousehold.portfolioValue || 0)) };
  for (const item of PfHousehold.items) {
    const amount = Math.max(0, Number(item.amount || 0));
    const meta = PfHousehold.categories[item.category] || {};
    byCategory[item.category] = (byCategory[item.category] || 0) + amount;
    if (meta.kind === 'liability') {
      liabilities += amount;
      if (item.retirement_eligible) retirementCapital -= amount;
    } else {
      assets += amount;
      if (item.retirement_eligible) retirementCapital += amount;
    }
  }
  return {
    assets,
    liabilities,
    netWorth: assets - liabilities,
    retirementCapital,
    byCategory,
    debtRatio: assets > 0 ? liabilities / assets * 100 : 0,
  };
}

function _pfHouseholdOfficialBracket(value, distribution = PfHousehold.distribution) {
  const points = distribution?.official_percentiles || [];
  if (!points.length) return '비교 자료 없음';
  if (value < points[0].amount) return '하위 10% 구간';
  for (let i = 1; i < points.length; i += 1) {
    if (value < points[i].amount) {
      const upperTop = 100 - points[i].percentile;
      const lowerTop = 100 - points[i - 1].percentile;
      return `상위 ${upperTop}~${lowerTop}% 구간`;
    }
  }
  return '상위 10% 이내';
}

function pfHouseholdEstimatePercentile(value, distribution = PfHousehold.distribution) {
  const amount = Number(value || 0);
  const points = distribution?.official_percentiles || [];
  if (!points.length || amount <= 0) return 0;
  const first = points[0];
  if (amount <= first.amount) return Math.max(0, first.percentile * amount / first.amount);
  const tail = distribution?.estimated_tail;
  if (tail && amount >= Number(tail.threshold_amount)) {
    const thresholdP = Number(tail.threshold_percentile) / 100;
    const shape = Number(tail.shape);
    const scale = Number(tail.scale);
    const excess = amount - Number(tail.threshold_amount);
    if (scale > 0 && shape !== 0) {
      const survival = Math.pow(1 + shape * excess / scale, -1 / shape);
      return Math.max(tail.threshold_percentile, Math.min(99.99, (1 - (1 - thresholdP) * survival) * 100));
    }
  }
  for (let i = 1; i < points.length; i += 1) {
    const left = points[i - 1];
    const right = points[i];
    if (amount <= right.amount) {
      const denominator = Math.log1p(right.amount) - Math.log1p(left.amount);
      const ratio = denominator > 0
        ? (Math.log1p(amount) - Math.log1p(left.amount)) / denominator
        : 0;
      return left.percentile + Math.max(0, Math.min(1, ratio)) * (right.percentile - left.percentile);
    }
  }
  return 90;
}

function _pfHouseholdQuantile(percentile, distribution = PfHousehold.distribution) {
  const p = Math.max(0, Math.min(99.9, Number(percentile || 0)));
  const points = distribution?.official_percentiles || [];
  if (!points.length || p <= 0) return 0;
  if (p <= points[0].percentile) return points[0].amount * p / points[0].percentile;
  const tail = distribution?.estimated_tail;
  if (tail && p >= Number(tail.threshold_percentile)) {
    const thresholdP = Number(tail.threshold_percentile) / 100;
    const targetP = p / 100;
    const survival = (1 - targetP) / (1 - thresholdP);
    const shape = Number(tail.shape);
    const scale = Number(tail.scale);
    const excess = scale / shape * (Math.pow(survival, -shape) - 1);
    return Number(tail.threshold_amount) + excess;
  }
  for (let i = 1; i < points.length; i += 1) {
    const left = points[i - 1];
    const right = points[i];
    if (p <= right.percentile) {
      const ratio = (p - left.percentile) / (right.percentile - left.percentile);
      const logValue = Math.log1p(left.amount) + ratio * (Math.log1p(right.amount) - Math.log1p(left.amount));
      return Math.expm1(logValue);
    }
  }
  return points[points.length - 1].amount;
}

function _pfHouseholdRankText(percentile) {
  if (percentile <= 0) return '순자산 입력 필요';
  const top = Math.max(0.01, 100 - percentile);
  if (top < 0.1) return '추정 상위 0.1% 이내';
  if (top < 1) return `추정 상위 ${top.toFixed(1)}%`;
  return `추정 상위 ${top.toFixed(top < 10 ? 1 : 0)}%`;
}

function _pfHouseholdRenderSummary(summary) {
  const root = document.getElementById('pfHouseholdSummary');
  if (!root) return;
  const percentile = pfHouseholdEstimatePercentile(summary.netWorth);
  root.innerHTML = `
    <article><span>총자산</span><strong>${_pfHouseholdMoney(summary.assets)}</strong><small>포트폴리오 자동 반영</small></article>
    <article><span>총부채</span><strong class="${summary.liabilities > 0 ? 'negative' : ''}">${_pfHouseholdMoney(summary.liabilities)}</strong><small>자산 대비 ${summary.debtRatio.toFixed(1)}%</small></article>
    <article class="pf-hh-summary-primary"><span>가구 순자산</span><strong>${_pfHouseholdMoney(summary.netWorth)}</strong><small>총자산 − 총부채</small></article>
    <article><span>전국 위치</span><strong>${escapeHtml(_pfHouseholdRankText(percentile))}</strong><small>${escapeHtml(_pfHouseholdOfficialBracket(summary.netWorth))} · 가구 기준</small></article>`;
}

function _pfHouseholdRenderWealth(summary) {
  const chart = document.getElementById('pfHouseholdWealthChart');
  const badge = document.getElementById('pfHouseholdRankBadge');
  const thresholds = document.getElementById('pfHouseholdThresholds');
  const distribution = PfHousehold.distribution;
  if (!chart || !badge || !thresholds || !distribution) return;
  const percentile = pfHouseholdEstimatePercentile(summary.netWorth, distribution);
  badge.textContent = _pfHouseholdRankText(percentile);
  badge.dataset.tone = percentile >= 90 ? 'top' : percentile >= 50 ? 'middle' : 'base';

  const width = 760, height = 274, left = 54, right = 734, top = 18, bottom = 220;
  const x = p => left + (right - left) * p / 100;
  const userValue = Math.max(0, summary.netWorth);
  const maxValue = Math.max(_pfHouseholdQuantile(99), userValue);
  const maxLog = Math.log10(maxValue / 1_000_000 + 1);
  const y = value => bottom - (bottom - top) * Math.log10(Math.max(0, value) / 1_000_000 + 1) / Math.max(1, maxLog);
  const curvePoints = [];
  for (let p = 0; p <= 99; p += 1) curvePoints.push([x(p), y(_pfHouseholdQuantile(p))]);
  const linePath = curvePoints.map((point, index) => `${index ? 'L' : 'M'}${point[0].toFixed(1)},${point[1].toFixed(1)}`).join(' ');
  const areaPath = `${linePath} L${x(99).toFixed(1)},${bottom} L${x(0).toFixed(1)},${bottom} Z`;
  const markerP = Math.min(99, Math.max(0, percentile));
  const markerX = x(markerP);
  const markerY = y(userValue);
  const gridValues = [0, distribution.median, distribution.official_percentiles[7].amount, distribution.official_percentiles[8].amount, maxValue];
  const grids = [...new Set(gridValues.map(v => Math.round(v)))].map(value => {
    const gy = y(value);
    return `<line x1="${left}" x2="${right}" y1="${gy}" y2="${gy}" class="pf-hh-chart-grid" />
      <text x="${left - 8}" y="${gy + 4}" text-anchor="end" class="pf-hh-chart-axis">${escapeHtml(value ? _pfHouseholdMoney(value).replace('원', '') : '0')}</text>`;
  }).join('');
  const officialDots = distribution.official_percentiles.map(point =>
    `<circle cx="${x(point.percentile)}" cy="${y(point.amount)}" r="3" class="pf-hh-official-dot"><title>P${point.percentile} 공식 경계 ${_pfHouseholdMoney(point.amount)}</title></circle>`
  ).join('');
  const xTicks = [0, 20, 40, 60, 80, 90, 100].map(p =>
    `<text x="${x(p)}" y="${bottom + 24}" text-anchor="middle" class="pf-hh-chart-axis">P${p}</text>`
  ).join('');
  chart.innerHTML = `<svg viewBox="0 0 ${width} ${height}" role="img" aria-label="전국 가구 순자산 백분위 곡선에서 우리 가구의 위치">
    <defs><linearGradient id="pfHhWealthFill" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="#4f46e5" stop-opacity=".34"/><stop offset="1" stop-color="#4f46e5" stop-opacity=".02"/></linearGradient></defs>
    ${grids}<path d="${areaPath}" fill="url(#pfHhWealthFill)"/><path d="${linePath}" class="pf-hh-wealth-line"/>
    ${officialDots}${xTicks}
    <line x1="${markerX}" x2="${markerX}" y1="${markerY}" y2="${bottom}" class="pf-hh-user-line"/>
    <circle cx="${markerX}" cy="${markerY}" r="6" class="pf-hh-user-dot"><title>내 순자산 ${_pfHouseholdMoney(userValue)}</title></circle>
    <g transform="translate(${Math.min(right - 132, Math.max(left + 4, markerX - 62))},${Math.max(top + 4, markerY - 42)})"><rect width="124" height="30" rx="15" class="pf-hh-user-label-bg"/><text x="62" y="20" text-anchor="middle" class="pf-hh-user-label">내 위치 P${percentile.toFixed(1)}</text></g>
  </svg>`;

  const lookup = p => distribution.official_percentiles.find(row => row.percentile === p)?.amount || 0;
  thresholds.innerHTML = `
    <div><span>중위 가구 P50</span><strong>${_pfHouseholdMoney(lookup(50))}</strong></div>
    <div><span>상위 20% 진입 P80</span><strong>${_pfHouseholdMoney(lookup(80))}</strong></div>
    <div><span>상위 10% 진입 P90</span><strong>${_pfHouseholdMoney(lookup(90))}</strong></div>
    <div><span>조사 기준</span><strong>${escapeHtml(distribution.as_of)} 현재</strong></div>`;
}

function _pfHouseholdRenderMix(summary) {
  const root = document.getElementById('pfHouseholdMix');
  if (!root) return;
  const parts = Object.entries(summary.byCategory)
    .filter(([category, amount]) => category !== 'liability' && amount > 0)
    .map(([category, amount]) => ({
      category,
      amount,
      label: category === 'portfolio' ? '투자 포트폴리오' : (PfHousehold.categories[category]?.label || category),
      color: PF_HH_COLORS[category] || '#94a3b8',
    }))
    .sort((a, b) => b.amount - a.amount);
  let offset = 0;
  const segments = parts.map(part => {
    const start = summary.assets > 0 ? offset / summary.assets * 100 : 0;
    offset += part.amount;
    const end = summary.assets > 0 ? offset / summary.assets * 100 : 0;
    return `${part.color} ${start.toFixed(2)}% ${end.toFixed(2)}%`;
  });
  const legend = parts.length ? parts.map(part => `
    <div class="pf-hh-mix-row"><span class="pf-hh-mix-dot" style="background:${part.color}"></span><span>${escapeHtml(part.label)}</span><strong>${_pfHouseholdMoney(part.amount)}</strong><em>${summary.assets > 0 ? (part.amount / summary.assets * 100).toFixed(1) : '0.0'}%</em></div>`).join('')
    : '<div class="pf-risk-empty">자산을 입력하면 구성이 표시됩니다.</div>';
  root.innerHTML = `<div class="pf-hh-donut-wrap">
      <div class="pf-hh-donut" style="background:${segments.length ? `conic-gradient(${segments.join(',')})` : 'var(--border)'}"><div><strong>${_pfHouseholdMoney(summary.netWorth)}</strong><span>순자산</span></div></div>
      <div class="pf-hh-debt-meter"><span><b>부채비율</b><strong>${summary.debtRatio.toFixed(1)}%</strong></span><div><i style="width:${Math.min(100, summary.debtRatio)}%"></i></div></div>
    </div><div class="pf-hh-mix-list">${legend}</div>`;
}

function _pfHouseholdCategoryOptions(selected) {
  return Object.entries(PfHousehold.categories).map(([key, meta]) =>
    `<option value="${escapeHtml(key)}"${key === selected ? ' selected' : ''}>${escapeHtml(meta.label)}</option>`
  ).join('');
}

function _pfHouseholdOwnerOptions(selected) {
  return Object.entries(PfHousehold.owners).map(([key, label]) =>
    `<option value="${escapeHtml(key)}"${key === selected ? ' selected' : ''}>${escapeHtml(label)}</option>`
  ).join('');
}

function _pfHouseholdRenderAssets() {
  const root = document.getElementById('pfHouseholdAssets');
  if (!root) return;
  const portfolioRow = `<div class="pf-hh-asset-row pf-hh-asset-fixed">
    <span class="pf-hh-asset-type"><i style="background:${PF_HH_COLORS.portfolio}"></i>투자 포트폴리오</span>
    <span>보유종목 자동 합산</span><span>가구 공동</span><strong>${_pfHouseholdMoney(PfHousehold.portfolioValue)}</strong>
    <label class="pf-hh-retire-check"><input type="checkbox" checked disabled> 포함</label><span></span>
  </div>`;
  const rows = PfHousehold.items.map((item, index) => {
    const color = PF_HH_COLORS[item.category] || '#94a3b8';
    return `<div class="pf-hh-asset-row" data-index="${index}">
      <label><span class="pf-hh-mobile-label">분류</span><select aria-label="자산 분류" onchange="pfHouseholdUpdateItem(${index},'category',this.value)">${_pfHouseholdCategoryOptions(item.category)}</select></label>
      <label><span class="pf-hh-mobile-label">이름</span><input aria-label="자산 이름" maxlength="60" value="${escapeHtml(item.name || '')}" oninput="pfHouseholdUpdateItem(${index},'name',this.value)"></label>
      <label><span class="pf-hh-mobile-label">소유</span><select aria-label="소유 구분" onchange="pfHouseholdUpdateItem(${index},'owner',this.value)">${_pfHouseholdOwnerOptions(item.owner)}</select></label>
      <label class="pf-hh-amount-field"><span class="pf-hh-mobile-label">금액</span><i style="background:${color}"></i><input aria-label="자산 금액" inputmode="numeric" value="${Number(item.amount || 0).toLocaleString('ko-KR')}" oninput="pfHouseholdUpdateItem(${index},'amount',this.value)" onblur="pfHouseholdNormalizeAmount(this,${index})"><span>원</span></label>
      <label class="pf-hh-retire-check"><input type="checkbox" ${item.retirement_eligible ? 'checked' : ''} onchange="pfHouseholdUpdateItem(${index},'retirement_eligible',this.checked)"> 노후재원</label>
      <button class="pf-hh-remove-btn" type="button" aria-label="${escapeHtml(item.name || '자산')} 삭제" onclick="pfHouseholdRemoveAsset(${index})">삭제</button>
    </div>`;
  }).join('');
  root.innerHTML = `<div class="pf-hh-asset-head"><span>분류</span><span>자산 이름</span><span>소유</span><span>현재 금액</span><span>노후재원</span><span></span></div>${portfolioRow}${rows || '<div class="pf-hh-assets-empty">아직 추가 자산이 없습니다. 부동산이나 여유자금부터 등록해 보세요.</div>'}`;
}

function _pfHouseholdDefaultItem() {
  return {
    asset_id: `draft-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
    category: 'cash',
    name: '여유자금',
    owner: 'household',
    amount: 0,
    retirement_eligible: true,
  };
}

function _pfHouseholdSetDirty() {
  PfHousehold.dirty = true;
  const status = document.getElementById('pfHouseholdSaveStatus');
  if (status) status.textContent = '저장되지 않은 변경사항';
}

function pfHouseholdAddAsset() {
  PfHousehold.items.push(_pfHouseholdDefaultItem());
  _pfHouseholdSetDirty();
  _pfHouseholdRenderAssets();
  _pfHouseholdRenderInsights();
  const rows = document.querySelectorAll('#pfHouseholdAssets .pf-hh-asset-row:not(.pf-hh-asset-fixed)');
  rows[rows.length - 1]?.querySelector('input[aria-label="자산 이름"]')?.focus();
}

function pfHouseholdRemoveAsset(index) {
  if (!PfHousehold.items[index]) return;
  PfHousehold.items.splice(index, 1);
  _pfHouseholdSetDirty();
  _pfHouseholdRenderAssets();
  _pfHouseholdRenderInsights();
}

function _pfHouseholdParseAmount(value) {
  const parsed = Number(String(value ?? '').replace(/[^0-9.-]/g, ''));
  return Number.isFinite(parsed) ? Math.max(0, Math.round(parsed)) : 0;
}

function pfHouseholdUpdateItem(index, field, value) {
  const item = PfHousehold.items[index];
  if (!item) return;
  if (field === 'amount') item.amount = _pfHouseholdParseAmount(value);
  else if (field === 'retirement_eligible') item.retirement_eligible = Boolean(value);
  else item[field] = value;
  if (field === 'category') {
    item.retirement_eligible = Boolean(PfHousehold.categories[value]?.retirement_default);
    _pfHouseholdRenderAssets();
  }
  _pfHouseholdSetDirty();
  _pfHouseholdRenderInsights();
}

function pfHouseholdNormalizeAmount(input, index) {
  const amount = _pfHouseholdParseAmount(input.value);
  if (PfHousehold.items[index]) PfHousehold.items[index].amount = amount;
  input.value = amount.toLocaleString('ko-KR');
}

function _pfHouseholdFillRetirementForm() {
  const p = PfHousehold.retirement || {};
  const values = {
    pfHrHouseholdType: p.household_type || 'couple',
    pfHrCurrentAge: p.current_age ?? '',
    pfHrRetirementAge: p.retirement_age ?? 65,
    pfHrPlanToAge: p.plan_to_age ?? 90,
    pfHrMonthlySpending: p.monthly_spending ?? 0,
    pfHrPublicPension: p.monthly_public_pension ?? 0,
    pfHrOtherIncome: p.monthly_other_income ?? 0,
    pfHrContribution: p.monthly_contribution ?? 0,
    pfHrReturn: p.annual_return_pct ?? 4,
    pfHrInflation: p.inflation_pct ?? 2,
  };
  Object.entries(values).forEach(([id, value]) => {
    const input = document.getElementById(id);
    if (input) input.value = value;
  });
}

function _pfHouseholdFormNumber(id, fallback = 0, nullable = false) {
  const raw = document.getElementById(id)?.value;
  if ((raw === '' || raw === undefined) && nullable) return null;
  const number = Number(raw);
  return Number.isFinite(number) ? number : fallback;
}

function pfHouseholdRetirementChanged({ markDirty = true } = {}) {
  if (!PfHousehold.retirement) PfHousehold.retirement = {};
  PfHousehold.retirement = {
    household_type: document.getElementById('pfHrHouseholdType')?.value || 'couple',
    current_age: _pfHouseholdFormNumber('pfHrCurrentAge', 0, true),
    retirement_age: _pfHouseholdFormNumber('pfHrRetirementAge', 65),
    plan_to_age: _pfHouseholdFormNumber('pfHrPlanToAge', 90),
    monthly_spending: _pfHouseholdFormNumber('pfHrMonthlySpending', 0),
    monthly_public_pension: _pfHouseholdFormNumber('pfHrPublicPension', 0),
    monthly_other_income: _pfHouseholdFormNumber('pfHrOtherIncome', 0),
    monthly_contribution: _pfHouseholdFormNumber('pfHrContribution', 0),
    annual_return_pct: _pfHouseholdFormNumber('pfHrReturn', 4),
    inflation_pct: _pfHouseholdFormNumber('pfHrInflation', 2),
  };
  if (markDirty) _pfHouseholdSetDirty();
  _pfHouseholdRenderRetirement(_pfHouseholdSummaryData());
}

function pfHouseholdApplyRetirementReference() {
  const type = document.getElementById('pfHrHouseholdType')?.value || 'couple';
  const amount = PfHousehold.retirementReference?.adequate_monthly_spending?.[type];
  const input = document.getElementById('pfHrMonthlySpending');
  if (!amount || !input) return;
  input.value = amount;
  pfHouseholdRetirementChanged();
  if (typeof showToast === 'function') showToast(`최신 적정 노후생활비 ${_pfHouseholdMoney(amount)}을 적용했습니다.`, 'success');
}

function pfHouseholdCalculateRetirement(summary, profile = PfHousehold.retirement) {
  if (!profile || profile.current_age === null || profile.current_age === undefined || profile.current_age === '') return null;
  const currentAge = Number(profile.current_age);
  const retirementAge = Math.max(currentAge, Number(profile.retirement_age || 65));
  const planToAge = Math.max(retirementAge + 1, Number(profile.plan_to_age || 90));
  const yearsToRetirement = Math.max(0, retirementAge - currentAge);
  const retirementYears = Math.max(1, planToAge - retirementAge);
  const nominalReturn = Number(profile.annual_return_pct || 0) / 100;
  const inflation = Number(profile.inflation_pct || 0) / 100;
  const realReturn = Math.max(-0.95, (1 + nominalReturn) / (1 + inflation) - 1);
  const growth = Math.pow(1 + realReturn, yearsToRetirement);
  const accumulationFactor = yearsToRetirement <= 0 ? 0
    : Math.abs(realReturn) < 1e-9 ? yearsToRetirement
      : (growth - 1) / realReturn;
  const currentCapital = Number(summary.retirementCapital || 0);
  const projectedCapital = currentCapital * growth
    + Math.max(0, Number(profile.monthly_contribution || 0)) * 12 * accumulationFactor;
  const monthlyIncome = Math.max(0, Number(profile.monthly_public_pension || 0))
    + Math.max(0, Number(profile.monthly_other_income || 0));
  const monthlyGap = Math.max(0, Number(profile.monthly_spending || 0) - monthlyIncome);
  const annuityFactor = Math.abs(realReturn) < 1e-9
    ? retirementYears
    : (1 - Math.pow(1 + realReturn, -retirementYears)) / realReturn;
  const requiredCapital = monthlyGap * 12 * annuityFactor;
  const fundingGap = Math.max(0, requiredCapital - projectedCapital);
  const coveragePct = requiredCapital <= 0 ? 999 : Math.max(0, projectedCapital / requiredCapital * 100);
  const additionalMonthly = fundingGap <= 0 ? 0
    : yearsToRetirement > 0 && accumulationFactor > 0 ? fundingGap / accumulationFactor / 12
      : fundingGap / 12;
  return {
    currentAge, retirementAge, planToAge, yearsToRetirement, retirementYears,
    realReturn, currentCapital, projectedCapital, monthlyIncome, monthlyGap,
    requiredCapital, fundingGap, coveragePct, additionalMonthly,
  };
}

function _pfHouseholdRetirementTone(coverage) {
  if (coverage >= 120) return { tone: 'strong', label: '충분' };
  if (coverage >= 100) return { tone: 'ready', label: '목표 도달' };
  if (coverage >= 80) return { tone: 'watch', label: '보완 필요' };
  return { tone: 'danger', label: '준비 부족' };
}

function _pfHouseholdRenderRetirement(summary) {
  const root = document.getElementById('pfHouseholdRetirementResult');
  if (!root) return;
  const result = pfHouseholdCalculateRetirement(summary);
  if (!result) {
    root.innerHTML = `<div class="pf-hh-retirement-empty"><span>01</span><h4>현재 나이를 입력해 주세요</h4><p>노후재원으로 표시한 자산과 은퇴까지의 저축 기간을 반영해 준비율을 계산합니다.</p><div><strong>현재 노후재원</strong><b>${_pfHouseholdMoney(summary.retirementCapital)}</b></div></div>`;
    return;
  }
  const status = _pfHouseholdRetirementTone(result.coveragePct);
  const coverageDisplay = result.coveragePct >= 999 ? '충분' : `${result.coveragePct.toFixed(0)}%`;
  const progress = Math.min(100, result.coveragePct);
  const advice = result.fundingGap <= 0
    ? `현재 가정대로라면 목표자금보다 ${_pfHouseholdMoney(Math.max(0, result.projectedCapital - result.requiredCapital))} 여유가 있습니다.`
    : result.yearsToRetirement > 0
      ? `부족분을 메우려면 은퇴 전까지 월 ${_pfHouseholdMoney(result.additionalMonthly)}의 추가 준비가 필요합니다.`
      : `현재 기준 목표자금보다 ${_pfHouseholdMoney(result.fundingGap)} 부족합니다.`;
  root.innerHTML = `<div class="pf-hh-readiness pf-hh-readiness-${status.tone}">
      <div class="pf-hh-readiness-score"><span>${escapeHtml(status.label)}</span><strong>${coverageDisplay}</strong><small>노후자금 준비율</small></div>
      <div class="pf-hh-readiness-main">
        <div class="pf-hh-progress"><i style="width:${progress}%"></i></div>
        <p>${escapeHtml(advice)}</p>
        <div class="pf-hh-retire-stats">
          <div><span>은퇴 시점 예상자금</span><strong>${_pfHouseholdMoney(result.projectedCapital)}</strong></div>
          <div><span>필요 노후자금</span><strong>${_pfHouseholdMoney(result.requiredCapital)}</strong></div>
          <div><span>월 생활비 부족분</span><strong>${_pfHouseholdMoney(result.monthlyGap)}</strong></div>
          <div><span>실질 기대수익률</span><strong>${(result.realReturn * 100).toFixed(1)}%</strong></div>
        </div>
        <div class="pf-hh-retire-timeline"><span>현재 ${result.currentAge}세</span><i></i><span>은퇴 ${result.retirementAge}세</span><i></i><span>계획 ${result.planToAge}세</span></div>
      </div>
    </div>`;
}

function _pfHouseholdRenderInsights() {
  if (!PfHousehold.loaded) return;
  const summary = _pfHouseholdSummaryData();
  _pfHouseholdRenderSummary(summary);
  _pfHouseholdRenderWealth(summary);
  _pfHouseholdRenderMix(summary);
  _pfHouseholdRenderRetirement(summary);
}

function _pfHouseholdApplyPayload(data) {
  PfHousehold.items = Array.isArray(data.items) ? data.items.map(item => ({ ...item })) : [];
  PfHousehold.retirement = { ...(data.retirement || {}) };
  PfHousehold.categories = data.categories || {};
  PfHousehold.owners = data.owners || {};
  PfHousehold.distribution = data.distribution || null;
  PfHousehold.retirementReference = data.retirement_reference || null;
  PfHousehold.updatedAt = data.updated_at || null;
  PfHousehold.portfolioValue = _pfHouseholdPortfolioValue();
  PfHousehold.loaded = true;
  PfHousehold.dirty = false;

  const wealthLink = document.getElementById('pfHouseholdWealthSource');
  if (wealthLink && PfHousehold.distribution?.source_url) wealthLink.href = PfHousehold.distribution.source_url;
  const retirementLink = document.getElementById('pfHouseholdRetirementSource');
  if (retirementLink && PfHousehold.retirementReference?.source_url) retirementLink.href = PfHousehold.retirementReference.source_url;
  _pfHouseholdFillRetirementForm();
  _pfHouseholdRenderAssets();
  _pfHouseholdRenderInsights();
}

async function pfLoadHouseholdAssets({ force = false } = {}) {
  if (PfHousehold.loading || (PfHousehold.loaded && !force)) {
    if (PfHousehold.loaded) pfHouseholdPortfolioValueChanged();
    return;
  }
  const state = document.getElementById('pfHouseholdLoadState');
  const content = document.getElementById('pfHouseholdContent');
  PfHousehold.loading = true;
  if (state) {
    state.style.display = 'block';
    state.dataset.state = 'loading';
    state.textContent = '가계 자산 정보를 불러오는 중입니다.';
  }
  if (content) content.style.display = 'none';
  try {
    const data = await apiFetchJson('/api/household-assets', { errorMessage: '가계 자산 정보를 불러오지 못했습니다.' });
    _pfHouseholdApplyPayload(data);
    if (state) state.style.display = 'none';
    if (content) content.style.display = 'block';
    const status = document.getElementById('pfHouseholdSaveStatus');
    if (status) status.textContent = data.updated_at ? '저장된 정보를 불러왔습니다.' : '새 자산계획';
  } catch (error) {
    if (state) {
      state.dataset.state = 'error';
      state.textContent = error?.status === 401
        ? '로그인 후 가계 자산 통합관리를 사용할 수 있습니다.'
        : '가계 자산 정보를 불러오지 못했습니다. 잠시 후 다시 시도해 주세요.';
    }
    if (typeof reportApiError === 'function') reportApiError(error, '가계 자산', { silent: true });
  } finally {
    PfHousehold.loading = false;
  }
}

async function pfSaveHouseholdAssets() {
  if (PfHousehold.saving || !PfHousehold.loaded) return;
  pfHouseholdRetirementChanged({ markDirty: false });
  const invalid = PfHousehold.items.find(item => !String(item.name || '').trim());
  if (invalid) {
    if (typeof showToast === 'function') showToast('모든 자산의 이름을 입력해 주세요.', 'warning');
    return;
  }
  const button = document.getElementById('pfHouseholdSaveBtn');
  const status = document.getElementById('pfHouseholdSaveStatus');
  PfHousehold.saving = true;
  if (button) { button.disabled = true; button.textContent = '저장 중…'; }
  if (status) status.textContent = '안전하게 저장하는 중';
  try {
    const data = await apiFetchJson('/api/household-assets', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ items: PfHousehold.items, retirement: PfHousehold.retirement }),
      errorMessage: '가계 자산 저장에 실패했습니다.',
    });
    _pfHouseholdApplyPayload(data);
    if (status) status.textContent = '방금 저장됨';
    if (typeof showToast === 'function') showToast('가계 자산과 노후계획을 저장했습니다.', 'success');
  } catch (error) {
    if (status) status.textContent = '저장 실패';
    if (typeof reportApiError === 'function') reportApiError(error, '가계 자산 저장');
  } finally {
    PfHousehold.saving = false;
    if (button) { button.disabled = false; button.textContent = '전체 저장'; }
  }
}

window.addEventListener('beforeunload', event => {
  if (!PfHousehold.dirty) return;
  event.preventDefault();
  event.returnValue = '';
});
