import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { JSDOM } from 'jsdom';

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, '..', '..');
const householdSrc = readFileSync(join(root, 'static', 'js', 'portfolio-household.js'), 'utf8');
const performanceSrc = readFileSync(join(root, 'static', 'js', 'portfolio-performance.js'), 'utf8');
const indexSrc = readFileSync(join(root, 'static', 'index.html'), 'utf8');

const distribution = {
  official_percentiles: [
    [10, 12_100_000], [20, 51_080_000], [30, 102_960_000],
    [40, 164_720_000], [50, 238_600_000], [60, 330_500_000],
    [70, 461_800_000], [80, 693_800_000], [90, 1_100_200_000],
  ].map(([percentile, amount]) => ({ percentile, amount })),
  estimated_tail: {
    threshold_percentile: 80,
    threshold_amount: 693_800_000,
    shape: 0.3804559967071106,
    scale: 512_396_544.4154327,
  },
};

function appendScript(window, source) {
  const script = window.document.createElement('script');
  script.textContent = source;
  window.document.body.appendChild(script);
}

function householdWindow() {
  const dom = new JSDOM('<!doctype html><html><body></body></html>', {
    runScripts: 'dangerously',
    url: 'https://app.example.com/portfolio',
  });
  dom.window.escapeHtml = value => String(value);
  appendScript(dom.window, householdSrc);
  return dom.window;
}

test('official percentile points and the estimated upper tail round-trip', () => {
  const w = householdWindow();
  assert.ok(Math.abs(w.pfHouseholdEstimatePercentile(238_600_000, distribution) - 50) < 0.001);
  assert.ok(Math.abs(w.pfHouseholdEstimatePercentile(1_100_200_000, distribution) - 90) < 0.001);

  const p95Amount = w._pfHouseholdQuantile(95, distribution);
  assert.ok(p95Amount > 1_620_000_000 && p95Amount < 1_640_000_000);
  assert.ok(Math.abs(w.pfHouseholdEstimatePercentile(p95Amount, distribution) - 95) < 0.001);
  assert.equal(w._pfHouseholdOfficialBracket(800_000_000, distribution), '상위 10~20% 구간');
  assert.equal(w._pfHouseholdOfficialBracket(1_200_000_000, distribution), '상위 10% 이내');
});

test('top-one-percent milestones expose the upper tail through top 0.01%', () => {
  const w = householdWindow();
  const milestones = w._pfHouseholdTopMilestones(distribution);

  assert.deepEqual(Array.from(milestones, item => item.topShare), [1, 0.5, 0.1, 0.05, 0.01]);
  assert.ok(milestones[0].amount > 3_550_000_000 && milestones[0].amount < 3_570_000_000);
  assert.ok(milestones[2].amount > 9_440_000_000 && milestones[2].amount < 9_480_000_000);
  assert.ok(milestones[4].amount > 23_600_000_000 && milestones[4].amount < 23_650_000_000);
  for (const item of milestones) {
    assert.ok(Math.abs(w.pfHouseholdEstimatePercentile(item.amount, distribution) - item.percentile) < 0.001);
  }
  assert.equal(w._pfHouseholdRankText(99.9), '추정 상위 0.1%');
  assert.equal(w._pfHouseholdRankText(99.95), '추정 상위 0.05%');
});

test('retirement money inputs format thousands separators and still parse numerically', () => {
  const w = householdWindow();
  const input = w.document.createElement('input');
  input.id = 'pfHrMonthlySpending';
  input.value = '2981000';
  w.document.body.appendChild(input);
  input.setSelectionRange(7, 7);

  w.pfHouseholdMoneyInputChanged(input);

  assert.equal(input.value, '2,981,000');
  assert.equal(w._pfHouseholdFormNumber('pfHrMonthlySpending'), 2_981_000);
  assert.match(indexSrc, /id="pfHrMonthlySpending"[^>]+type="text"[^>]+inputmode="numeric"/);
  assert.match(indexSrc, /id="pfHrPublicPension"[^>]+type="text"[^>]+inputmode="numeric"/);
});

test('retirement calculator improves when monthly contributions are added', () => {
  const w = householdWindow();
  const summary = { retirementCapital: 300_000_000 };
  const base = {
    household_type: 'couple', current_age: 45, retirement_age: 65, plan_to_age: 90,
    monthly_spending: 2_981_000, monthly_public_pension: 1_200_000,
    monthly_other_income: 0, monthly_contribution: 0,
    annual_return_pct: 4, inflation_pct: 2,
  };
  const withoutSavings = w.pfHouseholdCalculateRetirement(summary, base);
  const withSavings = w.pfHouseholdCalculateRetirement(summary, { ...base, monthly_contribution: 1_000_000 });

  assert.equal(withoutSavings.yearsToRetirement, 20);
  assert.equal(withoutSavings.retirementYears, 25);
  assert.ok(withoutSavings.requiredCapital > 0);
  assert.ok(withSavings.projectedCapital > withoutSavings.projectedCapital);
  assert.ok(withSavings.coveragePct > withoutSavings.coveragePct);
  assert.equal(w.pfHouseholdCalculateRetirement(summary, { ...base, current_age: null }), null);
});

test('portfolio tab switch exposes household view and lazy-loads it', () => {
  const dom = new JSDOM(`<!doctype html><html><body>
    <div id="portfolioView">
      <button class="pf-tab active" data-tab="holdings"></button>
      <button class="pf-tab" data-tab="performance"></button>
      <button class="pf-tab" data-tab="household"></button>
      <div id="pfHoldingsTab"></div><div id="pfPerformanceTab"></div><div id="pfHouseholdTab"></div>
    </div>
  </body></html>`, { runScripts: 'dangerously', url: 'https://app.example.com/portfolio' });
  const { window: w } = dom;
  appendScript(w, performanceSrc);
  let loads = 0;
  w.pfLoadHouseholdAssets = () => { loads += 1; };

  w.pfSwitchTab('household');

  assert.equal(loads, 1);
  assert.equal(w.document.getElementById('pfHouseholdTab').style.display, '');
  assert.equal(w.document.getElementById('pfHoldingsTab').style.display, 'none');
  assert.ok(w.document.getElementById('portfolioView').classList.contains('pf-household-active'));
  assert.equal(w.document.querySelector('[data-tab="household"]').getAttribute('aria-selected'), 'true');
});
