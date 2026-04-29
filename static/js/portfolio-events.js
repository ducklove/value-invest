// Portfolio delegated DOM event handlers.
// Split from static/js/portfolio.js to keep portfolio features maintainable.
// --- Delegated event handlers ---------------------------------------------
// All row-level actions used to live in inline `onclick="fn('${code}')"`,
// which interpolates user-controlled strings (stock codes/names) into a JS
// string context. escapeHtml is not safe there — `'` becomes `&#39;` which
// the HTML parser decodes back to `'` before the JS evaluates, allowing
// break-out. We replace those with CSS classes + data attributes and a
// single document-level delegated handler, which also removes the per-render
// listener churn and prevents accumulated `document.click` listeners from
// menu/picker code.
(function initPfDelegation() {
  const onReady = () => {
    document.addEventListener('pointerdown', (e) => {
      if (e.target.closest && e.target.closest('.js-pf-analyze')) {
        _pfMarkPointerInteraction();
      }
    }, true);
    document.addEventListener('pointerup', (e) => {
      if (e.target.closest && e.target.closest('.js-pf-analyze')) {
        _pfMarkPointerInteraction(300);
      }
    }, true);
    document.addEventListener('click', (e) => {
      const t = e.target;
      const codeFromTr = (el) => {
        const host = el.closest('[data-code]');
        return host ? host.dataset.code : null;
      };
      let el;
      if (t.closest('.js-pf-row-drag')) {
        e.preventDefault();
        return;
      } else if ((el = t.closest('.js-pf-analyze'))) {
        const code = codeFromTr(el);
        if (code) { e.preventDefault(); pfGoAnalyze(code, e); }
      } else if ((el = t.closest('.js-pf-save'))) {
        const code = codeFromTr(el);
        if (code) savePortfolioEdit(code);
      } else if (t.closest('.js-pf-cancel')) {
        cancelPortfolioEdit();
      } else if ((el = t.closest('.js-pf-edit'))) {
        const code = codeFromTr(el);
        if (code) startPortfolioEdit(code);
      } else if ((el = t.closest('.js-pf-delete'))) {
        const code = codeFromTr(el);
        if (code) deletePortfolioItem(code);
      } else if ((el = t.closest('.js-pf-bench-picker'))) {
        const code = codeFromTr(el);
        if (code) pfShowBenchmarkPicker(code, el);
      } else if ((el = t.closest('.js-pf-bench-set'))) {
        const code = codeFromTr(el);
        if (code) pfSetBenchmark(code, el.dataset.bench || '');
      } else if ((el = t.closest('.js-pf-gold-gap'))) {
        const asset = el.dataset.gapAsset || _goldGapInfoForCode(codeFromTr(el)).asset;
        if (asset) _openGoldGapDashboard(asset);
      } else if ((el = t.closest('.js-pf-insight-action'))) {
        e.preventDefault();
        const action = pfAssetInsightActions[Number(el.dataset.actionIdx)];
        if (action) action.run();
      } else if ((el = t.closest('.js-pf-tag-add'))) {
        e.preventDefault();
        const panel = el.closest('.pf-insight-tag-panel');
        const input = panel ? panel.querySelector('.pf-insight-tag-input') : null;
        const code = el.dataset.code || panel?.dataset.code || pfAssetInsightCode;
        pfAddAssetTag(code, input ? input.value : '').catch(err => showToast(err.message));
      } else if ((el = t.closest('.js-pf-tag-remove'))) {
        e.preventDefault();
        pfRemoveAssetTag(el.dataset.code || pfAssetInsightCode, el.dataset.tag || '').catch(err => showToast(err.message));
      } else if ((el = t.closest('.js-pf-tag-suggest'))) {
        e.preventDefault();
        pfAddAssetTag(el.dataset.code || pfAssetInsightCode, el.dataset.tag || '').catch(err => showToast(err.message));
      } else if ((el = t.closest('.js-pf-cf-delete'))) {
        const id = Number(el.dataset.cfId);
        if (!isNaN(id)) deleteCashflow(id);
      } else if ((el = t.closest('.js-pf-nav-zoom'))) {
        const days = Number(el.dataset.zoomDays);
        if (!isNaN(days)) _navZoomToDays(days);
      } else if ((el = t.closest('.js-pf-value-zoom'))) {
        const days = Number(el.dataset.zoomDays);
        if (!isNaN(days)) _valueZoomToDays(days);
      } else if ((el = t.closest('.js-pf-target-clear'))) {
        const code = codeFromTr(el);
        if (code) {
          e.preventDefault();
          e.stopPropagation();
          clearPortfolioTargetPrice(code);
        }
      }
    });
    document.addEventListener('change', (e) => {
      const t = e.target;
      let el;
      if ((el = t.closest('.js-pf-group'))) {
        const host = el.closest('[data-code]');
        if (host) pfChangeGroup(host.dataset.code, el.value);
      } else if ((el = t.closest('.js-pf-col-toggle'))) {
        pfToggleCol(el.dataset.colKey, el.checked);
      }
    });
    document.addEventListener('keydown', (e) => {
      const el = e.target.closest && e.target.closest('.pf-insight-tag-input');
      if (!el || e.key !== 'Enter') return;
      e.preventDefault();
      const panel = el.closest('.pf-insight-tag-panel');
      const code = panel?.dataset.code || pfAssetInsightCode;
      pfAddAssetTag(code, el.value).catch(err => showToast(err.message));
    });
  };
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', onReady);
  } else {
    onReady();
  }
})();

window.__VALUE_INVEST_PORTFOLIO_SPLIT_LOADED__ = true;
