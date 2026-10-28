import {test, expect} from '@playwright/test';
import {readFileSync} from 'node:fs';

const script = readFileSync(new URL('../../static/js/stock-hover-chart.js', import.meta.url), 'utf8');
const styles = readFileSync(new URL('../../static/css/base.css', import.meta.url), 'utf8');

test('시세 갱신으로 종목 행을 교체해도 마우스 아래 일중 미리보기가 유지된다', async ({page}) => {
  const liveBase = process.env.HOVER_VERIFY_BASE_URL;
  let source = script;
  let css = styles;
  let intraday = {supported: true, currency: 'KRW', prevClose: 100,
    points: [{t: '09:00', p: 101}, {t: '09:01', p: 102}]};
  if (liveBase) {
    for (const [path, assign] of [
      ['/js/stock-hover-chart.js', text => {source = text;}],
      ['/css/base.css', text => {css = text;}],
      ['/api/stocks/000660/intraday', text => {intraday = JSON.parse(text);}],
    ]) {
      const response = await page.request.get(liveBase + path);
      expect(response.ok()).toBe(true);
      assign(await response.text());
    }
    expect(intraday.supported).toBe(true);
    expect(intraday.points.length).toBeGreaterThan(1);
  }
  await page.setContent(`<table><tbody id="pfBody"><tr data-code="000660">
    <td class="pf-stock-cell"><a class="pf-stock-link"><strong>SK하이닉스</strong></a></td>
  </tr></tbody></table><button>표 밖</button>`);
  await page.addStyleTag({content: css});
  await page.evaluate(data => {
    window.cssToken = (_name, fallback) => fallback;
    window.escapeHtml = String;
    window.apiFetchJson = async () => data;
  }, intraday);
  await page.addScriptTag({content: source});
  await page.locator('.pf-stock-link').hover();
  const tip = page.locator('#stockHoverTip');
  await expect(tip).toBeVisible();
  await page.evaluate(() => {
    window.hoverHiddenCount = 0;
    window.hoverCanvas = document.querySelector('#stockHoverTip canvas');
    new MutationObserver(records => {
      for (const record of records) {
        if (record.attributeName === 'class' && !record.target.classList.contains('visible')) window.hoverHiddenCount++;
      }
    }).observe(document.getElementById('stockHoverTip'), {attributes: true});
    let paints = 0;
    window.repaintTimer = setInterval(() => {
      const body = document.getElementById('pfBody');
      body.innerHTML = body.innerHTML;
      if (++paints === 8) clearInterval(window.repaintTimer);
      window.hoverPaints = paints;
    }, 100);
  });
  await expect.poll(() => page.evaluate(() => window.hoverPaints)).toBe(8);
  expect(await page.evaluate(() => window.hoverHiddenCount)).toBe(0);
  expect(await page.evaluate(() => window.hoverCanvas === document.querySelector('#stockHoverTip canvas'))).toBe(true);
  await expect(tip).toBeVisible();
  await page.getByRole('button', {name: '표 밖'}).hover();
  await expect(tip).toBeHidden();
});
