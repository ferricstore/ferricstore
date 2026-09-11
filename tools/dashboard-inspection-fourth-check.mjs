import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import Module from 'node:module';

if (process.env.NODE_PATH) Module._initPaths();
const {chromium} = Module.createRequire(import.meta.url)('playwright');
const base = process.env.DASHBOARD_URL || 'http://localhost:4010';
const out = process.env.DASHBOARD_OUT_DIR || 'outputs/dashboard-fourth-fixes/inspection';
await fs.mkdir(out, {recursive: true});
const browser = await chromium.launch({channel: 'chrome', headless: true});
const results = [];
async function check(name, run) {
  try { await run(); results.push({name, pass: true}); }
  catch (error) { results.push({name, pass: false, error: error.message}); }
}

try {
  const context = await browser.newContext({viewport: {width: 1280, height: 900}});
  const page = await context.newPage();
  const errors = [];
  page.on('pageerror', error => errors.push(error.message));
  await check('long missing keys stay inside the desktop page', async () => {
    await page.goto(`${base}/dashboard/keyspace?key=${encodeURIComponent('review-missing-' + 'a'.repeat(320))}`);
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), true);
    assert.match(await page.locator('.kv-inspector').innerText(), /review-missing-/);
    await page.screenshot({path: `${out}/long-key.png`, fullPage: true});
  });
  await check('scanned and visible key counts are distinct', async () => {
    await page.goto(`${base}/dashboard/keyspace?mode=prefix&prefix=&limit=50`);
    assert.match(await page.locator('main').innerText(), /keys returned/);
    assert.match(await page.locator('main').innerText(), /entries scanned/);
    assert.match(await page.locator('main').innerText(), /Compound metadata is excluded/);
    assert.match(await page.locator('main').innerText(), /Protected workflow and server records remain hidden/);
  });
  await check('keyboard table focus uses the shared visible accent', async () => {
    await page.goto(`${base}/dashboard/security`);
    const region = page.getByRole('region', {name: 'ACL account list'});
    await page.keyboard.press('Tab');
    await region.focus();
    assert.equal(await region.evaluate(el => getComputedStyle(el).outlineColor), 'rgb(132, 222, 202)');
    assert.equal(await region.evaluate(el => getComputedStyle(el).outlineStyle), 'solid');
    await page.screenshot({path: `${out}/table-focus.png`});
  });
  await check('signal identity is visible without opening the journal inspector', async () => {
    await page.goto(`${base}/dashboard/flow/detail-browser-history?partition_key=review-detail`);
    const signal = page.locator('.journal-step-trigger .journal-signal-name');
    assert.equal(await signal.innerText(), 'historical-review-signal');
    assert.equal(await signal.isVisible(), true);
  });
  await check('metadata overflow opens with the keyboard and performs no request', async () => {
    await page.goto(`${base}/dashboard/flow/metadata-fourth-review?partition_key=review-detail#workflow-data`);
    const details = page.locator('.flow-metadata-overflow');
    const requests = [];
    page.on('request', request => requests.push(request.url()));
    const summary = details.locator('summary');
    await summary.focus();
    await page.keyboard.press('Enter');
    assert.match(await details.innerText(), /z_match.risk=high/);
    assert.equal(await details.locator('.flow-metadata-entries').isVisible(), true);
    assert.equal(requests.filter(url => /\/values?\b|flow\/query|state_meta/.test(url)).length, 0);
    await page.screenshot({path: `${out}/metadata-expanded.png`});
  });
  for (const width of [1280, 1440, 1920]) {
    await check(`States identifiers and containment at ${width}px`, async () => {
      await page.setViewportSize({width, height: 1000});
      await page.goto(`${base}/dashboard/flow/states`);
      assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), true);
      await page.screenshot({path: `${out}/states-${width}.png`, fullPage: true});
      const measuredWidth = await page.locator('.flow-states-table th:nth-child(2)').evaluate(el => el.getBoundingClientRect().width);
      assert.ok(measuredWidth >= 239, `Runtime status column measured ${measuredWidth}px`);
      const clippedHeaders = await page.locator('.flow-states-table th').evaluateAll(headers =>
        headers.filter(el => el.scrollWidth > el.clientWidth + 1).map(el => el.textContent.trim()));
      assert.deepEqual(clippedHeaders, [], 'State headers must remain fully readable');
    });
  }
  await check('no-JavaScript custom dates apply in one submission', async () => {
    const nojs = await browser.newContext({javaScriptEnabled: false, viewport: {width: 1440, height: 1000}});
    try {
      const p = await nojs.newPage();
      await p.goto(`${base}/dashboard/flow/states`);
      await p.locator('[name=time_mode]').selectOption('custom');
      await p.locator('[name=from]').fill('2026-09-01T00:00');
      await p.locator('[name=to]').fill('2026-10-01T00:00');
      await p.locator('.flow-state-filter-form button[type=submit]').click();
      const url = new URL(p.url());
      assert.equal(url.searchParams.get('time_mode'), 'custom');
      assert.equal(url.searchParams.get('from'), '2026-09-01T00:00');
      assert.equal(await p.locator('[name=from]').inputValue(), '2026-09-01T00:00');
      assert.equal(await p.evaluate(() => document.documentElement.scrollWidth <= innerWidth), true);
      const from = await p.locator('[name=from]').boundingBox();
      const to = await p.locator('[name=to]').boundingBox();
      assert.ok(Math.abs(from.y - to.y) < 1, 'Custom time bounds share a row');
      assert.ok(Math.min(from.width, to.width) >= 260, 'Both timestamps have room for complete values');
      await p.screenshot({path: `${out}/nojs-custom.png`});
    } finally { await nojs.close(); }
  });
  assert.deepEqual(errors, []);
  await context.close();
} finally {
  await browser.close();
  await fs.writeFile(`${out}/results.json`, JSON.stringify(results, null, 2));
}
console.log(JSON.stringify(results, null, 2));
if (results.some(result => !result.pass)) process.exitCode = 1;
