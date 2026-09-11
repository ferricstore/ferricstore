import Module from 'node:module';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
if (process.env.NODE_PATH) Module._initPaths();
const {chromium} = Module.createRequire(import.meta.url)('playwright');
const base = process.env.DASHBOARD_URL || 'http://localhost:4000';
const out = process.env.DASHBOARD_OUT_DIR || 'test-results/dashboard-shell-served';
await fs.mkdir(out, {recursive: true});
const browser = await chromium.launch({channel: 'chrome', headless: true});
const results = [];
async function check(name, run) {
  const context = await browser.newContext({viewport: {width: 1440, height: 1000}});
  const page = await context.newPage();
  page.setDefaultTimeout(6000);
  const errors = [];
  page.on('pageerror', error => errors.push(error.message));
  try {
    const evidence = await run(page);
    assert.deepEqual(errors, []);
    await page.screenshot({path: `${out}/${name}.png`, fullPage: true});
    results.push({name, status: 'passed', evidence});
    console.log(`PASS ${name}`);
  } catch (error) {
    results.push({name, status: 'failed', error: error.stack, errors});
    await page.screenshot({path: `${out}/${name}-failed.png`, fullPage: true});
    console.error(`FAIL ${name}: ${error.message}`);
  } finally {
    await fs.writeFile(`${out}/results.json`, JSON.stringify(results, null, 2));
    await page.unrouteAll({behavior: 'ignoreErrors'});
    await context.close();
  }
}
try {
  await check('served-assets-and-skip-focus', async page => {
    await page.goto(base + '/dashboard');
    await page.locator('[data-dashboard-live-status="live"]').waitFor();
    assert.equal(await page.evaluate(() => typeof window.dashboardCopyText), 'function');
    const resources = await page.evaluate(() => performance.getEntriesByType('resource').filter(r => r.name.includes('/dashboard/assets/')).map(r => ({url: r.name, bytes: r.decodedBodySize})));
    assert.equal(resources.filter(r => r.bytes > 0).length, 2);
    await page.keyboard.press('Tab');
    assert.equal(await page.locator('.dashboard-skip-link').evaluate(n => n === document.activeElement), true);
    await page.keyboard.press('Enter');
    assert.equal(await page.locator('main').evaluate(n => n === document.activeElement), true);
    for (const width of [1280, 1440, 1920]) {
      await page.setViewportSize({width, height: 1000});
      assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), true);
      await page.screenshot({path: `${out}/overview-${width}.png`});
    }
    return resources;
  });
  await check('served-metric-tooltip-zoom', async page => {
    await page.goto(base + '/dashboard/flow/states');
    const icon = page.locator('th .info-icon').first();
    await icon.focus();
    const tooltip = page.locator('[data-dashboard-tooltip]');
    await tooltip.waitFor({state: 'visible'});
    assert.ok((await tooltip.innerText()).length > 20);
    assert.ok((await icon.locator('xpath=..').getAttribute('aria-label')).length < 30);
    const inspect = () => tooltip.evaluate(n => {
      const r = n.getBoundingClientRect();
      return {left:r.left, top:r.top, right:r.right, bottom:r.bottom, width:innerWidth, height:innerHeight,
        topmost:n.contains(document.elementFromPoint(r.x + r.width / 2, r.y + r.height / 2))};
    });
    let geometry = await inspect();
    assert.ok(geometry.top >= 0 && geometry.bottom <= geometry.height && geometry.right <= geometry.width && geometry.topmost);
    await page.screenshot({path: `${out}/metric-tooltip.png`});
    await page.evaluate(() => { document.documentElement.style.zoom = '2'; window.dispatchEvent(new Event('resize')); });
    await icon.scrollIntoViewIfNeeded();
    await icon.focus();
    await page.waitForTimeout(100);
    geometry = await inspect();
    assert.ok(geometry.top >= 0 && geometry.bottom <= geometry.height && geometry.right <= geometry.width && geometry.topmost, JSON.stringify(geometry));
    await page.screenshot({path: `${out}/metric-tooltip-200-percent.png`});
    await page.keyboard.press('Escape');
    assert.equal(await tooltip.isVisible(), false);
    return geometry;
  });
  await check('served-table-focus-pause-resume', async page => {
    let requests = 0;
    page.on('request', request => { if (request.url().includes('/dashboard/api/keyspace')) requests += 1; });
    await page.goto(base + '/dashboard/keyspace');
    await page.locator('[data-dashboard-live-status="live"]').waitFor();
    const table = page.locator('.table-scroll').first();
    await table.focus();
    const before = requests;
    const interval = Number(await page.locator('body').getAttribute('data-dashboard-live-interval-ms'));
    await page.waitForTimeout(interval + 500);
    assert.ok(requests > before);
    assert.equal(await page.locator('[data-dashboard-live-status]').getAttribute('data-dashboard-live-status'), 'live');
    await page.getByRole('button', {name:'Pause live updates', exact:true}).click();
    const paused = requests;
    await page.waitForTimeout(interval + 500);
    assert.equal(requests, paused);
    assert.match(await page.locator('[data-dashboard-live-age]').innerText(), /Updated/);
    await page.getByRole('button', {name:'Resume live updates', exact:true}).click();
    await page.locator('[data-dashboard-live-status="live"]').waitFor();
    assert.ok(requests > paused);
    return {requests};
  });
  await check('served-hung-request-stale-recovery', async page => {
    await page.clock.install();
    let hold = false;
    let held = 0;
    await page.route('**/dashboard/api/overview', async route => {
      if (hold) { held += 1; return; }
      await route.continue();
    });
    await page.goto(base + '/dashboard');
    await page.locator('[data-dashboard-live-status="live"]').waitFor();
    hold = true;
    await page.clock.runFor(2000);
    await page.waitForFunction(() => document.querySelector('[data-dashboard-live-age]').textContent.includes('Updated'));
    await page.clock.runFor(17000);
    await page.locator('[data-dashboard-live-status="stale"]').waitFor();
    assert.ok(held > 0);
    assert.match(await page.locator('[data-dashboard-live-age]').innerText(), /ago/);
    assert.equal(await page.locator('[data-dashboard-live-retry]').isEnabled(), true);
    hold = false;
    await page.locator('[data-dashboard-live-retry]').click();
    await page.clock.runFor(10);
    await page.locator('[data-dashboard-live-status="live"]').waitFor();
    return {held};
  });
  await check('served-stale-action-contract', async page => {
    let changed = false;
    await page.route('**/dashboard/api/flow/nightly-audit-0088?*', async route => {
      const response = await route.fetch({timeout: 5000});
      const payload = await response.json();
      if (changed) payload.action_snapshot = {...payload.action_snapshot, version: 999, available: true};
      await route.fulfill({response, json: payload});
    });
    await page.goto(base + '/dashboard/flow/nightly-audit-0088?partition_key=system');
    await page.locator('[data-dashboard-live-status="live"]').waitFor();
    const panel = page.locator('[data-flow-action-snapshot-version]');
    const original = await panel.getAttribute('data-flow-action-snapshot-version');
    assert.equal(await panel.evaluate(n => n.open), false);
    changed = true;
    await page.locator('[data-flow-action-stale-summary]').waitFor({state:'visible'});
    assert.equal(await panel.evaluate(n => n.open), false);
    assert.equal(await panel.getAttribute('data-flow-action-snapshot-version'), original);
    assert.equal(await panel.locator('button[type=submit]:enabled').count(), 0);
    assert.equal(await panel.locator('[data-flow-action-stale]').getAttribute('hidden'), null);
    return {reviewedVersion:original, currentVersion:999, controlledResponse:true};
  });
} finally {
  await fs.writeFile(`${out}/results.json`, JSON.stringify(results, null, 2));
  await browser.close();
}
console.log(`${results.filter(r => r.status === 'passed').length}/${results.length} passed`);
if (results.some(r => r.status === 'failed')) process.exitCode = 1;
