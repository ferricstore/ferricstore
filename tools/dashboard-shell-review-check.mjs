import Module from 'node:module';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
if (process.env.NODE_PATH) Module._initPaths();
const require = Module.createRequire(import.meta.url);
const { chromium } = require('playwright');
const axe = require('axe-core');
const base = process.env.DASHBOARD_URL || 'http://localhost:4006';
const out = process.env.DASHBOARD_OUT_DIR || 'test-results/dashboard-shell-review';
await fs.mkdir(out, { recursive: true });
const results = [];
const browser = await chromium.launch({ channel: 'chrome', headless: true });
async function check(name, action) {
  if (process.env.DASHBOARD_CHECK && !name.includes(process.env.DASHBOARD_CHECK)) return;
  const context = await browser.newContext({ viewport: { width: 1440, height: 1000 } });
  const page = await context.newPage();
  const errors = [];
  page.on('pageerror', e => errors.push(e.message));
  try {
    const evidence = await action(page, context);
    assert.deepEqual(errors, [], 'No uncaught browser errors');
    await page.screenshot({ path: `${out}/${name}.png`, fullPage: true });
    results.push({ name, status: 'passed', evidence });
    console.log(`PASS ${name}`);
  } catch (error) {
    results.push({ name, status: 'failed', error: error.stack, errors });
    await page.screenshot({ path: `${out}/${name}-failed.png`, fullPage: true });
    console.error(`FAIL ${name}: ${error.message}`);
  } finally { await context.close(); }
}
try {
  await check('native-help', async page => {
    for (const path of ['/dashboard/flow', '/dashboard/flow/states', '/dashboard/doctor']) {
      await page.goto(base + path);
      assert.equal(await page.locator('#keyboard-shortcuts-modal').count(), 1);
      const opener = page.locator('[data-dashboard-shortcuts-open]');
      await opener.click();
      assert.equal(await page.locator('#keyboard-shortcuts-modal').evaluate(d => d.open), true);
      for (let i = 0; i < 3; i++) {
        await page.keyboard.press('Tab');
        assert.equal(await page.evaluate(() => !!document.activeElement.closest('#keyboard-shortcuts-modal')), true);
      }
      await page.keyboard.press('Escape');
      assert.equal(await page.locator('#keyboard-shortcuts-modal').isVisible(), false);
      assert.equal(await opener.evaluate(e => e === document.activeElement), true);
    }
  });
  await check('search-shortcut', async page => {
    await page.goto(base + '/dashboard/flow/query');
    await page.keyboard.press('/');
    assert.equal(await page.evaluate(() => document.activeElement.tagName), 'INPUT');
    assert.equal(await page.evaluate(() => document.activeElement.getClientRects().length > 0), true);
  });
  await check('clipboard-failure', async page => {
    await page.addInitScript(() => {
      Object.defineProperty(navigator, 'clipboard', { value: { writeText: () => Promise.reject(new Error('Denied')) } });
      document.execCommand = () => false;
    });
    await page.goto(base + '/dashboard/flow/nightly-audit-0088?partition_key=system');
    const button = page.locator('.copy-btn-inline').first();
    await button.click();
    assert.equal(await button.textContent(), 'Copy failed');
  });
  await check('clipboard-success', async page => {
    await page.addInitScript(() => Object.defineProperty(navigator, 'clipboard', { value: { writeText: async text => { window.copiedValue = text; } } }));
    await page.goto(base + '/dashboard/flow/nightly-audit-0088?partition_key=system');
    const button = page.locator('.copy-btn-inline').first();
    const expected = await button.getAttribute('data-copy-text');
    await button.click();
    assert.equal(await button.textContent(), 'Copied');
    assert.equal(await page.evaluate(() => window.copiedValue), expected);
  });
  await check('scoped-sidebar', async page => {
    await page.goto(base + '/dashboard/flow/states?type=invoice_dispatch&state=queued&partition_key=customer-2048');
    await page.locator('.sidebar a').filter({ hasText: /^Workers$/ }).click();
    const url = new URL(page.url());
    assert.equal(url.searchParams.get('type'), 'invoice_dispatch');
    assert.equal(url.searchParams.get('partition_key'), 'customer-2048');
    assert.equal(url.searchParams.has('state'), false);
  });
  await check('streams-live-count', async page => {
    await page.route('**/dashboard/api/streams', async route => {
      const response = await route.fetch();
      const payload = await response.json();
      payload.components.streams_top = '<table data-dashboard-row-count="1"><tbody><tr><td>review:events</td></tr></tbody></table>';
      await route.fulfill({ response, json: payload });
    });
    await page.goto(base + '/dashboard/streams');
    const count = page.locator('[data-dashboard-disclosure-count="streams_top"]');
    await page.waitForFunction(() => document.querySelector('[data-dashboard-disclosure-count="streams_top"]')?.textContent === '1');
    assert.equal(await count.textContent(), '1');
    assert.equal(await count.evaluate(n => n.closest('details').open), false, 'Do not force open a user-collapsed section');
  });
  await check('snapshot-refresh', async page => {
    await page.goto(base + '/dashboard/doctor');
    assert.equal(await page.locator('.subpage-header [data-dashboard-snapshot]').isVisible(), true);
    assert.equal(await page.locator('[data-dashboard-refresh]').isVisible(), true);
    assert.equal(await page.locator('body').getAttribute('data-dashboard-live-url'), null);
    await page.locator('[data-dashboard-refresh]').click();
    await page.waitForLoadState();
    assert.equal(new URL(page.url()).pathname, '/dashboard/doctor');
  });
  await check('filter-refresh-preservation', async page => {
    let generation = 0;
    let requests = 0;
    const component = () => '<div data-dashboard-filter-control><label>Filter streams <input data-dashboard-table-filter data-dashboard-filter-target="#fixture-streams"></label><span data-dashboard-filter-status></span></div>' +
      '<div class="table-scroll"><table id="fixture-streams" data-dashboard-row-count="' + (generation === 2 ? 0 : 2) + '"><tbody>' +
      (generation === 2 ? '<tr><td colspan="2">No streams</td></tr>' : '<tr><td>alpha</td><td>' + generation + '</td></tr><tr><td>beta</td><td>1</td></tr>') + '</tbody></table></div>';
    await page.route('**/dashboard/api/streams', async route => {
      requests += 1;
      await route.fulfill({ json: { generated_at_ms: Date.now(), components: { streams_top: component() } } });
    });
    await page.goto(base + '/dashboard/streams');
    const input = page.locator('[data-dashboard-filter-target="#fixture-streams"]');
    await input.waitFor({ state: 'attached' });
    await input.evaluate(n => { n.closest('details').open = true; });
    await input.fill('alpha');
    await input.blur();
    generation = 1;
    await page.waitForFunction(() => document.querySelector('#fixture-streams tbody tr td:nth-child(2)')?.textContent === '1');
    assert.equal(await input.inputValue(), 'alpha');
    assert.equal(await page.locator('#fixture-streams tbody tr:visible').count(), 1);
    await page.locator('#fixture-streams tbody tr').first().evaluate(n => { window.retainedRow = n; });
    const before = requests;
    const interval = Number(await page.locator('body').getAttribute('data-dashboard-live-interval-ms'));
    await page.waitForTimeout(interval + 500);
    assert.ok(requests > before, 'Identical live response received');
    assert.equal(await page.evaluate(() => window.retainedRow === document.querySelector('#fixture-streams tbody tr')), true, 'Identical payload does not redraw filtered DOM');
    generation = 2;
    await page.waitForFunction(() => document.querySelector('#fixture-streams td')?.textContent === 'No streams');
    assert.equal(await input.inputValue(), 'alpha');
    assert.match(await page.locator('[data-dashboard-filter-control]:has(input[data-dashboard-filter-target="#fixture-streams"]) [data-dashboard-filter-status]').innerText(), /0 of 0 loaded rows/);
    return { requests };
  });
  await check('live-workflow-disappearance', async page => {
    let requests = 0;
    await page.route('**/dashboard/api/flow/nightly-audit-0088?*', async route => {
      requests += 1;
      await route.fulfill({ json: { detail_unavailable: true, components: {} } });
    });
    await page.goto(base + '/dashboard/flow/nightly-audit-0088?partition_key=system');
    await page.locator('[data-dashboard-live-status="unavailable"]').waitFor();
    assert.equal(await page.locator('#workflow-timeline, #workflow-metadata, #workflow-actions').count(), 0);
    await page.waitForTimeout(2300);
    assert.equal(requests, 1, 'Automatic requests stop after record disappears');
    await page.locator('[data-dashboard-live-retry]').click();
    await page.waitForLoadState();
    await page.locator('[data-dashboard-live-status="unavailable"]').waitFor();
    assert.equal(requests, 2, 'Explicit retry reloads the scoped detail');
  });
  await check('query-page-export', async page => {
    await page.goto(base + '/dashboard/flow/query?kind=list&type=ai_agent_pipeline&partition_key=tenant-openai&limit=40');
    const serialized = await page.locator('#flow-query-export').textContent();
    const downloadEvent = page.waitForEvent('download');
    await page.locator('[data-dashboard-download-json]').click();
    const download = await downloadEvent;
    const file = await fs.readFile(await download.path(), 'utf8');
    assert.equal(file, serialized, 'Download retains exact integers and projected JSON without a JS parse round-trip');
    const data = JSON.parse(file);
    assert.ok(data.rows.length > 0);
    assert.equal(data.columns.includes('payload'), false, 'Default export does not include payloads');
    return { rows: data.rows.length, bytes: Buffer.byteLength(file), filename: download.suggestedFilename() };
  });
  await check('missing-workflow', async page => {
    await page.goto(base + '/dashboard/flow/review-missing?partition_key=none');
    assert.equal(await page.locator('[data-dashboard-live-status]').count(), 0);
    assert.equal(await page.locator('#workflow-timeline').count(), 0);
    assert.equal(await page.locator('[data-flow-signal-form]').count(), 0);
    assert.match(await page.locator('main').innerText(), /not found|not available/i);
  });
  await check('schedule-confirmation', async page => {
    await page.goto(base + '/dashboard/flow/schedules');
    const form = page.locator('form:has(input[name=action][value=create])');
    await page.locator('#flow-schedule-create-panel > summary').click();
    const id = `shell-review-${Date.now()}`;
    await form.locator('[name=id]').fill(id);
    await form.locator('[name=schedule_kind]').selectOption('delay');
    await form.locator('[name=delay_ms]').fill('86400000');
    await form.locator('[name=target_type]').fill('review-target');
    await form.getByRole('button', { name: 'Review schedule', exact: true }).click();
    await page.waitForLoadState();
    const review = page.locator('[data-schedule-review]');
    assert.match(await review.innerText(), /Creates a new active schedule/);
    await review.getByRole('button', { name: 'Create schedule', exact: true }).click();
    await page.waitForLoadState();
    const row = page.locator('tr').filter({ hasText: id });
    assert.equal(await row.count(), 1, 'Reviewed definition creates a real schedule');
    await row.locator('summary').filter({ hasText: 'Delete' }).click();
    const button = row.getByRole('button', { name: 'Confirm Delete', exact: true });
    await button.scrollIntoViewIfNeeded();
    assert.equal(await button.evaluate(n => {
      const r = n.getBoundingClientRect();
      return n.contains(document.elementFromPoint(r.x + r.width / 2, r.y + r.height / 2));
    }), true);
    await button.click();
    await page.waitForLoadState();
    const cancelled = page.locator('tbody tr').filter({ hasText: id });
    assert.match(await cancelled.innerText(), /cancelled/);
    assert.equal(await cancelled.locator('form, button').count(), 0, 'Cancelled schedules retain definitions but no mutation controls');
  });
  await check('table-filter-and-sticky-header', async page => {
    await page.goto(base + '/dashboard/flow/policies');
    const input = page.locator('[data-dashboard-table-filter]').first();
    await input.fill('invoice_dispatch');
    const selector = await input.getAttribute('data-dashboard-filter-target');
    const matches = await page.locator(`${selector} tbody tr:visible`).allTextContents();
    assert.ok(matches.length > 0);
    assert.ok(matches.every(text => text.includes('invoice_dispatch')));
    await input.fill('');
    const scroll = page.locator(`${selector}`).locator('xpath=ancestor-or-self::*[contains(@class,"table-scroll")]').first();
    const result = await scroll.evaluate(n => {
      n.scrollTop = 500;
      const th = n.querySelector('th');
      return { scrollTop: n.scrollTop, delta: th.getBoundingClientRect().top - n.getBoundingClientRect().top };
    });
    assert.ok(result.scrollTop > 0);
    assert.ok(Math.abs(result.delta) < 3);
    return result;
  });
  await check('absolute-and-bounded-schedules', async page => {
    const utc = offset => new Date(Date.now() + offset * 86400000).toISOString().slice(0, 19);
    for (const kind of ['one_shot', 'interval']) {
      await page.goto(base + '/dashboard/flow/schedules');
      await page.locator('#flow-schedule-create-panel > summary').click();
      const form = page.locator('form:has(input[name=action][value=create])');
      const id = `shell-timing-${kind}-${Date.now()}`;
      await form.locator('[name=id]').fill(id);
      await form.locator('[name=target_type]').fill('review-target');
      await form.locator('[name=schedule_kind]').selectOption(kind);
      if (kind === 'one_shot') {
        await form.locator('[name=at_utc]').fill(utc(2));
        assert.equal(await form.locator('[name=start_at_utc]').isDisabled(), true);
      } else {
        await form.locator('[name=every_ms]').fill('60000');
        await form.locator('[name=start_at_utc]').fill(utc(2));
        await form.locator('[name=end_at_utc]').fill(utc(3));
        assert.equal(await form.locator('[name=at_utc]').isDisabled(), true);
      }
      await form.getByRole('button', { name: 'Review schedule', exact: true }).click();
      await page.waitForLoadState();
      const review = page.locator('[data-schedule-review]');
      assert.match(await review.innerText(), /Next fire \(UTC\)/);
      await review.getByRole('button', { name: 'Create schedule', exact: true }).click();
      await page.waitForLoadState();
      const row = page.locator('tbody tr').filter({ hasText: id });
      assert.equal(await row.count(), 1, 'Valid UTC timing creates a real schedule');
      await row.locator('summary').filter({ hasText: 'Delete' }).click();
      await row.getByRole('button', { name: 'Confirm Delete', exact: true }).click();
      await page.waitForLoadState();
      assert.match(await page.locator('tbody tr').filter({ hasText: id }).innerText(), /cancelled/);
    }
  });
  await check('desktop-accessibility', async page => {
    const evidence = [];
    for (const path of ['/dashboard', '/dashboard/flow', '/dashboard/flow/states', '/dashboard/flow/query?kind=list&type=ai_agent_pipeline&partition_key=tenant-openai&limit=40', '/dashboard/flow/governance', '/dashboard/login']) {
      await page.goto(base + path);
      await page.evaluate(axe.source);
      const violations = await page.evaluate(async () => (await axe.run(document, { runOnly: { type: 'tag', values: ['wcag2a', 'wcag2aa', 'wcag21aa', 'best-practice'] } })).violations.map(v => ({ id: v.id, nodes: v.nodes.map(n => n.target) })));
      assert.deepEqual(violations, [], `${path} accessibility`);
      for (const width of [1280, 1440, 1920]) {
        await page.setViewportSize({ width, height: 1000 });
        assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), true, `${path} width ${width}`);
      }
      evidence.push({ path, violations });
    }
    return evidence;
  });
  await check('asset-navigation', async (page, context) => {
    const cdp = await context.newCDPSession(page);
    await cdp.send('Network.enable');
    const responses = [];
    let cacheHits = 0;
    cdp.on('Network.requestServedFromCache', () => { cacheHits += 1; });
    cdp.on('Network.responseReceived', event => { if (event.response.url.includes('/dashboard/assets/')) responses.push(event.response); });
    const first = await page.goto(base + '/dashboard');
    const firstBytes = (await first.body()).length;
    await page.goto(base + '/dashboard/flow/states');
    assert.ok(firstBytes < 40000, `HTML bytes ${firstBytes}`);
    const resourceCacheHits = await page.evaluate(() => performance.getEntriesByType('resource').filter(r => r.name.includes('/dashboard/assets/') && r.transferSize === 0 && r.decodedBodySize > 0).length);
    assert.ok(cacheHits > 0 || resourceCacheHits > 0 || responses.some(r => r.fromDiskCache), 'Shared assets reused from browser cache');
    return { firstHtmlBytes: firstBytes, cacheHits, resourceCacheHits, assetResponses: responses.map(r => ({ url: r.url, cached: !!r.fromDiskCache })) };
  });
} finally {
  await browser.close();
  await fs.writeFile(`${out}/results.json`, JSON.stringify(results, null, 2));
}
if (results.some(r => r.status === 'failed')) process.exitCode = 1;
