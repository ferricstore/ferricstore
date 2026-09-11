import Module from 'node:module';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';

if (process.env.NODE_PATH) Module._initPaths();
const require = Module.createRequire(import.meta.url);
const { chromium } = require('playwright');
const base = process.env.DASHBOARD_URL || 'http://localhost:4010';
const out = process.env.DASHBOARD_OUT_DIR || 'outputs/dashboard-seventh-fixes/served';
await fs.mkdir(out, { recursive: true });
const browser = await chromium.launch({ channel: 'chrome', headless: true });
const results = [];
const routes = ['/dashboard', '/dashboard/flow', '/dashboard/flow/states', '/dashboard/flow/due',
  '/dashboard/flow/workers', '/dashboard/flow/schedules', '/dashboard/flow/failures',
  '/dashboard/flow/query', '/dashboard/flow/lineage', '/dashboard/flow/signals',
  '/dashboard/flow/policies', '/dashboard/flow/governance', '/dashboard/flow/retention',
  '/dashboard/keyspace', '/dashboard/prefixes', '/dashboard/reads', '/dashboard/storage',
  '/dashboard/commands', '/dashboard/streams', '/dashboard/pubsub', '/dashboard/slowlog',
  '/dashboard/merge', '/dashboard/clients', '/dashboard/raft', '/dashboard/security',
  '/dashboard/capabilities', '/dashboard/config', '/dashboard/doctor',
  '/dashboard/flow/nightly-audit-0088?partition_key=system'];

async function check(name, action, options = {}) {
  const context = await browser.newContext({ viewport: { width: 1440, height: 1000 }, ...options });
  const page = await context.newPage();
  const errors = [];
  page.on('pageerror', error => errors.push(error.message));
  page.setDefaultTimeout(10000);
  // Only the explicit review-only cleanup check below may issue a POST.
  await context.route('**/*', route => {
    const request = route.request();
    const reviewOnly = new URL(request.url()).origin === base &&
      new URL(request.url()).pathname === '/dashboard/flow/retention' &&
      new URLSearchParams(request.postData() || '').get('action') === 'review_cleanup';
    return ['GET', 'HEAD'].includes(request.method()) || reviewOnly ? route.continue() : route.abort();
  });
  try {
    await action(page);
    assert.deepEqual(errors, []);
    assert.ok(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth + 1), 'document overflow');
    await page.screenshot({ path: `${out}/${name}.png`, fullPage: true });
    results.push({ name, status: 'passed' });
    console.log(`PASS ${name}`);
  } catch (error) {
    results.push({ name, status: 'failed', error: error.stack });
    console.error(`FAIL ${name}: ${error.message}`);
    await page.screenshot({ path: `${out}/${name}-failed.png`, fullPage: true });
  } finally { await context.close(); }
}

try {
  await check('route-and-desktop-geometry-sweep', async page => {
    for (const width of [1280, 1440, 1920]) {
      await page.setViewportSize({ width, height: 1000 });
      for (const route of routes) {
        const response = await page.goto(base + route, { waitUntil: 'domcontentloaded' });
        assert.equal(response.status(), 200, `${width}: ${route}`);
        await page.locator('main').waitFor();
        assert.ok(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth + 1), `${width}: ${route} overflow`);
      }
    }
  });
  await check('early-resume-and-recent-rate-history', async page => {
    await page.goto(base + '/dashboard', { waitUntil: 'domcontentloaded' });
    await page.getByRole('button', { name: 'Pause live updates', exact: true }).click();
    await page.getByRole('button', { name: 'Resume live updates', exact: true }).click();
    await page.locator('[data-dashboard-live-status="live"]').waitFor();
    await page.waitForFunction(() => document.querySelector('[data-recent-status]')?.textContent.startsWith('Observed '));
    assert.equal(await page.locator('[data-dashboard-recent-rates]').count(), 1);
    await page.locator('[data-dashboard-disclosure-key="recent-rate-observations"] > summary').click();
    await page.locator('body').click({ position: { x: 1000, y: 180 } });
    await page.waitForResponse(response => response.url().includes('/dashboard/api/overview') && response.ok());
    assert.ok(await page.locator('[data-dashboard-disclosure-key="recent-rate-observations"]').evaluate(node => node.open));
    const count = await page.locator('[data-recent-history] tr').count();
    assert.ok(count >= 1 && count <= 30);
  });
  await check('empty-query-recovery-then-explicit-run', async page => {
    await page.goto(base + '/dashboard/flow/query?kind=list&type=invoice_dispatch&partition_key=customer-1042&state=failed');
    await page.locator('[data-flow-query-clear-optional]').click();
    const form = page.locator('[data-flow-query-form]');
    assert.equal(await form.locator('[name=state]').inputValue(), '');
    assert.equal(await form.locator('[name=type]').inputValue(), 'invoice_dispatch');
    assert.equal(await form.locator('[name=partition_key]').inputValue(), 'customer-1042');
    assert.ok(new URL(page.url()).searchParams.get('state') === 'failed', 'recovery must not execute');
    await form.locator('[data-flow-query-run-action]').click();
    await page.waitForURL(url => url.pathname === '/dashboard/flow/query' && !url.searchParams.get('state'));
    assert.ok(await page.getByRole('link', { name: 'fifo-invoice-01', exact: true }).count());
  });
  await check('fql-reference-retains-raw-draft', async page => {
    await page.goto(base + '/dashboard/flow/query');
    await page.locator('[data-flow-query-mode-tab=advanced]').click();
    const input = page.locator('[name=fql]');
    const draft = 'FROM runs WHERE partition_key = @partition AND run_id = @id RETURN RECORD';
    await input.fill(draft);
    await page.getByText('FQL1 reference', { exact: true }).click();
    await page.getByText('Indexed metadata', { exact: true }).click();
    assert.equal(await input.inputValue(), draft);
    assert.ok(await page.getByText('RETURN RECORD', { exact: true }).count());
  });
  await check('unscanned-signals-and-client-search', async page => {
    await page.goto(base + '/dashboard/flow/signals');
    assert.ok(await page.getByText('Not scanned', { exact: true }).count());
    await page.goto(base + '/dashboard/clients?q=seventh-no-matching-connection');
    assert.equal(await page.locator('input[name=q]').inputValue(), 'seventh-no-matching-connection');
    assert.match(await page.locator('main').textContent(), /scanned|registered/i);
    const response = await page.goto(base + '/dashboard/clients?q=' + 'x'.repeat(257));
    assert.equal(response.status(), 422);
    assert.equal(await page.locator('input[name=q]').inputValue(), 'x'.repeat(257));
    assert.equal(await page.locator('[data-dashboard-live-url]').count(), 0);
  });
  await check('schedule-real-hydrated-editor', async page => {
    await page.goto(base + '/dashboard/flow/schedules?id=review-seventh-schedule&edit=true');
    const form = page.locator('#flow-schedule-create-panel form[data-schedule-draft]');
    assert.equal(await form.locator('[name=id]').inputValue(), 'review-seventh-schedule');
    assert.equal(await form.locator('[name=editing]').inputValue(), 'true');
    assert.ok(Number(await form.locator('[name=original_version]').inputValue()) > 0);
    assert.equal(await form.locator('[name=target_partition]').inputValue(), 'system');
    assert.match(await form.locator('[name=target_payload]').inputValue(), /nightly/);
    await form.locator('[name=schedule_kind]').selectOption('delay');
    assert.ok(await form.locator('[name=start_at_utc]').isHidden());
    assert.ok(await form.locator('[name=timezone]').isHidden());
  });
  await check('cleanup-explicit-review-does-not-submit-mutation', async page => {
    await page.goto(base + '/dashboard/flow/retention');
    const reviewButton = page.getByRole('button', { name: 'Review global cleanup', exact: true });
    const form = reviewButton.locator('xpath=ancestor::form');
    await form.locator('[name=limit]').fill('7');
    await reviewButton.click();
    await page.waitForURL(url => url.searchParams.get('reviewed_limit') === '7');
    assert.ok(await page.getByText('Impact unknown', { exact: true }).count());
    assert.equal(await page.locator('[name=reviewed_limit]').inputValue(), '7');
    assert.equal(await page.locator('[name=confirm_cleanup]').isChecked(), false);
  });
} finally {
  await browser.close();
  await fs.writeFile(`${out}/results.json`, JSON.stringify(results, null, 2));
}
if (results.some(result => result.status !== 'passed')) process.exitCode = 1;
