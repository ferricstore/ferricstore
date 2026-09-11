import Module from 'node:module';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';

if (process.env.NODE_PATH) Module._initPaths();
const { chromium } = Module.createRequire(import.meta.url)('playwright');
const base = process.env.DASHBOARD_URL || 'http://localhost:4010';
const out = process.env.DASHBOARD_OUT_DIR || 'outputs/dashboard-sixth-fixes/served';
await fs.mkdir(out, { recursive: true });
const browser = await chromium.launch({ channel: 'chrome', headless: true });
const results = [];
async function check(name, action) {
  const context = await browser.newContext({ viewport: { width: 1280, height: 800 } });
  const page = await context.newPage();
  const errors = [];
  page.on('pageerror', error => errors.push(error.message));
  page.setDefaultTimeout(8000);
  try {
    await action(page);
    assert.deepEqual(errors, []);
    assert.ok(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth + 1));
    await page.screenshot({ path: `${out}/${name}.png`, fullPage: true });
    results.push({ name, status: 'passed' });
    console.log(`PASS ${name}`);
  } catch (error) {
    results.push({ name, status: 'failed', error: error.message });
    console.error(`FAIL ${name}: ${error.message}`);
    await page.screenshot({ path: `${out}/${name}-failed.png`, fullPage: true });
  } finally { await context.close(); }
}
try {
  await check('states-filter-draft-and-live-scope', async page => {
    const queries = [];
    page.on('request', request => {
      if (request.url().includes('/dashboard/api/flow/states')) queries.push(new URL(request.url()));
    });
    await page.goto(base + '/dashboard/flow/states?type=invoice_dispatch&state=queued&partition_key=customer-1042');
    await page.locator('[data-dashboard-live-status="live"]').waitFor();
    const before = queries.length;
    await page.locator('#flow-state-type-filter').fill('unsent_type');
    await page.locator('#flow-state-type-filter').blur();
    await page.locator('[data-dashboard-filter-draft]').waitFor({ state: 'visible' });
    await page.waitForResponse(response => response.url().includes('/dashboard/api/flow/states'));
    assert.ok(queries.length > before);
    assert.ok(queries.every(url => url.searchParams.get('type') === 'invoice_dispatch' && url.searchParams.get('partition_key') === 'customer-1042' && url.searchParams.get('state') === 'queued'));
    assert.equal(await page.locator('#flow-state-type-filter').inputValue(), 'unsent_type');
    await page.locator('#flow-state-type-filter').fill('invoice_dispatch');
    await page.locator('[data-dashboard-filter-draft]').waitFor({ state: 'hidden' });
  });
  await check('stored-and-workflow-state-guided-query', async page => {
    await page.goto(base + '/dashboard/flow/query?kind=list&type=invoice_dispatch&partition_key=customer-1042');
    assert.ok(await page.getByRole('columnheader', { name: 'Stored state', exact: true }).isVisible());
    assert.ok(await page.getByRole('columnheader', { name: 'Workflow state', exact: true }).isVisible());
    const row = page.locator('tr').filter({ hasText: 'fifo-invoice-01' });
    assert.ok(await row.count());
    assert.match(await row.first().textContent(), /running/);
    assert.match(await row.first().textContent(), /queued/);
  });
  await check('security-active-navigation-visible', async page => {
    await page.addInitScript(() => sessionStorage.setItem('ferricstore.sidebar.groups.v1', JSON.stringify({ System: true, Workflows: true, 'KV / Data': true, Messaging: true, Operations: true, 'Control Plane': true })));
    await page.goto(base + '/dashboard/security');
    assert.ok(await page.locator('.sidebar [aria-current="page"]').evaluate(item => {
      const box = item.getBoundingClientRect(), sidebar = item.closest('.sidebar').getBoundingClientRect();
      return box.top >= sidebar.top && box.bottom <= sidebar.bottom;
    }));
    assert.equal(await page.evaluate(() => scrollY), 0);
  });
  await check('open-mode-login-is-truthful', async page => {
    await page.goto(base + '/dashboard/login');
    assert.ok(await page.getByText('Protected mode off', { exact: true }).isVisible());
    assert.match(await page.locator('body').textContent(), /signing in does not enable ACL enforcement/);
    assert.equal(await page.getByText('Protected access', { exact: true }).count(), 0);
  });
  await check('empty-slowlog-distinguishes-no-samples', async page => {
    await page.goto(base + '/dashboard/slowlog');
    assert.equal(await page.getByText('No samples', { exact: true }).count(), 2);
  });
} finally {
  await browser.close();
  await fs.writeFile(`${out}/results.json`, JSON.stringify(results, null, 2));
}
if (results.some(result => result.status !== 'passed')) process.exitCode = 1;
