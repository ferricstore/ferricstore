import Module from 'node:module';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';

if (process.env.NODE_PATH) Module._initPaths();
const require = Module.createRequire(import.meta.url);
const { chromium } = require('playwright');
const base = process.env.DASHBOARD_URL || 'http://localhost:4010';
const out = process.env.DASHBOARD_OUT_DIR || 'outputs/dashboard-fifth-fixes/served';
await fs.mkdir(out, { recursive: true });
const browser = await chromium.launch({ channel: 'chrome', headless: true });
const context = await browser.newContext({ viewport: { width: 1440, height: 1000 } });
const page = await context.newPage();
const results = [];
const routes = [
  '/dashboard', '/dashboard/flow', '/dashboard/flow/states', '/dashboard/flow/due',
  '/dashboard/flow/workers', '/dashboard/flow/failures', '/dashboard/flow/query',
  '/dashboard/flow/query?kind=list&type=invoice_dispatch&partition_key=customer-1042',
  '/dashboard/flow/query?kind=stuck&type=invoice_dispatch&partition_key=customer-2048',
  '/dashboard/flow/lineage', '/dashboard/flow/signals', '/dashboard/flow/schedules',
  '/dashboard/flow/policies?edit=invoice_dispatch&state=queued',
  '/dashboard/flow/governance', '/dashboard/flow/retention',
  '/dashboard/keyspace', '/dashboard/prefixes', '/dashboard/reads', '/dashboard/storage',
  '/dashboard/commands', '/dashboard/security', '/dashboard/config', '/dashboard/slowlog',
  '/dashboard/clients', '/dashboard/raft', '/dashboard/streams', '/dashboard/pubsub',
  '/dashboard/flow/nightly-audit-0088?partition_key=system'
];
try {
  for (const [index, route] of routes.entries()) {
    const errors = [];
    const onError = error => errors.push(error.message);
    page.on('pageerror', onError);
    try {
      const response = await page.goto(base + route);
      assert.equal(response.status(), 200, route);
      await page.locator('main').waitFor();
      assert.ok(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth + 1), `${route}: document overflow`);
      assert.equal(await page.locator('h1').count(), 1, 'one page heading');
      const small = await page.locator('.badge,.sampled-tag,.nav-group > summary,.nav-subgroup-label,.flow-field-help,.top-bar .metric .label').evaluateAll(nodes => nodes
        .filter(e => e.getClientRects().length && parseFloat(getComputedStyle(e).fontSize) < 12)
        .map(e => ({ text: e.textContent.trim(), size: getComputedStyle(e).fontSize })));
      assert.deepEqual(small, []);
      assert.deepEqual(errors, []);
      const name = `${String(index + 1).padStart(2, '0')}-${route.split('?')[0].split('/').at(-1)}`;
      await page.screenshot({ path: `${out}/${name}.png`, fullPage: true });
      results.push({ route, status: 'passed' });
      console.log(`PASS ${route}`);
    } catch (error) {
      results.push({ route, status: 'failed', error: error.message, errors });
      console.error(`FAIL ${route}: ${error.message}`);
      await page.screenshot({ path: `${out}/${index + 1}-failed.png`, fullPage: true });
    } finally { page.off('pageerror', onError); }
  }
  for (const width of [1280, 1920]) {
    await page.setViewportSize({ width, height: 1000 });
    for (const route of ['/dashboard/flow/states', '/dashboard/flow/query', '/dashboard/keyspace', '/dashboard/flow/policies']) {
      await page.goto(base + route);
      assert.ok(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth + 1), `${width} ${route}: document overflow`);
      results.push({ route, width, status: 'passed' });
    }
  }
  await page.setViewportSize({ width: 1440, height: 1000 });
  await page.goto(base + '/dashboard/keyspace');
  const prefixForm = page.getByRole('form', { name: 'Prefix sample' });
  await prefixForm.locator('[name=prefix]').fill('dashboard-fifth:');
  await prefixForm.locator('[name=include_internal]').check();
  await Promise.all([page.waitForNavigation(), prefixForm.getByRole('button', { name: 'Sample keys' }).click()]);
  assert.match(await page.locator('table').first().textContent(), /dashboard-fifth:order:1042/);
  assert.match(await page.locator('table').first().textContent(), /hash field/);
  assert.equal(await page.getByRole('columnheader', { name: 'Physical kind', exact: true }).count(), 1);
  assert.match(await page.locator('table').first().textContent(), /stream metadata/);
  assert.match(await page.locator('table').first().textContent(), /\\u0000/);
  results.push({ journey: 'Stored compound metadata appears only after explicit opt-in', status: 'passed' });
  await page.screenshot({ path: `${out}/compound-metadata.png`, fullPage: true });

  for (const [query, message] of [
    ['user=default&command=NOT_A_COMMAND', 'Unsupported command'],
    ['user=dashboard-fifth-missing-user&command=GET', 'User does not exist'],
    ['user=default&command=&key=&channel=&route_path=', 'Enter a command, key, channel, or route to check.']
  ]) {
    await page.goto(base + '/dashboard/security?' + query);
    assert.match(await page.locator('main').textContent(), new RegExp(message));
    assert.equal(await page.getByText('Command allowed', { exact: true }).count(), 0);
    results.push({ journey: message, status: 'passed' });
  }
  await page.screenshot({ path: `${out}/acl-target-validation.png`, fullPage: true });
} finally {
  await context.close();
  await browser.close();
  await fs.writeFile(`${out}/results.json`, JSON.stringify(results, null, 2));
}
if (results.some(result => result.status !== 'passed')) process.exitCode = 1;
