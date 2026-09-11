import assert from 'node:assert/strict';
import Module from 'node:module';
import fs from 'node:fs/promises';

if (process.env.NODE_PATH) Module._initPaths();
const { chromium } = Module.createRequire(import.meta.url)('playwright');
const base = process.env.DASHBOARD_URL || 'http://localhost:4000';
const out = process.env.DASHBOARD_OUT_DIR || 'test-results/dashboard-query-review';
const path = '/dashboard/flow/query?kind=list&type=invoice_dispatch&partition_key=customer-1042&limit=2';
const results = [];
await fs.mkdir(out, { recursive: true });
const browser = await chromium.launch({ channel: 'chrome', headless: process.env.HEADFUL !== '1' });

async function check(name, options, run) {
  const context = await browser.newContext({ viewport: { width: 1440, height: 1000 }, ...options });
  const page = await context.newPage();
  const errors = [];
  page.on('pageerror', error => errors.push(error.message));
  try {
    const evidence = await run(page);
    assert.deepEqual(errors, []);
    await page.screenshot({ path: `${out}/${name}.png`, fullPage: true });
    results.push({ name, passed: true, evidence });
    console.log(`PASS ${name}`);
  } catch (error) {
    results.push({ name, passed: false, error: error.stack, errors });
    await page.screenshot({ path: `${out}/${name}-failed.png`, fullPage: true });
    console.error(`FAIL ${name}: ${error.message}`);
  } finally {
    await context.close();
  }
}

async function paging(page) {
  const response = await page.goto(base + path);
  assert.equal(response.status(), 200);
  const table = page.locator('.flow-query-projection-table');
  const headers = await table.locator('thead th').allTextContents();
  assert.deepEqual(headers, ['Workflow', 'Type', 'Runtime status', 'Updated']);
  const first = await table.locator('tbody tr td:first-child').allTextContents();
  assert.equal(first.length, 2);
  const pages = page.getByRole('navigation', { name: 'Query result pages' });
  await pages.getByRole('button', { name: 'Next page', exact: true }).click();
  assert.equal((await pages.locator('[aria-current="page"]').textContent()).trim(), 'Page 2');
  assert.deepEqual(await table.locator('thead th').allTextContents(), headers);
  const second = await table.locator('tbody tr td:first-child').allTextContents();
  assert.notDeepEqual(second, first);
  await pages.getByRole('button', { name: 'Previous page', exact: true }).click();
  assert.equal((await pages.locator('[aria-current="page"]').textContent()).trim(), 'Page 1');
  assert.deepEqual(await table.locator('tbody tr td:first-child').allTextContents(), first);
  await pages.getByRole('button', { name: 'Next page', exact: true }).click();
  await pages.getByRole('button', { name: 'First page', exact: true }).click();
  assert.deepEqual(await table.locator('tbody tr td:first-child').allTextContents(), first);
  return { headers, first, second };
}

try {
  await check('guided-pagination-and-post-scope', {}, async page => {
    const evidence = await paging(page);
    const queryLink = page.locator('.sidebar a').filter({ hasText: /^Query Studio$/ });
    const scope = new URL(await queryLink.getAttribute('href'), base);
    assert.equal(scope.searchParams.get('type'), 'invoice_dispatch');
    assert.equal(scope.searchParams.get('partition_key'), 'customer-1042');
    for (const width of [1280, 1920]) {
      await page.setViewportSize({ width, height: 1000 });
      assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth + 1), true);
    }
    return evidence;
  });

  await check('pagination-and-skip-link-without-javascript', { javaScriptEnabled: false }, async page => {
    const evidence = await paging(page);
    assert.equal(await page.locator('a[href="#dashboard-main"]').count(), 1);
    assert.equal(await page.locator('main#dashboard-main[tabindex="-1"]').count(), 1);
    assert.equal(await page.getByRole('heading', { name: /^Result/ }).count(), 1);
    return evidence;
  });

  await check('typed-predicate-labels-and-copy-failure', {}, async page => {
    await page.addInitScript(() => {
      Object.defineProperty(navigator, 'clipboard', { value: undefined });
      document.execCommand = () => false;
    });
    await page.goto(base + path);
    await page.locator('[data-flow-query-kind]').selectOption('search');
    await page.locator('details.flow-query-advanced > summary').click();
    await page.getByRole('combobox', { name: 'Attribute value', exact: true }).fill(' true ');
    await page.getByRole('combobox', { name: 'State meta value', exact: true }).fill('false');
    await page.getByRole('combobox', { name: 'Attribute value type', exact: true }).selectOption('boolean');
    await page.getByRole('combobox', { name: 'Attribute value type', exact: true }).selectOption('string');
    assert.equal(await page.getByRole('combobox', { name: 'Attribute value', exact: true }).inputValue(), ' true ');
    await page.getByRole('tab', { name: 'Raw FQL draft', exact: true }).click();
    await page.getByRole('button', { name: 'Copy FQL', exact: true }).click();
    await page.getByText('Copy failed. Select and copy manually.', { exact: true }).waitFor();
    return { copy: 'failure is explicit', value: ' true ' };
  });
} finally {
  await browser.close();
  await fs.writeFile(`${out}/results.json`, JSON.stringify(results, null, 2));
}

if (results.some(result => !result.passed)) process.exitCode = 1;
