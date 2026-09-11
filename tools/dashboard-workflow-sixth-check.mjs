import Module from 'node:module';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import { spawnSync } from 'node:child_process';

if (process.env.NODE_PATH) Module._initPaths();
const require = Module.createRequire(import.meta.url);
const { chromium } = require('playwright');
const source = 'apps/ferricstore_server/lib/ferricstore_server/health/dashboard';
const render = spawnSync('elixir', ['-pa', '_build/test/lib/*/ebin', ...['layout/styles.ex', 'layout.ex', 'render/flow_fifo.ex', 'render/flow_tables/records.ex'].flatMap(file => ['-r', `${source}/${file}`]), 'outputs/dashboard-sixth-fixes/workflow/render-fixtures.exs'], { encoding: 'utf8', env: { ...process.env, ERL_FLAGS: '+S 2:2' } });
assert.equal(render.status, 0, render.stderr);
const assets = JSON.parse(render.stdout);
const out = 'outputs/dashboard-sixth-fixes/workflow';
await fs.mkdir(out, { recursive: true });
const browser = await chromium.launch({ channel: 'chrome', headless: true });
const results = [];
const origin = 'http://dashboard-workflow-sixth.test';

async function check(name, fixture, width, action) {
  const context = await browser.newContext({ viewport: { width, height: 900 } });
  const page = await context.newPage();
  let components = assets.fixtures[fixture];
  let documentLoads = 0;
  const errors = [];
  page.on('pageerror', error => errors.push(error.message));
  await page.route(`${origin}/**`, route => {
    if (new URL(route.request().url()).pathname === '/live') return route.fulfill({ json: { components } });
    documentLoads++;
    const mounts = ['flow_states_sources', 'flow_states_table', 'flow_fifo_lanes', 'flow_recent_records'].map(key => `<div data-live-component="${key}">${components[key]}</div>`).join('');
    return route.fulfill({ contentType: 'text/html', body: `<!doctype html><html><head><meta charset="utf-8"><style>${assets.css}</style></head><body data-dashboard-live-url="/live" data-dashboard-live-interval-ms="500"><div class="layout">${assets.sidebar}<main class="main-content"><div class="content">${mounts}</div></main></div>${assets.script}</body></html>` });
  });
  try {
    await page.goto(`${origin}/dashboard/flow/states?type=invoice&partition_key=tenant-demo`);
    await action(page, next => { components = assets.fixtures[next]; }, () => documentLoads);
    const noOverflow = await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth);
    assert.equal(noOverflow, true, 'page must not overflow horizontally');
    assert.deepEqual(errors, []);
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
  await check('cold-unavailable-desktop1280', 'unavailable', 1280, async (page, update, loads) => {
    const source = page.locator('[data-live-component="flow_states_sources"]');
    assert.match(await source.textContent(), /Terminal records unavailable/);
    assert.doesNotMatch(await page.locator('main').textContent(), /No Flow states discovered|No Flow records discovered|0 matching records/);
    await page.screenshot({ path: `${out}/cold-unavailable-initial-desktop1280.png`, fullPage: true });
    const retry = source.getByRole('button', { name: 'Retry current scope' });
    await retry.focus();
    await page.keyboard.press('Enter');
    await page.waitForEvent('load');
    assert.equal(loads(), 2);
    assert.equal(new URL(page.url()).search, '?type=invoice&partition_key=tenant-demo');
    update('recovered');
    await source.locator('button').waitFor({ state: 'detached' });
    assert.match(await page.locator('[data-live-component="flow_recent_records"]').textContent(), /invoice-1024/);
  });
  await check('cold-partial-desktop1440', 'partial', 1440, async page => {
    assert.match(await page.locator('[data-live-component="flow_states_sources"]').textContent(), /Partial results/);
    assert.match(await page.locator('[data-live-component="flow_states_table"]').textContent(), /Partial results/);
    assert.equal(await page.getByRole('columnheader', { name: 'Stored state', exact: true }).count(), 1);
    assert.match(await page.locator('[data-live-component="flow_recent_records"]').textContent(), /manual_compliance_review/);
  });
  await check('fifo-unavailable-desktop1280', 'policy', 1280, async page => {
    assert.match(await page.locator('[data-live-component="flow_fifo_lanes"]').textContent(), /FIFO coverage unavailable/);
    assert.doesNotMatch(await page.locator('[data-live-component="flow_fifo_lanes"]').textContent(), /No FIFO lanes discovered/);
    assert.match(await page.locator('[data-live-component="flow_states_table"]').textContent(), /Unavailable/);
  });
} finally {
  await browser.close();
  await fs.writeFile(`${out}/browser-results.json`, JSON.stringify(results, null, 2));
}
if (results.some(result => result.status !== 'passed')) process.exitCode = 1;
