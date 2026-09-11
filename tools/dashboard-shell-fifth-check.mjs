import Module from 'node:module';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import { spawnSync } from 'node:child_process';

if (process.env.NODE_PATH) Module._initPaths();
const require = Module.createRequire(import.meta.url);
const { chromium } = require('playwright');
const source = 'apps/ferricstore_server/lib/ferricstore_server/health/dashboard';
const render = spawnSync('elixir', ['-pa', '_build/test/lib/jason/ebin', '-r', `${source}/format.ex`, '-r', `${source}/render/recent_rates.ex`, '-r', `${source}/layout/styles.ex`, '-r', `${source}/layout.ex`, '-e', `
defmodule FerricstoreServer.Acl do
  def protected_mode?, do: false
end
alias FerricstoreServer.Health.Dashboard.Layout
IO.write(Jason.encode!(%{
  script: Layout.dashboard_live_script(), css: Layout.Styles.stylesheet(),
  pages: Map.new(["keyspace", "prefixes", "commands"], fn route ->
    {route, Layout.render_sidebar_static(route) <> "<main class=\\"main-content\\" id=\\"dashboard-main\\" tabindex=\\"-1\\">" <>
      Layout.render_subpage_header(route) <> Layout.render_kv_subnav(route) <>
      "<label>Exact key <input class=\\"flow-search-input\\" name=\\"key\\"></label><span class=\\"sampled-tag\\">sampled</span><span class=\\"flow-field-help\\">Bounded sample</span><span class=\\"badge\\">ready</span></main>"}
  end)
}))`], { encoding: 'utf8', env: { ...process.env, ERL_FLAGS: '+S 2:2' } });
assert.equal(render.status, 0, render.stderr);
const assets = JSON.parse(render.stdout);
const out = process.env.DASHBOARD_OUT_DIR || 'outputs/dashboard-fifth-fixes/shell';
await fs.mkdir(out, { recursive: true });
const browser = await chromium.launch({ channel: 'chrome', headless: true });
const results = [];
const origin = 'http://dashboard-shell-fifth.test';

async function check(name, action, init) {
  const context = await browser.newContext({ viewport: { width: 1280, height: 900 } });
  const page = await context.newPage();
  const errors = [];
  page.on('pageerror', error => errors.push(error.message));
  if (init) await page.addInitScript(init);
  await page.route(`${origin}/**`, route => {
    const key = new URL(route.request().url()).pathname.split('/').at(-1);
    return route.fulfill({ contentType: 'text/html', body: `<!doctype html><html><head><style>${assets.css}</style></head><body><div class="layout">${assets.pages[key] || assets.pages.keyspace}</div>${assets.script}</body></html>` });
  });
  try {
    await page.goto(`${origin}/dashboard/keyspace`);
    await action(page, context);
    assert.deepEqual(errors, []);
    results.push({ name, status: 'passed' });
    console.log(`PASS ${name}`);
  } catch (error) {
    results.push({ name, status: 'failed', error: error.message });
    console.error(`FAIL ${name}: ${error.message}`);
    await page.screenshot({ path: `${out}/${name}.png` });
  } finally { await context.close(); }
}

try {
  await check('sidebar-keeps-explicit-groups-across-navigation', async page => {
    const group = page.locator('[data-dashboard-nav-group="Operations"]');
    assert.equal(await group.evaluate(e => e.open), false);
    await group.locator('summary').click();
    await page.locator('.sidebar a[href="/dashboard/prefixes"]').click();
    assert.equal(await group.evaluate(e => e.open), true);
    await group.locator('summary').click();
    await page.reload();
    assert.equal(await group.evaluate(e => e.open), false);
  });
  await check('active-group-reopens-and-preferences-stay-bounded', async page => {
    await page.addInitScript(() => sessionStorage.setItem('ferricstore.sidebar.groups.v1', JSON.stringify({ 'KV / Data': false, Operations: true, invented: true })));
    await page.reload();
    assert.equal(await page.locator('[data-dashboard-nav-group="KV / Data"]').evaluate(e => e.open), true);
    const operations = page.locator('[data-dashboard-nav-group="Operations"]');
    assert.equal(await operations.evaluate(e => e.open), true);
    await operations.locator('summary').click();
    const keys = await page.evaluate(() => Object.keys(JSON.parse(sessionStorage.getItem('ferricstore.sidebar.groups.v1'))));
    assert.equal(keys.includes('invented'), false);
    assert.ok(keys.length <= 6);
  });
  await check('malformed-preferences-fall-back-to-active-group', async page => {
    await page.addInitScript(() => sessionStorage.setItem('ferricstore.sidebar.groups.v1', 'invalid json'));
    await page.reload();
    assert.equal(await page.locator('[data-dashboard-nav-group="KV / Data"]').evaluate(e => e.open), true);
  });
  await check('blocked-storage-does-not-break-navigation', async page => {
    await page.locator('.sidebar a[href="/dashboard/prefixes"]').click();
    assert.equal(await page.locator('[data-dashboard-nav-group="KV / Data"]').evaluate(e => e.open), true);
  }, () => { Object.defineProperty(window, 'sessionStorage', { get() { throw new Error('blocked'); } }); });
  await check('kv-navigation-has-one-label-and-order', async page => {
    const read = selector => page.locator(selector).evaluateAll(nodes => nodes.map(e => [e.getAttribute('href'), e.textContent.trim()]));
    assert.deepEqual(await read('[data-dashboard-nav-group="KV / Data"] a'), await read('[aria-label="KV dashboard sections"] a'));
  });
  await check('labels-readable-without-desktop-overflow', async page => {
    const sizes = await page.locator('.sampled-tag,.badge,.flow-field-help,.nav-group > summary,.nav-subgroup-label').evaluateAll(nodes => nodes.map(e => ({ text: e.textContent, size: parseFloat(getComputedStyle(e).fontSize) })));
    assert.deepEqual(sizes.filter(e => e.size < 12), []);
    for (const width of [1280, 1440, 1920]) {
      await page.setViewportSize({ width, height: 900 });
      assert.ok(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth + 1));
    }
  });
  await check('shortcut-help-matches-global-search', async page => {
    await page.keyboard.press('/');
    assert.equal(await page.locator('input[name=key]').evaluate(e => e === document.activeElement), true);
    await page.keyboard.press('Escape');
    await page.locator('[data-dashboard-shortcuts-open]').click();
    assert.match(await page.locator('#keyboard-shortcuts-modal').textContent(), /Focus search \(when available\)/);
  });
  await check('draft-can-cancel-shared-refresh', async page => {
    await page.evaluate(() => {
      window.refreshEvents = 0;
      document.addEventListener('dashboard:before-refresh', event => { window.refreshEvents++; event.preventDefault(); });
    });
    await page.locator('[data-dashboard-refresh]').click();
    assert.equal(await page.evaluate(() => window.refreshEvents), 1);
  });
} finally {
  await browser.close();
  await fs.writeFile(`${out}/results.json`, JSON.stringify(results, null, 2));
}
if (results.some(result => result.status !== 'passed')) process.exitCode = 1;
