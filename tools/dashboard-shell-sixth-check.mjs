import Module from 'node:module';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import { spawnSync } from 'node:child_process';

if (process.env.NODE_PATH) Module._initPaths();
const require = Module.createRequire(import.meta.url);
const { chromium } = require('playwright');
const source = 'apps/ferricstore_server/lib/ferricstore_server/health/dashboard';
const render = spawnSync('elixir', ['-pa', '_build/test/lib/jason/ebin', ...['format.ex', 'render/recent_rates.ex', 'layout/styles.ex', 'layout.ex', 'render/table_value.ex'].flatMap(file => ['-r', `${source}/${file}`]), '-e', `
defmodule FerricstoreServer.Acl do
  def protected_mode?, do: false
end
alias FerricstoreServer.Health.Dashboard.Layout
alias FerricstoreServer.Health.Dashboard.Render.TableValue
values = for id <- [1, 2], do: ~s(<div data-row="#{id}">) <> TableValue.render(String.duplicate("command#{id} ", 12), "command") <> "</div>"
IO.write(Jason.encode!(%{script: Layout.dashboard_live_script(), css: Layout.Styles.stylesheet(), sidebar: Layout.render_sidebar_static("security"), rows: Enum.join(values), reversed: Enum.join(Enum.reverse(values)), header: Layout.render_subpage_header("security")}))
`], { encoding: 'utf8', env: { ...process.env, ERL_FLAGS: '+S 2:2' } });
assert.equal(render.status, 0, render.stderr);
const assets = JSON.parse(render.stdout);
const out = process.env.DASHBOARD_OUT_DIR || 'outputs/dashboard-sixth-fixes/shell';
await fs.mkdir(out, { recursive: true });
const browser = await chromium.launch({ channel: 'chrome', headless: true });
const results = [];
const origin = 'http://dashboard-shell-sixth.test';
async function check(name, action, init) {
  const context = await browser.newContext({ viewport: { width: 1280, height: 800 } });
  const page = await context.newPage();
  const errors = [];
  let components = {};
  let revision = 0;
  page.on('pageerror', error => errors.push(error.message));
  if (init) await page.addInitScript(init);
  await page.route(`${origin}/**`, route => {
    if (new URL(route.request().url()).pathname === '/live') {
      return route.fulfill({ json: { components: { ...components, revision: `<span>${revision}</span>` } } });
    }
    return route.fulfill({ contentType: 'text/html', body: `<!doctype html><html><head><style>${assets.css}</style></head><body data-dashboard-live-url="/live" data-dashboard-live-interval-ms="500"><div class="layout"><div data-live-component="sidebar">${assets.sidebar}</div><main class="main-content">${assets.header}<form class="flow-filter-form" action="/dashboard/flow/states" method="get"><label>Type <input name="type" value="invoices"></label><label>State <select name="state"><option>queued</option><option>failed</option></select></label><input name="inactive_range" value="15m" type="hidden" data-fixture-inactive><button>Apply</button><button type="reset">Reset</button></form><p id="applied">Showing invoices / queued</p><div data-live-component="rows">${assets.rows}</div><div data-live-component="anonymous"><details><summary>Anonymous A</summary>Original</details></div><div data-live-component="revision"></div></main></div>${assets.script}<script>document.addEventListener('DOMContentLoaded', () => { document.querySelector('[data-fixture-inactive]').disabled = true; });</script></body></html>` });
  });
  const refresh = async update => {
    components = update;
    revision++;
    await page.evaluate(() => { document.activeElement.blur(); window.getSelection().removeAllRanges(); });
    await page.waitForFunction(n => document.querySelector('[data-live-component="revision"]').textContent === String(n), revision);
  };
  try {
    await page.goto(`${origin}/dashboard/flow/states`);
    await page.locator('[data-dashboard-live-status="live"]').waitFor();
    await action(page, refresh);
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
  await check('preferences-survive-live-sidebar-replacement', async (page, refresh) => {
    await refresh({ sidebar: assets.sidebar + '<span hidden>updated</span>' });
    const preference = page.locator('[data-dashboard-character-shortcuts]');
    assert.equal(await preference.isChecked(), false, 'saved off must be rendered off after patch');
    await page.locator('[data-dashboard-shortcuts-open]').click();
    await preference.check();
    assert.equal(await page.evaluate(() => localStorage.getItem('ferricstore_character_shortcuts')), 'on');
    await preference.uncheck();
    assert.equal(await page.evaluate(() => localStorage.getItem('ferricstore_character_shortcuts')), 'off');
    await page.keyboard.press('Escape');
    const group = page.locator('[data-dashboard-nav-group="Operations"]');
    await group.locator('summary').click();
    await page.waitForFunction(() => JSON.parse(sessionStorage.getItem('ferricstore.sidebar.groups.v1')).Operations === true);
  }, () => localStorage.setItem('ferricstore_character_shortcuts', 'off'));
  await check('disclosures-follow-values-not-array-index', async (page, refresh) => {
    await page.locator('[data-row="1"] summary').click();
    await refresh({ rows: assets.reversed });
    assert.equal(await page.locator('[data-row="1"] details').evaluate(e => e.open), true);
    assert.equal(await page.locator('[data-row="2"] details').evaluate(e => e.open), false);
  });
  await check('anonymous-disclosures-never-transfer-to-another-record', async (page, refresh) => {
    await page.locator('[data-live-component="anonymous"] summary').click();
    await refresh({ anonymous: '<details><summary>Anonymous B</summary>Different record</details>' });
    assert.equal(await page.locator('[data-live-component="anonymous"] details').evaluate(e => e.open), false);
  });
  await check('unapplied-filters-stay-explicit-after-blur-and-refresh', async (page, refresh) => {
    await page.locator('input[name="type"]').fill('payments');
    await refresh({ rows: assets.reversed });
    const notice = page.locator('[data-dashboard-filter-draft]');
    assert.equal(await notice.count(), 1);
    assert.equal(await notice.isVisible(), true);
    assert.match(await notice.textContent(), /not applied.*previous scope/i);
    assert.equal(await page.locator('#applied').textContent(), 'Showing invoices / queued');
    await page.locator('input[name="type"]').fill('invoices');
    assert.equal(await notice.isVisible(), false);
    await page.locator('select').selectOption('failed');
    assert.equal(await notice.isVisible(), true);
    await page.getByRole('button', { name: 'Reset', exact: true }).click();
    await notice.waitFor({ state: 'hidden' });
  });
  await check('active-navigation-is-visible-after-restoring-expanded-groups', async page => {
    const visible = await page.locator('.sidebar [aria-current="page"]').evaluate(item => {
      const box = item.getBoundingClientRect();
      const sidebar = item.closest('.sidebar').getBoundingClientRect();
      return box.top >= sidebar.top && box.bottom <= sidebar.bottom && box.bottom <= innerHeight;
    });
    assert.equal(visible, true);
    assert.equal(await page.evaluate(() => scrollY), 0, 'only the sidebar may scroll');
  }, () => sessionStorage.setItem('ferricstore.sidebar.groups.v1', JSON.stringify({ Workflows: true, Streams: true, 'KV / Data': true, Operations: true, Instance: true })));
} finally {
  await browser.close();
  await fs.writeFile(`${out}/results.json`, JSON.stringify(results, null, 2));
}
if (results.some(result => result.status !== 'passed')) process.exitCode = 1;
