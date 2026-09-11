import Module from 'node:module';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import { spawnSync } from 'node:child_process';

if (process.env.NODE_PATH) Module._initPaths();
const require = Module.createRequire(import.meta.url);
const { chromium } = require('playwright');
const source = 'apps/ferricstore_server/lib/ferricstore_server/health/dashboard';
const render = spawnSync('elixir', ['-pa', '_build/test/lib/*/ebin', ...['format.ex', 'render/recent_rates.ex', 'layout/styles.ex', 'layout.ex', 'render/overview.ex', 'render/flow_query_controls.ex'].flatMap(file => ['-r', `${source}/${file}`]), '-e', `
alias FerricstoreServer.Health.Dashboard.{Layout, Render.Overview}
data = %{overview: %{status: :ok, total_keys: 123456}, hotcold: %{ops_per_sec: 99.0, total_lookups: 0, hit_ratio: 0, sample_rate: 1}, memory: %{pressure_level: :normal, total_bytes: 123456789, max_bytes: 234567890, ratio: 0.5}, connections: %{active: 100}, cluster: %{cluster_mode: :standalone, node_name: :nonode@nohost}}
IO.write(Jason.encode!(%{script: Layout.dashboard_live_script(), query_script: FerricstoreServer.Health.Dashboard.Render.FlowQueryControls.render_flow_query_mode_script(:guided), css: Layout.Styles.stylesheet(), header: Overview.render_top_bar(data), long_header: Overview.render_top_bar(put_in(data, [:cluster, :node_name], :\"ferricstore-production-eu-west-1-long-node-name@host.example.internal\"))}))
`], { encoding: 'utf8', env: { ...process.env, ERL_FLAGS: '+S 2:2' } });
assert.equal(render.status, 0, render.stderr);
const assets = JSON.parse(render.stdout);
const out = process.env.DASHBOARD_OUT_DIR || 'outputs/dashboard-seventh-fixes/presentation';
await fs.mkdir(out, { recursive: true });
const browser = await chromium.launch({ channel: 'chrome', headless: true });
const results = [];
const origin = 'http://dashboard-presentation-seventh.test';
async function check(name, size, body, action, live = false) {
  const context = await browser.newContext({ viewport: size, isMobile: false });
  const page = await context.newPage();
  const errors = [];
  page.on('pageerror', error => errors.push(error.message));
  await page.route(`${origin}/**`, async route => {
    if (new URL(route.request().url()).pathname === '/live') {
      await new Promise(resolve => setTimeout(resolve, 350));
      return route.fulfill({ json: { generated_at_ms: Date.now(), components: { top_bar: assets.header, revision: '<span>updated</span>' } } }).catch(() => {});
    }
    return route.fulfill({ contentType: 'text/html', body: `<!doctype html><html lang="en"><head><style>${assets.css}</style></head><body ${live ? 'data-dashboard-live-url="/live" data-dashboard-live-interval-ms="500"' : ''}>${body}${assets.script}</body></html>` });
  });
  try {
    await page.goto(origin);
    await action(page);
    assert.deepEqual(errors, []);
    results.push({ name, status: 'passed' });
    console.log(`PASS ${name}`);
  } catch (error) {
    results.push({ name, status: 'failed', error: error.message });
    console.error(`FAIL ${name}: ${error.message}`);
  } finally {
    await page.screenshot({ path: `${out}/${name}.png`, fullPage: true });
    await context.close();
  }
}
try {
  await check('returned-draft-warns-without-another-edit', { width: 1280, height: 900 }, '<main><form data-dashboard-returned-draft="true"><label>Reason<input value="retained reason"></label><button>Submit</button></form><a href="/other">Leave</a></main>', async page => {
    let warned = false;
    page.on('dialog', async dialog => { warned = true; await dialog.dismiss(); });
    await page.locator('input').click();
    await page.getByRole('link', { name: 'Leave' }).click();
    assert.equal(warned, true, 'server-returned unsaved input needs a navigation guard');
    assert.equal(new URL(page.url()).pathname, '/');
  });
  const queryForm = `<section data-flow-query-workbench><div role="tablist"><button data-flow-query-mode-tab="guided">Guided</button><button data-flow-query-mode-tab="advanced">Raw</button></div><div data-flow-query-mode="guided" id="flow-query-panel-guided"><form data-flow-query-form action="/dashboard/flow/query"><input name="type" value="orders"><input name="partition_key" value="literal-any"><input name="kind" value="search"><input name="state" value="failed"><input name="run_state" value="review"><input name="attribute_key" value="region"><input name="attribute_value" value="eu"><input name="from_ms" value="1"><input name="limit" value="40"></form></div><div data-flow-query-mode="advanced" id="flow-query-panel-advanced" hidden><form data-flow-query-workbench-form action="/dashboard/flow/query"><textarea name="fql">raw draft stays</textarea><textarea name="params_json">{"keep":true}</textarea></form></div>${assets.query_script}</section><section class="flow-query-empty"><a href="#flow-query-panel-guided" data-flow-query-recover="guided" data-flow-query-recover-field="type">Review query scope</a><button data-flow-query-clear-optional>Remove optional filters</button><p data-flow-query-recovery-status></p></section>`;
  await check('empty-query-recovery-edits-draft-without-executing', { width: 1280, height: 900 }, queryForm, async page => {
    let submissions = 0;
    await page.exposeFunction('recordSubmission', () => submissions++);
    await page.evaluate(() => document.addEventListener('submit', event => { event.preventDefault(); window.recordSubmission(); }));
    await page.getByRole('button', { name: 'Remove optional filters' }).click();
    assert.equal(await page.locator('[name="state"]').inputValue(), '');
    assert.equal(await page.locator('[name="kind"]').inputValue(), 'list');
    assert.equal(await page.locator('[name="type"]').inputValue(), 'orders');
    assert.equal(await page.locator('[name="partition_key"]').inputValue(), 'literal-any');
    assert.equal(await page.locator('[name="limit"]').inputValue(), '40');
    assert.equal(await page.locator('[name="fql"]').inputValue(), 'raw draft stays');
    assert.equal(submissions, 0);
    await page.getByRole('link', { name: 'Review query scope' }).click();
    assert.equal(await page.locator('[name="type"]').evaluate(node => node === document.activeElement), true);
  });
  await check('early-resume-keeps-focus-and-refreshes', { width: 1280, height: 900 }, `<div data-live-component="top_bar">${assets.header}</div><main tabindex="-1"><div data-live-component="revision"></div></main>`, async page => {
    const toggle = page.locator('[data-dashboard-live-toggle]');
    await toggle.click();
    assert.equal(await toggle.textContent(), 'Resume');
    await toggle.click();
    await page.waitForFunction(() => document.body.dataset.dashboardLiveLastUpdateMs, null, { timeout: 3500 });
    assert.equal(await toggle.evaluate(node => node === document.activeElement), true);
    assert.equal(await page.locator('[data-dashboard-live-status]').count(), 1);
    assert.equal(await page.locator('[data-dashboard-live-status]').getAttribute('data-dashboard-live-status'), 'live');
  }, true);
  const filter = `<main class="content"><form class="flow-filter-form flow-state-filter-form">${['Type', 'Stored state', 'Partition', 'ID'].map(label => `<label class="flow-state-filter-field"><span>${label}</span><input type="search" name="${label}" value="sample"></label>`).join('')}</form></main>`;
  await check('state-filter-desktop-200-percent-reflow', { width: 640, height: 500 }, filter, async page => {
    const boxes = await page.locator('.flow-state-filter-field').evaluateAll(nodes => nodes.map(node => node.querySelector('input').getBoundingClientRect().toJSON()));
    for (const [i, a] of boxes.entries()) for (const b of boxes.slice(i + 1)) {
      assert.ok(a.right <= b.left || b.right <= a.left || a.bottom <= b.top || b.bottom <= a.top, 'filter controls overlap');
    }
    assert.equal(await page.locator('form').evaluate(node => node.scrollWidth <= node.clientWidth + 1), true);
  });
  for (const width of [1280, 1440, 1920]) {
    await check(`header-identity-${width}`, { width, height: 900 }, assets.header + assets.long_header, async page => {
      assert.equal(await page.locator('.top-bar .val').evaluateAll(nodes => nodes.every(node => {
        const a = node.getBoundingClientRect(), b = node.parentElement.getBoundingClientRect();
        return a.left >= b.left - 1 && a.right <= b.right + 1;
      })), true, 'header values overflow their metric tracks');
      assert.equal(await page.locator('.top-bar-identity').count(), 2, 'identity must be separately grouped');
      assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), true);
    });
  }
  for (const cls of ['flow-policy-table', 'flow-schedules-table']) {
    const table = `<main class="content"><div class="table-scroll" style="width:900px"><table class="${cls}"><thead><tr><th>Identity</th>${'<th>Metadata</th>'.repeat(10)}<th>Actions</th></tr></thead><tbody><tr><td>orders</td>${'<td>123456789</td>'.repeat(10)}<td><button>Review</button></td></tr></tbody></table></div></main>`;
    await check(`pinned-${cls}`, { width: 1280, height: 900 }, table, async page => {
      const scroller = page.locator('.table-scroll');
      await scroller.evaluate(node => { node.scrollLeft = 450; });
      const visible = await page.locator('tbody tr').evaluate(row => {
        const bounds = row.closest('.table-scroll').getBoundingClientRect();
        const first = row.firstElementChild.getBoundingClientRect(), last = row.lastElementChild.getBoundingClientRect();
        return first.left >= bounds.left - 1 && last.right <= bounds.right + 1;
      });
      assert.equal(visible, true, 'identity and actions must remain visible while metadata scrolls');
    });
  }
} finally {
  await browser.close();
  await fs.writeFile(`${out}/results.json`, JSON.stringify(results, null, 2));
}
if (results.some(result => result.status !== 'passed')) process.exitCode = 1;
