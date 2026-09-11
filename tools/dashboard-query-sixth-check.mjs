import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import Module from 'node:module';
import {spawnSync} from 'node:child_process';
if (process.env.NODE_PATH) Module._initPaths();
const {chromium} = Module.createRequire(import.meta.url)('playwright');
const source = await fs.readFile('apps/ferricstore_server/lib/ferricstore_server/health/dashboard/render/flow_query_controls.ex', 'utf8');
const script = source.slice(source.indexOf('def render_flow_query_mode_script')).match(/<script>([\s\S]*?)<\/script>/)[1];
const browser = await chromium.launch({channel: 'chrome', headless: true});
const failures = [];
const results = [];
const out = 'outputs/dashboard-sixth-fixes/query';
await fs.mkdir(out, {recursive: true});
async function test(name, run, width = 1440) {
  const context = await browser.newContext({viewport: {width, height: 900}});
  const page = await context.newPage();
  const dialogs = [], errors = [];
  page.on('dialog', async dialog => { dialogs.push(dialog.type()); await dialog.accept(); });
  page.on('pageerror', error => errors.push(error.message));
  try { await run(page, dialogs); assert.deepEqual(errors, []); results.push({name, status: 'passed'}); console.log(`PASS ${name}`); }
  catch (error) { failures.push(name); results.push({name, status: 'failed', error: error.message}); console.error(`FAIL ${name}: ${error.message.slice(0, 900)}`); }
  finally { await context.close(); }
}
async function fixture(page) {
  await page.route('http://sixth-query.test/**', route => {
    const advanced = route.request().method() === 'POST';
    return route.fulfill({contentType: 'text/html', body: `<!doctype html><section data-flow-query-workbench data-flow-query-draft-scope="account">
      <button data-flow-query-mode-tab="guided">Guided</button><button data-flow-query-mode-tab="advanced">Raw</button>
      <div data-flow-query-mode="guided"><form data-flow-query-form action="/dashboard/flow/query" method="get"><input name="type" value="${advanced ? 'executed' : 'default'}"><input name="partition_key" value="tenant"><button>Run Guided</button></form></div>
      <div data-flow-query-mode="advanced"><form data-flow-query-workbench-form action="/dashboard/flow/query" method="post"><textarea name="fql">query</textarea><textarea name="params_json">{}</textarea><button name="action" value="run">Run FQL</button><button name="action" value="explain">Explain</button><button name="action" value="analyze">Analyze</button></form></div>
      <script>${script.replace('#{active}', advanced ? 'advanced' : 'guided')}</script></section>
      <form action="/dashboard/flow/query" method="post"><input type="hidden" name="action" value="run"><input type="hidden" name="surface" value="advanced"><button>Next page</button><button>Previous page</button><button>First page</button></form>`});
  });
  await page.goto('http://sixth-query.test/dashboard/flow/query');
}
try {
  for (const action of ['Run FQL', 'Explain', 'Analyze', 'Next page', 'Previous page', 'First page']) {
    await test(`named action ${action} retains unsent Guided sibling without beforeunload`, async (page, dialogs) => {
      await fixture(page);
      await page.locator('[name="type"]').fill('unsent-guided');
      await page.locator('[name="partition_key"]').fill('unsent-partition');
      await page.getByRole('button', {name: 'Raw', exact: true}).click();
      await Promise.all([page.waitForNavigation(), page.getByRole('button', {name: action, exact: true}).click()]);
      assert.deepEqual(dialogs, []);
      assert.equal(await page.locator('[name="type"]').inputValue(), 'unsent-guided');
      assert.equal(await page.locator('[name="partition_key"]').inputValue(), 'unsent-partition');
      assert.equal(await page.evaluate(() => sessionStorage.getItem('ferricstore.query.inflight-draft.v1')), null);
    });
  }
  for (const prefix of ['ASCII ', 'é😀 ', 'line1\n漢字 😀 ']) {
    await test(`positioned caret selects Unicode-safe offending span ${JSON.stringify(prefix)}`, async page => {
      const start = prefix.length;
      await page.setContent(`<section data-flow-query-workbench><button data-flow-query-mode-tab="advanced">Raw</button><div data-flow-query-mode="advanced"><form data-flow-query-workbench-form><textarea name="fql" data-flow-query-first-error data-flow-query-error-start="${start}" data-flow-query-error-end="${start + 1}">${prefix}!</textarea></form></div><script>${script.replace('#{active}', 'advanced')}</script></section>`);
      assert.deepEqual(await page.locator('textarea').evaluate(field => [field.selectionStart, field.selectionEnd]), [start, start + 1]);
      assert.equal(await page.evaluate(() => document.activeElement.name), 'fql');
    });
  }
  for (const action of ['Next page', 'Previous page', 'First page']) {
    await test(`named action ${action} confirms active Raw draft discard exactly once`, async (page, dialogs) => {
      await fixture(page);
      await page.locator('[name="type"]').fill('retained-guided');
      await page.getByRole('button', {name: 'Raw', exact: true}).click();
      await page.locator('textarea[name="fql"]').fill('unsubmitted raw edits');
      await Promise.all([page.waitForNavigation(), page.getByRole('button', {name: action, exact: true}).click()]);
      assert.deepEqual(dialogs, ['confirm']);
      assert.equal(await page.locator('[name="type"]').inputValue(), 'retained-guided');
    });
  }
  const rendered = spawnSync('elixir', ['-pa', '_build/test/lib/*/ebin', 'tools/dashboard-query-sixth-fixtures.exs'], {encoding: 'utf8', env: {...process.env, ERL_FLAGS: '+S 2:2'}});
  assert.equal(rendered.status, 0, rendered.stderr);
  const assets = JSON.parse(rendered.stdout);
  for (const width of [1280, 1440]) {
    await test(`rendered typed charts JSON inspection and ticks at ${width}`, async page => {
      const charts = ['typed', 'empty', 'remainder', 'binary'].map(name => `<section id="${name}">${assets[name]}</section>`).join('');
      const ticks = Object.entries(assets.ticks).map(([maximum, html]) => `<section data-maximum="${maximum}">${html}</section>`).join('');
      await page.setContent(`<!doctype html><html><head><style>${assets.css}</style></head><body><main style="padding:24px"><h1>Query results</h1>${assets.structured}${charts}${ticks}</main></body></html>`);
      const requests = [];
      page.on('request', request => requests.push(request.url()));
      const inspector = page.locator('summary[aria-label="Inspect full projected state_meta"]');
      await inspector.focus();
      await page.keyboard.press('Enter');
      const value = page.locator('pre[aria-label="Full projected state_meta"]');
      assert.equal(await value.isVisible(), true);
      assert.match(await value.textContent(), /^\{\n  "/);
      assert.deepEqual(JSON.parse(await value.textContent()), assets.expected_json);
      assert.deepEqual(requests, []);
      assert.deepEqual(JSON.parse(await page.locator('#flow-query-export').textContent()).rows, [[assets.expected_json]]);
      for (const summary of await page.locator('.flow-query-visualization > summary').all()) await summary.click();
      assert.deepEqual((await page.locator('#typed .flow-query-chart-label').allTextContents()).sort(), ['1', '"1"', 'true', '"true"', '1.0'].sort());
      assert.deepEqual(await page.locator('#empty .flow-query-chart-percent').allTextContents(), ['25%', '25%', '25%', '25%']);
      assert.equal(await page.locator('#empty .flow-query-donut-total').textContent(), '4');
      assert.equal(await page.locator('#remainder .flow-query-chart-label').last().textContent(), 'Remaining categories');
      assert.equal(await page.locator('#remainder .flow-query-chart-label').first().textContent(), '"Other"');
      assert.deepEqual((await page.locator('#binary .flow-query-chart-label').allTextContents()).sort(), ['Base64 /wA=', '"Base64 /wA="'].sort());
      for (const section of await page.locator('[data-maximum]').all()) {
        const maximum = Number(await section.getAttribute('data-maximum'));
        const ticks = await section.locator('.flow-query-time-tick').evaluateAll(nodes => nodes.map(node => ({label: Number(node.textContent), y: Number(node.getAttribute('y'))})));
        assert.equal(new Set(ticks.map(tick => tick.label)).size, ticks.length);
        for (const tick of ticks) assert.ok(Math.abs(tick.y - 3 - (130 - tick.label * 112 / maximum)) <= 0.02);
      }
      assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), true);
      await page.screenshot({path: `${out}/charts-json-${width}.png`, fullPage: true});
    }, width);
  }
  if (process.env.QUERY_BASE_URL) {
    await test('served history value opens exact scoped historical event', async page => {
      await page.goto(`${process.env.QUERY_BASE_URL}/dashboard/flow/query?kind=history&id=detail-browser-history&partition_key=review-detail&limit=7`);
      const link = page.locator('a[href*="history_event="]').first();
      const destination = new URL(await link.getAttribute('href'), page.url());
      const event = destination.searchParams.get('history_event');
      assert.ok(event);
      assert.equal(destination.pathname, '/dashboard/flow/detail-browser-history');
      assert.equal(destination.searchParams.get('partition_key'), 'review-detail');
      const valueResponse = page.waitForResponse(response => response.url().includes('/flow/value?'));
      await link.click();
      const response = await valueResponse;
      assert.equal(new URL(response.url()).searchParams.get('history_event'), event);
      assert.equal((await response.json()).status, 'ok');
      await page.locator('#flow-value-modal').waitFor({state: 'visible'});
      assert.match(await page.locator('#flow-value-modal-provenance').textContent(), /Historical event/);
      assert.ok((await page.locator('#flow-value-modal-provenance').textContent()).includes(event));
      await page.screenshot({path: `${out}/served-history-event.png`, fullPage: true});
    });
    await test('served Raw structured projection expands all returned JSON without fetching', async (page, dialogs) => {
      await page.goto(`${process.env.QUERY_BASE_URL}/dashboard/flow/query?inspect=true&type=inspection-review&partition_key=review-detail`);
      await page.locator('[data-flow-query-mode-tab="advanced"]').click();
      await page.locator('textarea[name="fql"]').fill('FROM runs WHERE partition_key = @partition AND type = @type ORDER BY updated_at_ms DESC LIMIT 40 RETURN RECORDS (run_id, state_meta)');
      await page.locator('textarea[name="params_json"]').fill('{"partition":"review-detail","type":"inspection-review"}');
      await Promise.all([page.waitForNavigation(), page.getByRole('button', {name: 'Run FQL', exact: true}).click()]);
      assert.deepEqual(dialogs, []);
      const requests = [];
      page.on('request', request => requests.push(request.url()));
      await page.locator('summary[aria-label="Inspect full projected state_meta"]').first().click();
      const readable = await page.locator('pre[aria-label="Full projected state_meta"]').first().textContent();
      assert.match(readable, /^\{\n  "/);
      const json = JSON.parse(readable);
      assert.ok(Object.values(json).reduce((count, group) => count + Object.keys(group).length, 0) >= 49);
      assert.deepEqual(requests, []);
      const exported = JSON.parse(await page.locator('#flow-query-export').textContent());
      assert.deepEqual(exported.rows.find(row => row[0] === 'metadata-fourth-review')[1], json);
      await page.screenshot({path: `${out}/served-raw-json.png`, fullPage: true});
    });
    await test('served Unicode parser error selects its actual offending character', async page => {
      await page.goto(`${process.env.QUERY_BASE_URL}/dashboard/flow/query`);
      await page.locator('[data-flow-query-mode-tab="advanced"]').click();
      const prefix = "FROM runs\nWHERE type = 'é😀' AND ";
      await page.locator('textarea[name="fql"]').fill(prefix + '!');
      await page.locator('textarea[name="params_json"]').fill('{}');
      await Promise.all([page.waitForNavigation(), page.getByRole('button', {name: 'Run FQL', exact: true}).click()]);
      assert.deepEqual(await page.locator('textarea[name="fql"]').evaluate(field => [field.selectionStart, field.selectionEnd]), [prefix.length, prefix.length + 1]);
      assert.equal(await page.evaluate(() => document.activeElement.name), 'fql');
      await page.screenshot({path: `${out}/served-unicode-caret.png`, fullPage: true});
    });
  }
} finally {
  await browser.close();
  await fs.writeFile(`${out}/browser-results.json`, JSON.stringify(results, null, 2));
}
if (failures.length) process.exitCode = 1;
