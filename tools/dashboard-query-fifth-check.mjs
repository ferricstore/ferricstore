import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import Module from 'node:module';
if (process.env.NODE_PATH) Module._initPaths();
const {chromium} = Module.createRequire(import.meta.url)('playwright');
const source = await fs.readFile('apps/ferricstore_server/lib/ferricstore_server/health/dashboard/render/flow_query_controls.ex', 'utf8');
const extract = name => source.slice(source.indexOf(`def ${name}`)).match(/<script>([\s\S]*?)<\/script>/)[1];
const docs = {list: {command: 'FLOW.QUERY'}, search: {command: 'FLOW.QUERY'}, stuck: {command: 'FLOW.QUERY'}};
const dynamic = extract('render_flow_query_dynamic_script').replace('#{docs_json}', JSON.stringify(docs));
const mode = extract('render_flow_query_mode_script').replace('#{active}', 'guided');
const origin = 'http://query-fifth.test';
const browser = await chromium.launch({channel: 'chrome', headless: true});
const failures = [];
async function test(name, fn) {
  const context = await browser.newContext({viewport: {width: 1440, height: 900}});
  const page = await context.newPage();
  const errors = [];
  page.on('pageerror', error => errors.push(error.message));
  try { await fn(page); assert.deepEqual(errors, []); console.log(`PASS ${name}`); }
  catch (error) { failures.push(name); console.error(`FAIL ${name}: ${error.message.slice(0, 800)}`); }
  finally { await context.close(); }
}
async function fixture(page) {
  await page.route(`${origin}/**`, route => route.fulfill({contentType: 'text/html', body: `<!doctype html>
  <button id="refresh" onclick="if(document.dispatchEvent(new Event('dashboard:before-refresh',{cancelable:true}))) location.reload()">Refresh</button>
  <a href="/elsewhere" id="navigate">Elsewhere</a>
  <section data-flow-query-workbench data-flow-query-draft-scope="test-user">
    <button data-flow-query-mode-tab="guided">Guided</button><button data-flow-query-mode-tab="advanced">Raw</button>
    <div data-flow-query-mode="guided"><form action="/dashboard/flow/query" data-flow-query-form>
    <select name="kind" data-flow-query-kind><option>list</option><option>search</option><option>stuck</option></select>
    <input name="type" value="email"><input name="partition_key" value="tenant">
    <details class="flow-query-advanced"><summary>Predicates and time bounds</summary>
      <span data-flow-query-time-from-label></span><input type="datetime-local" name="from">
      <span data-flow-query-time-to-label></span><input type="datetime-local" name="to">
      <span data-flow-query-time-error hidden></span><span data-flow-query-direction-label></span>
      <span data-flow-query-search-requirement hidden>Search requires an indexed attribute or state metadata predicate.</span>
      <div data-flow-query-kinds="list search"><input name="attribute_key">
        <div data-flow-query-kinds="list search" data-flow-query-scalar-group data-flow-query-scalar-key="attribute_key">
        <select name="attribute_value_type" data-flow-query-scalar-type><option value="string">Text</option><option value="integer">Integer</option><option value="null">Null</option></select>
        <input name="attribute_value" data-flow-query-scalar-value><span data-flow-query-scalar-error hidden></span></div></div>
      <input name="state_meta_state"><input name="state_meta_key"><input name="state_meta_value">
    </details><button type="submit">Run</button></form><script>${dynamic}</script></div>
    <div data-flow-query-mode="advanced"><form method="post" action="/dashboard/flow/query" data-flow-query-workbench-form>
      <textarea name="fql">default query</textarea><textarea name="params_json">{}</textarea><button type="submit">Run FQL</button>
    </form></div><script>${mode}</script></section>
    <form action="/dashboard/flow/query" method="post"><input type="hidden" name="surface" value="guided"><button>Next page</button></form>`}));
  await page.goto(`${origin}/dashboard/flow/query`);
}
try {
  await test('collapsed invalid scalar expands and receives focus', async page => {
    await fixture(page);
    await page.locator('summary').click();
    await page.locator('[name="attribute_key"]').fill('priority');
    await page.locator('[name="attribute_value_type"]').selectOption('integer');
    await page.locator('[name="attribute_value"]').fill('oops');
    await page.locator('summary').click();
    await page.getByRole('button', {name: 'Run', exact: true}).click();
    assert.equal(await page.locator('details').getAttribute('open'), '');
    assert.equal(await page.locator('[data-flow-query-scalar-error]').isVisible(), true);
    assert.equal(await page.evaluate(() => document.activeElement.name), 'attribute_value');
  });
  await test('Search opens required predicates and blocks incomplete submission visibly', async page => {
    await fixture(page);
    await page.locator('[name="kind"]').selectOption('search');
    assert.equal(await page.locator('details').getAttribute('open'), '');
    await page.locator('summary').click();
    await page.getByRole('button', {name: 'Run', exact: true}).click();
    assert.equal(await page.locator('details').getAttribute('open'), '');
    assert.equal(await page.evaluate(() => document.activeElement.name), 'attribute_key');
  });
  await test('empty Text is legal while empty Integer remains invalid', async page => {
    await fixture(page);
    await page.locator('summary').click();
    await page.locator('[name="attribute_key"]').fill('priority');
    assert.equal(await page.locator('[name="attribute_value"]').evaluate(el => el.checkValidity()), true);
    await page.locator('[name="attribute_value_type"]').selectOption('integer');
    assert.equal(await page.locator('[name="attribute_value"]').evaluate(el => el.checkValidity()), false);
    await page.locator('[name="attribute_value_type"]').selectOption('null');
    assert.equal(await page.locator('[name="attribute_value"]').isDisabled(), true);
  });
  await test('dirty Refresh cancel retains Raw and no draft is stored', async page => {
    await fixture(page);
    await page.getByRole('button', {name: 'Raw', exact: true}).click();
    await page.locator('[name="fql"]').fill('unsubmitted query');
    await page.locator('[name="params_json"]').fill('{"private":"only in memory"}');
    let prompted = false;
    page.once('dialog', async dialog => { prompted = true; await dialog.dismiss(); });
    await page.locator('#refresh').click();
    assert.equal(prompted, true);
    assert.equal(await page.locator('[name="fql"]').inputValue(), 'unsubmitted query');
    assert.equal(await page.evaluate(() => sessionStorage.getItem('ferricstore.query.inflight-draft.v1')), null);
  });
  await test('empty Text predicate submits its explicit empty value', async page => {
    await fixture(page);
    await page.locator('[name="kind"]').selectOption('search');
    await page.locator('[name="attribute_key"]').fill('flag');
    await Promise.all([page.waitForNavigation(), page.getByRole('button', {name: 'Run', exact: true}).click()]);
    const params = new URL(page.url()).searchParams;
    assert.equal(params.get('attribute_key'), 'flag');
    assert.equal(params.has('attribute_value'), true);
    assert.equal(params.get('attribute_value'), '');
  });
  await test('dirty native navigation triggers beforeunload and cancellation retains draft', async page => {
    await fixture(page);
    await page.getByRole('button', {name: 'Raw', exact: true}).click();
    await page.locator('[name="fql"]').fill('unsent');
    let prompted = false;
    page.once('dialog', async dialog => { prompted = dialog.type() === 'beforeunload'; await dialog.dismiss(); });
    await page.locator('#navigate').click({noWaitAfter: true});
    await page.waitForTimeout(100);
    assert.equal(prompted, true);
    assert.equal(await page.locator('[name="fql"]').inputValue(), 'unsent');
  });
  await test('operation changes identify the exact clock', async page => {
    await fixture(page);
    await page.locator('summary').click();
    assert.equal(await page.locator('[data-flow-query-time-from-label]').textContent(), 'Updated time from UTC');
    await page.locator('[name="kind"]').selectOption('stuck');
    assert.equal(await page.locator('[data-flow-query-time-from-label]').textContent(), 'Lease deadline from UTC');
    assert.equal(await page.locator('[data-flow-query-direction-label]').textContent(), 'Latest lease deadline first');
  });
  await test('accepting dirty Refresh asks once and discards only after confirmation', async page => {
    await fixture(page);
    await page.getByRole('button', {name: 'Raw', exact: true}).click();
    await page.locator('[name="fql"]').fill('unsent');
    const dialogs = [];
    page.on('dialog', async dialog => { dialogs.push(dialog.type()); await dialog.accept(); });
    await Promise.all([page.waitForNavigation(), page.locator('#refresh').click()]);
    assert.deepEqual(dialogs, ['confirm']);
    assert.equal(await page.locator('[name="fql"]').inputValue(), 'default query');
  });
  await test('unchanged and reverted drafts refresh without warnings', async page => {
    await fixture(page);
    const dialogs = [];
    page.on('dialog', async dialog => { dialogs.push(dialog.type()); await dialog.dismiss(); });
    await page.locator('[name="type"]').fill('changed');
    await page.locator('[name="type"]').fill('email');
    await Promise.all([page.waitForNavigation(), page.locator('#refresh').click()]);
    assert.deepEqual(dialogs, []);
  });
  await test('pagination cannot discard the dirty submitted-mode editor without confirmation', async page => {
    await fixture(page);
    await page.locator('[name="type"]').fill('unsubmitted-guided');
    let prompted = false;
    page.once('dialog', async dialog => { prompted = true; await dialog.dismiss(); });
    await page.getByRole('button', {name: 'Next page', exact: true}).click();
    assert.equal(prompted, true);
    assert.equal(await page.locator('[name="type"]').inputValue(), 'unsubmitted-guided');
  });
  if (process.env.QUERY_BASE_URL) await test('served query controls retain dirty Raw on shared Refresh and reveal invalid predicates', async page => {
    await page.goto(`${process.env.QUERY_BASE_URL}/dashboard/flow/query?inspect=true&type=invoice_dispatch&partition_key=customer-1042`);
    await page.locator('[data-flow-query-mode-tab="advanced"]').click();
    await page.locator('textarea[name="fql"]').fill('FROM runs WHERE partition_key = @partition LIMIT 13 RETURN RECORDS');
    await page.locator('textarea[name="params_json"]').fill('{"partition":"customer-1042"}');
    let prompted = false;
    page.once('dialog', async dialog => { prompted = true; await dialog.dismiss(); });
    await page.locator('[data-dashboard-refresh]').click();
    assert.equal(prompted, true);
    assert.equal(await page.locator('textarea[name="fql"]').inputValue(), 'FROM runs WHERE partition_key = @partition LIMIT 13 RETURN RECORDS');
    await page.locator('[data-flow-query-mode-tab="guided"]').click();
    await page.locator('[name="kind"]').selectOption('search');
    assert.equal(await page.locator('.flow-query-advanced').getAttribute('open'), '');
    await page.locator('[name="attribute_key"]').fill('priority');
    await page.locator('[name="attribute_value_type"]').selectOption('integer');
    await page.locator('[name="attribute_value"]').fill('oops');
    await page.locator('.flow-query-advanced > summary').click();
    await page.locator('[data-flow-query-run-action]').click();
    assert.equal(await page.evaluate(() => document.activeElement.name), 'attribute_value');
    await fs.mkdir('outputs/dashboard-fifth-fixes/query', {recursive: true});
    await page.screenshot({path: 'outputs/dashboard-fifth-fixes/query/invalid-predicate-desktop.png', fullPage: true});
  });
  if (process.env.QUERY_BASE_URL) await test('served expired leases and Analyze expose deadline and comparable plan evidence', async page => {
    await page.goto(`${process.env.QUERY_BASE_URL}/dashboard/flow/query?kind=stuck&type=invoice_dispatch&partition_key=customer-2048`);
    assert.equal(await page.getByRole('columnheader', {name: 'Lease deadline', exact: true}).count(), 1);
    assert.match(await page.locator('.flow-query-projection-table').textContent(), /expired at capture/);
    await fs.mkdir('outputs/dashboard-fifth-fixes/query', {recursive: true});
    await page.screenshot({path: 'outputs/dashboard-fifth-fixes/query/expired-deadlines-desktop.png', fullPage: true});
    await page.locator('[data-flow-query-mode-tab="advanced"]').click();
    await page.locator('textarea[name="fql"]').fill('FROM runs WHERE partition_key = @partition AND type = @type ORDER BY updated_at_ms DESC LIMIT 7 RETURN RECORDS (run_id, type, state, updated_at_ms)');
    await page.locator('textarea[name="params_json"]').fill('{"partition":"customer-1042","type":"invoice_dispatch"}');
    await Promise.all([page.waitForNavigation(), page.getByRole('button', {name: 'Analyze', exact: true}).click()]);
    const wall = page.locator('.flow-query-plan-metrics tbody tr').filter({has: page.getByRole('rowheader', {name: 'Wall time', exact: true})});
    assert.equal(await wall.count(), 1);
    assert.notEqual((await wall.locator('td').nth(1).textContent()).trim(), '-');
    assert.notEqual((await wall.locator('td').nth(2).textContent()).trim(), '-');
    const alternatives = page.getByRole('region', {name: 'Alternative query plans', exact: true});
    for (const name of ['Index', 'Estimated cost', 'Cost delta']) {
      assert.equal(await alternatives.getByRole('columnheader', {name, exact: true}).count(), 1);
    }
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), true);
    await page.screenshot({path: 'outputs/dashboard-fifth-fixes/query/analyze-comparison-desktop.png', fullPage: true});
  });
} finally { await browser.close(); }
if (failures.length) process.exitCode = 1;
