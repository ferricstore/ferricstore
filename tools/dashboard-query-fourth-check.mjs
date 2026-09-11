import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import Module from 'node:module';

if (process.env.NODE_PATH) Module._initPaths();
const {chromium} = Module.createRequire(import.meta.url)('playwright');
const source = await fs.readFile('apps/ferricstore_server/lib/ferricstore_server/health/dashboard/render/flow_query_controls.ex', 'utf8');
const script = source.slice(source.indexOf('def render_flow_query_mode_script')).match(/<script>([\s\S]*?)<\/script>/)[1];
const origin = 'http://query-fourth.test';
const key = 'ferricstore.query.inflight-draft.v1';
const raw = {fql: 'FROM runs WHERE state = "failed" LIMIT 13', params_json: '{"private":"draft value"}'};
const browser = await chromium.launch({channel: 'chrome', headless: true});
const failures = [];

async function fixture({scope = 'account-a', unavailable = false} = {}) {
  const context = await browser.newContext();
  let nextScope = scope;
  const errors = [];
  const page = await context.newPage();
  page.on('pageerror', error => errors.push(error.message));
  await page.route(`${origin}/**`, async route => {
    const advanced = route.request().method() === 'POST';
    await route.fulfill({contentType: 'text/html', body: `<!doctype html><section data-flow-query-workbench data-flow-query-draft-scope="${nextScope}">
      <button data-flow-query-mode-tab="guided">Guided draft</button><button data-flow-query-mode-tab="advanced">Raw FQL draft</button>
      <div data-flow-query-mode="guided"><form data-flow-query-form action="/dashboard/flow/query" method="get">
        <input name="type" value="${advanced ? '' : 'email'}"><input name="partition_key" value="tenant-a"><input name="rev" type="checkbox">
        <input name="_csrf_token" type="hidden" value="do-not-store"><button type="submit">Run Guided</button>
      </form></div>
      <div data-flow-query-mode="advanced"><form data-flow-query-workbench-form action="/dashboard/flow/query" method="post">
        <textarea name="fql">default query</textarea><textarea name="params_json">{}</textarea><button type="submit">Run FQL</button>
      </form></div><script>${script.replace('#{active}', advanced ? 'advanced' : 'guided')}</script></section>`});
  });
  if (unavailable) await context.addInitScript(() => {
    Storage.prototype.setItem = () => { throw new DOMException('Quota exceeded', 'QuotaExceededError'); };
  });
  await page.goto(`${origin}/dashboard/flow/query`);
  return {page, context, errors, setScope: value => { nextScope = value; }};
}

async function editRaw(page, draft = raw) {
  await page.locator('[data-flow-query-mode-tab="advanced"]').click();
  await page.locator('[name="fql"]').fill(draft.fql);
  await page.locator('[name="params_json"]').fill(draft.params_json);
  await page.locator('[data-flow-query-mode-tab="guided"]').click();
}

async function submit(page, label) {
  await Promise.all([page.waitForNavigation({waitUntil: 'domcontentloaded'}), page.getByRole('button', {name: label, exact: true}).click()]);
}

async function test(name, run) {
  try { await run(); console.log(`PASS ${name}`); }
  catch (error) { failures.push(name); console.error(`FAIL ${name}: ${error.message.slice(0, 1200)}`); }
}

try {
  await test('Raw draft survives Guided submission, repeated submission and consume-on-return', async () => {
    const {page, context, errors} = await fixture();
    try {
      await editRaw(page);
      await submit(page, 'Run Guided');
      assert.equal(await page.locator('[name="fql"]').inputValue(), raw.fql);
      assert.equal(await page.locator('[name="params_json"]').inputValue(), raw.params_json);
      assert.equal(await page.evaluate(key => sessionStorage.getItem(key), key), null);
      assert.equal(page.url().includes('private'), false);
      await submit(page, 'Run Guided');
      assert.equal(await page.locator('[name="fql"]').inputValue(), raw.fql);
      assert.deepEqual(errors, []);
    } finally { await context.close(); }
  });

  await test('Guided draft survives Raw submission including checkbox state without transferring queries', async () => {
    const {page, context} = await fixture();
    try {
      await page.locator('[name="type"]').fill('unsent-guided-type');
      await page.locator('[name="rev"]').check();
      await page.locator('[data-flow-query-mode-tab="advanced"]').click();
      await submit(page, 'Run FQL');
      assert.equal(await page.locator('[name="type"]').inputValue(), 'unsent-guided-type');
      assert.equal(await page.locator('[name="rev"]').isChecked(), true);
      assert.equal(await page.locator('[name="fql"]').inputValue(), 'default query');
      assert.equal(await page.evaluate(key => sessionStorage.getItem(key), key), null);
    } finally { await context.close(); }
  });

  await test('unchanged submitted Guided context survives a Raw submission', async () => {
    const {page, context} = await fixture();
    try {
      await page.locator('[data-flow-query-mode-tab="advanced"]').click();
      await submit(page, 'Run FQL');
      assert.equal(await page.locator('[name="type"]').inputValue(), 'email');
      assert.equal(await page.evaluate(key => sessionStorage.getItem(key), key), null);
    } finally { await context.close(); }
  });

  await test('account changes never restore previous account drafts and consume the entry', async () => {
    const {page, context, setScope} = await fixture();
    try {
      await editRaw(page);
      setScope('account-b');
      await submit(page, 'Run Guided');
      assert.equal(await page.locator('[name="fql"]').inputValue(), 'default query');
      assert.equal(await page.evaluate(key => sessionStorage.getItem(key), key), null);
    } finally { await context.close(); }
  });

  for (const [name, entry] of [
    ['expired', {scope: 'account-a', createdAt: Date.now() - 6 * 60 * 1000, mode: 'advanced', fields: [{name: 'fql', value: 'stale', checked: false}]}],
    ['future timestamp', {scope: 'account-a', createdAt: Date.now() + 60 * 1000, mode: 'advanced', fields: [{name: 'fql', value: 'stale', checked: false}]}],
    ['malformed', '{invalid'],
    ['oversized stored', 'x'.repeat(128 * 1024 + 1)]
  ]) await test(`${name} entries are discarded without restoration`, async () => {
    const {page, context} = await fixture();
    try {
      await page.evaluate(({key, entry}) => sessionStorage.setItem(key, typeof entry === 'string' ? entry : JSON.stringify(entry)), {key, entry});
      await page.reload();
      assert.equal(await page.locator('[name="fql"]').inputValue(), 'default query');
      assert.equal(await page.evaluate(key => sessionStorage.getItem(key), key), null);
    } finally { await context.close(); }
  });

  for (const [name, options, draft] of [
    ['quota failure', {unavailable: true}, raw],
    ['unverified identity', {scope: ''}, raw],
    ['oversized draft', {}, {...raw, fql: 'x'.repeat(128 * 1024 + 1)}]
  ]) await test(`${name} requires confirmation and cancellation retains the draft`, async () => {
    const {page, context} = await fixture(options);
    try {
      await editRaw(page, draft);
      let prompted = false;
      page.once('dialog', async dialog => { prompted = true; await dialog.dismiss(); });
      await page.getByRole('button', {name: 'Run Guided', exact: true}).click();
      assert.equal(prompted, true);
      assert.equal(await page.locator('[name="fql"]').inputValue(), draft.fql);
    } finally { await context.close(); }
  });

  await test('returned field errors receive focus and clear stale feedback after editing', async () => {
    const {page, context, errors} = await fixture();
    try {
      for (const field of ['fql', 'params_json']) {
        await page.setContent(`<section data-flow-query-workbench><button data-flow-query-mode-tab="guided">Guided</button><button data-flow-query-mode-tab="advanced">Raw</button>
          <div data-flow-query-mode="guided"><form data-flow-query-form></form></div><div data-flow-query-mode="advanced"><form data-flow-query-workbench-form>
          <textarea name="${field}" aria-invalid="true" aria-describedby="field-error" data-flow-query-first-error>invalid</textarea>
          <span id="field-error" class="flow-field-error" data-flow-query-server-error>Invalid input</span></form></div>
          <script>${script.replace('#{active}', 'advanced')}</script></section>`);
        assert.equal(await page.evaluate(() => document.activeElement.name), field);
        await page.locator('textarea').fill('corrected');
        assert.equal(await page.locator('textarea').getAttribute('aria-invalid'), null);
        assert.equal(await page.locator('#field-error').isVisible(), false);
      }
      assert.deepEqual(errors, []);
    } finally { await context.close(); }
  });

  if (process.env.QUERY_BASE_URL) await test('live dashboard preserves Raw draft and returns focused field-associated HTTP errors', async () => {
    const context = await browser.newContext({viewport: {width: 1440, height: 900}});
    const page = await context.newPage();
    const errors = [];
    page.on('pageerror', error => errors.push(error.message));
    try {
      await page.goto(`${process.env.QUERY_BASE_URL}/dashboard/flow/query`);
      await editRaw(page);
      const guided = page.locator('[data-flow-query-form]');
      await guided.locator('[name="type"]').fill('query-fourth-readonly');
      await guided.locator('[name="partition_key"]').fill('query-fourth-readonly');
      await Promise.all([page.waitForNavigation({waitUntil: 'domcontentloaded'}), guided.getByRole('button', {name: 'Run', exact: true}).click()]);
      await page.locator('[data-flow-query-mode-tab="advanced"]').click();
      assert.equal(await page.locator('[name="fql"]').inputValue(), raw.fql);
      assert.equal(await page.locator('[name="params_json"]').inputValue(), raw.params_json);
      assert.equal(await page.evaluate(key => sessionStorage.getItem(key), key), null);
      assert.equal(page.url().includes('private'), false);
      await page.locator('[name="params_json"]').fill('{invalid<&');
      await submit(page, 'Run FQL');
      const params = page.locator('[name="params_json"]');
      assert.equal(await params.getAttribute('aria-invalid'), 'true');
      assert.equal(await params.getAttribute('aria-describedby'), 'flow-query-params-json-error');
      assert.equal(await page.evaluate(() => document.activeElement.name), 'params_json');
      assert.equal(await params.inputValue(), '{invalid<&');
      assert.equal(await page.locator('#flow-query-params-json-error').isVisible(), true);
      await fs.mkdir('outputs/dashboard-fourth-fixes/query', {recursive: true});
      await page.screenshot({path: 'outputs/dashboard-fourth-fixes/query/invalid-parameters-desktop.png', fullPage: true});
      await params.fill('{}');
      await page.locator('[name="fql"]').fill('FROM runs WHERE <bad>');
      await submit(page, 'Run FQL');
      assert.equal(await page.locator('[name="fql"]').getAttribute('aria-invalid'), 'true');
      assert.equal(await page.evaluate(() => document.activeElement.name), 'fql');
      assert.equal(await page.locator('#flow-query-fql-error').isVisible(), true);
      assert.deepEqual(errors, []);
    } finally { await context.close(); }
  });
} finally { await browser.close(); }

if (failures.length) process.exitCode = 1;
