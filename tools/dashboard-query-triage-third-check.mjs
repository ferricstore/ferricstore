import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import Module from 'node:module';

if (process.env.NODE_PATH) Module._initPaths();
const {chromium} = Module.createRequire(import.meta.url)('playwright');
const renderRoot = 'apps/ferricstore_server/lib/ferricstore_server/health/dashboard/render';
const controls = await fs.readFile(`${renderRoot}/flow_query_controls.ex`, 'utf8');
const filters = await fs.readFile(`${renderRoot}/flow_filters.ex`, 'utf8');
const forms = await fs.readFile(`${renderRoot}/flow_form_scripts.ex`, 'utf8');
const scriptFor = (source, marker) => {
  const tail = source.slice(source.indexOf(marker));
  return tail.match(/<script>([\s\S]*?)<\/script>/)[1];
};
const browser = await chromium.launch({channel: 'chrome', headless: true});
try {
  const page = await browser.newPage({viewport: {width: 1280, height: 720}});
  const errors = [];
  page.on('pageerror', error => errors.push(error.message));
  const imported = {fql: 'FROM runs WHERE state = @state ORDER BY updated_at_ms ASC LIMIT 7', params_json: '{"state":"failed"}'};
  await page.setContent(`<section data-flow-query-workbench>
    <button data-flow-query-mode-tab="guided">Guided draft</button>
    <button data-flow-query-mode-tab="advanced">Raw FQL draft</button>
    <div data-flow-query-mode="guided"><form data-flow-query-form><input name="state" value="failed"></form></div>
    <div data-flow-query-mode="advanced">
      <button data-flow-query-import='${JSON.stringify(imported)}'>Import</button>
      <span data-flow-query-import-status></span>
      <form data-flow-query-workbench-form><textarea name="fql">raw draft</textarea><textarea name="params_json">{}</textarea></form>
    </div>
    <script>${scriptFor(controls, 'def render_flow_query_mode_script').replace('#{active}', 'guided')}</script>
  </section>`);
  await page.locator('[data-flow-query-mode-tab="advanced"]').click();
  assert.equal(await page.locator('[name="fql"]').inputValue(), 'raw draft');
  page.once('dialog', dialog => dialog.dismiss());
  await page.locator('[data-flow-query-import]').click();
  assert.equal(await page.locator('[name="fql"]').inputValue(), 'raw draft');
  page.once('dialog', dialog => dialog.accept());
  await page.locator('[data-flow-query-import]').click();
  assert.equal(await page.locator('[name="fql"]').inputValue(), imported.fql);
  assert.equal(await page.locator('[name="params_json"]').inputValue(), imported.params_json);
  await page.locator('[data-flow-query-mode-tab="guided"]').click();
  await page.locator('[name="state"]').fill('queued');
  await page.locator('[data-flow-query-mode-tab="advanced"]').click();
  assert.equal(await page.locator('[data-flow-query-import]').isDisabled(), true);
  assert.equal(await page.locator('[name="fql"]').inputValue(), imported.fql);
  console.log('PASS independent drafts, deliberate exact import, reject overwrite, invalidate stale import');

  await page.setContent(`<form class="flow-state-filter-form">
    <select name="time_mode"><option value="all">All</option><option value="relative">Relative</option><option value="custom">Custom</option></select>
    <label data-flow-time-mode="relative"><select name="range"><option>15m</option></select></label>
    <label data-flow-time-mode="custom"><input name="from" type="datetime-local"></label>
    <label data-flow-time-mode="custom"><input name="to" type="datetime-local"></label>
  </form><script>${scriptFor(filters, 'defp render_state_time_validation_script')}</script>`);
  await page.locator('[name="time_mode"]').selectOption('all');
  assert.equal(await page.locator('[data-flow-time-mode]:visible').count(), 0);
  await page.locator('[name="time_mode"]').selectOption('relative');
  assert.equal(await page.locator('[name="range"]').isEnabled(), true);
  assert.equal(await page.locator('[data-flow-time-mode="custom"]:visible').count(), 0);
  await page.locator('[name="time_mode"]').selectOption('custom');
  assert.equal(await page.locator('[data-flow-time-mode="custom"]:visible').count(), 2);
  assert.equal(await page.locator('[name="range"]').isDisabled(), true);
  await page.locator('[name="from"]').fill('2026-09-10T10:00');
  await page.locator('[name="to"]').fill('2026-09-09T10:00');
  assert.equal(await page.locator('[name="to"]').evaluate(input => input.validity.customError), true);
  await page.locator('[name="time_mode"]').selectOption('all');
  assert.equal(await page.locator('form').evaluate(form => form.checkValidity()), true);
  assert.deepEqual(errors, []);
  console.log('PASS progressive time disclosure, disabled inactive fields, custom range validation');

  const scheduleInputs = ['cron', 'every_ms', 'delay_ms', 'at_utc', 'overlap_policy', 'max_fires', 'start_at_utc', 'end_at_utc', 'timezone'];
  await page.setContent(`<details id="flow-schedule-create-panel" open><form>
    <select name="schedule_kind"><option value="interval">Interval</option><option value="cron">Cron</option></select>
    ${scheduleInputs.map(name => `<label><input name="${name}" value="${name === 'every_ms' ? '60000' : ''}"></label>`).join('')}
    <section data-schedule-review>
      <input name="review_fingerprint" value="reviewed" data-schedule-review-field>
      <label><input name="confirm_replace" type="checkbox">Confirm replacement</label>
      <button type="submit" data-schedule-confirm>Replace</button>
    </section>
    <p data-schedule-review-stale hidden>Review again</p>
  </form></details><script>${scriptFor(forms, 'def schedule_script')}</script>`);
  await page.locator('[name="confirm_replace"]').check();
  assert.equal(await page.locator('[data-schedule-review]').isVisible(), true);
  await page.locator('[name="every_ms"]').fill('120000');
  assert.equal(await page.locator('[data-schedule-review]').isVisible(), false);
  assert.equal(await page.locator('[data-schedule-confirm]').isDisabled(), true);
  assert.equal(await page.locator('[name="review_fingerprint"]').isDisabled(), true);
  assert.equal(await page.locator('[data-schedule-review-stale]').isVisible(), true);
  assert.deepEqual(errors, []);
  console.log('PASS schedule definition edits invalidate confirmation and reviewed fields');
} finally {
  await browser.close();
}
