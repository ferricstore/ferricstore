import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import Module from 'node:module';

if (process.env.NODE_PATH) Module._initPaths();
const {chromium} = Module.createRequire(import.meta.url)('playwright');
const base = process.env.DASHBOARD_URL || 'http://localhost:4000';
const out = process.env.DASHBOARD_OUT_DIR || 'outputs/dashboard-third-fixes/schedules';
const id = `schedule-third-review-${Date.now()}`;
const future = new Date(Date.now() + 86_400_000).toISOString().slice(0, 19);
const results = [];
await fs.mkdir(out, {recursive: true});
const browser = await chromium.launch({channel: 'chrome', headless: true});
const context = await browser.newContext({viewport: {width: 1280, height: 900}});
const page = await context.newPage();
const errors = [];
page.on('pageerror', error => errors.push(error.message));

async function submit(button, expectedStatus) {
  const response = page.waitForResponse(response => response.request().method() === 'POST' && new URL(response.url()).pathname === '/dashboard/flow/schedules');
  await button.click();
  assert.equal((await response).status(), expectedStatus);
  await page.waitForLoadState('load');
}

async function inspectFixture(target = page) {
  const response = await target.goto(`${base}/dashboard/flow/schedules?id=${encodeURIComponent(id)}`);
  assert.equal(response.status(), 200);
  return target.locator('tbody tr').filter({has: target.locator(`input[name="id"][value="${id}"]`)});
}

async function fillInterval(replace = false) {
  const panel = page.locator('#flow-schedule-create-panel');
  if ((await panel.getAttribute('open')) === null) await panel.locator(':scope > summary').click();
  const form = panel.locator('form');
  await form.locator('[name="id"]').fill(id);
  await form.locator('[name="schedule_kind"]').selectOption('interval');
  await form.locator('[name="every_ms"]').fill(replace ? '120000' : '60000');
  await form.locator('[name="target_type"]').fill(replace ? 'schedule-review-after' : 'schedule-review-before');
  await form.locator('[name="target_partition"]').fill('disposable-third-review');
  await form.locator('[name="start_at_utc"]').fill(future);
  if (replace) await form.locator('[name="overwrite"]').check();
  return form;
}

try {
  await page.goto(`${base}/dashboard/flow/schedules`);
  let form = await fillInterval();
  await submit(form.getByRole('button', {name: 'Review schedule', exact: true}), 200);
  await submit(page.locator('[data-schedule-confirm]'), 302);
  let row = await inspectFixture();
  assert.equal(await row.count(), 1);
  await submit(row.getByRole('button', {name: 'Pause', exact: true}), 302);
  row = await inspectFixture();
  assert.equal(await row.getByRole('button', {name: 'Resume', exact: true}).count(), 1);

  form = await fillInterval(true);
  await submit(form.getByRole('button', {name: 'Review schedule', exact: true}), 200);
  let review = page.locator('[data-schedule-review]');
  assert.match(await review.innerText(), /Current definition[\s\S]*Replacement definition/);
  assert.match(await review.innerText(), /Paused schedule will become active/);
  assert.match(await review.innerText(), /Fire count resets to 0/);
  assert.match(await review.innerText(), /60000 ms[\s\S]*120000 ms/);
  assert.match(await review.innerText(), /Next fire \(UTC\)/);
  await review.scrollIntoViewIfNeeded();
  assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), true);
  await page.screenshot({path: `${out}/replacement-review.png`, fullPage: true});

  await page.locator('#flow-schedule-create-panel [name="every_ms"]').fill('180000');
  assert.equal(await review.isVisible(), false);
  assert.equal(await page.locator('[data-schedule-confirm]').isDisabled(), true);
  assert.equal(await page.locator('[data-schedule-review-stale]').isVisible(), true);
  await page.locator('#flow-schedule-create-panel [name="every_ms"]').fill('120000');
  await submit(page.getByRole('button', {name: 'Review schedule', exact: true}), 200);
  review = page.locator('[data-schedule-review]');
  await review.locator('[name="confirm_replace"]').check();
  await submit(review.getByRole('button', {name: 'Confirm replacement', exact: true}), 302);
  row = await inspectFixture();
  assert.match(await row.innerText(), /schedule-review-after/);
  assert.equal(await row.getByRole('button', {name: 'Pause', exact: true}).count(), 1);
  await row.getByText('Schedule definition', {exact: true}).click();
  assert.match(await row.innerText(), /120000/);
  results.push({name: 'paused replacement review, invalidation and confirmed mutation', passed: true, id});

  const noJs = await browser.newContext({javaScriptEnabled: false, viewport: {width: 1280, height: 900}});
  const noJsPage = await noJs.newPage();
  await noJsPage.goto(`${base}/dashboard/flow/schedules`);
  await noJsPage.locator('#flow-schedule-create-panel > summary').click();
  const noJsForm = noJsPage.locator('#flow-schedule-create-panel form');
  await noJsForm.locator('[name="id"]').fill(`${id}-nojs-preview-only`);
  await noJsForm.locator('[name="cron"]').fill('0 9 * * *');
  await noJsForm.locator('[name="timezone"]').fill('Asia/Jerusalem');
  await noJsForm.locator('[name="target_type"]').fill('schedule-review-preview-only');
  const previewResponse = noJsPage.waitForResponse(response => response.request().method() === 'POST');
  await noJsForm.getByRole('button', {name: 'Review schedule', exact: true}).click();
  assert.equal((await previewResponse).status(), 200);
  await noJsPage.waitForLoadState('load');
  assert.match(await noJsPage.locator('[data-schedule-review]').innerText(), /Next fire \(selected timezone\)[\s\S]*Asia\/Jerusalem/);
  await noJsPage.locator('[data-schedule-review]').scrollIntoViewIfNeeded();
  await noJsPage.screenshot({path: `${out}/cron-review-no-js.png`, fullPage: true});
  await noJs.close();
  results.push({name: 'server-rendered cron preview without JavaScript', passed: true});
  assert.deepEqual(errors, []);
} catch (error) {
  results.push({name: 'schedule review scenario', passed: false, error: error.stack});
  await page.screenshot({path: `${out}/failure.png`, fullPage: true});
  process.exitCode = 1;
} finally {
  try {
    const row = await inspectFixture();
    if (await row.count()) {
      const action = row.locator('details.flow-action-confirm').filter({hasText: 'Confirm Delete'});
      if (await action.count()) {
        await action.locator(':scope > summary').click();
        await submit(action.getByRole('button', {name: 'Confirm Delete', exact: true}), 302);
        await page.goto(`${base}/dashboard/flow/schedules?id=${encodeURIComponent(id)}`);
        assert.match(await page.locator('tbody').innerText(), /cancelled/);
        results.push({name: 'disposable fixture cancelled', passed: true, id});
      }
    }
  } catch (error) {
    results.push({name: 'fixture cleanup', passed: false, id, error: error.stack});
    process.exitCode = 1;
  }
  await browser.close();
  await fs.writeFile(`${out}/results.json`, JSON.stringify({results, errors}, null, 2));
  for (const result of results) console.log(`${result.passed ? 'PASS' : 'FAIL'} ${result.name}`);
}
