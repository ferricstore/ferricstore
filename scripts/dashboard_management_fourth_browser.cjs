const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { chromium } = require(process.env.TMPDIR + '/ferricstore-browser-tools/node_modules/playwright');

const fixtureDir = path.resolve(process.argv[2] || 'outputs/dashboard-fourth-fixes/management/fixtures');
const output = path.resolve(process.argv[3] || 'outputs/dashboard-fourth-fixes/management/browser');
fs.mkdirSync(output, { recursive: true });
const results = [];

(async () => {
  const browser = await chromium.launch({ channel: 'chrome', headless: true });
  try {
    async function test(name, fixture, run) {
      const context = await browser.newContext({ viewport: { width: 1440, height: 1000 } });
      const page = await context.newPage();
      const errors = [];
      const posts = [];
      page.on('pageerror', error => errors.push(error.message));
      await page.route('http://management.test/**', async route => {
        if (route.request().method() === 'POST') {
          posts.push(route.request().postData());
          await route.fulfill({ contentType: 'text/html', body: '<!doctype html><html><body><h1>Reviewed without mutation</h1></body></html>' });
        } else {
          await route.fulfill({ contentType: 'text/html', body: fs.readFileSync(path.join(fixtureDir, fixture + '.html'), 'utf8') });
        }
      });
      try {
        await page.goto('http://management.test/' + fixture, { waitUntil: 'load' });
        const evidence = await run(page, posts);
        assert.deepEqual(errors, [], 'No browser runtime errors');
        await page.screenshot({ path: path.join(output, name + '.png'), fullPage: true });
        results.push({ name, status: 'passed', evidence });
      } catch (error) {
        results.push({ name, status: 'failed', error: error.stack, pageErrors: errors });
        await page.screenshot({ path: path.join(output, name + '-failed.png'), fullPage: true }).catch(() => {});
      } finally {
        await context.close();
      }
    }

    await test('schedule-draft-navigation', 'schedules', async page => {
      await page.locator('#flow-schedule-create-panel > summary').click();
      const id = page.locator('#flow-schedule-create-panel input[name=id]');
      await id.fill('unfinished-customer-export');
      await page.locator('input[name=cron]').fill('0 9 * * *');
      await page.locator('textarea[name=target_payload]').fill('{"fixture":"private-draft"}');
      let dialogs = 0;
      page.on('dialog', async dialog => { assert.equal(dialog.type(), 'beforeunload'); dialogs++; await dialog.dismiss(); });
      await page.getByRole('button', { name: 'Filter', exact: true }).click({ noWaitAfter: true });
      await page.waitForTimeout(100);
      assert.equal(dialogs, 1, 'Catalog Filter must warn about lost creation edits');
      assert.equal(await id.inputValue(), 'unfinished-customer-export');
      assert.equal(await page.locator('textarea[name=target_payload]').inputValue(), '{"fixture":"private-draft"}');
      assert.equal(await page.locator('[data-schedule-dirty-status]').isVisible(), true);
      const storage = await page.evaluate(() => ({ local: { ...localStorage }, session: { ...sessionStorage } }));
      assert.equal(JSON.stringify(storage).includes('private-draft'), false);
      return { dialogs, retained: true, storage };
    });

    await test('schedule-server-error-focus', 'schedule-error', async page => {
      assert.equal(await page.evaluate(() => document.activeElement.name), 'target_payload');
      const payload = page.locator('textarea[name=target_payload]');
      assert.equal(await payload.inputValue(), '{invalid');
      assert.equal(await payload.getAttribute('aria-invalid'), 'true');
      assert.equal(await payload.getAttribute('aria-describedby'), 'schedule-create-target_payload-error');
      assert.equal(await page.locator('#schedule-create-target_payload-error').isVisible(), true);
      let dialogs = 0;
      page.on('dialog', async dialog => { dialogs++; await dialog.dismiss(); });
      await page.getByRole('button', { name: 'Filter', exact: true }).click({ noWaitAfter: true });
      await page.waitForTimeout(100);
      assert.equal(dialogs, 1, 'A retained server draft also needs discard protection');
      return { associated: true, dialogs };
    });

    await test('schedule-json-validation-and-review', 'schedules', async (page, posts) => {
      await page.locator('#flow-schedule-create-panel > summary').click();
      await page.locator('#flow-schedule-create-panel input[name=id]').fill('new-customer-export');
      await page.locator('input[name=cron]').fill('0 9 * * *');
      await page.locator('input[name=target_type]').fill('customer-export');
      const payload = page.locator('textarea[name=target_payload]');
      await payload.fill('{invalid');
      await page.getByRole('button', { name: 'Review schedule', exact: true }).click();
      assert.equal(posts.length, 0);
      assert.equal(await page.evaluate(() => document.activeElement.name), 'target_payload');
      assert.equal(await payload.getAttribute('aria-invalid'), 'true');
      assert.equal(await page.locator('#schedule-create-target_payload-error').isVisible(), true);
      await payload.fill('{"region":"eu"}');
      let dialogs = 0;
      page.on('dialog', async dialog => { dialogs++; await dialog.dismiss(); });
      await page.getByRole('button', { name: 'Review schedule', exact: true }).click();
      await page.getByRole('heading', { name: 'Reviewed without mutation' }).waitFor();
      assert.equal(posts.length, 1);
      assert.equal(dialogs, 0, 'Intentional review submission must not trigger discard protection');
      assert.equal(new URLSearchParams(posts[0]).get('preview'), 'true');
      return { invalidPostCount: 0, validReviewPostCount: posts.length, dialogs };
    });

    await test('policy-numeric-contract', 'policies', async page => {
      const form = page.locator('[data-policy-editor]');
      const retries = form.locator('[name=max_retries]');
      const observations = [];
      for (const value of ['1e3', '1.5', '-1', '']) {
        await retries.fill(value);
        const result = await retries.evaluate(input => ({ value: input.value, valid: input.form.checkValidity(), invalid: input.getAttribute('aria-invalid'), message: input.validationMessage }));
        assert.equal(result.valid, false);
        assert.equal(result.invalid, 'true');
        assert.ok(result.message.length > 0);
        assert.equal(await page.locator('#policy-max_retries-error').isVisible(), true);
        assert.match(await page.locator('[data-policy-preview=retry]').innerText(), /invalid retries/);
        observations.push(result);
      }
      await retries.fill('1000');
      assert.equal(await form.evaluate(element => element.checkValidity()), true);
      assert.equal(await retries.getAttribute('aria-invalid'), 'false');
      assert.equal(await page.locator('#policy-max_retries-error').isVisible(), false);
      for (const [name, value] of [['jitter_pct', '101'], ['max_active_ms', '31536000001'], ['retention_ttl_ms', '0'], ['history_max_events', '0']]) {
        const field = form.locator('[name=' + name + ']');
        const original = await field.inputValue();
        await field.fill(value);
        assert.equal(await field.evaluate(input => input.checkValidity()), false);
        assert.equal(await field.getAttribute('aria-invalid'), 'true');
        await field.fill(original);
        assert.equal(await field.evaluate(input => input.checkValidity()), true);
      }
      await form.locator('[name=max_active_ms]').fill('');
      assert.equal(await form.evaluate(element => element.checkValidity()), true);
      assert.equal(await page.locator('[data-policy-preview=max-active]').innerText(), 'unlimited');
      return observations;
    });

    await test('policy-server-error-focus', 'policy-error', async page => {
      assert.equal(await page.evaluate(() => document.activeElement.name), 'max_retries');
      assert.equal(await page.locator('[name=max_retries]').inputValue(), '1e3');
      assert.equal(await page.locator('[data-policy-editor]').evaluate(form => form.checkValidity()), false);
      return { focus: 'max_retries', retained: '1e3' };
    });

    await test('policy-visible-edit-link', 'policies', async page => {
      const link = page.locator('#flow-policy-catalog tbody tr:first-child td:first-child a');
      await link.scrollIntoViewIfNeeded();
      const box = await link.boundingBox();
      assert.ok(box.x >= 220 && box.x + box.width <= 1440, 'Type edit link stays in the initial horizontal viewport');
      assert.match(await link.getAttribute('href'), /edit=customer-reconciliation&edit_state=#flow-policy-editor/);
      return box;
    });

    await test('schedule-stable-definition', 'schedules', async page => {
      const table = page.locator('.flow-schedules-table');
      const widths = () => table.locator('tbody > tr:first-child > td').evaluateAll(cells => cells.map(cell => ({ x: cell.getBoundingClientRect().x, width: cell.getBoundingClientRect().width })));
      const before = await widths();
      const details = page.locator('.flow-schedule-definition-row').first();
      assert.equal(await details.locator('td').getAttribute('colspan'), '12');
      await details.locator('summary').click();
      const after = await widths();
      before.forEach((cell, index) => {
        assert.ok(Math.abs(cell.x - after[index].x) < 1, 'Primary column positions remain stable');
        assert.ok(Math.abs(cell.width - after[index].width) < 1, 'Primary column widths remain stable');
      });
      assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), true);
      return { before, after };
    });

    await test('schedule-visible-filter-labels', 'schedules', async page => {
      await page.setViewportSize({ width: 1280, height: 800 });
      const form = page.getByRole('form', { name: 'Schedule filters', exact: true });
      const labels = await form.locator('label > span').allTextContents();
      assert.deepEqual(labels, ['ID contains', 'State', 'Kind', 'Limit']);
      for (const label of await form.locator('label').all()) assert.equal(await label.isVisible(), true);
      assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), true);
      return labels;
    });

    await test('retention-truthful-preview', 'retention', async page => {
      assert.equal(await page.getByRole('button', { name: 'Refresh sampled preview', exact: true }).count(), 1);
      assert.match(await page.locator('[role=status]').innerText(), /does not apply the per-shard cleanup limit or simulate global cleanup/);
      assert.match(await page.locator('[aria-label="Retention sample metrics"]').innerText(), /Pending index operations\s+17/i);
      assert.equal(await page.locator('input[name=confirm_cleanup]').count(), 1);
      return { pending: 17, label: 'Pending index operations', previewDoesNotSimulate: true };
    });
  } finally {
    await browser.close();
  }
  fs.writeFileSync(path.join(output, 'results.json'), JSON.stringify(results, null, 2));
  console.log(JSON.stringify(results, null, 2));
  if (results.some(result => result.status === 'failed')) process.exitCode = 1;
})().catch(error => { console.error(error); process.exitCode = 1; });
