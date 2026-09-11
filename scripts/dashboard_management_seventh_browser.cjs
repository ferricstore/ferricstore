const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { chromium } = require(process.env.TMPDIR + '/ferricstore-browser-tools/node_modules/playwright');

const fixtureDir = path.resolve(process.argv[2] || 'outputs/dashboard-seventh-fixes/management/fixtures');
const output = path.resolve(process.argv[3] || 'outputs/dashboard-seventh-fixes/management/browser');
fs.mkdirSync(output, { recursive: true });
const results = [];
const assets = JSON.parse(fs.readFileSync(path.join(fixtureDir, 'assets.json'), 'utf8'));

(async () => {
  const browser = await chromium.launch({ channel: 'chrome', headless: true });
  async function check(name, fixture, run, width = 1440, javaScriptEnabled = true) {
    if (process.env.MANAGEMENT_CHECK_FILTER && !name.includes(process.env.MANAGEMENT_CHECK_FILTER)) return;
    const context = await browser.newContext({ viewport: { width, height: 1000 }, javaScriptEnabled });
    const page = await context.newPage();
    const errors = [], posts = [];
    page.on('pageerror', error => errors.push(error.message));
    await page.route('http://management-seventh.test/**', async route => {
      const url = new URL(route.request().url());
      if (assets[url.pathname]) return route.fulfill({ contentType: assets[url.pathname].content_type, body: assets[url.pathname].body });
      if (route.request().method() === 'POST') {
        posts.push(new URLSearchParams(route.request().postData()));
        return route.fulfill({ contentType: 'text/html', body: '<!doctype html><html><body><h1>Submitted without mutation</h1></body></html>' });
      }
      const body = url.pathname === '/' + fixture
        ? fs.readFileSync(path.join(fixtureDir, fixture + '.html'), 'utf8')
        : '<!doctype html><html><body><h1>Reloaded scope</h1></body></html>';
      return route.fulfill({ contentType: 'text/html', body });
    });
    try {
      await page.goto('http://management-seventh.test/' + fixture);
      const evidence = await run(page, posts);
      assert.deepEqual(errors, [], 'No runtime exceptions');
      await page.screenshot({ path: path.join(output, name + '.png'), fullPage: true });
      results.push({ name, status: 'passed', evidence });
      console.log('PASS ' + name);
    } catch (error) {
      results.push({ name, status: 'failed', error: error.stack, pageErrors: errors });
      console.error('FAIL ' + name + ': ' + error.message);
      await page.screenshot({ path: path.join(output, name + '-failed.png'), fullPage: true }).catch(() => {});
    } finally { await context.close(); }
  }

  try {
    for (const width of [1280, 1440, 1920, 640]) {
      for (const fixture of ['schedules-edit', 'policies', 'retention-review']) {
        await check(fixture + '-' + width, fixture, async page => {
          assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth + 1), true, 'No document overflow');
          const bad = await page.locator('.flow-management-group input:visible, .flow-management-group select:visible, .flow-management-group textarea:visible').evaluateAll(nodes => nodes.filter(node => {
            const a = node.getBoundingClientRect(), b = node.closest('.flow-management-group').getBoundingClientRect();
            return a.left < b.left - 1 || a.right > b.right + 1;
          }).map(node => node.name));
          assert.deepEqual(bad, [], 'Fields stay inside their semantic group');
          return { width, documentOverflow: false, overflowingFields: bad };
        }, width);
      }
    }

    await check('retention-review-definition-grid', 'retention-review', async page => {
      const geometry = await page.locator('.flow-definition-list').evaluate(node => {
        const style = getComputedStyle(node);
        const term = node.querySelector('dt').getBoundingClientRect();
        const value = node.querySelector('dd').getBoundingClientRect();
        return { display: style.display, rowGap: parseFloat(style.rowGap), columnGap: parseFloat(style.columnGap), termRight: term.right, valueLeft: value.left, termTop: term.top, valueTop: value.top };
      });
      assert.equal(geometry.display, 'grid', 'Review scope and impact use a deliberate definition grid');
      assert.ok(geometry.rowGap >= 6 && geometry.columnGap >= 12, 'Separate review facts with stable gaps');
      assert.ok(geometry.valueLeft > geometry.termRight, 'Labels and values occupy distinct columns');
      assert.ok(Math.abs(geometry.termTop - geometry.valueTop) <= 1, 'Each value aligns with its label');
      return geometry;
    });

    await check('schedule-exact-units-and-review-context', 'schedules-edit', async (page, posts) => {
      const form = page.locator('#flow-schedule-create-panel form');
      assert.equal(await form.locator('[name=every_ms]').inputValue(), '60001');
      assert.equal(await form.locator('[name=every_ms_unit]').inputValue(), 'milliseconds');
      assert.equal(await page.locator('[data-schedule-dirty-status]').isVisible(), false);
      await form.locator('[name=every_ms_unit]').selectOption('seconds');
      assert.equal(await form.locator('[name=every_ms]').inputValue(), '60.001');
      await page.getByRole('button', { name: 'Review schedule', exact: true }).click();
      await page.getByRole('heading', { name: 'Submitted without mutation' }).waitFor();
      assert.equal(posts.length, 1);
      assert.equal(posts[0].get('original_version'), '7');
      assert.equal(posts[0].get('original_state'), 'paused');
      assert.equal(posts[0].get('editing'), 'true');
      assert.equal(posts[0].get('return_id'), 'customer-reconciliation-europe');
      assert.equal(posts[0].get('every_ms'), '60.001');
      assert.equal(posts[0].get('every_ms_unit'), 'seconds');
      return { originalVersion: 7, exactMs: 60001, postedReview: true };
    });

    await check('schedule-kind-hides-irrelevant-fields', 'schedules-edit', async page => {
      await page.locator('[name=schedule_kind]').selectOption('delay');
      assert.equal(await page.locator('[data-schedule-recurrence]').isVisible(), false);
      assert.equal(await page.locator('[name=start_at_utc]').isVisible(), false);
      assert.equal(await page.locator('[name=end_at_utc]').isVisible(), false);
      assert.equal(await page.locator('[name=every_ms_unit]').isEnabled(), false);
      assert.equal(await page.locator('[name=delay_ms_unit]').isEnabled(), true);
      return { recurrenceHidden: true, inactiveUnitsDisabled: true };
    });

    await check('policy-human-units-and-complete-overrides', 'policies', async page => {
      assert.equal(await page.locator('[name=retention_ttl_ms]').inputValue(), '7');
      assert.equal(await page.locator('[name=retention_ttl_ms_unit]').inputValue(), 'days');
      const panel = page.locator('.flow-policy-overrides');
      await panel.locator('summary').click();
      await panel.locator('[data-policy-override-search]').fill('last<&state');
      const rows = panel.locator('[data-policy-override-name]:visible');
      assert.equal(await rows.count(), 1);
      const href = await rows.locator('a').getAttribute('href');
      assert.equal(new URL(href, 'http://management-seventh.test').searchParams.get('edit_state'), 'last<&state');
      return { retentionDays: 7, finalOverrideFound: true };
    });

    await check('returned-policy-draft-and-explicit-discard', 'policy-error', async page => {
      assert.equal(await page.locator('[data-policy-dirty-status]').isVisible(), true);
      const dialogs = [];
      page.on('dialog', async dialog => {
        dialogs.push(dialog.type());
        if (dialog.type() === 'confirm') await dialog.accept(); else await dialog.dismiss();
      });
      await page.locator('input[name=max_retries]').click();
      await page.locator('form[aria-label="Policy scope"] button').click({ noWaitAfter: true });
      await page.waitForTimeout(100);
      assert.deepEqual(dialogs, ['beforeunload']);
      assert.equal(await page.locator('[name=max_retries]').inputValue(), '9');
      await page.locator('[data-discard-draft]').click();
      await page.getByRole('heading', { name: 'Reloaded scope' }).waitFor();
      assert.deepEqual(dialogs, ['beforeunload', 'confirm']);
      return { warnedBeforeTyping: true, explicitDiscard: true, dialogs };
    });

    await check('cleanup-review-confirmation-and-exact-limit', 'retention-review', async (page, posts) => {
      await page.getByRole('button', { name: 'Run Cleanup', exact: true }).click();
      assert.equal(posts.length, 0);
      await page.locator('[name=confirm_cleanup]').check();
      await page.getByRole('button', { name: 'Run Cleanup', exact: true }).click();
      await page.getByRole('heading', { name: 'Submitted without mutation' }).waitFor();
      assert.equal(posts[0].get('limit'), '1');
      assert.equal(posts[0].get('reviewed_limit'), '1');
      assert.equal(posts[0].get('confirm_cleanup'), 'true');
      return { unconfirmedPosts: 0, exactGlobalLimit: 1 };
    });

    await check('cleanup-invalid-limit-retained', 'retention-error', async page => {
      assert.equal(await page.locator('[name=limit]').inputValue(), 'not-a-number');
      assert.equal(await page.getByRole('button', { name: 'Run Cleanup', exact: true }).count(), 0);
      return { retained: true, requiresNewReview: true };
    });

    await check('read-only-schedule-no-editor', 'schedules-view-only', async page => {
      assert.equal(await page.locator('#flow-schedule-create-panel').count(), 0);
      assert.equal(await page.getByRole('link', { name: 'Edit', exact: true }).count(), 0);
      return { mutationControls: false };
    });

    await check('no-js-grouped-schedule-definition', 'schedules-edit', async page => {
      assert.equal(await page.locator('[name=every_ms]').inputValue(), '60001');
      assert.equal(await page.locator('[name=every_ms_unit]').inputValue(), 'milliseconds');
      assert.equal(await page.locator('[name=delay_ms]').isEnabled(), false);
      assert.equal(await page.locator('[name=timezone]').isVisible(), false);
      return { exactMs: 60001, nativeUnitControl: true };
    }, 1280, false);
  } finally {
    await browser.close();
    fs.writeFileSync(path.join(output, 'results.json'), JSON.stringify(results, null, 2));
  }
  if (results.some(row => row.status === 'failed')) process.exitCode = 1;
})();
