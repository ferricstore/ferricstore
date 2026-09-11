import Module from 'node:module';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import { spawnSync } from 'node:child_process';

if (process.env.NODE_PATH) Module._initPaths();
const require = Module.createRequire(import.meta.url);
const { chromium } = require('playwright');
const source = 'apps/ferricstore_server/lib/ferricstore_server/health/dashboard';
const render = spawnSync('elixir', ['-pa', '_build/test/lib/jason/ebin', '-r', `${source}/format.ex`, '-r', `${source}/render/recent_rates.ex`, '-r', `${source}/layout/styles.ex`, '-r', `${source}/layout.ex`, '-e', `
alias FerricstoreServer.Health.Dashboard.{Format, Layout}
IO.write(Jason.encode!(%{
  script: Layout.dashboard_live_script(), css: Layout.Styles.stylesheet(),
  help: Layout.render_keyboard_shortcuts_modal(), tooltip: Format.info_icon("The complete metric explanation remains available outside the table."),
  skip: if(function_exported?(Layout, :render_skip_link, 0), do: Layout.render_skip_link(), else: ""),
  times: [Format.format_timestamp_ms(1788949000001), Format.format_timestamp_ms(1788949000999), Format.format_timestamp_us(1788949000001001)]
}))`], { encoding: 'utf8', env: { ...process.env, ERL_FLAGS: '+S 2:2' } });
assert.equal(render.status, 0, render.stderr);
const assets = JSON.parse(render.stdout);
const out = process.env.DASHBOARD_OUT_DIR || 'test-results/dashboard-shell-hardening';
await fs.mkdir(out, { recursive: true });
const origin = 'http://dashboard-shell.test';
const path = '/dashboard/flow/fixture?partition_key=literal%20scope#journal-flow-event-selected';
const browser = await chromium.launch({ channel: 'chrome', headless: true });
const results = [];
const rows = Array.from({ length: 35 }, (_, i) => `<tr><td>row-${i}</td><td>${i}</td></tr>`).join('');
const component = revision => `<details data-dashboard-nav-group="fixture" open><summary>Group</summary><a href="/dashboard/flow">Overview</a></details><details data-dashboard-disclosure-key="extra"><summary>Details</summary><p>Details content</p></details><div class="table-scroll" data-dashboard-scroll-key="records" tabindex="0" style="height:180px"><table><thead><tr><th>Metric ${assets.tooltip}</th><th>Value</th></tr></thead><tbody>${rows}</tbody></table></div><span data-revision>${revision}</span>`;
const html = (live = true) => `<!doctype html><html><head><style>${assets.css}</style></head><body ${live ? 'data-dashboard-live-url="/dashboard/api/fixture" data-dashboard-live-interval-ms="500"' : ''}>${assets.skip}<nav class="sidebar"><a href="/dashboard/flow/query">Query</a><button data-dashboard-shortcuts-open>Keyboard help</button></nav>${assets.help}<main id="dashboard-main" tabindex="-1"><div class="subpage-header"><h1>Shell fixture</h1></div><div data-live-component="records">${component(0)}</div><form data-dashboard-single-submit action="/submit" method="post"><input name="draft" value="keep"><button type="submit" name="action" value="inspect">Inspect</button></form><div data-flow-action-snapshot-version="3"><p data-flow-action-stale hidden role="status">Actions changed. Review again.</p><form><input name="expected_version" value="3"><button type="submit">Rewind</button></form></div><div data-dashboard-workflow-scope hidden><a data-dashboard-route="/dashboard/flow/query" href="/dashboard/flow/query?type=literal%20type&amp;partition_key=literal%20scope">Query scope</a></div><div data-dashboard-filter-control><label>Filter <input data-dashboard-table-filter data-dashboard-filter-target="#filter-table"></label><span data-dashboard-filter-status role="status"></span></div><table id="filter-table"><thead><tr><th>Name</th></tr></thead><tbody><tr><td>alpha</td></tr><tr><td>beta</td></tr></tbody></table><a class="flow-value-ref-link" href="#flow-value-cmVm" data-flow-value-ref="ref">payload</a><div id="flow-value-cmVm" data-flow-value-ref="ref" data-flow-value-state="ready"><pre data-flow-value-preview>content</pre></div><dialog id="flow-value-modal" hidden aria-labelledby="flow-value-modal-title"><h2 id="flow-value-modal-title">Value</h2><span id="flow-value-modal-ref"></span><button data-flow-value-modal-close>Close value</button><pre id="flow-value-modal-body"></pre><span id="flow-value-modal-status" role="status"></span><button id="flow-value-modal-copy">Copy</button><button id="flow-value-modal-retry" hidden>Retry</button><span id="flow-value-modal-copy-status" role="status"></span></dialog></main>${assets.script}</body></html>`;

async function check(name, action, { live = true, init, overview = false, extraHtml = '' } = {}) {
  if (process.env.DASHBOARD_CHECK_FILTER && !name.includes(process.env.DASHBOARD_CHECK_FILTER)) return;
  const context = await browser.newContext({ viewport: { width: 1280, height: 900 } });
  const page = await context.newPage();
  page.setDefaultTimeout(2500);
  const errors = [];
  page.on('pageerror', error => errors.push(error.message));
  let revision = 0;
  let mode = 'success';
  let requests = 0;
  let extra = {};
  await page.clock.install();
  if (init) await page.addInitScript(init);
  await page.route(`${origin}/**`, async route => {
    const url = new URL(route.request().url());
    if (url.pathname === '/dashboard/api/fixture') {
      requests += 1;
      if (mode === 'hang') return;
      if (mode === 'unauthorized') return route.fulfill({ status: 401, body: '' });
      if (mode === 'error') return route.fulfill({ status: 503, body: '' });
      return route.fulfill({ json: { generated_at_ms: await page.evaluate(() => Date.now()), components: { records: component(revision) }, ...extra } });
    }
    let markup = overview ? html(live).replace(assets.skip, '<header><div class="top-bar"></div></header>' + assets.skip).replace('class="subpage-header"', 'class="fixture-heading"') : html(live);
    markup = markup.replace('</main>', extraHtml + '</main>').replace('<h2 id="flow-value-modal-title">', '<dl id="flow-value-modal-provenance"></dl><h2 id="flow-value-modal-title">');
    markup = markup.replace('<nav class="sidebar">', '<div class="layout"><nav class="sidebar">').replace('<main id="dashboard-main"', '<main class="main-content" id="dashboard-main"').replace('</main>', '</main></div>');
    await route.fulfill({ contentType: 'text/html', body: markup });
  });
  try {
    await page.goto(origin + path);
    if (live) await page.locator('[data-dashboard-live-status="live"]').waitFor();
    const controls = { setRevision: value => { revision = value; }, setMode: value => { mode = value; }, setExtra: value => { extra = value; }, requests: () => requests };
    const evidence = await action(page, controls, context);
    assert.deepEqual(errors, []);
    results.push({ name, status: 'passed', evidence });
    console.log(`PASS ${name}`);
  } catch (error) {
    results.push({ name, status: 'failed', error: error.message, errors });
    console.error(`FAIL ${name}: ${error.message}`);
    await page.screenshot({ path: `${out}/${name}-failed.png` });
  } finally { await context.close(); }
}

try {
  await check('history-event-survives-on-demand-value-request', async page => {
    let requested;
    await page.route('**/dashboard/api/flow/value?*', route => {
      requested = new URL(route.request().url());
      return route.fulfill({ json: { state: 'ready', value: 'historical content' } });
    });
    await page.evaluate(() => {
      document.querySelector('[data-flow-value-state]').dataset.flowValueState = 'unloaded';
      const current = new URL(location.href);
      current.searchParams.set('history_event', '123-2');
      current.searchParams.set('history_count', '5');
      history.replaceState(null, '', current);
    });
    const request = page.waitForRequest('**/dashboard/api/flow/value?*');
    await page.locator('.flow-value-ref-link').first().click();
    await request;
    assert.equal(requested.searchParams.get('history_event'), '123-2');
    assert.equal(requested.searchParams.get('history_count'), '5');
  }, { live: false });
  await check('review-rewind-hides-inapplicable-utc-field', async page => {
    assert.equal(await page.locator('[data-flow-rewind-time-field]').isVisible(),false);
    await page.locator('[data-flow-rewind-time-field]').evaluate(e=>{e.hidden=false;});
    assert.equal(await page.locator('[data-flow-rewind-time-field]').isVisible(),true);
  }, {live:false, extraHtml:'<label class="flow-policy-field" data-flow-rewind-time-field hidden><span>Run at UTC</span><input type="datetime-local"></label>'});
  await check('review-state-identity-does-not-overlap-counts', async page => {
    for (const cell of await page.locator('.flow-states-table tbody td').all()) {
      const geometry = await cell.evaluate(e => ({client:e.clientWidth,scroll:e.scrollWidth}));
      assert.ok(geometry.scroll <= geometry.client + 1, JSON.stringify(geometry));
    }
  }, {live:false, extraHtml:'<div class="table-scroll"><table class="flow-states-table"><thead><tr><th>Type</th><th>Runtime status</th><th>Failed</th></tr></thead><tbody><tr><td class="mono"><a href="#">dashboard_action_review_workflow_with_long_type</a></td><td class="mono"><a href="#">awaiting_customer_approval</a></td><td>7</td></tr></tbody></table></div>'});
  await check('review-value-hash-preserves-event-and-cache', async page => {
    let valueRequests = 0;
    page.on('request', request => { if (request.url().includes('/dashboard/api/flow/value')) valueRequests += 1; });
    for (const [anchor, expected] of [['flow-value-cmVm', 'Current record'], ['flow-value-cmVm:event:MTIzLTI', '123-2'], ['flow-value-cmVm:event:MTIzLTM', '123-3']]) {
      await page.goto(origin + '/dashboard/flow/fixture#' + anchor);
      await page.reload();
      assert.match(await page.locator('#flow-value-modal-provenance').textContent(), new RegExp(expected));
      assert.equal(await page.locator('#flow-value-modal-body').textContent(), 'content');
      await page.keyboard.press('Escape');
    }
    assert.equal(valueRequests, 0, 'historical provenance must reuse the cached value');
  }, {live:false, extraHtml:'<a class="flow-value-ref-link" href="#flow-value-cmVm:event:MTIzLTI" data-flow-value-ref="ref" data-flow-value-source="historical" data-flow-value-event="123-2">Historical first</a><a class="flow-value-ref-link" href="#flow-value-cmVm:event:MTIzLTM" data-flow-value-ref="ref" data-flow-value-source="historical" data-flow-value-event="123-3">Historical second</a><script>document.querySelector(".flow-value-ref-link").dataset.flowValueSource="current";</script>'});
  await check('review-action-anchor-opens-destination', async page => {
    await page.locator('[data-action-jump]').click();
    assert.equal(await page.locator('.flow-operations-panel').evaluate(e=>e.open), true);
    await page.goto(origin + '/dashboard/flow/fixture#workflow-actions');
    assert.equal(await page.locator('.flow-operations-panel').evaluate(e=>e.open), true);
  }, {live:false, extraHtml:'<a data-action-jump href="#workflow-actions">Actions</a><section id="workflow-actions"><details class="flow-operations-panel"><summary>Workflow Actions</summary><button>Review action</button></details></section>'});
  await check('review-value-provenance-follows-clicked-reference', async page => {
    await page.evaluate(() => {
      document.body.dataset.flowWorkflow = 'workflow<&';
      document.body.dataset.flowPartition = ' partition ';
      Object.assign(document.querySelector('.flow-value-ref-link').dataset, {
        flowValueSource:'historical',flowValueEvent:'123-2',flowValueAction:'<script>Signal</script>',flowValueTime:'2026-09-09 12:34:56.001 UTC'
      });
      document.querySelector('[data-flow-value-state]').dataset.flowValueSource = 'current';
    });
    await page.locator('.flow-value-ref-link').click();
    const provenance = page.locator('#flow-value-modal-provenance');
    assert.match(await provenance.textContent(), /Historical event/);
    assert.match(await provenance.textContent(), /workflow<&/);
    assert.match(await provenance.textContent(), / partition /);
    assert.match(await provenance.textContent(), /123-2/);
    assert.match(await provenance.textContent(), /<script>Signal<\/script>/);
    assert.equal(await provenance.locator('script').count(), 0);
    await page.keyboard.press('Escape');
    await page.locator('.flow-value-ref-link').evaluate(e=>{ e.dataset.flowValueSource='current'; });
    await page.locator('.flow-value-ref-link').click();
    assert.match(await provenance.textContent(), /Current record/);
    assert.doesNotMatch(await provenance.textContent(), /123-2|Historical event/);
  }, {live:false});
  await check('review-focused-component-retains-its-freshness', async (page, control) => {
    const root = page.locator('[data-dashboard-live-url]');
    const captured = await root.getAttribute('data-dashboard-live-last-update-ms');
    await page.locator('[data-live-component] a').focus();
    control.setRevision(1);
    await page.clock.runFor(1600);
    assert.equal(await page.locator('[data-revision]').textContent(), '0');
    assert.equal(await root.getAttribute('data-dashboard-live-last-update-ms'), captured);
    assert.equal(await page.locator('[data-dashboard-live-status]').getAttribute('data-dashboard-live-status'), 'paused');
    await page.locator('#dashboard-main').focus();
    await page.clock.runFor(600);
    await page.waitForFunction(() => document.querySelector('[data-revision]').textContent === '1');
    assert.equal(await page.locator('[data-dashboard-live-status]').getAttribute('data-dashboard-live-status'), 'live');
  });
  await check('review-metric-help-has-contextual-name', async page => {
    assert.equal(await page.locator('.info-icon').getAttribute('aria-label'), 'About Metric');
  }, { live: false });
  await check('review-shortcuts-dialog-is-centered', async page => {
    await page.locator('[data-dashboard-shortcuts-open]').click();
    const r = await page.locator('.keyboard-modal').boundingBox();
    assert.ok(r.width >= 400 && r.width <= 520);
    assert.ok(Math.abs(r.x + r.width / 2 - 640) <= 1);
    assert.ok(Math.abs(r.y + r.height / 2 - 450) <= 1);
  }, { live: false });
  await check('review-subpage-sidebar-uses-viewport', async page => {
    const r = await page.locator('.sidebar').boundingBox();
    assert.equal(r.y, 0);
    assert.equal(r.height, 900);
  }, { live: false });
  await check('review-input-boundary-and-focus-contrast', async page => {
    await page.locator('input[name=draft]').evaluate(input => { input.classList.add('flow-search-input'); });
    for (const queryField of [false, true]) {
    await page.locator('input[name=draft]').evaluate((input, enabled) => input.parentElement.classList.toggle('flow-query-field', enabled), queryField);
    await page.locator('#dashboard-main').focus();
    const contrast = await page.locator('input[name=draft]').evaluate(input => {
      const luminance = rgb => {
        const c = rgb.match(/[\d.]+/g).slice(0, 3).map(n => {
          const v = Number(n) / 255;
          return v <= 0.04045 ? v / 12.92 : ((v + 0.055) / 1.055) ** 2.4;
        });
        return c[0] * 0.2126 + c[1] * 0.7152 + c[2] * 0.0722;
      };
      const style = getComputedStyle(input);
      const a = luminance(style.borderTopColor), b = luminance(style.backgroundColor);
      return (Math.max(a, b) + 0.05) / (Math.min(a, b) + 0.05);
    });
    assert.ok(contrast >= 3, `field boundary contrast ${contrast}`);
    await page.locator('input[name=draft]').focus();
    const outline = await page.locator('input[name=draft]').evaluate(e => getComputedStyle(e).outlineStyle);
    assert.notEqual(outline, 'none');
    }
  }, { live: false });
  await check('hung-poll-enters-stale-and-retries', async (page, control) => {
    control.setMode('hang');
    await page.clock.runFor(600);
    await page.clock.runFor(17000);
    assert.equal(await page.locator('[data-dashboard-live-status]').getAttribute('data-dashboard-live-status'), 'stale');
    assert.equal(await page.locator('[data-dashboard-live-retry]').isVisible(), true);
    assert.doesNotMatch(await page.locator('[data-dashboard-live-status]').innerText(), /Updated just now/);
    control.setMode('success');
    await page.locator('[data-dashboard-live-retry]').click();
    await page.clock.runFor(1);
    await page.locator('[data-dashboard-live-status="live"]').waitFor();
    return { requests: control.requests() };
  });
  await check('patch-retains-scroll-and-both-disclosure-states', async (page, control) => {
    await page.locator('[data-dashboard-nav-group]').evaluate(e => { e.open = false; });
    await page.locator('[data-dashboard-disclosure-key]').evaluate(e => { e.open = true; });
    await page.locator('.table-scroll').evaluate(e => { e.scrollTop = 300; });
    control.setRevision(1);
    await page.clock.runFor(600);
    await page.waitForFunction(() => document.querySelector('[data-revision]').textContent === '1');
    assert.equal(await page.locator('[data-dashboard-nav-group]').evaluate(e => e.open), false);
    assert.equal(await page.locator('[data-dashboard-disclosure-key]').evaluate(e => e.open), true);
    assert.equal(await page.locator('.table-scroll').evaluate(e => e.scrollTop), 300);
  });
  await check('table-reading-and-explicit-pause', async (page, control) => {
    await page.locator('.table-scroll').focus();
    control.setRevision(1);
    await page.clock.runFor(600);
    await page.waitForFunction(() => document.querySelector('[data-revision]').textContent === '1');
    assert.equal(await page.locator('[data-revision]').textContent(), '1');
    assert.equal(await page.locator('.table-scroll').evaluate(e => e === document.activeElement), true);
    await page.locator('[data-dashboard-live-toggle]').click();
    const before = control.requests();
    await page.clock.runFor(6000);
    assert.equal(control.requests(), before);
    assert.match(await page.locator('[data-dashboard-live-status]').innerText(), /Updated [\d]+s ago/);
    await page.locator('[data-dashboard-live-toggle]').click();
    await page.clock.runFor(1);
    await page.locator('[data-dashboard-live-status="live"]').waitFor();
  });
  await check('tooltip-is-unclipped-and-header-name-concise', async page => {
    await page.locator('.info-icon').focus();
    const tooltip = page.locator('[data-dashboard-tooltip]');
    await tooltip.waitFor({ state: 'visible' });
    assert.equal(await page.getByRole('columnheader', { name: 'Metric', exact: true }).count(), 1);
    assert.equal(await tooltip.evaluate(e => !e.closest('.table-scroll')), true);
    assert.equal(await tooltip.evaluate(e => { const r = e.getBoundingClientRect(); return r.left >= 0 && r.right <= innerWidth && r.top >= 0 && r.bottom <= innerHeight; }), true);
    await page.keyboard.press('Escape');
    assert.equal(await tooltip.isVisible(), false);
  }, { live: false });
  await check('skip-link-focuses-main', async page => {
    await page.keyboard.press('Tab');
    assert.equal(await page.evaluate(() => document.activeElement.matches('.dashboard-skip-link')), true);
    await page.keyboard.press('Enter');
    assert.equal(await page.evaluate(() => document.activeElement.id), 'dashboard-main');
  }, { live: false });
  await check('skip-link-precedes-live-topbar-controls', async page => {
    await page.keyboard.press('Tab');
    assert.equal(await page.evaluate(() => document.activeElement.matches('.dashboard-skip-link')), true);
  }, { overview: true });
  await check('tooltip-position-respects-css-zoom', async page => {
    await page.evaluate(() => {
      document.documentElement.style.zoom = '2';
      Object.assign(document.querySelector('.info-icon').style, {position: 'fixed', left: '560px', top: '150px'});
    });
    await page.locator('.info-icon').focus();
    assert.equal(await page.locator('[data-dashboard-tooltip]').evaluate(e => {
      const r = e.getBoundingClientRect();
      return r.left >= 0 && r.right <= innerWidth && r.top >= 0 && r.bottom <= innerHeight;
    }), true);
  }, { live: false });
  await check('modal-isolates-shortcuts-and-setting-persists', async page => {
    await page.locator('[data-dashboard-shortcuts-open]').click();
    await page.keyboard.press('g');
    await page.keyboard.press('o');
    assert.equal(new URL(page.url()).pathname, '/dashboard/flow/fixture');
    await page.locator('[data-dashboard-character-shortcuts]').uncheck();
    await page.keyboard.press('Escape');
    await page.keyboard.press('g');
    await page.keyboard.press('o');
    assert.equal(new URL(page.url()).pathname, '/dashboard/flow/fixture');
    await page.reload();
    await page.locator('[data-dashboard-shortcuts-open]').click();
    assert.equal(await page.locator('[data-dashboard-character-shortcuts]').isChecked(), false);
  }, { live: false });
  await check('modified-payload-click-is-native', async page => {
    const intercepted = await page.locator('.flow-value-ref-link').evaluate(link => {
      const event = new MouseEvent('click', { bubbles: true, cancelable: true, metaKey: true, button: 0 });
      link.dispatchEvent(event);
      return event.defaultPrevented;
    });
    assert.equal(intercepted, false);
    assert.equal(await page.locator('#flow-value-modal').evaluate(e => e.open), false);
  }, { live: false });
  await check('auth-expiry-retains-anchor', async (page, control) => {
    control.setMode('unauthorized');
    await page.clock.runFor(600);
    await page.waitForURL('**/dashboard/login?*');
    assert.equal(new URL(page.url()).searchParams.get('next'), path);
  });
  await check('value-clipboard-failure-is-truthful', async page => {
    await page.locator('.flow-value-ref-link').click();
    await page.locator('#flow-value-modal-copy').click();
    await page.waitForFunction(() => document.querySelector('#flow-value-modal-copy-status').textContent.length > 0);
    assert.match(await page.locator('#flow-value-modal-copy-status').textContent(), /Copy failed/);
    assert.equal(await page.locator('#flow-value-modal-body').textContent(), 'content');
    assert.equal(await page.locator('#flow-value-modal-copy').evaluate(e => e === document.activeElement), true);
  }, { live: false, init: () => { Object.defineProperty(navigator, 'clipboard', { value: undefined }); document.execCommand = () => false; } });
  await check('literal-time-precision-and-utc', async () => {
    assert.notEqual(assets.times[0], assets.times[1]);
    assert.match(assets.times[0], /2026-09-09 10:16:40\.001 UTC/);
    assert.match(assets.times[1], /\.999 UTC/);
    assert.match(assets.times[2], /\.001001 UTC/);
  }, { live: false });
  await check('no-match-filter-has-message-and-count', async page => {
    await page.locator('[data-dashboard-table-filter]').fill('absent');
    assert.match(await page.locator('[data-table-filter-empty]').innerText(), /No loaded rows match/);
    assert.equal(await page.locator('[data-dashboard-filter-status]').innerText(), '0 of 2 loaded rows');
    await page.locator('[data-dashboard-table-filter]').fill('alpha');
    assert.equal(await page.locator('[data-table-filter-empty]:visible').count(), 0);
    assert.equal(await page.locator('[data-dashboard-filter-status]').innerText(), '1 of 2 loaded rows');
  }, { live: false });
  await check('single-submit-preserves-action', async page => {
    await page.route(`${origin}/submit`, async route => { await route.fulfill({ body: route.request().postData() }); });
    await page.locator('[data-dashboard-single-submit] button').click();
    await page.waitForURL('**/submit');
    assert.match(await page.locator('body').innerText(), /action=inspect/);
  }, { live: false });
  await check('server-scope-and-stale-actions-contract', async (page, control) => {
    assert.equal(await page.locator('.sidebar a').getAttribute('href'), '/dashboard/flow/query?type=literal%20type&partition_key=literal%20scope');
    await page.locator('[data-dashboard-workflow-scope] a').evaluate(link => {
      link.title = 'Query keeps the reviewed scope';
      link.setAttribute('aria-description', 'Exact time bounds are retained');
    });
    control.setExtra({ action_snapshot: { version: 4, state: 'running', available: true } });
    await page.clock.runFor(600);
    await page.locator('[data-flow-action-stale]').waitFor({ state: 'visible' });
    assert.equal(await page.locator('[data-flow-action-stale]').isVisible(), true);
    assert.equal(await page.locator('[data-flow-action-snapshot-version] button').isDisabled(), true);
    assert.equal(await page.locator('[name=expected_version]').inputValue(), '3');
    assert.equal(await page.locator('.sidebar a').getAttribute('title'), 'Query keeps the reviewed scope');
    assert.equal(await page.locator('.sidebar a').getAttribute('aria-description'), 'Exact time bounds are retained');
    await page.locator('[data-dashboard-workflow-scope] a').evaluate(link => {
      link.removeAttribute('title');
      link.removeAttribute('aria-description');
    });
    await page.clock.runFor(600);
    await page.waitForFunction(() => !document.querySelector('.sidebar a').hasAttribute('title'));
    assert.equal(await page.locator('.sidebar a').getAttribute('aria-description'), null);
  });
  await check('stale-actions-visible-in-closed-disclosure', async (page, control) => {
    await page.locator('[data-flow-action-snapshot-version]').evaluate(panel => {
      const details = document.createElement('details');
      details.dataset.flowActionSnapshotVersion = panel.dataset.flowActionSnapshotVersion;
      details.innerHTML = '<summary>Workflow Actions</summary>' + panel.innerHTML;
      panel.replaceWith(details);
    });
    control.setExtra({ action_snapshot: { version: 4, state: 'running', available: true } });
    await page.clock.runFor(600);
    await page.locator('[data-flow-action-stale-summary]').waitFor({ state: 'visible' });
    assert.equal(await page.locator('[data-flow-action-stale-summary]').innerText(), 'Review required');
    assert.equal(await page.locator('[data-flow-action-snapshot-version]').evaluate(panel => panel.open), false);
    assert.equal(await page.locator('[name=expected_version]').inputValue(), '3');
    assert.equal(await page.locator('[data-flow-action-snapshot-version] button').isDisabled(), true);
  });
} finally {
  await browser.close();
  await fs.writeFile(`${out}/results.json`, JSON.stringify(results, null, 2));
}
const failed = results.filter(result => result.status === 'failed');
console.log(`${results.length - failed.length}/${results.length} passed`);
if (failed.length) process.exitCode = 1;
