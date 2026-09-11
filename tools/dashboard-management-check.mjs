import assert from "node:assert/strict";
import { mkdir, writeFile } from "node:fs/promises";
import { createRequire } from "node:module";
import Module from "node:module";
if (process.env.NODE_PATH) Module._initPaths();
const { chromium } = createRequire(import.meta.url)("playwright");
const base = process.env.DASHBOARD_URL || "http://localhost:4000";
const out = process.env.DASHBOARD_OUT_DIR || "test-results/dashboard-management";
await mkdir(out, {recursive:true});
const browser = await chromium.launch({channel:"chrome", headless:process.env.HEADFUL !== "1"});
const results = [];
async function check(name, fn, options = {}) {
  if (process.env.DASHBOARD_CHECK && !name.includes(process.env.DASHBOARD_CHECK)) return;
  const context = await browser.newContext({viewport:{width:1440,height:1000}, ...options});
  const page = await context.newPage();
  page.setDefaultTimeout(5000);
  page.setDefaultNavigationTimeout(5000);
  const errors = [];
  page.on("pageerror", error => errors.push(error.message));
  try {
    await fn(page);
    assert.deepEqual(errors, []);
    results.push({name, passed:true});
    console.log(`PASS ${name}`);
  } catch(error) {
    results.push({name, passed:false, error:error.stack});
    console.error(`FAIL ${name}: ${error.message}`);
  } finally {
    await writeFile(`${out}/report.json`, JSON.stringify(results,null,2));
    await page.unrouteAll({behavior:'wait'});
    await context.close();
  }
}

try {
  await check("Due comparison stays compact across desktop widths and refreshes exact counts", async page => {
    for (const width of [1280, 1440, 1920]) {
      await page.setViewportSize({width, height:900});
      await page.goto(base + '/dashboard/flow/due');
      const summary = page.getByRole('region', {name:'Sampled due and scheduled work', exact:true});
      assert.ok((await summary.boundingBox()).height < 160, 'two-count comparison consumes too much space');
      assert.equal(await summary.locator('.chart-card, .chart-grid').count(), 0);
      assert.equal(await summary.locator('.chart-bar-value').count(), 2);
      assert.ok((await summary.innerText()).includes('does not establish claimability'));
      assert.ok(await page.evaluate(()=>document.documentElement.scrollWidth <= innerWidth));
    }
    const liveUrl = new URL(await page.locator('body').getAttribute('data-dashboard-live-url'), base).href;
    await page.route(liveUrl, async route => {
      const response = await route.fetch();
      const payload = await response.json();
      payload.components.flow_due_chart += '<span data-due-refreshed></span>';
      await route.fulfill({response, json:payload});
    });
    await page.waitForSelector('[data-due-refreshed]', {state:'attached'});
    assert.equal(await page.locator('.flow-due-summary .chart-bar-value').count(), 2);
    await page.setViewportSize({width:1440,height:900});
    await page.screenshot({path:`${out}/due-compact.png`});
  });

  await check("worker lease details support keyboard inspection without reads or column shifts", async page => {
    let valueRequests = 0;
    page.on('request', request => { if(request.url().includes('/value?')) valueRequests++; });
    await page.goto(base + '/dashboard/flow/workers');
    const row = page.locator('.flow-worker-records-table tbody tr').filter({has:page.getByRole('link',{name:'fifo-invoice-01',exact:true})});
    const details = row.locator('.flow-worker-lease-details');
    const control = details.locator('summary');
    assert.equal(await row.locator('td').count(), 4);
    assert.equal(await details.getAttribute('open'), null);
    assert.equal(await details.locator('dl').isVisible(), false);
    assert.ok((await row.innerText()).includes('billing-worker-customer-1042'));
    assert.ok((await row.innerText()).includes('customer-1042'));
    const before = await row.locator('td').evaluateAll(nodes => nodes.map(node => node.getBoundingClientRect().width));
    const liveUrl = new URL(await page.locator('body').getAttribute('data-dashboard-live-url'), base).href;
    let polls = 0;
    await page.route(liveUrl, async route => {
      polls++;
      const response = await route.fetch();
      const payload = await response.json();
      payload.components.flow_running_records += '<span data-worker-records-refreshed></span>';
      await route.fulfill({response, json:payload});
    });
    await control.focus();
    await page.keyboard.press('Enter');
    assert.ok(await details.locator('dl').isVisible());
    assert.ok((await details.locator('dd').first().innerText()).includes('billing-worker-customer-1042'));
    assert.equal(await details.locator('dd').nth(1).innerText(), '1');
    const after = await row.locator('td').evaluateAll(nodes => nodes.map(node => node.getBoundingClientRect().width));
    assert.deepEqual(after, before);
    await page.waitForTimeout(2300);
    assert.equal(polls, 0);
    assert.ok(await control.evaluate(node => node === document.activeElement));
    await page.locator('.subpage-title').click();
    await page.waitForSelector('[data-worker-records-refreshed]', {state:'attached'});
    assert.equal(await details.getAttribute('open'), '');
    assert.equal(valueRequests, 0);
    for (const width of [1280,1440,1920]) {
      await page.setViewportSize({width,height:900});
      await control.focus();
      await details.locator('dd').first().evaluate(node => {node.textContent = 'lease-' + 'x'.repeat(1024);});
      assert.ok(await page.evaluate(()=>document.documentElement.scrollWidth <= innerWidth));
      assert.ok(await details.evaluate(node=>node.scrollWidth <= node.clientWidth));
    }
    await page.setViewportSize({width:1440,height:900});
    await page.goto(base + '/dashboard/flow/workers');
    await page.screenshot({path:`${out}/workers-compact.png`});
    await row.locator('summary').click();
    await page.screenshot({path:`${out}/worker-lease-details.png`});
  });

  await check("States rejects reversed dates in the browser and recovers after correction", async page => {
    await page.goto(base + "/dashboard/flow/states?type=invoice_dispatch");
    const form = page.locator('.flow-state-filter-form');
    await form.locator('[name="from"]').fill('2026-09-08T00:00');
    await form.locator('[name="to"]').fill('2026-09-01T00:00');
    assert.equal(await form.evaluate(node => node.checkValidity()), false);
    assert.equal(await form.locator('[name="to"]').evaluate(node => node.validationMessage), 'From UTC must not be later than To UTC');
    await form.locator('[name="range"]').selectOption('1h');
    assert.equal(await form.evaluate(node => node.checkValidity()), true);
    await form.locator('[name="range"]').selectOption('');
    await form.locator('[name="to"]').fill('2026-09-09T00:00');
    assert.equal(await form.evaluate(node => node.checkValidity()), true);
    await form.getByRole('button', {name:'Apply', exact:true}).click();
    await page.waitForURL(url => url.searchParams.get('to') === '2026-09-09T00:00');
    assert.notEqual(await page.locator('body').getAttribute('data-dashboard-live-url'), '');
  });

  await check("States server validation retains malformed drafts without JavaScript", async page => {
    const response = await page.goto(base + '/dashboard/flow/states?' + new URLSearchParams({from:'bad<&', to:'2026-09-01T00:00'}));
    assert.equal(response.status(), 422);
    const form = page.locator('.flow-state-filter-form');
    assert.equal(await form.locator('[name="from"]').inputValue(), 'bad<&');
    assert.equal(await form.locator('[name="from"]').getAttribute('aria-invalid'), 'true');
    assert.equal(await page.locator('body').getAttribute('data-dashboard-live-url'), '');
    assert.equal(await page.locator('[data-live-component="flow_states_table"]').count(), 0);
    assert.ok((await page.getByRole('alert').innerText()).includes('Query not run'));
    await page.screenshot({path:`${out}/states-date-error.png`});
    await form.locator('[name="from"]').fill('2026-08-01T00:00');
    await form.getByRole('button', {name:'Apply', exact:true}).click();
    await page.waitForURL(url => url.searchParams.get('from') === '2026-08-01T00:00');
    assert.equal(await page.locator('[aria-invalid="true"]').count(), 0);
    assert.notEqual(await page.locator('body').getAttribute('data-dashboard-live-url'), '');
  }, {javaScriptEnabled:false});

  await check("invalid States requests do not poll or display a successful empty result", async page => {
    let polls = 0;
    page.on('request', request => { if(request.url().includes('/dashboard/api/flow/states')) polls++; });
    const response = await page.goto(base + '/dashboard/flow/states?from=2000&to=1000');
    assert.equal(response.status(), 422);
    await page.waitForTimeout(2300);
    assert.equal(polls, 0);
    assert.equal(await page.locator('[data-live-component="flow_states_table"]').count(), 0);
    await page.getByRole('link', {name:'Clear', exact:true}).click();
    await page.waitForURL(url => url.pathname.endsWith('/states') && !url.search);
    assert.notEqual(await page.locator('body').getAttribute('data-dashboard-live-url'), '');
  });

  await check("Workers and Retention prioritize records and retain secondary details", async page => {
    await page.goto(base + '/dashboard/flow/workers');
    const details = page.locator('#flow-worker-breakdown');
    assert.equal(await details.getAttribute('open'), null);
    const records = page.locator('[data-live-component="flow_running_records"] .table-scroll');
    assert.ok((await records.boundingBox()).y < (await details.boundingBox()).y);
    await details.locator('summary').focus();
    await page.keyboard.press('Enter');
    await page.locator('.subpage-title').click();
    await page.waitForTimeout(2300);
    assert.equal(await details.getAttribute('open'), '');
    let zeroBars = 0;
    for (const line of await page.locator('#flow-worker-breakdown .chart-bar-line').filter({hasText:'Expired'}).all()) {
      if ((await line.locator('.chart-bar-value').innerText()) === '0') {
        zeroBars++;
        assert.equal(await line.locator('.bar-red').evaluate(node=>node.getBoundingClientRect().width), 0);
      }
    }
    assert.ok(zeroBars > 0);
    await page.goto(base + '/dashboard/flow/retention');
    assert.equal(await page.locator('dl[aria-label="Retention sample metrics"]').count(),1);
    assert.ok((await page.locator('[name="limit"]').boundingBox()).width <= 200);
    assert.equal(await page.locator('[name="confirm_cleanup"]').isChecked(),false);
    assert.equal(await page.locator('#flow-retention-reference').getAttribute('open'),null);
    await page.locator('#flow-retention-reference > summary').click();
    assert.ok(await page.getByText('FLOW.RETENTION_CLEANUP [LIMIT <n>]', {exact:true}).isVisible());
    assert.ok(await page.evaluate(()=>document.documentElement.scrollWidth <= innerWidth));
    await page.screenshot({path:`${out}/retention-compact.png`});
  });

  await check("policy summary follows the complete current draft without requests", async page => {
    await page.goto(base + "/dashboard/flow/policies?" + new URLSearchParams({edit:'review<&',edit_state:'verification'}), {waitUntil:"networkidle"});
    let calls = 0;
    page.on("request", request => { if(request.url().startsWith(base)) calls++; });
    const form = page.locator("#flow-policy-editor form[data-policy-editor]");
    for (const name of ["indexed_attributes", "indexed_state_meta"]) {
      assert.equal(await form.locator(`[name="${name}"]`).isDisabled(), true, 'State overrides cannot edit type indexes');
    }
    for (const [name,value] of Object.entries({max_retries:"9",base_ms:"2000",max_ms:"60000",jitter_pct:"15",exhausted_to:"dead",max_active_ms:"10000",retention_ttl_ms:"1000",history_max_events:"42"})) {
      await form.locator(`[name="${name}"]`).fill(value);
    }
    await form.locator('[name="mode"]').selectOption("fifo");
    await form.locator('[name="backoff_kind"]').selectOption("fixed");
    const preview = page.locator(".flow-policy-preview");
    const text = await preview.innerText();
    for (const value of ["review<&","verification","fifo","9","2000","60000","15","dead","10000","1000","42"]) assert.ok(text.includes(value), value);
    assert.ok(text.includes("unchanged"), "state overrides cannot change type indexes");
    assert.equal(await form.locator('[name="state"]').isEditable(), false);
    assert.equal(await form.locator('[name="type"]').isEditable(), false);
    await form.locator('[name="max_retries"]').fill("");
    assert.ok((await preview.innerText()).includes("invalid"));
    await form.locator('[name="max_retries"]').fill("3");
    assert.equal(calls, 0);
    await preview.scrollIntoViewIfNeeded();
    await page.screenshot({path:`${out}/policy-summary.png`});
  });

  await check("schedule mode changes cannot leave hidden invalid controls", async page => {
    for (const [from, oldField, invalid] of [["interval","every_ms","0"],["delay","delay_ms","-1"]]) {
      await page.goto(base + "/dashboard/flow/schedules");
      await page.locator("#flow-schedule-create-panel > summary").click();
      const form = page.locator("#flow-schedule-create-panel form");
      await form.locator('[name="schedule_kind"]').selectOption(from);
      await form.locator(`[name="${oldField}"]`).fill(invalid);
      await form.locator('[name="schedule_kind"]').selectOption("cron");
      await form.locator('[name="cron"]').fill("0 9 * * *");
      await form.locator('[name="id"]').fill("not-created");
      await form.locator('[name="target_type"]').fill("test");
      assert.ok(await form.locator(`[name="${oldField}"]`).isDisabled());
      assert.ok(await form.evaluate(node => node.checkValidity()));
      const entries = await form.evaluate(node => Object.fromEntries(new FormData(node)));
      assert.equal(entries[oldField], undefined);
      await form.locator('[name="schedule_kind"]').selectOption(from);
      assert.equal(await form.locator(`[name="${oldField}"]`).inputValue(), invalid);
      assert.equal(await form.locator(`[name="${oldField}"]`).isDisabled(), false);
      await form.locator('[name="schedule_kind"]').selectOption("delay");
      assert.ok(await form.locator('[name="max_fires"]').isDisabled());
      assert.ok(await form.locator('[name="timezone"]').isDisabled());
      assert.ok(await form.locator('[name="overlap_policy"]').isDisabled());
    }
  });

  await check("policy summary handles long names, integer syntax and restored defaults", async page => {
    const type = 'x'.repeat(512);
    await page.goto(base + "/dashboard/flow/policies?" + new URLSearchParams({edit:type}));
    const form = page.locator('#flow-policy-editor form[data-policy-editor]');
    const preview = page.locator('.flow-policy-preview');
    await form.locator('[name="max_retries"]').fill('1e3');
    assert.ok((await preview.innerText()).includes('invalid'), 'the server accepts integer text, not scientific notation');
    await form.locator('[name="max_retries"]').fill('3');
    assert.ok(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), 'long draft names overflow the document');
    await form.evaluate(node => node.reset());
    assert.ok((await preview.innerText()).includes(type));
    assert.ok((await preview.innerText()).includes('3 retries'));
  });

  await check("policy page does not present a stale summary without JavaScript", async page => {
    await page.goto(base + '/dashboard/flow/policies?edit=user_lifecycle');
    assert.equal(await page.locator('.flow-policy-preview').isVisible(), false);
    assert.ok(await page.locator('#flow-policy-editor form[data-policy-editor]').isVisible());
  }, {javaScriptEnabled:false});

  for (const javaScriptEnabled of [true,false]) {
    await check(`schedule validation retains an escaped draft with JS ${javaScriptEnabled}`, async page => {
      await page.goto(base + "/dashboard/flow/schedules");
      await page.locator("#flow-schedule-create-panel > summary").click();
      const form = page.locator("#flow-schedule-create-panel form");
      const values = {id:"review-invalid-not-created",target_type:"review-type",target_partition:"partition<&",cron:"0 9 * * *",target_payload:'{"unfinished":"</textarea><script>window.bad=1</script>'};
      for (const [name,value] of Object.entries(values)) await form.locator(`[name="${name}"]`).fill(value);
      const response = page.waitForResponse(r => r.request().method()==="POST");
      await form.locator('button[type="submit"]').click();
      assert.equal((await response).status(), 422);
      await page.waitForLoadState("load");
      for (const [name,value] of Object.entries(values)) assert.equal(await form.locator(`[name="${name}"]`).inputValue(), value);
      assert.ok(await form.isVisible());
      assert.ok((await page.locator(".flow-alert-error").innerText()).includes("valid JSON"));
      assert.equal(await page.evaluate(() => window.bad), undefined);
      assert.equal(new URL(page.url()).search, "");
      if (javaScriptEnabled) await page.screenshot({path:`${out}/schedule-validation.png`});
    }, {javaScriptEnabled});
  }

  await check("governance search keeps its error and submitted fields visible", async page => {
    await page.goto(base + "/dashboard/flow/governance");
    const section = page.locator("details").filter({has:page.locator('form[aria-label="State metadata filters"]')});
    await section.locator("summary").click();
    const form = page.locator('form[aria-label="State metadata filters"]');
    for (const [name,value] of Object.entries({meta_type:"ai_agent_pipeline",meta_state:"running",meta_key:"token_usage",meta_value:"invalid",meta_partition_key:"tenant-openai"})) await form.locator(`[name="${name}"]`).fill(value);
    await form.locator('[name="meta_value_type"]').selectOption("integer");
    await form.getByRole("button",{name:"Search",exact:true}).click();
    await page.waitForURL(url => url.searchParams.has("meta_type"));
    assert.ok(await page.locator(".flow-alert-error").isVisible());
    assert.ok(await form.isVisible());
    assert.equal(await form.locator('[name="meta_value"]').inputValue(), "invalid");
    await page.locator(".flow-alert-error").scrollIntoViewIfNeeded();
    await page.screenshot({path:`${out}/governance-result.png`});
  });

  await check("policy scope loads FIFO and prevents saving an unloaded selection", async page => {
    await page.goto(base + '/dashboard/flow/policies?edit=invoice_dispatch&edit_state=queued');
    const form = page.locator('form[data-policy-editor]');
    assert.equal(await form.locator('[name="mode"]').inputValue(), 'fifo');
    assert.equal(await form.locator('[name="state"]').isEditable(), false);
    const scope = page.getByRole('form', {name:'Policy scope', exact:true});
    await scope.locator('[name="edit_state"]').fill('new-state');
    assert.ok(await form.getByRole('button',{name:'Save Policy',exact:true}).isDisabled());
    await scope.getByRole('button',{name:'Load policy',exact:true}).click();
    await page.waitForURL(url => url.searchParams.get('edit_state') === 'new-state');
    assert.equal(await form.locator('[name="state"]').inputValue(), 'new-state');
    assert.equal(await form.locator('[name="mode"]').inputValue(), 'parallel');
    assert.ok(await form.getByRole('button',{name:'Save Policy',exact:true}).isEnabled());
  });

  if (process.env.DASHBOARD_MANAGEMENT_FIXTURES === '1') {
    for (const javaScriptEnabled of [true,false]) {
      await check(`policy validation retains state and saves a corrected draft with JS ${javaScriptEnabled}`, async page => {
      await page.goto(base + '/dashboard/flow/policies?edit=management_review_policy&edit_state=queued');
      const form = page.locator('form[data-policy-editor]');
      assert.equal(await form.locator('[name="mode"]').inputValue(), 'fifo');
      assert.equal(await form.locator('[name="max_retries"]').inputValue(), '8');
      await form.locator('[name="history_max_events"]').fill('321');
      await form.locator('[name="exhausted_to"]').fill('running');
      const invalid = page.waitForResponse(r => r.request().method() === 'POST' && r.url().endsWith('/dashboard/flow/policies'));
      await form.getByRole('button',{name:'Save Policy',exact:true}).click();
      assert.equal((await invalid).status(), 422);
      await page.waitForLoadState('load');
      assert.equal(await form.locator('[name="state"]').inputValue(), 'queued');
      assert.equal(await form.locator('[name="exhausted_to"]').inputValue(), 'running');
      assert.equal(await form.locator('[name="history_max_events"]').inputValue(), '321');
      assert.ok(await page.getByRole('alert').isVisible());
      assert.ok(await form.getByRole('button',{name:'Save Policy',exact:true}).isEnabled());
      await form.locator('[name="exhausted_to"]').fill('failed');
      await form.getByRole('button',{name:'Save Policy',exact:true}).click();
      await page.waitForURL(url => url.searchParams.get('status') === 'ok');
      assert.equal(new URL(page.url()).searchParams.get('edit_state'), 'queued');
      assert.equal(await form.locator('[name="mode"]').inputValue(), 'fifo');
      assert.equal(await form.locator('[name="max_retries"]').inputValue(), '8');
      assert.equal(await form.locator('[name="history_max_events"]').inputValue(), '321');
      if (javaScriptEnabled) await page.screenshot({path:`${out}/policy-state-saved.png`});
    }, {javaScriptEnabled});
  }

    await check("governance exact metadata and page links preserve the full scope", async page => {
    const params = new URLSearchParams({scope:'selected-scope', approval_status:'pending', flow_id:'scoped-run', circuit_status:'open',
      meta_type:'management_review_metadata', meta_state:'queued', meta_key:'risk', meta_value:' high ', meta_value_type:'string',
      meta_partition_key:'management-review', limit:'1'});
    await page.goto(base + '/dashboard/flow/governance?' + params);
    const records = page.getByRole('region',{name:'Workflow state metadata results',exact:true});
    assert.ok((await records.innerText()).includes('mr-spaced'));
    assert.ok(!(await records.innerText()).includes('mr-plain'));
    const form = page.getByRole('form',{name:'State metadata filters',exact:true});
    assert.equal(await form.locator('[name="meta_value"]').inputValue(), ' high ');
    await form.locator('[name="meta_value"]').fill('high');
    await form.getByRole('button',{name:'Search',exact:true}).click();
    await page.waitForURL(url => url.searchParams.get('meta_value') === 'high');
    assert.equal(new URL(page.url()).searchParams.get('scope'), 'selected-scope');
    const first = await records.innerText();
    await page.getByRole('navigation',{name:'State metadata pages'}).getByRole('link',{name:'Next page',exact:true}).click();
    await page.waitForURL(url => url.searchParams.has('meta_cursor'));
    assert.notEqual(await records.innerText(), first);
    assert.equal(new URL(page.url()).searchParams.get('flow_id'), 'scoped-run');
    assert.equal(new URL(page.url()).searchParams.get('approval_status'), 'pending');
    await page.getByRole('form',{name:'Governance filters',exact:true}).getByRole('button',{name:'Refresh',exact:true}).click();
    await page.waitForURL(url => !url.searchParams.has('meta_cursor'));
    assert.equal(new URL(page.url()).searchParams.get('meta_value'), 'high');
    await form.locator('[name="meta_value"]').fill('');
    await form.getByRole('button',{name:'Search',exact:true}).click();
    await page.waitForURL(url => url.searchParams.get('meta_value') === '');
    assert.ok((await records.innerText()).includes('mr-empty'));
    for (const width of [1280,1440,1920]) {
      await page.setViewportSize({width,height:900});
      assert.ok(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth));
      for (const label of ['Workflow type','Metadata state','Indexed key','Exact value','Value type','Partition key','Max records']) {
        assert.ok(await form.getByText(label,{exact:true}).isVisible());
      }
    }
    await page.setViewportSize({width:1440,height:900});
    await page.screenshot({path:`${out}/governance-exact-query.png`});
    });
  } else {
    console.log('SKIP seeded management integration checks (set DASHBOARD_MANAGEMENT_FIXTURES=1 on an isolated fixture server)');
  }
} finally { await browser.close(); }
if(results.some(result=>!result.passed)) process.exitCode=1;
