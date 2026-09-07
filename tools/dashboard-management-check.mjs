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
    await context.close();
  }
}

try {
  await check("policy summary follows the complete current draft without requests", async page => {
    await page.goto(base + "/dashboard/flow/policies?edit=user_lifecycle", {waitUntil:"networkidle"});
    let calls = 0;
    page.on("request", request => { if(request.url().startsWith(base)) calls++; });
    const form = page.locator("#flow-policy-editor form");
    for (const [name,value] of Object.entries({type:"review<&",state:"verification",indexed_attributes:"region",indexed_state_meta:"risk",max_retries:"9",base_ms:"2000",max_ms:"60000",jitter_pct:"15",exhausted_to:"dead",max_active_ms:"10000",retention_ttl_ms:"1000",history_max_events:"42"})) {
      await form.locator(`[name="${name}"]`).fill(value);
    }
    await form.locator('[name="mode"]').selectOption("fifo");
    await form.locator('[name="backoff_kind"]').selectOption("fixed");
    const preview = page.locator(".flow-policy-preview");
    const text = await preview.innerText();
    for (const value of ["review<&","verification","fifo","9","2000","60000","15","dead","10000","1000","42"]) assert.ok(text.includes(value), value);
    assert.ok(text.includes("unchanged"), "state overrides cannot change type indexes");
    await form.locator('[name="state"]').fill("");
    assert.ok((await preview.innerText()).includes("region"));
    assert.ok((await preview.innerText()).includes("risk"));
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
    await page.goto(base + "/dashboard/flow/policies");
    const form = page.locator('#flow-policy-editor form');
    const preview = page.locator('.flow-policy-preview');
    await form.locator('[name="max_retries"]').fill('1e3');
    assert.ok((await preview.innerText()).includes('invalid'), 'the server accepts integer text, not scientific notation');
    await form.locator('[name="max_retries"]').fill('3');
    await form.locator('[name="type"]').fill('x'.repeat(512));
    assert.ok(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), 'long draft names overflow the document');
    await form.evaluate(node => node.reset());
    assert.ok((await preview.innerText()).includes('(type required)'));
    assert.ok((await preview.innerText()).includes('3 retries'));
  });

  await check("policy page does not present a stale summary without JavaScript", async page => {
    await page.goto(base + '/dashboard/flow/policies');
    assert.equal(await page.locator('.flow-policy-preview').isVisible(), false);
    assert.ok(await page.locator('#flow-policy-editor form').isVisible());
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
} finally { await browser.close(); }
if(results.some(result=>!result.passed)) process.exitCode=1;
