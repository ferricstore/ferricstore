import assert from "node:assert/strict";
import { mkdir, writeFile } from "node:fs/promises";
import { createRequire } from "node:module";
import Module from "node:module";

if (process.env.NODE_PATH) Module._initPaths();
const { chromium } = createRequire(import.meta.url)("playwright");
const base = process.env.DASHBOARD_URL || "http://localhost:4000";
const out = process.env.DASHBOARD_OUT_DIR || "test-results/dashboard-workflow";
const screenshots = process.argv.includes("--screenshots");
const detail = "/dashboard/flow/ai-pipeline-agent-402?partition_key=tenant-openai";
const query = "/dashboard/flow/query?kind=list&type=ai_agent_pipeline&partition_key=tenant-openai&limit=40";
const results = [];
const captures = [];
await mkdir(out, { recursive: true });
const browser = await chromium.launch({ channel: "chrome", headless: process.env.HEADFUL !== "1" });

async function check(name, fn) {
  if (process.env.DASHBOARD_CHECK && !name.includes(process.env.DASHBOARD_CHECK)) return;
  const context = await browser.newContext({ viewport: { width: 1440, height: 1000 } });
  const page = await context.newPage();
  const errors = [];
  page.on("pageerror", (error) => errors.push(error.message));
  try {
    await fn(page);
    assert.deepEqual(errors, [], "uncaught browser errors");
    results.push({ name, passed: true });
    console.log(`PASS ${name}`);
  } catch (error) {
    results.push({ name, passed: false, error: error.stack, pageErrors: errors });
    console.error(`FAIL ${name}: ${error.message}`);
  } finally {
    await context.close();
  }
}

async function goto(page, path) {
  const response = await page.goto(base + path, { waitUntil: "networkidle" });
  assert.equal(response.status(), 200);
}

try {
  await check("run list leads and workload disclosure preserves live refresh", async (page) => {
    await goto(page, "/dashboard/flow");
    assert.ok(await page.locator(".flow-runs-table tbody tr").count() > 1);
    assert.ok((await page.locator(".flow-runs-table").boundingBox()).y < 700);
    const disclosure = page.locator("details.dashboard-disclosure").first();
    assert.equal(await disclosure.getAttribute("open"), null);
    await disclosure.locator("summary").click();
    await page.waitForTimeout(2300);
    assert.notEqual(await disclosure.getAttribute("open"), null);
    assert.ok((await page.locator("[data-dashboard-instance]").innerText()).includes("4000"));
  });

  await check("journal selection, keyboard modes, hash links and metadata navigation", async (page) => {
    await goto(page, detail);
    assert.equal(await page.locator(".flow-history-controls").count(), 0);
    assert.ok((await page.locator(".flow-step-waterfall-row").last().innerText()).includes("No end event"));
    assert.equal(await page.locator(".flow-journal-inspector").isVisible(), false);
    assert.ok((await page.locator(".flow-journal-card .badge").first().innerText()).includes("on this page"));
    const step = page.locator(".journal-step-trigger").first();
    await step.click();
    assert.equal(await step.getAttribute("aria-expanded"), "true");
    assert.equal(await page.locator(".flow-journal-inspector .journal-event-inspector:visible").count(), 1);
    assert.ok(await page.locator(".flow-journal-workspace.has-selected-event").isVisible());
    await page.waitForTimeout(2300);
    assert.equal(await page.locator("[data-dashboard-live-status]").getAttribute("data-dashboard-live-status"), "paused");
    await page.locator(".subpage-title").click();
    await page.waitForTimeout(2300);
    assert.equal(await page.locator(".flow-journal-inspector .journal-event-inspector:visible").count(), 1);
    const journalTab = page.locator('[data-journal-view-toggle="tree"]');
    await journalTab.focus();
    await page.keyboard.press("ArrowRight");
    assert.equal(await page.locator('[data-journal-view-toggle="table"]').getAttribute("aria-selected"), "true");
    await page.locator('a[href^="#journal-flow-event-"]').first().click();
    assert.equal(await journalTab.getAttribute("aria-selected"), "true");
    await page.locator('.flow-detail-sections a[href="#workflow-data"]').click();
    assert.notEqual(await page.locator("#workflow-metadata").getAttribute("open"), null);
    assert.ok(await page.locator("#workflow-data").isVisible());
  });

  await check("journal tab focus survives live refresh and updates resume after leaving", async (page) => {
    await goto(page, detail);
    const liveUrl = await page.locator("body").getAttribute("data-dashboard-live-url");
    await page.route(new URL(liveUrl, base).href, async (route) => {
      const response = await route.fetch();
      const payload = await response.json();
      payload.components.flow_history += '<span data-test-live-refresh="true"></span>';
      await route.fulfill({ response, json: payload });
    });
    await page.locator("#journal-tab-tree").focus();
    await page.keyboard.press("ArrowRight");
    await page.waitForTimeout(2600);
    assert.ok(await page.locator("#journal-tab-table").evaluate((node) => node === document.activeElement), "polling discarded tab focus");
    assert.equal(await page.locator("#journal-tab-table").getAttribute("aria-selected"), "true");
    await page.locator(".subpage-title").click();
    await page.waitForSelector("[data-test-live-refresh]", { state: "attached" });
    assert.equal(await page.locator("#journal-tab-table").getAttribute("aria-selected"), "true");
  });

  await check("single-event detail omits empty widgets and discloses only selected event details", async (page) => {
    await goto(page, "/dashboard/flow/nightly-audit-0088?partition_key=system");
    assert.equal(await page.locator(".flow-step-waterfall").count(), 0);
    assert.equal(await page.locator(".flow-history-controls").count(), 0);
    assert.equal(await page.locator(".flow-journal-inspector").isVisible(), false);
    assert.ok((await page.locator(".flow-journal-card").innerText()).includes("1 event on this page"));
    for (const summary of await page.locator(".dashboard-disclosure > summary, .flow-operations-panel > summary").all()) {
      assert.equal(await summary.evaluate((node) => getComputedStyle(node).display), "list-item");
    }
    const step = page.locator(".journal-step-trigger");
    await step.press("Enter");
    assert.ok(await page.locator(".flow-journal-inspector").isVisible());
    await step.press("Enter");
    assert.equal(await page.locator(".flow-journal-inspector").isVisible(), false);
    assert.equal(await page.locator(".flow-journal-workspace.has-selected-event").count(), 0);
    await page.locator("#workflow-metadata > summary").press("Enter");
    assert.notEqual(await page.locator("#workflow-metadata").getAttribute("open"), null);
    await page.locator("#workflow-metadata > summary").press("Enter");
    assert.equal(await page.locator("#workflow-metadata").getAttribute("open"), null);
  });

  await check("non-default history size remains adjustable in both journal modes", async (page) => {
    await goto(page, detail + "&history_count=100");
    assert.ok(await page.locator(".flow-history-controls").isVisible());
    await page.locator("#journal-tab-table").click();
    assert.ok(await page.locator(".flow-history-controls").isVisible());
    await page.locator(".flow-history-count").filter({ hasText: /^50$/ }).click();
    await page.waitForLoadState("networkidle");
    assert.equal(new URL(page.url()).searchParams.get("history_count"), "50");
    assert.equal(new URL(page.url()).searchParams.get("partition_key"), "tenant-openai");
  });

  await check("journal keyboard focus remains visible before selection", async (page) => {
    await goto(page, detail);
    await page.keyboard.press("Tab");
    const step = page.locator(".journal-step-trigger").first();
    await step.focus();
    assert.ok(await step.evaluate((node) => node.matches(":focus-visible")));
    const outline = await step.evaluate((node) => {
      const style = getComputedStyle(node);
      return { width: parseFloat(style.outlineWidth), style: style.outlineStyle };
    });
    assert.ok(outline.width >= 2 && outline.style !== "none", JSON.stringify(outline));
    await page.keyboard.press("Enter");
    assert.equal(await step.getAttribute("aria-expanded"), "true");
  });

  await check("guided query filters and empty state agree with returned records", async (page) => {
    await goto(page, query);
    assert.ok((await page.locator(".flow-query-output").innerText()).includes("ai-pipeline-agent-402"));
    const table = page.locator(".flow-query-table-wrap");
    assert.deepEqual(await table.locator("th").allTextContents(), ["Workflow", "Type", "State", "Updated"]);
    assert.ok((await table.boundingBox()).y < 900, "returned rows must lead the results viewport");
    const charts = page.locator(".flow-query-visualization");
    assert.equal(await charts.getAttribute("open"), null);
    await charts.locator("summary").click();
    assert.ok(await charts.locator(".flow-query-donut").first().isVisible());
    const form = page.locator("[data-flow-query-form]");
    await form.locator('[name="state"]').fill("failed");
    await form.locator("[data-flow-query-run-action]").click();
    await page.waitForLoadState("networkidle");
    assert.ok(!(await page.locator(".flow-query-output").innerText()).includes("ai-pipeline-agent-402"));
    await page.locator('[data-flow-query-form] [name="state"]').fill("");
    await page.locator("[data-flow-query-run-action]").click();
    await page.waitForLoadState("networkidle");
    assert.ok((await page.locator(".flow-query-output").innerText()).includes("ai-pipeline-agent-402"));
    assert.ok(new URL(page.url()).searchParams.get("partition_key") === "tenant-openai");
  });

  await check("query provenance separates edited inputs from the last result without rerunning", async (page) => {
    await goto(page, query);
    let requests = 0;
    page.on("request", () => requests++);
    const warning = page.locator("[data-flow-query-draft-status]");
    const provenance = page.locator("[data-flow-query-provenance]");
    assert.ok(await provenance.isVisible());
    const captured = await provenance.innerText();
    assert.ok(captured.includes("tenant-openai") && captured.includes("ai_agent_pipeline"));
    assert.ok(await provenance.locator("time[datetime]").count() === 1);
    assert.equal(await warning.isVisible(), false);
    const state = page.locator('[data-flow-query-form] [name="state"]');
    await page.locator('[data-flow-query-fill="state"][data-flow-query-value="failed"]').click();
    assert.ok(await warning.isVisible());
    assert.ok((await warning.innerText()).includes("Filters changed"));
    assert.equal(await provenance.innerText(), captured, "last-run scope must not follow draft edits");
    assert.ok((await page.locator(".flow-query-output").innerText()).includes("ai-pipeline-agent-402"));
    await state.fill("");
    assert.equal(await warning.isVisible(), false, "reverting filters restores the matching state");
    await page.locator("#flow-query-tab-advanced").click();
    assert.ok(await warning.isVisible(), "a different editor is not the executed query");
    await page.locator("#flow-query-tab-guided").click();
    assert.equal(await warning.isVisible(), false);
    assert.equal(requests, 0, "editing and mode changes must not execute any request");
    await state.fill("failed");
    await page.locator("[data-flow-query-run-action]").click();
    await page.waitForLoadState("networkidle");
    assert.equal(await warning.isVisible(), false);
    assert.ok((await provenance.innerText()).includes("failed"));
    assert.equal(await page.locator(".flow-query-table tbody tr").innerText(), "No rows.");

    await page.locator("#flow-query-tab-advanced").click();
    await page.locator('[data-flow-query-workbench-form] [value="run"]').click();
    await page.waitForLoadState("networkidle");
    assert.equal(await warning.isVisible(), false);
    const params = page.locator('[name="params_json"]');
    const original = await params.inputValue();
    await params.fill('{"partition":"different","type":"ai_agent_pipeline"}');
    assert.ok(await warning.isVisible());
    await params.fill(original);
    assert.equal(await warning.isVisible(), false);
  });

  await check("all guided operations expose only relevant enabled fields", async (page) => {
    await goto(page, query);
    const form = page.locator("[data-flow-query-form]");
    const operations = await form.locator('[name="kind"] option').evaluateAll((options) => options.map((o) => o.value));
    assert.equal(operations.length, 10);
    for (const kind of operations) {
      await form.locator('[name="kind"]').selectOption(kind);
      const invalidHidden = await form.evaluate((node) => [...node.querySelectorAll("[data-flow-query-field][hidden] input")].filter((input) => !input.disabled).length);
      assert.equal(invalidHidden, 0, `${kind}: hidden fields must not submit`);
      const needsId = ["history", "by_parent", "by_root", "by_correlation"].includes(kind);
      assert.equal(await form.locator('[name="id"]').isEnabled(), needsId);
    }
  });

  await check("guided and FQL drafts survive mode changes without lossy conversion", async (page) => {
    await goto(page, query);
    await page.locator('[data-flow-query-form] [name="state"]').fill("payment_failed_review");
    await page.locator("#flow-query-tab-advanced").click();
    const fql = "FROM runs WHERE partition_key = @partition AND type = @type ORDER BY updated_at_ms DESC LIMIT 2 RETURN RECORDS (run_id, type, state)";
    const params = '{"partition":"tenant-openai","type":"ai_agent_pipeline"}';
    await page.locator('[name="fql"]').fill(fql);
    await page.locator('[name="params_json"]').fill(params);
    await page.locator("#flow-query-tab-guided").click();
    assert.equal(await page.locator('[data-flow-query-form] [name="state"]').inputValue(), "payment_failed_review");
    await page.locator("#flow-query-tab-guided").focus();
    await page.keyboard.press("ArrowRight");
    assert.equal(await page.locator('[name="fql"]').inputValue(), fql);
    assert.equal(await page.locator('[name="params_json"]').inputValue(), params);
    await page.locator('[data-flow-query-workbench-form] [value="run"]').click();
    await page.waitForLoadState("networkidle");
    const output = await page.locator(".flow-query-output").innerText();
    assert.ok(output.includes("ai-pipeline-agent-402"), output);
    await page.locator('[data-flow-query-workbench-form] [value="explain"]').click();
    await page.waitForLoadState("networkidle");
    assert.ok((await page.locator(".flow-query-output").innerText()).toLowerCase().includes("plan"));
  });

  await check("typed predicates and UTC ranges validate without hidden-field submission", async (page) => {
    await goto(page, query);
    const form = page.locator("[data-flow-query-form]");
    await form.locator('[name="kind"]').selectOption("search");
    await form.locator(".flow-query-advanced summary").click();
    await form.locator('[name="attribute_key"]').fill("tier");
    await form.locator('[name="attribute_value_type"]').selectOption("null");
    assert.ok(await form.locator('[name="attribute_value"]').isDisabled());
    await form.locator('[name="attribute_value_type"]').selectOption("boolean");
    await form.locator('[name="attribute_value"]').fill("maybe");
    assert.equal(await form.locator('[name="attribute_value"]').evaluate((el) => el.checkValidity()), false);
    await form.locator('[name="attribute_value"]').fill("true");
    assert.equal(await form.locator('[name="attribute_value"]').evaluate((el) => el.checkValidity()), true);
    await form.locator('[name="from"]').fill("2026-09-07T12:00");
    await form.locator('[name="to"]').fill("2026-09-06T12:00");
    assert.equal(await form.locator('[name="to"]').evaluate((el) => el.checkValidity()), false);
    await form.locator('[name="to"]').fill("2026-09-08T12:00");
    assert.equal(await form.locator('[name="to"]').evaluate((el) => el.checkValidity()), true);
  });

  await check("polling failure is visible and retry recovers", async (page) => {
    await page.route("**/dashboard/api/flow?*", (route) => route.fulfill({ status: 503, body: "unavailable" }));
    await page.route("**/dashboard/api/flow", (route) => route.fulfill({ status: 503, body: "unavailable" }));
    await goto(page, "/dashboard/flow");
    await page.waitForFunction(() => document.querySelector('[data-dashboard-live-status="stale"]'));
    assert.ok(await page.locator("[data-dashboard-live-retry]").isVisible());
    await page.unrouteAll();
    await page.locator("[data-dashboard-live-retry]").click();
    await page.waitForFunction(() => document.querySelector('[data-dashboard-live-status="live"]'));
  });

  await check("expired polling sessions return to login with scope preserved", async (page) => {
    await page.route("**/dashboard/api/flow?*", (route) => route.fulfill({ status: 401, body: "expired" }));
    await page.route("**/dashboard/login?*", (route) => route.fulfill({ status: 200, contentType: "text/html", body: "<h1>Login test fixture</h1>" }));
    const path = "/dashboard/flow?partition_key=tenant-openai";
    await goto(page, path);
    await page.waitForURL("**/dashboard/login?*");
    assert.equal(new URL(page.url()).searchParams.get("next"), path);
  });

  await check("payloads load only on demand with workflow and partition scope", async (page) => {
    const requests = [];
    await page.route("**/dashboard/api/flow/value?*", (route) => {
      requests.push(new URL(route.request().url()));
      return route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ status: "ok", value: "Synthetic browser payload" }) });
    });
    await goto(page, detail);
    await page.waitForTimeout(2300);
    assert.equal(requests.length, 0, "render and polling must not hydrate payloads");
    await page.locator(".flow-value-ref-link:visible").first().click();
    await page.waitForFunction(() => document.querySelector("#flow-value-modal-body")?.textContent === "Synthetic browser payload");
    assert.equal(requests.length, 1);
    assert.equal(requests[0].searchParams.get("flow"), "ai-pipeline-agent-402");
    assert.equal(requests[0].searchParams.get("partition_key"), "tenant-openai");
    assert.ok(requests[0].searchParams.get("ref"));
    await page.keyboard.press("Escape");
    assert.equal(await page.locator("#flow-value-modal").isVisible(), false);
  });

  await check("payload modal contains keyboard focus and restores the opener", async (page) => {
    await page.route("**/dashboard/api/flow/value?*", (route) => route.fulfill({
      status: 200, contentType: "application/json", body: JSON.stringify({ status: "ok", value: "Focus test payload" })
    }));
    await goto(page, detail);
    const opener = page.locator(".flow-value-ref-link:visible").first();
    const modal = page.locator("#flow-value-modal");
    await opener.focus();
    await page.keyboard.press("Enter");
    assert.ok(await modal.isVisible());
    for (const key of ["Tab", "Tab", "Tab", "Shift+Tab", "Shift+Tab", "Shift+Tab"]) {
      await page.keyboard.press(key);
      assert.ok(await modal.evaluate((node) => node.contains(document.activeElement)), `${key}: focus escaped the modal`);
    }
    await page.locator(".subpage-title").evaluate((node) => { node.tabIndex = -1; node.focus(); });
    assert.ok(await modal.evaluate((node) => node.contains(document.activeElement)), "background must be inert");
    await page.keyboard.press("Escape");
    assert.equal(await modal.isVisible(), false);
    assert.ok(await opener.evaluate((node) => node === document.activeElement));
    await opener.click();
    await modal.locator("button[data-flow-value-modal-close]").click();
    assert.equal(await modal.isVisible(), false);
    assert.ok(await opener.evaluate((node) => node === document.activeElement));
    await opener.click();
    await modal.locator(".flow-value-modal-backdrop").click({ position: { x: 5, y: 5 } });
    assert.equal(await modal.isVisible(), false);
    assert.ok(await opener.evaluate((node) => node === document.activeElement));
  });

  await check("value inspector ignores stale success and error responses after reopening", async (page) => {
    // Ignore transport cancellation to verify the response identity guard independently.
    await page.addInitScript(() => {
      const fetchOriginal = window.fetch;
      window.fetch = (url, options) => fetchOriginal(url, String(url).includes("/flow/value?") ? { ...options, signal: undefined } : options);
    });
    for (const stale of [{ status: "ok", value: "STALE A" }, { status: "error", error: "STALE ERROR" }]) {
      let release;
      let started;
      const gate = new Promise((resolve) => { release = resolve; });
      const first = new Promise((resolve) => { started = resolve; });
      let calls = 0;
      await page.route("**/dashboard/api/flow/value?*", async (route) => {
        if (++calls === 1) {
          started();
          await gate;
          await route.fulfill({ json: stale });
        } else {
          await route.fulfill({ json: { status: "ok", value: "RESULT B", truncated: false } });
        }
      });
      await goto(page, "/dashboard/flow/user-onboarding-7720?partition_key=tenant-acme");
      await page.getByRole("link", { name: "Open payload value", exact: true }).first().click();
      await first;
      await page.keyboard.press("Escape");
      await page.getByRole("link", { name: "Open result value", exact: true }).first().click();
      await page.waitForFunction(() => document.querySelector("#flow-value-modal-body").textContent === "RESULT B");
      release();
      await page.waitForTimeout(250);
      assert.equal(await page.locator("#flow-value-modal-body").innerText(), "RESULT B");
      assert.ok((await page.locator("#flow-value-modal-ref").innerText()).startsWith("result"));
      await page.unroute("**/dashboard/api/flow/value?*");
    }
  });

  await check("value inspector separates missing, error, empty and truncated values with scoped retry", async (page) => {
    const responses = [{ status: "missing" }, { status: "error", error: "Value lookup timed out." },
      { status: "ok", value: "", truncated: false }, { status: "ok", value: "missing", truncated: true }];
    const requests = [];
    await page.route("**/dashboard/api/flow/value?*", async (route) => {
      requests.push(new URL(route.request().url()));
      await new Promise((resolve) => setTimeout(resolve, 200));
      await route.fulfill({ json: responses.shift() });
    });
    await goto(page, detail + "&history_count=100&history_before=9999999999999-0");
    const open = page.getByRole("link", { name: "Open payload value", exact: true }).first();
    const modal = page.locator("#flow-value-modal");
    const copy = page.locator("#flow-value-modal-copy");
    await open.click();
    assert.ok(await copy.isDisabled(), "loading text must not be copyable");
    assert.ok(await copy.evaluate((node) => Number(getComputedStyle(node).opacity) < 1), "disabled Copy must look unavailable");
    await page.waitForFunction(() => document.querySelector("#flow-value-modal").dataset.state === "missing");
    assert.ok(await copy.isDisabled());
    assert.equal(await page.locator("#flow-value-modal-body").textContent(), "");
    assert.ok((await modal.innerText()).includes("No stored value"));
    await page.screenshot({path: `${out}/value-missing.png`});
    await modal.getByRole("button", { name: "Retry", exact: true }).click();
    await page.waitForFunction(() => document.querySelector("#flow-value-modal").dataset.state === "error");
    assert.ok(await copy.isDisabled());
    await modal.getByRole("button", { name: "Retry", exact: true }).click();
    await page.waitForFunction(() => document.querySelector("#flow-value-modal").dataset.state === "ready");
    assert.ok(await copy.isEnabled(), "an empty stored value is still data");
    assert.equal(await page.locator("#flow-value-modal-body").textContent(), "");
    await page.keyboard.press("Escape");
    await open.click();
    await page.waitForFunction(() => document.querySelector("#flow-value-modal").dataset.state === "ready");
    assert.equal(await copy.innerText(), "Copy preview");
    assert.ok((await modal.innerText()).includes("truncated"));
    await page.screenshot({path: `${out}/value-truncated.png`});
    assert.equal(requests.length, 4);
    for (const request of requests) {
      assert.equal(request.searchParams.get("partition_key"), "tenant-openai");
      assert.equal(request.searchParams.get("history_before"), "9999999999999-0");
      assert.equal(request.searchParams.get("history_count"), "100");
    }
  });

  await check("value inspector expired session preserves the workflow, page and selected value", async (page) => {
    await page.route("**/dashboard/api/flow/value?*", (route) => route.fulfill({ status: 401, body: "expired" }));
    await page.route("**/dashboard/login?*", (route) => route.fulfill({ contentType: "text/html", body: "<h1>Login fixture</h1>" }));
    await goto(page, detail + "&history_count=100");
    await page.getByRole("link", { name: "Open payload value", exact: true }).first().click();
    await page.waitForURL("**/dashboard/login?*");
    const next = new URL(new URL(page.url()).searchParams.get("next"), base);
    assert.equal(next.pathname, "/dashboard/flow/ai-pipeline-agent-402");
    assert.equal(next.searchParams.get("partition_key"), "tenant-openai");
    assert.equal(next.searchParams.get("history_count"), "100");
    assert.ok(next.hash.startsWith("#flow-value-"));
  });

  await check("projected query links navigate with the executed partition despite omitted columns", async (page) => {
    await goto(page, "/dashboard/flow/query?type=user_lifecycle&partition_key=tenant-acme");
    await page.locator("#flow-query-tab-advanced").click();
    await page.locator('[data-flow-query-workbench-form] [value="run"]').click();
    await page.waitForLoadState("networkidle");
    const link = page.locator(".flow-query-projection-table").getByRole("link", { name: "user-onboarding-7720", exact: true });
    assert.equal(await link.getAttribute("href"), "/dashboard/flow/user-onboarding-7720?partition_key=tenant-acme");
    await link.click();
    await page.waitForLoadState("networkidle");
    assert.equal(new URL(page.url()).searchParams.get("partition_key"), "tenant-acme");
    assert.ok(await page.getByRole("button", {name:"Copy partition key", exact:true}).isVisible());
  });

  await check("value inspector cancels closed requests and times out stalled reads", async (page) => {
    await page.clock.install();
    await goto(page, detail);
    const requests = [];
    page.on("requestfailed", (request) => {
      if (request.url().includes("/flow/value?")) requests.push(request.failure()?.errorText);
    });
    await page.route("**/dashboard/api/flow/value?*", () => {});
    const open = page.getByRole("link", {name:"Open payload value", exact:true}).first();
    await open.click();
    await page.keyboard.press("Escape");
    await page.waitForFunction(() => document.querySelector("#flow-value-modal").hidden);
    await page.waitForTimeout(100);
    assert.equal(requests.length, 1, "closing must cancel the in-flight read");
    await open.click();
    await page.clock.fastForward(15_001);
    await page.waitForFunction(() => document.querySelector("#flow-value-modal").dataset.state === "error");
    assert.ok((await page.locator("#flow-value-modal-status").textContent()).includes("timed out"));
    assert.ok(await page.locator("#flow-value-modal-copy").isDisabled());
    assert.ok(await page.getByRole("button", {name:"Retry", exact:true}).isVisible());
    await page.keyboard.press("Tab");
    assert.ok(await page.locator("#flow-value-modal").evaluate((node) => node.contains(document.activeElement)));
  });

  await check("workflow mutation forms retain confirmation and double-submit protection", async (page) => {
    await goto(page, detail);
    await page.locator(".flow-operations-panel > summary").click();
    assert.ok(await page.locator('[name="confirm_rewind"]').isVisible());
    const form = page.locator('form[action$="/signal"]');
    await form.locator('[name="signal"]').fill("browser-check");
    const submissions = await form.evaluate((node) => {
      let accepted = 0;
      const observe = (event) => {
        if (event.target === node) {
          if (!event.defaultPrevented) accepted++;
          event.preventDefault();
        }
      };
      document.addEventListener("submit", observe);
      node.dispatchEvent(new Event("submit", { bubbles: true, cancelable: true }));
      node.dispatchEvent(new Event("submit", { bubbles: true, cancelable: true }));
      document.removeEventListener("submit", observe);
      return accepted;
    });
    assert.equal(submissions, 1);
    assert.ok(await form.locator('button[type="submit"]').isDisabled());
  });

  if (screenshots) {
    for (const width of [1280, 1440, 1920]) {
      const context = await browser.newContext({ viewport: { width, height: 1000 } });
      const page = await context.newPage();
      for (const [name, path] of [["runs", "/dashboard/flow"], ["detail", detail], ["query", query], ["system", "/dashboard"], ["fifo", "/dashboard/flow/states"]]) {
        await goto(page, path);
        if (name === "detail") await page.locator(".journal-step-trigger").first().click();
        const metrics = await page.evaluate(() => ({
          width: innerWidth,
          scrollWidth: document.documentElement.scrollWidth,
          height: document.documentElement.scrollHeight,
          cards: document.querySelectorAll(".flow-card").length,
          cells: document.querySelectorAll("td").length,
          htmlBytes: new TextEncoder().encode(document.documentElement.outerHTML).length
        }));
        assert.ok(metrics.scrollWidth <= width + 1, `${name} overflows at ${width}px`);
        const file = `${out}/${name}-${width}.png`;
        await page.screenshot({ path: file, fullPage: true });
        captures.push({ name, file, ...metrics });
      }
      await context.close();
    }
  }
} finally {
  await browser.close();
  await writeFile(`${out}/report.json`, JSON.stringify({ results, captures }, null, 2));
}

if (results.some((result) => !result.passed)) process.exitCode = 1;
