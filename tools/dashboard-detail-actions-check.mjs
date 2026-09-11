import assert from "node:assert/strict";
import { mkdir, writeFile } from "node:fs/promises";
import { createRequire } from "node:module";
import Module from "node:module";

if (process.env.NODE_PATH) Module._initPaths();
const { chromium } = createRequire(import.meta.url)("playwright");
const base = process.env.DASHBOARD_URL;
const protectedBase = process.env.DASHBOARD_PROTECTED_URL;
const out = process.env.DASHBOARD_OUT_DIR || "test-results/dashboard-detail-actions";
assert.ok(base && protectedBase, "Set both URLs to isolated seeded review servers");
assert.equal(process.env.DASHBOARD_MUTATION_FIXTURES, "1", "These checks mutate detail-browser-* fixtures");
const results = [];
const browser = await chromium.launch({ channel: "chrome", headless: process.env.HEADFUL !== "1" });
await mkdir(out, { recursive: true });
const detailPath = (id) => `/dashboard/flow/${id}?partition_key=review-detail&history_count=100`;
const signalForm = (page) => page.locator("form[data-flow-signal-form]");
const rewindForm = (page) => page.locator('form[action$="/rewind"]');

async function check(name, options, fn) {
  if (process.env.DASHBOARD_CHECK && !name.includes(process.env.DASHBOARD_CHECK)) return;
  const context = await browser.newContext({ viewport: { width: 1440, height: 1000 }, ...options });
  const page = await context.newPage();
  const errors = [];
  page.on("pageerror", (error) => errors.push(error.message));
  try {
    await fn(page, context);
    assert.deepEqual(errors, [], "uncaught browser errors");
    results.push({ name, passed: true });
    console.log(`PASS ${name}`);
  } catch (error) {
    results.push({ name, passed: false, error: error.stack, pageErrors: errors });
    await page.screenshot({ path: `${out}/failure-${results.length}.png`, fullPage: true });
    console.error(`FAIL ${name}: ${error.message}`);
  } finally {
    await context.close();
  }
}

async function openActions(page, id) {
  const response = await page.goto(base + detailPath(id));
  assert.equal(response.status(), 200);
  await page.locator("#workflow-actions summary").click();
}

async function submit(page, form, expectedStatus) {
  const responsePromise = page.waitForResponse((response) =>
    response.request().method() === "POST" && response.request().isNavigationRequest());
  await form.locator('button[type="submit"]').click();
  assert.equal((await responsePromise).status(), expectedStatus);
  await page.waitForLoadState("domcontentloaded");
}

async function verifyDraft(page, values) {
  for (const [name, value] of Object.entries(values)) {
    assert.equal(await signalForm(page).locator(`[name="${name}"]`).inputValue(), value);
  }
  const review = new URL(await page.getByRole("link", { name: "Review workflow", exact: true }).getAttribute("href"), base);
  assert.equal(review.searchParams.get("partition_key"), "review-detail");
  assert.equal(review.searchParams.get("history_count"), "100");
  assert.equal(await page.locator("body").getAttribute("data-dashboard-live-url"), null);
  assert.ok(await page.locator('[role="alert"]').isVisible());
  assert.ok(await signalForm(page).locator('[name="_csrf_token"]').inputValue());
}

try {
  await check("signal error fields remain aligned and readable", { javaScriptEnabled: false }, async (page) => {
    await openActions(page, "detail-browser-history");
    await signalForm(page).locator('[name="signal"]').fill("validation-only");
    await signalForm(page).locator('[name="transition_to"]').fill("approved");
    await submit(page, signalForm(page), 422);
    const key = await signalForm(page).locator('[name="idempotency_key"]').boundingBox();
    const state = await signalForm(page).locator('[name="if_state"]').boundingBox();
    assert.ok(Math.abs(key.y - state.y) <= 1, "error help misaligns neighboring inputs");
    for (const selector of ["#flow-signal-state-help", "#flow-signal-state-error"]) {
      assert.equal(await page.locator(selector).evaluate((node) => getComputedStyle(node).textTransform), "none");
    }
    await page.screenshot({ path: `${out}/signal-field-alignment.png`, fullPage: true });
  });

  for (const javaScriptEnabled of [true, false]) {
    await check(`signal validation and draft correction with JavaScript ${javaScriptEnabled}`, { javaScriptEnabled }, async (page) => {
      const id = javaScriptEnabled ? "detail-browser-js" : "detail-browser-nojs";
      await openActions(page, id);
      const draft = { signal: "review-payment", transition_to: "approved", idempotency_key: `browser-${id}`, if_state: "" };
      for (const [name, value] of Object.entries(draft)) await signalForm(page).locator(`[name="${name}"]`).fill(value);
      if (javaScriptEnabled) {
        assert.equal(await signalForm(page).locator('[name="if_state"]').getAttribute("required"), "");
        await signalForm(page).getByRole("button", { name: "Send Signal", exact: true }).click();
        assert.equal(await signalForm(page).locator('[name="if_state"]').evaluate((node) => node.validity.valid), false);
        assert.ok(page.url().includes(`/flow/${id}?`));
        await signalForm(page).locator('[name="if_state"]').fill("wrong-state");
        draft.if_state = "wrong-state";
      }
      await submit(page, signalForm(page), 422);
      await verifyDraft(page, draft);
      await page.screenshot({ path: `${out}/signal-error-js-${javaScriptEnabled}.png`, fullPage: true });
      await signalForm(page).locator('[name="if_state"]').fill("ready");
      await submit(page, signalForm(page), 302);
      await page.waitForURL((url) => url.searchParams.get("status") === "signaled");
      assert.equal(new URL(page.url()).searchParams.get("history_count"), "100");
      assert.ok((await page.locator("#workflow-summary").innerText()).includes("approved"));
    });
  }

  await check("same-state concurrent update rejects the reviewed rewind and retains guards", {}, async (page, context) => {
    await openActions(page, "detail-browser-stale");
    const form = rewindForm(page);
    const target = await form.locator('select[name="to_event"] option').last().getAttribute("value");
    await form.locator('[name="to_event"]').selectOption(target);
    await form.locator('[name="confirm_rewind"]').check();
    const version = await form.locator('[name="expected_version"]').inputValue();
    const other = await context.newPage();
    await openActions(other, "detail-browser-stale");
    await signalForm(other).locator('[name="signal"]').fill("version-change");
    await submit(other, signalForm(other), 302);
    await other.waitForURL((url) => url.searchParams.get("status") === "signaled");
    await submit(page, form, 422);
    assert.ok((await page.locator('[role="alert"]').innerText()).includes("changed concurrently"));
    assert.equal(await rewindForm(page).locator('[name="expected_version"]').inputValue(), version);
    assert.equal(await rewindForm(page).locator('[name="expect_state"]').inputValue(), "ready");
    assert.equal(await rewindForm(page).locator('[name="to_event"]').inputValue(), target);
    assert.equal(await rewindForm(page).locator('[name="confirm_rewind"]').isChecked(), false);
    for (const width of [1280, 1440, 1920]) {
      await page.setViewportSize({ width, height: 900 });
      assert.ok(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth));
    }
    await page.setViewportSize({ width: 1440, height: 900 });
    await page.screenshot({ path: `${out}/stale-rewind.png`, fullPage: true });
    await page.getByRole("link", { name: "Review workflow", exact: true }).click();
    assert.equal(new URL(page.url()).searchParams.get("history_count"), "100");
    assert.notEqual(await rewindForm(page).locator('[name="expected_version"]').inputValue(), version);
    assert.equal(await rewindForm(page).locator('[name="expect_state"]').inputValue(), "ready");
  });

  await check("confirmed unchanged rewind succeeds", {}, async (page) => {
    await openActions(page, "detail-browser-rewind");
    const form = rewindForm(page);
    const target = await form.locator('select[name="to_event"] option').last().getAttribute("value");
    await form.locator('[name="to_event"]').selectOption(target);
    await form.locator('[name="confirm_rewind"]').check();
    await submit(page, form, 302);
    await page.waitForURL((url) => url.searchParams.get("status") === "rewound");
    assert.equal(new URL(page.url()).searchParams.get("history_count"), "100");
    assert.ok((await page.locator("#workflow-summary").innerText()).includes("queued"));
  });

  await check("invalid query date is visible and correction retains milliseconds", {}, async (page) => {
    const params = new URLSearchParams({ kind: "list", type: "dashboard_action_review", partition_key: "review-detail", from: 'bad"<&', to: "2027-01-01T00:00:02.123" });
    const response = await page.goto(`${base}/dashboard/flow/query?${params}`);
    assert.equal(response.status(), 422);
    const from = page.locator('input[name="from"]');
    const to = page.locator('input[name="to"]');
    assert.equal(await from.inputValue(), 'bad"<&');
    assert.equal(await from.getAttribute("aria-invalid"), "true");
    assert.ok(await from.isVisible());
    assert.equal(await to.inputValue(), "2027-01-01T00:00:02.123");
    await page.screenshot({ path: `${out}/query-date-error.png`, fullPage: true });
    await from.fill("2020-01-01T00:00:01.321");
    const [nextResponse] = await Promise.all([
      page.waitForResponse((res) => res.request().isNavigationRequest()),
      page.getByRole("button", { name: "Run", exact: true }).click(),
    ]);
    assert.equal(nextResponse.status(), 200);
    await page.waitForLoadState("domcontentloaded");
    assert.equal(await page.locator('input[name="from"]').inputValue(), "2020-01-01T00:00:01.321");
    assert.equal(await page.locator('input[name="to"]').inputValue(), "2027-01-01T00:00:02.123");
  });

  for (const username of ["review-reader", "review-historian"]) {
    await check(`protected detail and live history permission for ${username}`, {}, async (page) => {
      await page.goto(protectedBase + detailPath("detail-browser-history"));
      await page.locator('[name="username"]').fill(username);
      await page.locator('[name="password"]').fill("review-password");
      await page.locator('form[action="/dashboard/login"] button').click();
      await page.waitForURL((url) => url.pathname === "/dashboard/flow/detail-browser-history");
      const body = await page.locator("body").innerText();
      const liveUrl = await page.locator("body").getAttribute("data-dashboard-live-url");
      const response = await page.request.get(new URL(liveUrl, protectedBase).href);
      assert.equal(response.status(), 200);
      const live = await response.json();
      if (username === "review-reader") {
        assert.ok(body.includes("History restricted"));
        assert.equal(await page.locator(".journal-step-trigger").count(), 0);
        assert.ok(live.components.flow_history.includes("History restricted"));
        assert.ok(!JSON.stringify(live).includes("historical-review-signal"));
        assert.ok(!(await page.content()).includes("historical-review-signal"));
      } else {
        assert.ok(!body.includes("History restricted"));
        assert.equal(await page.locator(".journal-step-trigger").count(), 3);
        assert.ok(JSON.stringify(live).includes("historical-review-signal"));
      }
      await page.screenshot({ path: `${out}/${username}.png`, fullPage: true });
    });
  }
} finally {
  await browser.close();
  await writeFile(`${out}/report.json`, JSON.stringify(results, null, 2));
}
if (results.some((result) => !result.passed)) process.exitCode = 1;
