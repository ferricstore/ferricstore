import { mkdir, writeFile } from "node:fs/promises";
import { createRequire } from "node:module";
import Module from "node:module";

if (process.env.NODE_PATH) {
  Module._initPaths();
}

const require = createRequire(import.meta.url);
const { chromium } = require("playwright");

const args = process.argv.slice(2);

function argValue(name, fallback) {
  const idx = args.indexOf(name);
  if (idx >= 0 && args[idx + 1]) {
    return args[idx + 1];
  }

  return fallback;
}

const url =
  argValue("--url", process.env.DASHBOARD_URL) ||
  "http://127.0.0.1:62851/dashboard/flow";
const outDir = argValue("--out-dir", process.env.DASHBOARD_OUT_DIR || "test-results");
const waitMs = Number(argValue("--wait-ms", process.env.DASHBOARD_WAIT_MS || "3500"));
const headed = process.env.HEADFUL === "1" || args.includes("--headed");
const screenshotPath = `${outDir}/dashboard-inspect.png`;
const reportPath = `${outDir}/dashboard-inspect.json`;

await mkdir(outDir, { recursive: true });

const consoleMessages = [];
const pageErrors = [];
const requestFailures = [];
const responses = [];

const browser = await chromium.launch({
  channel: "chrome",
  headless: !headed
});

try {
  const page = await browser.newPage({ viewport: { width: 1440, height: 1200 } });

  page.on("console", (msg) => {
    consoleMessages.push({
      type: msg.type(),
      text: msg.text(),
      location: msg.location()
    });
  });

  page.on("pageerror", (error) => {
    pageErrors.push({ name: error.name, message: error.message, stack: error.stack });
  });

  page.on("requestfailed", (request) => {
    requestFailures.push({
      url: request.url(),
      method: request.method(),
      failure: request.failure()
    });
  });

  page.on("response", async (response) => {
    const responseUrl = response.url();

    if (responseUrl.includes("/dashboard")) {
      const headers = response.headers();
      responses.push({
        url: responseUrl,
        status: response.status(),
        contentType: headers["content-type"] || "",
        contentLength: Number(headers["content-length"] || 0)
      });
    }
  });

  const startedAt = Date.now();
  await page.goto(url, { waitUntil: "networkidle", timeout: 30_000 });
  await page.waitForTimeout(waitMs);

  const dom = await page.evaluate(() => {
    const count = (selector) => document.querySelectorAll(selector).length;
    const textLength = document.body?.innerText?.length || 0;
    const viewportHeight = window.innerHeight || 1;
    const scrollHeight = document.documentElement.scrollHeight || 0;

    const liveComponents = [...document.querySelectorAll("[data-live-component]")].map((node) => ({
      name: node.getAttribute("data-live-component"),
      htmlBytes: new TextEncoder().encode(node.innerHTML).length,
      rows: node.querySelectorAll("tr").length,
      cells: node.querySelectorAll("td").length
    }));

    const sections = [...document.querySelectorAll(".section-title")].map((node) =>
      node.textContent.trim().replace(/\s+/g, " ")
    );

    const cards = [...document.querySelectorAll(".flow-card")].map((node) => ({
      label: node.querySelector(".flow-card-label")?.textContent?.trim() || "",
      value: node.querySelector(".flow-card-value")?.textContent?.trim() || "",
      detail: node.querySelector(".flow-card-detail")?.textContent?.trim() || ""
    }));

    return {
      title: document.title,
      bodyLivePage: document.body?.dataset?.dashboardLivePage || null,
      bodyLiveUrl: document.body?.dataset?.dashboardLiveUrl || null,
      bodyLiveLastUpdateMs: document.body?.dataset?.dashboardLiveLastUpdateMs || null,
      bodyLiveError: document.body?.dataset?.dashboardLiveError || null,
      scrollHeight,
      viewportHeight,
      viewportScreens: Number((scrollHeight / viewportHeight).toFixed(2)),
      textLength,
      tables: count("table"),
      rows: count("tr"),
      cells: count("td"),
      cards: count(".flow-card"),
      charts: count("canvas"),
      sections,
      liveComponents,
      cardSummaries: cards
    };
  });

  await page.screenshot({ path: screenshotPath, fullPage: true });

  const report = {
    url,
    elapsedMs: Date.now() - startedAt,
    screenshotPath,
    dom,
    responses,
    consoleMessages,
    pageErrors,
    requestFailures
  };

  await writeFile(reportPath, `${JSON.stringify(report, null, 2)}\n`);

  console.log(JSON.stringify(report, null, 2));
} finally {
  await browser.close();
}
