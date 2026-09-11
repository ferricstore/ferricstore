import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import Module from 'node:module';
import { spawnSync } from 'node:child_process';

if (process.env.NODE_PATH) Module._initPaths();
const { chromium } = Module.createRequire(import.meta.url)('playwright');
const base = 'apps/ferricstore_server/lib/ferricstore_server/health/dashboard';
const render = spawnSync('elixir', ['-pa', '_build/test/lib/*/ebin', '-e', `
for path <- ~w(layout/styles layout render/admin render/capabilities render/security render/flow_policy render/flow_form_scripts), do: Code.require_file("${base}/" <> path <> ".ex")
alias FerricstoreServer.Health.Dashboard.{Layout, Flow.PolicyEditor}
alias FerricstoreServer.Health.Dashboard.Render.{Admin, Capabilities, Security, FlowPolicy, FlowFormScripts}
IO.write(Jason.encode!(%{
  css: Layout.Styles.stylesheet(), script: Layout.dashboard_live_script(),
  policy: FlowPolicy.render_flow_policy_editor(%{editor: %{PolicyEditor.empty() | type: "review"}}) <> FlowFormScripts.policy_script(),
  account: Security.render_account_management(%{can_manage_users: true}),
  config: Admin.render_config_parameters([%{parameter: "slowlog-log-slower-than", value: "/" <> String.duplicate("long-directory/", 30), source: "CONFIG GET", scope: "runtime", mutability: "read-only", notes: "Parameter notes remain readable beside long values."}]),
  capabilities: Capabilities.render_management_capability_summary(%{})
}))`], { encoding: 'utf8', env: { ...process.env, ERL_FLAGS: '+S 2:2' } });
assert.equal(render.status, 0, render.stderr);
const assets = JSON.parse(render.stdout);
const out = process.env.DASHBOARD_OUT_DIR || 'test-results/dashboard-management-third';
await fs.mkdir(out, {recursive:true});
const browser = await chromium.launch({channel:'chrome', headless:true});
const results = [];
const origin = 'http://dashboard-management.test';
async function check(name, html, test) {
  const context = await browser.newContext({viewport:{width:1280,height:900}});
  const page = await context.newPage();
  page.setDefaultTimeout(3000);
  const errors = [];
  page.on('pageerror', e => errors.push(e.message));
  await page.route(origin+'/**', route => route.fulfill({contentType:'text/html',body:`<!doctype html><html><head><style>${assets.css}</style></head><body><div class="layout"><nav class="sidebar"></nav><main class="main-content" id="dashboard-main"><div class="content"><a href="/elsewhere">Other page</a>${html}</div></main></div>${assets.script}</body></html>`}));
  try {
    await page.goto(origin+'/dashboard/flow/policies');
    await test(page);
    assert.deepEqual(errors, []);
    results.push({name,status:'passed'});
    console.log('PASS '+name);
  } catch (e) {
    results.push({name,status:'failed',error:e.message,errors});
    console.error('FAIL '+name+': '+e.stack);
    await page.screenshot({path:out+'/'+name+'-failed.png'});
  } finally { await context.close(); }
}
try {
  await check('account-profile-controls-and-permission-preview', assets.account, async page => {
    await page.locator('input[name=role][value=admin]').check();
    assert.equal(await page.locator('[data-acl-profile=observer]').isVisible(), false);
    assert.equal(await page.locator('[name=key_pattern]').isDisabled(), true);
    assert.match(await page.locator('[data-acl-profile-preview]').textContent(), /No scope restrictions/);
    await page.locator('input[name=role][value=custom]').check();
    assert.equal(await page.locator('[data-acl-profile=custom]').isVisible(), true);
    assert.equal(await page.locator('[name=modifiers]').isEnabled(), true);
    await page.locator('input[name=role][value=observer]').check();
    assert.equal(await page.locator('[name=modifiers]').isDisabled(), true);
    await page.locator('[name=key_pattern]').fill('<script>:*');
    assert.match(await page.locator('[data-acl-profile-preview]').textContent(), /<script>:\*/);
    assert.equal(await page.locator('[data-acl-profile-preview] script').count(), 0);
  });
  await check('policy-dirty-navigation-can-be-cancelled', assets.policy, async page => {
    await page.locator('[name=max_retries]').fill('11');
    assert.equal(await page.locator('[data-policy-dirty-status]').isVisible(), true);
    let prompted = false;
    page.on('dialog', async dialog => { prompted = true; await dialog.dismiss(); });
    await page.getByRole('link',{name:'Other page'}).click({noWaitAfter:true});
    await page.waitForTimeout(150);
    assert.equal(prompted, true);
    assert.equal(new URL(page.url()).pathname, '/dashboard/flow/policies');
    await page.locator('[name=max_retries]').fill('3');
    assert.equal(await page.locator('[data-policy-dirty-status]').isVisible(), false);
  });
  await check('policy-duration-keeps-exact-ms-and-readable-units', assets.policy, async page => {
    assert.match(await page.locator('[data-policy-preview=retention]').textContent(), /7d.*604800000 ms/);
    await page.locator('[name=retention_ttl_ms]').fill('9007199254740993');
    assert.match(await page.locator('[data-policy-preview=retention]').textContent(), /9007199254740993 ms/);
    await page.locator('[name=retention_ttl_ms]').fill('');
    assert.match(await page.locator('[data-policy-preview=retention]').textContent(), /invalid/);
  });
  await check('config-columns-ignore-long-value-min-content', assets.config, async page => {
    const widths = await page.locator('.config-parameters-table th').evaluateAll(nodes => nodes.map(n=>n.getBoundingClientRect().width));
    assert.ok(widths[0]>=170 && widths[5]>=220, JSON.stringify(widths));
    const region = page.locator('.table-scroll');
    assert.equal(await region.evaluate(e=>e.scrollWidth<=e.clientWidth+1),true);
    const cell = page.locator('.config-parameters-table td').nth(1);
    assert.equal(await cell.evaluate(e=>e.scrollWidth<=e.clientWidth+1),true);
  });
  await check('capability-probe-code-scale', assets.capabilities, async page => {
    const probe = page.locator('.ops-summary-code');
    assert.ok(await probe.evaluate(e=>parseFloat(getComputedStyle(e).fontSize))<=14);
  });
} finally { await browser.close(); }
await fs.writeFile(out+'/results.json',JSON.stringify(results,null,2));
console.log(`${results.filter(r=>r.status==='passed').length}/${results.length} passed`);
if(results.some(r=>r.status==='failed')) process.exitCode=1;
