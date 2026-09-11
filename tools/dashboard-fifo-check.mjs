import assert from 'node:assert/strict';
import {mkdir, writeFile} from 'node:fs/promises';
import {createRequire} from 'node:module';
import Module from 'node:module';
if(process.env.NODE_PATH) Module._initPaths();
const {chromium} = createRequire(import.meta.url)('playwright');
const base = process.env.DASHBOARD_URL || 'http://localhost:4000';
const out = process.env.DASHBOARD_OUT_DIR || 'test-results/dashboard-fifo';
await mkdir(out, {recursive:true});
const browser = await chromium.launch({channel:'chrome', headless:process.env.HEADFUL !== '1'});
const results = [];
const lanePath = partition => '/dashboard/flow/states?' + new URLSearchParams({type:'invoice_dispatch',state:'queued',partition_key:partition});
async function check(name, fn) {
  const context = await browser.newContext({viewport:{width:1440,height:1000}});
  const page = await context.newPage();
  page.setDefaultTimeout(8000);
  const errors = [];
  page.on('pageerror', error => errors.push(error.message));
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
  await check('running state retains logical FIFO policy and lanes through refresh', async page => {
    await page.goto(base + '/dashboard/flow/states?type=invoice_dispatch&state=running');
    const table = page.locator('[data-live-component="flow_states_table"]');
    assert.ok((await table.innerText()).includes('FIFO'));
    assert.ok(!(await table.innerText()).includes('parallel'));
    assert.equal(await page.locator('.flow-fifo-table tbody tr').count(), 2);
    const liveUrl = await page.locator('body').getAttribute('data-dashboard-live-url');
    const refreshed = page.waitForResponse(response => response.url() === new URL(liveUrl, base).href);
    const payload = await (await refreshed).json();
    assert.ok(payload.components.flow_states_table.includes('>FIFO<'));
    assert.ok(!payload.components.flow_states_table.includes('parallel'));
    assert.ok(payload.components.flow_fifo_lanes.includes('customer-1042'));
    assert.ok(payload.components.flow_fifo_lanes.includes('customer-2048'));
  });

  await check('overview stays compact and Inspect lane preserves all three scope fields', async page => {
    await page.goto(base + '/dashboard/flow/states?type=invoice_dispatch');
    assert.equal(await page.locator('.flow-fifo-table tbody tr').count(), 3);
    assert.equal(await page.locator('.flow-fifo-member-list').count(), 0);
    const row = page.locator('.flow-fifo-table tbody tr').filter({hasText:'customer-1042'});
    await row.getByRole('link',{name:'Inspect lane'}).click();
    await page.waitForURL(url=>url.searchParams.get('partition_key') === 'customer-1042');
    const url = new URL(page.url());
    assert.equal(url.searchParams.get('type'), 'invoice_dispatch');
    assert.equal(url.searchParams.get('state'), 'queued');
    assert.equal(await page.locator('.flow-fifo-table tbody tr').count(), 1);
    assert.equal(await page.locator('#flow-state-summaries').getAttribute('open'), null);
    const laneY = (await page.locator('.flow-fifo-table').boundingBox()).y;
    assert.ok(laneY < (await page.locator('#flow-state-summaries').boundingBox()).y);
  });

  await check('bounded member inspector supports keyboard and retains its state after refresh', async page => {
    await page.goto(base + lanePath('customer-1042'));
    const liveUrl = await page.locator('body').getAttribute('data-dashboard-live-url');
    let polls = 0;
    let values = 0;
    page.on('request', request => { if(request.url().includes('/value?')) values++; });
    await page.route(new URL(liveUrl, base).href, async route => {
      polls++;
      const response = await route.fetch();
      const payload = await response.json();
      payload.components.flow_fifo_lanes += '<span data-fifo-refreshed></span>';
      await route.fulfill({response, json:payload});
    });
    const inspector = page.locator('.flow-fifo-members');
    await inspector.locator('summary').focus();
    await page.keyboard.press('Enter');
    assert.equal(await inspector.locator('li').count(), 8);
    assert.ok((await inspector.locator('li').first().innerText()).includes('fifo-invoice-01'));
    assert.ok((await inspector.locator('li').first().innerText()).includes('Leased'));
    assert.ok((await inspector.innerText()).includes('2 more sampled members not shown'));
    await page.waitForTimeout(2300);
    assert.equal(polls,0);
    assert.ok(await inspector.locator('summary').evaluate(node=>node===document.activeElement));
    await page.locator('.subpage-title').click();
    await page.waitForSelector('[data-fifo-refreshed]',{state:'attached'});
    assert.equal(await inspector.getAttribute('open'), '');
    assert.equal(values,0);
    assert.ok(new URL(await page.locator('body').getAttribute('data-dashboard-live-url'),base).searchParams.get('partition_key') === 'customer-1042');
  });

  await check('hot sample does not claim completeness and related query includes the cold scheduled head', async page => {
    await page.goto(base + lanePath('customer-4096'));
    await page.locator('.flow-fifo-members > summary').click();
    const members = page.locator('.flow-fifo-member-list li');
    assert.equal(await members.count(),1);
    assert.ok((await members.nth(0).innerText()).includes('fifo-scheduled-02'));
    assert.ok((await page.locator('body').innerText()).includes('not the complete queue'));
    assert.ok(!(await page.locator('.flow-fifo-table').innerText()).includes('claimable'));
    await page.locator('.flow-fifo-members').getByRole('link',{name:'Related runs'}).click();
    await page.waitForURL(url=>url.pathname.endsWith('/query'));
    for(const id of ['fifo-scheduled-01','fifo-scheduled-02']) {
      assert.equal(await page.getByRole('link',{name:id,exact:true}).count(),1);
    }
  });

  await check('expired blocker and related runs remain scoped through navigation', async page => {
    await page.goto(base + lanePath('customer-2048'));
    await page.locator('.flow-fifo-members > summary').click();
    const member = page.locator('.flow-fifo-member-list li').first();
    assert.ok((await member.innerText()).includes('Lease expired'));
    await member.getByRole('link',{name:'fifo-expired-01',exact:true}).click();
    await page.waitForURL(url=>url.pathname.endsWith('/fifo-expired-01'));
    assert.equal(new URL(page.url()).searchParams.get('partition_key'),'customer-2048');
    await page.locator('.flow-detail-sections').getByRole('link',{name:'Related runs'}).click();
    await page.waitForURL(url=>url.pathname.endsWith('/query'));
    const params = new URL(page.url()).searchParams;
    assert.equal(params.get('type'),'invoice_dispatch');
    assert.equal(params.get('partition_key'),'customer-2048');
    assert.equal(params.get('state'),null);
    for(const id of ['fifo-expired-01','fifo-expired-02','fifo-expired-03']) {
      assert.equal(await page.getByRole('link',{name:id,exact:true}).count(),1);
    }
    assert.ok(!(await page.locator('body').innerText()).includes('fifo-invoice-01'));
  });

  await check('expanded lanes fit desktop widths and long identifiers', async page => {
    for(const width of [1280,1440,1920]) {
      await page.setViewportSize({width,height:1000});
      await page.goto(base + lanePath('customer-1042'));
      for (const field of await page.locator('.flow-state-filter-form .flow-filter-field').all()) {
        assert.ok(await field.evaluate(node => {
          const label = node.querySelector('span').getBoundingClientRect();
          const control = node.querySelector('input, select').getBoundingClientRect();
          return Math.abs(label.x - control.x) < 2 && control.y >= label.bottom && control.y - label.bottom < 10;
        }), 'filter label separated from control');
      }
      await page.locator('.flow-fifo-members > summary').click();
      assert.ok(await page.evaluate(()=>document.documentElement.scrollWidth <= innerWidth));
      await page.locator('[aria-label="FIFO lanes"]').screenshot({path:`${out}/fifo-${width}.png`});
      await page.locator('.flow-fifo-member-list a').first().evaluate(node => { node.textContent = 'workflow-' + 'x'.repeat(160); });
      assert.ok(await page.evaluate(()=>document.documentElement.scrollWidth <= innerWidth), 'long identifier layout fixture overflowed');
    }
  });

  await check('state summaries stay neutral for a blocked lane and persist across refresh', async page => {
    await page.goto(base + lanePath('customer-1042'));
    const summary = page.locator('#flow-state-summaries');
    await summary.locator('summary').click();
    assert.ok((await summary.innerText()).includes('Due time reached'));
    assert.ok(!(await summary.innerText()).includes('workers should drain'));
    assert.ok(!(await summary.innerText()).includes('no running sample'));
    assert.equal(await summary.locator('.bar-yellow, td.c-yellow').count(), 0);
    assert.ok((await page.locator('.flow-fifo-table').innerText()).includes('blocked by active flow'));
    await page.locator('.subpage-title').click();
    await page.waitForTimeout(2300);
    assert.equal(await summary.getAttribute('open'), '');
    assert.equal(new URL(await page.locator('body').getAttribute('data-dashboard-live-url'),base).searchParams.get('partition_key'),'customer-1042');
  });
} finally { await browser.close(); }
if(results.some(r=>!r.passed)) process.exitCode=1;
