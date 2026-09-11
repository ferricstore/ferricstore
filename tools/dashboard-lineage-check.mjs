import assert from 'node:assert/strict';
import {mkdir, writeFile} from 'node:fs/promises';
import {createRequire} from 'node:module';
import Module from 'node:module';
if (process.env.NODE_PATH) Module._initPaths();
const {chromium} = createRequire(import.meta.url)('playwright');
const base = process.env.DASHBOARD_URL || 'http://localhost:4000';
const out = process.env.DASHBOARD_OUT_DIR || 'test-results/dashboard-lineage';
const fixtureRoot = process.env.DASHBOARD_LINEAGE_TEST_ROOT;
const fixturePartition = process.env.DASHBOARD_LINEAGE_TEST_PARTITION;
await mkdir(out, {recursive:true});
const browser = await chromium.launch({channel:'chrome', headless:process.env.HEADFUL !== '1'});
const results = [];
const lineageUrl = (id, partition, limit = 40, cursor = '') => base + '/dashboard/flow/lineage?' +
  new URLSearchParams({id, partition_key:partition, mode:'root', limit:String(limit), ...(cursor ? {cursor} : {})});

async function check(name, fn) {
  const context = await browser.newContext({viewport:{width:1440,height:900}});
  const page = await context.newPage();
  page.setDefaultTimeout(5000);
  const errors = [];
  page.on('pageerror', error => errors.push(error.message));
  try {
    await fn(page);
    assert.deepEqual(errors, []);
    results.push({name, passed:true});
    console.log(`PASS ${name}`);
  } catch (error) {
    results.push({name, passed:false, error:error.stack});
    console.error(`FAIL ${name}: ${error.message}`);
  } finally {
    await context.close();
  }
}

try {
  await check('idle Lineage has required scope and no false result statistics', async page => {
    await page.goto(base + '/dashboard/flow/lineage');
    assert.equal(await page.getByLabel('Loaded lineage summary').count(), 0);
    assert.equal(await page.getByText('Relationship preview', {exact:true}).count(), 0);
    const form = page.locator('form[action="/dashboard/flow/lineage"]');
    await form.getByRole('button', {name:'Search',exact:true}).click();
    assert.equal(await form.locator('input[name="id"]').evaluate(el => el.validity.valueMissing), true);
    assert.equal(await form.locator('input[name="partition_key"]').evaluate(el => el.required), true);
  });

  await check('desktop Lineage is table-first and its preview is keyboard-accessible', async page => {
    for (const width of [1280,1440,1920]) {
      await page.setViewportSize({width,height:900});
      await page.goto(lineageUrl('order-processing-8891','tenant-acme'));
      const table = page.getByRole('heading',{name:'Lineage Records',exact:true});
      const preview = page.locator('summary').filter({hasText:'Relationship preview'});
      assert.ok((await table.boundingBox()).y < (await preview.boundingBox()).y);
      assert.equal(await page.locator('.flow-lineage-map').isVisible(), false);
      assert.ok((await page.locator('.flow-query-metadata').boundingBox()).height <= 90,
        'quality metadata should not push the record table below the first viewport');
      assert.ok((await page.locator('.flow-lineage-id-field input').boundingBox()).width >= 260);
      await preview.focus();
      await page.keyboard.press('Enter');
      assert.ok(await page.locator('.flow-lineage-map').isVisible());
      assert.ok((await page.locator('.flow-lineage-preview-count').innerText()).includes('1 of 1 loaded records'));
      assert.ok(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth));
    }
    await page.screenshot({path:`${out}/lineage-desktop.png`,fullPage:true});
  });

  await check('omitted values open details without fetching payload bytes', async page => {
    let valueRequests = 0;
    page.on('request', request => {if(request.url().includes('/value?')) valueRequests++;});
    await page.goto(lineageUrl('order-processing-8891','tenant-acme'));
    const rows = page.getByRole('region',{name:'Workflow lineage records',exact:true});
    assert.equal(await rows.getByText('none',{exact:true}).count(),0);
    await rows.getByRole('link',{name:'Inspect values',exact:true}).click();
    assert.ok(page.url().includes('/flow/order-processing-8891?partition_key=tenant-acme'));
    assert.ok(await page.getByRole('link',{name:'Open payload value',exact:true}).first().isVisible());
    assert.equal(valueRequests,0);
  });

  await check('invalid cursor is an actionable error and first-page recovery works', async page => {
    await page.goto(lineageUrl('order-processing-8891','tenant-acme',1,'invalid-cursor'));
    assert.ok((await page.getByRole('alert').innerText()).includes('restart from the first page'));
    assert.equal(await page.getByLabel('Loaded lineage summary').count(),0);
    await page.getByRole('link',{name:'First page',exact:true}).click();
    assert.equal(new URL(page.url()).searchParams.has('cursor'),false);
    assert.equal(await page.getByRole('alert').count(),0);
  });

  await check('manual Signals scans show a captured time and never start polling', async page => {
    let polls = 0;
    page.on('request', request => {if(request.url().includes('/dashboard/api/flow/signals')) polls++;});
    await page.goto(base + '/dashboard/flow/signals?type=order_fulfillment&partition_key=tenant-acme&scan=true');
    assert.match(await page.getByText(/^Scan captured /).innerText(), /Scan captured \d{4}-\d{2}-\d{2} .* UTC/);
    assert.equal(await page.locator('body').getAttribute('data-dashboard-live-url'),'');
    await page.waitForTimeout(2300);
    assert.equal(polls,0);
  });

  await check('Recovery quality stays compact and leaves records in the first viewport', async page => {
    await page.goto(base + '/dashboard/flow/failures?type=order_fulfillment&partition_key=tenant-acme&exact=true');
    const quality = page.locator('.flow-recovery-query-quality');
    assert.equal(await quality.count(),1);
    assert.ok((await quality.boundingBox()).height <= 180);
    assert.ok((await quality.innerText()).includes('projected exact'));
    assert.ok((await page.getByRole('region',{name:'Expired running leases',exact:true}).boundingBox()).y < 720);
  });

  if (fixtureRoot && fixturePartition) {
    await check('large Lineage preview stays capped and real pagination preserves scope', async page => {
      await page.goto(lineageUrl(fixtureRoot,fixturePartition,45));
      assert.equal(await page.locator('[aria-label="Workflow lineage records"] tbody tr').count(),45);
      await page.locator('summary').filter({hasText:'Relationship preview'}).click();
      assert.equal(await page.locator('.flow-lineage-node').count(),40);
      assert.ok((await page.locator('.flow-lineage-preview-count').innerText()).includes('40 of 45 loaded records'));
      await page.getByRole('link',{name:'View loaded table',exact:true}).click();
      assert.ok(page.url().endsWith('#flow-lineage-records'));

      await page.goto(lineageUrl(fixtureRoot,fixturePartition,20));
      const seen = [];
      for (const count of [20,20,5]) {
        const rows = page.locator('[aria-label="Workflow lineage records"] tbody tr');
        assert.equal(await rows.count(),count);
        seen.push(...await rows.locator('td:first-child').allTextContents());
        if (count === 20) {
          await page.getByRole('link',{name:'Next page',exact:true}).click();
          assert.equal(new URL(page.url()).searchParams.get('partition_key'),fixturePartition);
          assert.equal(new URL(page.url()).searchParams.get('id'),fixtureRoot);
        }
      }
      assert.equal(new Set(seen).size,45);
      assert.equal(await page.getByRole('link',{name:'Next page',exact:true}).count(),0);
      await page.locator('form[action="/dashboard/flow/lineage"]').getByRole('button',{name:'Search',exact:true}).click();
      assert.equal(new URL(page.url()).searchParams.has('cursor'),false);
      assert.equal(await page.locator('[aria-label="Workflow lineage records"] tbody tr').count(),20);
    });
  }
} finally {
  await browser.close();
  await writeFile(`${out}/report.json`,JSON.stringify(results,null,2));
}
if (results.some(result => !result.passed)) process.exitCode = 1;
