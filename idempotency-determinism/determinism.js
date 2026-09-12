(function () {
  'use strict';

  var currentScenario = 'double-charge';
  var currentApiMode = 'states'; // 'steps' shows the durable closure API
  var isAnimating = false;
  var animTimeouts = [];

  var doubleChargeContinueCode = `# ✓ DURABLE STEP: journal result + advance + renew ownership
from ferricstore import FlowClient

client = FlowClient.from_url("ferric://127.0.0.1:6388")
job = client.start_and_claim(
    "order-8492", type="checkout-saga",
    initial_state="charge", worker="checkout-1",
    payload={"amount": 15000},
)

# The closure may retry before its result commits. Keep this provider key stable.
job, charge = client.step(
    job,
    name="charge-customer:v1",
    run=lambda: {
        "id": stripe.Charge.create(
            amount=job.payload["amount"],
            idempotency_key=f"{job.id}:charge:v1",
        ).id,
    },
    to_state="reserve",
)
# job now carries the fresh ownership proof for the next step.`;

  var llmContinueCode = `# ✓ DURABLE STEP: save the expensive result before rendering
from ferricstore import FlowClient

client = FlowClient.from_url("ferric://127.0.0.1:6388")
job = client.start_and_claim(
    "report-204", type="ai-report",
    initial_state="query", worker="agent-1",
    payload={"prompt": "Summarize the launch research"},
)

job, summary = client.step(
    job,
    name="generate-summary:v1",
    run=lambda: llm.generate_once(
        operation_id=f"{job.id}:query:v1",
        prompt=job.payload["prompt"],
    ),
    to_state="render_pdf",
)
# A crash after this command resumes at render_pdf, not query.`;

  var inventoryContinueCode = `# ✓ DURABLE STEP: journal result plus an idempotent SQL mutation
from ferricstore import FlowClient

client = FlowClient.from_url("ferric://127.0.0.1:6388")
job = client.start_and_claim(
    "fulfill-8492", type="inventory-sync",
    initial_state="decrement", worker="warehouse-1",
    payload={"sku": "SKU-849", "qty": 1},
)

# The database must deduplicate this mutation ID if the closure retries.
job, remaining = client.step(
    job,
    name="decrement-inventory:v1",
    run=lambda: postgres.decrement_once(
        mutation_id=f"{job.id}:decrement:v1",
        sku=job.payload["sku"], qty=job.payload["qty"],
    ),
    to_state="send_email",
)`;

  var scenarios = {
    'double-charge': {
      title: '$150 checkout: a charge is retried',
      leftCode: `<span class="c-comment"># ❌ NAIVE SCRIPT: No step persistence</span>\n<span class="c-kw">import</span> stripe, inventory, email\n\n<span class="c-kw">def</span> <span class="c-fn">process_order</span>(order_id, amount):\n    <span class="c-danger"># ❌ Unmemoized: Re-executes on retry &amp; bills card again!\n    charge = stripe.Charge.create(amount=amount)</span>\n    inventory.lock_sku(order_id)\n    <span class="c-crash-line"># 💥 Worker crashes here (SIGKILL / OOM)</span>\n    email.send_receipt(charge.id)`,
      stepsCode: doubleChargeContinueCode,
      statesCode: `<span class="c-comment"># ✓ CURRENT SDK: durable states + guarded external effects</span>\n<span class="c-kw">from</span> ferricstore <span class="c-kw">import</span> WorkflowClient, complete, transition\n\nclient = WorkflowClient.from_url(<span class="c-str">"ferric://127.0.0.1:6388"</span>)\nflow = client.workflow(type=<span class="c-str">"checkout-saga"</span>, initial_state=<span class="c-str">"charge"</span>)\n\n<span class="c-decorator">@flow.state</span>(<span class="c-str">"charge"</span>)\n<span class="c-kw">def</span> <span class="c-fn">charge</span>(ctx):\n    <span class="c-decorator">@ctx.effect</span>(<span class="c-str">"charge"</span>, <span class="c-str">"stripe.charge"</span>, operation_digest=f<span class="c-str">"charge:{ctx.id}:v1"</span>)\n    <span class="c-kw">def</span> <span class="c-fn">call_stripe</span>():\n        <span class="c-kw">return</span> stripe.Charge.create(\n            amount=ctx.payload[<span class="c-str">"amount"</span>], idempotency_key=f<span class="c-str">"{ctx.id}:charge:v1"</span>\n        )\n    res = call_stripe()\n    <span class="c-kw">return</span> transition(<span class="c-str">"reserve"</span>, payload=ctx.payload, values={<span class="c-str">"tx_id"</span>: res.id.encode()})\n\n<span class="c-decorator">@flow.state</span>(<span class="c-str">"reserve"</span>)\n<span class="c-kw">def</span> <span class="c-fn">reserve</span>(ctx):\n    inventory.lock_sku(ctx.payload[<span class="c-str">"order_id"</span>], idempotency_key=f<span class="c-str">"{ctx.id}:reserve:v1"</span>)\n    <span class="c-crash-line"># 💥 A crash cannot roll the workflow state backward</span>\n    <span class="c-kw">return</span> transition(<span class="c-str">"send_receipt"</span>, payload=ctx.payload)\n\n<span class="c-decorator">@flow.state</span>(<span class="c-str">"send_receipt"</span>, claim_values=[<span class="c-str">"tx_id"</span>])\n<span class="c-kw">def</span> <span class="c-fn">send_receipt</span>(ctx):\n    <span class="c-decorator">@ctx.effect</span>(<span class="c-str">"receipt"</span>, <span class="c-str">"email.send"</span>, operation_digest=f<span class="c-str">"receipt:{ctx.id}:v1"</span>)\n    <span class="c-kw">def</span> <span class="c-fn">send</span>(): email.send(ctx.value(<span class="c-str">"tx_id"</span>))\n    send()\n    <span class="c-kw">return</span> complete(result=<span class="c-str">b"paid"</span>)`,
      s1Title: 'Charge Customer $150.00',
      s1LeftSub: 'Stripe API: POST /v1/charges (tx_a48f2910)',
      s1RightSubSteps: 'Continued to "reserve" with saved progress',
      s1RightSubStates: 'Saved state "charge" before continuing',
      s2Title: 'Lock Warehouse Inventory (SKU #849)',
      s2Sub: 'Stock decremented 10 ➔ 9 in Postgres',
      s3Title: 'Send Customer Email Receipt',
      leftOutcome: '$300.00 CHARGED (DOUBLE BILLED!)',
      leftOutcomeSub: 'Step 1 was not persisted to disk. Celery retry re-called Stripe with a new transaction ID.',
      rightOutcome: 'DURABLE STATE + GUARDED CHARGE',
      rightOutcomeSub: 'Saved workflow progress continues the task; Stripe still needs a stable idempotency key or ctx.effect guard.',
      naiveBilled: '$300.00',
      ferricBilled: '$150.00',
      divergence: 'Older write blocked',
      replayStepsText: `<span>Saved result: the retry resumes from the next step.</span><br><span>The stable Stripe key also prevents a repeated charge before the save.</span>`,
      replayStatesText: `<span>Saved progress: "charge" and "reserve" are already recorded.</span><br><span>The replacement worker starts at "send_receipt" after it takes over.</span><br><span>Receipt sent and the workflow completes.</span>`
    },
    'llm-tokens': {
      title: 'AI report: the 4,000-token result is retried',
      leftCode: `<span class="c-comment"># ❌ NAIVE SCRIPT: Wastes 4,000 tokens on retry</span>\n<span class="c-kw">import</span> openai, pdf, email\n\n<span class="c-kw">def</span> <span class="c-fn">generate_report</span>(prompt):\n    <span class="c-danger"># ❌ Expensive 4,000 token LLM query runs again on retry!\n    summary = openai.complete(prompt)</span>\n    pdf.render(summary)\n    <span class="c-crash-line"># 💥 Worker crashes during email upload</span>\n    email.send_attachment()`,
      stepsCode: llmContinueCode,
      statesCode: `<span class="c-comment"># ✓ CURRENT SDK: persist the LLM result between states</span>\n<span class="c-kw">from</span> ferricstore <span class="c-kw">import</span> WorkflowClient, complete, transition\n\nclient = WorkflowClient.from_url(<span class="c-str">"ferric://127.0.0.1:6388"</span>)\nflow = client.workflow(type=<span class="c-str">"ai-report"</span>, initial_state=<span class="c-str">"query"</span>)\n\n<span class="c-decorator">@flow.state</span>(<span class="c-str">"query"</span>)\n<span class="c-kw">def</span> <span class="c-fn">query</span>(ctx):\n    <span class="c-decorator">@ctx.effect</span>(<span class="c-str">"llm"</span>, <span class="c-str">"openai.complete"</span>, operation_digest=f<span class="c-str">"llm:{ctx.id}:v1"</span>)\n    <span class="c-kw">def</span> <span class="c-fn">call_llm</span>(): <span class="c-kw">return</span> openai.complete(ctx.payload[<span class="c-str">"prompt"</span>])\n    summary = call_llm()\n    <span class="c-kw">return</span> transition(<span class="c-str">"render"</span>, payload=ctx.payload, values={<span class="c-str">"summary"</span>: summary.encode()})\n\n<span class="c-decorator">@flow.state</span>(<span class="c-str">"render"</span>, claim_values=[<span class="c-str">"summary"</span>])\n<span class="c-kw">def</span> <span class="c-fn">render</span>(ctx):\n    pdf.render(ctx.value(<span class="c-str">"summary"</span>))\n    <span class="c-crash-line"># 💥 A retry resumes this state with the stored summary</span>\n    <span class="c-kw">return</span> transition(<span class="c-str">"send_email"</span>)\n\n<span class="c-decorator">@flow.state</span>(<span class="c-str">"send_email"</span>)\n<span class="c-kw">def</span> <span class="c-fn">send_email</span>(ctx):\n    <span class="c-decorator">@ctx.effect</span>(<span class="c-str">"email"</span>, <span class="c-str">"email.send"</span>, operation_digest=f<span class="c-str">"email:{ctx.id}:v1"</span>)\n    <span class="c-kw">def</span> <span class="c-fn">send</span>(): email.send()\n    send()\n    <span class="c-kw">return</span> complete(result=<span class="c-str">b"sent"</span>)`,
      s1Title: 'Synthesize 4,000-Token LLM Report',
      s1LeftSub: 'OpenAI API: 4,000 Tokens ($0.12)',
      s1RightSubSteps: 'Saved summary; continued with saved progress',
      s1RightSubStates: 'Saved the AI result before continuing',
      s2Title: 'Render 12-Page PDF Document',
      s2Sub: 'Generated PDF saved to local buffer',
      s3Title: 'Upload &amp; Email Attachment',
      leftOutcome: '8,000 TOKENS BURNED (2x API COST)',
      leftOutcomeSub: 'Naive retry re-executed Step 1 from scratch, wasting LLM latency and $0.24 API cost.',
      rightOutcome: 'DURABLE STATE + GUARDED LLM CALL',
      rightOutcomeSub: 'The saved result is reused; guard the LLM call if repeated token spend must be prevented.',
      naiveBilled: '8,000 Tokens',
      ferricBilled: '4,000 Tokens',
      divergence: 'Older write blocked',
      replayStepsText: `<span>Saved result: the retry resumes at render_pdf with the stored summary.</span><br><span>The stable operation ID protects the call before the save.</span>`,
      replayStatesText: `<span>Saved progress: "ai_query" and "render_pdf" are already recorded.</span><br><span>The replacement worker starts at "send_email" after it takes over.</span><br><span>Completed without burning duplicate LLM tokens.</span>`
    },
    'inventory-lock': {
      title: 'Inventory update: stock is decremented twice',
      leftCode: `<span class="c-comment"># ❌ NAIVE SCRIPT: Stock decremented twice</span>\n<span class="c-kw">import</span> postgres, email\n\n<span class="c-kw">def</span> <span class="c-fn">fulfill_order</span>(sku, qty):\n    <span class="c-danger"># ❌ Decrements stock in Postgres: stock = stock - 1 (qty=9)\n    postgres.execute("UPDATE inventory SET qty = qty - 1")</span>\n    <span class="c-crash-line"># 💥 Worker crashes</span>\n    email.send_confirmation()`,
      stepsCode: inventoryContinueCode,
      statesCode: `<span class="c-comment"># ✓ CURRENT SDK: idempotent external mutation + durable transition</span>\n<span class="c-kw">from</span> ferricstore <span class="c-kw">import</span> WorkflowClient, complete, transition\n\nclient = WorkflowClient.from_url(<span class="c-str">"ferric://127.0.0.1:6388"</span>)\nflow = client.workflow(type=<span class="c-str">"inventory-sync"</span>, initial_state=<span class="c-str">"decrement"</span>)\n\n<span class="c-decorator">@flow.state</span>(<span class="c-str">"decrement"</span>)\n<span class="c-kw">def</span> <span class="c-fn">decrement</span>(ctx):\n    <span class="c-decorator">@ctx.effect</span>(<span class="c-str">"decrement"</span>, <span class="c-str">"postgres.inventory"</span>, operation_digest=f<span class="c-str">"decrement:{ctx.id}:v1"</span>)\n    <span class="c-kw">def</span> <span class="c-fn">update</span>():\n        postgres.decrement(ctx.payload[<span class="c-str">"sku"</span>], ctx.payload[<span class="c-str">"qty"</span>], idempotency_key=f<span class="c-str">"{ctx.id}:decrement:v1"</span>)\n    update()\n    <span class="c-kw">return</span> transition(<span class="c-str">"send_email"</span>, payload=ctx.payload)\n\n<span class="c-decorator">@flow.state</span>(<span class="c-str">"send_email"</span>)\n<span class="c-kw">def</span> <span class="c-fn">send_email</span>(ctx):\n    <span class="c-decorator">@ctx.effect</span>(<span class="c-str">"email"</span>, <span class="c-str">"email.send"</span>, operation_digest=f<span class="c-str">"email:{ctx.id}:v1"</span>)\n    <span class="c-kw">def</span> <span class="c-fn">send</span>(): email.send()\n    send()\n    <span class="c-kw">return</span> complete(result=<span class="c-str">b"done"</span>)`,
      s1Title: 'Decrement Inventory (UPDATE items SET stock - 1)',
      s1LeftSub: 'Postgres: Stock updated 10 ➔ 9',
      s1RightSubSteps: 'Continued to "send_email" with saved progress',
      s1RightSubStates: 'Saved state "decrement" before continuing (Stock: 9)',
      s2Title: 'Generate Shipping Label &amp; Barcode',
      s2Sub: 'FedEx Tracking #9402 generated',
      s3Title: 'Send Confirmation Email',
      leftOutcome: 'INVENTORY CORRUPTED (STOCK = 8 INSTEAD OF 9)',
      leftOutcomeSub: 'Because Step 1 had no disk checkpoint, retry ran the SQL update twice for 1 single order.',
      rightOutcome: 'ACCURATE INVENTORY (STOCK = 9 EXACT)',
      rightOutcomeSub: 'Saved progress is not repeated; external inventory writes still need their own stable key.',
      naiveBilled: 'Stock: 8 (Bug)',
      ferricBilled: 'Stock: 9 (Exact)',
      divergence: 'Older write blocked',
      replayStepsText: `<span>Saved result: the retry resumes at send_email with the stored result.</span><br><span>The mutation ID separately prevents a second decrement before the save.</span>`,
      replayStatesText: `<span>Saved progress: "decrement" is already recorded.</span><br><span>The replacement worker starts at "send_email" after it takes over.</span><br><span>Inventory count is protected from a second decrement.</span>`
    }
  };

  var scenTabs = document.querySelectorAll('[data-scenario]');
  var scenTitle = document.querySelector('[data-scenario-title]');
  var apiToggles = document.querySelectorAll('[data-api-mode]');
  var rightTitle = document.querySelector('[data-right-title]');

  var leftCodeEl = document.querySelector('[data-left-code-content]');
  var rightCodeEl = document.querySelector('[data-right-code-content]');

  var btnAuto = document.querySelector('[data-btn-autoplay]');
  var btnStep1 = document.querySelector('[data-btn-step1]');
  var btnCrash = document.querySelector('[data-btn-crash]');
  var btnRetry = document.querySelector('[data-btn-retry]');
  var btnReset = document.querySelector('[data-btn-reset]');

  var termBody = document.querySelector('[data-term-body]');
  var termStatus = document.querySelector('[data-term-status]');

  var leftStep1 = document.querySelector('[data-left-step-1]');
  var leftStep2 = document.querySelector('[data-left-step-2]');
  var leftStep3 = document.querySelector('[data-left-step-3]');
  var leftRetryBox = document.querySelector('[data-left-retry-box]');

  var rightStep1 = document.querySelector('[data-right-step-1]');
  var rightStep2 = document.querySelector('[data-right-step-2]');
  var rightStep3 = document.querySelector('[data-right-step-3]');
  var rightReplayBox = document.querySelector('[data-right-replay-box]');

  var leftOutcome = document.querySelector('[data-left-outcome]');
  var leftOutcomeSub = document.querySelector('[data-left-outcome-sub]');
  var rightOutcome = document.querySelector('[data-right-outcome]');
  var rightOutcomeSub = document.querySelector('[data-right-outcome-sub]');
  var badgeScenario = document.querySelector('[data-badge-scenario]');
  var leftStatusPill = document.querySelector('[data-left-status-pill]');
  var rightStatusPill = document.querySelector('[data-right-status-pill]');

  var valNaiveBilled = document.querySelector('[data-val-naive-billed]');
  var valFerricBilled = document.querySelector('[data-val-ferric-billed]');
  var valReplayTime = document.querySelector('[data-val-replay-time]');
  var valDivergence = document.querySelector('[data-val-divergence]');

  function log(type, msg) {
    if (!termBody) return;
    var now = new Date();
    var ts = now.toTimeString().split(' ')[0] + '.' + String(now.getMilliseconds()).padStart(3, '0');
    var div = document.createElement('div');
    div.className = 'term-line ' + (type || 'info');
    div.innerHTML = '[' + ts + '] ' + msg;
    termBody.appendChild(div);
    termBody.scrollTop = termBody.scrollHeight;
  }

  function clearLogs() {
    if (termBody) termBody.innerHTML = '';
  }

  function clearAllTimeouts() {
    animTimeouts.forEach(function (t) { clearTimeout(t); });
    animTimeouts = [];
  }

  function renderScenarioDetails() {
    var data = scenarios[currentScenario];
    if (scenTitle) scenTitle.textContent = data.title;
    if (leftCodeEl) leftCodeEl.innerHTML = data.leftCode;
    if (rightCodeEl) rightCodeEl.innerHTML = currentApiMode === 'steps' ? data.stepsCode : data.statesCode;
    if (rightTitle) rightTitle.textContent = currentApiMode === 'steps' ? 'Save each action' : 'Save named states';
  }

  function resetToInitialState() {
    if (btnCrash) btnCrash.disabled = true;
    if (btnRetry) btnRetry.disabled = true;
    clearAllTimeouts();
    isAnimating = false;
    renderScenarioDetails();
    var data = scenarios[currentScenario];

    if (badgeScenario) {
      badgeScenario.textContent = 'READY TO RUN';
      badgeScenario.style.background = 'rgba(56, 189, 248, 0.18)';
      badgeScenario.style.color = '#7dd3fc';
    }
    if (leftStatusPill) leftStatusPill.textContent = 'No saved progress';
    if (rightStatusPill) rightStatusPill.textContent = 'Ready to save';

    // Left reset
    if (leftStep1) {
      leftStep1.className = 'stream-item';
      leftStep1.querySelector('strong').textContent = data.s1Title;
      leftStep1.querySelector('small').textContent = 'Pending...';
      leftStep1.querySelector('.s-badge').className = 's-badge';
      leftStep1.querySelector('.s-badge').textContent = 'Pending';
    }
    if (leftStep2) {
      leftStep2.className = 'stream-item';
      leftStep2.querySelector('strong').textContent = data.s2Title;
      leftStep2.querySelector('small').textContent = 'Pending...';
      leftStep2.querySelector('.s-badge').className = 's-badge';
      leftStep2.querySelector('.s-badge').textContent = 'Pending';
    }
    if (leftStep3) {
      leftStep3.className = 'stream-item';
      leftStep3.querySelector('strong').textContent = data.s3Title;
      leftStep3.querySelector('small').textContent = 'Pending...';
      leftStep3.querySelector('.s-badge').className = 's-badge';
      leftStep3.querySelector('.s-badge').textContent = 'Pending';
    }
    if (leftRetryBox) leftRetryBox.style.display = 'none';

    // Right reset
    if (rightStep1) {
      rightStep1.className = 'stream-item';
      rightStep1.querySelector('strong').textContent = currentApiMode === 'steps' ? ('Worker step: ' + data.s1Title) : ('State 1: @flow.state("charge")');
      rightStep1.querySelector('small').textContent = 'Pending...';
      rightStep1.querySelector('.s-badge').className = 's-badge';
      rightStep1.querySelector('.s-badge').textContent = 'Pending';
    }
    if (rightStep2) {
      rightStep2.className = 'stream-item';
      rightStep2.querySelector('strong').textContent = currentApiMode === 'steps' ? ('Next step: ' + data.s2Title) : ('State 2: @flow.state("reserve")');
      rightStep2.querySelector('small').textContent = 'Pending...';
      rightStep2.querySelector('.s-badge').className = 's-badge';
      rightStep2.querySelector('.s-badge').textContent = 'Pending';
    }
    if (rightStep3) {
      rightStep3.className = 'stream-item';
      rightStep3.querySelector('strong').textContent = currentApiMode === 'steps' ? ('Next step: ' + data.s3Title) : ('State 3: @flow.state("send_receipt")');
      rightStep3.querySelector('small').textContent = 'Pending...';
      rightStep3.querySelector('.s-badge').className = 's-badge';
      rightStep3.querySelector('.s-badge').textContent = 'Pending';
    }
    if (rightReplayBox) rightReplayBox.style.display = 'none';

    // Outcome text reset
    if (leftOutcome) leftOutcome.textContent = 'WAITING FOR EXECUTION';
    if (leftOutcomeSub) leftOutcomeSub.textContent = 'Choose a failure above, then run the example.';
    if (rightOutcome) rightOutcome.textContent = 'WAITING FOR EXECUTION';
    if (rightOutcomeSub) rightOutcomeSub.textContent = 'Saved progress is ready to compare with the retry.';

    if (valNaiveBilled) valNaiveBilled.textContent = '$0.00';
    if (valFerricBilled) valFerricBilled.textContent = '$0.00';
    if (valReplayTime) valReplayTime.textContent = '--';
    if (valDivergence) valDivergence.textContent = '0.0%';

    if (termStatus) termStatus.innerHTML = '<span class="pulse-dot"></span> SIMULATION READY';
  }

  function runInitialSteps(onDone) {
    resetToInitialState();
    isAnimating = true;
    if (badgeScenario) {
      badgeScenario.textContent = 'RUNNING';
      badgeScenario.style.background = 'rgba(139, 92, 246, 0.18)';
      badgeScenario.style.color = '#c4b5fd';
    }
    if (leftStatusPill) leftStatusPill.textContent = 'Running';
    if (rightStatusPill) rightStatusPill.textContent = 'Running';
    var data = scenarios[currentScenario];
    if (termStatus) termStatus.innerHTML = '<span class="pulse-dot" style="background:#8b5cf6; box-shadow: 0 0 8px #8b5cf6;"></span> EXECUTING INITIAL RUN...';

    log('info', 'Started the selected example with one worker.');

    // Step 1 Executing
    if (leftStep1) { leftStep1.className = 'stream-item is-executing'; leftStep1.querySelector('small').textContent = 'Executing...'; }
    if (rightStep1) { rightStep1.className = 'stream-item is-executing'; rightStep1.querySelector('small').textContent = 'Executing...'; }
    log('warn', 'Action 1: ' + data.s1Title);

    animTimeouts.push(setTimeout(function () {
      // Step 1 Completed
      if (leftStep1) {
        leftStep1.className = 'stream-item done';
        leftStep1.querySelector('small').textContent = data.s1LeftSub;
        leftStep1.querySelector('.s-badge').className = 's-badge green';
        leftStep1.querySelector('.s-badge').textContent = 'Committed';
      }
      if (rightStep1) {
        rightStep1.className = 'stream-item done';
        rightStep1.querySelector('small').textContent = currentApiMode === 'steps' ? data.s1RightSubSteps : data.s1RightSubStates;
        rightStep1.querySelector('.s-badge').className = 's-badge green';
        rightStep1.querySelector('.s-badge').textContent = 'Progress saved';
      }
      if (valNaiveBilled) valNaiveBilled.textContent = '$150.00';
      if (valFerricBilled) valFerricBilled.textContent = '$150.00';
      log('success', 'Action 1 saved before the command continued.');

      // Step 2 Executing
      if (leftStep2) { leftStep2.className = 'stream-item is-executing'; leftStep2.querySelector('small').textContent = 'Executing...'; }
      if (rightStep2) { rightStep2.className = 'stream-item is-executing'; rightStep2.querySelector('small').textContent = 'Executing...'; }
      log('info', 'Action 2: ' + data.s2Title);

      animTimeouts.push(setTimeout(function () {
        // Step 2 Completed
        if (leftStep2) {
          leftStep2.className = 'stream-item done';
          leftStep2.querySelector('small').textContent = data.s2Sub;
          leftStep2.querySelector('.s-badge').className = 's-badge green';
          leftStep2.querySelector('.s-badge').textContent = 'Committed';
        }
        if (rightStep2) {
          rightStep2.className = 'stream-item done';
          rightStep2.querySelector('small').textContent = 'Durable state committed before continuing';
          rightStep2.querySelector('.s-badge').className = 's-badge green';
          rightStep2.querySelector('.s-badge').textContent = 'Progress saved';
        }
        log('success', 'Action 2 saved before the crash point.');

        // Step 3 In-flight
        if (leftStep3) {
          leftStep3.className = 'stream-item is-executing';
          leftStep3.querySelector('small').textContent = 'Sending email in progress...';
          leftStep3.querySelector('.s-badge').className = 's-badge purple';
          leftStep3.querySelector('.s-badge').textContent = 'Running';
        }
        if (rightStep3) {
          rightStep3.className = 'stream-item is-executing';
          rightStep3.querySelector('small').textContent = 'Sending email in progress...';
          rightStep3.querySelector('.s-badge').className = 's-badge purple';
          rightStep3.querySelector('.s-badge').textContent = 'Running';
        }
        if (leftOutcome) leftOutcome.textContent = 'IN-FLIGHT EXECUTION (STEP 3 ACTIVE)';
        if (rightOutcome) rightOutcome.textContent = 'IN-FLIGHT EXECUTION (STEP 3 ACTIVE)';

        if (termStatus) termStatus.innerHTML = '<span class="pulse-dot" style="background:#38bdf8; box-shadow:0 0 8px #38bdf8;"></span> IN-FLIGHT EXECUTION (STEP 3)';
        if (btnCrash) btnCrash.disabled = false;
        isAnimating = false;
        if (onDone) onDone();
      }, 700));

    }, 700));
  }

  function injectCrash(onDone) {
    clearAllTimeouts();
    if (btnCrash) btnCrash.disabled = true;
    if (btnRetry) btnRetry.disabled = false;
    isAnimating = true;
    if (termStatus) termStatus.innerHTML = '<span class="pulse-dot" style="background:#ef4444; box-shadow: 0 0 8px #ef4444;"></span> WORKER CRASH DETECTED (SIGKILL)';
    if (badgeScenario) {
      badgeScenario.textContent = 'CRASH DETECTED';
      badgeScenario.style.background = 'rgba(239, 68, 68, 0.18)';
      badgeScenario.style.color = '#fca5a5';
    }
    if (leftStatusPill) leftStatusPill.textContent = 'Progress lost';
    if (rightStatusPill) rightStatusPill.textContent = 'Progress saved';

    log('danger', 'Worker stopped before the last action finished.');
    log('danger', 'Without saved progress, local memory was lost.');
    log('cyan', 'With FerricStore, earlier progress is ready for a replacement worker.');

    if (leftStep3) {
      leftStep3.className = 'stream-item crash is-crashed';
      leftStep3.querySelector('small').textContent = '💥 WORKER CRASH (SIGKILL / OOM)';
      leftStep3.querySelector('.s-badge').className = 's-badge red';
      leftStep3.querySelector('.s-badge').textContent = 'Crash';
    }
    if (rightStep3) {
      rightStep3.className = 'stream-item crash is-crashed';
      rightStep3.querySelector('small').textContent = '💥 WORKER CRASH (SIGKILL)';
      rightStep3.querySelector('.s-badge').className = 's-badge purple';
      rightStep3.querySelector('.s-badge').textContent = 'Resuming...';
    }

    if (leftOutcome) leftOutcome.textContent = 'WORKER STOPPED BEFORE COMPLETION';
    if (leftOutcomeSub) leftOutcomeSub.textContent = 'Local memory was lost. The retry starts from the first action.';
    if (rightOutcome) rightOutcome.textContent = 'SAVED PROGRESS AVAILABLE';
    if (rightOutcomeSub) rightOutcomeSub.textContent = 'Earlier progress is saved. A replacement worker can continue from the next action.';

    isAnimating = false;
    if (onDone) animTimeouts.push(setTimeout(onDone, 900));
  }

  function simulateWorkerRetry(onDone) {
    clearAllTimeouts();
    if (btnRetry) btnRetry.disabled = true;
    isAnimating = true;
    var data = scenarios[currentScenario];
    if (termStatus) termStatus.innerHTML = '<span class="pulse-dot" style="background:#f59e0b; box-shadow: 0 0 8px #f59e0b;"></span> REPLACEMENT WORKER RETRY IN PROGRESS...';
    if (badgeScenario) {
      badgeScenario.textContent = 'RETRY IN PROGRESS';
      badgeScenario.style.background = 'rgba(245, 158, 11, 0.18)';
      badgeScenario.style.color = '#fcd34d';
    }
    if (leftStatusPill) leftStatusPill.textContent = 'Repeating work';
    if (rightStatusPill) rightStatusPill.textContent = 'Using saved progress';

    log('warn', 'A replacement worker takes over the example.');

    // Left Side Retry: Re-runs Step 1
    if (leftStep1) {
      leftStep1.className = 'stream-item is-double-billed';
      leftStep1.querySelector('small').textContent = '🚨 RE-EXECUTING: Called Stripe API AGAIN with tx_3b91c048!';
      leftStep1.querySelector('.s-badge').className = 's-badge red';
      leftStep1.querySelector('.s-badge').textContent = '2x Double Charge!';
    }
    log('danger', 'No saved progress: the first action runs again.');

    // Right Side Retry: Instant Cache Hit
    if (rightStep1) {
      rightStep1.className = 'stream-item done is-cached';
      rightStep1.querySelector('small').textContent = '✓ Prior state committed; external effect guarded separately';
      rightStep1.querySelector('.s-badge').className = 's-badge green';
      rightStep1.querySelector('.s-badge').textContent = 'Saved state';
    }
    log('cyan', 'Saved progress: the first action is not run again.');

    animTimeouts.push(setTimeout(function () {
      // Step 2 on Left: Repeated
      if (leftStep2) {
        leftStep2.className = 'stream-item is-double-billed';
        leftStep2.querySelector('small').textContent = '🚨 RE-EXECUTING: Decremented inventory AGAIN (Stock = 8)!';
        leftStep2.querySelector('.s-badge').className = 's-badge red';
        leftStep2.querySelector('.s-badge').textContent = 'Corrupted (8)';
      }
      log('danger', 'The retry repeats the second action too.');

      // Step 2 on Right: Instant Cache Hit
      if (rightStep2) {
        rightStep2.className = 'stream-item done is-cached';
        rightStep2.querySelector('small').textContent = '✓ Prior state committed; mutation must be idempotent';
        rightStep2.querySelector('.s-badge').className = 's-badge green';
        rightStep2.querySelector('.s-badge').textContent = 'Saved state';
      }
      log('cyan', 'The saved state skips completed work; the outside mutation still needs its own stable key.');

      animTimeouts.push(setTimeout(function () {
        // Step 3 executes to completion on both
        if (leftStep3) {
          leftStep3.className = 'stream-item done';
          leftStep3.querySelector('small').textContent = 'Email receipt sent';
          leftStep3.querySelector('.s-badge').className = 's-badge green';
          leftStep3.querySelector('.s-badge').textContent = 'Done';
        }
        if (rightStep3) {
          rightStep3.className = 'stream-item done';
          rightStep3.querySelector('small').textContent = 'Email receipt sent & workflow complete!';
          rightStep3.querySelector('.s-badge').className = 's-badge green';
          rightStep3.querySelector('.s-badge').textContent = 'Completed';
        }

        // Show Retry/Replay Boxes
        if (leftRetryBox) leftRetryBox.style.display = 'block';
        if (rightReplayBox) {
          rightReplayBox.style.display = 'block';
          rightReplayBox.innerHTML = `<div class="replay-head">⚡ REPLACEMENT WORKER RESUMES AFTER RECLAIM:</div>` + (currentApiMode === 'steps' ? data.replayStepsText : data.replayStatesText);
        }

        // Final Outcome Displays
        if (leftOutcome) leftOutcome.textContent = data.leftOutcome;
        if (leftOutcomeSub) leftOutcomeSub.textContent = data.leftOutcomeSub;
        if (rightOutcome) rightOutcome.textContent = data.rightOutcome;
        if (rightOutcomeSub) rightOutcomeSub.textContent = data.rightOutcomeSub;

        if (valNaiveBilled) valNaiveBilled.textContent = data.naiveBilled;
        if (valFerricBilled) valFerricBilled.textContent = data.ferricBilled;
        if (valReplayTime) valReplayTime.textContent = 'After takeover';
        if (valDivergence) valDivergence.textContent = data.divergence;
        if (badgeScenario) {
          badgeScenario.textContent = 'SIMULATION COMPLETE';
          badgeScenario.style.background = 'rgba(16, 185, 129, 0.18)';
          badgeScenario.style.color = '#6ee7b7';
        }
        if (leftStatusPill) leftStatusPill.textContent = 'Retry complete';
        if (rightStatusPill) rightStatusPill.textContent = 'Saved progress used';

        if (termStatus) termStatus.innerHTML = '<span class="pulse-dot" style="background:#34d399; box-shadow: 0 0 8px #34d399;"></span> SIMULATION COMPLETE';
        log('success', 'Example complete. Saved workflow state and stable outside-action keys work together.');

        isAnimating = false;
        if (onDone) onDone();
      }, 700));

    }, 700));
  }

  function playFullAutoSimulation() {
    clearLogs();
    log('info', 'Running the full example...');
    runInitialSteps(function () {
      animTimeouts.push(setTimeout(function () {
        injectCrash(function () {
          animTimeouts.push(setTimeout(function () {
            simulateWorkerRetry();
          }, 800));
        });
      }, 800));
    });
  }

  // --- Scenario Tab Switching ---
  scenTabs.forEach(function (tab) {
    tab.addEventListener('click', function () {
      scenTabs.forEach(function (t) { t.classList.remove('is-active'); });
      tab.classList.add('is-active');
      scenTabs.forEach(function (t) { t.setAttribute('aria-selected', String(t === tab)); });
      currentScenario = tab.getAttribute('data-scenario') || 'double-charge';
      resetToInitialState();
      clearLogs();
      log('info', 'Selected ' + scenarios[currentScenario].title + '. Run the example when ready.');
    });
  });

  // --- API Toggle Switching ---
  apiToggles.forEach(function (btn) {
    btn.addEventListener('click', function () {
      apiToggles.forEach(function (b) { b.classList.remove('is-active'); });
      btn.classList.add('is-active');
      currentApiMode = btn.getAttribute('data-api-mode') || 'states';
      resetToInitialState();
      clearLogs();
      log('info', 'Selected ' + (currentApiMode === 'steps' ? 'save each action' : 'save named states') + '. Run the example when ready.');
    });
  });

  // --- Button Handlers ---
  if (btnAuto) {
    btnAuto.addEventListener('click', function () {
      playFullAutoSimulation();
    });
  }

  if (btnStep1) {
    btnStep1.addEventListener('click', function () {
      clearLogs();
      runInitialSteps();
    });
  }

  if (btnCrash) {
    btnCrash.addEventListener('click', function () {
      injectCrash();
    });
  }

  if (btnRetry) {
    btnRetry.addEventListener('click', function () {
      simulateWorkerRetry();
    });
  }

  if (btnReset) {
    btnReset.addEventListener('click', function () {
      resetToInitialState();
      clearLogs();
      log('info', 'Example reset. Choose a failure, then run it.');
    });
  }

  // Start on the first state so visitors can choose the failure before running it.
  resetToInitialState();
  clearLogs();
  log('info', 'Ready. Choose a failure, then run the example.');
})();
