(function () {
  'use strict';

  var currentScenario = 'double-charge';
  var currentApiMode = 'states'; // 'steps' shows the real fused continuation API
  var isAnimating = false;
  var animTimeouts = [];

  var doubleChargeContinueCode = `# ✓ REAL FUSED STEP: persist output + advance + renew ownership
from ferricstore import FlowClient

client = FlowClient.from_url("ferric://127.0.0.1:6388")
job = client.start_and_claim(
    "order-8492", type="checkout-saga",
    initial_state="charge", worker="checkout-1",
    payload={"amount": 15000},
)

# STEP_CONTINUE does not make Stripe exactly-once. Keep this key stable.
charge = stripe.Charge.create(
    amount=job.payload["amount"],
    idempotency_key=f"{job.id}:charge:v1",
)
job = client.step_continue(
    job.id,
    lease_token=job.lease_token,
    fencing_token=job.fencing_token,
    from_state=job.run_state,
    to_state="reserve",
    values={"tx_id": charge.id},
    worker="checkout-1",
    return_job=True,
)
# job now carries the fresh lease + fence for the next step.`;

  var llmContinueCode = `# ✓ REAL FUSED STEP: save the expensive result before rendering
from ferricstore import FlowClient

client = FlowClient.from_url("ferric://127.0.0.1:6388")
job = client.start_and_claim(
    "report-204", type="ai-report",
    initial_state="query", worker="agent-1",
    payload={"prompt": "Summarize the launch research"},
)

# Application guard: the same operation_id returns the same stored result.
summary = llm.generate_once(
    operation_id=f"{job.id}:query:v1",
    prompt=job.payload["prompt"],
)
job = client.step_continue(
    job.id,
    lease_token=job.lease_token,
    fencing_token=job.fencing_token,
    from_state=job.run_state,
    to_state="render_pdf",
    values={"summary": summary},
    worker="agent-1",
    return_job=True,
)
# A crash after this command resumes at render_pdf, not query.`;

  var inventoryContinueCode = `# ✓ REAL FUSED STEP: durable state plus an idempotent SQL mutation
from ferricstore import FlowClient

client = FlowClient.from_url("ferric://127.0.0.1:6388")
job = client.start_and_claim(
    "fulfill-8492", type="inventory-sync",
    initial_state="decrement", worker="warehouse-1",
    payload={"sku": "SKU-849", "qty": 1},
)

# The database must deduplicate this mutation ID if the worker retries.
remaining = postgres.decrement_once(
    mutation_id=f"{job.id}:decrement:v1",
    sku=job.payload["sku"], qty=job.payload["qty"],
)
job = client.step_continue(
    job.id,
    lease_token=job.lease_token,
    fencing_token=job.fencing_token,
    from_state=job.run_state,
    to_state="send_email",
    values={"remaining": remaining},
    worker="warehouse-1",
    return_job=True,
)`;

  var scenarios = {
    'double-charge': {
      title: 'Scenario A: E-Commerce Checkout ($150.00 Order #8492)',
      leftCode: `<span class="c-comment"># ❌ NAIVE SCRIPT: No step persistence</span>\n<span class="c-kw">import</span> stripe, inventory, email\n\n<span class="c-kw">def</span> <span class="c-fn">process_order</span>(order_id, amount):\n    <span class="c-danger"># ❌ Unmemoized: Re-executes on retry &amp; bills card again!\n    charge = stripe.Charge.create(amount=amount)</span>\n    inventory.lock_sku(order_id)\n    <span class="c-crash-line"># 💥 Worker crashes here (SIGKILL / OOM)</span>\n    email.send_receipt(charge.id)`,
      stepsCode: doubleChargeContinueCode,
      statesCode: `<span class="c-comment"># ✓ CURRENT SDK: durable states + guarded external effects</span>\n<span class="c-kw">from</span> ferricstore <span class="c-kw">import</span> WorkflowClient, complete, transition\n\nclient = WorkflowClient.from_url(<span class="c-str">"ferric://127.0.0.1:6388"</span>)\nflow = client.workflow(type=<span class="c-str">"checkout-saga"</span>, initial_state=<span class="c-str">"charge"</span>)\n\n<span class="c-decorator">@flow.state</span>(<span class="c-str">"charge"</span>)\n<span class="c-kw">def</span> <span class="c-fn">charge</span>(ctx):\n    <span class="c-decorator">@ctx.effect</span>(<span class="c-str">"charge"</span>, <span class="c-str">"stripe.charge"</span>, operation_digest=f<span class="c-str">"charge:{ctx.id}:v1"</span>)\n    <span class="c-kw">def</span> <span class="c-fn">call_stripe</span>():\n        <span class="c-kw">return</span> stripe.Charge.create(\n            amount=ctx.payload[<span class="c-str">"amount"</span>], idempotency_key=f<span class="c-str">"{ctx.id}:charge:v1"</span>\n        )\n    res = call_stripe()\n    <span class="c-kw">return</span> transition(<span class="c-str">"reserve"</span>, payload=ctx.payload, values={<span class="c-str">"tx_id"</span>: res.id.encode()})\n\n<span class="c-decorator">@flow.state</span>(<span class="c-str">"reserve"</span>)\n<span class="c-kw">def</span> <span class="c-fn">reserve</span>(ctx):\n    inventory.lock_sku(ctx.payload[<span class="c-str">"order_id"</span>], idempotency_key=f<span class="c-str">"{ctx.id}:reserve:v1"</span>)\n    <span class="c-crash-line"># 💥 A crash cannot roll the workflow state backward</span>\n    <span class="c-kw">return</span> transition(<span class="c-str">"send_receipt"</span>, payload=ctx.payload)\n\n<span class="c-decorator">@flow.state</span>(<span class="c-str">"send_receipt"</span>, claim_values=[<span class="c-str">"tx_id"</span>])\n<span class="c-kw">def</span> <span class="c-fn">send_receipt</span>(ctx):\n    <span class="c-decorator">@ctx.effect</span>(<span class="c-str">"receipt"</span>, <span class="c-str">"email.send"</span>, operation_digest=f<span class="c-str">"receipt:{ctx.id}:v1"</span>)\n    <span class="c-kw">def</span> <span class="c-fn">send</span>(): email.send(ctx.value(<span class="c-str">"tx_id"</span>))\n    send()\n    <span class="c-kw">return</span> complete(result=<span class="c-str">b"paid"</span>)`,
      s1Title: 'Charge Customer $150.00',
      s1LeftSub: 'Stripe API: POST /v1/charges (tx_a48f2910)',
      s1RightSubSteps: 'Continued to "reserve" with a fresh lease + fence',
      s1RightSubStates: 'State "charge" committed to Raft quorum',
      s2Title: 'Lock Warehouse Inventory (SKU #849)',
      s2Sub: 'Stock decremented 10 ➔ 9 in Postgres',
      s3Title: 'Send Customer Email Receipt',
      leftOutcome: '$300.00 CHARGED (DOUBLE BILLED!)',
      leftOutcomeSub: 'Step 1 was not persisted to disk. Celery retry re-called Stripe with a new transaction ID.',
      rightOutcome: 'DURABLE STATE + GUARDED CHARGE',
      rightOutcomeSub: 'FerricStore fences stale state writes; Stripe needs a stable idempotency key or ctx.effect guard.',
      naiveBilled: '$300.00',
      ferricBilled: '$150.00',
      divergence: 'Stale writes fenced',
      replayStepsText: `<span>FLOW.STEP_CONTINUE resumes from the committed next state.</span><br><span>The stable Stripe key separately prevents a repeated charge.</span>`,
      replayStatesText: `<span>⚡ FSM State Machine: States "charge" &amp; "reserve" already committed in Raft log.</span><br><span>⚡ Replacement worker loads state "send_receipt" after reclaim.</span><br><span>✉️ State "send_receipt": Dispatches receipt and calls complete()!</span>`
    },
    'llm-tokens': {
      title: 'Scenario B: AI Token & Compute Waste (4,000 Token Generation)',
      leftCode: `<span class="c-comment"># ❌ NAIVE SCRIPT: Wastes 4,000 tokens on retry</span>\n<span class="c-kw">import</span> openai, pdf, email\n\n<span class="c-kw">def</span> <span class="c-fn">generate_report</span>(prompt):\n    <span class="c-danger"># ❌ Expensive 4,000 token LLM query runs again on retry!\n    summary = openai.complete(prompt)</span>\n    pdf.render(summary)\n    <span class="c-crash-line"># 💥 Worker crashes during email upload</span>\n    email.send_attachment()`,
      stepsCode: llmContinueCode,
      statesCode: `<span class="c-comment"># ✓ CURRENT SDK: persist the LLM result between states</span>\n<span class="c-kw">from</span> ferricstore <span class="c-kw">import</span> WorkflowClient, complete, transition\n\nclient = WorkflowClient.from_url(<span class="c-str">"ferric://127.0.0.1:6388"</span>)\nflow = client.workflow(type=<span class="c-str">"ai-report"</span>, initial_state=<span class="c-str">"query"</span>)\n\n<span class="c-decorator">@flow.state</span>(<span class="c-str">"query"</span>)\n<span class="c-kw">def</span> <span class="c-fn">query</span>(ctx):\n    <span class="c-decorator">@ctx.effect</span>(<span class="c-str">"llm"</span>, <span class="c-str">"openai.complete"</span>, operation_digest=f<span class="c-str">"llm:{ctx.id}:v1"</span>)\n    <span class="c-kw">def</span> <span class="c-fn">call_llm</span>(): <span class="c-kw">return</span> openai.complete(ctx.payload[<span class="c-str">"prompt"</span>])\n    summary = call_llm()\n    <span class="c-kw">return</span> transition(<span class="c-str">"render"</span>, payload=ctx.payload, values={<span class="c-str">"summary"</span>: summary.encode()})\n\n<span class="c-decorator">@flow.state</span>(<span class="c-str">"render"</span>, claim_values=[<span class="c-str">"summary"</span>])\n<span class="c-kw">def</span> <span class="c-fn">render</span>(ctx):\n    pdf.render(ctx.value(<span class="c-str">"summary"</span>))\n    <span class="c-crash-line"># 💥 A retry resumes this state with the stored summary</span>\n    <span class="c-kw">return</span> transition(<span class="c-str">"send_email"</span>)\n\n<span class="c-decorator">@flow.state</span>(<span class="c-str">"send_email"</span>)\n<span class="c-kw">def</span> <span class="c-fn">send_email</span>(ctx):\n    <span class="c-decorator">@ctx.effect</span>(<span class="c-str">"email"</span>, <span class="c-str">"email.send"</span>, operation_digest=f<span class="c-str">"email:{ctx.id}:v1"</span>)\n    <span class="c-kw">def</span> <span class="c-fn">send</span>(): email.send()\n    send()\n    <span class="c-kw">return</span> complete(result=<span class="c-str">b"sent"</span>)`,
      s1Title: 'Synthesize 4,000-Token LLM Report',
      s1LeftSub: 'OpenAI API: 4,000 Tokens ($0.12)',
      s1RightSubSteps: 'Saved summary; continued with a fresh lease + fence',
      s1RightSubStates: 'State "ai_query" payload committed to disk',
      s2Title: 'Render 12-Page PDF Document',
      s2Sub: 'Generated PDF saved to local buffer',
      s3Title: 'Upload &amp; Email Attachment',
      leftOutcome: '8,000 TOKENS BURNED (2x API COST)',
      leftOutcomeSub: 'Naive retry re-executed Step 1 from scratch, wasting LLM latency and $0.24 API cost.',
      rightOutcome: 'DURABLE STATE + GUARDED LLM CALL',
      rightOutcomeSub: 'The state is durable; guard the LLM call if repeated token spend must be prevented.',
      naiveBilled: '8,000 Tokens',
      ferricBilled: '4,000 Tokens',
      divergence: 'Stale writes fenced',
      replayStepsText: `<span>FLOW.STEP_CONTINUE resumes at render_pdf with the stored summary.</span><br><span>An application guard separately prevents repeated model spend.</span>`,
      replayStatesText: `<span>⚡ FSM Replay: States "ai_query" &amp; "render_pdf" already committed in Raft log.</span><br><span>⚡ Replacement worker resumes at "send_email" after reclaim.</span><br><span>✉️ Completed without burning duplicate LLM tokens!</span>`
    },
    'inventory-lock': {
      title: 'Scenario C: Inventory Double-Decrement Bug',
      leftCode: `<span class="c-comment"># ❌ NAIVE SCRIPT: Stock decremented twice</span>\n<span class="c-kw">import</span> postgres, email\n\n<span class="c-kw">def</span> <span class="c-fn">fulfill_order</span>(sku, qty):\n    <span class="c-danger"># ❌ Decrements stock in Postgres: stock = stock - 1 (qty=9)\n    postgres.execute("UPDATE inventory SET qty = qty - 1")</span>\n    <span class="c-crash-line"># 💥 Worker crashes</span>\n    email.send_confirmation()`,
      stepsCode: inventoryContinueCode,
      statesCode: `<span class="c-comment"># ✓ CURRENT SDK: idempotent external mutation + durable transition</span>\n<span class="c-kw">from</span> ferricstore <span class="c-kw">import</span> WorkflowClient, complete, transition\n\nclient = WorkflowClient.from_url(<span class="c-str">"ferric://127.0.0.1:6388"</span>)\nflow = client.workflow(type=<span class="c-str">"inventory-sync"</span>, initial_state=<span class="c-str">"decrement"</span>)\n\n<span class="c-decorator">@flow.state</span>(<span class="c-str">"decrement"</span>)\n<span class="c-kw">def</span> <span class="c-fn">decrement</span>(ctx):\n    <span class="c-decorator">@ctx.effect</span>(<span class="c-str">"decrement"</span>, <span class="c-str">"postgres.inventory"</span>, operation_digest=f<span class="c-str">"decrement:{ctx.id}:v1"</span>)\n    <span class="c-kw">def</span> <span class="c-fn">update</span>():\n        postgres.decrement(ctx.payload[<span class="c-str">"sku"</span>], ctx.payload[<span class="c-str">"qty"</span>], idempotency_key=f<span class="c-str">"{ctx.id}:decrement:v1"</span>)\n    update()\n    <span class="c-kw">return</span> transition(<span class="c-str">"send_email"</span>, payload=ctx.payload)\n\n<span class="c-decorator">@flow.state</span>(<span class="c-str">"send_email"</span>)\n<span class="c-kw">def</span> <span class="c-fn">send_email</span>(ctx):\n    <span class="c-decorator">@ctx.effect</span>(<span class="c-str">"email"</span>, <span class="c-str">"email.send"</span>, operation_digest=f<span class="c-str">"email:{ctx.id}:v1"</span>)\n    <span class="c-kw">def</span> <span class="c-fn">send</span>(): email.send()\n    send()\n    <span class="c-kw">return</span> complete(result=<span class="c-str">b"done"</span>)`,
      s1Title: 'Decrement Inventory (UPDATE items SET stock - 1)',
      s1LeftSub: 'Postgres: Stock updated 10 ➔ 9',
      s1RightSubSteps: 'Continued to "send_email" with a fresh lease + fence',
      s1RightSubStates: 'State "decrement" committed (Stock: 9)',
      s2Title: 'Generate Shipping Label &amp; Barcode',
      s2Sub: 'FedEx Tracking #9402 generated',
      s3Title: 'Send Confirmation Email',
      leftOutcome: 'INVENTORY CORRUPTED (STOCK = 8 INSTEAD OF 9)',
      leftOutcomeSub: 'Because Step 1 had no disk checkpoint, retry ran the SQL update twice for 1 single order.',
      rightOutcome: 'ACCURATE INVENTORY (STOCK = 9 EXACT)',
      rightOutcomeSub: 'The committed decrement state is not revisited after reclaim; external inventory writes still need their own stable key.',
      naiveBilled: 'Stock: 8 (Bug)',
      ferricBilled: 'Stock: 9 (Exact)',
      divergence: 'Stale writes fenced',
      replayStepsText: `<span>FLOW.STEP_CONTINUE resumes at send_email.</span><br><span>The database mutation ID separately prevents a second decrement.</span>`,
      replayStatesText: `<span>⚡ FSM Replay: State "decrement" sealed in Raft log.</span><br><span>⚡ Replacement worker resumes at "send_email" after reclaim.</span><br><span>✉️ Inventory count protected from double-decrement!</span>`
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
    if (rightTitle) rightTitle.textContent = currentApiMode === 'steps' ? 'Fused Step Continue' : 'Durable SDK (States API - FSM)';
  }

  function resetToInitialState() {
    clearAllTimeouts();
    isAnimating = false;
    renderScenarioDetails();
    var data = scenarios[currentScenario];

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
    if (leftOutcomeSub) leftOutcomeSub.textContent = 'Click "⚡ Play Live Interactive Simulation" or "▶ 1. Initial Run" above.';
    if (rightOutcome) rightOutcome.textContent = 'WAITING FOR EXECUTION';
    if (rightOutcomeSub) rightOutcomeSub.textContent = 'Durable Raft quorum engine standing by at ferric://127.0.0.1:6388.';

    if (valNaiveBilled) valNaiveBilled.textContent = '$0.00';
    if (valFerricBilled) valFerricBilled.textContent = '$0.00';
    if (valReplayTime) valReplayTime.textContent = '--';
    if (valDivergence) valDivergence.textContent = '0.0%';

    if (termStatus) termStatus.innerHTML = '<span class="pulse-dot"></span> SIMULATION READY';
  }

  function runInitialSteps(onDone) {
    resetToInitialState();
    isAnimating = true;
    var data = scenarios[currentScenario];
    if (termStatus) termStatus.innerHTML = '<span class="pulse-dot" style="background:#8b5cf6; box-shadow: 0 0 8px #8b5cf6;"></span> EXECUTING INITIAL RUN...';

    log('info', '🚀 Dispatched workflow order #8492 to worker-pod-1...');

    // Step 1 Executing
    if (leftStep1) { leftStep1.className = 'stream-item is-executing'; leftStep1.querySelector('small').textContent = 'Executing...'; }
    if (rightStep1) { rightStep1.className = 'stream-item is-executing'; rightStep1.querySelector('small').textContent = 'Executing...'; }
    log('warn', '💳 Step 1: Calling external API for: ' + data.s1Title);

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
        rightStep1.querySelector('.s-badge').textContent = 'Raft Disk Saved';
      }
      if (valNaiveBilled) valNaiveBilled.textContent = '$150.00';
      if (valFerricBilled) valFerricBilled.textContent = '$150.00';
      log('success', '✓ Step 1 Success: Result durably committed before the command returned.');

      // Step 2 Executing
      if (leftStep2) { leftStep2.className = 'stream-item is-executing'; leftStep2.querySelector('small').textContent = 'Executing...'; }
      if (rightStep2) { rightStep2.className = 'stream-item is-executing'; rightStep2.querySelector('small').textContent = 'Executing...'; }
      log('info', '📦 Step 2: Executing ' + data.s2Title);

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
          rightStep2.querySelector('.s-badge').textContent = 'Raft Disk Saved';
        }
        log('success', '✓ Step 2 Success: State persisted to disk log.');

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
        isAnimating = false;
        if (onDone) onDone();
      }, 700));

    }, 700));
  }

  function injectCrash(onDone) {
    clearAllTimeouts();
    isAnimating = true;
    if (termStatus) termStatus.innerHTML = '<span class="pulse-dot" style="background:#ef4444; box-shadow: 0 0 8px #ef4444;"></span> WORKER CRASH DETECTED (SIGKILL)';

    log('danger', '💥 [FATAL CRASH] SIGKILL (Exit code 137 / Out-of-Memory). worker-pod-1 terminated unexpectedly!');
    log('danger', '🚨 Left Side: Volatile container RAM wiped to 0MB. All uncommitted in-memory states lost.');
    log('cyan', '🛡️ Right Side (FerricStore): Prior states safely recorded on Raft disk. Ready for replacement worker.');

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

    if (leftOutcome) leftOutcome.textContent = 'WORKER DIED MID-EXECUTION';
    if (leftOutcomeSub) leftOutcomeSub.textContent = 'In-memory RAM lost. Celery/SQS queue will now attempt to retry from line 1.';
    if (rightOutcome) rightOutcome.textContent = 'DISK CHECKPOINTS INTACT';
    if (rightOutcomeSub) rightOutcomeSub.textContent = 'Prior states are safe in the Raft log. A replacement worker can reclaim the flow.';

    isAnimating = false;
    if (onDone) animTimeouts.push(setTimeout(onDone, 900));
  }

  function simulateWorkerRetry(onDone) {
    clearAllTimeouts();
    isAnimating = true;
    var data = scenarios[currentScenario];
    if (termStatus) termStatus.innerHTML = '<span class="pulse-dot" style="background:#f59e0b; box-shadow: 0 0 8px #f59e0b;"></span> REPLACEMENT WORKER RETRY IN PROGRESS...';

    log('warn', '🔄 Queue scheduler spawns worker-pod-2 to resume workflow...');

    // Left Side Retry: Re-runs Step 1
    if (leftStep1) {
      leftStep1.className = 'stream-item is-double-billed';
      leftStep1.querySelector('small').textContent = '🚨 RE-EXECUTING: Called Stripe API AGAIN with tx_3b91c048!';
      leftStep1.querySelector('.s-badge').className = 's-badge red';
      leftStep1.querySelector('.s-badge').textContent = '2x Double Charge!';
    }
    log('danger', '❌ [HAZARD] Naive Worker has no disk checkpoint. Re-executing Step 1... CARD CHARGED AGAIN (+$150.00)!');

    // Right Side Retry: Instant Cache Hit
    if (rightStep1) {
      rightStep1.className = 'stream-item done is-cached';
      rightStep1.querySelector('small').textContent = '✓ Prior state committed; external effect guarded separately';
      rightStep1.querySelector('.s-badge').className = 's-badge green';
      rightStep1.querySelector('.s-badge').textContent = 'Durable state';
    }
    log('cyan', '⚡ [FERRICSTORE] Prior state already committed. Stable provider key protects the Stripe effect.');

    animTimeouts.push(setTimeout(function () {
      // Step 2 on Left: Repeated
      if (leftStep2) {
        leftStep2.className = 'stream-item is-double-billed';
        leftStep2.querySelector('small').textContent = '🚨 RE-EXECUTING: Decremented inventory AGAIN (Stock = 8)!';
        leftStep2.querySelector('.s-badge').className = 's-badge red';
        leftStep2.querySelector('.s-badge').textContent = 'Corrupted (8)';
      }
      log('danger', '❌ [HAZARD] Naive Worker decremented Postgres stock a second time! Inventory desynced.');

      // Step 2 on Right: Instant Cache Hit
      if (rightStep2) {
        rightStep2.className = 'stream-item done is-cached';
        rightStep2.querySelector('small').textContent = '✓ Prior state committed; mutation must be idempotent';
        rightStep2.querySelector('.s-badge').className = 's-badge green';
        rightStep2.querySelector('.s-badge').textContent = 'Durable state';
      }
      log('cyan', '⚡ [FERRICSTORE] Prior state is committed; inventory mutation uses its own idempotency boundary.');

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
        if (valReplayTime) valReplayTime.textContent = 'After reclaim';
        if (valDivergence) valDivergence.textContent = data.divergence;

        if (termStatus) termStatus.innerHTML = '<span class="pulse-dot" style="background:#34d399; box-shadow: 0 0 8px #34d399;"></span> SIMULATION COMPLETE';
        log('success', '🎉 Simulation Complete! State recovery is durable; external effects still use stable idempotency keys or ctx.effect.');

        isAnimating = false;
        if (onDone) onDone();
      }, 700));

    }, 700));
  }

  function playFullAutoSimulation() {
    clearLogs();
    log('info', '🎬 Starting full automatic live failure simulation...');
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
      currentScenario = tab.getAttribute('data-scenario') || 'double-charge';
      playFullAutoSimulation();
    });
  });

  // --- API Toggle Switching ---
  apiToggles.forEach(function (btn) {
    btn.addEventListener('click', function () {
      apiToggles.forEach(function (b) { b.classList.remove('is-active'); });
      btn.classList.add('is-active');
      currentApiMode = btn.getAttribute('data-api-mode') || 'states';
      renderScenarioDetails();
      log('info', 'Switched view to: ' + (currentApiMode === 'steps' ? 'Real FLOW.STEP_CONTINUE' : 'Current States API (@flow.state FSM)'));
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
      log('info', 'Simulation reset. Click "⚡ Play Live Interactive Simulation" to begin.');
    });
  }

  // Run automatically on first load so the user sees live motion immediately!
  playFullAutoSimulation();
})();
