(function () {
  "use strict";

  var activeArch = "memory";
  var currentStep = 0;
  var isPaused = true;
  var timer = null;

  var activeChoreoTab = "dualwrite";

  var codeSnippets = {
  "workflow": "# FerricStore Python SDK: one handler per durable state\nfrom ferricstore import WorkflowClient, complete, transition\n\nclient = WorkflowClient.from_url(\"ferric://127.0.0.1:6388\")\ncheckout = client.workflow(type=\"ai-checkout\", initial_state=\"charge\")\n\n@checkout.state(\"charge\")\ndef charge(ctx):\n    tx = stripe.charge(\n        ctx.payload[\"amount\"],\n        idempotency_key=f\"{ctx.id}:charge:v1\",\n    )\n    return transition(\"summarize\", values={\"tx_id\": tx.id})\n\n@checkout.state(\"summarize\")\ndef summarize(ctx):\n    summary = llm.generate(ctx.payload[\"prompt\"])\n    return transition(\"deliver\", values={\"summary\": summary})\n\n@checkout.state(\"deliver\")\ndef deliver(ctx):\n    email.send_receipt(\n        ctx.payload[\"email\"], tx_id=ctx.value(\"tx_id\"),\n        idempotency_key=f\"{ctx.id}:receipt:v1\",\n    )\n    return complete(result=b\"sent\")\n\ncheckout.worker().run()",
  "queue": "# Message Queue Choreography (Queue-per-Step): The Dual-Write & Observability Trap\nimport boto3, json\n\nsqs = boto3.client(\"sqs\")\n\ndef worker_step1_payment():\n    # Pull from payment_queue\n    msg = sqs.receive_message(QueueUrl=PAYMENT_QUEUE)\n    order = json.loads(msg[\"Body\"])\n    \n    # 1. Charge Stripe API ($200)\n    tx = stripe.charge(order[\"amount\"])\n    \n    # \ud83d\udca5 THE DUAL-WRITE DISASTER:\n    # If the process crashes or network drops RIGHT HERE before publishing to llm_queue:\n    # - Stripe charge succeeded ($200 billed).\n    # - Next queue NEVER received message.\n    # - payment_queue visibility timeout expires -> redelivers to Worker 1b -> CHARGES CARD AGAIN ($400)!\n    sqs.send_message(QueueUrl=LLM_QUEUE, MessageBody=json.dumps({\"order_id\": order[\"id\"], \"tx\": tx}))\n    sqs.delete_message(QueueUrl=PAYMENT_QUEUE, ReceiptHandle=msg[\"ReceiptHandle\"])",
  "db": "# DIY Database Polling: High Contention, Lock Timeouts, and Complex Boilerplate\nimport time, psycopg2\n\ndef worker_poll_loop():\n    conn = psycopg2.connect(DB_URL)\n    while True:\n        with conn.cursor() as cur:\n            cur.execute(\"\"\"\n                SELECT id, payload, step, locked_until \n                FROM jobs \n                WHERE status = \\\"PENDING\\\" AND (locked_until IS NULL OR locked_until < NOW())\n                FOR UPDATE SKIP LOCKED \n                LIMIT 1\n            \"\"\")\n            job = cur.fetchone()\n            if not job:\n                time.sleep(5)\n                continue\n                \n            job_id, payload, step, locked_until = job\n            cur.execute(\"UPDATE jobs SET locked_until = NOW() + INTERVAL \\\"30 SECONDS\\\" WHERE id = %s\", (job_id,))\n            conn.commit()",
  "memory": "# In-Memory Background Tasks: 100% Volatile (All State Lost on Crash)\nfrom fastapi import FastAPI, BackgroundTasks\n\napp = FastAPI()\n\ndef run_ai_order(order_data: dict):\n    # Stored only in Python process heap RAM\n    tx = stripe.charge(order_data[\"amount\"])\n    \n    # \ud83d\udca5 IF SERVER RESTARTS / OOM KILLED:\n    # Memory is wiped. The function terminates immediately.\n    # No recovery, no database row, no receipt sent. Customer loses $200!\n    summary = llm.generate(order_data[\"prompt\"])\n    email.send_receipt(order_data[\"email\"], tx)\n\n@app.post(\"/checkout\")\ndef checkout(order: dict, bg: BackgroundTasks):\n    bg.add_task(run_ai_order, order)\n    return {\"status\": \"processing_in_memory\"}"
};
  var paradigmData = {
  "memory": [
    {
      "step": 0,
      "title": "1. Charge $200.00",
      "badge": "PROGRESS IN MEMORY",
      "badgeClass": "badge-bad",
      "avatar": "\ud83d\udcb3",
      "desc": "The background task charges $200, but its transaction ID lives only in process memory.",
      "billed": "$200.00",
      "billedSub": "Billed in RAM only",
      "tokens": "0",
      "tokensSub": "Awaiting step 2",
      "latency": "0ms",
      "latencySub": "Local RAM memory",
      "reliability": "20% (F)",
      "reliabilityClass": "is-danger",
      "n3Title": "4. Task is lost",
      "n3Desc": "No recovery"
    },
    {
      "step": 1,
      "title": "2. Generate the AI summary ($0.45)",
      "badge": "PROGRESS IN MEMORY",
      "badgeClass": "badge-bad",
      "avatar": "\ud83e\udd16",
      "desc": "The AI service generates a 1,500-token summary. The result is held only in process memory.",
      "billed": "$200.00",
      "billedSub": "Billed in RAM only",
      "tokens": "1,500",
      "tokensSub": "$0.45 LLM cost",
      "latency": "0ms",
      "latencySub": "Local RAM memory",
      "reliability": "20% (F)",
      "reliabilityClass": "is-danger",
      "n3Title": "4. Task is lost",
      "n3Desc": "No recovery"
    },
    {
      "step": 2,
      "title": "3. Server stops; memory is cleared",
      "badge": "MEMORY LOST",
      "badgeClass": "badge-bad",
      "avatar": "\ud83d\udca5",
      "desc": "The server stops. Everything held in process memory disappears.",
      "billed": "$200.00 (Customer Charged)",
      "billedSub": "Ghost charge without record",
      "tokens": "1,500 (Wasted)",
      "tokensSub": "Tokens vaporized",
      "latency": "\u221e (Lost Forever)",
      "latencySub": "No retry mechanism",
      "reliability": "0% (FAILED)",
      "reliabilityClass": "is-danger",
      "n3Title": "4. Task is lost",
      "n3Desc": "No recovery"
    },
    {
      "step": 3,
      "title": "4. Task lost after the charge",
      "badge": "NO RECOVERY",
      "badgeClass": "badge-bad",
      "avatar": "\ud83d\udcb8",
      "desc": "The customer was charged $200 and 1,500 tokens were used, but the receipt was never sent and no order record remains.",
      "billed": "$200.00 (Stolen Money!)",
      "billedSub": "Unfulfilled transaction",
      "tokens": "1,500 (Lost)",
      "tokensSub": "Wasted compute",
      "latency": "Never Recovered",
      "latencySub": "Permanent data loss",
      "reliability": "0% (FAILED)",
      "reliabilityClass": "is-danger",
      "n3Title": "4. Order is lost",
      "n3Desc": "Not recovered"
    }
  ],
  "queue": [
    {
      "step": 0,
      "title": "1. Worker charges $200.00",
      "badge": "MESSAGE QUEUE",
      "badgeClass": "badge-warn",
      "avatar": "\ud83d\udcec",
      "desc": "A worker reads the payment message, charges $200, and prepares the next message.",
      "billed": "$200.00",
      "billedSub": "Initial charge",
      "tokens": "0",
      "tokensSub": "Awaiting LLM queue",
      "latency": "10ms",
      "latencySub": "Queue polling latency",
      "reliability": "45% (C)",
      "reliabilityClass": "is-danger",
      "n3Title": "4. Retry can repeat the charge",
      "n3Desc": "Possible duplicate"
    },
    {
      "step": 1,
      "title": "2. Worker generates the AI summary",
      "badge": "MESSAGE QUEUE",
      "badgeClass": "badge-warn",
      "avatar": "\ud83e\udd16",
      "desc": "The next worker calls the AI service for $0.45. Progress is spread across separate messages.",
      "billed": "$200.00",
      "billedSub": "Initial charge",
      "tokens": "1,500",
      "tokensSub": "First inference pass",
      "latency": "10ms",
      "latencySub": "Queue polling latency",
      "reliability": "45% (C)",
      "reliabilityClass": "is-danger",
      "n3Title": "4. Retry can repeat the charge",
      "n3Desc": "Possible duplicate"
    },
    {
      "step": 2,
      "title": "3. Server stops between messages",
      "badge": "NEXT MESSAGE NOT SENT",
      "badgeClass": "badge-warn",
      "avatar": "\ud83d\udca5",
      "desc": "The worker stops after the payment but before sending the next message. The queue later retries the original message.",
      "billed": "$200.00",
      "billedSub": "Step 1 already completed",
      "tokens": "1,500",
      "tokensSub": "Step 2 already completed",
      "latency": "30,000ms Delay",
      "latencySub": "Waiting for queue timeout",
      "reliability": "35% (POOR)",
      "reliabilityClass": "is-danger",
      "n3Title": "4. Retry starts at the first message",
      "n3Desc": "Whole job repeats"
    },
    {
      "step": 3,
      "title": "4. Retry charges the card again ($400)",
      "badge": "WHOLE MESSAGE RETRIED",
      "badgeClass": "badge-bad",
      "avatar": "\ud83d\ude31",
      "desc": "A replacement reads the original message and charges the card again, making $400 total. The full history is split across queues.",
      "billed": "$400.00 (2x DOUBLE BILL!)",
      "billedSub": "Duplicate charge penalty",
      "tokens": "3,000 (2x Consumed)",
      "tokensSub": "1,500 wasted tokens",
      "latency": "30,200ms Latency",
      "latencySub": "30s visibility timeout penalty",
      "reliability": "35% (POOR)",
      "reliabilityClass": "is-danger",
      "n3Title": "4. Card charged twice",
      "n3Desc": "$400 total"
    }
  ],
  "db": [
    {
      "step": 0,
      "title": "1. Worker charges $200 and saves a row",
      "badge": "ROW SAVED",
      "badgeClass": "badge-warn",
      "avatar": "\ud83d\uddc4\ufe0f",
      "desc": "The worker charges $200 and records the payment in a database row.",
      "billed": "$200.00",
      "billedSub": "Recorded in DB table",
      "tokens": "0",
      "tokensSub": "Awaiting LLM step",
      "latency": "25ms",
      "latencySub": "SQL write roundtrip",
      "reliability": "75% (B)",
      "reliabilityClass": "",
      "n3Title": "4. Scheduled recovery",
      "n3Desc": "Waits for the next poll"
    },
    {
      "step": 1,
      "title": "2. Worker generates the AI summary",
      "badge": "ROW SAVED",
      "badgeClass": "badge-warn",
      "avatar": "\ud83e\udd16",
      "desc": "The worker calls the AI service and prepares to save the new status.",
      "billed": "$200.00",
      "billedSub": "Recorded in DB table",
      "tokens": "1,500",
      "tokensSub": "$0.45 LLM cost",
      "latency": "25ms",
      "latencySub": "SQL write roundtrip",
      "reliability": "75% (B)",
      "reliabilityClass": "",
      "n3Title": "4. Scheduled recovery",
      "n3Desc": "Waits for the next poll"
    },
    {
      "step": 2,
      "title": "3. Worker stops while saving",
      "badge": "ROW LOCK RELEASES LATER",
      "badgeClass": "badge-warn",
      "avatar": "\ud83d\udca5",
      "desc": "The worker stops before the next save. The scheduled checker waits for the old lock to expire.",
      "billed": "$200.00",
      "billedSub": "Safe on disk",
      "tokens": "1,500",
      "tokensSub": "Unsaved in database",
      "latency": "15,000ms Polling Loop",
      "latencySub": "Waiting for cron interval",
      "reliability": "65% (B)",
      "reliabilityClass": "",
      "n3Title": "4. Scheduled recovery",
      "n3Desc": "Waits for the next poll"
    },
    {
      "step": 3,
      "title": "4. Scheduled checker resumes (15s delay)",
      "badge": "RECOVERED AFTER A WAIT",
      "badgeClass": "badge-warn",
      "avatar": "\ud83d\udc22",
      "desc": "The checker wakes after 15 seconds, sees the saved row, and delivers the receipt. It avoids a duplicate charge but adds polling delay.",
      "billed": "$200.00 (stable key)",
      "billedSub": "No duplicate charge",
      "tokens": "1,500",
      "tokensSub": "No repeated tokens in this scenario",
      "latency": "15,400ms Delay",
      "latencySub": "Cron polling lag",
      "reliability": "70% (MEDIOCRE)",
      "reliabilityClass": "",
      "n3Title": "4. Recovered after waiting",
      "n3Desc": "15s delay"
    }
  ],
  "workflow": [
    {
      "step": 0,
      "title": "1. Saved payment ($200.00)",
      "badge": "SAVED STATE + PROVIDER KEY",
      "badgeClass": "badge-good",
      "avatar": "\ud83d\udcb3",
      "desc": "The worker reuses one Stripe idempotency key and saves the payment state before continuing.",
      "billed": "$200.00",
      "billedSub": "Stable Stripe key reused",
      "tokens": "0",
      "tokensSub": "Awaiting LLM step",
      "latency": "Saved state",
      "latencySub": "Topology dependent",
      "reliability": "Configured durability",
      "reliabilityClass": "is-success",
      "n3Title": "Recovery after takeover",
      "n3Desc": "Provider key reused"
    },
    {
      "step": 1,
      "title": "2. Saved AI summary",
      "badge": "SAVED STATE + EFFECT GUARD",
      "badgeClass": "badge-good",
      "avatar": "\ud83e\udd16",
      "desc": "A stable effect ID guards the model call; the saved result is ready for the next state.",
      "billed": "$200.00",
      "billedSub": "Payment provider key reused",
      "tokens": "1,500",
      "tokensSub": "Saved result",
      "latency": "Saved state",
      "latencySub": "Topology dependent",
      "reliability": "Configured durability",
      "reliabilityClass": "is-success",
      "n3Title": "Saved result retained",
      "n3Desc": "Effect ID reused"
    },
    {
      "step": 2,
      "title": "3. Server stops; saved progress remains",
      "badge": "SAVED PROGRESS",
      "badgeClass": "badge-good",
      "avatar": "\ud83d\udee1\ufe0f",
      "desc": "Earlier results remain saved. A newer owner number blocks an old worker from writing after takeover.",
      "billed": "$200.00",
      "billedSub": "Provider key remains stable",
      "tokens": "1,500",
      "tokensSub": "Committed result retained",
      "latency": "After reclaim",
      "latencySub": "Depends on takeover timing",
      "reliability": "Saved progress",
      "reliabilityClass": "is-success",
      "n3Title": "4. New owner continues",
      "n3Desc": "Old write blocked"
    },
    {
      "step": 3,
      "title": "4. Replacement continues from saved state",
      "badge": "COMPLETED + OUTSIDE CALLS GUARDED",
      "badgeClass": "badge-good",
      "avatar": "\ud83c\udf89",
      "desc": "The replacement starts at the current saved state. Stable provider keys or ctx.effect still protect outside payment and delivery calls.",
      "billed": "$200.00 (provider key)",
      "billedSub": "effect protected separately",
      "tokens": "1,500 stored",
      "tokensSub": "Effect digest protects retry",
      "latency": "Takeover and resume",
      "latencySub": "Configuration dependent",
      "reliability": "Saved + guarded",
      "reliabilityClass": "is-success",
      "n3Title": "4. Guarded $200",
      "n3Desc": "Customer receives it"
    }
  ]
};
  var choreoAnimations = {
  "dualwrite": {
    "note": "<strong>The Dual-Write Race Condition:</strong> Worker 1 charges the credit card, but crashes before publishing to <code>llm_queue</code>. The queue visibility timeout fires and redelivers to Worker 1b, charging the card <strong>A SECOND TIME ($400 total)</strong>.",
    "render": "\n      <div class=\"dualwrite-anim-layout\">\n        <div class=\"dw-pipeline-row\">\n          <div class=\"dw-node is-success\">\n            <small>STEP 1 QUEUE</small>\n            <strong>payment_queue</strong>\n          </div>\n          <span class=\"dw-arrow\">\u2794</span>\n          <div class=\"dw-node is-success\">\n            <small>WORKER 1</small>\n            <strong>Stripe API ($200 \u2713)</strong>\n          </div>\n          <span class=\"dw-arrow\">\ud83d\udca5</span>\n          <div class=\"dw-node is-crashed\">\n            <small>CRASH POINT</small>\n            <strong>Process Dies!</strong>\n          </div>\n          <span class=\"dw-arrow\">\u274c</span>\n          <div class=\"dw-node is-lost\">\n            <small>STEP 2 QUEUE</small>\n            <strong>llm_queue (Never Receives)</strong>\n          </div>\n        </div>\n\n        <div class=\"dw-redelivery-banner\">\n          <div>\n            <strong>\ud83d\udea8 Visibility Timeout (30s) Expired on payment_queue:</strong>\n            <span>Redelivered to Worker 1b \u2794 Card billed AGAIN ($400.00 total) with duplicate API calls!</span>\n          </div>\n        </div>\n      </div>\n    "
  },
  "observability": {
    "note": "<strong>One workflow record, explicit history:</strong> Queue choreography can fragment state across queues and dead-letter queues. FerricStore exposes the current workflow record and its event history through the same client API; observed latency depends on deployment and query shape.",
    "render": "\n      <div class=\"obs-compare-layout\">\n        <div class=\"obs-side-box is-choreo\">\n          <div class=\"obs-header-tag bad\">\n            <span>\ud83d\udcec Queue Choreography (Black Box)</span>\n            <span>8 Endpoints</span>\n          </div>\n          <div class=\"obs-queues-grid\">\n            <div class=\"obs-q-pill is-searching\">\ud83d\udd0d payment_queue</div>\n            <div class=\"obs-q-pill is-searching\">\ud83d\udd0d payment_dlq</div>\n            <div class=\"obs-q-pill is-searching\">\ud83d\udd0d llm_queue</div>\n            <div class=\"obs-q-pill is-searching\">\ud83d\udd0d llm_dlq</div>\n          </div>\n          <div class=\"obs-status-result\">\n            <strong>Status: Unknown / Searching.</strong> Must correlate OpenTelemetry trace IDs across 4 disconnected microservices.\n          </div>\n        </div>\n\n        <div class=\"obs-side-box is-workflow\">\n          <div class=\"obs-header-tag good\">\n            <span>\ud83d\udee1\ufe0f FerricStore Workflow (Single Query)</span>\n            <span>Record + history APIs</span>\n          </div>\n          <div class=\"obs-workflow-timeline\">\n            <span>[ 1. Payment \u2713 ]</span>\n            <span>\u2794</span>\n            <span>[ 2. LLM \u2713 ]</span>\n            <span>\u2794</span>\n            <span style=\"color: #34d399; font-weight: 800;\">[ 3. Deliver \ud83d\ude80 ]</span>\n          </div>\n          <div class=\"obs-status-result\">\n            <strong>State: Deterministic.</strong> <code>client.get(\"order-9842\")</code> reads the current record; <code>client.history(\"order-9842\")</code> reads its events.\n          </div>\n        </div>\n      </div>\n    "
  },
  "saga": {
    "note": "<strong>Compensating Rollbacks (Saga Hell):</strong> When a downstream step fails (e.g. LLM blocked prompt), queues require custom reverse compensation queues with risks of dropped rollbacks. FerricStore defines rollbacks in code.",
    "render": "\n      <div class=\"saga-anim-layout\">\n        <div class=\"saga-flow-col\">\n          <div style=\"font-size: 11px; font-weight: 800; color: #f87171; text-transform: uppercase; margin-bottom: 10px;\">\n            \ud83d\udcec Queue Saga Cascade (Fragile)\n          </div>\n          <div class=\"saga-step-node is-failed\">\n            <span>Step 3: LLM Filter</span>\n            <strong>\u274c Content Blocked</strong>\n          </div>\n          <div class=\"saga-step-node is-compensating\">\n            <span>Reverse Queue: refund_queue</span>\n            <strong>\ud83d\udd04 Refunding Step 1...</strong>\n          </div>\n          <div style=\"font-size: 11.5px; color: var(--text-dim);\">\n            \u26a0\ufe0f If refund message drops in queue, customer money is stranded permanently.\n          </div>\n        </div>\n\n        <div class=\"saga-flow-col\" style=\"border-color: rgba(16, 185, 129, 0.35);\">\n          <div style=\"font-size: 11px; font-weight: 800; color: #34d399; text-transform: uppercase; margin-bottom: 10px;\">\n            \ud83d\udee1\ufe0f FerricStore Explicit Compensation\n          </div>\n          <div class=\"saga-step-node is-failed\">\n            <span>Step 3: LLM Filter</span>\n            <strong>\u274c Handled Gracefully</strong>\n          </div>\n          <div class=\"saga-step-node\" style=\"border-color: #10b981; background: rgba(16, 185, 129, 0.15); color: #6ee7b7;\">\n            <span>transition(\"refund\")</span>\n            <strong>\u2713 Durable Refund State</strong>\n          </div>\n          <div style=\"font-size: 11.5px; color: var(--text-muted);\">\n            \u2713 The workflow records and retries the modeled refund state; the payment call still needs a stable provider key.\n          </div>\n        </div>\n      </div>\n    "
  },
  "sprawl": {
    "note": "<strong>Infrastructure & Operational Sprawl:</strong> A 3-step queue pipeline requires 4 SQS queues, 4 DLQ alert topics, 4 worker container deployments, and 8 IAM policies. FerricStore handles all checkpoints inside 1 lightweight engine.",
    "render": "\n      <div class=\"sprawl-anim-layout\">\n        <div class=\"sprawl-box\">\n          <div style=\"font-size: 11px; font-weight: 800; color: #fbbf24; text-transform: uppercase; margin-bottom: 8px;\">\n            \ud83d\udcec Queue Infrastructure (16 Cloud Resources)\n          </div>\n          <div class=\"sprawl-badges-cloud\">\n            <span class=\"sprawl-badge-item\">sqs-payment</span>\n            <span class=\"sprawl-badge-item\">sqs-payment-dlq</span>\n            <span class=\"sprawl-badge-item\">sqs-llm</span>\n            <span class=\"sprawl-badge-item\">sqs-llm-dlq</span>\n            <span class=\"sprawl-badge-item\">sqs-email</span>\n            <span class=\"sprawl-badge-item\">sqs-email-dlq</span>\n            <span class=\"sprawl-badge-item\">ecs-payment-worker</span>\n            <span class=\"sprawl-badge-item\">ecs-llm-worker</span>\n            <span class=\"sprawl-badge-item\">ecs-email-worker</span>\n            <span class=\"sprawl-badge-item\">iam-policy-sqs</span>\n            <span class=\"sprawl-badge-item\">cw-alarm-dlq</span>\n            <span class=\"sprawl-badge-item\">outbox-table</span>\n          </div>\n          <small style=\"color: var(--text-dim);\">High AWS bill, complex Terraform scripts, 16 points of failure.</small>\n        </div>\n\n        <div class=\"sprawl-box sprawl-good\">\n          <div style=\"font-size: 11px; font-weight: 800; color: #34d399; text-transform: uppercase; margin-bottom: 8px;\">\n            \ud83d\udee1\ufe0f FerricStore Architecture (1 Engine)\n          </div>\n          <div class=\"sprawl-badges-cloud\">\n            <span class=\"sprawl-badge-item\">ferricstore-daemon</span>\n            <span class=\"sprawl-badge-item\">python-workflow-sdk</span>\n            <span class=\"sprawl-badge-item\">raft-disk-log</span>\n          </div>\n          <small style=\"color: #6ee7b7;\">A single integrated engine can reduce service sprawl. Memory and persistence latency depend on configuration, topology, and workload.</small>\n        </div>\n      </div>\n    "
  }
};

  // Primary Simulator DOM Elements
  var archButtons = document.querySelectorAll("[data-arch]");
  var stepNodes = document.querySelectorAll("[data-step-node]");
  var showcaseAvatar = document.querySelector("[data-showcase-avatar]");
  var showcaseBadge = document.querySelector("[data-showcase-badge]");
  var showcaseTitle = document.querySelector("[data-showcase-title]");
  var showcaseDesc = document.querySelector("[data-showcase-desc]");
  var stepCounter = document.querySelector("[data-step-counter]");

  var cardBilled = document.querySelector("[data-card-billed]");
  var valBilled = document.querySelector("[data-val-billed]");
  var subBilled = document.querySelector("[data-sub-billed]");

  var cardTokens = document.querySelector("[data-card-tokens]");
  var valTokens = document.querySelector("[data-val-tokens]");
  var subTokens = document.querySelector("[data-sub-tokens]");

  var cardLatency = document.querySelector("[data-card-latency]");
  var valLatency = document.querySelector("[data-val-latency]");
  var subLatency = document.querySelector("[data-sub-latency]");

  var cardReliability = document.querySelector("[data-card-reliability]");
  var valReliability = document.querySelector("[data-val-reliability]");
  var subReliability = document.querySelector("[data-sub-reliability]");

  var n3Title = document.querySelector("[data-n3-title]");
  var n3Desc = document.querySelector("[data-n3-desc]");

  var prevBtn = document.querySelector("[data-prev]");
  var pauseBtn = document.querySelector("[data-pause]");
  var nextBtn = document.querySelector("[data-next]");
  var replayBtn = document.querySelector("[data-replay]");
  var crashBtn = document.querySelector("[data-smash-crash]");
  var liveStatusText = document.querySelector("[data-live-status-text]");

  var codeTabs = document.querySelectorAll("[data-code-tab]");
  var codeContent = document.querySelector("[data-code-content]");
  if (codeContent) codeContent.textContent = codeSnippets[activeArch];

  // Choreography Deep Dive DOM Elements
  var choreoTabs = document.querySelectorAll("[data-choreo-tab]");
  var choreoArena = document.querySelector("[data-choreo-arena]");
  var choreoNote = document.querySelector("[data-choreo-note]");
  var choreoReplayBtn = document.querySelector("[data-choreo-replay]");

  function renderChoreo() {
    var data = choreoAnimations[activeChoreoTab];
    if (!data) return;

    choreoTabs.forEach(function (tab) {
      var isActive = tab.dataset.choreoTab === activeChoreoTab;
      tab.classList.toggle("is-active", isActive);
    });

    if (choreoArena) {
      choreoArena.innerHTML = data.render;
    }
    if (choreoNote) {
      choreoNote.innerHTML = data.note;
    }
  }

  function render() {
    var list = paradigmData[activeArch];
    if (prevBtn) prevBtn.disabled = currentStep === 0;
    if (nextBtn) nextBtn.disabled = currentStep >= list.length - 1;
    if (currentStep >= list.length) currentStep = 0;
    var data = list[currentStep];

    archButtons.forEach(function (btn) {
      var isActive = btn.dataset.arch === activeArch;
      btn.classList.toggle("is-active", isActive);
      btn.setAttribute("aria-selected", String(isActive));
    });

    stepNodes.forEach(function (node, idx) {
      var isDone = idx < currentStep;
      var isActive = idx === currentStep;
      node.classList.toggle("is-done", isDone);
      node.classList.toggle("is-active", isActive);

      var pill = node.querySelector(".node-state-pill");
      if (pill) {
        if (idx === 2 && isActive) {
          pill.textContent = "CRASHING";
        } else if (isDone) {
          pill.textContent = "✓ DONE";
        } else if (isActive) {
          pill.textContent = "ACTIVE";
        } else {
          pill.textContent = "PENDING";
        }
      }
    });

    if (n3Title) n3Title.textContent = data.n3Title;
    if (n3Desc) n3Desc.textContent = data.n3Desc;

    if (showcaseAvatar) showcaseAvatar.textContent = data.avatar;
    if (showcaseBadge) {
      showcaseBadge.textContent = data.badge;
      showcaseBadge.className = "showcase-badge " + data.badgeClass;
    }
    if (showcaseTitle) showcaseTitle.textContent = data.title;
    if (showcaseDesc) showcaseDesc.textContent = data.desc;
    if (stepCounter) stepCounter.textContent = "Step " + (currentStep + 1) + " of " + list.length;

    if (valBilled) valBilled.textContent = data.billed;
    if (subBilled) subBilled.textContent = data.billedSub;
    if (cardBilled) cardBilled.classList.toggle("is-danger", data.billed.indexOf("DOUBLE") !== -1 || data.billed.indexOf("Stolen") !== -1);

    if (valTokens) valTokens.textContent = data.tokens;
    if (subTokens) subTokens.textContent = data.tokensSub;
    if (cardTokens) cardTokens.classList.toggle("is-danger", data.tokens.indexOf("3,000") !== -1 || data.tokens.indexOf("Lost") !== -1);

    if (valLatency) valLatency.textContent = data.latency;
    if (subLatency) subLatency.textContent = data.latencySub;

    if (valReliability) valReliability.textContent = data.reliability;
    if (subReliability) subReliability.textContent = data.reliabilityClass === "is-success" ? "Saved-state boundary" : "Vulnerable to failure";
    if (cardReliability) {
      cardReliability.className = "telemetry-card " + data.reliabilityClass;
    }

    if (pauseBtn) pauseBtn.textContent = isPaused ? "▶ Play steps" : "⏸ Pause";
    if (liveStatusText) {
      liveStatusText.textContent = currentStep >= list.length - 1
        ? "Complete — reset to run again"
        : (isPaused ? "Paused — choose Next step or Play steps" : "Playing steps");
    }
  }

  function clearTimer() {
    if (timer !== null) {
      window.clearTimeout(timer);
      timer = null;
    }
  }

  function schedule() {
    clearTimer();
    if (isPaused || currentStep >= paradigmData[activeArch].length - 1) return;
    var wait = currentStep === 2 ? 3200 : (currentStep === 3 ? 4200 : 2200);
    timer = window.setTimeout(function () {
      currentStep = Math.min(currentStep + 1, paradigmData[activeArch].length - 1);
      if (currentStep >= paradigmData[activeArch].length - 1) isPaused = true;
      render();
      schedule();
    }, wait);
  }

  archButtons.forEach(function (btn) {
    btn.addEventListener("click", function () {
      clearTimer();
      activeArch = btn.dataset.arch;
      currentStep = 0;
      isPaused = true;
      render();
    });
  });

  stepNodes.forEach(function (node, idx) {
    node.setAttribute("role", "button");
    node.tabIndex = 0;
    function jump() {
      clearTimer();
      currentStep = idx;
      isPaused = true;
      render();
    }
    node.addEventListener("click", jump);
    node.addEventListener("keydown", function (e) {
      if (e.key === "Enter" || e.key === " ") {
        e.preventDefault();
        jump();
      }
    });
  });

  if (crashBtn) {
    crashBtn.addEventListener("click", function () {
      clearTimer();
      currentStep = 2;
      isPaused = true;
      render();
    });
  }

  if (pauseBtn) {
    pauseBtn.addEventListener("click", function () {
      clearTimer();
      if (currentStep >= paradigmData[activeArch].length - 1 && isPaused) currentStep = 0;
      isPaused = !isPaused;
      render();
      schedule();
    });
  }

  if (prevBtn) {
    prevBtn.addEventListener("click", function () {
      clearTimer();
      var len = paradigmData[activeArch].length;
      currentStep = Math.max(0, currentStep - 1);
      isPaused = true;
      render();
    });
  }

  if (nextBtn) {
    nextBtn.addEventListener("click", function () {
      clearTimer();
      var len = paradigmData[activeArch].length;
      currentStep = Math.min(currentStep + 1, len - 1);
      isPaused = true;
      render();
    });
  }

  if (replayBtn) {
    replayBtn.addEventListener("click", function () {
      clearTimer();
      currentStep = 0;
      isPaused = true;
      render();
    });
  }

  codeTabs.forEach(function (tab) {
    tab.addEventListener("click", function () {
      codeTabs.forEach(function (t) { t.classList.remove("is-active"); });
      tab.classList.add("is-active");
      var key = tab.dataset.codeTab;
      if (codeContent && codeSnippets[key]) {
        codeContent.textContent = codeSnippets[key];
      }
    });
  });

  choreoTabs.forEach(function (tab) {
    tab.addEventListener("click", function () {
      activeChoreoTab = tab.dataset.choreoTab;
      renderChoreo();
    });
  });

  if (choreoReplayBtn) {
    choreoReplayBtn.addEventListener("click", function () {
      renderChoreo();
    });
  }

  render();
  renderChoreo();
})();
