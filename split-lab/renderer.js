(function (global) {
  "use strict";

  global.FerricRenderers = global.FerricRenderers || {};

  function statusClass(status) {
    if (status === "COMPLETED") return "is-complete";
    if (status === "CRASHED") return "is-crashed";
    if (status === "RUNNING" || status === "AWAITING_CRASH") return "is-running";
    if (status === "PAUSED") return "is-recovering";
    return "";
  }

  global.FerricRenderers.lab = function (mount, engine) {
    mount.innerHTML = [
      '<div class="lab-shell">',
      '  <section class="lab-code" aria-label="Python workflow code">',
      '    <header><div><span class="file-dot" aria-hidden="true">py</span><strong>research_workflow.py</strong></div><span class="lab-code-mode">Verified states API</span></header>',
      '    <pre><code>',
      '<span data-code-phase="idle">from ferricstore import WorkflowClient, transition, complete</span>',
      '<span data-code-phase="idle">client = WorkflowClient.from_url("ferric://127.0.0.1:6388")</span>',
      '<span data-code-phase="idle">flow = client.workflow(type="research", initial_state="plan")</span>',
      '<span class="code-section" data-code-phase="plan">@flow.state("plan")</span>',
      '<span data-code-phase="plan">def plan(ctx):</span>',
      '<span data-code-phase="plan">    key = f"{ctx.id}:plan:v1"</span>',
      '<span data-code-phase="plan">    @ctx.effect("plan", "llm.plan", operation_digest="llm.plan:v1", idempotency_key=key)</span>',
      '<span data-code-phase="plan">    def call_model(): return make_plan(ctx.payload, idempotency_key=key)</span>',
      '<span data-code-phase="plan">    return transition("search", values={"plan": call_model()})</span>',
      '<span class="code-section" data-code-phase="search">@flow.state("search")</span>',
      '<span data-code-phase="search">def search(ctx):</span>',
      '<span data-code-phase="search">    key = f"{ctx.id}:search:v1"</span>',
      '<span data-code-phase="search">    @ctx.effect("search", "web.search", operation_digest="web.search:v1", idempotency_key=key)</span>',
      '<span data-code-phase="search">    def call_search(): return search_sources(ctx.value("plan"), idempotency_key=key)</span>',
      '<span data-code-phase="search">    facts = call_search()</span>',
      '<span data-code-phase="search">    return transition("summarize", values={"facts": facts})</span>',
      '<span class="code-section" data-code-phase="summarize">@flow.state("summarize")</span>',
      '<span data-code-phase="summarize">def summarize(ctx):</span>',
      '<span data-code-phase="summarize">    key = f"{ctx.id}:summary:v1"</span>',
      '<span data-code-phase="summarize">    @ctx.effect("summary", "llm.summarize", operation_digest="llm.summarize:v1", idempotency_key=key)</span>',
      '<span data-code-phase="summarize">    def call_model(): return summarize_facts(ctx.value("facts"), idempotency_key=key)</span>',
      '<span data-code-phase="complete">    return complete(result=call_model())</span>',
      '<span class="code-section" data-code-phase="idle">flow.start("report-204", payload={"topic": "workflow recovery"})</span>',
      '<span data-code-phase="idle">flow.worker().run()</span>',
      '    </code></pre>',
      '    <footer><span>Effects reserve the call; providers receive the same stable idempotency key.</span><strong data-code-pointer>Ready</strong></footer>',
      '  </section>',
      '  <section class="lab-trace" aria-label="Execution trace">',
      '    <header><div><span class="eyebrow">RUN</span><strong>report-204</strong></div><span class="status-pill" data-status>Idle</span></header>',
      '    <ol class="lab-progress" aria-label="Workflow progress">',
      '      <li data-rank="0"><span>01</span>Plan</li><li data-rank="1"><span>02</span>Search</li><li data-rank="2"><span>03</span>Summarize</li>',
      '      <li data-rank="3"><span>04</span>Crash</li><li data-rank="4"><span>05</span>Reclaim</li><li data-rank="5"><span>06</span>Done</li>',
      '    </ol>',
      '    <div class="lab-runtime">',
      '      <article class="lab-worker" data-worker-a><span>WORKER A</span><strong data-worker-a-label>Ready</strong><small>original lease owner</small></article>',
      '      <div class="lab-link" aria-hidden="true"><span></span><b>state</b></div>',
      '      <article class="lab-store"><span data-store-kicker>FERRICSTORE</span><strong data-store-title>Durable flow state</strong><dl><div><dt>Current state</dt><dd data-flow-state>ready</dd></div><div><dt>Fence</dt><dd data-fence>41</dd></div></dl><code data-persisted>no values yet</code></article>',
      '      <div class="lab-link" aria-hidden="true"><span></span><b>lease</b></div>',
      '      <article class="lab-worker" data-worker-b><span>WORKER B</span><strong data-worker-b-label>Standby</strong><small>compatible replacement</small></article>',
      '    </div>',
      '    <div class="lab-message"><span aria-hidden="true">◆</span><p data-message>Ready to run the AI research workflow.</p></div>',
      '  </section>',
      '</div>'
    ].join("");

    var status = mount.querySelector("[data-status]");
    var progress = Array.prototype.slice.call(mount.querySelectorAll("[data-rank]"));
    var codeLines = Array.prototype.slice.call(mount.querySelectorAll("[data-code-phase]"));
    var codePointer = mount.querySelector("[data-code-pointer]");
    var workerA = mount.querySelector("[data-worker-a]");
    var workerB = mount.querySelector("[data-worker-b]");
    var workerALabel = mount.querySelector("[data-worker-a-label]");
    var workerBLabel = mount.querySelector("[data-worker-b-label]");
    var store = mount.querySelector(".lab-store");
    var storeKicker = mount.querySelector("[data-store-kicker]");
    var storeTitle = mount.querySelector("[data-store-title]");
    var flowState = mount.querySelector("[data-flow-state]");
    var fence = mount.querySelector("[data-fence]");
    var persisted = mount.querySelector("[data-persisted]");
    var message = mount.querySelector("[data-message]");

    engine.subscribe(function (envelope) {
      var state = envelope.state;
      var rank = global.FerricDemo.phaseRank(state.phase);
      var codePhase = state.phase;
      if (codePhase === "replan") codePhase = "plan";
      if (codePhase === "research") codePhase = "search";
      if (codePhase === "resummarize" || codePhase === "crashed" || codePhase === "recovering" || codePhase === "restarting") codePhase = "summarize";

      status.className = "status-pill " + statusClass(state.status);
      status.textContent = state.status === "AWAITING_CRASH" ? "Crash ready" : global.FerricDemo.phaseLabel(state.phase);
      codePointer.textContent = global.FerricDemo.phaseLabel(state.phase);
      codeLines.forEach(function (line) {
        line.classList.toggle("is-active", line.dataset.codePhase === codePhase || (state.phase === "complete" && line.dataset.codePhase === "complete"));
      });
      progress.forEach(function (item) {
        var itemRank = Number(item.dataset.rank);
        item.classList.toggle("is-active", itemRank === rank);
        item.classList.toggle("is-done", itemRank < rank || state.phase === "complete");
      });

      workerA.classList.toggle("is-crashed", rank >= 3);
      workerA.classList.toggle("is-active", rank >= 0 && rank < 3);
      workerB.classList.toggle("is-active", rank >= 4);
      workerALabel.textContent = rank < 0 ? "Ready" : rank < 3 ? "Running" : "Lease expired";
      workerBLabel.textContent = rank >= 4 ? (state.phase === "complete" ? "Completed" : "Lease owner") : "Standby";

      var keys = Object.keys(state.persisted);
      var durable = state.mode === "durable";
      store.classList.toggle("is-volatile", !durable);
      storeKicker.textContent = durable ? "FERRICSTORE" : "PROCESS MEMORY";
      storeTitle.textContent = durable ? "Durable flow state" : "Volatile local state";
      flowState.textContent = global.FerricDemo.phaseLabel(state.phase);
      fence.textContent = durable ? String(state.fencingToken) : "none";
      persisted.textContent = keys.length ? "saved: " + keys.join(", ") : (rank >= 3 && !durable ? "memory lost on crash" : "no saved values");
      message.textContent = state.message;
    });
  };
})(window);
