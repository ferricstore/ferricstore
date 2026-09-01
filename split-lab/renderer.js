(function (global) {
  "use strict";

  global.FerricRenderers = global.FerricRenderers || {};

  var MODE_COPY = {
    unmanaged: {
      codeFile: "research_process.py",
      codeMode: "Process memory",
      storeKicker: "PROCESS MEMORY",
      storeTitle: "Volatile working state",
      workerADetail: "original process",
      workerBDetail: "replacement process",
      firstLink: "memory",
      secondLink: "restart",
      progress: ["Plan", "Search", "Summarize", "Crash", "Restart", "Finish"]
    },
    durable: {
      codeFile: "research_workflow.py",
      codeMode: "FerricStore states",
      storeKicker: "FERRICSTORE",
      storeTitle: "Durable workflow state",
      workerADetail: "original lease owner",
      workerBDetail: "compatible replacement",
      firstLink: "commit",
      secondLink: "lease",
      progress: ["Plan", "Search", "Summarize", "Crash", "Resume", "Finish"]
    }
  };

  function statusClass(status) {
    if (status === "COMPLETED") return "is-complete";
    if (status === "CRASHED") return "is-crashed";
    if (status === "RUNNING" || status === "AWAITING_CRASH") return "is-running";
    if (status === "PAUSED") return "is-recovering";
    return "";
  }

  function phaseIsReplay(phase) {
    return ["replan", "research", "resummarize"].indexOf(phase) !== -1;
  }

  global.FerricRenderers.lab = function (mount, engine) {
    var results = { unmanaged: null, durable: null };

    mount.innerHTML = [
      '<div class="lab-experience" data-lab-live>',
      '  <section class="lab-verdict" aria-labelledby="lab-verdict-title">',
      '    <header><div><h2 id="lab-verdict-title">Restart versus durable resume</h2><p data-verdict-summary>Run both paths to compare the same worker crash.</p></div><div class="lab-verdict-next"><strong data-verdict-title aria-live="polite">Waiting for both runs</strong><button type="button" data-verdict-action hidden>Run other path</button></div></header>',
      '    <div class="lab-receipts">',
      '      <article data-receipt-mode="unmanaged" class="is-current"><header><div><strong>Without FerricStore</strong><span>Process memory is lost</span></div><b data-receipt-status>Not run</b></header><dl><div><dt>Recovery</dt><dd data-receipt-recovery>—</dd></div><div><dt>State retained</dt><dd data-receipt-retained>—</dd></div><div><dt>Work repeated</dt><dd data-receipt-repeated>—</dd></div><div><dt>Fence</dt><dd data-receipt-fence>Not used</dd></div></dl></article>',
      '      <article data-receipt-mode="durable"><header><div><strong>With FerricStore</strong><span>Named state survives</span></div><b data-receipt-status>Not run</b></header><dl><div><dt>Recovery</dt><dd data-receipt-recovery>—</dd></div><div><dt>State retained</dt><dd data-receipt-retained>—</dd></div><div><dt>Work repeated</dt><dd data-receipt-repeated>—</dd></div><div><dt>Fence</dt><dd data-receipt-fence>—</dd></div></dl></article>',
      '    </div>',
      '  </section>',
      '  <div class="lab-shell">',
      '    <section class="lab-trace" aria-label="Execution trace">',
      '      <header><div><span class="lab-header-label">LIVE RUN</span><strong>report-204</strong></div><span class="status-pill" data-status>Idle</span></header>',
      '      <ol class="lab-progress" aria-label="Workflow progress">',
      '        <li data-rank="0"><span>01</span><strong data-progress-label>Plan</strong></li><li data-rank="1"><span>02</span><strong data-progress-label>Search</strong></li><li data-rank="2"><span>03</span><strong data-progress-label>Summarize</strong></li>',
      '        <li data-rank="3"><span>04</span><strong data-progress-label>Crash</strong></li><li data-rank="4"><span>05</span><strong data-progress-label>Restart</strong></li><li data-rank="5"><span>06</span><strong data-progress-label>Finish</strong></li>',
      '      </ol>',
      '      <div class="lab-runtime">',
      '        <article class="lab-worker" data-worker-a><span>WORKER A</span><strong data-worker-a-label>Ready</strong><small data-worker-a-detail>original process</small></article>',
      '        <div class="lab-link" aria-hidden="true"><span></span><b data-link-a>memory</b></div>',
      '        <article class="lab-store"><span data-store-kicker>PROCESS MEMORY</span><strong data-store-title>Volatile working state</strong><dl><div><dt>Current state</dt><dd data-flow-state>Ready</dd></div><div><dt>Fence</dt><dd data-fence>Not used</dd></div></dl><code data-persisted>no values yet</code></article>',
      '        <div class="lab-link" aria-hidden="true"><span></span><b data-link-b>restart</b></div>',
      '        <article class="lab-worker" data-worker-b><span>WORKER B</span><strong data-worker-b-label>Standby</strong><small data-worker-b-detail>replacement process</small></article>',
      '      </div>',
      '      <div class="lab-message"><span class="lab-message-marker" aria-hidden="true"></span><p data-message>Ready to run the restart path.</p></div>',
      '    </section>',
      '    <section class="lab-code" aria-label="Implementation code">',
      '      <header><div><span class="file-dot" aria-hidden="true">py</span><strong data-code-file>research_process.py</strong></div><span class="lab-code-mode" data-code-mode>Process memory</span></header>',
      '      <pre data-code-view="unmanaged"><code>',
      '<span data-code-phase="idle"># No durable workflow state</span>',
      '<span class="code-section" data-code-phase="plan">def run_report(payload):</span>',
      '<span data-code-phase="plan">    plan = make_plan(payload)</span>',
      '<span data-code-phase="search">    facts = search_sources(plan)</span>',
      '<span data-code-phase="summarize">    return summarize_facts(facts)</span>',
      '<span class="code-section" data-code-phase="crashed"># Process crashes during summarize.</span>',
      '<span data-code-phase="restarting"># Worker B has no checkpoint.</span>',
      '<span data-code-phase="replan">run_report(payload)  # starts again at Plan</span>',
      '      </code></pre>',
      '      <pre data-code-view="durable" hidden><code>',
      '<span data-code-phase="idle">from ferricstore import WorkflowClient, transition, complete</span>',
      '<span data-code-phase="idle">client = WorkflowClient.from_url("ferric://127.0.0.1:6388")</span>',
      '<span data-code-phase="idle">flow = client.workflow(type="research", initial_state="plan")</span>',
      '<span class="code-section" data-code-phase="plan">@flow.state("plan")</span>',
      '<span data-code-phase="plan">def plan(ctx):</span>',
      '<span data-code-phase="plan">    plan = make_plan(ctx.payload)</span>',
      '<span data-code-phase="plan">    return transition("search", values={"plan": plan})</span>',
      '<span class="code-section" data-code-phase="search">@flow.state("search")</span>',
      '<span data-code-phase="search">def search(ctx):</span>',
      '<span data-code-phase="search">    facts = search_sources(ctx.value("plan"))</span>',
      '<span data-code-phase="search">    return transition("summarize", values={"facts": facts})</span>',
      '<span class="code-section" data-code-phase="summarize">@flow.state("summarize")</span>',
      '<span data-code-phase="summarize">def summarize(ctx):</span>',
      '<span data-code-phase="summarize">    summary = summarize_facts(ctx.value("facts"))</span>',
      '<span data-code-phase="complete">    return complete(result=summary)</span>',
      '<span class="code-section" data-code-phase="idle">flow.start("report-204", payload={"topic": "workflow recovery"})</span>',
      '<span data-code-phase="idle">flow.worker().run()</span>',
      '      </code></pre>',
      '      <footer><span data-code-footer>Crash loses local results. Restart repeats Plan and Search.</span><strong data-code-pointer>Ready</strong></footer>',
      '    </section>',
      '  </div>',
      '</div>'
    ].join("");

    var status = mount.querySelector("[data-status]");
    var progress = Array.prototype.slice.call(mount.querySelectorAll("[data-rank]"));
    var progressLabels = Array.prototype.slice.call(mount.querySelectorAll("[data-progress-label]"));
    var codeViews = Array.prototype.slice.call(mount.querySelectorAll("[data-code-view]"));
    var codeFile = mount.querySelector("[data-code-file]");
    var codeMode = mount.querySelector("[data-code-mode]");
    var codeFooter = mount.querySelector("[data-code-footer]");
    var codePointer = mount.querySelector("[data-code-pointer]");
    var workerA = mount.querySelector("[data-worker-a]");
    var workerB = mount.querySelector("[data-worker-b]");
    var workerALabel = mount.querySelector("[data-worker-a-label]");
    var workerBLabel = mount.querySelector("[data-worker-b-label]");
    var workerADetail = mount.querySelector("[data-worker-a-detail]");
    var workerBDetail = mount.querySelector("[data-worker-b-detail]");
    var store = mount.querySelector(".lab-store");
    var storeKicker = mount.querySelector("[data-store-kicker]");
    var storeTitle = mount.querySelector("[data-store-title]");
    var flowState = mount.querySelector("[data-flow-state]");
    var fence = mount.querySelector("[data-fence]");
    var persisted = mount.querySelector("[data-persisted]");
    var linkA = mount.querySelector("[data-link-a]");
    var linkB = mount.querySelector("[data-link-b]");
    var message = mount.querySelector("[data-message]");
    var verdictTitle = mount.querySelector("[data-verdict-title]");
    var verdictSummary = mount.querySelector("[data-verdict-summary]");
    var verdictAction = mount.querySelector("[data-verdict-action]");

    function receiptFor(mode) {
      return mount.querySelector('[data-receipt-mode="' + mode + '"]');
    }

    function updateReceipt(mode, state) {
      var receipt = receiptFor(mode);
      var result = results[mode];
      if (!receipt) return;

      receipt.classList.toggle("is-current", state.mode === mode);
      receipt.classList.toggle("is-running", state.mode === mode && ["RUNNING", "AWAITING_CRASH", "CRASHED"].indexOf(state.status) !== -1);
      receipt.classList.toggle("is-complete", Boolean(result));
      receipt.querySelector("[data-receipt-status]").textContent = result
        ? "Complete"
        : state.mode === mode && state.status !== "IDLE" ? global.FerricDemo.phaseLabel(state.phase) : "Not run";
      receipt.querySelector("[data-receipt-recovery]").textContent = result ? result.recovery : "—";
      receipt.querySelector("[data-receipt-retained]").textContent = result ? result.retained : "—";
      receipt.querySelector("[data-receipt-repeated]").textContent = result ? result.repeated : "—";
      receipt.querySelector("[data-receipt-fence]").textContent = result ? result.fence : mode === "unmanaged" ? "Not used" : "—";
    }

    function updateVerdict(state) {
      updateReceipt("unmanaged", state);
      updateReceipt("durable", state);
      if (results.unmanaged && results.durable) {
        verdictTitle.textContent = "Same crash. Different recovery.";
        verdictSummary.textContent = "Restart repeated 2 states. Durable resume repeated 0.";
        verdictAction.hidden = true;
      } else if (results.unmanaged || results.durable) {
        verdictTitle.textContent = results.unmanaged ? "Restart path recorded" : "Durable resume recorded";
        verdictSummary.textContent = "Run the other path to keep both results side by side.";
        verdictAction.hidden = false;
        verdictAction.textContent = results.unmanaged ? "Run durable resume" : "Run restart path";
      } else {
        verdictTitle.textContent = state.status === "IDLE" ? "Waiting for both runs" : "Running the " + (state.mode === "durable" ? "durable resume" : "restart") + " path";
        verdictSummary.textContent = "The same crash either loses process memory or resumes from named state.";
        verdictAction.hidden = true;
      }
    }

    verdictAction.addEventListener("click", function () {
      var nextMode = results.unmanaged ? "durable" : "unmanaged";
      var modeButton = document.querySelector('[data-mode="' + nextMode + '"]');
      if (modeButton) modeButton.click();
      window.setTimeout(function () {
        var runButton = document.querySelector(".fs-primary-run");
        if (runButton && !runButton.disabled) runButton.click();
      }, 80);
    });

    function updateProgress(state, rank, copy) {
      progressLabels.forEach(function (label, index) {
        var text = copy.progress[index];
        if (state.mode === "unmanaged" && state.status === "COMPLETED" && index < 3) text += " ×2";
        else if (state.mode === "unmanaged" && phaseIsReplay(state.phase) && index === rank) text = "Repeat " + text.toLowerCase();
        label.textContent = text;
      });

      progress.forEach(function (item) {
        var itemRank = Number(item.dataset.rank);
        var active = itemRank === rank;
        var done = itemRank < rank || state.phase === "complete";
        if (state.mode === "unmanaged" && phaseIsReplay(state.phase)) {
          done = itemRank === 3 || itemRank === 4 || itemRank < rank;
        }
        item.classList.toggle("is-active", active);
        item.classList.toggle("is-done", done);
        item.classList.toggle("is-repeated", state.mode === "unmanaged" && (state.status === "COMPLETED" || phaseIsReplay(state.phase)) && itemRank < 3);
        if (active) item.setAttribute("aria-current", "step");
        else item.removeAttribute("aria-current");
      });
    }

    function updateWorkers(state) {
      var durable = state.mode === "durable";
      var workerAActive = state.activeWorker === "worker-a" && ["RUNNING", "AWAITING_CRASH"].indexOf(state.status) !== -1;
      var workerBActive = state.activeWorker === "worker-b" && state.status !== "IDLE";
      var workerAStopped = state.phase === "crashed" || state.previousWorker === "worker-a";

      workerA.classList.toggle("is-active", workerAActive);
      workerA.classList.toggle("is-crashed", workerAStopped);
      workerB.classList.toggle("is-active", workerBActive);
      workerB.classList.toggle("is-complete", state.activeWorker === "worker-b" && state.status === "COMPLETED");

      if (workerAStopped) workerALabel.textContent = durable ? "Lease expired" : "Process stopped";
      else workerALabel.textContent = workerAActive ? "Running" : "Ready";

      if (!workerBActive) workerBLabel.textContent = "Standby";
      else if (state.status === "COMPLETED") workerBLabel.textContent = "Completed";
      else if (durable) workerBLabel.textContent = "Resuming state";
      else if (phaseIsReplay(state.phase)) workerBLabel.textContent = "Repeating work";
      else workerBLabel.textContent = "Restarting";
    }

    engine.subscribe(function (envelope) {
      var state = envelope.state;
      var copy = MODE_COPY[state.mode];
      var rank = global.FerricDemo.phaseRank(state.phase);
      var codePhase = state.phase;
      var durable = state.mode === "durable";

      status.className = "status-pill " + statusClass(state.status);
      status.textContent = state.status === "AWAITING_CRASH" ? "Crash ready" : global.FerricDemo.phaseLabel(state.phase);
      codePointer.textContent = global.FerricDemo.phaseLabel(state.phase);

      codeViews.forEach(function (view) {
        var selected = view.dataset.codeView === state.mode;
        view.hidden = !selected;
        Array.prototype.slice.call(view.querySelectorAll("[data-code-phase]")).forEach(function (line) {
          var linePhase = line.dataset.codePhase;
          var active = linePhase === codePhase;
          if (state.mode === "durable" && ["crashed", "recovering"].indexOf(codePhase) !== -1) active = linePhase === "summarize";
          if (state.mode === "unmanaged" && ["research", "resummarize"].indexOf(codePhase) !== -1) active = linePhase === "replan";
          if (state.phase === "complete") active = linePhase === "complete" || (state.mode === "unmanaged" && linePhase === "replan");
          line.classList.toggle("is-active", active);
        });
      });

      codeFile.textContent = copy.codeFile;
      codeMode.textContent = copy.codeMode;
      codeFooter.textContent = durable
        ? "Saved state lets Worker B resume at Summarize."
        : "Crash loses local results. Restart repeats Plan and Search.";

      updateProgress(state, rank, copy);
      updateWorkers(state);

      workerADetail.textContent = copy.workerADetail;
      workerBDetail.textContent = copy.workerBDetail;
      store.classList.toggle("is-volatile", !durable);
      storeKicker.textContent = copy.storeKicker;
      storeTitle.textContent = copy.storeTitle;
      linkA.textContent = copy.firstLink;
      linkB.textContent = copy.secondLink;
      flowState.textContent = global.FerricDemo.phaseLabel(state.phase);
      fence.textContent = durable ? String(state.fencingToken) : "Not used";

      var keys = Object.keys(durable ? state.persisted : state.volatile);
      if (keys.length) persisted.textContent = (durable ? "saved: " : state.repeatedSteps.length ? "rebuilt: " : "in memory: ") + keys.join(", ");
      else if (!durable && rank >= 3) persisted.textContent = "lost on crash: plan, search";
      else persisted.textContent = "no values yet";

      message.textContent = state.message;

      if (state.status === "COMPLETED") {
        results[state.mode] = durable ? {
          recovery: "Durable resume",
          retained: "Plan + Search",
          repeated: "0 states",
          fence: String(state.previousFencingToken || 41) + " → " + String(state.fencingToken)
        } : {
          recovery: "Restart from Plan",
          retained: "None",
          repeated: String(state.repeatedSteps.length) + " states",
          fence: "Not used"
        };
      }
      updateVerdict(state);
    });
  };
})(window);
