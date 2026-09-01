(function () {
  "use strict";

  var configs = {
    "workflow-explainer": {
      code: "WF-01",
      title: "Workflows, without the hand-waving",
      summary: "Crash a five-state order and see which work is durable, which worker may continue, and where an external idempotency key still matters.",
      outcome: "See one committed workflow move from a failed worker to a replacement without losing its named state.",
      kind: "serial",
      steps: ["charge", "stock", "crash", "reclaim", "deliver"],
      target: ".stage-card",
      action: "Play the selected workflow",
      actionTarget: "[data-replay]"
    },
    "ai-agent-workflow": {
      code: "WF-02",
      title: "An agent can wait without staying alive",
      summary: "Park a long-running agent at a human approval boundary, restart its host, and resume from a durable external signal.",
      outcome: "Inspect the approval state before and after a simulated host restart.",
      kind: "gate",
      steps: ["research", "draft", "approval", "restart", "resume"],
      target: ".stage-card",
      action: "Run the approval workflow",
      actionTarget: "[data-replay]"
    },
    "travel-saga": {
      code: "WF-03",
      title: "Rollback is a workflow, not a cleanup script",
      summary: "Book flight, hotel, and car as explicit states, then follow compensation when a later reservation fails.",
      outcome: "Compare a hotel failure, flight error, and happy path against the same durable ledger.",
      kind: "compensate",
      steps: ["flight", "hotel", "car", "failure", "compensate"],
      target: ".stage-card",
      action: "Run the selected booking",
      actionTarget: "[data-replay]"
    },
    "subscription-dunning": {
      code: "WF-04",
      title: "A retry schedule you can inspect",
      summary: "Move a subscription through durable retry windows, customer messaging, recovery, and cancellation.",
      outcome: "Jump to a decline or recovery and see the current state remain explicit.",
      kind: "timeline",
      steps: ["day 0", "day 3", "day 7", "day 15", "day 21"],
      target: ".stage-card",
      action: "Run the dunning timeline",
      actionTarget: "[data-replay]"
    },
    "ticket-reservation": {
      code: "WF-05",
      title: "One seat. Two buyers. One valid owner.",
      summary: "Watch a reservation lease expire, a second buyer claim the seat, and fencing reject stale ownership.",
      outcome: "Follow ownership from hold through expiry to final resolution.",
      kind: "race",
      steps: ["seat open", "buyer A", "lease ends", "buyer B", "resolve"],
      target: ".demo-workspace",
      action: "Run the reservation race",
      actionTarget: "[data-btn-play]"
    },
    "canary-rollback": {
      code: "WF-06",
      title: "Deployment decisions that survive the deployer",
      summary: "Route a canary, wait through a durable soak period, detect a failure, and advance to rollback after a restart.",
      outcome: "See observation and rollback modeled as inspectable states rather than one fragile script.",
      kind: "branch",
      steps: ["route 10%", "soak", "restart", "5xx signal", "rollback"],
      target: ".demo-workspace",
      action: "Run the canary story",
      actionTarget: "[data-btn-play]"
    },
    "parallel-fanout": {
      code: "WF-07",
      title: "Retry one failed child, not the whole batch",
      summary: "Fan out sixteen independently recoverable children, fail chunk nine, and join the completed results.",
      outcome: "Watch successful children stay committed while one child is retried.",
      kind: "fanout",
      steps: ["split", "16 children", "chunk 9 fails", "retry 9", "join"],
      target: ".demo-workspace",
      action: "Run the fan-out",
      actionTarget: "[data-btn-play]"
    },
    "agent-loop": {
      code: "WF-08",
      title: "Bound the agent before the loop runs away",
      summary: "Persist a budget and circuit state so repeated agent work cannot silently exceed the intended limit or hammer a failing dependency.",
      outcome: "Test normal execution, a budget cap, and a downstream circuit breaker.",
      kind: "gate",
      steps: ["request", "budget", "model", "circuit", "result"],
      target: ".demo-workspace",
      action: "Run the guarded agent",
      actionTarget: "[data-btn-play]"
    },
    "split-lab": {
      code: "WF-09",
      title: "Crash the worker in the middle",
      summary: "Follow an AI research workflow through planning, search, summarization, lease expiry, and fenced reclaim.",
      outcome: "Verify that a replacement worker claims the durable middle state and continues from there.",
      kind: "handoff",
      steps: ["plan", "search", "summarize", "crash", "reclaim", "done"],
      target: "#direction-view",
      action: "Run crash and reclaim",
      actionTarget: "[data-action='run']"
    },
    "idempotency-determinism": {
      code: "WF-10",
      title: "Retries are safe only at a named boundary",
      summary: "Inject a crash between an external effect and a committed transition, then compare an unguarded retry with a stable operation identity.",
      outcome: "Inspect double-charge, token, and inventory scenarios without treating handler execution as exactly-once.",
      kind: "dedupe",
      steps: ["effect", "crash", "retry", "same key", "one result"],
      target: ".comparison-stage",
      action: "Run the selected retry",
      actionTarget: "[data-btn-autoplay]"
    },
    "zombie-fencing": {
      code: "CO-01",
      title: "The stale worker can return. Its write cannot win.",
      summary: "Freeze one worker, let its lease expire, promote a replacement, and submit the original worker’s stale write.",
      outcome: "Compare fencing generations at the write boundary.",
      kind: "fence",
      steps: ["token 41", "freeze", "token 42", "stale return", "reject"],
      target: ".stage-card",
      action: "Start the fencing race",
      actionTarget: "[data-freeze-btn]"
    },
    "architecture-comparison": {
      code: "AR-01",
      title: "Run the same failure through four architectures",
      summary: "Compare volatile memory, a queue, database polling, and explicit durable workflow state under one crash scenario.",
      outcome: "Inspect recovery behavior first; open the larger matrix only when you need the architectural detail.",
      kind: "compare",
      steps: ["memory", "queue", "database", "workflow"],
      target: ".stage-card",
      action: "Trigger the shared failure",
      actionTarget: "[data-smash-crash]"
    },
    "benchmark-explainer": {
      code: "AR-02",
      title: "Read the workflow benchmark by execution boundary",
      summary: "Separate worker-driven workflows from fused deterministic chains, then keep workload, hardware, units, and limitations attached to every number.",
      outcome: "Compare documented workflow modes without turning unlike systems into a winner claim.",
      kind: "throughput",
      steps: ["claim", "handler", "transition", "next lease", "complete"],
      target: "#workflow-modes",
      action: "Compare workflow modes",
      actionTarget: "[data-mode-button='fused']"
    },
    "hot-cold-storage": {
      code: "DS-01",
      title: "One keyspace across memory pressure",
      summary: "Follow values between the hot memory tier and disk-backed storage while the logical key remains available.",
      outcome: "Trigger pressure, inspect tier movement, and read a cold key.",
      kind: "tiers",
      steps: ["hot write", "pressure", "evict", "cold read", "promote"],
      target: ".demo-workspace",
      action: "Trigger memory pressure",
      actionTarget: "[data-btn-pressure]"
    },
    "rate-limiting-stream": {
      code: "QS-01",
      title: "Turn a burst into bounded work",
      summary: "Absorb webhook traffic into a durable stream, apply a throughput limit, and process explicit micro-batches.",
      outcome: "Adjust the rate and batch size, then trigger a 5,000-event burst.",
      kind: "buffer",
      steps: ["5,000 events", "stream", "rate gate", "batch", "workers"],
      target: ".stage-card",
      action: "Trigger the burst",
      actionTarget: "[data-burst-btn]"
    },
    "beginner-queue": {
      code: "QS-02",
      title: "Your first durable queue, state by state",
      summary: "Enqueue one job, let a worker claim it under a lease, crash the worker, and reclaim the eligible job safely.",
      outcome: "Learn ownership through the job itself before comparing queue products.",
      kind: "handoff",
      steps: ["queued", "claimed", "worker crash", "lease ends", "reclaimed"],
      target: ".demo-workspace",
      action: "Run the clean queue path",
      actionTarget: "[data-btn-clean]"
    },
    "cache-stampede": {
      code: "DS-02",
      title: "Ten thousand callers. One recomputation.",
      summary: "Expire a hot value and coordinate concurrent callers so one owner recomputes while the rest reuse the result.",
      outcome: "Compare the origin load with and without stampede protection.",
      kind: "converge",
      steps: ["10k callers", "expired key", "one owner", "recompute", "shared result"],
      target: ".demo-workspace",
      action: "Trigger the stampede",
      actionTarget: "[data-btn-stampede]"
    },
    "stream-vs-pubsub": {
      code: "QS-03",
      title: "Replayable delivery or live broadcast?",
      summary: "Compare consumer-group stream delivery with Pub/Sub fan-out and see what remains available after a subscriber is absent.",
      outcome: "Choose the primitive by delivery behavior, not by a generic messaging label.",
      kind: "split",
      steps: ["publish", "broadcast", "offline", "replay", "ack"],
      target: ".demo-workspace",
      action: "Run the selected delivery",
      actionTarget: "[data-btn-action-2]"
    },
    "hash-field-ttl": {
      code: "DS-03",
      title: "Expire fields without deleting the hash",
      summary: "Give individual hash fields independent lifetimes while the surrounding object and its other fields remain available.",
      outcome: "Set two fields with different TTLs and watch each expiration in place.",
      kind: "expiry",
      steps: ["profile", "2FA · 5s", "cart · 10s", "field expires", "hash remains"],
      target: ".demo-workspace",
      action: "Set the first field TTL",
      actionTarget: "[data-btn-set-2fa]"
    },
    "probabilistic-cache": {
      code: "DS-04",
      title: "Reject definite misses before the origin",
      summary: "Send valid and bogus keys through set membership, Bloom, and Cuckoo filter paths to reduce unnecessary lookups.",
      outcome: "Compare a valid key with a 10,000-key bogus request burst.",
      kind: "filter",
      steps: ["request", "filter", "definite miss", "possible hit", "origin"],
      target: ".demo-workspace",
      action: "Run a valid lookup",
      actionTarget: "[data-btn-valid]"
    }
  };

  var routeSignals = {
    "workflow-explainer": { step: "[data-station].is-active", attr: "data-station", count: 5, status: "[data-live-status-text], [data-narrative-title]" },
    "ai-agent-workflow": { step: "[data-agent-node].is-active", attr: "data-agent-node", count: 5, status: "[data-live-status], [data-narrative-title]" },
    "travel-saga": { step: "[data-saga-node].is-active", attr: "data-saga-node", count: 5, status: "[data-live-status], [data-narrative-title]" },
    "subscription-dunning": { step: "[data-tm-node].is-active", attr: "data-tm-node", count: 5, status: "[data-live-status], [data-narrative-title]" },
    "ticket-reservation": { step: "[data-step-indicator].is-active", attr: "data-step-indicator", base: 1, count: 3, status: "[data-live-status]" },
    "canary-rollback": { step: "[data-step-indicator].is-active", attr: "data-step-indicator", base: 1, count: 3, status: "[data-live-status]" },
    "split-lab": { step: "[data-rank].is-active", attr: "data-rank", count: 6, status: "[data-status], [data-message]" },
    "zombie-fencing": { step: "[data-stepper] li.is-active", attr: "data-step", count: 4, status: "[data-live-status], [data-current-run-step]" },
    "architecture-comparison": { step: "[data-step-node].is-active", attr: "data-step-node", count: 4, status: "[data-live-status-text], [data-narrative-title]" },
    "agent-loop": { status: "[data-live-status], [data-turns-counter]", number: "Turn\\s+(\\d+)", base: 0, count: 5, map: [["SUCCESS|COMPLETE", 4], ["CIRCUIT BREAKER|BUDGET CAP", 3], ["RUNAWAY|SLAMMING", 2]] },
    "beginner-queue": { status: "[data-live-status]", number: "(?:^|\\s)([123])\\.", base: 1, count: 3, map: [["RECLAIMED", 3], ["SAFE COMPLETION|COMPLETED", 4], ["CRASHED", 2]] },
    "cache-stampede": { status: "[data-live-status]", map: [["WAITER RELEASE|COMPLETE", 4], ["FETCH_OR_COMPUTE", 3], ["10,000 QUERIES", 1], ["WARM CACHE", 0]] },
    "parallel-fanout": { status: "[data-live-status]", map: [["JOB COMPLETE|COMPLETED", 4], ["RECOVERING|GATHERING", 3], ["WORKER #9 CRASH", 2], ["FANNING OUT", 1], ["READY", 0]] },
    "hot-cold-storage": { status: "[data-live-status]", map: [["COLD READ", 4], ["SAFE", 3], ["DEMOTING", 2], ["OOM|EVICTION", 1], ["HOT KEY|NORMAL", 0]] },
    "idempotency-determinism": { status: "[data-term-status], [data-left-outcome], [data-right-outcome]", map: [["SIMULATION COMPLETE", 4], ["RETRY IN PROGRESS|REPLACEMENT WORKER", 2], ["CRASH DETECTED|WORKER DIED", 1], ["EXECUTING|IN-FLIGHT", 0], ["READY|WAITING", 0]] },
    "hash-field-ttl": { status: "[data-live-status], [data-status-2fa], [data-status-cart]", actionIndex: 1, map: [["PURGED", 3]] },
    "probabilistic-cache": { status: "[data-live-status], [data-exp-title]", actionIndex: 3, map: [["DEFINITELY ABSENT|CRASHING POSTGRES", 2]] },
    "rate-limiting-stream": { status: "[data-current-run-value], [data-buffer-stat]", actionIndex: 1 },
    "stream-vs-pubsub": { status: "[data-live-status]", map: [["CONSUMER GROUP", 4], ["APPENDING", 3], ["MULTI-POD", 2], ["BROADCASTING", 1]] },
    "benchmark-explainer": { status: "[data-current-run-label], [aria-pressed='true'][data-mode-button]", actionIndex: 2 }
  };

  function routeId() {
    var parts = window.location.pathname.split("/").filter(Boolean);
    var last = parts[parts.length - 1] || "";
    if (last === "ferricstore") return "";
    return configs[last] ? last : "";
  }

  function svgLogo() {
    return '<svg viewBox="0 0 32 32" aria-hidden="true"><path d="M5 6h21v5H11v4h12v5H11v7H5z" fill="currentColor"/><path d="M22 15h5v12h-5z" fill="#d8682a"/></svg>';
  }

  function makeNav(config) {
    var nav = document.createElement("nav");
    nav.className = "fs-eval-nav";
    nav.setAttribute("aria-label", "Demo navigation");
    nav.innerHTML =
      '<a class="fs-brand" href="../">' + svgLogo() + '<span><strong>FerricStore</strong><small>Evaluation lab</small></span></a>' +
      '<span class="fs-route-name"><b>' + config.code + '</b><span>' + config.title + '</span></span>' +
      '<span class="fs-nav-links"><a href="../">All demos</a><a href="https://github.com/ferricstore/ferricstore#readme">Documentation</a><a href="https://github.com/ferricstore/ferricstore">Source</a></span>';
    return nav;
  }

  function makeSignature(config) {
    var figure = document.createElement("figure");
    figure.className = "fs-strip-signature is-" + config.kind;
    figure.style.setProperty("--fs-step-count", String(config.steps.length));
    figure.setAttribute("aria-label", "Mechanism map: " + config.steps.join(", "));
    var cells = config.steps.map(function (step, index) {
      var state = index === 0 ? " is-current" : index === config.steps.length - 1 ? " is-outcome" : "";
      return '<li class="' + state.trim() + '" data-fs-signature-step="' + index + '"><span>' + String(index + 1).padStart(2, "0") + '</span><strong>' + step + '</strong></li>';
    }).join("");
    figure.innerHTML = '<figcaption><span>Mechanism map</span><strong>' + config.outcome + '</strong></figcaption><ol>' + cells + '</ol><p class="fs-scroll-hint">Scroll the state rail to inspect every step.</p>';
    return figure;
  }

  function findMain() {
    var main = document.querySelector("main");
    if (main) return main;
    var mount = document.getElementById("ai-orchestration-demo");
    if (!mount) return null;
    main = document.createElement("main");
    main.className = "fs-generated-main";
    mount.before(main);
    main.appendChild(mount);
    return main;
  }

  function makeIntro(config, controls) {
    var intro = document.createElement("div");
    intro.className = "fs-intro";
    intro.innerHTML =
      '<div class="fs-intro-copy"><h1>' + config.title + '</h1><p>' + config.summary + '</p></div>' +
      '<div class="fs-run-group"><button type="button" class="fs-primary-run">' + config.action + '</button>' +
      '<p class="fs-run-status" aria-live="polite">Ready to run in this browser.</p></div>' +
      '<p class="fs-outcome"><strong>What to verify</strong><span>' + config.outcome + '</span></p>' +
      '<a class="fs-evidence-link" href="#evaluation-evidence">Inspect evaluation evidence</a>';
    if (controls) {
      controls.classList.add("fs-primary-controls");
      intro.insertBefore(controls, intro.querySelector(".fs-outcome"));
    }
    return intro;
  }

  function findPrimaryButton(config) {
    var candidates = Array.prototype.slice.call(document.querySelectorAll(config.actionTarget));
    var enabled = candidates.filter(function (button) { return !button.disabled && !button.hidden; });
    return enabled[0] || candidates[0] || null;
  }

  function disclosureLabel(node) {
    var cls = node.className || "";
    var text = (node.querySelector("h2, h3") || {}).textContent || "";
    if (/evidence-note|mode-boundary/i.test(cls)) return "How to read this evidence";
    if (/code|sdk/i.test(cls + " " + text)) return "Inspect implementation and SDK patterns";
    if (/matrix|comparison|architecture|use-case/i.test(cls + " " + text)) return "Compare architecture and trade-offs";
    if (/catastrophe|pain|failure/i.test(cls + " " + text)) return "Inspect the failure without this mechanism";
    if (/source|reference/i.test(cls + " " + text)) return "Sources and evidence boundaries";
    if (/faq|question/i.test(cls + " " + text)) return "Questions and technical boundaries";
    return text.trim() || "Technical detail";
  }

  function wrapDisclosure(node, label) {
    if (!node || node.closest("details.fs-disclosure")) return null;
    var details = document.createElement("details");
    details.className = "fs-disclosure";
    var summary = document.createElement("summary");
    summary.innerHTML = '<span>' + label + '</span><small>Open</small>';
    node.before(details);
    details.append(summary, node);
    return details;
  }

  function enhanceSecondary(main, firstView, hero) {
    var metrics = main.querySelector(":scope > .metrics-bar");
    if (metrics) {
      metrics.classList.add("fs-evidence-strip");
      firstView.after(metrics);
    }

    Array.prototype.slice.call(main.children).forEach(function (node) {
      if (node === firstView || node === metrics || node === hero || node.matches("script, footer")) return;
      if (node.classList.contains("fs-disclosure")) return;
      if (node.matches("section, aside") && !node.closest(".fs-first-view")) {
        wrapDisclosure(node, disclosureLabel(node));
      }
    });

    var accuracy = document.querySelector(".demo-accuracy-note");
    if (accuracy) {
      var accuracyDetails = wrapDisclosure(accuracy, "Accuracy and SDK boundary");
      if (accuracyDetails) {
        accuracyDetails.classList.add("fs-accuracy-disclosure");
        metrics ? metrics.after(accuracyDetails) : firstView.after(accuracyDetails);
      }
    }

    var destination = metrics || document.querySelector(".fs-accuracy-disclosure") || main.querySelector(".fs-disclosure");
    var evidenceLink = firstView.querySelector(".fs-evidence-link");
    if (destination) {
      destination.id = "evaluation-evidence";
    } else if (evidenceLink) {
      evidenceLink.hidden = true;
    }
  }

  function installTabKeyboard() {
    Array.prototype.slice.call(document.querySelectorAll('[role="tablist"]')).forEach(function (tablist) {
      if (tablist.dataset.fsKeyboardReady) return;
      tablist.dataset.fsKeyboardReady = "true";
      tablist.addEventListener("keydown", function (event) {
        if (["ArrowLeft", "ArrowRight", "ArrowUp", "ArrowDown", "Home", "End"].indexOf(event.key) === -1) return;
        var tabs = Array.prototype.slice.call(tablist.querySelectorAll('[role="tab"]')).filter(function (tab) {
          return !tab.disabled && !tab.hidden;
        });
        if (!tabs.length) return;
        var current = tabs.indexOf(document.activeElement);
        if (event.key === "Home") current = 0;
        else if (event.key === "End") current = tabs.length - 1;
        else {
          var direction = event.key === "ArrowLeft" || event.key === "ArrowUp" ? -1 : 1;
          current = (Math.max(0, current) + direction + tabs.length) % tabs.length;
        }
        event.preventDefault();
        tabs[current].focus();
        tabs[current].click();
      });
    });
  }

  function enhanceDemo(id, config) {
    document.body.dataset.fsSurface = "demo";
    document.body.dataset.fsDemo = id;

    var main = findMain();
    if (!main) return;
    var hero = main.querySelector(".demo-hero, .hero, .flagship-intro");
    var target = document.querySelector(config.target);
    if (!target) return;

    var controls = hero && hero.querySelector(".mode-container, .mode-toggle-wrap, #workflow-controls");
    if (!controls && id === "split-lab") controls = document.querySelector("#workflow-controls .workflow-modes");
    var firstView = document.createElement("section");
    firstView.className = "fs-first-view";
    firstView.setAttribute("aria-label", "Primary interactive experiment");
    var intro = makeIntro(config, controls);
    var experiment = document.createElement("div");
    experiment.className = "fs-experiment";
    experiment.append(makeSignature(config), target);
    firstView.append(intro, experiment);
    main.insertBefore(firstView, main.firstChild);

    if (hero) hero.classList.add("fs-retired-hero");

    var oldNav = document.querySelector(".demo-nav, .top-nav, .flagship-nav");
    var nav = makeNav(config);
    (oldNav || document.body.firstChild).before(nav);

    var skip = document.createElement("a");
    skip.className = "fs-skip-link";
    skip.href = "#fs-primary-action";
    skip.textContent = "Skip to experiment";
    document.body.insertBefore(skip, document.body.firstChild);
    var proxy = intro.querySelector(".fs-primary-run");
    proxy.id = "fs-primary-action";
    var signature = experiment.querySelector(".fs-strip-signature");
    var signatureSteps = Array.prototype.slice.call(signature.querySelectorAll("[data-fs-signature-step]"));
    var routeSignal = routeSignals[id] || {};
    var lastSignatureIndex = 0;

    function setSignatureIndex(index) {
      if (!Number.isFinite(index)) return;
      var normalized = Math.max(0, Math.min(signatureSteps.length - 1, Math.round(index)));
      lastSignatureIndex = normalized;
      signatureSteps.forEach(function (step, stepIndex) {
        step.classList.toggle("is-current", stepIndex === normalized);
        step.classList.toggle("is-done", stepIndex < normalized);
      });
      signature.dataset.fsCurrentStep = String(normalized + 1);
    }

    function liveStepIndex() {
      var signalText = Array.prototype.slice.call(document.querySelectorAll(routeSignal.status || "[data-live-status], [data-live-status-text], [data-status], [data-message], [data-term-status], [data-current-run-value]"))
        .map(function (node) { return node.textContent.trim().replace(/\s+/g, " "); }).filter(Boolean).join(" · ");
      if (routeSignal.map) {
        for (var mapIndex = 0; mapIndex < routeSignal.map.length; mapIndex += 1) {
          if (new RegExp(routeSignal.map[mapIndex][0], "i").test(signalText)) return routeSignal.map[mapIndex][1];
        }
      }
      if (routeSignal.number) {
        var match = signalText.match(new RegExp(routeSignal.number, "i"));
        if (match) {
          var numbered = Number(match[1]) - Number(routeSignal.base || 0);
          return routeSignal.count > 1 ? numbered * (signatureSteps.length - 1) / (routeSignal.count - 1) : numbered;
        }
      }
      var active = document.querySelector(routeSignal.step || '[data-rank].is-active, [data-station].is-active, [data-step].is-active, [aria-current="step"], .lab-progress .is-active, .station-node-item.is-active');
      if (!active) return NaN;
      var raw = routeSignal.attr ? active.getAttribute(routeSignal.attr) : active.getAttribute("data-rank") || active.getAttribute("data-station") || active.getAttribute("data-step");
      var numeric = raw !== null && raw !== "" && Number.isFinite(Number(raw))
        ? Number(raw) - Number(routeSignal.base || 0)
        : Array.prototype.slice.call(active.parentElement ? active.parentElement.children : []).indexOf(active);
      var nativeCount = Number(routeSignal.count || signatureSteps.length);
      return nativeCount > 1 ? numeric * (signatureSteps.length - 1) / (nativeCount - 1) : numeric;
    }

    function liveStatusText() {
      var candidates = Array.prototype.slice.call(document.querySelectorAll(routeSignal.status || '[data-live-status], [data-live-status-text], [data-status], [data-message], [data-workflow-hint], [data-narrative-title], [data-outcome-title], [data-term-status], [data-current-run-value]'));
      var live = candidates.find(function (node) {
        return node.textContent.trim() && window.getComputedStyle(node).display !== "none";
      });
      return live ? live.textContent.trim().replace(/\s+/g, " ").slice(0, 140) : "";
    }

    function syncExperiment() {
      var stepIndex = liveStepIndex();
      if (Number.isFinite(stepIndex)) setSignatureIndex(stepIndex);
      var original = findPrimaryButton(config);
      proxy.disabled = Boolean(original && original.disabled);
      var live = liveStatusText();
      if (live) intro.querySelector(".fs-run-status").textContent = "Live: " + live;
    }

    proxy.addEventListener("click", function () {
      var original = findPrimaryButton(config);
      var status = intro.querySelector(".fs-run-status");
      if (!original || original.disabled) {
        status.textContent = "The experiment is already running or waiting for its next available action.";
        target.scrollIntoView({ behavior: "smooth", block: "start" });
        return;
      }
      original.click();
      status.textContent = "Action sent to the live experiment.";
      if (Number.isFinite(routeSignal.actionIndex)) setSignatureIndex(routeSignal.actionIndex);
      if (id === "split-lab") {
        target.dataset.fsAutoCrash = "true";
        var attempts = 0;
        var crashTimer = window.setInterval(function () {
          attempts += 1;
          var crash = document.querySelector('[data-action="crash"]');
          if (crash && !crash.disabled) {
            window.clearInterval(crashTimer);
            target.dataset.fsAutoCrash = "false";
            crash.click();
          } else if (attempts > 100) {
            window.clearInterval(crashTimer);
          }
        }, 100);
      }
      window.setTimeout(syncExperiment, 0);
      window.setTimeout(syncExperiment, 180);
      window.setTimeout(syncExperiment, 700);
      target.scrollIntoView({ behavior: "smooth", block: "nearest" });
    });

    function selectedSetup() {
      var selected = document.querySelector('[role="tab"][aria-selected="true"], [aria-pressed="true"].scenario-choice');
      if (!selected) return "";
      var strong = selected.querySelector("strong");
      return (strong ? strong.textContent : selected.textContent).trim().replace(/\s+/g, " ").slice(0, 72);
    }

    function updateReadyStatus() {
      var selected = selectedSetup();
      intro.querySelector(".fs-run-status").textContent = selected
        ? "Current setup: " + selected + ". Ready to run."
        : "Ready to run in this browser.";
    }

    document.addEventListener("click", function (event) {
      if (event.target.closest('[role="tab"], .scenario-choice')) {
        window.setTimeout(updateReadyStatus, 0);
      }
    });

    window.setTimeout(function () {
      var reset = target.querySelector("[data-btn-reset], [data-reset-btn]");
      if (reset && !reset.disabled) reset.click();
      if (["workflow-explainer", "ai-agent-workflow", "travel-saga", "subscription-dunning", "architecture-comparison", "zombie-fencing"].indexOf(id) !== -1) {
        var pause = target.querySelector("[data-pause]");
        if (pause && /pause/i.test(pause.textContent)) pause.click();
      }
      updateReadyStatus();
      window.setTimeout(syncExperiment, 80);
    }, 0);

    var codePanels = Array.prototype.slice.call(target.querySelectorAll(".code-panel, .code-box"));
    if (codePanels.length) {
      target.classList.add("fs-code-collapsed");
      codePanels.forEach(function (panel, index) {
        if (!panel.id) panel.id = "fs-code-panel-" + id + "-" + (index + 1);
      });
      var codeButton = document.createElement("button");
      codeButton.type = "button";
      codeButton.className = "fs-code-toggle";
      codeButton.textContent = "Inspect code";
      codeButton.setAttribute("aria-expanded", "false");
      codeButton.setAttribute("aria-controls", codePanels.map(function (panel) { return panel.id; }).join(" "));
      codeButton.addEventListener("click", function () {
        var open = target.classList.toggle("fs-code-open");
        codeButton.textContent = open ? "Hide code" : "Inspect code";
        codeButton.setAttribute("aria-expanded", String(open));
      });
      intro.querySelector(".fs-run-group").appendChild(codeButton);
    }

    var experimentObserver = new MutationObserver(function () {
      if (id === "split-lab" && target.dataset.fsAutoCrash === "true") {
        var crash = document.querySelector('[data-action="crash"]');
        if (crash && !crash.disabled) {
          target.dataset.fsAutoCrash = "false";
          crash.click();
        }
      }
      window.requestAnimationFrame(syncExperiment);
    });
    experimentObserver.observe(target, { attributes: true, childList: true, characterData: true, subtree: true, attributeFilter: ["class", "aria-selected", "aria-pressed", "disabled"] });
    Array.prototype.slice.call(document.querySelectorAll(routeSignal.status || "")).forEach(function (statusNode) {
      if (!target.contains(statusNode)) experimentObserver.observe(statusNode, { attributes: true, childList: true, characterData: true, subtree: true });
    });

    if (id === "benchmark-explainer") {
      wrapDisclosure(target.querySelector(".fused-proof"), "Inspect the measured fused profiles");
    }

    enhanceSecondary(main, firstView, hero);
    installTabKeyboard();
  }

  function enhanceCatalog() {
    document.body.dataset.fsSurface = "catalog";
    var hero = document.querySelector(".hero");
    var catalog = document.querySelector(".catalog");
    if (!hero || !catalog) return;

    var path = document.createElement("nav");
    path.className = "fs-evaluation-path";
    path.setAttribute("aria-label", "Recommended evaluation path");
    path.innerHTML =
      '<p><strong>Recommended evaluation</strong><span>From concept to evidence in three demos.</span></p>' +
      '<ol>' +
      '<li><a href="./workflow-explainer/"><span>Understand</span><strong>Workflows for Humans</strong></a></li>' +
      '<li><a href="./split-lab/"><span>Break it</span><strong>Crash a live worker</strong></a></li>' +
      '<li><a href="./benchmark-explainer/"><span>Evaluate</span><strong>Read measured boundaries</strong></a></li>' +
      '</ol>';
    catalog.before(path);

    var visualHead = hero.querySelector(".visual-head strong");
    if (visualHead) visualHead.textContent = "workflow / report-204";
    installTabKeyboard();
  }

  document.body.classList.add("fs-dispatch");
  var id = routeId();
  if (id) enhanceDemo(id, configs[id]);
  else enhanceCatalog();
}());
