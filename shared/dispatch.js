(function () {
  "use strict";

  var configs = {
    "workflow-explainer": {
      code: "WF-01",
      title: "Workflows for humans",
      summary: "See how saving progress helps an order pick up after a crash.",
      outcome: "Watch what is saved, what may repeat, and where work continues after the crash.",
      kind: "serial",
      steps: ["charge", "stock", "crash", "reclaim", "deliver"],
      target: ".stage-card",
      action: "Run the order workflow",
      actionTarget: "[data-replay]"
    },
    "ai-agent-workflow": {
      code: "WF-02",
      title: "AI agent and human approval",
      summary: "Pause an AI task until a person approves it, even if the server restarts while it waits.",
      outcome: "Watch whether approval survives the restart and when the agent continues.",
      kind: "gate",
      steps: ["research", "draft", "approval", "restart", "resume"],
      target: ".stage-card",
      action: "Run approval scenario",
      actionTarget: "[data-replay]"
    },
    "travel-saga": {
      code: "WF-03",
      title: "When a travel booking fails",
      summary: "Book a flight, hotel, and car. See what gets cancelled if one booking fails.",
      outcome: "Watch which earlier bookings are undone, and in what order.",
      kind: "compensate",
      steps: ["flight", "hotel", "car", "failure", "compensate"],
      target: ".stage-card",
      action: "Run booking scenario",
      actionTarget: "[data-replay]"
    },
    "subscription-dunning": {
      code: "WF-04",
      title: "When a subscription payment fails",
      summary: "Try a failed payment again later, send reminders, or cancel the subscription.",
      outcome: "Watch when the next attempt happens and what changes if payment succeeds.",
      kind: "timeline",
      steps: ["Day 1: trial starts", "Day 14: warning email", "Day 15: payment attempt", "Day 18: retry #1", "Day 21: payment recovered"],
      target: ".stage-card",
      action: "Run billing timeline",
      actionTarget: "[data-replay]"
    },
    "ticket-reservation": {
      code: "WF-05",
      title: "Flash-sale reservation",
      summary: "Hold a ticket for a buyer, then release it if they do not pay in time.",
      outcome: "Watch why the first buyer cannot use an expired hold after someone else takes the seat.",
      kind: "race",
      steps: ["seat open", "buyer A", "lease ends", "buyer B", "resolve"],
      target: ".demo-workspace",
      action: "Run seat handoff",
      actionTarget: "[data-btn-play]"
    },
    "canary-rollback": {
      code: "WF-06",
      title: "Undo an unhealthy software update",
      summary: "Try an update on a small scale, check for problems, and decide whether to keep it.",
      outcome: "Watch the check that decides whether the update stays or returns to the previous version.",
      kind: "branch",
      steps: ["route 10%", "soak", "restart", "5xx signal", "rollback"],
      target: ".demo-workspace",
      action: "Run update check",
      actionTarget: "[data-btn-play]"
    },
    "parallel-fanout": {
      code: "WF-07",
      title: "Run many tasks together",
      summary: "Split a large job into smaller tasks and bring their results together.",
      outcome: "Watch which tasks repeat after one worker fails and which saved results stay in place.",
      kind: "fanout",
      steps: ["split", "16 children", "chunk 9 fails", "retry 9", "join"],
      target: ".demo-workspace",
      action: "Run all tasks",
      actionTarget: "[data-btn-play]"
    },
    "agent-loop": {
      code: "WF-08",
      title: "Set limits for an AI agent",
      summary: "Give an AI agent a spending limit and pause requests to a service that keeps failing.",
      outcome: "Watch how much budget is left and why requests stop.",
      kind: "gate",
      steps: ["request", "budget", "model", "circuit", "result"],
      target: ".demo-workspace",
      action: "Run normal agent",
      actionTarget: "[data-btn-play]"
    },
    "split-lab": {
      code: "WF-09",
      title: "Start over or carry on?",
      summary: "Interrupt an AI research task. Compare starting from scratch with continuing from saved progress.",
      outcome: "Watch where the replacement picks up and which work was already saved.",
      kind: "handoff",
      steps: ["plan", "search", "summarize", "crash", "restart", "finish"],
      target: "#direction-view",
      action: "Run restart path",
      actionTarget: "[data-action='run']"
    },
    "idempotency-determinism": {
      code: "WF-10",
      title: "Avoid repeating a payment",
      summary: "See why trying a task again needs care when it calls a payment provider or another service.",
      outcome: "Watch how the same action reference changes a repeated payment or service call.",
      kind: "dedupe",
      steps: ["effect", "crash", "retry", "same key", "one result"],
      target: ".comparison-stage",
      action: "Run retry example",
      actionTarget: "[data-btn-autoplay]"
    },
    "zombie-fencing": {
      code: "CO-01",
      title: "Stop an old worker changing the result",
      summary: "A worker does a job. See what happens when it returns after a replacement has taken over.",
      outcome: "Watch why FerricStore rejects the old worker's attempt to save changes.",
      kind: "fence",
      steps: ["token 41", "freeze", "token 42", "stale return", "reject"],
      target: ".stage-card",
      action: "Freeze worker A",
      actionType: "step",
      actionTarget: "[data-freeze-btn]"
    },
    "architecture-comparison": {
      code: "AR-01",
      title: "Four ways to handle a crash",
      summary: "Compare a script, a queue, a database approach, and a workflow when the same failure happens.",
      outcome: "Watch what each approach remembers and how work starts again.",
      kind: "compare",
      steps: ["memory", "queue", "database", "workflow"],
      target: ".stage-card",
      action: "Test a server crash",
      actionType: "transient",
      actionTarget: "[data-smash-crash]"
    },
    "benchmark-explainer": {
      code: "AR-02",
      title: "How fast can workflows run?",
      summary: "Understand the 54K and 104K results from two different tests, with each test's limits beside it.",
      outcome: "Watch what each test measures, which machines it uses, and what the numbers can tell you.",
      kind: "throughput",
      steps: ["claim", "handler", "transition", "next lease", "complete"],
      target: "#workflow-modes",
      action: "Show fused results",
      staticSelector: true,
      actionTarget: "[data-mode-button='fused']"
    },
    "hot-cold-storage": {
      code: "DS-01",
      title: "Move data between memory and disk",
      summary: "See how data moves to disk when memory fills up, and comes back when needed.",
      outcome: "Watch the same data stay available as its storage location changes.",
      kind: "tiers",
      steps: ["hot write", "pressure", "evict", "cold read", "promote"],
      target: ".demo-workspace",
      action: "Trigger memory pressure",
      actionTarget: "[data-btn-pressure]"
    },
    "rate-limiting-stream": {
      code: "QS-01",
      title: "Handle a sudden rush of jobs",
      summary: "Keep incoming jobs waiting safely, then process them in small groups at a set pace.",
      outcome: "Watch how many jobs are waiting and how quickly they are handled.",
      kind: "buffer",
      steps: ["5,000 events", "stream", "rate gate", "batch", "workers"],
      target: ".stage-card",
      action: "Set the burst to 5,000",
      actionType: "transient",
      actionTarget: "[data-burst-btn]"
    },
    "beginner-queue": {
      code: "QS-02",
      title: "How a job queue works",
      summary: "Add a job to a waiting list, pick it up, and try it again if something goes wrong.",
      outcome: "Watch who is doing the job and when a replacement can take over.",
      kind: "handoff",
      steps: ["queued", "claimed", "worker crash", "lease ends", "reclaimed"],
      target: ".demo-workspace",
      action: "Run a clean job",
      actionTarget: "[data-btn-clean]"
    },
    "cache-stampede": {
      code: "DS-02",
      title: "Share one result with many requests",
      summary: "When many people ask for the same missing result, calculate it once and share it.",
      outcome: "Watch one task calculate the result while the other requests wait.",
      kind: "converge",
      steps: ["10k callers", "expired key", "one owner", "recompute", "shared result"],
      target: ".demo-workspace",
      action: "Trigger the stampede",
      actionTarget: "[data-btn-stampede]"
    },
    "stream-vs-pubsub": {
      code: "QS-03",
      title: "What happens to missed messages?",
      summary: "Compare messages saved for later with live messages that reach only connected listeners.",
      outcome: "Watch which messages a listener can catch up on after reconnecting.",
      kind: "split",
      steps: ["publish", "broadcast", "offline", "replay", "ack"],
      target: ".demo-workspace",
      action: "Send an example message",
      actionType: "transient",
      actionTarget: "[data-btn-action-1]"
    },
    "hash-field-ttl": {
      code: "DS-03",
      title: "Let parts of a record expire",
      summary: "Set different time limits for pieces of information in the same record.",
      outcome: "Watch one piece expire while the rest of the record stays available.",
      kind: "expiry",
      steps: ["profile", "2FA · 5s", "cart · 10s", "field expires", "hash remains"],
      target: ".demo-workspace",
      action: "Set a 5-second login code",
      actionTarget: "[data-btn-set-2fa]"
    },
    "probabilistic-cache": {
      code: "DS-04",
      title: "Skip searches that cannot match",
      summary: "Use a quick check to rule out missing items before searching the main data store.",
      outcome: "Watch a definite no skip the search; a possible yes still needs checking.",
      kind: "filter",
      steps: ["request", "filter", "definite miss", "possible hit", "origin"],
      target: ".demo-workspace",
      action: "Check a real key",
      actionType: "transient",
      actionTarget: "[data-btn-valid]"
    }
  };

  var routeSignals = {
    "workflow-explainer": { step: "[data-station].is-active", attr: "data-station", count: 5, status: "[data-live-status-text], [data-narrative-title]", complete: "Finished" },
    "ai-agent-workflow": { step: "[data-agent-node].is-active", attr: "data-agent-node", count: 5, status: "[data-live-status], [data-narrative-title]", complete: "Finished" },
    "travel-saga": { step: "[data-saga-node].is-active", attr: "data-saga-node", count: 5, status: "[data-live-status], [data-narrative-title]", complete: "Finished" },
    "subscription-dunning": { step: "[data-tm-node].is-active", attr: "data-tm-node", count: 5, status: "[data-live-status], [data-narrative-title]", complete: "Finished" },
    "ticket-reservation": { step: "[data-step-indicator].is-active", attr: "data-step-indicator", base: 1, count: 3, status: "[data-live-status]", complete: "FENCED SEAT HANDOFF COMPLETE|DOUBLE BOOKING DISASTER" },
    "canary-rollback": { step: "[data-step-indicator].is-active", attr: "data-step-indicator", base: 1, count: 3, status: "[data-live-status]", complete: "5XX ERRORS \\(CANARY BROKEN IN PROD\\)|ROLLBACK STATE COMPLETE|ROLLBACK EFFECT COMPLETED" },
    "split-lab": { step: "[data-rank].is-active", attr: "data-rank", count: 6, status: "[data-status], [data-message]", complete: "COMPLETED" },
    "zombie-fencing": { step: "[data-stepper] li.is-active", attr: "data-step", count: 4, status: "[data-live-status], [data-current-run-step]" },
    "architecture-comparison": { step: "[data-step-node].is-active", attr: "data-step-node", count: 4, status: "[data-live-status-text], [data-narrative-title]" },
    "agent-loop": { status: "[data-live-status], [data-turns-counter]", number: "Turn\\s+(\\d+)", base: 0, count: 5, map: [["SUCCESS|COMPLETE", 4], ["CIRCUIT BREAKER|BUDGET CAP", 3], ["RUNAWAY|SLAMMING", 2]], complete: "SUCCESS: BRIEFING COMPLETE|BUDGET CAP HALT|CIRCUIT BREAKER TRIPPED" },
    "beginner-queue": { status: "[data-live-status]", number: "(?:^|\\s)([123])\\.", base: 1, count: 3, map: [["RECLAIMED", 3], ["SAFE COMPLETION|COMPLETED", 4], ["CRASHED", 2]], complete: "COMPLETED DURABLY|SAFE COMPLETION|RECLAIMED BY WORKER" },
    "cache-stampede": { status: "[data-live-status]", map: [["WAITER RELEASE|COMPLETE", 4], ["FETCH_OR_COMPUTE", 3], ["10,000 QUERIES", 1], ["WARM CACHE", 0]], complete: "WAITER RELEASE COMPLETE|10,000 QUERIES CRASHING POSTGRES" },
    "parallel-fanout": { status: "[data-live-status]", map: [["JOB COMPLETE|COMPLETED", 4], ["RECOVERING|GATHERING", 3], ["WORKER #9 CRASH", 2], ["FANNING OUT", 1], ["READY", 0]], complete: "JOB COMPLETE|COMPLETED WITH 15X WASTE" },
    "hot-cold-storage": { status: "[data-live-status]", map: [["COLD READ", 4], ["SAFE", 3], ["DEMOTING", 2], ["OOM|EVICTION", 1], ["HOT KEY|NORMAL", 0]], complete: "SAFE \\(0 KEYS DELETED\\)|OOM / EVICTION SPIKE" },
    "idempotency-determinism": { status: "[data-term-status], [data-left-outcome], [data-right-outcome]", map: [["SIMULATION COMPLETE", 4], ["RETRY IN PROGRESS|REPLACEMENT WORKER", 2], ["CRASH DETECTED|WORKER DIED", 1], ["EXECUTING|IN-FLIGHT", 0], ["READY|WAITING", 0]], complete: "SIMULATION COMPLETE" },
    "hash-field-ttl": { status: "[data-live-status], [data-status-2fa], [data-status-cart]", actionIndex: 1, map: [["PURGED", 3]], complete: "PURGED" },
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
    return '<svg viewBox="0 0 32 32" aria-hidden="true"><path d="M5 6h21v5H11v4h12v5H11v7H5z" fill="currentColor"/><path d="M22 15h5v12h-5z" fill="#c9e84c"/></svg>';
  }

  function makeNav(config) {
    var nav = document.createElement("nav");
    nav.className = "fs-eval-nav";
    nav.setAttribute("aria-label", "Demo menu");
    nav.innerHTML =
      '<a class="fs-brand" href="../">' + svgLogo() + '<span><strong>FerricStore</strong><small>Demo catalog</small></span></a>' +
      '<span class="fs-route-name"><b>' + config.code + '</b><span>' + config.title + '</span></span>' +
      '<span class="fs-nav-links"><a href="../">Browse demos</a><a href="https://github.com/ferricstore/ferricstore#readme">Docs</a><a href="https://github.com/ferricstore/ferricstore">Source</a></span>';
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
    figure.innerHTML = '<figcaption><span>What happens</span><strong>Follow each state as the scenario changes.</strong></figcaption><ol>' + cells + '</ol><p class="fs-scroll-hint">Scroll the state rail to inspect every step.</p>';
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
    var explanation = config.summary;
    intro.innerHTML =
      '<div class="fs-intro-copy"><h1>' + config.title + '</h1><p class="fs-explanation">' + explanation + '</p></div>' +
      '<div class="fs-run-group"><button type="button" class="fs-primary-run">' + config.action + '</button>' +
      '<p class="fs-run-status" aria-live="polite">Ready.</p>' +
      '<p class="fs-simulation-note">' + (config.staticSelector ? "Recorded results, not a live speed test." : "Interactive example, not a live server.") + '</p></div>' +
      '<p class="fs-outcome"><strong>What to watch for</strong><span>' + config.outcome + '</span></p>' +
      '<a class="fs-evidence-link" href="#evaluation-evidence">See evidence and limits</a>';
    if (controls) {
      controls.classList.add("fs-primary-controls");
      var choiceHint = document.createElement("p");
      choiceHint.className = "fs-choice-hint";
      choiceHint.textContent = config.staticSelector ? "Choose a test" : "1. Choose a scenario";
      intro.insertBefore(choiceHint, intro.querySelector(".fs-run-group"));
      intro.insertBefore(controls, intro.querySelector(".fs-run-group"));
      if (!config.staticSelector) intro.querySelector(".fs-run-group").insertAdjacentHTML("afterbegin", '<span class="fs-action-hint">2. Run it</span>');
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
    if (/evidence-note|mode-boundary/i.test(cls)) return "Evidence and limits";
    if (/matrix|comparison|architecture|use-case/i.test(cls + " " + text)) return text.trim() || "Compare options";
    if (/code|sdk/i.test(cls) || /sdk|implementation/i.test(text)) return "See implementation";
    if (/catastrophe|pain|failure/i.test(cls + " " + text)) return "See the failure";
    if (/source|reference/i.test(cls + " " + text)) return "Sources";
    if (/faq|question/i.test(cls + " " + text)) return "Questions and limits";
    return text.trim() || "More detail";
  }

  function wrapDisclosure(node, label) {
    if (!node || node.closest("details.fs-disclosure")) return null;
    var details = document.createElement("details");
    details.className = "fs-disclosure";
    var summary = document.createElement("summary");
    summary.innerHTML = '<span>' + label + '</span><small>Show</small>';
    node.before(details);
    details.append(summary, node);
    return details;
  }

  function enhanceSecondary(main, firstView, hero) {
    var metrics = main.querySelector(":scope > .metrics-bar");
    if (metrics) {
      metrics.classList.add("fs-evidence-strip");
      firstView.after(metrics);
      wrapDisclosure(metrics, "Inspect example metrics");
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
        metrics ? metrics.closest("details").after(accuracyDetails) : firstView.after(accuracyDetails);
      }
    }

    var directAccuracy = document.querySelector(".demo-accuracy");
    if (directAccuracy) {
      directAccuracy.classList.add("fs-accuracy-disclosure");
      directAccuracy.id = "evaluation-evidence";
    }

    var destination = directAccuracy || document.querySelector(".fs-accuracy-disclosure") || main.querySelector(".fs-disclosure");
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
      function syncTabOrder() {
        Array.prototype.slice.call(tablist.querySelectorAll('[role="tab"]')).forEach(function (tab) {
          tab.tabIndex = tab.getAttribute("aria-selected") === "true" ? 0 : -1;
        });
      }
      syncTabOrder();
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
        syncTabOrder();
      });
      tablist.addEventListener("click", function () { window.setTimeout(syncTabOrder, 0); });
    });
  }

  function installDisclosureKeyboard() {
    Array.prototype.slice.call(document.querySelectorAll("details > summary")).forEach(function (summary) {
      if (summary.dataset.fsKeyboardReady) return;
      summary.dataset.fsKeyboardReady = "true";
      summary.addEventListener("keydown", function (event) {
        if ((event.key !== "Enter" && event.key !== " ") || event.repeat) return;
        event.preventDefault();
        summary.click();
      });
    });
  }

  function normalizeLiveRegions(scope) {
    // The shared run status is the single announcement channel. Route-local
    // labels still update visually, but mirrored live regions no longer make
    // screen readers repeat the same transition several times. Keep this
    // scoped to the live experiment; page-level disclosures may have their own
    // announcement semantics.
    if (!scope) return;
    Array.prototype.slice.call(scope.querySelectorAll("[aria-live]")).filter(function (node) {
      return !node.classList.contains("fs-run-status");
    }).forEach(function (node) {
      node.removeAttribute("aria-live");
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

    var controlSelector = ".mode-container, .mode-toggle-wrap, #workflow-controls, .mode-switch, .paradigm-selector-grid, .scenario-selector-bar";
    var controls = hero && hero.querySelector(controlSelector);
    if (!controls) controls = document.querySelector(controlSelector);
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

    if (hero) {
      // The shared intro owns the page-level heading. Demote the legacy hero
      // heading before retiring it so assistive technology sees one h1.
      Array.prototype.slice.call(hero.querySelectorAll("h1")).forEach(function (heading) {
        var replacement = document.createElement("h2");
        replacement.className = heading.className;
        replacement.innerHTML = heading.innerHTML;
        heading.replaceWith(replacement);
      });
      hero.classList.add("fs-retired-hero");
    }

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
    var hasInteracted = false;
    var crashTimer = null;

    function stopAutoCrash() {
      if (crashTimer !== null) window.clearInterval(crashTimer);
      crashTimer = null;
      target.dataset.fsAutoCrash = "false";
    }

    function initialRunStatus() {
      return config.staticSelector
        ? "Choose a test to see its results."
        : "Ready to run.";
    }

    function selectedModeLabel() {
      var selected = document.querySelector('[data-mode-button][aria-pressed="true"], [data-mode-button][aria-selected="true"]');
      return selected ? selected.textContent.trim().replace(/\s+/g, " ") : "";
    }

    function syncStaticSelector() {
      if (!config.staticSelector) return;
      var selected = selectedModeLabel();
      var fused = document.querySelector('[data-mode-button="fused"]');
      var isFused = fused && (fused.getAttribute("aria-selected") === "true" || fused.getAttribute("aria-pressed") === "true");
      proxy.disabled = isFused;
      proxy.textContent = isFused ? "Fused results shown" : config.action;
      var staticStatus = intro.querySelector(".fs-run-status");
      staticStatus.dataset.state = "selection";
      staticStatus.textContent = selected
        ? "Showing " + selected + "."
        : initialRunStatus();
    }

    function syncProxyState() {
      if (config.staticSelector) {
        syncStaticSelector();
        return;
      }
      // Most route buttons restart a story. Never call that action "Continue"
      // or infer completion from words such as "saved" in an intermediate step.
      proxy.textContent = hasInteracted && config.actionTarget === "[data-replay]" ? "Replay from the start" : config.action;
    }

    function syncRunStatus(live) {
      var status = intro.querySelector(".fs-run-status");
      if (!status || config.staticSelector) return;
      var selected = selectedSetup();
      var text = hasInteracted && live ? live : (selected ? selected + " · Ready to run." : initialRunStatus());
      if (status.textContent !== text) status.textContent = text;
      syncProxyState();
    }

    function centerSignatureStep(step) {
      var rail = signature.querySelector("ol");
      if (!rail || !step || rail.scrollWidth <= rail.clientWidth) return;
      var targetLeft = step.offsetLeft - (rail.clientWidth - step.offsetWidth) / 2;
      var maxLeft = rail.scrollWidth - rail.clientWidth;
      rail.scrollTo({
        left: Math.max(0, Math.min(maxLeft, targetLeft)),
        behavior: window.matchMedia("(prefers-reduced-motion: reduce)").matches ? "auto" : "smooth"
      });
    }

    function syncWorkflowSignature(normalized) {
      if (id !== "workflow-explainer") return;
      var selectedMode = document.querySelector('[data-mode][aria-selected="true"]');
      var isDurable = Boolean(selectedMode && selectedMode.dataset.mode === "after");
      var labels = isDurable
        ? ["charge", "stock", "crash", "resume", "complete"]
        : ["charge", "stock", "crash", "restart", "penalty"];
      signature.dataset.fsWorkflowMode = isDurable ? "after" : "before";
      signature.setAttribute("aria-label", "Mechanism map: " + labels.join(", "));
      signatureSteps.forEach(function (step, stepIndex) {
        step.classList.remove("is-unsafe", "is-risk", "is-failure", "is-committed", "is-success");
        var label = step.querySelector("strong");
        if (label) label.textContent = labels[stepIndex];
        if (stepIndex < normalized) step.classList.add(isDurable ? "is-committed" : "is-unsafe");
        if (stepIndex === normalized) {
          step.classList.add(isDurable ? "is-success" : (normalized < 2 ? "is-risk" : "is-failure"));
          step.setAttribute("aria-current", "step");
        } else {
          step.removeAttribute("aria-current");
        }
      });
    }

    function splitLabMode() {
      var selected = document.querySelector('[data-mode][aria-pressed="true"]');
      return selected && selected.dataset.mode === "durable" ? "durable" : "unmanaged";
    }

    function syncSplitLabSignature(normalized) {
      if (id !== "split-lab") return;
      var durable = splitLabMode() === "durable";
      var labels = durable
        ? ["plan", "search", "summarize", "crash", "resume", "finish"]
        : ["plan", "search", "summarize", "crash", "restart", "repeat + finish"];
      var explanation = "Compare both paths after the same crash.";
      var caption = signature.querySelector("figcaption strong");

      signature.dataset.fsSplitMode = durable ? "durable" : "unmanaged";
      signature.setAttribute("aria-label", "Mechanism map: " + labels.join(", "));
      if (caption) caption.textContent = explanation;

      signatureSteps.forEach(function (step, stepIndex) {
        var label = step.querySelector("strong");
        if (label) label.textContent = labels[stepIndex];
        step.classList.remove("is-volatile", "is-repeat", "is-failure", "is-committed", "is-success");

        if (stepIndex < normalized) {
          if (stepIndex === 3) step.classList.add("is-failure");
          else if (stepIndex >= 4) step.classList.add(durable ? "is-success" : "is-repeat");
          else step.classList.add(durable ? "is-committed" : "is-volatile");
        }

        if (stepIndex === normalized) {
          if (stepIndex === 3) step.classList.add("is-failure");
          else if (stepIndex >= 4) step.classList.add(durable ? "is-success" : "is-repeat");
          else step.classList.add(durable ? "is-committed" : "is-volatile");
          step.setAttribute("aria-current", "step");
        } else {
          step.removeAttribute("aria-current");
        }
      });
    }

    function syncSplitLabFrame() {
      if (id !== "split-lab") return;
      var durable = splitLabMode() === "durable";
      var actionName = durable ? "saved-progress path" : "restart path";
      var outcome = intro.querySelector(".fs-outcome span");
      var runLabel = document.querySelector("[data-run-label]");
      var sourceLabel = runLabel ? runLabel.textContent.trim() : "Run workflow";
      var busy = /pause|waiting|crash original worker/i.test(sourceLabel);

      if (outcome) {
        outcome.textContent = !hasInteracted ? config.outcome : (durable
          ? "Watch Worker B resume at Summarize with the saved Plan and Search steps."
          : "Watch Worker B restart at Plan after the crash and repeat finished work.");
      }

      if (/run.*again/i.test(sourceLabel)) proxy.textContent = "Run " + actionName + " again";
      else if (/resume|continue/i.test(sourceLabel)) proxy.textContent = "Continue " + actionName;
      else if (busy) proxy.textContent = "Running " + actionName + "…";
      else proxy.textContent = "Run " + actionName;
      proxy.disabled = busy;
      syncSplitLabSignature(lastSignatureIndex);
    }

    function setSignatureIndex(index) {
      if (!Number.isFinite(index)) return;
      var normalized = Math.max(0, Math.min(signatureSteps.length - 1, Math.round(index)));
      lastSignatureIndex = normalized;
      signatureSteps.forEach(function (step, stepIndex) {
        step.classList.toggle("is-current", stepIndex === normalized);
        step.classList.toggle("is-done", stepIndex < normalized);
      });
      syncWorkflowSignature(normalized);
      syncSplitLabSignature(normalized);
      signature.dataset.fsCurrentStep = String(normalized + 1);
      if (id === "workflow-explainer" || id === "split-lab") {
        window.requestAnimationFrame(function () { centerSignatureStep(signatureSteps[normalized]); });
      }
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
      var explicitStatus = target.querySelector('[data-live-status-text], [data-live-status], [data-status]');
      if (explicitStatus && explicitStatus.textContent.trim()) return explicitStatus.textContent.trim().replace(/\s+/g, " ");
      var candidates = Array.prototype.slice.call(document.querySelectorAll(routeSignal.status || '[data-message], [data-workflow-hint], [data-narrative-title], [data-outcome-title], [data-term-status], [data-current-run-value]'));
      var live = candidates.find(function (node) {
        return node.textContent.trim() && window.getComputedStyle(node).display !== "none";
      });
      return live ? live.textContent.trim().replace(/\s+/g, " ").slice(0, 140) : "";
    }

    function syncExperiment() {
      var stepIndex = liveStepIndex();
      if (Number.isFinite(stepIndex)) setSignatureIndex(stepIndex);
      var activeStep = routeSignal.step ? document.querySelector(routeSignal.step) : null;
      if (activeStep) {
        Array.prototype.slice.call(activeStep.parentElement ? activeStep.parentElement.children : []).forEach(function (peer) {
          peer.removeAttribute("aria-current");
        });
        activeStep.setAttribute("aria-current", "step");
      }
      var original = findPrimaryButton(config);
      var live = liveStatusText();
      if (config.staticSelector) syncStaticSelector();
      else {
        proxy.disabled = !original || original.disabled;
        syncRunStatus(live);
      }
      syncSplitLabFrame();
    }

    proxy.addEventListener("click", function () {
      var original = findPrimaryButton(config);
      var status = intro.querySelector(".fs-run-status");
      if (!original || original.disabled) {
        status.textContent = "Choose an available action in the experiment below.";
        if (id !== "split-lab") target.scrollIntoView({ behavior: "smooth", block: "start" });
        return;
      }
      if (config.staticSelector) {
        original.click();
        syncStaticSelector();
        target.scrollIntoView({ behavior: "smooth", block: "start" });
        return;
      }
      hasInteracted = true;
      original.click();
      syncRunStatus(liveStatusText());
      syncProxyState();
      if (Number.isFinite(routeSignal.actionIndex)) setSignatureIndex(routeSignal.actionIndex);
      if (id === "split-lab") {
        stopAutoCrash();
        target.dataset.fsAutoCrash = "true";
        var attempts = 0;
        crashTimer = window.setInterval(function () {
          attempts += 1;
          var crash = document.querySelector('[data-action="crash"]');
          if (crash && !crash.disabled) {
            stopAutoCrash();
            crash.click();
          } else if (attempts > 100) {
            stopAutoCrash();
          }
        }, 100);
      }
      window.setTimeout(syncExperiment, 0);
      window.setTimeout(syncExperiment, 180);
      window.setTimeout(syncExperiment, 700);
      if (id === "split-lab" && window.matchMedia("(max-width: 780px)").matches) {
        var verdict = target.querySelector(".lab-verdict");
        if (verdict) {
          window.setTimeout(function () {
            verdict.scrollIntoView({
              behavior: window.matchMedia("(prefers-reduced-motion: reduce)").matches ? "auto" : "smooth",
              block: "start"
            });
          }, 120);
        }
      } else if (id !== "split-lab") {
        target.scrollIntoView({ behavior: "smooth", block: "nearest" });
      }
    });

    function selectedSetup() {
      var roots = [intro.querySelector(".fs-primary-controls"), target].filter(Boolean);
      var selected = null;
      var selector = '[data-scenario][aria-selected="true"], [data-mode][aria-selected="true"], [data-mode][aria-pressed="true"], [data-mode-btn][aria-selected="true"], [data-mode-btn][aria-pressed="true"], [data-mode-button][aria-selected="true"], [data-mode-button][aria-pressed="true"], [data-arch][aria-selected="true"], [data-arch][aria-pressed="true"], .scenario-choice[aria-pressed="true"]';
      roots.some(function (root) {
        selected = root.querySelector(selector);
        return Boolean(selected);
      });
      if (!selected) {
        selected = target.querySelector("[data-current-run-label]");
      }
      if (!selected) return "";
      var strong = selected.querySelector(".mode-tab-title") || selected.querySelector("strong") || selected.querySelector(":scope > span:first-child");
      return (strong ? strong.textContent : selected.textContent).trim().replace(/\s+/g, " ").slice(0, 72);
    }

    function updateReadyStatus() {
      if (config.staticSelector) {
        syncStaticSelector();
        return;
      }
      hasInteracted = false;
      syncRunStatus("");
      syncSplitLabFrame();
    }

    document.addEventListener("click", function (event) {
      var button = event.target.closest('button, [role="button"], [role="tab"]');
      if (!button) return;
      var controlRoot = intro.querySelector(".fs-primary-controls");
      var modeChoice = controlRoot && controlRoot.contains(button) && button.matches('[aria-selected], [aria-pressed]');
      var scenarioChoice = target.contains(button) && button.matches('.scenario-choice');
      var reset = target.contains(button) && button.matches('[data-btn-reset], [data-reset-btn], [data-action="reset"]');
      if (modeChoice || scenarioChoice || reset) {
        stopAutoCrash();
        window.setTimeout(function () {
          if (id === "split-lab" && modeChoice) setSignatureIndex(0);
          updateReadyStatus();
        }, 0);
      } else if (target.contains(button) || (controlRoot && controlRoot.contains(button))) {
        hasInteracted = true;
        window.setTimeout(syncExperiment, 0);
      }
    });

    target.addEventListener("input", function () {
      hasInteracted = true;
      window.setTimeout(syncExperiment, 0);
    });

    window.setTimeout(function () {
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
        // Code is supporting evidence; opening it must not silently change
        // the selected scenario or comparison mode.
        codeButton.textContent = open ? "Hide code" : "Inspect code";
        codeButton.setAttribute("aria-expanded", String(open));
      });
      intro.querySelector(".fs-run-group").appendChild(codeButton);
    }

    var experimentObserver = new MutationObserver(function () {
      normalizeLiveRegions(target);
      if (id === "split-lab" && target.dataset.fsAutoCrash === "true") {
        var crash = document.querySelector('[data-action="crash"]');
        if (crash && !crash.disabled) {
          stopAutoCrash();
          crash.click();
        }
      }
      window.requestAnimationFrame(syncExperiment);
    });
    experimentObserver.observe(target, { attributes: true, childList: true, characterData: true, subtree: true, attributeFilter: ["class", "aria-selected", "aria-pressed", "aria-live", "disabled"] });
    Array.prototype.slice.call(document.querySelectorAll(routeSignal.status || "")).forEach(function (statusNode) {
      if (!target.contains(statusNode)) experimentObserver.observe(statusNode, { attributes: true, childList: true, characterData: true, subtree: true });
    });

    if (id === "benchmark-explainer") {
      wrapDisclosure(target.querySelector(".fused-proof"), "Inspect the measured fused profiles");
    }

    enhanceSecondary(main, firstView, hero);
    normalizeLiveRegions(target);
    normalizeLiveRegions(controls);
    installDisclosureKeyboard();
    installTabKeyboard();
  }

  function enhanceCatalog() {
    document.body.dataset.fsSurface = "catalog";
    var hero = document.querySelector(".hero");
    var catalog = document.querySelector(".catalog");
    if (!hero || !catalog) return;
    // The product homepage supplies its own introduction and evaluation links.
    if (document.body.classList.contains("fs-home")) return;

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
