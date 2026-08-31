(function () {
  "use strict";

  var currentStep = 3;
  var isPaused = false;
  var timer = null;

  var states = [
    {
      stepIndex: 0,
      stageName: "Stage 1: Normal Execution (Worker A holds Lease Gen=1)",
      workerARole: "Active Leader (Lease Gen 1)",
      workerADesc: "Worker A claims Job #9842 with Generation 1. Heartbeat is healthy and active.",
      workerAStatus: "STATUS: ACTIVE (GEN=1)",
      workerAFrozen: false,
      workerARejected: false,
      workerBRole: "Standby Replica",
      workerBDesc: "Worker B is on standby in the worker pool, awaiting work or failover.",
      workerBStatus: "STATUS: STANDBY",
      workerBActive: false,
      vaultLease: "ACTIVE LEASE: GENERATION 1",
      vaultDecision: "ALL WRITES FROM WORKER A (GEN=1) ACCEPTED",
      valGen: "Generation 1",
      valBlocked: "0 Stale Writes",
      valCorrupt: "External DB must enforce fence"
    },
    {
      stepIndex: 1,
      stageName: "Stage 2: 20-Second Garbage Collection Pause (Worker A Freezes)",
      workerARole: "Frozen in GC Pause (20s)",
      workerADesc: "JVM / Node GC pause hits Worker A. Process is completely frozen; heartbeat stops responding.",
      workerAStatus: "STATUS: FROZEN (GC PAUSE 20s)",
      workerAFrozen: true,
      workerARejected: false,
      workerBRole: "Detecting Heartbeat Timeout",
      workerBDesc: "Worker B detects missing heartbeat from Worker A. Prepares leader election failover.",
      workerBStatus: "STATUS: HEARTBEAT TIMEOUT DETECTED",
      workerBActive: false,
      vaultLease: "LEASE EXPIRING (HEARTBEAT MISSED)",
      vaultDecision: "SUPERVISOR PREPARING LEASE TRANSFER",
      valGen: "Generation 1",
      valBlocked: "0 Stale Writes",
      valCorrupt: "External DB must enforce fence"
    },
    {
      stepIndex: 2,
      stageName: "Stage 3: Lease Transferred to Worker B (Lease Promoted to Gen=2)",
      workerARole: "Frozen / Stale",
      workerADesc: "Worker A remains frozen in GC pause. Its Generation 1 lease has been revoked.",
      workerAStatus: "STATUS: FROZEN (STALE LEASE)",
      workerAFrozen: true,
      workerARejected: false,
      workerBRole: "Active Leader (Lease Gen 2)",
      workerBDesc: "Worker B is elected successor! Increments fencing token to Gen 2 and safely commits step output to Raft log.",
      workerBStatus: "STATUS: COMMITTED TO RAFT LOG (GEN=2)",
      workerBActive: true,
      vaultLease: "ACTIVE LEASE: GENERATION 2",
      vaultDecision: "WORKER B COMMITTED WITH GEN=2",
      valGen: "Generation 2",
      valBlocked: "0 Stale Writes",
      valCorrupt: "External DB must enforce fence"
    },
    {
      stepIndex: 3,
      stageName: "Stage 4: Zombie Worker A Attempts Stale Write -> Intercepted & Blocked!",
      workerARole: "Zombie Worker (Stale Gen 1)",
      workerADesc: "Worker A unfreezes! Unaware it was replaced, Worker A attempts to write stale data with Generation 1.",
      workerAStatus: "ATTEMPTING STALE WRITE (GEN=1)...",
      workerAFrozen: false,
      workerARejected: true,
      workerBRole: "Safe Committed Owner",
      workerBDesc: "Worker B already completed the job cleanly with Generation 2.",
      workerBStatus: "STATUS: COMMITTED TO DISK (GEN=2)",
      workerBActive: true,
      vaultLease: "ACTIVE LEASE: GENERATION 2",
      vaultDecision: "REJECTED: incoming gen (1) < storage gen (2)<br><strong style='color: #34d399;'>FENCING_TOKEN_STALE — Flow mutation blocked</strong>",
      valGen: "Generation 2",
      valBlocked: "1 Stale Write Blocked",
      valCorrupt: "Stale Flow mutation rejected"
    }
  ];

  var stageNameEl = document.querySelector("[data-stage-name]");
  var stepperItems = document.querySelectorAll("[data-stepper] li");

  var workerAPanel = document.querySelector("[data-worker-a-panel]");
  var workerARole = document.querySelector("[data-worker-a-role]");
  var workerADesc = document.querySelector("[data-worker-a-desc]");
  var workerAStatus = document.querySelector("[data-worker-a-status]");

  var workerBPanel = document.querySelector("[data-worker-b-panel]");
  var workerBRole = document.querySelector("[data-worker-b-role]");
  var workerBDesc = document.querySelector("[data-worker-b-desc]");
  var workerBStatus = document.querySelector("[data-worker-b-status]");

  var vaultLease = document.querySelector("[data-vault-lease]");
  var vaultDecision = document.querySelector("[data-gk-decision]");

  var valGen = document.querySelector("[data-val-gen]");
  var valBlocked = document.querySelector("[data-val-blocked]");
  var valCorrupt = document.querySelector("[data-val-corrupt]");
  var valLatency = document.querySelector("[data-val-latency]");

  var pauseBtn = document.querySelector("[data-pause]");
  var replayBtn = document.querySelector("[data-replay]");

  var freezeBtn = document.querySelector("[data-freeze-btn]");
  var promoteBtn = document.querySelector("[data-promote-btn]");
  var zombieBtn = document.querySelector("[data-zombie-btn]");

  function render() {
    var data = states[currentStep];

    if (stageNameEl) stageNameEl.textContent = data.stageName;

    stepperItems.forEach(function (el, idx) {
      el.classList.remove("is-active", "is-done", "is-crash");
      if (idx < currentStep) {
        el.classList.add("is-done");
      } else if (idx === currentStep) {
        el.classList.add("is-active");
        if (currentStep === 3) el.classList.add("is-crash");
      }
    });

    if (workerAPanel) {
      workerAPanel.classList.toggle("is-frozen", data.workerAFrozen);
      workerAPanel.classList.toggle("is-zombie-rejected", data.workerARejected);
      workerAPanel.classList.toggle("is-active-gen", currentStep === 0);
    }
    if (workerARole) workerARole.textContent = data.workerARole;
    if (workerADesc) workerADesc.textContent = data.workerADesc;
    if (workerAStatus) {
      workerAStatus.textContent = data.workerAStatus;
      workerAStatus.className = "wc-status-box" + (data.workerARejected ? "" : (currentStep === 0 ? " good" : ""));
    }

    if (workerBPanel) {
      workerBPanel.classList.toggle("is-active-gen", data.workerBActive);
    }
    if (workerBRole) workerBRole.textContent = data.workerBRole;
    if (workerBDesc) workerBDesc.textContent = data.workerBDesc;
    if (workerBStatus) {
      workerBStatus.textContent = data.workerBStatus;
      workerBStatus.className = "wc-status-box" + (data.workerBActive ? " good" : "");
    }

    if (vaultLease) vaultLease.textContent = data.vaultLease;
    if (vaultDecision) vaultDecision.innerHTML = data.vaultDecision;

    if (valGen) valGen.textContent = data.valGen;
    if (valBlocked) valBlocked.textContent = data.valBlocked;
    if (valCorrupt) valCorrupt.textContent = data.valCorrupt;
    if (valLatency) valLatency.textContent = "Lease-dependent";

    if (pauseBtn) pauseBtn.textContent = isPaused ? "Play" : "Pause";
  }

  stepperItems.forEach(function (el) {
    el.addEventListener("click", function () {
      var stepIdx = parseInt(el.getAttribute("data-step"), 10);
      if (!isNaN(stepIdx)) {
        currentStep = stepIdx;
        isPaused = true;
        render();
      }
    });
  });

  if (freezeBtn) {
    freezeBtn.addEventListener("click", function () {
      currentStep = 1;
      isPaused = true;
      render();
    });
  }

  if (promoteBtn) {
    promoteBtn.addEventListener("click", function () {
      currentStep = 2;
      isPaused = true;
      render();
    });
  }

  if (zombieBtn) {
    zombieBtn.addEventListener("click", function () {
      currentStep = 3;
      isPaused = true;
      render();
    });
  }

  if (pauseBtn) {
    pauseBtn.addEventListener("click", function () {
      isPaused = !isPaused;
      render();
    });
  }

  if (replayBtn) {
    replayBtn.addEventListener("click", function () {
      currentStep = 0;
      isPaused = false;
      render();
    });
  }

  timer = setInterval(function () {
    if (!isPaused) {
      currentStep = (currentStep + 1) % states.length;
      render();
    }
  }, 4000);

  render();
})();
