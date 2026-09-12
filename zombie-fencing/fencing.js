(function () {
  "use strict";

  var currentStep = 0;
  var isPaused = true;
  var timer = null;

  var states = [
    {
      stepIndex: 0,
      stageName: "Stage 1: Worker A is active (owner number 1)",
      workerARole: "Current owner (number 1)",
      workerADesc: "Worker A owns Job #9842. It is active and responding.",
      workerAStatus: "STATUS: ACTIVE (OWNER 1)",
      workerAFrozen: false,
      workerARejected: false,
      workerBRole: "Waiting to take over",
      workerBDesc: "Worker B is waiting in the worker pool.",
      workerBStatus: "STATUS: WAITING",
      workerBActive: false,
      vaultLease: "CURRENT OWNER: 1",
      vaultDecision: "WRITES FROM OWNER 1 ACCEPTED",
      valGen: "Owner 1",
      valBlocked: "0 old writes",
      valCorrupt: "App must pass owner"
    },
    {
      stepIndex: 1,
      stageName: "Stage 2: Worker A pauses and stops responding",
      workerARole: "Paused (owner number 1)",
      workerADesc: "Worker A is frozen and no longer sends its heartbeat.",
      workerAStatus: "STATUS: PAUSED",
      workerAFrozen: true,
      workerARejected: false,
      workerBRole: "Notices the missing heartbeat",
      workerBDesc: "Worker B notices that Worker A is not responding and prepares to take over.",
      workerBStatus: "STATUS: WAITING FOR TAKEOVER",
      workerBActive: false,
      vaultLease: "OWNER 1 NO LONGER RESPONDING",
      vaultDecision: "PREPARING A NEW OWNER NUMBER",
      valGen: "Owner 1",
      valBlocked: "0 old writes",
      valCorrupt: "App must pass owner"
    },
    {
      stepIndex: 2,
      stageName: "Stage 3: Worker B takes over with owner number 2",
      workerARole: "Paused / old owner",
      workerADesc: "Worker A is still paused. Its owner number 1 is no longer current.",
      workerAStatus: "STATUS: PAUSED (OLD OWNER)",
      workerAFrozen: true,
      workerARejected: false,
      workerBRole: "Current owner (number 2)",
      workerBDesc: "Worker B takes over with owner number 2 and saves the step result.",
      workerBStatus: "STATUS: SAVED (OWNER 2)",
      workerBActive: true,
      vaultLease: "CURRENT OWNER: 2",
      vaultDecision: "OWNER 2 WRITE ACCEPTED",
      valGen: "Owner 2",
      valBlocked: "0 old writes",
      valCorrupt: "App must pass owner"
    },
    {
      stepIndex: 3,
      stageName: "Stage 4: Worker A returns and tries the old write",
      workerARole: "Old owner (number 1)",
      workerADesc: "Worker A returns and tries to write with its old owner number 1.",
      workerAStatus: "TRYING OLD WRITE (OWNER 1)…",
      workerAFrozen: false,
      workerARejected: true,
      workerBRole: "Safe current owner (number 2)",
      workerBDesc: "Worker B already saved the job with owner number 2.",
      workerBStatus: "STATUS: SAVED (OWNER 2)",
      workerBActive: true,
      vaultLease: "CURRENT OWNER: 2",
      vaultDecision: "REJECTED: owner 1 is older than owner 2.<br><strong style='color: #34d399;'>Old worker write blocked</strong>",
      valGen: "Owner 2",
      valBlocked: "1 old write blocked",
      valCorrupt: "App must check owner"
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
  var workerAToken = document.querySelector("[data-worker-a-token]");
  var workerBToken = document.querySelector("[data-worker-b-token]");

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
  var currentRunStep = document.querySelector("[data-current-run-step]");

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
    if (workerAToken) workerAToken.textContent = "OWNER: 1" + (currentStep >= 2 ? " (OLD)" : "");
    if (workerBToken) workerBToken.textContent = currentStep >= 2 ? "OWNER: 2 (CURRENT)" : "OWNER: WAITING";

    if (vaultLease) vaultLease.textContent = data.vaultLease;
    if (vaultDecision) vaultDecision.innerHTML = data.vaultDecision;

    if (valGen) valGen.textContent = data.valGen;
    if (valBlocked) valBlocked.textContent = data.valBlocked;
    if (valCorrupt) valCorrupt.textContent = data.valCorrupt;
    if (valLatency) valLatency.textContent = "Depends on lease";

    if (currentRunStep) {
      currentRunStep.textContent = "Step " + (currentStep + 1) + " of " + states.length + " · " + data.stageName.replace(/^Stage \d+:\s*/, "");
    }

    if (pauseBtn) pauseBtn.textContent = isPaused ? "▶ Play steps" : "⏸ Pause";
  }

  stepperItems.forEach(function (el) {
    el.setAttribute("role", "button");
    el.tabIndex = 0;
    el.addEventListener("keydown", function (event) {
      if (event.key === "Enter" || event.key === " ") {
        event.preventDefault();
        el.click();
      }
    });
    el.addEventListener("click", function () {
      var stepIdx = parseInt(el.getAttribute("data-step"), 10);
      if (!isNaN(stepIdx)) {
        clearTimer();
        currentStep = stepIdx;
        isPaused = true;
        render();
      }
    });
  });

  if (freezeBtn) {
    freezeBtn.addEventListener("click", function () {
      clearTimer();
      currentStep = 1;
      isPaused = true;
      render();
    });
  }

  if (promoteBtn) {
    promoteBtn.addEventListener("click", function () {
      clearTimer();
      currentStep = 2;
      isPaused = true;
      render();
    });
  }

  if (zombieBtn) {
    zombieBtn.addEventListener("click", function () {
      clearTimer();
      currentStep = 3;
      isPaused = true;
      render();
    });
  }

  if (pauseBtn) {
    pauseBtn.addEventListener("click", function () {
      clearTimer();
      if (currentStep === states.length - 1 && isPaused) currentStep = 0;
      isPaused = !isPaused;
      render();
      schedule();
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

  function clearTimer() {
    if (timer !== null) {
      window.clearTimeout(timer);
      timer = null;
    }
  }

  function schedule() {
    clearTimer();
    if (isPaused || currentStep >= states.length - 1) return;
    timer = window.setTimeout(function () {
      currentStep += 1;
      if (currentStep >= states.length - 1) isPaused = true;
      render();
      schedule();
    }, 4000);
  }

  render();
})();
