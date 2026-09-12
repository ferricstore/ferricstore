(function () {
  "use strict";

  var currentStep = 0;
  var isPaused = false;
  var hasStarted = false;
  var timer = null;

  var stepsData = [
  {
    "day": 1,
    "ttLabel": "Day 1: Trial starts (14-day access)",
    "badge": "TRIAL ACTIVE",
    "badgeClass": "trial",
    "narrativeBadge": "WAITING FOR DAY 14",
    "narrativeTitle": "Day 1: Trial starts",
    "narrativeDesc": "A 14-day trial starts. FerricStore records the next wake-up time, so no application process needs to sit and wait.",
    "code": "send_welcome()\nreturn transition('pre_billing_warning', run_at_ms=day_14_ms)",
    "acctBadge": "TRIAL ACTIVE (14 DAYS REMAINING)",
    "acctBadgeClass": "trial",
    "acctDesc": "The customer has full access during the trial. A payment method is on file for renewal.",
    "rev": "$0.00 (Trial)",
    "queries": "0 Queries",
    "drift": "0.00s",
    "grace": "14 Days",
    "activeNode": 0,
    "declined": false
  },
  {
    "day": 14,
    "ttLabel": "Day 14: Warning email",
    "badge": "TIMER EXPIRED",
    "badgeClass": "trial",
    "narrativeBadge": "WARNING READY",
    "narrativeTitle": "Day 14: Warning email",
    "narrativeDesc": "The warning becomes eligible on Day 14. A worker claims it when available and sends the notice.",
    "code": "send_warning()\nreturn transition('charge', run_at_ms=day_15_ms)",
    "acctBadge": "TRIAL ENDING IN 24H",
    "acctBadgeClass": "trial",
    "acctDesc": "Pre-billing warning sent to customer billing email.",
    "rev": "$0.00",
    "queries": "0 Queries",
    "drift": "0.00s",
    "grace": "24 Hours",
    "activeNode": 1,
    "declined": false
  },
  {
    "day": 15,
    "ttLabel": "Day 15: Payment attempt ($99/month)",
    "badge": "CARD DECLINED",
    "badgeClass": "pastdue",
    "narrativeBadge": "PAYMENT DECLINED",
    "narrativeTitle": "Day 15: Card declined",
    "narrativeDesc": "The card is declined for insufficient funds. The workflow enters a 7-day grace period and schedules retry #1 in 3 days.",
    "code": "attempt = charge_once()\nsend_dunning()\nreturn transition('retry_1', run_at_ms=day_18_ms)  # when declined",
    "acctBadge": "PAST DUE (DUNNING ACTIVE)",
    "acctBadgeClass": "pastdue",
    "acctDesc": "Payment declined. Account in 7-day grace period with active access.",
    "rev": "$0.00 (Pending)",
    "queries": "0 Queries",
    "drift": "0.00s",
    "grace": "7 Days Grace",
    "activeNode": 2,
    "declined": true
  },
  {
    "day": 18,
    "ttLabel": "Day 18: Retry #1 (SMS + email)",
    "badge": "DUNNING CADENCE 1",
    "badgeClass": "pastdue",
    "narrativeBadge": "RETRY #1 SCHEDULED",
    "narrativeTitle": "Day 18: Payment reminder",
    "narrativeDesc": "The workflow wakes on Day 18, sends an SMS with a card-update link, and schedules the final retry for Day 21.",
    "code": "attempt = charge_once()\nsend_update_link()\nreturn transition('retry_2', run_at_ms=day_21_ms)  # when declined",
    "acctBadge": "PAST DUE (RETRY #1)",
    "acctBadgeClass": "pastdue",
    "acctDesc": "Customer received SMS update link. 4 days of grace period remaining.",
    "rev": "$0.00 (Pending)",
    "queries": "0 Queries",
    "drift": "0.00s",
    "grace": "4 Days Left",
    "activeNode": 3,
    "declined": true
  },
  {
    "day": 21,
    "ttLabel": "Day 21: Payment recovered ($99)",
    "badge": "SUBSCRIPTION RECOVERED",
    "badgeClass": "active",
    "narrativeBadge": "PAYMENT RECOVERED",
    "narrativeTitle": "Day 21: $99 payment succeeds",
    "narrativeDesc": "The customer updates the card. The retry reuses a stable payment key and the workflow finishes durably.",
    "code": "payment = charge_once()\nreturn complete(result=payment)",
    "acctBadge": "ACTIVE SUBSCRIBER (PAID \u2713)",
    "acctBadgeClass": "active",
    "acctDesc": "Subscription renewed for next 30 days. Full team access preserved.",
    "rev": "+$99.00 (Recovered)",
    "queries": "0 Queries",
    "drift": "0.00s",
    "grace": "Renewed \u2713",
    "activeNode": 4,
    "declined": false
  }
];

  var slider = document.querySelector("[data-tt-slider]");
  var ttLabel = document.querySelector("[data-tt-label]");
  var nodes = document.querySelectorAll("[data-tm-node]");

  var acctBadge = document.querySelector("[data-account-badge]");
  var acctTitle = document.querySelector("[data-account-title]");
  var acctDesc = document.querySelector("[data-account-desc]");

  var narrativeBadge = document.querySelector("[data-narrative-badge]");
  var narrativeTitle = document.querySelector("[data-narrative-title]");
  var narrativeDesc = document.querySelector("[data-narrative-desc]");
  var narrativeCode = document.querySelector("[data-narrative-code]");

  var valRev = document.querySelector("[data-val-rev]");
  var valQueries = document.querySelector("[data-val-queries]");
  var valDrift = document.querySelector("[data-val-drift]");
  var valGrace = document.querySelector("[data-val-grace]");

  var prevBtn = document.querySelector("[data-prev]");
  var pauseBtn = document.querySelector("[data-pause]");
  var nextBtn = document.querySelector("[data-next]");
  var replayBtn = document.querySelector("[data-replay]");
  var liveStatus = document.querySelector("[data-live-status]");
  var currentRunStep = document.querySelector("[data-current-run-step]");

  var declinedToggle = document.querySelector("[data-declined-toggle]");
  var successToggle = document.querySelector("[data-success-toggle]");
  var codeExampleTabs = document.querySelectorAll("[data-code-example-tab]");
  var codeExamples = document.querySelectorAll("[data-code-example]");

  function render() {
    var data = stepsData[currentStep];

    if (slider) {
      slider.value = String(currentStep);
      slider.setAttribute("aria-valuetext", data.ttLabel);
    }
    if (ttLabel) ttLabel.textContent = data.ttLabel;

    nodes.forEach(function (node, idx) {
      var isDone = idx < currentStep;
      var isActive = idx === currentStep;
      node.classList.toggle("is-done", isDone);
      node.classList.toggle("is-active", isActive);
      node.classList.toggle("is-declined", data.declined && idx === 2);
      node.setAttribute("aria-pressed", String(isActive));
      if (isActive) node.setAttribute("aria-current", "step");
      else node.removeAttribute("aria-current");
      var nodeTitle = node.querySelector("strong");
      node.setAttribute("aria-label", (nodeTitle ? nodeTitle.textContent : "Billing milestone " + (idx + 1)) + ". " + (isActive ? "Active" : (isDone ? "Complete" : "Pending")));

      var pill = node.querySelector(".node-pill");
      if (pill) {
        if (isDone) pill.textContent = "✓ DONE";
        else if (isActive) pill.textContent = (idx === 2 && data.declined ? "DECLINED" : "ACTIVE");
        else pill.textContent = "PENDING";
      }
    });

    if (acctBadge) {
      acctBadge.textContent = data.acctBadge;
      acctBadge.className = "as-badge " + data.acctBadgeClass;
    }
    if (acctDesc) acctDesc.textContent = data.acctDesc;

    if (narrativeBadge) {
      narrativeBadge.textContent = data.narrativeBadge;
      narrativeBadge.className = "as-badge " + data.badgeClass;
    }
    if (narrativeTitle) narrativeTitle.textContent = data.narrativeTitle;
    if (narrativeDesc) narrativeDesc.textContent = data.narrativeDesc;
    if (narrativeCode) narrativeCode.textContent = data.code;

    if (valRev) valRev.textContent = data.rev;
    if (valQueries) valQueries.textContent = data.queries;
    if (valDrift) valDrift.textContent = data.drift;
    if (valGrace) valGrace.textContent = data.grace;

    if (currentRunStep) currentRunStep.textContent = (!hasStarted ? "Preview · " : "Step ") + (currentStep + 1) + " of " + stepsData.length + " · " + data.ttLabel;

    if (prevBtn) prevBtn.disabled = currentStep === 0;
    if (nextBtn) nextBtn.disabled = currentStep >= stepsData.length - 1;
    if (pauseBtn) {
      var finished = hasStarted && currentStep >= stepsData.length - 1 && isPaused;
      pauseBtn.textContent = finished ? "Finished" : (!hasStarted || isPaused ? "Play" : "Pause");
      pauseBtn.disabled = finished;
      pauseBtn.setAttribute("aria-label", finished ? "Billing schedule finished; choose Replay to run it again" : "Pause or play the billing schedule");
    }
    if (liveStatus) liveStatus.textContent = !hasStarted
      ? "Preview ready · choose Run to play"
      : (finished ? "Finished · choose Replay to run again" : (isPaused ? "Paused · choose Play to continue" : "Playing automatically · Pause to inspect")) + " · Step " + (currentStep + 1) + " of " + stepsData.length;
  }

  function clearTimer() {
    if (timer !== null) {
      window.clearTimeout(timer);
      timer = null;
    }
  }

  function schedule() {
    clearTimer();
    if (!hasStarted || isPaused || currentStep >= stepsData.length - 1) {
      if (hasStarted && currentStep >= stepsData.length - 1) {
        isPaused = true;
        render();
      }
      return;
    }
    var wait = (currentStep === 2) ? 4000 : 2800;
    timer = window.setTimeout(function () {
      currentStep += 1;
      render();
      schedule();
    }, wait);
  }

  if (slider) {
    slider.addEventListener("input", function () {
      currentStep = parseInt(slider.value, 10);
      hasStarted = true;
      isPaused = true;
      clearTimer();
      render();
      schedule();
    });
  }

  nodes.forEach(function (node, idx) {
    function selectNode() {
      currentStep = idx;
      hasStarted = true;
      isPaused = true;
      clearTimer();
      render();
    }
    node.addEventListener("click", selectNode);
    node.addEventListener("keydown", function (event) {
      if (event.key !== "Enter" && event.key !== " ") return;
      event.preventDefault();
      selectNode();
    });
  });

  if (declinedToggle) {
    declinedToggle.addEventListener("click", function () {
      currentStep = 2;
      hasStarted = true;
      isPaused = true;
      render();
      schedule();
    });
  }

  if (successToggle) {
    successToggle.addEventListener("click", function () {
      currentStep = 4;
      hasStarted = true;
      isPaused = true;
      render();
      schedule();
    });
  }

  if (pauseBtn) {
    pauseBtn.addEventListener("click", function () {
      if (!hasStarted) {
        hasStarted = true;
        isPaused = false;
      } else {
        isPaused = !isPaused;
      }
      render();
      schedule();
    });
  }

  if (prevBtn) {
    prevBtn.addEventListener("click", function () {
      hasStarted = true;
      isPaused = true;
      clearTimer();
      currentStep = Math.max(0, currentStep - 1);
      render();
    });
  }

  if (nextBtn) {
    nextBtn.addEventListener("click", function () {
      hasStarted = true;
      isPaused = true;
      clearTimer();
      currentStep = Math.min(stepsData.length - 1, currentStep + 1);
      render();
    });
  }

  if (replayBtn) {
    replayBtn.addEventListener("click", function () {
      currentStep = 0;
      hasStarted = true;
      isPaused = false;
      render();
      schedule();
    });
  }

  codeExampleTabs.forEach(function (tab) {
    tab.addEventListener("click", function () {
      var selected = tab.getAttribute("data-code-example-tab");
      codeExampleTabs.forEach(function (candidate) {
        var active = candidate === tab;
        candidate.classList.toggle("is-active", active);
        candidate.setAttribute("aria-selected", active ? "true" : "false");
      });
      codeExamples.forEach(function (example) {
        example.hidden = example.getAttribute("data-code-example") !== selected;
      });
    });
  });

  render();
})();
