(function () {
  "use strict";

  var currentStep = 0;
  var isPaused = false;
  var timer = null;

  var stepsData = [
  {
    "day": 1,
    "ttLabel": "Day 1: Trial Started (14-Day Free Access)",
    "badge": "TRIAL ACTIVE",
    "badgeClass": "trial",
    "narrativeBadge": "TIMER SCHEDULED",
    "narrativeTitle": "Day 1: Welcome Email Sent",
    "narrativeDesc": "Customer activates Acme Pro Plan. The flow persists a trial state with a scheduled next eligibility time; no application handler stays blocked.",
    "code": "return transition('send_warning', run_at_ms=day_14_ms)",
    "acctBadge": "TRIAL ACTIVE (14 DAYS REMAINING)",
    "acctBadgeClass": "trial",
    "acctDesc": "User exploring features with full access. Payment method on file.",
    "rev": "$0.00 (Trial)",
    "queries": "0 Queries",
    "drift": "0.00s",
    "grace": "14 Days",
    "activeNode": 0,
    "declined": false
  },
  {
    "day": 14,
    "ttLabel": "Day 14: Pre-Billing Warning Email",
    "badge": "TIMER EXPIRED",
    "badgeClass": "trial",
    "narrativeBadge": "14-DAY WAKEUP",
    "narrativeTitle": "Day 14: Notice Dispatched",
    "narrativeDesc": "The warning state became eligible on Day 14; worker scheduling determines the actual claim time.",
    "code": "email.send_warning(user_id, idempotency_key=f'{job.id}:warning:v1'); transition('charge', run_at_ms=day_15_ms)",
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
    "ttLabel": "Day 15: Charge Attempted ($99.00 / mo)",
    "badge": "CARD DECLINED",
    "badgeClass": "pastdue",
    "narrativeBadge": "GRACE PERIOD ACTIVE",
    "narrativeTitle": "Day 15: Card Declined",
    "narrativeDesc": "Stripe returned 'insufficient_funds'. Workflow entered grace period and scheduled Dunning Retry #1 in 3 days.",
    "code": "email.send_dunning(idempotency_key=f'{job.id}:dunning-1:v1'); transition('retry_1', run_at_ms=day_18_ms)",
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
    "ttLabel": "Day 18: Dunning Retry #1 (SMS + Email)",
    "badge": "DUNNING CADENCE 1",
    "badgeClass": "pastdue",
    "narrativeBadge": "AUTOMATED ESCALATION",
    "narrativeTitle": "Day 18: Multi-Channel Alert",
    "narrativeDesc": "Workflow wakes up on Day 18. Sends SMS warning with direct 1-click card update link. Schedules final retry for Day 21.",
    "code": "sms.send_update_link(idempotency_key=f'{job.id}:sms-1:v1'); transition('retry_2', run_at_ms=day_21_ms)",
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
    "ttLabel": "Day 21: Card Updated ($99.00 Recovered!)",
    "badge": "SUBSCRIPTION RECOVERED",
    "badgeClass": "active",
    "narrativeBadge": "CHURN PREVENTED",
    "narrativeTitle": "Day 21: $99.00 Charge Succeeded!",
    "narrativeDesc": "Customer updated the card. The retry reused a stable Stripe idempotency key and the workflow completed durably.",
    "code": "stripe.charge(9900, idempotency_key=f'{job.id}:invoice:2026-08:v1'); return complete(result=b'ACTIVE')",
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

  function render() {
    var data = stepsData[currentStep];

    if (slider) slider.value = String(currentStep);
    if (ttLabel) ttLabel.textContent = data.ttLabel;

    nodes.forEach(function (node, idx) {
      var isDone = idx < currentStep;
      var isActive = idx === currentStep;
      node.classList.toggle("is-done", isDone);
      node.classList.toggle("is-active", isActive);
      node.classList.toggle("is-declined", data.declined && idx === 2);

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

    if (narrativeBadge) narrativeBadge.textContent = data.narrativeBadge;
    if (narrativeTitle) narrativeTitle.textContent = data.narrativeTitle;
    if (narrativeDesc) narrativeDesc.textContent = data.narrativeDesc;
    if (narrativeCode) narrativeCode.textContent = data.code;

    if (valRev) valRev.textContent = data.rev;
    if (valQueries) valQueries.textContent = data.queries;
    if (valDrift) valDrift.textContent = data.drift;
    if (valGrace) valGrace.textContent = data.grace;

    if (currentRunStep) currentRunStep.textContent = "Step " + (currentStep + 1) + " of " + stepsData.length + " · " + data.ttLabel;

    if (pauseBtn) pauseBtn.textContent = isPaused ? "▶ Play" : "⏸ Pause";
    if (liveStatus) liveStatus.textContent = (isPaused ? "Paused" : "Running") + " · Dunning recovery · Step " + (currentStep + 1) + " of " + stepsData.length;
  }

  function clearTimer() {
    if (timer !== null) {
      window.clearTimeout(timer);
      timer = null;
    }
  }

  function schedule() {
    clearTimer();
    if (isPaused) return;
    var wait = (currentStep === 2) ? 4000 : 2800;
    timer = window.setTimeout(function () {
      currentStep = (currentStep + 1) % stepsData.length;
      render();
      schedule();
    }, wait);
  }

  if (slider) {
    slider.addEventListener("input", function () {
      currentStep = parseInt(slider.value, 10);
      render();
      schedule();
    });
  }

  nodes.forEach(function (node, idx) {
    node.addEventListener("click", function () {
      currentStep = idx;
      render();
      schedule();
    });
  });

  if (declinedToggle) {
    declinedToggle.addEventListener("click", function () {
      currentStep = 2;
      isPaused = true;
      render();
      schedule();
    });
  }

  if (successToggle) {
    successToggle.addEventListener("click", function () {
      currentStep = 4;
      isPaused = true;
      render();
      schedule();
    });
  }

  if (pauseBtn) {
    pauseBtn.addEventListener("click", function () {
      isPaused = !isPaused;
      render();
      schedule();
    });
  }

  if (prevBtn) {
    prevBtn.addEventListener("click", function () {
      currentStep = (currentStep - 1 + stepsData.length) % stepsData.length;
      render();
      schedule();
    });
  }

  if (nextBtn) {
    nextBtn.addEventListener("click", function () {
      currentStep = (currentStep + 1) % stepsData.length;
      render();
      schedule();
    });
  }

  if (replayBtn) {
    replayBtn.addEventListener("click", function () {
      currentStep = 0;
      isPaused = false;
      render();
      schedule();
    });
  }

  render();
  schedule();
})();
