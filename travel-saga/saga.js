(function () {
  "use strict";

  var currentMode = "hotelFail";
  var currentStep = 0;
  var isPaused = false;
  var hasStarted = false;
  var timer = null;

  var normalSteps = [
  {
    "badge": "PAYMENT CONFIRMED · $850",
    "badgeClass": "good",
    "title": "1. Customer payment authorized ($850)",
    "desc": "Stripe payment uses a stable provider key; the refund is an explicit compensation state.",
    "code": "stripe.charge(85_000, idempotency_key=f'{ctx.id}:charge:v1')",
    "charge": "+$850.00",
    "flight": "$0.00",
    "hotel": "$0.00",
    "refund": "$0.00",
    "balance": "$850.00 (Customer Hold)",
    "comp": "Normal Flow",
    "stranded": "$0.00",
    "cadence": "LIFO Sequence",
    "latency": "Deployment-dependent",
    "activeNode": 0,
    "failedNode": -1,
    "compensatedNodes": []
  },
  {
    "badge": "FLIGHT SEAT CONFIRMED",
    "badgeClass": "good",
    "title": "2. Flight seat reserved",
    "desc": "Flight seat uses a stable request ID; cancellation is an explicit next state.",
    "code": "delta.book_seat('DL402', request_id=f'{ctx.id}:flight:v1')",
    "charge": "+$850.00",
    "flight": "+$350.00",
    "hotel": "$0.00",
    "refund": "$0.00",
    "balance": "$500.00 (Unallocated)",
    "comp": "Normal Flow",
    "stranded": "$0.00",
    "cadence": "LIFO Sequence",
    "latency": "Deployment-dependent",
    "activeNode": 1,
    "failedNode": -1,
    "compensatedNodes": []
  },
  {
    "badge": "HOTEL ROOM CONFIRMED",
    "badgeClass": "good",
    "title": "3. Hotel room booked ($350)",
    "desc": "Hotel booking uses a stable request ID so a retried handler reuses the same operation.",
    "code": "marriott.book_suite(request_id=f'{ctx.id}:hotel:v1')",
    "charge": "+$850.00",
    "flight": "+$350.00",
    "hotel": "+$350.00",
    "refund": "$0.00",
    "balance": "$150.00 (Car Hold)",
    "comp": "Normal Flow",
    "stranded": "$0.00",
    "cadence": "LIFO Sequence",
    "latency": "Deployment-dependent",
    "activeNode": 2,
    "failedNode": -1,
    "compensatedNodes": []
  },
  {
    "badge": "BOOKING COMPLETE",
    "badgeClass": "good",
    "title": "4. Car rental booked ($150) · booking complete",
    "desc": "All 4 suppliers confirmed. Vacation confirmed with zero balance discrepancy.",
    "code": "rental = hertz.book_suv(request_id=f'{ctx.id}:car:v1')\nreturn complete(result={'status': 'BOOKING_COMPLETE', 'rental_id': rental.id})",
    "charge": "+$850.00",
    "flight": "+$350.00",
    "hotel": "+$350.00",
    "refund": "$0.00",
    "balance": "$0.00 (Balanced)",
    "comp": "Complete (0 Rollbacks)",
    "stranded": "$0.00",
    "cadence": "Balanced",
    "latency": "Deployment-dependent",
    "activeNode": 3,
    "failedNode": -1,
    "compensatedNodes": []
  }
];
  var hotelFailSteps = [
  {
    "badge": "PAYMENT CONFIRMED",
    "badgeClass": "good",
    "title": "1. Customer payment authorized ($850)",
    "desc": "Stripe payment succeeded with a stable provider idempotency key; refund is an explicit state.",
    "code": "stripe.charge(85_000, idempotency_key=f'{ctx.id}:charge:v1')",
    "charge": "+$850.00",
    "flight": "$0.00",
    "hotel": "$0.00",
    "refund": "$0.00",
    "balance": "$850.00",
    "comp": "Normal",
    "stranded": "$0.00",
    "cadence": "Pending",
    "latency": "Deployment-dependent",
    "activeNode": 0,
    "failedNode": -1,
    "compensatedNodes": []
  },
  {
    "badge": "FLIGHT SEAT CONFIRMED",
    "badgeClass": "good",
    "title": "2. Flight seat reserved",
    "desc": "Delta seat is held with a stable request ID; cancellation is an explicit state.",
    "code": "delta.book_seat('DL402', request_id=f'{ctx.id}:flight:v1')",
    "charge": "+$850.00",
    "flight": "+$350.00",
    "hotel": "$0.00",
    "refund": "$0.00",
    "balance": "$500.00",
    "comp": "Normal",
    "stranded": "$0.00",
    "cadence": "Pending",
    "latency": "Deployment-dependent",
    "activeNode": 1,
    "failedNode": -1,
    "compensatedNodes": []
  },
  {
    "badge": "HOTEL UNAVAILABLE",
    "badgeClass": "bad",
    "title": "3. Hotel has no rooms available",
    "desc": "HotelSoldOutException transitions the durable workflow into explicit compensation states.",
    "code": "return transition('cancel_seat')  # then refund_card",
    "charge": "+$850.00",
    "flight": "+$350.00",
    "hotel": "\u274c SOLD OUT",
    "refund": "$0.00",
    "balance": "$850.00 (Unwinding...)",
    "comp": "SAGA UNWINDING",
    "stranded": "$0.00",
    "cadence": "LIFO Triggered",
    "latency": "Deployment-dependent",
    "activeNode": 2,
    "failedNode": 2,
    "compensatedNodes": []
  },
  {
    "badge": "UNDOING STEP 2 · FLIGHT RELEASED",
    "badgeClass": "warn",
    "title": "Undo step 2: release the flight seat",
    "desc": "The cancellation state uses a stable request ID, so reclaim cannot release the seat twice.",
    "code": "delta.cancel_seat(ctx.value('seat_id'), request_id=f'{ctx.id}:cancel:v1')",
    "charge": "+$850.00",
    "flight": "$0.00 (Cancelled)",
    "hotel": "\u274c Cancelled",
    "refund": "$0.00",
    "balance": "$850.00 (Pending Refund)",
    "comp": "Reversing Step 2",
    "stranded": "$0.00",
    "cadence": "Step 2 Cancelled",
    "latency": "Deployment-dependent",
    "activeNode": 1,
    "failedNode": 2,
    "compensatedNodes": [
      1
    ]
  },
  {
    "badge": "BOOKING ROLLED BACK · $850 REFUNDED",
    "badgeClass": "good",
    "title": "Undo step 1: refund the customer",
    "desc": "The refund state uses a stable Stripe key, so a retried handler reuses the same refund operation.",
    "code": "stripe.refund(ctx.value('tx_id'), idempotency_key=f'{ctx.id}:refund:v1')",
    "charge": "$0.00 (Refunded)",
    "flight": "$0.00 (Released)",
    "hotel": "$0.00",
    "refund": "-$850.00",
    "balance": "$0.00 (Balanced)",
    "comp": "Fully Reversible",
    "stranded": "$0.00",
    "cadence": "Clean Exit",
    "latency": "Deployment-dependent",
    "activeNode": 0,
    "failedNode": 2,
    "compensatedNodes": [
      1,
      0
    ]
  }
];

  var flightFailSteps = [
  {
    "badge": "PAYMENT CONFIRMED",
    "badgeClass": "good",
    "title": "1. Customer payment authorized ($850)",
    "desc": "Stripe payment succeeded with a stable provider idempotency key; refund is an explicit state.",
    "code": "stripe.charge(85_000, idempotency_key=f'{ctx.id}:charge:v1')",
    "charge": "+$850.00",
    "flight": "$0.00",
    "hotel": "$0.00",
    "refund": "$0.00",
    "balance": "$850.00",
    "comp": "Normal",
    "stranded": "$0.00",
    "cadence": "Pending",
    "latency": "Deployment-dependent",
    "activeNode": 0,
    "failedNode": -1,
    "compensatedNodes": []
  },
  {
    "badge": "FLIGHT REQUEST FAILED",
    "badgeClass": "bad",
    "title": "2. Flight request failed; outcome is unknown",
    "desc": "A transport error does not prove whether the seat was created. The workflow records the ambiguous outcome and verifies the stable request ID before calling the hotel.",
    "code": "return transition('verify_flight')  # do not call hotel yet",
    "charge": "+$850.00",
    "flight": "⚠️ OUTCOME UNKNOWN",
    "hotel": "$0.00 (Not Called)",
    "refund": "$0.00",
    "balance": "$850.00 (Verifying Flight)",
    "comp": "VERIFYING OUTCOME",
    "stranded": "$0.00",
    "cadence": "Check Before Retry",
    "latency": "Deployment-dependent",
    "activeNode": 1,
    "failedNode": 1,
    "compensatedNodes": []
  },
  {
    "badge": "FLIGHT OUTCOME VERIFIED",
    "badgeClass": "warn",
    "title": "Delta confirms no seat was created",
    "desc": "The stable request ID is absent at Delta. There is no flight to cancel, so the saga skips the hotel and moves directly to refund_card.",
    "code": "seat = delta.get_booking(request_id=f'{ctx.id}:flight:v1')\nif not seat: return transition('refund_card')",
    "charge": "+$850.00",
    "flight": "$0.00 (Not Created)",
    "hotel": "$0.00 (Not Called)",
    "refund": "$0.00",
    "balance": "$850.00 (Pending Refund)",
    "comp": "SKIP FLIGHT CANCEL",
    "stranded": "$0.00",
    "cadence": "Verified Absent",
    "latency": "Deployment-dependent",
    "activeNode": 1,
    "failedNode": 1,
    "compensatedNodes": []
  },
  {
    "badge": "BOOKING ROLLED BACK · $850 REFUNDED",
    "badgeClass": "good",
    "title": "Undo step 1: refund the customer",
    "desc": "Only the card needs compensation. The refund uses a stable Stripe key, while the hotel and car were never called.",
    "code": "stripe.refund(ctx.value('tx_id'), idempotency_key=f'{ctx.id}:refund:v1')",
    "charge": "$0.00 (Refunded)",
    "flight": "$0.00 (Not Created)",
    "hotel": "$0.00 (Not Called)",
    "refund": "-$850.00",
    "balance": "$0.00 (Balanced)",
    "comp": "CARD ONLY",
    "stranded": "$0.00",
    "cadence": "Clean Exit",
    "latency": "Deployment-dependent",
    "activeNode": 0,
    "failedNode": 1,
    "compensatedNodes": [0]
  }
];

  function getSteps() {
    if (currentMode === "hotelFail") return hotelFailSteps;
    if (currentMode === "flightFail") return flightFailSteps;
    return normalSteps;
  }

  function getModeLabel() {
    if (currentMode === "hotelFail") return "Hotel unavailable";
    if (currentMode === "flightFail") return "Flight request fails";
    return "All bookings succeed";
  }

  var nodes = document.querySelectorAll("[data-saga-node]");
  var narrativeBadge = document.querySelector("[data-narrative-badge]");
  var narrativeTitle = document.querySelector("[data-narrative-title]");
  var narrativeDesc = document.querySelector("[data-narrative-desc]");
  var narrativeCode = document.querySelector("[data-narrative-code]");

  var ledgerCharge = document.querySelector("[data-ledger-charge]");
  var ledgerFlight = document.querySelector("[data-ledger-flight]");
  var ledgerHotel = document.querySelector("[data-ledger-hotel]");
  var ledgerRefund = document.querySelector("[data-ledger-refund]");
  var ledgerBalance = document.querySelector("[data-ledger-balance]");

  var valComp = document.querySelector("[data-val-comp]");
  var valStranded = document.querySelector("[data-val-stranded]");
  var valCadence = document.querySelector("[data-val-cadence]");
  var valLatency = document.querySelector("[data-val-latency]");

  var prevBtn = document.querySelector("[data-prev]");
  var pauseBtn = document.querySelector("[data-pause]");
  var nextBtn = document.querySelector("[data-next]");
  var replayBtn = document.querySelector("[data-replay]");
  var liveStatus = document.querySelector("[data-live-status]");
  var currentRunLabel = document.querySelector("[data-current-run-label]");
  var currentRunStep = document.querySelector("[data-current-run-step]");

  var failHotelBtn = document.querySelector("[data-fail-hotel]");
  var failFlightBtn = document.querySelector("[data-fail-flight]");
  var successRunBtn = document.querySelector("[data-success-run]");

  function render() {
    var steps = getSteps();
    if (currentStep >= steps.length) currentStep = 0;
    var data = steps[currentStep];

    nodes.forEach(function (node, idx) {
      var isCompensated = data.compensatedNodes.indexOf(idx) !== -1;
      var isFailed = data.failedNode === idx;
      var isActive = data.activeNode === idx;
      var isDone = idx < data.activeNode && !isCompensated && !isFailed;

      node.classList.toggle("is-active", isActive && !isFailed && !isCompensated);
      node.classList.toggle("is-done", isDone);
      node.classList.toggle("is-failed", isFailed);
      node.classList.toggle("is-compensated", isCompensated);
      if (isActive) node.setAttribute("aria-current", "step");
      else node.removeAttribute("aria-current");
      var nodeTitle = node.querySelector("strong");
      node.setAttribute("aria-label", (nodeTitle ? nodeTitle.textContent : "Booking step " + (idx + 1)) + ". " + (isFailed ? "Failed" : (isCompensated ? "Refunded" : (isActive ? "Active" : (isDone ? "Complete" : "Pending")))));

      var pill = node.querySelector(".node-pill");
      if (pill) {
        if (isFailed) pill.textContent = "FAILED";
        else if (isCompensated) pill.textContent = "REFUNDED";
        else if (isActive) pill.textContent = "ACTIVE";
        else if (isDone) pill.textContent = "✓ DONE";
        else pill.textContent = "PENDING";
      }
    });

    if (narrativeBadge) {
      narrativeBadge.textContent = data.badge;
      narrativeBadge.className = "saga-badge " + data.badgeClass;
    }
    if (narrativeTitle) narrativeTitle.textContent = data.title;
    if (narrativeDesc) narrativeDesc.textContent = data.desc;
    if (narrativeCode) narrativeCode.textContent = data.code;

    if (ledgerCharge) ledgerCharge.textContent = data.charge;
    if (ledgerFlight) ledgerFlight.textContent = data.flight;
    if (ledgerHotel) ledgerHotel.textContent = data.hotel;
    if (ledgerRefund) ledgerRefund.textContent = data.refund;
    if (ledgerBalance) ledgerBalance.textContent = data.balance;

    if (valComp) valComp.textContent = data.comp;
    if (valStranded) valStranded.textContent = data.stranded;
    if (valCadence) valCadence.textContent = data.cadence;
    if (valLatency) valLatency.textContent = data.latency;

    var modeLabel = getModeLabel();
    var stepTitle = data.title.replace(/^\d+\.\s*/, "");
    var stepLabel = (!hasStarted ? "Preview · " : "Step ") + (currentStep + 1) + " of " + steps.length + " · " + stepTitle;
    if (currentRunLabel) currentRunLabel.textContent = modeLabel;
    if (currentRunStep) currentRunStep.textContent = stepLabel;

    [
      { button: failHotelBtn, mode: "hotelFail" },
      { button: failFlightBtn, mode: "flightFail" },
      { button: successRunBtn, mode: "normal" }
    ].forEach(function (choice) {
      if (!choice.button) return;
      var selected = currentMode === choice.mode;
      choice.button.classList.toggle("is-selected", selected);
      choice.button.setAttribute("aria-pressed", String(selected));
    });

    if (prevBtn) prevBtn.disabled = currentStep === 0;
    if (nextBtn) nextBtn.disabled = currentStep >= steps.length - 1;
    if (pauseBtn) {
      var finished = hasStarted && currentStep >= steps.length - 1 && isPaused;
      pauseBtn.textContent = finished ? "Finished" : (!hasStarted || isPaused ? "Play" : "Pause");
      pauseBtn.disabled = finished;
      pauseBtn.setAttribute("aria-label", finished ? "Booking path finished; choose Replay to run it again" : "Pause or play the booking flow");
    }
    if (liveStatus) liveStatus.textContent = !hasStarted
      ? "Preview ready · choose Run to play"
      : (finished ? "Finished · choose Replay to run again" : (isPaused ? "Paused · choose Play to continue" : "Playing automatically · Pause to inspect")) + " · " + modeLabel + " · Step " + (currentStep + 1) + " of " + steps.length;
  }

  function clearTimer() {
    if (timer !== null) {
      window.clearTimeout(timer);
      timer = null;
    }
  }

  function schedule() {
    clearTimer();
    var steps = getSteps();
    if (!hasStarted || isPaused || currentStep >= steps.length - 1) {
      if (hasStarted && currentStep >= steps.length - 1) {
        isPaused = true;
        render();
      }
      return;
    }
    var wait = (currentStep === 2) ? 3800 : 2600;
    timer = window.setTimeout(function () {
      var steps = getSteps();
      currentStep += 1;
      render();
      schedule();
    }, wait);
  }

  // These stations represent bookings, not timeline indexes. Failure paths
  // include verification and refunds, so use Previous/Next to inspect them.

  if (failHotelBtn) {
    failHotelBtn.addEventListener("click", function () {
      currentMode = "hotelFail";
      currentStep = 0;
      hasStarted = false;
      isPaused = false;
      clearTimer();
      render();
    });
  }

  if (successRunBtn) {
    successRunBtn.addEventListener("click", function () {
      currentMode = "normal";
      currentStep = 0;
      hasStarted = false;
      isPaused = false;
      clearTimer();
      render();
    });
  }

  if (failFlightBtn) {
    failFlightBtn.addEventListener("click", function () {
      currentMode = "flightFail";
      currentStep = 0;
      hasStarted = false;
      isPaused = false;
      clearTimer();
      render();
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
      var len = getSteps().length;
      hasStarted = true;
      isPaused = true;
      clearTimer();
      currentStep = Math.max(0, currentStep - 1);
      render();
    });
  }

  if (nextBtn) {
    nextBtn.addEventListener("click", function () {
      var len = getSteps().length;
      hasStarted = true;
      isPaused = true;
      clearTimer();
      currentStep = Math.min(len - 1, currentStep + 1);
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

  render();
})();
