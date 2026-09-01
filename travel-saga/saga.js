(function () {
  "use strict";

  var currentMode = "hotelFail";
  var currentStep = 0;
  var isPaused = false;
  var timer = null;

  var normalSteps = [
  {
    "badge": "\ud83d\udcb3 STRIPE CHARGED ($850.00)",
    "badgeClass": "good",
    "title": "1. Customer Card Authorized ($850.00)",
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
    "badge": "\u2708\ufe0f DELTA SEAT CONFIRMED",
    "badgeClass": "good",
    "title": "2. Delta Flight DL402 Seat Reserved",
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
    "badge": "\ud83c\udfe8 MARRIOTT ROOM CONFIRMED",
    "badgeClass": "good",
    "title": "3. Marriott Suite Booked ($350.00)",
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
    "badge": "\ud83c\udf89 VACATION BUNDLE COMPLETE",
    "badgeClass": "good",
    "title": "4. Hertz SUV Rented ($150.00) · Booking Complete",
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
    "badge": "\ud83d\udcb3 STEP 1: PAYMENT OK",
    "badgeClass": "good",
    "title": "1. Customer Card Authorized ($850.00)",
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
    "badge": "\u2708\ufe0f STEP 2: FLIGHT OK",
    "badgeClass": "good",
    "title": "2. Delta Flight DL402 Seat Reserved",
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
    "badge": "\u274c STEP 3 FAILED (HOTEL SOLD OUT)",
    "badgeClass": "bad",
    "title": "3. Marriott API: 0 Rooms Available!",
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
    "badge": "\ud83d\udd04 UNWINDING STEP 2: FLIGHT RELEASED",
    "badgeClass": "warn",
    "title": "Compensating Delta Flight DL402",
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
    "badge": "\u2713 SAGA COMPLETE: $850.00 REFUNDED",
    "badgeClass": "good",
    "title": "Compensating Step 1: Customer Fully Refunded",
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
    "badge": "💳 STEP 1: PAYMENT OK",
    "badgeClass": "good",
    "title": "1. Customer Card Authorized ($850.00)",
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
    "badge": "❌ STEP 2: DELTA HTTP 500",
    "badgeClass": "bad",
    "title": "2. Delta Booking Returned an Ambiguous 500",
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
    "badge": "🔎 FLIGHT OUTCOME VERIFIED",
    "badgeClass": "warn",
    "title": "Delta Confirms No Seat Was Created",
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
    "badge": "✓ SAGA COMPLETE: $850.00 REFUNDED",
    "badgeClass": "good",
    "title": "Compensating Step 1: Customer Fully Refunded",
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
    if (currentMode === "hotelFail") return "Hotel sold out";
    if (currentMode === "flightFail") return "Delta HTTP 500";
    return "Happy path";
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
    var stepLabel = "Step " + (currentStep + 1) + " of " + steps.length + " · " + stepTitle;
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

    if (pauseBtn) pauseBtn.textContent = isPaused ? "▶ Play" : "⏸ Pause";
    if (liveStatus) liveStatus.textContent = (isPaused ? "Paused" : "Running") + " · " + modeLabel + " · Step " + (currentStep + 1) + " of " + steps.length;
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
    var wait = (currentStep === 2) ? 3800 : 2600;
    timer = window.setTimeout(function () {
      var steps = getSteps();
      currentStep = (currentStep + 1) % steps.length;
      render();
      schedule();
    }, wait);
  }

  if (failHotelBtn) {
    failHotelBtn.addEventListener("click", function () {
      currentMode = "hotelFail";
      currentStep = 0;
      isPaused = false;
      render();
      schedule();
    });
  }

  if (successRunBtn) {
    successRunBtn.addEventListener("click", function () {
      currentMode = "normal";
      currentStep = 0;
      isPaused = false;
      render();
      schedule();
    });
  }

  if (failFlightBtn) {
    failFlightBtn.addEventListener("click", function () {
      currentMode = "flightFail";
      currentStep = 0;
      isPaused = false;
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
      var len = getSteps().length;
      currentStep = (currentStep - 1 + len) % len;
      render();
      schedule();
    });
  }

  if (nextBtn) {
    nextBtn.addEventListener("click", function () {
      var len = getSteps().length;
      currentStep = (currentStep + 1) % len;
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
