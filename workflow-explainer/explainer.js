(function () {
  "use strict";

  var mode = "before"; // "before" (Without Workflows) or "after" (With FerricStore)
  var currentStep = 0;
  var isPaused = false;
  var timer = null;

  var stepsData = {
    before: [
      {
        stationIndex: 0,
        title: "1. 💳 Credit Card Charged ($150.00)",
        badge: "STEP 1 · VOLATILE RAM",
        badgeType: "bad",
        desc: "Customer clicks \"Buy Sneakers\". The server charges $150 to their card, but only stores the confirmation in temporary server memory.",
        checkpointVal: "Step 1 [Payment] In Temporary RAM (Unsaved)",
        packagePos: "0%",
        pkgIcon: "💳",
        pkgKicker: "ORDER #9842 · $150.00",
        pkgStatus: "Stored in RAM Only",
        pkgClass: "",
        bankAmount: "$150.00",
        bankClass: "",
        smsType: "alert-bad",
        smsMsg: "<strong>💳 Bank Notification:</strong> Charged $150.00 at SneakerStore (Kept only in temporary server memory)."
      },
      {
        stationIndex: 1,
        title: "2. 📦 Item Picked from Warehouse",
        badge: "STEP 2 · VOLATILE RAM",
        badgeType: "bad",
        desc: "Warehouse reserves sneaker size. The reservation ID is also kept only in temporary server memory.",
        checkpointVal: "Step 2 [Warehouse] In Temporary RAM (Unsaved)",
        packagePos: "25%",
        pkgIcon: "📦",
        pkgKicker: "ORDER #9842 · $150.00",
        pkgStatus: "Item Picked (RAM)",
        pkgClass: "",
        bankAmount: "$150.00",
        bankClass: "",
        smsType: "alert-bad",
        smsMsg: "<strong>📦 Warehouse Update:</strong> Stock reserved. Step 2 done."
      },
      {
        stationIndex: 2,
        title: "3. 💥 Server Crash / Power Outage!",
        badge: "💥 DISASTER OCCURS",
        badgeType: "bad",
        desc: "The cloud server suddenly crashes (OOM / timeout / network drop). Because there were no checkpoints, ALL memory is instantly wiped clean!",
        checkpointVal: "MEMORY WIPED · ALL PROGRESS LOST",
        packagePos: "50%",
        pkgIcon: "💥",
        pkgKicker: "CRASH OCCURRED",
        pkgStatus: "SERVER DIED · RAM LOST",
        pkgClass: "is-exploded",
        bankAmount: "$150.00",
        bankClass: "",
        smsType: "alert-bad",
        smsMsg: "<strong>💥 SERVER CRASH:</strong> Process RAM vanished. The server completely forgot it already charged the customer!"
      },
      {
        stationIndex: 3,
        title: "4. 🔄 Blind Full Restart (Customer Double-Billed!)",
        badge: "⚠️ UNCHECKPOINTED RETRY",
        badgeType: "bad",
        desc: "A generic retry script restarts the order from the beginning. It doesn\x27t know Step 1 already ran, so it charges the customer\x27s card A SECOND TIME!",
        checkpointVal: "RESTARTING FROM STEP 1 (NO CHECKPOINTS)",
        packagePos: "75%",
        pkgIcon: "😱",
        pkgKicker: "DOUBLE BILLED · $300.00",
        pkgStatus: "CHARGED TWICE!",
        pkgClass: "is-exploded",
        bankAmount: "$300.00 (2x!)",
        bankClass: "is-double-charged",
        smsType: "alert-bad",
        smsMsg: "<strong>🚨 DOUBLE CHARGE ALERT:</strong> Card billed AGAIN for $150.00 (Total: $300.00)! Customer gets angry bank alert."
      },
      {
        stationIndex: 4,
        title: "5. ⚠️ Chaotic Outcome: Broken State & Refunds",
        badge: "⚠️ HIGH COST FAILURE",
        badgeType: "bad",
        desc: "The order eventually arrives, but the customer was billed twice ($300), warehouse inventory was deducted twice, and support must spend hours issuing refunds.",
        checkpointVal: "COMPLETED WITH 2X COST PENALTY",
        packagePos: "100%",
        pkgIcon: "💸",
        pkgKicker: "SUPPORT NIGHTMARE",
        pkgStatus: "2x Charge Penalty",
        pkgClass: "is-exploded",
        bankAmount: "$300.00",
        bankClass: "is-double-charged",
        smsType: "alert-bad",
        smsMsg: "<strong>Support Nightmare:</strong> $150 duplicate charge, 2x warehouse stock deducted, and an angry customer review."
      }
    ],
    after: [
      {
        stationIndex: 0,
        title: "1. 💳 Credit Card Charged ($150.00)",
        badge: "STEP 1 · CHECKPOINT SAVED",
        badgeType: "good",
        desc: "Customer clicks \"Buy Sneakers\". The app charges $150 with a provider idempotency key, then durably advances workflow state.",
        checkpointVal: "Step 1 [Payment] Committed to Durable State",
        packagePos: "0%",
        pkgIcon: "💳",
        pkgKicker: "ORDER #9842 · $150.00",
        pkgStatus: "Saved on Disk",
        pkgClass: "is-shielded",
        bankAmount: "$150.00",
        bankClass: "",
        smsType: "alert-good",
        smsMsg: "<strong>💳 Bank Notification:</strong> Charged $150.00. FerricStore durably saved payment receipt to disk."
      },
      {
        stationIndex: 1,
        title: "2. 📦 Item Picked from Warehouse",
        badge: "STEP 2 · CHECKPOINT SAVED",
        badgeType: "good",
        desc: "Warehouse reserves sneaker size and the workflow commits the next state. External payment and inventory calls remain protected by their stable provider keys.",
        checkpointVal: "Step 2 [Warehouse] Committed to Durable State",
        packagePos: "25%",
        pkgIcon: "📦",
        pkgKicker: "ORDER #9842 · $150.00",
        pkgStatus: "Saved on Disk",
        pkgClass: "is-shielded",
        bankAmount: "$150.00",
        bankClass: "",
        smsType: "alert-good",
        smsMsg: "<strong>📦 Checkpoint Committed:</strong> Payment ($150) + Warehouse Stock locked in FerricStore disk log."
      },
      {
        stationIndex: 2,
        title: "3. 💥 Server Crash (State Safe in FerricStore!)",
        badge: "🛡️ SHIELDED BY FERRICSTORE",
        badgeType: "good",
        desc: "The cloud server crashes mid-order! But unlike volatile RAM, FerricStore holds all completed steps safely on disk. Zero data is lost.",
        checkpointVal: "FERRICSTORE HOLDS CHECKPOINTS ON DISK",
        packagePos: "50%",
        pkgIcon: "🛡️",
        pkgKicker: "CRASH ISOLATED",
        pkgStatus: "SAFE ON DISK",
        pkgClass: "is-shielded",
        bankAmount: "$150.00",
        bankClass: "",
        smsType: "alert-good",
        smsMsg: "<strong>🛡️ COMMITTED STATE RECOVERED:</strong> The worker died, but the committed receipts remain durable. A compatible worker can reclaim the current state."
      },
      {
        stationIndex: 3,
        title: "4. 🚀 Rescue Server Reclaims Durable State",
        badge: "✓ FENCED RESUME",
        badgeType: "good",
        desc: "A new server claims the current state with a newer fence. The payment remains protected by its provider idempotency key.",
        checkpointVal: "DURABLE STATE RECLAIMED WITH NEW FENCE",
        packagePos: "75%",
        pkgIcon: "🚚",
        pkgKicker: "GUARDED BILL · $150.00",
        pkgStatus: "Courier Dispatched",
        pkgClass: "is-shielded",
        bankAmount: "$150.00 (provider key)",
        bankClass: "",
        smsType: "alert-good",
        smsMsg: "<strong>✓ PROVIDER KEY REUSED:</strong> The workflow resumes from Step 4 and reuses the stable payment idempotency key. The payment provider decides duplicate-call behavior."
      },
      {
        stationIndex: 4,
        title: "5. 🎉 Delivery with Durable Completion",
        badge: "✓ DURABLE COMPLETION",
        badgeType: "good",
        desc: "Order delivered on time. Durable state and guarded external effects let recovery continue without a stale worker overwriting newer progress.",
        checkpointVal: "WORKFLOW COMPLETED DURABLY",
        packagePos: "100%",
        pkgIcon: "🎉",
        pkgKicker: "HAPPY CUSTOMER",
        pkgStatus: "Delivered Flawlessly!",
        pkgClass: "is-shielded",
        bankAmount: "$150.00",
        bankClass: "",
        smsType: "alert-good",
        smsMsg: "<strong>🎉 Successful Execution:</strong> The charge uses a stable provider idempotency key, committed workflow states are reused, and the order completes after recovery."
      }
    ]
  };

  // DOM Elements
  var modeButtons = document.querySelectorAll("[data-mode]");
  var stationNodes = document.querySelectorAll("[data-station]");
  var progressFill = document.querySelector("[data-progress-fill]");
  
  var glidingPackage = document.querySelector("[data-package]");
  var pkgIcon = document.querySelector("[data-pkg-icon]");
  var pkgKicker = document.querySelector("[data-pkg-kicker]");
  var pkgStatus = document.querySelector("[data-pkg-status]");

  var bankCard = document.querySelector("[data-bank-card]");
  var bankAmount = document.querySelector("[data-bank-amount]");
  var smsBox = document.querySelector("[data-sms-box]");
  var smsContent = document.querySelector("[data-sms-content]");
  
  var narrativeBadge = document.querySelector("[data-narrative-badge]");
  var narrativeTitle = document.querySelector("[data-narrative-title]");
  var narrativeDesc = document.querySelector("[data-narrative-desc]");
  var stepIndicator = document.querySelector("[data-step-indicator]");
  var checkpointVal = document.querySelector("[data-checkpoint-val]");

  // Dynamic Station Labels
  var st4Icon = document.querySelector("[data-st-4-icon]");
  var st4Title = document.querySelector("[data-st-4-title]");
  var st4Sub = document.querySelector("[data-st-4-sub]");
  var st5Icon = document.querySelector("[data-st-5-icon]");
  var st5Title = document.querySelector("[data-st-5-title]");
  var st5Sub = document.querySelector("[data-st-5-sub]");

  var prevBtn = document.querySelector("[data-prev]");
  var pauseBtn = document.querySelector("[data-pause]");
  var nextBtn = document.querySelector("[data-next]");
  var replayBtn = document.querySelector("[data-replay]");
  var crashBtn = document.querySelector("[data-smash-crash]");
  var liveStatusText = document.querySelector("[data-live-status-text]");

  function updateDynamicLabels() {
    if (mode === "after") {
      if (st4Icon) st4Icon.setAttribute("href", "#wf-icon-resume");
      if (st4Title) st4Title.textContent = "Durable Resume";
      if (st4Sub) st4Sub.textContent = "0 Duplicate Work";
      if (st5Icon) st5Icon.setAttribute("href", "#wf-icon-complete");
      if (st5Title) st5Title.textContent = "Exact $150";
      if (st5Sub) st5Sub.textContent = "Happy Customer";
    } else {
      if (st4Icon) st4Icon.setAttribute("href", "#wf-icon-restart");
      if (st4Title) st4Title.textContent = "Full Restart";
      if (st4Sub) st4Sub.textContent = "Repeats Step 1 & 2";
      if (st5Icon) st5Icon.setAttribute("href", "#wf-icon-penalty");
      if (st5Title) st5Title.textContent = "$300 Penalty";
      if (st5Sub) st5Sub.textContent = "Double Billed!";
    }
  }

  function render() {
    var list = stepsData[mode];
    if (currentStep >= list.length) currentStep = 0;
    var step = list[currentStep];

    updateDynamicLabels();

    // Mode Buttons
    modeButtons.forEach(function (btn) {
      var isActive = btn.dataset.mode === mode;
      btn.classList.toggle("is-active", isActive);
      btn.setAttribute("aria-selected", String(isActive));
    });

    // 5 Station Nodes
    stationNodes.forEach(function (node, idx) {
      var isDone = idx < currentStep;
      var isActive = idx === currentStep;
      node.classList.toggle("is-done", isDone);
      node.classList.toggle("is-active", isActive);
      node.classList.toggle("is-unsafe", mode === "before" && isDone);
      node.classList.toggle("is-committed", mode === "after" && isDone);
      node.classList.toggle("is-risk", mode === "before" && isActive && currentStep < 2);
      node.classList.toggle("is-failure", mode === "before" && isActive && currentStep >= 2);
      node.classList.toggle("is-success", mode === "after" && isActive);
      node.setAttribute("aria-pressed", String(isActive));
      if (isActive) node.setAttribute("aria-current", "step");
      else node.removeAttribute("aria-current");
      var nodeTitle = node.querySelector(".node-title");
      var nodeSub = node.querySelector(".node-sub");
      node.setAttribute("aria-label", (nodeTitle ? nodeTitle.textContent : "Stage " + (idx + 1)) + (nodeSub ? ". " + nodeSub.textContent : ""));
    });

    // Progress Line Fill
    if (progressFill) {
      progressFill.style.width = (currentStep * 25) + "%";
      progressFill.dataset.state = mode === "after" ? "success" : (currentStep < 2 ? "risk" : "failure");
    }

    // Moving Package Box
    if (glidingPackage) {
      glidingPackage.style.left = step.packagePos;
      glidingPackage.className = "gliding-package " + step.pkgClass;
      if (pkgIcon) pkgIcon.textContent = step.pkgIcon;
      if (pkgKicker) pkgKicker.textContent = step.pkgKicker;
      if (pkgStatus) pkgStatus.textContent = step.pkgStatus;
    }

    // Bank Card Widget
    if (bankCard) {
      bankCard.className = "bank-card " + step.bankClass;
    }
    if (bankAmount) bankAmount.textContent = step.bankAmount;
    if (smsBox) smsBox.className = "sms-push-box " + step.smsType;
    if (smsContent) smsContent.innerHTML = step.smsMsg;

    // Narrative & Checkpoint
    if (narrativeBadge) {
      narrativeBadge.textContent = step.badge;
      narrativeBadge.className = "narrative-badge " + (step.badgeType === "good" ? "is-good" : "is-bad");
    }
    if (narrativeTitle) narrativeTitle.textContent = step.title;
    if (narrativeDesc) narrativeDesc.textContent = step.desc;
    if (checkpointVal) checkpointVal.textContent = step.checkpointVal;
    if (stepIndicator) stepIndicator.textContent = "Step " + (currentStep + 1) + " of " + list.length;

    // Controls
    if (pauseBtn) pauseBtn.textContent = isPaused ? "▶ Play" : "⏸ Pause";
    if (liveStatusText) liveStatusText.textContent = isPaused ? "Paused" : "Auto-advancing animation";
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
    var wait = currentStep === 2 ? 3200 : (currentStep === 4 ? 4000 : 2200);
    timer = window.setTimeout(function () {
      currentStep = (currentStep + 1) % stepsData[mode].length;
      render();
      schedule();
    }, wait);
  }

  // Event Listeners
  modeButtons.forEach(function (btn) {
    btn.addEventListener("click", function () {
      mode = btn.dataset.mode;
      currentStep = 0;
      render();
      schedule();
    });
  });

  stationNodes.forEach(function (node, idx) {
    function jump() {
      currentStep = idx;
      render();
      schedule();
    }
    node.addEventListener("click", jump);
    node.addEventListener("keydown", function (e) {
      if (e.key === "Enter" || e.key === " ") {
        e.preventDefault();
        jump();
      }
    });
  });

  if (crashBtn) {
    crashBtn.addEventListener("click", function () {
      currentStep = 2; // Jump directly to crash step
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
      var len = stepsData[mode].length;
      currentStep = (currentStep - 1 + len) % len;
      render();
      schedule();
    });
  }

  if (nextBtn) {
    nextBtn.addEventListener("click", function () {
      var len = stepsData[mode].length;
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
