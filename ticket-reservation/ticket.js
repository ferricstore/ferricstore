(function () {
  'use strict';

  var currentMode = 'after';
  var currentSdk = 'states';
  var currentStep = 1;
  var animTimeouts = [];

  // DOM Elements
  var modeButtons = document.querySelectorAll('[data-mode-btn]');
  var sdkButtons = document.querySelectorAll('[data-sdk-style]');

  var codeBlocks = document.querySelectorAll('[data-code]');
  var codeTitle = document.querySelector('[data-code-title]');
  var codeKicker = document.querySelector('[data-code-kicker]');
  var codeBadge = document.querySelector('[data-code-badge]');

  var stepPills = document.querySelectorAll('[data-step-indicator]');
  var step3Text = document.querySelector('[data-step-3-text]');

  var buyer1Card = document.querySelector('[data-buyer-1-card]');
  var buyer1Tag = document.querySelector('[data-buyer-1-tag]');
  var buyer1TimerBox = document.querySelector('[data-buyer-1-timer-box]');
  var timerDigits = document.querySelector('[data-timer-digits]');
  var timerFill = document.querySelector('[data-timer-fill]');
  var buyer1Note = document.querySelector('[data-buyer-1-note]');

  var seatBox = document.querySelector('[data-seat-box]');
  var seatOwner = document.querySelector('[data-seat-owner]');

  var buyer2Card = document.querySelector('[data-buyer-2-card]');
  var buyer2Tag = document.querySelector('[data-buyer-2-tag]');
  var buyer2Status = document.querySelector('[data-buyer-2-status]');
  var buyer2Note = document.querySelector('[data-buyer-2-note]');

  var expIcon = document.querySelector('[data-exp-icon]');
  var expTitle = document.querySelector('[data-exp-title]');
  var expDesc = document.querySelector('[data-exp-desc]');

  var livePill = document.querySelector('[data-live-pill]');
  var liveStatus = document.querySelector('[data-live-status]');

  var metricCpu = document.querySelector('[data-metric-cpu]');
  var metricReclaim = document.querySelector('[data-metric-reclaim]');
  var metricRisk = document.querySelector('[data-metric-risk]');
  var metricRev = document.querySelector('[data-metric-rev]');

  var outcomeCallout = document.querySelector('[data-outcome-callout]');
  var outcomeLabel = document.querySelector('[data-outcome-label]');
  var outcomeTitle = document.querySelector('[data-outcome-title]');
  var outcomeSub = document.querySelector('[data-outcome-sub]');

  var btnPlay = document.querySelector('[data-btn-play]');
  var btnStep1 = document.querySelector('[data-btn-step-1]');
  var btnStep2 = document.querySelector('[data-btn-step-2]');
  var btnStep3 = document.querySelector('[data-btn-step-3]');
  var btnReset = document.querySelector('[data-btn-reset]');

  function clearAllTimeouts() { animTimeouts.forEach(function (t) { clearTimeout(t); }); animTimeouts = []; }

  function highlightCodeLine(targetLine, isCrash) {
    var activeBlock = document.querySelector('[data-code="' + (currentMode === 'before' ? 'before' : ('after-' + currentSdk)) + '"]');
    if (!activeBlock) return;
    activeBlock.querySelectorAll('[data-line]').forEach(function (l) {
      l.classList.remove('is-active', 'is-crash');
    });
    if (targetLine) {
      var row = activeBlock.querySelector('[data-line="' + targetLine + '"]');
      if (row) row.classList.add(isCrash ? 'is-crash' : 'is-active');
    }
  }

  function updateActiveCodeBlock() {
    codeBlocks.forEach(function (block) {
      var target = block.getAttribute('data-code');
      if (currentMode === 'before') {
        block.hidden = target !== 'before';
      } else {
        block.hidden = target !== ('after-' + currentSdk);
      }
    });

    if (currentMode === 'before') {
      if (codeTitle) codeTitle.textContent = 'redis_checkout.py';
      if (codeKicker) codeKicker.textContent = 'REDIS EXPIRE (UNSYNCHRONIZED)';
      if (codeBadge) { codeBadge.textContent = 'VOLATILE LOCK'; codeBadge.style.background = 'rgba(239,68,68,0.25)'; codeBadge.style.color = '#fca5a5'; }
      if (step3Text) step3Text.textContent = 'Race Condition (Double-Book)';
    } else {
      if (codeTitle) codeTitle.textContent = currentSdk === 'steps' ? 'ticket_continue.py' : 'ticket_fsm.py';
      if (codeKicker) codeKicker.textContent = currentSdk === 'steps' ? 'FLOW.STEP_CONTINUE API' : 'FERRICSTORE STATES API';
      if (codeBadge) { codeBadge.textContent = 'DURABLE CART TIMER'; codeBadge.style.background = 'rgba(16,185,129,0.25)'; codeBadge.style.color = '#6ee7b7'; }
      if (step3Text) step3Text.textContent = 'Fenced Seat Handoff';
    }
  }

  function updateStepperUI(step) {
    currentStep = step;
    stepPills.forEach(function (pill) {
      var num = parseInt(pill.getAttribute('data-step-indicator'), 10);
      pill.classList.remove('is-active', 'is-done');
      if (num === step) pill.classList.add('is-active');
      else if (num < step) pill.classList.add('is-done');
    });
  }

  // --- STEP 1: Buyer #1 Holds Seat ---
  function runStep1() {
    clearAllTimeouts();
    updateStepperUI(1);

    if (livePill) livePill.className = 'live-pill';
    if (liveStatus) liveStatus.textContent = 'STEP 1: HELD IN CART (10:00)';

    // Buyer 1
    if (buyer1Card) buyer1Card.className = 'buyer-card buyer-1 is-owner';
    if (buyer1Tag) { buyer1Tag.className = 'buyer-tag'; buyer1Tag.textContent = '🛒 In Cart (Holding Seat)'; }
    if (timerDigits) timerDigits.textContent = '10:00';
    if (timerFill) { timerFill.style.width = '100%'; timerFill.style.background = 'linear-gradient(90deg, #f59e0b, #ef4444)'; }
    if (buyer1Note) buyer1Note.textContent = 'Holding Seat A12 for 10:00 checkout...';

    // Seat
    if (seatBox) {
      seatBox.style.borderColor = '#f59e0b';
      seatBox.style.background = '#1e293b';
      seatBox.style.boxShadow = '0 0 20px rgba(245, 158, 11, 0.4)';
    }
    if (seatOwner) {
      seatOwner.textContent = 'Held by: Buyer #1';
      seatOwner.style.color = '#fde047';
      seatOwner.style.borderColor = '#f59e0b';
    }

    // Buyer 2
    if (buyer2Card) buyer2Card.className = 'buyer-card buyer-2';
    if (buyer2Tag) { buyer2Tag.className = 'buyer-tag waiting'; buyer2Tag.textContent = '⏳ Waiting Room (Pos #1)'; }
    if (buyer2Status) buyer2Status.textContent = 'Waiting for Seat A12...';
    if (buyer2Note) buyer2Note.textContent = 'Ready to claim if Buyer #1 abandons';

    // Explanation
    if (expIcon) expIcon.textContent = '💡';
    if (expTitle) expTitle.textContent = 'Step 1: Buyer #1 locks Seat #A12 for 10 minutes';
    if (expDesc) expDesc.textContent = currentMode === 'before'
      ? 'Redis sets a volatile key: redis.set("seat:A12", "buyer_1", ex=600). Click "2. 10m Expires" to fast-forward.'
      : 'FerricStore persists the hold state without keeping an application handler blocked. Click "2. 10m Expires" to fast-forward.';

    // Metrics
    if (currentMode === 'before') {
      if (metricCpu) metricCpu.textContent = '50,000 DB Polling Queries / sec';
      if (metricReclaim) metricReclaim.textContent = '45.0 sec (Cron Lag)';
      if (metricRisk) metricRisk.textContent = 'High (Double-Booking Hazard)';
      if (metricRev) metricRev.textContent = '72% (Cart Leakage)';

      if (outcomeCallout) outcomeCallout.className = 'outcome-callout bad';
      if (outcomeLabel) outcomeLabel.textContent = 'CONCURRENCY MODE';
      if (outcomeTitle) outcomeTitle.textContent = 'REDIS VOLATILE KEY (EX=600)';
      if (outcomeSub) outcomeSub.textContent = 'Key is not synchronized with database transactions. Race conditions will occur at second 600.';
      highlightCodeLine(5);
    } else {
      if (metricCpu) metricCpu.textContent = 'No waiting handler held';
      if (metricReclaim) metricReclaim.textContent = 'After hold expiry';
      if (metricRisk) metricRisk.textContent = 'State-Guarded Handoff';
      if (metricRev) metricRev.textContent = 'Captured in this run';

      if (outcomeCallout) outcomeCallout.className = 'outcome-callout good';
      if (outcomeLabel) outcomeLabel.textContent = 'CONCURRENCY MODE';
      if (outcomeTitle) outcomeTitle.textContent = 'FERRICSTORE DURABLE TIMER';
      if (outcomeSub) outcomeSub.textContent = 'The durable hold expires; the next buyer can claim ownership with a newer fence.';
      highlightCodeLine(4);
    }
  }

  // --- STEP 2: 10m Timer Expires ---
  function runStep2(onDone) {
    updateStepperUI(2);

    if (livePill) livePill.className = 'live-pill is-crash';
    if (liveStatus) liveStatus.textContent = 'STEP 2: 10:00 TIMER EXPIRED';

    // Buyer 1 timer hits 0
    if (timerDigits) timerDigits.textContent = '00:00';
    if (timerFill) timerFill.style.width = '0%';
    if (buyer1Note) buyer1Note.textContent = '10 minutes elapsed. Buyer #1 walked away.';

    // Explanation
    if (expIcon) expIcon.textContent = '⏰';
    if (expTitle) expTitle.textContent = 'Step 2: 10-Minute Cart Hold Elapses';
    if (expDesc) expDesc.textContent = currentMode === 'before'
      ? 'Redis key seat:A12 vanishes. But database still has status="held"! Click "3. Resolution" to see what happens.'
      : 'Workflow wakes up from Raft log instantly. Checks payment status. Click "3. Resolution" to see the handoff.';

    highlightCodeLine(currentMode === 'before' ? 8 : 7, true);
    if (onDone) animTimeouts.push(setTimeout(onDone, 1000));
  }

  // --- STEP 3: Resolution ---
  function runStep3() {
    updateStepperUI(3);

    if (currentMode === 'before') {
      // REDIS DISASTER
      if (livePill) livePill.className = 'live-pill is-crash';
      if (liveStatus) liveStatus.textContent = '💥 DOUBLE BOOKING DISASTER';

      if (buyer1Card) buyer1Card.className = 'buyer-card buyer-1 is-conflict';
      if (buyer1Tag) { buyer1Tag.className = 'buyer-tag conflict'; buyer1Tag.textContent = '🚨 Charged $280.00'; }
      if (buyer1Note) buyer1Note.textContent = 'Clicked Pay at sec 599. Stripe charge succeeded!';

      if (buyer2Card) buyer2Card.className = 'buyer-card buyer-2 is-conflict';
      if (buyer2Tag) { buyer2Tag.className = 'buyer-tag conflict'; buyer2Tag.textContent = '🚨 Charged $280.00'; }
      if (buyer2Status) buyer2Status.textContent = '🚨 Reserved Seat A12 at sec 600!';
      if (buyer2Note) buyer2Note.textContent = 'Saw key expire and checked out. Stripe charged $280!';

      if (seatBox) {
        seatBox.style.borderColor = '#ef4444';
        seatBox.style.background = 'rgba(239, 68, 68, 0.3)';
        seatBox.style.boxShadow = '0 0 30px rgba(239, 68, 68, 0.8)';
      }
      if (seatOwner) {
        seatOwner.textContent = '💥 CONFLICT: SOLD TWICE ($560)';
        seatOwner.style.color = '#fca5a5';
        seatOwner.style.borderColor = '#ef4444';
      }

      if (expIcon) expIcon.textContent = '💥';
      if (expTitle) expTitle.textContent = 'Step 3 Disaster: Both Buyers Billed for 1 Seat!';
      if (expDesc) expDesc.textContent = 'Because Redis key expiration was not atomic with the DB transaction, both Buyer #1 and Buyer #2 were charged $280 for Seat A12. Venue security will turn away Buyer #2 at the gate!';

      if (outcomeCallout) outcomeCallout.className = 'outcome-callout bad';
      if (outcomeLabel) outcomeLabel.textContent = 'FLASH SALE DISASTER';
      if (outcomeTitle) outcomeTitle.textContent = 'DOUBLE-BOOKING &amp; CHARGEBACK DISASTER';
      if (outcomeSub) outcomeSub.textContent = 'Redis expiration lag caused Seat #A12 to be sold to Buyer #1 and Buyer #2 simultaneously ($560 total charged for 1 seat).';

      highlightCodeLine(10, true);

    } else {
      // FERRICSTORE FENCED OWNERSHIP WIN
      if (livePill) livePill.className = 'live-pill';
      if (liveStatus) liveStatus.textContent = '✓ FENCED SEAT HANDOFF COMPLETE';

      if (buyer1Card) buyer1Card.className = 'buyer-card buyer-1';
      if (buyer1Tag) { buyer1Tag.className = 'buyer-tag waiting'; buyer1Tag.textContent = '❌ Hold Expired (Unpaid)'; }
      if (buyer1Note) buyer1Note.textContent = 'Cart hold safely released with zero charge.';

      if (buyer2Card) buyer2Card.className = 'buyer-card buyer-2 is-winner';
      if (buyer2Tag) { buyer2Tag.className = 'buyer-tag winner'; buyer2Tag.textContent = '✓ SEAT ASSIGNED (NEW FENCE)'; }
      if (buyer2Status) buyer2Status.textContent = '🎉 Seat #A12 is in your cart!';
      if (buyer2Note) buyer2Note.textContent = 'Instant 10:00 checkout window opened for Sarah!';

      if (seatBox) {
        seatBox.style.borderColor = '#10b981';
        seatBox.style.background = 'rgba(16, 185, 129, 0.25)';
        seatBox.style.boxShadow = '0 0 25px rgba(16, 185, 129, 0.5)';
      }
      if (seatOwner) {
        seatOwner.textContent = '✓ Transferred to: Buyer #2';
        seatOwner.style.color = '#6ee7b7';
        seatOwner.style.borderColor = '#10b981';
      }

      if (expIcon) expIcon.textContent = '🎉';
      if (expTitle) expTitle.textContent = 'Step 3 Success: Fenced Seat Handoff';
      if (expDesc) expDesc.textContent = 'The expired hold advanced to Buyer #2 with a newer fence. A stale worker cannot overwrite that state; payment remains a separately guarded effect.';

      if (outcomeCallout) outcomeCallout.className = 'outcome-callout good';
      if (outcomeLabel) outcomeLabel.textContent = 'CONCURRENCY OUTCOME';
      if (outcomeTitle) outcomeTitle.textContent = 'FENCED OWNERSHIP TRANSFER';
      if (outcomeSub) outcomeSub.textContent = 'The hold expired and Buyer #2 acquired a newer ownership fence. Use provider idempotency for payment.';

      highlightCodeLine(11);
    }
  }

  function playStory() {
    clearAllTimeouts();
    runStep1();
    animTimeouts.push(setTimeout(function () {
      runStep2(function () {
        runStep3();
      });
    }, 1200));
  }

  // --- Mode Buttons ---
  modeButtons.forEach(function (btn) {
    btn.addEventListener('click', function () {
      modeButtons.forEach(function (b) { b.classList.remove('is-selected'); b.setAttribute('aria-selected', 'false'); });
      btn.classList.add('is-selected');
      btn.setAttribute('aria-selected', 'true');
      currentMode = btn.getAttribute('data-mode-btn') || 'after';
      document.body.setAttribute('data-mode', currentMode);
      updateActiveCodeBlock();
      playStory();
    });
  });

  // --- SDK Buttons ---
  sdkButtons.forEach(function (btn) {
    btn.addEventListener('click', function () {
      sdkButtons.forEach(function (b) { b.classList.remove('is-active'); });
      btn.classList.add('is-active');
      currentSdk = btn.getAttribute('data-sdk-style') || 'states';
      updateActiveCodeBlock();
    });
  });

  // --- Playback Buttons ---
  if (btnPlay) btnPlay.addEventListener('click', playStory);
  if (btnStep1) btnStep1.addEventListener('click', runStep1);
  if (btnStep2) btnStep2.addEventListener('click', function () { runStep2(); });
  if (btnStep3) btnStep3.addEventListener('click', runStep3);
  if (btnReset) btnReset.addEventListener('click', runStep1);

  // Init
  updateActiveCodeBlock();
  playStory();
})();
