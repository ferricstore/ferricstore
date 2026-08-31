(function () {
  'use strict';

  var currentMode = 'before';
  var animTimeouts = [];

  // DOM Elements
  var modeButtons = document.querySelectorAll('[data-mode-btn]');
  var codeBlocks = document.querySelectorAll('[data-code]');
  var codeTitle = document.querySelector('[data-code-title]');
  var codeKicker = document.querySelector('[data-code-kicker]');

  var metricSetup = document.querySelector('[data-metric-setup]');
  var metricSetupSub = document.querySelector('[data-metric-setup-sub]');
  var metricReclaim = document.querySelector('[data-metric-reclaim]');
  var metricReclaimSub = document.querySelector('[data-metric-reclaim-sub]');
  var metricDup = document.querySelector('[data-metric-dup]');
  var metricDupSub = document.querySelector('[data-metric-dup-sub]');
  var metricDur = document.querySelector('[data-metric-dur]');
  var metricDurSub = document.querySelector('[data-metric-dur-sub]');

  var enqueueCard = document.querySelector('[data-enqueue-card]');
  var enqueueVal = document.querySelector('[data-enqueue-val]');
  var enqueueBadge = document.querySelector('[data-enqueue-badge]');

  var stateCard = document.querySelector('[data-state-card]');
  var stateVal = document.querySelector('[data-state-val]');
  var stateBadge = document.querySelector('[data-state-badge]');
  var stateFill = document.querySelector('[data-state-fill]');
  var stateSub = document.querySelector('[data-state-sub]');

  var workerCard = document.querySelector('[data-worker-card]');
  var workerVal = document.querySelector('[data-worker-val]');
  var workerBadge = document.querySelector('[data-worker-badge]');
  var workerFill = document.querySelector('[data-worker-fill]');
  var workerSub = document.querySelector('[data-worker-sub]');

  var livePill = document.querySelector('[data-live-pill]');
  var liveStatus = document.querySelector('[data-live-status]');
  var termStream = document.querySelector('[data-term-stream]');

  var expIcon = document.querySelector('[data-exp-icon]');
  var expTitle = document.querySelector('[data-exp-title]');
  var expDesc = document.querySelector('[data-exp-desc]');

  var outcomeCallout = document.querySelector('[data-outcome-callout]');
  var outcomeLabel = document.querySelector('[data-outcome-label]');
  var outcomeTitle = document.querySelector('[data-outcome-title]');
  var outcomeSub = document.querySelector('[data-outcome-sub]');

  var btnClean = document.querySelector('[data-btn-clean]');
  var btnCrash = document.querySelector('[data-btn-crash]');
  var btnReclaim = document.querySelector('[data-btn-reclaim]');
  var btnReset = document.querySelector('[data-btn-reset]');

  function log(type, msg) {
    if (!termStream) return;
    var now = new Date();
    var ts = now.toTimeString().split(' ')[0] + '.' + String(now.getMilliseconds()).padStart(3, '0');
    var div = document.createElement('div');
    div.className = 'term-row ' + (type || 'info');
    div.innerHTML = '[' + ts + '] ' + msg;
    termStream.appendChild(div);
    termStream.scrollTop = termStream.scrollHeight;
  }

  function clearLogs() { if (termStream) termStream.innerHTML = ''; }
  function clearAllTimeouts() { animTimeouts.forEach(function (t) { clearTimeout(t); }); animTimeouts = []; }

  function highlightCodeLine(targetLine, isCrash) {
    var activeBlock = document.querySelector('[data-code="' + currentMode + '"]');
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
      block.hidden = block.getAttribute('data-code') !== currentMode;
    });

    if (currentMode === 'before') {
      if (codeTitle) codeTitle.textContent = 'celery_tasks.py';
      if (codeKicker) codeKicker.textContent = 'CELERY + RABBITMQ + REDIS + SQS';
    } else {
      if (codeTitle) codeTitle.textContent = 'queue_worker.py';
      if (codeKicker) codeKicker.textContent = 'FERRICSTORE QUEUE CLIENT API';
    }
  }

  // --- ACTION 1: Clean Run (Enqueue -> Claim -> Complete) ---
  function runCleanJob() {
    clearLogs();
    clearAllTimeouts();
    updateActiveCodeBlock();

    if (livePill) livePill.className = 'live-pill';
    if (liveStatus) liveStatus.textContent = '1. ENQUEUED IN RAFT LOG';

    log('info', 'FLOW.CREATE email-8492 TYPE email PAYLOAD "welcome:user_42"...');

    if (stateCard) stateCard.className = 'q-card';
    if (stateVal) stateVal.textContent = 'STATE: queued';
    if (stateBadge) { stateBadge.className = 'q-badge ok'; stateBadge.textContent = '✓ Raft Committed'; }
    if (stateFill) stateFill.style.width = '33%';

    if (workerCard) workerCard.className = 'q-card';
    if (workerVal) workerVal.textContent = 'Worker Pool Idle';
    if (workerBadge) { workerBadge.className = 'q-badge ok'; workerBadge.textContent = 'Waiting for Work'; }
    if (workerFill) workerFill.style.width = '0%';

    highlightCodeLine(currentMode === 'before' ? 3 : 7);

    animTimeouts.push(setTimeout(function () {
      if (liveStatus) liveStatus.textContent = '2. CLAIMED BY WORKER #1';
      log('cyan', 'FLOW.CLAIM_DUE ➔ Worker #1 claimed email-8492 (Lease Token: #101, Fencing: #101)...');

      if (stateVal) stateVal.textContent = 'STATE: processing';
      if (stateBadge) { stateBadge.className = 'q-badge ok'; stateBadge.textContent = 'Lease: 30s Active'; }
      if (stateFill) stateFill.style.width = '66%';

      if (workerVal) workerVal.textContent = 'Worker #1 Active';
      if (workerBadge) { workerBadge.className = 'q-badge ok'; workerBadge.textContent = 'Fencing Token: 101'; }
      if (workerFill) workerFill.style.width = '66%';

      highlightCodeLine(currentMode === 'before' ? 7 : 11);
    }, 450));

    animTimeouts.push(setTimeout(function () {
      if (liveStatus) liveStatus.textContent = '3. COMPLETED DURABLY';
      log('success', '✓ FLOW.COMPLETE email-8492 RESULT "sent" (completion persisted).');

      if (stateVal) stateVal.textContent = 'STATE: completed';
      if (stateBadge) { stateBadge.className = 'q-badge ok'; stateBadge.textContent = '✓ Done'; }
      if (stateFill) stateFill.style.width = '100%';

      if (workerVal) workerVal.textContent = 'Worker #1 Idle';
      if (workerBadge) { workerBadge.className = 'q-badge ok'; workerBadge.textContent = 'Lease Released'; }
      if (workerFill) workerFill.style.width = '100%';

      if (expIcon) expIcon.textContent = '⚡';
      if (expTitle) expTitle.textContent = 'Background Job Completed Durably';
      if (expDesc) expDesc.textContent = 'Worker #1 claimed the job, dispatched the email, and acknowledged completion. Click "💥 2. Worker Crash" to simulate what happens during a server failure!';

      if (outcomeCallout) outcomeCallout.className = 'outcome-callout good';
      if (outcomeLabel) outcomeLabel.textContent = 'QUEUE EXECUTION OUTCOME';
      if (outcomeTitle) outcomeTitle.textContent = 'CLEAN 1-ROUNDTRIP EXECUTION';
      if (outcomeSub) outcomeSub.textContent = 'Job enqueued, claimed, and completed. External effects still require a stable idempotency key.';

      highlightCodeLine(currentMode === 'before' ? 7 : 12);
    }, 950));
  }

  // --- ACTION 2: Worker Crash Mid-Job ---
  function runWorkerCrash() {
    clearLogs();
    clearAllTimeouts();
    updateActiveCodeBlock();

    log('info', 'Worker #1 claimed email-8492 and began processing...');
    log('danger', '💥 [SIMULATED FAILURE] Worker #1 process killed (SIGKILL / Out-Of-Memory reboot)!');

    if (currentMode === 'before') {
      if (livePill) livePill.className = 'live-pill is-crash';
      if (liveStatus) liveStatus.textContent = '💥 CELERY WORKER CRASHED';

      log('danger', '⏳ [SQS TIMEOUT LAG] SQS message locked for 300 seconds (5 minutes) before retry.');
      log('danger', '🧟 [ZOMBIE RISK] If Worker #1 wakes up, customer will be charged TWICE.');

      if (stateCard) stateCard.className = 'q-card is-tripped';
      if (stateVal) stateVal.textContent = 'STATE: STUCK (5m Delay)';
      if (stateBadge) { stateBadge.className = 'q-badge tripped'; stateBadge.textContent = '🚨 300s Visibility Lag'; }

      if (workerCard) workerCard.className = 'q-card is-tripped';
      if (workerVal) workerVal.textContent = 'Worker #1 DEAD';
      if (workerBadge) { workerBadge.className = 'q-badge tripped'; workerBadge.textContent = 'No Fencing Token'; }

      if (expIcon) expIcon.textContent = '💥';
      if (expTitle) expTitle.textContent = 'Celery / SQS Visibility Timeout Lag';
      if (expDesc) expDesc.textContent = 'Because SQS relies on long visibility timeouts, the crashed job remains locked in limbo for 5 minutes, causing massive queue backlog!';

      if (outcomeCallout) outcomeCallout.className = 'outcome-callout bad';
      if (outcomeLabel) outcomeLabel.textContent = 'TRADITIONAL QUEUE FAILURE';
      if (outcomeTitle) outcomeTitle.textContent = '5-MINUTE DELAY &amp; ZOMBIE DUPLICATION';
      if (outcomeSub) outcomeSub.textContent = 'Worker crashed mid-job. Message is locked for 300s. Unmanaged retries cause duplicate customer charges.';

      highlightCodeLine(5, true);

    } else {
      if (livePill) livePill.className = 'live-pill is-crash';
      if (liveStatus) liveStatus.textContent = '💥 WORKER #1 CRASHED (LEASE EXPIRED)';

      log('warn', '⚠️ [HEARTBEAT LAPSED] Worker #1 heartbeat timed out. FerricStore flagged lease for immediate reclaim.');

      if (stateCard) stateCard.className = 'q-card is-warning';
      if (stateVal) stateVal.textContent = 'STATE: DUE_FOR_RECLAIM';
      if (stateBadge) { stateBadge.className = 'q-badge warn'; stateBadge.textContent = '⚡ Eligible after lease expiry'; }

      if (workerCard) workerCard.className = 'q-card is-tripped';
      if (workerVal) workerVal.textContent = 'Worker #1 Crashed';
      if (workerBadge) { workerBadge.className = 'q-badge tripped'; workerBadge.textContent = 'Token 101 Revoked'; }

      if (expIcon) expIcon.textContent = '🛡️';
      if (expTitle) expTitle.textContent = 'Worker #1 Crashed: Token 101 Revoked';
      if (expDesc) expDesc.textContent = 'After Worker #1\'s lease expires, the job becomes claimable. Click "🛡️ 3. Fenced Retry" to give Worker #2 a newer fence.';

      highlightCodeLine(9);
    }
  }

  // --- ACTION 3: Fenced Auto-Reclaim & Retry ---
  function runFencedRetry() {
    clearLogs();
    clearAllTimeouts();
    updateActiveCodeBlock();

    if (currentMode === 'before') {
      log('danger', '💥 In Celery, SQS still has 240 seconds left on visibility timeout...');
    } else {
      if (livePill) livePill.className = 'live-pill';
      if (liveStatus) liveStatus.textContent = '✓ RECLAIMED BY WORKER #2';

      log('cyan', 'FLOW.CLAIM_DUE ➔ Worker #2 claimed the eligible job with new Fencing Token #102.');
      log('info', 'Worker #2 re-executes email dispatch safely...');

      if (stateCard) stateCard.className = 'q-card';
      if (stateVal) stateVal.textContent = 'STATE: retrying (Worker #2)';
      if (stateBadge) { stateBadge.className = 'q-badge ok'; stateBadge.textContent = 'Lease #102 Active'; }
      if (stateFill) stateFill.style.width = '75%';

      if (workerCard) workerCard.className = 'q-card';
      if (workerVal) workerVal.textContent = 'Worker #2 Active';
      if (workerBadge) { workerBadge.className = 'q-badge ok'; workerBadge.textContent = 'Fencing Token: 102'; }
      if (workerFill) workerFill.style.width = '80%';

      animTimeouts.push(setTimeout(function () {
        log('success', '✓ FLOW.COMPLETE email-8492 FENCING 102 ➔ Completed!');
        log('cyan', '🛡️ [FENCING TEST] Stale Worker #1 woke up with Token 101 ➔ Engine REJECTED stale write!');

        if (stateVal) stateVal.textContent = 'STATE: completed';
        if (stateBadge) { stateBadge.className = 'q-badge ok'; stateBadge.textContent = '✓ Durable'; }
        if (stateFill) stateFill.style.width = '100%';

        if (liveStatus) liveStatus.textContent = '✓ SAFE COMPLETION (0 DUPLICATES)';

        if (expIcon) expIcon.textContent = '🛡️';
        if (expTitle) expTitle.textContent = 'Fenced Recovery After Lease Expiry';
        if (expDesc) expDesc.textContent = 'Worker #2 completed the job with Fencing Token 102. When the zombie Worker #1 tried to write with stale Token 101, FerricStore rejected it, preventing duplicate customer charges!';

        if (outcomeCallout) outcomeCallout.className = 'outcome-callout good';
        if (outcomeLabel) outcomeLabel.textContent = 'FENCING LEASE OUTCOME';
        if (outcomeTitle) outcomeTitle.textContent = 'FENCED RESUMPTION + GUARDED EFFECT';
        if (outcomeSub) outcomeSub.textContent = 'Worker #2 safely finished the job. Stale zombie write from Worker #1 was blocked by monotonic Raft fencing tokens.';

        highlightCodeLine(12);
      }, 650));
    }
  }

  // --- Mode Buttons ---
  modeButtons.forEach(function (btn) {
    btn.addEventListener('click', function () {
      modeButtons.forEach(function (b) { b.classList.remove('is-selected'); b.setAttribute('aria-selected', 'false'); });
      btn.classList.add('is-selected');
      btn.setAttribute('aria-selected', 'true');
      currentMode = btn.getAttribute('data-mode-btn') || 'after';
      document.body.setAttribute('data-mode', currentMode);
      runCleanJob();
    });
  });

  // --- Playback Buttons ---
  if (btnClean) btnClean.addEventListener('click', runCleanJob);
  if (btnCrash) btnCrash.addEventListener('click', runWorkerCrash);
  if (btnReclaim) btnReclaim.addEventListener('click', runFencedRetry);
  if (btnReset) btnReset.addEventListener('click', runCleanJob);

  // Init
  runCleanJob();
})();
