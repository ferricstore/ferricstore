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
  var enqueueDetail = document.querySelector('[data-enqueue-detail]');
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

  function setActionAvailability(clean, crash, reclaim) {
    if (btnClean) btnClean.disabled = !clean;
    if (btnCrash) btnCrash.disabled = !crash;
    if (btnReclaim) btnReclaim.disabled = !reclaim;
  }

  function updateModeMetrics() {
    if (currentMode === 'before') {
      if (metricSetup) metricSetup.textContent = 'Several moving parts';
      if (metricSetupSub) metricSetupSub.textContent = 'Example: Celery + SQS + worker';
      if (metricReclaim) metricReclaim.textContent = 'Configured wait';
      if (metricReclaimSub) metricReclaimSub.textContent = 'Example configuration: 300 seconds';
      if (metricDup) metricDup.textContent = 'Needs an outside guard';
      if (metricDupSub) metricDupSub.textContent = 'An email request may be tried again';
      if (metricDur) metricDur.textContent = 'Depends on setup';
      if (metricDurSub) metricDurSub.textContent = 'Retention and acknowledgements vary';
      [metricSetup, metricReclaim, metricDup, metricDur].forEach(function (metric) {
        if (metric) metric.className = 'metric-val text-red';
      });
    } else {
      if (metricSetup) metricSetup.textContent = 'One engine';
      if (metricSetupSub) metricSetupSub.textContent = 'Example configuration: one binary (<20MB)';
      if (metricReclaim) metricReclaim.textContent = 'After lease expiry';
      if (metricReclaimSub) metricReclaimSub.textContent = 'The active claim duration is configurable';
      if (metricDup) metricDup.textContent = 'Old state writes rejected';
      if (metricDupSub) metricDupSub.textContent = 'Outside effects still need a stable key';
      if (metricDur) metricDur.textContent = 'Durable queue state';
      if (metricDurSub) metricDurSub.textContent = 'Depends on configured topology';
      if (metricSetup) metricSetup.className = 'metric-val text-green';
      if (metricReclaim) metricReclaim.className = 'metric-val text-cyan';
      if (metricDup) metricDup.className = 'metric-val text-green';
      if (metricDur) metricDur.className = 'metric-val text-green';
    }
  }

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
      if (codeKicker) codeKicker.textContent = 'WITHOUT FERRICSTORE · EXAMPLE QUEUE SETUP';
    } else {
      if (codeTitle) codeTitle.textContent = 'queue_worker.py';
      if (codeKicker) codeKicker.textContent = 'FERRICSTORE QUEUE CLIENT API';
    }
  }

  function resetSimulation() {
    clearLogs();
    clearAllTimeouts();
    updateActiveCodeBlock();
    updateModeMetrics();
    setActionAvailability(true, false, false);

    if (livePill) livePill.className = 'live-pill';
    if (liveStatus) liveStatus.textContent = 'READY TO RUN';
    if (enqueueVal) enqueueVal.textContent = 'email-8492';
    if (enqueueCard) enqueueCard.className = 'q-card';
    if (stateCard) stateCard.className = 'q-card';
    if (workerCard) workerCard.className = 'q-card';
    if (stateFill) stateFill.style.width = '0%';
    if (workerFill) workerFill.style.width = '0%';
    highlightCodeLine();

    if (currentMode === 'before') {
      if (enqueueDetail) enqueueDetail.textContent = 'Simple queue setup';
      if (enqueueBadge) { enqueueBadge.className = 'q-badge ok'; enqueueBadge.textContent = 'Example email'; }
      if (stateSub) stateSub.textContent = 'Waiting list';
      if (stateVal) stateVal.textContent = 'Ready to add';
      if (stateBadge) { stateBadge.className = 'q-badge ok'; stateBadge.textContent = 'No job yet'; }
      if (workerSub) workerSub.textContent = 'Waiting for a job';
      if (workerVal) workerVal.textContent = 'Worker #1 ready';
      if (workerBadge) { workerBadge.className = 'q-badge ok'; workerBadge.textContent = 'No job held'; }
      if (expIcon) expIcon.textContent = '💡';
      if (expTitle) expTitle.textContent = 'Preview: the job is waiting';
      if (expDesc) expDesc.textContent = 'Choose this mode to inspect a simplified queue setup. Run step 1 to add the email, then step 2 to stop the worker.';
      if (outcomeCallout) outcomeCallout.className = 'outcome-callout good';
      if (outcomeLabel) outcomeLabel.textContent = 'What this run shows';
      if (outcomeTitle) outcomeTitle.textContent = 'Ready to compare the two queue setups';
      if (outcomeSub) outcomeSub.textContent = 'Run the named actions to see who holds the job, why it waits, and whether a replacement worker can try it.';
    } else {
      if (enqueueDetail) enqueueDetail.textContent = 'FLOW.CREATE command';
      if (enqueueBadge) { enqueueBadge.className = 'q-badge ok'; enqueueBadge.textContent = 'Stable key included'; }
      if (stateSub) stateSub.textContent = 'Raft-committed state';
      if (stateVal) stateVal.textContent = 'Ready to add';
      if (stateBadge) { stateBadge.className = 'q-badge ok'; stateBadge.textContent = 'Not saved yet'; }
      if (workerSub) workerSub.textContent = 'Lease + state guard';
      if (workerVal) workerVal.textContent = 'Worker #1 ready';
      if (workerBadge) { workerBadge.className = 'q-badge ok'; workerBadge.textContent = 'No lease'; }
      if (expIcon) expIcon.textContent = '🛡️';
      if (expTitle) expTitle.textContent = 'Preview: ready for a durable queue run';
      if (expDesc) expDesc.textContent = 'Run step 1 to save the job, let Worker #1 claim it, and inspect the completion. A stable key still protects the outside email request.';
      if (outcomeCallout) outcomeCallout.className = 'outcome-callout good';
      if (outcomeLabel) outcomeLabel.textContent = 'What this run shows';
      if (outcomeTitle) outcomeTitle.textContent = 'Ready to run With FerricStore';
      if (outcomeSub) outcomeSub.textContent = 'The run will show saved state, a temporary worker claim, and safe recovery after that claim expires.';
    }
  }

  // --- ACTION 1: Clean Run (Enqueue -> Claim -> Complete) ---
  function runCleanJob() {
    clearLogs();
    clearAllTimeouts();
    updateActiveCodeBlock();
    updateModeMetrics();
    setActionAvailability(false, true, false);

    if (livePill) livePill.className = 'live-pill';
    if (liveStatus) liveStatus.textContent = '1. ADDED TO QUEUE';
    if (enqueueCard) enqueueCard.className = 'q-card';
    if (stateCard) stateCard.className = 'q-card';
    if (workerCard) workerCard.className = 'q-card';
    if (stateFill) stateFill.style.width = '33%';
    if (workerFill) workerFill.style.width = '0%';
    if (stateVal) stateVal.textContent = 'STATE: queued';
    if (workerVal) workerVal.textContent = 'Worker #1 ready';
    if (workerBadge) { workerBadge.className = 'q-badge ok'; workerBadge.textContent = 'Waiting for job'; }
    if (enqueueVal) enqueueVal.textContent = 'email-8492';

    if (currentMode === 'before') {
      if (enqueueDetail) enqueueDetail.textContent = 'Simple queue setup';
      if (enqueueBadge) { enqueueBadge.className = 'q-badge ok'; enqueueBadge.textContent = 'Added'; }
      if (stateSub) stateSub.textContent = 'Waiting list';
      if (stateBadge) { stateBadge.className = 'q-badge ok'; stateBadge.textContent = 'Waiting for worker'; }
      if (workerSub) workerSub.textContent = 'Waiting for a job';
      log('info', 'Added email-8492 to the example queue with payload "welcome:user_42"...');
    } else {
      if (enqueueDetail) enqueueDetail.textContent = 'FLOW.CREATE command';
      if (enqueueBadge) { enqueueBadge.className = 'q-badge ok'; enqueueBadge.textContent = 'Stable key included'; }
      if (stateSub) stateSub.textContent = 'Raft-committed state';
      if (stateBadge) { stateBadge.className = 'q-badge ok'; stateBadge.textContent = 'State saved'; }
      if (workerSub) workerSub.textContent = 'Lease + state guard';
      if (liveStatus) liveStatus.textContent = '1. ENQUEUED IN RAFT LOG';
      log('info', 'FLOW.CREATE email-8492 TYPE email PAYLOAD "welcome:user_42"...');
    }

    highlightCodeLine(currentMode === 'before' ? 3 : 7);

    animTimeouts.push(setTimeout(function () {
      if (liveStatus) liveStatus.textContent = '2. CLAIMED BY WORKER #1';
      log('cyan', currentMode === 'before'
        ? 'Worker #1 picked up email-8492 and began processing.'
        : 'FLOW.CLAIM_DUE ➔ Worker #1 claimed email-8492 (Lease Token: #101, Fencing: #101)...');

      if (stateVal) stateVal.textContent = 'STATE: processing';
      if (stateBadge) { stateBadge.className = 'q-badge ok'; stateBadge.textContent = currentMode === 'before' ? 'Worker is processing' : 'Lease: 30s Active'; }
      if (stateFill) stateFill.style.width = '66%';

      if (workerVal) workerVal.textContent = 'Worker #1 active';
      if (workerBadge) { workerBadge.className = 'q-badge ok'; workerBadge.textContent = currentMode === 'before' ? 'Holding job' : 'Fencing Token: 101'; }
      if (workerFill) workerFill.style.width = '66%';

      highlightCodeLine(currentMode === 'before' ? 7 : 11);
    }, 450));

    animTimeouts.push(setTimeout(function () {
      if (liveStatus) liveStatus.textContent = currentMode === 'before' ? '3. JOB COMPLETED' : '3. COMPLETED DURABLY';
      log('success', currentMode === 'before'
        ? '✓ Worker #1 finished email-8492 (result "sent").'
        : '✓ FLOW.COMPLETE email-8492 RESULT "sent" (completion persisted).');

      if (stateVal) stateVal.textContent = 'STATE: completed';
      if (stateBadge) { stateBadge.className = 'q-badge ok'; stateBadge.textContent = '✓ Done'; }
      if (stateFill) stateFill.style.width = '100%';

      if (workerVal) workerVal.textContent = 'Worker #1 idle';
      if (workerBadge) { workerBadge.className = 'q-badge ok'; workerBadge.textContent = currentMode === 'before' ? 'Job released' : 'Lease Released'; }
      if (workerFill) workerFill.style.width = '100%';

      if (expIcon) expIcon.textContent = '⚡';
      if (expTitle) expTitle.textContent = currentMode === 'before' ? 'The example job completed' : 'Background job completed durably';
      if (expDesc) expDesc.textContent = currentMode === 'before'
        ? 'Worker #1 finished the email example. Click "💥 2. Simulate worker crash" to inspect what this setup waits for.'
        : 'Worker #1 claimed the job, dispatched the email, and acknowledged completion. Click "💥 2. Simulate worker crash" to inspect recovery.';

      if (outcomeCallout) outcomeCallout.className = 'outcome-callout good';
      if (outcomeLabel) outcomeLabel.textContent = currentMode === 'before' ? 'WITHOUT FERRICSTORE · RUN OUTCOME' : 'WITH FERRICSTORE · RUN OUTCOME';
      if (outcomeTitle) outcomeTitle.textContent = currentMode === 'before' ? 'JOB COMPLETED IN THE EXAMPLE QUEUE' : 'JOB SAVED, CLAIMED, AND COMPLETED';
      if (outcomeSub) outcomeSub.textContent = currentMode === 'before'
        ? 'The worker finished the example email. A later retry can repeat the outside request, so keep a stable idempotency key.'
        : 'The job was enqueued, claimed, and completed. External effects still require a stable idempotency key.';

      highlightCodeLine(currentMode === 'before' ? 7 : 12);
      setActionAvailability(true, true, false);
    }, 950));
  }

  // --- ACTION 2: Worker Crash Mid-Job ---
  function runWorkerCrash() {
    clearLogs();
    clearAllTimeouts();
    updateActiveCodeBlock();
    updateModeMetrics();
    setActionAvailability(false, false, true);

    log('info', 'Worker #1 claimed email-8492 and began processing...');
    log('danger', '💥 [SIMULATED FAILURE] Worker #1 process stopped (SIGKILL / Out-Of-Memory reboot)!');

    if (currentMode === 'before') {
      if (livePill) livePill.className = 'live-pill is-crash';
      if (liveStatus) liveStatus.textContent = '2. WORKER #1 CRASHED';

      log('danger', '⏳ [EXAMPLE VISIBILITY SETTING] Message waits up to 300 seconds (5 minutes) before retry.');
      log('danger', 'The email request may be attempted again after the wait; the provider needs a stable key.');

      if (stateCard) stateCard.className = 'q-card is-tripped';
      if (stateVal) stateVal.textContent = 'STATE: waiting for retry';
      if (stateBadge) { stateBadge.className = 'q-badge tripped'; stateBadge.textContent = 'Example: 300s wait'; }

      if (workerCard) workerCard.className = 'q-card is-tripped';
      if (workerVal) workerVal.textContent = 'Worker #1 stopped';
      if (workerBadge) { workerBadge.className = 'q-badge tripped'; workerBadge.textContent = 'No state-write guard'; }

      if (expIcon) expIcon.textContent = '💥';
      if (expTitle) expTitle.textContent = 'The worker stopped; the job is waiting';
      if (expDesc) expDesc.textContent = 'In this example, the queue waits for the configured 300-second visibility setting. Step 3 stands in for that wait and lets Worker #2 try the job.';

      if (outcomeCallout) outcomeCallout.className = 'outcome-callout bad';
      if (outcomeLabel) outcomeLabel.textContent = 'WITHOUT FERRICSTORE · CRASH PATH';
      if (outcomeTitle) outcomeTitle.textContent = 'WAITING FOR THE EXAMPLE VISIBILITY TIMEOUT';
      if (outcomeSub) outcomeSub.textContent = 'Worker #1 stopped while processing. The message waits for the configured 300s example setting; an outside email request still needs a stable key.';

      highlightCodeLine(5, true);

    } else {
      if (livePill) livePill.className = 'live-pill is-crash';
      if (liveStatus) liveStatus.textContent = '💥 WORKER #1 CRASHED (LEASE EXPIRED)';

      log('warn', '⚠️ [LEASE EXPIRED] Worker #1 stopped. The job can be claimed again after the active lease ends.');

      if (stateCard) stateCard.className = 'q-card is-warning';
      if (stateVal) stateVal.textContent = 'STATE: waiting for reclaim';
      if (stateBadge) { stateBadge.className = 'q-badge warn'; stateBadge.textContent = 'Retry after lease expiry'; }

      if (workerCard) workerCard.className = 'q-card is-tripped';
      if (workerVal) workerVal.textContent = 'Worker #1 stopped';
      if (workerBadge) { workerBadge.className = 'q-badge tripped'; workerBadge.textContent = 'Claim 101 ended'; }

      if (expIcon) expIcon.textContent = '🛡️';
      if (expTitle) expTitle.textContent = 'The worker stopped; the job can be claimed again';
      if (expDesc) expDesc.textContent = 'After Worker #1\'s lease expires, the job becomes claimable. Click "🛡️ 3. Run recovery retry" to let Worker #2 use a newer lease number.';

      if (outcomeCallout) outcomeCallout.className = 'outcome-callout bad';
      if (outcomeLabel) outcomeLabel.textContent = 'WITH FERRICSTORE · CRASH PATH';
      if (outcomeTitle) outcomeTitle.textContent = 'WAITING FOR THE ACTIVE LEASE TO END';
      if (outcomeSub) outcomeSub.textContent = 'The job is still saved. After the active lease expires, another worker can claim it with a newer number while an old state write is rejected.';

      highlightCodeLine(9);
    }
  }

  // --- ACTION 3: Fenced Auto-Reclaim & Retry ---
  function runFencedRetry() {
    clearLogs();
    clearAllTimeouts();
    updateActiveCodeBlock();
    updateModeMetrics();
    setActionAvailability(true, false, false);

    if (currentMode === 'before') {
      if (livePill) livePill.className = 'live-pill is-crash';
      if (liveStatus) liveStatus.textContent = '3. RECLAIMED BY WORKER #2';
      log('warn', 'Example timing note: an early retry could still have 240 seconds left on the 300-second visibility setting.');
      log('info', 'Worker #2 can try email-8492 after the configured wait.');
      if (stateCard) stateCard.className = 'q-card is-warning';
      if (stateVal) stateVal.textContent = 'STATE: retrying';
      if (stateBadge) { stateBadge.className = 'q-badge warn'; stateBadge.textContent = 'Wait represented by step 3'; }
      if (workerCard) workerCard.className = 'q-card';
      if (workerVal) workerVal.textContent = 'Worker #2 active';
      if (workerBadge) { workerBadge.className = 'q-badge warn'; workerBadge.textContent = 'New attempt'; }
      if (workerFill) workerFill.style.width = '80%';
      if (expIcon) expIcon.textContent = '↻';
      if (expTitle) expTitle.textContent = 'A replacement worker can try the job';
      if (expDesc) expDesc.textContent = 'This simple setup hands the email job to Worker #2 after the configured wait. It does not fence a late Worker #1 state write, so the outside provider still needs the stable key.';
      if (outcomeCallout) outcomeCallout.className = 'outcome-callout bad';
      if (outcomeLabel) outcomeLabel.textContent = 'WITHOUT FERRICSTORE · RETRY OUTCOME';
      if (outcomeTitle) outcomeTitle.textContent = 'RETRY FINISHED, EXTERNAL GUARD STILL NEEDED';
      if (outcomeSub) outcomeSub.textContent = 'Worker #2 can finish the queue attempt after the example wait. A stable idempotency key keeps a repeated email request from becoming a second outside effect.';
      animTimeouts.push(setTimeout(function () {
        if (liveStatus) liveStatus.textContent = '3. JOB COMPLETED (RETRY FINISHED)';
        log('success', '✓ Worker #2 finished email-8492 (result "sent").');
        if (stateVal) stateVal.textContent = 'STATE: completed';
        if (stateBadge) { stateBadge.className = 'q-badge ok'; stateBadge.textContent = 'Retry finished'; }
        if (workerVal) workerVal.textContent = 'Worker #2 idle';
        if (workerBadge) { workerBadge.className = 'q-badge ok'; workerBadge.textContent = 'Job released'; }
        if (workerFill) workerFill.style.width = '100%';
        if (expTitle) expTitle.textContent = 'The retry finished; guard the outside request';
        if (expDesc) expDesc.textContent = 'The queue completed its second attempt. The email provider still needs the same stable key if Worker #1 may have reached it before stopping.';
        highlightCodeLine(7);
      }, 650));
    } else {
      if (livePill) livePill.className = 'live-pill';
      if (liveStatus) liveStatus.textContent = '✓ RECLAIMED BY WORKER #2';

      log('cyan', 'FLOW.CLAIM_DUE ➔ Worker #2 claimed the eligible job with newer lease number #102.');
      log('info', 'Worker #2 tries the email request with the same stable key...');

      if (stateCard) stateCard.className = 'q-card';
      if (stateVal) stateVal.textContent = 'STATE: retrying (Worker #2)';
      if (stateBadge) { stateBadge.className = 'q-badge ok'; stateBadge.textContent = 'Lease #102 active'; }
      if (stateFill) stateFill.style.width = '75%';

      if (workerCard) workerCard.className = 'q-card';
      if (workerVal) workerVal.textContent = 'Worker #2 active';
      if (workerBadge) { workerBadge.className = 'q-badge ok'; workerBadge.textContent = 'Lease number: 102'; }
      if (workerFill) workerFill.style.width = '80%';

      animTimeouts.push(setTimeout(function () {
        log('success', '✓ FLOW.COMPLETE email-8492 FENCING 102 ➔ Completed!');
        log('cyan', '🛡️ [STATE GUARD] Worker #1 woke up with old number 101 ➔ Engine rejected the stale state write.');

        if (stateVal) stateVal.textContent = 'STATE: completed';
        if (stateBadge) { stateBadge.className = 'q-badge ok'; stateBadge.textContent = '✓ State saved'; }
        if (stateFill) stateFill.style.width = '100%';

        if (liveStatus) liveStatus.textContent = '✓ SAFE COMPLETION (0 DUPLICATES)';

        if (expIcon) expIcon.textContent = '🛡️';
        if (expTitle) expTitle.textContent = 'Recovery after the lease expired';
        if (expDesc) expDesc.textContent = 'Worker #2 completed the job with lease number 102. When Worker #1 tried to save with old number 101, FerricStore rejected that stale state write; the outside email still relies on the stable key.';

        if (outcomeCallout) outcomeCallout.className = 'outcome-callout good';
        if (outcomeLabel) outcomeLabel.textContent = 'WITH FERRICSTORE · RECOVERY OUTCOME';
        if (outcomeTitle) outcomeTitle.textContent = 'NEW WORKER FINISHED; OLD STATE WRITE BLOCKED';
        if (outcomeSub) outcomeSub.textContent = 'Worker #2 finished with lease number 102. FerricStore rejected Worker #1\'s stale state write; the outside email uses the same stable key across attempts.';

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
      resetSimulation();
    });
  });

  // --- Playback Buttons ---
  if (btnClean) btnClean.addEventListener('click', runCleanJob);
  if (btnCrash) btnCrash.addEventListener('click', runWorkerCrash);
  if (btnReclaim) btnReclaim.addEventListener('click', runFencedRetry);
  if (btnReset) btnReset.addEventListener('click', resetSimulation);

  // Init
  resetSimulation();
})();
