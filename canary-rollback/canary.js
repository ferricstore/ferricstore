(function () {
  'use strict';

  var currentMode = 'before';
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

  var envoyCard = document.querySelector('[data-envoy-card]');
  var envoyVal = document.querySelector('[data-envoy-val]');
  var envoyBadge = document.querySelector('[data-envoy-badge]');
  var envoyFill = document.querySelector('[data-envoy-fill]');
  var envoySub = document.querySelector('[data-envoy-sub]');

  var soakCard = document.querySelector('[data-soak-card]');
  var soakVal = document.querySelector('[data-soak-val]');
  var soakBadge = document.querySelector('[data-soak-badge]');
  var soakFill = document.querySelector('[data-soak-fill]');
  var soakSub = document.querySelector('[data-soak-sub]');

  var datadogCard = document.querySelector('[data-datadog-card]');
  var datadogVal = document.querySelector('[data-datadog-val]');
  var datadogBadge = document.querySelector('[data-datadog-badge]');
  var datadogFill = document.querySelector('[data-datadog-fill]');

  var expIcon = document.querySelector('[data-exp-icon]');
  var expTitle = document.querySelector('[data-exp-title]');
  var expDesc = document.querySelector('[data-exp-desc]');

  var livePill = document.querySelector('[data-live-pill]');
  var liveStatus = document.querySelector('[data-live-status]');

  var metricCpu = document.querySelector('[data-metric-cpu]');
  var metricCrash = document.querySelector('[data-metric-crash]');
  var metricErrors = document.querySelector('[data-metric-errors]');
  var metricRollback = document.querySelector('[data-metric-rollback]');

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
      if (codeTitle) codeTitle.textContent = 'deploy_script.py';
      if (codeKicker) codeKicker.textContent = 'UNMANAGED TIME.SLEEP (FRAGILE)';
      if (codeBadge) { codeBadge.textContent = 'VOLATILE SCRIPT'; codeBadge.style.background = 'rgba(239,68,68,0.25)'; codeBadge.style.color = '#fca5a5'; }
      if (step3Text) step3Text.textContent = 'Broken Canary Stuck Live';
    } else {
      if (codeTitle) codeTitle.textContent = currentSdk === 'steps' ? 'canary_step.py' : 'canary_fsm.py';
      if (codeKicker) codeKicker.textContent = currentSdk === 'steps' ? 'DURABLE STEP API' : 'FERRICSTORE STATE HANDLERS';
      if (codeBadge) { codeBadge.textContent = 'DURABLE CANARY'; codeBadge.style.background = 'rgba(16,185,129,0.25)'; codeBadge.style.color = '#6ee7b7'; }
      if (step3Text) step3Text.textContent = 'Datadog 5xx ➔ rollback state';
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

  // --- STEP 1: Route 10% Canary ---
  function runStep1() {
    clearAllTimeouts();
    updateStepperUI(1);

    if (livePill) livePill.className = 'live-pill';
    if (liveStatus) liveStatus.textContent = 'STEP 1: 10% CANARY LIVE (24H SOAK)';

    // Envoy
    if (envoyCard) envoyCard.className = 'canary-card';
    if (envoyVal) envoyVal.textContent = '10% Canary (v2.4.0)';
    if (envoyBadge) { envoyBadge.className = 'c-badge canary'; envoyBadge.textContent = '10% v2.4 / 90% v2.3'; }
    if (envoyFill) envoyFill.style.width = '10%';
    if (envoySub) envoySub.textContent = 'Routing live user traffic';

    // Soak Timer
    if (soakCard) soakCard.className = 'canary-card';
    if (soakVal) soakVal.textContent = '0h 01m / 24h 00m (Soak Started)';
    if (soakBadge) { soakBadge.className = 'c-badge ok'; soakBadge.textContent = currentMode === 'before' ? '⚠️ Blocking Thread (illustrative)' : '💤 Durable observation state'; }
    if (soakFill) soakFill.style.width = '2%';
    if (soakSub) soakSub.textContent = currentMode === 'before' ? 'Holding open worker thread in memory' : 'No application handler held while waiting';

    // Datadog
    if (datadogCard) datadogCard.className = 'canary-card';
    if (datadogVal) datadogVal.textContent = '0.02% (Healthy)';
    if (datadogBadge) { datadogBadge.className = 'c-badge ok'; datadogBadge.textContent = '✓ Normal Health'; }
    if (datadogFill) { datadogFill.style.width = '4%'; datadogFill.style.background = 'linear-gradient(90deg, #10b981, #0284c7)'; }

    // Explanation
    if (expIcon) expIcon.textContent = '🚀';
    if (expTitle) expTitle.textContent = 'Step 1: Envoy shifts 10% traffic to v2.4.0 (24h Soak Started)';
    if (expDesc) expDesc.textContent = currentMode === 'before'
      ? 'Calling time.sleep(86400) blocks an OS thread for 24 hours. Click "2. Crash at Hr 18" to simulate a server reboot.'
      : 'The flow persists an observation state and later advances on a schedule or durable signal. Click "2. Crash at Hr 18" to test reclaim.';

    // Metrics
    if (currentMode === 'before') {
      if (metricCpu) metricCpu.textContent = 'Blocks an OS thread';
      if (metricCrash) metricCrash.textContent = '0% (Lost on Reboot)';
      if (metricErrors) metricErrors.textContent = 'Unmonitored after crash';
      if (metricRollback) metricRollback.textContent = 'Manual PagerDuty (45m)';

      if (outcomeCallout) outcomeCallout.className = 'outcome-callout bad';
      if (outcomeLabel) outcomeLabel.textContent = 'UNMANAGED PIPELINE HAZARD';
      if (outcomeTitle) outcomeTitle.textContent = 'VOLATILE IN-MEMORY 24-HOUR SLEEP';
      if (outcomeSub) outcomeSub.textContent = 'Holding 24-hour sleep in Python memory wastes worker threads and will be wiped if the runner pod reboots.';
      highlightCodeLine(10);
    } else {
      if (metricCpu) metricCpu.textContent = 'No waiting handler held';
      if (metricCrash) metricCrash.textContent = 'Lease-based recovery';
      if (metricErrors) metricErrors.textContent = 'Active Signal Watch';
      if (metricRollback) metricRollback.textContent = 'Effect-dependent';

      if (outcomeCallout) outcomeCallout.className = 'outcome-callout good';
      if (outcomeLabel) outcomeLabel.textContent = 'CANARY RELIABILITY OUTCOME';
      if (outcomeTitle) outcomeTitle.textContent = 'PARKED SOAK STATE &amp; EXPLICIT RECOVERY';
      if (outcomeSub) outcomeSub.textContent = 'Workflow sleeps in Raft quorum with 0 open threads. If the host crashes, it resumes precisely where it left off.';
      highlightCodeLine(8);
    }
  }

  // --- STEP 2: Host Crash at Hour 18 ---
  function runStep2(onDone) {
    updateStepperUI(2);

    if (currentMode === 'before') {
      // Unmanaged Crash: State wiped!
      if (livePill) livePill.className = 'live-pill is-crash';
      if (liveStatus) liveStatus.textContent = '💥 RUNNER CRASHED (STATE LOST)';

      if (soakCard) soakCard.className = 'canary-card is-tripped';
      if (soakVal) soakVal.textContent = '❌ DEAD SCRIPT (Hour 18)';
      if (soakBadge) { soakBadge.className = 'c-badge tripped'; soakBadge.textContent = '💥 Memory Wiped'; }
      if (soakSub) soakSub.textContent = 'Kubernetes pod reboot wiped sleep timer from RAM!';

      if (envoyCard) envoyCard.className = 'canary-card is-warning';
      if (envoySub) envoySub.textContent = '10% Canary orphaned in production with no monitoring!';

      if (expIcon) expIcon.textContent = '💥';
      if (expTitle) expTitle.textContent = 'Step 2 Disaster: Host Reboot Wiped Deployment Script!';
      if (expDesc) expDesc.textContent = 'Because time.sleep() was running in Python RAM, the Kubernetes node reboot killed the process. The 10% canary is now a zombie deployment running in production with nobody monitoring it! Click "3. 5xx Rollback" to see the consequence.';

      highlightCodeLine(8, true);
    } else {
      // FerricStore: resumes after the state is reclaimed.
      if (livePill) livePill.className = 'live-pill';
      if (liveStatus) liveStatus.textContent = '✓ HOST REBOOTED ➔ STATE RECLAIMED';

      if (soakCard) soakCard.className = 'canary-card';
      if (soakVal) soakVal.textContent = '18h 32m / 24h 00m (Resumed!)';
      if (soakBadge) { soakBadge.className = 'c-badge ok'; soakBadge.textContent = '✓ Reclaimed with new fence'; }
      if (soakFill) soakFill.style.width = '77%';
      if (soakSub) soakSub.textContent = 'Re-loaded from replicated Raft log without losing a second';

      if (expIcon) expIcon.textContent = '🧠';
      if (expTitle) expTitle.textContent = 'Step 2 Success: Zero State Lost on Host Reboot';
      if (expDesc) expDesc.textContent = 'The Kubernetes runner pod crashed, but the observation state remained durable. A replacement worker reclaimed it with a newer fence. Click "3. 5xx Rollback" to simulate an error signal.';

      highlightCodeLine(8);
    }

    if (onDone) animTimeouts.push(setTimeout(onDone, 1200));
  }

  // --- STEP 3: Datadog 5xx Alert -> Rollback ---
  function runStep3() {
    updateStepperUI(3);

    if (currentMode === 'before') {
      // UNMANAGED FAILURE: 5xx spike ignored, canary stays broken
      if (livePill) livePill.className = 'live-pill is-crash';
      if (liveStatus) liveStatus.textContent = '🚨 2.4% 5XX ERRORS (CANARY BROKEN IN PROD)';

      if (datadogCard) datadogCard.className = 'canary-card is-tripped';
      if (datadogVal) datadogVal.textContent = '2.40% 5xx Spike!';
      if (datadogBadge) { datadogBadge.className = 'c-badge tripped'; datadogBadge.textContent = '🚨 Threshold Breached'; }
      if (datadogFill) { datadogFill.style.width = '85%'; datadogFill.style.background = '#ef4444'; }

      if (envoyCard) envoyCard.className = 'canary-card is-tripped';
      if (envoyVal) envoyVal.textContent = '10% Canary (STILL ROUTING!)';
      if (envoyBadge) { envoyBadge.className = 'c-badge tripped'; envoyBadge.textContent = '🚨 Broken Canary Live'; }
      if (envoySub) envoySub.textContent = 'No active listener to rollback! Customers receiving 500 errors.';

      if (expIcon) expIcon.textContent = '🚨';
      if (expTitle) expTitle.textContent = 'Step 3 Disaster: 2.4% 5xx Errors Leaking to Real Users!';
      if (expDesc) expDesc.textContent = 'Datadog fired a 5xx alert, but because the deployment script crashed in Step 2, nobody is listening. 10% of production traffic continues hitting the broken container until an on-call engineer wakes up at 3 AM!';

      if (outcomeCallout) outcomeCallout.className = 'outcome-callout bad';
      if (outcomeLabel) outcomeLabel.textContent = 'PRODUCTION OUTAGE';
      if (outcomeTitle) outcomeTitle.textContent = 'BROKEN CANARY LEAK &amp; SLA PENALTIES';
      if (outcomeSub) outcomeSub.textContent = 'Deployment script died during 24h sleep. Broken v2.4.0 left routing 10% traffic for 45 minutes, violating 99.99% uptime SLA.';

      highlightCodeLine(9, true);

    } else {
      // FERRICSTORE STATE-GUARDED ROLLBACK
      if (livePill) livePill.className = 'live-pill';
      if (liveStatus) liveStatus.textContent = '✓ ROLLBACK STATE COMPLETE';

      if (datadogCard) datadogCard.className = 'canary-card is-tripped';
      if (datadogVal) datadogVal.textContent = '2.40% 5xx Spike Detected';
      if (datadogBadge) { datadogBadge.className = 'c-badge tripped'; datadogBadge.textContent = '⚡ Signal Triggered'; }
      if (datadogFill) { datadogFill.style.width = '85%'; datadogFill.style.background = '#ef4444'; }

      if (envoyCard) envoyCard.className = 'canary-card';
      if (envoyVal) envoyVal.textContent = '0% Canary / 100% Stable (v2.3.9)';
      if (envoyBadge) { envoyBadge.className = 'c-badge ok'; envoyBadge.textContent = '✓ Rollback effect complete'; }
      if (envoyFill) { envoyFill.style.width = '0%'; }
      if (envoySub) envoySub.textContent = 'All traffic restored to stable v2.3.9';

      if (expIcon) expIcon.textContent = '🛡️';
      if (expTitle) expTitle.textContent = 'Step 3 Success: State-Guarded Rollback';
      if (expDesc) expDesc.textContent = 'The application webhook sent a deduplicated signal while the workflow was in soak. A worker claimed rollback and ran the guarded Envoy restore effect; timing depends on worker availability and the external control plane.';

      if (outcomeCallout) outcomeCallout.className = 'outcome-callout good';
      if (outcomeLabel) outcomeLabel.textContent = 'CANARY RELIABILITY OUTCOME';
      if (outcomeTitle) outcomeTitle.textContent = 'ROLLBACK EFFECT COMPLETED';
      if (outcomeSub) outcomeSub.textContent = 'The state-guarded signal advanced the parked soak to rollback. The guarded Envoy effect restored the modeled route weights; production timing depends on the control plane.';

      highlightCodeLine(12);
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
