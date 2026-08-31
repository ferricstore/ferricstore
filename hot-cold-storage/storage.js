(function () {
  'use strict';

  var currentMode = 'before';
  var animTimeouts = [];

  // DOM Elements
  var modeButtons = document.querySelectorAll('[data-mode-btn]');
  var datasetSlider = document.querySelector('[data-dataset-slider]');
  var datasetVal = document.querySelector('[data-dataset-val]');

  var metricCost = document.querySelector('[data-metric-cost]');
  var metricCostSub = document.querySelector('[data-metric-cost-sub]');
  var metricRam = document.querySelector('[data-metric-ram]');
  var metricRamSub = document.querySelector('[data-metric-ram-sub]');
  var metricEviction = document.querySelector('[data-metric-eviction]');
  var metricEvictionSub = document.querySelector('[data-metric-eviction-sub]');
  var metricLatency = document.querySelector('[data-metric-latency]');
  var metricLatencySub = document.querySelector('[data-metric-latency-sub]');

  var hotRamVal = document.querySelector('[data-hot-ram-val]');
  var hotBadge = document.querySelector('[data-hot-badge]');
  var coldDiskVal = document.querySelector('[data-cold-disk-val]');
  var coldBadge = document.querySelector('[data-cold-badge]');

  var keydirStatus = document.querySelector('[data-keydir-status]');
  var kdHotVal = document.querySelector('[data-kd-hot-val]');
  var kdLocator = document.querySelector('[data-kd-locator]');
  var kdPath = document.querySelector('[data-kd-path]');

  var ramPct = document.querySelector('[data-ram-pct]');
  var ramFill = document.querySelector('[data-ram-fill]');
  var diskPct = document.querySelector('[data-disk-pct]');
  var diskFill = document.querySelector('[data-disk-fill]');

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

  var btnNormal = document.querySelector('[data-btn-normal]');
  var btnPressure = document.querySelector('[data-btn-pressure]');
  var btnCold = document.querySelector('[data-btn-cold]');
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

  function updateDatasetCalculations() {
    var sizeGb = parseInt(datasetSlider ? datasetSlider.value : 1000, 10);
    if (datasetVal) datasetVal.textContent = sizeGb === 1000 ? '1,000 GB (1 TB)' : (sizeGb + ' GB');

    if (currentMode === 'before') {
      // REDIS 100% IN-MEMORY
      if (metricCost) metricCost.textContent = 'Provider quote required';
      if (metricCostSub) metricCostSub.textContent = 'Pure RAM sizing for ' + sizeGb + ' GB';

      if (metricRam) metricRam.textContent = sizeGb + ' GB RAM';
      if (metricRamSub) metricRamSub.textContent = '100% of data locked in RAM';

      if (metricEviction) metricEviction.textContent = 'High Risk (OOM / Evict)';
      if (metricEvictionSub) metricEvictionSub.textContent = 'LRU deletes keys when full';

      if (metricLatency) metricLatency.textContent = 'In-memory path';
      if (metricLatencySub) metricLatencySub.textContent = 'No disk tiering available';

      if (hotRamVal) hotRamVal.textContent = sizeGb + ' GB RAM Used';
      if (hotBadge) { hotBadge.className = 'tier-badge'; hotBadge.textContent = '⚠️ Expensive RAM'; }
      if (coldDiskVal) coldDiskVal.textContent = '0 GB (RAM Only)';
      if (coldBadge) { coldBadge.className = 'tier-badge'; coldBadge.textContent = 'No Cold Tier'; }

      if (ramPct) ramPct.textContent = '100% Saturated (' + sizeGb + ' GB)';
      if (ramFill) { ramFill.style.width = '100%'; ramFill.style.background = '#ef4444'; }
      if (diskPct) diskPct.textContent = '0% Used (No Tiering)';
      if (diskFill) diskFill.style.width = '0%';

      if (keydirStatus) { keydirStatus.textContent = 'STATUS: RAM ONLY'; keydirStatus.style.borderColor = '#ef4444'; keydirStatus.style.color = '#fca5a5'; }
      if (kdHotVal) kdHotVal.textContent = '{"name":"Alice", "bio":"..."}';
      if (kdLocator) kdLocator.textContent = 'None (Volatile RAM Object)';
      if (kdPath) kdPath.textContent = 'In-Memory Hash Table (Evicts on OOM)';

      if (outcomeCallout) outcomeCallout.className = 'outcome-callout bad';
      if (outcomeLabel) outcomeLabel.textContent = 'REDIS RAM TAX HAZARD';
      if (outcomeTitle) outcomeTitle.textContent = 'HIGH CLOUD COST &amp; LRU KEY EVICTION';
      if (outcomeSub) outcomeSub.textContent = 'A pure in-memory model needs RAM for the full ' + sizeGb + ' GB dataset plus overhead. Pressure behavior depends on the configured eviction and persistence policies.';

    } else {
      // FERRICSTORE HOT RAM + COLD NVMe
      var ramUsed = Math.max(1, sizeGb * 0.004).toFixed(1); // illustrative 0.4% hot set
      var diskUsed = Math.max(0, sizeGb - Number(ramUsed)).toFixed(1);

      if (metricCost) metricCost.textContent = 'Provider quote required';
      if (metricCostSub) metricCostSub.textContent = 'RAM + NVMe deployment';

      if (metricRam) metricRam.textContent = ramUsed + ' GB RAM';
      if (metricRamSub) metricRamSub.textContent = 'Illustrative 0.4% hot set for ' + sizeGb + ' GB';

      if (metricEviction) metricEviction.textContent = 'Disk record retained';
      if (metricEvictionSub) metricEvictionSub.textContent = 'MemoryGuard preserves disk records';

      if (metricLatency) metricLatency.textContent = 'Direct offset read';
      if (metricLatencySub) metricLatencySub.textContent = 'Direct offset read, zero file scanning';

      if (hotRamVal) hotRamVal.textContent = ramUsed + ' GB RAM Used';
      if (hotBadge) { hotBadge.className = 'tier-badge ok'; hotBadge.textContent = '✓ In-memory hot reads'; }
      if (coldDiskVal) coldDiskVal.textContent = diskUsed + ' GB On Disk';
      if (coldBadge) { coldBadge.className = 'tier-badge ok'; coldBadge.textContent = '✓ Direct Pread'; }

      if (ramPct) ramPct.textContent = Math.round((ramUsed / 4.0) * 100) + '% Used (' + ramUsed + ' GB / 4GB)';
      if (ramFill) { ramFill.style.width = Math.round((ramUsed / 4.0) * 100) + '%'; ramFill.style.background = 'linear-gradient(90deg, #10b981, #f59e0b)'; }
      if (diskPct) diskPct.textContent = Math.round((sizeGb / 2000) * 100) + '% Used (' + sizeGb + ' GB / 2TB)';
      if (diskFill) { diskFill.style.width = Math.round((sizeGb / 2000) * 100) + '%'; diskFill.style.background = 'linear-gradient(90deg, #0284c7, #38bdf8)'; }

      if (keydirStatus) { keydirStatus.textContent = 'STATUS: COLD NVMe'; keydirStatus.style.borderColor = '#0284c7'; keydirStatus.style.color = '#38bdf8'; }
      if (kdHotVal) kdHotVal.textContent = 'NIL (Demoted by MemoryGuard)';
      if (kdLocator) kdLocator.textContent = 'File #14, Offset: 8,388,608, Size: 128 KB';
      if (kdPath) kdPath.textContent = 'Single POSIX pread() by offset';

      if (outcomeCallout) outcomeCallout.className = 'outcome-callout good';
      if (outcomeLabel) outcomeLabel.textContent = 'STORAGE ARCHITECTURE OUTCOME';
      if (outcomeTitle) outcomeTitle.textContent = 'BOUNDED HOT SET + DISK-BACKED VALUES';
      if (outcomeSub) outcomeSub.textContent = 'This model places ' + sizeGb + ' GB on disk with a bounded hot set. Production memory, price, and latency require deployment-specific sizing.';
    }
  }

  // --- ACTION 1: Normal Hot Key Read/Write ---
  function runNormalHotRead() {
    clearLogs();
    clearAllTimeouts();
    if (livePill) livePill.className = 'live-pill';
    if (liveStatus) liveStatus.textContent = 'HOT KEY READ: IN-MEMORY PATH';

    log('info', 'GET session:user:42 ➔ Key found in Hot ETS RAM table...');

    animTimeouts.push(setTimeout(function () {
      log('success', '✓ [HOT READ] Value served from the ETS-backed in-memory path without a cold-value disk read.');
      if (keydirStatus) { keydirStatus.textContent = 'STATUS: HOT RAM'; keydirStatus.style.borderColor = '#10b981'; keydirStatus.style.color = '#6ee7b7'; }
      if (kdHotVal) kdHotVal.textContent = '"session_token_xyz8492" (IN RAM)';
      if (kdPath) kdPath.textContent = 'ETS-backed in-memory lookup';

      if (expIcon) expIcon.textContent = '⚡';
      if (expTitle) expTitle.textContent = 'Hot Key Read From ETS RAM';
      if (expDesc) expDesc.textContent = 'Active sessions and selected small values use the in-memory hot path. Measure actual latency on the target deployment.';
    }, 400));
  }

  // --- ACTION 2: Simulate 100% RAM Pressure Spike (MemoryGuard) ---
  function runMemoryPressure() {
    clearLogs();
    clearAllTimeouts();

    if (currentMode === 'before') {
      if (livePill) livePill.className = 'live-pill is-crash';
      if (liveStatus) liveStatus.textContent = '🚨 OOM / EVICTION SPIKE';

      log('danger', '💥 [REDIS SATURATION] RAM exceeded 100%! Redis triggered maxmemory-policy: allkeys-lru.');
      log('danger', '🗑️ [DATA LOSS] 100,000 user shopping carts and active sessions deleted permanently!');

      if (expIcon) expIcon.textContent = '💥';
      if (expTitle) expTitle.textContent = 'Redis Silent LRU Eviction Disaster';
      if (expDesc) expDesc.textContent = 'Because Redis has no disk tiering, it had to delete your users data to prevent crashing the server.';
    } else {
      if (livePill) livePill.className = 'live-pill is-crash';
      if (liveStatus) liveStatus.textContent = '⚡ MEMORYGUARD DEMOTING TO NVMe';

      log('warn', '⚠️ [MEMORYGUARD ALERT] RAM usage reached 92% threshold (3.68 GB / 4.0 GB)...');

      animTimeouts.push(setTimeout(function () {
        log('cyan', '💤 [TIERING PROTECTION] MemoryGuard demoted 50,000 cold keys from RAM to NVMe disk.');
        log('success', '✓ [DISK RECORD RETAINED] Cached RAM value released; disk file/offset remains in Keydir.');
        log('success', '🛡️ In this model, RAM usage drops while disk-backed value locators remain available.');

        if (livePill) livePill.className = 'live-pill';
        if (liveStatus) liveStatus.textContent = '✓ SAFE (0 KEYS DELETED)';

        if (keydirStatus) { keydirStatus.textContent = 'STATUS: COLD NVMe (SAFE)'; keydirStatus.style.borderColor = '#0284c7'; keydirStatus.style.color = '#38bdf8'; }
        if (kdHotVal) kdHotVal.textContent = 'NIL (Demoted to Save RAM)';
        if (kdLocator) kdLocator.textContent = 'File #14, Offset: 8,388,608, Size: 128 KB';
        if (kdPath) kdPath.textContent = 'Direct POSIX pread() by offset';

        if (expIcon) expIcon.textContent = '🛡️';
        if (expTitle) expTitle.textContent = 'MemoryGuard Safely Relieved RAM Pressure';
        if (expDesc) expDesc.textContent = 'The model releases hot value copies while retaining disk locators. Availability still follows storage health and the configured durability topology.';
      }, 700));
    }
  }

  // --- ACTION 3: Read Cold Key From Disk ---
  function runColdRead() {
    clearLogs();
    clearAllTimeouts();

    if (currentMode === 'before') {
      log('danger', '❌ Redis has no cold disk tier. All reads must come from RAM.');
    } else {
      if (livePill) livePill.className = 'live-pill';
      if (liveStatus) liveStatus.textContent = 'COLD READ: DIRECT PREAD';

      log('info', 'GET user:84920:profile ➔ Key found in Keydir. Hot value is NIL (Cold tier).');

      animTimeouts.push(setTimeout(function () {
        log('cyan', '📖 [DIRECT DISK READ] Reading File #14 at physical byte offset 8,388,608 (128 KB)...');
        log('success', '✓ [READ COMPLETE] A direct POSIX pread() returned the modeled 128KB payload. Timing depends on hardware and workload.');

        if (expIcon) expIcon.textContent = '❄️';
        if (expTitle) expTitle.textContent = 'Cold NVMe Read via Direct Offset';
        if (expDesc) expDesc.textContent = 'FerricStore avoids full disk scans by storing the exact byte offset in the Keydir, delivering sub-millisecond cold data retrieval.';
      }, 500));
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
      updateDatasetCalculations();
      runNormalHotRead();
    });
  });

  // --- Slider ---
  if (datasetSlider) {
    datasetSlider.addEventListener('input', updateDatasetCalculations);
  }

  // --- Playback Buttons ---
  if (btnNormal) btnNormal.addEventListener('click', runNormalHotRead);
  if (btnPressure) btnPressure.addEventListener('click', runMemoryPressure);
  if (btnCold) btnCold.addEventListener('click', runColdRead);
  if (btnReset) btnReset.addEventListener('click', function () { updateDatasetCalculations(); runNormalHotRead(); });

  // Init
  updateDatasetCalculations();
  runNormalHotRead();
})();
