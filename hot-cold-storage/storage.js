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
  var ramCapacity = document.querySelector('[data-ram-capacity]');
  var diskPct = document.querySelector('[data-disk-pct]');
  var diskFill = document.querySelector('[data-disk-fill]');
  var diskCapacity = document.querySelector('[data-disk-capacity]');

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
      if (metricCostSub) metricCostSub.textContent = 'RAM-only sizing for ' + sizeGb + ' GB';

      if (metricRam) metricRam.textContent = sizeGb + ' GB RAM';
      if (metricRamSub) metricRamSub.textContent = '100% of data locked in RAM';

      if (metricEviction) metricEviction.textContent = 'Policy decides';
      if (metricEvictionSub) metricEvictionSub.textContent = 'Writes may fail or eligible keys may be evicted';

      if (metricLatency) metricLatency.textContent = 'RAM-only path';
      if (metricLatencySub) metricLatencySub.textContent = 'There is no cold disk tier in this mode';

      if (hotRamVal) hotRamVal.textContent = sizeGb.toLocaleString() + ' GB RAM Required';
      if (hotBadge) { hotBadge.className = 'tier-badge'; hotBadge.textContent = 'RAM-only mode'; }
      if (coldDiskVal) coldDiskVal.textContent = '0 GB · No cold tier';
      if (coldBadge) { coldBadge.className = 'tier-badge'; coldBadge.textContent = 'Not used'; }

      if (ramCapacity) ramCapacity.textContent = 'RAM REQUIRED FOR DATASET · NO FIXED CAPACITY';
      if (ramPct) ramPct.textContent = '100% of dataset (' + sizeGb.toLocaleString() + ' GB)';
      if (ramFill) { ramFill.style.width = '100%'; ramFill.style.background = '#ef4444'; }
      if (diskCapacity) diskCapacity.textContent = 'NVMe SSD DISK · NO COLD TIER IN THIS MODE';
      if (diskPct) diskPct.textContent = '0% used (no cold tier)';
      if (diskFill) diskFill.style.width = '0%';

      if (keydirStatus) { keydirStatus.textContent = 'STATUS: RAM ONLY'; keydirStatus.style.borderColor = '#ef4444'; keydirStatus.style.color = '#fca5a5'; }
      if (kdHotVal) kdHotVal.textContent = '{"name":"Alice", "bio":"..."}';
      if (kdLocator) kdLocator.textContent = 'None (Volatile RAM Object)';
      if (kdPath) kdPath.textContent = 'In-Memory Hash Table (Evicts on OOM)';

      if (outcomeCallout) outcomeCallout.className = 'outcome-callout bad';
      if (outcomeLabel) outcomeLabel.textContent = 'RAM-ONLY MODEL';
      if (outcomeTitle) outcomeTitle.textContent = 'ALL VALUES NEED RAM IN THIS EXAMPLE';
      if (outcomeSub) outcomeSub.textContent = 'This model needs RAM for the full ' + sizeGb.toLocaleString() + ' GB dataset plus overhead. Pressure behavior depends on the configured eviction and persistence policies.';

    } else {
      // FERRICSTORE HOT RAM + COLD NVMe
      var ramUsed = Math.max(1, sizeGb * 0.004).toFixed(1); // illustrative 0.4% hot set
      var diskUsed = Math.max(0, sizeGb - Number(ramUsed)).toFixed(1);

      if (metricCost) metricCost.textContent = 'Provider quote required';
      if (metricCostSub) metricCostSub.textContent = 'RAM + NVMe deployment';

      if (metricRam) metricRam.textContent = ramUsed + ' GB RAM';
      if (metricRamSub) metricRamSub.textContent = 'Illustrative 0.4% hot set for ' + sizeGb.toLocaleString() + ' GB';

      if (metricEviction) metricEviction.textContent = 'Disk record can remain';
      if (metricEvictionSub) metricEvictionSub.textContent = 'MemoryGuard can release the RAM copy';

      if (metricLatency) metricLatency.textContent = 'Direct offset read';
      if (metricLatencySub) metricLatencySub.textContent = 'Keydir points to the value; no full-file scan';

      if (hotRamVal) hotRamVal.textContent = ramUsed + ' GB RAM Used';
      if (hotBadge) { hotBadge.className = 'tier-badge ok'; hotBadge.textContent = '✓ In-memory hot reads'; }
      if (coldDiskVal) coldDiskVal.textContent = diskUsed + ' GB On Disk';
      if (coldBadge) { coldBadge.className = 'tier-badge ok'; coldBadge.textContent = '✓ Direct pread()'; }

      if (ramCapacity) ramCapacity.textContent = 'SERVER RAM CAPACITY (4.0 GB MAX)';
      if (ramPct) ramPct.textContent = Math.round((ramUsed / 4.0) * 100) + '% used (' + ramUsed + ' GB / 4.0 GB)';
      if (ramFill) { ramFill.style.width = Math.round((ramUsed / 4.0) * 100) + '%'; ramFill.style.background = 'linear-gradient(90deg, #10b981, #f59e0b)'; }
      if (diskCapacity) diskCapacity.textContent = 'NVMe SSD DISK (2,000 GB CAPACITY)';
      if (diskPct) diskPct.textContent = Math.round((sizeGb / 2000) * 100) + '% used (' + sizeGb.toLocaleString() + ' GB / 2,000 GB)';
      if (diskFill) { diskFill.style.width = Math.round((sizeGb / 2000) * 100) + '%'; diskFill.style.background = 'linear-gradient(90deg, #0284c7, #38bdf8)'; }

      if (keydirStatus) { keydirStatus.textContent = 'STATUS: COLD NVMe'; keydirStatus.style.borderColor = '#0284c7'; keydirStatus.style.color = '#38bdf8'; }
      if (kdHotVal) kdHotVal.textContent = 'NIL (Demoted by MemoryGuard)';
      if (kdLocator) kdLocator.textContent = 'File #14, Offset: 8,388,608, Size: 128 KB';
      if (kdPath) kdPath.textContent = 'Single POSIX pread() by offset';

      if (outcomeCallout) outcomeCallout.className = 'outcome-callout good';
      if (outcomeLabel) outcomeLabel.textContent = 'STORAGE ARCHITECTURE OUTCOME';
      if (outcomeTitle) outcomeTitle.textContent = 'HOT VALUES IN RAM; COLD VALUES ON DISK';
      if (outcomeSub) outcomeSub.textContent = 'This illustrative model places ' + sizeGb.toLocaleString() + ' GB on disk with a bounded hot set. Production memory, price, and latency require deployment-specific sizing.';
    }
  }

  // --- ACTION 1: Normal Hot Key Read/Write ---
  function runNormalHotRead(isModeReset) {
    clearLogs();
    clearAllTimeouts();
    if (livePill) livePill.className = 'live-pill';
    if (liveStatus) liveStatus.textContent = isModeReset ? 'NORMAL (READY)' : (currentMode === 'before' ? 'HOT KEY READ: RAM-ONLY PATH' : 'HOT KEY READ: IN-MEMORY PATH');

    if (!isModeReset) log('info', 'GET session:user:42 ➔ Key found in Hot ETS RAM table...');

    if (isModeReset) {
      if (expIcon) expIcon.textContent = '💡';
      if (expTitle) expTitle.textContent = 'Mode selected; ready to run';
      if (expDesc) expDesc.textContent = 'Choose Read from memory, Test RAM pressure, or Read from disk to run this mode.';
      if (outcomeLabel) outcomeLabel.textContent = 'READY';
      if (outcomeTitle) outcomeTitle.textContent = 'CHOOSE AN ACTION TO TEST THIS MODE';
      if (outcomeSub) outcomeSub.textContent = 'The mode changed; no storage action has run yet.';
      return;
    }

    animTimeouts.push(setTimeout(function () {
      log('success', '✓ [HOT READ] Value served from the ETS-backed in-memory path without a cold-value disk read.');
      if (keydirStatus) { keydirStatus.textContent = currentMode === 'before' ? 'STATUS: RAM ONLY' : 'STATUS: HOT RAM'; keydirStatus.style.borderColor = '#10b981'; keydirStatus.style.color = '#6ee7b7'; }
      if (kdHotVal) kdHotVal.textContent = '"session_token_xyz8492" (IN RAM)';
      if (kdLocator) kdLocator.textContent = currentMode === 'before' ? 'None (RAM-only value)' : 'File #14, Offset: 8,388,608, Size: 128 KB';
      if (kdPath) kdPath.textContent = currentMode === 'before' ? 'In-memory lookup' : 'ETS-backed in-memory lookup';

      if (expIcon) expIcon.textContent = '⚡';
      if (expTitle) expTitle.textContent = currentMode === 'before' ? 'Read from RAM-only storage' : 'Read from the hot RAM tier';
      if (expDesc) expDesc.textContent = currentMode === 'before'
        ? 'This mode keeps the value in RAM. Try Test RAM pressure to see what the configured policy does when the limit is reached.'
        : 'Active sessions and selected small values use the in-memory hot path. Try Test RAM pressure to see MemoryGuard release a RAM copy.';
    }, 400));
  }

  // --- ACTION 2: Simulate 100% RAM Pressure Spike (MemoryGuard) ---
  function runMemoryPressure() {
    clearLogs();
    clearAllTimeouts();

    if (currentMode === 'before') {
      if (livePill) livePill.className = 'live-pill is-crash';
      if (liveStatus) liveStatus.textContent = '🚨 OOM / EVICTION SPIKE';

      log('danger', '💥 [RAM LIMIT] The dataset reached the configured in-memory limit; this run uses maxmemory-policy: allkeys-lru.');
      log('danger', '🗑️ [EVICTION] This run models 100,000 cached carts and active sessions being evicted.');

      if (expIcon) expIcon.textContent = '💥';
      if (expTitle) expTitle.textContent = 'RAM pressure caused eviction in this run';
      if (expDesc) expDesc.textContent = 'This RAM-only example uses an LRU policy when memory fills. Eligible cached values are evicted; the exact behavior depends on the configured policy.';
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
      if (livePill) livePill.className = 'live-pill is-crash';
      if (liveStatus) liveStatus.textContent = 'COLD READ: NO COLD TIER';
      log('danger', '❌ This RAM-only mode has no cold disk tier, so the requested value must be in RAM.');
      if (expIcon) expIcon.textContent = '❄️';
      if (expTitle) expTitle.textContent = 'No disk-backed value in this mode';
      if (expDesc) expDesc.textContent = 'Switch to With FerricStore to inspect a cold value addressed by its Keydir file offset.';
    } else {
      if (livePill) livePill.className = 'live-pill';
      if (liveStatus) liveStatus.textContent = 'COLD READ: DIRECT PREAD';

      log('info', 'GET user:84920:profile ➔ Key found in Keydir. Hot value is NIL (Cold tier).');

      animTimeouts.push(setTimeout(function () {
        log('cyan', '📖 [DIRECT DISK READ] Reading File #14 at physical byte offset 8,388,608 (128 KB)...');
        log('success', '✓ [READ COMPLETE] A direct POSIX pread() returned the modeled 128KB payload. Timing depends on hardware and workload.');

        if (expIcon) expIcon.textContent = '❄️';
        if (expTitle) expTitle.textContent = 'Read a cold value from NVMe';
        if (expDesc) expDesc.textContent = 'The Keydir stores the exact byte offset, so this example avoids a full disk scan. Measure latency on the target hardware and workload.';
      }, 500));
    }
  }

  // --- Mode Buttons ---
  modeButtons.forEach(function (btn) {
    btn.addEventListener('click', function () {
      clearAllTimeouts();
      modeButtons.forEach(function (b) { b.classList.remove('is-selected'); b.setAttribute('aria-selected', 'false'); });
      btn.classList.add('is-selected');
      btn.setAttribute('aria-selected', 'true');
      currentMode = btn.getAttribute('data-mode-btn') || 'after';
      document.body.setAttribute('data-mode', currentMode);
      updateDatasetCalculations();
      runNormalHotRead(true);
    });
  });

  // --- Slider ---
  if (datasetSlider) {
    datasetSlider.addEventListener('input', updateDatasetCalculations);
  }

  // --- Playback Buttons ---
  if (btnNormal) btnNormal.addEventListener('click', function () { runNormalHotRead(false); });
  if (btnPressure) btnPressure.addEventListener('click', runMemoryPressure);
  if (btnCold) btnCold.addEventListener('click', runColdRead);
  if (btnReset) btnReset.addEventListener('click', function () { clearAllTimeouts(); clearLogs(); updateDatasetCalculations(); runNormalHotRead(true); });

  // Init
  updateDatasetCalculations();
  runNormalHotRead(true);
})();
