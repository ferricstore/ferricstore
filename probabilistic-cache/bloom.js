(function () {
  'use strict';

  var currentMode = 'set';
  var animTimeouts = [];

  // DOM Elements
  var modeButtons = document.querySelectorAll('[data-mode-btn]');
  var codeBlocks = document.querySelectorAll('[data-code]');
  var codeTitle = document.querySelector('[data-code-title]');
  var codeKicker = document.querySelector('[data-code-kicker]');

  var metricRam = document.querySelector('[data-metric-ram]');
  var metricRamSub = document.querySelector('[data-metric-ram-sub]');
  var metricSpeed = document.querySelector('[data-metric-speed]');
  var metricShield = document.querySelector('[data-metric-shield]');
  var metricShieldSub = document.querySelector('[data-metric-shield-sub]');
  var metricGuar = document.querySelector('[data-metric-guar]');
  var metricGuarSub = document.querySelector('[data-metric-guar-sub]');

  var queryCard = document.querySelector('[data-query-card]');
  var queryVal = document.querySelector('[data-query-val]');
  var queryBadge = document.querySelector('[data-query-badge]');
  var querySub = document.querySelector('[data-query-sub]');

  var matrixCard = document.querySelector('[data-matrix-card]');
  var matrixTitle = matrixCard ? matrixCard.querySelector('.b-head strong') : null;
  var matrixSub = matrixCard ? matrixCard.querySelector('.b-head small') : null;
  var matrixBadge = document.querySelector('[data-matrix-badge]');
  var bit1 = document.querySelector('[data-bit-1]');
  var bit2 = document.querySelector('[data-bit-2]');
  var bit3 = document.querySelector('[data-bit-3]');

  var dbCard = document.querySelector('[data-db-card]');
  var dbVal = document.querySelector('[data-db-val]');
  var dbBadge = document.querySelector('[data-db-badge]');
  var dbFill = document.querySelector('[data-db-fill]');
  var dbSub = document.querySelector('[data-db-sub]');

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

  var btnValid = document.querySelector('[data-btn-valid]');
  var btnSpam = document.querySelector('[data-btn-spam]');
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

  function highlightCodeLine(targetLine) {
    var activeBlock = document.querySelector('[data-code="' + currentMode + '"]');
    if (!activeBlock) return;
    activeBlock.querySelectorAll('[data-line]').forEach(function (l) {
      l.classList.remove('is-active');
    });
    if (targetLine) {
      var row = activeBlock.querySelector('[data-line="' + targetLine + '"]');
      if (row) row.classList.add('is-active');
    }
  }

  function updateModeUI() {
    codeBlocks.forEach(function (block) {
      block.hidden = block.getAttribute('data-code') !== currentMode;
    });

    if (currentMode === 'set') {
      if (codeTitle) codeTitle.textContent = 'unshielded_redis_set.py';
      if (codeKicker) codeKicker.textContent = 'RAW STRING SET BASELINE';

      if (metricRam) metricRam.textContent = '950 MB RAM (illustrative)';
      if (metricRamSub) metricRamSub.textContent = '10M-item model; implementation varies';
      if (metricSpeed) metricSpeed.textContent = 'Exact set lookup';
      if (metricShield) metricShield.textContent = 'No missing-key guard';
      if (metricShieldSub) metricShieldSub.textContent = 'Missing requests continue to the database in this model';
      if (metricGuar) metricGuar.textContent = 'Not applicable to a raw set';
      if (metricGuarSub) metricGuarSub.textContent = 'The set stores exact members';

      if (outcomeCallout) outcomeCallout.className = 'outcome-callout bad';
      if (outcomeLabel) outcomeLabel.textContent = 'RAW SET MODEL';
      if (outcomeTitle) outcomeTitle.textContent = 'THE FULL KEY LIST STAYS IN MEMORY';
      if (outcomeSub) outcomeSub.textContent = 'This illustrative 10M-item model uses about 950 MB. Missing-key requests continue to the database in the example.';

    } else {
      if (codeTitle) codeTitle.textContent = 'bloom_shield.py';
      if (codeKicker) codeKicker.textContent = 'FERRICSTORE BLOOM FILTER API';

      if (metricRam) metricRam.textContent = '11.98 MB (illustrative)';
      if (metricRamSub) metricRamSub.textContent = '10M-item model; implementation overhead is additional';
      if (metricSpeed) metricSpeed.textContent = 'Bitwise membership test';
      if (metricShield) metricShield.textContent = 'Ready to check';
      if (metricShieldSub) metricShieldSub.textContent = 'Run the missing-key example below';
      if (metricGuar) metricGuar.textContent = 'No false negatives';
      if (metricGuarSub) metricGuarSub.textContent = 'A negative is definite; a positive still needs checking';

      if (outcomeCallout) outcomeCallout.className = 'outcome-callout good';
      if (outcomeLabel) outcomeLabel.textContent = 'BLOOM FILTER MODEL';
      if (outcomeTitle) outcomeTitle.textContent = 'A NEGATIVE CHECK CAN SKIP THE DATABASE';
      if (outcomeSub) outcomeSub.textContent = 'This illustrative 10M-item, 1% false-positive configuration uses an approximately 11.98 MB bit array; implementation overhead is additional.';
    }
  }

  // --- ACTION 1: Valid Key (Alice) ---
  function runValidKey(isModeReset) {
    clearLogs();
    clearAllTimeouts();
    updateModeUI();

    if (livePill) livePill.className = 'live-pill';
    if (liveStatus) {
      liveStatus.textContent = isModeReset
        ? 'READY · CHOOSE A KEY'
        : (currentMode === 'set' ? 'STEP 1 · STANDARD SET LOOKUP' : 'STEP 1 · BLOOM FILTER CHECK');
    }

    if (queryVal) queryVal.textContent = '"user_alice"';
    if (queryBadge) { queryBadge.className = 'b-badge ok'; queryBadge.textContent = '✓ Registered key'; }
    if (querySub) querySub.textContent = 'Known user in this example';

    if (matrixTitle) matrixTitle.textContent = currentMode === 'set' ? 'RAW STRING SET LOOKUP' : 'BLOOM FILTER CHECK';
    if (matrixSub) matrixSub.textContent = currentMode === 'set' ? 'Exact membership lookup' : 'Three hash positions';

    if (bit1) { bit1.className = 'bit-slot'; bit1.querySelector('b').textContent = '1'; }
    if (bit2) { bit2.className = 'bit-slot'; bit2.querySelector('b').textContent = '1'; }
    if (bit3) { bit3.className = 'bit-slot'; bit3.querySelector('b').textContent = '1'; }
    if (matrixBadge) {
      matrixBadge.className = 'b-badge ok';
      matrixBadge.textContent = currentMode === 'set' ? 'Exact member found' : 'All three positions match (1-1-1)';
    }

    if (dbVal) dbVal.textContent = '0 database queries in this run';
    if (dbBadge) { dbBadge.className = 'b-badge ok'; dbBadge.textContent = '✓ No SQL needed in this model'; }
    if (dbFill) dbFill.style.width = '0%';

    if (!isModeReset) log('info', currentMode === 'set'
      ? 'SISMEMBER valid_users user_alice ➔ Checking the raw set...'
      : 'BF.EXISTS bloom:valid_users user_alice ➔ Checking the Bloom filter...');

    if (isModeReset) {
      if (expIcon) expIcon.textContent = '💡';
      if (expTitle) expTitle.textContent = 'Mode selected; ready to run';
      if (expDesc) expDesc.textContent = 'Choose Check registered key or Check 10,000 missing keys to run this example.';
      if (outcomeLabel) outcomeLabel.textContent = 'READY';
      if (outcomeTitle) outcomeTitle.textContent = 'CHOOSE A KEY TO CHECK';
      if (outcomeSub) outcomeSub.textContent = 'The mode changed; no membership check has run yet.';
      highlightCodeLine(null);
      return;
    }

    animTimeouts.push(setTimeout(function () {
      log('success', currentMode === 'set'
        ? '✓ [SISMEMBER MATCH] user_alice was found in the raw set.'
        : '✓ [BF.EXISTS MATCH] Three hash positions [48291, 108420, 892011] all evaluate to 1.');
      log('success', currentMode === 'set'
        ? '⚡ [SERVED] The exact set lookup found user_alice.'
        : '⚡ [SERVED] The positive check passed; the application can verify user_alice.');

      if (expIcon) expIcon.textContent = '⚡';
      if (expTitle) expTitle.textContent = currentMode === 'set'
        ? 'Exact set lookup found the key'
        : 'Possible match: verify the record';
      if (expDesc) expDesc.textContent = currentMode === 'set'
        ? 'The raw set found this registered user. Click Check 10,000 missing keys to see what happens when misses continue to the database.'
        : 'All three positions matched. A positive Bloom check can still be a false positive, so the application should verify the record.';

      highlightCodeLine(currentMode === 'set' ? 7 : 7);
    }, 300));
  }

  // --- ACTION 2: Attacker Spam 10k Bogus Keys ---
  function runAttackerSpam() {
    clearLogs();
    clearAllTimeouts();
    updateModeUI();

    if (queryVal) queryVal.textContent = '"bot_fake_9482910"';
    if (queryBadge) { queryBadge.className = 'b-badge tripped'; queryBadge.textContent = 'Missing key'; }
    if (querySub) querySub.textContent = '10,000 requests for keys that are not present';

    if (currentMode === 'set') {
      if (livePill) livePill.className = 'live-pill is-crash';
      if (liveStatus) liveStatus.textContent = '💥 10,000 QUERIES CRASHING POSTGRES';

      if (dbVal) dbVal.textContent = '10,000 SQL queries in this model';
      if (dbBadge) { dbBadge.className = 'b-badge tripped'; dbBadge.textContent = 'Modeled 504'; }
      if (dbFill) { dbFill.style.width = '100%'; dbFill.style.background = '#ef4444'; }

      log('danger', '🚨 [MISSING KEYS] 10,000 requests asked for keys that are not present.');
      log('danger', '💥 [CACHE MISS] All 10,000 misses continued to the Postgres database.');
      log('danger', '📉 [MODELED RESULT] The connection pool returned HTTP 504 Gateway Timeout.');

      if (expIcon) expIcon.textContent = '💥';
      if (expTitle) expTitle.textContent = 'Missing keys continue to the database';
      if (expDesc) expDesc.textContent = 'In this model, every missing-key request reaches Postgres because the raw set has no compact negative check.';

      highlightCodeLine(7);

    } else {
      if (livePill) livePill.className = 'live-pill';
      if (liveStatus) liveStatus.textContent = 'STEP 2 · DEFINITELY ABSENT KEYS BLOCKED';

      if (bit1) { bit1.className = 'bit-slot'; bit1.querySelector('b').textContent = '1'; }
      if (bit2) { bit2.className = 'bit-slot is-zero'; bit2.querySelector('b').textContent = '0'; }
      if (bit3) { bit3.className = 'bit-slot'; bit3.querySelector('b').textContent = '1'; }
      if (matrixBadge) { matrixBadge.className = 'b-badge tripped'; matrixBadge.textContent = 'Position #2 = 0 ➔ definitely missing'; }

      if (dbVal) dbVal.textContent = '0 database queries in this run';
      if (dbBadge) { dbBadge.className = 'b-badge ok'; dbBadge.textContent = '✓ Definite negatives'; }
      if (dbFill) dbFill.style.width = '0%';

      log('warn', '🚨 [MISSING KEYS] 10,000 requests asked for keys that are not present.');

      animTimeouts.push(setTimeout(function () {
        log('success', '🛡️ [DEFINITE NEGATIVE] Position #2 evaluated to 0. The key cannot be present.');
        log('success', '✓ [NO SQL] In this run, every missing key was rejected before database verification.');

        if (expIcon) expIcon.textContent = '🛡️';
        if (expTitle) expTitle.textContent = 'Definite negative: no database query';
        if (expDesc) expDesc.textContent = 'Because one required position was unset, the key is definitely absent. This modeled request skips the database; positive checks may still be false positives.';

        highlightCodeLine(8);
      }, 350));
    }
  }

  // --- Mode Switch Buttons ---
  modeButtons.forEach(function (btn) {
    btn.addEventListener('click', function () {
      clearAllTimeouts();
      modeButtons.forEach(function (b) { b.classList.remove('is-selected'); b.setAttribute('aria-selected', 'false'); });
      btn.classList.add('is-selected');
      btn.setAttribute('aria-selected', 'true');
      currentMode = btn.getAttribute('data-mode-btn') || 'bloom';
      document.body.setAttribute('data-mode', currentMode);
      updateModeUI();
      runValidKey(true);
    });
  });

  // --- Playback Buttons ---
  if (btnValid) btnValid.addEventListener('click', function () { runValidKey(false); });
  if (btnSpam) btnSpam.addEventListener('click', runAttackerSpam);
  if (btnReset) btnReset.addEventListener('click', function () { runValidKey(true); });

  // Init
  runValidKey(true);
})();
