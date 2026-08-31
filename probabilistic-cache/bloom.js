(function () {
  'use strict';

  var currentMode = 'bloom';
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
  var metricGuar = document.querySelector('[data-metric-guar]');

  var queryCard = document.querySelector('[data-query-card]');
  var queryVal = document.querySelector('[data-query-val]');
  var queryBadge = document.querySelector('[data-query-badge]');
  var querySub = document.querySelector('[data-query-sub]');

  var matrixCard = document.querySelector('[data-matrix-card]');
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
      if (codeKicker) codeKicker.textContent = 'RAW STRING SET (NO PROBABILISTIC FILTER)';

      if (metricRam) metricRam.textContent = '950 MB RAM (High Cost)';
      if (metricRamSub) metricRamSub.textContent = 'Dict entry & robj struct overhead';
      if (metricSpeed) metricSpeed.textContent = 'Hash-table lookup';
      if (metricShield) metricShield.textContent = '🚨 0% Protected (DB Penetration)';
      if (metricGuar) metricGuar.textContent = 'N/A (Raw Set)';

      if (outcomeCallout) outcomeCallout.className = 'outcome-callout bad';
      if (outcomeLabel) outcomeLabel.textContent = 'RAW SET HAZARD';
      if (outcomeTitle) outcomeTitle.textContent = '950 MB RAM CONSUMED &amp; DATABASE PENETRATION';
      if (outcomeSub) outcomeSub.textContent = 'Storing 10M keys in raw Sets consumes nearly 1GB RAM. Bogus key misses bypass the cache and hammer Postgres with 10,000 queries.';

    } else {
      if (codeTitle) codeTitle.textContent = 'bloom_shield.py';
      if (codeKicker) codeKicker.textContent = 'FERRICSTORE BLOOM FILTER API';

      if (metricRam) metricRam.textContent = '11.98 MB (99% Saved)';
      if (metricRamSub) metricRamSub.textContent = 'vs 950 MB for raw string sets';
      if (metricSpeed) metricSpeed.textContent = 'Bitwise membership test';
      if (metricShield) metricShield.textContent = 'Blocked in this run';
      if (metricGuar) metricGuar.textContent = '0.00% False Negatives';

      if (outcomeCallout) outcomeCallout.className = 'outcome-callout good';
      if (outcomeLabel) outcomeLabel.textContent = 'PROBABILISTIC CACHE BENEFIT';
      if (outcomeTitle) outcomeTitle.textContent = '99% RAM REDUCTION &amp; ZERO DATABASE PENETRATION';
      if (outcomeSub) outcomeSub.textContent = 'This illustrative 10M-item, 1% false-positive configuration uses an approximately 11.98 MB bit array; implementation overhead is additional.';
    }
  }

  // --- ACTION 1: Valid Key (Alice) ---
  function runValidKey() {
    clearLogs();
    clearAllTimeouts();
    updateModeUI();

    if (queryVal) queryVal.textContent = '"user_alice"';
    if (queryBadge) { queryBadge.className = 'b-badge ok'; queryBadge.textContent = '✓ Legitimate User'; }
    if (querySub) querySub.textContent = 'Valid registered user';

    if (bit1) { bit1.className = 'bit-slot'; bit1.querySelector('b').textContent = '1'; }
    if (bit2) { bit2.className = 'bit-slot'; bit2.querySelector('b').textContent = '1'; }
    if (bit3) { bit3.className = 'bit-slot'; bit3.querySelector('b').textContent = '1'; }
    if (matrixBadge) { matrixBadge.className = 'b-badge ok'; matrixBadge.textContent = 'All Bits Match (1-1-1)'; }

    if (dbVal) dbVal.textContent = '0 DB Queries (Served Cache)';
    if (dbBadge) { dbBadge.className = 'b-badge ok'; dbBadge.textContent = '✓ Healthy in this model'; }
    if (dbFill) dbFill.style.width = '0%';

    log('info', 'GET user:user_alice ➔ Testing Bloom Filter bloom:valid_users...');

    animTimeouts.push(setTimeout(function () {
      log('success', '✓ [BF.EXISTS MATCH] 3 hash slots [48291, 108420, 892011] all evaluate to 1.');
      log('success', '⚡ [SERVED] Served user_alice after a positive membership check and application lookup.');

      if (expIcon) expIcon.textContent = '⚡';
      if (expTitle) expTitle.textContent = 'Positive Membership Check: Verify the Record';
      if (expDesc) expDesc.textContent = 'All 3 hash bits matched (1-1-1). FerricStore served the user from cache. Click "🚨 2. Attacker Spam" to simulate malicious penetration attacks!';

      highlightCodeLine(currentMode === 'set' ? 7 : 7);
    }, 300));
  }

  // --- ACTION 2: Attacker Spam 10k Bogus Keys ---
  function runAttackerSpam() {
    clearLogs();
    clearAllTimeouts();
    updateModeUI();

    if (queryVal) queryVal.textContent = '"bot_fake_9482910"';
    if (queryBadge) { queryBadge.className = 'b-badge tripped'; queryBadge.textContent = '🚨 Malicious Bogus Key'; }
    if (querySub) querySub.textContent = '10,000 non-existent attack queries';

    if (currentMode === 'set') {
      if (livePill) livePill.className = 'live-pill is-crash';
      if (liveStatus) liveStatus.textContent = '💥 10,000 QUERIES CRASHING POSTGRES';

      if (dbVal) dbVal.textContent = '10,000 SQL Queries (Pool Exhausted)';
      if (dbBadge) { dbBadge.className = 'b-badge tripped'; dbBadge.textContent = '💥 Postgres 504'; }
      if (dbFill) { dbFill.style.width = '100%'; dbFill.style.background = '#ef4444'; }

      log('danger', '🚨 [PENETRATION ATTACK] Attacker sent 10,000 random non-existent IDs!');
      log('danger', '💥 [CACHE MISS FALLTHROUGH] All 10,000 misses fell through to Postgres database!');
      log('danger', '📉 [DATABASE OUTAGE] Postgres connection pool saturated ➔ HTTP 504 Gateway Timeout!');

      if (expIcon) expIcon.textContent = '💥';
      if (expTitle) expTitle.textContent = 'The Cache Penetration Vulnerability';
      if (expDesc) expDesc.textContent = 'Without a probabilistic filter, non-existent key requests bypass the cache and hammer Postgres with 10,000 SQL queries, crashing the database.';

      highlightCodeLine(7);

    } else {
      if (bit1) { bit1.className = 'bit-slot'; bit1.querySelector('b').textContent = '1'; }
      if (bit2) { bit2.className = 'bit-slot is-zero'; bit2.querySelector('b').textContent = '0'; }
      if (bit3) { bit3.className = 'bit-slot'; bit3.querySelector('b').textContent = '1'; }
      if (matrixBadge) { matrixBadge.className = 'b-badge tripped'; matrixBadge.textContent = 'Bit #2 = 0 ➔ 100% NON-EXISTENT'; }

      if (dbVal) dbVal.textContent = '0 DB Queries in this run';
      if (dbBadge) { dbBadge.className = 'b-badge ok'; dbBadge.textContent = '✓ Definitive negatives'; }
      if (dbFill) dbFill.style.width = '0%';

      log('warn', '🚨 [PENETRATION ATTACK] Attacker sent 10,000 random non-existent IDs...');

      animTimeouts.push(setTimeout(function () {
        log('success', '🛡️ [BITWISE REJECTION] Hash bit #2 evaluated to 0. Key guaranteed not to exist!');
        log('success', '✓ [POSTGRES SAFE] In this run, all requested keys had a definitive negative result, so no database verification was needed.');

        if (expIcon) expIcon.textContent = '🛡️';
        if (expTitle) expTitle.textContent = 'Definitive Negative Check: No Database Query';
        if (expDesc) expDesc.textContent = 'Because a required bit was unset, the key is definitely absent. This modeled request skips the database; positive checks may still be false positives.';

        highlightCodeLine(8);
      }, 350));
    }
  }

  // --- Mode Switch Buttons ---
  modeButtons.forEach(function (btn) {
    btn.addEventListener('click', function () {
      modeButtons.forEach(function (b) { b.classList.remove('is-selected'); b.setAttribute('aria-selected', 'false'); });
      btn.classList.add('is-selected');
      btn.setAttribute('aria-selected', 'true');
      currentMode = btn.getAttribute('data-mode-btn') || 'bloom';
      document.body.setAttribute('data-mode', currentMode);
      updateModeUI();
      runValidKey();
    });
  });

  // --- Playback Buttons ---
  if (btnValid) btnValid.addEventListener('click', runValidKey);
  if (btnSpam) btnSpam.addEventListener('click', runAttackerSpam);
  if (btnReset) btnReset.addEventListener('click', runValidKey);

  // Init
  runValidKey();
})();
