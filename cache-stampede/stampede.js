(function () {
  'use strict';

  var currentMode = 'before';
  var animTimeouts = [];

  // DOM Elements
  var modeButtons = document.querySelectorAll('[data-mode-btn]');
  var codeBlocks = document.querySelectorAll('[data-code]');
  var codeTitle = document.querySelector('[data-code-title]');
  var codeKicker = document.querySelector('[data-code-kicker]');

  var metricQueries = document.querySelector('[data-metric-queries]');
  var metricQueriesSub = document.querySelector('[data-metric-queries-sub]');
  var metricPool = document.querySelector('[data-metric-pool]');
  var metricPoolSub = document.querySelector('[data-metric-pool-sub]');
  var metricLatency = document.querySelector('[data-metric-latency]');
  var metricLatencySub = document.querySelector('[data-metric-latency-sub]');
  var metricHealth = document.querySelector('[data-metric-health]');
  var metricHealthSub = document.querySelector('[data-metric-health-sub]');

  var reqCard = document.querySelector('[data-req-card]');
  var reqVal = document.querySelector('[data-req-val]');
  var reqBadge = document.querySelector('[data-req-badge]');
  var reqSub = document.querySelector('[data-req-sub]');

  var barrierCard = document.querySelector('[data-barrier-card]');
  var barrierVal = document.querySelector('[data-barrier-val]');
  var barrierBadge = document.querySelector('[data-barrier-badge]');
  var barrierFill = document.querySelector('[data-barrier-fill]');
  var barrierSub = document.querySelector('[data-barrier-sub]');

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

  var btnWarm = document.querySelector('[data-btn-warm]');
  var btnStampede = document.querySelector('[data-btn-stampede]');
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
    var barrierTitle = document.querySelector('[data-barrier-title]');
    var barrierDescription = document.querySelector('[data-barrier-sub]');
    if (barrierTitle) barrierTitle.textContent = currentMode === 'before' ? 'INDEPENDENT REQUESTS' : 'ONE WORKER COMPUTES';
    if (barrierDescription) barrierDescription.textContent = currentMode === 'before' ? 'Each cache miss starts its own refresh' : 'Other requests wait for the shared result';
    codeBlocks.forEach(function (block) {
      block.hidden = block.getAttribute('data-code') !== currentMode;
    });

    if (currentMode === 'before') {
      if (codeTitle) codeTitle.textContent = 'unshielded_redis.py';
      if (codeKicker) codeKicker.textContent = 'UNCOORDINATED CACHE REFRESH';
    } else {
      if (codeTitle) codeTitle.textContent = 'stampede_shield.py';
      if (codeKicker) codeKicker.textContent = 'FERRICSTORE FETCH_OR_COMPUTE API';
    }
  }

  // --- ACTION 1: Normal Warm Cache Hit ---
  function runWarmHit(isModeReset) {
    clearLogs();
    clearAllTimeouts();
    updateActiveCodeBlock();

    if (livePill) livePill.className = 'live-pill';
    if (liveStatus) liveStatus.textContent = isModeReset ? 'WARM CACHE READY' : 'WARM CACHE HIT';

    if (!isModeReset) log('info', 'GET product:iphone16_pro ➔ Cache Hit (TTL remaining: 48s)...');

    if (metricQueries) metricQueries.textContent = '0 queries in this warm-cache run';
    if (metricQueriesSub) metricQueriesSub.textContent = 'The value is already in memory';
    if (metricPool) metricPool.textContent = '0 / 100 Connections (0%)';
    if (metricPoolSub) metricPoolSub.textContent = 'No database connection needed';
    if (metricLatency) metricLatency.textContent = 'In-memory path';
    if (metricLatencySub) metricLatencySub.textContent = 'Timing depends on the workload';
    if (metricHealth) metricHealth.textContent = 'Healthy in this model';
    if (metricHealthSub) metricHealthSub.textContent = 'Warm-cache example';

    if (barrierCard) barrierCard.className = 'arena-card';
    if (barrierVal) barrierVal.textContent = 'Value is warm';
    if (barrierBadge) { barrierBadge.className = 'a-badge ok'; barrierBadge.textContent = '✓ Ready in RAM'; }
    if (barrierFill) barrierFill.style.width = '0%';

    if (dbCard) dbCard.className = 'arena-card';
    if (dbVal) dbVal.textContent = '0 Queries (0% Load)';
    if (dbBadge) { dbBadge.className = 'a-badge ok'; dbBadge.textContent = '✓ Idle'; }
    if (dbFill) dbFill.style.width = '0%';

    if (expIcon) expIcon.textContent = '⚡';
    if (expTitle) expTitle.textContent = isModeReset ? 'Mode selected; ready to run' : 'Read a value already in memory';
    if (expDesc) expDesc.textContent = isModeReset
      ? 'Choose Read warm value to confirm the cached path, or expire the key to test the refresh behavior.'
      : 'Read warm value serves the cached value without a database query. Then expire the key to compare the refresh paths.';

    if (outcomeCallout) outcomeCallout.className = 'outcome-callout good';
    if (outcomeLabel) outcomeLabel.textContent = isModeReset ? 'READY' : 'THIS EXAMPLE';
    if (outcomeTitle) outcomeTitle.textContent = isModeReset ? 'CHOOSE AN ACTION TO TEST THIS MODE' : 'THE WARM VALUE NEEDS NO DATABASE QUERY';
    if (outcomeSub) outcomeSub.textContent = isModeReset
      ? 'The setup changed; no scenario has run yet.'
      : 'The value for product:iphone16_pro is already in memory. Expire it to see how many refreshes reach the database.';

    if (!isModeReset) highlightCodeLine(currentMode === 'before' ? 2 : 5);
  }

  // --- ACTION 2: 10,000 Request Stampede on Expired Key ---
  function runStampede() {
    clearLogs();
    clearAllTimeouts();
    updateActiveCodeBlock();

    log('warn', '⏳ [SECOND 60.00] product:iphone16_pro cache TTL expired.');
    log('danger', '👥 [SURGE] 10,000 concurrent requests arrived together.');

    if (currentMode === 'before') {
      // REDIS STAMPEDE OUTAGE
      if (livePill) livePill.className = 'live-pill is-crash';
      if (liveStatus) liveStatus.textContent = '💥 10,000 QUERIES CRASHING POSTGRES';

      log('danger', '💥 [CACHE MISS] All 10,000 requests missed and sent SQL queries to Postgres.');
      log('danger', '🚨 [POOL LIMIT] The modeled Postgres connection pool (100 max) reached 100%.');
      log('danger', '📉 [MODELED RESULT] 9,900 clients timed out with HTTP 504 Gateway Timeout.');

      if (metricQueries) metricQueries.textContent = '10,000 DB queries in this model';
      if (metricQueriesSub) metricQueriesSub.textContent = 'Every request refreshed the same value';

      if (metricPool) metricPool.textContent = '100 / 100 Max (100% used)';
      if (metricPoolSub) metricPoolSub.textContent = 'Modeled pool limit reached';

      if (metricLatency) metricLatency.textContent = '18,400 ms (modeled timeout)';
      if (metricLatencySub) metricLatencySub.textContent = 'Modeled client wait before timeout';

      if (metricHealth) metricHealth.textContent = '504 in this model';
      if (metricHealthSub) metricHealthSub.textContent = 'The modeled pool could not serve every request';

      if (barrierCard) barrierCard.className = 'arena-card is-tripped';
      if (barrierVal) barrierVal.textContent = 'No coordination (10,000 misses)';
      if (barrierBadge) { barrierBadge.className = 'a-badge tripped'; barrierBadge.textContent = 'All requests refreshed'; }
      if (barrierFill) { barrierFill.style.width = '100%'; barrierFill.style.background = '#ef4444'; }

      if (dbCard) dbCard.className = 'arena-card is-tripped';
      if (dbVal) dbVal.textContent = '10,000 queries (100% used)';
      if (dbBadge) { dbBadge.className = 'a-badge tripped'; dbBadge.textContent = 'Modeled pool limit'; }
      if (dbFill) { dbFill.style.width = '100%'; dbFill.style.background = '#ef4444'; }

      if (expIcon) expIcon.textContent = '💥';
      if (expTitle) expTitle.textContent = 'Uncoordinated refresh in this example';
      if (expDesc) expDesc.textContent = 'All 10,000 requests refreshed the same expired value at once. This modeled pool limit returned timeouts; actual behavior depends on the application and database.';

      if (outcomeCallout) outcomeCallout.className = 'outcome-callout bad';
      if (outcomeLabel) outcomeLabel.textContent = 'MODELED OUTCOME';
      if (outcomeTitle) outcomeTitle.textContent = '10,000 QUERIES REACHED THE DATABASE';
      if (outcomeSub) outcomeSub.textContent = 'In this model, cache expiry caused 10,000 concurrent SQL queries and the 100-connection pool returned 504 timeouts.';

      highlightCodeLine(6, true);

    } else {
      // FERRICSTORE FETCH_OR_COMPUTE
      if (livePill) livePill.className = 'live-pill';
      if (liveStatus) liveStatus.textContent = 'FETCH_OR_COMPUTE: 1 QUERY RUNNING';

      log('cyan', '🛡️ [ONE OWNER] Worker #1 acquired the compute lease for product:iphone16_pro.');
      log('cyan', '⏳ [WAITERS] 9,999 duplicate requests queued in memory (0 DB queries sent).');

      if (barrierCard) barrierCard.className = 'arena-card';
      if (barrierVal) barrierVal.textContent = 'Worker #1 is computing…';
      if (barrierBadge) { barrierBadge.className = 'a-badge ok'; barrierBadge.textContent = '9,999 waiting in RAM'; }
      if (barrierFill) { barrierFill.style.width = '100%'; barrierFill.style.background = 'linear-gradient(90deg, #0284c7, #38bdf8)'; }

      if (dbCard) dbCard.className = 'arena-card';
      if (dbVal) dbVal.textContent = '1 query running (1% load)';
      if (dbBadge) { dbBadge.className = 'a-badge ok'; dbBadge.textContent = '✓ One query in this model'; }
      if (dbFill) { dbFill.style.width = '2%'; dbFill.style.background = '#10b981'; }

      highlightCodeLine(5);

      animTimeouts.push(setTimeout(function () {
        log('success', '✓ [QUERY COMPLETE] Worker #1 finished the Postgres query in 12ms and set a new 60s TTL.');
        log('success', '⚡ [RELEASE] FerricStore returned the stored result to all 9,999 waiting clients.');

        if (livePill) livePill.className = 'live-pill';
        if (liveStatus) liveStatus.textContent = '✓ WAITER RELEASE COMPLETE';

        if (metricQueries) metricQueries.textContent = '1 query in this model';
        if (metricQueriesSub) metricQueriesSub.textContent = '9,999 repeated queries avoided in this run';

        if (metricPool) metricPool.textContent = '1 / 100 Connections (1%)';
        if (metricPoolSub) metricPoolSub.textContent = 'One compute owner in this model';
        if (metricLatency) metricLatency.textContent = 'Workload-dependent';
        if (metricHealth) metricHealth.textContent = 'Healthy in this model';
        if (metricHealthSub) metricHealthSub.textContent = 'Waiting requests received the stored result';

        if (barrierVal) barrierVal.textContent = 'Stored result shared';
        if (barrierBadge) { barrierBadge.className = 'a-badge ok'; barrierBadge.textContent = '✓ 10,000 served'; }

        if (dbVal) dbVal.textContent = '1 Query Finished (0.01s)';

        if (expIcon) expIcon.textContent = '🛡️';
        if (expTitle) expTitle.textContent = 'With FerricStore, one worker computes';
        if (expDesc) expDesc.textContent = 'Worker #1 computed the query once in 12ms. FerricStore returned the stored result to all 9,999 waiting clients with one database connection in this model.';

        if (outcomeCallout) outcomeCallout.className = 'outcome-callout good';
        if (outcomeLabel) outcomeLabel.textContent = 'MODELED OUTCOME';
        if (outcomeTitle) outcomeTitle.textContent = 'ONE QUERY, THEN THE RESULT IS SHARED';
        if (outcomeSub) outcomeSub.textContent = 'Worker #1 fetched from Postgres in 12ms. FerricStore returned the result to all 9,999 waiting clients with one DB query in this model.';

        highlightCodeLine(8);
      }, 700));
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
      runWarmHit(true);
    });
  });

  // --- Playback Buttons ---
  if (btnWarm) btnWarm.addEventListener('click', function () { runWarmHit(false); });
  if (btnStampede) btnStampede.addEventListener('click', runStampede);
  if (btnReset) btnReset.addEventListener('click', function () { clearAllTimeouts(); clearLogs(); runWarmHit(true); });

  // Init
  runWarmHit(true);
})();
