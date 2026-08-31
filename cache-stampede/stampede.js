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
    codeBlocks.forEach(function (block) {
      block.hidden = block.getAttribute('data-code') !== currentMode;
    });

    if (currentMode === 'before') {
      if (codeTitle) codeTitle.textContent = 'unshielded_redis.py';
      if (codeKicker) codeKicker.textContent = 'STANDARD REDIS (NO STAMPEDE SHIELD)';
    } else {
      if (codeTitle) codeTitle.textContent = 'stampede_shield.py';
      if (codeKicker) codeKicker.textContent = 'FERRICSTORE FETCH_OR_COMPUTE API';
    }
  }

  // --- ACTION 1: Normal Warm Cache Hit ---
  function runWarmHit() {
    clearLogs();
    clearAllTimeouts();
    updateActiveCodeBlock();

    if (livePill) livePill.className = 'live-pill';
    if (liveStatus) liveStatus.textContent = 'WARM CACHE HIT';

    log('info', 'GET product:iphone16_pro ➔ Cache Hit (TTL remaining: 48s)...');

    if (metricQueries) metricQueries.textContent = '0 queries in this warm-cache run';
    if (metricPool) metricPool.textContent = '0 / 100 Connections (0%)';
    if (metricLatency) metricLatency.textContent = 'In-memory path';
    if (metricHealth) metricHealth.textContent = 'Healthy in this model';

    if (barrierCard) barrierCard.className = 'arena-card';
    if (barrierVal) barrierVal.textContent = 'Cache Warm (No Lock Needed)';
    if (barrierBadge) { barrierBadge.className = 'a-badge ok'; barrierBadge.textContent = '✓ RAM Ready'; }
    if (barrierFill) barrierFill.style.width = '0%';

    if (dbCard) dbCard.className = 'arena-card';
    if (dbVal) dbVal.textContent = '0 Queries (0% Load)';
    if (dbBadge) { dbBadge.className = 'a-badge ok'; dbBadge.textContent = '✓ Idle'; }
    if (dbFill) dbFill.style.width = '0%';

    if (expIcon) expIcon.textContent = '⚡';
    if (expTitle) expTitle.textContent = 'Warm Cache Hit: In-Memory Response';
    if (expDesc) expDesc.textContent = 'Requests are served directly from RAM without touching the database. Click "💥 10,000 Stampede" to simulate an expired cache key!';

    if (outcomeCallout) outcomeCallout.className = 'outcome-callout good';
    if (outcomeLabel) outcomeLabel.textContent = 'NORMAL OPERATIONAL STATE';
    if (outcomeTitle) outcomeTitle.textContent = 'SERVED FROM THE WARM CACHE IN THIS RUN';
    if (outcomeSub) outcomeSub.textContent = 'Cache key product:iphone16_pro is warm and answering queries without database load in this run.';

    highlightCodeLine(currentMode === 'before' ? 2 : 5);
  }

  // --- ACTION 2: 10,000 Request Stampede on Expired Key ---
  function runStampede() {
    clearLogs();
    clearAllTimeouts();
    updateActiveCodeBlock();

    log('warn', '⏳ [SECOND 60.00] product:iphone16_pro cache TTL expired!');
    log('danger', '👥 [SURGE] 10,000 concurrent user requests arrived at the exact same millisecond!');

    if (currentMode === 'before') {
      // REDIS STAMPEDE OUTAGE
      if (livePill) livePill.className = 'live-pill is-crash';
      if (liveStatus) liveStatus.textContent = '💥 10,000 QUERIES CRASHING POSTGRES';

      log('danger', '💥 [DOGPILING DISASTER] All 10,000 requests missed cache and sent SQL queries to Postgres!');
      log('danger', '🚨 [POOL EXHAUSTION] Postgres connection pool (100 max) saturated to 100% in 4ms!');
      log('danger', '📉 [SITE OUTAGE] 9,900 clients timed out with HTTP 504 Gateway Timeout!');

      if (metricQueries) metricQueries.textContent = '10,000 DB Queries (Crash!)';
      if (metricQueriesSub) metricQueriesSub.textContent = '10,000 duplicate SQL queries';

      if (metricPool) metricPool.textContent = '100 / 100 Max (100% Saturated)';
      if (metricPoolSub) metricPoolSub.textContent = 'Connection pool exhausted';

      if (metricLatency) metricLatency.textContent = '18,400 ms (Timeout)';
      if (metricLatencySub) metricLatencySub.textContent = 'Users staring at error pages';

      if (metricHealth) metricHealth.textContent = '🚨 504 Gateway Outage';
      if (metricHealthSub) metricHealthSub.textContent = 'Postgres unresponsive';

      if (barrierCard) barrierCard.className = 'arena-card is-tripped';
      if (barrierVal) barrierVal.textContent = 'No Shield (10,000 Misses)';
      if (barrierBadge) { barrierBadge.className = 'a-badge tripped'; barrierBadge.textContent = '🚨 0% Protection'; }
      if (barrierFill) { barrierFill.style.width = '100%'; barrierFill.style.background = '#ef4444'; }

      if (dbCard) dbCard.className = 'arena-card is-tripped';
      if (dbVal) dbVal.textContent = '10,000 Queries (100% Saturated)';
      if (dbBadge) { dbBadge.className = 'a-badge tripped'; dbBadge.textContent = '💥 Pool Crashed'; }
      if (dbFill) { dbFill.style.width = '100%'; dbFill.style.background = '#ef4444'; }

      if (expIcon) expIcon.textContent = '💥';
      if (expTitle) expTitle.textContent = 'The Dogpiling / Thundering Herd Outage';
      if (expDesc) expDesc.textContent = 'Because Redis lacks native stampede protection, all 10,000 requests simultaneously queried Postgres, taking down the entire database!';

      if (outcomeCallout) outcomeCallout.className = 'outcome-callout bad';
      if (outcomeLabel) outcomeLabel.textContent = 'PRODUCTION OUTAGE HAZARD';
      if (outcomeTitle) outcomeTitle.textContent = '10,000 QUERIES EXHAUSTED DATABASE POOL';
      if (outcomeSub) outcomeSub.textContent = 'Cache expiry caused 10,000 concurrent SQL queries, crashing Postgres connection pools and returning 504 Gateway Timeouts to users.';

      highlightCodeLine(6, true);

    } else {
      // FERRICSTORE FETCH_OR_COMPUTE
      if (livePill) livePill.className = 'live-pill';
      if (liveStatus) liveStatus.textContent = 'FETCH_OR_COMPUTE: 1 QUERY RUNNING';

      log('cyan', '🛡️ [SHIELD ACTIVE] Worker #1 acquired single in-process compute lease for product:iphone16_pro.');
      log('cyan', '⏳ [IN-MEMORY QUEUE] 9,999 duplicate requests queued safely in memory (0 DB queries sent).');

      if (barrierCard) barrierCard.className = 'arena-card';
      if (barrierVal) barrierVal.textContent = 'Worker #1 Computing...';
      if (barrierBadge) { barrierBadge.className = 'a-badge ok'; barrierBadge.textContent = '9,999 Queued in RAM'; }
      if (barrierFill) { barrierFill.style.width = '100%'; barrierFill.style.background = 'linear-gradient(90deg, #0284c7, #38bdf8)'; }

      if (dbCard) dbCard.className = 'arena-card';
      if (dbVal) dbVal.textContent = '1 Query Running (1% Load)';
      if (dbBadge) { dbBadge.className = 'a-badge ok'; dbBadge.textContent = '✓ Healthy in this model'; }
      if (dbFill) { dbFill.style.width = '2%'; dbFill.style.background = '#10b981'; }

      highlightCodeLine(5);

      animTimeouts.push(setTimeout(function () {
        log('success', '✓ [QUERY COMPLETE] Worker #1 finished Postgres query in 12ms. Setting new 60s TTL in Keydir.');
        log('success', '⚡ [BROADCAST] FerricStore released the stored result to all 9,999 waiting clients!');

        if (livePill) livePill.className = 'live-pill';
        if (liveStatus) liveStatus.textContent = '✓ WAITER RELEASE COMPLETE';

        if (metricQueries) metricQueries.textContent = '1 Query (9,999 Prevented!)';
        if (metricQueriesSub) metricQueriesSub.textContent = '9,999 repeated queries avoided in this run';

        if (metricPool) metricPool.textContent = '1 / 100 Connections (1%)';
        if (metricLatency) metricLatency.textContent = 'Workload-dependent';
        if (metricHealth) metricHealth.textContent = 'Healthy in this model';

        if (barrierVal) barrierVal.textContent = 'Broadcast Complete';
        if (barrierBadge) { barrierBadge.className = 'a-badge ok'; barrierBadge.textContent = '✓ 10,000 Served'; }

        if (dbVal) dbVal.textContent = '1 Query Finished (0.01s)';

        if (expIcon) expIcon.textContent = '🛡️';
        if (expTitle) expTitle.textContent = 'Dogpiling Shield: 9,999 DB Queries Eliminated';
        if (expDesc) expDesc.textContent = 'Worker #1 computed the SQL query once in 12ms. FerricStore broadcasted the stored result to all 9,999 waiting clients with 1 single DB connection!';

        if (outcomeCallout) outcomeCallout.className = 'outcome-callout good';
        if (outcomeLabel) outcomeLabel.textContent = 'DOGPILING SHIELD OUTCOME';
        if (outcomeTitle) outcomeTitle.textContent = '9,999 DATABASE QUERIES PREVENTED';
        if (outcomeSub) outcomeSub.textContent = 'Worker #1 fetched from Postgres in 12ms. FerricStore broadcasted the stored result to all 9,999 waiting clients with 1 single DB query.';

        highlightCodeLine(8);
      }, 700));
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
      runWarmHit();
    });
  });

  // --- Playback Buttons ---
  if (btnWarm) btnWarm.addEventListener('click', runWarmHit);
  if (btnStampede) btnStampede.addEventListener('click', runStampede);
  if (btnReset) btnReset.addEventListener('click', runWarmHit);

  // Init
  runWarmHit();
})();
