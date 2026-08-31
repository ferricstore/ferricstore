(function () {
  'use strict';

  var currentMode = 'before';
  var currentSdk = 'states';
  var animTimeouts = [];

  // DOM Elements
  var modeButtons = document.querySelectorAll('[data-mode-btn]');
  var sdkButtons = document.querySelectorAll('[data-sdk-style]');

  var codeBlocks = document.querySelectorAll('[data-code]');
  var codeTitle = document.querySelector('[data-code-title]');
  var codeKicker = document.querySelector('[data-code-kicker]');
  var codeBadge = document.querySelector('[data-code-badge]');

  var guardBudgetCard = document.querySelector('[data-guard-budget-card]');
  var guardBudgetVal = document.querySelector('[data-guard-budget-val]');
  var guardBudgetBadge = document.querySelector('[data-guard-budget-badge]');
  var guardBudgetFill = document.querySelector('[data-guard-budget-fill]');

  var guardBreakerCard = document.querySelector('[data-guard-breaker-card]');
  var guardBreakerVal = document.querySelector('[data-guard-breaker-val]');
  var guardBreakerBadge = document.querySelector('[data-guard-breaker-badge]');
  var guardBreakerFill = document.querySelector('[data-guard-breaker-fill]');

  var turnsStack = document.querySelector('[data-turns-stack]');
  var turnsCounter = document.querySelector('[data-turns-counter]');

  var livePill = document.querySelector('[data-live-pill]');
  var liveStatus = document.querySelector('[data-live-status]');
  var termStream = document.querySelector('[data-term-stream]');

  var metricBudget = document.querySelector('[data-metric-budget]');
  var metricBreaker = document.querySelector('[data-metric-breaker]');
  var metricRehydrate = document.querySelector('[data-metric-rehydrate]');
  var metricDedup = document.querySelector('[data-metric-dedup]');

  var outcomeCallout = document.querySelector('[data-outcome-callout]');
  var outcomeLabel = document.querySelector('[data-outcome-label]');
  var outcomeTitle = document.querySelector('[data-outcome-title]');
  var outcomeSub = document.querySelector('[data-outcome-sub]');

  var btnPlay = document.querySelector('[data-btn-play]');
  var btnBudget = document.querySelector('[data-btn-budget]');
  var btnBreaker = document.querySelector('[data-btn-breaker]');
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
      if (codeTitle) codeTitle.textContent = 'unmanaged_agent.py';
      if (codeKicker) codeKicker.textContent = 'BLIND WHILE-TRUE (UNPROTECTED)';
      if (codeBadge) { codeBadge.textContent = 'NO GUARDS'; codeBadge.style.background = 'rgba(239,68,68,0.25)'; codeBadge.style.color = '#fca5a5'; }
    } else {
      if (codeTitle) codeTitle.textContent = currentSdk === 'steps' ? 'agent_continue.py' : 'agent_fsm.py';
      if (codeKicker) codeKicker.textContent = currentSdk === 'steps' ? 'FLOW.STEP_CONTINUE API' : 'FERRICSTORE STATES API';
      if (codeBadge) { codeBadge.textContent = 'BUDGET + BREAKER'; codeBadge.style.background = 'rgba(16,185,129,0.25)'; codeBadge.style.color = '#6ee7b7'; }
    }
  }

  function resetToIdle() {
    clearAllTimeouts();
    if (turnsStack) turnsStack.innerHTML = '';
    if (turnsCounter) turnsCounter.textContent = 'Turn 0 of 4';

    // Reset Budget Guard
    if (guardBudgetCard) guardBudgetCard.className = 'guard-card';
    if (guardBudgetVal) guardBudgetVal.textContent = '$0.00 / $5.00';
    if (guardBudgetBadge) { guardBudgetBadge.className = 'guard-badge ok'; guardBudgetBadge.textContent = '✓ Within Safe Limit'; }
    if (guardBudgetFill) { guardBudgetFill.style.width = '0%'; guardBudgetFill.style.background = 'linear-gradient(90deg, #10b981, #f59e0b)'; }

    // Reset Breaker Guard
    if (guardBreakerCard) guardBreakerCard.className = 'guard-card';
    if (guardBreakerVal) guardBreakerVal.textContent = 'State: CLOSED (Healthy)';
    if (guardBreakerBadge) { guardBreakerBadge.className = 'guard-badge ok'; guardBreakerBadge.textContent = '0 / 3 Errors'; }
    if (guardBreakerFill) { guardBreakerFill.style.width = '0%'; guardBreakerFill.style.background = 'linear-gradient(90deg, #10b981, #ef4444)'; }

    if (livePill) livePill.className = 'live-pill';
    if (liveStatus) liveStatus.textContent = 'READY';

    if (currentMode === 'before') {
      if (metricBudget) metricBudget.textContent = 'No Limit ($4,000 Risk)';
      if (metricBreaker) metricBreaker.textContent = 'Disabled (Slams 503 APIs)';
      if (metricRehydrate) metricRehydrate.textContent = 'Lost on Crash (0.0ms)';
      if (metricDedup) metricDedup.textContent = '0% (Duplicate Tools)';

      if (outcomeCallout) outcomeCallout.className = 'outcome-callout bad';
      if (outcomeLabel) outcomeLabel.textContent = 'UNPROTECTED AGENT HAZARD';
      if (outcomeTitle) outcomeTitle.textContent = 'RUNAWAY SPEND &amp; 3RD-PARTY OVERLOAD';
      if (outcomeSub) outcomeSub.textContent = 'Agent lacks financial budget caps and service circuit breakers. Infinite loops drain balances; outages cause 429 bans.';
    } else {
      if (metricBudget) metricBudget.textContent = '$5.00 Hard Ceiling';
      if (metricBreaker) metricBreaker.textContent = 'Active (3-Fail Trip)';
      if (metricRehydrate) metricRehydrate.textContent = 'Durable resume';
      if (metricDedup) metricDedup.textContent = 'Fenced state writes';

      if (outcomeCallout) outcomeCallout.className = 'outcome-callout good';
      if (outcomeLabel) outcomeLabel.textContent = 'AGENT GUARDIAN OUTCOME';
      if (outcomeTitle) outcomeTitle.textContent = 'DUAL RELIABILITY SAFEGUARDS ACTIVE';
      if (outcomeSub) outcomeSub.textContent = 'Budget cap protects your financial balance. Circuit breaker protects downstream 3rd-party APIs from overload during outages.';
    }
  }

  function addTurnRow(num, action, cost, status) {
    if (!turnsStack) return;
    var row = document.createElement('div');
    row.className = 'turn-item-compact ' + status;
    row.innerHTML = `<div class="t-info"><strong>Turn ${num}: ${action}</strong><small>${status === 'done' ? '✓ Checkpointed' : (status === 'crashed' ? '❌ 503 Error' : '⚡ Trip Breaker')}</small></div><span class="t-cost">+${cost}</span>`;
    turnsStack.appendChild(row);
  }

  // --- SCENARIO 1: Normal Run (4 turns within budget, healthy API) ---
  function runNormalSimulation() {
    clearLogs();
    resetToIdle();
    log('info', '🤖 Starting autonomous market research agent query: "Analyze EV battery supply chain trends"...');

    var turns = [
      { num: 1, action: 'Search Perplexity for EV battery raw material prices', cost: '$0.08', spend: 0.08 },
      { num: 2, action: 'Scrape Bloomberg Lithium index &amp; cathode data', cost: '$0.12', spend: 0.20 },
      { num: 3, action: 'Execute Python pandas correlation model', cost: '$0.10', spend: 0.30 },
      { num: 4, action: 'Synthesize executive briefing via GPT-4o', cost: '$0.14', spend: 0.44 }
    ];

    turns.forEach(function (t, idx) {
      animTimeouts.push(setTimeout(function () {
        if (turnsCounter) turnsCounter.textContent = `Turn ${t.num} of 4`;
        addTurnRow(t.num, t.action, t.cost, 'done');
        log('success', `✓ [TURN ${t.num}] ${t.action} completed. Next workflow state committed durably.`);

        var pct = Math.round((t.spend / 5.00) * 100);
        if (guardBudgetVal) guardBudgetVal.textContent = `$${t.spend.toFixed(2)} / $5.00`;
        if (guardBudgetFill) guardBudgetFill.style.width = pct + '%';

        highlightCodeLine(idx === 0 ? 3 : 15);
      }, (idx + 1) * 600));
    });

    animTimeouts.push(setTimeout(function () {
      if (liveStatus) liveStatus.textContent = 'SUCCESS: BRIEFING COMPLETE';
      log('cyan', '🎉 Workflow finished in 4 turns ($0.44 total). Budget safe (9% used), Circuit Breaker healthy (0 errors).');
    }, 3000));
  }

  // --- SCENARIO 2: Test Financial Budget Limit ($5.00 Ceiling) ---
  function runBudgetCapSimulation() {
    clearLogs();
    resetToIdle();
    log('warn', '💸 [SIMULATION] Injecting runaway infinite reasoning loop (agent stuck hallucinating tool calls)...');

    if (currentMode === 'before') {
      // Unmanaged: Burns $48.50
      if (livePill) livePill.className = 'live-pill is-crash';
      if (liveStatus) liveStatus.textContent = 'RUNAWAY LOOP BURNING BUDGET';
      log('danger', '💥 [NO BUDGET GUARD] Agent looped 500 times in while-True! OpenAI API charged $4,850.00!');

      for (var i = 1; i <= 4; i++) {
        addTurnRow(i * 10, 'Hallucinated tool call loop #' + (i * 10), '+$12.50', 'crashed');
      }
      if (guardBudgetCard) guardBudgetCard.className = 'guard-card is-tripped';
      if (guardBudgetVal) guardBudgetVal.textContent = '$4,850.00 (NO LIMIT!)';
      if (guardBudgetBadge) { guardBudgetBadge.className = 'guard-badge tripped'; guardBudgetBadge.textContent = '🚨 $4,850 OVERDRAW'; }
      if (guardBudgetFill) { guardBudgetFill.style.width = '100%'; guardBudgetFill.style.background = '#ef4444'; }
      highlightCodeLine(2, true);
    } else {
      // FerricStore: Hard stop at $5.00
      log('info', 'Turn 1-6 executing... Tracking cumulative token cost monotonically...');
      addTurnRow(1, 'Research search', '$0.80', 'done');
      addTurnRow(2, 'Deep web scrape', '$1.40', 'done');
      addTurnRow(3, 'Data modeling', '$1.50', 'done');
      addTurnRow(4, 'Attempting Turn 4 ($1.60)...', '$1.60', 'crashed');

      animTimeouts.push(setTimeout(function () {
        if (guardBudgetCard) guardBudgetCard.className = 'guard-card is-tripped';
        if (guardBudgetVal) guardBudgetVal.textContent = '$5.00 / $5.00 (MAX CAP)';
        if (guardBudgetBadge) { guardBudgetBadge.className = 'guard-badge tripped'; guardBudgetBadge.textContent = '🛑 Budget Cap Tripped'; }
        if (guardBudgetFill) { guardBudgetFill.style.width = '100%'; guardBudgetFill.style.background = '#ef4444'; }

        if (livePill) livePill.className = 'live-pill is-crash';
        if (liveStatus) liveStatus.textContent = '🛑 BUDGET CAP HALT ($5.00)';
        log('danger', '🛑 [BUDGET ENFORCER] Cumulative spend hit $5.00 hard limit. Execution halted! Saved $4,845 in runaway credit loss.');
        highlightCodeLine(8, true);
      }, 700));
    }
  }

  // --- SCENARIO 3: Test 3rd-Party Circuit Breaker (OpenAI 503 Outage) ---
  function runBreakerSimulation() {
    clearLogs();
    resetToIdle();
    log('warn', '⚡ [SIMULATION] Simulating 3rd-party OpenAI API outage (HTTP 503 Service Unavailable)...');

    if (currentMode === 'before') {
      // Unmanaged: Slams 503 API
      if (livePill) livePill.className = 'live-pill is-crash';
      if (liveStatus) liveStatus.textContent = 'SLAMMING 503 API (429 BAN)';
      log('danger', '💥 [NO CIRCUIT BREAKER] Agent retried 100 times in 1 second during 503 outage. OpenAI issued 429 IP Rate Limit Ban!');

      addTurnRow(1, 'OpenAI think() -> 503 Outage', '$0.00', 'crashed');
      addTurnRow(2, 'Immediate retry #2 -> 503 Outage', '$0.00', 'crashed');
      addTurnRow(3, 'Immediate retry #3 -> 429 BAN', '$0.00', 'crashed');

      if (guardBreakerCard) guardBreakerCard.className = 'guard-card is-tripped';
      if (guardBreakerVal) guardBreakerVal.textContent = 'State: OVERLOADED (429 Ban)';
      if (guardBreakerBadge) { guardBreakerBadge.className = 'guard-badge tripped'; guardBreakerBadge.textContent = '🚨 100 Failed Retries'; }
      if (guardBreakerFill) { guardBreakerFill.style.width = '100%'; guardBreakerFill.style.background = '#ef4444'; }
      highlightCodeLine(3, true);
    } else {
      // FerricStore: Trips Breaker to OPEN
      log('info', 'Turn 1: OpenAI returns HTTP 503. Recording failure 1/3 in Circuit Breaker...');
      addTurnRow(1, 'OpenAI think() -> HTTP 503', '$0.00', 'crashed');

      animTimeouts.push(setTimeout(function () {
        log('warn', 'Turn 2: OpenAI returns HTTP 503. Recording failure 2/3 in Circuit Breaker...');
        addTurnRow(2, 'OpenAI think() -> HTTP 503', '$0.00', 'crashed');
        if (guardBreakerBadge) guardBreakerBadge.textContent = '2 / 3 Errors';
        if (guardBreakerFill) guardBreakerFill.style.width = '66%';
      }, 600));

      animTimeouts.push(setTimeout(function () {
        log('danger', 'Turn 3: OpenAI returns HTTP 503 (3rd consecutive failure). TRIPPING CIRCUIT BREAKER TO OPEN!');
        addTurnRow(3, 'OpenAI think() -> 3rd 503', '$0.00', 'crashed');

        if (guardBreakerCard) guardBreakerCard.className = 'guard-card is-tripped';
        if (guardBreakerVal) guardBreakerVal.textContent = 'State: OPEN (Throttling 30s)';
        if (guardBreakerBadge) { guardBreakerBadge.className = 'guard-badge tripped'; guardBreakerBadge.textContent = '⚡ BREAKER TRIPPED'; }
        if (guardBreakerFill) { guardBreakerFill.style.width = '100%'; guardBreakerFill.style.background = '#ef4444'; }

        if (livePill) livePill.className = 'live-pill is-crash';
        if (liveStatus) liveStatus.textContent = '⚡ CIRCUIT BREAKER TRIPPED (OPEN)';
        log('cyan', '💤 [CIRCUIT BREAKER OPEN] Stopped outbound calls to api.openai.com. Flow entered a durable cooldown state for a scheduled retry.');
        highlightCodeLine(12, true);
      }, 1300));
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
      updateActiveCodeBlock();
      runNormalSimulation();
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
  if (btnPlay) btnPlay.addEventListener('click', runNormalSimulation);
  if (btnBudget) btnBudget.addEventListener('click', runBudgetCapSimulation);
  if (btnBreaker) btnBreaker.addEventListener('click', runBreakerSimulation);
  if (btnReset) btnReset.addEventListener('click', function () { resetToIdle(); log('info', 'Reset. Choose a test button to run.'); });

  // Init
  updateActiveCodeBlock();
  runNormalSimulation();
})();
