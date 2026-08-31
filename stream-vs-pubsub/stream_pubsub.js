(function () {
  'use strict';

  var currentMode = 'pubsub';
  var animTimeouts = [];

  // DOM Elements
  var modeButtons = document.querySelectorAll('[data-mode-btn]');
  var codeBlocks = document.querySelectorAll('[data-code]');
  var codeTitle = document.querySelector('[data-code-title]');
  var codeKicker = document.querySelector('[data-code-kicker]');
  var codeBadge = document.querySelector('[data-code-badge]');

  var metricLbl1 = document.querySelector('[data-metric-lbl-1]');
  var metricVal1 = document.querySelector('[data-metric-val-1]');
  var metricSub1 = document.querySelector('[data-metric-sub-1]');

  var metricLbl2 = document.querySelector('[data-metric-lbl-2]');
  var metricVal2 = document.querySelector('[data-metric-val-2]');
  var metricSub2 = document.querySelector('[data-metric-sub-2]');

  var metricLbl3 = document.querySelector('[data-metric-lbl-3]');
  var metricVal3 = document.querySelector('[data-metric-val-3]');
  var metricSub3 = document.querySelector('[data-metric-sub-3]');

  var metricLbl4 = document.querySelector('[data-metric-lbl-4]');
  var metricVal4 = document.querySelector('[data-metric-val-4]');
  var metricSub4 = document.querySelector('[data-metric-sub-4]');

  var targetName = document.querySelector('[data-target-name]');

  var titleLeft = document.querySelector('[data-title-left]');
  var subLeft = document.querySelector('[data-sub-left]');
  var valLeft = document.querySelector('[data-val-left]');
  var badgeLeft = document.querySelector('[data-badge-left]');
  var codeLeft = document.querySelector('[data-code-left]');

  var titleMid = document.querySelector('[data-title-mid]');
  var subMid = document.querySelector('[data-sub-mid]');
  var valMid = document.querySelector('[data-val-mid]');
  var badgeMid = document.querySelector('[data-badge-mid]');
  var fillMid = document.querySelector('[data-fill-mid]');

  var titleRight = document.querySelector('[data-title-right]');
  var subRight = document.querySelector('[data-sub-right]');
  var valRight = document.querySelector('[data-val-right]');
  var badgeRight = document.querySelector('[data-badge-right]');
  var fillRight = document.querySelector('[data-fill-right]');

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

  var btnAction1 = document.querySelector('[data-btn-action-1]');
  var btnAction2 = document.querySelector('[data-btn-action-2]');
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

    if (currentMode === 'pubsub') {
      if (codeTitle) codeTitle.textContent = 'pubsub_broadcast.py';
      if (codeKicker) codeKicker.textContent = 'FERRICSTORE PUBSUB API';
      if (codeBadge) { codeBadge.textContent = 'LIVE PUBSUB'; codeBadge.style.background = 'rgba(6,182,212,0.25)'; codeBadge.style.color = '#67e8f9'; }

      if (targetName) targetName.textContent = 'cache:invalidation';
      if (btnAction1) btnAction1.textContent = '⚡ 1. Broadcast Event';
      if (btnAction2) btnAction2.textContent = '👥 2. Multi-Subscriber Fanout';

      if (metricLbl1) metricLbl1.textContent = 'MESSAGING ARCHITECTURE';
      if (metricVal1) { metricVal1.textContent = 'In-Memory Socket Push'; metricVal1.className = 'metric-val text-cyan'; }
      if (metricSub1) metricSub1.textContent = 'Zero disk/WAL write overhead';

      if (metricLbl2) metricLbl2.textContent = 'FANOUT LATENCY';
      if (metricVal2) { metricVal2.textContent = 'Workload Dependent'; metricVal2.className = 'metric-val text-green'; }
      if (metricSub2) metricSub2.textContent = 'Direct TCP socket delivery';

      if (metricLbl3) metricLbl3.textContent = 'STORAGE FOOTPRINT';
      if (metricVal3) { metricVal3.textContent = 'No Message Log'; metricVal3.className = 'metric-val text-green'; }
      if (metricSub3) metricSub3.textContent = 'Messages are not retained for replay';

      if (metricLbl4) metricLbl4.textContent = 'BEST USED FOR';
      if (metricVal4) { metricVal4.textContent = 'Cache Invalidation & UI Toasts'; metricVal4.className = 'metric-val text-cyan'; }
      if (metricSub4) metricSub4.textContent = 'Real-time client alerts';

      if (titleLeft) titleLeft.textContent = 'EVENT PUBLISHER';
      if (subLeft) subLeft.textContent = 'In-Memory Dispatch';
      if (valLeft) valLeft.textContent = 'PUBLISH event';
      if (badgeLeft) badgeLeft.textContent = 'Live push';
      if (codeLeft) codeLeft.textContent = 'PUBLISH "cache:invalidation" "product:42"';

      if (titleMid) titleMid.textContent = 'IN-MEMORY FANOUT';
      if (subMid) subMid.textContent = 'No Replay Log';
      if (valMid) valMid.textContent = 'Ephemeral Message';
      if (badgeMid) badgeMid.textContent = '✓ No Message Persistence';

      if (titleRight) titleRight.textContent = 'CONNECTED SUBSCRIBERS';
      if (subRight) subRight.textContent = '3 Active Microservice Pods';
      if (valRight) valRight.textContent = '3 / 3 Delivered';
      if (badgeRight) badgeRight.textContent = '✓ Live Push';

      if (outcomeCallout) outcomeCallout.className = 'outcome-callout good';
      if (outcomeLabel) outcomeLabel.textContent = 'PUBSUB ARCHITECTURE BENEFIT';
      if (outcomeTitle) outcomeTitle.textContent = 'EPHEMERAL FANOUT TO ACTIVE SUBSCRIBERS';
      if (outcomeSub) outcomeSub.textContent = 'Pub/Sub pushes notifications to listeners that are connected now; it does not create a replayable message log.';

    } else {
      if (codeTitle) codeTitle.textContent = 'stream_worker.py';
      if (codeKicker) codeKicker.textContent = 'FERRICSTORE STREAMS API';
      if (codeBadge) { codeBadge.textContent = 'DURABLE STREAM'; codeBadge.style.background = 'rgba(139,92,246,0.25)'; codeBadge.style.color = '#c4b5fd'; }

      if (targetName) targetName.textContent = 'events:orders';
      if (btnAction1) btnAction1.textContent = '⚡ 1. Append Order (XADD)';
      if (btnAction2) btnAction2.textContent = '👷 2. Worker Read & ACK';

      if (metricLbl1) metricLbl1.textContent = 'MESSAGING ARCHITECTURE';
      if (metricVal1) { metricVal1.textContent = 'Durable Raft Append Log'; metricVal1.className = 'metric-val text-purple'; }
      if (metricSub1) metricSub1.textContent = 'NVMe SSD replicated storage';

      if (metricLbl2) metricLbl2.textContent = 'ID MONOTONICITY';
      if (metricVal2) { metricVal2.textContent = 'Hybrid Logical Clock (HLC)'; metricVal2.className = 'metric-val text-green'; }
      if (metricSub2) metricSub2.textContent = 'Strict ordering across shards';

      if (metricLbl3) metricLbl3.textContent = 'WORKER COORDINATION';
      if (metricVal3) { metricVal3.textContent = 'Consumer Groups (XGROUP)'; metricVal3.className = 'metric-val text-cyan'; }
      if (metricSub3) metricSub3.textContent = 'Load balanced with XACK';

      if (metricLbl4) metricLbl4.textContent = 'BEST USED FOR';
      if (metricVal4) { metricVal4.textContent = 'Orders, Ledgers & Task Queues'; metricVal4.className = 'metric-val text-purple'; }
      if (metricSub4) metricSub4.textContent = 'Replay from retained stream ID';

      if (titleLeft) titleLeft.textContent = 'EVENT PRODUCER';
      if (subLeft) subLeft.textContent = 'Mode-34 Batch Fast-Path';
      if (valLeft) valLeft.textContent = 'XADD event';
      if (badgeLeft) badgeLeft.textContent = 'HLC Timestamped';
      if (codeLeft) codeLeft.textContent = 'XADD "events:orders" * "order_id" "1001"';

      if (titleMid) titleMid.textContent = 'NVMe RAFT LOG';
      if (subMid) subMid.textContent = 'Persistent Storage Log';
      if (valMid) valMid.textContent = 'ID: 1718000000000-0';
      if (badgeMid) badgeMid.textContent = '✓ Raft Committed';

      if (titleRight) titleRight.textContent = 'CONSUMER GROUP';
      if (subRight) subRight.textContent = 'Group: workers / Consumer: w1';
      if (valRight) valRight.textContent = 'Offset: 1718000-0';
      if (badgeRight) badgeRight.textContent = '✓ XACK Committed';

      if (outcomeCallout) outcomeCallout.className = 'outcome-callout purple';
      if (outcomeLabel) outcomeLabel.textContent = 'STREAMS ARCHITECTURE BENEFIT';
      if (outcomeTitle) outcomeTitle.textContent = 'DURABLE ORDER LOGS &amp; CONSUMER-GROUP REPLAY';
      if (outcomeSub) outcomeSub.textContent = 'Streams persist events with HLC IDs. Consumer groups coordinate work and can redeliver unacknowledged entries after reconnecting.';
    }
  }

  // --- ACTION 1: Primary Action ---
  function runAction1() {
    clearLogs();
    clearAllTimeouts();
    updateModeUI();

    if (currentMode === 'pubsub') {
      if (liveStatus) liveStatus.textContent = 'BROADCASTING IN MEMORY';
      log('info', 'PUBLISH cache:invalidation "product:42:price_updated"...');

      animTimeouts.push(setTimeout(function () {
        log('success', '⚡ [PUBSUB DISPATCH] Message pushed to 3 active subscriber sockets.');
        log('cyan', '💾 [EPHEMERAL] No replayable message entry was created.');

        if (expIcon) expIcon.textContent = '⚡';
        if (expTitle) expTitle.textContent = 'In-Memory Broadcast to Active Listeners';
        if (expDesc) expDesc.textContent = 'All 3 connected web pods received the cache-bust signal. Actual latency depends on the deployment and workload.';

        highlightCodeLine(3);
      }, 300));

    } else {
      if (liveStatus) liveStatus.textContent = 'APPENDING TO DURABLE STREAM';
      log('info', 'XADD events:orders * order_id 1001 amount 250.00...');

      animTimeouts.push(setTimeout(function () {
        log('cyan', '📜 [HLC ID] Generated cluster-monotonic ID: 1718000000000-0.');
        log('success', '✓ [DISK COMMIT] Order committed to NVMe Raft log. Stored durably for replay.');

        if (expIcon) expIcon.textContent = '📜';
        if (expTitle) expTitle.textContent = 'Order Appended to Durable Raft Log';
        if (expDesc) expDesc.textContent = 'The order event is safely persisted to NVMe SSD with a monotonic Hybrid Logical Clock (HLC) ID. Click "👷 2. Worker Read & ACK"!';

        highlightCodeLine(3);
      }, 300));
    }
  }

  // --- ACTION 2: Secondary Action ---
  function runAction2() {
    clearLogs();
    clearAllTimeouts();
    updateModeUI();

    if (currentMode === 'pubsub') {
      if (liveStatus) liveStatus.textContent = 'MULTI-POD FANOUT: 100 PODS';
      log('info', 'Broadcasting user session update to 100 microservice pods...');

      animTimeouts.push(setTimeout(function () {
        log('success', '⚡ [FANOUT] The event was pushed to 100 active subscribers.');
        log('cyan', '✓ Zero database queries, zero disk locks, zero queue backlog.');

        if (expIcon) expIcon.textContent = '👥';
        if (expTitle) expTitle.textContent = 'High-Concurrency 100-Pod Broadcast';
        if (expDesc) expDesc.textContent = 'Pub/Sub scales effortlessly to thousands of connected sockets without adding any disk I/O or background queue lag.';

        highlightCodeLine(6);
      }, 350));

    } else {
      if (liveStatus) liveStatus.textContent = 'CONSUMER GROUP XREADGROUP & XACK';
      log('info', 'XREADGROUP GROUP workers w1 STREAMS events:orders >...');

      animTimeouts.push(setTimeout(function () {
        log('cyan', 'Worker w1 claimed order #1001 (PEL entry active)...');
        log('success', '✓ XACK events:orders workers 1718000000000-0 ➔ Acknowledged!');

        if (expIcon) expIcon.textContent = '👷';
        if (expTitle) expTitle.textContent = 'Consumer Group Work Coordination';
        if (expDesc) expDesc.textContent = 'Worker w1 processed the order and acknowledged it. If w1 had crashed mid-task, another worker could reclaim it with XCLAIM with 0 lost work!';

        highlightCodeLine(7);
      }, 400));
    }
  }

  // --- Mode Switch Buttons ---
  modeButtons.forEach(function (btn) {
    btn.addEventListener('click', function () {
      modeButtons.forEach(function (b) { b.classList.remove('is-selected'); b.setAttribute('aria-selected', 'false'); });
      btn.classList.add('is-selected');
      btn.setAttribute('aria-selected', 'true');
      currentMode = btn.getAttribute('data-mode-btn') || 'pubsub';
      document.body.setAttribute('data-mode', currentMode);
      runAction1();
    });
  });

  // --- Playback Buttons ---
  if (btnAction1) btnAction1.addEventListener('click', runAction1);
  if (btnAction2) btnAction2.addEventListener('click', runAction2);
  if (btnReset) btnReset.addEventListener('click', runAction1);

  // Init
  runAction1();
})();
