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

  function resetSimulation() {
    clearLogs();
    clearAllTimeouts();
    updateModeUI();
    if (livePill) livePill.className = 'live-pill';
    if (liveStatus) liveStatus.textContent = 'READY TO RUN';
    if (fillMid) fillMid.style.width = '0%';
    if (fillRight) fillRight.style.width = '0%';
    highlightCodeLine();

    if (currentMode === 'pubsub') {
      if (valLeft) valLeft.textContent = 'Ready to publish';
      if (badgeLeft) badgeLeft.textContent = 'Ready now';
      if (valMid) valMid.textContent = 'Message goes out now';
      if (badgeMid) badgeMid.textContent = '✓ No saved entry';
      if (valRight) valRight.textContent = '0 of 3 received';
      if (badgeRight) badgeRight.textContent = 'Waiting for message';
      if (expIcon) expIcon.textContent = '💡';
      if (expTitle) expTitle.textContent = 'Preview: live message, no saved copy';
      if (expDesc) expDesc.textContent = 'Pub/Sub reaches listeners connected now. Run step 1, then step 2 to see who receives it and what a reader cannot catch up on later.';
      if (outcomeCallout) outcomeCallout.className = 'outcome-callout good';
      if (outcomeLabel) outcomeLabel.textContent = 'What this run shows';
      if (outcomeTitle) outcomeTitle.textContent = 'Ready to compare live and saved messages';
      if (outcomeSub) outcomeSub.textContent = 'Run the named actions to see who receives the message now and which mode keeps a saved entry for a worker.';
    } else {
      if (valLeft) valLeft.textContent = 'Ready to save';
      if (badgeLeft) badgeLeft.textContent = 'Ordered ID';
      if (valMid) valMid.textContent = 'No saved event yet';
      if (badgeMid) badgeMid.textContent = 'Waiting for step 1';
      if (valRight) valRight.textContent = 'Waiting for a read';
      if (badgeRight) badgeRight.textContent = 'Not acknowledged yet';
      if (expIcon) expIcon.textContent = '📜';
      if (expTitle) expTitle.textContent = 'Preview: a saved event can wait';
      if (expDesc) expDesc.textContent = 'Run step 1 to save the order, then step 2 to let Worker w1 read and acknowledge it.';
      if (outcomeCallout) outcomeCallout.className = 'outcome-callout purple';
      if (outcomeLabel) outcomeLabel.textContent = 'What this run shows';
      if (outcomeTitle) outcomeTitle.textContent = 'Ready to compare live and saved messages';
      if (outcomeSub) outcomeSub.textContent = 'Run the named actions to see who receives the message now and which mode keeps a saved entry for a worker.';
    }
  }

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
      if (btnAction1) btnAction1.textContent = '⚡ 1. Send example message';
      if (btnAction2) btnAction2.textContent = '👥 2. Show delivery';

      if (metricLbl1) metricLbl1.textContent = 'HOW MESSAGES MOVE';
      if (metricVal1) { metricVal1.textContent = 'Live broadcast'; metricVal1.className = 'metric-val text-cyan'; }
      if (metricSub1) metricSub1.textContent = 'Connected listeners receive it now';

      if (metricLbl2) metricLbl2.textContent = 'WHERE IT GOES';
      if (metricVal2) { metricVal2.textContent = 'Not saved for later'; metricVal2.className = 'metric-val text-green'; }
      if (metricSub2) metricSub2.textContent = 'No saved entry to catch up on';

      if (metricLbl3) metricLbl3.textContent = 'WHO RECEIVES IT';
      if (metricVal3) { metricVal3.textContent = 'Every connected listener'; metricVal3.className = 'metric-val text-green'; }
      if (metricSub3) metricSub3.textContent = 'The same event goes to each one';

      if (metricLbl4) metricLbl4.textContent = 'GOOD FIT';
      if (metricVal4) { metricVal4.textContent = 'Cache refreshes and live alerts'; metricVal4.className = 'metric-val text-cyan'; }
      if (metricSub4) metricSub4.textContent = 'Updates that can arrive now';

      if (titleLeft) titleLeft.textContent = 'Event publisher';
      if (subLeft) subLeft.textContent = 'Live dispatch';
      if (valLeft) valLeft.textContent = 'Ready to publish';
      if (badgeLeft) badgeLeft.textContent = 'Ready now';
      if (codeLeft) codeLeft.textContent = 'PUBLISH "cache:invalidation" "product:42"';

      if (titleMid) titleMid.textContent = 'Live broadcast';
      if (subMid) subMid.textContent = 'Not saved for later';
      if (valMid) valMid.textContent = 'Message goes out now';
      if (badgeMid) badgeMid.textContent = '✓ No saved entry';

      if (titleRight) titleRight.textContent = 'Connected listeners';
      if (subRight) subRight.textContent = '3 active listeners';
      if (valRight) valRight.textContent = '3 of 3 received';
      if (badgeRight) badgeRight.textContent = '✓ Received now';

      if (outcomeCallout) outcomeCallout.className = 'outcome-callout good';
      if (outcomeLabel) outcomeLabel.textContent = 'PUBSUB ARCHITECTURE BENEFIT';
      if (outcomeTitle) outcomeTitle.textContent = 'EPHEMERAL FANOUT TO ACTIVE SUBSCRIBERS';
      if (outcomeSub) outcomeSub.textContent = 'Pub/Sub pushes notifications to listeners that are connected now; it does not create a replayable message log.';

    } else {
      if (codeTitle) codeTitle.textContent = 'stream_worker.py';
      if (codeKicker) codeKicker.textContent = 'FERRICSTORE STREAMS API';
      if (codeBadge) { codeBadge.textContent = 'DURABLE STREAM'; codeBadge.style.background = 'rgba(139,92,246,0.25)'; codeBadge.style.color = '#c4b5fd'; }

      if (targetName) targetName.textContent = 'events:orders';
      if (btnAction1) btnAction1.textContent = '⚡ 1. Save order event';
      if (btnAction2) btnAction2.textContent = '👷 2. Worker reads and acknowledges';

      if (metricLbl1) metricLbl1.textContent = 'HOW MESSAGES MOVE';
      if (metricVal1) { metricVal1.textContent = 'Saved stream entry'; metricVal1.className = 'metric-val text-purple'; }
      if (metricSub1) metricSub1.textContent = 'Committed through configured durability';

      if (metricLbl2) metricLbl2.textContent = 'MESSAGE ORDER';
      if (metricVal2) { metricVal2.textContent = 'Ordered event IDs'; metricVal2.className = 'metric-val text-green'; }
      if (metricSub2) metricSub2.textContent = 'The example shows an HLC timestamp';

      if (metricLbl3) metricLbl3.textContent = 'WHO READS IT';
      if (metricVal3) { metricVal3.textContent = 'One consumer-group worker'; metricVal3.className = 'metric-val text-cyan'; }
      if (metricSub3) metricSub3.textContent = 'Acknowledgement tracks progress';

      if (metricLbl4) metricLbl4.textContent = 'GOOD FIT';
      if (metricVal4) { metricVal4.textContent = 'Orders and background jobs'; metricVal4.className = 'metric-val text-purple'; }
      if (metricSub4) metricSub4.textContent = 'Read again from a saved position';

      if (titleLeft) titleLeft.textContent = 'Event producer';
      if (subLeft) subLeft.textContent = 'Save an order event';
      if (valLeft) valLeft.textContent = 'Ready to save';
      if (badgeLeft) badgeLeft.textContent = 'Ordered ID';
      if (codeLeft) codeLeft.textContent = 'XADD "events:orders" * "order_id" "1001"';

      if (titleMid) titleMid.textContent = 'Saved event log';
      if (subMid) subMid.textContent = 'Durable storage record';
      if (valMid) valMid.textContent = 'ID: 1718000000000-0';
      if (badgeMid) badgeMid.textContent = '✓ Saved';

      if (titleRight) titleRight.textContent = 'Consumer-group worker';
      if (subRight) subRight.textContent = 'Worker w1 reads saved work';
      if (valRight) valRight.textContent = 'Waiting for a read';
      if (badgeRight) badgeRight.textContent = 'Not acknowledged yet';

      if (outcomeCallout) outcomeCallout.className = 'outcome-callout purple';
      if (outcomeLabel) outcomeLabel.textContent = 'STREAMS ARCHITECTURE BENEFIT';
      if (outcomeTitle) outcomeTitle.textContent = 'SAVED ENTRIES CAN BE READ AGAIN';
      if (outcomeSub) outcomeSub.textContent = 'Streams persist events with ordered IDs. A consumer group tracks acknowledgement and may redeliver an unacknowledged entry after reconnecting.';
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
        if (expTitle) expTitle.textContent = 'Live broadcast reached active listeners';
        if (liveStatus) liveStatus.textContent = 'DELIVERED TO 3 CONNECTED LISTENERS';
        if (valLeft) valLeft.textContent = 'Message sent';
        if (expDesc) expDesc.textContent = 'All 3 connected listeners received the cache-bust signal. A listener that was not connected for this event has no saved entry to catch up on.';

        highlightCodeLine(3);
      }, 300));

    } else {
      if (liveStatus) liveStatus.textContent = 'APPENDING TO DURABLE STREAM';
      log('info', 'XADD events:orders * order_id 1001 amount 250.00...');

      animTimeouts.push(setTimeout(function () {
        log('cyan', '📜 [HLC ID] Generated cluster-monotonic ID: 1718000000000-0.');
        log('success', '✓ [DISK COMMIT] Order committed to NVMe Raft log. Stored durably for replay.');

        if (expIcon) expIcon.textContent = '📜';
        if (expTitle) expTitle.textContent = 'Order saved in the event log';
        if (liveStatus) liveStatus.textContent = 'ENTRY SAVED · READY FOR WORKER';
        if (valLeft) valLeft.textContent = 'Order sent';
        if (expDesc) expDesc.textContent = 'The order event is persisted to NVMe SSD with an ordered Hybrid Logical Clock (HLC) ID. Click "👷 2. Worker reads and acknowledges" to let Worker w1 pick it up.';

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
      if (subRight) subRight.textContent = '100 active listeners';
      if (valRight) valRight.textContent = '0 of 100 received';
      log('info', 'Broadcasting user session update to 100 microservice pods...');

      animTimeouts.push(setTimeout(function () {
        log('success', '⚡ [FANOUT] The event was pushed to 100 active subscribers.');
        log('cyan', '✓ Zero database queries, zero disk locks, zero queue backlog.');

        if (expIcon) expIcon.textContent = '👥';
        if (expTitle) expTitle.textContent = 'The message reached 100 active listeners';
        if (liveStatus) liveStatus.textContent = 'DELIVERED TO 100 CONNECTED LISTENERS';
        if (valRight) valRight.textContent = '100 of 100 received';
        if (valLeft) valLeft.textContent = 'Message sent';
        if (expDesc) expDesc.textContent = 'The event was sent to 100 active listeners. Pub/Sub keeps this path live; it does not add a saved message entry for later readers.';

        highlightCodeLine(6);
      }, 350));

    } else {
      if (liveStatus) liveStatus.textContent = 'CONSUMER GROUP XREADGROUP & XACK';
      log('info', 'XREADGROUP GROUP workers w1 STREAMS events:orders >...');

      animTimeouts.push(setTimeout(function () {
        log('cyan', 'Worker w1 claimed order #1001 (PEL entry active)...');
        log('success', '✓ XACK events:orders workers 1718000000000-0 ➔ Acknowledged!');

        if (expIcon) expIcon.textContent = '👷';
        if (expTitle) expTitle.textContent = 'Worker w1 read and acknowledged the order';
        if (liveStatus) liveStatus.textContent = 'WORKER FINISHED · ENTRY ACKNOWLEDGED';
        if (valLeft) valLeft.textContent = 'Order sent';
        if (valRight) valRight.textContent = 'Order processed';
        if (badgeRight) badgeRight.textContent = '✓ Acknowledged';
        if (fillRight) fillRight.style.width = '100%';
        if (expDesc) expDesc.textContent = 'Worker w1 processed the order and acknowledged it. If w1 stops before acknowledgement, another worker may read the saved entry again after reconnecting.';

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
      resetSimulation();
    });
  });

  // --- Playback Buttons ---
  if (btnAction1) btnAction1.addEventListener('click', runAction1);
  if (btnAction2) btnAction2.addEventListener('click', runAction2);
  if (btnReset) btnReset.addEventListener('click', resetSimulation);

  // Init
  resetSimulation();
})();
