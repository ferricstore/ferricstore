(function () {
  'use strict';

  var currentMode = 'before';
  var currentSdk = 'states';
  var animTimeouts = [];
  var totalChunks = 16;

  var modeButtons = document.querySelectorAll('[data-mode-btn]');
  var sdkButtons = document.querySelectorAll('[data-sdk-style]');

  var codeBlocks = document.querySelectorAll('[data-code]');
  var codeTitle = document.querySelector('[data-code-title]');
  var codeKicker = document.querySelector('[data-code-kicker]');
  var codeBadge = document.querySelector('[data-code-badge]');

  var tilesGrid = document.querySelector('[data-tiles-grid]');
  var chunksCounter = document.querySelector('[data-chunks-counter]');
  var barrierBar = document.querySelector('[data-barrier-bar]');
  var barrierText = document.querySelector('[data-barrier-text]');

  var livePill = document.querySelector('[data-live-pill]');
  var liveStatus = document.querySelector('[data-live-status]');
  var termStream = document.querySelector('[data-term-stream]');

  var metricFail = document.querySelector('[data-metric-fail]');
  var metricChunks = document.querySelector('[data-metric-chunks]');
  var metricChunksSub = document.querySelector('[data-metric-chunks-sub]');
  var metricLatency = document.querySelector('[data-metric-latency]');
  var metricEff = document.querySelector('[data-metric-eff]');
  var metricEffSub = document.querySelector('[data-metric-eff-sub]');

  var outcomeCallout = document.querySelector('[data-outcome-callout]');
  var outcomeLabel = document.querySelector('[data-outcome-label]');
  var outcomeTitle = document.querySelector('[data-outcome-title]');
  var outcomeSub = document.querySelector('[data-outcome-sub]');

  var btnPlay = document.querySelector('[data-btn-play]');
  var btnStep = document.querySelector('[data-btn-step]');
  var btnCrash = document.querySelector('[data-btn-crash]');
  var btnResume = document.querySelector('[data-btn-resume]');
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

  function buildTiles() {
    if (!tilesGrid) return;
    tilesGrid.innerHTML = '';
    for (var i = 1; i <= totalChunks; i++) {
      var tile = document.createElement('div');
      tile.className = 'chunk-tile';
      tile.setAttribute('data-tile', i);
      tile.innerHTML = `<span class="c-id">#${i}</span><span class="c-status">Pending</span>`;
      tilesGrid.appendChild(tile);
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
      if (codeTitle) codeTitle.textContent = 'celery_transcode.py';
      if (codeKicker) codeKicker.textContent = 'CELERY CHORDS (UNPROTECTED)';
      if (codeBadge) { codeBadge.textContent = 'VOLATILE BARRIER'; codeBadge.style.background = 'rgba(239,68,68,0.2)'; codeBadge.style.color = '#fca5a5'; }
    } else {
      if (codeTitle) codeTitle.textContent = currentSdk === 'steps' ? 'transcode_step.py' : 'transcode_fsm.py';
      if (codeKicker) codeKicker.textContent = currentSdk === 'steps' ? 'DURABLE STEP API' : 'FERRICSTORE STATE HANDLERS';
      if (codeBadge) { codeBadge.textContent = 'DURABLE PARALLEL'; codeBadge.style.background = 'rgba(16,185,129,0.15)'; codeBadge.style.color = '#34d399'; }
    }
  }

  function resetSimulation() {
    clearAllTimeouts();
    buildTiles();
    if (chunksCounter) chunksCounter.textContent = '0 / 16 Finished';
    if (barrierBar) barrierBar.className = 'barrier-row ' + (currentMode === 'before' ? 'bad' : 'good');
    if (barrierText) barrierText.textContent = currentMode === 'before' ? 'BARRIER: WAITING FOR 16 CHUNKS' : 'BARRIER: READY TO GATHER & STITCH';

    if (livePill) livePill.className = 'live-pill';
    if (liveStatus) liveStatus.textContent = 'READY';

    if (currentMode === 'before') {
      if (metricChunks) metricChunks.textContent = '16 Chunks (100%)';
      if (metricChunksSub) metricChunksSub.textContent = 'Celery chord state lost in memory';
      if (metricLatency) metricLatency.textContent = '+14.2 sec (Hang)';
      if (metricEff) metricEff.textContent = '0% (15 Wasted Chunks)';
      if (metricEffSub) metricEffSub.textContent = 'Full job restarts from Chunk #1';

      if (outcomeCallout) outcomeCallout.className = 'outcome-callout bad';
      if (outcomeLabel) outcomeLabel.textContent = 'CELERY CRASH IMPACT';
      if (outcomeTitle) outcomeTitle.textContent = '16x FULL JOB RE-RUN (15 WASTED CHUNKS)';
      if (outcomeSub) outcomeSub.textContent = 'Worker #9 crash wiped in-memory chord state. Queue restarted the entire transcode.';
    } else {
      if (metricChunks) metricChunks.textContent = '1 Chunk Only';
      if (metricChunksSub) metricChunksSub.textContent = '15 Chunks loaded from disk cache';
      if (metricLatency) metricLatency.textContent = 'After child completion';
      if (metricEff) metricEff.textContent = 'Committed chunks reused';
      if (metricEffSub) metricEffSub.textContent = '0 redundant GPU cycles';

      if (outcomeCallout) outcomeCallout.className = 'outcome-callout good';
      if (outcomeLabel) outcomeLabel.textContent = 'FERRICSTORE RECOVERY IMPACT';
      if (outcomeTitle) outcomeTitle.textContent = 'EXACT-1 CHUNK RESUME (0 WASTED COMPUTE)';
      if (outcomeSub) outcomeSub.textContent = 'Completed child results stayed durable. Only unfinished Chunk #9 was reclaimed.';
    }
  }

  function runFanOut(onDone) {
    resetSimulation();
    if (liveStatus) liveStatus.textContent = 'FANNING OUT 16 WORKERS';
    log('info', '🚀 Dispatching 16-chunk parallel transcode job (4K Ultra-HD)...');

    var tiles = tilesGrid ? tilesGrid.querySelectorAll('.chunk-tile') : [];
    tiles.forEach(function (t) {
      t.className = 'chunk-tile running';
      t.querySelector('.c-status').textContent = 'Rendering...';
    });

    var completed = 0;
    for (var i = 1; i <= totalChunks; i++) {
      (function (idx) {
        var delay = 250 + Math.random() * 500;
        animTimeouts.push(setTimeout(function () {
          if (idx !== 9) {
            if (tiles[idx - 1]) {
              tiles[idx - 1].className = 'chunk-tile done';
              tiles[idx - 1].querySelector('.c-status').textContent = currentMode === 'before' ? '✓ RAM' : '✓ Raft Disk';
            }
            completed++;
            if (chunksCounter) chunksCounter.textContent = completed + ' / 16 Finished';
          } else {
            if (tiles[idx - 1]) tiles[idx - 1].querySelector('.c-status').textContent = 'In-Flight 88%';
          }
        }, delay));
      })(i);
    }

    animTimeouts.push(setTimeout(function () {
      log('success', '✓ 15 of 16 Chunks finished rendering. Chunk #9 still rendering in-flight.');
      if (onDone) onDone();
    }, 900));
  }

  function injectCrash(onDone) {
    if (livePill) livePill.className = 'live-pill is-crash';
    if (liveStatus) liveStatus.textContent = 'WORKER #9 CRASH';
    log('danger', '💥 [SIGKILL] Worker #9 died unexpectedly (OOM / Node Eviction) at 88% progress!');

    var tile9 = tilesGrid ? tilesGrid.querySelector('[data-tile="9"]') : null;
    if (tile9) {
      tile9.className = 'chunk-tile crashed';
      tile9.querySelector('.c-status').textContent = '💥 KILLED';
    }

    if (currentMode === 'before') {
      log('danger', '🚨 [CELERY BROKEN] Chord barrier lost. 15 in-memory chunks cannot be gathered!');
      if (barrierText) barrierText.textContent = '💥 CHORD BARRIER BROKEN (ALL CHUNKS LOST)';
      if (barrierBar) barrierBar.className = 'barrier-row bad';
    } else {
      log('cyan', '🛡️ [FERRICSTORE] 15 chunks secured in Raft quorum. Ready to resume only #9.');
      if (barrierText) barrierText.textContent = '⚡ 15 CHUNKS SECURED ON DISK. AWAITING #9';
      if (barrierBar) barrierBar.className = 'barrier-row good';
    }

    if (onDone) animTimeouts.push(setTimeout(onDone, 900));
  }

  function recoverAndGather(onDone) {
    if (livePill) livePill.className = 'live-pill';
    if (liveStatus) liveStatus.textContent = 'RECOVERING &amp; GATHERING';

    var tiles = tilesGrid ? tilesGrid.querySelectorAll('.chunk-tile') : [];

    if (currentMode === 'before') {
      log('danger', '❌ [CELERY RECOVERY] All 16 chunks re-dispatched from scratch! 15 finished chunks wasted.');
      tiles.forEach(function (t) {
        t.className = 'chunk-tile running';
        t.querySelector('.c-status').textContent = '🚨 Re-rendering!';
      });
      animTimeouts.push(setTimeout(function () {
        tiles.forEach(function (t) {
          t.className = 'chunk-tile done';
          t.querySelector('.c-status').textContent = '✓ Done';
        });
        if (chunksCounter) chunksCounter.textContent = '16 / 16 Re-done';
        if (barrierText) barrierText.textContent = '⚠️ FULL RE-RUN FINISHED (WASTED 15 CHUNKS)';
        if (liveStatus) liveStatus.textContent = 'COMPLETED WITH 15x WASTE';
        if (onDone) onDone();
      }, 900));
    } else {
      log('cyan', '⚡ [FERRICSTORE RECOVERY] Completed child results loaded from durable state.');
      tiles.forEach(function (t, idx) {
        if (idx !== 8) {
          t.className = 'chunk-tile cached';
          t.querySelector('.c-status').textContent = '✓ Durable result';
        }
      });

      var tile9 = tilesGrid ? tilesGrid.querySelector('[data-tile="9"]') : null;
      if (tile9) {
        tile9.className = 'chunk-tile running';
        tile9.querySelector('.c-status').textContent = 'Rendering #9...';
      }

      animTimeouts.push(setTimeout(function () {
        if (tile9) {
          tile9.className = 'chunk-tile done';
          tile9.querySelector('.c-status').textContent = '✓ Raft Disk';
        }
        if (chunksCounter) chunksCounter.textContent = '16 / 16 Gathered';
        if (barrierText) barrierText.textContent = '🎉 ALL 16 CHUNKS GATHERED ➔ 4K VIDEO STITCHED!';
        if (barrierBar) barrierBar.className = 'barrier-row good';
        if (liveStatus) liveStatus.textContent = 'JOB COMPLETE (0 WASTED COMPUTE)';
        log('success', '🎉 [SUCCESS] Parent joined after all child flows completed; previously completed results were preserved.');
        if (onDone) onDone();
      }, 700));
    }
  }

  function playSimulation() {
    clearLogs();
    log('info', '🎬 Starting live automated parallel transcode simulation in mode: "' + currentMode + '"...');
    runFanOut(function () {
      animTimeouts.push(setTimeout(function () {
        injectCrash(function () {
          animTimeouts.push(setTimeout(function () {
            recoverAndGather();
          }, 800));
        });
      }, 700));
    });
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
      resetSimulation();
      playSimulation();
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
  if (btnPlay) btnPlay.addEventListener('click', playSimulation);
  if (btnStep) btnStep.addEventListener('click', function () { clearLogs(); runFanOut(); });
  if (btnCrash) btnCrash.addEventListener('click', function () { injectCrash(); });
  if (btnResume) btnResume.addEventListener('click', function () { recoverAndGather(); });
  if (btnReset) btnReset.addEventListener('click', function () { resetSimulation(); clearLogs(); log('info', 'Reset. Click "⚡ Play Simulation" to begin.'); });

  // Init
  buildTiles();
  updateActiveCodeBlock();
  resetSimulation();
  playSimulation();
})();
