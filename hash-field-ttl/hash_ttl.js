(function () {
  'use strict';

  var currentMode = 'before';
  var timerInterval = null;
  var animTimeouts = [];

  var state = {
    auth_2fa: { value: '"849201"', ttlRemaining: 5.0, active: true },
    cart_hold: { value: '"sku_iphone16"', ttlRemaining: 10.0, active: true }
  };

  // DOM Elements
  var modeButtons = document.querySelectorAll('[data-mode-btn]');
  var codeBlocks = document.querySelectorAll('[data-code]');
  var codeTitle = document.querySelector('[data-code-title]');
  var codeKicker = document.querySelector('[data-code-kicker]');

  var metricKeys = document.querySelector('[data-metric-keys]');
  var metricKeysSub = document.querySelector('[data-metric-keys-sub]');
  var metricRam = document.querySelector('[data-metric-ram]');
  var metricRamSub = document.querySelector('[data-metric-ram-sub]');
  var metricAtomic = document.querySelector('[data-metric-atomic]');
  var metricPrecision = document.querySelector('[data-metric-precision]');

  var row2fa = document.querySelector('[data-row-2fa]');
  var val2fa = document.querySelector('[data-val-2fa]');
  var ttl2fa = document.querySelector('[data-ttl-2fa]');
  var status2fa = document.querySelector('[data-status-2fa]');

  var rowCart = document.querySelector('[data-row-cart]');
  var valCart = document.querySelector('[data-val-cart]');
  var ttlCart = document.querySelector('[data-ttl-cart]');
  var statusCart = document.querySelector('[data-status-cart]');

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

  var btnSet2fa = document.querySelector('[data-btn-set-2fa]');
  var btnSetCart = document.querySelector('[data-btn-set-cart]');
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
  function clearAll() {
    if (timerInterval) clearInterval(timerInterval);
    timerInterval = null;
    animTimeouts.forEach(function (t) { clearTimeout(t); });
    animTimeouts = [];
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

    if (currentMode === 'before') {
      if (codeTitle) codeTitle.textContent = 'legacy_key_explosion.py';
      if (codeKicker) codeKicker.textContent = 'REDIS PRE-7.4 (NO SUB-KEY TTL)';

      if (metricKeys) metricKeys.textContent = '10,000,000 Keys (Bloat)';
      if (metricKeysSub) metricKeysSub.textContent = '10 separate keys per user';
      if (metricRam) metricRam.textContent = '3.2 GB RAM (Wasted)';
      if (metricRamSub) metricRamSub.textContent = 'Dict header & alloc overhead';
      if (metricAtomic) metricAtomic.textContent = '❌ Non-Atomic Multi-Key';
      if (metricPrecision) metricPrecision.textContent = 'Cron Sweep Lag';

      if (outcomeCallout) outcomeCallout.className = 'outcome-callout bad';
      if (outcomeLabel) outcomeLabel.textContent = 'KEY EXPLOSION OVERHEAD';
      if (outcomeTitle) outcomeTitle.textContent = '3.2 GB WASTED ON SEPARATE REDIS KEYS';
      if (outcomeSub) outcomeSub.textContent = 'Because legacy Redis only expired top-level keys, transient fields exploded into 10 million distinct keys, wasting 90% of RAM on dict headers.';

    } else {
      if (codeTitle) codeTitle.textContent = 'user_session_ttl.py';
      if (codeKicker) codeKicker.textContent = 'FERRICSTORE HEXPIRE API';

      if (metricKeys) metricKeys.textContent = '1,000,000 Keys (90% Saved)';
      if (metricKeysSub) metricKeysSub.textContent = '1 compound key per user object';
      if (metricRam) metricRam.textContent = '320 MB (10x Leaner)';
      if (metricRamSub) metricRamSub.textContent = 'Zero redundant dict header overhead';
      if (metricAtomic) metricAtomic.textContent = 'Single-shard atomic';
      if (metricPrecision) metricPrecision.textContent = 'Millisecond TTL command';

      if (outcomeCallout) outcomeCallout.className = 'outcome-callout good';
      if (outcomeLabel) outcomeLabel.textContent = 'ADVANCED KV BENEFIT';
      if (outcomeTitle) outcomeTitle.textContent = '90% RAM SAVINGS &amp; ZERO KEY EXPLOSION';
      if (outcomeSub) outcomeSub.textContent = 'By supporting HEXPIRE natively on compound hashes, FerricStore eliminates millions of redundant top-level keys and enables atomic single-shard updates.';
    }
  }

  function startCountdown() {
    if (timerInterval) clearInterval(timerInterval);

    timerInterval = setInterval(function () {
      // 2FA Timer
      if (state.auth_2fa.active) {
        state.auth_2fa.ttlRemaining = Math.max(0, state.auth_2fa.ttlRemaining - 0.1);
        if (ttl2fa) ttl2fa.textContent = '⏳ ' + state.auth_2fa.ttlRemaining.toFixed(1) + 's';

        if (state.auth_2fa.ttlRemaining <= 0) {
          state.auth_2fa.active = false;
          if (row2fa) row2fa.className = 'f-row expired-row';
          if (ttl2fa) { ttl2fa.className = 'ttl-badge dead'; ttl2fa.textContent = '💀 EXPIRED (0.0s)'; }
          if (status2fa) { status2fa.className = 'status-chip dead'; status2fa.textContent = 'PURGED'; }
          if (val2fa) val2fa.textContent = '(nil / purged)';

          log('danger', '⏳ [HEXPIRE SWEEP] user:42 -> auth_2fa expired (5.0s elapsed). Field purged from ETS.');
          log('success', '✓ [PARENT SAFE] user:42 fields "name" and "role" remain unchanged while the selected field expires.');
        }
      }

      // Cart Timer
      if (state.cart_hold.active) {
        state.cart_hold.ttlRemaining = Math.max(0, state.cart_hold.ttlRemaining - 0.1);
        if (ttlCart) ttlCart.textContent = '⏳ ' + state.cart_hold.ttlRemaining.toFixed(1) + 's';

        if (state.cart_hold.ttlRemaining <= 0) {
          state.cart_hold.active = false;
          if (rowCart) rowCart.className = 'f-row expired-row';
          if (ttlCart) { ttlCart.className = 'ttl-badge dead'; ttlCart.textContent = '💀 EXPIRED (0.0s)'; }
          if (statusCart) { statusCart.className = 'status-chip dead'; statusCart.textContent = 'PURGED'; }
          if (valCart) valCart.textContent = '(nil / purged)';

          log('danger', '⏳ [HEXPIRE SWEEP] user:42 -> cart_hold expired (10.0s elapsed). Item released to inventory.');
        }
      }
    }, 100);
  }

  // --- ACTION 1: Set 2FA with 5s TTL ---
  function set2fa() {
    clearAll();
    state.auth_2fa = { value: '"849201"', ttlRemaining: 5.0, active: true };

    if (row2fa) row2fa.className = 'f-row expiring';
    if (val2fa) val2fa.textContent = '"849201"';
    if (ttl2fa) { ttl2fa.className = 'ttl-badge exp'; ttl2fa.textContent = '⏳ 5.0s'; }
    if (status2fa) { status2fa.className = 'status-chip warn'; status2fa.textContent = 'COUNTDOWN'; }

    log('info', 'HSET user:42 auth_2fa "849201"...');
    log('cyan', 'HEXPIRE user:42 5 FIELDS 1 auth_2fa ➔ Set sub-key millisecond TTL.');

    highlightCodeLine(7);
    startCountdown();
  }

  // --- ACTION 2: Add Cart with 10s TTL ---
  function setCart() {
    clearAll();
    state.cart_hold = { value: '"sku_iphone16"', ttlRemaining: 10.0, active: true };

    if (rowCart) rowCart.className = 'f-row expiring';
    if (valCart) valCart.textContent = '"sku_iphone16"';
    if (ttlCart) { ttlCart.className = 'ttl-badge exp'; ttlCart.textContent = '⏳ 10.0s'; }
    if (statusCart) { statusCart.className = 'status-chip warn'; statusCart.textContent = 'COUNTDOWN'; }

    log('info', 'HSET user:42 cart_hold "sku_iphone16"...');
    log('cyan', 'HEXPIRE user:42 10 FIELDS 1 cart_hold ➔ Set 10-second flash sale cart hold.');

    highlightCodeLine(11);
    startCountdown();
  }

  // --- Mode Switch Buttons ---
  modeButtons.forEach(function (btn) {
    btn.addEventListener('click', function () {
      modeButtons.forEach(function (b) { b.classList.remove('is-selected'); b.setAttribute('aria-selected', 'false'); });
      btn.classList.add('is-selected');
      btn.setAttribute('aria-selected', 'true');
      currentMode = btn.getAttribute('data-mode-btn') || 'after';
      document.body.setAttribute('data-mode', currentMode);
      updateModeUI();
      set2fa();
    });
  });

  // --- Playback Buttons ---
  if (btnSet2fa) btnSet2fa.addEventListener('click', set2fa);
  if (btnSetCart) btnSetCart.addEventListener('click', setCart);
  if (btnReset) btnReset.addEventListener('click', function () {
    clearAll();
    clearLogs();
    set2fa();
    setCart();
  });

  // Init
  updateModeUI();
  set2fa();
  setCart();
})();
