(function () {
  'use strict';

  var currentMode = 'before';
  var timerInterval = null;
  var animTimeouts = [];

  var state = {
    auth_2fa: { value: '"849201"', ttlRemaining: 5.0, active: true },
    cart_hold: { value: null, ttlRemaining: 0, active: false }
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
  var metricAtomicSub = document.querySelector('[data-metric-atomic-sub]');
  var metricPrecision = document.querySelector('[data-metric-precision]');
  var metricPrecisionSub = document.querySelector('[data-metric-precision-sub]');

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
    var layoutSummary = document.querySelector('[data-hash-summary]');
    if (layoutSummary) layoutSummary.textContent = currentMode === 'before' ? 'Temporary values live in separate keys' : 'One hash; selected fields expire independently';
    codeBlocks.forEach(function (block) {
      block.hidden = block.getAttribute('data-code') !== currentMode;
    });

    if (currentMode === 'before') {
      if (codeTitle) codeTitle.textContent = 'separate_expiry_keys.py';
      if (codeKicker) codeKicker.textContent = 'SEPARATE-KEY EXPIRY EXAMPLE';

      if (metricKeys) metricKeys.textContent = '10,000,000 keys';
      if (metricKeysSub) metricKeysSub.textContent = 'Illustrative 1M-user model · 10 per user';
      if (metricRam) metricRam.textContent = '3.2 GB (illustrative)';
      if (metricRamSub) metricRamSub.textContent = 'Modeled dictionary metadata';
      if (metricAtomic) metricAtomic.textContent = 'Several keys to coordinate';
      if (metricAtomicSub) metricAtomicSub.textContent = 'The values live outside one record';
      if (metricPrecision) metricPrecision.textContent = 'Separate expiry commands';
      if (metricPrecisionSub) metricPrecisionSub.textContent = 'TTL means time to live';

      if (outcomeCallout) outcomeCallout.className = 'outcome-callout bad';
      if (outcomeLabel) outcomeLabel.textContent = 'SEPARATE-KEY EXAMPLE';
      if (outcomeTitle) outcomeTitle.textContent = 'TEMPORARY VALUES USE SEPARATE KEYS';
      if (outcomeSub) outcomeSub.textContent = 'This illustrative 1M-user model uses 10,000,000 top-level keys and 3.2 GB of modeled metadata.';

    } else {
      if (codeTitle) codeTitle.textContent = 'user_session_ttl.py';
      if (codeKicker) codeKicker.textContent = 'FERRICSTORE HEXPIRE API';

      if (metricKeys) metricKeys.textContent = '1,000,000 keys';
      if (metricKeysSub) metricKeysSub.textContent = 'Illustrative 1M-user model · 1 per user';
      if (metricRam) metricRam.textContent = '320 MB (illustrative)';
      if (metricRamSub) metricRamSub.textContent = 'Modeled compact field tables';
      if (metricAtomic) metricAtomic.textContent = 'One record to update';
      if (metricAtomicSub) metricAtomicSub.textContent = 'Single-shard path for this hash';
      if (metricPrecision) metricPrecision.textContent = 'Field-level TTL command';
      if (metricPrecisionSub) metricPrecisionSub.textContent = 'Selected fields can expire independently';

      if (outcomeCallout) outcomeCallout.className = 'outcome-callout good';
      if (outcomeLabel) outcomeLabel.textContent = 'WITH FERRICSTORE';
      if (outcomeTitle) outcomeTitle.textContent = 'ONE RECORD; SELECTED FIELDS EXPIRE';
      if (outcomeSub) outcomeSub.textContent = 'This illustrative model keeps the permanent profile fields in user:42 while HEXPIRE removes only the field whose timer ends.';
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

          log('danger', currentMode === 'before'
            ? '⏳ [KEY EXPIRY] user:42:auth_2fa expired after 5.0s. The separate top-level key was removed.'
            : '⏳ [HEXPIRE SWEEP] user:42 -> auth_2fa expired after 5.0s. The selected field was purged.');
          log('success', currentMode === 'before'
            ? '✓ [PROFILE UNCHANGED] The separate user:42:profile key remains, at the cost of another top-level key.'
            : '✓ [PARENT SAFE] user:42 fields "name" and "role" remain unchanged while the selected field expires.');
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

          log('danger', currentMode === 'before'
            ? '⏳ [KEY EXPIRY] user:42:cart_hold expired after 10.0s. The separate key was removed.'
            : '⏳ [HEXPIRE SWEEP] user:42 -> cart_hold expired after 10.0s. Item released to inventory.');
        }
      }
    }, 100);
  }

  // --- ACTION 1: Set 2FA with 5s TTL ---
  function set2fa(isModeReset) {
    clearAll();
    state.auth_2fa = { value: '"849201"', ttlRemaining: 5.0, active: true };
    state.cart_hold = { value: null, ttlRemaining: 0, active: false };

    if (livePill) livePill.className = 'live-pill';
    if (liveStatus) {
      liveStatus.textContent = isModeReset
        ? 'READY · CHOOSE A FIELD ACTION'
        : (currentMode === 'before' ? 'STEP 1 · SET LOGIN CODE' : 'STEP 1 · SET 2FA FIELD');
    }

    if (row2fa) row2fa.className = 'f-row expiring';
    if (val2fa) val2fa.textContent = '"849201"';
    if (ttl2fa) { ttl2fa.className = isModeReset ? 'ttl-badge wait' : 'ttl-badge exp'; ttl2fa.textContent = isModeReset ? 'Not running' : '⏳ 5.0s'; }
    if (status2fa) { status2fa.className = isModeReset ? 'status-chip wait' : 'status-chip warn'; status2fa.textContent = isModeReset ? 'READY' : 'COUNTDOWN'; }

    if (rowCart) rowCart.className = 'f-row waiting-row';
    if (valCart) valCart.textContent = '—';
    if (ttlCart) { ttlCart.className = 'ttl-badge wait'; ttlCart.textContent = 'Not set'; }
    if (statusCart) { statusCart.className = 'status-chip wait'; statusCart.textContent = 'STEP 2'; }

    if (isModeReset) {
      if (val2fa) val2fa.textContent = '—';
      if (row2fa) row2fa.className = 'f-row waiting-row';
      if (expIcon) expIcon.textContent = '💡';
      if (expTitle) expTitle.textContent = 'Mode selected; ready to run';
      if (expDesc) expDesc.textContent = 'Choose Set login code for 5s or Hold cart item for 10s to start a timer.';
      if (outcomeLabel) outcomeLabel.textContent = 'READY';
      if (outcomeTitle) outcomeTitle.textContent = 'CHOOSE A FIELD TO START';
      if (outcomeSub) outcomeSub.textContent = 'The mode changed; no expiry timer is running yet.';
      highlightCodeLine(null);
      return;
    }

    log('info', currentMode === 'before'
      ? 'SETEX user:42:auth_2fa 5 "849201" ➔ Created a separate key for the login code.'
      : 'HSET user:42 auth_2fa "849201" ➔ Stored the code in user:42.');
    if (currentMode === 'after') log('cyan', 'HEXPIRE user:42 5 FIELDS 1 auth_2fa ➔ Set sub-key millisecond TTL.');

    highlightCodeLine(7);
    startCountdown();
  }

  // --- ACTION 2: Add Cart with 10s TTL ---
  function setCart() {
    clearAll();
    state.cart_hold = { value: '"sku_iphone16"', ttlRemaining: 10.0, active: true };

    if (livePill) livePill.className = 'live-pill';
    if (liveStatus) {
      liveStatus.textContent = currentMode === 'before'
        ? 'STEP 2 · HOLD CART ITEM'
        : 'STEP 2 · CART FIELD TTL';
    }

    if (rowCart) rowCart.className = 'f-row expiring';
    if (valCart) valCart.textContent = '"sku_iphone16"';
    if (ttlCart) { ttlCart.className = 'ttl-badge exp'; ttlCart.textContent = '⏳ 10.0s'; }
    if (statusCart) { statusCart.className = 'status-chip warn'; statusCart.textContent = 'COUNTDOWN'; }

    log('info', currentMode === 'before'
      ? 'SETEX user:42:cart_hold 10 "sku_iphone16" ➔ Created a separate cart-hold key.'
      : 'HSET user:42 cart_hold "sku_iphone16" ➔ Stored the hold in user:42.');
    if (currentMode === 'after') log('cyan', 'HEXPIRE user:42 10 FIELDS 1 cart_hold ➔ Set 10-second flash sale cart hold.');

    highlightCodeLine(11);
    startCountdown();
  }

  // --- Mode Switch Buttons ---
  modeButtons.forEach(function (btn) {
    btn.addEventListener('click', function () {
      clearAll();
      clearLogs();
      modeButtons.forEach(function (b) { b.classList.remove('is-selected'); b.setAttribute('aria-selected', 'false'); });
      btn.classList.add('is-selected');
      btn.setAttribute('aria-selected', 'true');
      currentMode = btn.getAttribute('data-mode-btn') || 'after';
      document.body.setAttribute('data-mode', currentMode);
      updateModeUI();
      set2fa(true);
    });
  });

  // --- Playback Buttons ---
  if (btnSet2fa) btnSet2fa.addEventListener('click', function () { set2fa(false); });
  if (btnSetCart) btnSetCart.addEventListener('click', setCart);
  if (btnReset) btnReset.addEventListener('click', function () {
    clearAll();
    clearLogs();
    set2fa(true);
  });

  // Init
  updateModeUI();
  set2fa(true);
})();
