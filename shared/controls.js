(function (global) {
  "use strict";

  function mount(target, engine) {
    if (!target || !engine) return null;

    target.innerHTML = [
      '<div class="workflow-toolbar" role="toolbar" aria-label="Workflow controls">',
      '  <div class="workflow-modes" role="group" aria-label="Comparison mode">',
      '    <button type="button" data-mode="durable" aria-pressed="true"><strong>With FerricStore</strong><span>Durable, fenced resume</span></button>',
      '    <button type="button" data-mode="unmanaged" aria-pressed="false"><strong>Without FerricStore</strong><span>In-memory restart</span></button>',
      '  </div>',
      '  <div class="workflow-actions">',
      '    <button type="button" class="action-run" data-action="run"><span aria-hidden="true">▶</span><span data-run-label>Run workflow</span></button>',
      '    <button type="button" class="action-crash" data-action="crash" disabled><span aria-hidden="true">⚡</span>Crash worker</button>',
      '    <button type="button" data-action="step"><span aria-hidden="true">⏭</span>Step</button>',
      '    <button type="button" data-action="reset" disabled><span aria-hidden="true">↺</span>Reset</button>',
      '  </div>',
      '</div>',
      '<p class="workflow-hint" data-workflow-hint>Run the workflow, then crash Worker A during summarization.</p>',
      '<p class="sr-only" data-announcer aria-live="polite" aria-atomic="true"></p>'
    ].join("");

    var modeButtons = Array.prototype.slice.call(target.querySelectorAll("[data-mode]"));
    var runButton = target.querySelector('[data-action="run"]');
    var runLabel = target.querySelector("[data-run-label]");
    var crashButton = target.querySelector('[data-action="crash"]');
    var stepButton = target.querySelector('[data-action="step"]');
    var resetButton = target.querySelector('[data-action="reset"]');
    var hint = target.querySelector("[data-workflow-hint]");
    var announcer = target.querySelector("[data-announcer]");

    modeButtons.forEach(function (button) {
      button.addEventListener("click", function () {
        engine.setMode(button.dataset.mode);
      });
    });

    runButton.addEventListener("click", function () {
      var state = engine.snapshot();
      if (state.status === "RUNNING") engine.pause();
      else engine.start();
    });
    crashButton.addEventListener("click", function () { engine.crashWorker(); });
    stepButton.addEventListener("click", function () { engine.step(); });
    resetButton.addEventListener("click", function () { engine.reset(); });

    function update(envelope) {
      var state = envelope.state;
      modeButtons.forEach(function (button) {
        var active = button.dataset.mode === state.mode;
        button.setAttribute("aria-pressed", active ? "true" : "false");
        button.classList.toggle("is-active", active);
      });

      var waiting = state.status === "AWAITING_CRASH";
      crashButton.disabled = !state.crashReady;
      crashButton.classList.toggle("is-ready", state.crashReady);
      crashButton.setAttribute(
        "aria-label",
        state.crashReady ? "Crash Worker A now" : "Crash Worker, available during summarization"
      );

      if (state.status === "RUNNING") runLabel.textContent = "Pause";
      else if (state.status === "PAUSED") runLabel.textContent = "Resume";
      else if (state.status === "COMPLETED") runLabel.textContent = "Run again";
      else if (waiting) runLabel.textContent = "Waiting for crash";
      else runLabel.textContent = "Run workflow";

      runButton.disabled = waiting;
      stepButton.disabled = waiting || state.status === "COMPLETED";
      resetButton.disabled = state.phase === "idle";
      hint.textContent = state.message;

      if (envelope.type !== "engine:ready") announcer.textContent = state.message;
    }

    var unsubscribe = engine.subscribe(update);

    function onKeydown(event) {
      var tag = event.target && event.target.tagName;
      var interactive = event.target && event.target.closest && event.target.closest("button, a, input, textarea, select, [contenteditable]");
      if (interactive || tag === "INPUT" || tag === "TEXTAREA" || tag === "SELECT" || event.metaKey || event.ctrlKey || event.altKey) return;
      var key = event.key.toLowerCase();
      if (key === " " || key === "k") {
        event.preventDefault();
        runButton.click();
      } else if (key === "c" && !crashButton.disabled) {
        event.preventDefault();
        crashButton.click();
      } else if (key === "s" && !stepButton.disabled) {
        event.preventDefault();
        stepButton.click();
      } else if (key === "r" && !resetButton.disabled) {
        event.preventDefault();
        resetButton.click();
      }
    }

    global.addEventListener("keydown", onKeydown);

    return {
      destroy: function () {
        unsubscribe();
        global.removeEventListener("keydown", onKeydown);
      }
    };
  }

  global.FerricControls = Object.freeze({ mount: mount });
})(window);
