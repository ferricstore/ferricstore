(function (global) {
  "use strict";

  function mount(target, engine) {
    if (!target || !engine) return null;

    var splitLab = document.body && document.body.dataset.design === "lab";
    var copy = splitLab ? {
      toolbarLabel: "Choose a recovery path and run it",
      modeGroupLabel: "Recovery path",
      firstModeTitle: "Restart from the beginning",
      firstModeDetail: "No saved progress after the crash",
      secondModeTitle: "Resume saved progress",
      secondModeDetail: "FerricStore keeps completed states",
      runLabel: "Run this path",
      crashLabel: "Crash original worker",
      stepLabel: "Next state",
      resetLabel: "Reset path",
      currentViewLabel: "Selected path",
      currentUnmanaged: "Restart from the beginning",
      currentDurable: "Resume saved progress",
      hint: "Choose a path, then run it. The crash happens during Summarize."
    } : {
      toolbarLabel: "Workflow controls",
      modeGroupLabel: "Comparison mode",
      firstModeTitle: "Without FerricStore",
      firstModeDetail: "In-memory restart",
      secondModeTitle: "With FerricStore",
      secondModeDetail: "Durable, fenced resume",
      runLabel: "Run workflow",
      crashLabel: "Crash worker",
      stepLabel: "Step",
      resetLabel: "Reset",
      currentViewLabel: "Current view",
      currentUnmanaged: "Without FerricStore",
      currentDurable: "With FerricStore",
      hint: "Run the workflow, then crash Worker A during summarization."
    };

    target.innerHTML = [
      '<div class="workflow-toolbar" role="toolbar" aria-label="' + copy.toolbarLabel + '">',
      '  <div class="workflow-modes" role="group" aria-label="' + copy.modeGroupLabel + '">',
      '    <button type="button" data-mode="unmanaged" aria-pressed="true"><strong>' + copy.firstModeTitle + '</strong><span>' + copy.firstModeDetail + '</span></button>',
      '    <button type="button" data-mode="durable" aria-pressed="false"><strong>' + copy.secondModeTitle + '</strong><span>' + copy.secondModeDetail + '</span></button>',
      '  </div>',
      '  <div class="workflow-actions">',
      '    <button type="button" class="action-run" data-action="run"><span aria-hidden="true">▶</span><span data-run-label>' + copy.runLabel + '</span></button>',
      '    <button type="button" class="action-crash" data-action="crash" disabled><span aria-hidden="true">⚡</span>' + copy.crashLabel + '</button>',
      '    <button type="button" data-action="step"><span aria-hidden="true">⏭</span>' + copy.stepLabel + '</button>',
      '    <button type="button" data-action="reset" disabled><span aria-hidden="true">↺</span>' + copy.resetLabel + '</button>',
      '  </div>',
      '</div>',
      '<p class="workflow-current-view"><span>' + copy.currentViewLabel + '</span><strong data-current-mode-label>' + copy.currentUnmanaged + '</strong></p>',
      '<p class="workflow-hint" data-workflow-hint>' + copy.hint + '</p>',
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
    var currentModeLabel = target.querySelector("[data-current-mode-label]");

    function plainHint(state) {
      if (!splitLab) return state.message;
      var hints = {
        idle: "Choose a path, then run it. The crash happens during Summarize.",
        plan: "Worker A is planning. Continue until the crash point.",
        replan: "The restart path is repeating Plan from the beginning.",
        search: state.mode === "durable" ? "Worker A saved the search results. Continue to Summarize." : "Worker A holds search results in memory. Continue to Summarize.",
        research: "The restart path is repeating Search after the crash.",
        summarize: "Ready: crash the original worker now to test recovery.",
        resummarize: "The restart path is summarizing the repeated results.",
        crashed: "Worker A stopped. Continue to see what the replacement can recover.",
        recovering: "Worker B resumed at Summarize with saved Plan and Search.",
        restarting: "Worker B is starting again at Plan because progress was not saved.",
        complete: state.mode === "durable"
          ? "Done. Saved Plan and Search were used; no earlier states repeated."
          : "Done. The restart path repeated Plan and Search (2 states)."
      };
      return hints[state.phase] || state.message;
    }

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
      if (currentModeLabel) {
        currentModeLabel.textContent = state.mode === "unmanaged" ? copy.currentUnmanaged : copy.currentDurable;
      }

      var waiting = state.status === "AWAITING_CRASH";
      crashButton.disabled = !state.crashReady;
      crashButton.classList.toggle("is-ready", state.crashReady);
      crashButton.setAttribute(
        "aria-label",
        state.crashReady ? (splitLab ? "Crash the original worker during Summarize" : "Crash Worker A now") : (splitLab ? "Crash the original worker when Summarize is active" : "Crash Worker, available during summarization")
      );

      if (state.status === "RUNNING") runLabel.textContent = splitLab ? "Pause path" : "Pause";
      else if (state.status === "PAUSED") runLabel.textContent = splitLab ? "Continue path" : "Resume";
      else if (state.status === "COMPLETED") runLabel.textContent = splitLab ? "Run path again" : "Run again";
      else if (waiting) runLabel.textContent = splitLab ? "Crash original worker" : "Waiting for crash";
      else runLabel.textContent = copy.runLabel;

      runButton.disabled = waiting;
      stepButton.disabled = waiting || state.status === "COMPLETED";
      resetButton.disabled = state.phase === "idle";
      hint.textContent = plainHint(state);

      if (envelope.type !== "engine:ready") announcer.textContent = plainHint(state);
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
