(function () {
  "use strict";

  var MODES = {
    worker: {
      kicker: "REAL APPLICATION WORK",
      title: "54,060 worker-driven workflows/s",
      summary: "Workers claim a named state, run normal application code, and report a durable transition or completion. The measured Azure run used one tiny handler, so it measures orchestration capacity—not the speed of your business logic.",
      answerTitle: "What does 54K mean?",
      answer: "One server started, coordinated, claimed, and durably completed roughly 54,000 one-state synthetic workflows each second while producers and workers ran together.",
      throughput: "54,060/s",
      throughputNote: "1M live Azure run",
      steps: "1",
      stepsNote: "one worker handler",
      latency: "≈18.5 s",
      latencyNote: "1M workflows end-to-end",
      command: "create + claim + finish",
      commandNote: "application code between boundaries",
      boundaryTitle: "Use this model when steps perform real work.",
      boundary: "The handler may call other services and may run again after a crash, so external effects still need idempotency.",
      ariaLabel: "Worker-driven workflow sequence",
      stepsVisual: [
        ["01", "START", "producer creates Flow", ""],
        ["02", "CLAIM", "worker receives lease", ""],
        ["03", "RUN CODE", "API, model, payment…", "app-step"],
        ["04", "COMMIT", "transition or complete", ""]
      ]
    },
    fused: {
      kicker: "PRE-DECLARED STATE CHAIN",
      title: "104,696 fused three-step workflows/s",
      summary: "FerricStore records the created state, intermediate state history, and completion for an already-known chain in one durable command. No Worker is dispatched between the three states.",
      answerTitle: "What does 104K mean?",
      answer: "The server persisted about 104,000 complete three-step state histories each second in this 100K-workflow local run. It did not execute 104,000 payments, API calls, or model requests.",
      throughput: "104,696/s",
      throughputNote: "100K local fused run",
      steps: "3",
      stepsNote: "314,088 logical steps/s",
      latency: "27.0 ms p99",
      latencyNote: "p50 18.1 ms",
      command: "one fused chain",
      commandNote: "no code between states",
      boundaryTitle: "Use this only when the whole state sequence is already known.",
      boundary: "If a payment, model call, approval, or other handler must run between states, use the worker-driven path instead.",
      ariaLabel: "Fused deterministic workflow sequence",
      stepsVisual: [
        ["01", "ONE COMMAND", "run_steps_many", "fused-command"],
        ["02", "STEP 1", "record first state", ""],
        ["03", "STEP 2", "record transition", ""],
        ["04", "STEP 3", "record completion", ""]
      ]
    }
  };

  var buttons = Array.prototype.slice.call(document.querySelectorAll("[data-mode-button]"));
  var visual = document.querySelector("[data-mode-visual]");

  function setText(selector, value) {
    var node = document.querySelector(selector);
    if (node) node.textContent = value;
  }

  function renderVisual(mode) {
    visual.replaceChildren();
    mode.stepsVisual.forEach(function (item, index) {
      if (index > 0) {
        var arrow = document.createElement("i");
        arrow.setAttribute("aria-hidden", "true");
        arrow.textContent = "→";
        visual.appendChild(arrow);
      }

      var step = document.createElement("div");
      step.className = "mode-step" + (item[3] ? " " + item[3] : "");

      var number = document.createElement("small");
      number.textContent = item[0];
      var title = document.createElement("strong");
      title.textContent = item[1];
      var detail = document.createElement("span");
      detail.textContent = item[2];

      step.append(number, title, detail);
      visual.appendChild(step);
    });
    visual.setAttribute("aria-label", mode.ariaLabel);
  }

  function selectMode(name) {
    var mode = MODES[name];
    if (!mode) return;

    buttons.forEach(function (button) {
      button.setAttribute("aria-selected", String(button.dataset.modeButton === name));
    });

    setText("[data-mode-kicker]", mode.kicker);
    setText("[data-mode-title]", mode.title);
    setText("[data-mode-summary]", mode.summary);
    setText("[data-mode-answer-title]", mode.answerTitle);
    setText("[data-mode-answer]", mode.answer);
    setText("[data-mode-throughput]", mode.throughput);
    setText("[data-mode-throughput-note]", mode.throughputNote);
    setText("[data-mode-steps]", mode.steps);
    setText("[data-mode-steps-note]", mode.stepsNote);
    setText("[data-mode-latency]", mode.latency);
    setText("[data-mode-latency-note]", mode.latencyNote);
    setText("[data-mode-command]", mode.command);
    setText("[data-mode-command-note]", mode.commandNote);
    setText("[data-mode-boundary-title]", mode.boundaryTitle);
    setText("[data-mode-boundary]", mode.boundary);
    renderVisual(mode);
  }

  buttons.forEach(function (button) {
    button.addEventListener("click", function () {
      selectMode(button.dataset.modeButton);
    });
  });

  selectMode("worker");
}());
