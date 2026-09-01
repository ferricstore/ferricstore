(function () {
  "use strict";

  var WORKFLOW_RATE = 54060;
  var STREAM_MAX_RATE = 687797;
  var PUBSUB_FANOUT = 8;
  var pubsubModes = {
    legacy: { publishes: 184921 },
    batch: { publishes: 293794 }
  };

  var viewTabs = document.querySelectorAll("[data-view-tab]");
  var headlineCards = document.querySelectorAll("[data-select-view]");
  var panels = document.querySelectorAll("[data-view-panel]");
  var workbench = document.querySelector("#benchmark-workbench");

  function formatInteger(value) {
    return Math.round(value).toLocaleString("en-US");
  }

  function formatCompact(value) {
    if (value >= 1000000) {
      return (value / 1000000).toFixed(value >= 10000000 ? 1 : 2).replace(/\.0+$/, "") + "M";
    }
    if (value >= 1000) return Math.round(value / 1000) + "K";
    return String(Math.round(value));
  }

  function selectView(view, moveToWorkbench) {
    viewTabs.forEach(function (tab) {
      var active = tab.getAttribute("data-view-tab") === view;
      tab.classList.toggle("is-active", active);
      tab.setAttribute("aria-selected", active ? "true" : "false");
    });

    headlineCards.forEach(function (card) {
      var active = card.getAttribute("data-select-view") === view;
      card.classList.toggle("is-active", active);
      card.setAttribute("aria-selected", active ? "true" : "false");
    });

    panels.forEach(function (panel) {
      panel.hidden = panel.getAttribute("data-view-panel") !== view;
    });

    if (moveToWorkbench) {
      workbench.scrollIntoView({ behavior: "smooth", block: "start" });
    }
  }

  viewTabs.forEach(function (tab) {
    tab.addEventListener("click", function () {
      selectView(tab.getAttribute("data-view-tab"), false);
    });
  });

  headlineCards.forEach(function (card) {
    card.addEventListener("click", function () {
      selectView(card.getAttribute("data-select-view"), true);
    });
  });

  var workflowTarget = document.querySelector("[data-workflow-target]");
  var workflowTargetLabel = document.querySelector("[data-workflow-target-label]");
  var workflowUnits = document.querySelector("[data-workflow-units]");
  var workflowRatio = document.querySelector("[data-workflow-ratio]");

  function updateWorkflowCalculator() {
    var target = Number(workflowTarget.value);
    var ratio = target / WORKFLOW_RATE;
    workflowTargetLabel.textContent = formatInteger(target) + "/s";
    workflowUnits.textContent = String(Math.ceil(ratio));
    workflowRatio.textContent = ratio.toFixed(2) + "×";
  }

  workflowTarget.addEventListener("input", updateWorkflowCalculator);

  var streamOptions = document.querySelectorAll("[data-stream-option]");
  var streamHeadline = document.querySelector("[data-stream-headline]");
  var streamLabel = document.querySelector("[data-stream-label]");
  var streamRate = document.querySelector("[data-stream-rate]");
  var streamBar = document.querySelector("[data-stream-bar]");
  var streamBacklog = document.querySelector("[data-stream-backlog]");
  var streamBacklogLabel = document.querySelector("[data-stream-backlog-label]");
  var streamSeconds = document.querySelector("[data-stream-seconds]");
  var streamPerMs = document.querySelector("[data-stream-per-ms]");
  var selectedStreamRate = STREAM_MAX_RATE;

  function formatDuration(seconds) {
    if (seconds < 60) return "≈" + seconds.toFixed(1) + "s";
    return "≈" + (seconds / 60).toFixed(1) + "m";
  }

  function updateStreamCalculator() {
    var backlogMillions = Number(streamBacklog.value);
    var backlog = backlogMillions * 1000000;
    streamBacklogLabel.textContent = backlogMillions + "M entries";
    streamSeconds.textContent = formatDuration(backlog / selectedStreamRate);
    streamPerMs.textContent = formatInteger(selectedStreamRate / 1000);
  }

  function selectStreamOption(option) {
    selectedStreamRate = Number(option.getAttribute("data-rate"));
    var label = option.getAttribute("data-label");
    streamOptions.forEach(function (candidate) {
      var active = candidate === option;
      candidate.classList.toggle("is-active", active);
      candidate.setAttribute("aria-checked", active ? "true" : "false");
    });
    streamHeadline.textContent = formatInteger(selectedStreamRate) + "/s";
    streamLabel.textContent = label;
    streamRate.textContent = formatInteger(selectedStreamRate) + " entries/s";
    streamBar.style.setProperty("--stream-scale", String(selectedStreamRate / STREAM_MAX_RATE));
    updateStreamCalculator();
  }

  streamOptions.forEach(function (option) {
    option.addEventListener("click", function () { selectStreamOption(option); });
  });
  streamBacklog.addEventListener("input", updateStreamCalculator);

  var pubsubOptions = document.querySelectorAll("[data-pubsub-option]");
  var pubsubPublishes = document.querySelector("[data-pubsub-publishes]");
  var pubsubOutput = document.querySelector("[data-pubsub-output]");
  var pubsubDeliveries = document.querySelector("[data-pubsub-deliveries]");

  function selectPubsubMode(option) {
    var mode = option.getAttribute("data-pubsub-option");
    var data = pubsubModes[mode];
    var deliveries = data.publishes * PUBSUB_FANOUT;
    pubsubOptions.forEach(function (candidate) {
      var active = candidate === option;
      candidate.classList.toggle("is-active", active);
      candidate.setAttribute("aria-checked", active ? "true" : "false");
    });
    pubsubPublishes.textContent = formatInteger(data.publishes) + "/s";
    pubsubOutput.textContent = "≈" + formatCompact(deliveries) + "/s";
    pubsubDeliveries.textContent = formatCompact(deliveries) + "/s";
  }

  pubsubOptions.forEach(function (option) {
    option.addEventListener("click", function () { selectPubsubMode(option); });
  });

  updateWorkflowCalculator();
  updateStreamCalculator();
  selectPubsubMode(document.querySelector('[data-pubsub-option="batch"]'));
  selectView("workflows", false);
}());
