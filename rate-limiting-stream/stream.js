(function () {
  "use strict";

  var inboundRate = 2500;
  var outboundLimit = 100;
  var isPaused = false;

  var inboundSlider = document.querySelector("[data-inbound-slider]");
  var inboundLabel = document.querySelector("[data-inbound-label]");
  var outboundSlider = document.querySelector("[data-outbound-slider]");
  var outboundLabel = document.querySelector("[data-outbound-label]");

  var pipeIngest = document.querySelector("[data-pipe-ingest]");
  var pipeBatch = document.querySelector("[data-pipe-batch]");
  var pipeBucket = document.querySelector("[data-pipe-bucket]");
  var pipeDispatch = document.querySelector("[data-pipe-dispatch]");

  var bufferBar = document.querySelector("[data-buffer-bar]");
  var bufferStat = document.querySelector("[data-buffer-stat]");

  var valInbound = document.querySelector("[data-val-inbound]");
  var valOutbound = document.querySelector("[data-val-outbound]");
  var valDropped = document.querySelector("[data-val-dropped]");
  var valLatency = document.querySelector("[data-val-latency]");
  var currentRunValue = document.querySelector("[data-current-run-value]");
  var statusSummary = document.querySelector("[data-status-summary]");

  var pauseBtn = document.querySelector("[data-pause]");
  var resetBtn = document.querySelector("[data-reset-btn]");
  var burstBtn = document.querySelector("[data-burst-btn]");

  function update() {
    if (inboundLabel) inboundLabel.textContent = inboundRate.toLocaleString() + " events / sec";
    if (outboundLabel) outboundLabel.textContent = outboundLimit.toLocaleString() + " req / sec (configured)";

    var batchSize = Math.min(100, Math.max(10, Math.round(inboundRate / 50)));
    var bufferPercent = (Math.round(inboundRate / 100) / 5000) * 100;

    if (pipeIngest) pipeIngest.textContent = inboundRate.toLocaleString() + " Webhooks/s";
    if (pipeBatch) pipeBatch.textContent = batchSize + " items / group (example)";
    if (pipeBucket) pipeBucket.textContent = "Up to " + outboundLimit + " requests / sec";
    if (pipeDispatch) pipeDispatch.textContent = outboundLimit + " requests/s (configured)";

    if (bufferBar) {
      bufferBar.style.width = bufferPercent + "%";
    }
    if (bufferStat) {
      bufferStat.textContent = "Example buffer: " + Math.round((inboundRate / 100)) + " / 5,000 · waiting for API limit " + outboundLimit + "/s";
    }

    if (valInbound) valInbound.textContent = isPaused ? "0 / s (Paused)" : inboundRate.toLocaleString() + " / s";
    if (valOutbound) valOutbound.textContent = isPaused ? "0 / s" : outboundLimit.toLocaleString() + " / s";
    if (valDropped) valDropped.textContent = "0 in this run";
    if (valLatency) valLatency.textContent = "Workload-dependent";
    if (currentRunValue) {
      currentRunValue.textContent = "Shopify webhook surge → OpenAI categorization · "
        + inboundRate.toLocaleString() + " configured inbound → "
        + outboundLimit.toLocaleString() + " configured downstream"
        + (isPaused ? " · Paused" : "");
    }

    if (pauseBtn) pauseBtn.textContent = isPaused ? "▶ Resume example" : "⏸ Pause example";
    if (statusSummary) {
      statusSummary.innerHTML = isPaused
        ? "⏸ <strong>Paused:</strong> incoming work is held in the example buffer; no requests leave for the API."
        : "🛡️ <strong>With FerricStore:</strong> incoming webhooks wait in saved buffer space while the API accepts only the configured rate.";
    }
  }

  if (inboundSlider) {
    inboundSlider.addEventListener("input", function () {
      inboundRate = parseInt(inboundSlider.value, 10);
      update();
    });
  }

  if (outboundSlider) {
    outboundSlider.addEventListener("input", function () {
      outboundLimit = parseInt(outboundSlider.value, 10);
      update();
    });
  }

  if (burstBtn) {
    burstBtn.addEventListener("click", function () {
      inboundRate = 5000;
      if (inboundSlider) inboundSlider.value = "5000";
      update();
    });
  }

  if (pauseBtn) {
    pauseBtn.addEventListener("click", function () {
      isPaused = !isPaused;
      update();
    });
  }

  if (resetBtn) {
    resetBtn.addEventListener("click", function () {
      inboundRate = 2500;
      outboundLimit = 100;
      if (inboundSlider) inboundSlider.value = "2500";
      if (outboundSlider) outboundSlider.value = "100";
      isPaused = false;
      update();
    });
  }

  update();
})();
