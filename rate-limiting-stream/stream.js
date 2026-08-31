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

  var pauseBtn = document.querySelector("[data-pause]");
  var resetBtn = document.querySelector("[data-reset-btn]");
  var burstBtn = document.querySelector("[data-burst-btn]");

  function update() {
    if (inboundLabel) inboundLabel.textContent = inboundRate.toLocaleString() + " events / sec";
    if (outboundLabel) outboundLabel.textContent = outboundLimit.toLocaleString() + " req / sec (Max)";

    var batchSize = Math.min(100, Math.max(10, Math.round(inboundRate / 50)));
    var bufferPercent = Math.min(100, Math.max(5, Math.round((inboundRate / 5000) * 85)));

    if (pipeIngest) pipeIngest.textContent = inboundRate.toLocaleString() + " Webhooks/s";
    if (pipeBatch) pipeBatch.textContent = batchSize + " Items / Batch";
    if (pipeBucket) pipeBucket.textContent = outboundLimit + " Tokens / Sec";
    if (pipeDispatch) pipeDispatch.textContent = outboundLimit + " Req/s Clean";

    if (bufferBar) {
      bufferBar.style.width = isPaused ? "0%" : bufferPercent + "%";
    }
    if (bufferStat) {
      bufferStat.textContent = "Queue Buffer: " + Math.round((inboundRate / 100)) + " / 5,000 · Durable buffer";
    }

    if (valInbound) valInbound.textContent = isPaused ? "0 / s (Paused)" : inboundRate.toLocaleString() + " / s";
    if (valOutbound) valOutbound.textContent = isPaused ? "0 / s" : outboundLimit.toLocaleString() + " / s";
    if (valDropped) valDropped.textContent = "0 in this run";
    if (valLatency) valLatency.textContent = "Workload-dependent";

    if (pauseBtn) pauseBtn.textContent = isPaused ? "▶ Resume Stream" : "⏸ Pause Stream";
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
