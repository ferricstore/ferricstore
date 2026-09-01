(function () {
  "use strict";

  var path = location.pathname;
  var gallery = path === "/" || document.querySelectorAll("article.concept").length > 1;
  var workflowDemo = /split-lab|workflow-explainer|architecture-comparison|ai-agent-workflow|travel-saga|subscription-dunning|zombie-fencing|idempotency-determinism|parallel-fanout|canary-rollback|ticket-reservation|agent-loop|beginner-queue/.test(path);
  var note = document.createElement("aside");
  note.className = "demo-accuracy-note";
  note.setAttribute("aria-label", "Product accuracy note");
  note.innerHTML = gallery
    ? '<span aria-hidden="true">i</span><p><strong>How to read these demos:</strong> Workflow handlers are at-least-once, external effects need guarded execution plus stable idempotency keys, and animated performance/cost figures are illustrative rather than benchmarks.</p><details><summary>Accuracy notes</summary><p>Current workflows use explicit states, durable transitions, leases, fencing, signals, and guarded effects. Data latency, capacity, memory, availability, and cost depend on hardware, topology, settings, and workload.</p></details>'
    : workflowDemo
      ? '<span aria-hidden="true">i</span><p><strong>Durability boundary:</strong> FerricFlow persists explicit state and fences stale writes. Durable closures replay committed results; external operations still need a stable provider/application idempotency key for the pre-commit retry window.</p><details><summary>Current SDK pattern</summary><p>Python SDK 0.13.1 supports <code>client.workflow(...)</code>, <code>@flow.state(...)</code>, <code>transition(...)</code>/<code>complete(...)</code>, durable <code>ctx.step(name=..., run=..., to_state=...)</code>, and direct <code>client.step(job, ...)</code>/<code>client.advance(job, ...)</code>. These job-based APIs infer identity, partition, state, lease, and fencing information; do not pass a worker or <code>return_job</code> flag. The worker still executes the closure. Derive external-operation keys from workflow ID + logical operation + version and reuse them across retries. Scenario timings and costs are illustrative, not benchmarks.</p></details>'
      : '<span aria-hidden="true">i</span><p><strong>Illustrative scenario:</strong> Commands and data structures are real FerricStore features. Latency, capacity, availability, memory, and price figures depend on hardware, topology, durability settings, and workload.</p><details><summary>How to read metrics</summary><p>Treat animated numbers as teaching aids, not benchmark results or infrastructure quotes. Validate production claims with a reproducible benchmark on the intended deployment.</p></details>';

  var anchor = document.querySelector("nav, .nav, .demo-nav, .topbar");
  if (anchor && anchor.parentNode) anchor.insertAdjacentElement("afterend", note);
  else document.body.insertAdjacentElement("afterbegin", note);

  function labelConceptualCode(root) {
    var blocks = [];
    if (root.closest) {
      var parentBlock = root.closest("pre");
      if (parentBlock) blocks.push(parentBlock);
    }
    if (root.querySelectorAll) blocks = blocks.concat(Array.from(root.querySelectorAll("pre")));
    Array.from(new Set(blocks)).forEach(function (block) {
      var previous = block.previousElementSibling;
      var existing = previous && previous.classList.contains("code-accuracy-label") ? previous : null;
      var conceptual = /(@client\.workflow|job\.effect|ctx\.sleep|sleep_with_signal|ctx\.parallel|ctx\.circuit_breaker|ctx\.compensate|job\.fanout)/.test(block.textContent);
      if (!conceptual) {
        if (existing) existing.remove();
        return;
      }
      if (existing) return;
      var label = document.createElement("div");
      label.className = "code-accuracy-label";
      label.textContent = "Conceptual pseudocode — current Python SDK uses WorkflowClient, @flow.state, explicit outcomes, and ctx.step for durable closures.";
      block.insertAdjacentElement("beforebegin", label);
    });
  }

  function wrapTables(root) {
    var tables = [];
    if (root.matches && root.matches("table")) tables.push(root);
    if (root.querySelectorAll) tables = tables.concat(Array.from(root.querySelectorAll("table")));
    tables.forEach(function (table) {
      if (table.parentElement && table.parentElement.classList.contains("accuracy-table-scroll")) return;
      var wrapper = document.createElement("div");
      wrapper.className = "accuracy-table-scroll";
      table.parentNode.insertBefore(wrapper, table);
      wrapper.appendChild(table);
    });
  }

  function enhance(root) {
    labelConceptualCode(root);
    wrapTables(root);
    if (root.querySelectorAll) {
      root.querySelectorAll(
        "[data-live-status], [data-live-status-text], [data-live-text], " +
        "[data-narrative-title], [data-outcome-title], [data-left-outcome], " +
        "[data-right-outcome], [data-term-status], [data-buffer-stat], " +
        "[data-dbg-log], [data-flow-log], [data-fql-log], [data-prob-log], " +
        "[data-sim-log], [data-stampede-log], [data-vote-status]"
      ).forEach(function (element) {
        if (!element.hasAttribute("aria-live")) element.setAttribute("aria-live", "polite");
      });
    }
  }

  enhance(document.body);

  function installCurrentContext() {
    if (document.querySelector("[data-current-run-label], .demo-current-context")) return;
    var host = document.querySelector(
      ".mode-switch, .mode-toggle-wrap, .paradigm-selector-grid, .scenario-selector-bar"
    );
    if (!host) return;

    var context = document.createElement("div");
    context.className = "demo-current-context";
    context.setAttribute("role", "status");
    context.setAttribute("aria-live", "polite");
    var kicker = document.createElement("span");
    kicker.textContent = host.classList.contains("scenario-selector-bar") ? "Current scenario" : "Current view";
    var value = document.createElement("strong");
    context.append(kicker, value);
    host.insertAdjacentElement("afterend", context);

    function updateCurrentContext() {
      var selected = host.querySelector(
        '[aria-selected="true"], [aria-pressed="true"], .is-selected, .is-active'
      );
      if (!selected) {
        value.textContent = "Ready";
        return;
      }
      var label = selected.querySelector(".mode-tab-title")
        || selected.querySelector(".mode-text-wrap strong")
        || selected.querySelector("strong")
        || selected.querySelector("span")
        || selected;
      value.textContent = label.textContent.replace(/\s+/g, " ").trim();
    }

    updateCurrentContext();
    new MutationObserver(updateCurrentContext).observe(host, {
      attributes: true,
      subtree: true,
      attributeFilter: ["class", "aria-selected", "aria-pressed"]
    });
  }

  installCurrentContext();
  var observer = new MutationObserver(function (mutations) {
    observer.disconnect();
    mutations.forEach(function (mutation) {
      mutation.addedNodes.forEach(function (node) { if (node.nodeType === Node.ELEMENT_NODE) enhance(node); });
    });
    observer.observe(document.body, { childList: true, subtree: true });
  });
  observer.observe(document.body, { childList: true, subtree: true });
}());
