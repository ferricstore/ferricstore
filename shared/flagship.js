(function (global) {
  "use strict";

  var mount = document.getElementById("ai-orchestration-demo");
  if (!mount || !global.FerricWorkflow || !global.FerricControls) return;

  var design = document.body.dataset.design || "lab";
  var directions = [
    { id: "lab", path: "split-lab", short: "Split Lab", title: "Start over or carry on?", desc: "Run the same research task after a crash: restart from the beginning, or resume the progress that was saved." }
  ];
  var meta = directions.filter(function (item) { return item.id === design; })[0] || directions[0];

  document.title = meta.short + " · FerricStore AI Orchestration";

  function escapeHtml(value) {
    return String(value == null ? "" : value)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#039;");
  }

  function phaseRank(phase) {
    var ranks = {
      idle: -1,
      plan: 0,
      replan: 0,
      search: 1,
      research: 1,
      summarize: 2,
      resummarize: 2,
      crashed: 3,
      recovering: 4,
      restarting: 4,
      complete: 5
    };
    return Object.prototype.hasOwnProperty.call(ranks, phase) ? ranks[phase] : -1;
  }

  function phaseLabel(phase) {
    return {
      idle: "Ready",
      plan: "Planning",
      replan: "Repeat plan",
      search: "Search sources",
      research: "Repeat search",
      summarize: "Summarizing",
      resummarize: "Repeat summary",
      crashed: "Worker stopped",
      recovering: "Resuming saved state",
      restarting: "Starting over",
      complete: "Completed"
    }[phase] || phase;
  }

  global.FerricDemo = {
    escapeHtml: escapeHtml,
    phaseRank: phaseRank,
    phaseLabel: phaseLabel,
    directions: directions
  };

  mount.innerHTML = [
    '<div class="flagship-root">',
    '  <nav class="flagship-nav" aria-label="Demo navigation">',
    '    <a class="flagship-brand" href="../"><span class="brand-mark" aria-hidden="true">F</span><span><strong>FerricStore</strong><small>AI orchestration demo</small></span></a>',
    '    <a class="compare-link" href="../">All demos</a>',
    '  </nav>',
    '  <main class="flagship-main">',
    '    <header class="flagship-intro">',
    '      <div><span class="direction-kicker">Interactive recovery demo</span>',
    '      <h1>' + escapeHtml(meta.title) + '</h1>',
    '      <p>' + escapeHtml(meta.desc) + '</p></div>',
    '      <div id="workflow-controls"></div>',
    '    </header>',
    '    <section id="direction-view" class="direction-view" aria-label="Interactive AI workflow demonstration"></section>',
    '    <details class="demo-accuracy">',
    '      <summary>Accuracy and SDK notes</summary>',
    '      <div><p><strong>Saved-progress boundary:</strong> FerricStore persists named workflow states and blocks an old worker from writing after a replacement takes over. A compatible worker can reclaim the current state after the lease expires.</p>',
    '      <p><strong>External effects:</strong> Handlers remain at-least-once. Use <code>@ctx.effect(...)</code> to reserve, guard, and confirm a call. <code>operation_digest</code> versions the logical operation; <code>idempotency_key</code> stays stable for the external request. Neither makes the handler exactly-once.</p>',
    '      <p>This is an interaction model, not a performance benchmark. Counts and identifiers are fixed scenario data used to explain the state transition.</p></div>',
    '    </details>',
    '  </main>',
    '</div>'
  ].join("");

  var reduceMotion = global.matchMedia && global.matchMedia("(prefers-reduced-motion: reduce)").matches;
  var engine = global.FerricWorkflow.createEngine({ delay: reduceMotion ? 250 : 850, mode: "unmanaged" });
  global.FerricControls.mount(document.getElementById("workflow-controls"), engine);

  var rendererFactory = global.FerricRenderers && global.FerricRenderers[design];
  if (!rendererFactory) {
    document.getElementById("direction-view").innerHTML = '<p class="renderer-error">This direction could not be loaded.</p>';
    return;
  }
  rendererFactory(document.getElementById("direction-view"), engine);
})(window);
