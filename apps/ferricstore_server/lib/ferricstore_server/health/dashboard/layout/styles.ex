defmodule FerricstoreServer.Health.Dashboard.Layout.Styles do
  @moduledoc false

  def stylesheet do
    """
      * { margin: 0; padding: 0; box-sizing: border-box; }
      body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, monospace; background: #0d1117; color: #c9d1d9; padding: 0; min-height: 100vh; }
      [data-live-component] { display: contents; }

      /* Top bar */
      .top-bar { display: flex; align-items: center; gap: 24px; padding: 12px 20px; background: #161b22; border-bottom: 1px solid #30363d; flex-wrap: wrap; }
      .top-bar .logo { font-size: 1.1rem; font-weight: 700; color: #58a6ff; white-space: nowrap; }
      .top-bar .metric { display: flex; flex-direction: column; align-items: center; min-width: 80px; }
      .top-bar .metric .label { font-size: 0.65rem; color: #8b949e; text-transform: uppercase; letter-spacing: 0.5px; }
      .top-bar .metric .val { font-size: 1.1rem; font-weight: 700; color: #f0f6fc; }
      .top-bar .sep { width: 1px; height: 32px; background: #30363d; flex-shrink: 0; }

      /* Status badge */
      .status-dot { display: inline-block; width: 10px; height: 10px; border-radius: 50%; margin-right: 6px; vertical-align: middle; }
      .dot-green { background: #3fb950; box-shadow: 0 0 6px #3fb95066; }
      .dot-yellow { background: #d29922; box-shadow: 0 0 6px #d2992266; }
      .dot-red { background: #f85149; box-shadow: 0 0 6px #f8514966; }

      /* Memory bar in top bar */
      .mem-bar-wrap { width: 80px; height: 6px; background: #21262d; border-radius: 3px; margin-top: 2px; overflow: hidden; }
      .mem-bar-fill { height: 100%; border-radius: 3px; transition: width 0.3s; }

      /* Main content */
      .content { padding: 16px 20px 80px; max-width: 1200px; margin: 0 auto; }

      /* Section headers */
      .section-title { font-size: 0.9rem; font-weight: 600; color: #79c0ff; margin: 24px 0 12px; text-transform: uppercase; letter-spacing: 0.5px; }
      .section-title:first-child { margin-top: 8px; }
      .page-intro { background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 12px 14px; margin: 0 0 16px; color: #9da7b3; line-height: 1.45; }
      .page-intro-title { color: #f0f6fc; font-weight: 700; margin-bottom: 4px; }
      .kv-panel, .kv-inspector { background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 12px 14px; margin-bottom: 16px; }
      .kv-command-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(240px, 1fr)); gap: 12px; margin-bottom: 16px; }
      .kv-command-group { background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 12px 14px; min-width: 0; }
      .kv-command-title { color: #f0f6fc; font-weight: 700; margin-bottom: 4px; }
      .kv-command-purpose { color: #8b949e; font-size: 0.78rem; margin-bottom: 8px; }

      /* Hero hit rate */
      .cache-hero { display: flex; gap: 24px; margin-bottom: 16px; flex-wrap: wrap; }
      .hit-rate-card { background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 20px 28px; text-align: center; min-width: 180px; flex: 0 0 auto; }
      .hit-rate-num { font-size: 3rem; font-weight: 800; line-height: 1.1; }
      .hit-rate-label { font-size: 0.75rem; color: #8b949e; text-transform: uppercase; letter-spacing: 0.5px; margin-top: 2px; }
      .hit-rate-sub { font-size: 0.8rem; color: #8b949e; margin-top: 8px; }
      .hit-rate-sub span { color: #c9d1d9; font-weight: 600; }

      /* Source breakdown */
      .source-card { background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 20px 24px; flex: 1; min-width: 200px; }
      .source-row { display: flex; align-items: center; justify-content: space-between; padding: 8px 0; }
      .source-row + .source-row { border-top: 1px solid #21262d; }
      .source-name { font-size: 0.85rem; color: #c9d1d9; }
      .source-detail { font-size: 0.7rem; color: #484f58; }
      .source-pct { font-size: 1.1rem; font-weight: 700; }
      .source-bar-wrap { width: 100%; height: 4px; background: #21262d; border-radius: 2px; margin-top: 4px; }
      .source-bar-fill { height: 100%; border-radius: 2px; }

      /* Operational summary cards */
      .ops-summary-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 12px; margin-bottom: 16px; }
      .ops-summary-card { background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 14px 16px; min-width: 0; }
      .ops-summary-label { font-size: 0.68rem; color: #8b949e; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 6px; }
      .ops-summary-value { font-size: 1.45rem; font-weight: 800; color: #f0f6fc; overflow-wrap: anywhere; }
      .ops-summary-detail { color: #8b949e; font-size: 0.72rem; margin-top: 4px; overflow-wrap: anywhere; }

      /* FerricFlow */
      .flow-card-grid, .flow-detail-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: 14px; margin-bottom: 16px; }
      .flow-detail-grid { grid-template-columns: minmax(260px, 2fr) repeat(auto-fit, minmax(180px, 1fr)); }
      .flow-card { background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 16px 18px; min-width: 0; }
      .flow-card-wide { grid-column: span 2; }
      .flow-card-label { font-size: 0.68rem; color: #8b949e; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 6px; }
      .flow-card-value { font-size: 1.7rem; font-weight: 800; color: #f0f6fc; overflow-wrap: anywhere; }
      .flow-card-detail { font-size: 0.72rem; color: #8b949e; margin-top: 4px; }
      .flow-nav-row { display: flex; align-items: center; justify-content: space-between; gap: 12px; flex-wrap: wrap; margin: 4px 0 18px; }
      .flow-tabs { display: flex; gap: 10px; flex-wrap: wrap; margin: 4px 0 18px; }
      .flow-nav-row .flow-tabs { margin: 0; }
      .flow-tab-group { display: flex; align-items: center; gap: 6px; flex-wrap: wrap; padding: 5px 6px; border: 1px solid #21262d; border-radius: 8px; background: rgba(22, 27, 34, 0.55); }
      .flow-tab-group-label { color: #8b949e; font-size: 0.62rem; text-transform: uppercase; letter-spacing: 0.5px; padding: 0 3px; }
      .flow-tab-group-links { display: flex; gap: 6px; flex-wrap: wrap; }
      .flow-tab { display: inline-flex; align-items: center; border: 1px solid #30363d; background: #161b22; color: #c9d1d9; text-decoration: none; border-radius: 999px; padding: 6px 12px; font-size: 0.78rem; }
      .flow-tab:hover { background: #1c2128; }
      .flow-tab.active { color: #58a6ff; border-color: #1f6feb; background: #0f1b2d; font-weight: 700; }
      .flow-search { display: flex; align-items: center; gap: 6px; min-width: min(100%, 360px); }
      .flow-search-input { flex: 1; min-width: 0; height: 32px; background: #0d1117; color: #c9d1d9; border: 1px solid #30363d; border-radius: 6px; padding: 0 10px; font-size: 0.78rem; }
      .flow-search-input:focus { outline: none; border-color: #1f6feb; box-shadow: 0 0 0 2px rgba(31, 111, 235, 0.18); }
      .flow-search-button { height: 32px; border: 1px solid #30363d; background: #21262d; color: #f0f6fc; border-radius: 6px; padding: 0 12px; font-size: 0.78rem; cursor: pointer; }
      .flow-search-button:hover { background: #30363d; }
      .flow-danger-button { border-color: rgba(248, 81, 73, 0.55); color: #ffb3ad; }
      .flow-danger-button:hover { background: rgba(248, 81, 73, 0.14); }
      .flow-filter-panel { display: flex; align-items: center; justify-content: space-between; gap: 12px; flex-wrap: wrap; background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 10px 12px; margin-bottom: 16px; }
      .flow-filter-form { display: flex; align-items: center; gap: 8px; flex-wrap: wrap; }
      .flow-filter-form label { font-size: 0.7rem; color: #8b949e; text-transform: uppercase; letter-spacing: 0.5px; }
      .flow-filter-form select { min-width: 220px; }
      .flow-filter-form input[type="search"] { min-width: 160px; }
      .flow-query-help { display: grid; gap: 5px; background: #0d1117; border: 1px solid #30363d; border-radius: 8px; padding: 10px 12px; margin-bottom: 12px; color: #9da7b3; font-size: 0.8rem; }
      .flow-query-help-main { display: flex; gap: 8px; align-items: baseline; flex-wrap: wrap; color: #c9d1d9; }
      .flow-query-command { color: #79c0ff; font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; font-weight: 700; }
      .flow-query-help-detail { color: #8b949e; line-height: 1.4; }
      .flow-query-field { display: grid; gap: 4px; min-width: 170px; }
      .flow-query-field[hidden], .flow-query-check[hidden] { display: none !important; }
      .flow-query-field .flow-search-input { width: 100%; }
      .flow-field-help { color: #6e7681; font-size: 0.66rem; text-transform: none; letter-spacing: 0; line-height: 1.25; max-width: 220px; }
      .flow-query-check { align-self: end; height: 32px; }
      .flow-filter-range { flex: 0 0 150px; max-width: 150px; min-width: 150px; }
      .flow-filter-time { flex: 0 0 172px; max-width: 172px; min-width: 172px; }
      .flow-filter-limit { flex: 0 0 78px; max-width: 78px; }
      .flow-filter-clear { color: #79c0ff; text-decoration: none; font-size: 0.78rem; }
      .flow-filter-clear:hover { text-decoration: underline; }
      .flow-filter-note { color: #8b949e; font-size: 0.78rem; }
      .flow-check-label { display: inline-flex; align-items: center; gap: 6px; color: #8b949e; }
      .flow-check-label input { margin: 0; accent-color: #1f6feb; }
      .flow-policy-panel { background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 14px 16px 16px; margin-bottom: 18px; }
      .flow-policy-panel .section-title { margin-top: 0; }
      .flow-policy-form { display: grid; gap: 12px; }
      .flow-policy-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: 10px; align-items: end; }
      .flow-policy-field { display: grid; gap: 5px; min-width: 0; }
      .flow-policy-field span { color: #8b949e; font-size: 0.68rem; text-transform: uppercase; letter-spacing: 0.5px; }
      .flow-policy-actions { display: flex; justify-content: flex-end; }
      .flow-policy-action { display: inline-flex; align-items: center; justify-content: center; text-decoration: none; }
      .flow-policy-preview { display: grid; gap: 5px; color: #c9d1d9; background: #0d1117; border: 1px solid #30363d; border-radius: 6px; padding: 10px 12px; font-size: 0.8rem; }
      .flow-policy-preview-title { color: #79c0ff; font-size: 0.68rem; text-transform: uppercase; letter-spacing: 0.5px; }
      .flow-alert { border-radius: 6px; padding: 8px 10px; margin-bottom: 12px; font-size: 0.8rem; }
      .flow-alert-ok { background: rgba(35, 134, 54, 0.18); border: 1px solid rgba(35, 134, 54, 0.55); color: #a5d6a7; }
      .flow-alert-error { background: rgba(248, 81, 73, 0.14); border: 1px solid rgba(248, 81, 73, 0.55); color: #ffb3ad; }
      .flow-issue-row { display: flex; gap: 12px; flex-wrap: wrap; margin-bottom: 16px; }
      .flow-issue { display: flex; align-items: center; gap: 8px; background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 10px 12px; font-size: 0.82rem; color: #c9d1d9; }
      .flow-pill { display: inline-block; background: #21262d; color: #8b949e; border: 1px solid #30363d; border-radius: 999px; padding: 1px 7px; font-size: 0.68rem; margin: 1px 2px 1px 0; white-space: nowrap; }
      .flow-pill.flow-value-ref-link { color: #f0f6fc; }
      .flow-link { color: #79c0ff; text-decoration: none; }
      .flow-link:hover { text-decoration: underline; }
      .chart-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 16px; margin-bottom: 16px; }
      .chart-card { background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 16px; min-height: 220px; }
      .chart-title { font-size: 0.75rem; color: #8b949e; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 12px; }
      .chart-card canvas { width: 100% !important; max-height: 220px; }
      .chart-empty { color: #8b949e; padding: 28px 0; text-align: center; }
      .chart-bars { display: grid; gap: 14px; }
      .chart-row { display: grid; grid-template-columns: minmax(120px, 220px) 1fr; gap: 14px; align-items: start; }
      .chart-row-label { color: #f0f6fc; font-weight: 600; overflow-wrap: anywhere; }
      .chart-row-bars { display: grid; gap: 6px; }
      .chart-bar-line { display: grid; grid-template-columns: 76px 1fr 64px; gap: 10px; align-items: center; }
      .chart-bar-label { color: #9da7b3; font-size: 0.78rem; }
      .chart-bar-value { color: #f0f6fc; font-size: 0.78rem; text-align: right; }
      .chart-bar-track { height: 10px; border-radius: 999px; background: #0d1117; overflow: hidden; border: 1px solid #30363d; }
      .chart-bar-fill { display: block; height: 100%; border-radius: 999px; min-width: 2px; }
      .bar-green { background: #3fb950; }
      .bar-yellow { background: #d29922; }
      .bar-red { background: #f85149; }
      .bar-blue { background: #58a6ff; }
      .flow-timeline-caption { color: #8b949e; font-size: 0.74rem; }
      .flow-step-waterfall { display: grid; gap: 8px; min-width: 0; }
      .flow-step-waterfall-scroll { overflow-x: auto; border: 1px solid #30363d; border-radius: 8px; background: #0d1117; }
      .flow-step-waterfall-header, .flow-step-waterfall-row { min-width: 820px; display: grid; grid-template-columns: minmax(180px, 240px) minmax(420px, 1fr) 92px; gap: 12px; align-items: center; }
      .flow-step-waterfall-header { min-height: 42px; padding: 0 12px; border-bottom: 1px solid #30363d; color: #8b949e; font-size: 0.68rem; font-weight: 700; text-transform: uppercase; letter-spacing: 0.5px; }
      .flow-step-waterfall-row { min-height: 50px; padding: 8px 12px; border-bottom: 1px solid rgba(48, 54, 61, 0.72); color: #f0f6fc; text-decoration: none; }
      .flow-step-waterfall-row:last-child { border-bottom: none; }
      .flow-step-waterfall-row:hover, .flow-step-waterfall-row:focus { background: #10243a; }
      .flow-step-waterfall-label { display: grid; gap: 3px; min-width: 0; }
      .flow-step-waterfall-step { color: #f0f6fc; font-weight: 700; overflow-wrap: anywhere; }
      .flow-step-waterfall-state { color: #8b949e; font-size: 0.72rem; overflow-wrap: anywhere; }
      .flow-step-waterfall-axis, .flow-step-waterfall-track { position: relative; min-width: 0; }
      .flow-step-waterfall-axis { align-self: stretch; }
      .flow-step-waterfall-axis-label { position: absolute; top: 50%; transform: translate(-50%, -50%); color: #6e7681; font-size: 0.66rem; font-variant-numeric: tabular-nums; white-space: nowrap; }
      .flow-step-waterfall-track { height: 28px; border: 1px solid #30363d; border-radius: 6px; overflow: hidden; background: linear-gradient(to right, rgba(48, 54, 61, 0.84) 1px, transparent 1px) 0 0 / 25% 100%, #010409; }
      .flow-step-waterfall-bar { position: absolute; top: 6px; bottom: 6px; min-width: 4px; border-radius: 4px; opacity: 0.92; box-shadow: 0 0 0 1px rgba(240, 246, 252, 0.16); }
      .flow-step-waterfall-bar.bar-green { background: #3fb950; }
      .flow-step-waterfall-bar.bar-yellow { background: #d29922; }
      .flow-step-waterfall-bar.bar-red { background: #f85149; }
      .flow-step-waterfall-bar.bar-blue { background: #58a6ff; }
      .flow-step-waterfall-marker { position: absolute; top: 4px; bottom: 4px; width: 2px; transform: translateX(-1px); background: rgba(240, 246, 252, 0.72); z-index: 1; }
      .flow-step-waterfall-duration { display: grid; gap: 3px; justify-items: end; color: #f0f6fc; font-size: 0.76rem; font-variant-numeric: tabular-nums; }
      .flow-step-waterfall-duration span + span { color: #8b949e; font-size: 0.68rem; }
      .timeline-event-row:target { outline: 2px solid #58a6ff; outline-offset: -2px; background: #10243a; }
      .flow-history-controls { display: flex; justify-content: space-between; align-items: center; gap: 12px; margin: 8px 0 12px; flex-wrap: wrap; }
      .flow-history-pages, .flow-history-counts { display: flex; align-items: center; gap: 8px; flex-wrap: wrap; }
      .flow-history-page-link, .flow-history-count { display: inline-flex; align-items: center; justify-content: center; min-height: 28px; border: 1px solid #30363d; border-radius: 6px; padding: 0 10px; color: #f0f6fc; background: #21262d; text-decoration: none; font-size: 0.76rem; }
      .flow-history-page-link:hover, .flow-history-page-link:focus, .flow-history-count:hover, .flow-history-count:focus { border-color: #58a6ff; color: #f0f6fc; background: #161b22; }
      .flow-history-page-disabled { color: #6e7681; background: #0d1117; cursor: default; }
      .flow-history-count-active { border-color: #58a6ff; color: #79c0ff; background: #10243a; }
      .flow-value-row:target { outline: 2px solid #58a6ff; outline-offset: -2px; background: #10243a; }
      .flow-event-link { color: #79c0ff; text-decoration: none; }
      .flow-event-link:hover, .flow-event-link:focus { text-decoration: underline; }
      .flow-value-ref-link { color: #f0f6fc; text-decoration: none; }
      .flow-value-ref-link:hover, .flow-value-ref-link:focus { border-color: #58a6ff; color: #f0f6fc; }
      .flow-value-preview { margin: 0; max-width: 520px; max-height: 220px; overflow: auto; white-space: pre-wrap; overflow-wrap: anywhere; color: #c9d1d9; font-size: 0.76rem; line-height: 1.4; }
      .flow-value-modal[hidden] { display: none; }
      .flow-value-modal { position: fixed; inset: 0; z-index: 1000; display: grid; place-items: center; padding: 24px; }
      .flow-value-modal-backdrop { position: absolute; inset: 0; background: rgba(1, 4, 9, 0.76); }
      .flow-value-modal-panel { position: relative; width: min(920px, 100%); max-height: min(760px, calc(100vh - 48px)); display: flex; flex-direction: column; gap: 12px; background: #161b22; border: 1px solid #30363d; border-radius: 8px; box-shadow: 0 18px 60px rgba(0, 0, 0, 0.44); padding: 16px; }
      .flow-value-modal-header { display: flex; align-items: flex-start; justify-content: space-between; gap: 16px; }
      .flow-value-modal-header .section-title { margin-bottom: 4px; }
      .flow-value-modal-ref { color: #8b949e; font-size: 0.76rem; overflow-wrap: anywhere; }
      .flow-value-modal-close { height: 32px; border: 1px solid #30363d; background: #21262d; color: #f0f6fc; border-radius: 6px; padding: 0 12px; font-size: 0.78rem; cursor: pointer; }
      .flow-value-modal-close:hover { background: #30363d; }
      .flow-value-modal-body { flex: 1; min-height: 220px; max-height: 560px; overflow: auto; margin: 0; padding: 12px; border: 1px solid #30363d; border-radius: 6px; background: #0d1117; color: #c9d1d9; white-space: pre-wrap; overflow-wrap: anywhere; font-size: 0.8rem; line-height: 1.45; }
      .flow-value-modal-actions { display: flex; align-items: center; gap: 10px; }
      .flow-section-note { color: #8b949e; font-size: 0.78rem; margin: -4px 0 10px; }
      .flow-lineage-map { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 10px; margin-bottom: 16px; }
      .flow-lineage-node { display: grid; gap: 4px; min-width: 0; padding: 10px 12px; border: 1px solid #30363d; border-radius: 8px; background: #161b22; color: #f0f6fc; text-decoration: none; }
      .flow-lineage-node:hover, .flow-lineage-node:focus { border-color: #58a6ff; background: #10243a; }
      .flow-lineage-node-id { font-family: 'SFMono-Regular', Consolas, monospace; overflow-wrap: anywhere; }
      .flow-lineage-node-meta { color: #8b949e; font-size: 0.76rem; }
      .flow-lineage-empty { color: #8b949e; background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 14px 16px; }

      /* Compact table */
      table { width: 100%; border-collapse: collapse; background: #161b22; border: 1px solid #30363d; border-radius: 6px; overflow: hidden; font-size: 0.82rem; }
      th { background: #21262d; color: #8b949e; font-size: 0.7rem; text-transform: uppercase; letter-spacing: 0.5px; padding: 6px 12px; text-align: left; }
      td { padding: 6px 12px; border-top: 1px solid #21262d; }
      tr:hover td { background: #1c2128; }

      /* Status colors */
      .c-green { color: #3fb950; }
      .c-yellow { color: #d29922; }
      .c-red { color: #f85149; }
      .c-muted { color: #8b949e; }

      /* Badges */
      .badge { display: inline-block; padding: 2px 8px; border-radius: 12px; font-size: 0.7rem; font-weight: 600; }
      .badge-ok { background: #238636; color: #fff; }
      .badge-warning { background: #9e6a03; color: #fff; }
      .badge-pressure { background: #da3633; color: #fff; }
      .badge-reject { background: #8b1a1a; color: #fff; }
      .badge-merging { background: #1f6feb; color: #fff; }
      .badge-idle { background: #30363d; color: #c9d1d9; }

      /* Memory pressure alert */
      .pressure-alert { background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 16px 20px; margin-bottom: 16px; }
      .pressure-alert.level-warning { border-color: #9e6a03; }
      .pressure-alert.level-pressure { border-color: #da3633; }
      .pressure-alert.level-reject { border-color: #f85149; border-width: 2px; }
      .pressure-header { display: flex; align-items: center; gap: 8px; margin-bottom: 8px; }
      .pressure-bar-wrap { width: 100%; height: 8px; background: #21262d; border-radius: 4px; overflow: hidden; margin: 8px 0; }
      .pressure-bar-fill { height: 100%; border-radius: 4px; }
      .pressure-details { font-size: 0.8rem; color: #8b949e; }
      .pressure-details span { color: #c9d1d9; font-weight: 600; }
      .pressure-action { font-size: 0.75rem; color: #d29922; margin-top: 6px; font-style: italic; }

      /* Connections inline */
      .conn-row { display: flex; gap: 24px; align-items: center; background: #161b22; border: 1px solid #30363d; border-radius: 6px; padding: 10px 16px; font-size: 0.85rem; flex-wrap: wrap; }
      .conn-item .conn-label { font-size: 0.7rem; color: #8b949e; text-transform: uppercase; letter-spacing: 0.3px; }
      .conn-item .conn-val { font-weight: 700; color: #f0f6fc; }

      /* Sidebar */
      .layout { display: flex; min-height: calc(100vh - 54px); }
      .sidebar { width: 200px; flex-shrink: 0; background: #161b22; border-right: 1px solid #30363d; padding: 12px 0; position: sticky; top: 0; height: calc(100vh - 54px); overflow-y: auto; }
      .sidebar a { display: flex; align-items: center; gap: 8px; padding: 8px 16px; text-decoration: none; color: #c9d1d9; font-size: 0.82rem; transition: background 0.1s; border-left: 3px solid transparent; }
      .sidebar a:hover { background: #1c2128; }
      .sidebar a.active { background: #1c2128; border-left-color: #58a6ff; color: #58a6ff; font-weight: 600; }
      .sidebar .nav-label { flex: 1; }
      .sidebar .nav-badge { font-size: 0.65rem; color: #8b949e; background: #21262d; padding: 1px 6px; border-radius: 8px; white-space: nowrap; }
      .sidebar .nav-section { font-size: 0.65rem; color: #8b949e; text-transform: uppercase; letter-spacing: 0.5px; padding: 16px 16px 4px; }
      .main-content { flex: 1; min-width: 0; }

      /* Sub-page header (still used inside main-content on sub-pages) */
      .subpage-header { display: flex; align-items: center; gap: 16px; padding: 12px 20px; background: #0d1117; border-bottom: 1px solid #30363d; }
      .subpage-title { font-size: 1.1rem; font-weight: 700; color: #f0f6fc; }

      /* Footer */
      .footer { position: fixed; bottom: 0; left: 0; right: 0; background: #0d1117; border-top: 1px solid #21262d; padding: 6px 20px; font-size: 0.7rem; color: #8b949e; display: flex; justify-content: space-between; flex-wrap: wrap; gap: 8px; }

      /* Tooltip */
      .info-icon { position: relative; display: inline-block; width: 14px; height: 14px; border-radius: 50%; background: #30363d; color: #8b949e; font-size: 10px; text-align: center; line-height: 14px; cursor: help; margin-left: 4px; vertical-align: middle; outline: none; }
      .info-icon:hover,
      .info-icon:focus { background: #58a6ff; color: #0d1117; }
      .info-icon::after { content: attr(data-tooltip); position: absolute; left: 50%; bottom: calc(100% + 8px); transform: translateX(-50%); z-index: 50; width: max-content; min-width: 180px; max-width: 280px; padding: 8px 10px; border-radius: 6px; border: 1px solid #30363d; background: #0d1117; color: #f0f6fc; box-shadow: 0 8px 24px rgba(1, 4, 9, 0.45); font-size: 0.72rem; line-height: 1.35; font-weight: 400; white-space: normal; text-align: left; visibility: hidden; opacity: 0; pointer-events: none; transition: opacity 120ms ease, visibility 120ms ease; }
      .info-icon::before { content: ""; position: absolute; left: 50%; bottom: calc(100% + 3px); transform: translateX(-50%) rotate(45deg); z-index: 51; width: 8px; height: 8px; background: #0d1117; border-right: 1px solid #30363d; border-bottom: 1px solid #30363d; visibility: hidden; opacity: 0; pointer-events: none; transition: opacity 120ms ease, visibility 120ms ease; }
      .info-icon:hover::after,
      .info-icon:focus::after,
      .info-icon:hover::before,
      .info-icon:focus::before { visibility: visible; opacity: 1; }

      .mono { font-family: 'SFMono-Regular', Consolas, monospace; font-size: 0.82rem; }

      /* Sampling indicator */
      .sampled-tag { display: inline-block; font-size: 0.55rem; color: #8b949e; background: #21262d; padding: 0px 4px; border-radius: 3px; vertical-align: middle; font-weight: 400; letter-spacing: 0; text-transform: none; cursor: help; }

      /* Responsive */
      @media (max-width: 768px) {
        .layout { flex-direction: column; }
        .sidebar { width: 100%; height: auto; position: static; border-right: none; border-bottom: 1px solid #30363d; padding: 8px 0; display: flex; flex-wrap: wrap; overflow-x: auto; }
        .sidebar a { padding: 6px 12px; border-left: none; border-bottom: 2px solid transparent; font-size: 0.75rem; }
        .sidebar a.active { border-left: none; border-bottom-color: #58a6ff; }
        .sidebar .nav-section { display: none; }
        .top-bar { gap: 12px; padding: 10px 12px; }
        .top-bar .metric .val { font-size: 0.9rem; }
        .top-bar .sep { display: none; }
        .content { padding: 12px 12px 70px; }
        .hit-rate-num { font-size: 2.2rem; }
        .cache-hero { flex-direction: column; }
        .hit-rate-card { min-width: unset; }
        .flow-card-wide { grid-column: span 1; }
        .flow-nav-row { align-items: stretch; }
        .flow-tabs, .flow-search, .flow-filter-form, .flow-policy-actions { width: 100%; }
        .flow-search, .flow-filter-form { align-items: stretch; }
        .flow-filter-form label, .flow-filter-form select, .flow-filter-form input, .flow-filter-form button, .flow-search-input, .flow-search-button { width: 100%; max-width: none; }
        table { display: block; overflow-x: auto; white-space: nowrap; }
      }
    """
  end
end
