defmodule FerricstoreServer.Health.Dashboard.Layout.Styles do
  @moduledoc false

  def stylesheet do
    """
      * { margin: 0; padding: 0; box-sizing: border-box; }
      body { font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #090a0f; color: #cbd5e1; padding: 0; min-height: 100vh; }
      [data-live-component] { display: contents; }

      /* Top bar */
      .top-bar { display: flex; align-items: center; gap: 24px; padding: 12px 24px; background: #12131a; border-bottom: 1px solid #222530; flex-wrap: wrap; }
      .top-bar .logo { font-size: 1.15rem; font-weight: 700; color: #818cf8; white-space: nowrap; letter-spacing: -0.3px; }
      .top-bar .metric { display: flex; flex-direction: column; align-items: center; min-width: 80px; }
      .top-bar .metric .label { font-size: 0.65rem; color: #94a3b8; text-transform: uppercase; letter-spacing: 0.5px; }
      .top-bar .metric .val { font-size: 1.1rem; font-weight: 700; color: #f8fafc; font-family: 'JetBrains Mono', monospace; }
      .top-bar .sep { width: 1px; height: 32px; background: #222530; flex-shrink: 0; }

      /* Status badge */
      .status-dot { display: inline-block; width: 10px; height: 10px; border-radius: 50%; margin-right: 8px; vertical-align: middle; }
      .dot-green { background: #10b981; box-shadow: 0 0 8px rgba(16, 185, 129, 0.45); }
      .dot-yellow { background: #f59e0b; box-shadow: 0 0 8px rgba(245, 158, 11, 0.45); }
      .dot-red { background: #ef4444; box-shadow: 0 0 8px rgba(239, 68, 68, 0.45); }

      /* Memory bar in top bar */
      .mem-bar-wrap { width: 80px; height: 6px; background: #1e293b; border-radius: 3px; margin-top: 4px; overflow: hidden; }
      .mem-bar-fill { height: 100%; border-radius: 3px; transition: width 0.3s; }

      /* Main content */
      .content { padding: 24px 32px 80px; max-width: 1600px; margin: 0 auto; }

      /* Section headers */
      .section-title { font-size: 0.9rem; font-weight: 600; color: #818cf8; margin: 28px 0 12px; text-transform: uppercase; letter-spacing: 0.7px; }
      .section-title:first-child { margin-top: 8px; }
      .page-intro { background: #12131a; border: 1px solid #222530; border-radius: 8px; padding: 14px 18px; margin: 0 0 20px; color: #94a3b8; line-height: 1.5; font-size: 0.88rem; }
      .page-intro-title { color: #f8fafc; font-weight: 700; margin-bottom: 6px; font-size: 0.95rem; }
      .kv-panel, .kv-inspector { background: #12131a; border: 1px solid #222530; border-radius: 8px; padding: 14px 18px; margin-bottom: 20px; }
      .kv-command-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(240px, 1fr)); gap: 14px; margin-bottom: 20px; }
      .kv-command-group { background: #12131a; border: 1px solid #222530; border-radius: 8px; padding: 14px 18px; min-width: 0; }
      .kv-command-title { color: #f8fafc; font-weight: 700; margin-bottom: 6px; }
      .kv-command-purpose { color: #94a3b8; font-size: 0.78rem; margin-bottom: 10px; }

      /* Hero hit rate */
      .cache-hero { display: flex; gap: 24px; margin-bottom: 20px; flex-wrap: wrap; }
      .hit-rate-card { background: #12131a; border: 1px solid #222530; border-radius: 8px; padding: 22px 30px; text-align: center; min-width: 180px; flex: 0 0 auto; }
      .hit-rate-num { font-size: 3.2rem; font-weight: 800; line-height: 1.1; font-family: 'JetBrains Mono', monospace; }
      .hit-rate-label { font-size: 0.75rem; color: #94a3b8; text-transform: uppercase; letter-spacing: 0.5px; margin-top: 4px; }
      .hit-rate-sub { font-size: 0.8rem; color: #94a3b8; margin-top: 10px; }
      .hit-rate-sub span { color: #f8fafc; font-weight: 600; font-family: 'JetBrains Mono', monospace; }

      /* Source breakdown */
      .source-card { background: #12131a; border: 1px solid #222530; border-radius: 8px; padding: 22px 26px; flex: 1; min-width: 200px; }
      .source-row { display: flex; align-items: center; justify-content: space-between; padding: 10px 0; }
      .source-row + .source-row { border-top: 1px solid #1e293b; }
      .source-name { font-size: 0.85rem; color: #e2e8f0; }
      .source-detail { font-size: 0.7rem; color: #64748b; }
      .source-pct { font-size: 1.15rem; font-weight: 700; font-family: 'JetBrains Mono', monospace; }
      .source-bar-wrap { width: 100%; height: 4px; background: #1e293b; border-radius: 2px; margin-top: 6px; }
      .source-bar-fill { height: 100%; border-radius: 2px; }

      /* Operational summary cards */
      .ops-summary-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 14px; margin-bottom: 20px; }
      .ops-summary-card { background: #12131a; border: 1px solid #222530; border-radius: 8px; padding: 16px 18px; min-width: 0; }
      .ops-summary-label { font-size: 0.68rem; color: #94a3b8; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 8px; }
      .ops-summary-value { font-size: 1.45rem; font-weight: 800; color: #f8fafc; overflow-wrap: anywhere; font-family: 'JetBrains Mono', monospace; }
      .ops-summary-detail { color: #94a3b8; font-size: 0.72rem; margin-top: 6px; overflow-wrap: anywhere; }

      /* FerricFlow */
      .flow-card-grid, .flow-detail-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: 14px; margin-bottom: 20px; }
      .flow-detail-grid { grid-template-columns: minmax(260px, 2fr) repeat(auto-fit, minmax(180px, 1fr)); }
      .flow-card { background: #12131a; border: 1px solid #222530; border-radius: 8px; padding: 16px 18px; min-width: 0; }
      .flow-card-wide { grid-column: span 2; }
      .flow-card-label { font-size: 0.68rem; color: #94a3b8; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 8px; }
      .flow-card-value { font-size: 1.7rem; font-weight: 800; color: #f8fafc; overflow-wrap: anywhere; font-family: 'JetBrains Mono', monospace; }
      .flow-card-detail { font-size: 0.72rem; color: #94a3b8; margin-top: 6px; }
      .flow-nav-row { display: flex; align-items: center; justify-content: space-between; gap: 12px; flex-wrap: wrap; margin: 4px 0 18px; }
      .flow-tabs { display: flex; gap: 10px; flex-wrap: wrap; margin: 4px 0 18px; }
      .flow-nav-row .flow-tabs { margin: 0; }
      .flow-tab-group { display: flex; align-items: center; gap: 6px; flex-wrap: wrap; padding: 5px 6px; border: 1px solid #222530; border-radius: 8px; background: rgba(18, 19, 26, 0.55); }
      .flow-tab-group-label { color: #94a3b8; font-size: 0.62rem; text-transform: uppercase; letter-spacing: 0.5px; padding: 0 3px; }
      .flow-tab-group-links { display: flex; gap: 6px; flex-wrap: wrap; }
      .flow-tab { display: inline-flex; align-items: center; border: 1px solid #222530; background: #12131a; color: #cbd5e1; text-decoration: none; border-radius: 999px; padding: 6px 14px; font-size: 0.78rem; transition: background 0.15s, border-color 0.15s, color 0.15s; }
      .flow-tab:hover { background: #1e293b; color: #f8fafc; }
      .flow-tab.active { color: #818cf8; border-color: #6366f1; background: rgba(99, 102, 241, 0.1); font-weight: 600; }
      .flow-search { display: flex; align-items: center; gap: 6px; min-width: min(100%, 360px); }
      .flow-search-input { flex: 1; min-width: 0; height: 32px; background: #090a0f; color: #cbd5e1; border: 1px solid #222530; border-radius: 6px; padding: 0 10px; font-size: 0.78rem; font-family: 'JetBrains Mono', monospace; }
      .flow-search-input:focus { outline: none; border-color: #6366f1; box-shadow: 0 0 0 2px rgba(99, 102, 241, 0.18); }
      .flow-search-button { height: 32px; border: 1px solid #222530; background: #1e293b; color: #f8fafc; border-radius: 6px; padding: 0 14px; font-size: 0.78rem; cursor: pointer; transition: background 0.15s; }
      .flow-search-button:hover { background: #334155; }
      .flow-danger-button { border-color: rgba(239, 68, 68, 0.55); color: #fca5a5; }
      .flow-danger-button:hover { background: rgba(239, 68, 68, 0.14); }
      .flow-filter-panel { display: flex; align-items: center; justify-content: space-between; gap: 12px; flex-wrap: wrap; background: #12131a; border: 1px solid #222530; border-radius: 8px; padding: 10px 14px; margin-bottom: 20px; }
      .flow-filter-form { display: flex; align-items: center; gap: 8px; flex-wrap: wrap; }
      .flow-filter-form label { font-size: 0.7rem; color: #94a3b8; text-transform: uppercase; letter-spacing: 0.5px; }
      .flow-filter-form select { min-width: 220px; }
      .flow-filter-form input[type="search"] { min-width: 160px; }
      .flow-query-help { display: grid; gap: 5px; background: #090a0f; border: 1px solid #222530; border-radius: 8px; padding: 10px 14px; margin-bottom: 12px; color: #94a3b8; font-size: 0.8rem; }
      .flow-query-help-main { display: flex; gap: 8px; align-items: baseline; flex-wrap: wrap; color: #e2e8f0; }
      .flow-query-command { color: #38bdf8; font-family: 'JetBrains Mono', monospace; font-weight: 700; }
      .flow-query-help-detail { color: #94a3b8; line-height: 1.45; }
      .flow-query-field { display: grid; gap: 4px; min-width: 170px; }
      .flow-query-field[hidden], .flow-query-check[hidden] { display: none !important; }
      .flow-query-field .flow-search-input { width: 100%; }
      .flow-field-help { color: #64748b; font-size: 0.66rem; text-transform: none; letter-spacing: 0; line-height: 1.25; max-width: 220px; }
      .flow-query-check { align-self: end; height: 32px; }
      .flow-filter-range { flex: 0 0 150px; max-width: 150px; min-width: 150px; }
      .flow-filter-time { flex: 0 0 172px; max-width: 172px; min-width: 172px; }
      .flow-filter-limit { flex: 0 0 78px; max-width: 78px; }
      .flow-filter-clear { color: #38bdf8; text-decoration: none; font-size: 0.78rem; }
      .flow-filter-clear:hover { text-decoration: underline; }
      .flow-filter-note { color: #94a3b8; font-size: 0.78rem; }
      .flow-check-label { display: inline-flex; align-items: center; gap: 6px; color: #94a3b8; }
      .flow-check-label input { margin: 0; accent-color: #6366f1; }
      .flow-policy-panel { background: #12131a; border: 1px solid #222530; border-radius: 8px; padding: 16px 18px 18px; margin-bottom: 20px; }
      .flow-policy-panel .section-title { margin-top: 0; }
      .flow-policy-form { display: grid; gap: 12px; }
      .flow-policy-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: 10px; align-items: end; }
      .flow-policy-field { display: grid; gap: 5px; min-width: 0; }
      .flow-policy-field span { color: #94a3b8; font-size: 0.68rem; text-transform: uppercase; letter-spacing: 0.5px; }
      .flow-policy-actions { display: flex; justify-content: flex-end; }
      .flow-policy-action { display: inline-flex; align-items: center; justify-content: center; text-decoration: none; }
      .flow-policy-preview { display: grid; gap: 5px; color: #cbd5e1; background: #090a0f; border: 1px solid #222530; border-radius: 6px; padding: 10px 14px; font-size: 0.8rem; }
      .flow-policy-preview-title { color: #38bdf8; font-size: 0.68rem; text-transform: uppercase; letter-spacing: 0.5px; }
      .flow-alert { border-radius: 6px; padding: 10px 12px; margin-bottom: 14px; font-size: 0.8rem; }
      .flow-alert-ok { background: rgba(16, 185, 129, 0.12); border: 1px solid rgba(16, 185, 129, 0.4); color: #a7f3d0; }
      .flow-alert-error { background: rgba(239, 68, 68, 0.12); border: 1px solid rgba(239, 68, 68, 0.4); color: #fca5a5; }
      .flow-bars { display: grid; gap: 8px; background: #12131a; border: 1px solid #222530; border-radius: 8px; padding: 14px; margin-bottom: 20px; }
      .flow-bar-row { display: grid; grid-template-columns: 92px 1fr 64px; align-items: center; gap: 10px; color: #94a3b8; font-size: 0.78rem; }
      .flow-bar-track { height: 10px; background: #090a0f; border: 1px solid #222530; border-radius: 999px; overflow: hidden; }
      .flow-bar-track span { display: block; min-width: 2px; height: 100%; border-radius: 999px; }
      .flow-bar-track .status-good { background: #10b981; }
      .flow-bar-track .status-warn { background: #f59e0b; }
      .flow-bar-track .status-bad { background: #ef4444; }
      .flow-issue-row { display: flex; gap: 12px; flex-wrap: wrap; margin-bottom: 20px; }
      .flow-issue { display: flex; align-items: center; gap: 8px; background: #12131a; border: 1px solid #222530; border-radius: 8px; padding: 10px 14px; font-size: 0.82rem; color: #cbd5e1; }
      .flow-pill { display: inline-block; background: #1e293b; color: #94a3b8; border: 1px solid #222530; border-radius: 999px; padding: 2px 8px; font-size: 0.68rem; margin: 1px 2px 1px 0; white-space: nowrap; }
      .flow-pill.flow-value-ref-link { color: #f8fafc; }
      .flow-link { color: #38bdf8; text-decoration: none; }
      .flow-link:hover { text-decoration: underline; }
      .chart-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 16px; margin-bottom: 20px; }
      .chart-card { background: #12131a; border: 1px solid #222530; border-radius: 8px; padding: 16px; min-height: 220px; }
      .chart-title { font-size: 0.75rem; color: #94a3b8; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 12px; }
      .chart-card canvas { width: 100% !important; max-height: 220px; }
      .chart-empty { color: #94a3b8; padding: 28px 0; text-align: center; }
      .chart-bars { display: grid; gap: 14px; }
      .chart-row { display: grid; grid-template-columns: minmax(120px, 220px) 1fr; gap: 14px; align-items: start; }
      .chart-row-label { color: #f8fafc; font-weight: 600; overflow-wrap: anywhere; }
      .chart-row-bars { display: grid; gap: 6px; }
      .chart-bar-line { display: grid; grid-template-columns: 76px 1fr 64px; gap: 10px; align-items: center; }
      .chart-bar-label { color: #94a3b8; font-size: 0.78rem; }
      .chart-bar-value { color: #f8fafc; font-size: 0.78rem; text-align: right; font-family: 'JetBrains Mono', monospace; }
      .chart-bar-track { height: 10px; border-radius: 999px; background: #090a0f; overflow: hidden; border: 1px solid #222530; }
      .chart-bar-fill { display: block; height: 100%; border-radius: 999px; min-width: 2px; }
      .bar-green { background: #10b981; }
      .bar-yellow { background: #f59e0b; }
      .bar-red { background: #ef4444; }
      .bar-blue { background: #3b82f6; }
      .flow-timeline-caption { color: #94a3b8; font-size: 0.74rem; }
      .flow-step-waterfall { display: grid; gap: 8px; min-width: 0; }
      .flow-step-waterfall-scroll { overflow-x: auto; border: 1px solid #222530; border-radius: 8px; background: #090a0f; }
      .flow-step-waterfall-header, .flow-step-waterfall-row { min-width: 820px; display: grid; grid-template-columns: minmax(180px, 240px) minmax(420px, 1fr) 92px; gap: 12px; align-items: center; }
      .flow-step-waterfall-header { min-height: 42px; padding: 0 12px; border-bottom: 1px solid #222530; color: #94a3b8; font-size: 0.68rem; font-weight: 700; text-transform: uppercase; letter-spacing: 0.5px; }
      .flow-step-waterfall-row { min-height: 50px; padding: 8px 12px; border-bottom: 1px solid rgba(34, 37, 48, 0.72); color: #f8fafc; text-decoration: none; transition: background 0.15s; }
      .flow-step-waterfall-row:last-child { border-bottom: none; }
      .flow-step-waterfall-row:hover, .flow-step-waterfall-row:focus { background: rgba(99, 102, 241, 0.12); }
      .flow-step-waterfall-label { display: grid; gap: 3px; min-width: 0; }
      .flow-step-waterfall-step { color: #f8fafc; font-weight: 700; overflow-wrap: anywhere; }
      .flow-step-waterfall-state { color: #94a3b8; font-size: 0.72rem; overflow-wrap: anywhere; }
      .flow-step-waterfall-axis, .flow-step-waterfall-track { position: relative; min-width: 0; }
      .flow-step-waterfall-axis { align-self: stretch; }
      .flow-step-waterfall-axis-label { position: absolute; top: 50%; transform: translate(-50%, -50%); color: #64748b; font-size: 0.66rem; font-variant-numeric: tabular-nums; white-space: nowrap; font-family: 'JetBrains Mono', monospace; }
      .flow-step-waterfall-track { height: 28px; border: 1px solid #222530; border-radius: 6px; overflow: hidden; background: linear-gradient(to right, rgba(34, 37, 48, 0.84) 1px, transparent 1px) 0 0 / 25% 100%, #030712; }
      .flow-step-waterfall-bar { position: absolute; top: 6px; bottom: 6px; min-width: 4px; border-radius: 4px; opacity: 0.92; box-shadow: 0 0 0 1px rgba(248, 250, 252, 0.16); }
      .flow-step-waterfall-bar.bar-green { background: #10b981; }
      .flow-step-waterfall-bar.bar-yellow { background: #f59e0b; }
      .flow-step-waterfall-bar.bar-red { background: #ef4444; }
      .flow-step-waterfall-bar.bar-blue { background: #3b82f6; }
      .flow-step-waterfall-marker { position: absolute; top: 4px; bottom: 4px; width: 2px; transform: translateX(-1px); background: rgba(248, 250, 252, 0.72); z-index: 1; }
      .flow-step-waterfall-duration { display: grid; gap: 3px; justify-items: end; color: #f8fafc; font-size: 0.76rem; font-variant-numeric: tabular-nums; font-family: 'JetBrains Mono', monospace; }
      .flow-step-waterfall-duration span + span { color: #94a3b8; font-size: 0.68rem; }
      .timeline-event-row:target { outline: 2px solid #6366f1; outline-offset: -2px; background: rgba(99, 102, 241, 0.12); }
      .flow-history-controls { display: flex; justify-content: space-between; align-items: center; gap: 12px; margin: 8px 0 12px; flex-wrap: wrap; }
      .flow-history-pages, .flow-history-counts { display: flex; align-items: center; gap: 8px; flex-wrap: wrap; }
      .flow-history-page-link, .flow-history-count { display: inline-flex; align-items: center; justify-content: center; min-height: 28px; border: 1px solid #222530; border-radius: 6px; padding: 0 12px; color: #f8fafc; background: #1e293b; text-decoration: none; font-size: 0.76rem; transition: background 0.15s; }
      .flow-history-page-link:hover, .flow-history-page-link:focus, .flow-history-count:hover, .flow-history-count:focus { border-color: #6366f1; color: #f8fafc; background: #12131a; }
      .flow-history-page-disabled { color: #64748b; background: #090a0f; cursor: default; }
      .flow-history-count-active { border-color: #6366f1; color: #818cf8; background: rgba(99, 102, 241, 0.12); }
      .flow-value-row:target { outline: 2px solid #6366f1; outline-offset: -2px; background: rgba(99, 102, 241, 0.12); }
      .flow-event-link { color: #38bdf8; text-decoration: none; }
      .flow-event-link:hover, .flow-event-link:focus { text-decoration: underline; }
      .flow-value-ref-link { color: #f8fafc; text-decoration: none; }
      .flow-value-ref-link:hover, .flow-value-ref-link:focus { border-color: #6366f1; color: #f8fafc; }
      .flow-value-preview { margin: 0; max-width: 520px; max-height: 220px; overflow: auto; white-space: pre-wrap; overflow-wrap: anywhere; color: #cbd5e1; font-size: 0.76rem; line-height: 1.45; font-family: 'JetBrains Mono', monospace; }
      .flow-value-modal[hidden] { display: none; }
      .flow-value-modal { position: fixed; inset: 0; z-index: 1000; display: grid; place-items: center; padding: 24px; }
      .flow-value-modal-backdrop { position: absolute; inset: 0; background: rgba(3, 7, 18, 0.8); }
      .flow-value-modal-panel { position: relative; width: min(920px, 100%); max-height: min(760px, calc(100vh - 48px)); display: flex; flex-direction: column; gap: 12px; background: #12131a; border: 1px solid #222530; border-radius: 8px; box-shadow: 0 20px 80px rgba(0, 0, 0, 0.6); padding: 18px; }
      .flow-value-modal-header { display: flex; align-items: flex-start; justify-content: space-between; gap: 16px; }
      .flow-value-modal-header .section-title { margin-bottom: 4px; }
      .flow-value-modal-ref { color: #94a3b8; font-size: 0.76rem; overflow-wrap: anywhere; font-family: 'JetBrains Mono', monospace; }
      .flow-value-modal-close { height: 32px; border: 1px solid #222530; background: #1e293b; color: #f8fafc; border-radius: 6px; padding: 0 14px; font-size: 0.78rem; cursor: pointer; transition: background 0.15s; }
      .flow-value-modal-close:hover { background: #334155; }
      .flow-value-modal-body { flex: 1; min-height: 220px; max-height: 560px; overflow: auto; margin: 0; padding: 14px; border: 1px solid #222530; border-radius: 6px; background: #090a0f; color: #cbd5e1; white-space: pre-wrap; overflow-wrap: anywhere; font-size: 0.8rem; line-height: 1.45; font-family: 'JetBrains Mono', monospace; }
      .flow-value-modal-actions { display: flex; align-items: center; gap: 10px; }
      .flow-section-note { color: #94a3b8; font-size: 0.78rem; margin: -4px 0 10px; }
      .flow-lineage-map { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 10px; margin-bottom: 16px; }
      .flow-lineage-node { display: grid; gap: 4px; min-width: 0; padding: 12px 14px; border: 1px solid #222530; border-radius: 8px; background: #12131a; color: #f8fafc; text-decoration: none; transition: border-color 0.15s, background 0.15s; }
      .flow-lineage-node:hover, .flow-lineage-node:focus { border-color: #6366f1; background: rgba(99, 102, 241, 0.12); }
      .flow-lineage-node-id { font-family: 'JetBrains Mono', monospace; overflow-wrap: anywhere; }
      .flow-lineage-node-meta { color: #94a3b8; font-size: 0.76rem; }
      .flow-lineage-empty { color: #94a3b8; background: #12131a; border: 1px solid #222530; border-radius: 8px; padding: 14px 16px; }

      /* Separated table with rounded corners (Bugfix) */
      table { width: 100%; border-collapse: separate; border-spacing: 0; background: #12131a; border: 1px solid #222530; border-radius: 8px; overflow: hidden; font-size: 0.82rem; }
      th { background: #1e293b; color: #94a3b8; font-size: 0.7rem; font-weight: 600; text-transform: uppercase; letter-spacing: 0.5px; padding: 10px 14px; text-align: left; border-bottom: 1px solid #222530; }
      td { padding: 10px 14px; border-bottom: 1px solid #1e293b; }
      tr:last-child td { border-bottom: none; }
      tr:hover td { background: #1c1d26; }
      .table-scroll { max-width: 100%; overflow-x: auto; border-radius: 8px; -webkit-overflow-scrolling: touch; scrollbar-width: thin; scrollbar-color: #334155 #090a0f; }
      .table-scroll:focus { outline: 2px solid rgba(99, 102, 241, 0.5); outline-offset: 2px; }
      .table-scroll::-webkit-scrollbar { height: 8px; }
      .table-scroll::-webkit-scrollbar-track { background: #090a0f; border-radius: 8px; }
      .table-scroll::-webkit-scrollbar-thumb { background: #334155; border-radius: 8px; }
      .table-scroll table { min-width: 100%; }

      /* Status colors */
      .c-green { color: #10b981; }
      .c-yellow { color: #f59e0b; }
      .c-red { color: #ef4444; }
      .c-muted { color: #94a3b8; }

      /* Badges */
      .badge { display: inline-block; padding: 2px 8px; border-radius: 12px; font-size: 0.7rem; font-weight: 600; }
      .badge-ok { background: rgba(16, 185, 129, 0.12); color: #34d399; border: 1px solid rgba(16, 185, 129, 0.3); }
      .badge-warning { background: rgba(245, 158, 11, 0.12); color: #fbbf24; border: 1px solid rgba(245, 158, 11, 0.3); }
      .badge-pressure { background: rgba(239, 68, 68, 0.12); color: #f87171; border: 1px solid rgba(239, 68, 68, 0.3); }
      .badge-reject { background: rgba(220, 38, 38, 0.2); color: #fca5a5; border: 1px solid rgba(220, 38, 38, 0.4); }
      .badge-merging { background: rgba(99, 102, 241, 0.12); color: #818cf8; border: 1px solid rgba(99, 102, 241, 0.3); }
      .badge-idle { background: #1e293b; color: #94a3b8; border: 1px solid #222530; }

      /* Memory pressure alert */
      .pressure-alert { background: #12131a; border: 1px solid #222530; border-radius: 8px; padding: 16px 20px; margin-bottom: 16px; }
      .pressure-alert.level-warning { border-color: #f59e0b; }
      .pressure-alert.level-pressure { border-color: #ef4444; }
      .pressure-alert.level-reject { border-color: #dc2626; border-width: 2px; }
      .pressure-header { display: flex; align-items: center; gap: 8px; margin-bottom: 8px; }
      .pressure-bar-wrap { width: 100%; height: 8px; background: #1e293b; border-radius: 4px; overflow: hidden; margin: 8px 0; }
      .pressure-bar-fill { height: 100%; border-radius: 4px; }
      .pressure-details { font-size: 0.8rem; color: #94a3b8; }
      .pressure-details span { color: #f8fafc; font-weight: 600; }
      .pressure-action { font-size: 0.75rem; color: #fbbf24; margin-top: 6px; font-style: italic; }

      /* Connections inline */
      .conn-row { display: flex; gap: 24px; align-items: center; background: #12131a; border: 1px solid #222530; border-radius: 6px; padding: 12px 18px; font-size: 0.85rem; flex-wrap: wrap; }
      .conn-item .conn-label { font-size: 0.7rem; color: #94a3b8; text-transform: uppercase; letter-spacing: 0.3px; }
      .conn-item .conn-val { font-weight: 700; color: #f8fafc; font-family: 'JetBrains Mono', monospace; }

      /* Sidebar */
      .layout { display: flex; min-height: calc(100vh - 54px); }
      .sidebar { width: 220px; flex-shrink: 0; background: #12131a; border-right: 1px solid #222530; padding: 16px 0; position: sticky; top: 0; height: calc(100vh - 54px); overflow-y: auto; }
      .sidebar a { display: flex; align-items: center; gap: 8px; padding: 10px 20px; text-decoration: none; color: #cbd5e1; font-size: 0.82rem; transition: background 0.15s, border-left-color 0.15s, color 0.15s; border-left: 3px solid transparent; }
      .sidebar a:hover { background: #1e293b; color: #f8fafc; }
      .sidebar a.active { background: rgba(99, 102, 241, 0.08); border-left-color: #6366f1; color: #818cf8; font-weight: 600; }
      .sidebar a.nav-subitem { padding-left: 32px; font-size: 0.78rem; color: #94a3b8; }
      .sidebar a.nav-subitem.active { color: #818cf8; }
      .sidebar .nav-label { flex: 1; }
      .sidebar .nav-badge { font-size: 0.65rem; color: #94a3b8; background: #1e293b; padding: 2px 6px; border-radius: 8px; white-space: nowrap; border: 1px solid #222530; }
      .sidebar .nav-section { font-size: 0.65rem; color: #64748b; text-transform: uppercase; letter-spacing: 0.5px; padding: 18px 20px 6px; }
      .sidebar .nav-section:first-child { padding-top: 4px; }
      .main-content { flex: 1; min-width: 0; }

      /* Sub-page header */
      .subpage-header { display: flex; align-items: center; gap: 16px; padding: 14px 24px; background: #090a0f; border-bottom: 1px solid #222530; }
      .subpage-title { font-size: 1.15rem; font-weight: 700; color: #f8fafc; letter-spacing: -0.2px; }

      /* Footer */
      .footer { position: fixed; bottom: 0; left: 0; right: 0; background: #090a0f; border-top: 1px solid #222530; padding: 8px 24px; font-size: 0.7rem; color: #64748b; display: flex; justify-content: space-between; flex-wrap: wrap; gap: 8px; z-index: 50; }

      /* Tooltip */
      .info-icon { position: relative; display: inline-block; width: 14px; height: 14px; border-radius: 50%; background: #222530; color: #94a3b8; font-size: 10px; text-align: center; line-height: 14px; cursor: help; margin-left: 4px; vertical-align: middle; outline: none; }
      .info-icon:hover,
      .info-icon:focus { background: #6366f1; color: #f8fafc; }
      .info-icon::after { content: attr(data-tooltip); position: absolute; left: 50%; bottom: calc(100% + 8px); transform: translateX(-50%); z-index: 50; width: max-content; min-width: 180px; max-width: 280px; padding: 8px 10px; border-radius: 6px; border: 1px solid #222530; background: #12131a; color: #cbd5e1; box-shadow: 0 8px 24px rgba(0,0,0,0.5); font-size: 0.72rem; line-height: 1.35; font-weight: 400; white-space: normal; text-align: left; visibility: hidden; opacity: 0; pointer-events: none; transition: opacity 120ms ease, visibility 120ms ease; }
      .info-icon::before { content: ""; position: absolute; left: 50%; bottom: calc(100% + 3px); transform: translateX(-50%) rotate(45deg); z-index: 51; width: 8px; height: 8px; background: #12131a; border-right: 1px solid #222530; border-bottom: 1px solid #222530; visibility: hidden; opacity: 0; pointer-events: none; transition: opacity 120ms ease, visibility 120ms ease; }
      .info-icon:hover::after,
      .info-icon:focus::after,
      .info-icon:hover::before,
      .info-icon:focus::before { visibility: visible; opacity: 1; }

      .mono { font-family: 'JetBrains Mono', Consolas, monospace; font-size: 0.82rem; }

      /* Sampling indicator */
      .sampled-tag { display: inline-block; font-size: 0.55rem; color: #94a3b8; background: #1e293b; padding: 1px 5px; border-radius: 3px; vertical-align: middle; font-weight: 400; letter-spacing: 0; text-transform: none; cursor: help; border: 1px solid #222530; }

      /* Responsive */
      @media (max-width: 768px) {
        html, body { max-width: 100%; overflow-x: hidden; }
        .layout { flex-direction: column; }
        .sidebar { width: 100%; height: auto; position: static; border-right: none; border-bottom: 1px solid #222530; padding: 8px 0; display: flex; flex-wrap: wrap; overflow-x: auto; }
        .sidebar a { padding: 8px 12px; border-left: none; border-bottom: 2px solid transparent; font-size: 0.75rem; }
        .sidebar a.nav-subitem { padding-left: 12px; font-size: 0.75rem; }
        .sidebar a.active { border-left: none; border-bottom-color: #6366f1; background: transparent; }
        .sidebar .nav-section { display: none; }
        .top-bar { gap: 12px; padding: 10px 16px; }
        .top-bar .metric .val { font-size: 0.9rem; }
        .top-bar .sep { display: none; }
        .content { padding: 16px 16px 70px; }
        .hit-rate-num { font-size: 2.2rem; }
        .cache-hero { flex-direction: column; }
        .hit-rate-card { min-width: unset; }
        .flow-card-wide { grid-column: span 1; }
        .flow-nav-row { align-items: stretch; }
        .flow-tabs, .flow-search, .flow-filter-form, .flow-policy-actions { width: 100%; }
        .flow-search, .flow-filter-form { align-items: stretch; }
        .flow-filter-form label, .flow-filter-form select, .flow-filter-form input, .flow-filter-form button, .flow-search-input, .flow-search-button { width: 100%; max-width: none; }
        table { white-space: nowrap; }
        .content > table, .content :not(.table-scroll) > table { display: block; width: 100%; max-width: 100%; min-width: 0; overflow-x: auto; -webkit-overflow-scrolling: touch; scrollbar-width: thin; scrollbar-color: #334155 #090a0f; }
        .table-scroll { box-shadow: inset -18px 0 16px -18px rgba(129, 140, 248, 0.85); }
        .table-scroll table { width: max-content; min-width: 100%; }
      }
    """
  end
end
