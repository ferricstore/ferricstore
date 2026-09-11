defmodule FerricstoreServer.Health.Dashboard.Layout.Styles do
  @moduledoc false

  def stylesheet do
    """
      :root {
        color-scheme: dark;
        --surface-base: #101314;
        --surface-raised: #171b1d;
        --surface-selected: #252d2e;
        --line: #30383a;
        --line-strong: #596565;
        --text-body: #d3dcda;
        --text-strong: #f2f6f5;
        --text-muted: #a5b4b0;
        --accent: #84deca;
        --accent-solid: #246b5c;
        --accent-pressed: #195043;
        --accent-wash: #1c3630;
      }
      * { margin: 0; padding: 0; box-sizing: border-box; }
      ::selection { background: var(--accent-pressed); color: var(--text-strong); }
      :focus-visible { outline: 2px solid var(--accent); outline-offset: 3px; }
      .dashboard-skip-link { position: fixed; left: 12px; top: 8px; z-index: 1000; transform: translateY(-160%); padding: 10px 14px; background: var(--surface-raised); color: var(--text-strong); border: 1px solid var(--line-strong); border-radius: 4px; }
      .dashboard-skip-link:focus { transform: translateY(0); }
      input, textarea { caret-color: var(--accent); }
      button, input, select, textarea { font: inherit; }
      .sr-only { position: absolute; width: 1px; height: 1px; padding: 0; margin: -1px; overflow: hidden; clip: rect(0, 0, 0, 0); white-space: nowrap; border: 0; }
      .flow-field-error { color: #f5a3a8; font-size: 0.78rem; line-height: 1.4; }
      input[aria-invalid="true"], select[aria-invalid="true"], textarea[aria-invalid="true"] { border-color: #f5a3a8; }
      .flow-alert code { overflow-wrap: anywhere; white-space: pre-wrap; }
      .flow-metadata-entries { display: flex; flex-wrap: wrap; gap: 6px; margin-top: 10px; }
      .flow-metadata-overflow, .flow-reference-lookup { margin-top: 8px; white-space: normal; }
      .journal-signal-name { overflow-wrap: anywhere; min-width: 0; }
      .flow-query-workbench-form .flow-field-error { text-transform: none; letter-spacing: 0; font-weight: 400; overflow-wrap: anywhere; }
      .flow-query-reference { margin: 12px 0 18px; padding-block: 12px; border-block: 1px solid var(--line); }
      .flow-query-reference-terms { display: grid; grid-template-columns: 150px minmax(0, 1fr); gap: 10px 18px; margin-block: 16px; font-size: 0.8rem; line-height: 1.5; }
      .flow-query-reference-terms dt { color: var(--text-strong); font-weight: 600; }
      .flow-query-reference-terms dd { margin: 0; min-width: 0; overflow-wrap: anywhere; }
      .flow-query-reference-example { margin-block: 10px; }
      .flow-query-reference-example pre { white-space: pre-wrap; overflow-wrap: anywhere; margin-top: 10px; font-size: 0.8rem; }
      .flow-query-empty { padding-block: 16px; border-block: 1px solid var(--line); margin-block: 16px; }
      .flow-query-empty h3 { font-size: 0.9rem; margin-bottom: 8px; }
      .flow-query-empty p { font-size: 0.8rem; overflow-wrap: anywhere; }
      .table-scroll .flow-schedules-table { table-layout: fixed; min-width: 1650px; }
      .table-scroll .flow-policy-table { min-width: 1480px; }
      :is(.flow-policy-table, .flow-schedules-table) tr > :first-child:not([colspan]) { position: sticky; left: 0; z-index: 2; background: var(--surface-raised); min-width: 180px; width: 180px; border-right: 1px solid var(--line); }
      :is(.flow-policy-table, .flow-schedules-table) tr > :last-child:not([colspan]) { position: sticky; right: 0; z-index: 2; background: var(--surface-raised); min-width: 210px; width: 210px; border-left: 1px solid var(--line); white-space: normal; }
      :is(.flow-policy-table, .flow-schedules-table) thead tr > :is(:first-child, :last-child) { z-index: 3; background: var(--surface-selected); }
      .flow-schedule-definition-row > td { white-space: normal; }
      .flow-schedule-definition-row .dashboard-disclosure { margin: 0; }
      .flow-schedule-definition-row summary { width: fit-content; max-width: 100%; }
      .flow-schedule-definition-row dl { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 12px 20px; }
      .flow-schedule-definition-row dl > div { min-width: 0; }
      .flow-schedule-definition-row dd { overflow-wrap: anywhere; }
      #flow-schedule-create-panel .flow-policy-grid { align-items: start; }
      .flow-management-group { min-width: 0; margin: 20px 0 0; padding: 14px 0 0; border: 0; border-top: 1px solid var(--line); }
      .flow-management-group legend { padding: 0 8px 0 0; color: var(--text-strong); font-size: 0.875rem; font-weight: 600; }
      .flow-management-group .flow-policy-grid { grid-template-columns: repeat(3, minmax(0, 1fr)); }
      .flow-policy-field[hidden] { display: none; }
      .flow-definition-list { display: grid; grid-template-columns: 140px minmax(0, 1fr); gap: 8px 16px; margin: 12px 0 16px; font-size: 0.875rem; line-height: 1.5; }
      .flow-definition-list dt { color: var(--text-muted); }
      .flow-definition-list dd { margin: 0; min-width: 0; overflow-wrap: anywhere; }
      .flow-duration-control { display: grid; grid-template-columns: minmax(0, 1fr) 136px; gap: 6px; min-width: 0; }
      .flow-duration-control .flow-search-input { width: 100%; min-width: 0; }
      .flow-policy-override-list { display: grid; gap: 6px; margin-top: 10px; max-height: 260px; overflow: auto; }
      .flow-policy-override-list .flow-pill { width: fit-content; max-width: 100%; overflow-wrap: anywhere; white-space: normal; }
      body { font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: var(--surface-base); color: var(--text-body); padding: 0; min-height: 100vh; }
      [data-live-component] { display: contents; }

      /* Top bar */
      .top-bar { display: grid; grid-template-columns: minmax(0, 1fr) auto; align-items: center; gap: 12px 20px; padding: 12px 18px; background: var(--surface-raised); border-bottom: 1px solid var(--line); }
      .top-bar-identity { display: grid; grid-template-columns: auto minmax(100px, 220px) minmax(120px, 180px) minmax(70px, 100px); align-items: center; gap: 18px; min-width: 0; }
      .top-bar-metrics { display: grid; grid-template-columns: repeat(6, minmax(0, 1fr)); gap: 12px 20px; grid-column: 1 / -1; min-width: 0; padding-top: 10px; border-top: 1px solid var(--line); }
      .top-bar > .dashboard-live-status { grid-column: 2; grid-row: 1; }
      .top-bar .logo { font-size: 1.15rem; font-weight: 700; color: var(--accent); white-space: nowrap; letter-spacing: 0; }
      .top-bar .metric { display: flex; flex-direction: column; align-items: flex-start; min-width: 0; }
      .top-bar .metric .label { font-size: 0.75rem; color: var(--text-muted); text-transform: uppercase; letter-spacing: 0; }
      .top-bar .metric .val { max-width: 100%; min-width: 0; overflow-wrap: anywhere; font-size: 1.1rem; font-weight: 700; color: var(--text-strong); font-family: 'JetBrains Mono', monospace; }
      .top-bar .sep { display: none; }

      /* Status badge */
      .status-dot { display: inline-block; width: 8px; height: 8px; border-radius: 50%; margin-right: 8px; vertical-align: middle; background: var(--text-muted); }
      .dot-green { background: #10b981; }
      .dot-yellow { background: #f59e0b; }
      .dot-red { background: #ef4444; }

      /* Memory bar in top bar */
      .mem-bar-wrap { width: 80px; height: 6px; background: var(--surface-selected); border-radius: 3px; margin-top: 4px; overflow: hidden; }
      .mem-bar-fill { height: 100%; border-radius: 3px; transition: width 0.3s; }

      /* Main content */
      .content { padding: 20px 28px 32px; max-width: 1600px; margin: 0 auto; }

      /* Section headers */
      .section-title { font-size: 0.9rem; font-weight: 600; color: var(--text-strong); margin: 24px 0 12px; letter-spacing: 0; }
      h2.section-title, h3.section-title { line-height: normal; }
      .section-title:first-child { margin-top: 8px; }
      .page-intro { margin: 0 0 16px; color: var(--text-muted); line-height: 1.5; font-size: 0.82rem; }
      .kv-panel, .kv-inspector { background: var(--surface-raised); border: 1px solid var(--line); border-radius: 8px; padding: 14px 18px; margin-bottom: 20px; }
      .kv-command-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(240px, 1fr)); gap: 14px; margin-bottom: 20px; }
      .kv-command-group { background: var(--surface-raised); border: 1px solid var(--line); border-radius: 8px; padding: 14px 18px; min-width: 0; }
      .kv-command-title { color: var(--text-strong); font-weight: 700; margin-bottom: 6px; }
      .kv-query-modes { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); align-items: start; gap: 24px; margin-block: 16px 24px; }
      .kv-query-mode { display: grid; align-content: start; gap: 12px; width: 100%; min-width: 0; border: 0; border-top: 1px solid var(--line); padding: 14px 0 0; }
      .kv-query-mode legend { color: var(--text-strong); font-size: 0.875rem; font-weight: 600; padding-right: 12px; }
      .kv-query-modes > form { align-items: stretch; margin: 0; }
      .kv-query-mode label { display: grid; gap: 6px; font-size: 0.75rem; }
      .kv-query-mode .flow-check-label { display: flex; align-items: center; }
      .kv-query-mode .flow-search-input { width: 100%; min-width: 0; max-width: none; }
      .kv-query-mode .flow-filter-note { flex-basis: 100%; }
      .dashboard-table-value { display: block; width: 36ch; max-width: 36ch; white-space: normal; }
      .dashboard-table-value > summary { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; cursor: pointer; }
      .dashboard-table-value-full { margin-top: 8px; max-height: 240px; overflow: auto; white-space: pre-wrap; overflow-wrap: anywhere; font: inherit; }
      .kv-command-purpose { color: var(--text-muted); font-size: 0.78rem; margin-bottom: 10px; }

      /* Hero hit rate */
      .cache-hero { display: flex; gap: 24px; margin-bottom: 20px; flex-wrap: wrap; }
      .hit-rate-card { background: var(--surface-raised); border: 1px solid var(--line); border-radius: 8px; padding: 22px 30px; text-align: center; min-width: 180px; flex: 0 0 auto; }
      .hit-rate-num { font-size: 3.2rem; font-weight: 800; line-height: 1.1; font-family: 'JetBrains Mono', monospace; }
      .hit-rate-num.hit-rate-empty { font-size: 1rem; font-weight: 600; padding: 12px 0; }
      .hit-rate-label { font-size: 0.75rem; color: var(--text-muted); text-transform: uppercase; letter-spacing: 0; margin-top: 4px; }
      .hit-rate-sub { font-size: 0.8rem; color: var(--text-muted); margin-top: 10px; }
      .hit-rate-sub span { color: var(--text-strong); font-weight: 600; font-family: 'JetBrains Mono', monospace; }

      /* Source breakdown */
      .source-card { background: var(--surface-raised); border: 1px solid var(--line); border-radius: 8px; padding: 22px 26px; flex: 1; min-width: 200px; }
      .source-row { display: flex; align-items: center; justify-content: space-between; padding: 10px 0; }
      .source-row + .source-row { border-top: 1px solid var(--surface-selected); }
      .source-name { font-size: 0.85rem; color: #e2e8f0; }
      .source-detail { font-size: 0.75rem; color: var(--text-muted); }
      .source-pct { font-size: 1.15rem; font-weight: 700; font-family: 'JetBrains Mono', monospace; }
      .source-pct.source-pct-empty { font-size: 0.875rem; font-weight: 400; max-width: 12ch; white-space: normal; }
      .source-bar-wrap { width: 100%; height: 4px; background: var(--surface-selected); border-radius: 2px; margin-top: 6px; }
      .source-bar-fill { height: 100%; border-radius: 2px; }

      /* Operational summary cards */
      .ops-summary-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 14px; margin-bottom: 20px; }
      .ops-summary-card { background: var(--surface-raised); border: 1px solid var(--line); border-radius: 8px; padding: 16px 18px; min-width: 0; }
      .ops-summary-label { font-size: 0.75rem; color: var(--text-muted); text-transform: uppercase; letter-spacing: 0; margin-bottom: 8px; }
      .ops-summary-value { font-size: 1.45rem; font-weight: 800; color: var(--text-strong); overflow-wrap: anywhere; font-family: 'JetBrains Mono', monospace; }
      .ops-summary-detail { color: var(--text-muted); font-size: 0.75rem; margin-top: 6px; overflow-wrap: anywhere; }

      /* FerricFlow */
      .flow-card-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(145px, 1fr)); gap: 12px; margin-bottom: 20px; }
      .flow-detail-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: 14px; margin-bottom: 20px; }
      .flow-detail-grid { grid-template-columns: minmax(260px, 2fr) repeat(auto-fit, minmax(180px, 1fr)); }
      .flow-card { background: var(--surface-raised); border: 1px solid var(--line); border-radius: 8px; padding: 16px 18px; min-width: 0; }
      .flow-card-wide { grid-column: span 2; }
      .flow-card-label { font-size: 0.75rem; color: var(--text-muted); text-transform: uppercase; letter-spacing: 0; margin-bottom: 8px; }
      .flow-card-value { font-size: 1.7rem; font-weight: 800; color: var(--text-strong); overflow-wrap: anywhere; font-family: 'JetBrains Mono', monospace; }
      .flow-card-detail { font-size: 0.75rem; color: var(--text-muted); margin-top: 6px; }
      .flow-overview-ribbon, .flow-projection-ledger { display: grid; grid-template-columns: repeat(auto-fit, minmax(145px, 1fr)); margin: 0 0 20px; border: 1px solid var(--line); border-radius: 6px; background: var(--surface-raised); overflow: hidden; }
      .flow-overview-ribbon > div, .flow-projection-ledger > div { min-width: 0; padding: 10px 14px; border-right: 1px solid var(--line); }
      .flow-overview-ribbon > div:last-child, .flow-projection-ledger > div:last-child { border-right: 0; }
      .flow-overview-ribbon dt, .flow-projection-ledger dt { color: var(--text-muted); font-size: 0.75rem; font-weight: 700; text-transform: uppercase; }
      .flow-overview-ribbon dd, .flow-projection-ledger dd { margin: 3px 0 1px; color: var(--text-strong); font-size: 1.05rem; font-weight: 700; font-family: 'JetBrains Mono', monospace; overflow-wrap: anywhere; }
      .flow-overview-ribbon span, .flow-projection-ledger span { display: block; color: var(--text-muted); font-size: 0.75rem; line-height: 1.35; }
      .flow-attention-strip { display: flex; align-items: center; flex-wrap: wrap; gap: 12px 24px; padding: 12px 0; margin: 0 0 12px; border-block: 1px solid var(--line); font-size: 0.82rem; }
      .flow-attention-strip h2 { font-size: 0.875rem; color: #fca5a5; }
      .flow-attention-strip > span { display: inline-flex; align-items: center; gap: 8px; }
      .flow-attention-strip > a { margin-left: auto; text-decoration: underline; text-underline-offset: 3px; }
      .flow-execution-summary { display: grid; grid-template-columns: minmax(0, 2fr) minmax(90px, 0.65fr) minmax(70px, 0.5fr) minmax(160px, 1fr); gap: 20px; margin: 12px 0 20px; }
      .flow-execution-summary > div { min-width: 0; }
      .flow-execution-summary dt { color: var(--text-muted); font-size: 0.75rem; margin-bottom: 6px; }
      .flow-execution-summary dd { margin: 0; font-size: 0.875rem; color: var(--text-strong); font-variant-numeric: tabular-nums; overflow-wrap: anywhere; }
      .flow-nav-row { display: flex; align-items: center; justify-content: space-between; gap: 12px; flex-wrap: wrap; margin: 4px 0 18px; }
      .flow-tabs { display: flex; gap: 10px; flex-wrap: wrap; margin: 4px 0 18px; }
      .flow-nav-row .flow-tabs { margin: 0; }
      .flow-tab-group { display: flex; align-items: center; gap: 6px; flex-wrap: wrap; padding: 5px 6px; border: 1px solid var(--line); border-radius: 8px; background: rgba(18, 19, 26, 0.55); }
      .flow-tab-group-label { color: var(--text-muted); font-size: 0.75rem; text-transform: uppercase; letter-spacing: 0; padding: 0 3px; }
      .flow-tab-group-links { display: flex; gap: 6px; flex-wrap: wrap; }
      .flow-tab { display: inline-flex; align-items: center; border: 1px solid var(--line); background: var(--surface-raised); color: var(--text-body); text-decoration: none; border-radius: 999px; padding: 6px 14px; font-size: 0.78rem; transition: background 0.15s, border-color 0.15s, color 0.15s; }
      .flow-tab:hover { background: var(--surface-selected); color: var(--text-strong); }
      .flow-tab.active { color: var(--accent); border-color: var(--accent-solid); background: rgba(99, 102, 241, 0.1); font-weight: 600; }
      .flow-search { display: flex; align-items: center; gap: 6px; min-width: min(100%, 360px); }
      .flow-search-input { flex: 1; min-width: 0; height: 32px; background: var(--surface-base); color: var(--text-body); border: 1px solid #68787b; border-radius: 6px; padding: 0 10px; font-size: 0.78rem; font-family: 'JetBrains Mono', monospace; }
      textarea.flow-schedule-payload { height: auto; min-height: 8rem; padding: 10px; line-height: 1.5; resize: vertical; }
      .flow-index-identity { max-width: 28ch; min-width: 16ch; overflow-wrap: anywhere; white-space: normal; }
      .flow-index-build-identity { min-width: 14ch; max-width: 32ch; overflow-wrap: anywhere; white-space: normal; }
      .flow-index-validation { max-width: 30ch; overflow-wrap: anywhere; white-space: normal; }
      .flow-index-validation code { white-space: normal; overflow-wrap: anywhere; }
      .flow-search-input:focus { outline: 2px solid var(--accent); outline-offset: 2px; border-color: var(--accent); box-shadow: none; }
      .flow-search-button { display: inline-flex; align-items: center; justify-content: center; text-decoration: none; height: 32px; border: 1px solid var(--line); background: var(--surface-selected); color: var(--text-strong); border-radius: 6px; padding: 0 14px; font-size: 0.78rem; cursor: pointer; transition: background 0.15s; }
      .flow-search-button:hover { background: #334155; }
      .flow-investigation-context { display: flex; align-items: center; gap: 8px; min-width: 0; flex-wrap: wrap; }
      .flow-investigation-context-label { color: var(--text-muted); font-size: 0.75rem; font-weight: 700; text-transform: uppercase; }
      .flow-investigation-scope { display: inline-flex; align-items: center; gap: 5px; min-width: 0; }
      .flow-context-chip { max-width: 220px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; border: 1px solid var(--line); border-radius: 4px; background: var(--surface-raised); color: var(--text-body); padding: 3px 7px; font-size: 0.75rem; }
      .flow-context-links { display: inline-flex; align-items: center; gap: 4px; margin-left: auto; }
      .flow-context-link { border: 1px solid transparent; border-radius: 4px; color: var(--text-muted); padding: 4px 7px; font-size: 0.75rem; text-decoration: none; white-space: nowrap; }
      .flow-context-link:hover, .flow-context-link:focus-visible { border-color: var(--line-strong); color: var(--text-strong); outline: none; }
      .flow-context-link[aria-current="page"] { border-color: var(--accent-pressed); color: var(--accent); background: var(--accent-wash); }
      .flow-danger-button { border-color: rgba(239, 68, 68, 0.55); color: #fca5a5; }
      .flow-danger-button:hover { background: rgba(239, 68, 68, 0.14); }
      .flow-filter-panel { display: flex; align-items: center; justify-content: space-between; gap: 12px; flex-wrap: wrap; background: var(--surface-raised); border: 1px solid var(--line); border-radius: 8px; padding: 10px 14px; margin-bottom: 20px; }
      .flow-filter-form { display: flex; align-items: center; gap: 8px; flex-wrap: wrap; }
      .flow-filter-form label { font-size: 0.75rem; color: var(--text-muted); text-transform: uppercase; letter-spacing: 0; }
      .flow-filter-field { display: grid; gap: 4px; min-width: 0; }
      .flow-policy-scope { align-items: end; margin-bottom: 16px; }
      .flow-policy-scope .flow-policy-field { flex: 1; min-width: 180px; }
      .flow-policy-field small { font-size: 0.75rem; color: var(--text-muted); }
      .acl-tester-panel { display: block; }
      .acl-tester-form { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 12px; align-items: start; }
      .acl-tester-form label { display: grid; gap: 6px; min-width: 0; }
      .acl-tester-form input, .acl-tester-form select { width: 100%; min-width: 0; max-width: none; }
      .acl-tester-form .flow-field-error { display: block; text-transform: none; letter-spacing: 0; font-weight: 400; overflow-wrap: anywhere; }
      .acl-tester-form button { align-self: end; }
      .acl-tester-panel #acl-target-error { display: block; margin-block: 8px; }
      [data-acl-profile][hidden], [data-flow-time-mode][hidden] { display: none; }
      [data-acl-profile] { border: 0; padding: 0; min-width: 0; }
      [data-acl-profile] legend { font-size: 0.78rem; color: var(--text-muted); margin-bottom: 8px; }
      .ops-summary-value.ops-summary-code { font-size: 0.82rem; font-weight: 500; font-family: 'JetBrains Mono', monospace; }
      .ops-summary-value.ops-summary-timestamp { font-size: 0.82rem; font-weight: 500; white-space: nowrap; }
      .stream-latest-time { font-size: 0.75rem; white-space: nowrap; }
      .flow-index-services { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 12px; margin: 16px 0; }
      .flow-index-services > div { display: grid; align-content: start; gap: 4px; min-width: 0; }
      .flow-index-services dt { color: var(--text-muted); font-size: 0.75rem; }
      .flow-index-services dd { margin: 0; font-family: 'JetBrains Mono', monospace; font-size: 0.82rem; overflow-wrap: anywhere; }
      .config-parameters-table { width: 100%; table-layout: fixed; }
      .config-parameters-table .config-name { width: 20%; }
      .config-parameters-table .config-value { width: 24%; }
      .config-parameters-table .config-source { width: 10%; }
      .config-parameters-table .config-scope { width: 11%; }
      .config-parameters-table .config-mode { width: 12%; }
      .config-parameters-table .config-notes { width: 23%; }
      .config-parameters-table td, .config-parameters-table th { overflow-wrap: anywhere; }
      .config-parameters-table pre { white-space: pre-wrap; overflow-wrap: anywhere; max-height: 240px; overflow: auto; }
      #flow-policy-editor .flow-policy-panel { background: none; border: 0; border-top: 1px solid var(--line); border-radius: 0; box-shadow: none; padding: 16px 0; }
      .flow-governance-meta-form { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 12px; align-items: end; }
      .flow-governance-meta-form .flow-search-input { width: 100%; max-width: none; }
      .flow-governance-meta-form .flow-search-button { justify-self: start; }
      .flow-filter-form .flow-filter-field .flow-search-input { width: 100%; text-transform: none; }
      .flow-filter-form .flow-filter-field .flow-filter-limit { width: 80px; }
      .flow-filter-form.flow-state-filter-form { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); align-items: end; gap: 12px; width: 100%; flex: 1 0 100%; }
      .flow-state-filter-form .flow-search-input { min-width: 0; max-width: none; }
      .flow-filter-time-group { grid-column: span 3; display: grid; grid-template-columns: minmax(130px, 0.8fr) repeat(2, minmax(220px, 1fr)); gap: 12px; border: 0; padding: 0; min-width: 0; }
      .flow-filter-time-group legend { color: var(--text-muted); font-size: 0.75rem; padding-bottom: 6px; }
      .flow-state-filter-form:has(noscript p) .flow-filter-time-group { grid-column: 1 / -1; grid-template-columns: minmax(140px, 0.7fr) minmax(160px, 0.9fr) repeat(2, minmax(260px, 1fr)); }
      .flow-state-filter-form noscript { grid-column: 1 / -1; }
      .flow-filter-actions { display: flex; flex-wrap: wrap; align-items: end; gap: 8px; }
      .flow-filter-actions .flow-filter-clear { padding-block: 8px; }
      .flow-state-filter-form .flow-filter-actions { grid-column: 1 / -1; }
      .table-scroll .flow-states-table { table-layout: fixed; min-width: 1560px; }
      .flow-states-table th:first-child { width: 190px; }
      .flow-states-table th:nth-child(2) { width: 240px; }
      .flow-states-table th:nth-child(6) { width: 145px; }
      .flow-states-table th:nth-child(7) { width: 105px; }
      .flow-states-table th:first-child, .flow-states-table td:first-child { position: sticky; left: 0; z-index: 1; background: var(--surface-raised); }
      .flow-states-table th:nth-child(2), .flow-states-table td:nth-child(2) { position: sticky; left: 190px; z-index: 1; background: var(--surface-raised); border-right: 1px solid var(--line-strong); }
      .flow-states-table thead th:nth-child(-n+2) { background: var(--surface-selected); z-index: 3; }
      .flow-states-table td:nth-child(-n+2) { white-space: normal; overflow-wrap: anywhere; }
      .flow-action-target, dl.flow-action-review, .flow-value-modal-provenance { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 10px 18px; margin-block: 12px; min-width: 0; }
      .flow-action-target dt, .flow-action-review dt, .flow-value-modal-provenance dt, .flow-raw-event-details dt { font-size: 0.75rem; color: var(--text-muted); margin-bottom: 3px; }
      .flow-action-target dd, .flow-action-review dd, .flow-value-modal-provenance dd, .flow-raw-event-details dd { font-size: 0.8rem; overflow-wrap: anywhere; min-width: 0; margin: 0; }
      .flow-action-target dd, .flow-value-modal-provenance dd { white-space: pre-wrap; }
      [data-flow-rewind-time-field][hidden] { display: none; }
      .flow-action-review { font-size: 0.8rem; padding-block: 10px; border-block: 1px solid var(--line); overflow-wrap: anywhere; }
      .flow-schedule-review { grid-column: 1 / -1; min-width: 0; margin-top: 12px; padding-block: 16px; border-block: 1px solid var(--line); }
      .flow-schedule-review h3, .flow-schedule-review h4 { font-size: 0.88rem; margin-bottom: 8px; }
      .flow-schedule-review p { font-size: 0.8rem; margin-bottom: 12px; }
      .flow-schedule-review .flow-policy-grid { align-items: start; gap: 20px; }
      .flow-schedule-review .flow-policy-grid > div { min-width: 0; }
      .flow-schedule-review dl { display: grid; grid-template-columns: minmax(110px, 0.4fr) minmax(0, 1fr); gap: 6px 12px; font-size: 0.8rem; }
      .flow-schedule-review dt { color: var(--text-muted); }
      .flow-schedule-review dd { margin: 0; overflow-wrap: anywhere; white-space: pre-wrap; }
      .flow-schedule-review pre { max-height: 240px; overflow: auto; white-space: pre-wrap; overflow-wrap: anywhere; font-size: 0.78rem; }
      .flow-circuit-review { margin-block: 16px; padding-block: 16px; border-block: 1px solid var(--line); font-size: 0.8rem; overflow-wrap: anywhere; }
      .flow-circuit-review > p { margin-bottom: 10px; white-space: pre-wrap; }
      .flow-circuit-review > details { margin-right: 8px; vertical-align: top; }
      .flow-history-full-detail pre { max-height: 320px; overflow: auto; white-space: pre-wrap; overflow-wrap: anywhere; font-size: 0.78rem; }
      .flow-raw-event-details dl { display: grid; gap: 10px; margin-block: 10px; }
      .bar-neutral { background: var(--text-muted); }
      .flow-filter-form select { min-width: 220px; }
      .flow-filter-form input[type="search"] { min-width: 160px; }
      .flow-state-filter-form input[type="search"], .flow-state-filter-form select { min-width: 0; width: 100%; }
      .flow-failure-triage { margin-bottom: 18px; }
      .flow-failure-filter-panel { margin-bottom: 10px; }
      .flow-failure-filter-form { flex: 1 1 auto; }
      .flow-scan-mode { display: inline-flex; align-items: stretch; margin: 0; padding: 0; border: 1px solid #334155; border-radius: 6px; overflow: hidden; }
      .flow-scan-mode legend { position: absolute; width: 1px; height: 1px; padding: 0; margin: -1px; overflow: hidden; clip: rect(0, 0, 0, 0); white-space: nowrap; border: 0; }
      .flow-scan-mode label { position: relative; display: inline-flex; align-items: center; margin: 0; cursor: pointer; }
      .flow-scan-mode label + label { border-left: 1px solid #334155; }
      .flow-scan-mode input { position: absolute; opacity: 0; pointer-events: none; }
      .flow-scan-mode span { min-width: 66px; padding: 8px 12px; color: var(--text-muted); background: var(--surface-base); text-align: center; text-transform: none; }
      .flow-scan-mode input:checked + span { color: var(--accent); background: #3730a3; }
      .flow-scan-mode input:focus-visible + span { outline: 2px solid var(--accent); outline-offset: -2px; }
      .flow-failure-summary-ribbon { display: grid; grid-template-columns: repeat(4, minmax(130px, 1fr)); margin: 0; border: 1px solid var(--line); border-radius: 6px; background: var(--surface-raised); overflow: hidden; }
      .flow-failure-summary-ribbon > div { min-width: 0; padding: 10px 14px; }
      .flow-failure-summary-ribbon > div + div { border-left: 1px solid var(--line); }
      .flow-failure-summary-ribbon dt { color: var(--text-muted); font-size: 0.75rem; font-weight: 700; text-transform: uppercase; }
      .flow-failure-summary-ribbon dd { margin: 3px 0 1px; color: var(--text-strong); font-size: 1.1rem; font-weight: 700; font-variant-numeric: tabular-nums; }
      .flow-failure-summary-ribbon span { display: block; color: var(--text-muted); font-size: 0.75rem; }
      .flow-query-help { display: flex; align-items: center; justify-content: space-between; gap: 14px; background: linear-gradient(90deg, var(--accent-wash) 0%, rgba(14, 165, 233, 0.04) 100%); border: 1px solid rgba(99, 102, 241, 0.2); border-radius: 8px; padding: 10px 14px; margin-bottom: 12px; color: var(--text-body); font-size: 0.8rem; }
      .flow-query-help-main { display: flex; gap: 10px; align-items: center; flex-wrap: wrap; color: var(--text-strong); font-weight: 600; }
      .flow-query-command { background: rgba(56, 189, 248, 0.15); color: #38bdf8; border: 1px solid rgba(56, 189, 248, 0.3); padding: 2px 7px; border-radius: 4px; font-family: 'JetBrains Mono', monospace; font-weight: 700; font-size: 0.76rem; }
      .flow-query-help-detail { color: var(--text-muted); font-size: 0.76rem; line-height: 1.4; font-weight: 400; }
      .flow-query-discovery { display: block; margin: 6px 0 0; padding: 0; background: #0a0c12; border: 1px solid #1c202e; border-radius: 6px; color: var(--text-muted); font-size: 0.75rem; line-height: 1.4; }
      .flow-query-discovery-summary { display: flex; align-items: center; justify-content: space-between; gap: 12px; padding: 7px 10px; cursor: pointer; list-style-position: inside; }
      .flow-query-discovery-summary:hover, .flow-query-discovery-summary:focus-visible { background: #10131c; outline: none; }
      .flow-query-discovery-hint { margin-left: auto; color: var(--text-muted); font-size: 0.75rem; font-weight: 400; }
      .flow-query-discovery-title { color: var(--accent); font-weight: 700; font-size: 0.75rem; white-space: nowrap; display: inline-flex; align-items: center; gap: 4px; }
      .flow-query-discovery-title code { color: #c7d2fe; background: rgba(129, 140, 248, 0.12); padding: 1px 5px; border-radius: 3px; }
      .flow-query-discovery-groups { display: flex; flex-wrap: wrap; align-items: center; gap: 8px 14px; padding: 8px 10px 10px; border-top: 1px solid #1c202e; }
      .flow-query-discovery-groups > div { display: inline-flex; align-items: baseline; gap: 5px; min-width: 0; }
      .flow-query-discovery-groups > div > span { color: var(--text-muted); font-size: 0.75rem; font-weight: 700; text-transform: uppercase; letter-spacing: 0.04em; white-space: nowrap; }
      .flow-query-discovery-groups > div > div { display: inline-flex; align-items: baseline; flex-wrap: wrap; gap: 3px 5px; min-width: 0; }
      .flow-query-discovery code { color: var(--text-body); font-family: 'JetBrains Mono', monospace; font-size: 0.75rem; overflow-wrap: anywhere; background: rgba(99, 102, 241, 0.1); border: 1px solid rgba(99, 102, 241, 0.22); padding: 1px 6px; border-radius: 4px; }
      .flow-query-discovery-choice { appearance: none; color: var(--text-body); font-family: 'JetBrains Mono', monospace; font-size: 0.75rem; line-height: 1.4; letter-spacing: 0; overflow-wrap: anywhere; background: rgba(99, 102, 241, 0.1); border: 1px solid rgba(99, 102, 241, 0.22); padding: 1px 6px; border-radius: 4px; cursor: pointer; transition: background 0.15s, border-color 0.15s, color 0.15s; }
      .flow-query-discovery-choice:hover, .flow-query-discovery-choice:focus-visible { background: rgba(99, 102, 241, 0.3); border-color: var(--accent); color: #ffffff; outline: none; }
      .flow-query-discovery-empty, .flow-query-discovery-more { color: var(--text-muted); font-style: italic; font-size: 0.75rem; }
      .flow-query-discovery-more::before { content: "... "; }
      .flow-query-discovery-message { padding: 8px 12px; color: var(--text-muted); }
      .flow-query-discovery-value { display: inline-flex; align-items: baseline; gap: 4px; min-width: 0; }
      .flow-query-discovery-value small { color: var(--text-muted); font-size: 0.75rem; }
      .flow-query-copy-status { min-width: 58px; color: var(--text-muted); font-size: 0.75rem; }
      .flow-query-field { display: flex; flex-direction: column; gap: 5px; min-width: 0; }
      .flow-query-field > span:first-child, .flow-query-field > label { font-size: 0.75rem; font-weight: 600; text-transform: uppercase; letter-spacing: 0.04em; color: var(--text-muted); white-space: nowrap; }
      .flow-query-field[hidden], .flow-query-check[hidden], .flow-query-predicate-group[hidden] { display: none !important; }
      .flow-query-field .flow-search-input { width: 100%; height: 36px; background: var(--surface-base); border: 1px solid #68787b; border-radius: 6px; padding: 0 10px; color: var(--text-strong); font-size: 0.8rem; font-family: 'JetBrains Mono', monospace; }
      .flow-query-field .flow-search-input:focus { outline: 2px solid var(--accent); outline-offset: 2px; border-color: var(--accent); box-shadow: none; }
      .flow-field-help { color: var(--text-muted); font-size: 0.75rem; text-transform: none; letter-spacing: 0; line-height: 1.3; }
      .flow-query-predicate-group { display: flex; align-items: flex-end; align-self: stretch; flex: 1 1 360px; gap: 8px; flex-wrap: wrap; min-width: 0; margin: 0; padding: 6px 0 0; border: 0; }
      .flow-query-predicate-group legend { width: 100%; padding: 0; color: var(--text-muted); font-size: 0.75rem; font-weight: 600; text-transform: uppercase; letter-spacing: 0; }
      .flow-query-scalar-input { display: grid; grid-template-columns: minmax(96px, 0.45fr) minmax(120px, 1fr); gap: 6px; min-width: 0; }
      .flow-query-scalar-input select { min-width: 0; height: 36px; background: #08090e; border: 1px solid #262a38; border-radius: 6px; padding: 0 10px; color: var(--text-strong); font-family: 'JetBrains Mono', monospace; font-size: 0.78rem; }
      .flow-query-check { align-self: end; height: 36px; display: flex; align-items: center; }
      .flow-filter-range { flex: 0 0 150px; max-width: 150px; min-width: 150px; }
      .flow-filter-time { flex: 0 0 172px; max-width: 172px; min-width: 172px; }
      .flow-filter-limit { flex: none; width: 78px; max-width: 78px; }
      .flow-filter-clear { color: #38bdf8; text-decoration: none; font-size: 0.78rem; }
      .flow-filter-clear:hover { text-decoration: underline; }
      .flow-filter-note { color: var(--text-muted); font-size: 0.76rem; }
      .dashboard-filter-draft { flex-basis: 100%; grid-column: 1 / -1; margin: 4px 0 0; color: #e5be79; font-size: 0.78rem; }
      .dashboard-filter-draft[hidden] { display: none; }
      .flow-check-label { display: inline-flex; align-items: center; gap: 6px; color: var(--text-muted); font-size: 0.76rem; }
      .flow-check-label input { margin: 0; accent-color: var(--accent-solid); }
      .flow-policy-panel { background: #10121a; border: 1px solid #1e2230; border-radius: 10px; padding: 16px 20px; margin-bottom: 20px; box-shadow: 0 4px 20px rgba(0,0,0,0.3); }
      .flow-policy-panel .section-title { margin-top: 0; }
      .flow-retention-controls { border-block: 1px solid var(--line); padding: 16px 0; margin-bottom: 20px; }
      .flow-retention-controls .section-title { margin-top: 0; }
      .flow-retention-limit { width: 12rem; max-width: 100%; }
      .acl-flash { margin: 0 0 20px; border-left: 3px solid; padding: 12px 14px; font-size: 0.82rem; line-height: 1.45; }
      .acl-flash-ok { border-color: #10b981; background: rgba(16,185,129,0.08); color: #a7f3d0; }
      .acl-flash-error { border-color: #ef4444; background: rgba(239,68,68,0.08); color: #fecaca; }
      .acl-management { background: var(--surface-raised); border: 1px solid var(--line); border-top: 2px solid #10b981; border-radius: 6px; padding: 18px; margin: 0 0 24px; }
      .acl-management-heading { display: flex; justify-content: space-between; align-items: flex-start; gap: 18px; margin-bottom: 18px; }
      .acl-management-heading .section-title { margin: 0 0 5px; }
      .acl-management-heading p { color: var(--text-muted); font-size: 0.78rem; line-height: 1.45; }
      .acl-create-form { display: grid; gap: 16px; }
      .acl-form-grid { display: grid; grid-template-columns: repeat(3, minmax(0,1fr)); gap: 12px; }
      .acl-scope-grid { grid-template-columns: repeat(2, minmax(0,1fr)); }
      .acl-form-grid label, .acl-modifier-field { display: grid; gap: 7px; color: var(--text-body); font-size: 0.75rem; font-weight: 600; }
      .acl-form-grid .flow-search-input { width: 100%; max-width: none; }
      .acl-role-selector { display: grid; grid-template-columns: repeat(3,minmax(0,1fr)); border: 0; gap: 1px; background: #303442; }
      .acl-role-selector legend { padding: 0 0 8px; color: var(--text-body); font-size: 0.75rem; font-weight: 600; }
      .acl-role-selector label { display: flex; align-items: flex-start; gap: 9px; min-width: 0; background: #0d0f15; padding: 12px; cursor: pointer; }
      .acl-role-selector input { margin-top: 2px; accent-color: #10b981; }
      .acl-role-selector span { display: grid; gap: 3px; min-width: 0; }
      .acl-role-selector strong { color: var(--text-strong); font-size: 0.78rem; }
      .acl-role-selector small { color: var(--text-muted); font-size: 0.75rem; line-height: 1.35; }
      .acl-modifier-field span { color: var(--text-muted); font-weight: 400; }
      .acl-modifier-field textarea, .acl-inline-editor textarea { width: 100%; resize: vertical; border: 1px solid #303442; border-radius: 4px; background: var(--surface-base); color: #e2e8f0; padding: 9px 10px; outline: none; }
      .acl-modifier-field textarea:focus, .acl-inline-editor textarea:focus { border-color: var(--accent-solid); box-shadow: 0 0 0 3px rgba(99,102,241,0.12); }
      .acl-form-actions { display: flex; justify-content: flex-end; }
      .acl-primary-button { border-color: #059669; background: #047857; min-width: 150px; }
      .acl-primary-button:hover { background: #059669; }
      .acl-readonly-panel { display: flex; align-items: center; justify-content: space-between; gap: 16px; border: 1px solid var(--line); border-left: 3px solid var(--text-muted); background: var(--surface-raised); padding: 14px 16px; margin-bottom: 20px; }
      .acl-readonly-panel div { display: grid; gap: 4px; }
      .acl-readonly-panel strong { color: #e2e8f0; font-size: 0.82rem; }
      .acl-readonly-panel span { color: var(--text-muted); font-size: 0.75rem; }
      .acl-readonly-panel code { color: #c7d2fe; }
      .acl-row-actions { display: flex; align-items: flex-start; gap: 8px; min-width: 360px; }
      .acl-row-actions form { margin: 0; }
      .acl-text-button, .acl-inline-editor > summary { border: 0; background: transparent; color: var(--accent); font: 500 0.72rem 'Inter',sans-serif; cursor: pointer; white-space: nowrap; }
      .acl-text-button:hover, .acl-inline-editor > summary:hover { color: var(--accent); }
      .acl-delete-button { color: #fca5a5; }
      .acl-action-note { color: var(--text-muted); font-size: 0.75rem; }
      .acl-rule-summary { width: clamp(240px, 42vw, 520px); max-width: 520px; max-height: 10rem; overflow: auto; white-space: normal; overflow-wrap: anywhere; line-height: 1.45; }
      .acl-inline-editor > summary { list-style: none; }
      .acl-inline-editor > summary::-webkit-details-marker { display: none; }
      .acl-inline-editor[open] form { display: grid; gap: 9px; width: 280px; margin-top: 8px; border: 1px solid #303442; background: var(--surface-raised); padding: 14px; }
      .acl-inline-editor label { display: grid; gap: 5px; color: var(--text-muted); font-size: 0.75rem; }
      .acl-inline-editor input { width: 100%; height: 32px; border: 1px solid #303442; border-radius: 4px; background: var(--surface-base); color: var(--text-strong); padding: 0 9px; }
      .acl-inline-editor p { color: var(--text-muted); font-size: 0.75rem; line-height: 1.45; }
      .acl-inline-editor p strong { color: var(--text-strong); }
      .flow-policy-form { display: grid; gap: 12px; }
      .flow-policy-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: 10px; align-items: end; }
      .flow-policy-field { display: grid; gap: 5px; min-width: 0; }
      .flow-policy-field span { color: var(--text-muted); font-size: 0.75rem; text-transform: uppercase; letter-spacing: 0; }
      [data-flow-signal-form] .flow-policy-grid { align-items: start; }
      [data-flow-signal-form] .flow-field-help, [data-flow-signal-form] .flow-field-error { text-transform: none; font-size: 0.75rem; line-height: 1.4; }
      [data-flow-signal-form] .flow-field-error { color: #fca5a5; }
      .flow-policy-actions { display: flex; justify-content: flex-end; }
      #flow-reclaim-form .flow-policy-actions { gap: 12px; flex-wrap: wrap; align-items: center; }
      #flow-reclaim-form .flow-policy-actions label { min-width: 0; }
      #flow-reclaim-form .flow-policy-actions button { flex-shrink: 0; }
      .flow-policy-action { display: inline-flex; align-items: center; justify-content: center; text-decoration: none; }
      .flow-policy-preview { display: grid; min-width: 0; overflow-wrap: anywhere; gap: 5px; color: var(--text-body); background: var(--surface-base); border: 1px solid var(--line); border-radius: 6px; padding: 10px 14px; font-size: 0.8rem; }
      .flow-policy-preview-title { color: #38bdf8; font-size: 0.75rem; text-transform: uppercase; letter-spacing: 0; }
      .flow-alert { border-radius: 6px; padding: 10px 12px; margin-bottom: 14px; font-size: 0.8rem; }
      .flow-alert-ok { background: rgba(16, 185, 129, 0.12); border: 1px solid rgba(16, 185, 129, 0.4); color: #a7f3d0; }
      .flow-alert-error { background: rgba(239, 68, 68, 0.12); border: 1px solid rgba(239, 68, 68, 0.4); color: #fca5a5; }
      .flow-query-metadata { display: grid; grid-template-columns: repeat(auto-fit, minmax(260px, 1fr)); gap: 16px; margin: 18px 0; }
      .flow-query-usage > summary { padding-block: 10px; border-top: 1px solid var(--line); color: var(--text-strong); cursor: pointer; font-size: 0.78rem; font-weight: 600; }
      .flow-query-metadata-group { min-width: 0; border-top: 1px solid var(--line); padding-top: 10px; }
      .flow-query-metadata-title { color: var(--text-strong); font-size: 0.76rem; font-weight: 700; margin-bottom: 8px; }
      .flow-query-metadata-list { display: grid; gap: 6px; }
      .flow-query-metadata-list > div { display: flex; justify-content: space-between; gap: 12px; min-width: 0; }
      .flow-query-metadata-list dt { color: var(--text-muted); font-size: 0.75rem; }
      .flow-query-metadata-list dd { color: var(--text-strong); font-size: 0.75rem; font-family: 'JetBrains Mono', monospace; text-align: right; overflow-wrap: anywhere; }
      .flow-query-page-note { grid-column: 1 / -1; margin: 0; }
      .flow-query-mode-tabs { display: inline-flex; align-items: center; gap: 4px; padding: 4px; border: 1px solid var(--line); border-radius: 8px; background: var(--surface-base); }
      .flow-query-mode-tabs button { min-width: 96px; height: 32px; padding: 0 14px; border: 0; border-radius: 6px; background: transparent; color: var(--text-muted); font-size: 0.78rem; font-weight: 600; cursor: pointer; transition: all 0.15s ease; }
      .flow-query-mode-tabs button:hover { color: var(--text-strong); background: var(--surface-selected); }
      .flow-query-mode-tabs button[aria-selected="true"] { color: #ffffff; background: var(--accent-solid); }
      .flow-query-mode-tabs button:focus-visible { outline: 2px solid var(--accent); outline-offset: 2px; }
      [data-flow-query-mode][hidden] { display: none !important; }
      .flow-query-workbench-form { display: grid; gap: 14px; }
      .flow-query-workbench-form label { display: grid; gap: 6px; color: var(--text-muted); font-size: 0.75rem; font-weight: 600; text-transform: uppercase; letter-spacing: 0.04em; }
      .flow-query-editor, .flow-query-params { width: 100%; min-width: 0; resize: vertical; border: 1px solid #262a38; border-radius: 8px; background: #08090e; color: var(--text-strong); padding: 12px 14px; font-size: 0.82rem; line-height: 1.6; letter-spacing: 0; outline: none; font-family: 'JetBrains Mono', monospace; }
      .flow-query-editor { min-height: 146px; }
      .flow-query-params { min-height: 96px; }
      .flow-query-editor:focus, .flow-query-params:focus { border-color: var(--accent-solid); box-shadow: 0 0 0 3px rgba(99, 102, 241, 0.25); }
      .flow-query-actions { display: flex; align-items: center; justify-content: flex-end; gap: 10px; flex-wrap: wrap; }
      .flow-search-button.secondary { background: #161822; border: 1px solid #292d3e; color: var(--text-body); box-shadow: none; }
      .flow-search-button.secondary:hover { background: #222636; color: var(--text-strong); border-color: #383f58; box-shadow: 0 2px 8px rgba(0, 0, 0, 0.25); }
      .flow-query-table-wrap { width: 100%; max-width: 100%; overflow-x: auto; border-radius: 8px; scrollbar-width: thin; scrollbar-color: #334155 var(--surface-base); }
      .flow-query-table-wrap table { min-width: 100%; }
      .flow-query-projection-table { width: max-content; }
      .flow-query-projection-table th, .flow-query-projection-table td { max-width: 420px; overflow-wrap: anywhere; white-space: normal; }
      .flow-query-pagination { display: flex; justify-content: flex-end; margin-top: 12px; }
      nav.flow-query-pagination { gap: 8px; align-items: center; flex-wrap: wrap; }
      nav.flow-query-pagination > form { margin: 0; }
      .flow-query-explain { display: grid; gap: 18px; }
      .flow-query-plan-section { min-width: 0; padding-top: 14px; border-top: 1px solid var(--line); }
      .flow-query-plan-section:first-child { padding-top: 0; border-top: 0; }
      .flow-query-plan-title { color: var(--text-strong); font-size: 0.8rem; font-weight: 700; margin-bottom: 10px; }
      .flow-query-plan-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(190px, 1fr)); gap: 10px 18px; }
      .flow-query-plan-grid > div { min-width: 0; }
      .flow-query-plan-grid dt { color: var(--text-muted); font-size: 0.75rem; text-transform: uppercase; letter-spacing: 0; }
      .flow-query-plan-grid dd { margin-top: 3px; color: #e2e8f0; overflow-wrap: anywhere; }
      .flow-query-plan-metrics th:first-child { min-width: 170px; }
      .flow-query-plan-metrics td, .flow-query-plan-metrics th { white-space: nowrap; }
      .flow-query-visualization { margin: 0 0 18px; border-top: 1px solid var(--line); border-bottom: 1px solid var(--line); }
      .flow-query-visualization summary { display: list-item; min-height: 44px; padding-block: 12px; color: var(--text-strong); font-size: 0.78rem; font-weight: 700; cursor: pointer; list-style-position: inside; }
      .flow-query-visualization summary > .badge { float: right; margin-left: 12px; }
      .flow-query-chart-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(320px, 1fr)); gap: 24px; padding: 8px 0 20px; }
      .flow-query-chart { min-width: 0; }
      .flow-query-chart-title { color: var(--text-strong); font-size: 0.76rem; font-weight: 700; margin-bottom: 10px; }
      .flow-query-donut-layout { display: grid; grid-template-columns: 132px minmax(0, 1fr); align-items: center; gap: 16px; min-width: 0; }
      .flow-query-donut { display: block; width: 132px; height: 132px; overflow: visible; }
      .flow-query-donut-track { fill: none; stroke: var(--surface-selected); stroke-width: 14; }
      .flow-query-chart-segment { fill: none; stroke: var(--flow-query-chart-color); stroke-width: 14; transform: rotate(-90deg); transform-origin: 60px 60px; }
      .flow-query-donut-total { fill: var(--text-strong); font-size: 1.05rem; font-weight: 700; font-family: 'JetBrains Mono', monospace; }
      .flow-query-donut-caption { fill: var(--text-muted); font-size: 0.75rem; text-transform: uppercase; }
      .flow-query-chart-legend { display: grid; gap: 7px; min-width: 0; list-style: none; }
      .flow-query-chart-legend li { display: grid; grid-template-columns: 9px minmax(60px, 1fr) auto 42px; align-items: center; gap: 7px; min-width: 0; }
      .flow-query-chart-swatch { width: 9px; height: 9px; border-radius: 2px; background: var(--flow-query-chart-color); }
      .flow-query-chart-label { color: var(--text-body); font-size: 0.75rem; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
      .flow-query-chart-legend strong { color: var(--text-strong); font-size: 0.75rem; text-align: right; }
      .flow-query-chart-percent { color: var(--text-muted); font-size: 0.75rem; text-align: right; }
      .flow-query-chart-time { grid-column: 1 / -1; }
      .flow-query-time-chart { display: block; width: 100%; height: 190px; }
      .flow-query-time-grid { stroke: var(--line); stroke-width: 1; }
      .flow-query-time-tick { fill: var(--text-muted); font-size: 0.75rem; font-family: 'JetBrains Mono', monospace; }
      .flow-query-time-bar { fill: var(--flow-query-chart-color); shape-rendering: geometricPrecision; }
      .flow-query-time-range { display: flex; justify-content: space-between; gap: 16px; color: var(--text-muted); font-size: 0.75rem; }
      .flow-query-chart-color-0 { --flow-query-chart-color: #2dd4bf; }
      .flow-query-chart-color-1 { --flow-query-chart-color: #fbbf24; }
      .flow-query-chart-color-2 { --flow-query-chart-color: #fb7185; }
      .flow-query-chart-color-3 { --flow-query-chart-color: #60a5fa; }
      .flow-query-chart-color-4 { --flow-query-chart-color: #a78bfa; }
      .flow-query-chart-color-5 { --flow-query-chart-color: #4ade80; }
      .flow-query-chart-color-6 { --flow-query-chart-color: #fb923c; }
      .flow-query-chart-color-7 { --flow-query-chart-color: #22d3ee; }
      .flow-query-chart-color-8 { --flow-query-chart-color: #f472b6; }
      .flow-query-chart-color-9 { --flow-query-chart-color: #a3e635; }
      .flow-bars { display: grid; gap: 8px; background: var(--surface-raised); border: 1px solid var(--line); border-radius: 8px; padding: 14px; margin-bottom: 20px; }
      .flow-bar-row { display: grid; grid-template-columns: 92px 1fr 64px; align-items: center; gap: 10px; color: var(--text-muted); font-size: 0.78rem; }
      .flow-bar-track { height: 10px; background: var(--surface-base); border: 1px solid var(--line); border-radius: 999px; overflow: hidden; }
      .flow-bar-track span { display: block; min-width: 2px; height: 100%; border-radius: 999px; }
      .flow-bar-track .status-good { background: #10b981; }
      .flow-bar-track .status-warn { background: #f59e0b; }
      .flow-bar-track .status-bad { background: #ef4444; }
      .flow-issue-row { display: flex; gap: 12px; flex-wrap: wrap; margin-bottom: 20px; }
      .flow-issue { display: flex; align-items: center; gap: 8px; background: var(--surface-raised); border: 1px solid var(--line); border-radius: 8px; padding: 10px 14px; font-size: 0.82rem; color: var(--text-body); }
      .flow-pill { display: inline-block; background: var(--surface-selected); color: var(--text-muted); border: 1px solid var(--line); border-radius: 999px; padding: 2px 8px; font-size: 0.75rem; margin: 1px 2px 1px 0; white-space: nowrap; }
      .flow-pill.flow-value-ref-link { color: var(--text-strong); }
      .flow-link { color: #38bdf8; text-decoration: underline; text-underline-offset: 3px; }
      .flow-link:hover { text-decoration: underline; }
      .chart-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 16px; margin-bottom: 20px; }
      .flow-due-summary { padding: 12px 0; margin-bottom: 16px; border-block: 1px solid var(--line); }
      .flow-due-summary .section-title { margin: 0 0 8px; }
      .flow-due-summary .flow-section-note { margin: 0 0 10px; }
      .chart-card { background: var(--surface-raised); border: 1px solid var(--line); border-radius: 8px; padding: 16px; min-height: 220px; }
      .chart-title { font-size: 0.75rem; color: var(--text-muted); text-transform: uppercase; letter-spacing: 0; margin-bottom: 12px; }
      .chart-card canvas { width: 100% !important; max-height: 220px; }
      .chart-empty { color: var(--text-muted); padding: 28px 0; text-align: center; }
      .chart-bars { display: grid; gap: 14px; }
      .chart-row { display: grid; grid-template-columns: minmax(120px, 220px) 1fr; gap: 14px; align-items: start; }
      .chart-row-label { color: var(--text-strong); font-weight: 600; overflow-wrap: anywhere; }
      .chart-row-bars { display: grid; gap: 6px; }
      .chart-bar-line { display: grid; grid-template-columns: 76px 1fr 64px; gap: 10px; align-items: center; }
      .chart-bar-label { color: var(--text-muted); font-size: 0.78rem; }
      .chart-bar-value { color: var(--text-strong); font-size: 0.78rem; text-align: right; font-family: 'JetBrains Mono', monospace; }
      .chart-bar-track { height: 10px; border-radius: 999px; background: var(--surface-base); overflow: hidden; border: 1px solid var(--line); }
      .chart-bar-fill { display: block; height: 100%; border-radius: 999px; }
      .flow-state-pressure-region { margin-bottom: 20px; border: 1px solid var(--line); border-radius: 6px; background: var(--surface-raised); }
      .flow-state-pressure-matrix { min-width: 760px; }
      .flow-state-pressure-matrix th, .flow-state-pressure-matrix td { padding: 9px 12px; }
      .flow-state-pressure-label { max-width: 260px; color: var(--text-strong); font-size: 0.76rem; font-weight: 600; text-align: left; }
      .flow-state-pressure-label span { display: block; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
      .flow-state-pressure-count, .flow-state-pressure-value { color: #dbe4f0; font-family: 'JetBrains Mono', monospace; font-size: 0.75rem; font-variant-numeric: tabular-nums; }
      .flow-state-pressure-cell { min-width: 112px; white-space: nowrap; }
      .flow-state-pressure-track { display: inline-block; width: 62px; height: 7px; margin-right: 8px; border: 1px solid var(--line); border-radius: 999px; background: var(--surface-base); overflow: hidden; vertical-align: middle; }
      .flow-state-pressure-fill { display: block; height: 100%; border-radius: 999px; }
      .flow-state-pressure-value { display: inline-block; min-width: 32px; text-align: right; vertical-align: middle; }
      .flow-fifo-table { width: 100%; table-layout: fixed; min-width: 850px; }
      .flow-fifo-table th:nth-child(1) { width: 23%; }
      .flow-fifo-table th:nth-child(2) { width: 29%; }
      .flow-fifo-table th:nth-child(3) { width: 12%; }
      .flow-fifo-table th:nth-child(4) { width: 36%; }
      .flow-fifo-table td, .flow-fifo-table th { vertical-align: top; white-space: normal; overflow-wrap: anywhere; }
      .flow-fifo-table .flow-fifo-lane { text-align: left; font-weight: 400; text-transform: none; }
      .flow-fifo-lane > * { display: block; margin-bottom: 5px; }
      .flow-fifo-lane > a { font-size: 0.76rem; color: var(--text-body); text-decoration: underline; }
      .flow-fifo-activity > div { margin-top: 7px; font-size: 0.76rem; }
      .flow-fifo-counts { line-height: 1.8; font-variant-numeric: tabular-nums; }
      .flow-fifo-members > summary { cursor: pointer; color: var(--text-strong); }
      .flow-fifo-members .flow-section-note { margin: 10px 0; }
      .flow-fifo-member-list { list-style: none; margin: 0; padding: 0; }
      .flow-fifo-member-list > li { padding: 10px 0; border-top: 1px solid var(--line); }
      .flow-fifo-member-heading { display: flex; flex-wrap: wrap; align-items: baseline; gap: 6px 12px; }
      .flow-fifo-member-heading > a { flex: 1; min-width: 100px; }
      .flow-fifo-member-sequence, .flow-fifo-member-timing { display: block; color: var(--text-muted); font-size: 0.75rem; margin-top: 5px; }
      .bar-green { background: #10b981; }
      .bar-yellow { background: #f59e0b; }
      .bar-red { background: #ef4444; }
      .bar-blue { background: #3b82f6; }
      .flow-timeline-caption { color: var(--text-muted); font-size: 0.75rem; }
      .flow-timing-section { margin-bottom: 20px; }
      .flow-timing-section > .flow-timeline-caption { margin-top: 8px; }
      .flow-step-waterfall { display: grid; gap: 8px; min-width: 0; }
      .flow-step-waterfall-scroll { overflow-x: auto; border: 1px solid var(--line); border-radius: 8px; background: var(--surface-base); }
      .flow-step-waterfall-header, .flow-step-waterfall-row { min-width: 820px; display: grid; grid-template-columns: minmax(180px, 240px) minmax(420px, 1fr) 92px; gap: 12px; align-items: center; }
      .flow-step-waterfall-header { min-height: 42px; padding: 0 12px; border-bottom: 1px solid var(--line); color: var(--text-muted); font-size: 0.75rem; font-weight: 700; text-transform: uppercase; letter-spacing: 0; }
      .flow-step-waterfall-row { min-height: 50px; padding: 8px 12px; border-bottom: 1px solid rgba(34, 37, 48, 0.72); color: var(--text-strong); text-decoration: none; transition: background 0.15s; }
      .flow-step-waterfall-row:last-child { border-bottom: none; }
      .flow-step-waterfall-row:hover, .flow-step-waterfall-row:focus { background: var(--accent-wash); }
      .flow-step-waterfall-label { display: grid; gap: 3px; min-width: 0; }
      .flow-step-waterfall-step { color: var(--text-strong); font-weight: 700; overflow-wrap: anywhere; }
      .flow-step-waterfall-state { color: var(--text-muted); font-size: 0.75rem; overflow-wrap: anywhere; }
      .flow-step-waterfall-axis, .flow-step-waterfall-track { position: relative; min-width: 0; }
      .flow-step-waterfall-axis { align-self: stretch; }
      .flow-step-waterfall-axis-label { position: absolute; top: 50%; transform: translate(-50%, -50%); color: var(--text-muted); font-size: 0.75rem; font-variant-numeric: tabular-nums; white-space: nowrap; font-family: 'JetBrains Mono', monospace; }
      .flow-step-waterfall-axis-label[data-axis-edge="start"] { transform: translate(0, -50%); }
      .flow-step-waterfall-axis-label[data-axis-edge="end"] { transform: translate(-100%, -50%); }
      .flow-step-waterfall-track { height: 28px; border: 1px solid var(--line); border-radius: 6px; overflow: hidden; background: linear-gradient(to right, rgba(34, 37, 48, 0.84) 1px, transparent 1px) 0 0 / 25% 100%, #030712; }
      .flow-step-waterfall-bar { position: absolute; top: 6px; bottom: 6px; min-width: 4px; border-radius: 4px; opacity: 0.92; box-shadow: 0 0 0 1px rgba(248, 250, 252, 0.16); }
      .flow-step-waterfall-bar.bar-green { background: #10b981; }
      .flow-step-waterfall-bar.bar-yellow { background: #f59e0b; }
      .flow-step-waterfall-bar.bar-red { background: #ef4444; }
      .flow-step-waterfall-bar.bar-blue { background: #3b82f6; }
      .flow-step-waterfall-marker { position: absolute; top: 4px; bottom: 4px; width: 2px; transform: translateX(-1px); background: rgba(248, 250, 252, 0.72); z-index: 1; }
      .flow-step-waterfall-duration { display: grid; gap: 3px; justify-items: end; color: var(--text-strong); font-size: 0.76rem; font-variant-numeric: tabular-nums; font-family: 'JetBrains Mono', monospace; }
      .flow-step-waterfall-duration span + span { color: var(--text-muted); font-size: 0.75rem; }
      .timeline-event-row:target { outline: 2px solid var(--accent-solid); outline-offset: -2px; background: var(--accent-wash); }
      .journal-step:target { outline: 2px solid var(--accent-solid); outline-offset: 3px; background: var(--accent-wash); }
      .flow-history-controls { display: flex; justify-content: space-between; align-items: center; gap: 12px; margin: 8px 0 12px; flex-wrap: wrap; }
      .flow-history-pages, .flow-history-counts { display: flex; align-items: center; gap: 8px; flex-wrap: wrap; }
      .flow-history-page-link, .flow-history-count { display: inline-flex; align-items: center; justify-content: center; min-height: 28px; border: 1px solid var(--line); border-radius: 6px; padding: 0 12px; color: var(--text-strong); background: var(--surface-selected); text-decoration: none; font-size: 0.76rem; transition: background 0.15s; }
      .flow-history-page-link:hover, .flow-history-page-link:focus, .flow-history-count:hover, .flow-history-count:focus { border-color: var(--accent-solid); color: var(--text-strong); background: var(--surface-raised); }
      .flow-history-page-disabled { color: var(--text-muted); background: var(--surface-base); cursor: default; }
      .flow-history-count-active { border-color: var(--accent-solid); color: var(--accent); background: var(--accent-wash); }
      .flow-value-row:target { outline: 2px solid var(--accent-solid); outline-offset: -2px; background: var(--accent-wash); }
      .flow-event-link { color: #38bdf8; text-decoration: none; }
      .flow-event-link:hover, .flow-event-link:focus { text-decoration: underline; }
      .flow-value-ref-link { color: var(--text-strong); text-decoration: none; }
      .flow-value-ref-link:hover, .flow-value-ref-link:focus { border-color: var(--accent-solid); color: var(--text-strong); }
      .flow-value-preview { margin: 0; max-width: 520px; max-height: 220px; overflow: auto; white-space: pre-wrap; overflow-wrap: anywhere; color: var(--text-body); font-size: 0.76rem; line-height: 1.45; font-family: 'JetBrains Mono', monospace; }
      .flow-value-modal[hidden] { display: none; }
      .flow-value-modal [hidden] { display: none !important; }
      .flow-value-modal { position: fixed; inset: 0; width: 100%; height: 100%; max-width: none; max-height: none; margin: 0; border: 0; background: transparent; color: inherit; display: grid; place-items: center; padding: 24px; }
      .flow-value-modal-backdrop { position: absolute; inset: 0; background: rgba(3, 7, 18, 0.8); }
      .flow-value-modal-panel { position: relative; width: min(920px, 100%); max-height: min(760px, calc(100vh - 48px)); display: flex; flex-direction: column; gap: 12px; background: var(--surface-raised); border: 1px solid var(--line); border-radius: 8px; box-shadow: 0 20px 80px rgba(0, 0, 0, 0.6); padding: 18px; }
      .flow-value-modal-header { display: flex; align-items: flex-start; justify-content: space-between; gap: 16px; }
      .flow-value-modal-header .section-title { margin-bottom: 4px; }
      .flow-value-modal-ref { color: var(--text-muted); font-size: 0.76rem; overflow-wrap: anywhere; font-family: 'JetBrains Mono', monospace; }
      .flow-value-modal-close { height: 32px; border: 1px solid var(--line); background: var(--surface-selected); color: var(--text-strong); border-radius: 6px; padding: 0 14px; font-size: 0.78rem; cursor: pointer; transition: background 0.15s; }
      .flow-value-modal-close:hover { background: #334155; }
      .flow-value-modal-body { flex: 1; min-height: 220px; max-height: 560px; overflow: auto; margin: 0; padding: 14px; border: 1px solid var(--line); border-radius: 6px; background: var(--surface-base); color: var(--text-body); white-space: pre-wrap; overflow-wrap: anywhere; font-size: 0.8rem; line-height: 1.45; font-family: 'JetBrains Mono', monospace; }
      .flow-value-modal-actions { display: flex; align-items: center; gap: 10px; }
      .flow-value-modal-actions button:disabled { opacity: 0.5; cursor: not-allowed; }
      .flow-section-note { color: var(--text-muted); font-size: 0.78rem; margin: -4px 0 10px; }
      .flow-lineage-map { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 10px; margin-bottom: 16px; }
      .flow-lineage-lookup { padding: 0 0 16px; margin-bottom: 16px; border-bottom: 1px solid var(--line); }
      .flow-lineage-filter-form { display: grid; grid-template-columns: minmax(110px, 0.6fr) minmax(260px, 2fr) minmax(180px, 1fr) 76px auto; align-items: end; gap: 12px; }
      .flow-lineage-filter-form label { display: grid; gap: 6px; min-width: 0; }
      .flow-lineage-filter-form .flow-search-input { width: 100%; min-width: 0; }
      .flow-lineage-hint-links { display: flex; flex-wrap: wrap; gap: 8px; padding: 8px 0; }
      .flow-lineage-preview-count { margin-top: 12px; }
      .flow-lineage-page .flow-query-metadata-list { grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: 12px; }
      .flow-lineage-page .flow-query-metadata-list > div { display: block; }
      .flow-lineage-page .flow-query-metadata-list dd { text-align: left; margin-top: 4px; }
      .flow-recovery-query-quality { display: grid; grid-template-columns: repeat(auto-fit, minmax(260px, 1fr)); gap: 24px; margin: 16px 0; }
      .flow-recovery-query-quality .section-title { margin: 0 0 8px; }
      .flow-recovery-query-quality .flow-query-metadata { margin: 0; }
      .flow-recovery-query-quality .flow-query-metadata-group { border: 0; padding: 0; }
      .flow-recovery-query-quality .flow-query-metadata-title { display: none; }
      .flow-lineage-node-meta { overflow-wrap: anywhere; }
      @media (max-width: 1100px) { .flow-lineage-filter-form { grid-template-columns: minmax(100px, 1fr) minmax(220px, 2fr); } .flow-lineage-filter-form .flow-search-button { justify-self: start; } }
      .flow-lineage-node { display: grid; gap: 4px; min-width: 0; padding: 12px 14px; border: 1px solid var(--line); border-radius: 8px; background: var(--surface-raised); color: var(--text-strong); text-decoration: none; transition: border-color 0.15s, background 0.15s; }
      .flow-lineage-node:hover, .flow-lineage-node:focus { border-color: var(--accent-solid); background: var(--accent-wash); }
      .flow-lineage-node-id { font-family: 'JetBrains Mono', monospace; overflow-wrap: anywhere; }
      .flow-lineage-node-meta { color: var(--text-muted); font-size: 0.76rem; }
      .flow-lineage-empty { color: var(--text-muted); background: var(--surface-raised); border: 1px solid var(--line); border-radius: 8px; padding: 14px 16px; }

      /* Separated table with rounded corners (Bugfix) */
      table { width: 100%; border-collapse: separate; border-spacing: 0; background: var(--surface-raised); border: 1px solid var(--line); border-radius: 8px; overflow: hidden; font-size: 0.82rem; }
      th { background: var(--surface-selected); color: var(--text-muted); font-size: 0.75rem; font-weight: 600; text-transform: uppercase; letter-spacing: 0; padding: 10px 14px; text-align: left; border-bottom: 1px solid var(--line); }
      td { padding: 10px 14px; border-bottom: 1px solid var(--surface-selected); }
      tr:last-child td { border-bottom: none; }
      tr:hover td { background: var(--surface-selected); }
      .table-scroll { max-width: 100%; overflow-x: auto; border-radius: 8px; -webkit-overflow-scrolling: touch; scrollbar-width: thin; scrollbar-color: #334155 var(--surface-base); }
      .table-scroll:focus-visible { outline: 2px solid var(--accent); outline-offset: 3px; }
      .table-scroll::-webkit-scrollbar { height: 8px; }
      .table-scroll::-webkit-scrollbar-track { background: var(--surface-base); border-radius: 8px; }
      .table-scroll::-webkit-scrollbar-thumb { background: #334155; border-radius: 8px; }
      .table-scroll table { min-width: 100%; overflow: visible; }
      .flow-runs-table { table-layout: fixed; min-width: 960px !important; font-variant-numeric: tabular-nums; }
      .flow-runs-table th:nth-child(1) { width: 26%; }
      .flow-runs-table th:nth-child(2) { width: 17%; }
      .flow-runs-table th:nth-child(3) { width: 20%; }
      .flow-runs-table th:nth-child(4) { width: 20%; }
      .flow-runs-table th:nth-child(5) { width: 8%; }
      .flow-runs-table th:nth-child(6) { width: 9%; }
      .flow-runs-table td { vertical-align: top; padding: 12px; }
      .flow-worker-records-table { table-layout: fixed; font-variant-numeric: tabular-nums; }
      .flow-worker-records-table th:nth-child(1) { width: 36%; }
      .flow-worker-records-table th:nth-child(2) { width: 24%; }
      .flow-worker-records-table th:nth-child(3), .flow-worker-records-table th:nth-child(4) { width: 20%; }
      .flow-worker-records-table td { vertical-align: top; overflow-wrap: anywhere; }
      .flow-worker-identity .flow-run-secondary { white-space: normal; overflow-wrap: anywhere; }
      .flow-worker-lease-details { margin-top: 8px; font-size: 0.75rem; }
      .flow-worker-lease-details > summary { cursor: pointer; color: var(--accent); }
      .flow-worker-lease-details dl { margin-top: 10px; }
      .flow-worker-lease-details dt { color: var(--text-muted); margin-top: 8px; }
      .flow-worker-lease-details dd { margin: 3px 0 0; font-family: 'JetBrains Mono', monospace; overflow-wrap: anywhere; }
      .flow-run-identity .flow-link { display: block; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
      .flow-run-secondary { display: block; margin-top: 4px; color: var(--text-muted); font-size: 0.75rem; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
      .flow-run-step { display: block; margin-bottom: 6px; overflow-wrap: break-word; }
      .flow-run-reason { display: block; overflow-wrap: anywhere; line-height: 1.45; }
      .flow-run-timing > span { display: block; white-space: nowrap; font-size: 0.75rem; margin-bottom: 6px; }
      .flow-run-timing .c-muted { display: block; font-size: 0.75rem; }
      .flow-row-actions > * { display: block; margin-bottom: 8px; }
      .table-scroll th { position: sticky; top: 0; z-index: 1; }

      /* Status colors */
      .c-green { color: #10b981; }
      .c-yellow { color: #f59e0b; }
      .c-red { color: #ef4444; }
      .c-muted { color: var(--text-muted); }

      /* Badges */
      .badge { display: inline-block; padding: 2px 8px; border-radius: 12px; font-size: 0.75rem; font-weight: 600; }
      .badge-ok { background: rgba(16, 185, 129, 0.12); color: #34d399; border: 1px solid rgba(16, 185, 129, 0.3); }
      .badge-warning { background: rgba(245, 158, 11, 0.12); color: #fbbf24; border: 1px solid rgba(245, 158, 11, 0.3); }
      .badge-pressure { background: rgba(239, 68, 68, 0.12); color: #f87171; border: 1px solid rgba(239, 68, 68, 0.3); }
      .badge-reject { background: rgba(220, 38, 38, 0.2); color: #fca5a5; border: 1px solid rgba(220, 38, 38, 0.4); }
      .badge-merging { background: var(--accent-wash); color: var(--accent); border: 1px solid rgba(99, 102, 241, 0.3); }
      .badge-idle { background: var(--surface-selected); color: var(--text-muted); border: 1px solid var(--line); }

      /* Memory pressure alert */
      .pressure-alert { background: var(--surface-raised); border: 1px solid var(--line); border-radius: 8px; padding: 16px 20px; margin-bottom: 16px; }
      .pressure-alert.level-warning { border-color: #f59e0b; }
      .pressure-alert.level-pressure { border-color: #ef4444; }
      .pressure-alert.level-reject { border-color: #dc2626; border-width: 2px; }
      .pressure-header { display: flex; align-items: center; gap: 8px; margin-bottom: 8px; }
      .pressure-bar-wrap { width: 100%; height: 8px; background: var(--surface-selected); border-radius: 4px; overflow: hidden; margin: 8px 0; }
      .pressure-bar-fill { height: 100%; border-radius: 4px; }
      .pressure-details { font-size: 0.8rem; color: var(--text-muted); }
      .pressure-details span { color: var(--text-strong); font-weight: 600; }
      .pressure-action { font-size: 0.75rem; color: #fbbf24; margin-top: 6px; font-style: italic; }

      /* Connections inline */
      .conn-row { display: flex; gap: 24px; align-items: center; background: var(--surface-raised); border: 1px solid var(--line); border-radius: 6px; padding: 12px 18px; font-size: 0.85rem; flex-wrap: wrap; }
      .conn-item .conn-label { font-size: 0.75rem; color: var(--text-muted); text-transform: uppercase; letter-spacing: 0; }
      .conn-item .conn-val { font-weight: 700; color: var(--text-strong); font-family: 'JetBrains Mono', monospace; }

      /* Sidebar */
      .layout { display: flex; min-height: calc(100vh - 54px); }
      .sidebar { width: 220px; flex-shrink: 0; background: var(--surface-raised); border-right: 1px solid var(--line); padding: 16px 0; position: sticky; top: 0; height: 100vh; overflow-y: auto; }
      body:has(> header .top-bar) .sidebar { height: calc(100vh - 54px); }
      .sidebar a { display: flex; align-items: center; gap: 8px; padding: 10px 20px; text-decoration: none; color: var(--text-body); font-size: 0.82rem; transition: background 0.15s, border-left-color 0.15s, color 0.15s; border-left: 3px solid transparent; }
      .sidebar a:hover { background: var(--surface-selected); color: var(--text-strong); }
      .sidebar a.active { background: var(--accent-wash); border-left-color: var(--accent-solid); color: var(--accent); font-weight: 600; }
      .sidebar a.nav-subitem { padding-left: 32px; font-size: 0.78rem; color: var(--text-muted); }
      .sidebar a.nav-subitem.active { color: var(--accent); }
      .nav-subgroup + .nav-subgroup { border-top: 1px solid rgba(41, 45, 56, 0.72); margin-top: 5px; padding-top: 5px; }
      .nav-subgroup-label { padding: 7px 20px 4px; color: var(--text-muted); font-size: 0.75rem; font-weight: 700; text-transform: uppercase; }
      .sidebar .nav-label { flex: 1; }
      .sidebar .nav-badge { font-size: 0.75rem; color: var(--text-muted); background: var(--surface-selected); padding: 2px 6px; border-radius: 8px; white-space: nowrap; border: 1px solid var(--line); }
      .nav-group { border-bottom: 1px solid rgba(34, 37, 48, 0.65); }
      .nav-group > summary { cursor: pointer; list-style: none; font-size: 0.75rem; color: var(--text-muted); text-transform: uppercase; padding: 13px 20px 9px; user-select: none; }
      .nav-group > summary::-webkit-details-marker { display: none; }
      .nav-group > summary::after { content: "+"; float: right; color: var(--text-muted); }
      .nav-group[open] > summary::after { content: "−"; }
      .nav-group > summary:hover, .nav-group > summary:focus { color: var(--text-strong); background: #171922; }
      .nav-group-links { padding-bottom: 6px; }
      .sidebar-session { margin: 18px 16px 0; border-top: 1px solid var(--line); padding: 14px 4px 0; display: flex; align-items: center; justify-content: space-between; gap: 10px; }
      .sidebar-session span { color: var(--text-muted); font-size: 0.75rem; }
      .sidebar-session button { border: 0; background: transparent; color: var(--accent); font-size: 0.75rem; cursor: pointer; }
      .sidebar-session button:hover { color: var(--accent); }
      .main-content { flex: 1; min-width: 0; }

      /* Sub-page header */
      .subpage-header { display: flex; align-items: center; gap: 16px; padding: 14px 24px; background: var(--surface-base); border-bottom: 1px solid var(--line); min-height: 62px; flex-wrap: wrap; }
      .subpage-title { font-size: 1rem; font-weight: 600; color: var(--text-strong); letter-spacing: 0; }
      .dashboard-brand { color: var(--text-strong); font-size: 1rem; font-weight: 700; text-decoration: none; padding-right: 16px; border-right: 1px solid var(--line); }
      .dashboard-instance { max-width: 240px; color: var(--text-muted); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; margin-left: auto; }
      .dashboard-live-status { margin-left: auto; display: inline-flex; align-items: center; gap: 7px; min-height: 28px; color: var(--text-muted); font-size: 0.75rem; flex-wrap: wrap; }
      .dashboard-live-status button { min-height: 28px; padding: 3px 8px; border: 1px solid var(--line-strong); border-radius: 4px; background: var(--surface-selected); color: var(--text-strong); cursor: pointer; }
      .dashboard-live-status [data-dashboard-live-age] { font-variant-numeric: tabular-nums; }
      .dashboard-live-dot { width: 7px; height: 7px; border-radius: 50%; background: #f59e0b; }
      .dashboard-live-status[data-dashboard-live-status="live"] .dashboard-live-dot { background: #10b981; }
      .dashboard-live-status[data-dashboard-live-status="paused"] .dashboard-live-dot { background: #f59e0b; }
      .dashboard-live-status[data-dashboard-live-status="stale"] .dashboard-live-dot,
      .dashboard-live-status[data-dashboard-live-status="expired"] .dashboard-live-dot { background: #ef4444; }
      .dashboard-live-status button { border: 1px solid var(--line-strong); border-radius: 5px; background: #171922; color: #e2e8f0; padding: 4px 8px; cursor: pointer; font: inherit; }
      .dashboard-live-status button:hover { border-color: var(--accent); color: var(--text-strong); }
      .dashboard-live-status button:disabled { opacity: 0.55; cursor: wait; }
      .flow-action-confirm { position: relative; display: inline-block; vertical-align: middle; }
      .flow-action-confirm > summary { list-style: none; cursor: pointer; }
      .flow-action-confirm > summary::-webkit-details-marker { display: none; }
      .flow-action-confirm-panel { position: static; display: grid; gap: 8px; min-width: 220px; max-width: 320px; margin-top: 6px; padding: 12px; border: 1px solid var(--line-strong); border-radius: 6px; background: var(--surface-raised); white-space: normal; }
      .flow-action-confirm-panel span { color: var(--text-muted); overflow-wrap: anywhere; }
      .flow-action-confirm-panel form { display: grid; gap: 10px; min-width: 0; }
      .flow-action-confirm-panel .flow-search-input { min-width: 0; max-width: 100%; width: 100%; }
      .flow-action-confirm-panel .flow-check-label { align-items: start; overflow-wrap: anywhere; }
      .flow-action-confirm-panel .flow-check-label input { flex-shrink: 0; margin-top: 2px; }
      .flow-operations-panel, .dashboard-disclosure { margin: 18px 0; border-block: 1px solid var(--line); background: transparent; }
      .flow-operations-panel > summary, .dashboard-disclosure > summary { display: list-item; list-style-position: inside; cursor: pointer; padding: 12px 16px; color: var(--text-strong); font-weight: 600; }
      .flow-operations-panel > summary > .c-muted { float: right; margin-left: 16px; }
      .flow-operations-panel > summary > [data-flow-action-stale-summary] { margin-left: 12px; }
      .flow-operations-panel > summary::after, .dashboard-disclosure > summary::after, .flow-query-visualization > summary::after { content: ""; display: block; clear: both; }
      .flow-operations-panel > summary::marker, .dashboard-disclosure > summary::marker, .flow-query-visualization > summary::marker { color: var(--accent); }
      .flow-operations-panel-body, .dashboard-disclosure-body { padding: 0 16px 16px; border-top: 1px solid var(--line); }
      .operator-attention { margin-bottom: 20px; }
      .operator-attention-list { display: grid; gap: 8px; }
      .operator-attention-item, .operator-attention-clear { display: flex; align-items: center; justify-content: space-between; gap: 18px; padding: 10px 14px; border: 1px solid var(--line); border-radius: 6px; background: var(--surface-raised); }
      .operator-attention-clear { justify-content: flex-start; }
      .operator-attention-item > div { display: grid; gap: 3px; min-width: 0; }
      .operator-attention-item span, .operator-attention-clear { color: var(--text-muted); font-size: 0.78rem; }
      .operator-attention-item a { white-space: nowrap; color: var(--accent); font-size: 0.78rem; }
      .operator-attention-warning { border-left: 3px solid #f59e0b; }
      .operator-attention-degraded { border-left: 3px solid #ef4444; }

      /* Footer */
      .footer { position: static; background: var(--surface-base); border-top: 1px solid var(--line); padding: 8px 24px; font-size: 0.75rem; color: var(--text-muted); display: flex; justify-content: space-between; flex-wrap: wrap; gap: 8px; }

      /* Tooltip */
      .info-icon { position: relative; display: inline-block; width: 18px; height: 18px; padding: 0; border: 1px solid var(--line-strong); border-radius: 50%; background: var(--surface-selected); color: var(--text-body); font-size: 11px; text-align: center; line-height: 16px; cursor: help; margin-left: 4px; vertical-align: middle; }
      .info-icon:hover,
      .info-icon:focus { background: var(--accent-solid); color: var(--text-strong); }
      .dashboard-tooltip { position: fixed; inset: auto; margin: 0; z-index: 1000; width: max-content; max-width: min(320px, calc(100vw - 16px)); max-height: calc(100vh - 16px); overflow: auto; padding: 10px 12px; border-radius: 4px; border: 1px solid var(--line-strong); background: var(--surface-raised); color: var(--text-body); box-shadow: 0 8px 24px rgba(0,0,0,0.35); font-size: 0.78rem; line-height: 1.45; font-weight: 400; white-space: normal; text-align: left; }
      .dashboard-tooltip[hidden] { display: none; }

      .mono { font-family: 'JetBrains Mono', Consolas, monospace; font-size: 0.82rem; }

      /* Sampling indicator */
      .sampled-tag { display: inline-block; font-size: 0.75rem; color: var(--text-muted); background: var(--surface-selected); padding: 1px 5px; border-radius: 3px; vertical-align: middle; font-weight: 400; letter-spacing: 0; text-transform: none; cursor: help; border: 1px solid var(--line); }

      /* Flow Detail Master-Detail Canvas */
      .flow-canvas { display: grid; grid-template-columns: minmax(360px, 1.15fr) minmax(420px, 1.25fr); gap: 20px; align-items: start; margin-bottom: 24px; }
      .flow-canvas-full { grid-template-columns: 1fr; }
      .flow-journal-card, .flow-inspector-card { background: transparent; border-block: 1px solid var(--line); margin-bottom: 20px; }
      .flow-card-header { display: flex; align-items: center; justify-content: space-between; padding: 12px 18px; border-bottom: 1px solid var(--line); background: var(--surface-raised); gap: 10px; flex-wrap: wrap; }
      .flow-card-header-title { font-size: 0.84rem; font-weight: 700; color: var(--text-strong); display: flex; align-items: center; gap: 8px; }

      /* Diagnostic Hero Banner */
      .flow-diagnostic-hero { padding: 8px 0; margin-bottom: 12px; display: flex; align-items: center; justify-content: space-between; gap: 20px; flex-wrap: wrap; }
      .hero-failed .flow-hero-title, .hero-blocked .flow-hero-title { color: #fca5a5; }
      .flow-hero-main { display: flex; align-items: center; gap: 16px; min-width: 0; flex: 1; }
      .flow-hero-icon { width: 42px; height: 42px; border-radius: 8px; display: flex; align-items: center; justify-content: center; font-size: 1.25rem; font-weight: 700; flex-shrink: 0; background: #1a1c26; border: 1px solid var(--line); }
      .flow-hero-info { display: grid; gap: 4px; min-width: 0; }
      .flow-hero-title { font-size: 1.05rem; font-weight: 700; color: var(--text-strong); display: flex; align-items: center; gap: 10px; flex-wrap: wrap; }
      .flow-hero-subtitle { color: var(--text-muted); font-size: 0.8rem; line-height: 1.4; }
      .flow-hero-actions { display: flex; align-items: center; gap: 10px; flex-wrap: wrap; }

      /* Interactive Step Journal */
      .flow-journal-workspace { display: grid; grid-template-columns: minmax(0, 1fr); align-items: start; }
      .flow-journal-workspace.has-selected-event { grid-template-columns: minmax(0, 1.65fr) minmax(270px, 1fr); }
      .flow-journal-inspector[hidden] { display: none; }
      .flow-journal-tree { padding: 20px 20px 20px 0; display: grid; gap: 0; position: relative; min-width: 0; }
      .flow-journal-inspector { padding: 20px; border-left: 1px solid var(--line); min-width: 0; position: sticky; top: 12px; max-height: calc(100vh - 36px); overflow: auto; font-size: 0.82rem; }
      .journal-step { position: relative; padding-left: 32px; padding-bottom: 18px; transition: all 0.15s ease; outline: none; }
      .journal-step:last-child { padding-bottom: 4px; }
      .journal-step::before { content: ""; position: absolute; left: 11px; top: 22px; bottom: -2px; width: 2px; background: var(--line); transition: background 0.15s; }
      .journal-step:last-child::before { display: none; }
      .journal-step-node { position: absolute; left: 3px; top: 4px; width: 18px; height: 18px; border-radius: 50%; border: 3px solid var(--surface-raised); background: #334155; transition: all 0.15s ease; z-index: 2; }
      .journal-step-node.node-ok { background: #10b981; }
      .journal-step-node.node-warn { background: #f59e0b; }
      .journal-step-node.node-error { background: #ef4444; }
      .journal-step-node.node-active { background: var(--accent); }
      .journal-step-trigger { cursor: pointer; border-radius: 6px; outline: none; }
      .journal-step-trigger:focus-visible { outline: 3px solid var(--accent); outline-offset: 3px; }
      .journal-step-body { border: 1px solid transparent; border-radius: 4px; padding: 10px 12px; transition: background 0.15s ease; }
      .journal-step-trigger:hover .journal-step-body, .journal-step-trigger:focus .journal-step-body { border-color: var(--accent-pressed); background: var(--surface-base); }
      .journal-step.is-selected .journal-step-body { border-color: var(--accent); background: var(--accent-wash); }
      .journal-step-top { display: flex; align-items: center; justify-content: space-between; gap: 10px; margin-bottom: 6px; flex-wrap: wrap; }
      .journal-step-title { font-weight: 700; color: var(--text-strong); font-size: 0.85rem; font-family: 'JetBrains Mono', monospace; }
      .journal-step-duration { font-size: 0.75rem; color: var(--text-muted); font-family: 'JetBrains Mono', monospace; }
      .journal-step-meta { display: flex; align-items: center; gap: 8px; color: var(--text-muted); font-size: 0.75rem; flex-wrap: wrap; }
      .journal-step-values { padding-left: 14px; }
      .journal-event-inspector { margin: 0; padding: 0; }
      .journal-event-inspector[hidden] { display: none !important; }
      .journal-event-inspector-title { margin-bottom: 10px; color: var(--text-strong); font-size: 0.76rem; font-weight: 700; }
      .journal-event-inspector-grid { display: grid; grid-template-columns: minmax(0, 1fr); gap: 16px; margin: 0; }
      .journal-event-inspector-item { min-width: 0; }
      .journal-event-inspector-item dt { margin-bottom: 4px; color: var(--text-muted); font-size: 0.75rem; font-weight: 500; }
      .journal-event-inspector-item dd { margin: 0; color: #dbe4f0; overflow-wrap: anywhere; }

      /* Inspector Panels */
      .inspector-nav { display: flex; border-bottom: 1px solid var(--line); background: var(--surface-raised); overflow-x: auto; }
      .inspector-tab-btn { padding: 11px 16px; border: 0; background: transparent; color: var(--text-muted); font-size: 0.78rem; font-weight: 600; cursor: pointer; border-bottom: 2px solid transparent; transition: all 0.15s; white-space: nowrap; }
      .inspector-tab-btn:hover { color: var(--text-strong); }
      .inspector-tab-btn.active { color: var(--accent); border-bottom-color: var(--accent-solid); background: var(--accent-wash); }
      .inspector-panel { padding: 18px 20px; display: grid; gap: 16px; }
      .inspector-panel[hidden] { display: none !important; }
      .payload-box { background: var(--surface-base); border: 1px solid var(--line); border-radius: 6px; overflow: hidden; }
      .payload-box-header { display: flex; align-items: center; justify-content: space-between; padding: 8px 12px; background: var(--surface-raised); border-bottom: 1px solid var(--line); font-size: 0.75rem; color: var(--text-muted); font-weight: 600; }
      .payload-box-body { padding: 12px; margin: 0; color: var(--text-body); font-size: 0.78rem; font-family: 'JetBrains Mono', monospace; line-height: 1.45; max-height: 320px; overflow: auto; white-space: pre-wrap; word-break: break-word; }
      .inspector-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 12px; }
      .inspector-stat { background: var(--surface-base); border: 1px solid var(--line); border-radius: 6px; padding: 10px 12px; }
      .inspector-stat-label { font-size: 0.75rem; color: var(--text-muted); text-transform: uppercase; letter-spacing: 0; margin-bottom: 4px; }
      .inspector-stat-val { font-size: 0.9rem; font-weight: 700; color: var(--text-strong); font-family: 'JetBrains Mono', monospace; }

      /* Facet filter buttons on Workflow Explorer */
      .flow-facets { display: flex; gap: 8px; flex-wrap: wrap; margin-bottom: 16px; }
      .flow-facet-btn { display: inline-flex; align-items: center; gap: 6px; padding: 6px 14px; border-radius: 6px; border: 1px solid var(--line); background: var(--surface-raised); color: var(--text-muted); font-size: 0.78rem; font-weight: 600; text-decoration: none; cursor: pointer; transition: all 0.15s; }
      .flow-facet-btn:hover { background: var(--surface-selected); color: var(--text-strong); border-color: #334155; }
      .flow-facet-btn.active { background: var(--accent-wash); color: var(--accent); border-color: var(--accent-solid); }
      .flow-facet-count { font-size: 0.75rem; padding: 1px 6px; border-radius: 999px; background: var(--surface-selected); color: var(--text-body); }
      .flow-facet-btn.active .flow-facet-count { background: var(--accent-pressed); color: var(--accent); }

      /* View Toggle (Journal vs Table) */
      .view-toggle { display: inline-flex; border: 1px solid var(--line); border-radius: 6px; background: var(--surface-base); padding: 2px; }
      .view-toggle button { border: 0; background: transparent; color: var(--text-muted); font-size: 0.75rem; font-weight: 600; padding: 4px 10px; border-radius: 4px; cursor: pointer; }
      .view-toggle button.active { background: var(--surface-selected); color: var(--text-strong); }

      /* Breadcrumb Navigation */
      .flow-entity-header { margin-bottom: 20px; padding: 0 0 12px; border-bottom: 1px solid var(--line); }
      .flow-entity-header .flow-breadcrumb { margin-bottom: 0; }
      .flow-entity-identity { display: inline-flex; align-items: center; gap: 8px; min-width: 0; }
      .flow-entity-scope { max-width: min(260px, 40vw); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
      .flow-breadcrumb { display: flex; align-items: center; gap: 8px; font-size: 0.76rem; color: var(--text-muted); margin-bottom: 14px; flex-wrap: wrap; }
      .flow-breadcrumb a { color: var(--text-muted); text-decoration: none; transition: color 0.15s; }
      .flow-breadcrumb a:hover { color: var(--text-strong); }
      .flow-breadcrumb-sep { color: #475569; font-size: 0.75rem; }
      .flow-breadcrumb-current { color: var(--text-strong); font-weight: 600; font-family: 'JetBrains Mono', monospace; }
      .flow-detail-sections { display: flex; gap: 16px; flex-wrap: wrap; margin: 0 0 20px; border-bottom: 1px solid var(--line); }
      .flow-detail-sections a { border-radius: 4px; color: var(--text-muted); padding: 5px 9px; font-size: 0.75rem; text-decoration: none; }
      .flow-detail-sections a:hover, .flow-detail-sections a:focus-visible { color: var(--text-strong); background: var(--surface-selected); outline: none; }
      .workflow-detail-section { scroll-margin-top: 16px; }
      .flow-status-indicator { flex: 0 0 auto; }
      .flow-debug-disclosure { margin: 20px 0; }
      .flow-debug-disclosure > summary { cursor: pointer; color: var(--accent); font-size: 0.78rem; font-weight: 600; }
      .flow-debug-disclosure-body { padding-top: 4px; }

      .flow-query-workspace { display: grid; grid-template-columns: minmax(0, 1fr); gap: 18px; }
      .flow-query-input, .flow-query-output { min-width: 0; }
      .flow-query-output { border-top: 1px solid var(--line); padding-top: 2px; }
      .flow-query-provenance { margin: 10px 0; color: var(--text-muted); font-size: 0.78rem; overflow-wrap: anywhere; }
      .flow-query-provenance p { margin: 4px 0; }
      .flow-query-result-count { margin-left: 8px; color: var(--text-muted); font-size: 0.8rem; font-weight: 400; }
      .flow-query-provenance summary { cursor: pointer; }
      .flow-query-provenance pre { max-height: 240px; overflow: auto; white-space: pre-wrap; margin: 8px 0; }
      .flow-query-draft-status { color: #fbbf24; font-size: 0.82rem; margin: 10px 0; }
      .flow-query-console { padding: 0; }
      .flow-query-toolbar { display: flex; align-items: center; justify-content: space-between; gap: 16px; margin-bottom: 12px; flex-wrap: wrap; }
      .flow-query-payload-note { color: var(--text-muted); font-size: 0.75rem; }
      .flow-query-form { display: grid; gap: 12px; width: 100%; }
      .flow-query-fields { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); align-items: start; gap: 14px 20px; }
      .flow-query-primary-fields { grid-template-columns: repeat(auto-fit, minmax(140px, 1fr)); gap: 12px; }
      @media (min-width: 1280px) {
        .flow-query-primary-fields:has(> [data-flow-query-field="run_state"]:not([hidden])) { grid-template-columns: minmax(180px, 1.2fr) repeat(2, minmax(160px, 1fr)) repeat(2, minmax(120px, 0.85fr)) minmax(64px, 0.4fr); }
      }
      .flow-query-console .flow-query-field { display: grid; gap: 6px; margin: 0; min-width: 0; font-size: 0.75rem; text-transform: none; }
      .flow-query-console .flow-search-input { width: 100%; height: 36px; min-width: 0; max-width: none; font-size: 0.82rem; }
      .flow-query-console .flow-field-help { font-size: 0.75rem; color: var(--text-muted); line-height: 1.4; }
      .flow-query-console .flow-query-help { border: 0; border-radius: 0; background: transparent; padding: 0; margin-bottom: 12px; align-items: start; }
      .flow-query-console .flow-query-help-detail { max-width: 65ch; }
      .flow-query-console .flow-query-discovery { border: 0; border-block: 1px solid var(--line); border-radius: 0; background: transparent; }
      .flow-query-console .flow-query-discovery-summary { padding: 8px 0; }
      .flow-query-advanced { border-block: 1px solid var(--line); }
      .flow-query-advanced > summary { cursor: pointer; padding: 12px 0; color: var(--text-body); font-size: 0.82rem; }
      .flow-query-advanced-body { display: grid; gap: 20px; padding: 8px 0 20px; }
      .flow-query-console .flow-query-predicate-group { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 14px 20px; }
      .flow-query-console .flow-query-predicate-group legend { font-size: 0.75rem; text-transform: none; margin-bottom: 8px; }
      .flow-query-console .flow-query-actions { margin: 0; padding: 0; }
      .flow-query-console .flow-query-run { background: var(--accent-solid); color: #fff; border-color: var(--accent-solid); min-width: 96px; }
      .flow-query-console .flow-query-run:hover { background: var(--accent-pressed); }
      .flow-query-console input::placeholder, .flow-query-console textarea::placeholder { color: var(--text-muted); opacity: 1; }
      .flow-field-error { color: #fca5a5; font-size: 0.75rem; line-height: 1.4; }
      .flow-field-error[hidden] { display: none !important; }
      .flow-query-console [aria-invalid="true"] { border-color: #f87171; }

      /* Copy Button Inline */
      .copy-btn-inline { border: 1px solid var(--line); background: var(--surface-raised); color: var(--text-muted); border-radius: 4px; padding: 2px 7px; font-size: 0.75rem; cursor: pointer; transition: all 0.15s; display: inline-flex; align-items: center; gap: 4px; vertical-align: middle; }
      .copy-btn-inline:hover { border-color: var(--accent-solid); color: var(--accent); background: var(--surface-selected); }
      .copy-btn-inline.copied { border-color: #10b981; color: #10b981; background: rgba(16,185,129,0.1); }

      /* Live Pulse Animations */
      @keyframes pulseLiveGreen {
        0% { transform: scale(0.95); box-shadow: 0 0 0 0 rgba(16, 185, 129, 0.7); }
        70% { transform: scale(1); box-shadow: 0 0 0 6px rgba(16, 185, 129, 0); }
        100% { transform: scale(0.95); box-shadow: 0 0 0 0 rgba(16, 185, 129, 0); }
      }
      @keyframes pulseLiveAmber {
        0% { transform: scale(0.95); box-shadow: 0 0 0 0 rgba(245, 158, 11, 0.7); }
        70% { transform: scale(1); box-shadow: 0 0 0 6px rgba(245, 158, 11, 0); }
        100% { transform: scale(0.95); box-shadow: 0 0 0 0 rgba(245, 158, 11, 0); }
      }
      .pulse-dot-green { width: 8px; height: 8px; border-radius: 50%; background: #10b981; display: inline-block; animation: pulseLiveGreen 2s infinite; vertical-align: middle; }
      .pulse-dot-amber { width: 8px; height: 8px; border-radius: 50%; background: #f59e0b; display: inline-block; animation: pulseLiveAmber 2s infinite; vertical-align: middle; }

      /* Keyboard Shortcuts Modal */
      .keyboard-modal { color: var(--text-body); margin: auto; width: 480px; padding: 0; border: 1px solid var(--line); border-radius: 6px; background: var(--surface-raised); max-width: calc(100vw - 48px); max-height: calc(100vh - 48px); }
      .keyboard-modal::backdrop { background: rgba(0,0,0,0.7); }
      .keyboard-modal:not([open]) { display: none; }
      .dashboard-snapshot { display: flex; align-items: center; gap: 10px; margin-left: auto; font-size: 0.75rem; color: var(--text-muted); }
      .dashboard-snapshot[hidden] { display: none; }
      .dashboard-snapshot a { text-decoration: none; }
      .dashboard-help-button { margin: 14px 20px; background: none; color: var(--text-muted); border: 0; padding: 6px 0; cursor: pointer; text-align: left; }
      .sidebar-session strong { overflow-wrap: anywhere; font-size: 0.75rem; }
      .table-scroll { max-height: min(65vh, 720px); overflow-y: auto; }
      .flow-failure-summary-ribbon dd > span { font-weight: 400; }
      .flow-overview-ribbon dd > span, .flow-projection-ledger dd > span { font-weight: 400; font-family: var(--font-sans, sans-serif); margin-top: 4px; }
      .top-bar .metric .val { white-space: normal; }
      .keyboard-card { padding: 22px 24px; width: 100%; }
      .keyboard-header { display: flex; align-items: center; justify-content: space-between; gap: 16px; margin-bottom: 16px; border-bottom: 1px solid var(--line); padding-bottom: 12px; }
      .dashboard-modal-close { flex: 0 0 auto; border: 1px solid var(--line-strong); border-radius: 4px; padding: 5px 9px; background: var(--surface-selected); color: var(--text-strong); cursor: pointer; }
      .keyboard-title { font-size: 0.95rem; font-weight: 700; color: var(--text-strong); }
      .keyboard-row { display: flex; align-items: center; justify-content: space-between; padding: 8px 0; border-bottom: 1px solid var(--line); font-size: 0.78rem; color: var(--text-body); }
      .keyboard-row:last-child { border-bottom: none; }
      .keyboard-shortcut-setting { display: flex; gap: 8px; align-items: center; margin-bottom: 12px; font-size: 0.78rem; color: var(--text-body); }
      .keyboard-keys { display: flex; gap: 4px; }
      kbd { background: var(--surface-selected); border: 1px solid #334155; border-radius: 4px; padding: 2px 6px; font-size: 0.75rem; font-family: 'JetBrains Mono', monospace; color: var(--text-strong); box-shadow: 0 1px 2px rgba(0,0,0,0.4); }

      /* Responsive */
      @media (max-width: 1100px) {
        .flow-management-group .flow-policy-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
        .top-bar-identity { grid-template-columns: auto minmax(0, 1fr); }
        .flow-state-filter-form { grid-template-columns: repeat(2, minmax(0, 1fr)); }
        .acl-tester-form { grid-template-columns: repeat(2, minmax(0, 1fr)); }
        .flow-state-filter-form .flow-filter-time-group { grid-column: 1 / -1; }
        .flow-journal-workspace, .flow-journal-workspace.has-selected-event { grid-template-columns: minmax(0, 1fr); }
        .flow-journal-inspector { position: static; border-left: 0; border-top: 1px solid var(--line); max-height: none; }
        .flow-execution-summary { grid-template-columns: minmax(0, 2fr) minmax(0, 1fr); }
        .flow-query-fields { grid-template-columns: repeat(2, minmax(0, 1fr)); }
        .flow-query-console .flow-query-predicate-group { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      }
      @media (max-width: 768px) {
        .flow-definition-list { grid-template-columns: minmax(0, 1fr); gap: 4px; }
        .flow-definition-list dd { margin-bottom: 8px; }
        .flow-query-reference-terms { grid-template-columns: minmax(0, 1fr); gap: 6px; }
        .flow-management-group .flow-policy-grid { grid-template-columns: minmax(0, 1fr); }
        .top-bar { grid-template-columns: minmax(0, 1fr); }
        .top-bar > .dashboard-live-status { grid-row: auto; grid-column: 1; justify-self: start; margin-left: 0; }
        .top-bar-metrics { grid-template-columns: repeat(3, minmax(0, 1fr)); }
        .acl-tester-form, .kv-query-modes { grid-template-columns: minmax(0, 1fr); }
        html, body { max-width: 100%; overflow-x: hidden; }
        .layout { flex-direction: column; }
        .sidebar { width: 100%; height: auto; position: static; border-right: none; border-bottom: 1px solid var(--line); padding: 8px 0; display: flex; flex-wrap: wrap; overflow-x: auto; }
        .sidebar a { padding: 8px 12px; border-left: none; border-bottom: 2px solid transparent; font-size: 0.75rem; }
        .sidebar a.nav-subitem { padding-left: 12px; font-size: 0.75rem; }
        .sidebar a.active { border-left: none; border-bottom-color: var(--accent-solid); background: transparent; }
        .sidebar .nav-section { display: none; }
        .top-bar { gap: 12px; padding: 10px 16px; }
        .top-bar .metric .val { font-size: 0.9rem; }
        .top-bar .sep { display: none; }
        .content { padding: 16px; }
        .hit-rate-num { font-size: 2.2rem; }
        .cache-hero { flex-direction: column; }
        .hit-rate-card { min-width: unset; }
        .flow-card-wide { grid-column: span 1; }
        .flow-nav-row { align-items: stretch; }
        .flow-tabs, .flow-search, .flow-filter-form, .flow-policy-actions { width: 100%; }
        .flow-search, .flow-filter-form { align-items: stretch; }
        .flow-search { flex-direction: column; }
        .flow-filter-form label, .flow-filter-form select, .flow-filter-form input, .flow-filter-form button, .flow-search-input, .flow-search-button { width: 100%; max-width: none; }
        .flow-query-mode-tabs { display: flex; width: 100%; }
        .flow-query-mode-tabs button { flex: 1; }
        .flow-query-discovery-groups { grid-template-columns: 1fr; }
        .flow-query-predicate-group { flex: 1 1 100%; width: 100%; }
        .flow-query-scalar-input { grid-template-columns: 1fr; }
        .flow-query-actions { align-items: stretch; flex-direction: column; }
        .flow-query-actions .flow-search-button, .flow-query-pagination .flow-search-button { width: 100%; }
        .flow-query-plan-grid { grid-template-columns: 1fr; }
        .flow-query-chart-grid { grid-template-columns: 1fr; }
        .flow-query-donut-layout { grid-template-columns: 112px minmax(0, 1fr); gap: 12px; }
        .flow-query-donut { width: 112px; height: 112px; }
        .flow-query-time-chart { height: 150px; }
        .acl-form-grid, .acl-scope-grid, .acl-role-selector { grid-template-columns: 1fr; }
        .acl-management-heading, .acl-readonly-panel { align-items: stretch; flex-direction: column; }
        .acl-form-actions .flow-search-button { width: 100%; }
        .acl-row-actions { min-width: 320px; }
        .sidebar-session { margin: 0 8px; padding: 8px; border-top: 0; border-left: 1px solid var(--line); }
        table { white-space: nowrap; }
        .content > table, .content :not(.table-scroll) > table { display: block; width: 100%; max-width: 100%; min-width: 0; overflow-x: auto; -webkit-overflow-scrolling: touch; scrollbar-width: thin; scrollbar-color: #334155 var(--surface-base); }
        .flow-query-table-wrap > .flow-query-projection-table { display: table; width: max-content; min-width: 100%; max-width: none; overflow: visible; }
        .flow-query-projection-table th, .flow-query-projection-table td { min-width: 108px; white-space: nowrap; overflow-wrap: normal; }
        .table-scroll { box-shadow: inset -18px 0 16px -18px rgba(129, 140, 248, 0.85); }
        .table-scroll table { width: max-content; min-width: 100%; }
      }
    """
  end
end
