# Browser DevTools

FerricStore keeps browser inspection tooling out of production and repo package
dependencies. Use these commands as ephemeral debugging tools.

## Playwright Dashboard Inspector

Inspect a live dashboard:

```sh
tools/dashboard-inspect.sh --url http://127.0.0.1:62851/dashboard/flow
```

Useful options:

```sh
tools/dashboard-inspect.sh \
  --url http://127.0.0.1:62851/dashboard/flow \
  --wait-ms 5000

HEADFUL=1 tools/dashboard-inspect.sh \
  --url http://127.0.0.1:62851/dashboard/flow
```

The wrapper installs Playwright into `${TMPDIR:-/tmp}/ferricstore-browser-tools`,
not into the repository.

The script writes:

- `test-results/dashboard-inspect.png`
- `test-results/dashboard-inspect.json`

The JSON report includes console errors, failed requests, dashboard API payload
sizes, live component sizes, table/cell counts, charts, and scroll height.

## Chrome DevTools MCP

This repo includes `.mcp.json` for MCP clients that read project-local MCP
configuration. For Codex, add it globally:

```sh
codex mcp add chrome-devtools \
  --env CHROME_DEVTOOLS_MCP_NO_USAGE_STATISTICS=1 \
  --env CHROME_DEVTOOLS_MCP_NO_UPDATE_CHECKS=1 \
  -- npx -y chrome-devtools-mcp@1.0.1 --no-usage-statistics --no-performance-crux
```

Codex MCP tools are loaded when the session starts. Restart or reload Codex
after adding the server.
