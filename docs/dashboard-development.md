# Dashboard UI Development

The OSS dashboard is server-rendered Elixir/EEx with shared CSS and progressively
enhanced JavaScript. Keep rendering changes separate from query planning,
authorization, routing, and mutation execution.

## Workflow UI Contract

- The workflow overview leads with current-scope records. Secondary state and
  worker summaries are available in the Workload breakdown disclosure.
- Runtime state and logical workflow state are distinct. A lease indicates
  ownership, not proof of current worker activity. Custom state names must not
  be classified as terminal through substring matching.
- Ordinary queued, scheduled, and FIFO waiting is neutral. Expired leases and
  terminal failures require attention. Sampled counts are not global totals.
- Due time does not establish claimability. State summaries must not diagnose
  worker starvation from zero running records in a filtered sample. Attention
  actions retain the same type and partition on initial and live renders.
- Exact type/state/partition views with observed FIFO lanes lead with the lane
  inspector. State summaries remain available in a native disclosure; broader
  and empty views keep the state table first. Live component IDs stay stable.
- States filter labels and controls stay grouped, with range/from/to in one UTC
  fieldset. Long logical steps allow wrapping at underscores without changing
  their text or inserting unescaped HTML.
- Overview type summaries are a pure reduction of the authorized, filtered
  sample. Never replace them with unscoped `FLOW.INFO` counts; automatic
  partitions, explicit partitions, and custom states must keep the same scope.
- Empty optional type/state fields mean no predicate. Literal `all`, `ALL`, and
  `All` are valid, distinct identifiers, not wildcard values. Preserve them in
  form parsing, scoped navigation, and live-refresh URLs.
- Detail pages lead with execution status and history. Journal and Raw Events
  share history pagination. Metadata, relationships, diagnostics, and mutations
  remain available without dominating the initial view.
- Omit the waterfall when the loaded history contains no measurable interval.
  The journal still retains every loaded event. Timing stays capped at 80 events
  and labels truncated windows. The inspector takes space only after selection.
- A waterfall's final event has no measured duration without a successor. Show
  `No end event`, not `0ms` or an assumed ongoing execution. Zero remains valid
  for two events with the same timestamp. Interval pairing is linear-time.
- Hide history paging controls only for successful, explicitly complete default
  pages. Cursor views, non-default page sizes, unknown completeness, and errors
  retain their controls. Use native disclosure markers for expandable sections.
- Query Studio preserves independent Guided and Raw FQL drafts. Hidden fields
  are disabled; active predicates remain discoverable. Client scalar/date
  validation supplements, but never replaces, server validation.
- Returned query rows precede charts and usage details. Charts describe the
  current page, not the complete workflow population. Query quality remains
  visible, and authenticated continuation cursors are unchanged.
- Query operation, scope, state, and limit share an adaptive primary field row.
  Successful result counts sit in the result header; idle and error messages
  remain visible without being mistaken for a successful empty result.
- Guided run tables show supported returned fields only; omitted worker/value
  metadata must not be represented as confirmed absence. Scoped detail links
  remain the path to those fields. History tables retain their event metadata.
- Successful query results retain a captured UTC timestamp and executed inputs.
  Edits and editor-mode changes flag drafts that differ from displayed results;
  reverting the inputs clears the warning. This is local form comparison, not
  automatic query execution, and does not load payloads or poll for results.
- HTTP detail pages and live refresh do not fetch referenced values. Opening a
  value uses the existing flow/ref/partition-scoped, authorization-checked value
  endpoint. Do not reintroduce eager payload reads for presentation purposes.
- Projected run links retain exact partition scope from the prepared request,
  independently of returned columns. An explicit row partition takes priority.
  Without either, show the ID without a link and suggest including
  `partition_key`; never guess a partition by sampling unrelated records.
- Value requests preserve bounded history-page context (`history_count`,
  `history_before`, `history_after`). The server verifies record ACL and reference
  ownership against the current record and that history page before one value
  read. Request parameters cannot override the authenticated ACL identity.
- The value inspector distinguishes loading, missing, error, and ready states.
  Only ready data is copyable, including an empty string or the literal `missing`.
  Retry retains scope; expired sessions retain page and value selection through
  login. Close/reopen cancels old requests and ignores stale responses even if
  cancellation is ineffective. Requests time out after 15 seconds.
- Preview formatting decodes or inspects at most an 8 KiB binary prefix and
  returns at most 8 KiB of valid UTF-8, including the truncation marker. The
  endpoint includes `truncated`; the UI labels truncated copies as previews.
- Polling must preserve selection and scope, pause during inspection/editing,
  expose stale results, and redirect expired sessions to login.
- Pause replacement while any journal control has focus, including its mode
  tabs. Resume after focus leaves. The value inspector uses a native modal
  dialog for background isolation, contains Tab/Shift+Tab, and restores focus
  to the opener after Escape, Close, or backdrop dismissal.
- Policy review text is derived from the current form locally, using text nodes
  rather than HTML interpolation. It distinguishes state overrides from type
  defaults: mode applies only to a state override, indexes remain unchanged for
  state overrides, and max active duration remains type-level. Invalid numeric
  input is not presented as valid policy. Without JavaScript, omit the review
  summary instead of displaying a stale server snapshot.
- Schedule timing controls are enabled and required only for the selected kind,
  on initial render and after a mode change. Switching retains inactive draft
  values locally but excludes them from validation and submission. Timezone is
  cron-only; overlap and max fires are recurring-only.
- Failed schedule creation returns HTTP 422 with an escaped, allowlisted draft
  and an open form. The HTTP request-size limit bounds the draft. Never put
  payload drafts in URLs or browser storage. The error page does not read the
  schedule catalog or change ACL requirements. Success still redirects and keeps
  list filters; CSRF and existing mutation guards remain mandatory.
- Governance state-metadata searches keep their disclosure open for submitted
  filters, results, and validation errors. Initial idle pages stay collapsed.
- FIFO overview renders at most 40 observed lanes and labels truncation. A
  single-lane view retains at most eight metadata-only members, with leased
  members first and then known state-entry sequence. Missing sequence remains
  explicit. Scheduled cold-parked records can be outside the hot sample; never
  label a sampled due member as definitely claimable or assign a global queue
  position. Inspect lane retains type, logical state, and partition.
- Related runs retains the exact type and partition but deliberately omits
  state, so running, waiting, scheduled, and terminal records remain eligible.
  It uses the existing bounded indexed query and destination ACL checks. Do not
  replace it with a global sample or add an implicit queued-state predicate.
- Native member disclosures keep their open state across live replacement.
  Focus within an inspector pauses polling; no member expansion reads history
  or referenced payloads. Collector and renderer remain separate modules.
- A detail request shares one bounded durable sample between record lookup and
  FIFO rendering. Keep it request-local, retain state-entry sequence metadata,
  and preserve the payload-free bounded fallback for records outside the sample.
  Do not repeat the global scan for the member widget or cache it across users.

Shared theme tokens live in `layout/styles.ex`. Use neutral surfaces, clear
focus indicators, and restrained semantic status colors. Do not animate a
leased state as if it were a measured worker heartbeat.

## Automated Tests

Run the dashboard rendering, collection, ACL, HTTP, and mutation-safety suite:

```sh
mix test apps/ferricstore_server/test/ferricstore_server/health/dashboard \
  apps/ferricstore_server/test/ferricstore_server/health/dashboard_test.exs --seed 0
```

`workflow_widgets_test.exs` covers status semantics and composition. The
`workflow_widgets` tag also selects the HTTP regression proving zero value
reads before an explicit request. Keep existing authorization and bounded-query
tests when changing page structure.

## Desktop Browser Checks

On a development machine, start the isolated synthetic demo. It disables
protected mode and uses HTTP port 4000 and native port 6389; do not expose it as
a production deployment.

```sh
mix run --no-start --no-halt scripts/run_dashboard_demo.exs
```

The browser harness expects this demo's workflow IDs and Chrome. Install its
Playwright dependency outside the repository, then run:

```sh
npm --prefix "${TMPDIR:-/tmp}/ferricstore-browser-tools" install --no-save playwright@1.60.0
NODE_PATH="${TMPDIR:-/tmp}/ferricstore-browser-tools/node_modules" \
  node tools/dashboard-workflow-check.mjs --screenshots
NODE_PATH="${TMPDIR:-/tmp}/ferricstore-browser-tools/node_modules" \
  node tools/dashboard-management-check.mjs
NODE_PATH="${TMPDIR:-/tmp}/ferricstore-browser-tools/node_modules" \
  node tools/dashboard-fifo-check.mjs
```

Set `HEADFUL=1` for a visible browser, `DASHBOARD_URL` for a different demo URL,
or `DASHBOARD_OUT_DIR` for an output directory. The harness checks filters, all
ten Guided operations, FQL/Explain, validation, journal navigation, polling
recovery, duplicate-submit guards, and lazy value requests. Expiry and value
responses are browser fixtures; server authorization is covered by ExUnit.
The management harness checks policy draft summaries and reset/invalid input,
schedule mode changes, real invalid-JSON POSTs with and without JavaScript, and
visible governance search errors. It does not create schedules or save policies.
The inspector checks also simulate out-of-order responses with transport
cancellation disabled, missing/error retries, and login return context.

Screenshot checks cover desktop widths 1280, 1440, and 1920 with document
overflow assertions. The FIFO harness covers three populated lanes: a live
lease, an expired lease, and a cold scheduled head ahead of a due member. It
checks keyboard disclosure, bounded previews, polling, scoped navigation, and
actual indexed result links. The demo waits for validated query indexes and
asserts that the cold scheduled head is queryable before announcing readiness.
Mobile is outside this review's scope.

The September 2026 Impeccable scanner was unavailable in full mode because its
Puppeteer and HTML parser dependencies were missing. Its regex fallback is not
contrast or layout certification. Use the screenshots and interaction checks
as the rendered evidence; do not report a clean detector result as full QA.
