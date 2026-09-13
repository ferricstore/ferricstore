# Dashboard UI Development

The OSS dashboard is server-rendered Elixir/EEx with shared CSS and progressively
enhanced JavaScript. Keep rendering changes separate from query planning,
authorization, routing, and mutation execution.

## Shared Shell and Operational Data

- `Dashboard.Assets` embeds the shared CSS/JavaScript and their SHA-256 paths at
  compile time. Only those exact paths are public and immutable; no request data,
  sessions, configuration, or filesystem paths belong in assets. HTML and live
  JSON remain private/no-store, and asset responses never set session cookies.
- Keep one native keyboard-help dialog per page. Contain keyboard focus, restore
  the opener, and pause component replacement while dialogs or confirmations
  are open. Confirmation panels participate in table layout rather than being
  clipped overlays. CSRF, expected-version checks, and single-submit guards
  remain authoritative independently of presentation.
- Snapshot pages show their captured UTC time and explicit refresh control;
  adding freshness does not add polling. Doctor notices describe the status at
  submission time, not a claim about a job's subsequent progress.
- Sidebar groups retain explicit open/closed choices in bounded session storage;
  the active destination's group always opens on navigation. Storage failure
  falls back to the server-rendered active group. KV sidebar and local navigation
  share one route/label/order definition. Shared refresh dispatches a cancelable
  `dashboard:before-refresh` event before navigation so draft owners can intervene.
  Preference events are delegated to the current DOM; live sidebar replacement
  reconciles shortcut checkboxes without registering duplicate listeners. Reveal
  the active destination inside the sidebar on navigation only, retaining manual
  sidebar scrolling and disclosure choices through subsequent live updates.
- Operational labels, caveats, and badges use a 12px minimum at the default root
  size. Table overflow belongs to a labeled, keyboard-focusable local scroller,
  not the document; long values remain available through bounded disclosures.
- Local table filters search only loaded, authorized rows. Preserve their values
  through live replacement, do not count empty placeholder rows, and do not
  fetch more data. Compare live components with their last server HTML so local
  filtering does not cause identical responses to redraw the table. Disclosure
  counts update independently of whether the user opened the section.
- Live requests have a deadline independent of the freshness clock. Retain the
  last successful update age during failures and pauses; Retry uses the existing
  bounded backoff. Preserve keyed scroll offsets and both open and closed
  disclosures during replacement. Pause/Resume does not override an edited form.
  Disclosure keys identify a record and field, never a row index. `TableValue`
  accepts an explicit record identity for repeated values; anonymous disclosures
  are not restored onto potentially different records. GET filter forms show
  unapplied changes beside their controls while live results retain applied scope.
- Clipboard callers share `window.dashboardCopyText`, which reports success
  only after a successful clipboard write or a true fallback result. Failure
  leaves the content selectable and shows manual-copy recovery.
- Every page has a focusable `dashboard-main` skip target and semantic section
  headings. Shared tooltips are viewport-clamped and remain usable inside
  scrolling tables; their definitions do not become the column's accessible name.
- Timestamp helpers include UTC and preserve millisecond or microsecond precision.
  Callers must not append another timezone suffix. Unavailable times stay explicit.
- Read summaries distinguish no observations from measured zero. Rate labels
  identify since-start averages. Memory shows process RSS and tracked allocation
  budgets from the same MemoryGuard snapshot; projection health separates current
  degradation, missing telemetry, and historical failure counters. Consensus
  indices remain exact integers. Storage labels identify shared versus per-shard
  accounting. Runtime Parameters uses one redacted effective CONFIG snapshot.
- Account validation returns a read-free HTTP 422 recovery page. Preserve only
  escaped, allowlisted non-secret draft fields, clear credentials, and default
  new accounts to Observer. Session identity comes from the verified request.
- Key/prefix inputs are literal identities, including whitespace. Exact lookup
  and prefix browsing are separate modes. Apply ACL visibility while admitting
  sampled rows, with an independent 10,000-key scan budget; budget exhaustion is
  not proof that the user's remaining data is absent.
- Keyspace reports returned keys separately from scanned entries. Restricted
  accounts never receive raw scan counts, and active internal-key exclusions
  remain visible. Missing long keys wrap without losing the literal identity.
  Compound metadata opt-in admits only readable public logical keys. Protected
  Flow/server records and undecodable internal identities remain excluded;
  callers cannot bypass this by requesting a physical internal key directly.
  Exact and prefix forms identify their independent scope and share an explicit
  clear-both action. Physical kinds are distinct from the inspector's logical
  type. Physical keys use JSON string notation for UTF-8 and labeled Base64 for
  other bytes, so NUL separators and literal escape sequences remain distinct.
- The ACL tester validates account existence/enabled state and supported command
  names before showing permissions. At least one command, key, channel, or route
  is required for a check; a pattern match does not prove a command exists.
  Runtime config keeps exact byte values beside human units and distinguishes
  unavailable values, empty strings, and parameter-specific disabled settings.
- Stream groups, waiters, and Pub/Sub snapshots admit ACL-visible rows before
  retaining their bounded result set, within a 10,000-entry traversal budget.
  Activity remains a retained recent window, not a complete history. Prefix
  hotness uses direct ETS lookups for displayed rows; absent telemetry is not zero.
  OSS server capability discovery advertises its installed ACL management adapter
  without enabling unrelated enterprise capabilities.

## Workflow UI Contract

- The workflow overview leads with current-scope records. Secondary state and
  worker summaries are available in the Workload breakdown disclosure.
- Stored state is the literal `state` field, including custom states. Workflow
  state uses the existing logical-state derivation (`run_state` while running).
  Activity labels require lease/due evidence; they are not synonyms for state.
  A lease indicates
  ownership, not proof of current worker activity. Custom state names must not
  be classified as terminal through substring matching.
- Ordinary queued, scheduled, and FIFO waiting is neutral. Expired leases and
  terminal failures require attention. Sampled counts are not global totals.
  A bare `running` label does not establish a healthy lease. Failure evidence
  includes each workflow's literal partition beside its investigation link.
- Due time does not establish claimability. State summaries must not diagnose
  worker starvation from zero running records in a filtered sample. Attention
  actions retain the same type and partition on initial and live renders.
  Due charts and record diagnostics use timing facts, not inferred eligibility.
  Zero chart values have no filled bar; the minimum visible width applies only
  to positive values.
- Exact type/state/partition views with observed FIFO lanes lead with the lane
  inspector. State summaries remain available in a native disclosure; broader
  and empty views keep the state table first. Live component IDs stay stable.
- States filter labels and controls stay grouped, with range/from/to in one UTC
  fieldset. Long logical steps allow wrapping at underscores without changing
  their text or inserting unescaped HTML.
  Malformed, out-of-range, or reversed custom dates return HTTP 422 with field
  errors and escaped drafts, before any hot sample or cold terminal lookup.
  Invalid requests render no results and do not poll. Explicit relative/custom
  mode enables only its own controls; switching retains the inactive local draft.
  Blank bounds and equal timestamps remain valid.
- States cold-terminal summaries use a fixed 100-record maximum, independent of
  Recent Limit, which only bounds displayed recent rows. Failed/timed-out cold
  reads produce unavailable or partial coverage, not successful empty results.
  Policy failures produce unknown mode and explicit incomplete FIFO coverage,
  never an inferred parallel mode. State and lane summaries reuse one policy
  lookup per sampled type in the request; no background polling is added.
- Workers leads with worker leases and running records. Lease distribution and
  FIFO lanes remain in a native disclosure whose state survives live updates.
  Running rows lead with workflow/type/partition, worker, status, and UTC lease
  deadline. Lease tokens and fencing values remain in per-workflow native
  disclosures, keyed by ID and partition. Opening one does not read storage;
  focus pauses replacement, and expansion survives refresh without column shifts.
  The Due Work comparison is an unframed, content-height summary of the two
  sampled counts, not a fixed-height chart card.
  Retention uses a compact metric ribbon and global limit field; candidates
  precede command references. Global cleanup warnings, explicit confirmation,
  CSRF, and authorization remain unchanged.
- Overview type summaries are a pure reduction of the authorized, filtered
  sample. Never replace them with unscoped `FLOW.INFO` counts; automatic
  partitions, explicit partitions, and custom states must keep the same scope.
- Empty optional type/state fields mean no predicate. Literal `all`, `ALL`,
  `All`, and `any` are distinct identifiers, not wildcard values. Preserve them in
  form parsing, scoped navigation, and live-refresh URLs.
- Sidebar and keyboard navigation retain compatible type/partition predicates,
  but never transfer a state predicate into an unrelated view. Overview, Workers,
  and Due collectors and live URLs must apply the same retained scope. The
  Overview summary exposes both predicates and explicitly clears all scope.
- Scope links use one server-rendered route contract, including on POST query
  pages. `Flow.QueryScope` derives transferable scope from the bound request, not
  inactive Guided inputs. Updated-time bounds retain exact milliseconds; event,
  created-time, and lease-time predicates are not relabeled as current-run updates.
- Detail pages lead with execution status and history. Journal and Raw Events
  share history pagination. Metadata, relationships, diagnostics, and mutations
  remain available without dominating the initial view.
- Detail history requires `FLOW.HISTORY` independently of `FLOW.GET`. Check the
  command permission before reading history, and render an explicit restricted
  state in HTML and live JSON. Historical value references are available only
  through authorized history; current-record references remain available to a
  principal with the existing record read permission.
- Prepare mutation capabilities from command and write-key grants before
  rendering controls. Use the same effective scope as the form: explicit
  partition when provided, workflow ID for automatic partition routing. Rewind
  also requires history access. Server mutation authorization remains mandatory.
  Missing records omit execution controls and polling; a disappearance during
  live refresh clears execution sections and requires explicit retry.
- Signal and Rewind use the exact URL workflow ID for both authorization and
  execution, including significant whitespace. A submitted body ID cannot
  override it. Rewind carries the state and version from the reviewed page;
  Raft apply checks both before mutation. A duplicate or stale submission must
  fail even when another mutation leaves the same state. Never replace a stale
  version with a fresh one automatically.
- Live detail exposes a lightweight action snapshot. A changed or missing record
  marks reviewed actions stale without opening a closed disclosure or losing its
  draft. Refresh and review is explicit; expected versions are never rewritten.
- Rejected Signal/Rewind submissions return HTTP 422 with escaped, allowlisted
  drafts and bounded history context. Their recovery page reads no record or
  history, so mutation-only users cannot gain read access. Success redirects
  with the same history scope. Confirmation and CSRF remain mandatory; Signal
  requires If State when Transition To is set, with and without JavaScript.
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
  Guided date bounds share the States validator: malformed or reversed bounds
  return HTTP 422 before query execution or option discovery. Keep raw invalid
  text visible and open its disclosure. Valid UTC and numeric millisecond bounds
  retain millisecond precision when rendered and resubmitted.
- Returned query rows precede charts and usage details. Charts describe the
  current page, not the complete workflow population. Query quality remains
  visible, and authenticated continuation cursors are unchanged.
  Group category identity retains scalar types, empty strings, and nulls. A
  synthetic remainder is separate from a literal category named Other. Time
  ticks represent the actual axis positions, including fractional midpoints.
- Guided run pages retain their operation-specific presentation through continuation.
  Expired-lease queries project the lease deadline without loading payloads.
  Raw projections retain their requested fields and distinguish string values
  from boolean/null values without altering JSON types. Previous/First navigation
  carries at most 16 cursors and 32 KiB of validated history in POST forms, not
  browser storage. It adds no total-count query or automatic next-page fetch.
- Current-page JSON export uses shared `Flow.QueryProjection` selectors in the
  returned order. It preserves scalar types and exact integer text, represents
  non-UTF-8 binary values as base64, and stops at 1 MiB including HTML escaping.
  Download the serialized text directly, without parsing and re-encoding it in
  JavaScript. Export performs no query, next-page request, or payload lookup.
  Single-row results omit redundant charts; time charts also expose exact bucket
  values in an accessible table.
  Structured projected values have native JSON disclosures, bounded to 64 KiB
  per cell and 1 MiB per page. Stop encoding after the page budget is exhausted
  and label the limit. These inspect already-returned data without a network
  request; they do not change Raw projections or current-page exports. Binary
  chart categories use an explicit Base64 label, separate from literal text.
- Query operation, scope, state, and limit share an adaptive primary field row.
  Successful result counts sit in the result header; idle and error messages
  remain visible without being mistaken for a successful empty result.
- Guided run tables show supported returned fields only; omitted worker/value
  metadata must not be represented as confirmed absence. Scoped detail links
  remain the path to those fields. History tables retain their event metadata.
  Guided defaults include `run_state` and distinguish Stored state from Workflow
  state; Raw FQL projections and exports remain exactly user-selected.
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
  `history_before`, `history_after`, or inclusive `history_event`). Historical
  Query Studio links navigate to the scoped detail page containing that event.
  The server verifies record ACL and reference
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
  defaults: mode applies only to a state override. State saves cannot modify
  type-wide indexes or max active duration, including tampered HTTP submissions.
  Type defaults remain separately editable through an explicit link. Invalid numeric
  input is not presented as valid policy. Without JavaScript, omit the review
  summary instead of displaying a stale server snapshot.
- Schedule timing controls are enabled and required only for the selected kind,
  on initial render and after a mode change. Switching retains inactive draft
  values locally but excludes them from validation and submission. Timezone is
  cron-only; overlap and max fires are recurring-only.
  JSON payload editing uses a resizable multiline control. Index lifecycle tables
  keep validation and statistics ahead of infrequent build/retirement details;
  opaque build identities stay copyable inside wrapping disclosures.
- Management pages prepare write capabilities through the real POST requirement
  builders; hidden or denied controls never replace server authorization. Reuse
  single-submit guards and preserve named submitter values before disabling them.
- Schedule ID filtering is an exact, authorized lookup rather than filtering an
  already limited list. Definitions expose original timing metadata separately
  from mutable next-fire time; unavailable fields in existing records stay unknown.
- Absolute one-shot times and optional recurring start/end bounds are explicitly
  UTC, independent of browser timezone. Server validation rejects malformed,
  reversed, and kind-incompatible timing before mutation and preserves drafts.
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
  Runtime `running` filters retain leased lanes. Execution mode comes from each
  record's logical state policy, not the runtime label; aggregates spanning
  different logical modes explicitly show `mixed`. Reuse the bounded sample
  and per-request policy cache without additional storage reads.
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

Policy editing loads an explicit type and optional state before showing editable
settings. `Flow.PolicyEditor` prepares the effective values outside the renderer.
The form carries the loaded policy generation for atomic conflict detection;
changing the scope selector requires a new load. Invalid saves return an escaped,
allowlisted draft with HTTP 422 and do not read the catalog for error rendering.
Keep state-scoped FIFO, retry, retention, and governance settings intact when
editing unrelated fields.

Policies also exposes the existing query-index lifecycle snapshot: generation,
build state, validation failures, retirement, and statistics freshness. Check
`FLOW.QUERY.INDEXES` permission before the one status read. The underlying
registry remains bounded to 32 entries; the renderer does not read indexes.

Governance metadata queries use `Flow.QueryResult` to retain page and quality
metadata. Continuation keeps both metadata predicates and overview filters;
submitting filters starts at the first page. Exact string values retain all
whitespace, and a submitted blank value matches an empty string, not a missing
predicate. These searches still require `FLOW.QUERY` authorization. Keep the
100-record cap and explicit queries; do not introduce automatic pagination.

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

The sixth-review checks exercise live replacement and real named form actions,
not only static HTML contracts:

```sh
node tools/dashboard-shell-sixth-check.mjs
node tools/dashboard-shell-hardening-check.mjs
node tools/dashboard-query-sixth-check.mjs
node tools/dashboard-workflow-sixth-check.mjs
DASHBOARD_URL=http://localhost:4010 node tools/dashboard-sixth-served-check.mjs
```

Use the isolated synthetic preview, never a user's data directory. Set
`NODE_PATH` to the installed Playwright runtime when it is not a local dependency.

### Review And Action Contracts

Guided and Raw query editors keep independent drafts. Importing a submitted
Guided query into Raw is explicit and confirms replacement of an existing Raw
draft. The imported FQL and parameters come from the submitted server plan,
including its resolved time bounds, not a second browser implementation of the
query builder. Editing Guided invalidates that import until the next submission.
Operations without an exact FQL equivalent do not offer a partial import.

Query submissions preserve only the unsubmitted sibling draft through a bounded,
consume-on-return browser handoff. The handoff is scoped to the current enabled
account, expires after five minutes, and never enters the URL. Storage failure
requires explicit discard confirmation. Query and schedule field errors retain
escaped drafts, identify the invalid editor, and restore useful keyboard focus.
Schedule catalog filtering warns before discarding an unfinished definition.
Query refresh and navigation also warn before discarding unsubmitted edits;
they do not persist query contents beyond the existing bounded submission handoff.
Required Search predicates and invalid controls open their containing disclosure.
Text predicates accept the empty string; null is a distinct typed choice. Time
bounds and direction labels name the operation's clock. Explain normalizes
wall-time units and exposes alternative index identity and cost from its existing
response rather than issuing extra planner requests.

State time controls remain usable without JavaScript. The explicit Time mode
decides which bounds the server applies; JavaScript only hides and disables the
inactive controls. Table focus uses the shared accent and state identifiers keep
enough width for ordinary names at desktop sizes.

Relationship links preserve known routing identities: parent detail uses the
parent partition, while root/correlation links query relationships within the
inspected partition. Unknown partitions require an explicit lookup instead of
guessing from a governance scope. Signal journal summaries show an escaped,
256-byte bounded name. Detail can expand already-loaded state metadata; Governance
keeps its 32-entry preview, prioritizes the matching key, and links to full detail.
Neither path adds payload reads, polling, or automatic query pagination.

Retention's Refresh sampled preview is not a simulation of global cleanup. Keep
the global cleanup review, confirmation, and aggregate limit explicit. The pending index
operations metric is distinct from durable projection lag.

Workflow actions repeat the literal workflow ID, partition, and reviewed version.
Rewind requires an explicit history event and distinguishes keeping that event's
schedule, running now, and an explicit UTC time. The journal's Event intervals
show elapsed time between recorded events, not inferred task execution duration.
Value inspection identifies current versus historical provenance even when the
underlying reference is cached. Long inline error details remain bounded to
8 KiB per field and 64 KiB in total per loaded history page; Raw events reuse
the journal disclosure rather than embedding diagnostics twice. Truncation is
visible and does not trigger another history or payload fetch.

Circuit Open and Close have separate review forms and required impact
confirmation. Their reviewed snapshot is checked inside the existing atomic
mutation retry, including absent-to-created races. Open-only settings cannot
prevent Close. Schedule creation provides a non-mutating next-fire review through
the existing scheduler parser, including timezone and UTC bounds. Replacement
also reads the exact existing schedule, discloses activation and fire-count reset,
and requires its reviewed state/version. Editing the definition or waiting more
than five minutes invalidates the review. Draft fingerprints detect changes;
they are not credentials and do not replace CSRF or command/key authorization.

Policy fields are enabled only where they apply: FIFO mode on a state override,
dynamic indexes and max active duration on type defaults. A changed policy draft shows unsaved status and
warns before navigation. Duration previews retain exact milliseconds alongside
human units without converting large integers through JavaScript Number. Account
profiles likewise enable only relevant scope controls and show their effective
access. All account mutation forms use the shared duplicate-submit guard.

Freshness advances only when every supplied live component can be applied.
Focused content remains stable without claiming that older content was refreshed.
States places exceptions first and keeps its identity columns visible while
scrolling. Due rows reuse already-loaded FIFO lanes for blocker context; Signals
separates record coverage from inspected histories and matching event counts.

### Investigation And Safe Editing

Retention has two distinct views: bounded sampled candidates and an explicit
global cleanup review. Cleanup's limit is one aggregate command budget across
shards, not a limit per shard. Review never executes cleanup, and does not claim
an exact impact count. Execution requires a matching limit, a review no older
than five minutes, a new confirmation, and the usual ACL/CSRF checks. Errors
retain the limit; candidate counts distinguish omitted rows from no matches.

Schedule editing hydrates a single authorized definition and checks its original
state/version before review and again through the guarded mutation. Untouched
target metadata and compatible scheduling options survive a one-field edit.
Replacing a schedule reactivates it and resets its fire count; this is explicit
in review. Policy and schedule forms group related decisions, offer exact unit
conversion without floating-point rounding, and provide deliberate discard.
Returned policy, ACL, and governance drafts remain unsaved; secrets are never
echoed. Governance error rendering performs no implicit follow-up reads, and
fresh review requires independent authorization.

Consensus gets committed position from Raft server status and completed applied
position from storage, not the server's queued-apply position. Its bounded reads
keep partial/unavailable data distinct from zero lag. Doctor, KV, Slow Log, and
Command Catalog similarly distinguish collection failure, absence, and no
samples. Client investigation retains at most 500 matches and scans at most
10,000 registry entries per request, without messaging every connection. A
deleted continuation requires an explicit restart; a connection listing is not
a transactional snapshot under churn.

Recent rates reuse existing overview snapshots. Counters remain exact integers
until bounded BigInt subtraction; the tab retains at most 31 observations over
60 seconds. Counter resets, sampling changes, restarts, and gaps over 15 seconds
restart the window. There is no extra polling, server cache, or persistent
browser storage. Expandable interval history exposes the observations rather
than implying a server-side monitoring time series.

FQL reference examples are prepared in regression tests. Empty-result recovery
only edits the query draft; it never runs a broader query automatically. State
and worker chart caps disclose omitted groups and their table ordering. Signals
is unmeasured until scanned, and terminal failure takes precedence over generic
completion color. Shared live-status controls preserve their node and focus
through component replacement, so Resume can trigger the first refresh.

The seventh-review browser checks and rate reducer tests run without a server:

```sh
NODE_PATH="${TMPDIR:-/tmp}/ferricstore-browser-tools/node_modules" \
  node tools/dashboard-presentation-seventh-check.mjs
node tools/dashboard-seventh-rates-test.mjs
```

`scripts/dashboard_management_seventh_fixture.exs` generates populated render
fixtures without store writes. Its replacement-review fingerprint is expressly
invalid for mutations. Browser checks of these fixtures complement, but do not
replace, route, ACL, stale-version, and real-store integration tests.

`tools/dashboard-seventh-served-check.mjs` checks 29 routes at three desktop
widths and exercises live refresh, recent observations, query recovery, raw
reference access, client validation, schedule hydration, and review-only cleanup
against an isolated demo. It blocks all mutation transport other than the
explicit `review_cleanup` POST. The hydration case requires a disposable schedule
named `review-seventh-schedule`, targeting `scheduled_audit` in partition `system`
with a JSON payload containing `nightly`. Do not run fixture harnesses against a
production instance.

The extracted-source regression harnesses exercise these contracts without
starting another application or modifying the demo:

```sh
NODE_PATH="${TMPDIR:-/tmp}/ferricstore-browser-tools/node_modules" \
  node tools/dashboard-shell-hardening-check.mjs
NODE_PATH="${TMPDIR:-/tmp}/ferricstore-browser-tools/node_modules" \
  node tools/dashboard-management-third-check.mjs
NODE_PATH="${TMPDIR:-/tmp}/ferricstore-browser-tools/node_modules" \
  node tools/dashboard-query-triage-third-check.mjs
```

Lineage is table-first. Its summary counts only the visible current page; the
optional relationship preview is capped at 40 loaded records and labels that
boundary. Next-page links preserve the id, relation, partition, and limit, while
submitting a changed lookup starts over without the old cursor. Query quality
is retained, and omitted value references lead to explicit detail inspection,
not a claim that the workflow has no values. Failed queries never render empty
success statistics. Recovery keeps the same bounded two-query budget and shows
per-source quality and continuation availability without automatically fetching
more pages. Manual Signals scans display their captured UTC time and stay paused.

On a development machine, start the isolated synthetic demo. It disables
protected mode; do not expose it as a production deployment. In the main
checkout it uses dashboard port 4000 and native port 6389. Linked Git worktrees
keep their OS-assigned development ports. Each demo creates a fresh data
directory under that checkout's `tmp/` and prints its actual URLs and data path.

```sh
mix run --no-start --no-halt scripts/run_dashboard_demo.exs
```

For a worktree, pass the printed dashboard URL to browser harnesses through
`DASHBOARD_URL` (or `QUERY_BASE_URL` where documented below). Do not reuse a
different checkout's URL or `FERRICSTORE_DASHBOARD_DEMO_DATA_DIR`. An explicit
data-directory override is retained across runs and must belong to this demo.

The browser harness expects this demo's workflow IDs and Chrome. Install its
Playwright dependency outside the repository, then run:

```sh
npm --prefix "${TMPDIR:-/tmp}/ferricstore-browser-tools" install --no-save playwright@1.60.0 axe-core
NODE_PATH="${TMPDIR:-/tmp}/ferricstore-browser-tools/node_modules" \
  node tools/dashboard-workflow-check.mjs --screenshots
NODE_PATH="${TMPDIR:-/tmp}/ferricstore-browser-tools/node_modules" \
  node tools/dashboard-management-check.mjs
NODE_PATH="${TMPDIR:-/tmp}/ferricstore-browser-tools/node_modules" \
  node tools/dashboard-fifo-check.mjs
NODE_PATH="${TMPDIR:-/tmp}/ferricstore-browser-tools/node_modules" \
  node tools/dashboard-lineage-check.mjs
NODE_PATH="${TMPDIR:-/tmp}/ferricstore-browser-tools/node_modules" \
  DASHBOARD_URL=http://localhost:4000 node tools/dashboard-shell-review-check.mjs
NODE_PATH="${TMPDIR:-/tmp}/ferricstore-browser-tools/node_modules" \
  DASHBOARD_URL=http://localhost:4000 node tools/dashboard-shell-served-check.mjs
NODE_PATH="${TMPDIR:-/tmp}/ferricstore-browser-tools/node_modules" \
  DASHBOARD_URL=http://localhost:4000 node tools/dashboard-query-review-check.mjs
NODE_PATH="${TMPDIR:-/tmp}/ferricstore-browser-tools/node_modules" \
  node tools/dashboard-shell-fifth-check.mjs
NODE_PATH="${TMPDIR:-/tmp}/ferricstore-browser-tools/node_modules" \
  QUERY_BASE_URL=http://localhost:4010 node tools/dashboard-query-fifth-check.mjs
```

The fifth query check runs source-rendered regressions without a server when
`QUERY_BASE_URL` is omitted. Its optional served cases expect the isolated
`invoice_dispatch` FIFO examples. The shell check uses independent synthetic
browser contexts and does not contact a running store. Both close their contexts.

The shell harness requires a disposable demo. It creates and cancels schedules,
checks keyboard/help and clipboard success/failure, retained scope, live filtering,
absent records, typed export, sticky headers, desktop accessibility, and asset
cache reuse. Live count/disappearance cases use explicit transport fixtures;
server-side scope and absence behavior are covered by `shell_review_test.exs`.
The served-shell check also verifies loaded assets, first-Tab skip navigation,
tooltip geometry under 200% CSS zoom, and explicit pause/resume. The query check
covers stable paging with and without JavaScript, bounded scope navigation,
accessible predicate controls, and truthful copy failure. These are desktop
checks; CSS zoom coverage is not a native browser-zoom certification.

The Lineage harness covers desktop layout, keyboard preview controls, required
scope, explicit value inspection, invalid-cursor recovery, and paused Signals
scans. Set `DASHBOARD_LINEAGE_TEST_ROOT` and `DASHBOARD_LINEAGE_TEST_PARTITION` to
an isolated 45-record fixture to additionally exercise the preview cap and
three real result pages. HTTP cursor integration is also covered by
`lineage_review_test.exs` without requiring the demo fixture.

`tools/dashboard-detail-actions-check.mjs` exercises real Signal/Rewind POSTs
with and without JavaScript, draft recovery, stale-version rejection, query date
correction, and protected history access. It requires two fresh isolated servers
and `DASHBOARD_MUTATION_FIXTURES=1`; it intentionally mutates only its fixtures.
For each server, seed `detail-browser-js`, `detail-browser-nojs`,
`detail-browser-stale`, `detail-browser-rewind`, and `detail-browser-history` with
type `dashboard_action_review` and partition `review-detail`. Create each in
`queued`, send signal `historical-review-signal`, then transition to `ready`.
The protected server needs users `review-reader` (`+FLOW.GET`) and
`review-historian` (`+FLOW.GET`, `+FLOW.HISTORY`), both with `-@all`, `%R~*`, and
password `review-password`. These credentials are for local disposable tests
only. Set `DASHBOARD_URL` and `DASHBOARD_PROTECTED_URL` to the respective servers.
Do not enable these mutation checks against an existing user environment.

Set `HEADFUL=1` for a visible browser, `DASHBOARD_URL` for a different demo URL,
or `DASHBOARD_OUT_DIR` for an output directory. The harness checks filters, all
ten Guided operations, FQL/Explain, validation, journal navigation, polling
recovery, duplicate-submit guards, and lazy value requests. Expiry and value
responses are browser fixtures; server authorization is covered by ExUnit.

The management harness runs additional real-save and indexed-query integration
checks with `DASHBOARD_MANAGEMENT_FIXTURES=1`. Use an isolated fixture server:
`management_review_policy` has state `queued` in FIFO mode with eight retries;
`management_review_metadata` indexes state metadata key `risk`. In partition
`management-review`, create queued records `mr-spaced` with risk `" high "`,
`mr-plain-a` and `mr-plain-b` with risk `"high"`, and `mr-empty` with risk `""`.
The checks modify only that fixture policy's history limit. The ordinary demo
does not contain these fixtures, so those three checks are explicitly skipped
unless enabled. Equivalent real-store regressions run in ExUnit without a demo.
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
