# Demo clarity review — 2026-09-12

## Scope and implementation

Impeccable-guided refinement of all 20 public static demo routes. Six Luna agents
contributed bounded code batches; the main agent integrated and corrected the
changes, reviewed the browser behavior, and verified the release.

- One short, plain-language explanation per demo; less repeated introductory copy.
- Selected mode, a primary action, and a matching status at the entry point.
- Native experiment visuals retained; duplicate generic mechanism rails removed.
- Code, example metrics, and technical boundaries available through disclosures.
- Manual navigation stops autoplay; end states do not wrap silently to step one.
- Mode switches reset stories rather than unexpectedly starting them.
- Recovery actions gated where they require a prior run or failure.
- More accurate mode-specific labels and consistent reset/timer behavior.
- Existing measured benchmark values retained; whole-chain timings are not
  presented as measured individual-step timings.

## Verification performed

`node scripts/check-static-demos.mjs` passes: 20 routes, 26 classic JavaScript
files parsed, and 220 local source links/assets resolved. `git diff --check` passes.

All 20 demo routes were loaded at 390×844 and 1440×900. The 40-view browser
check found one page heading per route, no page-level horizontal overflow,
no broken in-page anchor targets, and no browser console errors. Each primary
action fit within the first viewport. Initial checks also found no unnamed
visible form controls or failed image loads. Desktop sliders that had undersized
hit areas were expanded to 44px and rechecked.

Browser interaction coverage:

- Order workflow: both recovery modes, manual stages, play/pause, replay, and
  terminal-step controls.
- AI approval: host restart while waiting, approval, request changes, and replay.
- Travel: hotel failure, flight failure, and successful booking through final
  states, including disabled Next at completion.
- Billing: decline and recovery jumps, keyboard slider navigation, and reset.
- Reservation and canary: both modes and each numbered story action.
- Parallel work: complete restart and saved-result recovery; invalid recovery
  actions disabled before the required stage.
- Agent limits: normal-run reset, budget limits, and outage protection in both
  modes.
- Split lab: both full crash paths; side-by-side receipts show two repeated
  states versus zero, with saved Plan/Search and old-write rejection.
- Retry examples: payment, AI work, and inventory scenarios through completion.
- Ownership fencing: pause, replacement takeover, and rejection of the old write.
- Architecture comparison: all four approaches through their terminal step.
- Benchmark: both recorded modes, their selected values, and selector state.
- Queue: clean-run start, crash, retry, and completion in both modes.
- Rate limit: burst, keyboard slider change, pause, resume, and reset.
- Messaging: live broadcast and stream acknowledgement; listener totals and
  completion readouts verified.
- Storage: memory pressure in both modes and disk-read behavior.
- Shared cache: uncoordinated burst versus one shared result through completion.
- Field expiry: both field actions and expiry in FerricStore mode; permanent
  name and role remain after expiry.
- Membership filter: registered and absent-key actions in both modes.
- Supporting details: expanded mobile accuracy disclosure without page overflow.

## Screenshots

- `demo-clarity-workflow-desktop.png` — 1440×900
- `demo-clarity-workflow-mobile.png` — 390×844

## Limits

This verifies the static UI and representative scenario paths, not every timing
interleaving. It does not run a real FerricStore server, execute the displayed
Python SDK examples, remeasure benchmarks, or validate every external source URL.
Automated DOM checks and keyboard interactions are not a full screen-reader or
real-user usability study. Native horizontally scrollable stage rails remain
intentional on small screens and are labeled as scrollable.
