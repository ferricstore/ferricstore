# Complete demo coverage follow-up — 2026-09-12

## What the follow-up found

The preceding release reached all 20 routes, but its outer-shell improvements
did not consistently reach the inner experiments. A visual review found dark
legacy canvases, white headings on pale backgrounds, 12–13px controls, and mode
switches that missed shared selectors when the switch was the root element.
This follow-up addresses those gaps rather than treating asset coverage as proof
of finished UI work. Impeccable guided the typography, contrast, and responsive
refinement; existing mechanisms, simulation logic, and recorded numbers remain.

## Implemented

- Consistent light reading surfaces inside all experiments; dark surfaces kept
  for code and activity logs, with matching foreground colors.
- Readable narrative headings, field names, status labels, and recovery receipts.
- 15px experiment buttons with 44px minimum height; 16px explanatory paragraphs
  and disclosure prose; 14px supporting labels and code.
- Root and nested scenario switches normalized, with clear selected states and
  mobile side padding. Existing disabled states remain distinct.
- More room for the benchmark sequence and restart/resume diagram.
- Mobile worker tiles use two columns; architecture stage labels stack rather
  than collide; slider labels and ticks have separate grid tracks.
- Cache-version update on the catalog and every demo HTML entry point.

## Route-by-route coverage

Each route below received a desktop and mobile visual inspection, primary-action
smoke test, and overflow check. The code toggle was opened wherever present;
all shared supporting disclosures were opened at 390×844.

| Route | Desktop 1440×900 | Mobile 390×844 |
| --- | --- | --- |
| workflow-explainer | Reviewed | Reviewed |
| ai-agent-workflow | Reviewed | Reviewed |
| travel-saga | Reviewed | Reviewed |
| subscription-dunning | Reviewed | Reviewed |
| ticket-reservation | Reviewed | Reviewed |
| canary-rollback | Reviewed | Reviewed |
| parallel-fanout | Reviewed | Reviewed |
| agent-loop | Reviewed | Reviewed |
| split-lab | Reviewed | Reviewed |
| idempotency-determinism | Reviewed | Reviewed |
| zombie-fencing | Reviewed | Reviewed |
| architecture-comparison | Reviewed | Reviewed |
| benchmark-explainer | Reviewed | Reviewed |
| hot-cold-storage | Reviewed | Reviewed |
| rate-limiting-stream | Reviewed | Reviewed |
| beginner-queue | Reviewed | Reviewed |
| cache-stampede | Reviewed | Reviewed |
| stream-vs-pubsub | Reviewed | Reviewed |
| hash-field-ttl | Reviewed | Reviewed |
| probabilistic-cache | Reviewed | Reviewed |

## Verification and limits

- No page-level horizontal overflow during the tested initial, running, expanded
  code, or expanded supporting-section views; no console errors in the audit tab.
- All primary actions remain in the first mobile viewport. Initial mobile checks
  found no failed images or experiment buttons below 14px text / 44px height.
- Shared disclosure paragraphs and list items did not overflow their containers.
- Text-contrast scan plus screenshots caught remaining legacy colors. The scan
  is a diagnostic, not a WCAG certification: color-mix/sRGB values, gradients,
  disabled controls, opacity, and emoji need visual interpretation.
- `node scripts/check-static-demos.mjs`: 20 routes, 26 JavaScript files,
  220 local links/assets pass. `git diff --check` passes.
- Intentional internal horizontal scrolling remains for long code, field tables,
  and stage rails. This is not page overflow.
- No SDK code, simulation logic, or benchmark measurements changed. This turn
  rechecked entry actions, not every timed scenario combination; the preceding
  scenario test coverage is recorded in `demo-clarity-2026-09-12.md`.
- Real screen-reader testing, user research, and backend/SDK integration tests
  remain outside this static visual verification.

Representative screenshots: `demo-inner-ticket-desktop.png` and
`demo-inner-ai-mobile.png` in this directory.
