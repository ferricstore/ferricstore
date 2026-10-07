# Curated review evidence

This directory commits compact summaries, the rewrite-index memory proofs, and
the source bundles needed by archived benchmark variants. Benchmark scripts
write their complete output and local run logs here, so historical reports can
also refer to files retained only in the original investigation workspace.
Those local paths are evidence identifiers, not additional passing CI checks.

The archived variants are explicit benchmark controls. Normal production and
default benchmark modes use the retained implementation. Git-based legacy
controls pin `e5f59ba7959710773729812343ae9bcd62d8d15f` (0.11.23), rather than
moving with the latest commit. A shallow checkout must fetch that revision
before using those controls.

For current release-preparation status and CI, see
[`docs/release-0.11.24.md`](../../docs/release-0.11.24.md). The review reports
record workload-specific tradeoffs and distinguish successful snapshots from
application-stop returns.
