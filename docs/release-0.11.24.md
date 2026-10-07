# FerricStore 0.11.24 release preparation

Branch: `codex/release-0.11.24-runtime-hardening`.

Status: preparing the pull request and CI. Merge to `main` and publication of
`v0.11.24` require the owner's explicit approval after CI verification.

## Scope

- Repair temporary rewrite-index retention and reclaim existing abandoned
  indexes conservatively; preserve live writers, sidecars and rollback trust.
- Harden durable promoted publication, compaction relocation/removal ordering,
  atomic single-field HSET, and bounded explicit append groups.
- Bound Flow reconciliation pages and shared source waits; coalesce handled
  history requests while retaining durable flush/watermark barriers.
- Repair caller/waiter/lease cleanup and native inbound memory accounting.
- Retain the measured embedded dispatch, metadata-read, heartbeat-fold and
  verified fallback-scan corrections, with documented workload tradeoffs.
- Align startup storage-call budgets and snapshot runtime-lock exclusion with
  the validated recovery protocol.

The native wire protocol, SDK minimum server version, and installed WARaft
dependency stay compatible. Automatic synchronous HSET coalescing defaults off.
The independent WARaft timer candidate remains outside this release.

## Verification

Before release preparation, the current cleanup source passed 173 scoped
tests/checks, including 15 three-node cluster checks and both performance
guards. Formatting, warnings-as-errors compilation, and the two existing
warning-level Credo profiles passed. The
[cleanup follow-up](rewrite-offset-index-cleanup-followup.md) contains exact
full-cap reproduction and ETS allocation evidence.

The earlier complete 14,291-test checkpoint predates the later history/page,
read-ahead and cleanup changes. Subsequent broader local verification has
incomplete/time-limited runs; this release's complete verification is the PR's
Linux/macOS and specialized CI matrix, recorded here once finished.

Long snapshot writer-handoff timeouts remain an open workload limitation. This
release does not infer healthy snapshots from a successful application-stop
return or claim a universal durable-write latency improvement. See the
[overall performance review](overall-performance-review-0.11.23.md) for retained
and rejected policies and historical evidence.

## Publication sequence after approval

1. Merge the approval-ready PR to `main` and verify its main-branch checks.
2. Tag the verified merge commit as `v0.11.24`.
3. Verify the precompiled NIF release matrix, core Hex publication and multi-arch
   container publication, including image startup smoke tests.
4. Record the release URL and publication outcomes.
