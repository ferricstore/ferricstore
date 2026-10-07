# FerricStore 0.11.24 release preparation

Branch: `codex/release-0.11.24-runtime-hardening`.

Pull request: [#48](https://github.com/ferricstore/ferricstore/pull/48).

Status: CI verification and fixes in progress. Merge to `main` and publication
of `v0.11.24` require the owner's explicit approval after CI verification.

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

## CI follow-up

The first CI run found newly published advisories affecting Mint 1.10.1 and
Rust 1.99 strict compiler/Clippy findings. The HTTP client minimum is raised to
patched Mint 1.10.2. Native test assertions retain their exact empty/nonempty
conditions with explicit diagnostics, and the peak-count test uses the
equivalent `fetch_max` atomic supported by the existing Rust baseline.
Local verification passes all three Rust 1.99 Clippy targets, 667 native Rust
tests, 19 targeted HTTP tests, both Hex dependency audits and lock hygiene.
Bitcask Clippy also passes on Rust 1.98. The regenerated lock selects Mint
1.10.2 and its compatible HPAX 1.1.0 dependency; WARaft remains 0.1.0.

The next matrix passes both builds, all server partitions, architecture,
performance guards, HTTP quality and all five official SDKs. Cluster and
shard-kill failures expose test peers creating publication latch tables inside
short-lived RPC workers. Their contexts now have a supervised owner with
explicit teardown; the owner-lifetime regression and all 30 backend cluster
cases pass locally. The shared-file-server suspension guard retains its
500-ms completion condition and now captures blocked callers on failure.

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
