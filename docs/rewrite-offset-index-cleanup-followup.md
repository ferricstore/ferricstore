# Retiring temporary WARaft rewrite indexes

2026-10-06. The directory lifecycle leaked rebuildable RAM state even after a
failed rewrite removed its staging directory. The 8,192-position limit was per
directory: each retry used a new name and retained another 8,192 positions plus
its `dir_marker` and `last_index` rows. This explains the reported
23 × 8,194 = **188,462** abandoned rows; these are offsets, not new Flow records.

## Reproduction

`bench/rewrite_index_baseline_probe.exs` loads the committed provider at
`e5f59ba7959710773729812343ae9bcd62d8d15f` into an isolated VM, using private
source/BEAM files and a fresh fixture. It injects an error at the final staging
directory sync, after registering a full-cap rewrite. Three actual failures
leave **8,194**, **16,388**, and **24,582** temporary rows, respectively. Every
corresponding staging directory is absent from disk.

Artifact: `bench/results/rewrite-index-baseline-proof-2.json`. This is a defect
reproduction on the committed baseline, not a matched throughput comparison
with the accumulated working-tree corrections.

## Correction

The segment-log provider now retires a directory's runtime state after a
successful removal or directory rename. Failed removals/renames retain the
existing error behavior. Retirement removes the directory's offset rows,
configuration/trim/latest-config caches, prune state, and obsolete temporary
generation rows. It closes the calling process's cached sidecar readers.
Canonical-path generations advance so readers in other processes reopen stale
descriptors on their next access; those process-local reader caches remain
bounded to 16 entries.

This uses a **memory-only** retirement path. The recovery reset
`clear_offset_registry_for_dir/1` also removes derived disk sidecars, so it must
not be used to clean up renamed/replacement directories. Payload, sidecar,
rewrite-marker and directory-sync ordering retain their existing recovery
protocol.

Sidecar trust follows the physical log through a rename. In particular, a
rollback carries the source's `offset_index_untrusted` fence back to the live
path. A failing-first regression caught the initial cleanup clearing this
fence: a CRC-valid slot can still point to an older repeated projection frame.
The corrected rename retains verified fallback until a complete rebuild.

## Reclaiming existing abandoned indexes

The explicit API is:

```elixir
:ferricstore_waraft_spike_segment_log.reclaim_abandoned_rewrite_indexes()
# {:ok, %{directories: count, offset_entries: count}}
```

`Ferricstore.OperationalGuard` runs it on its first capacity check and every
30 seconds thereafter. An existing guard state also arms maintenance on its
next check after code upgrade. When the guard is disabled, the explicit API
remains available. A successful nonempty pass emits:

```text
[:ferricstore, :waraft, :segment_log, :rewrite_index_reclaim]
measurements: %{directories: count, offset_entries: count}
```

Reclamation selects directory marker rows in pages of 128 and bulk-deletes the
eligible offsets in one registry traversal. Only missing directories whose
basename is `segment_log.rewrite.staging.<digits>` or
`segment_log.rewrite.backup.<digits>` qualify. Existing directories, symlinks,
unsafe/error paths, malformed names, and canonical paths are retained.

Every live writer owner protects its directory, including cached raw-file and
legacy writer entries. Maintenance neither waits for nor invalidates live
writers. It rechecks candidates, removes only exact dead-owner registry rows,
and checks absence/ownership again before reclaiming derived state. It does
not delete files or act as a disk-artifact recovery sweeper.

Discovery uses the existing offset `dir_marker` rows. The API does not scan all
`persistent_term` values to discover hypothetical metadata-only orphans. For
the reproduced abandoned indexes it also retires their associated metadata and
generations; future rewrite removals retire state even before offsets exist.

## Resource proof

`bench/rewrite_index_reclaim_probe.exs` exercises ten real full-cap failures on
the corrected provider, then seeds 23 historical abandoned full-cap indexes.
It records workspace source hashes and the loaded provider MD5.

| Observation | Offset entries | ETS bytes |
| --- | ---: | ---: |
| After each of ten populated failed rewrites | 8,194 | 1,496,504 |
| Canonical index plus 23 abandoned indexes | 196,656 | 33,092,216 |
| After reclaiming all 23 abandoned indexes | 8,194 | 2,938,296 |

All ten failed stages contain 8,194 rows before the injected failure and leave
only the canonical index afterward. Reclamation removes exactly **188,462
rows**, reducing the reported ETS allocation by **30,153,920 bytes** (~30.2 MB).
The allocator's post-reclaim footprint remains above the initial baseline;
this is not a claim that node RSS returns exactly to baseline. These are ETS
memory measurements, not RSS or a workload latency gate.

Artifacts:

- `bench/results/rewrite-index-reclaim-proof-2.json`: corrected production mode.
- `bench/results/rewrite-index-raw-control-proof-1.json`: identical entry/memory
  outcomes in a privately compiled raw-scan control.

The raw-scan loader now changes only the fallback descriptor's read-ahead
option. Loading an entire historical `part_05.hrl` would remove the new cleanup
helpers and mix incompatible source cohorts. Publication identities now include
all seven provider sections and the operational guard.

## Verification and status

The final targeted run passes **104 tests** in 6.3 seconds, seed 873483:

- rewrite cleanup, real full-cap failures, 23-index reclamation;
- 129-directory pagination, metadata/generation retirement, dead-owner cleanup;
- live/in-flight/legacy writer protection and a real paused raw writer;
- successful replacement, interrupted swap rollback and untrusted-sidecar fence;
- cross-process reader reopening and canonical/active/malformed/symlink isolation;
- operational guard maintenance, segment-log recovery, reader security and
  bounded fallback scans.

Log: `bench/results/rewrite-index-cleanup-final-verification-2.log`. Fixture
cleanup removes only the unique test root's global rows/caches so full-cap
tests do not contaminate later guard measurements.

An additional backend/application/retention lifecycle run exceeded its outer
**600-second** limit without an ExUnit completion result. Its progress log is
`bench/results/rewrite-index-lifecycle-verification.log`; it is **incomplete**,
not a passing full-suite checkpoint. Subsequent scoped verification completes:

| Scope | Result | Log |
| --- | --- | --- |
| Snapshot durability/rollback and Flow locator recovery | 11 passed, 390 excluded; 100.1 s | `rewrite-index-snapshot-recovery-verification.log` |
| Application lifecycle, retention integration, storage source guards and both performance guards | 43 passed; 136.1 s | `rewrite-index-application-guards-verification.log` |
| Three-node cluster suite | 15 passed; 89.6 s | `rewrite-index-cluster-verification.log` |

These are **173 passing tests/checks** across the completed scoped runs, not
the default whole-application suite. All logs are under `bench/results/`, seed
873483. The specified static checks pass:

```text
mise exec -- mix format --check-formatted
mise exec -- mix compile --warnings-as-errors
mise exec -- mix credo suggest --only warning --min-priority high --files-excluded 'apps/*/test/**/*'
mise exec -- mix credo suggest --only warning --min-priority high --ignore-checks UnusedListOperation,ExpensiveEmptyEnumCheck
git diff --check
git diff --exit-code -- mix.lock
```

This records the two scoped Credo profiles, not an unrestricted clean-Credo
claim. Earlier broad timing failures
and long snapshot handoff failures remain documented in the
[offset-scan follow-up](offset-fallback-scan-followup.md).

The correction is being prepared for 0.11.24; follow the
[release status](release-0.11.24.md) for PR/CI and approval. It has not been
deployed to or used to reclaim indexes on the user's running server, and no
long-workload shutdown improvement is claimed from this memory fix.
