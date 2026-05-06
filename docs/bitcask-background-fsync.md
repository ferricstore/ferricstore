# Bitcask Durability: Checkpointed Writes + Full File/Dir Fsync

This design covers the complete Bitcask durability story end-to-end:

1. **Active-log checkpointer** — replace per-apply fsync in the Raft state
   machine with a background, shared mechanism (amortized to ≤ 10s).
2. **File/dir fsync coverage** — every namespace-mutating site (rotation,
   compaction, hint files, prob structures, dedicated-file promotion,
   shard init, manifest persistence) gets a matching fsync so crash
   recovery is consistent.
3. **Unify with the pre-existing shard-level `fsync_needed` mechanism** —
   the Shard GenServer already runs a deferred `v2_fsync_async` timer;
   the new checkpointer is NOT a parallel second system, it replaces
   the duplicate ad-hoc logic.

**Status**: foundations landed, integration work pending. See
[Appendix B](#appendix-b-priority-ranked-follow-up-work).

## Already landed

| | What | Where |
|---|------|-------|
| ✅ | `NIF.v2_fsync_dir(path)` directory-fsync helper | `bitcask/src/lib.rs::fsync_dir` |
| ✅ | 10 Rust edge-case tests for `fsync_dir` | `lib.rs::fsync_dir_tests` |
| ✅ | `prob_fsync` helper + per-write fsync in bloom/cuckoo/cms/topk | `bitcask/src/lib.rs::prob_fsync` + each module |
| ✅ | 8 Rust edge-case tests for `prob_fsync` | `lib.rs::prob_fsync_tests` |
| ✅ | 11 Elixir durability-invariant tests for prob commands | `test/ferricstore/prob_durability_test.exs` |
| ✅ | Release-mode allocation-test flake fixed (pre-existing) | `async_io.rs::large_allocation_failure_in_task_is_contained` |

## Problem

Every FerricStore write incurs **two fsyncs**:

1. **Ra WAL fsync** (`fdatasync` via patched `ra_log_wal`) — durable consensus.
2. **Bitcask fsync** (`v2_fsync` inside `StateMachine.apply/3`) — durable
   data file after `v2_append_batch_nosync`.

The Bitcask fsync:
- Runs synchronously inside the Ra state-machine apply/3 callback.
- Blocks the Ra server ~100–200µs per batch on NVMe.
- Is **redundant** — data is already in the fsynced Ra WAL. On crash,
  replay rewrites any lost Bitcask bytes during recovery.

Profiling on Azure confirmed Ra servers spending significant time in
`v2_fsync` despite the WAL being the source of truth.

## Proposal

Remove the per-apply Bitcask fsync from `StateMachine.apply/3`. Data files
become page-cache-only between checkpoints. A single **`BitcaskCheckpointer`
GenServer per shard** fsyncs the shard's active file every N seconds
(default 10s), matching how Postgres treats heap/indexes (WAL durability;
data files checkpointed periodically).

```
           Today (synchronous per-batch fsync)
Ra apply  →  v2_append_batch_nosync  →  v2_fsync  →  ETS update  →  return
            ~10µs                        ~100–200µs     ~1µs

           Proposed (background checkpoint)
Ra apply  →  v2_append_batch_nosync  →  ETS update  →  return
            ~10µs                        ~1µs

Checkpointer (every 10s, per shard):
   if fsync_needed flag is set:
     clear flag
     v2_fsync_async(active_file_path)
     on success → emit telemetry; on error → re-raise flag for next tick
```

## Durability model

| | Today | Proposed |
|---|-------|----------|
| **Process crash** | Ra WAL durable, Bitcask durable. Replay is a no-op from the state machine's PoV. | Ra WAL durable, Bitcask may be ≤ 10s stale. Replay re-applies post-checkpoint commands and rebuilds Bitcask exactly. **No data loss.** |
| **Kernel panic** | Ra WAL durable, Bitcask durable. | Ra WAL durable, Bitcask ≤ 10s stale. Same as above. **No data loss**; recovery replays up to 10s of writes. |
| **Acknowledged client data** | Always durable. | Always durable (same) — Ra WAL is the truth. |
| **Replay time after kernel panic** | ~milliseconds | ~10–30s at 40 K writes/s (2.4M entries). Each replay entry is cheap (`append_batch_nosync` + ETS insert). |

Linux kernel writeback (`vm.dirty_writeback_centisecs` default 500 =
every 5s) already drains dirty pages to disk asynchronously regardless
of fsync cadence, so the "up to 10s un-durable" claim is the worst
case — in steady state far less is unsynced.

## Comparison with Postgres

| Aspect | Postgres | FerricStore (proposed) |
|--------|----------|------------------------|
| WAL durability | per-commit `fsync` of `pg_wal` | per-batch `fdatasync` of Ra WAL |
| Data-file durability | checkpoints (default 5 min, 1 GB `max_wal_size`) | checkpoints (default 10s per shard) |
| Recovery source | WAL replay from last checkpoint | Ra WAL replay from last applied index |
| Crash safety | full WAL replay recovers all ACKed data | same |
| Checkpoint I/O | dirty buffers flushed by bgwriter + checkpointer | `v2_fsync_async` per active file |

Postgres defaults to 5 minutes; we pick 10s to start because Bitcask
files are append-only and smaller, and we want faster recovery bounds.
Tunable via `:checkpoint_interval_ms`.

## Reconciling with the existing shard-level `fsync_needed`

**This was an earlier gap in the design.** The Shard GenServer already
has `fsync_needed: false` in state + a deferred-fsync timer at
`shard.ex:729-737` that calls `NIF.v2_fsync_async` when the flag is
set. That mechanism covers the **async write path**
(`Shard.Flush.flush_pending_nosync` and BitcaskWriter casts).

The new `BitcaskCheckpointer` **replaces** that ad-hoc shard-level timer
rather than running in parallel. One shared mechanism, one shared flag,
covering all paths. Concretely:

- **Delete** the `fsync_needed` field from `Ferricstore.Store.Shard.state`.
- **Delete** the timer branch in the shard's `handle_info(:flush_timer, ...)`
  that fires `v2_fsync_async` based on `fsync_needed`.
- **Move** the flag to a per-shard `:atomics` ref stored in the
  `FerricStore.Instance` struct so it can be read/written from any
  process: state machine (writer), BitcaskWriter (writer), and
  `BitcaskCheckpointer` (reader + clearer).

Design detail: `atomics` gives cross-process atomic read-modify-write
at ~5ns, doesn't require a GenServer hop, and its ref is shareable via
the Instance struct.

## Design

### Components

1. **Remove** `quorum_fsync(state)` from `StateMachine.flush_pending_writes/1`.
2. **Remove** the shard GenServer's `fsync_needed` flag + timer
   (replaced by the shared `BitcaskCheckpointer`).
3. **Add** `Ferricstore.Store.BitcaskCheckpointer` — one GenServer per
   shard, supervised under `Ferricstore.Store.ShardSupervisor`.
4. **Add** per-shard `checkpoint_flags` atomics ref to
   `FerricStore.Instance`. State machine + BitcaskWriter set their bit
   after every `v2_append_batch_nosync`. The checkpointer clears the
   bit before firing the async fsync (see rotation-race note below).
5. **Call-site fsync coverage** — see the per-site fixes in
   [File/dir fsync coverage](#filedir-fsync-coverage).
6. **Graceful shutdown** — see [Shutdown ordering](#shutdown-ordering).

### The `checkpoint_flags` atomic ref

```elixir
# FerricStore.Instance
defstruct [
  ...,
  :checkpoint_flags,  # :atomics ref, 1 slot per shard
]

# Owner:   FerricStore.Instance.build/2 creates the atomics ref
# Writers: StateMachine.flush_pending_writes, BitcaskWriter.flush
# Reader + clearer: BitcaskCheckpointer (only place that clears)
```

State machine:
```elixir
defp flush_pending_writes(state) do
  case Process.put(:sm_pending_writes, []) do
    [] -> :ok
    pending ->
      NIF.v2_append_batch_nosync(state.active_file_path, batch)
      # One atomic write per apply batch, not per command — the
      # state machine is single-threaded per shard.
      :atomics.put(state.instance_ctx.checkpoint_flags,
                   state.shard_index + 1, 1)
      update_ets_locations(...)
  end
end
```

Checkpointer:
```elixir
def handle_info(:checkpoint, state) do
  ctx = FerricStore.Instance.get(state.instance_name)
  flag_idx = state.shard_index + 1
  if :atomics.get(ctx.checkpoint_flags, flag_idx) == 1 do
    # Clear BEFORE firing the async fsync. Any write that races with
    # this clear re-sets the flag, so the NEXT tick picks it up. The
    # current fsync may miss some bytes — acceptable because Ra WAL
    # is the source of truth.
    :atomics.put(ctx.checkpoint_flags, flag_idx, 0)
    NIF.v2_fsync_async(self(), state.next_corr, active_path(ctx, state.shard_index))
    ...
  end
  Process.send_after(self(), :checkpoint, state.interval_ms)
  {:noreply, state}
end

# Retry on failure
def handle_info({:tokio_complete, _corr, {:error, reason}}, state) do
  # Re-raise the flag so the next tick retries, AND set disk pressure.
  ctx = FerricStore.Instance.get(state.instance_name)
  :atomics.put(ctx.checkpoint_flags, state.shard_index + 1, 1)
  DiskPressure.set(ctx, state.shard_index)
  :telemetry.execute([:ferricstore, :bitcask, :checkpoint],
                     %{shard_index: state.shard_index},
                     %{status: :error, reason: reason})
  Logger.error("Checkpointer shard=#{state.shard_index}: fsync failed: #{inspect(reason)}")
  {:noreply, state}
end
```

This avoids fsync syscalls on idle shards and self-heals transient
disk errors.

### Active-file rotation (shared log)

**Location**: `shard/flush.ex:maybe_rotate_file/1`

**Invariant**: each shard has exactly one active file at a time
(`Ferricstore.Store.ActiveFile` maps `shard_index → {file_id, path, dir}`,
single entry). Old files are read-only after rotation.

**Why the checkpointer tracks only ONE file per shard**: old files are
finalized. Once they're fsynced at rotation handoff, no new bytes land
in them. The checkpointer just reads `ActiveFile.get(idx)` each tick
so rotations-between-ticks are handled naturally.

**The rotation-race problem**: between the last append to file `N` and
`ActiveFile.publish(N+1)`, the checkpointer might tick and fsync `N+1`
(new, with nothing to flush) instead of `N` (old, with dirty tail). Fix:
rotation handler fsyncs the outgoing file SYNCHRONOUSLY before publishing.

Required `maybe_rotate_file` changes:

```elixir
def maybe_rotate_file(state) do
  if state.active_file_size >= state.max_active_file_size do
    # 1. Fsync the outgoing file data synchronously. This transfers
    #    "responsibility for file N" from the checkpointer's future
    #    work to "done, don't worry about it".
    NIF.v2_fsync(state.active_file_path)

    # 2. Write hint file + fsync its data.
    write_hint_for_file(state, state.active_file_id)   # now calls sync inside the NIF

    # 3. Create new file.
    new_id = state.active_file_id + 1
    sp = state.shard_data_path
    new_path = ShardETS.file_path(sp, new_id)
    File.touch!(new_path)

    # 4. Make the new filename durable — otherwise a kernel panic
    #    between touch! and first append can lose the filename entry.
    NIF.v2_fsync_dir(sp)

    # 5. Publish the new active file to ActiveFile registry.
    Ferricstore.Store.ActiveFile.publish(state.index, new_id, new_path, sp)

    # ... rest unchanged (scheduler notify, state update)
  else
    state
  end
end
```

### Compaction

**Location**: `shard.ex:handle_call({:run_compaction, file_ids}, ...)`

Current sequence per file id:

1. Read live offsets from `NNNNN.log` → write to `compact_<fid>.log`
   via `NIF.v2_copy_records`, which internally calls `writer.sync()` ✅
2. `File.rename!(dest, source)` — atomic rename.
3. `File.rm(source)` for files with zero live data.
4. On copy error: `File.rm(dest)`.

Gaps are all **dir-entry durability** (file data is already fsynced):

- After 2 (rename): directory entry change not durable. Recovery
  auto-cleans stale `compact_*.log` so worst case is "unmerged file
  on disk" — no data loss, merge work wasted.
- After 3 (rm): dir entry not durable. Deleted file reappears on crash.
- After 4 (error cleanup): same class, idempotent.

**Fix**: one `NIF.v2_fsync_dir(sp)` call after the whole reduce loop
(covers every rename/rm in the batch). Amortized because every op in
compaction touches the same shard dir.

### Hint files

**Location**: `shard/flush.ex:write_hint_for_file/2` →
`NIF.v2_write_hint_file`

Hint files accelerate recovery by providing a pre-built keydir index.

Current behavior: data written to hint file via `HintWriter` but the
NIF does NOT `sync_data` after `commit()` — hint data sits in page
cache. On kernel panic the hint can be empty, partial, or absent.

Impact: recovery falls back to full log scan (slow, correct). No data
loss; degraded recovery speed.

Fix (two places):

1. Inside `HintWriter::commit` (Rust), call `file.sync_data()` before
   returning `Ok(())`.
2. After `v2_write_hint_file` returns in Elixir, call
   `NIF.v2_fsync_dir(state.shard_data_path)` so the new `.hint`
   filename is durable.

This is already effectively covered by step 4 of the rotation fix
above (since hints are written during rotation).

### Probabilistic structures (bloom / cuckoo / cms / topk)

✅ **Landed**. Every write NIF calls `prob_fsync()` before `:ok`.

Remaining gap: `*_file_create` writes the new file's data (fsynced
via `sync_all()`) but doesn't dir-fsync the parent. A kernel panic
right after `BF.RESERVE` can vanish the file; next `BF.ADD` fails
ENOENT.

Fix: after each `*_file_create` NIF returns `{:ok, :ok}`, the Elixir
caller calls `NIF.v2_fsync_dir(prob_dir)`. (Alternatively bake this
into the Rust NIF — either works, Elixir-side is simpler.)

### Dedicated (promoted) Bitcask files

Each promoted collection (hash / set / zset with > 100 entries) gets
its own `data/dedicated/shard_N/{type}:{sha256}/` directory with its
own log files. Writes use `v2_append_record` / `v2_append_tombstone` /
`v2_append_batch`, all of which call `writer.sync()` — so per-write
file-data fsync is covered.

The remaining gaps are all around **namespace mutation** (create /
rename / rm of files and dirs) and **non-atomic state transitions**.

#### D1. `Promotion.open_dedicated/4` — dedicated dir + initial file

Current:
```elixir
File.mkdir_p!(path)
active_file = Path.join(path, "00000.log")
unless File.exists?(active_file) do
  File.touch!(active_file)
end
```

Gaps:
- `mkdir_p!` — parent `dedicated/shard_N/` not fsynced. Low severity
  (idempotent).
- `touch!(active_file)` — dedicated dir's new entry for `00000.log`
  not fsynced. **Medium severity: the new empty log file may disappear
  on kernel panic; next `v2_append_batch` fails on reopen.**

Fix:
```elixir
File.mkdir_p!(path)
NIF.v2_fsync_dir(Path.dirname(path))   # dir entry for new subdir
active_file = Path.join(path, "00000.log")
unless File.exists?(active_file) do
  File.touch!(active_file)
  NIF.v2_fsync_dir(path)               # dedicated dir's entry for 00000.log
end
```

#### D2. `Promotion.promote_collection!/6` — 🚨 non-atomic, runs outside Raft

The sequence today:

```elixir
# Step 1: write dedicated data (fsynced inside the NIF)
NIF.v2_append_batch(dedicated_active, batch)

# Step 2: tombstone every compound key in the SHARED log
Enum.each(entries, fn {key, _, _} ->
  NIF.v2_append_tombstone(active_path, key)     # each tombstone fsynced
end)

# Step 3: write the marker in the SHARED log
NIF.v2_append_record(active_path, mk, type_str, 0)  # marker fsynced
```

Each step is individually durable, but the three steps are not atomic
with respect to each other. Promotion is triggered **outside the Raft
state machine** (from Shard GenServer in `shard/compound.ex:634`), so
Raft replay cannot redo a promotion — recovery reads whatever is
physically on disk.

Failure modes on kernel panic:

| Crash point | Shared log | Dedicated dir | Recovery outcome |
|-------------|------------|----------------|-------------------|
| After 1, before any of 2 | compound keys still live | has everything | data duplicated; `recover_promoted` doesn't process dedicated (no marker) — no data loss |
| Partway through 2 | some tombstoned, others live | has everything | partial view — some keys appear deleted, rest readable from shared. **Ugly, but recoverable by re-running promotion.** |
| All of 2 done, before 3 | everything tombstoned, no marker | has everything | **DATA APPEARS DELETED** from client — marker missing so `recover_promoted` doesn't see the dedicated dir. **Real data loss.** |
| After 3 | tombstones + marker | has everything | normal ✅ |

There are two viable fixes. Option A (safer + lower-risk) is shippable
alongside the checkpointer; Option B is the longer-term architectural
cleanup.

**Option A — marker-first with recovery-logic update**:

```elixir
# 1. Write marker FIRST in shared log. Durability sequence pt.1:
#    marker committed → recovery knows "this collection was being promoted"
NIF.v2_append_record(active_path, mk, type_str, 0)

# 2. Write dedicated data. If we crash before this completes,
#    recover_promoted must fall back to the compound keys in the
#    shared log (which aren't tombstoned yet).
NIF.v2_append_batch(dedicated_active, batch)

# 3. Tombstone compound keys in shared log.
Enum.each(entries, fn {key, _, _} ->
  NIF.v2_append_tombstone(active_path, key)
end)
```

**Recovery logic changes required** (otherwise Option A isn't safe
either):

```elixir
# In recover_promoted (currently in promotion.ex):
#
#   if marker exists:
#     open dedicated dir
#     if dedicated is populated: trust dedicated (normal path)
#     if dedicated is empty / missing:
#       fall back to scanning compound keys in shared log
#       (they weren't tombstoned, they're still authoritative)
```

Without the recovery fallback, a crash after step 1 and before step 2
leaves marker pointing at an empty dedicated dir and the collection
still appears empty.

**Option B — atomic promotion via Raft**:

Route `{:promote, type, redis_key, entries}` through the state machine
as a single command. `apply/3` performs marker + dedicated write +
tombstones within a single with_pending_writes batch. Replicas all
see the same event via the Raft log, so replay is deterministic and
cross-replica divergence disappears.

Bigger refactor. Recommended for a follow-up milestone, not this one.

#### D3. Dedicated-file rotation

**Location**: `shard/compound.ex` around `File.touch!(new_file)`.

Dedicated files rotate on size threshold, same shape as shared
rotation. Same two gaps:
- outgoing active file not fsynced synchronously before switching,
- dedicated dir not fsynced after new-file creation.

Fix: same pattern as rotation of the shared log (synchronous
`v2_fsync` + `v2_fsync_dir`).

Note: until dedicated files join the checkpointer, per-write
`writer.sync()` in the append NIFs covers most of the durability —
the remaining gap is only about the filename of the NEW file during
rotation.

#### D4. `Promotion.cleanup_promoted!/5` — destructor

```elixir
NIF.v2_append_tombstone(active_path, mk)   # marker tombstone (fsynced)
File.rm_rf!(path)                          # delete dedicated dir
```

Gap: `rm_rf!` doesn't dir-fsync the parent. On kernel panic the
dedicated dir reappears; `recover_promoted` won't process it (marker
is tombstoned) — but disk space leaks and the next promotion with the
same hash collides with orphan files.

Fix: `NIF.v2_fsync_dir(Path.join([data_dir, "dedicated", "shard_N"]))`
after `rm_rf!`.

#### D5. `list_log_files` recovery-time cleanup

**Location**: `promotion.ex:list_log_files/1`.

Recovery-time code deletes stale `compact_*.log` temp files from
crashed compactions. Same dir-fsync gap; idempotent (next recovery
re-rms the same files). Low severity.

Fix: `NIF.v2_fsync_dir(dir)` after the cleanup loop.

### Shard init

**Location**: `shard.ex` around `File.mkdir_p!(path)` +
`File.touch!(active_file_path)`.

First-time shard startup creates the shard dir + `00000.log`. Same
dir-fsync-on-create pattern. Rare (first boot) but worth closing
for consistency.

Fix:
```elixir
File.mkdir_p!(path)
NIF.v2_fsync_dir(Path.dirname(path))
File.touch!(active_file_path)
NIF.v2_fsync_dir(path)
```

### Manifest / Config

**Locations**: `merge/manifest.ex:74-80`, `config.ex:400-405`.

Both use `tmp + File.rename` for atomic persistence. The rename is
atomic file-to-file but the dir-entry change isn't fsynced. A crash
mid-rename leaves either old-or-new (atomicity preserved) but
durability of the completed rename isn't.

Fix (lower priority — self-healing on replay):
```elixir
File.write(tmp_path, content)
NIF.v2_fsync(tmp_path)                     # data durable
File.rename(tmp_path, final_path)
NIF.v2_fsync_dir(Path.dirname(final_path)) # rename durable
```

### Shutdown ordering

Graceful shutdown must avoid two hazards:

1. **Checkpointer fsyncing stale data** while the state machine is
   still applying new writes (the fsync could miss the last byte).
2. **Supervisor kills** checkpointer mid-fsync → BEAM terminates
   before Tokio completes fsync → dirty pages remain un-durable.

Ordering, enforced by `Ferricstore.Store.ShardSupervisor` via
`:rest_for_one` strategy and child ordering:

1. Stop the Batcher (drain pending Raft submissions).
2. Stop the state machine (ra.stop_server) — no more applies.
3. Flush BitcaskWriter (drain queued writer work).
4. Checkpointer performs a **synchronous** `v2_fsync` of the active
   file (not `v2_fsync_async` — we need it to complete before
   termination).
5. Checkpointer terminates.

If the BEAM is killed hard (SIGKILL) this ordering doesn't apply,
but Ra WAL replay recovers correctly — no data loss, just longer
recovery time.

### Configuration

```elixir
config :ferricstore,
  checkpoint_interval_ms: 10_000   # default 10s
  checkpoint_idle_ms: 250          # wait for a short idle gap before fsync
  checkpoint_max_delay_ms: 180_000 # force fsync after about 3 minutes
```

`checkpoint_max_delay_ms` bounds kernel-panic replay work. Ra WAL remains
the acknowledged durability boundary; Bitcask fsync is a replay-time
checkpoint. Keeping the forced checkpoint window near 3 minutes avoids
injecting periodic active-file fsync stalls into continuous hot write paths.

Note: the **call-site fsync coverage** (rotation handoff, dir fsyncs,
prob-file fixes) is NOT gated on this flag. Those fixes are pure
additions with no perf regression — they run unconditionally.

### Telemetry

```elixir
# Per checkpoint:
:telemetry.execute([:ferricstore, :bitcask, :checkpoint],
                   %{duration_us: d, shard_index: idx},
                   %{status: :ok | :error, reason: term() | nil})

# When the flag timer skips (no writes since last tick):
:telemetry.execute([:ferricstore, :bitcask, :checkpoint_skipped],
                   %{shard_index: idx}, %{})

# When disk pressure triggers from failed fsync:
# (reuses existing DiskPressure telemetry)
```

Operators can plot checkpoint latency, frequency, skipped ticks, and
error rates.

## Failure modes

### Checkpoint fsync fails (disk full, I/O error)

- Log at WARN.
- `DiskPressure.set(ctx, shard_index)` — this already throttles new writes.
- Re-raise `fsync_needed` flag so next tick retries.
- Do NOT crash the checkpointer.

### Process dies mid-checkpoint

- Tokio thread finishes the fsync independently (off-BEAM). Result
  message arrives at a dead pid → discarded.
- Supervisor restarts the checkpointer; next tick retries via flag.

### Kernel panic between checkpoints

- Bitcask may be ≤ 10s stale on that shard.
- Ra WAL is durable; replay rebuilds the stale portion on restart.
- No ACKed data is lost.

### Very slow disk (fsync takes > 10s)

- Next tick would fire while the previous async fsync is still pending.
- Checkpointer tracks `in_flight: boolean` and skips ticks when an
  fsync is outstanding.
- If fsync takes > 10s consistently, we emit a telemetry warning and
  set DiskPressure.

### Dirty writeback doesn't happen

In theory the Linux kernel's `vm.dirty_writeback_centisecs` timer keeps
dirty pages flowing to disk even without our fsync calls. In practice
on containers/VMs this setting can be disabled — hence we fsync
explicitly rather than relying on kernel-side behavior.

## Testing

1. **Unit** (Rust): checkpointer timer fires at the configured interval.
2. **Unit** (Rust): `fsync_needed` flag correctly gates fsync calls —
   no syscall when idle, syscall after a write.
3. **Integration**: 1M writes → kill BEAM mid-checkpoint → restart →
   verify all writes are readable (Ra replay rebuilds Bitcask).
4. **Integration**: artificially slow `v2_fsync_async` (Tokio sleep);
   verify no piling of in-flight fsyncs.
5. **Chaos**: kernel panic simulation (forcibly kill the VM without
   clean shutdown); restart; measure recovery time. Target <30s for
   2.4M replayed entries.
6. **Benchmark**: async write throughput before/after. Target
   10–30 % improvement at saturation (Ra servers no longer block
   100–200µs per apply batch).
7. **Call-site fsync tests** (already landed for prob; add for
   rotation, compaction, promotion once those fixes land).

## Migration / rollout

1. Land the checkpointer with `checkpoint_max_delay_ms` high enough that hot
   paths normally fsync on idle gaps, not during continuous writes.
2. Enable in dev/staging with `180_000`. Observe telemetry.
3. Benchmark production-like workloads.
4. Keep the default at `180_000` for production once verified.
5. Post-rollout: lower to 60s, 30s, or 10s if recovery time targets require it
   (shorter interval → shorter replay window).

The **call-site fsync fixes** (rotation handoff, dir fsyncs, prob-file
per-write fsync, D1/D4/D5 dir fsyncs, D2 promotion reorder) ship
incrementally as pure correctness improvements — no config flag
needed.

## Open questions (resolved)

- ~~One checkpointer per shard vs global?~~ **Resolved: per-shard.**
  Each shard has its own Tokio thread via `v2_fsync_async`; fsyncs
  parallelize naturally. Simpler than a global coordinator.
- ~~Should checkpoint rotate the log when a shard has been idle for a
  long time?~~ **Resolved: no.** Log rotation is size-driven.
- ~~fdatasync vs fsync?~~ **Resolved: fdatasync** (`sync_data` in
  Rust). Append-only files don't change metadata on append, so
  `fdatasync` is correct and ~30% faster.

## Expected impact

- **Throughput**: per-shard write throughput up (Ra servers spend
  less time in `v2_fsync`). 10–30 % improvement at saturation based
  on published data from similar systems.
- **Latency**: p99 write latency drops (no 100–200µs fsync stall per
  apply batch).
- **Recovery time**: slightly longer (≤ 10s of WAL to replay) but
  still seconds, not minutes.
- **Code simplicity**: state machine loses the fsync dance in
  `apply/3`; shard GenServer loses the ad-hoc `fsync_needed` timer;
  checkpointer is a small self-contained GenServer.
- **Durability guarantees**: NO weaker than today. The Ra WAL
  remains the source of truth; what changes is when dirty Bitcask
  pages hit disk, not whether ACKed writes survive crash.

---

## Appendix A: At-a-glance gap summary

All gaps are described in the body above. Use this table as the
cross-reference index: one row per file-system operation, linked to
the section that covers it.

| # | Site | File op | File-data fsync today | Dir fsync today | Status |
|---|------|---------|----------------------|-----------------|--------|
| 1 | `state_machine.flush_pending_writes` | append-nosync | per-apply `quorum_fsync` ✅ | — | **Replaced by checkpointer** |
| 2 | `Shard.Flush.flush_pending_nosync` | append-nosync | deferred `v2_fsync_async` via shard `fsync_needed` | — | **Unified into checkpointer** |
| 3 | `Shard.Flush.flush_pending_sync` | append-sync | per-call ✅ | — | **Consider switching to `_nosync` + checkpoint** |
| 4 | `maybe_rotate_file` (shared) — old active | flip | ❌ | — | **Fix: synchronous `v2_fsync` before publish** |
| 5 | `maybe_rotate_file` (shared) — `touch!(new_path)` | create | N/A (empty) | ❌ | **Fix: `v2_fsync_dir` after touch** |
| 6 | `write_hint_for_file` | create `.hint` | ❌ | ❌ | **Fix: `sync_data` in NIF + `v2_fsync_dir`** |
| 7 | `v2_copy_records` compaction dest | create + write | `writer.sync()` ✅ | — | OK ✅ |
| 8 | Compaction `File.rename!` | rename | N/A | ❌ | **Fix: `v2_fsync_dir` after reduce loop** |
| 9 | Compaction `File.rm(source)` all-dead | remove | N/A | ❌ | Same fix ↑ |
| 10 | Compaction error `File.rm(dest)` | remove | N/A | ❌ | Same fix ↑ |
| 11 | `Promotion.open_dedicated` — `mkdir_p!` | create dir | N/A | ❌ | **Fix: `v2_fsync_dir(parent)`** |
| 12 | `Promotion.open_dedicated` — `touch!(00000.log)` | create | N/A | ❌ | **Fix: `v2_fsync_dir(dedicated_dir)`** |
| 13 | `Promotion.promote_collection!` | multi-step | per-NIF ✅ | ❌ | 🚨 **Fix: marker-first reorder + recovery fallback (Option A) or move under Raft (Option B)** |
| 14 | Dedicated-file rotation | flip + create | partial (per-write) | ❌ | **Fix: same as shared rotation** |
| 15 | `Promotion.cleanup_promoted!` — `rm_rf!` | remove dir | N/A | ❌ | **Fix: `v2_fsync_dir` after rm** |
| 16 | `list_log_files` stale-compact cleanup | remove | N/A | ❌ | **Fix: `v2_fsync_dir` after cleanup loop** |
| 17 | bloom/cuckoo/cms/topk `*_file_add` etc. | pread/pwrite | `prob_fsync` ✅ | — | Done ✅ |
| 18 | bloom/cuckoo/cms/topk `*_file_create` | create | `sync_all` ✅ | ❌ | **Fix: `v2_fsync_dir(prob_dir)` after create** |
| 19 | `state_machine.ex` prob delete `File.rm` | remove | N/A | ❌ | Small follow-up (replay covers) |
| 20 | Shard init | create dir + file | N/A | ❌ | **Fix: two `v2_fsync_dir` calls** |
| 21 | `Merge.Manifest.write_manifest` | write + rename | ❌ | ❌ | Low priority |
| 22 | `Config.save_config` | write + rename | ❌ | ❌ | Low priority |
| 23 | Ra WAL fdatasync | — | per-batch ✅ | N/A | Out of scope (ra patch) |

Legend: ❌ = missing fsync, ✅ = already fsynced, — = not applicable.

---

## Appendix B: Priority-ranked follow-up work

Ranked by "damage if it fires" × "probability":

| # | Gap | Impact | Effort | PR |
|---|-----|--------|--------|---|
| 1 | **Promotion atomicity (D2)** — marker-first reorder + `recover_promoted` fallback logic | 🚨 Real data loss on crash mid-promotion | Medium — reorder + ~20 lines recovery | Separate PR |
| 2 | **Rotation handoff fsync + dir fsync** (rows 4, 5, 6) | Last writes before rotation lost; hint file missing on kernel panic | Small — 3 NIF calls in `maybe_rotate_file` | Can bundle with checkpointer PR |
| 3 | **Checkpointer + unify with shard-level `fsync_needed`** (rows 1, 2) | Performance: remove per-apply fsync | Medium — new GenServer + atomics ref + delete shard timer | Main PR |
| 4 | **Promotion `open_dedicated` dir-fsyncs (D1)** | Fresh dedicated file can vanish on kernel panic → data loss on first append | Tiny — 2 `v2_fsync_dir` calls | Bundle with #1 |
| 5 | **Compaction dir fsync** (rows 8–10) | Merged files can "un-merge" on crash; recovery wastes merge work | Tiny — 1 `v2_fsync_dir` after compaction | Small PR |
| 6 | **Prob-file create dir fsync** (row 18) | Fresh prob file can vanish on kernel panic | Tiny — call after each `*_file_create` | Small PR |
| 7 | **Dedicated-file rotation (D3)** | Same as shared rotation but for promoted collections | Small — copy rotation fix pattern | Bundle with #2 or separate |
| 8 | **`Promotion.cleanup_promoted!` dir fsync (D4)** | Orphan dedicated dirs wasting disk + collision risk | Tiny | Bundle with #1 |
| 9 | **Shard init dir fsyncs** (row 20) | First-boot filename can vanish on kernel panic (extremely rare) | Tiny | Bundle with #2 |
| 10 | **Atomic promotion under Raft (Option B for D2)** | Architectural cleanup; eliminates a whole class of race | Medium-Large | Follow-up milestone |
| 11 | **Manifest / Config dir fsync** (rows 21–22) | Cross-shard state can be lost on crash; self-healing on replay | Tiny | Low priority |
| 12 | **`sync_data` instead of `sync_all` at prob-create** | Tiny perf win | Trivial | Housekeeping |

---

## Appendix C: Non-goals

- **Torn writes inside a single syscall**: Linux guarantees page-level
  atomicity (`write()` ≤ 4 KB is torn-proof). Larger writes can be
  torn; Bitcask record framing + CRC detects and discards torn
  records on recovery.
- **Silent disk corruption** (bit rot after fsync): out of scope —
  needs per-record CRC + background scrubbing. Bitcask already has
  per-record CRC.
- **Device-level cache**: `fdatasync` on NVMe flushes the disk write
  cache on modern hardware. Budget hardware with lying caches is out
  of scope.
- **Replication lag / quorum failures**: Ra handles them; this design
  only covers local durability on a single node.
- **Application-level crash consistency** (e.g., "this set of writes
  must be all-or-nothing"): needs MULTI/EXEC atomicity, separate
  design.
