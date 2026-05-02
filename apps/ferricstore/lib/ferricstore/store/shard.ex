defmodule Ferricstore.Store.Shard do
  @moduledoc """
  GenServer managing one Bitcask partition backed by an ETS hot-read cache.

  ## v2 Architecture: Pure Stateless NIFs

  All state lives in Elixir (ETS keydir + GenServer state). Rust NIFs are
  pure stateless functions: `v2_append_record`, `v2_pread_at`, `v2_fsync`,
  `v2_append_batch`, `v2_append_tombstone`, `v2_scan_file`, hint file I/O.
  No Rust-side Store resource, HashMap keydir, or Mutex.

  ## Write path: group commit

  1. The key is written to ETS immediately (reads see it at once).
  2. The entry is appended to an in-memory pending list.
  3. A recurring `:drain_pending` timer fires every `@flush_interval_ms`
     and calls `NIF.v2_append_batch_nosync/2` with all accumulated
     entries, then updates ETS entries with their disk locations
     (file_id, offset, value_size). This step moves bytes from BEAM
     memory to the kernel page cache — **no fsync**.
     Data-file durability is owned by `Ferricstore.Store.BitcaskCheckpointer`,
     which runs on its own, longer tick and issues `v2_fsync` against
     the same active file.
  4. File rotation occurs when the active file exceeds 256 MB.

  ## Read path: ETS bypass

  `Router.get/1` and `Router.get_meta/1` read ETS directly without going
  through this GenServer for hot (cached) keys. Cold keys (value=nil in ETS)
  have their disk location (file_id, offset) stored in the ETS 7-tuple,
  enabling direct `v2_pread_at` without scanning.

  ## ETS layout

  Each entry is a 7-tuple `{key, value, expire_at_ms, lfu_counter, file_id, offset, value_size}`
  where `expire_at_ms = 0` means the key never expires. The `file_id`, `offset`,
  and `value_size` fields enable cold reads without scanning, STRLEN on cold keys,
  and sendfile zero-copy. Expired entries are lazily evicted on read.

  ## Process registration

  Shards register under the name returned by
  `Ferricstore.Store.Router.shard_name/1`, e.g.
  `:"Ferricstore.Store.Shard.0"`.
  """

  use GenServer

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Store.Router
  alias Ferricstore.Store.Shard.Compound, as: ShardCompound
  alias Ferricstore.Store.Shard.ETS, as: ShardETS
  alias Ferricstore.Store.Shard.Flush, as: ShardFlush
  alias Ferricstore.Store.Shard.Lifecycle, as: ShardLifecycle
  alias Ferricstore.Store.Shard.NativeOps, as: ShardNativeOps
  alias Ferricstore.Store.Shard.Reads, as: ShardReads
  alias Ferricstore.Store.Shard.Transaction, as: ShardTransaction
  alias Ferricstore.Store.Shard.Writes, as: ShardWrites

  require Logger

  # How often (ms) to flush the pending write queue to disk.
  # 1ms gives up to 50k batched writes/s per shard (4 shards → 200k/s total).
  @flush_interval_ms 1

  # Default maximum active file size before rotation (256 MB).
  # Configurable via :max_active_file_size application env.
  @default_max_active_file_size 256 * 1024 * 1024

  # Default fragmentation thresholds for per-file dead bytes tracking.
  @default_fragmentation_threshold 0.5
  @default_dead_bytes_threshold 134_217_728

  defstruct [
    :ets,
    :keydir,
    :index,
    :data_dir,
    # Cached result of DataDir.shard_data_path(data_dir, index).
    # Computed once during init; avoids string concat on every cold read/flush.
    :shard_data_path,
    # FerricStore.Instance context — holds all per-instance refs (shard_names,
    # slot_map, keydir_refs, atomics, config) needed to route operations without
    # global state. Passed to Router.* calls instead of persistent_term lookups.
    :instance_ctx,
    :active_file_id,
    :active_file_path,
    :active_file_size,
    pending: [],
    pending_count: 0,
    flush_in_flight: nil,
    write_version: 0,
    sweep_at_ceiling_count: 0,
    sweep_struggling: false,
    promoted_instances: %{},
    # Per-file dead bytes tracking: %{file_id => {total_bytes, dead_bytes}}
    file_stats: %{},
    # Merge config overrides for fragmentation thresholds
    merge_config: %{},
    # Map from correlation_id => {from, key} for in-flight Tokio async reads.
    # Correlation IDs fix the LIFO ordering bug from the old list-based approach.
    pending_reads: %{},
    # Monotonically increasing counter for async read/write correlation IDs.
    next_correlation_id: 0,
    # Whether this shard has Raft infrastructure (Batcher + ra server).
    # Application-supervised shards (0-3) always have Raft. Isolated test
    # shards with ad-hoc indices use the direct write path instead.
    raft?: true,
    # Maximum active file size before rotation. Cached from Application env
    # at init time. Updated via handle_cast(:update_max_active_file_size, n).
    max_active_file_size: 256 * 1024 * 1024,
    writes_paused: false
  ]

  # -------------------------------------------------------------------
  # Public API
  # -------------------------------------------------------------------

  @doc """
  Starts a shard GenServer.

  ## Options

    * `:index` (required) -- zero-based shard index
    * `:data_dir` (required) -- base directory for Bitcask data files
    * `:flush_interval_ms` -- batch-commit interval in ms (default: #{@flush_interval_ms})
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    index = Keyword.fetch!(opts, :index)
    ctx = Keyword.get(opts, :instance_ctx)
    name = if ctx, do: Router.shard_name(ctx, index), else: :"Ferricstore.Store.Shard.#{index}"
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  # -------------------------------------------------------------------
  # GenServer callbacks
  # -------------------------------------------------------------------

  @impl true

  def init(opts) do
    # Supervised shutdown reaches terminate/2 only when the GenServer traps
    # exits. That path drains pending writes and writes the active hint file.
    Process.flag(:trap_exit, true)

    index = Keyword.fetch!(opts, :index)
    data_dir = Keyword.fetch!(opts, :data_dir)
    flush_ms = Keyword.get(opts, :flush_interval_ms, @flush_interval_ms)
    ctx = Keyword.get(opts, :instance_ctx)

    path = Ferricstore.DataDir.shard_data_path(data_dir, index)
    dir_created? = not Ferricstore.FS.dir?(path)
    Ferricstore.FS.mkdir_p!(path)

    if dir_created? do
      _ = NIF.v2_fsync_dir(Path.dirname(path))
    end

    # v2: scan data_dir for existing .log files, find highest file_id
    {active_file_id, active_file_size} = ShardLifecycle.discover_active_file(path)
    active_file_path = file_path(path, active_file_id)

    # Ensure the active file exists (touch it)
    # credo:disable-for-next-line Credo.Check.Refactor.UnlessWithElse
    file_created? =
      unless Ferricstore.FS.exists?(active_file_path) do
        Ferricstore.FS.touch!(active_file_path)
        true
      else
        false
      end

    if dir_created? or file_created? do
      _ = NIF.v2_fsync_dir(path)
    end

    # Create/clear named ETS tables.
    # Use instance-scoped names from ctx if available, else default naming.
    keydir_name =
      if ctx, do: elem(ctx.keydir_refs, index), else: :"keydir_#{index}"

    keydir =
      case :ets.whereis(keydir_name) do
        :undefined ->
          :ets.new(keydir_name, [
            :set,
            :public,
            :named_table,
            {:read_concurrency, true},
            {:write_concurrency, :auto},
            {:decentralized_counters, true}
          ])

        _ref ->
          :ets.delete_all_objects(keydir_name)
          # Reset off-heap binary byte counter for this shard
          if ctx != nil and ctx.keydir_binary_bytes != nil do
            :atomics.put(ctx.keydir_binary_bytes, index + 1, 0)
          end

          keydir_name
      end

    # Remove any leftover hot_cache table from a previous run.
    case :ets.whereis(:"hot_cache_#{index}") do
      :undefined -> :ok
      _ref -> :ets.delete(:"hot_cache_#{index}")
    end

    ets = keydir

    # v2: recover ETS keydir from hint files or by scanning log files BEFORE
    # starting Raft. This ensures cold entries ({key, nil, ..., fid, off, vsize})
    # are in ETS when ra replays WAL entries via apply/3. Without this, replayed
    # read-modify-write commands (INCR, APPEND, etc.) see ETS misses during
    # replay and start from nil instead of the correct prior value.
    # 7-tuple format: {key, value, expire_at_ms, lfu_counter, file_id, offset, value_size}
    # Must run BEFORE recover_promoted so PM: markers are in ETS.
    ShardLifecycle.recover_keydir(path, keydir, index)

    # Start the Raft server for this shard (unless explicitly disabled).
    raft? =
      if Keyword.get(opts, :raft_enabled, true) do
        ShardLifecycle.start_raft_if_available(
          index,
          path,
          active_file_id,
          active_file_path,
          ets,
          ctx.name
        )
      else
        false
      end

    # Recover promoted collection instances
    promoted = Ferricstore.Store.Promotion.recover_promoted(path, keydir, data_dir, index, ctx)

    # Migrate existing prob files: scan prob dir for files without
    # corresponding metadata markers in the keydir. Write markers so
    # DEL can clean up prob files and BF.INFO/CMS.INFO can recover metadata.
    ShardLifecycle.migrate_prob_files(path, keydir, index)

    # Publish active file metadata to ActiveFile registry
    Ferricstore.Store.ActiveFile.publish(ctx, index, active_file_id, active_file_path, path)

    # Compute per-file dead bytes stats from disk sizes + ETS live data.
    file_stats = compute_file_stats(path, keydir)

    # Read merge config for fragmentation thresholds
    merge_config_overrides = Keyword.get(opts, :merge_config, %{})

    merge_config = %{
      fragmentation_threshold:
        Map.get(
          merge_config_overrides,
          :fragmentation_threshold,
          @default_fragmentation_threshold
        ),
      dead_bytes_threshold:
        Map.get(merge_config_overrides, :dead_bytes_threshold, @default_dead_bytes_threshold)
    }

    schedule_drain_pending(flush_ms)
    ShardLifecycle.schedule_expiry_sweep()
    ShardLifecycle.schedule_frag_check()
    max_file_size = if ctx, do: ctx.max_active_file_size, else: @default_max_active_file_size

    {:ok,
     %__MODULE__{
       ets: keydir,
       keydir: keydir,
       index: index,
       data_dir: data_dir,
       shard_data_path: path,
       instance_ctx: ctx,
       active_file_id: active_file_id,
       active_file_path: active_file_path,
       active_file_size: active_file_size,
       pending: [],
       flush_in_flight: nil,
       promoted_instances: promoted,
       file_stats: file_stats,
       merge_config: merge_config,
       raft?: raft?,
       max_active_file_size: max_file_size
     }, {:continue, {:flush_interval, flush_ms}}}
  end

  defp file_path(shard_path, file_id), do: ShardETS.file_path(shard_path, file_id)

  # -------------------------------------------------------------------
  # handle_continue
  # -------------------------------------------------------------------

  @impl true
  def handle_continue({:flush_interval, ms}, state) do
    # Store flush interval in process dictionary so handle_info can reschedule.
    Process.put(:flush_interval_ms, ms)
    {:noreply, state}
  end

  # -------------------------------------------------------------------
  # handle_call — reads
  # -------------------------------------------------------------------

  @impl true
  def handle_call({:get, key}, from, state), do: ShardReads.handle_get(key, from, state)

  def handle_call({:get_file_ref, key}, _from, state),
    do: ShardReads.handle_get_file_ref(key, state)

  def handle_call({:get_meta, key}, from, state), do: ShardReads.handle_get_meta(key, from, state)

  # Compound key scan: returns all live entries matching a prefix.
  # Used by HSCAN, SSCAN, ZSCAN via the compound_scan store callback.
  # Uses :ets.select match spec instead of :ets.foldl full-table scan.
  def handle_call({:scan_prefix, prefix}, _from, state) do
    state =
      if ShardETS.prefix_has_pending_cold?(state.keydir, prefix) do
        ShardFlush.flush_pending_for_read(state)
      else
        state
      end

    results = prefix_scan_entries(state, prefix, state.shard_data_path)
    {:reply, Enum.sort_by(results, fn {field, _} -> field end), state}
  end

  # Count entries matching a compound key prefix.
  # Uses :ets.select match spec instead of :ets.foldl full-table scan.
  def handle_call({:count_prefix, prefix}, _from, state) do
    {:reply, prefix_count_entries(state, prefix), state}
  end

  def handle_call({:exists, key}, _from, state), do: ShardReads.handle_exists(key, state)

  def handle_call(:keys, _from, state), do: ShardReads.handle_keys(state)

  # Returns the active file info (file_id + path). Used by callers that
  # need to reach the shard's current Bitcask file without copying the
  # entire Shard state via :sys.get_state.
  def handle_call(:get_active_file, _from, state) do
    {:reply, {state.active_file_id, state.active_file_path}, state}
  end

  # Returns the current write_version for WATCH support.
  # Combines the Shard's internal counter (incremented for async/non-raft writes)
  # with the shared atomic counter (incremented by Router for quorum bypass writes).
  # This ensures WATCH detects mutations regardless of which write path was used.
  def handle_call({:get_version, _key}, _from, state) do
    ctx = state.instance_ctx

    shared =
      if ctx do
        size = :counters.info(ctx.write_version).size
        if state.index < size, do: :counters.get(ctx.write_version, state.index + 1), else: 0
      else
        Ferricstore.Store.WriteVersion.get(state.index)
      end

    {:reply, state.write_version + shared, state}
  end

  # -------------------------------------------------------------------
  # handle_call — writes
  # -------------------------------------------------------------------

  # Delete all entries matching a compound key prefix.
  # Uses :ets.select match spec instead of :ets.foldl full-table scan.
  def handle_call({:delete_prefix, prefix}, _from, state) do
    ShardWrites.handle_delete_prefix(prefix, state)
  end

  def handle_call({:put, _key, _value, _expire_at_ms}, _from, %{writes_paused: true} = state) do
    {:reply, {:error, "ERR shard writes paused for sync"}, state}
  end

  def handle_call({:forwarded_quorum, origin_node, command}, from, state) do
    forwarded_from = Ferricstore.Raft.Batcher.remote_origin_from(origin_node, from)
    previous_origin = Process.get(:ferricstore_forward_origin)
    Process.put(:ferricstore_forward_origin, origin_node)

    try do
      handle_forwarded_quorum(command, forwarded_from, state)
    after
      if previous_origin == nil do
        Process.delete(:ferricstore_forward_origin)
      else
        Process.put(:ferricstore_forward_origin, previous_origin)
      end
    end
  end

  def handle_call({:put, key, value, expire_at_ms}, from, state) do
    ShardWrites.handle_put(key, value, expire_at_ms, from, state)
  end

  # Atomic increment: reads current value, parses as integer, adds delta, writes back.
  # Returns {:ok, new_integer} or {:error, reason}.
  def handle_call({:incr, key, delta}, from, state) do
    ShardWrites.handle_incr(key, delta, from, state)
  end

  # Atomic float increment: reads current value, parses as float, adds delta, writes back.
  # Returns {:ok, new_float_string} or {:error, reason}.
  def handle_call({:incr_float, key, delta}, from, state) do
    ShardWrites.handle_incr_float(key, delta, from, state)
  end

  # Atomic append: reads current value (or ""), appends suffix, writes back.
  # Returns {:ok, new_byte_length}.
  def handle_call({:append, key, suffix}, from, state) do
    ShardWrites.handle_append(key, suffix, from, state)
  end

  # Atomic get-and-set: returns old value (or nil), sets new value.
  def handle_call({:getset, key, new_value}, from, state) do
    ShardWrites.handle_getset(key, new_value, from, state)
  end

  # Atomic get-and-delete: returns value (or nil), deletes key.
  def handle_call({:getdel, key}, from, state) do
    ShardWrites.handle_getdel(key, from, state)
  end

  # Atomic get-and-update-expiry: returns value, updates TTL.
  # expire_at_ms = 0 means PERSIST (remove expiry).
  def handle_call({:getex, key, expire_at_ms}, from, state) do
    ShardWrites.handle_getex(key, expire_at_ms, from, state)
  end

  # Atomic set-range: overwrites portion of string at offset with value.
  # Zero-pads if key doesn't exist or string is shorter than offset.
  # Returns {:ok, new_byte_length}.
  def handle_call({:setrange, key, offset, value}, from, state) do
    ShardWrites.handle_setrange(key, offset, value, from, state)
  end

  def handle_call({:delete, key}, from, state) do
    ShardWrites.handle_delete(key, from, state)
  end

  # -------------------------------------------------------------------
  # handle_call — compound operations (promotion-aware)
  # -------------------------------------------------------------------

  def handle_call({:compound_get, redis_key, compound_key}, _from, state) do
    ShardCompound.handle_compound_get(redis_key, compound_key, state)
  end

  def handle_call({:compound_batch_get, redis_key, compound_keys}, _from, state) do
    ShardCompound.handle_compound_batch_get(redis_key, compound_keys, state)
  end

  def handle_call({:compound_get_meta, redis_key, compound_key}, _from, state) do
    ShardCompound.handle_compound_get_meta(redis_key, compound_key, state)
  end

  def handle_call({:compound_batch_get_meta, redis_key, compound_keys}, _from, state) do
    ShardCompound.handle_compound_batch_get_meta(redis_key, compound_keys, state)
  end

  def handle_call({:compound_put, redis_key, compound_key, value, expire_at_ms}, _from, state) do
    ShardCompound.handle_compound_put(redis_key, compound_key, value, expire_at_ms, state)
  end

  def handle_call({:compound_delete, redis_key, compound_key}, _from, state) do
    ShardCompound.handle_compound_delete(redis_key, compound_key, state)
  end

  def handle_call({:compound_scan, redis_key, prefix}, _from, state) do
    ShardCompound.handle_compound_scan(redis_key, prefix, state)
  end

  def handle_call({:compound_count, redis_key, prefix}, _from, state) do
    ShardCompound.handle_compound_count(redis_key, prefix, state)
  end

  def handle_call({:compound_delete_prefix, redis_key, prefix}, _from, state) do
    ShardCompound.handle_compound_delete_prefix(redis_key, prefix, state)
  end

  # -------------------------------------------------------------------
  # handle_call — native commands: CAS, LOCK, UNLOCK, EXTEND, RATELIMIT.ADD
  # -------------------------------------------------------------------

  def handle_call({:cas, key, expected, new_value, ttl_ms}, _from, state) do
    ShardNativeOps.handle_cas(key, expected, new_value, ttl_ms, state)
  end

  def handle_call({:lock, key, owner, ttl_ms}, _from, state) do
    ShardNativeOps.handle_lock(key, owner, ttl_ms, state)
  end

  def handle_call({:unlock, key, owner}, _from, state) do
    ShardNativeOps.handle_unlock(key, owner, state)
  end

  def handle_call({:extend, key, owner, ttl_ms}, _from, state) do
    ShardNativeOps.handle_extend(key, owner, ttl_ms, state)
  end

  def handle_call({:ratelimit_add, key, window_ms, max, count}, _from, state) do
    ShardNativeOps.handle_ratelimit_add(key, window_ms, max, count, state)
  end

  # 6-tuple variant: includes pre-computed now_ms from Router.raft_write.
  # In cluster mode this MUST go through Raft (replicated) just like the
  # 5-tuple variant — otherwise a follower's ratelimit-add lands locally
  # only and other nodes never see the increment. Falls back to direct in
  # non-Raft mode.
  def handle_call({:ratelimit_add, key, window_ms, max, count, _now_ms}, _from, state) do
    ShardNativeOps.handle_ratelimit_add(key, window_ms, max, count, state)
  end

  # -------------------------------------------------------------------
  # handle_call — list operations
  # -------------------------------------------------------------------

  def handle_call({:list_op, key, operation}, _from, state) do
    ShardNativeOps.handle_list_op(key, operation, state)
  end

  def handle_call({:list_op_lmove, src_key, dst_key, from_dir, to_dir}, _from, state) do
    ShardNativeOps.handle_list_op_lmove(src_key, dst_key, from_dir, to_dir, state)
  end

  # -------------------------------------------------------------------
  # handle_call — transaction execution (single-shard atomic batch)
  # -------------------------------------------------------------------

  def handle_call({:tx_execute, queue, sandbox_namespace}, _from, state) do
    ShardTransaction.handle_tx_execute(queue, sandbox_namespace, state)
  end

  # Check if a redis_key has been promoted to dedicated storage.
  def handle_call({:promoted?, redis_key}, _from, state) do
    {:reply, Map.has_key?(state.promoted_instances, redis_key), state}
  end

  # -------------------------------------------------------------------
  # handle_call — pause/resume writes (cluster data sync)
  # -------------------------------------------------------------------

  def handle_call({:pause_writes}, _from, state) do
    state = await_in_flight(state)
    state = flush_pending_sync(state)
    {:reply, :ok, %{state | writes_paused: true}}
  end

  def handle_call({:resume_writes}, _from, state) do
    {:reply, :ok, %{state | writes_paused: false}}
  end

  def handle_call(:enable_raft, _from, state) do
    {:reply, :ok, %{state | raft?: true}}
  end

  # -------------------------------------------------------------------
  # handle_call — stats, merge, admin
  # -------------------------------------------------------------------

  def handle_call(:shard_stats, _from, state) do
    state = await_in_flight(state)
    state = flush_pending_sync(state)
    sp = state.shard_data_path
    key_count = :ets.info(state.keydir, :size)

    # Compute file-level stats for merge scheduler
    {total_bytes, live_bytes, dead_bytes, file_count} =
      case Ferricstore.FS.ls(sp) do
        {:ok, files} ->
          log_files = Enum.filter(files, &String.ends_with?(&1, ".log"))
          fc = length(log_files)

          total =
            Enum.reduce(log_files, 0, fn name, acc ->
              case File.stat(Path.join(sp, name)) do
                {:ok, %{size: s}} -> acc + s
                _ -> acc
              end
            end)

          # Estimate: live = total / file_count (single active), dead = total - live
          live = if fc > 0, do: div(total, fc), else: 0
          dead = total - live
          {total, live, dead, fc}

        _ ->
          {0, 0, 0, 0}
      end

    frag = if total_bytes > 0, do: dead_bytes / total_bytes, else: 0.0

    {:reply, {:ok, {total_bytes, live_bytes, dead_bytes, file_count, key_count, frag}}, state}
  end

  def handle_call(:file_sizes, _from, state) do
    state = await_in_flight(state)
    state = flush_pending_sync(state)
    sp = state.shard_data_path

    sizes =
      case Ferricstore.FS.ls(sp) do
        {:ok, files} ->
          files
          |> Enum.filter(&String.ends_with?(&1, ".log"))
          |> Enum.flat_map(fn name ->
            fid = name |> String.trim_trailing(".log") |> String.to_integer()

            case File.stat(Path.join(sp, name)) do
              {:ok, %{size: size}} -> [{fid, size}]
              {:error, _} -> []
            end
          end)

        _ ->
          []
      end

    {:reply, {:ok, sizes}, state}
  end

  def handle_call({:run_compaction, _file_ids}, _from, %{writes_paused: true} = state) do
    {:reply, {:error, "ERR shard writes paused for sync"}, state}
  end

  def handle_call({:run_compaction, file_ids}, _from, state) do
    state = await_in_flight(state)
    state = flush_pending_sync(state)
    # Router async/RMW paths can leave small values queued in BitcaskWriter with
    # ETS file_id=:pending. Drain those writes before compaction snapshots ETS,
    # otherwise a source file can be removed while the writer still targets it.
    Ferricstore.Store.BitcaskWriter.flush(state.index)
    sp = state.shard_data_path

    # v2 compaction: for each file_id, collect live key offsets from ETS,
    # copy them to a new file, then replace the old file.
    # Track statistics for the merge scheduler.
    {total_written, total_dropped, total_reclaimed} =
      Enum.reduce(file_ids, {0, 0, 0}, fn fid, {written, dropped, reclaimed} ->
        source = file_path(sp, fid)

        live_entries =
          :ets.foldl(
            fn {key, _value, _exp, _lfu, f, off, _vsize}, acc ->
              if f == fid, do: [{key, off} | acc], else: acc
            end,
            [],
            state.keydir
          )

        if live_entries != [] do
          offsets = Enum.map(live_entries, fn {_key, off} -> off end)

          old_size =
            case File.stat(source) do
              {:ok, %{size: s}} -> s
              _ -> 0
            end

          dest = Path.join(sp, "compact_#{fid}.log")

          tombstone_offsets = tombstone_offsets(source)

          copy_result =
            if tombstone_offsets == [] do
              NIF.v2_copy_records(source, dest, offsets)
            else
              NIF.v2_copy_records_preserve_tombstones(
                source,
                dest,
                offsets,
                tombstone_offsets
              )
            end

          case copy_result do
            {:ok, results} when length(results) == length(live_entries) ->
              remove_hint_for_file(sp, fid)
              Ferricstore.FS.rename!(dest, source)
              update_compacted_ets_locations(state.keydir, fid, live_entries, results)

              new_size =
                case File.stat(source) do
                  {:ok, %{size: s}} -> s
                  _ -> 0
                end

              {written + length(live_entries), dropped, reclaimed + max(old_size - new_size, 0)}

            {:ok, results} ->
              Logger.error(
                "Shard #{state.index}: compaction copy_records result mismatch for #{source}: expected #{length(live_entries)}, got #{length(results)}"
              )

              _ = Ferricstore.FS.rm(dest)
              {written, dropped, reclaimed}

            {:error, reason} ->
              Logger.error(
                "Shard #{state.index}: compaction copy_records failed for #{source}: #{inspect(reason)}"
              )

              _ = Ferricstore.FS.rm(dest)
              {written, dropped, reclaimed}
          end
        else
          # Tombstones are not represented in ETS, but they can still be
          # semantically live because they suppress older values in lower file
          # ids. Per-file compaction cannot prove those older values are gone,
          # so keep tombstone-only files for correctness.
          old_size =
            case File.stat(source) do
              {:ok, %{size: s}} -> s
              _ -> 0
            end

          if tombstone_file?(source) do
            remove_hint_for_file(sp, fid)
            {written, dropped, reclaimed}
          else
            remove_hint_for_file(sp, fid)
            _ = Ferricstore.FS.rm(source)
            {written, dropped, reclaimed + old_size}
          end
        end
      end)

    # Dir fsync makes rename/rm entries durable so a kernel panic after
    # compaction doesn't resurrect pre-merge filenames.
    _ = NIF.v2_fsync_dir(sp)

    # Reset file_stats for compacted files: dead bytes are now gone,
    # total bytes reflect the new compacted file size.
    new_file_stats =
      Enum.reduce(file_ids, state.file_stats, fn fid, fs ->
        case File.stat(file_path(sp, fid)) do
          {:ok, %{size: new_size}} ->
            Map.put(fs, fid, {new_size, 0})

          _ ->
            # File was deleted entirely (all dead)
            Map.delete(fs, fid)
        end
      end)

    {:reply, {:ok, {total_written, total_dropped, total_reclaimed}},
     %{state | file_stats: new_file_stats}}
  end

  def handle_call(:available_disk_space, _from, state) do
    {:reply, NIF.v2_available_disk_space(state.shard_data_path), state}
  end

  # Synchronous flush — used by tests and by delete to ensure durability.
  def handle_call(:flush, _from, state) do
    state = await_in_flight(state)
    state = flush_pending_sync(state)
    {:reply, :ok, state}
  end

  # Synchronous expiry sweep — used by tests to trigger a sweep and wait for
  # completion before making assertions.
  def handle_call(:expiry_sweep, _from, state) do
    state = ShardLifecycle.do_expiry_sweep(state)
    {:reply, :ok, state}
  end

  # -------------------------------------------------------------------
  # handle_call — catch-all for unhandled commands
  #
  # MUST be the LAST handle_call clause.
  # -------------------------------------------------------------------

  # Catch-all for commands not handled above (prob commands, server_command,
  # raft_apply_hook, etc.). Routes through Batcher → Raft when Raft is
  # enabled, or directly to state machine when Raft is disabled.
  def handle_call(command, from, state) when is_tuple(command) do
    if state.raft? do
      # Forward through Batcher for Raft consensus, same as put/delete.
      Ferricstore.Raft.Batcher.write_async(state.index, command, from)
      {:noreply, state}
    else
      # No Raft — apply directly via state machine.
      sm_state = %{
        shard_index: state.index,
        shard_data_path: state.shard_data_path,
        active_file_id: state.active_file_id,
        active_file_path: state.active_file_path,
        ets: state.ets,
        data_dir: state.data_dir,
        applied_count: 0,
        release_cursor_interval: 20_000,
        cross_shard_locks: %{},
        cross_shard_intents: %{},
        instance_ctx: state.instance_ctx,
        instance_name: if(state.instance_ctx, do: state.instance_ctx.name, else: :default)
      }

      case Ferricstore.Raft.StateMachine.apply(%{}, command, sm_state) do
        {_new_state, result} -> {:reply, result, state}
        {_new_state, result, _effects} -> {:reply, result, state}
      end
    end
  end

  defp handle_forwarded_quorum(
         {:put, _key, _value, _expire_at_ms},
         _from,
         %{writes_paused: true} = state
       ) do
    {:reply, {:error, "ERR shard writes paused for sync"}, state}
  end

  defp handle_forwarded_quorum({:put, key, value, expire_at_ms}, from, state) do
    ShardWrites.handle_put(key, value, expire_at_ms, from, state)
  end

  defp handle_forwarded_quorum({:delete, key}, from, state) do
    ShardWrites.handle_delete(key, from, state)
  end

  defp handle_forwarded_quorum({:incr, key, delta}, from, state) do
    ShardWrites.handle_incr(key, delta, from, state)
  end

  defp handle_forwarded_quorum({:incr_float, key, delta}, from, state) do
    ShardWrites.handle_incr_float(key, delta, from, state)
  end

  defp handle_forwarded_quorum({:append, key, suffix}, from, state) do
    ShardWrites.handle_append(key, suffix, from, state)
  end

  defp handle_forwarded_quorum({:getset, key, new_value}, from, state) do
    ShardWrites.handle_getset(key, new_value, from, state)
  end

  defp handle_forwarded_quorum({:getdel, key}, from, state) do
    ShardWrites.handle_getdel(key, from, state)
  end

  defp handle_forwarded_quorum({:getex, key, expire_at_ms}, from, state) do
    ShardWrites.handle_getex(key, expire_at_ms, from, state)
  end

  defp handle_forwarded_quorum({:setrange, key, offset, value}, from, state) do
    ShardWrites.handle_setrange(key, offset, value, from, state)
  end

  defp handle_forwarded_quorum(
         {:compound_put, redis_key, compound_key, value, expire_at_ms},
         _from,
         state
       ) do
    ShardCompound.handle_compound_put(redis_key, compound_key, value, expire_at_ms, state)
  end

  defp handle_forwarded_quorum({:compound_delete, redis_key, compound_key}, _from, state) do
    ShardCompound.handle_compound_delete(redis_key, compound_key, state)
  end

  defp handle_forwarded_quorum({:compound_delete_prefix, redis_key, prefix}, _from, state) do
    ShardCompound.handle_compound_delete_prefix(redis_key, prefix, state)
  end

  defp handle_forwarded_quorum({:cas, key, expected, new_value, ttl_ms}, _from, state) do
    ShardNativeOps.handle_cas(key, expected, new_value, ttl_ms, state)
  end

  defp handle_forwarded_quorum({:lock, key, owner, ttl_ms}, _from, state) do
    ShardNativeOps.handle_lock(key, owner, ttl_ms, state)
  end

  defp handle_forwarded_quorum({:unlock, key, owner}, _from, state) do
    ShardNativeOps.handle_unlock(key, owner, state)
  end

  defp handle_forwarded_quorum({:extend, key, owner, ttl_ms}, _from, state) do
    ShardNativeOps.handle_extend(key, owner, ttl_ms, state)
  end

  defp handle_forwarded_quorum({:ratelimit_add, key, window_ms, max, count}, _from, state) do
    ShardNativeOps.handle_ratelimit_add(key, window_ms, max, count, state)
  end

  defp handle_forwarded_quorum(
         {:ratelimit_add, key, window_ms, max, count, _now_ms},
         _from,
         state
       ) do
    ShardNativeOps.handle_ratelimit_add(key, window_ms, max, count, state)
  end

  defp handle_forwarded_quorum({:list_op, key, operation}, _from, state) do
    ShardNativeOps.handle_list_op(key, operation, state)
  end

  defp handle_forwarded_quorum({:list_op_lmove, src_key, dst_key, from_dir, to_dir}, _from, state) do
    ShardNativeOps.handle_list_op_lmove(src_key, dst_key, from_dir, to_dir, state)
  end

  defp handle_forwarded_quorum(command, from, state) when is_tuple(command) do
    handle_call(command, from, state)
  end

  defp tombstone_file?(path) do
    case NIF.v2_scan_tombstones(path) do
      {:ok, [_ | _]} -> true
      _ -> false
    end
  end

  defp tombstone_offsets(path) do
    case NIF.v2_scan_tombstones(path) do
      {:ok, tombstones} ->
        Enum.map(tombstones, fn {_key, offset, _record_size, _expire_at_ms} -> offset end)

      _ ->
        []
    end
  end

  defp remove_hint_for_file(shard_path, fid) do
    # Compaction rewrites or invalidates offsets in the paired log file.
    # Dropping the hint forces startup to scan the log instead of trusting
    # stale offsets that can resurrect deleted keys.
    hint_name = "#{String.pad_leading(Integer.to_string(fid), 5, "0")}.hint"
    _ = Ferricstore.FS.rm(Path.join(shard_path, hint_name))
    :ok
  end

  defp update_compacted_ets_locations(keydir, fid, live_entries, results) do
    Enum.zip(live_entries, results)
    |> Enum.each(fn {{key, old_offset}, {new_offset, _new_size}} ->
      case :ets.lookup(keydir, key) do
        [{^key, _value, _exp, _lfu, ^fid, ^old_offset, _vsize}] ->
          :ets.update_element(keydir, key, {6, new_offset})

        _ ->
          :ok
      end
    end)
  end

  # -------------------------------------------------------------------
  # handle_info
  # -------------------------------------------------------------------

  @impl true
  # Handle pending writes from tx_execute. These are queued via send/2
  # during transaction execution to persist ETS-only writes to Bitcask.
  def handle_info({:tx_pending_write, key, value, expire_at_ms}, state) do
    new_pending = [{key, value, expire_at_ms} | state.pending]
    new_version = state.write_version + 1
    new_state = %{state | pending: new_pending, write_version: new_version}

    new_state =
      if state.flush_in_flight == nil,
        do: flush_pending(new_state),
        else: new_state

    {:noreply, new_state}
  end

  def handle_info({:tx_pending_delete, key}, state) do
    if state.raft? do
      raft_write(state, {:delete, key})
      new_version = state.write_version + 1
      {:noreply, %{state | write_version: new_version}}
    else
      state = await_in_flight(state)
      state = flush_pending_sync(state)
      state = track_delete_dead_bytes(state, key)

      case NIF.v2_append_tombstone(state.active_file_path, key) do
        {:ok, _} ->
          new_pending = Enum.reject(state.pending, fn {k, _, _} -> k == key end)
          new_version = state.write_version + 1
          {:noreply, %{state | pending: new_pending, write_version: new_version}}

        {:error, reason} ->
          Logger.error(
            "Shard #{state.index}: tombstone write failed for tx_pending_delete: #{inspect(reason)}"
          )

          {:noreply, state}
      end
    end
  end

  def handle_info(:drain_pending, state) do
    # Drain any pending writes from BEAM memory to the active file
    # (page cache only — NO fsync). BitcaskCheckpointer is responsible
    # for actual disk durability on its own, longer tick.
    state = flush_pending(state)
    schedule_drain_pending(Process.get(:flush_interval_ms, @flush_interval_ms))
    {:noreply, state}
  end

  # Periodic fragmentation re-evaluation for idle shards.
  # Catches shards that accumulated dead data then stopped receiving writes.
  # Disk pressure is intentionally not cleared here; only a successful append or
  # fsync proves that the shard can accept async writes again.
  def handle_info(:frag_check, state) do
    state = maybe_notify_fragmentation(state)
    ShardLifecycle.schedule_frag_check()
    {:noreply, state}
  end

  # Active expiry sweep: scan ETS for expired keys and delete them.
  # When the sweep finds nothing to expire and there are no pending writes
  # or in-flight flushes, hibernate the GenServer to trigger a full GC
  # and shrink the heap. This reclaims memory accumulated during busy periods
  # on idle shards (memory audit L1).
  def handle_info(:expiry_sweep, state) do
    state = ShardLifecycle.do_expiry_sweep(state)
    ShardLifecycle.schedule_expiry_sweep()

    if state.sweep_at_ceiling_count == 0 and
         state.pending == [] and
         state.pending_count == 0 and
         state.flush_in_flight == nil do
      {:noreply, state, :hibernate}
    else
      {:noreply, state}
    end
  end

  # Handle async io_uring completion message from the NIF background thread.
  def handle_info({:io_complete, op_id, result}, state) do
    if state.flush_in_flight == op_id do
      case result do
        :ok ->
          {:noreply, %{state | flush_in_flight: nil}}

        {:error, reason} ->
          # The async flush failed. Log the error but clear in-flight so
          # the next timer tick can attempt another flush. The keydir was
          # updated optimistically by prepare_batch_for_async — on the next
          # store open, log replay will reconcile.
          Logger.error(
            "Shard #{state.index}: async flush failed for op #{op_id}: #{inspect(reason)}"
          )

          {:noreply, %{state | flush_in_flight: nil}}
      end
    else
      # Stale or unknown op_id — ignore.
      {:noreply, state}
    end
  end

  # Handle Tokio async completion with correlation ID.
  # Dispatches to fsync completion (flush_in_flight match) or read completion
  # (pending_reads lookup).
  def handle_info({:tokio_complete, corr_id, :ok, value}, state) do
    cond do
      # Async fsync completion — value is :ok for fsync
      corr_id == state.flush_in_flight ->
        {:noreply, %{state | flush_in_flight: nil}}

      # Async read completion — look up in pending_reads
      true ->
        case Map.pop(state.pending_reads, corr_id) do
          {{from, key}, rest_pending} ->
            # Simple GET cold-read completion.
            if value != nil do
              cold_read_warm_ets(state, key, value)
            end

            GenServer.reply(from, value)
            {:noreply, %{state | pending_reads: rest_pending}}

          {{from, key, :meta, exp}, rest_pending} ->
            # GET_META cold-read completion — reply with {value, expire_at_ms}.
            if value != nil do
              cold_read_warm_ets(state, key, value)
            end

            GenServer.reply(from, if(value != nil, do: {value, exp}, else: nil))
            {:noreply, %{state | pending_reads: rest_pending}}

          {nil, _} ->
            # Unknown correlation_id — could be a stale fsync or read. Ignore.
            {:noreply, state}
        end
    end
  end

  def handle_info({:tokio_complete, corr_id, :error, reason}, state) do
    if corr_id == state.flush_in_flight do
      # Async fsync error completion.
      Logger.error(
        "Shard #{state.index}: async fsync failed for corr_id #{corr_id}: #{inspect(reason)}"
      )

      {:noreply, %{state | flush_in_flight: nil}}
    else
      case Map.pop(state.pending_reads, corr_id) do
        {{from, _key}, rest_pending} ->
          GenServer.reply(from, nil)
          {:noreply, %{state | pending_reads: rest_pending}}

        {{from, _key, :meta, _exp}, rest_pending} ->
          GenServer.reply(from, nil)
          {:noreply, %{state | pending_reads: rest_pending}}

        {nil, _} ->
          {:noreply, state}
      end
    end
  end

  # Legacy v1 3-tuple format (no correlation ID) — keep for backward compat
  # during rolling upgrades. Once all async NIFs use correlation IDs, remove.
  def handle_info({:tokio_complete, :ok, _value}, state) do
    {:noreply, state}
  end

  def handle_info({:tokio_complete, :error, _reason}, state) do
    {:noreply, state}
  end

  # Catch-all for unexpected messages. Without this, any unmatched message
  # (stale timer, DOWN from a linked process, etc.) would crash the shard
  # GenServer, causing a restart and temporary unavailability.
  def handle_info(_msg, state) do
    {:noreply, state}
  end

  # -------------------------------------------------------------------
  # Graceful shutdown (spec 2C.6, step 8)
  #
  # OTP calls terminate/2 when the supervisor stops this child during
  # application shutdown (children are stopped in reverse start order).
  # We flush pending writes, write the Bitcask hint file, and emit
  # telemetry so operators can observe shutdown timing.
  # -------------------------------------------------------------------

  @impl true
  def terminate(reason, state), do: ShardLifecycle.do_terminate(reason, state)

  # -------------------------------------------------------------------
  # Private: flush
  # -------------------------------------------------------------------

  defp flush_pending(state), do: ShardFlush.flush_pending(state)
  defp flush_pending_sync(state), do: ShardFlush.flush_pending_sync(state)
  defp await_in_flight(state), do: ShardFlush.await_in_flight(state)
  defp track_delete_dead_bytes(state, key), do: ShardFlush.track_delete_dead_bytes(state, key)
  defp maybe_notify_fragmentation(state), do: ShardFlush.maybe_notify_fragmentation(state)

  defp compute_file_stats(shard_path, keydir),
    do: ShardFlush.compute_file_stats(shard_path, keydir)

  defp schedule_drain_pending(ms), do: ShardFlush.schedule_drain_pending(ms)

  # -------------------------------------------------------------------
  # Private: read helpers (delegates to Shard.Reads / Shard.ETS)
  # -------------------------------------------------------------------

  defp prefix_scan_entries(state_or_keydir, prefix, shard_data_path),
    do: ShardETS.prefix_scan_entries(state_or_keydir, prefix, shard_data_path)

  defp prefix_count_entries(state_or_keydir, prefix),
    do: ShardETS.prefix_count_entries(state_or_keydir, prefix)

  defp cold_read_warm_ets(state, key, value),
    do: ShardETS.cold_read_warm_ets(state, key, value)

  # -------------------------------------------------------------------
  # Private: Raft write helpers
  # -------------------------------------------------------------------

  # Submits a write command through Raft via the Batcher (group commit).
  defp raft_write(%__MODULE__{index: index}, command) do
    Ferricstore.Raft.Batcher.write(index, command)
  end
end
