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
  alias Ferricstore.Flow.NativeOrderedIndex, as: NativeFlowIndex
  alias Ferricstore.Flow.OrderedIndex, as: FlowIndex
  alias Ferricstore.Raft.Backend, as: RaftBackend
  alias Ferricstore.Store.CompoundKey
  alias Ferricstore.Store.BlobValue
  alias Ferricstore.Store.ColdRead
  alias Ferricstore.Store.Router
  alias Ferricstore.Store.Shard.Compound, as: ShardCompound
  alias Ferricstore.Store.Shard.ETS, as: ShardETS
  alias Ferricstore.Store.Shard.Flush, as: ShardFlush
  alias Ferricstore.Store.Shard.Lifecycle, as: ShardLifecycle
  alias Ferricstore.Store.Shard.NativeOps, as: ShardNativeOps
  alias Ferricstore.Store.Shard.Reads, as: ShardReads
  alias Ferricstore.Store.Shard.Transaction, as: ShardTransaction
  alias Ferricstore.Store.Shard.Writes, as: ShardWrites
  alias Ferricstore.Store.Shard.ZSetIndex

  require Logger

  # How often (ms) to flush the pending write queue to disk.
  # 1ms gives up to 50k batched writes/s per shard (4 shards → 200k/s total).
  @flush_interval_ms 1
  @cold_read_timeout_ms 10_000
  @cold_read_compaction_retry_attempts 8
  @cold_read_compaction_retry_delay_ms 1
  @standalone_global_key "__standalone_global__"
  @standalone_flow_many_commands [
    :flow_create_many,
    :flow_complete_many,
    :flow_transition_many,
    :flow_retry_many,
    :flow_fail_many,
    :flow_cancel_many
  ]

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
    :active_file_preallocated_to,
    pending: [],
    pending_count: 0,
    last_flush_error: nil,
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
    writes_paused: false,
    zset_score_index: nil,
    zset_score_lookup: nil,
    flow_index: nil,
    flow_lookup: nil,
    zset_index_ready: MapSet.new(),
    standalone_batch: [],
    standalone_batch_timer: nil,
    standalone_flush_ref: nil,
    standalone_flush_entries: [],
    standalone_inflight_keys: MapSet.new(),
    standalone_waiting: [],
    standalone_write_barrier: false
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

    try do
      index = Keyword.fetch!(opts, :index)
      data_dir = Keyword.fetch!(opts, :data_dir)
      flush_ms = Keyword.get(opts, :flush_interval_ms, @flush_interval_ms)
      ctx = Keyword.get(opts, :instance_ctx)
      fsync_dir_fun = Keyword.get(opts, :fsync_dir_fun, &NIF.v2_fsync_dir/1)

      if ctx && !Ferricstore.ReplicationMode.raft?() do
        :ok = Ferricstore.Store.StandaloneTxLog.recover_once(data_dir)
      end

      path = Ferricstore.DataDir.shard_data_path(data_dir, index)

      {active_file_id, active_file_size, active_file_path} =
        ensure_initial_files!(path, index, fsync_dir_fun)

      # Create/clear named ETS tables.
      # Use instance-scoped names from ctx if available, else default naming.
      keydir_name =
        if ctx, do: elem(ctx.keydir_refs, index), else: :"keydir_#{index}"

      keydir = prepare_rebuilding_keydir(keydir_name, ctx, index)

      # Remove any leftover hot_cache table from a previous run.
      case :ets.whereis(:"hot_cache_#{index}") do
        :undefined -> :ok
        _ref -> :ets.delete(:"hot_cache_#{index}")
      end

      instance_name = if ctx, do: ctx.name, else: :default
      {zset_score_index, zset_score_lookup} = ZSetIndex.table_names(instance_name, index)
      ensure_zset_index_table!(zset_score_index, :ordered_set)
      ensure_zset_index_table!(zset_score_lookup, :set)
      {flow_index, flow_lookup} = FlowIndex.table_names(instance_name, index)
      ensure_zset_index_table!(flow_index, :ordered_set)
      ensure_zset_index_table!(flow_lookup, :set)

      # v2: recover ETS keydir from hint files or by scanning log files BEFORE
      # starting Raft. This ensures cold entries ({key, nil, ..., fid, off, vsize})
      # are in ETS when ra replays WAL entries via apply/3. Without this, replayed
      # read-modify-write commands (INCR, APPEND, etc.) see ETS misses during
      # replay and start from nil instead of the correct prior value.
      # 7-tuple format: {key, value, expire_at_ms, lfu_counter, file_id, offset, value_size}
      # Must run BEFORE recover_promoted so PM: markers are in ETS.
      profile_startup_phase(index, :recover_keydir, fn ->
        ShardLifecycle.recover_keydir(path, keydir, index, ctx)
      end)

      profile_startup_phase(index, :flow_native_index_init, fn ->
        case Ferricstore.Flow.NativeOrderedIndex.get(flow_index, flow_lookup) do
          nil ->
            Ferricstore.Flow.NativeOrderedIndex.register(
              flow_index,
              flow_lookup,
              Ferricstore.Flow.NativeOrderedIndex.new()
            )

          _native ->
            :ok
        end
      end)

      profile_startup_phase(index, :flow_history_projector_recover, fn ->
        :ok = Ferricstore.Flow.HistoryProjector.recover(ctx, index, path, keydir)
      end)

      profile_startup_phase(index, :flow_lmdb_rebuild, fn ->
        if Ferricstore.Flow.LMDB.mirror?() do
          Ferricstore.Flow.LMDBRebuilder.reconcile_shard(
            path,
            keydir,
            index,
            ctx,
            zset_score_index,
            zset_score_lookup,
            flow_index,
            flow_lookup
          )
        else
          :ok
        end
      end)

      profile_startup_phase(index, :flow_native_index_rebuild, fn ->
        Ferricstore.Flow.NativeOrderedIndex.merge_from_ets(flow_index, flow_lookup)
      end)

      keydir = publish_rebuilt_keydir(keydir, keydir_name)
      ets = keydir

      # Only the default application instance owns Raft. Custom embedded shards
      # run local/direct, and direct shard tests pass non-default instance_ctx.
      raft? =
        if ctx && ctx.name == :default && Ferricstore.ReplicationMode.raft?() &&
             Ferricstore.Raft.Backend.selected() == :ra do
          profile_startup_phase(index, :start_raft, fn ->
            ShardLifecycle.start_raft_if_available(
              index,
              path,
              active_file_id,
              active_file_path,
              ets,
              ctx.name,
              # During full application boot, start all Ra servers first and
              # let Application elect/wait for every shard in parallel. During
              # a supervised shard restart, this process is the only restart
              # coordinator, so it must restore a writable Ra leader itself.
              wait_for_leader: not Ferricstore.Application.starting?(),
              blob_side_channel_threshold_bytes: ctx.blob_side_channel_threshold_bytes,
              zset_score_index_name: zset_score_index,
              zset_score_lookup_name: zset_score_lookup,
              flow_index_name: flow_index,
              flow_lookup_name: flow_lookup
            )
          end)
        else
          false
        end

      # Recover promoted collection instances
      promoted =
        profile_startup_phase(index, :recover_promoted, fn ->
          Ferricstore.Store.Promotion.recover_promoted(path, keydir, data_dir, index, ctx)
        end)

      # Migrate existing prob files: scan prob dir for files without
      # corresponding metadata markers in the keydir. Write markers so
      # DEL can clean up prob files and BF.INFO/CMS.INFO can recover metadata.
      profile_startup_phase(index, :migrate_prob_files, fn ->
        ShardLifecycle.migrate_prob_files(path, keydir, index, ctx)
      end)

      # Publish active file metadata to ActiveFile registry
      Ferricstore.Store.ActiveFile.publish(ctx, index, active_file_id, active_file_path, path)

      # Compute per-file dead bytes stats from disk sizes + ETS live data.
      file_stats =
        profile_startup_phase(index, :compute_file_stats, fn ->
          compute_file_stats(path, keydir)
        end)

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
         max_active_file_size: max_file_size,
         zset_score_index: zset_score_index,
         zset_score_lookup: zset_score_lookup,
         flow_index: flow_index,
         flow_lookup: flow_lookup,
         zset_index_ready: MapSet.new()
       }, {:continue, {:flush_interval, flush_ms}}}
    catch
      {:shard_init_failed, reason} -> {:stop, reason}
    end
  end

  defp file_path(shard_path, file_id), do: ShardETS.file_path(shard_path, file_id)

  defp ensure_zset_index_table!(table_name, table_type) do
    case :ets.whereis(table_name) do
      :undefined ->
        :ets.new(table_name, [
          table_type,
          :public,
          :named_table,
          {:read_concurrency, true},
          {:write_concurrency, :auto}
        ])

      _tid ->
        :ets.delete_all_objects(table_name)
        table_name
    end
  end

  defp ensure_initial_files!(path, index, fsync_dir_fun) do
    dir_created? = not Ferricstore.FS.dir?(path)
    Ferricstore.FS.mkdir_p!(path)

    maybe_fsync_startup_dir!(
      dir_created?,
      Path.dirname(path),
      :create_shard_dir,
      index,
      fsync_dir_fun
    )

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

    maybe_fsync_startup_dir!(
      dir_created? or file_created?,
      path,
      :create_active_file,
      index,
      fsync_dir_fun
    )

    {active_file_id, active_file_size, active_file_path}
  end

  defp maybe_fsync_startup_dir!(false, _path, _phase, _index, _fsync_dir_fun), do: :ok

  defp maybe_fsync_startup_dir!(true, path, phase, index, fsync_dir_fun) do
    case fsync_dir_fun.(path) do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.error(
          "Shard #{index} startup failed to fsync #{phase} directory #{inspect(path)}: #{inspect(reason)}"
        )

        throw({:shard_init_failed, {:fsync_dir_failed, phase, reason}})
    end
  end

  defp prepare_rebuilding_keydir(keydir_name, ctx, index) do
    # Do not expose an empty/partial final keydir during startup recovery.
    # Router reads treat an existing table miss as authoritative, so rebuild
    # into a private startup table and publish it only after recovery finishes.
    delete_keydir_table(keydir_name)
    reset_keydir_binary_counter(ctx, index)

    temp_name = rebuilding_keydir_name(keydir_name)
    delete_keydir_table(temp_name)

    :ets.new(temp_name, keydir_table_options())
  end

  defp publish_rebuilt_keydir(temp_name, keydir_name) do
    delete_keydir_table(keydir_name)
    :ets.rename(temp_name, keydir_name)
    keydir_name
  end

  defp rebuilding_keydir_name(keydir_name), do: :"#{keydir_name}.__rebuilding__"

  defp keydir_table_options do
    [
      :set,
      :public,
      :named_table,
      {:read_concurrency, true},
      {:write_concurrency, :auto},
      {:decentralized_counters, true}
    ]
  end

  defp delete_keydir_table(name) do
    case :ets.whereis(name) do
      :undefined -> :ok
      _ref -> :ets.delete(name)
    end
  end

  defp reset_keydir_binary_counter(nil, _index), do: :ok
  defp reset_keydir_binary_counter(%{keydir_binary_bytes: nil}, _index), do: :ok

  defp reset_keydir_binary_counter(%{keydir_binary_bytes: keydir_binary_bytes}, index) do
    :atomics.put(keydir_binary_bytes, index + 1, 0)
  end

  defp profile_startup_phase(index, phase, fun) when is_function(fun, 0) do
    {duration_us, result} = :timer.tc(fun)

    :telemetry.execute(
      [:ferricstore, :shard, :startup_phase],
      %{duration_us: duration_us},
      %{shard_index: index, phase: phase}
    )

    result
  end

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
    {:reply, max(state.write_version, shared_write_version(state)), state}
  end

  # -------------------------------------------------------------------
  # handle_call — writes
  # -------------------------------------------------------------------

  # Delete all entries matching a compound key prefix.
  # Uses :ets.select match spec instead of :ets.foldl full-table scan.
  def handle_call({:delete_prefix, prefix}, _from, state) do
    maybe_route_default_waraft_write({:delete_prefix, prefix}, state, fn ->
      prefix
      |> ShardWrites.handle_delete_prefix(state)
      |> track_write_version_result(state)
    end)
  end

  def handle_call({:put, _key, _value, _expire_at_ms}, _from, %{writes_paused: true} = state) do
    {:reply, {:error, "ERR shard writes paused for sync"}, state}
  end

  def handle_call(command, _from, %{writes_paused: true} = state)
      when is_tuple(command) and command != {:pause_writes} and command != {:resume_writes} do
    {:reply, {:error, "ERR shard writes paused for sync"}, state}
  end

  def handle_call({:standalone_commit, command}, from, state) do
    if default_waraft_write_state?(state) do
      reply_default_waraft_write(command, state)
    else
      state =
        state
        |> enqueue_standalone_commit(from, command)
        |> maybe_flush_full_standalone_batch()

      {:noreply, state}
    end
  end

  def handle_call(:standalone_cross_shard_barrier_acquire, _from, state) do
    state = drain_standalone_commits_for_sync(state)

    cond do
      state.standalone_write_barrier ->
        {:reply, {:error, :standalone_cross_shard_barrier_busy}, state}

      state.writes_paused ->
        {:reply, {:error, :prior_standalone_write_failed}, state}

      state.last_flush_error != nil ->
        {:reply, {:error, state.last_flush_error}, %{state | writes_paused: true}}

      true ->
        {:reply, :ok, %{state | standalone_write_barrier: true}}
    end
  end

  def handle_call(:standalone_cross_shard_barrier_release, _from, state) do
    state =
      cond do
        state.writes_paused ->
          state
          |> clear_standalone_write_barrier()
          |> reject_queued_standalone_commits(
            {:standalone_durability_failed, :prior_standalone_write_failed}
          )

        state.last_flush_error != nil ->
          state
          |> clear_standalone_write_barrier()
          |> reject_queued_standalone_commits(
            {:standalone_durability_failed, state.last_flush_error}
          )
          |> Map.put(:writes_paused, true)

        true ->
          state
          |> clear_standalone_write_barrier()
          |> drain_standalone_waiting()
          |> flush_ready_standalone_batch()
      end

    {:reply, :ok, state}
  end

  def handle_call(
        {:standalone_commit_sync, {:cross_shard_tx, _shard_batches} = command},
        _from,
        state
      ) do
    if default_waraft_write_state?(state) do
      reply_default_waraft_write(command, state)
    else
      state = drain_standalone_commits_for_sync(state)

      cond do
        state.writes_paused ->
          {:reply, {:error, {:standalone_durability_failed, :prior_standalone_write_failed}},
           state}

        state.last_flush_error != nil ->
          {:reply, {:error, {:standalone_durability_failed, state.last_flush_error}},
           %{state | writes_paused: true}}

        true ->
          sm_state = direct_sm_state(state)

          case run_standalone_command(command, sm_state) do
            {:ok, new_sm_state, result} ->
              {:reply, result, apply_direct_sm_state(state, new_sm_state)}

            {:error, new_sm_state, reason} ->
              state =
                if new_sm_state do
                  apply_direct_sm_state(state, new_sm_state)
                else
                  state
                end

              {:reply, {:error, {:standalone_durability_failed, reason}},
               %{state | writes_paused: true}}
          end
      end
    end
  end

  def handle_call(:standalone_commit_debug, _from, state) do
    {:reply,
     %{
       batch_count: length(state.standalone_batch),
       waiting_count: length(state.standalone_waiting),
       inflight_count: length(state.standalone_flush_entries),
       inflight_keys: MapSet.to_list(state.standalone_inflight_keys)
     }, state}
  end

  def handle_call(:write_status, _from, state) do
    {:reply,
     %{
       writes_paused: state.writes_paused,
       last_flush_error: state.last_flush_error,
       pending_count: state.pending_count,
       standalone_batch_count: length(state.standalone_batch),
       standalone_waiting_count: length(state.standalone_waiting),
       standalone_flush_inflight: state.standalone_flush_ref != nil
     }, state}
  end

  def handle_call({:forwarded_quorum, origin_node, command}, from, state) do
    forwarded_from = Ferricstore.Raft.Batcher.remote_origin_from(origin_node, from)
    previous_origin = Process.get(:ferricstore_forward_origin)
    Process.put(:ferricstore_forward_origin, origin_node)

    try do
      if default_waraft_write_state?(state) do
        reply_default_waraft_write(command, state)
      else
        handle_forwarded_quorum(command, forwarded_from, state)
      end
    after
      if previous_origin == nil do
        Process.delete(:ferricstore_forward_origin)
      else
        Process.put(:ferricstore_forward_origin, previous_origin)
      end
    end
  end

  def handle_call({:put, key, value, expire_at_ms}, from, state) do
    maybe_route_default_waraft_write({:put, key, value, expire_at_ms}, state, fn ->
      key
      |> ShardWrites.handle_put(value, expire_at_ms, from, state)
      |> track_write_version_result(state)
    end)
  end

  # Atomic increment: reads current value, parses as integer, adds delta, writes back.
  # Returns {:ok, new_integer} or {:error, reason}.
  def handle_call({:incr, key, delta}, from, state) do
    maybe_route_default_waraft_write({:incr, key, delta}, state, fn ->
      key
      |> ShardWrites.handle_incr(delta, from, state)
      |> track_write_version_result(state)
    end)
  end

  # Atomic float increment: reads current value, parses as float, adds delta, writes back.
  # Returns {:ok, new_float_string} or {:error, reason}.
  def handle_call({:incr_float, key, delta}, from, state) do
    maybe_route_default_waraft_write({:incr_float, key, delta}, state, fn ->
      key
      |> ShardWrites.handle_incr_float(delta, from, state)
      |> track_write_version_result(state)
    end)
  end

  # Atomic append: reads current value (or ""), appends suffix, writes back.
  # Returns {:ok, new_byte_length}.
  def handle_call({:append, key, suffix}, from, state) do
    maybe_route_default_waraft_write({:append, key, suffix}, state, fn ->
      key
      |> ShardWrites.handle_append(suffix, from, state)
      |> track_write_version_result(state)
    end)
  end

  # Atomic get-and-set: returns old value (or nil), sets new value.
  def handle_call({:getset, key, new_value}, from, state) do
    maybe_route_default_waraft_write({:getset, key, new_value}, state, fn ->
      key
      |> ShardWrites.handle_getset(new_value, from, state)
      |> track_write_version_result(state)
    end)
  end

  # Atomic get-and-delete: returns value (or nil), deletes key.
  def handle_call({:getdel, key}, from, state) do
    maybe_route_default_waraft_write({:getdel, key}, state, fn ->
      key
      |> ShardWrites.handle_getdel(from, state)
      |> track_write_version_result(state)
    end)
  end

  # Atomic get-and-update-expiry: returns value, updates TTL.
  # expire_at_ms = 0 means PERSIST (remove expiry).
  def handle_call({:getex, key, expire_at_ms}, from, state) do
    maybe_route_default_waraft_write({:getex, key, expire_at_ms}, state, fn ->
      key
      |> ShardWrites.handle_getex(expire_at_ms, from, state)
      |> track_write_version_result(state)
    end)
  end

  # Atomic set-range: overwrites portion of string at offset with value.
  # Zero-pads if key doesn't exist or string is shorter than offset.
  # Returns {:ok, new_byte_length}.
  def handle_call({:setrange, key, offset, value}, from, state) do
    maybe_route_default_waraft_write({:setrange, key, offset, value}, state, fn ->
      key
      |> ShardWrites.handle_setrange(offset, value, from, state)
      |> track_write_version_result(state)
    end)
  end

  def handle_call({:delete, key}, from, state) do
    maybe_route_default_waraft_write({:delete, key}, state, fn ->
      key
      |> ShardWrites.handle_delete(from, state)
      |> track_write_version_result(state)
    end)
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
    maybe_route_default_waraft_write(
      {:compound_put, redis_key, compound_key, value, expire_at_ms},
      state,
      fn ->
        redis_key
        |> ShardCompound.handle_compound_put(compound_key, value, expire_at_ms, state)
        |> track_write_version_result(state)
      end
    )
  end

  def handle_call({:compound_batch_put, redis_key, entries}, _from, state) do
    maybe_route_default_waraft_write({:compound_batch_put, redis_key, entries}, state, fn ->
      redis_key
      |> ShardCompound.handle_compound_batch_put(entries, state)
      |> track_write_version_result(state)
    end)
  end

  def handle_call({:compound_delete, redis_key, compound_key}, _from, state) do
    maybe_route_default_waraft_write({:compound_delete, redis_key, compound_key}, state, fn ->
      redis_key
      |> ShardCompound.handle_compound_delete(compound_key, state)
      |> track_write_version_result(state)
    end)
  end

  def handle_call({:compound_batch_delete, redis_key, compound_keys}, _from, state) do
    maybe_route_default_waraft_write(
      {:compound_batch_delete, redis_key, compound_keys},
      state,
      fn ->
        redis_key
        |> ShardCompound.handle_compound_batch_delete(compound_keys, state)
        |> track_write_version_result(state)
      end
    )
  end

  def handle_call({:compound_delete_prefix, redis_key, prefix}, _from, state) do
    maybe_route_default_waraft_write({:compound_delete_prefix, redis_key, prefix}, state, fn ->
      redis_key
      |> ShardCompound.handle_compound_delete_prefix(prefix, state)
      |> track_write_version_result(state)
    end)
  end

  def handle_call({:compound_scan, redis_key, prefix}, _from, state) do
    ShardCompound.handle_compound_scan(redis_key, prefix, state)
  end

  def handle_call({:compound_fields, redis_key, prefix}, _from, state) do
    ShardCompound.handle_compound_fields(redis_key, prefix, state)
  end

  def handle_call({:compound_count, redis_key, prefix}, _from, state) do
    ShardCompound.handle_compound_count(redis_key, prefix, state)
  end

  def handle_call({:zset_score_range, redis_key, min_bound, max_bound, reverse?}, _from, state) do
    ShardCompound.handle_zset_score_range(redis_key, min_bound, max_bound, reverse?, state)
  end

  def handle_call(
        {:zset_score_range_slice, redis_key, min_bound, max_bound, reverse?, offset, count},
        _from,
        state
      ) do
    ShardCompound.handle_zset_score_range_slice(
      redis_key,
      min_bound,
      max_bound,
      reverse?,
      offset,
      count,
      state
    )
  end

  def handle_call({:zset_score_count, redis_key, min_bound, max_bound}, _from, state) do
    ShardCompound.handle_zset_score_count(redis_key, min_bound, max_bound, state)
  end

  def handle_call({:zset_score_count_many, queries}, _from, state) do
    ShardCompound.handle_zset_score_count_many(queries, state)
  end

  def handle_call({:zset_score_count_all_many_no_build, keys}, _from, state) do
    ShardCompound.handle_zset_score_count_all_many_no_build(keys, state)
  end

  def handle_call({:zset_rank_range, redis_key, start_idx, stop_idx, reverse?}, _from, state) do
    ShardCompound.handle_zset_rank_range(redis_key, start_idx, stop_idx, reverse?, state)
  end

  def handle_call({:zset_member_rank, redis_key, member, reverse?}, _from, state) do
    ShardCompound.handle_zset_member_rank(redis_key, member, reverse?, state)
  end

  def handle_call(
        {:flow_index_score_range_slice, key, min_bound, max_bound, reverse?, offset, count},
        _from,
        state
      ) do
    reply =
      case native_flow_index_for_read(state, key) do
        :not_native ->
          {:ok,
           FlowIndex.range_slice(
             state.flow_index,
             key,
             min_bound,
             max_bound,
             reverse?,
             offset,
             count
           )}

        {:ok, native} ->
          {:ok,
           NativeFlowIndex.range_slice(native, key, min_bound, max_bound, reverse?, offset, count)}

        :unavailable ->
          :unavailable
      end

    {:reply, reply, state}
  end

  def handle_call({:flow_index_rank_range, key, start_idx, stop_idx, reverse?}, _from, state) do
    reply =
      case native_flow_index_for_read(state, key) do
        :not_native ->
          {:ok, FlowIndex.rank_range(state.flow_index, key, start_idx, stop_idx, reverse?)}

        {:ok, native} ->
          {:ok, NativeFlowIndex.rank_range(native, key, start_idx, stop_idx, reverse?)}

        :unavailable ->
          :unavailable
      end

    {:reply, reply, state}
  end

  def handle_call({:flow_index_rank_range_many, requests}, _from, state) do
    reply =
      Enum.reduce_while(requests, {:ok, []}, fn {key, start_idx, stop_idx, reverse?},
                                                {:ok, acc} ->
        case native_flow_index_for_read(state, key) do
          :not_native ->
            result = FlowIndex.rank_range(state.flow_index, key, start_idx, stop_idx, reverse?)
            {:cont, {:ok, [result | acc]}}

          {:ok, native} ->
            result = NativeFlowIndex.rank_range(native, key, start_idx, stop_idx, reverse?)
            {:cont, {:ok, [result | acc]}}

          :unavailable ->
            {:halt, :unavailable}
        end
      end)
      |> case do
        {:ok, results} -> {:ok, Enum.reverse(results)}
        :unavailable -> :unavailable
      end

    {:reply, reply, state}
  end

  def handle_call({:flow_index_count_all, key}, _from, state) do
    reply =
      case native_flow_index_for_read(state, key) do
        :not_native -> {:ok, FlowIndex.count_all(state.flow_lookup, key)}
        {:ok, native} -> {:ok, NativeFlowIndex.count_all(native, key)}
        :unavailable -> :unavailable
      end

    {:reply, reply, state}
  end

  def handle_call({:flow_index_count_all_many, keys}, _from, state) do
    reply =
      case native_flow_index_for_count_many(state, keys) do
        :not_native_or_mixed ->
          Enum.reduce_while(keys, {:ok, []}, fn key, {:ok, acc} ->
            case native_flow_index_for_read(state, key) do
              :not_native ->
                {:cont, {:ok, [FlowIndex.count_all(state.flow_lookup, key) | acc]}}

              {:ok, native} ->
                {:cont, {:ok, [NativeFlowIndex.count_all(native, key) | acc]}}

              :unavailable ->
                {:halt, :unavailable}
            end
          end)
          |> case do
            {:ok, counts} -> {:ok, Enum.reverse(counts)}
            :unavailable -> :unavailable
          end

        {:ok, native} ->
          {:ok, NativeFlowIndex.count_many(native, keys)}

        :unavailable ->
          :unavailable
      end

    {:reply, reply, state}
  end

  # -------------------------------------------------------------------
  # handle_call — native commands: CAS, LOCK, UNLOCK, EXTEND, RATELIMIT.ADD
  # -------------------------------------------------------------------

  def handle_call({:cas, key, expected, new_value, ttl_ms}, _from, state) do
    maybe_route_default_waraft_write({:cas, key, expected, new_value, ttl_ms}, state, fn ->
      key
      |> ShardNativeOps.handle_cas(expected, new_value, ttl_ms, state)
      |> track_write_version_result(state)
    end)
  end

  def handle_call({:lock, key, owner, ttl_ms}, _from, state) do
    maybe_route_default_waraft_write({:lock, key, owner, ttl_ms}, state, fn ->
      key
      |> ShardNativeOps.handle_lock(owner, ttl_ms, state)
      |> track_write_version_result(state)
    end)
  end

  def handle_call({:unlock, key, owner}, _from, state) do
    maybe_route_default_waraft_write({:unlock, key, owner}, state, fn ->
      key
      |> ShardNativeOps.handle_unlock(owner, state)
      |> track_write_version_result(state)
    end)
  end

  def handle_call({:extend, key, owner, ttl_ms}, _from, state) do
    maybe_route_default_waraft_write({:extend, key, owner, ttl_ms}, state, fn ->
      key
      |> ShardNativeOps.handle_extend(owner, ttl_ms, state)
      |> track_write_version_result(state)
    end)
  end

  def handle_call({:ratelimit_add, key, window_ms, max, count}, _from, state) do
    maybe_route_default_waraft_write({:ratelimit_add, key, window_ms, max, count}, state, fn ->
      key
      |> ShardNativeOps.handle_ratelimit_add(window_ms, max, count, state)
      |> track_write_version_result(state)
    end)
  end

  # 6-tuple variant: includes pre-computed now_ms from Router.raft_write.
  # In cluster mode this MUST go through Raft (replicated) just like the
  # 5-tuple variant — otherwise a follower's ratelimit-add lands locally
  # only and other nodes never see the increment. Falls back to direct in
  # non-Raft mode.
  def handle_call({:ratelimit_add, key, window_ms, max, count, _now_ms}, _from, state) do
    maybe_route_default_waraft_write(
      {:ratelimit_add, key, window_ms, max, count},
      state,
      fn ->
        key
        |> ShardNativeOps.handle_ratelimit_add(window_ms, max, count, state)
        |> track_write_version_result(state)
      end
    )
  end

  # -------------------------------------------------------------------
  # handle_call — list operations
  # -------------------------------------------------------------------

  def handle_call({:list_op, key, operation}, _from, state) do
    maybe_route_default_waraft_write({:list_op, key, operation}, state, fn ->
      key
      |> ShardNativeOps.handle_list_op(operation, state)
      |> track_write_version_result(state)
    end)
  end

  def handle_call({:list_op_lmove, src_key, dst_key, from_dir, to_dir}, _from, state) do
    maybe_route_default_waraft_write(
      {:list_op_lmove, src_key, dst_key, from_dir, to_dir},
      state,
      fn ->
        src_key
        |> ShardNativeOps.handle_list_op_lmove(dst_key, from_dir, to_dir, state)
        |> track_write_version_result(state)
      end
    )
  end

  # -------------------------------------------------------------------
  # handle_call — transaction execution (single-shard atomic batch)
  # -------------------------------------------------------------------

  def handle_call({:tx_execute, queue, sandbox_namespace}, _from, state) do
    maybe_route_default_waraft_write({:tx_execute, queue, sandbox_namespace}, state, fn ->
      queue
      |> ShardTransaction.handle_tx_execute(sandbox_namespace, state)
      |> track_write_version_result(state)
    end)
  end

  # Check if a redis_key has been promoted to dedicated storage.
  def handle_call({:promoted?, redis_key}, _from, state) do
    {:reply, Map.has_key?(state.promoted_instances, redis_key), state}
  end

  # -------------------------------------------------------------------
  # handle_call — pause/resume writes (cluster data sync)
  # -------------------------------------------------------------------

  def handle_call({:pause_writes}, _from, state) do
    state = drain_standalone_commits_for_sync(state)
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

  def handle_call(:start_raft, _from, %{raft?: true} = state) do
    {:reply, :ok, state}
  end

  def handle_call(:start_raft, _from, state) do
    result =
      ShardLifecycle.start_raft_if_available(
        state.index,
        state.shard_data_path,
        state.active_file_id,
        state.active_file_path,
        state.ets,
        if(state.instance_ctx, do: state.instance_ctx.name, else: :default),
        wait_for_leader: false,
        blob_side_channel_threshold_bytes:
          if(state.instance_ctx,
            do: state.instance_ctx.blob_side_channel_threshold_bytes,
            else: 0
          ),
        active_file_preallocated_to: state.active_file_preallocated_to,
        zset_score_index_name: state.zset_score_index,
        zset_score_lookup_name: state.zset_score_lookup,
        flow_index_name: state.flow_index,
        flow_lookup_name: state.flow_lookup
      )

    if result do
      {:reply, :ok, %{state | raft?: true}}
    else
      {:reply, {:error, :batcher_not_started}, state}
    end
  catch
    kind, reason ->
      {:reply, {:error, {kind, reason}}, state}
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
    try do
      state = await_in_flight(state)
      state = sync_active_file_from_registry(state)
      state = flush_pending_sync(state)
      # Router async/RMW paths can leave small values queued in BitcaskWriter with
      # ETS file_id=:pending. Drain those writes before compaction snapshots ETS,
      # otherwise a source file can be removed while the writer still targets it.
      case Ferricstore.Store.BitcaskWriter.flush(state.instance_ctx, state.index) do
        :ok ->
          :ok

        {:error, reason} ->
          Logger.warning(
            "Shard #{state.index}: compaction aborted because BitcaskWriter flush failed: #{inspect(reason)}"
          )

          throw({:bitcask_writer_flush_failed, reason, state})
      end

      state = sync_active_file_from_registry(state)
      sp = state.shard_data_path

      # v2 compaction: for each file_id, collect live key offsets from ETS,
      # copy them to a new file, then replace the old file.
      # Track statistics for the merge scheduler.
      live_entries_by_fid = group_compaction_live_entries(state, file_ids)

      {total_written, total_dropped, total_reclaimed, compacted_file_ids, skipped_file_ids,
       failures} =
        Enum.reduce(file_ids, {0, 0, 0, [], [], []}, fn fid,
                                                        {written, dropped, reclaimed, compacted,
                                                         skipped, failures} ->
          source = file_path(sp, fid)
          live_entries = Map.get(live_entries_by_fid, fid, [])

          cond do
            fid == state.active_file_id ->
              {written, dropped, reclaimed, compacted, skipped, failures}

            live_entries != [] ->
              offsets = Enum.map(live_entries, fn {_key, off} -> off end)

              old_size =
                case File.stat(source) do
                  {:ok, %{size: s}} -> s
                  _ -> 0
                end

              dest = Path.join(sp, "compact_#{fid}.log")

              tombstone_offsets = needed_tombstone_offsets(sp, fid, source)

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

                  {written + length(live_entries), dropped,
                   reclaimed + max(old_size - new_size, 0), [fid | compacted], skipped, failures}

                {:ok, results} ->
                  Logger.error(
                    "Shard #{state.index}: compaction copy_records result mismatch for #{source}: expected #{length(live_entries)}, got #{length(results)}"
                  )

                  remove_compaction_temp(state, dest)

                  failure = {fid, {:copy_result_mismatch, length(live_entries), length(results)}}
                  {written, dropped, reclaimed, compacted, skipped, [failure | failures]}

                {:error, reason} ->
                  maybe_emit_compaction_crc_mismatch(state, fid, source, dest, reason)

                  Logger.error(
                    "Shard #{state.index}: compaction copy_records failed for #{source}: #{inspect(reason)}"
                  )

                  remove_compaction_temp(state, dest)

                  {written, dropped, reclaimed, compacted, skipped,
                   [{fid, {:copy_failed, reason}} | failures]}
              end

            true ->
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

                if tombstone_file_still_needed?(sp, fid, source) do
                  {written, dropped, reclaimed, compacted, [fid | skipped], failures}
                else
                  case remove_compacted_source(state, source) do
                    :ok ->
                      {written, dropped, reclaimed + old_size, [fid | compacted], skipped,
                       failures}

                    {:error, reason} ->
                      {written, dropped, reclaimed, compacted, skipped,
                       [{fid, {:remove_failed, reason}} | failures]}
                  end
                end
              else
                remove_hint_for_file(sp, fid)

                case remove_compacted_source(state, source) do
                  :ok ->
                    {written, dropped, reclaimed + old_size, [fid | compacted], skipped, failures}

                  {:error, reason} ->
                    {written, dropped, reclaimed, compacted, skipped,
                     [{fid, {:remove_failed, reason}} | failures]}
                end
              end
          end
        end)

      # Dir fsync makes rename/rm entries durable so a kernel panic after
      # compaction doesn't resurrect pre-merge filenames. If this fails, the
      # namespace was changed in this running process, so keep file_stats in
      # sync with reality but return an error to the scheduler/operator.
      dir_fsync_failure =
        if compacted_file_ids == [] do
          nil
        else
          case compaction_fsync_dir(state, sp) do
            :ok ->
              nil

            {:error, reason} ->
              Logger.error(
                "Shard #{state.index}: compaction directory fsync failed for #{sp}: #{inspect(reason)}"
              )

              {:dir_fsync_failed, reason}
          end
        end

      # Reset file_stats for compacted files: dead bytes are now gone,
      # total bytes reflect the new compacted file size.
      new_file_stats =
        Enum.reduce(compacted_file_ids, state.file_stats, fn fid, fs ->
          case File.stat(file_path(sp, fid)) do
            {:ok, %{size: new_size}} ->
              Map.put(fs, fid, {new_size, 0})

            _ ->
              # File was deleted entirely (all dead)
              Map.delete(fs, fid)
          end
        end)

      reply =
        case {dir_fsync_failure, failures} do
          {nil, []} ->
            if compacted_file_ids == [] and skipped_file_ids != [] do
              {:error, {:no_compactable_files, Enum.reverse(skipped_file_ids)}}
            else
              {:ok, {total_written, total_dropped, total_reclaimed}}
            end

          {nil, [_ | _]} ->
            {:error, {:compaction_failed, Enum.reverse(failures)}}

          {failure, []} ->
            {:error, {:compaction_failed, [failure]}}

          {failure, [_ | _]} ->
            {:error, {:compaction_failed, Enum.reverse(failures, [failure])}}
        end

      {:reply, reply, %{state | file_stats: new_file_stats}}
    catch
      {:bitcask_writer_flush_failed, reason, abort_state} ->
        {:reply, {:error, {:bitcask_writer_flush_failed, reason}}, abort_state}
    end
  end

  def handle_call(:available_disk_space, _from, state) do
    {:reply, NIF.v2_available_disk_space(state.shard_data_path), state}
  end

  # Synchronous flush — used by tests and by delete to ensure durability.
  def handle_call(:flush, _from, state) do
    state =
      state
      |> flush_standalone_batch()
      |> await_standalone_flush()
      |> await_in_flight()

    state = flush_pending_sync(state)

    case Map.get(state, :last_flush_error) do
      nil -> {:reply, :ok, state}
      reason -> {:reply, {:error, {:flush_failed, reason}}, state}
    end
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
    cond do
      default_waraft_write_state?(state) ->
        reply_default_waraft_write(command, state)

      state.raft? ->
        # Forward through Batcher for Raft consensus, same as put/delete.
        Ferricstore.Raft.Batcher.write_async(state.index, command, from)
        {:noreply, state}

      true ->
        # No Raft — apply directly via state machine.
        sm_state = direct_sm_state(state)

        case Ferricstore.Raft.StateMachine.apply(%{}, command, sm_state) do
          {new_sm_state, result} ->
            {:reply, result, apply_direct_sm_state(state, new_sm_state)}

          {new_sm_state, result, _effects} ->
            {:reply, result, apply_direct_sm_state(state, new_sm_state)}
        end
    end
  end

  defp native_flow_index_for_count_many(_state, []), do: :not_native_or_mixed

  defp native_flow_index_for_count_many(state, keys) do
    case native_flow_index_for_read(state, hd(keys)) do
      :not_native ->
        :not_native_or_mixed

      :unavailable ->
        :unavailable

      {:ok, native} ->
        Enum.reduce_while(keys, {:ok, native}, fn key, {:ok, first_native} ->
          case native_flow_index_for_read(state, key) do
            {:ok, ^first_native} -> {:cont, {:ok, first_native}}
            :unavailable -> {:halt, :unavailable}
            _other -> {:halt, :not_native_or_mixed}
          end
        end)
        |> case do
          {:ok, native} -> {:ok, native}
          :unavailable -> :unavailable
          :not_native_or_mixed -> :not_native_or_mixed
        end
    end
  end

  defp native_flow_index_for_read(state, key) do
    if native_flow_lifecycle_index_key?(key) do
      case NativeFlowIndex.get(state.flow_index, state.flow_lookup) do
        nil -> :unavailable
        native -> {:ok, native}
      end
    else
      :not_native
    end
  end

  defp native_flow_lifecycle_index_key?(key) when is_binary(key) do
    :binary.match(key, "}:d:") != :nomatch or
      :binary.match(key, "}:i:s:") != :nomatch or
      :binary.match(key, "}:i:r:") != :nomatch or
      :binary.match(key, "}:i:w:") != :nomatch
  end

  defp native_flow_lifecycle_index_key?(_key), do: false

  defp maybe_emit_compaction_crc_mismatch(state, fid, source, dest, reason) do
    if compaction_crc_mismatch?(reason) do
      :telemetry.execute(
        [:ferricstore, :bitcask, :compaction_crc_mismatch],
        %{count: 1},
        %{
          shard_index: state.index,
          file_id: fid,
          path: source,
          dest: dest,
          reason: inspect(reason)
        }
      )
    end
  end

  defp compaction_crc_mismatch?(reason) do
    reason
    |> inspect()
    |> String.downcase()
    |> String.contains?("crc mismatch")
  end

  defp apply_direct_sm_state(state, sm_state) do
    %{
      state
      | active_file_id: Map.get(sm_state, :active_file_id, state.active_file_id),
        active_file_path: Map.get(sm_state, :active_file_path, state.active_file_path),
        active_file_size: Map.get(sm_state, :active_file_size, state.active_file_size),
        file_stats: Map.get(sm_state, :file_stats, state.file_stats)
    }
  end

  defp direct_sm_state(state) do
    %{
      shard_index: state.index,
      data_dir: state.data_dir,
      data_dir_expanded: Path.expand(state.data_dir),
      shard_data_path: state.shard_data_path,
      shard_data_path_expanded: Path.expand(state.shard_data_path),
      active_file_id: state.active_file_id,
      active_file_path: state.active_file_path,
      active_file_size: state.active_file_size,
      file_stats: state.file_stats,
      merge_config: state.merge_config,
      max_active_file_size: state.max_active_file_size,
      ets: state.ets,
      applied_count: 0,
      release_cursor_interval:
        Application.get_env(:ferricstore, :release_cursor_interval, 200_000),
      cross_shard_locks: %{},
      cross_shard_intents: %{},
      instance_ctx: state.instance_ctx,
      instance_name: if(state.instance_ctx, do: state.instance_ctx.name, else: :default),
      zset_score_index_name: state.zset_score_index,
      zset_score_lookup_name: state.zset_score_lookup,
      flow_index_name: state.flow_index,
      flow_lookup_name: state.flow_lookup,
      flow_lmdb_path: Ferricstore.Flow.LMDB.path(state.shard_data_path),
      flow_async_history: flow_async_history_enabled?()
    }
  end

  defp flow_async_history_enabled? do
    case Application.get_env(:ferricstore, :flow_async_history) do
      value when value in [true, "1", "true"] -> true
      value when value in [false, "0", "false"] -> false
      _ -> System.get_env("FLOW_ASYNC_HISTORY", "true") in ["1", "true"]
    end
  end

  defp enqueue_standalone_commit(state, from, command) do
    entry = {from, command, standalone_command_keys(command)}

    if state.standalone_write_barrier or
         standalone_entry_conflicts?(entry, state.standalone_inflight_keys) do
      %{state | standalone_waiting: state.standalone_waiting ++ [entry]}
    else
      enqueue_ready_standalone_commit(state, entry)
    end
  end

  defp enqueue_ready_standalone_commit(state, entry) do
    timer =
      if state.standalone_batch_timer == nil and state.standalone_flush_ref == nil do
        Process.send_after(self(), :standalone_commit_flush, standalone_commit_delay_ms())
      else
        state.standalone_batch_timer
      end

    %{
      state
      | standalone_batch: [entry | state.standalone_batch],
        standalone_batch_timer: timer
    }
  end

  defp maybe_flush_full_standalone_batch(state) do
    if length(state.standalone_batch) >= standalone_commit_max_ops() do
      flush_standalone_batch(state)
    else
      state
    end
  end

  defp standalone_commit_delay_ms do
    :ferricstore
    |> Application.get_env(:standalone_fsync_max_delay_ms, @flush_interval_ms)
    |> normalize_positive_integer(@flush_interval_ms)
  end

  defp standalone_commit_max_ops do
    :ferricstore
    |> Application.get_env(:standalone_fsync_max_ops, 1024)
    |> normalize_positive_integer(1024)
  end

  defp normalize_positive_integer(value, _default) when is_integer(value) and value > 0, do: value
  defp normalize_positive_integer(_value, default), do: default

  defp flush_standalone_batch(%{standalone_batch: []} = state) do
    %{state | standalone_batch_timer: nil}
  end

  defp flush_standalone_batch(%{standalone_flush_ref: ref} = state) when ref != nil do
    %{state | standalone_batch_timer: nil}
  end

  defp flush_standalone_batch(state) do
    if timer = state.standalone_batch_timer do
      Process.cancel_timer(timer)
    end

    entries = Enum.reverse(state.standalone_batch)
    sm_state = direct_sm_state(state)
    ref = make_ref()
    parent = self()

    _pid =
      spawn_link(fn ->
        send(parent, {:standalone_commit_flushed, ref, run_standalone_batch(entries, sm_state)})
      end)

    %{
      state
      | standalone_batch: [],
        standalone_batch_timer: nil,
        standalone_flush_ref: ref,
        standalone_flush_entries: entries,
        standalone_inflight_keys: standalone_entries_keys(entries)
    }
  end

  defp run_standalone_batch(entries, sm_state) do
    commands = Enum.map(entries, fn {_from, command, _keys} -> command end)

    try do
      if Enum.any?(commands, &match?({:cross_shard_tx, _}, &1)) do
        run_standalone_commands_sequential(commands, sm_state)
      else
        case Ferricstore.Raft.StateMachine.apply_standalone_batch(commands, sm_state) do
          {new_sm_state, {:ok, results}} when is_list(results) ->
            {:ok, new_sm_state, results}

          {new_sm_state, {:error, reason}} ->
            {:error, new_sm_state, reason}

          {new_sm_state, result} ->
            {:ok, new_sm_state, List.wrap(result)}
        end
      end
    rescue
      error ->
        {:error, nil, {:exception, error}}
    catch
      kind, reason ->
        {:error, nil, {kind, reason}}
    end
  end

  defp run_standalone_commands_sequential(commands, sm_state) do
    Enum.reduce_while(commands, {:ok, sm_state, []}, fn command, {:ok, acc_state, results} ->
      case run_standalone_command(command, acc_state) do
        {:ok, new_state, result} ->
          {:cont, {:ok, new_state, [result | results]}}

        {:error, new_state, reason} ->
          {:halt, {:error, new_state, reason}}
      end
    end)
    |> case do
      {:ok, new_state, results} -> {:ok, new_state, Enum.reverse(results)}
      {:error, new_state, reason} -> {:error, new_state, reason}
    end
  end

  defp run_standalone_command({:cross_shard_tx, _shard_batches} = command, sm_state) do
    case Ferricstore.Raft.StateMachine.apply_standalone_command(command, sm_state) do
      {new_sm_state, {:error, reason}} -> {:error, new_sm_state, reason}
      {new_sm_state, result} -> {:ok, new_sm_state, result}
      {new_sm_state, {:error, reason}, _effects} -> {:error, new_sm_state, reason}
      {new_sm_state, result, _effects} -> {:ok, new_sm_state, result}
    end
  end

  defp run_standalone_command(command, sm_state) do
    case Ferricstore.Raft.StateMachine.apply_standalone_batch([command], sm_state) do
      {new_sm_state, {:ok, [result]}} -> {:ok, new_sm_state, result}
      {new_sm_state, {:ok, results}} when is_list(results) -> {:ok, new_sm_state, results}
      {new_sm_state, {:error, reason}} -> {:error, new_sm_state, reason}
      {new_sm_state, result} -> {:ok, new_sm_state, result}
    end
  end

  defp handle_standalone_flush_result(ref, result, %{standalone_flush_ref: ref} = state) do
    entries = state.standalone_flush_entries

    state =
      %{
        state
        | standalone_flush_ref: nil,
          standalone_flush_entries: [],
          standalone_inflight_keys: MapSet.new()
      }

    case result do
      {:ok, new_sm_state, results} ->
        state = apply_direct_sm_state(state, new_sm_state)
        reply_standalone_results(entries, results)

        state
        |> drain_standalone_waiting()
        |> flush_ready_standalone_batch()

      {:error, new_sm_state, reason} ->
        state =
          if new_sm_state do
            apply_direct_sm_state(state, new_sm_state)
          else
            state
          end

        reply_standalone_error(entries, {:standalone_durability_failed, reason})
        reply_standalone_error(state.standalone_batch, {:standalone_durability_failed, reason})
        reply_standalone_error(state.standalone_waiting, {:standalone_durability_failed, reason})

        %{
          state
          | writes_paused: true,
            standalone_batch: [],
            standalone_batch_timer: nil,
            standalone_waiting: []
        }
    end
  end

  defp handle_standalone_flush_result(_ref, _result, state), do: state

  defp flush_ready_standalone_batch(%{standalone_batch: []} = state), do: state
  defp flush_ready_standalone_batch(state), do: flush_standalone_batch(state)

  defp clear_standalone_write_barrier(state), do: %{state | standalone_write_barrier: false}

  defp reject_queued_standalone_commits(state, reason) do
    if timer = state.standalone_batch_timer do
      Process.cancel_timer(timer)
    end

    reply_standalone_error(state.standalone_batch, reason)
    reply_standalone_error(state.standalone_waiting, reason)

    %{state | standalone_batch: [], standalone_batch_timer: nil, standalone_waiting: []}
  end

  defp await_standalone_flush(%{standalone_flush_ref: nil} = state), do: state

  defp await_standalone_flush(%{standalone_flush_ref: ref} = state) do
    receive do
      {:standalone_commit_flushed, ^ref, result} ->
        ref
        |> handle_standalone_flush_result(result, state)
        |> await_standalone_flush()
    after
      30_000 ->
        %{state | last_flush_error: {:standalone_commit_flush_timeout, ref}}
    end
  end

  defp drain_standalone_commits_for_sync(state) do
    state
    |> drain_standalone_waiting()
    |> flush_ready_standalone_batch()
    |> await_standalone_flush()
  end

  defp drain_standalone_waiting(%{standalone_write_barrier: true} = state), do: state

  defp drain_standalone_waiting(state) do
    {ready, waiting} =
      Enum.split_with(state.standalone_waiting, fn entry ->
        not standalone_entry_conflicts?(entry, state.standalone_inflight_keys)
      end)

    %{
      state
      | standalone_batch: Enum.reverse(ready) ++ state.standalone_batch,
        standalone_waiting: waiting
    }
  end

  defp standalone_entry_conflicts?({_from, _command, keys}, inflight_keys) do
    (standalone_global_keys?(keys) and MapSet.size(inflight_keys) > 0) or
      standalone_global_keys?(inflight_keys) or
      not MapSet.disjoint?(keys, inflight_keys)
  end

  defp standalone_global_keys?(keys), do: MapSet.member?(keys, @standalone_global_key)

  defp standalone_entries_keys(entries) do
    Enum.reduce(entries, MapSet.new(), fn {_from, _command, keys}, acc ->
      MapSet.union(acc, keys)
    end)
  end

  defp standalone_command_keys({:batch, commands}) when is_list(commands) do
    commands
    |> Enum.reduce(MapSet.new(), fn command, acc ->
      MapSet.union(acc, standalone_command_keys(command))
    end)
    |> standalone_nonempty_keys()
  end

  defp standalone_command_keys({:async, _origin, command}) when is_tuple(command) do
    standalone_command_keys(command)
  end

  defp standalone_command_keys({:async, command}) when is_tuple(command) do
    standalone_command_keys(command)
  end

  defp standalone_command_keys({command, %{hlc_ts: _remote_ts}}) when is_tuple(command) do
    standalone_command_keys(command)
  end

  defp standalone_command_keys({:cross_shard_tx, shard_batches}) when is_list(shard_batches) do
    shard_batches
    |> Enum.reduce(MapSet.new(), fn
      {_shard_idx, entries, _namespace}, acc when is_list(entries) ->
        Enum.reduce(entries, acc, fn entry, acc ->
          MapSet.union(acc, standalone_cross_shard_entry_keys(entry))
        end)

      _other, acc ->
        MapSet.put(acc, @standalone_global_key)
    end)
    |> standalone_nonempty_keys()
  end

  defp standalone_command_keys({:cross_shard_tx, _shard_batches}) do
    standalone_global_keys()
  end

  defp standalone_command_keys({:server_command, _command}) do
    standalone_global_keys()
  end

  defp standalone_command_keys({:list_op_lmove, source, destination, _from_dir, _to_dir}) do
    MapSet.new([standalone_lock_key(source), standalone_lock_key(destination)])
  end

  defp standalone_command_keys({:pfmerge, destination, sources}) when is_list(sources) do
    standalone_lock_keys([destination | sources])
  end

  defp standalone_command_keys({:pfmerge, destination, source_keys, _source_sketches})
       when is_list(source_keys) do
    standalone_lock_keys([destination | source_keys])
  end

  defp standalone_command_keys({:cms_merge, destination, sources, _weights, _create_params})
       when is_list(sources) do
    standalone_lock_keys([destination | sources])
  end

  defp standalone_command_keys({:lock_keys, keys, _owner_ref, _expire_at_ms})
       when is_list(keys) do
    standalone_lock_keys(keys)
  end

  defp standalone_command_keys({:unlock_keys, keys, _owner_ref}) when is_list(keys) do
    standalone_lock_keys(keys)
  end

  defp standalone_command_keys({:locked_put, key, _value, _expire_at_ms, _owner_ref}) do
    MapSet.new([standalone_lock_key(key)])
  end

  defp standalone_command_keys({:locked_delete, key, _owner_ref}) do
    MapSet.new([standalone_lock_key(key)])
  end

  defp standalone_command_keys({:locked_delete_prefix, prefix, _owner_ref}) do
    MapSet.new([standalone_lock_key(prefix)])
  end

  defp standalone_command_keys({:compound_put, redis_key, _compound_key, _value, _expire_at_ms}) do
    MapSet.new([standalone_lock_key(redis_key)])
  end

  defp standalone_command_keys({:compound_delete, redis_key, _compound_key}) do
    MapSet.new([standalone_lock_key(redis_key)])
  end

  defp standalone_command_keys({:compound_delete_prefix, redis_key, _prefix}) do
    MapSet.new([standalone_lock_key(redis_key)])
  end

  defp standalone_command_keys({:compound_put, compound_key, _value, _expire_at_ms}) do
    MapSet.new([standalone_lock_key(compound_key)])
  end

  defp standalone_command_keys({:compound_delete, compound_key}) do
    MapSet.new([standalone_lock_key(compound_key)])
  end

  defp standalone_command_keys({:compound_delete_prefix, prefix}) do
    MapSet.new([standalone_lock_key(prefix)])
  end

  defp standalone_command_keys({command, route_key, %{records: records}})
       when command in @standalone_flow_many_commands and is_list(records) do
    standalone_flow_record_keys(route_key, records)
  end

  defp standalone_command_keys({:flow_spawn_children, route_key, %{children: children}})
       when is_list(children) do
    standalone_flow_record_keys(route_key, children)
  end

  defp standalone_command_keys({:flow_claim_due, _route_key, _attrs}) do
    # Claims discover and mutate record keys by scanning due indexes, so the
    # exact write set is not knowable before the state machine runs.
    standalone_global_keys()
  end

  defp standalone_command_keys(command) when is_tuple(command) and tuple_size(command) > 1 do
    case elem(command, 1) do
      key when is_binary(key) -> MapSet.new([standalone_lock_key(key)])
      _other -> standalone_global_keys()
    end
  end

  defp standalone_command_keys(_command), do: standalone_global_keys()

  defp standalone_lock_keys(keys) do
    keys
    |> Enum.reduce(MapSet.new(), fn key, acc ->
      if is_binary(key) do
        MapSet.put(acc, standalone_lock_key(key))
      else
        MapSet.put(acc, @standalone_global_key)
      end
    end)
    |> standalone_nonempty_keys()
  end

  defp standalone_lock_key(key) when is_binary(key) do
    Ferricstore.Store.CompoundKey.extract_redis_key(key)
  end

  defp standalone_nonempty_keys(keys) do
    if MapSet.size(keys) == 0 do
      standalone_global_keys()
    else
      keys
    end
  end

  defp standalone_global_keys, do: MapSet.new([@standalone_global_key])

  defp standalone_cross_shard_entry_keys({_pos, tx_entry}),
    do: standalone_tx_entry_keys(tx_entry)

  defp standalone_cross_shard_entry_keys(_entry), do: standalone_global_keys()

  defp standalone_tx_entry_keys({_name, args, command}) do
    command
    |> standalone_tx_command_keys(args)
    |> standalone_lock_keys()
  end

  defp standalone_tx_entry_keys(_entry), do: standalone_global_keys()

  defp standalone_tx_command_keys({:msetnx, args}, _args), do: every_other_arg(args)

  defp standalone_tx_command_keys({:copy, source, destination, _replace?}, _args),
    do: [source, destination]

  defp standalone_tx_command_keys({:rename, source, destination}, _args),
    do: [source, destination]

  defp standalone_tx_command_keys({:renamenx, source, destination}, _args),
    do: [source, destination]

  defp standalone_tx_command_keys({:lmove, source, destination, _from_dir, _to_dir}, _args),
    do: [source, destination]

  defp standalone_tx_command_keys({:smove, source, destination, _member}, _args),
    do: [source, destination]

  defp standalone_tx_command_keys({command, [destination | sources]}, _args)
       when command in [:sdiffstore, :sinterstore, :sunionstore] and is_list(sources),
       do: [destination | sources]

  defp standalone_tx_command_keys(_command, args) when is_list(args),
    do: standalone_tx_args_keys(args)

  defp standalone_tx_command_keys(_command, _args), do: [@standalone_global_key]

  defp standalone_tx_args_keys(["MSETNX" | args]), do: every_other_arg(args)
  defp standalone_tx_args_keys([source, destination | _rest]), do: [source, destination]
  defp standalone_tx_args_keys(_args), do: [@standalone_global_key]

  defp every_other_arg(args) when is_list(args) do
    args
    |> Enum.with_index()
    |> Enum.filter(fn {_arg, index} -> rem(index, 2) == 0 end)
    |> Enum.map(fn {arg, _index} -> arg end)
  end

  defp every_other_arg(_args), do: [@standalone_global_key]

  defp standalone_flow_record_keys(route_key, records) do
    records
    |> Enum.reduce_while([route_key], fn
      %{id: id} = attrs, acc when is_binary(id) ->
        key = Ferricstore.Flow.Keys.state_key(id, Map.get(attrs, :partition_key))
        {:cont, [key | acc]}

      _record, _acc ->
        {:halt, :global}
    end)
    |> case do
      :global -> standalone_global_keys()
      keys -> standalone_lock_keys(keys)
    end
  end

  defp reply_standalone_results(entries, results) when length(entries) == length(results) do
    Enum.zip(entries, results)
    |> Enum.each(fn {{from, _command, _keys}, result} -> GenServer.reply(from, result) end)
  end

  defp reply_standalone_results([{from, {:batch, _commands}, _keys}], results) do
    GenServer.reply(from, {:ok, results})
  end

  defp reply_standalone_results(entries, [result]) do
    Enum.each(entries, fn {from, _command, _keys} -> GenServer.reply(from, result) end)
  end

  defp reply_standalone_results(entries, results) do
    reply_standalone_error(entries, {:standalone_result_mismatch, length(results)})
  end

  defp reply_standalone_error(entries, reason) do
    Enum.each(entries, fn {from, _command, _keys} -> GenServer.reply(from, {:error, reason}) end)
  end

  @doc false
  def __standalone_command_keys_for_test__(command) do
    command
    |> standalone_command_keys()
    |> MapSet.to_list()
    |> Enum.sort()
  end

  @doc false
  def __standalone_command_keys_conflict_for_test__(left, right) do
    left_keys = standalone_command_keys(left)
    right_keys = standalone_command_keys(right)

    standalone_entry_conflicts?({nil, left, left_keys}, right_keys)
  end

  defp maybe_route_default_waraft_write(command, state, local_fun) do
    if default_waraft_write_state?(state) do
      reply_default_waraft_write(command, state)
    else
      local_fun.()
    end
  end

  defp default_waraft_write_state?(%{instance_ctx: %{name: :default}}), do: RaftBackend.waraft?()
  defp default_waraft_write_state?(_state), do: false

  defp reply_default_waraft_write(command, state) do
    result =
      case command do
        {:batch, commands} when is_list(commands) ->
          RaftBackend.write_batch(state.index, commands)

        _other ->
          RaftBackend.write(state.index, command)
      end

    case result do
      {:error, _reason} ->
        {:reply, result, state}

      _ok ->
        new_state = %{state | write_version: state.write_version + 1}
        bump_shared_write_version(new_state, 1)
        {:reply, result, new_state}
    end
  end

  defp shared_write_version(%{instance_ctx: %{write_version: write_version}, index: index}) do
    counter_value(write_version, index)
  end

  defp shared_write_version(%{instance_ctx: nil, index: index}) do
    Ferricstore.Store.WriteVersion.get(index)
  rescue
    _ -> 0
  end

  defp counter_value(write_version, index) do
    size = :counters.info(write_version).size
    if index < size, do: :counters.get(write_version, index + 1), else: 0
  rescue
    _ -> 0
  end

  defp track_write_version_result({:reply, reply, new_state}, old_state) do
    {:reply, reply, mirror_direct_write_version(new_state, old_state)}
  end

  defp track_write_version_result({:noreply, new_state}, old_state) do
    {:noreply, mirror_direct_write_version(new_state, old_state)}
  end

  defp track_write_version_result(other, _old_state), do: other

  # Custom/direct shards do not go through Router's quorum bump, so mirror
  # their local WATCH token into the instance counter that survives shard restart.
  defp mirror_direct_write_version(
         %{instance_ctx: %{name: :default}, raft?: false} = new_state,
         _old_state
       ) do
    new_state
  end

  defp mirror_direct_write_version(
         %{write_version: new_version} = new_state,
         %{raft?: false, write_version: old_version}
       )
       when new_version > old_version do
    bump_shared_write_version(new_state, new_version - old_version)
    new_state
  end

  defp mirror_direct_write_version(new_state, _old_state), do: new_state

  defp bump_shared_write_version(
         %{instance_ctx: %{write_version: write_version}, index: index},
         delta
       ) do
    size = :counters.info(write_version).size
    if index < size, do: :counters.add(write_version, index + 1, delta)
    :ok
  rescue
    _ -> :ok
  end

  defp bump_shared_write_version(%{instance_ctx: nil, index: index}, delta) do
    ref = :persistent_term.get(:ferricstore_write_versions)
    size = :counters.info(ref).size
    if index < size, do: :counters.add(ref, index + 1, delta)
    :ok
  rescue
    _ -> :ok
  end

  defp sync_active_file_from_registry(%{instance_ctx: nil} = state), do: state

  defp sync_active_file_from_registry(state) do
    case Ferricstore.Store.ActiveFile.get(state.instance_ctx, state.index) do
      {fid, path, shard_path}
      when fid != state.active_file_id or path != state.active_file_path ->
        active_size = active_file_size(path)

        %{
          state
          | active_file_id: fid,
            active_file_path: path,
            active_file_size: active_size,
            shard_data_path: shard_path,
            file_stats: Map.put(state.file_stats, fid, {active_size, 0})
        }

      _current ->
        state
    end
  rescue
    _ -> state
  end

  defp active_file_size(path) do
    case File.stat(path) do
      {:ok, %{size: size}} -> size
      {:error, _reason} -> 0
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

  defp handle_forwarded_quorum({:batch, commands}, _from, state) when is_list(commands) do
    {:reply, Ferricstore.Raft.Batcher.write(state.index, {:batch, commands}), state}
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

  defp handle_forwarded_quorum({:compound_batch_delete, redis_key, compound_keys}, _from, state) do
    ShardCompound.handle_compound_batch_delete(redis_key, compound_keys, state)
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

  defp remove_compacted_source(state, source) do
    case Ferricstore.FS.rm(source) do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.error(
          "Shard #{state.index}: compaction failed to remove source #{source}: #{inspect(reason)}"
        )

        {:error, reason}
    end
  end

  # A tombstone-only file protects deleted keys only when older log history
  # still contains a live version of one of those keys. Keeping the file just
  # because any lower fid exists leaks tombstone-only logs for unrelated keys.
  defp tombstone_file_still_needed?(shard_path, fid, tombstone_path) do
    with {:ok, tombstones} <- NIF.v2_scan_tombstones(tombstone_path),
         masked_keys =
           MapSet.new(tombstones, fn {key, _offset, _record_size, _expire_at_ms} -> key end),
         false <- MapSet.size(masked_keys) == 0,
         {:ok, files} <- Ferricstore.FS.ls(shard_path),
         {:ok, states} <- scan_lower_tombstone_key_states(shard_path, files, fid, masked_keys) do
      Enum.any?(masked_keys, fn key -> Map.get(states, key) == :live end)
    else
      true -> false
      _ -> true
    end
  end

  defp scan_lower_tombstone_key_states(shard_path, files, fid, masked_keys) do
    candidate_files =
      files
      |> Enum.flat_map(fn name ->
        with true <- String.ends_with?(name, ".log"),
             false <- String.starts_with?(name, "compact_"),
             {other_fid, ""} <- Integer.parse(String.trim_trailing(name, ".log")),
             true <- other_fid < fid do
          [{other_fid, Path.join(shard_path, name)}]
        else
          _ -> []
        end
      end)
      |> Enum.sort_by(fn {other_fid, _path} -> -other_fid end)

    started_at = System.monotonic_time()
    masked_key_count = MapSet.size(masked_keys)

    now_ms = Ferricstore.HLC.now_ms()

    result =
      candidate_files
      |> Enum.reduce_while({:ok, %{}, masked_keys, 0}, fn {_other_fid, path},
                                                          {:ok, states, unresolved_keys,
                                                           files_scanned} ->
        next_files_scanned = files_scanned + 1

        case NIF.v2_scan_key_states(path, MapSet.to_list(unresolved_keys)) do
          {:ok, records} ->
            file_states =
              Enum.reduce(records, %{}, fn {key, expire_at_ms, tombstone?}, acc ->
                Map.put(
                  acc,
                  key,
                  tombstone_dependency_state(tombstone?, expire_at_ms, now_ms)
                )
              end)

            next_states = Map.merge(states, file_states)

            next_unresolved_keys =
              Enum.reduce(Map.keys(file_states), unresolved_keys, &MapSet.delete(&2, &1))

            if MapSet.size(next_unresolved_keys) == 0 do
              {:halt, {:ok, next_states, next_unresolved_keys, next_files_scanned}}
            else
              {:cont, {:ok, next_states, next_unresolved_keys, next_files_scanned}}
            end

          {:error, reason} ->
            {:halt, {:error, reason, next_files_scanned}}
        end
      end)

    case result do
      {:ok, states, unresolved_keys, files_scanned} ->
        emit_tombstone_dependency_scan(
          shard_path,
          fid,
          :ok,
          started_at,
          length(candidate_files),
          files_scanned,
          masked_key_count,
          masked_key_count - MapSet.size(unresolved_keys)
        )

        {:ok, states}

      {:error, reason, files_scanned} ->
        emit_tombstone_dependency_scan(
          shard_path,
          fid,
          :error,
          started_at,
          length(candidate_files),
          files_scanned,
          masked_key_count,
          0,
          reason
        )

        {:error, reason}
    end
  end

  defp emit_tombstone_dependency_scan(
         shard_path,
         fid,
         status,
         started_at,
         candidate_files,
         files_scanned,
         masked_keys,
         resolved_keys,
         reason \\ nil
       ) do
    metadata = %{
      shard_path: shard_path,
      fid: fid,
      status: status
    }

    metadata =
      if reason == nil do
        metadata
      else
        Map.put(metadata, :reason, reason)
      end

    :telemetry.execute(
      [:ferricstore, :bitcask, :tombstone_dependency_scan],
      %{
        candidate_files: candidate_files,
        files_scanned: files_scanned,
        masked_keys: masked_keys,
        resolved_keys: resolved_keys,
        duration_us:
          System.convert_time_unit(System.monotonic_time() - started_at, :native, :microsecond)
      },
      metadata
    )
  end

  defp tombstone_offsets(path) do
    case NIF.v2_scan_tombstones(path) do
      {:ok, tombstones} ->
        Enum.map(tombstones, fn {_key, offset, _record_size, _expire_at_ms} -> offset end)

      _ ->
        []
    end
  end

  defp needed_tombstone_offsets(shard_path, fid, path) do
    with {:ok, tombstones} <- NIF.v2_scan_tombstones(path),
         false <- tombstones == [],
         tombstone_by_key =
           Map.new(tombstones, fn {key, offset, _record_size, _expire_at_ms} -> {key, offset} end),
         masked_keys = Map.keys(tombstone_by_key) |> MapSet.new(),
         {:ok, files} <- Ferricstore.FS.ls(shard_path),
         {:ok, states} <- scan_lower_tombstone_key_states(shard_path, files, fid, masked_keys) do
      tombstone_by_key
      |> Enum.filter(fn {key, _offset} -> Map.get(states, key) == :live end)
      |> Enum.map(fn {_key, offset} -> offset end)
    else
      true ->
        []

      _ ->
        tombstone_offsets(path)
    end
  end

  defp remove_hint_for_file(shard_path, fid) do
    # Compaction rewrites or invalidates offsets in the paired log file.
    # Dropping the hint forces startup to scan the log instead of trusting
    # stale offsets that can resurrect deleted keys.
    hint_name = "#{String.pad_leading(Integer.to_string(fid), 5, "0")}.hint"
    hint_path = Path.join(shard_path, hint_name)

    case Ferricstore.FS.rm(hint_path) do
      :ok ->
        :ok

      {:error, {:not_found, _}} ->
        :ok

      {:error, reason} ->
        Logger.warning(
          "failed to remove stale compaction hint file #{hint_path}: #{inspect(reason)}"
        )
    end

    :ok
  end

  defp remove_compaction_temp(state, path) do
    case Ferricstore.FS.rm(path) do
      :ok ->
        :ok

      {:error, {:not_found, _}} ->
        :ok

      {:error, reason} ->
        Logger.warning(
          "Shard #{state.index}: failed to remove compaction temp file #{path}: #{inspect(reason)}"
        )
    end
  end

  defp group_compaction_live_entries(_state, []), do: %{}

  defp group_compaction_live_entries(state, file_ids) do
    target_fids = MapSet.new(file_ids)
    now_ms = Ferricstore.HLC.now_ms()

    :ets.foldl(
      fn
        {key, _value, expire_at_ms, _lfu, fid, off, _vsize}, acc
        when expire_at_ms == 0 or expire_at_ms > now_ms ->
          if MapSet.member?(target_fids, fid) and fid != state.active_file_id and
               shared_compaction_entry?(state, key, fid, fid) do
            Map.update(acc, fid, [{key, off}], &[{key, off} | &1])
          else
            acc
          end

        {_key, _value, _expire_at_ms, _lfu, _fid, _off, _vsize}, acc ->
          acc
      end,
      %{},
      state.keydir
    )
  end

  defp tombstone_dependency_state(true, _expire_at_ms, _now_ms), do: :tombstone
  defp tombstone_dependency_state(false, 0, _now_ms), do: :live

  defp tombstone_dependency_state(false, expire_at_ms, now_ms) when expire_at_ms > now_ms,
    do: :live

  defp tombstone_dependency_state(false, _expire_at_ms, _now_ms), do: :expired

  defp shared_compaction_entry?(state, key, fid, target_fid) do
    fid == target_fid and not promoted_data_compound_entry?(state, key)
  end

  # Promoted collection data is stored in dedicated Bitcask dirs but reuses the
  # same ETS location tuple shape. Shared-log compaction must not interpret
  # those file ids and offsets as shared-log locations.
  defp promoted_data_compound_entry?(state, <<"H:", _rest::binary>> = key),
    do: promoted_parent?(state, key)

  defp promoted_data_compound_entry?(state, <<"S:", _rest::binary>> = key),
    do: promoted_parent?(state, key)

  defp promoted_data_compound_entry?(state, <<"Z:", _rest::binary>> = key),
    do: promoted_parent?(state, key)

  defp promoted_data_compound_entry?(_state, _key), do: false

  defp promoted_parent?(state, compound_key) do
    redis_key = CompoundKey.extract_redis_key(compound_key)
    Map.has_key?(state.promoted_instances, redis_key)
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
  def handle_info(
        {:tx_pending_compound_write, redis_key, compound_key, value, expire_at_ms},
        state
      ) do
    case ShardCompound.handle_compound_put(redis_key, compound_key, value, expire_at_ms, state) do
      {:reply, :ok, new_state} ->
        {:noreply, new_state}

      {:reply, {:error, reason}, new_state} ->
        Logger.error(
          "Shard #{state.index}: tx promoted compound write failed: #{inspect(reason)}"
        )

        {:noreply, new_state}

      {:reply, _other, new_state} ->
        {:noreply, new_state}
    end
  end

  def handle_info({:tx_pending_compound_delete, redis_key, compound_key}, state) do
    case ShardCompound.handle_compound_delete(redis_key, compound_key, state) do
      {:reply, :ok, new_state} ->
        {:noreply, new_state}

      {:reply, {:error, reason}, new_state} ->
        Logger.error(
          "Shard #{state.index}: tx promoted compound delete failed: #{inspect(reason)}"
        )

        {:noreply, new_state}

      {:reply, _other, new_state} ->
        {:noreply, new_state}
    end
  end

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

  def handle_info(:standalone_commit_flush, state) do
    {:noreply, flush_standalone_batch(state)}
  end

  def handle_info({:standalone_commit_flushed, ref, result}, state) do
    {:noreply, handle_standalone_flush_result(ref, result, state)}
  end

  # Periodic fragmentation re-evaluation for idle shards.
  # Catches shards that accumulated dead data then stopped receiving writes.
  # Disk pressure is intentionally not cleared here; only a successful append or
  # fsync proves that the shard can accept writes again.
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
  def handle_info({:cold_read_timeout, corr_id}, state) do
    case pop_pending_read(state.pending_reads, corr_id, :keep_timer) do
      {{from, _key, _exp, _fid, _off, _vsize} = pending_entry, rest_pending} ->
        emit_pending_read_error(state, pending_entry, :timeout)
        GenServer.reply(from, nil)
        {:noreply, %{state | pending_reads: rest_pending}}

      {{from, _key, :meta, _exp, _fid, _off, _vsize} = pending_entry, rest_pending} ->
        emit_pending_read_error(state, pending_entry, :timeout)
        GenServer.reply(from, nil)
        {:noreply, %{state | pending_reads: rest_pending}}

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

  def handle_info({:cold_read_retry, pending_entry, attempts_left}, state) do
    retry_or_reply_nil_cold_read(state, pending_entry, attempts_left)
  end

  def handle_info({:tokio_complete, corr_id, :ok, value}, state) do
    cond do
      # Async fsync completion — value is :ok for fsync
      corr_id == state.flush_in_flight ->
        {:noreply, %{state | flush_in_flight: nil}}

      # Async read completion — look up in pending_reads
      true ->
        case pop_pending_read(state.pending_reads, corr_id, :cancel_timer) do
          {{from, key, exp, fid, off, vsize}, rest_pending} ->
            # Simple GET cold-read completion. Warm only if the ETS entry still
            # points at the same disk location read by this request.
            if value != nil do
              case materialize_pending_cold_value(state, value) do
                {:ok, materialized} ->
                  ShardETS.cold_read_warm_ets(state, key, materialized, exp, fid, off, vsize)
                  GenServer.reply(from, materialized)
                  {:noreply, %{state | pending_reads: rest_pending}}

                {:error, _reason} ->
                  retry_or_reply_nil_cold_read(
                    %{state | pending_reads: rest_pending},
                    {from, key, exp, fid, off, vsize},
                    @cold_read_compaction_retry_attempts
                  )
              end
            else
              retry_or_reply_nil_cold_read(
                %{state | pending_reads: rest_pending},
                {from, key, exp, fid, off, vsize},
                @cold_read_compaction_retry_attempts
              )
            end

          {{from, key, :meta, exp, fid, off, vsize}, rest_pending} ->
            # GET_META cold-read completion. The reply may linearize before a
            # later overwrite, but ETS warming must still be location-checked.
            if value != nil do
              case materialize_pending_cold_value(state, value) do
                {:ok, materialized} ->
                  ShardETS.cold_read_warm_ets(state, key, materialized, exp, fid, off, vsize)
                  GenServer.reply(from, {materialized, exp})
                  {:noreply, %{state | pending_reads: rest_pending}}

                {:error, _reason} ->
                  retry_or_reply_nil_cold_read(
                    %{state | pending_reads: rest_pending},
                    {from, key, :meta, exp, fid, off, vsize},
                    @cold_read_compaction_retry_attempts
                  )
              end
            else
              retry_or_reply_nil_cold_read(
                %{state | pending_reads: rest_pending},
                {from, key, :meta, exp, fid, off, vsize},
                @cold_read_compaction_retry_attempts
              )
            end

          {{from, _key}, rest_pending} ->
            # Legacy in-memory pending entry without disk location. Reply but
            # do not warm, because the current ETS location may be newer than
            # the value returned by the async read.
            GenServer.reply(from, value)
            {:noreply, %{state | pending_reads: rest_pending}}

          {{from, _key, :meta, exp}, rest_pending} ->
            # Legacy in-memory pending entry without disk location. Reply but
            # skip warming for the same stale-completion reason as simple GET.
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
      case pop_pending_read(state.pending_reads, corr_id, :cancel_timer) do
        {{from, _key, _exp, _fid, _off, _vsize} = pending_entry, rest_pending} ->
          emit_pending_read_error(state, pending_entry, reason)
          GenServer.reply(from, nil)
          {:noreply, %{state | pending_reads: rest_pending}}

        {{from, _key, :meta, _exp, _fid, _off, _vsize} = pending_entry, rest_pending} ->
          emit_pending_read_error(state, pending_entry, reason)
          GenServer.reply(from, nil)
          {:noreply, %{state | pending_reads: rest_pending}}

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

  defp pop_pending_read(pending_reads, corr_id, timer_action) do
    case Map.pop(pending_reads, corr_id) do
      {{:pending_read, entry, timer_ref}, rest_pending} ->
        maybe_cancel_pending_timer(timer_action, timer_ref)
        {entry, rest_pending}

      other ->
        other
    end
  end

  defp maybe_cancel_pending_timer(:cancel_timer, timer_ref) when is_reference(timer_ref) do
    _ = Process.cancel_timer(timer_ref, async: false, info: false)
    :ok
  end

  defp maybe_cancel_pending_timer(_timer_action, _timer_ref), do: :ok

  defp materialize_pending_cold_value(%{data_dir: data_dir, index: shard_index} = state, value) do
    BlobValue.maybe_materialize(
      data_dir,
      shard_index,
      BlobValue.threshold(Map.get(state, :instance_ctx)),
      value
    )
  end

  defp materialize_pending_cold_value(_state, value), do: {:ok, value}

  defp emit_pending_read_error(state, {_from, _key, _exp, fid, _off, _vsize}, reason) do
    emit_pending_read_error_for_fid(state, fid, reason)
  end

  defp emit_pending_read_error(state, {_from, _key, :meta, _exp, fid, _off, _vsize}, reason) do
    emit_pending_read_error_for_fid(state, fid, reason)
  end

  defp emit_pending_read_error(_state, _pending_entry, _reason), do: :ok

  defp emit_pending_read_error_for_fid(%{shard_data_path: shard_data_path}, fid, reason)
       when is_binary(shard_data_path) and is_integer(fid) and fid >= 0 do
    shard_data_path
    |> ShardETS.file_path(fid)
    |> ColdRead.emit_pread_error(reason)
  rescue
    _ -> :ok
  end

  defp emit_pending_read_error_for_fid(_state, _fid, _reason), do: :ok

  defp retry_or_reply_nil_cold_read(state, pending_entry, attempts_left) do
    case resolve_nil_cold_read(state, pending_entry, attempts_left) do
      {:reply, from, reply} ->
        GenServer.reply(from, reply)
        {:noreply, state}

      {:retry_later, pending_entry, next_attempts_left} ->
        Process.send_after(
          self(),
          {:cold_read_retry, pending_entry, next_attempts_left},
          @cold_read_compaction_retry_delay_ms
        )

        {:noreply, state}

      {:resubmit, pending_entry} ->
        resubmit_cold_read(state, pending_entry)
    end
  end

  defp resolve_nil_cold_read(
         state,
         {from, key, _exp, fid, off, vsize} = pending_entry,
         attempts_left
       ) do
    case ShardETS.ets_lookup(state, key) do
      {:hit, value, _expire_at_ms} ->
        {:reply, from, value}

      {:cold, ^fid, ^off, ^vsize, _exp} when attempts_left > 0 ->
        {:retry_later, pending_entry, attempts_left - 1}

      {:cold, ^fid, ^off, ^vsize, _exp} ->
        emit_cold_retry_exhausted(state, pending_entry, :get, :missing_live_cold_entry)
        emit_pending_read_error(state, pending_entry, :missing_live_cold_entry)
        {:reply, from, nil}

      {:cold, new_fid, new_off, new_vsize, new_exp} ->
        {:resubmit, {from, key, new_exp, new_fid, new_off, new_vsize}}

      :expired ->
        {:reply, from, nil}

      :miss ->
        {:reply, from, nil}

      _ ->
        {:reply, from, nil}
    end
  end

  defp resolve_nil_cold_read(
         state,
         {from, key, :meta, _exp, fid, off, vsize} = pending_entry,
         attempts_left
       ) do
    case ShardETS.ets_lookup(state, key) do
      {:hit, value, expire_at_ms} ->
        {:reply, from, {value, expire_at_ms}}

      {:cold, ^fid, ^off, ^vsize, _exp} when attempts_left > 0 ->
        {:retry_later, pending_entry, attempts_left - 1}

      {:cold, ^fid, ^off, ^vsize, _exp} ->
        emit_cold_retry_exhausted(state, pending_entry, :get_meta, :missing_live_cold_entry)
        emit_pending_read_error(state, pending_entry, :missing_live_cold_entry)
        {:reply, from, nil}

      {:cold, new_fid, new_off, new_vsize, new_exp} ->
        {:resubmit, {from, key, :meta, new_exp, new_fid, new_off, new_vsize}}

      :expired ->
        {:reply, from, nil}

      :miss ->
        {:reply, from, nil}

      _ ->
        {:reply, from, nil}
    end
  end

  defp resolve_nil_cold_read(_state, {from, _key}, _attempts_left), do: {:reply, from, nil}

  defp resolve_nil_cold_read(_state, {from, _key, :meta, _exp}, _attempts_left),
    do: {:reply, from, nil}

  defp emit_cold_retry_exhausted(
         %{index: shard_index, shard_data_path: shard_data_path},
         {_from, key, _exp, fid, off, vsize},
         operation,
         reason
       )
       when is_binary(shard_data_path) and is_integer(fid) and fid >= 0 do
    emit_cold_retry_exhausted_for_location(
      shard_index,
      shard_data_path,
      key,
      fid,
      off,
      vsize,
      operation,
      reason
    )
  end

  defp emit_cold_retry_exhausted(
         %{index: shard_index, shard_data_path: shard_data_path},
         {_from, key, :meta, _exp, fid, off, vsize},
         operation,
         reason
       )
       when is_binary(shard_data_path) and is_integer(fid) and fid >= 0 do
    emit_cold_retry_exhausted_for_location(
      shard_index,
      shard_data_path,
      key,
      fid,
      off,
      vsize,
      operation,
      reason
    )
  end

  defp emit_cold_retry_exhausted(_state, _pending_entry, _operation, _reason), do: :ok

  defp emit_cold_retry_exhausted_for_location(
         shard_index,
         shard_data_path,
         key,
         fid,
         off,
         vsize,
         operation,
         reason
       ) do
    path = ShardETS.file_path(shard_data_path, fid)

    :telemetry.execute(
      [:ferricstore, :store, :cold_read_retry_exhausted],
      %{count: 1, attempts: @cold_read_compaction_retry_attempts},
      %{
        source: :shard,
        operation: operation,
        shard_index: shard_index,
        redis_key_hash: :erlang.phash2(key),
        path: path,
        file_id: fid,
        offset: off,
        value_size: vsize,
        reason: reason
      }
    )
  rescue
    _ -> :ok
  end

  defp resubmit_cold_read(state, {from, key, _exp, fid, off, _vsize} = pending_entry) do
    path = ShardETS.file_path(state.shard_data_path, fid)
    corr_id = state.next_correlation_id + 1

    case NIF.v2_pread_at_key_async(self(), corr_id, path, off, key) do
      :ok ->
        timer_ref =
          Process.send_after(self(), {:cold_read_timeout, corr_id}, @cold_read_timeout_ms)

        {:noreply,
         %{
           state
           | next_correlation_id: corr_id,
             pending_reads:
               Map.put(state.pending_reads, corr_id, {:pending_read, pending_entry, timer_ref})
         }}

      {:error, reason} ->
        emit_pending_read_error(state, pending_entry, reason)
        GenServer.reply(from, nil)
        {:noreply, state}
    end
  end

  defp resubmit_cold_read(state, {from, key, :meta, _exp, fid, off, _vsize} = pending_entry) do
    path = ShardETS.file_path(state.shard_data_path, fid)
    corr_id = state.next_correlation_id + 1

    case NIF.v2_pread_at_key_async(self(), corr_id, path, off, key) do
      :ok ->
        timer_ref =
          Process.send_after(self(), {:cold_read_timeout, corr_id}, @cold_read_timeout_ms)

        {:noreply,
         %{
           state
           | next_correlation_id: corr_id,
             pending_reads:
               Map.put(state.pending_reads, corr_id, {:pending_read, pending_entry, timer_ref})
         }}

      {:error, reason} ->
        emit_pending_read_error(state, pending_entry, reason)
        GenServer.reply(from, nil)
        {:noreply, state}
    end
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
  def terminate(reason, state) do
    state
    |> flush_standalone_batch()
    |> await_standalone_flush()
    |> then(&ShardLifecycle.do_terminate(reason, &1))
  end

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

  defp compaction_fsync_dir(state, path) do
    case Map.get(state, :compaction_fsync_dir_fun) do
      fun when is_function(fun, 1) -> fun.(path)
      _ -> NIF.v2_fsync_dir(path)
    end
  end

  defp schedule_drain_pending(ms), do: ShardFlush.schedule_drain_pending(ms)

  # -------------------------------------------------------------------
  # Private: read helpers (delegates to Shard.Reads / Shard.ETS)
  # -------------------------------------------------------------------

  defp prefix_scan_entries(state_or_keydir, prefix, shard_data_path),
    do: ShardETS.prefix_scan_entries(state_or_keydir, prefix, shard_data_path)

  defp prefix_count_entries(state_or_keydir, prefix),
    do: ShardETS.prefix_count_entries(state_or_keydir, prefix)

  # -------------------------------------------------------------------
  # Private: Raft write helpers
  # -------------------------------------------------------------------

  # Submits a write command through Raft via the Batcher (group commit).
  defp raft_write(%__MODULE__{index: index}, command) do
    Ferricstore.Raft.Batcher.write(index, command)
  end
end
