defmodule FerricStore.Instance do
  @moduledoc """
  Instance context for a FerricStore instance.

  Each instance owns its own shards, ETS tables, Raft system, atomics,
  and config — fully isolated from other instances. The context struct
  holds all references needed to route operations without any global
  state (no persistent_term lookups).

  Created by `FerricStore.Instance.Supervisor.start_link/2` and cached
  in persistent_term per module name for ~0ns access via `__instance__/0`.

  ## Fields

  All fields are set once at startup and never change (except atomics/counters
  which are mutable shared references).
  """

  @type t :: %__MODULE__{
          name: atom(),
          data_dir: binary(),
          data_dir_expanded: binary(),
          shard_count: non_neg_integer(),
          slot_map: tuple(),
          shard_names: tuple(),
          keydir_refs: tuple(),
          ra_system: atom(),
          pressure_flags: reference(),
          disk_pressure: reference(),
          checkpoint_flags: reference(),
          checkpoint_in_flight: reference(),
          replay_safe_index: reference(),
          replay_safe_requested_index: reference(),
          replay_safe_persist_failures: reference(),
          last_applied_index: reference(),
          last_released_cursor_index: reference(),
          pending_release_cursor_checkpoint_count: reference(),
          release_cursor_blocked_apply_count: reference(),
          write_version: reference(),
          stats_counter: reference(),
          lfu_decay_time: non_neg_integer(),
          lfu_log_factor: non_neg_integer(),
          lfu_initial_ref: reference(),
          hot_cache_max_value_size: non_neg_integer(),
          sync_flush_timeout_ms: non_neg_integer(),
          max_active_file_size: non_neg_integer(),
          read_sample_rate: non_neg_integer(),
          eviction_policy: atom(),
          max_memory_bytes: non_neg_integer(),
          keydir_max_ram: non_neg_integer(),
          memory_limit: non_neg_integer(),
          keydir_binary_bytes: reference(),
          hotness_table: atom() | reference(),
          config_table: atom() | reference(),
          connected_clients_fn: (-> non_neg_integer()),
          process_rss_fn: (-> non_neg_integer() | nil) | nil,
          server_info_fn: (-> map()),
          raft_apply_hook: (term() -> term()) | nil
        }

  defstruct [
    :name,
    :data_dir,
    :data_dir_expanded,
    :shard_count,
    :slot_map,
    :shard_names,
    :keydir_refs,
    :ra_system,
    :pressure_flags,
    :disk_pressure,
    :checkpoint_flags,
    :checkpoint_in_flight,
    :replay_safe_index,
    :replay_safe_requested_index,
    :replay_safe_persist_failures,
    :last_applied_index,
    :last_released_cursor_index,
    :pending_release_cursor_checkpoint_count,
    :release_cursor_blocked_apply_count,
    :write_version,
    :stats_counter,
    :lfu_decay_time,
    :lfu_log_factor,
    :lfu_initial_ref,
    :hot_cache_max_value_size,
    :sync_flush_timeout_ms,
    :max_active_file_size,
    :read_sample_rate,
    :eviction_policy,
    :max_memory_bytes,
    :keydir_max_ram,
    :memory_limit,
    :hotness_table,
    :config_table,
    :keydir_binary_bytes,
    :latch_refs,
    connected_clients_fn: nil,
    process_rss_fn: nil,
    server_info_fn: nil,
    raft_apply_hook: nil
  ]

  @doc """
  Builds the instance context from the given options.

  This creates all the shared mutable references (atomics, counters, ETS tables)
  and computes the immutable config values. The returned context is stored in
  persistent_term for the module name.
  """
  @spec build(atom(), keyword()) :: t()
  def build(name, opts) do
    shard_count = Keyword.get(opts, :shard_count, 4)
    data_dir = Keyword.get(opts, :data_dir, "data")
    data_dir_expanded = Path.expand(data_dir)

    # Slot map: 1024 slots -> shard indices. Use the shared builder so
    # Router.shard_for/2 and CLUSTER.SLOTS expose the same ownership.
    slot_map = build_slot_map(shard_count)

    # Per-shard ETS tables (anonymous — no global name pollution)
    keydir_refs = build_keydir_tables(name, shard_count)

    # Per-shard latch tables — one ETS per shard, used by local direct RMW
    # helpers. Default-instance RMW commands go through Raft; these latches
    # are for embedded/custom contexts that mutate directly.
    latch_refs = build_latch_tables(name, shard_count)

    # Shard process names (via Registry or atoms)
    shard_names = build_shard_names(name, shard_count)

    # Shared mutable references.
    # For the :default instance, reuse the existing global refs created by
    # application.ex (MemoryGuard, DiskPressure, WriteVersion, Stats).
    # For custom instances, create fresh isolated refs.
    {pressure_flags, disk_pressure, write_version, stats_counter} =
      if name == :default do
        {
          try_get_pt(:ferricstore_pressure_flags, fn -> :atomics.new(3, signed: false) end),
          try_get_pt(:ferricstore_disk_pressure, fn ->
            :atomics.new(shard_count, signed: false)
          end),
          try_get_pt(:ferricstore_write_versions, fn ->
            :counters.new(shard_count, [:write_concurrency])
          end),
          :counters.new(10, [:atomics])
        }
      else
        {
          :atomics.new(3, signed: false),
          :atomics.new(shard_count, signed: false),
          :counters.new(shard_count, [:write_concurrency]),
          :counters.new(10, [:atomics])
        }
      end

    # Per-shard dirty flag for the BitcaskCheckpointer. 1 = "a nosync
    # append happened since the last fsync_async". The checkpointer
    # clears the flag after marking the fsync in-flight; writers re-set
    # it on every batch. Read/written from any process — no GenServer hop.
    checkpoint_flags =
      if name == :default do
        try_get_pt(:ferricstore_checkpoint_flags, fn ->
          :atomics.new(shard_count, signed: false)
        end)
      else
        :atomics.new(shard_count, signed: false)
      end

    # Per-shard marker for checkpoint fsync calls that are currently in flight.
    # Set before `checkpoint_flags` is cleared so Raft release_cursor never
    # observes a false-clean shard while Bitcask bytes are still page-cache-only.
    checkpoint_in_flight =
      if name == :default do
        try_get_pt(:ferricstore_checkpoint_in_flight, fn ->
          :atomics.new(shard_count, signed: false)
        end)
      else
        :atomics.new(shard_count, signed: false)
      end

    replay_safe_index =
      if name == :default do
        try_get_pt(:ferricstore_replay_safe_index, fn ->
          :atomics.new(shard_count, signed: false)
        end)
      else
        :atomics.new(shard_count, signed: false)
      end

    replay_safe_requested_index =
      if name == :default do
        try_get_pt(:ferricstore_replay_safe_requested_index, fn ->
          :atomics.new(shard_count, signed: false)
        end)
      else
        :atomics.new(shard_count, signed: false)
      end

    replay_safe_persist_failures =
      if name == :default do
        try_get_pt(:ferricstore_replay_safe_persist_failures, fn ->
          :atomics.new(shard_count, signed: false)
        end)
      else
        :atomics.new(shard_count, signed: false)
      end

    # Per-shard Ra index observability for the current coarse WAL/Bitcask gate.
    # These atomics do not affect release_cursor correctness; they let INFO show
    # how far Ra apply has advanced beyond the last emitted release cursor.
    last_applied_index =
      if name == :default do
        try_get_pt(:ferricstore_last_applied_index, fn ->
          :atomics.new(shard_count, signed: false)
        end)
      else
        :atomics.new(shard_count, signed: false)
      end

    last_released_cursor_index =
      if name == :default do
        try_get_pt(:ferricstore_last_released_cursor_index, fn ->
          :atomics.new(shard_count, signed: false)
        end)
      else
        :atomics.new(shard_count, signed: false)
      end

    pending_release_cursor_checkpoint_count =
      if name == :default do
        try_get_pt(:ferricstore_pending_release_cursor_checkpoint_count, fn ->
          :atomics.new(shard_count, signed: false)
        end)
      else
        :atomics.new(shard_count, signed: false)
      end

    release_cursor_blocked_apply_count =
      if name == :default do
        try_get_pt(:ferricstore_release_cursor_blocked_apply_count, fn ->
          :atomics.new(shard_count, signed: false)
        end)
      else
        :atomics.new(shard_count, signed: false)
      end

    # Per-shard counter for off-heap binary bytes in ETS keydirs.
    # :ets.info(:memory) doesn't count refc binaries (> 64 bytes).
    # We track insertions/deletions to give MemoryGuard accurate numbers.
    keydir_binary_bytes =
      if name == :default do
        try_get_pt(:ferricstore_keydir_binary_bytes, fn ->
          :atomics.new(shard_count, signed: true)
        end)
      else
        :atomics.new(shard_count, signed: true)
      end

    # LFU config
    lfu_decay_time = Keyword.get(opts, :lfu_decay_time, 1)
    lfu_log_factor = Keyword.get(opts, :lfu_log_factor, 10)
    lfu_initial_ref = :atomics.new(2, signed: false)

    # Hotness and config ETS tables (reuse existing for :default instance)
    hotness_name = if name == :default, do: :ferricstore_hotness, else: :"#{name}_hotness"
    config_name = if name == :default, do: :ferricstore_config, else: :"#{name}_config"

    hotness_table =
      case :ets.whereis(hotness_name) do
        :undefined ->
          :ets.new(hotness_name, [
            :set,
            :public,
            :named_table,
            {:read_concurrency, true},
            {:write_concurrency, :auto},
            {:decentralized_counters, true}
          ])

        _ref ->
          hotness_name
      end

    config_table =
      case :ets.whereis(config_name) do
        :undefined ->
          :ets.new(config_name, [:set, :public, :named_table, {:read_concurrency, true}])

        _ref ->
          config_name
      end

    # Memory limits
    max_memory_bytes = Keyword.get(opts, :max_memory_bytes, 1_073_741_824)
    keydir_max_ram = Keyword.get(opts, :keydir_max_ram, 256 * 1024 * 1024)
    memory_limit = Keyword.get(opts, :memory_limit) || detect_memory_limit()

    ctx = %__MODULE__{
      name: name,
      data_dir: data_dir,
      data_dir_expanded: data_dir_expanded,
      shard_count: shard_count,
      slot_map: slot_map,
      shard_names: shard_names,
      keydir_refs: keydir_refs,
      latch_refs: latch_refs,
      ra_system: :"#{name}_raft",
      pressure_flags: pressure_flags,
      disk_pressure: disk_pressure,
      checkpoint_flags: checkpoint_flags,
      checkpoint_in_flight: checkpoint_in_flight,
      replay_safe_index: replay_safe_index,
      replay_safe_requested_index: replay_safe_requested_index,
      replay_safe_persist_failures: replay_safe_persist_failures,
      last_applied_index: last_applied_index,
      last_released_cursor_index: last_released_cursor_index,
      pending_release_cursor_checkpoint_count: pending_release_cursor_checkpoint_count,
      release_cursor_blocked_apply_count: release_cursor_blocked_apply_count,
      write_version: write_version,
      stats_counter: stats_counter,
      lfu_decay_time: lfu_decay_time,
      lfu_log_factor: lfu_log_factor,
      lfu_initial_ref: lfu_initial_ref,
      hot_cache_max_value_size: Keyword.get(opts, :hot_cache_max_value_size, 65_536),
      sync_flush_timeout_ms:
        Keyword.get(
          opts,
          :sync_flush_timeout_ms,
          Application.get_env(:ferricstore, :sync_flush_timeout_ms, 5_000)
        ),
      max_active_file_size: Keyword.get(opts, :max_active_file_size, 256 * 1024 * 1024),
      read_sample_rate: Keyword.get(opts, :read_sample_rate, 100),
      eviction_policy: Keyword.get(opts, :eviction_policy, :volatile_lfu),
      max_memory_bytes: max_memory_bytes,
      keydir_max_ram: keydir_max_ram,
      memory_limit: memory_limit,
      keydir_binary_bytes: keydir_binary_bytes,
      hotness_table: hotness_table,
      config_table: config_table,
      connected_clients_fn: Keyword.get(opts, :connected_clients_fn, fn -> 0 end),
      process_rss_fn: Keyword.get(opts, :process_rss_fn),
      server_info_fn: Keyword.get(opts, :server_info_fn, fn -> %{} end)
    }

    # Cache in persistent_term for ~0ns access via __instance__/0
    :persistent_term.put({FerricStore.Instance, name}, ctx)

    ctx
  end

  @doc """
  Retrieves the cached instance context for the given module name.
  """
  @spec get(atom()) :: t()
  def get(name) do
    :persistent_term.get({FerricStore.Instance, name})
  end

  @doc """
  Injects optional callbacks into an existing instance.

  Used by server apps (e.g., ferricstore_server) to provide server-specific
  functions without the library needing to know about the server.

  Accepted keys: `:connected_clients_fn`, `:process_rss_fn`, `:server_info_fn`.
  """
  @spec inject_callbacks(atom(), keyword()) :: t()
  def inject_callbacks(name, callbacks) do
    ctx = get(name)
    updated = struct!(ctx, callbacks)
    :persistent_term.put({FerricStore.Instance, name}, updated)
    updated
  end

  @doc """
  Removes the cached instance context.
  """
  @spec cleanup(atom()) :: :ok
  def cleanup(name) do
    cleanup_instance_tables(fetch_cached(name))
    :persistent_term.erase({FerricStore.Instance, name})
    :ok
  rescue
    ArgumentError -> :ok
  end

  # ---------------------------------------------------------------------------
  # Private: build helpers
  # ---------------------------------------------------------------------------

  defp fetch_cached(name) do
    {:ok, :persistent_term.get({FerricStore.Instance, name})}
  rescue
    ArgumentError -> :error
  end

  defp cleanup_instance_tables({:ok, %__MODULE__{name: :default}}), do: :ok
  defp cleanup_instance_tables(:error), do: :ok

  defp cleanup_instance_tables({:ok, %__MODULE__{} = ctx}) do
    Ferricstore.Store.ActiveFile.cleanup_instance(ctx)

    ctx.keydir_refs
    |> tuple_values()
    |> Enum.each(&delete_ets_table/1)

    ctx.latch_refs
    |> tuple_values()
    |> Enum.each(&delete_ets_table/1)

    delete_ets_table(ctx.hotness_table)
    delete_ets_table(ctx.config_table)
  end

  defp tuple_values(tuple) when is_tuple(tuple), do: Tuple.to_list(tuple)
  defp tuple_values(_other), do: []

  defp delete_ets_table(table) when is_atom(table) or is_reference(table) do
    case :ets.whereis(table) do
      :undefined -> :ok
      _tid -> :ets.delete(table)
    end
  rescue
    ArgumentError -> :ok
  end

  defp delete_ets_table(_table), do: :ok

  defp build_slot_map(shard_count) do
    Ferricstore.Store.SlotMap.build_uniform(shard_count)
  end

  defp build_keydir_tables(name, shard_count) do
    # For the :default instance, use the existing naming convention
    # that Shard.init creates (:"keydir_0", :"keydir_1", etc.)
    # For custom instances, use instance-scoped names.
    0..(shard_count - 1)
    |> Enum.map(fn i ->
      table_name =
        if name == :default, do: :"keydir_#{i}", else: :"#{name}_keydir_#{i}"

      # Don't create the table here — Shard.init creates it.
      # Just record the name so Router can find it.
      table_name
    end)
    |> List.to_tuple()
  end

  defp build_shard_names(name, shard_count) do
    0..(shard_count - 1)
    |> Enum.map(fn i ->
      if name == :default,
        do: :"Ferricstore.Store.Shard.#{i}",
        else: :"#{name}.Shard.#{i}"
    end)
    |> List.to_tuple()
  end

  # Per-shard latch tables used by inline RMW local-origin helpers.
  # Created here so they're ready before any RMW can be issued. :named_table
  # so other processes can look them up directly via the name; :public so
  # callers can :ets.insert_new without a GenServer hop.
  defp build_latch_tables(name, shard_count) do
    0..(shard_count - 1)
    |> Enum.map(fn i ->
      table_name =
        if name == :default,
          do: :"ferricstore_latch_#{i}",
          else: :"#{name}_latch_#{i}"

      # Idempotent: recreate only if it doesn't already exist (tests may
      # call Instance.new/1 multiple times).
      case :ets.whereis(table_name) do
        :undefined ->
          :ets.new(table_name, [
            :public,
            :set,
            :named_table,
            {:read_concurrency, true},
            {:write_concurrency, :auto}
          ])

        _ref ->
          :ets.delete_all_objects(table_name)
      end

      table_name
    end)
    |> List.to_tuple()
  end

  # Try to get an existing persistent_term ref, fall back to creating a new one.
  defp try_get_pt(key, fallback_fn) do
    try do
      :persistent_term.get(key)
    rescue
      ArgumentError -> fallback_fn.()
    end
  end

  defp detect_memory_limit do
    cgroup_v2_limit() || cgroup_v1_limit() || host_total_memory() || 1_073_741_824
  end

  defp cgroup_v2_limit do
    case File.read("/sys/fs/cgroup/memory.max") do
      {:ok, "max\n"} ->
        nil

      {:ok, data} ->
        case Integer.parse(String.trim(data)) do
          {bytes, _} when bytes > 0 -> bytes
          _ -> nil
        end

      _ ->
        nil
    end
  end

  defp cgroup_v1_limit do
    case File.read("/sys/fs/cgroup/memory/memory.limit_in_bytes") do
      {:ok, data} ->
        case Integer.parse(String.trim(data)) do
          {bytes, _} when bytes > 0 and bytes < 4_611_686_018_427_387_904 -> bytes
          _ -> nil
        end

      _ ->
        nil
    end
  end

  defp host_total_memory do
    try do
      data = apply(:memsup, :get_system_memory_data, [])

      case data do
        list when is_list(list) -> Keyword.get(list, :total_memory)
        _ -> nil
      end
    rescue
      _ -> nil
    catch
      _, _ -> nil
    end
  end
end
