defmodule Ferricstore.Application do
  @moduledoc """
  OTP Application for the FerricStore core engine.

  Starts the core supervision tree: shards, Raft, ETS tables, merge
  schedulers, PubSub, and MemoryGuard. Network-facing children (Ranch
  TCP/TLS listener, HTTP health endpoint) are started by the separate
  `:ferricstore_server` application.

  ## Supervision tree (`:one_for_one`)

  ```
  Ferricstore.Supervisor
  ├── Ferricstore.Stats                   (global counters & run metadata)
  ├── Ferricstore.SlowLog                 (slow command log)
  ├── Ferricstore.AuditLog                (audit trail)
  ├── Ferricstore.Config                  (runtime config)
  ├── Ferricstore.NamespaceConfig         (per-namespace overrides)
  ├── (ACL moved to ferricstore_server)
  ├── Ferricstore.HLC                     (Hybrid Logical Clock)
  ├── Ferricstore.Raft.WARaftBackend      (durable Raft backend)
  ├── Ferricstore.Store.BitcaskWriter (x N) (background Bitcask flushers)
  ├── Ferricstore.Store.ShardSupervisor   (one_for_one over N Shard GenServers)
  ├── Ferricstore.Merge.Supervisor        (Semaphore + N Scheduler GenServers)
  ├── Ferricstore.PubSub
  ├── Ferricstore.FetchOrCompute
  └── Ferricstore.MemoryGuard
  ```

  `Stats` starts first so counters are available before any connection arrives.
  The `ShardSupervisor` must start **before** the Ranch listener (in the server
  app) so that the key-value store is ready before any client connection arrives.

  ## Configuration (application env)

    * `:data_dir`         - Bitcask data directory (default: `"data"`)
    * `:shard_count`      - Number of shards (default: `System.schedulers_online()`)
    * `:max_memory_bytes` - Memory budget for eviction (default: `1 GiB`)
  """

  use Application

  require Logger

  @default_large_value_warning_bytes 512 * 1024
  @large_value_blob_ref_read_timeout_ms 1_000

  @impl true
  @spec start(Application.start_type(), term()) ::
          {:ok, pid()} | {:ok, pid(), term()} | {:error, term()}
  def start(_type, _args) do
    mark_starting()

    try do
      data_dir = Application.get_env(:ferricstore, :data_dir, "data")
      :ok = Ferricstore.Raft.Backend.put_running!(:waraft)

      shard_count =
        case Application.get_env(:ferricstore, :shard_count, 0) do
          0 -> System.schedulers_online()
          n when is_integer(n) and n > 0 -> n
        end

      app_state = %{data_dir: data_dir, shard_count: shard_count}

      Logger.info("FerricStore starting")

      # Create the on-disk directory layout (spec 2B.4) before any process
      # tries to open shard directories or WARaft segment logs.
      Ferricstore.DataDir.ensure_layout!(data_dir, shard_count)

      replication_mode = Ferricstore.ReplicationMode.resolve!(data_dir, shard_count)

      Ferricstore.ReplicationMode.put_current(replication_mode)

      case Ferricstore.ReplicationMode.read(data_dir) do
        {:error, :enoent} ->
          :ok = Ferricstore.ReplicationMode.mark_raft!(data_dir, shard_count, 0, %{})

        _ ->
          :ok
      end

      # Cache LFU config in persistent_term for hot-path reads (~5ns vs ~250ns).
      # Must run before any shard starts touching keys.
      Ferricstore.Store.LFU.init_config_cache()

      # Cache hot_cache_max_value_size in persistent_term for zero-overhead
      # hot-path reads. Values larger than this threshold are stored as nil
      # in ETS (cold) to avoid copying large binaries on every :ets.lookup.
      :persistent_term.put(
        :ferricstore_hot_cache_max_value_size,
        Application.get_env(:ferricstore, :hot_cache_max_value_size, 65_536)
      )

      # Initialize per-shard atomic write version counters (used by WATCH/EXEC
      # and the Shard-bypass quorum write path in Router).
      Ferricstore.Store.WriteVersion.init(shard_count)

      # Initialize per-shard disk pressure flags (reject writes on ENOSPC).
      Ferricstore.Store.DiskPressure.init(shard_count)

      # Initialize the active file registry (ETS + atomics generation counter).
      # Replaces persistent_term for active file metadata to avoid global GC
      # on file rotation — critical for embedded mode with many host processes.
      Ferricstore.Store.ActiveFile.init(shard_count)

      # Publish max_active_file_size once at startup. Shards read this via
      # persistent_term.get (~5ns) at init. Never written again at runtime.
      :persistent_term.put(
        :ferricstore_max_active_file_size,
        Application.get_env(:ferricstore, :max_active_file_size, 8 * 1024 * 1024 * 1024)
      )

      # Initialize MemoryGuard pressure flags as atomics (3 slots).
      # Slot 1: keydir_full (reject new key writes at :reject level)
      # Slot 2: reject_writes (reject ALL writes at :reject + :noeviction)
      # Slot 3: skip_promotion (don't re-cache cold reads at :pressure level)
      ref = :atomics.new(3, signed: false)
      :persistent_term.put(:ferricstore_pressure_flags, ref)

      # Initialize keyspace notification events config in persistent_term
      # (default: empty string = disabled). Updated by Config.apply_side_effect
      # when CONFIG SET notify-keyspace-events is called.
      :persistent_term.put(:ferricstore_keyspace_events, "")

      # Initialize the slot map BEFORE any shard starts. This builds
      # a uniform 1024-slot -> shard mapping and stores it in persistent_term.
      # Also sets :ferricstore_shard_count.
      Ferricstore.Store.SlotMap.init(shard_count)

      :persistent_term.put(
        :ferricstore_promotion_threshold,
        Application.get_env(:ferricstore, :promotion_threshold, 100)
      )

      :persistent_term.put(
        :ferricstore_read_sample_rate,
        Application.get_env(:ferricstore, :read_sample_rate, 100)
      )

      # Initialize waiter registry ETS for blocking commands
      Ferricstore.Waiters.init()
      Ferricstore.Flow.ClaimWaiters.init()
      # Client tracking ETS tables initialized by FerricstoreServer.ClientTracking
      # Initialize stream metadata ETS tables (owned by this long-lived process)
      Ferricstore.Commands.Stream.init_tables()
      # Start Erlang distribution if cluster is configured.
      # Must happen before WARaft starts so cluster peers can communicate
      # across nodes.
      maybe_start_distribution()

      # Build the default instance context. This creates the Instance struct
      # with all refs (atomics, counters, ETS tables) and caches it in
      # persistent_term as {:FerricStore.Instance, :default}.
      # All code that calls FerricStore.Instance.get(:default) will find it.
      # Note: we pass the EXISTING refs (pressure_flags, etc.) rather than
      # creating new ones, since the global init above already created them.
      default_ctx =
        FerricStore.Instance.build(:default,
          data_dir: data_dir,
          shard_count: shard_count,
          max_memory_bytes:
            Application.get_env(
              :ferricstore,
              :max_memory_bytes,
              Ferricstore.OperationalLimits.memory_limit_bytes()
            ),
          keydir_max_ram: Application.get_env(:ferricstore, :keydir_max_ram, 256 * 1024 * 1024),
          eviction_policy: Application.get_env(:ferricstore, :eviction_policy, :volatile_lfu),
          hot_cache_max_value_size:
            Application.get_env(:ferricstore, :hot_cache_max_value_size, 65_536),
          max_active_file_size:
            Application.get_env(:ferricstore, :max_active_file_size, 8 * 1024 * 1024 * 1024),
          read_sample_rate: Application.get_env(:ferricstore, :read_sample_rate, 100),
          lfu_decay_time: Application.get_env(:ferricstore, :lfu_decay_time, 1),
          lfu_log_factor: Application.get_env(:ferricstore, :lfu_log_factor, 10)
        )

      # Background Bitcask writers — one per shard. Must start BEFORE the
      # ShardSupervisor because StateMachine.apply sends casts to these
      # processes during shard init/recovery when replaying the Raft log.
      bitcask_writer_children =
        Enum.map(0..(shard_count - 1), fn i ->
          Supervisor.child_spec(
            {Ferricstore.Store.BitcaskWriter, shard_index: i},
            id: :"bitcask_writer_#{i}"
          )
        end)

      flow_lmdb_writer_children =
        Enum.map(0..(shard_count - 1), fn i ->
          Supervisor.child_spec(
            {Ferricstore.Flow.LMDBWriter,
             shard_index: i, data_dir: data_dir, instance_ctx: default_ctx},
            id: :"flow_lmdb_writer_#{i}"
          )
        end)

      # Optional libcluster node discovery (DNS, Kubernetes labels, or gossip).
      # When topologies are configured, Cluster.Supervisor is the first child so
      # that node discovery begins before the store is ready to serve traffic.
      # When no topologies are configured (nil or []), the supervisor is omitted.
      cluster_children = cluster_supervisor_children()

      # Core children: always started regardless of mode.
      children =
        [
          # Start first so supervisor shutdown stops it last. This gives the
          # default instance the same cleanup guarantee as custom instances:
          # no stale persistent_term context after the application is stopped.
          Supervisor.child_spec({FerricStore.Instance.Cleanup, :default},
            id: :default_instance_cleanup,
            restart: :temporary
          )
        ] ++
          cluster_children ++
          [
            Ferricstore.Stats,
            Ferricstore.SlowLog,
            Ferricstore.AuditLog,
            Ferricstore.Config,
            Ferricstore.NamespaceConfig,
            Ferricstore.Doctor,
            Ferricstore.HLC,
            Ferricstore.QuorumMetrics,
            Ferricstore.PrefixMetricsCache,
            Ferricstore.Waiters.Monitor,
            Ferricstore.Flow.HistoryProjector.TableOwner,
            Ferricstore.Store.BlobStore.TableOwner,
            Ferricstore.Raft.WARaftBackend.BatcherSupervisor,
            {Ferricstore.Store.KeydirTableOwner, instance_ctx: default_ctx}
          ] ++
          bitcask_writer_children ++
          [{Ferricstore.Flow.LMDBFlushCoordinator, []}] ++
          flow_lmdb_writer_children ++
          [
            {Ferricstore.Store.ShardSupervisor,
             data_dir: data_dir, shard_count: shard_count, instance_ctx: default_ctx}
          ] ++
          [
            {Ferricstore.Merge.Supervisor, data_dir: data_dir, shard_count: shard_count},
            Ferricstore.PubSub,
            Ferricstore.FetchOrCompute,
            {Ferricstore.OperationalGuard, instance_ctx: default_ctx},
            {Ferricstore.Flow.Scheduler, ctx: default_ctx},
            Ferricstore.Flow.RetentionSweeper,
            {Ferricstore.Store.BlobGCSweeper, instance_ctx: default_ctx},
            {Ferricstore.MemoryGuard, memory_guard_opts()},
            Ferricstore.Cluster.Manager
          ]

      {max_r, max_s} = Application.get_env(:ferricstore, :supervisor_max_restarts, {20, 10})

      opts = [
        strategy: :one_for_one,
        name: Ferricstore.Supervisor,
        max_restarts: max_r,
        max_seconds: max_s
      ]

      case Supervisor.start_link(children, opts) do
        {:ok, pid} ->
          case Ferricstore.Raft.WARaftBackend.start(default_ctx, waraft_backend_opts()) do
            :ok ->
              :ok = Ferricstore.Flow.LMDB.ensure_shard_dirs(data_dir, shard_count)
              mark_started(shard_count)
              {:ok, pid, app_state}

            {:error, reason} ->
              stop_started_supervisor(pid)
              cleanup_failed_start()
              {:error, {:raft_election_failed, reason}}
          end

        result ->
          cleanup_failed_start()
          result
      end
    after
      clear_starting()
    end
  end

  @doc false
  @spec starting?() :: boolean()
  def starting? do
    :persistent_term.get({__MODULE__, :starting}, false)
  end

  defp waraft_backend_opts do
    [
      log_module: Ferricstore.Raft.WARaftBackend.default_log_module(),
      commit_batch_interval_ms: Ferricstore.Raft.WARaftBackend.default_commit_batch_interval_ms(),
      commit_batch_max: Ferricstore.Raft.WARaftBackend.default_commit_batch_max()
    ]
  end

  defp mark_starting do
    :persistent_term.put({__MODULE__, :starting}, true)
  end

  defp clear_starting do
    :persistent_term.put({__MODULE__, :starting}, false)
  end

  defp mark_started(shard_count, ready? \\ true) do
    # Mark the node as ready for Kubernetes readiness probes (spec 2C.1).
    # In embedded mode, set_ready(true) is still called so that
    # Health.ready?() returns true for any code that checks it.
    Ferricstore.Health.set_ready(ready?)

    :telemetry.execute(
      [:ferricstore, :node, :startup_complete],
      %{duration_ms: System.monotonic_time(:millisecond)},
      %{shard_count: shard_count}
    )

    # Step 6 - Large value check:
    # Scan keydir for values exceeding the configured threshold.
    # Pure RAM scan -- keydir already holds value_size per entry, no disk reads.
    # Non-blocking: fires before any traffic is served so operator sees the
    # warning immediately.
    check_large_values(shard_count)
  end

  defp stop_started_supervisor(pid) do
    Supervisor.stop(pid)
  catch
    :exit, _reason -> :ok
  end

  @impl true
  def prep_stop(state) do
    t0 = System.monotonic_time(:millisecond)

    # Step 1: Mark not ready — Kubernetes stops routing traffic
    Ferricstore.Health.set_ready(false)
    Logger.info("Shutdown: marked not ready")

    :telemetry.execute(
      [:ferricstore, :node, :shutdown_started],
      %{uptime_ms: t0},
      %{}
    )

    {shard_count, data_dir} = runtime_shutdown_config(state)

    waraft_result = shutdown_stop_waraft_backend()
    bitcask_writer_result = shutdown_flush_bitcask_writers(shard_count)
    flow_lmdb_result = shutdown_flush_flow_lmdb_writers(shard_count)
    bitcask_fsync_result = shutdown_fsync_bitcask(shard_count, data_dir)
    shutdown_flush_shards(shard_count)
    wal_rollover_result = :ok

    elapsed = System.monotonic_time(:millisecond) - t0

    case {waraft_result, bitcask_writer_result, flow_lmdb_result, bitcask_fsync_result,
          wal_rollover_result} do
      {:ok, :ok, :ok, :ok, :ok} ->
        Logger.info("Shutdown: graceful flush complete in #{elapsed}ms")

      {waraft_result, writer_result, flow_lmdb_result, bitcask_result, wal_result} ->
        Logger.warning(
          "Shutdown: graceful flush complete with warnings in #{elapsed}ms " <>
            "(waraft=#{inspect(waraft_result)}, bitcask_writer=#{inspect(writer_result)}, flow_lmdb=#{inspect(flow_lmdb_result)}, bitcask_fsync=#{inspect(bitcask_result)}, wal_rollover=#{inspect(wal_result)})"
        )
    end

    state
  end

  @impl true
  def stop(state) do
    _ = state
    _ = Ferricstore.Raft.WARaftBackend.stop()

    FerricStore.Instance.cleanup(:default)
    Ferricstore.Raft.Backend.clear_running()
    :ok
  end

  defp cleanup_failed_start do
    Ferricstore.Health.set_ready(false)
    _ = Ferricstore.Raft.WARaftBackend.stop()
    FerricStore.Instance.cleanup(:default)
    Ferricstore.Raft.Backend.clear_running()
    :ok
  end

  defp runtime_shutdown_config(%{shard_count: shard_count, data_dir: data_dir})
       when is_integer(shard_count) and is_binary(data_dir) do
    {shard_count, data_dir}
  end

  defp runtime_shutdown_config(_state), do: runtime_shutdown_config()

  defp runtime_shutdown_config do
    try do
      ctx = FerricStore.Instance.get(:default)
      {ctx.shard_count, ctx.data_dir}
    rescue
      ArgumentError ->
        shard_count = :persistent_term.get(:ferricstore_shard_count, configured_shard_count())
        data_dir = Application.get_env(:ferricstore, :data_dir, "data")
        {shard_count, data_dir}
    end
  end

  defp configured_shard_count do
    case Application.get_env(:ferricstore, :shard_count, 0) do
      0 -> System.schedulers_online()
      n when is_integer(n) and n > 0 -> n
      _ -> 4
    end
  end

  defp shutdown_stop_waraft_backend do
    # Stop WARaft while shard keydir ETS tables are still alive. WAraft storage
    # can otherwise deliver late apply messages during supervisor teardown and
    # crash when Shard termination has already removed a keydir table.
    result =
      try do
        Ferricstore.Raft.WARaftBackend.stop()
      catch
        :exit, reason -> {:error, {:waraft_stop_exit, reason}}
        kind, reason -> {:error, {:waraft_stop_failed, kind, reason}}
      end

    case result do
      :ok -> Logger.info("Shutdown: WARaft backend stopped")
      {:error, reason} -> Logger.warning("Shutdown: WARaft stop incomplete: #{inspect(reason)}")
    end

    result
  end

  defp shutdown_flush_bitcask_writers(shard_count) do
    # Step 3: Flush all BitcaskWriters — drain deferred disk writes
    result =
      try do
        Ferricstore.Store.BitcaskWriter.flush_all(shard_count)
      catch
        :exit, reason -> {:error, {:flush_all_exit, reason}}
      end

    case result do
      :ok ->
        Logger.info("Shutdown: BitcaskWriters flushed")

      {:error, reason} ->
        Logger.warning("Shutdown: BitcaskWriter flush incomplete: #{inspect(reason)}")
    end

    result
  end

  defp shutdown_flush_flow_lmdb_writers(shard_count) do
    _ = Ferricstore.Flow.LMDBWriter.suspend_all(shard_count, flush: false)
    Logger.info("Shutdown: Flow LMDB lagged projection flush skipped")
    :ok
  catch
    :exit, reason ->
      _ = Ferricstore.Flow.LMDBWriter.suspend_all(shard_count, flush: false)
      Logger.warning("Shutdown: Flow LMDB writer flush failed: #{inspect(reason)}")
      {:error, reason}
  end

  defp shutdown_fsync_bitcask(shard_count, data_dir) do
    # Step 3b: Fsync all active Bitcask log files.
    # BitcaskWriter uses v2_append_batch_nosync (data in OS page cache only).
    # Without fsync, a subsequent Process.exit(:kill) can lose unsynced data
    # on Linux (Docker overlayfs). macOS APFS retains page cache across kills.

    result = fsync_bitcask_for_shutdown(shard_count, data_dir)

    case result do
      :ok ->
        Logger.info("Shutdown: Bitcask files fsynced")

      {:error, failures} ->
        Logger.warning("Shutdown: Bitcask fsync incomplete: #{inspect(failures)}")
    end

    result
  end

  defp shutdown_active_file_path(shard_index) do
    case Ferricstore.Store.ActiveFile.get(shard_index) do
      {_file_id, active_file_path, _shard_data_path} -> active_file_path
    end
  rescue
    _ -> nil
  catch
    _, _ -> nil
  end

  @doc false
  def fsync_bitcask_for_shutdown(shard_count, data_dir, opts \\ []) do
    active_file_path = Keyword.get(opts, :active_file_path, &shutdown_active_file_path/1)
    exists? = Keyword.get(opts, :exists?, &Ferricstore.FS.exists?/1)
    fsync = Keyword.get(opts, :fsync, &Ferricstore.Bitcask.NIF.v2_fsync/1)
    list_log_files = Keyword.get(opts, :list_log_files, &shutdown_list_log_files/1)

    failures =
      0..(shard_count - 1)
      |> Enum.reduce([], fn shard_index, acc ->
        case fsync_bitcask_shard_for_shutdown(
               shard_index,
               data_dir,
               active_file_path,
               exists?,
               fsync,
               list_log_files
             ) do
          :ok -> acc
          {:error, reason} -> [{shard_index, reason} | acc]
        end
      end)
      |> Enum.reverse()

    case failures do
      [] -> :ok
      failures -> {:error, failures}
    end
  end

  defp fsync_bitcask_shard_for_shutdown(
         shard_index,
         data_dir,
         active_file_path,
         exists?,
         fsync,
         list_log_files
       ) do
    shard_path = Ferricstore.DataDir.shard_data_path(data_dir, shard_index)
    active_path = active_file_path.(shard_index)

    cond do
      active_path && exists?.(active_path) ->
        case fsync.(active_path) do
          :ok -> :ok
          {:ok, _} -> :ok
          {:error, reason} -> {:error, {:active_file_fsync_failed, active_path, reason}}
          other -> {:error, {:active_file_fsync_failed, active_path, other}}
        end

      true ->
        fsync_log_files_for_shutdown(shard_path, fsync, list_log_files)
    end
  catch
    kind, reason -> {:error, {:fsync_crashed, kind, reason}}
  end

  defp fsync_log_files_for_shutdown(shard_path, fsync, list_log_files) do
    case list_log_files.(shard_path) do
      {:ok, files} ->
        files
        |> Enum.filter(&String.ends_with?(&1, ".log"))
        |> Enum.reduce(:ok, fn filename, acc ->
          path = Path.join(shard_path, filename)

          case {acc, fsync.(path)} do
            {:ok, :ok} -> :ok
            {:ok, {:ok, _}} -> :ok
            {:ok, {:error, reason}} -> {:error, {:log_file_fsync_failed, path, reason}}
            {:ok, other} -> {:error, {:log_file_fsync_failed, path, other}}
            {{:error, _} = error, _} -> error
          end
        end)

      {:error, reason} ->
        {:error, {:list_log_files_failed, shard_path, reason}}
    end
  end

  defp shutdown_list_log_files(shard_path), do: Ferricstore.FS.ls(shard_path)

  defp shutdown_flush_shards(shard_count) do
    # Step 4: Flush all shards — hint files + fsync
    # (terminate/1 on each shard will also do this, but doing it here
    # while the system is still healthy is more reliable)
    for i <- 0..(shard_count - 1) do
      name = :"Ferricstore.Store.Shard.#{i}"

      try do
        GenServer.call(name, :flush, 5_000)
      catch
        :exit, _ -> :ok
      end
    end

    Logger.info("Shutdown: shards flushed")
  end

  # ---------------------------------------------------------------------------
  # Large value check (Step 6)
  # ---------------------------------------------------------------------------

  @doc """
  Scans all shard ETS tables for values exceeding the configured threshold.

  Returns `{count, largest_key, largest_size}` where `count` is the number of
  entries whose value exceeds `threshold_bytes`, `largest_key` is the key with
  the largest value, and `largest_size` is its size in bytes.

  Returns `{0, nil, 0}` when no large values are found.

  This is a mostly RAM scan. Normal cold values use the size stored in ETS.
  Blob side-channel values store only a fixed-size ref in Bitcask, so those
  entries read the small ref record and decode its logical payload size.

  ## Parameters

    * `shard_count` -- number of shards to scan
    * `threshold_bytes` -- values larger than this are flagged (default:
      `Application.get_env(:ferricstore, :embedded_large_value_warning_bytes, 512 * 1024)`)

  """
  @spec scan_large_values(non_neg_integer(), non_neg_integer()) ::
          {non_neg_integer(), binary() | nil, non_neg_integer()}
  def scan_large_values(shard_count, threshold_bytes \\ nil) do
    threshold =
      threshold_bytes ||
        Application.get_env(
          :ferricstore,
          :embedded_large_value_warning_bytes,
          @default_large_value_warning_bytes
        )

    ctx = default_instance_ctx()

    Enum.reduce(0..(shard_count - 1), {0, nil, 0}, fn i, {count, largest_key, largest_size} ->
      keydir = :"keydir_#{i}"

      try do
        :ets.foldl(
          fn
            {key, value, _expire_at_ms, _lfu, _fid, _off, _vsize}, {c, lk, ls}
            when is_binary(value) ->
              size = byte_size(value)

              if size > threshold do
                if size > ls do
                  {c + 1, key, size}
                else
                  {c + 1, lk, ls}
                end
              else
                {c, lk, ls}
              end

            {key, nil, _exp, _lfu, fid, off, vsize}, {c, lk, ls}
            when is_integer(vsize) and vsize > 0 ->
              # Cold key (value evicted from RAM) -- use vsize from disk location.
              # Blob values store a 48-byte ref in Bitcask, but the ref carries
              # the original logical payload size used by operators.
              logical_size = large_value_logical_size(ctx, i, key, fid, off, vsize)

              if logical_size > threshold do
                if logical_size > ls do
                  {c + 1, key, logical_size}
                else
                  {c + 1, lk, ls}
                end
              else
                {c, lk, ls}
              end

            _entry, acc ->
              acc
          end,
          {count, largest_key, largest_size},
          keydir
        )
      rescue
        ArgumentError ->
          # ETS table does not exist (shard may be restarting).
          {count, largest_key, largest_size}
      end
    end)
  end

  defp default_instance_ctx do
    try do
      FerricStore.Instance.get(:default)
    rescue
      ArgumentError -> nil
    end
  end

  defp large_value_logical_size(ctx, shard_index, key, fid, off, vsize) do
    if blob_ref_sized_location?(ctx, fid, off, vsize) do
      path =
        ctx.data_dir
        |> Ferricstore.DataDir.shard_data_path(shard_index)
        |> Ferricstore.Store.Shard.ETS.file_path(fid)

      case Ferricstore.Store.ColdRead.pread_keyed(
             path,
             off,
             key,
             @large_value_blob_ref_read_timeout_ms
           ) do
        {:ok, value} ->
          case Ferricstore.Store.BlobRef.decode(value) do
            {:ok, %Ferricstore.Store.BlobRef{size: logical_size}} -> logical_size
            :error -> vsize
          end

        {:error, reason} ->
          Ferricstore.Store.ColdRead.emit_pread_error(path, reason)
          vsize
      end
    else
      vsize
    end
  end

  defp blob_ref_sized_location?(ctx, fid, off, vsize) do
    is_map(ctx) and is_binary(Map.get(ctx, :data_dir)) and
      Ferricstore.Store.BlobValue.threshold(ctx) > 0 and
      is_integer(fid) and fid >= 0 and is_integer(off) and off >= 0 and
      Ferricstore.Store.BlobRef.encoded_size?(vsize)
  end

  # Runs the large value check and emits a warning + telemetry if any are found.
  defp check_large_values(shard_count) do
    case scan_large_values(shard_count) do
      {0, _key, _size} ->
        :ok

      {count, largest_key, largest_size} ->
        Logger.warning(
          "Embedded large value check: #{count} value(s) exceed threshold; " <>
            "largest key=#{inspect(largest_key)} (#{largest_size} bytes)"
        )

        :telemetry.execute(
          [:ferricstore, :embedded, :large_values_detected],
          %{count: count, largest_size: largest_size},
          %{largest_key: largest_key}
        )
    end
  end

  # ---------------------------------------------------------------------------
  # Cluster supervisor (libcluster)
  # ---------------------------------------------------------------------------

  # Returns a list containing the Cluster.Supervisor child spec when libcluster
  # topologies are configured, or an empty list when they are not. This makes
  # libcluster entirely optional -- the application starts cleanly without it.
  @spec cluster_supervisor_children() :: [Supervisor.child_spec()]
  defp cluster_supervisor_children do
    case Application.get_env(:libcluster, :topologies) do
      nil ->
        []

      [] ->
        []

      :disabled ->
        []

      topologies when is_list(topologies) ->
        [{Cluster.Supervisor, [topologies, [name: Ferricstore.ClusterSupervisor]]}]
    end
  end

  # ---------------------------------------------------------------------------
  # Erlang distribution (cluster mode)
  # ---------------------------------------------------------------------------

  defp maybe_start_distribution do
    case Application.get_env(:ferricstore, :node_name) do
      nil ->
        :ok

      name ->
        unless Node.alive?() do
          {:ok, _} = Node.start(name)
          cookie = Application.get_env(:ferricstore, :cookie, :ferricstore)
          Node.set_cookie(cookie)
          Logger.info("Started Erlang distribution: #{name}, cookie set")
        end
    end
  end

  # ---------------------------------------------------------------------------
  # MemoryGuard options
  # ---------------------------------------------------------------------------

  defp memory_guard_opts do
    opts = []

    opts =
      case Application.get_env(:ferricstore, :max_memory_bytes) do
        nil -> opts
        val -> Keyword.put(opts, :max_memory_bytes, val)
      end

    opts =
      case Application.get_env(:ferricstore, :eviction_policy) do
        nil -> opts
        val -> Keyword.put(opts, :eviction_policy, val)
      end

    case Application.get_env(:ferricstore, :memory_guard_interval_ms) do
      nil -> opts
      val -> Keyword.put(opts, :interval_ms, val)
    end
  end
end
