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
  ├── Ferricstore.Raft.Batcher (x N)     (group-commit batchers)
  ├── Ferricstore.Store.BitcaskWriter (x N) (background Bitcask flushers)
  ├── Ferricstore.Store.RmwCoordinator (x N) (async RMW contention fallback)
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

  @impl true
  @spec start(Application.start_type(), term()) :: {:ok, pid()} | {:error, term()}
  def start(_type, _args) do
    data_dir = Application.get_env(:ferricstore, :data_dir, "data")

    shard_count =
      case Application.get_env(:ferricstore, :shard_count, 0) do
        0 -> System.schedulers_online()
        n when is_integer(n) and n > 0 -> n
      end

    Logger.info("FerricStore starting")

    # Create the on-disk directory layout (spec 2B.4) before any process
    # tries to open shard directories or Raft WALs.
    Ferricstore.DataDir.ensure_layout!(data_dir, shard_count)

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

    # Initialize per-shard disk pressure flags (reject async writes on ENOSPC).
    Ferricstore.Store.DiskPressure.init(shard_count)

    # Initialize the active file registry (ETS + atomics generation counter).
    # Replaces persistent_term for active file metadata to avoid global GC
    # on file rotation — critical for embedded mode with many host processes.
    Ferricstore.Store.ActiveFile.init(shard_count)

    # Publish max_active_file_size once at startup. Shards read this via
    # persistent_term.get (~5ns) at init. Never written again at runtime.
    :persistent_term.put(
      :ferricstore_max_active_file_size,
      Application.get_env(:ferricstore, :max_active_file_size, 256 * 1024 * 1024)
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
    # Client tracking ETS tables initialized by FerricstoreServer.ClientTracking
    # Initialize stream metadata ETS tables (owned by this long-lived process)
    Ferricstore.Commands.Stream.init_tables()
    # Load the patched ra_log_wal with async fdatasync BEFORE starting
    # the ra system, so the patched module is in place when the WAL starts.
    # NOTE: When using the local ra fork (path dep), the fork already includes
    # the async fdatasync changes directly in source. Skip hot-load to avoid
    # overriding the fork's beam with a potentially stale patched version.
    install_patched_wal()

    # Ra formats one snapshot debug event with `~b` even though the size can be
    # `undefined`; use our delegate so long debug/chaos runs do not spam
    # formatter crashes while keeping the rest of Ra logging intact.
    :ok = Ferricstore.Raft.SafeRaLogger.install_filter()
    :ok = :ra_env.configure_logger(Ferricstore.Raft.SafeRaLogger)

    # Start Erlang distribution if cluster is configured.
    # Must happen before ra system start so ra can communicate across nodes.
    maybe_start_distribution()

    # Start the ra system before shards so that Shard.init can start ra servers.
    :ok = Ferricstore.Raft.Cluster.start_system(data_dir)

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
        max_memory_bytes: Application.get_env(:ferricstore, :max_memory_bytes, 1_073_741_824),
        keydir_max_ram: Application.get_env(:ferricstore, :keydir_max_ram, 256 * 1024 * 1024),
        eviction_policy: Application.get_env(:ferricstore, :eviction_policy, :volatile_lfu),
        hot_cache_max_value_size:
          Application.get_env(:ferricstore, :hot_cache_max_value_size, 65_536),
        max_active_file_size:
          Application.get_env(:ferricstore, :max_active_file_size, 256 * 1024 * 1024),
        read_sample_rate: Application.get_env(:ferricstore, :read_sample_rate, 100),
        lfu_decay_time: Application.get_env(:ferricstore, :lfu_decay_time, 1),
        lfu_log_factor: Application.get_env(:ferricstore, :lfu_log_factor, 10)
      )

    batcher_children =
      Enum.map(0..(shard_count - 1), fn i ->
        shard_id = Ferricstore.Raft.Cluster.shard_server_id(i)

        Supervisor.child_spec(
          {Ferricstore.Raft.Batcher, shard_index: i, shard_id: shard_id},
          id: :"batcher_#{i}"
        )
      end)

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

    # Async RMW fallback coordinator — one per shard. Handles contended RMW
    # commands that lost the per-key latch in Router.async_rmw. See
    # docs/async-rmw-design.md.
    rmw_coordinator_children =
      Enum.map(0..(shard_count - 1), fn i ->
        Supervisor.child_spec(
          {Ferricstore.Store.RmwCoordinator, shard_index: i},
          id: :"rmw_coordinator_#{i}"
        )
      end)

    # Optional libcluster node discovery (DNS, Kubernetes labels, or gossip).
    # When topologies are configured, Cluster.Supervisor is the first child so
    # that node discovery begins before the store is ready to serve traffic.
    # When no topologies are configured (nil or []), the supervisor is omitted.
    cluster_children = cluster_supervisor_children()

    # Core children: always started regardless of mode.
    children =
      cluster_children ++
        [
          Ferricstore.Stats,
          Ferricstore.SlowLog,
          Ferricstore.AuditLog,
          Ferricstore.Config,
          Ferricstore.NamespaceConfig,
          Ferricstore.HLC,
          Ferricstore.QuorumMetrics,
          Ferricstore.PrefixMetricsCache
        ] ++
        batcher_children ++
        bitcask_writer_children ++
        rmw_coordinator_children ++
        [
          {Ferricstore.Store.ShardSupervisor,
           data_dir: data_dir, shard_count: shard_count, instance_ctx: default_ctx}
        ] ++
        [
          {Ferricstore.Merge.Supervisor, data_dir: data_dir, shard_count: shard_count},
          Ferricstore.PubSub,
          Ferricstore.FetchOrCompute,
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
      {:ok, pid} = result ->
        case Ferricstore.Raft.Cluster.trigger_shard_elections_parallel(shard_count) do
          :ok ->
            mark_started(shard_count)
            result

          {:error, reason} ->
            stop_started_supervisor(pid)
            cleanup_failed_start()
            {:error, {:raft_election_failed, reason}}
        end

      result ->
        cleanup_failed_start()
        result
    end
  end

  defp mark_started(shard_count) do
    # Mark the node as ready for Kubernetes readiness probes (spec 2C.1).
    # In embedded mode, set_ready(true) is still called so that
    # Health.ready?() returns true for any code that checks it.
    Ferricstore.Health.set_ready(true)

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

    {shard_count, data_dir} = runtime_shutdown_config()

    shutdown_flush_batchers(shard_count)
    shutdown_flush_bitcask_writers(shard_count)
    shutdown_fsync_bitcask(shard_count, data_dir)
    shutdown_flush_shards(shard_count)
    wal_rollover_result = shutdown_wal_rollover(data_dir)
    shutdown_check_snapshots(shard_count)

    elapsed = System.monotonic_time(:millisecond) - t0

    case wal_rollover_result do
      :ok ->
        Logger.info("Shutdown: graceful flush complete in #{elapsed}ms")

      {:error, reason} ->
        Logger.warning(
          "Shutdown: graceful flush complete with warnings in #{elapsed}ms " <>
            "(wal_rollover=#{inspect(reason)})"
        )
    end

    state
  end

  @impl true
  def stop(_state) do
    _ = Ferricstore.Raft.Cluster.stop_system()
    FerricStore.Instance.cleanup(:default)
    :ok
  end

  defp cleanup_failed_start do
    Ferricstore.Health.set_ready(false)
    _ = Ferricstore.Raft.Cluster.stop_system()
    FerricStore.Instance.cleanup(:default)
    :ok
  end

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

  defp shutdown_flush_batchers(shard_count) do
    # Step 2: Flush all Raft batchers — drain pending commands to Raft
    for i <- 0..(shard_count - 1) do
      try do
        Ferricstore.Raft.Batcher.flush(i)
      catch
        :exit, _ -> :ok
      end
    end

    Logger.info("Shutdown: batchers flushed")
  end

  defp shutdown_flush_bitcask_writers(shard_count) do
    # Step 3: Flush all BitcaskWriters — drain deferred disk writes
    try do
      Ferricstore.Store.BitcaskWriter.flush_all(shard_count)
    catch
      :exit, _ -> :ok
    end

    Logger.info("Shutdown: BitcaskWriters flushed")
  end

  defp shutdown_fsync_bitcask(shard_count, data_dir) do
    # Step 3b: Fsync all active Bitcask log files.
    # BitcaskWriter uses v2_append_batch_nosync (data in OS page cache only).
    # Without fsync, a subsequent Process.exit(:kill) can lose unsynced data
    # on Linux (Docker overlayfs). macOS APFS retains page cache across kills.

    for i <- 0..(shard_count - 1) do
      try do
        shard_path = Ferricstore.DataDir.shard_data_path(data_dir, i)
        active_file_path = shutdown_active_file_path(i)

        if active_file_path && Ferricstore.FS.exists?(active_file_path) do
          Ferricstore.Bitcask.NIF.v2_fsync(active_file_path)
        else
          # Fallback: fsync all log files in the shard directory
          case Ferricstore.FS.ls(shard_path) do
            {:ok, files} ->
              files
              |> Enum.filter(&String.ends_with?(&1, ".log"))
              |> Enum.each(fn f ->
                Ferricstore.Bitcask.NIF.v2_fsync(Path.join(shard_path, f))
              end)

            _ ->
              :ok
          end
        end
      catch
        _, _ -> :ok
      end
    end

    Logger.info("Shutdown: Bitcask files fsynced")
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

  defp shutdown_wal_rollover(data_dir) do
    # Step 5: Force WAL rollover and poll until segment writer finishes.
    # After force_roll_over, the old WAL file is handed to the segment writer.
    # When the segment writer finishes processing it, the old WAL file is deleted.
    # We poll for the old file's deletion — concrete, no side effects.
    case wal_rollover_for_shutdown(data_dir) do
      :ok ->
        Logger.info("Shutdown: WAL rolled over")
        :ok

      {:error, reason} ->
        Logger.warning("Shutdown: WAL rollover incomplete: #{inspect(reason)}")
        {:error, reason}
    end
  end

  @doc false
  def wal_rollover_for_shutdown(data_dir, opts \\ []) do
    force_rollover =
      Keyword.get(opts, :force_rollover, fn wal_name ->
        :ra_log_wal.force_roll_over(wal_name)
      end)

    list_wal_files = Keyword.get(opts, :list_wal_files, &list_wal_files/1)
    max_attempts = Keyword.get(opts, :max_attempts, 100)
    poll_interval_ms = Keyword.get(opts, :poll_interval_ms, 50)

    try do
      ra_dir = Path.join(data_dir, "ra")
      wal_name = :ra_system.derive_names(Ferricstore.Raft.Cluster.system_name()).wal

      # Snapshot WAL files before rollover
      wal_files_before = list_wal_files.(ra_dir)

      case force_rollover.(wal_name) do
        :ok ->
          # Poll until the pre-rollover WAL files are deleted by segment writer.
          await_wal_files_consumed(
            ra_dir,
            wal_files_before,
            max_attempts,
            poll_interval_ms,
            list_wal_files
          )

        {:ok, _} ->
          await_wal_files_consumed(
            ra_dir,
            wal_files_before,
            max_attempts,
            poll_interval_ms,
            list_wal_files
          )

        {:error, reason} ->
          {:error, {:force_rollover_failed, reason}}

        other ->
          {:error, {:force_rollover_failed, other}}
      end
    catch
      kind, reason -> {:error, {kind, reason}}
    end
  end

  defp shutdown_check_snapshots(shard_count) do
    # Step 6: Check snapshot state for each shard and log warning if
    # there are many entries since last snapshot (will need replay on restart)
    for i <- 0..(shard_count - 1) do
      try do
        server_id = Ferricstore.Raft.Cluster.shard_server_id(i)

        case :ra.member_overview(server_id) do
          {:ok, overview} ->
            check_snapshot_gap(i, overview)

          {:ok, overview, _leader} ->
            check_snapshot_gap(i, overview)

          _ ->
            :ok
        end
      catch
        _, _ -> :ok
      end
    end
  end

  defp list_wal_files(ra_dir) do
    case Ferricstore.FS.ls(ra_dir) do
      {:ok, files} -> Enum.filter(files, &String.ends_with?(&1, ".wal"))
      _ -> []
    end
  end

  defp await_wal_files_consumed(ra_dir, old_files, max, interval, list_fun) do
    do_await_wal_files_consumed(ra_dir, old_files, max, interval, list_fun)
  end

  defp do_await_wal_files_consumed(_ra_dir, [], _max, _interval, _list_fun), do: :ok

  defp do_await_wal_files_consumed(_ra_dir, old_files, 0, _interval, _list_fun) do
    Logger.warning("Shutdown: segment writer still processing WAL files after timeout")
    {:error, {:wal_files_unconsumed, old_files}}
  end

  defp do_await_wal_files_consumed(ra_dir, old_files, attempts, interval, list_fun) do
    current = list_fun.(ra_dir)
    remaining = Enum.filter(old_files, fn f -> f in current end)

    if remaining == [] do
      :ok
    else
      Process.sleep(interval)
      do_await_wal_files_consumed(ra_dir, remaining, attempts - 1, interval, list_fun)
    end
  end

  defp check_snapshot_gap(shard_index, overview) do
    last_applied = Map.get(overview, :last_applied, 0)
    snapshot_index = Map.get(overview, :snapshot_index, 0)
    # -1 means no snapshot ever taken
    snapshot_index = if snapshot_index == -1, do: 0, else: snapshot_index
    gap = last_applied - snapshot_index

    if gap > 5_000 do
      Logger.warning(
        "Shutdown: shard #{shard_index} has #{gap} entries since last snapshot " <>
          "(last_applied=#{last_applied}, snapshot_index=#{snapshot_index}). " <>
          "Next restart will replay these entries. Consider reducing release_cursor_interval."
      )
    else
      Logger.info("Shutdown: shard #{shard_index} snapshot gap=#{gap} (ok)")
    end
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

  This is a pure RAM scan -- ETS already holds the full value per entry, so no
  disk reads are needed.

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

            {key, nil, _exp, _lfu, _fid, _off, vsize}, {c, lk, ls}
            when is_integer(vsize) and vsize > 0 ->
              # Cold key (value evicted from RAM) -- use vsize from disk location
              if vsize > threshold do
                if vsize > ls do
                  {c + 1, key, vsize}
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

  # ---------------------------------------------------------------------------
  # Patched ra_log_wal (async fdatasync)
  # ---------------------------------------------------------------------------

  # Compiles and hot-loads a patched version of ra_log_wal that decouples
  # fdatasync from the batch processing loop. The patched module:
  #
  # 1. Writes data to the kernel buffer synchronously (fast)
  # 2. Spawns a linked process to run fdatasync asynchronously
  # 3. While fdatasync runs, keeps accepting new entries
  # 4. When fdatasync completes, notifies ALL accumulated writers
  #
  # Writers are ONLY notified AFTER fdatasync, preserving Raft durability.
  #
  # This must be called BEFORE ra_system:start/1 so the patched module is
  # loaded before the WAL process starts.
  # Check if ra is a local path dependency (fork) vs hex package.

  @spec install_patched_wal() :: :ok | :error
  defp install_patched_wal do
    priv_dir = :code.priv_dir(:ferricstore)
    beam_path = Path.join(priv_dir, "patched/ra_log_wal.beam")

    if Ferricstore.FS.exists?(beam_path) do
      binary = File.read!(beam_path)
      :code.purge(:ra_log_wal)
      {:module, :ra_log_wal} = :code.load_binary(:ra_log_wal, ~c"ra_log_wal.erl", binary)
      Logger.info("Loaded patched ra_log_wal with async fdatasync")
      :ok
    else
      Logger.error(
        "Patched ra_log_wal.beam not found at #{beam_path}. " <>
          "Run `mix compile` to generate it from priv/patched/ra_log_wal.erl"
      )

      :error
    end
  end
end
