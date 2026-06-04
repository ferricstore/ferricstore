defmodule Ferricstore.Test.ShardHelpers do
  @moduledoc """
  Shared helpers for tests that interact with application-supervised shards.

  Use this module in any test that kills or restarts shards to ensure the
  supervisor tree is fully healthy before and after the test.

  Also provides dynamic key discovery helpers for tests that need keys on
  specific or different shards, without hardcoding key-to-shard mappings.
  """

  alias Ferricstore.Store.Router

  @doc """
  Synchronously flushes all pending async writes on all application-supervised
  shards to disk.

  Call this before killing a shard in tests that verify crash durability, to
  ensure rapid consecutive puts (which may still be in state.pending due to
  the async io_uring batch window) are committed to the Bitcask log before the
  crash is simulated.
  """
  @spec flush_all_shards() :: :ok
  def flush_all_shards do
    shard_count = shard_count()

    Enum.each(0..(shard_count - 1), fn i ->
      name = :"Ferricstore.Store.Shard.#{i}"

      case Process.whereis(name) do
        pid when is_pid(pid) -> GenServer.call(pid, :flush, 30_000)
        nil -> :ok
      end
    end)

    # Also flush background BitcaskWriter processes so deferred writes
    # from StateMachine.apply are on disk before tests verify disk state.
    Ferricstore.Store.BitcaskWriter.flush_all(shard_count)

    # Fsync all active Bitcask log files. BitcaskWriter uses nosync writes
    # (data in OS page cache only). Without explicit fsync, data can be lost
    # if a shard is killed before the OS flushes to disk (especially on Linux).
    data_dir = Application.get_env(:ferricstore, :data_dir, "data")

    for i <- 0..(shard_count - 1) do
      try do
        active_path =
          case Ferricstore.Store.ActiveFile.get(i) do
            {_file_id, path, _shard_data_path} -> path
            _ -> nil
          end

        if active_path && File.exists?(active_path) do
          Ferricstore.Bitcask.NIF.v2_fsync(active_path)
        else
          shard_path = Ferricstore.DataDir.shard_data_path(data_dir, i)

          case File.ls(shard_path) do
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
  end

  @doc """
  Deletes all keys across every shard. Equivalent to FLUSHDB.

  Call this in `setup` callbacks to prevent key accumulation across tests —
  a growing keydir makes KEYS/DBSIZE calls progressively slower and can cause
  GenServer timeouts when a test run accumulates thousands of keys.
  """
  @spec flush_all_keys() :: :ok
  def flush_all_keys do
    alias Ferricstore.Store.Router

    reset_server_auth_state()
    reset_memory_guard_pressure()

    shard_count = shard_count()
    flush_timeout = 30_000
    ready_timeout = flush_timeout

    # Flush background BitcaskWriter so deferred writes are on disk
    # before we snapshot keys for deletion.
    Ferricstore.Store.BitcaskWriter.flush_all(shard_count)

    # Delete every key on each shard directly via that shard's Raft batcher.
    # We must NOT use Router.delete/1 because it re-hashes the key, which
    # routes compound keys (H:, S:, Z:, T: prefixed) to the wrong shard —
    # compound keys live on their parent's shard, not the shard determined
    # by hashing the compound key string. Use one delete batch per shard so a
    # restart-heavy full suite cannot spend 30s per key waiting on stale leader
    # state during cleanup.
    wait_default_waraft_ready(ready_timeout)

    Enum.each(0..(shard_count - 1), fn i ->
      shard = Router.shard_name(FerricStore.Instance.get(:default), i)

      keys =
        try do
          GenServer.call(shard, :keys, flush_timeout)
        catch
          :exit, _ -> []
        end

      delete_keys_on_shard(i, keys)
    end)

    # Clear cross-shard locks and intents through WARaft so tests start clean.
    Enum.each(0..(shard_count - 1), fn i ->
      case clear_locks_strict(i) do
        :ok ->
          :ok

        {:error, reason} ->
          raise "Shard #{i} clear_locks failed during cleanup: #{inspect(reason)}"
      end
    end)

    # Prove the WARaft write/apply path is usable before handing control back.
    wait_default_waraft_ready(flush_timeout)

    ctx = FerricStore.Instance.get(:default)

    # Flow secondary indexes are native-only and rebuilt from durable Flow
    # records. The key delete path above removes the durable records; reset the
    # native projection too so test setup cannot leak in-memory index state.
    Ferricstore.Flow.NativeOrderedIndex.reset_all(ctx.name, shard_count)
    clear_flow_projection_storage(ctx, shard_count)

    # Safety net: clear any remaining compound key entries from ETS.
    # After the per-shard deletes and drain above this should be a no-op,
    # but guards against edge cases where NIF tombstones haven't propagated.
    Enum.each(0..(shard_count - 1), fn i ->
      # Single-table keydir has 7-element tuples {key, value, expire_at_ms, lfu_counter, file_id, offset, value_size}
      try do
        :ets.select_delete(:"keydir_#{i}", [{{:"$1", :_, :_, :_, :_, :_, :_}, [], [true]}])
      rescue
        ArgumentError -> :ok
      end
    end)

    # Clear disk pressure flags. A previous test may have hit a transient
    # NIF flush error (e.g. a rotation race) and set the pressure atomic.
    # The production code only clears pressure on a subsequent successful
    # Shard flush, which may not happen between tests — so the async write
    # path rejects new writes with "ERR disk pressure on shard N".
    Enum.each(0..(shard_count - 1), fn i ->
      Ferricstore.Store.DiskPressure.clear(ctx, i)
    end)

    # Fully reset namespace config overrides so per-prefix commit windows
    # cannot leak across tests and alter batching timings.
    Ferricstore.NamespaceConfig.reset_all()
    reset_server_auth_state()

    # The safety-net ETS clear above bypasses normal insert/delete hooks, so
    # reset the auxiliary memory accounting that MemoryGuard reads lock-free.
    # Otherwise a prior test can leave phantom keydir bytes and make later
    # command tests fail with KEYDIR_FULL despite empty ETS tables.
    reset_keydir_binary_counters(ctx, shard_count)
    reset_memory_guard_pressure()
  end

  def reset_server_auth_state do
    Ferricstore.Config.set("requirepass", "")

    acl = Module.concat([FerricstoreServer, Acl])

    if Code.ensure_loaded?(acl) and function_exported?(acl, :reset!, 0) and
         Process.whereis(acl) do
      apply(acl, :reset!, [])
    end

    :ok
  rescue
    _ -> :ok
  end

  defp clear_flow_projection_storage(ctx, shard_count) do
    Enum.each(0..max(shard_count - 1, -1)//1, fn shard_index ->
      :ok = Ferricstore.Flow.HistoryProjector.discard(ctx, shard_index)
    end)

    :ok = Ferricstore.Flow.LMDBWriter.discard_all(ctx.name, shard_count)
    :ok = Ferricstore.Flow.LMDB.clear_all(ctx.data_dir, shard_count)

    Enum.each(0..max(shard_count - 1, -1)//1, fn shard_index ->
      history_dir =
        ctx.data_dir
        |> Ferricstore.DataDir.shard_data_path(shard_index)
        |> Ferricstore.Flow.HistoryProjector.history_dir()

      :ok = Ferricstore.FS.rm_rf(history_dir)
    end)
  end

  defp delete_keys_on_shard(_shard_index, []), do: :ok

  defp delete_keys_on_shard(shard_index, keys) do
    case Ferricstore.Raft.Backend.write_delete_batch(shard_index, keys) do
      {:ok, results} when is_list(results) ->
        if length(results) == length(keys) do
          :ok
        else
          raise "Shard #{shard_index} WARaft delete cleanup returned #{length(results)} result(s) for #{length(keys)} key(s)"
        end

      {:error, reason} ->
        raise "Shard #{shard_index} WARaft delete cleanup failed: #{inspect(reason)}"

      other ->
        raise "Shard #{shard_index} WARaft delete cleanup returned unexpected result: #{inspect(other)}"
    end
  end

  defp clear_locks_strict(shard_index) do
    case Ferricstore.Raft.Backend.write(shard_index, {:clear_locks}) do
      :ok -> :ok
      {:ok, :ok} -> :ok
      {:ok, {:applied_at, _index, :ok}} -> :ok
      {:error, _reason} = error -> {:error, error}
      other -> {:error, other}
    end
  catch
    :exit, reason -> {:error, {:exit, reason}}
  end

  defp wait_default_waraft_ready(timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms

    Enum.each(0..(shard_count() - 1), fn shard_index ->
      wait_waraft_shard_ready(shard_index, deadline)
    end)
  end

  defp wait_waraft_shard_ready(shard_index, deadline) do
    result =
      try do
        Ferricstore.Raft.Backend.write(shard_index, {:clear_locks})
      catch
        :exit, reason -> {:error, reason}
      end

    case result do
      :ok ->
        :ok

      {:ok, :ok} ->
        :ok

      {:ok, {:applied_at, _index, :ok}} ->
        :ok

      {:error, reason} ->
        retry_waraft_shard_ready(shard_index, deadline, reason)

      other ->
        retry_waraft_shard_ready(shard_index, deadline, other)
    end
  end

  defp retry_waraft_shard_ready(shard_index, deadline, reason) do
    if System.monotonic_time(:millisecond) > deadline do
      raise "Shard #{shard_index} WARaft path did not become ready before timeout: #{inspect(reason)}"
    end

    Process.sleep(100)
    wait_waraft_shard_ready(shard_index, deadline)
  end

  @doc """
  Waits until every default-instance shard can accept and apply a WARaft command.

  This keeps the historical helper name for existing tests, but WARaft is now
  the only default backend. The probe command is idempotent and does not create
  user keys.
  """
  @spec wait_default_pipeline_ready(non_neg_integer()) :: :ok
  def wait_default_pipeline_ready(timeout_ms \\ 60_000), do: wait_default_waraft_ready(timeout_ms)

  defp reset_keydir_binary_counters(%{keydir_binary_bytes: ref}, shard_count)
       when is_reference(ref) do
    Enum.each(1..shard_count, fn idx ->
      :atomics.put(ref, idx, 0)
    end)
  end

  defp reset_keydir_binary_counters(_ctx, _shard_count), do: :ok

  @doc """
  Restores the shared application MemoryGuard to the normal test baseline.

  Pressure tests deliberately force tiny budgets or reject flags. Those flags
  are read lock-free on the write path, so leaked state can make later command
  tests fail with `KEYDIR_FULL` even after all keys were flushed.
  """
  @spec reset_memory_guard_pressure() :: :ok
  def reset_memory_guard_pressure do
    case Process.whereis(Ferricstore.MemoryGuard) do
      nil ->
        :ok

      _pid ->
        try do
          :sys.resume(Ferricstore.MemoryGuard)
        catch
          :exit, _ -> :ok
        end

        try do
          Ferricstore.MemoryGuard.reconfigure(%{
            max_memory_bytes: Application.get_env(:ferricstore, :max_memory_bytes, 1_073_741_824),
            keydir_max_ram: Application.get_env(:ferricstore, :keydir_max_ram, 64 * 1024 * 1024),
            hot_cache_min_ram: Application.get_env(:ferricstore, :hot_cache_min_ram, 0),
            hot_cache_max_ram: :auto,
            eviction_policy: Application.get_env(:ferricstore, :eviction_policy, :volatile_lru)
          })

          :sys.replace_state(Ferricstore.MemoryGuard, fn state ->
            %{state | last_pressure_level: :ok, keydir_pressure_level: :ok}
          end)

          Ferricstore.MemoryGuard.reset_pressure_flags()
        catch
          :exit, _ -> :ok
        end
    end
  end

  @doc """
  Waits until every default-instance shard can accept a public quorum write.

  `FerricStore.await_ready/1` verifies process and leader health, but tests that
  restart the app late in the suite also need the batcher/Ra reply path to be
  live before issuing public writes. Probe writes are idempotent and callers
  usually follow this with `flush_all_keys/0` to remove them.
  """
  @spec wait_default_quorum_writable(non_neg_integer()) :: :ok
  def wait_default_quorum_writable(timeout_ms \\ 60_000) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    ctx = FerricStore.Instance.get(:default)

    Enum.each(0..(shard_count() - 1), fn shard_index ->
      key = writable_probe_key(ctx, shard_index)
      wait_probe_write(ctx, shard_index, key, deadline)
    end)
  end

  defp writable_probe_key(ctx, shard_index) do
    prefix = "__ferricstore_ready_probe_#{shard_index}_"

    Enum.find_value(0..100_000, fn n ->
      key = prefix <> Integer.to_string(n)
      if Router.shard_for(ctx, key) == shard_index, do: key
    end) || raise "could not find readiness probe key for shard #{shard_index}"
  end

  defp wait_probe_write(ctx, shard_index, key, deadline) do
    case Router.put(ctx, key, "1") do
      :ok ->
        :ok

      {:error, _reason} ->
        retry_probe_write(ctx, shard_index, key, deadline)
    end
  catch
    :exit, _reason ->
      retry_probe_write(ctx, shard_index, key, deadline)
  end

  defp retry_probe_write(ctx, shard_index, key, deadline) do
    if System.monotonic_time(:millisecond) > deadline do
      raise "Shard #{shard_index} did not accept quorum writes before readiness timeout"
    end

    Process.sleep(100)
    wait_probe_write(ctx, shard_index, key, deadline)
  end

  @doc """
  Resets shared mutable state that can leak between tests: waiters registry,
  client tracking tables, and slow log. Call in `setup` for any test that
  cares about a clean global environment.
  """
  @spec flush_global_state() :: :ok
  def flush_global_state do
    # Waiters
    if :ets.whereis(:ferricstore_waiters) != :undefined do
      :ets.delete_all_objects(:ferricstore_waiters)
    end

    # Client tracking
    for table <- [:ferricstore_tracking, :ferricstore_tracking_connections] do
      if :ets.whereis(table) != :undefined do
        :ets.delete_all_objects(table)
      end
    end

    # Slow log
    if :ets.whereis(:ferricstore_slowlog) != :undefined do
      :ets.delete_all_objects(:ferricstore_slowlog)
    end

    # Audit log
    if :ets.whereis(:ferricstore_audit_log) != :undefined do
      :ets.delete_all_objects(:ferricstore_audit_log)
    end

    :ok
  end

  @doc """
  Flushes WARaft namespace batchers before restart-heavy tests.
  """
  @spec compact_wal() :: :ok
  def compact_wal do
    Ferricstore.Raft.Batcher.flush_all(shard_count(), 10_000)
    |> case do
      :ok -> :ok
      {:error, _reason} -> :ok
    end
  end

  @doc """
  Waits until all application-supervised shards are alive and have
  completed their `init/1` (ETS warmed from Bitcask, Raft server started).

  Polls every 20ms up to `timeout_ms`. Raises if any shard hasn't restarted
  in time. Call this in `on_exit` callbacks after tests that kill shards.

  After confirming each process is registered and alive, makes a synchronous
  `GenServer.call(name, :flush)` to each shard. Because `GenServer.start_link`
  registers the name before `init/1` returns, Process.whereis can succeed while
  init is still running. The GenServer.call blocks until init completes,
  guaranteeing that ETS is fully warmed from Bitcask and the shard is ready
  to serve reads.
  """
  @spec wait_shards_alive(non_neg_integer()) :: :ok
  def wait_shards_alive(timeout_ms \\ 30_000) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    shard_count = shard_count()

    Enum.each(0..(shard_count - 1), fn i ->
      name = :"Ferricstore.Store.Shard.#{i}"

      result =
        Enum.reduce_while(Stream.repeatedly(fn -> Process.sleep(20) end), :waiting, fn _, _ ->
          pid = Process.whereis(name)

          cond do
            is_pid(pid) and Process.alive?(pid) ->
              {:halt, :ok}

            System.monotonic_time(:millisecond) > deadline ->
              {:halt, {:timeout, name}}

            true ->
              {:cont, :waiting}
          end
        end)

      case result do
        :ok -> :ok
        {:timeout, name} -> raise "Shard #{inspect(name)} did not restart within #{timeout_ms}ms"
      end
    end)

    # Make a synchronous GenServer.call to each shard to confirm init/1 has
    # completed. The name is registered before init returns, so Process.whereis
    # can succeed while init is still running (warming ETS from Bitcask, setting
    # up the Raft server). The :flush call blocks until init completes and is
    # harmless (flushes the empty pending list on a fresh shard).
    # Give each GenServer.call enough time for WARaft replay (can take 7+
    # seconds on CI with large WAL files). Minimum 15s per shard.
    remaining_ms = max(deadline - System.monotonic_time(:millisecond), 15_000)

    Enum.each(0..(shard_count - 1), fn i ->
      name = :"Ferricstore.Store.Shard.#{i}"

      try do
        GenServer.call(name, :flush, remaining_ms)
      catch
        :exit, _ -> :ok
      end
    end)

    :ok
  end

  # ---------------------------------------------------------------------------
  # Dynamic key discovery helpers
  # ---------------------------------------------------------------------------

  @doc """
  Finds a key string that routes to the given shard index.

  Iterates candidate keys `"dynkey_0"`, `"dynkey_1"`, ... until one hashes
  to `shard_idx`. Returns the matching key string.
  """
  @spec key_for_shard(non_neg_integer()) :: binary()
  def key_for_shard(shard_idx) do
    i =
      Enum.find(0..100_000, fn i ->
        Router.shard_for(FerricStore.Instance.get(:default), "dynkey_#{i}") == shard_idx
      end)

    "dynkey_#{i}"
  end

  @doc """
  Finds `n` keys that each route to DIFFERENT shards.

  Returns a list of `n` key strings, each on a distinct shard.
  Raises if fewer than `n` shards exist.
  """
  @spec keys_on_different_shards(pos_integer()) :: [binary()]
  def keys_on_different_shards(n) do
    shard_count = shard_count()
    target_shards = Enum.take(0..(shard_count - 1), n)
    Enum.map(target_shards, &key_for_shard/1)
  end

  @doc """
  Finds 2 keys that route to the SAME shard.

  Returns `{key_a, key_b}` where both hash to the same shard index.
  """
  @spec keys_on_same_shard() :: {binary(), binary()}
  def keys_on_same_shard do
    shard = Router.shard_for(FerricStore.Instance.get(:default), "same_a")

    other =
      Enum.find(0..100_000, fn i ->
        Router.shard_for(FerricStore.Instance.get(:default), "same_#{i}") == shard and
          "same_#{i}" != "same_a"
      end)

    {"same_a", "same_#{other}"}
  end

  @doc """
  Finds 2 keys that route to different shards under the given namespace prefix.

  Returns `{key_a, key_b}` (without the namespace prefix) where
  `Router.shard_for(FerricStore.Instance.get(:default), ns <> key_a) != Router.shard_for(FerricStore.Instance.get(:default), ns <> key_b)`.
  """
  @spec cross_shard_keys_for_namespace(binary()) :: {binary(), binary()}
  def cross_shard_keys_for_namespace(ns) do
    key_a = "nskey_0"
    shard_a = Router.shard_for(FerricStore.Instance.get(:default), ns <> key_a)

    i =
      Enum.find(1..100_000, fn i ->
        Router.shard_for(FerricStore.Instance.get(:default), ns <> "nskey_#{i}") != shard_a
      end)

    {key_a, "nskey_#{i}"}
  end

  defp shard_count do
    case FerricStore.Instance.get(:default) do
      %{shard_count: count} when is_integer(count) and count > 0 ->
        count

      _ ->
        configured_shard_count()
    end
  rescue
    _ -> configured_shard_count()
  catch
    _, _ -> configured_shard_count()
  end

  defp configured_shard_count do
    :persistent_term.get(
      :ferricstore_shard_count,
      Application.get_env(:ferricstore, :shard_count, 4)
    )
  end

  # ---------------------------------------------------------------------------
  # Safe shard kill with supervisor budget awareness
  # ---------------------------------------------------------------------------

  @doc """
  Kills a shard process safely, respecting the supervisor's max_restarts budget.

  Tracks kills in a persistent_term counter. If too many kills have happened
  in the current window, sleeps to let the supervisor budget reset before
  killing. After the kill, waits for the shard to fully restart (including
  Raft leader election).

  Use this instead of raw `Process.exit(pid, :kill)` in all shard-kill tests.

  ## Parameters

    * `shard_index` -- zero-based shard index to kill
    * `opts` -- keyword options:
      * `:timeout` -- max ms to wait for restart (default: 10_000)

  ## Returns

    * `:ok` on successful kill + restart

  ## Example

      ShardHelpers.kill_shard_safely(0)
      assert Router.get(FerricStore.Instance.get(:default), key) == "still_here"
  """
  @spec kill_shard_safely(non_neg_integer(), keyword()) :: :ok
  def kill_shard_safely(shard_index, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, 30_000)

    # Rate-limit kills to stay under the supervisor's max_restarts budget.
    # Test config allows 1000 restarts in 60s, so 100ms spacing is generous.
    last_kill_key = :ferricstore_test_last_kill_ms

    last_kill =
      try do
        :persistent_term.get(last_kill_key)
      rescue
        ArgumentError -> System.monotonic_time(:millisecond)
      end

    now = System.monotonic_time(:millisecond)
    elapsed = now - last_kill

    if elapsed < 100 do
      Process.sleep(100 - elapsed)
    end

    name = :"Ferricstore.Store.Shard.#{shard_index}"
    pid = Process.whereis(name)

    if is_nil(pid) or not Process.alive?(pid) do
      # Already dead — just wait for restart
      wait_shards_alive(timeout)
      :ok
    else
      ref = Process.monitor(pid)
      Process.exit(pid, :kill)

      receive do
        {:DOWN, ^ref, :process, ^pid, _reason} -> :ok
      after
        2_000 -> raise "Shard #{shard_index} did not die within 2000ms"
      end

      :persistent_term.put(last_kill_key, System.monotonic_time(:millisecond))
      wait_shards_alive(timeout)
      :ok
    end
  end

  @doc """
  Kills the shard that owns the given key. Convenience wrapper around
  `kill_shard_safely/2`.
  """
  @spec kill_shard_for_key(binary(), keyword()) :: :ok
  def kill_shard_for_key(key, opts \\ []) do
    kill_shard_safely(Router.shard_for(FerricStore.Instance.get(:default), key), opts)
  end

  @doc """
  Polls `fun` until it returns a truthy value, sleeping `interval_ms` between
  attempts. Returns `:ok` on success. Raises with `msg` if the condition is
  not met within `attempts * interval_ms`.

  Use this after shard kill/restart to wait for data recovery before asserting.
  After a shard restart, the ETS keydir is empty until `init/1` finishes
  recovering from Bitcask. `Router.get` returns `nil` for keys that are not
  yet in ETS (the `:miss` fast path), so immediate assertions on recovered
  values will fail on slow CI runners.

  ## Example

      ShardHelpers.kill_shard_safely(0)
      ShardHelpers.eventually(fn -> Router.get(FerricStore.Instance.get(:default), key) == "expected" end,
                              "key should survive shard restart")
  """
  @spec eventually((-> boolean()), binary(), pos_integer(), pos_integer()) :: :ok
  def eventually(fun, msg \\ "condition not met", attempts \\ 100, interval_ms \\ 100) do
    result =
      try do
        fun.()
      rescue
        _ -> false
      catch
        :exit, _ -> false
      end

    if result do
      :ok
    else
      if attempts > 1 do
        Process.sleep(interval_ms)
        eventually(fun, msg, attempts - 1, interval_ms)
      else
        raise ExUnit.AssertionError, message: "Timed out: #{msg}"
      end
    end
  end

  @doc """
  Starts Erlang distribution for cluster-style tests without leaving the default
  FerricStore application in a broken Ra identity state.

  Ra server IDs include the Erlang node name. If a test calls `Node.start/2`
  after FerricStore has already booted, existing Ra servers keep their old
  `:nonode@nohost` IDs and become unreachable through Ra membership APIs. When
  this helper has to start distribution, it stops FerricStore first and restarts
  it after the node name is stable.
  """
  @spec ensure_distribution_started!(atom() | binary()) :: :ok
  def ensure_distribution_started!(prefix \\ :ferric_runner) do
    case Node.self() do
      :nonode@nohost ->
        server_started? = application_started?(:ferricstore_server)
        store_started? = application_started?(:ferricstore)
        data_dir = Application.get_env(:ferricstore, :data_dir, "data")

        stop_app_if_started(:ferricstore_server)
        stop_app_if_started(:ferricstore)
        Ferricstore.Raft.WARaftBackend.stop()

        prefix = prefix |> to_string() |> String.trim_leading(":")
        node_name = :"#{prefix}_#{:erlang.unique_integer([:positive])}"
        start_distribution!(node_name)

        if store_started? do
          restart_with_data_dir(data_dir, server_started?,
            clean?: clean_restart_data_dir?(data_dir)
          )
        end

        :ok

      _node ->
        :ok
    end
  end

  defp clean_restart_data_dir?(data_dir) do
    expanded = Path.expand(data_dir)
    tmp = Path.expand(System.tmp_dir!())

    expanded == tmp or
      String.starts_with?(expanded, tmp <> "/") or
      String.contains?(expanded, "ferricstore_test")
  end

  @doc """
  Sets up an isolated test environment with a fresh temp data directory.

  Switches `Application.get_env(:ferricstore, :data_dir)` to a new temp dir
  and restarts all shards so they pick up the clean directory. Returns a map
  with `:original_dir` and `:tmp_dir` for use in `on_exit`.

  Use in `setup` callbacks for tests that need complete isolation from other
  tests (e.g. graceful shutdown, crash recovery):

      setup do
        ctx = ShardHelpers.setup_isolated_data_dir()

        on_exit(fn ->
          ShardHelpers.teardown_isolated_data_dir(ctx)
        end)
      end
  """
  @spec setup_isolated_data_dir() :: map()
  def setup_isolated_data_dir do
    original_dir = Application.get_env(:ferricstore, :data_dir, "data")

    tmp_dir =
      Path.join(System.tmp_dir!(), "ferricstore_isolated_#{System.unique_integer([:positive])}")

    server_started? = application_started?(:ferricstore_server)

    restart_with_data_dir(tmp_dir, server_started?, clean?: true)

    %{original_dir: original_dir, tmp_dir: tmp_dir, server_started?: server_started?}
  end

  @doc """
  Tears down the isolated test environment. Ensures shards are alive
  for the next test.
  """
  @spec teardown_isolated_data_dir(map()) :: :ok
  def teardown_isolated_data_dir(%{
        original_dir: original_dir,
        tmp_dir: tmp_dir,
        server_started?: server_started?
      }) do
    restart_with_data_dir(original_dir, server_started?, clean?: false)
    File.rm_rf!(tmp_dir)
    :ok
  end

  @doc """
  Restarts FerricStore against the current data directory without deleting it.

  Use this for graceful shutdown/restart tests that need to exercise the real
  application lifecycle while preserving records written before shutdown.
  """
  @spec restart_current_data_dir(map()) :: :ok
  def restart_current_data_dir(%{tmp_dir: data_dir, server_started?: server_started?}) do
    restart_with_data_dir(data_dir, server_started?, clean?: false)
  end

  defp restart_with_data_dir(data_dir, server_started?, opts) do
    # Keep Ra isolation coarse-grained. Force-deleting individual Ra servers
    # while the Ra system/WAL is live can leave old UID entries in WAL and make
    # the next shard restart fail with `:gap_between_snapshot_and_log_range`.
    stop_app_if_started(:ferricstore_server)
    stop_app_if_started(:ferricstore)
    Ferricstore.Raft.WARaftBackend.stop()

    # Setup needs a clean temp dir. Teardown must restore the original data dir
    # without deleting real state that existed before the isolated test.
    if Keyword.fetch!(opts, :clean?) do
      File.rm_rf!(data_dir)
    end

    Application.put_env(:ferricstore, :data_dir, data_dir)

    {:ok, _} = Application.ensure_all_started(:ferricstore)

    wait_shards_alive(30_000)

    if server_started? do
      {:ok, _} = Application.ensure_all_started(:ferricstore_server)
    end

    Ferricstore.Health.set_ready(true)
    :ok
  end

  defp application_started?(app) do
    Enum.any?(Application.started_applications(), fn {started_app, _desc, _vsn} ->
      started_app == app
    end)
  end

  defp stop_app_if_started(app) do
    if application_started?(app) do
      _ = Application.stop(app)
    end
  end

  defp start_distribution!(node_name) do
    task = Task.async(fn -> Node.start(node_name, :shortnames) end)

    case Task.yield(task, 10_000) || Task.shutdown(task, :brutal_kill) do
      {:ok, {:ok, _}} ->
        :ok

      {:ok, {:error, reason}} ->
        raise "Failed to start Erlang distribution (#{inspect(reason)}). " <>
                "Try running with: elixir --sname test -S mix test"

      _ ->
        raise "Node.start timed out after 10s. " <>
                "Run with: elixir --sname test -S mix test"
    end
  end
end
