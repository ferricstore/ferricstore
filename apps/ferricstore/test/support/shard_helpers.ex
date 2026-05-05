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

    # Drain Raft batchers first. With Raft enabled, Router.put sends writes
    # to Batcher.write_async which pipelines them to ra. Batcher.flush
    # submits all pending slots and waits for in-flight commands to apply.
    # Without this, writes may still be in the Batcher pipeline when we
    # flush shards and BitcaskWriter — the data never reaches Bitcask.
    Enum.each(0..(shard_count - 1), fn i ->
      try do
        Ferricstore.Raft.Batcher.flush(i)
      catch
        :exit, _ -> :ok
      end
    end)

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
    alias Ferricstore.Raft.Batcher

    shard_count = Application.get_env(:ferricstore, :shard_count, 4)

    # Under full-suite load, the batcher can be slow to respond.
    # Use a generous timeout for test cleanup (production Batcher.flush uses 10s).
    flush_timeout = 30_000

    # Ensure all ra shard processes are alive before flushing.
    # A previous test may have crashed ra (e.g. FunctionClauseError under load),
    # leaving the batcher blocked. Restart dead shards first.
    ensure_ra_shards_alive(shard_count)

    # Before flushing, unstick any Batcher whose pending queue is blocked on
    # orphan correlations from a prior test (ra leader crash, lost acks, etc).
    # Without this, :flush can wait forever for replies that will never
    # arrive, causing 60s setup timeouts that cascade through the suite.
    Enum.each(0..(shard_count - 1), fn i ->
      Batcher.reset_pending(i)
    end)

    # Batcher.flush now waits for all in-flight async commands (tracked in
    # `pending` with :async_no_reply) to apply via ra_event before replying.
    # AsyncApplyWorker is deprecated — no drain needed.
    Enum.each(0..(shard_count - 1), fn i ->
      try do
        GenServer.call(Batcher.batcher_name(i), :flush, flush_timeout)
      catch
        :exit, _ -> :ok
      end
    end)

    # Flush background BitcaskWriter so deferred writes are on disk
    # before we snapshot keys for deletion.
    Ferricstore.Store.BitcaskWriter.flush_all(shard_count)

    # Delete every key on each shard directly via that shard's GenServer.
    # We must NOT use Router.delete/1 because it re-hashes the key, which
    # routes compound keys (H:, S:, Z:, T: prefixed) to the wrong shard —
    # compound keys live on their parent's shard, not the shard determined
    # by hashing the compound key string.
    Enum.each(0..(shard_count - 1), fn i ->
      shard = Router.shard_name(FerricStore.Instance.get(:default), i)

      keys =
        try do
          GenServer.call(shard, :keys, flush_timeout)
        catch
          :exit, _ -> []
        end

      Enum.each(keys, fn key ->
        try do
          GenServer.call(shard, {:delete, key}, flush_timeout)
        catch
          :exit, _ -> :ok
        end
      end)
    end)

    # The deletes above go through the Raft batcher (async). Flush the
    # pipeline again so the tombstones are applied before we return.
    Enum.each(0..(shard_count - 1), fn i ->
      try do
        GenServer.call(Batcher.batcher_name(i), :flush, flush_timeout)
      catch
        :exit, _ -> :ok
      end
    end)

    # Clear cross-shard locks and intents via Raft so tests start clean.
    Enum.each(0..(shard_count - 1), fn i ->
      shard_id = Ferricstore.Raft.Cluster.shard_server_id(i)

      try do
        :ra.process_command(shard_id, {:clear_locks})
      catch
        :exit, _ -> :ok
      end
    end)

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
    ctx = FerricStore.Instance.get(:default)

    Enum.each(0..(shard_count - 1), fn i ->
      Ferricstore.Store.DiskPressure.clear(ctx, i)
    end)

    # Fully reset namespace config overrides so per-prefix commit windows
    # cannot leak across tests and alter batching timings.
    Ferricstore.NamespaceConfig.reset_all()
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
  Forces a WAL rollover on the Raft system. This triggers the segment writer
  to flush mem tables and allows ra to take snapshots, truncating the WAL.

  Call before shard-kill tests to ensure WAL is small — a 256MB WAL from
  previous tests causes 30s+ replay on restart, hanging tests.
  """
  @spec compact_wal() :: :ok
  def compact_wal do
    try do
      wal_name = :ra_system.derive_names(Ferricstore.Raft.Cluster.system_name()).wal
      :ra_log_wal.force_roll_over(wal_name)
      # Give the segment writer time to process the rolled WAL
      Process.sleep(500)
    catch
      _, _ -> :ok
    end

    :ok
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
    # Give each GenServer.call enough time for Raft WAL replay (can take 7+
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

    # When Raft is enabled, also wait for each shard's ra server to have
    # an elected leader. Without a leader, Batcher.write calls will fail.
    # This check runs AFTER the GenServer.call ping above, so the ra server
    # started inside Shard.init is guaranteed to exist (the old ra server
    # has already been force-deleted and replaced).
    wait_raft_leaders(deadline)
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

  # ---------------------------------------------------------------------------
  # Internal
  # ---------------------------------------------------------------------------

  # Checks that all ra shard processes are alive and have a leader.
  # If a ra process crashed (e.g. FunctionClauseError under load),
  # restart it so subsequent tests don't cascade-fail with batcher timeouts.
  defp ensure_ra_shards_alive(shard_count) do
    alias Ferricstore.Raft.Cluster
    system = Cluster.system_name()

    Enum.each(0..(shard_count - 1), fn i ->
      server_id = Cluster.shard_server_id(i)
      {shard_name, _node} = server_id

      case Process.whereis(shard_name) do
        nil ->
          # ra process is dead — restart it via ra:restart_server
          # which recovers from WAL (the data is still on disk)
          try do
            :ra.restart_server(system, server_id)
          catch
            _, _ -> :ok
          end

          # Wait for leader election
          eventually(
            fn -> {:ok, _, _} = :ra.members(server_id) end,
            "shard #{i} ra restart",
            20,
            200
          )

        pid when is_pid(pid) ->
          # Process exists but might be in a bad state — verify it responds
          case :ra.members(server_id) do
            {:ok, _, _} ->
              :ok

            _ ->
              # No leader — trigger election and wait for it to complete.
              # A bare Process.sleep(200) is not enough under full-suite load;
              # elections can take longer, and flush_all_keys relies on a
              # healthy leader to receive ra_event replies.
              try do
                :ra.trigger_election(server_id)
              catch
                _, _ -> :ok
              end

              eventually(
                fn -> {:ok, _, _} = :ra.members(server_id) end,
                "shard #{i} leader election",
                20,
                200
              )
          end
      end
    end)
  end

  defp shard_count do
    :persistent_term.get(
      :ferricstore_shard_count,
      Application.get_env(:ferricstore, :shard_count, 4)
    )
  end

  # Polls ra.members/1 for each shard until all ra servers report a leader.
  defp wait_raft_leaders(deadline) do
    alias Ferricstore.Raft.Cluster
    shard_count = shard_count()

    Enum.each(0..(shard_count - 1), fn i ->
      server_id = Cluster.shard_server_id(i)

      Enum.reduce_while(Stream.repeatedly(fn -> Process.sleep(20) end), :waiting, fn _, _ ->
        cond do
          System.monotonic_time(:millisecond) > deadline ->
            {:halt, :ok}

          true ->
            case :ra.members(server_id) do
              {:ok, _members, _leader} -> {:halt, :ok}
              _ -> {:cont, :waiting}
            end
        end
      end)
    end)
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

  defp restart_with_data_dir(data_dir, server_started?, opts) do
    # Keep Ra isolation coarse-grained. Force-deleting individual Ra servers
    # while the Ra system/WAL is live can leave old UID entries in WAL and make
    # the next shard restart fail with `:gap_between_snapshot_and_log_range`.
    stop_app_if_started(:ferricstore_server)
    stop_app_if_started(:ferricstore)
    stop_ra_system()

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

  defp stop_ra_system do
    try do
      :ra_system.stop(Ferricstore.Raft.Cluster.system_name())
    catch
      _, _ -> :ok
    end

    :ok
  end
end
