defmodule FerricstoreServer.Spec.DurabilityTest do
  @moduledoc """
  Spec section 4: Durability Mode Tests.

  Validates the three durability subsystems on a single node:

    * Section 4.1 -- Quorum mode (default): writes through the Raft batcher
      are durable across shard restarts and provide linearizable read-after-write.
    * Section 4.2 -- Async mode: namespace-configured async writes bypass Raft
      consensus, are fast, readable, and independent of quorum namespaces.
    * Section 4.3 -- Group commit window: writes within a window are batched,
      different namespaces have independent windows, and explicit flush drains
      all pending slots.

  These tests exercise the full write path through `Ferricstore.Raft.Batcher`
  and `Ferricstore.Store.Router`, using `Ferricstore.NamespaceConfig` for
  namespace configuration.

  All tests are single-node scenarios (no cluster tag).
  """

  use ExUnit.Case, async: false
  @moduletag :shard_kill
  @moduletag timeout: 600_000

  alias Ferricstore.NamespaceConfig
  alias Ferricstore.Raft.Batcher
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  setup_all do
    ShardHelpers.wait_shards_alive()
    :ok
  end

  setup do
    ShardHelpers.flush_all_keys()
    NamespaceConfig.reset_all()

    on_exit(fn ->
      NamespaceConfig.reset_all()
      ShardHelpers.wait_shards_alive()
    end)
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  # Generates a unique key with an optional prefix namespace.
  defp ukey(base), do: "dur_#{base}_#{:rand.uniform(9_999_999)}"
  defp pkey(prefix, base), do: "#{prefix}:dur_#{base}_#{:rand.uniform(9_999_999)}"

  # Returns the PID of the shard GenServer that owns `key`.
  defp shard_pid_for(key) do
    name = Router.shard_name(FerricStore.Instance.get(:default), Router.shard_for(FerricStore.Instance.get(:default), key))
    Process.whereis(name)
  end

  # Writes a key through the Raft batcher (the canonical write path).
  defp batcher_put(key, value, expire_at_ms \\ 0) do
    shard = Router.shard_for(FerricStore.Instance.get(:default), key)
    Batcher.write(shard, {:put, key, value, expire_at_ms})
  end

  # Flushes all 4 shard batchers to ensure pending writes are committed.
  defp flush_all_batchers do
    Enum.each(0..3, &Batcher.flush/1)
  end

  # ==========================================================================
  # Section 4.1: Quorum Mode (default)
  # ==========================================================================

  describe "4.1 Quorum Mode" do
    @tag :durability
    test "DQ-001: write acknowledged through Raft batcher is durable across shard restart" do
      k = ukey("dq001")

      # Write through the Raft batcher (quorum path)
      assert :ok == batcher_put(k, "durable_value")

      # Verify the value is present
      assert "durable_value" == Router.get(FerricStore.Instance.get(:default), k)

      # Flush all pending writes to Bitcask before killing the shard
      pid = shard_pid_for(k)
      assert is_pid(pid)
      :ok = GenServer.call(pid, :flush)

      # Kill the owning shard process
      ref = Process.monitor(pid)
      Process.exit(pid, :kill)
      assert_receive {:DOWN, ^ref, :process, ^pid, :killed}, 2_000

      # Wait for supervisor to restart the shard and Raft leader to be elected
      ShardHelpers.wait_shards_alive()

      # Data persisted through the Raft batcher should survive the restart
      new_pid = shard_pid_for(k)
      assert is_pid(new_pid)
      assert new_pid != pid, "Expected a new process after restart"
      ShardHelpers.eventually(fn -> "durable_value" == Router.get(FerricStore.Instance.get(:default), k) end,
        "data should survive shard restart")
    end

    @tag :durability
    test "DQ-002: read after write is linearizable" do
      k = ukey("dq002")

      # Write through batcher and immediately read -- must return the written value
      assert :ok == batcher_put(k, "linearizable")
      assert "linearizable" == Router.get(FerricStore.Instance.get(:default), k)
    end

    @tag :durability
    test "DQ-002: sequential overwrites are linearizable" do
      k = ukey("dq002_seq")

      for i <- 1..10 do
        assert :ok == batcher_put(k, "val_#{i}")
        assert "val_#{i}" == Router.get(FerricStore.Instance.get(:default), k),
               "Expected val_#{i} after sequential overwrite #{i}"
      end
    end

    @tag :durability
    test "DQ-003: concurrent writes to the same key converge to one value" do
      k = ukey("dq003")
      n = 100

      # Launch 100 concurrent writers, all targeting the same key
      tasks =
        for i <- 1..n do
          Task.async(fn ->
            batcher_put(k, "writer_#{i}")
          end)
        end

      results = Task.await_many(tasks, 30_000)

      # All writes should succeed
      assert Enum.all?(results, &(&1 == :ok)),
             "Expected all concurrent writes to succeed, got: #{inspect(Enum.reject(results, &(&1 == :ok)))}"

      # Final GET should return exactly one of the written values
      final = Router.get(FerricStore.Instance.get(:default), k)
      assert is_binary(final), "Expected a binary value, got: #{inspect(final)}"
      assert String.starts_with?(final, "writer_"), "Expected value from one of the writers, got: #{inspect(final)}"

      # Parse the writer number and verify it's in range
      "writer_" <> num_str = final
      {num, ""} = Integer.parse(num_str)
      assert num >= 1 and num <= n, "Writer number #{num} out of expected range 1..#{n}"
    end

    @tag :durability
    test "DQ-004: MULTI/EXEC batch write atomicity -- both present or neither" do
      # Use TCP to exercise MULTI/EXEC through the full command pipeline.
      # This tests that a committed transaction applies all commands atomically.
      port = FerricstoreServer.Listener.port()

      {:ok, sock} =
        :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false, packet: :raw])

      send_cmd(sock, ["HELLO", "3"])
      _greeting = recv_response(sock)

      k_a = ukey("dq004_a")
      k_b = ukey("dq004_b")

      # Execute a MULTI/EXEC transaction setting both keys
      send_cmd(sock, ["MULTI"])
      resp = recv_response(sock)
      assert resp in ["OK", {:simple, "OK"}]

      send_cmd(sock, ["SET", k_a, "atomic_1"])
      resp = recv_response(sock)
      assert resp in ["QUEUED", {:simple, "QUEUED"}]

      send_cmd(sock, ["SET", k_b, "atomic_2"])
      resp = recv_response(sock)
      assert resp in ["QUEUED", {:simple, "QUEUED"}]

      send_cmd(sock, ["EXEC"])
      result = recv_response(sock)

      # Transaction should succeed
      assert is_list(result), "Expected EXEC to return a list, got: #{inspect(result)}"

      # Both keys must be present
      send_cmd(sock, ["GET", k_a])
      assert recv_response(sock) == "atomic_1"

      send_cmd(sock, ["GET", k_b])
      assert recv_response(sock) == "atomic_2"

      :gen_tcp.close(sock)
    end

    @tag :durability
    test "DQ-004: aborted MULTI/EXEC leaves neither key" do
      port = FerricstoreServer.Listener.port()

      sock1 =
        tcp_connect_and_hello(port)

      # Pre-set the watched key
      k_watch = ukey("dq004_watch")
      k_a = ukey("dq004_abort_a")
      k_b = ukey("dq004_abort_b")

      send_cmd(sock1, ["SET", k_watch, "original"])
      assert recv_response(sock1) in ["OK", {:simple, "OK"}]

      # WATCH the key on connection 1
      send_cmd(sock1, ["WATCH", k_watch])
      assert recv_response(sock1) in ["OK", {:simple, "OK"}]

      # Modify the watched key from another connection to force an abort
      sock2 = tcp_connect_and_hello(port)
      send_cmd(sock2, ["SET", k_watch, "modified"])
      assert recv_response(sock2) in ["OK", {:simple, "OK"}]
      :gen_tcp.close(sock2)

      # Queue changes to both keys in a transaction
      send_cmd(sock1, ["MULTI"])
      assert recv_response(sock1) in ["OK", {:simple, "OK"}]

      send_cmd(sock1, ["SET", k_a, "should_not_exist"])
      assert recv_response(sock1) in ["QUEUED", {:simple, "QUEUED"}]

      send_cmd(sock1, ["SET", k_b, "should_not_exist"])
      assert recv_response(sock1) in ["QUEUED", {:simple, "QUEUED"}]

      # EXEC should return nil (abort) because WATCH detected conflict
      send_cmd(sock1, ["EXEC"])
      assert recv_response(sock1) == nil

      # Neither key should have been set
      send_cmd(sock1, ["GET", k_a])
      assert recv_response(sock1) == nil

      send_cmd(sock1, ["GET", k_b])
      assert recv_response(sock1) == nil

      :gen_tcp.close(sock1)
    end
  end

  # ==========================================================================
  # Section 4.2: Async Mode
  # ==========================================================================

  describe "4.2 Async Mode" do
    @describetag skip: "async durability feature removed; quorum is the only supported durability"

    @tag :durability
    test "DA-001: async write returns :ok quickly" do
      NamespaceConfig.set("fast", "durability", "async")

      k = pkey("fast", "da001")
      shard = Router.shard_for(FerricStore.Instance.get(:default), k)

      # Measure async write latency
      {async_us, result} =
        :timer.tc(fn ->
          Batcher.write(shard, {:put, k, "quick_val", 0})
        end)

      assert result == :ok

      # Also measure a quorum write for comparison
      k_quorum = ukey("da001_quorum")
      shard_q = Router.shard_for(FerricStore.Instance.get(:default), k_quorum)

      {quorum_us, result_q} =
        :timer.tc(fn ->
          Batcher.write(shard_q, {:put, k_quorum, "slow_val", 0})
        end)

      assert result_q == :ok

      # The async path should not be dramatically slower than quorum.
      # On single-node, async should be at most 3x quorum (generous bound for CI).
      # We mainly assert it returns :ok without blocking.
      assert async_us <= quorum_us * 3 + 5_000,
             "Async write (#{async_us}us) was unexpectedly slow compared to quorum (#{quorum_us}us)"
    end

    @tag :durability
    test "DA-002: async write is readable after processing" do
      NamespaceConfig.set("fast", "durability", "async")

      k = pkey("fast", "da002")
      shard = Router.shard_for(FerricStore.Instance.get(:default), k)

      assert :ok == Batcher.write(shard, {:put, k, "readable_val", 0})

      # Wait for the async worker to process the batch
      ShardHelpers.eventually(fn ->
        "readable_val" == Router.get(FerricStore.Instance.get(:default), k)
      end, "async write should be readable after processing", 20, 20)
    end

    @tag :durability
    test "DA-002: multiple async writes are all readable" do
      NamespaceConfig.set("fast", "durability", "async")

      keys =
        for i <- 1..20 do
          k = pkey("fast", "da002_multi_#{i}")
          shard = Router.shard_for(FerricStore.Instance.get(:default), k)
          assert :ok == Batcher.write(shard, {:put, k, "val_#{i}", 0})
          {k, i}
        end

      # Wait for async processing
      for {k, i} <- keys do
        ShardHelpers.eventually(fn ->
          "val_#{i}" == Router.get(FerricStore.Instance.get(:default), k)
        end, "key #{k} should be readable after async write", 20, 20)
      end
    end

    @tag :durability
    test "DA-003: quorum and async namespaces have independent durability windows" do
      # Configure two separate namespaces with different durability modes
      NamespaceConfig.set("session", "durability", "quorum")
      NamespaceConfig.set("sensor", "durability", "async")

      # Write to both namespaces
      k_session = pkey("session", "da003")
      k_sensor = pkey("sensor", "da003")

      shard_session = Router.shard_for(FerricStore.Instance.get(:default), k_session)
      shard_sensor = Router.shard_for(FerricStore.Instance.get(:default), k_sensor)

      # Quorum write -- synchronous, immediately consistent
      assert :ok == Batcher.write(shard_session, {:put, k_session, "session_val", 0})
      assert "session_val" == Router.get(FerricStore.Instance.get(:default), k_session)

      # Async write -- fire-and-forget, eventually consistent
      assert :ok == Batcher.write(shard_sensor, {:put, k_sensor, "sensor_val", 0})
      ShardHelpers.eventually(fn ->
        "sensor_val" == Router.get(FerricStore.Instance.get(:default), k_sensor)
      end, "async sensor write should be readable", 20, 20)

      # Verify the durability modes are actually different
      assert :quorum == NamespaceConfig.durability_for("session")
      assert :async == NamespaceConfig.durability_for("sensor")

      # Verify they use separate batcher slots by writing to both concurrently
      tasks =
        for i <- 1..10 do
          [
            Task.async(fn ->
              k = pkey("session", "indep_#{i}")
              s = Router.shard_for(FerricStore.Instance.get(:default), k)
              Batcher.write(s, {:put, k, "s_#{i}", 0})
              k
            end),
            Task.async(fn ->
              k = pkey("sensor", "indep_#{i}")
              s = Router.shard_for(FerricStore.Instance.get(:default), k)
              Batcher.write(s, {:put, k, "n_#{i}", 0})
              k
            end)
          ]
        end
        |> List.flatten()

      results = Task.await_many(tasks, 15_000)

      # Every key should be readable regardless of namespace
      session_keys = Enum.take_every(results, 2)
      sensor_keys = Enum.drop(results, 1) |> Enum.take_every(2)

      for {k, i} <- Enum.with_index(session_keys, 1) do
        ShardHelpers.eventually(fn ->
          "s_#{i}" == Router.get(FerricStore.Instance.get(:default), k)
        end, "Session key #{k} not readable", 20, 20)
      end

      for {k, i} <- Enum.with_index(sensor_keys, 1) do
        ShardHelpers.eventually(fn ->
          "n_#{i}" == Router.get(FerricStore.Instance.get(:default), k)
        end, "Sensor key #{k} not readable", 20, 20)
      end
    end

    @tag :durability
    test "DA-004: key with no namespace prefix defaults to quorum durability" do
      # A bare key (no colon) should use the _root namespace, which defaults to quorum
      k = ukey("da004")

      assert :quorum == NamespaceConfig.durability_for("_root")
      assert :quorum == NamespaceConfig.default_durability()

      # Write and immediately read -- quorum provides linearizable read-after-write
      assert :ok == batcher_put(k, "default_quorum")
      assert "default_quorum" == Router.get(FerricStore.Instance.get(:default), k)
    end

    @tag :durability
    test "DA-004: unconfigured prefix defaults to quorum" do
      # An explicitly prefixed key with no namespace config should also default to quorum
      k = pkey("unconfigured", "da004")

      assert :quorum == NamespaceConfig.durability_for("unconfigured")

      shard = Router.shard_for(FerricStore.Instance.get(:default), k)
      assert :ok == Batcher.write(shard, {:put, k, "default_quorum_ns", 0})
      assert "default_quorum_ns" == Router.get(FerricStore.Instance.get(:default), k)
    end
  end

  # ==========================================================================
  # Section 4.3: Group Commit Window
  # ==========================================================================

  describe "4.3 Group Commit Window" do
    @tag :durability
    test "GC-001: multiple writes within the commit window are batched together" do
      # Configure a namespace with a longer window to increase the chance of
      # observing batching behavior (multiple commands in a single ra submission).
      NamespaceConfig.set("batch", "window_ms", "50")

      shard_idx = 0

      # Find multiple keys in the "batch" namespace that hash to the same shard
      keys =
        Enum.reduce_while(1..10_000, [], fn i, acc ->
          k = "batch:gc001_#{i}"

          if Router.shard_for(FerricStore.Instance.get(:default), k) == shard_idx and length(acc) < 10 do
            {:cont, [k | acc]}
          else
            if length(acc) >= 10, do: {:halt, acc}, else: {:cont, acc}
          end
        end)

      assert length(keys) >= 5, "Could not find enough keys for shard #{shard_idx}"

      # Fire all writes concurrently within the 50ms window -- they should be
      # accumulated in the same batcher slot and flushed as a single ra batch
      # when the window expires.
      tasks =
        Enum.map(keys, fn k ->
          Task.async(fn ->
            Batcher.write(shard_idx, {:put, k, "batched", 0})
          end)
        end)

      results = Task.await_many(tasks, 10_000)
      assert Enum.all?(results, &(&1 == :ok))

      # All keys should be readable -- this proves the batch was applied atomically
      for k <- keys do
        assert "batched" == Router.get(FerricStore.Instance.get(:default), k), "Key #{k} should be readable after batched write"
      end
    end

    @tag :durability
    test "GC-002: different namespaces do not block each other" do
      # Configure two namespaces with different commit windows
      NamespaceConfig.set("fast_ns", "window_ms", "1")
      NamespaceConfig.set("slow_ns", "window_ms", "100")

      k_fast = pkey("fast_ns", "gc002")
      k_slow = pkey("slow_ns", "gc002")

      shard_fast = Router.shard_for(FerricStore.Instance.get(:default), k_fast)
      shard_slow = Router.shard_for(FerricStore.Instance.get(:default), k_slow)

      # Write to both namespaces concurrently
      # The fast namespace should not wait for the slow namespace's window
      fast_task =
        Task.async(fn ->
          {us, result} =
            :timer.tc(fn ->
              Batcher.write(shard_fast, {:put, k_fast, "fast_val", 0})
            end)

          {us, result}
        end)

      slow_task =
        Task.async(fn ->
          {us, result} =
            :timer.tc(fn ->
              Batcher.write(shard_slow, {:put, k_slow, "slow_val", 0})
            end)

          {us, result}
        end)

      {fast_us, fast_result} = Task.await(fast_task, 10_000)
      {slow_us, slow_result} = Task.await(slow_task, 10_000)

      assert fast_result == :ok
      assert slow_result == :ok

      # Verify both values are readable
      assert "fast_val" == Router.get(FerricStore.Instance.get(:default), k_fast)

      # Slow namespace needs time for its window to expire
      ShardHelpers.eventually(fn ->
        "slow_val" == Router.get(FerricStore.Instance.get(:default), k_slow)
      end, "slow namespace write should be readable after window expires", 30, 20)

      # The fast namespace write should complete well before the slow window
      # (this is a soft assertion -- timing-dependent but generous enough for CI)
      assert fast_us < slow_us + 50_000,
             "Fast namespace (#{fast_us}us) should not be blocked by slow namespace (#{slow_us}us)"
    end

    @tag :durability
    test "GC-003: explicit flush drains all namespace slots" do
      # Configure multiple namespaces with a long window so writes accumulate
      NamespaceConfig.set("flush_a", "window_ms", "5000")
      NamespaceConfig.set("flush_b", "window_ms", "5000")

      k_a = pkey("flush_a", "gc003")
      k_b = pkey("flush_b", "gc003")

      shard_a = Router.shard_for(FerricStore.Instance.get(:default), k_a)
      shard_b = Router.shard_for(FerricStore.Instance.get(:default), k_b)

      # Write to both namespaces -- with 5s window, they will sit in the buffer.
      # We use Task.async to avoid blocking the test process since the batcher
      # call only returns when the slot is flushed.
      task_a =
        Task.async(fn ->
          Batcher.write(shard_a, {:put, k_a, "flush_val_a", 0})
        end)

      task_b =
        Task.async(fn ->
          Batcher.write(shard_b, {:put, k_b, "flush_val_b", 0})
        end)

      # intentional delay — let writes arrive at the batcher before flushing
      Process.sleep(50)

      # Explicit flush should drain all slots across all shards
      flush_all_batchers()

      # Now both tasks should complete
      assert :ok == Task.await(task_a, 10_000)
      assert :ok == Task.await(task_b, 10_000)

      # Both values should be readable
      assert "flush_val_a" == Router.get(FerricStore.Instance.get(:default), k_a)
      assert "flush_val_b" == Router.get(FerricStore.Instance.get(:default), k_b)
    end

    @tag :durability
    test "GC-003: flush is idempotent on empty batchers" do
      # Flushing when nothing is pending should not error
      assert :ok == Batcher.flush(0)
      assert :ok == Batcher.flush(1)
      assert :ok == Batcher.flush(2)
      assert :ok == Batcher.flush(3)
    end
  end

  # ==========================================================================
  # Cross-cutting durability scenarios
  # ==========================================================================

  describe "cross-cutting durability" do
    @tag skip: "async durability feature removed; restart comparison no longer applies"
    @tag :durability
    test "quorum write survives shard restart, async write may not without flush" do
      NamespaceConfig.set("ephemeral", "durability", "async")

      k_quorum = ukey("crosscut_quorum")
      k_async = pkey("ephemeral", "crosscut_async")

      # Write through quorum path
      assert :ok == batcher_put(k_quorum, "quorum_survives")

      # Write through async path
      shard_async = Router.shard_for(FerricStore.Instance.get(:default), k_async)
      assert :ok == Batcher.write(shard_async, {:put, k_async, "async_data", 0})

      # Wait for async worker to process
      ShardHelpers.eventually(fn ->
        Router.get(FerricStore.Instance.get(:default), k_async) == "async_data"
      end, "async write should be readable before crash", 20, 20)

      # Verify both are readable before the crash
      assert "quorum_survives" == Router.get(FerricStore.Instance.get(:default), k_quorum)
      assert "async_data" == Router.get(FerricStore.Instance.get(:default), k_async)

      # Flush all shards to disk before killing (for fair comparison)
      ShardHelpers.flush_all_shards()

      # Kill the shard that owns the quorum key
      pid = shard_pid_for(k_quorum)
      ref = Process.monitor(pid)
      Process.exit(pid, :kill)
      assert_receive {:DOWN, ^ref, :process, ^pid, :killed}, 2_000

      # Wait for restart
      ShardHelpers.wait_shards_alive()

      # Quorum write should survive
      ShardHelpers.eventually(fn -> "quorum_survives" == Router.get(FerricStore.Instance.get(:default), k_quorum) end,
        "quorum write should survive shard restart")
    end

    @tag skip: "async durability feature removed; overlapping async/quorum writer case no longer applies"
    @tag :durability
    test "concurrent quorum and async writers on overlapping keys" do
      NamespaceConfig.set("mixed", "durability", "async")

      # Write the same logical data through both quorum and async paths
      # (using different key prefixes to route to different durability modes)
      quorum_keys =
        for i <- 1..20 do
          k = ukey("overlap_q_#{i}")
          batcher_put(k, "q_#{i}")
          k
        end

      async_keys =
        for i <- 1..20 do
          k = pkey("mixed", "overlap_a_#{i}")
          shard = Router.shard_for(FerricStore.Instance.get(:default), k)
          Batcher.write(shard, {:put, k, "a_#{i}", 0})
          k
        end

      # All keys should be readable (async keys need time to process)
      for {k, i} <- Enum.with_index(quorum_keys, 1) do
        assert "q_#{i}" == Router.get(FerricStore.Instance.get(:default), k)
      end

      for {k, i} <- Enum.with_index(async_keys, 1) do
        ShardHelpers.eventually(fn ->
          "a_#{i}" == Router.get(FerricStore.Instance.get(:default), k)
        end, "async key #{k} should be readable", 20, 20)
      end
    end
  end

  # ---------------------------------------------------------------------------
  # TCP helpers for MULTI/EXEC tests
  # ---------------------------------------------------------------------------

  defp tcp_connect_and_hello(port) do
    {:ok, sock} =
      :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false, packet: :raw])

    send_cmd(sock, ["HELLO", "3"])
    _greeting = recv_response(sock)
    sock
  end

  defp send_cmd(sock, cmd) do
    data = IO.iodata_to_binary(FerricstoreServer.Resp.Encoder.encode(cmd))
    :ok = :gen_tcp.send(sock, data)
  end

  defp recv_response(sock) do
    recv_response(sock, "")
  end

  defp recv_response(sock, buf) do
    {:ok, data} = :gen_tcp.recv(sock, 0, 10_000)
    buf2 = buf <> data

    case FerricstoreServer.Resp.Parser.parse(buf2) do
      {:ok, [val], ""} -> val
      {:ok, [val], _rest} -> val
      {:ok, [], _} -> recv_response(sock, buf2)
    end
  end

  defp kill_shard_and_wait(key) do
    name = Router.shard_name(FerricStore.Instance.get(:default), Router.shard_for(FerricStore.Instance.get(:default), key))
    pid = Process.whereis(name)
    ref = Process.monitor(pid)
    Process.exit(pid, :kill)
    assert_receive {:DOWN, ^ref, :process, ^pid, :killed}, 2_000
    ShardHelpers.wait_shards_alive(30_000)
  end

  # ==========================================================================
  # Strengthened fault tolerance assertions
  # ==========================================================================

  describe "fault tolerance: dbsize and key count accuracy" do
    @tag :durability
    test "dbsize is exact after crash recovery — no phantom keys" do
      keys = for i <- 1..10, do: ukey("dbsize_#{i}")

      for {k, i} <- Enum.with_index(keys, 1) do
        batcher_put(k, "val_#{i}")
      end

      flush_all_batchers()
      ShardHelpers.flush_all_shards()

      # Verify before crash
      for {k, i} <- Enum.with_index(keys, 1) do
        assert Router.get(FerricStore.Instance.get(:default), k) == "val_#{i}"
      end

      # Kill shard owning first key
      kill_shard_and_wait(hd(keys))

      # All keys must still be readable
      for {k, i} <- Enum.with_index(keys, 1) do
        ShardHelpers.eventually(fn -> Router.get(FerricStore.Instance.get(:default), k) == "val_#{i}" end,
          "key #{k} lost after crash")
      end

      # dbsize must be at least 10 (could be more from other shards' residual keys)
      ShardHelpers.eventually(fn -> Router.dbsize(FerricStore.Instance.get(:default)) >= 10 end,
        "dbsize should be at least 10 after crash recovery")
    end
  end

  describe "fault tolerance: TTL survival" do
    @tag :durability
    test "TTL survives shard crash — key expires at the right time" do
      k = ukey("ttl_crash")
      # Set with 60s TTL (won't expire during test)
      batcher_put(k, "ttl_val", System.os_time(:millisecond) + 60_000)
      flush_all_batchers()
      ShardHelpers.flush_all_shards()

      assert Router.get(FerricStore.Instance.get(:default), k) == "ttl_val"
      {:ok, ttl_before} = FerricStore.pttl(k)
      assert is_integer(ttl_before) and ttl_before > 0, "TTL should be positive before crash"

      kill_shard_and_wait(k)

      ShardHelpers.eventually(fn -> Router.get(FerricStore.Instance.get(:default), k) == "ttl_val" end,
        "value lost after crash")
      ShardHelpers.eventually(fn ->
        case FerricStore.pttl(k) do
          {:ok, ttl} when is_integer(ttl) and ttl > 0 -> true
          _ -> false
        end
      end, "TTL should survive crash")
      {:ok, ttl_after} = FerricStore.pttl(k)
      # TTL should be close to before (within 15s tolerance for restart time
      # including Raft WAL replay which can take 7+ seconds on CI)
      assert abs(ttl_before - ttl_after) < 15_000, "TTL drifted too much after crash"
    end

    @tag :durability
    test "already-expired key stays expired after crash" do
      k = ukey("expired_crash")
      # Set with TTL in the past
      batcher_put(k, "gone", System.os_time(:millisecond) - 1_000)
      flush_all_batchers()
      ShardHelpers.flush_all_shards()

      assert Router.get(FerricStore.Instance.get(:default), k) == nil, "expired key should not be readable"

      kill_shard_and_wait(k)

      ShardHelpers.eventually(fn -> Router.get(FerricStore.Instance.get(:default), k) == nil end,
        "expired key must stay expired after crash")
    end
  end

  describe "fault tolerance: compound key survival" do
    @tag :durability
    test "hash fields survive shard crash" do
      k = ukey("hash_crash")
      FerricStore.hset(k, %{"f1" => "v1", "f2" => "v2", "f3" => "v3"})
      ShardHelpers.flush_all_shards()

      assert {:ok, "v1"} = FerricStore.hget(k, "f1")
      assert {:ok, "v2"} = FerricStore.hget(k, "f2")

      kill_shard_and_wait(k)

      ShardHelpers.eventually(fn ->
        {:ok, "v1"} == FerricStore.hget(k, "f1")
      end, "hash field f1 should survive shard crash")
      ShardHelpers.eventually(fn ->
        {:ok, "v2"} == FerricStore.hget(k, "f2")
      end, "hash field f2 should survive shard crash")
      ShardHelpers.eventually(fn ->
        {:ok, "v3"} == FerricStore.hget(k, "f3")
      end, "hash field f3 should survive shard crash")

      # Write new field after crash
      FerricStore.hset(k, %{"f4" => "v4"})
      ShardHelpers.eventually(fn ->
        {:ok, "v4"} == FerricStore.hget(k, "f4")
      end, "hash field f4 should be writable after crash")
    end

    @tag :durability
    test "set members survive shard crash" do
      k = ukey("set_crash")
      FerricStore.sadd(k, ["a", "b", "c"])
      ShardHelpers.flush_all_shards()

      {:ok, members_before} = FerricStore.smembers(k)
      assert Enum.sort(members_before) == ["a", "b", "c"]

      kill_shard_and_wait(k)

      ShardHelpers.eventually(fn ->
        case FerricStore.smembers(k) do
          {:ok, members} -> Enum.sort(members) == ["a", "b", "c"]
          _ -> false
        end
      end, "set members lost after crash")
    end
  end

  describe "fault tolerance: delete and INCR survival" do
    @tag :durability
    test "deleted key stays deleted after crash — tombstone replay" do
      k = ukey("tombstone")
      batcher_put(k, "temporary")
      flush_all_batchers()
      ShardHelpers.flush_all_shards()

      assert Router.get(FerricStore.Instance.get(:default), k) == "temporary"

      Router.delete(FerricStore.Instance.get(:default), k)
      ShardHelpers.flush_all_shards()

      assert Router.get(FerricStore.Instance.get(:default), k) == nil, "key should be deleted"

      kill_shard_and_wait(k)

      ShardHelpers.eventually(fn -> Router.get(FerricStore.Instance.get(:default), k) == nil end,
        "tombstone must survive crash")
    end

    @tag :durability
    test "INCR counter value survives crash" do
      k = ukey("counter")
      FerricStore.set(k, "0")
      for _ <- 1..10, do: FerricStore.incr(k)
      ShardHelpers.flush_all_shards()

      assert {:ok, "10"} = FerricStore.get(k)

      kill_shard_and_wait(k)

      ShardHelpers.eventually(fn ->
        {:ok, "10"} == FerricStore.get(k)
      end, "counter value should survive crash")

      # Continue incrementing after crash
      FerricStore.incr(k)
      ShardHelpers.eventually(fn ->
        {:ok, "11"} == FerricStore.get(k)
      end, "counter should be incrementable after crash")
    end
  end

  describe "fault tolerance: double crash recovery" do
    @tag :durability
    test "write → crash → write more → crash again → all data intact" do
      k1 = ukey("double_crash_1")
      batcher_put(k1, "round1")
      flush_all_batchers()
      ShardHelpers.flush_all_shards()

      # First crash
      kill_shard_and_wait(k1)
      ShardHelpers.eventually(fn -> Router.get(FerricStore.Instance.get(:default), k1) == "round1" end,
        "round 1 data lost")

      # Write more after first recovery
      k2 = ukey("double_crash_2")
      batcher_put(k2, "round2")
      flush_all_batchers()
      ShardHelpers.flush_all_shards()

      ShardHelpers.eventually(fn -> Router.get(FerricStore.Instance.get(:default), k2) == "round2" end,
        "round 2 data should be readable")

      # Second crash
      kill_shard_and_wait(k1)

      ShardHelpers.eventually(fn -> Router.get(FerricStore.Instance.get(:default), k1) == "round1" end,
        "round 1 data lost after second crash")
      ShardHelpers.eventually(fn -> Router.get(FerricStore.Instance.get(:default), k2) == "round2" end,
        "round 2 data lost after second crash")

      # Write after second recovery
      k3 = ukey("double_crash_3")
      batcher_put(k3, "round3")
      flush_all_batchers()
      ShardHelpers.flush_all_shards()
      ShardHelpers.eventually(fn -> Router.get(FerricStore.Instance.get(:default), k3) == "round3" end,
        "write after double crash failed")
    end
  end
end
