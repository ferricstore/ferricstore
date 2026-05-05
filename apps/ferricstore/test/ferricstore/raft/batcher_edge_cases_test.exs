defmodule Ferricstore.Raft.BatcherEdgeCasesTest do
  @moduledoc """
  Edge case tests for Batcher changes from the write-throughput-optimization
  branch: write_batch backpressure, timer-0 flush, extract_prefix guards,
  terminate with mixed pending shapes, and DEBUG commands.
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Raft.Batcher
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  setup_all do
    ShardHelpers.wait_shards_alive()
    :ok
  end

  setup do
    on_exit(fn ->
      ShardHelpers.wait_shards_alive()
    end)
  end

  defp ctx, do: FerricStore.Instance.get(:default)
  defp ukey(base), do: "edge_#{base}_#{:rand.uniform(9_999_999)}"

  defp same_shard_key(shard_index, base) do
    Stream.repeatedly(fn -> ukey(base) end)
    |> Enum.find(&(Router.shard_for(ctx(), &1) == shard_index))
  end

  defp fill_pending(idx, count) do
    for i <- 1..count do
      corr = make_ref()
      batch = [{:put, "edge:pending:#{idx}:#{i}", "v", 0}]
      Batcher.__inject_async_pending__(idx, corr, batch, 0)
    end
  end

  # ---------------------------------------------------------------------------
  # extract_prefix edge cases
  # ---------------------------------------------------------------------------

  describe "extract_prefix/1" do
    test "non-binary key returns _root" do
      assert Batcher.extract_prefix({:put, 42, "v", 0}) == "_root"
    end

    test "nil key returns _root" do
      assert Batcher.extract_prefix({:put, nil, "v", 0}) == "_root"
    end

    test "atom key returns _root" do
      assert Batcher.extract_prefix({:delete, :some_atom}) == "_root"
    end

    test "empty binary key returns _root" do
      assert Batcher.extract_prefix({:put, "", "v", 0}) == "_root"
    end

    test "key with only colon returns empty prefix" do
      assert Batcher.extract_prefix({:put, ":value", "v", 0}) == ""
    end

    test "key with multiple colons takes first segment" do
      assert Batcher.extract_prefix({:put, "a:b:c:d", "v", 0}) == "a"
    end
  end

  # ---------------------------------------------------------------------------
  # write_batch backpressure (@max_pending)
  # ---------------------------------------------------------------------------

  describe "write_batch backpressure" do
    test "write_batch succeeds under normal conditions" do
      k = ukey("batch_ok")
      shard_index = Router.shard_for(ctx(), k)

      task =
        Task.async(fn ->
          GenServer.call(
            Batcher.batcher_name(shard_index),
            {:write, {:put, k, "batch_val", 0}},
            10_000
          )
        end)

      assert Task.await(task, 10_000) == :ok
      assert Router.get(ctx(), k) == "batch_val"
    end

    test "write_batch with multiple commands in a single batch" do
      keys = for i <- 1..5, do: ukey("multi_#{i}")
      shard_index = Router.shard_for(ctx(), hd(keys))
      same_shard_keys = Enum.filter(keys, fn k -> Router.shard_for(ctx(), k) == shard_index end)

      cmds = Enum.map(same_shard_keys, fn k -> {:put, k, "multi_val", 0} end)

      ref = make_ref()
      from = {self(), ref}
      Batcher.write_batch(shard_index, cmds, from)

      assert_receive {^ref, {:ok, results}}, 10_000
      assert length(results) == length(cmds)

      for k <- same_shard_keys do
        assert Router.get(ctx(), k) == "multi_val"
      end
    end

    test "write_batch with no commands replies with an empty result and keeps batcher alive" do
      shard_index = 0
      batcher_pid = Process.whereis(Batcher.batcher_name(shard_index))
      assert is_pid(batcher_pid)
      monitor_ref = Process.monitor(batcher_pid)

      try do
        ref = make_ref()
        :ok = Batcher.write_batch(shard_index, [], {self(), ref})

        assert_receive {^ref, {:ok, []}}, 1_000
        refute_receive {:DOWN, ^monitor_ref, :process, ^batcher_pid, _reason}, 100
        assert Process.alive?(batcher_pid)
      after
        Process.demonitor(monitor_ref, [:flush])
      end
    end

    test "immediate Ra pipeline failure replies and does not leak pending" do
      k = ukey("pipeline_fail")
      shard_index = Router.shard_for(ctx(), k)
      batcher_name = Batcher.batcher_name(shard_index)

      original_shard_id = :sys.get_state(batcher_name).shard_id

      :sys.replace_state(batcher_name, fn state ->
        %{state | shard_id: {:missing_ferricstore_ra_server, node()}}
      end)

      on_exit(fn ->
        :sys.replace_state(batcher_name, fn state -> %{state | shard_id: original_shard_id} end)
        Batcher.reset_pending(shard_index)
      end)

      ref = make_ref()
      Batcher.write_batch(shard_index, [{:put, k, "lost", 0}], {self(), ref})

      assert_receive {^ref, {:error, _reason}}, 500

      state = :sys.get_state(batcher_name)
      assert map_size(state.pending) == 0
    end
  end

  # ---------------------------------------------------------------------------
  # write_batch timer-0 flush accumulation
  # ---------------------------------------------------------------------------

  describe "write_batch timer-0 flush" do
    test "concurrent batches accumulate before timer-0 fires" do
      keys = for i <- 1..20, do: ukey("accum_#{i}")
      shard_index = Router.shard_for(ctx(), hd(keys))
      same_shard_keys = Enum.filter(keys, fn k -> Router.shard_for(ctx(), k) == shard_index end)

      tasks =
        Enum.map(same_shard_keys, fn k ->
          Task.async(fn ->
            ref = make_ref()
            from = {self(), ref}
            Batcher.write_batch(shard_index, [{:put, k, "accum", 0}], from)

            receive do
              {^ref, result} -> result
            after
              10_000 -> {:error, :timeout}
            end
          end)
        end)

      results = Task.await_many(tasks, 15_000)
      # credo:disable-for-next-line Credo.Check.Readability.Semicolons
      assert Enum.all?(results, fn
               {:ok, _} -> true
               _ -> false
             end)

      for k <- same_shard_keys do
        assert Router.get(ctx(), k) == "accum"
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Terminate with mixed pending entry shapes
  # ---------------------------------------------------------------------------

  describe "terminate replies to all pending callers" do
    test "batcher restarts cleanly after kill" do
      k = ukey("terminate_restart")
      shard_index = Router.shard_for(ctx(), k)
      batcher_name = Batcher.batcher_name(shard_index)

      pid = Process.whereis(batcher_name)
      assert is_pid(pid)

      ref = Process.monitor(pid)
      Process.exit(pid, :kill)

      receive do
        {:DOWN, ^ref, :process, ^pid, :killed} -> :ok
      after
        2_000 -> flunk("batcher didn't die")
      end

      ShardHelpers.wait_shards_alive()

      new_pid = Process.whereis(batcher_name)
      assert is_pid(new_pid)
      assert new_pid != pid
      assert Process.alive?(new_pid)

      :ok = Batcher.write(shard_index, {:put, k, "after_restart", 0})
      assert Router.get(ctx(), k) == "after_restart"
    end

    test "terminate replies to callers waiting for local apply" do
      shard_index = 0
      batcher_name = Batcher.batcher_name(shard_index)
      pid = Process.whereis(batcher_name)
      assert is_pid(pid)

      %{last_local_applied: last_local_applied} = :sys.get_state(batcher_name)
      ra_index = last_local_applied + 1_000
      corr = make_ref()
      reply_ref = make_ref()

      :ok =
        Batcher.__inject_quorum_pending_at__(
          shard_index,
          corr,
          [{self(), reply_ref}],
          :single,
          System.monotonic_time()
        )

      send(batcher_name, {:ra_event, :leader, {:applied, [{corr, {:applied_at, ra_index, :ok}}]}})
      refute_receive {^reply_ref, _}, 50

      monitor_ref = Process.monitor(pid)
      :ok = GenServer.stop(batcher_name, :shutdown, 2_000)

      assert_receive {^reply_ref, {:error, :batcher_terminated}}, 1_000
      assert_receive {:DOWN, ^monitor_ref, :process, ^pid, :shutdown}, 1_000

      ShardHelpers.wait_shards_alive()
      assert Process.whereis(batcher_name) != pid
    end
  end

  # ---------------------------------------------------------------------------
  # Flush with mixed pending (async + quorum)
  # ---------------------------------------------------------------------------

  describe "flush with mixed pending" do
    test "flush waits for both async and quorum in-flight commands" do
      k = ukey("flush_mixed")
      shard_index = Router.shard_for(ctx(), k)

      :ok = Batcher.write(shard_index, {:put, k, "flushed", 0})
      :ok = Batcher.flush(shard_index)

      assert Router.get(ctx(), k) == "flushed"
    end

    test "flush on empty batcher returns immediately" do
      assert :ok == Batcher.flush(0)
      assert :ok == Batcher.flush(1)
    end
  end

  describe "forwarded quorum replies" do
    test "single forwarded caller receives applied index wrapper" do
      k = ukey("forwarded_single")
      shard_index = Router.shard_for(ctx(), k)
      origin_node = :follower@nohost
      ref = make_ref()

      :ok =
        Batcher.write_async_quorum_forwarded(
          shard_index,
          {:put, k, "forwarded", 0},
          {self(), ref},
          origin_node
        )

      assert_receive {^ref, {:remote_applied_at, ra_index, :ok}}, 10_000
      assert is_integer(ra_index) and ra_index > 0
    end

    test "batch forwarded caller receives applied index wrappers per result" do
      k1 = ukey("forwarded_batch_a")
      shard_index = Router.shard_for(ctx(), k1)
      k2 = same_shard_key(shard_index, "forwarded_batch_b")
      origin_node = :follower@nohost
      ref = make_ref()

      :ok =
        Batcher.write_batch_forwarded(
          shard_index,
          [{:put, k1, "v1", 0}, {:put, k2, "v2", 0}],
          {self(), ref},
          origin_node
        )

      assert_receive {^ref,
                      {:ok,
                       [
                         {:remote_applied_at, ra_index, :ok},
                         {:remote_applied_at, ra_index, :ok}
                       ]}},
                     10_000

      assert is_integer(ra_index) and ra_index > 0
    end
  end

  describe "single-write backpressure" do
    setup do
      idx = 0
      fill_pending(idx, 64)

      on_exit(fn -> Batcher.reset_pending(idx) end)

      {:ok, idx: idx}
    end

    test "Batcher.write rejects when pending is full", %{idx: idx} do
      key = same_shard_key(idx, "single_overload")

      assert {:error, :overloaded} = Batcher.write(idx, {:put, key, "v", 0})
    end

    test "write_async_quorum replies overloaded when pending is full", %{idx: idx} do
      key = same_shard_key(idx, "forced_overload")
      ref = make_ref()

      :ok = Batcher.write_async_quorum(idx, {:put, key, "v", 0}, {self(), ref})

      assert_receive {^ref, {:error, :overloaded}}, 1_000
    end

    test "forwarded write_batch replies overloaded without crashing when pending is full", %{
      idx: idx
    } do
      key = same_shard_key(idx, "forwarded_batch_overload")
      ref = make_ref()
      batcher_pid = Process.whereis(Batcher.batcher_name(idx))
      monitor_ref = Process.monitor(batcher_pid)

      try do
        :ok =
          Batcher.write_batch_forwarded(
            idx,
            [{:put, key, "v", 0}],
            {self(), ref},
            :follower@nohost
          )

        assert_receive {^ref, {:error, :overloaded}}, 1_000
        refute_receive {:DOWN, ^monitor_ref, :process, ^batcher_pid, _reason}, 100
        assert Process.alive?(batcher_pid)
      after
        Process.demonitor(monitor_ref, [:flush])
      end
    end

    test "async_submit emits dropped telemetry when pending is full", %{idx: idx} do
      handler = {:edge_backpressure, self(), make_ref()}

      :ok =
        :telemetry.attach(
          handler,
          [:ferricstore, :batcher, :async_dropped],
          fn event, measurements, metadata, pid ->
            send(pid, {:telemetry, event, measurements, metadata})
          end,
          self()
        )

      try do
        key = same_shard_key(idx, "async_overload")

        :ok = Batcher.async_submit(idx, {:put, key, "v", 0})

        assert_receive {:telemetry, [:ferricstore, :batcher, :async_dropped], %{batch_size: 1},
                        %{reason: :overloaded}},
                       1_000
      after
        :telemetry.detach(handler)
      end
    end
  end

  # ---------------------------------------------------------------------------
  # reset_pending clears state and unblocks callers
  # ---------------------------------------------------------------------------

  describe "reset_pending" do
    test "reset_pending on clean batcher returns :ok" do
      assert :ok == Batcher.reset_pending(0)
    end

    test "reset_pending clears injected async entries" do
      idx = 0
      corr = make_ref()
      batch = [{:put, "edge:fake_reset", "v", 0}]
      Batcher.__inject_async_pending__(idx, corr, batch, 0)

      assert Batcher.__has_pending__(idx, corr)

      :ok = Batcher.reset_pending(idx)

      refute Batcher.__has_pending__(idx, corr)
    end
  end

  # ---------------------------------------------------------------------------
  # Sweep clears stale async entries (quorum sweep tested via async_retry_test)
  # ---------------------------------------------------------------------------

  describe "sweep clears stale entries" do
    test "sweep on clean batcher is a no-op" do
      :ok = Batcher.__sweep_pending_now__(0)
      :ok = Batcher.__sweep_pending_now__(1)
    end

    test "sweep removes old async entry but keeps fresh one" do
      idx = 0
      old_corr = make_ref()
      fresh_corr = make_ref()
      batch = [{:put, "edge:sweep_test", "v", 0}]

      old_mono = System.monotonic_time() - System.convert_time_unit(60_000, :millisecond, :native)
      Batcher.__inject_async_pending_at__(idx, old_corr, batch, 0, old_mono)
      Batcher.__inject_async_pending__(idx, fresh_corr, batch, 0)

      assert Batcher.__has_pending__(idx, old_corr)
      assert Batcher.__has_pending__(idx, fresh_corr)

      Batcher.__sweep_pending_now__(idx)

      refute Batcher.__has_pending__(idx, old_corr)
      assert Batcher.__has_pending__(idx, fresh_corr)

      Batcher.reset_pending(idx)
    end
  end

  # ---------------------------------------------------------------------------
  # quorum submit through Router
  # ---------------------------------------------------------------------------

  describe "router submit" do
    setup do
      Ferricstore.NamespaceConfig.reset("edgeasync")

      on_exit(fn ->
        Ferricstore.NamespaceConfig.reset("edgeasync")
      end)
    end

    test "quorum writes are readable after flush" do
      k = "edgeasync:submit_#{:rand.uniform(9_999_999)}"
      shard_index = Router.shard_for(ctx(), k)

      :ok = Router.put(ctx(), k, "async_val", 0)

      Batcher.flush(shard_index)

      assert Router.get(ctx(), k) == "async_val"
    end

    test "multiple quorum commands batch together" do
      keys = for i <- 1..10, do: "edgeasync:batch_#{i}_#{:rand.uniform(9_999_999)}"

      for k <- keys do
        :ok = Router.put(ctx(), k, "batched_async", 0)
      end

      for i <- 0..3 do
        Batcher.flush(i)
      end

      for k <- keys do
        assert Router.get(ctx(), k) == "batched_async"
      end
    end
  end

  # ---------------------------------------------------------------------------
  # write_async_quorum (RMW ops forced through consensus)
  # ---------------------------------------------------------------------------

  describe "write_async_quorum" do
    setup do
      Ferricstore.NamespaceConfig.reset("edgermw")

      on_exit(fn ->
        Ferricstore.NamespaceConfig.reset("edgermw")
      end)
    end

    test "write_async_quorum submits through quorum slot" do
      k = "edgermw:forced_#{:rand.uniform(9_999_999)}"
      shard_index = Router.shard_for(ctx(), k)

      task =
        Task.async(fn ->
          ref = make_ref()
          from = {self(), ref}
          Batcher.write_async_quorum(shard_index, {:put, k, "quorum_forced", 0}, from)

          receive do
            {^ref, result} -> result
          after
            10_000 -> {:error, :timeout}
          end
        end)

      result = Task.await(task, 10_000)
      assert result == :ok

      assert Router.get(ctx(), k) == "quorum_forced"
    end
  end

  # ---------------------------------------------------------------------------
  # DEBUG BATCHER-STATS command
  # ---------------------------------------------------------------------------

  describe "DEBUG BATCHER-STATS" do
    test "returns stats for all shards in single line" do
      result = Ferricstore.Commands.Server.handle("DEBUG", ["BATCHER-STATS"], nil)

      assert {:simple, stats_line} = result
      assert is_binary(stats_line)

      assert String.contains?(stats_line, "B0:")
      assert String.contains?(stats_line, "WAL:")
      assert String.contains?(stats_line, "R0:")
      assert String.contains?(stats_line, "mq=")
      assert String.contains?(stats_line, " | ")

      refute String.contains?(stats_line, "\n")
    end
  end

  # ---------------------------------------------------------------------------
  # DEBUG SET-DURABILITY command
  # ---------------------------------------------------------------------------

  describe "DEBUG SET-DURABILITY" do
    test "rejects old durability modes" do
      result = Ferricstore.Commands.Server.handle("DEBUG", ["SET-DURABILITY", "async"], nil)
      assert {:error, msg} = result
      assert String.contains?(msg, "durability mode has been removed")

      result = Ferricstore.Commands.Server.handle("DEBUG", ["SET-DURABILITY", "quorum"], nil)
      assert {:error, msg} = result
      assert String.contains?(msg, "durability mode has been removed")
    end

    test "rejects invalid durability mode via catch-all" do
      result = Ferricstore.Commands.Server.handle("DEBUG", ["SET-DURABILITY", "invalid"], nil)
      assert {:error, _msg} = result
    end
  end

  # ---------------------------------------------------------------------------
  # ns_config_changed invalidates cache and flushes slots
  # ---------------------------------------------------------------------------

  describe "ns_config_changed" do
    test "namespace config change flushes pending writes" do
      k = ukey("ns_change")
      shard_index = Router.shard_for(ctx(), k)

      :ok = Batcher.write(shard_index, {:put, k, "before_change", 0})

      batcher_pid = Process.whereis(Batcher.batcher_name(shard_index))
      send(batcher_pid, :ns_config_changed)

      Process.sleep(50)

      assert Router.get(ctx(), k) == "before_change"
    end
  end

  # ---------------------------------------------------------------------------
  # Catch-all handle_info doesn't crash
  # ---------------------------------------------------------------------------

  describe "unexpected messages" do
    test "batcher ignores unknown messages without crashing" do
      batcher_pid = Process.whereis(Batcher.batcher_name(0))
      ref = Process.monitor(batcher_pid)

      send(batcher_pid, :totally_unexpected_message)
      send(batcher_pid, {:random, :tuple, 42})
      send(batcher_pid, "string message")

      Process.sleep(50)

      assert Process.alive?(batcher_pid)

      refute_received {:DOWN, ^ref, :process, ^batcher_pid, _}

      Process.demonitor(ref, [:flush])
    end
  end
end
