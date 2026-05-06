defmodule FerricstoreServer.Integration.RaCrashResilienceTest do
  @moduledoc """
  Tests that verify data integrity when ra processes crash.

  Scenarios:
  1. SET succeeds, ra crashes, GET must still return the value
  2. SET succeeds, ra crashes, another SET on same key must work after recovery
  3. Multiple keys written, ra crashes mid-stream, reads must be consistent
  4. Ra crash during write — client must get error, not silent success
  5. Concurrent readers during ra crash must get correct data or errors, never wrong data
  """

  use ExUnit.Case, async: false
  @moduletag timeout: 120_000

  alias Ferricstore.Store.Router
  alias Ferricstore.Raft.Cluster
  alias Ferricstore.Test.ShardHelpers

  setup do
    Ferricstore.MemoryGuard.reset_pressure_flags()
    ShardHelpers.wait_shards_alive(30_000)
    ShardHelpers.flush_all_keys()

    on_exit(fn ->
      Ferricstore.MemoryGuard.reset_pressure_flags()
      ShardHelpers.wait_shards_alive(30_000)
    end)

    ctx = FerricStore.Instance.get(:default)
    {:ok, ctx: ctx}
  end

  describe "read-after-write survives ra crash" do
    test "GET returns value after SET when ra crashes between them", %{ctx: ctx} do
      # Find a key and its shard
      key = "resilience_raw_1"
      idx = Router.shard_for(ctx, key)

      # SET the key through Raft — must succeed
      assert :ok = Router.put(ctx, key, "durable_value", 0)

      # Verify it's readable
      assert "durable_value" == Router.get(ctx, key)

      # Now kill the ra process for this shard
      shard_id = Cluster.shard_server_id(idx)
      {shard_name, _node} = shard_id
      ra_pid = Process.whereis(shard_name)
      assert ra_pid != nil, "ra process for shard #{idx} not found"

      ref = Process.monitor(ra_pid)
      Process.exit(ra_pid, :kill)
      assert_receive {:DOWN, ^ref, :process, ^ra_pid, :killed}, 5000

      # GET must still return the value — ETS is owned by Shard GenServer, not ra
      assert "durable_value" == Router.get(ctx, key),
             "GET returned wrong value after ra crash — data integrity violation"
    end

    test "multiple keys survive ra crash", %{ctx: ctx} do
      # Write 100 keys across shards
      keys =
        for i <- 1..100 do
          key = "resilience_multi_#{i}"
          Router.put(ctx, key, "value_#{i}", 0)
          key
        end

      # Verify all readable
      for {key, i} <- Enum.with_index(keys, 1) do
        assert "value_#{i}" == Router.get(ctx, key)
      end

      # Kill ra on ALL shards
      for shard_idx <- 0..(ctx.shard_count - 1) do
        shard_id = Cluster.shard_server_id(shard_idx)
        {shard_name, _node} = shard_id

        case Process.whereis(shard_name) do
          nil -> :ok
          pid -> Process.exit(pid, :kill)
        end
      end

      Process.sleep(100)

      # ALL reads must still work — ETS is intact
      for {key, i} <- Enum.with_index(keys, 1) do
        value = Router.get(ctx, key)

        assert value == "value_#{i}",
               "Key #{key}: expected value_#{i}, got #{inspect(value)} — data loss after ra crash"
      end
    end

    test "GET never returns nil for a key that was successfully SET", %{ctx: ctx} do
      key = "resilience_never_nil"
      idx = Router.shard_for(ctx, key)

      # SET the key
      assert :ok = Router.put(ctx, key, "must_exist", 0)

      # Start a reader that continuously GETs the key
      test_pid = self()

      reader =
        Task.async(fn ->
          for _ <- 1..200 do
            value = Router.get(ctx, key)

            if value == nil do
              send(test_pid, {:nil_read, System.monotonic_time(:millisecond)})
            end

            Process.sleep(5)
          end
        end)

      # While reader is running, crash ra for this shard
      Process.sleep(50)
      shard_id = Cluster.shard_server_id(idx)
      {shard_name, _node} = shard_id

      case Process.whereis(shard_name) do
        nil -> :ok
        pid -> Process.exit(pid, :kill)
      end

      Task.await(reader, 30_000)

      # Check if any nil reads happened
      nil_reads =
        receive_all_matching(fn
          {:nil_read, _ts} -> true
          _ -> false
        end)

      assert nil_reads == [],
             "Got #{length(nil_reads)} nil reads for a key that exists — " <>
               "this means GET returns wrong data (nil instead of the value) during ra crash"
    end
  end

  describe "write during ra crash" do
    test "SET while ra is dead returns error, not silent success", %{ctx: ctx} do
      key = "resilience_write_dead"
      idx = Router.shard_for(ctx, key)

      # Kill ra for this shard
      shard_id = Cluster.shard_server_id(idx)
      {shard_name, _node} = shard_id
      ra_pid = Process.whereis(shard_name)
      assert ra_pid != nil
      Process.exit(ra_pid, :kill)
      Process.sleep(200)

      # Try to SET — should get an error, NOT :ok
      result =
        try do
          Router.put(ctx, key, "should_fail", 0)
        catch
          :exit, reason -> {:exit, reason}
        end

      case result do
        :ok ->
          # If it returned :ok, the value MUST be readable
          value = Router.get(ctx, key)

          assert value == "should_fail",
                 "SET returned :ok but GET returns #{inspect(value)} — " <>
                   "silent success with data loss"

        {:error, _msg} ->
          # Error is acceptable — client knows the write failed
          assert true

        {:exit, _reason} ->
          # Exit/crash is acceptable — client knows something went wrong
          assert true
      end
    end

    test "writes after ra recovery work correctly", %{ctx: ctx} do
      key = "resilience_post_recovery"
      idx = Router.shard_for(ctx, key)

      # Write before crash
      assert :ok = Router.put(ctx, key, "before_crash", 0)
      assert "before_crash" == Router.get(ctx, key)

      # Kill ra
      shard_id = Cluster.shard_server_id(idx)
      {shard_name, _node} = shard_id

      case Process.whereis(shard_name) do
        nil -> :ok
        pid -> Process.exit(pid, :kill)
      end

      # Wait for ra to recover
      system = Cluster.system_name()

      ShardHelpers.eventually(
        fn ->
          case Process.whereis(shard_name) do
            nil ->
              try do
                :ra.restart_server(system, shard_id)
              catch
                _, _ -> :ok
              end

              flunk("ra not restarted yet")

            pid ->
              assert Process.alive?(pid)
          end
        end,
        "ra restart for shard #{idx}",
        30,
        200
      )

      # Wait for leader election
      ShardHelpers.eventually(
        fn ->
          {:ok, _, _} = :ra.members(shard_id)
        end,
        "leader election for shard #{idx}",
        20,
        200
      )

      # Write after recovery must work
      assert :ok = Router.put(ctx, key, "after_recovery", 0)
      assert "after_recovery" == Router.get(ctx, key)
    end
  end

  describe "concurrent operations during ra crash" do
    test "concurrent readers get correct data or errors during ra crash", %{ctx: ctx} do
      # Pre-populate keys
      keys =
        for i <- 1..20 do
          key = "resilience_conc_#{i}"
          Router.put(ctx, key, "stable_#{i}", 0)
          key
        end

      test_pid = self()

      # Start readers
      readers =
        for {key, i} <- Enum.with_index(keys, 1) do
          Task.async(fn ->
            bad_reads =
              for _ <- 1..100, reduce: [] do
                acc ->
                  value = Router.get(ctx, key)

                  cond do
                    value == "stable_#{i}" -> acc
                    value == nil -> [{key, :nil_read} | acc]
                    true -> [{key, :wrong_value, value} | acc]
                  end
              end

            send(test_pid, {:reader_done, key, bad_reads})
            bad_reads
          end)
        end

      # Crash all ra processes while readers are active
      Process.sleep(20)

      for shard_idx <- 0..(ctx.shard_count - 1) do
        shard_id = Cluster.shard_server_id(shard_idx)
        {shard_name, _node} = shard_id

        case Process.whereis(shard_name) do
          nil -> :ok
          pid -> Process.exit(pid, :kill)
        end
      end

      # Wait for all readers
      results = Task.await_many(readers, 30_000)
      all_bad = List.flatten(results)

      nil_reads = Enum.filter(all_bad, &match?({_, :nil_read}, &1))
      wrong_values = Enum.filter(all_bad, &match?({_, :wrong_value, _}, &1))

      if wrong_values != [] do
        flunk(
          "#{length(wrong_values)} reads returned WRONG data (not nil, not expected value): " <>
            "#{inspect(Enum.take(wrong_values, 5))}"
        )
      end

      if nil_reads != [] do
        flunk(
          "#{length(nil_reads)} reads returned nil for keys that exist — " <>
            "data appears missing during ra crash. Keys: " <>
            inspect(nil_reads |> Enum.map(&elem(&1, 0)) |> Enum.uniq())
        )
      end
    end
  end

  # Drains all matching messages from the mailbox
  defp receive_all_matching(match_fn) do
    receive_all_matching(match_fn, [])
  end

  defp receive_all_matching(match_fn, acc) do
    receive do
      msg ->
        if match_fn.(msg) do
          receive_all_matching(match_fn, [msg | acc])
        else
          receive_all_matching(match_fn, acc)
        end
    after
      0 -> Enum.reverse(acc)
    end
  end
end
