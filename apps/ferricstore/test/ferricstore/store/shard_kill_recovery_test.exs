defmodule Ferricstore.Store.ShardKillRecoveryTest do
  @moduledoc """
  Shard-kill recovery tests.

  Verifies that killing shard GenServer processes does not lose committed data,
  and that the supervisor restarts shards correctly. Covers all data types
  (strings, hashes, lists, sets) and edge cases (rapid kills, concurrent
  writes during kill, ETS keydir rebuild, component kills).
  """

  use ExUnit.Case, async: false
  @moduletag :global_state
  @moduletag :shard_kill
  @moduletag timeout: 120_000

  alias Ferricstore.Store.Router
  alias Ferricstore.Raft.WARaftBackend
  alias Ferricstore.Test.ShardHelpers

  # Generous eventually timeout: 200 attempts * 200ms = 40s.
  # After a shard kill the ra WAL must replay and the Shard init must
  # rebuild the keydir from Bitcask. On CI this can take 10-20s.
  @eventually_attempts 200
  @eventually_interval 200

  setup_all do
    default_ctx = FerricStore.Instance.get(:default)
    active_ctx = WARaftBackend.context!(:ferricstore_waraft_backend)
    identity_fields = [:name, :data_dir_expanded, :shard_count, :keydir_refs]

    assert Map.take(active_ctx, identity_fields) == Map.take(default_ctx, identity_fields)

    ShardHelpers.wait_shards_alive(30_000)
    ShardHelpers.compact_wal()
    :ok
  end

  setup do
    ShardHelpers.wait_shards_alive(30_000)

    on_exit(fn ->
      ShardHelpers.wait_shards_alive(30_000)
    end)
  end

  # Unique key helper to avoid collisions between tests.
  # Every test uses unique keys so we do NOT need flush_all_keys() in setup,
  # which avoids generating thousands of Raft delete commands that bloat the WAL.
  defp ukey(base), do: "skr_#{base}_#{System.unique_integer([:positive])}"

  # Find a unique key that routes to the given shard index.
  defp ukey_for_shard(shard_idx) do
    Enum.find_value(0..200_000, fn i ->
      candidate = "skr_shard#{shard_idx}_#{System.unique_integer([:positive])}_#{i}"

      if Router.shard_for(FerricStore.Instance.get(:default), candidate) == shard_idx,
        do: candidate
    end)
  end

  # Write data through Raft and ensure it is fully committed to disk.
  defp write_and_flush(key, value) do
    Router.put(FerricStore.Instance.get(:default), key, value)
    ShardHelpers.flush_all_shards()
  end

  defp eventually(fun, msg) do
    ShardHelpers.eventually(fun, msg, @eventually_attempts, @eventually_interval)
  end

  # =========================================================================
  # Basic recovery
  # =========================================================================

  describe "basic recovery" do
    test "1. kill shard, string data survives" do
      key = ukey_for_shard(0)
      write_and_flush(key, "string_value")

      ShardHelpers.kill_shard_safely(0)
      ShardHelpers.wait_shards_alive(30_000)

      eventually(
        fn -> Router.get(FerricStore.Instance.get(:default), key) == "string_value" end,
        "string key should survive shard kill"
      )
    end

    test "2. kill shard, hash data survives" do
      key = ukey("hash")
      shard_idx = Router.shard_for(FerricStore.Instance.get(:default), key)

      FerricStore.hset(key, %{"field1" => "val1", "field2" => "val2"})
      ShardHelpers.flush_all_shards()

      ShardHelpers.kill_shard_safely(shard_idx)
      ShardHelpers.wait_shards_alive(30_000)

      eventually(
        fn ->
          {:ok, m} = FerricStore.hgetall(key)
          m == %{"field1" => "val1", "field2" => "val2"}
        end,
        "hash data should survive shard kill"
      )
    end

    test "3. kill shard, list data survives" do
      key = ukey("list")
      shard_idx = Router.shard_for(FerricStore.Instance.get(:default), key)

      {:ok, _} = FerricStore.rpush(key, ["a", "b", "c"])
      ShardHelpers.flush_all_shards()

      ShardHelpers.kill_shard_safely(shard_idx)
      ShardHelpers.wait_shards_alive(30_000)

      eventually(
        fn ->
          {:ok, elems} = FerricStore.lrange(key, 0, -1)
          elems == ["a", "b", "c"]
        end,
        "list data should survive shard kill"
      )
    end

    test "4. kill shard, set data survives" do
      key = ukey("set")
      shard_idx = Router.shard_for(FerricStore.Instance.get(:default), key)

      {:ok, _} = FerricStore.sadd(key, ["x", "y", "z"])
      ShardHelpers.flush_all_shards()

      ShardHelpers.kill_shard_safely(shard_idx)
      ShardHelpers.wait_shards_alive(30_000)

      eventually(
        fn ->
          {:ok, members} = FerricStore.smembers(key)
          Enum.sort(members) == ["x", "y", "z"]
        end,
        "set data should survive shard kill"
      )
    end

    test "5. kill shard twice in a row, data survives both" do
      key = ukey_for_shard(0)
      write_and_flush(key, "survive_twice")

      ShardHelpers.kill_shard_safely(0)
      ShardHelpers.wait_shards_alive(30_000)

      eventually(
        fn -> Router.get(FerricStore.Instance.get(:default), key) == "survive_twice" end,
        "data should survive first kill"
      )

      ShardHelpers.kill_shard_safely(0)
      ShardHelpers.wait_shards_alive(30_000)

      eventually(
        fn -> Router.get(FerricStore.Instance.get(:default), key) == "survive_twice" end,
        "data should survive second kill"
      )
    end

    test "6. kill all 4 shards sequentially, all data survives" do
      keys =
        for i <- 0..3 do
          k = ukey_for_shard(i)
          Router.put(FerricStore.Instance.get(:default), k, "shard_#{i}_data")
          {k, i}
        end

      ShardHelpers.flush_all_shards()

      for i <- 0..3 do
        ShardHelpers.kill_shard_safely(i)
        ShardHelpers.wait_shards_alive(30_000)
      end

      for {k, i} <- keys do
        eventually(
          fn -> Router.get(FerricStore.Instance.get(:default), k) == "shard_#{i}_data" end,
          "shard #{i} data should survive sequential kill"
        )
      end
    end

    test "7. kill shard, write new data after restart, kill again, both old and new survive" do
      key_old = ukey_for_shard(0)
      write_and_flush(key_old, "old_data")

      ShardHelpers.kill_shard_safely(0)
      ShardHelpers.wait_shards_alive(30_000)

      eventually(
        fn -> Router.get(FerricStore.Instance.get(:default), key_old) == "old_data" end,
        "old data should survive first kill"
      )

      # Write new data to shard 0 after restart
      key_new = ukey_for_shard(0)
      write_and_flush(key_new, "new_data")

      ShardHelpers.kill_shard_safely(0)
      ShardHelpers.wait_shards_alive(30_000)

      eventually(
        fn -> Router.get(FerricStore.Instance.get(:default), key_old) == "old_data" end,
        "old data should survive second kill"
      )

      eventually(
        fn -> Router.get(FerricStore.Instance.get(:default), key_new) == "new_data" end,
        "new data should survive second kill"
      )
    end
  end

  # =========================================================================
  # Edge cases
  # =========================================================================

  describe "edge cases" do
    test "8. kill shard immediately after write (no flush) — no crash" do
      key = ukey_for_shard(0)
      Router.put(FerricStore.Instance.get(:default), key, "maybe_lost")
      # Intentionally NO flush — data may or may not be on disk

      ShardHelpers.kill_shard_safely(0)
      ShardHelpers.wait_shards_alive(30_000)

      # Data may or may not survive (the WARaft segment log has the command, so
      # replay during init should recover it). The critical requirement
      # is that the shard recovers without crashing.
      eventually(
        fn ->
          result = Router.get(FerricStore.Instance.get(:default), key)
          result == "maybe_lost" or result == nil
        end,
        "shard should recover after kill without flush"
      )
    end

    test "9. kill shard during active writes — no crash, writers get errors gracefully" do
      key = ukey_for_shard(0)
      write_and_flush(key, "base_value")

      test_pid = self()

      writers =
        for i <- 1..3 do
          spawn(fn ->
            results =
              Enum.map(1..10, fn j ->
                try do
                  Router.put(FerricStore.Instance.get(:default), "#{key}_w#{i}_#{j}", "val_#{j}")
                  :ok
                rescue
                  _ -> :error
                catch
                  :exit, _ -> :error
                end
              end)

            send(test_pid, {:writer_done, i, results})
          end)
        end

      # Give writers a moment to start, then kill the shard
      Process.sleep(10)
      ShardHelpers.kill_shard_safely(0)
      ShardHelpers.wait_shards_alive(30_000)

      # Collect writer results — all should have completed (no infinite hangs)
      for _ <- writers do
        assert_receive {:writer_done, _i, results}, 30_000
        assert Enum.all?(results, &(&1 in [:ok, :error]))
      end

      # The base value written before kill should survive
      eventually(
        fn -> Router.get(FerricStore.Instance.get(:default), key) == "base_value" end,
        "base value should survive despite concurrent kill"
      )
    end

    test "10. kill shard, verify ETS keydir is rebuilt (size > 0 after recovery)" do
      key = ukey_for_shard(0)
      write_and_flush(key, "keydir_test")

      ShardHelpers.kill_shard_safely(0)
      ShardHelpers.wait_shards_alive(30_000)

      # After wait_shards_alive, init/1 has completed and the keydir is
      # rebuilt from Bitcask. The key we wrote should be in the keydir.
      eventually(
        fn ->
          info = :ets.info(:keydir_0, :size)
          is_integer(info) and info > 0
        end,
        "ETS keydir should be rebuilt with size > 0 after shard recovery"
      )
    end

    test "11. kill BitcaskWriter, shard still serves reads" do
      key = ukey_for_shard(0)
      write_and_flush(key, "readable_after_writer_kill")

      writer_name = Ferricstore.Store.BitcaskWriter.writer_name(0)
      pid = Process.whereis(writer_name)

      if pid do
        ref = Process.monitor(pid)
        Process.exit(pid, :kill)

        receive do
          {:DOWN, ^ref, :process, ^pid, _} -> :ok
        after
          2_000 -> :ok
        end
      end

      # Shard should still serve reads even with BitcaskWriter dead
      eventually(
        fn ->
          Router.get(FerricStore.Instance.get(:default), key) == "readable_after_writer_kill"
        end,
        "shard should serve reads after BitcaskWriter kill"
      )
    end

    test "12. kill Batcher, pending writes fail gracefully" do
      key = ukey("batcher_kill")
      write_and_flush(key, "before_batcher_kill")

      batcher_name = Ferricstore.Raft.Batcher.batcher_name(0)
      pid = Process.whereis(batcher_name)

      if pid do
        ref = Process.monitor(pid)
        Process.exit(pid, :kill)

        receive do
          {:DOWN, ^ref, :process, ^pid, _} -> :ok
        after
          2_000 -> :ok
        end
      end

      # Wait for batcher to restart
      eventually(
        fn ->
          new_pid = Process.whereis(batcher_name)
          is_pid(new_pid) and Process.alive?(new_pid)
        end,
        "Batcher should restart after kill"
      )

      # Data written before kill should survive
      eventually(
        fn -> Router.get(FerricStore.Instance.get(:default), key) == "before_batcher_kill" end,
        "data should survive Batcher kill"
      )
    end
  end

  # =========================================================================
  # Timing / stress
  # =========================================================================

  describe "timing" do
    test "13. rapid kills: kill shard 0, immediately kill shard 1, both recover" do
      key0 = ukey_for_shard(0)
      key1 = ukey_for_shard(1)

      Router.put(FerricStore.Instance.get(:default), key0, "shard0_rapid")
      Router.put(FerricStore.Instance.get(:default), key1, "shard1_rapid")
      ShardHelpers.flush_all_shards()

      ShardHelpers.kill_shard_safely(0)
      ShardHelpers.kill_shard_safely(1)
      ShardHelpers.wait_shards_alive(30_000)

      eventually(
        fn -> Router.get(FerricStore.Instance.get(:default), key0) == "shard0_rapid" end,
        "shard 0 data should survive rapid kill"
      )

      eventually(
        fn -> Router.get(FerricStore.Instance.get(:default), key1) == "shard1_rapid" end,
        "shard 1 data should survive rapid kill"
      )
    end

    test "14. kill and restart 10 times in a loop — all recoveries succeed" do
      key = ukey_for_shard(0)
      write_and_flush(key, "loop_survivor")

      for round <- 1..10 do
        ShardHelpers.kill_shard_safely(0)
        ShardHelpers.wait_shards_alive(30_000)

        eventually(
          fn -> Router.get(FerricStore.Instance.get(:default), key) == "loop_survivor" end,
          "data should survive kill round #{round}"
        )
      end
    end
  end
end
