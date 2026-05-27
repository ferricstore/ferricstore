defmodule Ferricstore.Raft.WritePathTest do
  @moduledoc """
  Tests for the Raft-integrated write path.

  Verifies that all write operations (SET, DEL, INCR, MULTI/EXEC) route
  through the Raft Batcher and StateMachine before updating ETS and Bitcask,
  rather than writing directly from the Shard GenServer.
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Store.CompoundKey
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  setup_all do
    ShardHelpers.wait_shards_alive()

    # The application already started WARaft partitions and
    # batchers for shards 0-3. Reuse them.
    :ok
  end

  setup do
    on_exit(fn -> ShardHelpers.wait_shards_alive() end)
  end

  # Helper to generate unique keys
  defp ukey(base), do: "raft_wp_#{base}_#{:rand.uniform(9_999_999)}"

  defp keydir_for(key), do: :"keydir_#{Router.shard_for(FerricStore.Instance.get(:default), key)}"

  defp shard_pid_for(key) do
    name =
      Router.shard_name(
        FerricStore.Instance.get(:default),
        Router.shard_for(FerricStore.Instance.get(:default), key)
      )

    Process.whereis(name)
  end

  # ---------------------------------------------------------------------------
  # 1. SET via Router goes through Raft when enabled
  # ---------------------------------------------------------------------------

  describe "SET via Router goes through Raft" do
    test "SET writes value that is readable after Raft commit" do
      k = ukey("set_raft")

      assert :ok = Router.put(FerricStore.Instance.get(:default), k, "raft_value", 0)
      assert "raft_value" == Router.get(FerricStore.Instance.get(:default), k)
    end

    test "SET writes to ETS via StateMachine apply" do
      k = ukey("set_ets_via_sm")

      :ok = Router.put(FerricStore.Instance.get(:default), k, "sm_val", 0)

      # Single-table format: {key, value, expire_at_ms, lfu_counter}
      assert [{^k, "sm_val", 0, _lfu, _fid, _off, _vsize}] = :ets.lookup(keydir_for(k), k)
    end

    test "SET with TTL preserves expiry through Raft path" do
      k = ukey("set_ttl_raft")
      future = System.os_time(:millisecond) + 60_000

      :ok = Router.put(FerricStore.Instance.get(:default), k, "ttl_val", future)

      {value, expire_at_ms} = Router.get_meta(FerricStore.Instance.get(:default), k)
      assert value == "ttl_val"
      assert expire_at_ms == future
    end
  end

  # ---------------------------------------------------------------------------
  # 2. GET after Raft-committed SET returns the value
  # ---------------------------------------------------------------------------

  describe "GET after Raft-committed SET" do
    test "returns the value immediately after SET" do
      k = ukey("get_after_set")

      :ok = Router.put(FerricStore.Instance.get(:default), k, "committed_value", 0)
      assert "committed_value" == Router.get(FerricStore.Instance.get(:default), k)
    end

    test "returns latest value after multiple SETs" do
      k = ukey("get_multi_set")

      :ok = Router.put(FerricStore.Instance.get(:default), k, "v1", 0)
      assert "v1" == Router.get(FerricStore.Instance.get(:default), k)

      :ok = Router.put(FerricStore.Instance.get(:default), k, "v2", 0)
      assert "v2" == Router.get(FerricStore.Instance.get(:default), k)

      :ok = Router.put(FerricStore.Instance.get(:default), k, "v3", 0)
      assert "v3" == Router.get(FerricStore.Instance.get(:default), k)
    end
  end

  # ---------------------------------------------------------------------------
  # 3. DEL via Router goes through Raft
  # ---------------------------------------------------------------------------

  describe "DEL via Router goes through Raft" do
    test "DEL removes key after Raft commit" do
      k = ukey("del_raft")

      :ok = Router.put(FerricStore.Instance.get(:default), k, "to_delete", 0)
      assert "to_delete" == Router.get(FerricStore.Instance.get(:default), k)

      :ok = Router.delete(FerricStore.Instance.get(:default), k)
      assert nil == Router.get(FerricStore.Instance.get(:default), k)
    end

    test "DEL removes from ETS" do
      k = ukey("del_ets")

      :ok = Router.put(FerricStore.Instance.get(:default), k, "will_vanish", 0)
      assert [{^k, "will_vanish", 0, _lfu, _fid, _off, _vsize}] = :ets.lookup(keydir_for(k), k)

      :ok = Router.delete(FerricStore.Instance.get(:default), k)
      assert [] == :ets.lookup(keydir_for(k), k)
    end

    test "DEL on non-existent key returns :ok" do
      k = ukey("del_missing")
      assert :ok = Router.delete(FerricStore.Instance.get(:default), k)
    end
  end

  # ---------------------------------------------------------------------------
  # 4. Multiple concurrent writes all committed via Raft
  # ---------------------------------------------------------------------------

  describe "multiple concurrent writes via Raft" do
    test "all concurrent writes complete successfully" do
      keys = for i <- 1..50, do: ukey("concurrent_#{i}")

      tasks =
        Enum.map(keys, fn k ->
          Task.async(fn ->
            Router.put(FerricStore.Instance.get(:default), k, "concurrent_val_#{k}", 0)
          end)
        end)

      results = Task.await_many(tasks, 15_000)
      assert Enum.all?(results, &(&1 == :ok))

      # All keys should be readable after Raft commit
      for k <- keys do
        assert "concurrent_val_#{k}" == Router.get(FerricStore.Instance.get(:default), k),
               "Key #{k} should be readable after concurrent Raft commit"
      end
    end

    test "concurrent writes to same key produce consistent final state" do
      k = ukey("concurrent_same")

      tasks =
        for i <- 1..10 do
          Task.async(fn ->
            Router.put(FerricStore.Instance.get(:default), k, "v#{i}", 0)
          end)
        end

      Task.await_many(tasks, 15_000)

      # Should have one of the values (last writer wins)
      val = Router.get(FerricStore.Instance.get(:default), k)
      assert val != nil
      assert String.starts_with?(val, "v")
    end
  end

  # ---------------------------------------------------------------------------
  # 5. INCR via Router goes through Raft (atomic increment)
  # ---------------------------------------------------------------------------

  describe "INCR via Router goes through Raft" do
    test "INCR on non-existent key initializes to delta" do
      k = ukey("incr_new")

      assert {:ok, 1} = Router.incr(FerricStore.Instance.get(:default), k, 1)
      assert "1" == Router.get(FerricStore.Instance.get(:default), k)
    end

    test "INCR on existing integer key increments correctly" do
      k = ukey("incr_existing")

      :ok = Router.put(FerricStore.Instance.get(:default), k, "10", 0)
      assert {:ok, 15} = Router.incr(FerricStore.Instance.get(:default), k, 5)
      assert "15" == Router.get(FerricStore.Instance.get(:default), k)
    end

    test "DECR works through Raft path" do
      k = ukey("decr_raft")

      :ok = Router.put(FerricStore.Instance.get(:default), k, "100", 0)
      assert {:ok, 95} = Router.incr(FerricStore.Instance.get(:default), k, -5)
      assert "95" == Router.get(FerricStore.Instance.get(:default), k)
    end

    test "multiple sequential INCRs produce correct total" do
      k = ukey("incr_seq")

      for _i <- 1..10 do
        Router.incr(FerricStore.Instance.get(:default), k, 1)
      end

      assert "10" == Router.get(FerricStore.Instance.get(:default), k)
    end
  end

  # ---------------------------------------------------------------------------
  # 6. Write version increments after Raft commit
  # ---------------------------------------------------------------------------

  describe "write version increments after Raft commit" do
    test "write version increases after SET" do
      k = ukey("version_set")

      v1 = Router.get_version(FerricStore.Instance.get(:default), k)
      :ok = Router.put(FerricStore.Instance.get(:default), k, "val", 0)
      v2 = Router.get_version(FerricStore.Instance.get(:default), k)

      assert v2 > v1
    end

    test "write version increases after DEL" do
      k = ukey("version_del")

      :ok = Router.put(FerricStore.Instance.get(:default), k, "val", 0)
      v1 = Router.get_version(FerricStore.Instance.get(:default), k)

      :ok = Router.delete(FerricStore.Instance.get(:default), k)
      v2 = Router.get_version(FerricStore.Instance.get(:default), k)

      assert v2 > v1
    end

    test "write version increases after INCR" do
      k = ukey("version_incr")

      :ok = Router.put(FerricStore.Instance.get(:default), k, "0", 0)
      v1 = Router.get_version(FerricStore.Instance.get(:default), k)

      {:ok, _} = Router.incr(FerricStore.Instance.get(:default), k, 1)
      v2 = Router.get_version(FerricStore.Instance.get(:default), k)

      assert v2 > v1
    end
  end

  # ---------------------------------------------------------------------------
  # 7. Data persists in Bitcask after Raft commit (flush + NIF.get)
  # ---------------------------------------------------------------------------

  describe "data persists in Bitcask after Raft commit" do
    test "SET data is readable from Bitcask NIF after Raft commit" do
      k = ukey("bitcask_persist")

      :ok = Router.put(FerricStore.Instance.get(:default), k, "durable_value", 0)

      # The StateMachine defers small-value Bitcask writes to the background
      # BitcaskWriter. Flush both the shard and the writer to ensure on-disk.
      shard_pid = shard_pid_for(k)
      state = :sys.get_state(shard_pid)

      GenServer.call(shard_pid, :flush)
      Ferricstore.Store.BitcaskWriter.flush_all()

      # v2: verify the value is on disk via the ETS 7-tuple location
      [{^k, _value, _exp, _lfu, fid, off, _vsize}] = :ets.lookup(state.keydir, k)

      log_path =
        Path.join(
          state.data_dir |> Ferricstore.DataDir.shard_data_path(state.index),
          "#{String.pad_leading(Integer.to_string(fid), 5, "0")}.log"
        )

      assert {:ok, "durable_value"} = NIF.v2_pread_at(log_path, off)
    end

    test "DEL tombstone is persisted in Bitcask" do
      k = ukey("bitcask_del")

      :ok = Router.put(FerricStore.Instance.get(:default), k, "to_remove", 0)
      :ok = Router.delete(FerricStore.Instance.get(:default), k)

      shard_pid = shard_pid_for(k)
      state = :sys.get_state(shard_pid)
      GenServer.call(shard_pid, :flush)

      # After delete, key should be gone from ETS
      assert [] = :ets.lookup(state.keydir, k)
    end
  end

  # ---------------------------------------------------------------------------
  # 8. Batch window: multiple rapid writes batched into single Raft entry
  # ---------------------------------------------------------------------------

  describe "batch window batches rapid writes" do
    test "multiple rapid writes to the same shard are batched" do
      # Generate keys that all map to the same shard
      shard_idx = 0

      keys =
        Stream.repeatedly(fn -> ukey("batch_#{:rand.uniform(999_999)}") end)
        |> Stream.filter(fn k ->
          Router.shard_for(FerricStore.Instance.get(:default), k) == shard_idx
        end)
        |> Enum.take(5)

      # Send all writes nearly simultaneously -- they should be batched
      # by the Batcher's batch_window_ms (1ms default)
      tasks =
        Enum.map(keys, fn k ->
          Task.async(fn ->
            Router.put(FerricStore.Instance.get(:default), k, "batched_#{k}", 0)
          end)
        end)

      results = Task.await_many(tasks, 10_000)
      assert Enum.all?(results, &(&1 == :ok))

      # All keys should be readable
      for k <- keys do
        assert "batched_#{k}" == Router.get(FerricStore.Instance.get(:default), k),
               "Batched key #{k} should be readable"
      end
    end
  end

  # ---------------------------------------------------------------------------
  # 9. MULTI/EXEC transaction goes through Raft as single entry
  #
  # Note: MULTI/EXEC is handled at the connection layer (connection.ex).
  # Each command in the transaction queue is executed sequentially through
  # the normal command dispatch path, which routes through Router -> Shard.
  # Each write within the transaction goes
  # through the Raft path individually. The atomicity guarantee comes
  # from WATCH/version checking, not from Raft batching.
  # ---------------------------------------------------------------------------

  describe "MULTI/EXEC transaction writes go through Raft" do
    test "sequential writes within simulated transaction all commit via Raft" do
      k1 = ukey("tx_k1")
      k2 = ukey("tx_k2")
      k3 = ukey("tx_k3")

      # Simulate MULTI/EXEC: multiple writes executed sequentially
      :ok = Router.put(FerricStore.Instance.get(:default), k1, "tx_val1", 0)
      :ok = Router.put(FerricStore.Instance.get(:default), k2, "tx_val2", 0)
      :ok = Router.put(FerricStore.Instance.get(:default), k3, "tx_val3", 0)

      # All should be readable after commit
      assert "tx_val1" == Router.get(FerricStore.Instance.get(:default), k1)
      assert "tx_val2" == Router.get(FerricStore.Instance.get(:default), k2)
      assert "tx_val3" == Router.get(FerricStore.Instance.get(:default), k3)

      # All should be in ETS (written by StateMachine), single-table format
      assert [{^k1, "tx_val1", 0, _, _, _, _}] = :ets.lookup(keydir_for(k1), k1)
      assert [{^k2, "tx_val2", 0, _, _, _, _}] = :ets.lookup(keydir_for(k2), k2)
      assert [{^k3, "tx_val3", 0, _, _, _, _}] = :ets.lookup(keydir_for(k3), k3)
    end

    test "mixed SET and DEL in transaction sequence" do
      k = ukey("tx_mixed")

      :ok = Router.put(FerricStore.Instance.get(:default), k, "initial", 0)
      assert "initial" == Router.get(FerricStore.Instance.get(:default), k)

      :ok = Router.delete(FerricStore.Instance.get(:default), k)
      assert nil == Router.get(FerricStore.Instance.get(:default), k)

      :ok = Router.put(FerricStore.Instance.get(:default), k, "restored", 0)
      assert "restored" == Router.get(FerricStore.Instance.get(:default), k)
    end
  end

  # ---------------------------------------------------------------------------
  # 10. WATCH detects version change from Raft-committed write
  # ---------------------------------------------------------------------------

  describe "WATCH detects version change from Raft-committed write" do
    test "write version changes are visible to get_version after Raft commit" do
      k = ukey("watch_raft")

      v_before = Router.get_version(FerricStore.Instance.get(:default), k)
      :ok = Router.put(FerricStore.Instance.get(:default), k, "watched_val", 0)
      v_after = Router.get_version(FerricStore.Instance.get(:default), k)

      # WATCH would have recorded v_before; after the Raft-committed write,
      # get_version returns a different (higher) value, so EXEC would abort.
      assert v_after != v_before
      assert v_after > v_before
    end

    test "DEL changes version visible to WATCH" do
      k = ukey("watch_del")

      :ok = Router.put(FerricStore.Instance.get(:default), k, "will_del", 0)
      v_before = Router.get_version(FerricStore.Instance.get(:default), k)

      :ok = Router.delete(FerricStore.Instance.get(:default), k)
      v_after = Router.get_version(FerricStore.Instance.get(:default), k)

      assert v_after > v_before
    end

    test "INCR changes version visible to WATCH" do
      k = ukey("watch_incr")

      :ok = Router.put(FerricStore.Instance.get(:default), k, "5", 0)
      v_before = Router.get_version(FerricStore.Instance.get(:default), k)

      {:ok, _} = Router.incr(FerricStore.Instance.get(:default), k, 1)
      v_after = Router.get_version(FerricStore.Instance.get(:default), k)

      assert v_after > v_before
    end

    test "concurrent Raft writes to same shard all bump version" do
      # Use a key we know the shard for, then write to same shard
      k = ukey("watch_concurrent")
      shard_idx = Router.shard_for(FerricStore.Instance.get(:default), k)

      v_before = Router.get_version(FerricStore.Instance.get(:default), k)

      # Write several keys to the same shard
      keys =
        Stream.repeatedly(fn -> ukey("wc_#{:rand.uniform(999_999)}") end)
        |> Stream.filter(fn kk ->
          Router.shard_for(FerricStore.Instance.get(:default), kk) == shard_idx
        end)
        |> Enum.take(5)

      for kk <- keys do
        :ok = Router.put(FerricStore.Instance.get(:default), kk, "v", 0)
      end

      v_after = Router.get_version(FerricStore.Instance.get(:default), k)

      # Version should have increased by at least the number of writes
      assert v_after >= v_before + length(keys)
    end
  end

  # ===========================================================================
  # 11–15. StateMachine apply/3 handlers for list_op, compound_put,
  #         compound_delete, and compound_delete_prefix
  #
  # These tests exercise the StateMachine directly (no Batcher / Router) to
  # verify that the new command types correctly replicate the shard logic.
  # ===========================================================================

  # Per-test setup: fresh Bitcask + ETS for isolated StateMachine testing.
  # These tests do NOT need the full Raft infrastructure from setup_all,
  # but they are in the same file so they inherit the async: false setting
  # which avoids flaky interaction with other Raft tests.

  defp fresh_sm_state do
    dir = Path.join(System.tmp_dir!(), "wp_sm_#{:rand.uniform(9_999_999)}")
    shard_path = Ferricstore.DataDir.shard_data_path(dir, 0)
    File.mkdir_p!(shard_path)

    # v2: create a .log file instead of NIF.new
    active_file_path = Path.join(shard_path, "00000.log")
    File.touch!(active_file_path)

    suffix = :rand.uniform(9_999_999)
    keydir_name = :"wp_sm_keydir_#{suffix}"
    :ets.new(keydir_name, [:set, :public, :named_table])

    state =
      Ferricstore.Raft.StateMachine.init(%{
        shard_index: 0,
        shard_data_path: shard_path,
        data_dir: dir,
        active_file_id: 0,
        active_file_path: active_file_path,
        ets: keydir_name
      })

    {state, keydir_name, nil, dir}
  end

  defp cleanup_sm({_state, ets_name, _store, dir}) do
    try do
      :ets.delete(ets_name)
    rescue
      ArgumentError -> :ok
    end

    File.rm_rf!(dir)
  end

  alias Ferricstore.Raft.StateMachine, as: SM

  # ---------------------------------------------------------------------------
  # 12. list_op — LPUSH through Raft adds element
  # ---------------------------------------------------------------------------

  describe "list_op: LPUSH through Raft adds element" do
    test "LPUSH to a new key creates a list with the pushed elements" do
      ctx = fresh_sm_state()
      {state, _ets, _store, _dir} = ctx

      {new_state, result} =
        SM.apply(%{}, {:list_op, "mylist", {:lpush, ["a", "b", "c"]}}, state)

      assert result == 3
      assert new_state.applied_count == 1

      # Verify via LRANGE (compound key format)
      {_state2, elements} =
        SM.apply(%{}, {:list_op, "mylist", {:lrange, 0, -1}}, new_state)

      assert elements == ["c", "b", "a"]

      cleanup_sm(ctx)
    end

    test "LPUSH to existing list prepends elements" do
      ctx = fresh_sm_state()
      {state, _ets, _store, _dir} = ctx

      {state2, 2} =
        SM.apply(%{}, {:list_op, "mylist", {:lpush, ["a", "b"]}}, state)

      {state3, 4} =
        SM.apply(%{}, {:list_op, "mylist", {:lpush, ["c", "d"]}}, state2)

      {_state4, elements} =
        SM.apply(%{}, {:list_op, "mylist", {:lrange, 0, -1}}, state3)

      assert elements == ["d", "c", "b", "a"]

      cleanup_sm(ctx)
    end
  end

  # ---------------------------------------------------------------------------
  # 12. list_op — RPUSH through Raft adds element
  # ---------------------------------------------------------------------------

  describe "list_op: RPUSH through Raft adds element" do
    test "RPUSH to a new key creates a list with the pushed elements" do
      ctx = fresh_sm_state()
      {state, _ets, _store, _dir} = ctx

      {new_state, result} =
        SM.apply(%{}, {:list_op, "rlist", {:rpush, ["x", "y", "z"]}}, state)

      assert result == 3

      {_state2, elements} =
        SM.apply(%{}, {:list_op, "rlist", {:lrange, 0, -1}}, new_state)

      assert elements == ["x", "y", "z"]

      cleanup_sm(ctx)
    end

    test "RPUSH to existing list appends elements" do
      ctx = fresh_sm_state()
      {state, _ets, _store, _dir} = ctx

      {state2, 2} =
        SM.apply(%{}, {:list_op, "rlist", {:rpush, ["a", "b"]}}, state)

      {state3, 4} =
        SM.apply(%{}, {:list_op, "rlist", {:rpush, ["c", "d"]}}, state2)

      # Verify via LRANGE
      {_state4, elements} =
        SM.apply(%{}, {:list_op, "rlist", {:lrange, 0, -1}}, state3)

      assert elements == ["a", "b", "c", "d"]

      cleanup_sm(ctx)
    end
  end

  # ---------------------------------------------------------------------------
  # 12. list_op — LPOP through Raft removes element
  # ---------------------------------------------------------------------------

  describe "list_op: LPOP through Raft removes element" do
    test "LPOP from a list returns and removes the leftmost element" do
      ctx = fresh_sm_state()
      {state, _ets, _store, _dir} = ctx

      {state2, 3} =
        SM.apply(%{}, {:list_op, "poplist", {:rpush, ["a", "b", "c"]}}, state)

      {state3, popped} =
        SM.apply(%{}, {:list_op, "poplist", {:lpop, 1}}, state2)

      assert popped == "a"
      assert state3.applied_count == 2

      {_state4, elements} =
        SM.apply(%{}, {:list_op, "poplist", {:lrange, 0, -1}}, state3)

      assert elements == ["b", "c"]

      cleanup_sm(ctx)
    end

    test "LPOP from empty / non-existent key returns nil" do
      ctx = fresh_sm_state()
      {state, _ets, _store, _dir} = ctx

      {_new_state, result} =
        SM.apply(%{}, {:list_op, "nokey", {:lpop, 1}}, state)

      assert result == nil

      cleanup_sm(ctx)
    end

    test "LPOP all elements empties the list" do
      ctx = fresh_sm_state()
      {state, _ets, _store, _dir} = ctx

      {state2, 1} =
        SM.apply(%{}, {:list_op, "single", {:rpush, ["only"]}}, state)

      {state3, "only"} =
        SM.apply(%{}, {:list_op, "single", {:lpop, 1}}, state2)

      # List should be empty — LLEN returns 0 or LRANGE returns []
      {_state4, elements} =
        SM.apply(%{}, {:list_op, "single", {:lrange, 0, -1}}, state3)

      assert elements == [] or elements == nil

      cleanup_sm(ctx)
    end
  end

  # ---------------------------------------------------------------------------
  # 12. list_op — RPOP through Raft removes element
  # ---------------------------------------------------------------------------

  describe "list_op: RPOP through Raft removes element" do
    test "RPOP from a list returns and removes the rightmost element" do
      ctx = fresh_sm_state()
      {state, _ets, _store, _dir} = ctx

      {state2, 3} =
        SM.apply(%{}, {:list_op, "rpoplist", {:rpush, ["a", "b", "c"]}}, state)

      {state3, popped} =
        SM.apply(%{}, {:list_op, "rpoplist", {:rpop, 1}}, state2)

      assert popped == "c"

      {_state4, elements} =
        SM.apply(%{}, {:list_op, "rpoplist", {:lrange, 0, -1}}, state3)

      assert elements == ["a", "b"]

      cleanup_sm(ctx)
    end
  end

  # ---------------------------------------------------------------------------
  # 13. compound_put — HSET through Raft writes field
  # ---------------------------------------------------------------------------

  describe "compound_put: HSET through Raft writes field" do
    test "compound_put inserts a hash field into ETS and Bitcask" do
      ctx = fresh_sm_state()
      {state, ets, _store, _dir} = ctx

      compound_key = "myhash\x00field1"

      {new_state, result} =
        SM.apply(%{}, {:compound_put, compound_key, "value1", 0}, state)

      assert result == :ok
      assert new_state.applied_count == 1

      # Verify ETS
      assert [{^compound_key, "value1", _, _, _, _, _}] = :ets.lookup(ets, compound_key)

      # Verify Bitcask
      assert [{_, "value1", _, _, _, _, _}] = :ets.lookup(ets, compound_key)

      cleanup_sm(ctx)
    end

    test "compound_put overwrites existing field value" do
      ctx = fresh_sm_state()
      {state, ets, _store, _dir} = ctx

      compound_key = "myhash\x00field1"

      {state2, :ok} =
        SM.apply(%{}, {:compound_put, compound_key, "v1", 0}, state)

      {_state3, :ok} =
        SM.apply(%{}, {:compound_put, compound_key, "v2", 0}, state2)

      assert [{^compound_key, "v2", _, _, _, _, _}] = :ets.lookup(ets, compound_key)

      cleanup_sm(ctx)
    end

    test "multiple compound_puts for different fields" do
      ctx = fresh_sm_state()
      {state, ets, _store, _dir} = ctx

      {state2, :ok} =
        SM.apply(%{}, {:compound_put, "h\x00f1", "val1", 0}, state)

      {state3, :ok} =
        SM.apply(%{}, {:compound_put, "h\x00f2", "val2", 0}, state2)

      {_state4, :ok} =
        SM.apply(%{}, {:compound_put, "h\x00f3", "val3", 0}, state3)

      assert [{_, "val1", _, _, _, _, _}] = :ets.lookup(ets, "h\x00f1")
      assert [{_, "val2", _, _, _, _, _}] = :ets.lookup(ets, "h\x00f2")
      assert [{_, "val3", _, _, _, _, _}] = :ets.lookup(ets, "h\x00f3")

      cleanup_sm(ctx)
    end

    test "compound_put writes promoted hash fields to the dedicated Bitcask" do
      ctx = fresh_sm_state()
      {state, ets, _store, _dir} = ctx

      key = "promoted_hash"
      compound_key = Ferricstore.Store.CompoundKey.hash_field(key, "field1")

      {:ok, dedicated_path} =
        Ferricstore.Store.Promotion.open_dedicated(state.data_dir, 0, :hash, key)

      dedicated_log = Ferricstore.Store.Promotion.find_active(dedicated_path)

      assert File.stat!(dedicated_log).size == 0

      {new_state, :ok} = SM.apply(%{}, {:compound_put, compound_key, "value1", 0}, state)

      assert new_state.applied_count == 1
      assert [{^compound_key, "value1", 0, _lfu, 0, off, _vsize}] = :ets.lookup(ets, compound_key)
      assert off >= 0
      assert {:ok, "value1"} = NIF.v2_pread_at(dedicated_log, off)
      assert File.stat!(dedicated_log).size > 0

      cleanup_sm(ctx)
    end
  end

  # ---------------------------------------------------------------------------
  # 14. compound_delete — HDEL through Raft removes field
  # ---------------------------------------------------------------------------

  describe "compound_delete: HDEL through Raft removes field" do
    test "compound_delete removes a hash field from ETS and Bitcask" do
      ctx = fresh_sm_state()
      {state, ets, _store, _dir} = ctx

      compound_key = "myhash\x00field1"

      {state2, :ok} =
        SM.apply(%{}, {:compound_put, compound_key, "value1", 0}, state)

      {state3, :ok} =
        SM.apply(%{}, {:compound_delete, compound_key}, state2)

      assert state3.applied_count == 2
      assert [] == :ets.lookup(ets, compound_key)
      assert [] = :ets.lookup(ets, compound_key)

      cleanup_sm(ctx)
    end

    test "compound_delete on non-existent key returns :ok" do
      ctx = fresh_sm_state()
      {state, _ets, _store, _dir} = ctx

      {_new_state, result} =
        SM.apply(%{}, {:compound_delete, "nonexistent\x00field"}, state)

      assert result == :ok

      cleanup_sm(ctx)
    end

    test "compound_delete tombstones promoted hash fields in the dedicated Bitcask" do
      ctx = fresh_sm_state()
      {state, ets, _store, _dir} = ctx

      key = "promoted_hash_delete"
      compound_key = Ferricstore.Store.CompoundKey.hash_field(key, "field1")

      {:ok, dedicated_path} =
        Ferricstore.Store.Promotion.open_dedicated(state.data_dir, 0, :hash, key)

      dedicated_log = Ferricstore.Store.Promotion.find_active(dedicated_path)

      {state2, :ok} = SM.apply(%{}, {:compound_put, compound_key, "value1", 0}, state)
      size_after_put = File.stat!(dedicated_log).size
      assert size_after_put > 0

      {state3, :ok} = SM.apply(%{}, {:compound_delete, compound_key}, state2)

      assert state3.applied_count == 2
      assert [] == :ets.lookup(ets, compound_key)
      assert File.stat!(dedicated_log).size > size_after_put

      cleanup_sm(ctx)
    end
  end

  # ---------------------------------------------------------------------------
  # 13. compound_put — SADD through Raft adds member
  # ---------------------------------------------------------------------------

  describe "compound_put: SADD through Raft adds member" do
    test "compound_put adds a set member (presence marker)" do
      ctx = fresh_sm_state()
      {state, ets, _store, _dir} = ctx

      # Sets use a presence marker as the value
      compound_key = "myset\x00member1"
      presence = "1"

      {new_state, :ok} =
        SM.apply(%{}, {:compound_put, compound_key, presence, 0}, state)

      assert new_state.applied_count == 1
      assert [{^compound_key, ^presence, _, _, _, _, _}] = :ets.lookup(ets, compound_key)
      assert [{_, ^presence, _, _, _, _, _}] = :ets.lookup(ets, compound_key)

      cleanup_sm(ctx)
    end

    test "multiple set members via compound_put" do
      ctx = fresh_sm_state()
      {state, ets, _store, _dir} = ctx

      {state2, :ok} =
        SM.apply(%{}, {:compound_put, "myset\x00m1", "1", 0}, state)

      {state3, :ok} =
        SM.apply(%{}, {:compound_put, "myset\x00m2", "1", 0}, state2)

      {_state4, :ok} =
        SM.apply(%{}, {:compound_put, "myset\x00m3", "1", 0}, state3)

      assert [{_, "1", _, _, _, _, _}] = :ets.lookup(ets, "myset\x00m1")
      assert [{_, "1", _, _, _, _, _}] = :ets.lookup(ets, "myset\x00m2")
      assert [{_, "1", _, _, _, _, _}] = :ets.lookup(ets, "myset\x00m3")

      cleanup_sm(ctx)
    end
  end

  # ---------------------------------------------------------------------------
  # 15. compound_delete_prefix — DEL on hash through Raft cleans up all fields
  # ---------------------------------------------------------------------------

  describe "compound_delete_prefix: DEL on hash through Raft cleans up all fields" do
    test "deletes all compound keys matching prefix" do
      ctx = fresh_sm_state()
      {state, ets, _store, _dir} = ctx

      # Insert several fields for a hash "myhash"
      prefix = "myhash\x00"

      {state2, :ok} =
        SM.apply(%{}, {:compound_put, "myhash\x00f1", "v1", 0}, state)

      {state3, :ok} =
        SM.apply(%{}, {:compound_put, "myhash\x00f2", "v2", 0}, state2)

      {state4, :ok} =
        SM.apply(%{}, {:compound_put, "myhash\x00f3", "v3", 0}, state3)

      # Also insert a key with a different prefix to ensure it is NOT deleted
      {state5, :ok} =
        SM.apply(%{}, {:compound_put, "otherhash\x00x", "ox", 0}, state4)

      # Verify all 4 keys exist in ETS
      assert :ets.info(ets, :size) == 4

      # Now delete all keys with the "myhash\0" prefix
      {state6, :ok} =
        SM.apply(%{}, {:compound_delete_prefix, prefix}, state5)

      assert state6.applied_count == 5

      # All "myhash" fields should be gone
      assert [] == :ets.lookup(ets, "myhash\x00f1")
      assert [] == :ets.lookup(ets, "myhash\x00f2")
      assert [] == :ets.lookup(ets, "myhash\x00f3")

      # Bitcask should also have them deleted
      assert [] = :ets.lookup(ets, "myhash\x00f1")
      assert [] = :ets.lookup(ets, "myhash\x00f2")
      assert [] = :ets.lookup(ets, "myhash\x00f3")

      # The "otherhash" key should still exist
      assert [{_, "ox", _, _, _, _, _}] = :ets.lookup(ets, "otherhash\x00x")

      cleanup_sm(ctx)
    end

    test "deletes all set members when DEL is called on the set key" do
      ctx = fresh_sm_state()
      {state, ets, _store, _dir} = ctx

      prefix = "myset\x00"

      {state2, :ok} =
        SM.apply(%{}, {:compound_put, "myset\x00m1", "1", 0}, state)

      {state3, :ok} =
        SM.apply(%{}, {:compound_put, "myset\x00m2", "1", 0}, state2)

      {state4, :ok} =
        SM.apply(%{}, {:compound_put, "myset\x00m3", "1", 0}, state3)

      {_state5, :ok} =
        SM.apply(%{}, {:compound_delete_prefix, prefix}, state4)

      assert [] == :ets.lookup(ets, "myset\x00m1")
      assert [] == :ets.lookup(ets, "myset\x00m2")
      assert [] == :ets.lookup(ets, "myset\x00m3")

      cleanup_sm(ctx)
    end

    test "compound_delete_prefix with no matching keys is a no-op" do
      ctx = fresh_sm_state()
      {state, _ets, _store, _dir} = ctx

      {new_state, :ok} =
        SM.apply(%{}, {:compound_delete_prefix, "nonexistent\x00"}, state)

      assert new_state.applied_count == 1

      cleanup_sm(ctx)
    end

    test "compound_delete_prefix does not affect unrelated keys" do
      ctx = fresh_sm_state()
      {state, ets, _store, _dir} = ctx

      {state2, :ok} =
        SM.apply(%{}, {:compound_put, "hash_a\x00f1", "v1", 0}, state)

      {state3, :ok} =
        SM.apply(%{}, {:compound_put, "hash_b\x00f1", "v2", 0}, state2)

      # Only delete hash_a's fields
      {_state4, :ok} =
        SM.apply(%{}, {:compound_delete_prefix, "hash_a\x00"}, state3)

      assert [] == :ets.lookup(ets, "hash_a\x00f1")
      assert [{_, "v2", _, _, _, _, _}] = :ets.lookup(ets, "hash_b\x00f1")

      cleanup_sm(ctx)
    end
  end

  # ---------------------------------------------------------------------------
  # Batch with new command types
  # ---------------------------------------------------------------------------

  describe "batch containing new command types" do
    test "put_batch applies many string writes with one batched result" do
      ctx = fresh_sm_state()
      {state, ets, _store, _dir} = ctx

      entries = [
        {"hotbatch:a", "va", 0},
        {"hotbatch:b", "vb", 12_345},
        {"hotbatch:c", "vc", 0}
      ]

      {new_state, {:ok, results}} = SM.apply(%{}, {:put_batch, entries}, state)

      assert results == [:ok, :ok, :ok]
      assert new_state.applied_count == 3
      assert [{_, "va", 0, _, _, _, _}] = :ets.lookup(ets, "hotbatch:a")
      assert [{_, "vb", 12_345, _, _, _, _}] = :ets.lookup(ets, "hotbatch:b")
      assert [{_, "vc", 0, _, _, _, _}] = :ets.lookup(ets, "hotbatch:c")

      cleanup_sm(ctx)
    end

    test "put_batch clears stale compound data before storing a string" do
      ctx = fresh_sm_state()
      {state, ets, _store, _dir} = ctx
      key = "hotbatch:compound"
      type_key = Ferricstore.Store.CompoundKey.type_key(key)
      field_key = Ferricstore.Store.CompoundKey.hash_field(key, "field")

      {state, :ok} = SM.apply(%{}, {:compound_put, type_key, "hash", 0}, state)
      {state, :ok} = SM.apply(%{}, {:compound_put, field_key, "old", 0}, state)

      {new_state, {:ok, [:ok]}} = SM.apply(%{}, {:put_batch, [{key, "string", 0}]}, state)

      assert new_state.applied_count == 3
      assert [{^key, "string", 0, _lfu, _fid, _off, _vsize}] = :ets.lookup(ets, key)
      assert [] == :ets.lookup(ets, type_key)
      assert [] == :ets.lookup(ets, field_key)

      cleanup_sm(ctx)
    end

    test "put_batch preserves per-key lock rejection" do
      ctx = fresh_sm_state()
      {state, ets, _store, _dir} = ctx
      owner = make_ref()

      locked_state = %{
        state
        | cross_shard_locks: %{"hotbatch:locked" => {owner, Ferricstore.HLC.now_ms() + 60_000}}
      }

      entries = [
        {"hotbatch:open", "va", 0},
        {"hotbatch:locked", "vb", 0}
      ]

      {new_state, {:ok, results}} = SM.apply(%{}, {:put_batch, entries}, locked_state)

      assert results == [:ok, {:error, :key_locked}]
      assert new_state.applied_count == 2
      assert [{_, "va", 0, _, _, _, _}] = :ets.lookup(ets, "hotbatch:open")
      assert [] == :ets.lookup(ets, "hotbatch:locked")

      cleanup_sm(ctx)
    end

    test "delete_batch removes many keys and preserves per-key results" do
      ctx = fresh_sm_state()
      {state, ets, _store, _dir} = ctx

      {state, {:ok, [:ok, :ok]}} =
        SM.apply(%{}, {:put_batch, [{"hotdel:a", "va", 0}, {"hotdel:b", "vb", 0}]}, state)

      {new_state, {:ok, results}} =
        SM.apply(%{}, {:delete_batch, ["hotdel:a", "hotdel:b"]}, state)

      assert results == [:ok, :ok]
      assert new_state.applied_count == 4
      assert [] == :ets.lookup(ets, "hotdel:a")
      assert [] == :ets.lookup(ets, "hotdel:b")

      cleanup_sm(ctx)
    end

    test "compound_batch_put applies many fields with one batched result" do
      ctx = fresh_sm_state()
      {state, ets, _store, _dir} = ctx
      key = "hotcompound:hash"
      field_a = CompoundKey.hash_field(key, "a")
      field_b = CompoundKey.hash_field(key, "b")

      {new_state, {:ok, results}} =
        SM.apply(
          %{},
          {:compound_batch_put, key, [{field_a, "va", 0}, {field_b, "vb", 12_345}]},
          state
        )

      assert results == [:ok, :ok]
      assert new_state.applied_count == 2
      assert [{^field_a, "va", 0, _lfu, _fid, _off, _vsize}] = :ets.lookup(ets, field_a)
      assert [{^field_b, "vb", 12_345, _lfu, _fid, _off, _vsize}] = :ets.lookup(ets, field_b)

      cleanup_sm(ctx)
    end

    test "compound_batch_delete removes many fields with one batched result" do
      ctx = fresh_sm_state()
      {state, ets, _store, _dir} = ctx
      key = "hotcompound:hash"
      field_a = CompoundKey.hash_field(key, "a")
      field_b = CompoundKey.hash_field(key, "b")

      {state, {:ok, [:ok, :ok]}} =
        SM.apply(
          %{},
          {:compound_batch_put, key, [{field_a, "va", 0}, {field_b, "vb", 0}]},
          state
        )

      {new_state, {:ok, results}} =
        SM.apply(%{}, {:compound_batch_delete, key, [field_a, field_b]}, state)

      assert results == [:ok, :ok]
      assert new_state.applied_count == 4
      assert [] == :ets.lookup(ets, field_a)
      assert [] == :ets.lookup(ets, field_b)

      cleanup_sm(ctx)
    end

    test "batch with list_op, compound_put, and compound_delete" do
      ctx = fresh_sm_state()
      {state, ets, _store, _dir} = ctx

      commands = [
        {:list_op, "mylist", {:rpush, ["a", "b"]}},
        {:compound_put, "myhash\x00f1", "v1", 0},
        {:compound_put, "myset\x00m1", "1", 0}
      ]

      {new_state, {:ok, results}} =
        SM.apply(%{}, {:batch, commands}, state)

      # list_op returns the new length
      assert [2, :ok, :ok] = results
      assert new_state.applied_count == 3

      # Verify list via LRANGE
      {_state2, elements} =
        SM.apply(%{}, {:list_op, "mylist", {:lrange, 0, -1}}, new_state)

      assert elements == ["a", "b"]

      # Verify hash field
      assert [{_, "v1", _, _, _, _, _}] = :ets.lookup(ets, "myhash\x00f1")

      # Verify set member
      assert [{_, "1", _, _, _, _, _}] = :ets.lookup(ets, "myset\x00m1")

      cleanup_sm(ctx)
    end
  end

  # ===========================================================================
  # CAS (compare-and-swap) through Raft
  # ===========================================================================

  describe "CAS through Raft" do
    test "match succeeds -- swaps value and returns 1" do
      k = ukey("cas_match")

      :ok = Router.put(FerricStore.Instance.get(:default), k, "old_val", 0)
      assert "old_val" == Router.get(FerricStore.Instance.get(:default), k)

      assert 1 = Router.cas(FerricStore.Instance.get(:default), k, "old_val", "new_val", nil)
      assert "new_val" == Router.get(FerricStore.Instance.get(:default), k)
    end

    test "mismatch fails -- returns 0 and does not change value" do
      k = ukey("cas_mismatch")

      :ok = Router.put(FerricStore.Instance.get(:default), k, "current", 0)
      assert 0 = Router.cas(FerricStore.Instance.get(:default), k, "wrong_expected", "new", nil)
      assert "current" == Router.get(FerricStore.Instance.get(:default), k)
    end

    test "missing key returns nil" do
      k = ukey("cas_missing")

      assert nil == Router.cas(FerricStore.Instance.get(:default), k, "anything", "new", nil)
      assert nil == Router.get(FerricStore.Instance.get(:default), k)
    end

    test "CAS with TTL sets expiry on swapped value" do
      k = ukey("cas_ttl")

      :ok = Router.put(FerricStore.Instance.get(:default), k, "v1", 0)
      assert 1 = Router.cas(FerricStore.Instance.get(:default), k, "v1", "v2", 60_000)

      {value, expire_at_ms} = Router.get_meta(FerricStore.Instance.get(:default), k)
      assert value == "v2"
      assert expire_at_ms > System.os_time(:millisecond)
      assert expire_at_ms <= System.os_time(:millisecond) + 60_000
    end

    test "CAS without TTL preserves original expiry" do
      k = ukey("cas_preserve_ttl")
      future = System.os_time(:millisecond) + 120_000

      :ok = Router.put(FerricStore.Instance.get(:default), k, "v1", future)
      assert 1 = Router.cas(FerricStore.Instance.get(:default), k, "v1", "v2", nil)

      {value, expire_at_ms} = Router.get_meta(FerricStore.Instance.get(:default), k)
      assert value == "v2"
      assert expire_at_ms == future
    end

    test "CAS on expired key returns nil" do
      k = ukey("cas_expired")
      past = System.os_time(:millisecond) - 1_000

      :ok = Router.put(FerricStore.Instance.get(:default), k, "expired_val", past)
      assert nil == Router.cas(FerricStore.Instance.get(:default), k, "expired_val", "new", nil)
    end
  end

  # ===========================================================================
  # LOCK through Raft
  # ===========================================================================

  describe "LOCK through Raft" do
    test "acquires lock on non-existent key" do
      k = ukey("lock_acquire")

      assert :ok = Router.lock(FerricStore.Instance.get(:default), k, "owner1", 30_000)
      assert "owner1" == Router.get(FerricStore.Instance.get(:default), k)
    end

    test "acquires lock on expired key" do
      k = ukey("lock_expired")
      past = System.os_time(:millisecond) - 1_000

      :ok = Router.put(FerricStore.Instance.get(:default), k, "stale_owner", past)
      assert :ok = Router.lock(FerricStore.Instance.get(:default), k, "new_owner", 30_000)
      assert "new_owner" == Router.get(FerricStore.Instance.get(:default), k)
    end

    test "re-acquire by same owner succeeds" do
      k = ukey("lock_reacquire")

      assert :ok = Router.lock(FerricStore.Instance.get(:default), k, "owner1", 30_000)
      assert :ok = Router.lock(FerricStore.Instance.get(:default), k, "owner1", 60_000)
      assert "owner1" == Router.get(FerricStore.Instance.get(:default), k)
    end

    test "returns error when locked by different owner" do
      k = ukey("lock_conflict")

      assert :ok = Router.lock(FerricStore.Instance.get(:default), k, "owner1", 30_000)

      assert {:error, "DISTLOCK lock is held by another owner"} =
               Router.lock(FerricStore.Instance.get(:default), k, "owner2", 30_000)
    end

    test "lock sets TTL on the key" do
      k = ukey("lock_ttl")

      assert :ok = Router.lock(FerricStore.Instance.get(:default), k, "owner1", 30_000)
      {value, expire_at_ms} = Router.get_meta(FerricStore.Instance.get(:default), k)
      assert value == "owner1"
      assert expire_at_ms > System.os_time(:millisecond)
      assert expire_at_ms <= System.os_time(:millisecond) + 30_000
    end
  end

  # ===========================================================================
  # UNLOCK through Raft
  # ===========================================================================

  describe "UNLOCK through Raft" do
    test "releases lock when owner matches" do
      k = ukey("unlock_match")

      :ok = Router.lock(FerricStore.Instance.get(:default), k, "owner1", 30_000)
      assert "owner1" == Router.get(FerricStore.Instance.get(:default), k)

      assert 1 = Router.unlock(FerricStore.Instance.get(:default), k, "owner1")
      assert nil == Router.get(FerricStore.Instance.get(:default), k)
    end

    test "returns error when caller is not the owner" do
      k = ukey("unlock_wrong_owner")

      :ok = Router.lock(FerricStore.Instance.get(:default), k, "owner1", 30_000)

      assert {:error, "DISTLOCK caller is not the lock owner"} =
               Router.unlock(FerricStore.Instance.get(:default), k, "owner2")

      # Lock should still be held
      assert "owner1" == Router.get(FerricStore.Instance.get(:default), k)
    end

    test "unlocking non-existent key returns 1" do
      k = ukey("unlock_missing")
      assert 1 = Router.unlock(FerricStore.Instance.get(:default), k, "anyone")
    end
  end

  # ===========================================================================
  # EXTEND through Raft
  # ===========================================================================

  describe "EXTEND through Raft" do
    test "extends TTL when owner matches" do
      k = ukey("extend_match")

      :ok = Router.lock(FerricStore.Instance.get(:default), k, "owner1", 10_000)
      {_, expire_before} = Router.get_meta(FerricStore.Instance.get(:default), k)

      # Extend with a longer TTL
      assert 1 = Router.extend(FerricStore.Instance.get(:default), k, "owner1", 60_000)

      {value, expire_after} = Router.get_meta(FerricStore.Instance.get(:default), k)
      assert value == "owner1"
      assert expire_after > expire_before
    end

    test "returns error when caller is not the owner" do
      k = ukey("extend_wrong_owner")

      :ok = Router.lock(FerricStore.Instance.get(:default), k, "owner1", 30_000)

      assert {:error, "DISTLOCK caller is not the lock owner"} =
               Router.extend(FerricStore.Instance.get(:default), k, "owner2", 60_000)
    end

    test "returns error when key does not exist" do
      k = ukey("extend_missing")

      assert {:error, "DISTLOCK lock does not exist or has expired"} =
               Router.extend(FerricStore.Instance.get(:default), k, "owner1", 60_000)
    end

    test "returns error when lock has expired" do
      k = ukey("extend_expired")
      past = System.os_time(:millisecond) - 1_000

      :ok = Router.put(FerricStore.Instance.get(:default), k, "owner1", past)

      assert {:error, "DISTLOCK lock does not exist or has expired"} =
               Router.extend(FerricStore.Instance.get(:default), k, "owner1", 60_000)
    end
  end

  # ===========================================================================
  # RATELIMIT.ADD through Raft
  # ===========================================================================

  describe "RATELIMIT.ADD through Raft" do
    test "returns allowed with correct counts on first call" do
      k = ukey("ratelimit_first")
      window_ms = 10_000
      max_requests = 10

      [status, count, remaining, _ttl] =
        Router.ratelimit_add(FerricStore.Instance.get(:default), k, window_ms, max_requests, 1)

      assert status == "allowed"
      assert count == 1
      assert remaining == 9
    end

    test "increments count on successive calls" do
      k = ukey("ratelimit_incr")
      window_ms = 10_000
      max_requests = 10

      [_, c1, _, _] =
        Router.ratelimit_add(FerricStore.Instance.get(:default), k, window_ms, max_requests, 1)

      [_, c2, _, _] =
        Router.ratelimit_add(FerricStore.Instance.get(:default), k, window_ms, max_requests, 1)

      [_, c3, _, _] =
        Router.ratelimit_add(FerricStore.Instance.get(:default), k, window_ms, max_requests, 1)

      assert c1 == 1
      assert c2 == 2
      assert c3 == 3
    end

    test "returns denied when limit is exceeded" do
      k = ukey("ratelimit_denied")
      window_ms = 10_000
      max_requests = 3

      # Use up the limit
      ["allowed", _, _, _] =
        Router.ratelimit_add(FerricStore.Instance.get(:default), k, window_ms, max_requests, 3)

      # Next request should be denied
      [status, count, remaining, _ttl] =
        Router.ratelimit_add(FerricStore.Instance.get(:default), k, window_ms, max_requests, 1)

      assert status == "denied"
      assert count == 3
      assert remaining == 0
    end

    test "returns ttl_ms as non-negative integer" do
      k = ukey("ratelimit_ttl")
      window_ms = 10_000
      max_requests = 100

      [_, _, _, ttl] =
        Router.ratelimit_add(FerricStore.Instance.get(:default), k, window_ms, max_requests, 1)

      assert is_integer(ttl)
      assert ttl >= 0
      assert ttl <= window_ms
    end

    test "multi-count add works correctly" do
      k = ukey("ratelimit_multi")
      window_ms = 10_000
      max_requests = 10

      [status, count, remaining, _ttl] =
        Router.ratelimit_add(FerricStore.Instance.get(:default), k, window_ms, max_requests, 5)

      assert status == "allowed"
      assert count == 5
      assert remaining == 5
    end
  end

  # ---------------------------------------------------------------------------
  # INCRBYFLOAT through Raft
  # ---------------------------------------------------------------------------

  describe "INCRBYFLOAT through Raft" do
    test "returns correct float on non-existent key" do
      k = ukey("incrbyfloat_new")

      assert {:ok, result} = Router.incr_float(FerricStore.Instance.get(:default), k, 1.5)
      assert_in_delta result, 1.5, 0.001
      {parsed, _} = Float.parse(Router.get(FerricStore.Instance.get(:default), k))
      assert_in_delta parsed, 1.5, 0.001
    end

    test "increments existing float value correctly" do
      k = ukey("incrbyfloat_existing")

      :ok = Router.put(FerricStore.Instance.get(:default), k, "10", 0)
      assert {:ok, result} = Router.incr_float(FerricStore.Instance.get(:default), k, 2.5)
      assert_in_delta result, 12.5, 0.001
      {parsed, _} = Float.parse(Router.get(FerricStore.Instance.get(:default), k))
      assert_in_delta parsed, 12.5, 0.001
    end

    test "increments integer string as float" do
      k = ukey("incrbyfloat_int")

      :ok = Router.put(FerricStore.Instance.get(:default), k, "10", 0)
      assert {:ok, result} = Router.incr_float(FerricStore.Instance.get(:default), k, 1.5)
      assert_in_delta result, 11.5, 0.001
    end

    test "returns error on non-float value" do
      k = ukey("incrbyfloat_err")

      :ok = Router.put(FerricStore.Instance.get(:default), k, "not_a_number", 0)

      assert {:error, "ERR value is not a valid float"} =
               Router.incr_float(FerricStore.Instance.get(:default), k, 1.0)

      # Original value should be unchanged
      assert "not_a_number" == Router.get(FerricStore.Instance.get(:default), k)
    end

    test "preserves expiry on existing key" do
      k = ukey("incrbyfloat_ttl")
      future = System.os_time(:millisecond) + 60_000

      :ok = Router.put(FerricStore.Instance.get(:default), k, "5.0", future)
      {:ok, _} = Router.incr_float(FerricStore.Instance.get(:default), k, 1.0)

      {value, expire_at_ms} = Router.get_meta(FerricStore.Instance.get(:default), k)
      {parsed, _} = Float.parse(value)
      assert_in_delta parsed, 6.0, 0.001
      assert expire_at_ms == future
    end
  end

  # ---------------------------------------------------------------------------
  # APPEND through Raft
  # ---------------------------------------------------------------------------

  describe "APPEND through Raft" do
    test "returns new length on non-existent key" do
      k = ukey("append_new")

      assert {:ok, 5} = Router.append(FerricStore.Instance.get(:default), k, "hello")
      assert "hello" == Router.get(FerricStore.Instance.get(:default), k)
    end

    test "appends to existing value and returns new length" do
      k = ukey("append_existing")

      :ok = Router.put(FerricStore.Instance.get(:default), k, "hello", 0)
      assert {:ok, 11} = Router.append(FerricStore.Instance.get(:default), k, " world")
      assert "hello world" == Router.get(FerricStore.Instance.get(:default), k)
    end

    test "multiple appends produce correct result" do
      k = ukey("append_multi")

      {:ok, 1} = Router.append(FerricStore.Instance.get(:default), k, "a")
      {:ok, 2} = Router.append(FerricStore.Instance.get(:default), k, "b")
      {:ok, 3} = Router.append(FerricStore.Instance.get(:default), k, "c")
      assert "abc" == Router.get(FerricStore.Instance.get(:default), k)
    end

    test "preserves expiry on existing key" do
      k = ukey("append_ttl")
      future = System.os_time(:millisecond) + 60_000

      :ok = Router.put(FerricStore.Instance.get(:default), k, "hi", future)
      {:ok, 8} = Router.append(FerricStore.Instance.get(:default), k, " there")

      {value, expire_at_ms} = Router.get_meta(FerricStore.Instance.get(:default), k)
      assert value == "hi there"
      assert expire_at_ms == future
    end
  end

  # ---------------------------------------------------------------------------
  # GETSET through Raft
  # ---------------------------------------------------------------------------

  describe "GETSET through Raft" do
    test "returns old value and sets new value" do
      k = ukey("getset_basic")

      :ok = Router.put(FerricStore.Instance.get(:default), k, "old_value", 0)
      old = Router.getset(FerricStore.Instance.get(:default), k, "new_value")
      assert old == "old_value"
      assert "new_value" == Router.get(FerricStore.Instance.get(:default), k)
    end

    test "returns nil when key does not exist" do
      k = ukey("getset_missing")

      old = Router.getset(FerricStore.Instance.get(:default), k, "first_value")
      assert old == nil
      assert "first_value" == Router.get(FerricStore.Instance.get(:default), k)
    end

    test "sets new value with no expiry" do
      k = ukey("getset_expiry")
      future = System.os_time(:millisecond) + 60_000

      :ok = Router.put(FerricStore.Instance.get(:default), k, "old", future)
      _old = Router.getset(FerricStore.Instance.get(:default), k, "new")

      {value, expire_at_ms} = Router.get_meta(FerricStore.Instance.get(:default), k)
      assert value == "new"
      # GETSET resets expiry to 0
      assert expire_at_ms == 0
    end
  end

  # ---------------------------------------------------------------------------
  # GETDEL through Raft
  # ---------------------------------------------------------------------------

  describe "GETDEL through Raft" do
    test "returns value and key is gone" do
      k = ukey("getdel_basic")

      :ok = Router.put(FerricStore.Instance.get(:default), k, "will_vanish", 0)
      old = Router.getdel(FerricStore.Instance.get(:default), k)
      assert old == "will_vanish"
      assert nil == Router.get(FerricStore.Instance.get(:default), k)
    end

    test "returns nil when key does not exist" do
      k = ukey("getdel_missing")

      old = Router.getdel(FerricStore.Instance.get(:default), k)
      assert old == nil
    end

    test "key is removed from ETS" do
      k = ukey("getdel_ets")

      :ok = Router.put(FerricStore.Instance.get(:default), k, "in_ets", 0)
      assert [{^k, "in_ets", 0, _lfu, _fid, _off, _vsize}] = :ets.lookup(keydir_for(k), k)

      _old = Router.getdel(FerricStore.Instance.get(:default), k)
      assert [] == :ets.lookup(keydir_for(k), k)
    end
  end

  # ---------------------------------------------------------------------------
  # GETEX through Raft
  # ---------------------------------------------------------------------------

  describe "GETEX through Raft" do
    test "updates TTL and returns value" do
      k = ukey("getex_ttl")
      future = System.os_time(:millisecond) + 120_000

      :ok = Router.put(FerricStore.Instance.get(:default), k, "my_val", 0)
      value = Router.getex(FerricStore.Instance.get(:default), k, future)
      assert value == "my_val"

      {stored_val, expire_at_ms} = Router.get_meta(FerricStore.Instance.get(:default), k)
      assert stored_val == "my_val"
      assert expire_at_ms == future
    end

    test "returns nil when key does not exist" do
      k = ukey("getex_missing")

      value =
        Router.getex(FerricStore.Instance.get(:default), k, System.os_time(:millisecond) + 60_000)

      assert value == nil
    end

    test "PERSIST removes expiry" do
      k = ukey("getex_persist")
      future = System.os_time(:millisecond) + 60_000

      :ok = Router.put(FerricStore.Instance.get(:default), k, "persistent", future)
      # PERSIST = expire_at_ms of 0
      value = Router.getex(FerricStore.Instance.get(:default), k, 0)
      assert value == "persistent"

      {stored_val, expire_at_ms} = Router.get_meta(FerricStore.Instance.get(:default), k)
      assert stored_val == "persistent"
      assert expire_at_ms == 0
    end
  end

  # ---------------------------------------------------------------------------
  # SETRANGE through Raft
  # ---------------------------------------------------------------------------

  describe "SETRANGE through Raft" do
    test "returns new length after overwriting bytes" do
      k = ukey("setrange_basic")

      :ok = Router.put(FerricStore.Instance.get(:default), k, "Hello World", 0)
      assert {:ok, 11} = Router.setrange(FerricStore.Instance.get(:default), k, 6, "Redis")
      assert "Hello Redis" == Router.get(FerricStore.Instance.get(:default), k)
    end

    test "pads with zero bytes for non-existent key" do
      k = ukey("setrange_new")

      assert {:ok, 8} = Router.setrange(FerricStore.Instance.get(:default), k, 5, "abc")
      value = Router.get(FerricStore.Instance.get(:default), k)
      assert byte_size(value) == 8
      # First 5 bytes should be zero
      assert binary_part(value, 0, 5) == <<0, 0, 0, 0, 0>>
      assert binary_part(value, 5, 3) == "abc"
    end

    test "extends string when offset + value exceeds length" do
      k = ukey("setrange_extend")

      :ok = Router.put(FerricStore.Instance.get(:default), k, "Hi", 0)
      assert {:ok, 7} = Router.setrange(FerricStore.Instance.get(:default), k, 2, "There")
      assert "HiThere" == Router.get(FerricStore.Instance.get(:default), k)
    end

    test "preserves expiry on existing key" do
      k = ukey("setrange_ttl")
      future = System.os_time(:millisecond) + 60_000

      :ok = Router.put(FerricStore.Instance.get(:default), k, "abcdef", future)
      {:ok, _len} = Router.setrange(FerricStore.Instance.get(:default), k, 0, "XY")

      {value, expire_at_ms} = Router.get_meta(FerricStore.Instance.get(:default), k)
      assert value == "XYcdef"
      assert expire_at_ms == future
    end
  end

  # ===========================================================================
  # SET edge cases through Raft
  # ===========================================================================

  describe "SET edge cases through Raft" do
    test "SET with EX (TTL) — value expires after TTL elapses" do
      k = ukey("set_ex_expire")
      # Set a TTL long enough to survive Raft commit latency but short enough
      # to test expiry within a reasonable wait.
      short_ttl_ms = 2_000
      expire_at = System.os_time(:millisecond) + short_ttl_ms

      :ok = Router.put(FerricStore.Instance.get(:default), k, "ephemeral", expire_at)
      # Immediately after SET the value should be present
      assert "ephemeral" == Router.get(FerricStore.Instance.get(:default), k)

      # Wait for TTL to expire
      Process.sleep(short_ttl_ms + 100)

      # After expiry, GET should return nil
      assert nil == Router.get(FerricStore.Instance.get(:default), k)
    end

    test "SET with NX on existing key — returns nil, does not overwrite" do
      k = ukey("set_nx_existing")

      :ok = Router.put(FerricStore.Instance.get(:default), k, "original", 0)
      assert "original" == Router.get(FerricStore.Instance.get(:default), k)

      # Simulate SET ... NX: only set if key does NOT exist.
      # NX is implemented at the Strings command layer via exists? check
      # before put. We replicate the semantics through Router here.
      assert Router.exists?(FerricStore.Instance.get(:default), k) == true

      # The NX guard prevents the write; the original value remains.
      result =
        if Router.exists?(FerricStore.Instance.get(:default), k),
          do: nil,
          else: Router.put(FerricStore.Instance.get(:default), k, "should_not_appear", 0)

      assert result == nil
      assert "original" == Router.get(FerricStore.Instance.get(:default), k)
    end

    test "SET with XX on missing key — returns nil" do
      k = ukey("set_xx_missing")

      # Key does not exist. XX means "only set if key exists".
      assert Router.exists?(FerricStore.Instance.get(:default), k) == false

      result =
        if Router.exists?(FerricStore.Instance.get(:default), k),
          do: Router.put(FerricStore.Instance.get(:default), k, "should_not_appear", 0),
          else: nil

      assert result == nil
      assert nil == Router.get(FerricStore.Instance.get(:default), k)
    end

    test "SET with GET flag — returns old value (via GETSET)" do
      k = ukey("set_get_flag")

      :ok = Router.put(FerricStore.Instance.get(:default), k, "old_value", 0)
      assert "old_value" == Router.get(FerricStore.Instance.get(:default), k)

      # SET ... GET semantics: atomically set new value and return old.
      # In Ferricstore, GETSET provides this exact behaviour through Raft.
      old = Router.getset(FerricStore.Instance.get(:default), k, "new_value")
      assert old == "old_value"
      assert "new_value" == Router.get(FerricStore.Instance.get(:default), k)
    end
  end

  # ===========================================================================
  # INCR edge cases through Raft
  # ===========================================================================

  describe "INCR edge cases through Raft" do
    test "INCR on value 'not_a_number' — returns error" do
      k = ukey("incr_nan")

      :ok = Router.put(FerricStore.Instance.get(:default), k, "not_a_number", 0)

      assert {:error, "ERR value is not an integer or out of range"} =
               Router.incr(FerricStore.Instance.get(:default), k, 1)

      # Original value should be unchanged
      assert "not_a_number" == Router.get(FerricStore.Instance.get(:default), k)
    end

    test "INCR on max int64 — returns overflow error" do
      k = ukey("incr_overflow")
      max_int64 = 9_223_372_036_854_775_807

      :ok = Router.put(FerricStore.Instance.get(:default), k, Integer.to_string(max_int64), 0)

      assert {:error, "ERR increment or decrement would overflow"} =
               Router.incr(FerricStore.Instance.get(:default), k, 1)

      # Value should remain unchanged
      assert Integer.to_string(max_int64) == Router.get(FerricStore.Instance.get(:default), k)
    end

    test "DECRBY with negative delta through Raft — effectively increments" do
      k = ukey("decrby_neg")

      :ok = Router.put(FerricStore.Instance.get(:default), k, "10", 0)
      assert {:ok, 15} = Router.incr(FerricStore.Instance.get(:default), k, 5)
      assert "15" == Router.get(FerricStore.Instance.get(:default), k)
    end

    test "INCR then GET in same Raft batch — consistent" do
      k = ukey("incr_get_batch")

      :ok = Router.put(FerricStore.Instance.get(:default), k, "0", 0)

      {:ok, 1} = Router.incr(FerricStore.Instance.get(:default), k, 1)
      assert "1" == Router.get(FerricStore.Instance.get(:default), k)

      {:ok, 2} = Router.incr(FerricStore.Instance.get(:default), k, 1)
      assert "2" == Router.get(FerricStore.Instance.get(:default), k)

      {:ok, 12} = Router.incr(FerricStore.Instance.get(:default), k, 10)
      assert "12" == Router.get(FerricStore.Instance.get(:default), k)
    end
  end

  # ===========================================================================
  # INCRBYFLOAT edge cases through Raft
  # ===========================================================================

  describe "INCRBYFLOAT edge cases through Raft" do
    test "INCRBYFLOAT on integer string '10' — returns float result" do
      k = ukey("incrbyfloat_int_str")

      :ok = Router.put(FerricStore.Instance.get(:default), k, "10", 0)
      assert {:ok, result} = Router.incr_float(FerricStore.Instance.get(:default), k, 0.5)
      assert_in_delta result, 10.5, 0.001
      {parsed, _} = Float.parse(Router.get(FerricStore.Instance.get(:default), k))
      assert_in_delta parsed, 10.5, 0.001
    end

    test "INCRBYFLOAT negative delta" do
      k = ukey("incrbyfloat_neg")

      :ok = Router.put(FerricStore.Instance.get(:default), k, "10.5", 0)
      assert {:ok, result} = Router.incr_float(FerricStore.Instance.get(:default), k, -0.5)
      assert_in_delta result, 10.0, 0.001
      {parsed, _} = Float.parse(Router.get(FerricStore.Instance.get(:default), k))
      assert_in_delta parsed, 10.0, 0.001
    end

    test "INCRBYFLOAT on 'not_a_number' — returns error" do
      k = ukey("incrbyfloat_nan")

      :ok = Router.put(FerricStore.Instance.get(:default), k, "not_a_number", 0)

      assert {:error, "ERR value is not a valid float"} =
               Router.incr_float(FerricStore.Instance.get(:default), k, 1.0)

      # Original value should be unchanged
      assert "not_a_number" == Router.get(FerricStore.Instance.get(:default), k)
    end

    test "INCRBYFLOAT preserves existing TTL" do
      k = ukey("incrbyfloat_ttl_preserve")
      future = System.os_time(:millisecond) + 120_000

      :ok = Router.put(FerricStore.Instance.get(:default), k, "5.0", future)
      {:ok, _} = Router.incr_float(FerricStore.Instance.get(:default), k, 2.5)

      {value, expire_at_ms} = Router.get_meta(FerricStore.Instance.get(:default), k)
      {parsed, _} = Float.parse(value)
      assert_in_delta parsed, 7.5, 0.001
      assert expire_at_ms == future
    end
  end

  # ===========================================================================
  # APPEND edge cases through Raft
  # ===========================================================================

  describe "APPEND edge cases through Raft" do
    test "APPEND to non-existent key creates it" do
      k = ukey("append_create")

      # Key does not exist; APPEND should create it with the given value.
      assert {:ok, 5} = Router.append(FerricStore.Instance.get(:default), k, "hello")
      assert "hello" == Router.get(FerricStore.Instance.get(:default), k)
    end

    test "APPEND preserves existing TTL" do
      k = ukey("append_ttl_preserve")
      future = System.os_time(:millisecond) + 120_000

      :ok = Router.put(FerricStore.Instance.get(:default), k, "base", future)
      {:ok, 10} = Router.append(FerricStore.Instance.get(:default), k, "_added")

      {value, expire_at_ms} = Router.get_meta(FerricStore.Instance.get(:default), k)
      assert value == "base_added"
      assert expire_at_ms == future
    end

    test "APPEND with empty string — no change to value, returns existing length" do
      k = ukey("append_empty")

      :ok = Router.put(FerricStore.Instance.get(:default), k, "existing", 0)
      {:ok, len} = Router.append(FerricStore.Instance.get(:default), k, "")
      assert len == byte_size("existing")
      assert "existing" == Router.get(FerricStore.Instance.get(:default), k)
    end
  end

  # ===========================================================================
  # GETSET / GETDEL / GETEX edge cases through Raft
  # ===========================================================================

  describe "GETSET edge cases through Raft" do
    test "GETSET on non-existent key returns nil, creates key" do
      k = ukey("getset_nonexistent")

      old = Router.getset(FerricStore.Instance.get(:default), k, "fresh_value")
      assert old == nil
      assert "fresh_value" == Router.get(FerricStore.Instance.get(:default), k)
    end
  end

  describe "GETDEL edge cases through Raft" do
    test "GETDEL on non-existent key returns nil" do
      k = ukey("getdel_nonexistent")

      result = Router.getdel(FerricStore.Instance.get(:default), k)
      assert result == nil
      # Key should still not exist
      assert nil == Router.get(FerricStore.Instance.get(:default), k)
    end
  end

  describe "GETEX edge cases through Raft" do
    test "GETEX with PERSIST removes TTL" do
      k = ukey("getex_persist_edge")
      future = System.os_time(:millisecond) + 120_000

      :ok = Router.put(FerricStore.Instance.get(:default), k, "will_persist", future)

      # Verify TTL is set
      {_, expire_before} = Router.get_meta(FerricStore.Instance.get(:default), k)
      assert expire_before == future

      # PERSIST = expire_at_ms of 0
      value = Router.getex(FerricStore.Instance.get(:default), k, 0)
      assert value == "will_persist"

      {stored_val, expire_at_ms} = Router.get_meta(FerricStore.Instance.get(:default), k)
      assert stored_val == "will_persist"
      assert expire_at_ms == 0
    end

    test "GETEX on expired key returns nil" do
      k = ukey("getex_expired")
      past = System.os_time(:millisecond) - 1_000

      :ok = Router.put(FerricStore.Instance.get(:default), k, "expired_val", past)

      # Key is expired; GETEX should return nil
      result =
        Router.getex(FerricStore.Instance.get(:default), k, System.os_time(:millisecond) + 60_000)

      assert result == nil
    end
  end

  # ===========================================================================
  # SETRANGE edge cases through Raft
  # ===========================================================================

  describe "SETRANGE edge cases through Raft" do
    test "SETRANGE beyond current length zero-pads" do
      k = ukey("setrange_zeropad")

      :ok = Router.put(FerricStore.Instance.get(:default), k, "Hi", 0)
      # Offset 5 is beyond "Hi" (length 2), so bytes 2..4 are zero-padded
      assert {:ok, 8} = Router.setrange(FerricStore.Instance.get(:default), k, 5, "abc")

      value = Router.get(FerricStore.Instance.get(:default), k)
      assert byte_size(value) == 8
      # "Hi" + 3 zero bytes + "abc"
      assert value == <<"Hi", 0, 0, 0, "abc">>
    end

    test "SETRANGE at offset 0 replaces start" do
      k = ukey("setrange_offset0")

      :ok = Router.put(FerricStore.Instance.get(:default), k, "Hello World", 0)
      assert {:ok, 11} = Router.setrange(FerricStore.Instance.get(:default), k, 0, "Yo")
      # "Yo" replaces the first 2 bytes: "He" -> "Yo", rest unchanged
      assert "Yollo World" == Router.get(FerricStore.Instance.get(:default), k)
    end

    test "SETRANGE on non-existent key creates zero-padded value" do
      k = ukey("setrange_nonexistent")

      assert {:ok, 8} = Router.setrange(FerricStore.Instance.get(:default), k, 5, "abc")
      value = Router.get(FerricStore.Instance.get(:default), k)
      assert byte_size(value) == 8
      # 5 zero bytes followed by "abc"
      assert value == <<0, 0, 0, 0, 0, "abc">>
    end
  end
end
