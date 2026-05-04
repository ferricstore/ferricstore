defmodule Ferricstore.WritePathNosyncTest do
  @moduledoc """
  Tests for StateMachine nosync Bitcask write + background BitcaskWriter,
  extracted from WritePathOptimizationsTest.
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Store.{BitcaskWriter, Router}
  alias Ferricstore.Test.ShardHelpers

  setup_all do
    ShardHelpers.wait_shards_alive()
    :ok
  end

  setup do
    ShardHelpers.flush_all_keys()

    on_exit(fn ->
      ShardHelpers.wait_shards_alive()
    end)
  end

  # Helper: unique key with a given prefix
  defp ukey(prefix), do: "#{prefix}:#{:rand.uniform(9_999_999)}"

  # =========================================================================
  # StateMachine nosync Bitcask write + background BitcaskWriter
  # =========================================================================

  describe "StateMachine nosync + BitcaskWriter" do
    test "SET then GET returns correct value" do
      key = ukey("sm")
      assert :ok = Router.put(FerricStore.Instance.get(:default), key, "hello", 0)
      assert "hello" == Router.get(FerricStore.Instance.get(:default), key)
    end

    test "SET overwrites previous value" do
      key = ukey("sm")
      Router.put(FerricStore.Instance.get(:default), key, "first", 0)
      Router.put(FerricStore.Instance.get(:default), key, "second", 0)
      assert "second" == Router.get(FerricStore.Instance.get(:default), key)
    end

    test "DEL removes key" do
      key = ukey("sm")
      Router.put(FerricStore.Instance.get(:default), key, "val", 0)
      Router.delete(FerricStore.Instance.get(:default), key)
      assert nil == Router.get(FerricStore.Instance.get(:default), key)
    end

    test "SET with TTL — key expires correctly" do
      key = ukey("sm")
      expire_at = System.os_time(:millisecond) + 100
      Router.put(FerricStore.Instance.get(:default), key, "ephemeral", expire_at)

      assert "ephemeral" == Router.get(FerricStore.Instance.get(:default), key)
      Process.sleep(150)
      assert nil == Router.get(FerricStore.Instance.get(:default), key)
    end

    test "after BitcaskWriter flush, data is readable from cold path" do
      key = ukey("sm")
      Router.put(FerricStore.Instance.get(:default), key, "persistent", 0)
      BitcaskWriter.flush_all()

      # Verify ETS has a non-pending file_id after flush
      idx = Router.shard_for(FerricStore.Instance.get(:default), key)
      keydir = :"keydir_#{idx}"

      case :ets.lookup(keydir, key) do
        [{^key, _val, _exp, _lfu, fid, _off, _vsize}] ->
          assert fid != :pending

        [] ->
          flunk("Key not found in keydir after flush")
      end
    end

    test "SET large value (> 64KB) uses synchronous path" do
      key = ukey("sm")
      large_value = String.duplicate("x", 70_000)
      assert :ok = Router.put(FerricStore.Instance.get(:default), key, large_value, 0)

      # Large values go through the synchronous NIF path and store nil in ETS
      idx = Router.shard_for(FerricStore.Instance.get(:default), key)
      keydir = :"keydir_#{idx}"

      [{^key, ets_val, _exp, _lfu, fid, _off, vsize}] = :ets.lookup(keydir, key)

      # Value should be nil (cold) and file_id should NOT be :pending
      assert ets_val == nil
      assert fid != :pending
      assert vsize == byte_size(large_value)

      # GET should still return the full value (cold read from disk)
      assert large_value == Router.get(FerricStore.Instance.get(:default), key)
    end

    test "SET small value goes through background writer with :pending marker" do
      key = ukey("sm")
      small_value = "tiny"

      Router.put(FerricStore.Instance.get(:default), key, small_value, 0)

      # The value should be readable immediately (hot from ETS)
      assert small_value == Router.get(FerricStore.Instance.get(:default), key)

      # After flush, file_id should be updated from :pending to a real id
      BitcaskWriter.flush_all()

      idx = Router.shard_for(FerricStore.Instance.get(:default), key)
      keydir = :"keydir_#{idx}"
      [{^key, _val, _exp, _lfu, fid, _off, _vsize}] = :ets.lookup(keydir, key)
      assert fid != :pending
    end

    test "SET empty string value" do
      key = ukey("sm")
      Router.put(FerricStore.Instance.get(:default), key, "", 0)
      assert "" == Router.get(FerricStore.Instance.get(:default), key)
    end

    test "SET binary value with null bytes" do
      key = ukey("sm")
      value = <<0, 1, 2, 0, 255, 0>>
      Router.put(FerricStore.Instance.get(:default), key, value, 0)
      assert value == Router.get(FerricStore.Instance.get(:default), key)
    end

    test "SET very long key (10KB)" do
      key = String.duplicate("k", 10_000) <> ":#{:rand.uniform(999_999)}"
      Router.put(FerricStore.Instance.get(:default), key, "val", 0)
      assert "val" == Router.get(FerricStore.Instance.get(:default), key)
    end

    test "SET then immediate GET (read-your-own-writes)" do
      key = ukey("sm")
      Router.put(FerricStore.Instance.get(:default), key, "ryow", 0)
      # No flush — read should come from ETS hot cache
      assert "ryow" == Router.get(FerricStore.Instance.get(:default), key)
    end

    test "SET from process A, GET from process B" do
      key = ukey("sm")

      task =
        Task.async(fn ->
          Router.put(FerricStore.Instance.get(:default), key, "cross_process", 0)
        end)

      Task.await(task)

      # Read from this process
      assert "cross_process" == Router.get(FerricStore.Instance.get(:default), key)
    end

    test "50 concurrent SETs to different keys all succeed" do
      keys =
        for i <- 1..50 do
          key = ukey("conc#{i}")
          {key, "val_#{i}"}
        end

      tasks =
        Enum.map(keys, fn {k, v} ->
          Task.async(fn -> Router.put(FerricStore.Instance.get(:default), k, v, 0) end)
        end)

      results = Enum.map(tasks, &Task.await(&1, 10_000))
      assert Enum.all?(results, &(&1 == :ok))

      BitcaskWriter.flush_all()

      for {k, v} <- keys do
        assert v == Router.get(FerricStore.Instance.get(:default), k)
      end
    end

    test "50 concurrent SETs to same key — last write wins, no crash" do
      key = ukey("race")

      tasks =
        for i <- 1..50 do
          Task.async(fn -> Router.put(FerricStore.Instance.get(:default), key, "val_#{i}", 0) end)
        end

      Enum.each(tasks, &Task.await(&1, 10_000))

      # Should have some value, not nil
      value = Router.get(FerricStore.Instance.get(:default), key)
      assert value != nil
      assert String.starts_with?(value, "val_")
    end

    test "INCR 50 times concurrently — final value is 50" do
      key = ukey("incr")
      Router.put(FerricStore.Instance.get(:default), key, "0", 0)

      tasks =
        for _ <- 1..50 do
          Task.async(fn -> Router.incr(FerricStore.Instance.get(:default), key, 1) end)
        end

      Enum.each(tasks, &Task.await(&1, 10_000))

      assert {:ok, final} = Router.incr(FerricStore.Instance.get(:default), key, 0)
      assert final == 50
    end
  end

  # =========================================================================
  # Background BitcaskWriter
  # =========================================================================

  describe "background BitcaskWriter" do
    test "flush with no pending writes returns immediately" do
      # Should not hang or error
      assert :ok = BitcaskWriter.flush_all()
    end

    test "rapid writes all eventually reach disk" do
      keys =
        for i <- 1..100 do
          k = ukey("rapid#{i}")
          Router.put(FerricStore.Instance.get(:default), k, "v#{i}", 0)
          k
        end

      BitcaskWriter.flush_all()

      for k <- keys do
        idx = Router.shard_for(FerricStore.Instance.get(:default), k)
        keydir = :"keydir_#{idx}"

        case :ets.lookup(keydir, k) do
          [{^k, _v, _e, _lfu, fid, _off, _vsize}] ->
            assert fid != :pending, "Key #{k} still has :pending file_id after flush"

          [] ->
            flunk("Key #{k} not found in keydir after flush")
        end
      end
    end

    test "flush updates small-value keydir entries with usable disk locations" do
      ctx = FerricStore.Instance.get(:default)
      key = ukey("writer_location")
      value = "disk-loc"

      assert :ok = Router.put(ctx, key, value, 0)
      BitcaskWriter.flush_all()

      idx = Router.shard_for(ctx, key)
      keydir = :"keydir_#{idx}"
      [{^key, ^value, 0, _lfu, fid, off, vsize}] = :ets.lookup(keydir, key)

      assert is_integer(fid)
      assert vsize == byte_size(value)

      shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, idx)
      path = Path.join(shard_path, "#{String.pad_leading(Integer.to_string(fid), 5, "0")}.log")

      assert {:ok, ^value} = Ferricstore.Bitcask.NIF.v2_pread_at(path, off)
    end

    test "flush marks the supplied instance shard dirty for the Bitcask checkpointer" do
      shard_index = 20
      dir = Path.join(System.tmp_dir!(), "bitcask_writer_checkpoint_#{shard_index}")
      File.mkdir_p!(dir)
      path = Path.join(dir, "00000.log")
      File.touch!(path)

      keydir = :ets.new(:"bitcask_writer_checkpoint_#{shard_index}", [:set, :public])
      ctx = %{checkpoint_flags: :atomics.new(shard_index + 1, signed: false)}
      key = "writer:checkpoint"
      value = "checkpoint-me"

      {:ok, writer} = BitcaskWriter.start_link(shard_index: shard_index)

      on_exit(fn ->
        if Process.alive?(writer), do: GenServer.stop(writer, :normal, 5000)

        try do
          :ets.delete(keydir)
        rescue
          ArgumentError -> :ok
        end

        File.rm_rf(dir)
      end)

      :ets.insert(keydir, {key, value, 0, Ferricstore.Store.LFU.initial(), :pending, 0, 0})
      :atomics.put(ctx.checkpoint_flags, shard_index + 1, 0)

      assert :ok = BitcaskWriter.write(ctx, shard_index, path, 0, keydir, key, value, 0)
      assert :ok = BitcaskWriter.flush(shard_index)

      assert :atomics.get(ctx.checkpoint_flags, shard_index + 1) == 1,
             "BitcaskWriter nosync append must wake the BitcaskCheckpointer"
    end

    test "tombstone flush marks the supplied instance shard dirty for the Bitcask checkpointer" do
      shard_index = 21
      dir = Path.join(System.tmp_dir!(), "bitcask_writer_tombstone_checkpoint_#{shard_index}")
      File.mkdir_p!(dir)
      path = Path.join(dir, "00000.log")
      File.touch!(path)

      ctx = %{checkpoint_flags: :atomics.new(shard_index + 1, signed: false)}
      key = "writer:tombstone-checkpoint"

      {:ok, writer} = BitcaskWriter.start_link(shard_index: shard_index)

      on_exit(fn ->
        if Process.alive?(writer), do: GenServer.stop(writer, :normal, 5000)
        File.rm_rf(dir)
      end)

      :atomics.put(ctx.checkpoint_flags, shard_index + 1, 0)

      assert :ok = BitcaskWriter.delete(ctx, shard_index, path, key)
      assert :ok = BitcaskWriter.flush(shard_index)

      assert :atomics.get(ctx.checkpoint_flags, shard_index + 1) == 1,
             "BitcaskWriter tombstone append must wake the BitcaskCheckpointer"
    end

    test "failed flush marks the supplied instance under disk pressure" do
      ctx = Ferricstore.Test.IsolatedInstance.checkout()

      on_exit(fn ->
        Ferricstore.Store.DiskPressure.clear(ctx, 0)
        Ferricstore.Store.DiskPressure.clear(0)
        Ferricstore.Test.IsolatedInstance.checkin(ctx)
      end)

      keydir = elem(ctx.keydir_refs, 0)
      key = "writer_pressure:#{System.unique_integer([:positive])}"

      missing_dir =
        Path.join(System.tmp_dir!(), "missing_writer_dir_#{System.unique_integer([:positive])}")

      missing_path = Path.join(missing_dir, "00000.log")

      Ferricstore.Store.DiskPressure.clear(ctx, 0)
      Ferricstore.Store.DiskPressure.clear(0)

      BitcaskWriter.write(ctx, 0, missing_path, 0, keydir, key, "value", 0)
      BitcaskWriter.flush(0)

      assert Ferricstore.Store.DiskPressure.under_pressure?(ctx, 0)
    end

    test "BitcaskWriter writer_name returns correct atom" do
      assert BitcaskWriter.writer_name(0) == :"Ferricstore.Store.BitcaskWriter.0"
      assert BitcaskWriter.writer_name(3) == :"Ferricstore.Store.BitcaskWriter.3"
    end

    test "BitcaskWriter writer_name is instance-scoped for custom instances" do
      ctx = %{name: :"writer_instance_#{System.unique_integer([:positive])}"}
      shard_index = 70_000 + :rand.uniform(9_999)

      assert BitcaskWriter.writer_name(ctx, shard_index) != BitcaskWriter.writer_name(shard_index)

      {:ok, pid} = BitcaskWriter.start_link(shard_index: shard_index, instance_ctx: ctx)

      try do
        assert Process.whereis(BitcaskWriter.writer_name(ctx, shard_index)) == pid
        assert Process.whereis(BitcaskWriter.writer_name(shard_index)) == nil
      after
        if Process.alive?(pid), do: GenServer.stop(pid)
      end
    end

    test "all 4 BitcaskWriter processes are alive" do
      for i <- 0..3 do
        name = BitcaskWriter.writer_name(i)
        pid = Process.whereis(name)
        assert is_pid(pid), "BitcaskWriter.#{i} is not registered"
        assert Process.alive?(pid), "BitcaskWriter.#{i} is not alive"
      end
    end

    test "DELETE on key with pending background write flushes first" do
      # This tests the flush_pending_for_key path in StateMachine
      key = ukey("delpend")
      Router.put(FerricStore.Instance.get(:default), key, "val", 0)
      # Immediately delete — the StateMachine should flush the pending write
      # before writing the tombstone
      Router.delete(FerricStore.Instance.get(:default), key)
      BitcaskWriter.flush_all()

      assert nil == Router.get(FerricStore.Instance.get(:default), key)
    end
  end
end
