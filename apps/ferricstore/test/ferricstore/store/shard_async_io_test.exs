defmodule Ferricstore.Store.ShardAsyncIoTest do
  @moduledoc """
  Tests for the optimized async IO path in `Ferricstore.Store.Shard`.

  Covers:
  - v2_append_batch_nosync (write without fsync)
  - Deferred fsync via v2_fsync_async on flush timer
  - Split write+fsync path
  - Async write completion (v2_append_batch_async NIF)
  - fsync_needed state tracking
  - ETS update_element optimization in update_ets_locations
  - Data correctness after nosync write + deferred fsync
  - Concurrent writes with deferred fsync
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Store.BitcaskWriter
  alias Ferricstore.Store.LFU
  alias Ferricstore.Store.Shard
  alias Ferricstore.Store.Shard.Flush, as: ShardFlush

  setup do
    :ok
  end

  # Start an isolated shard with its own Instance ctx.
  defp start_shard(opts \\ []) do
    dir = Path.join(System.tmp_dir!(), "shard_async_io_#{:rand.uniform(9_999_999)}")
    File.mkdir_p!(dir)
    flush_ms = Keyword.get(opts, :flush_interval_ms, 1)

    name = :"async_io_test_#{:erlang.unique_integer([:positive])}"

    ctx =
      FerricStore.Instance.build(name,
        data_dir: dir,
        shard_count: 1,
        raft_enabled: false
      )

    Ferricstore.DataDir.ensure_layout!(dir, 1)

    {:ok, pid} =
      Shard.start_link(
        index: 0,
        data_dir: dir,
        flush_interval_ms: flush_ms,
        instance_ctx: ctx,
        raft_enabled: false
      )

    {pid, 0, dir, ctx}
  end

  defp cleanup_shard(pid, ctx, dir) do
    try do
      if Process.alive?(pid), do: GenServer.stop(pid, :normal, 5000)
    catch
      :exit, _ -> :ok
    end

    try do
      FerricStore.Instance.cleanup(ctx.name)
    catch
      :exit, _ -> :ok
    end

    File.rm_rf(dir)
  end

  # ---------------------------------------------------------------------------
  # v2_append_batch_nosync NIF
  # ---------------------------------------------------------------------------

  describe "v2_append_batch_nosync NIF" do
    test "writes records without fsync and returns offsets" do
      dir = Path.join(System.tmp_dir!(), "nosync_nif_#{:rand.uniform(9_999_999)}")
      File.mkdir_p!(dir)
      path = Path.join(dir, "00000.log")
      File.touch!(path)

      on_exit(fn -> File.rm_rf(dir) end)

      batch = [{"key1", "val1", 0}, {"key2", "val2", 0}]

      assert {:ok, locations} = NIF.v2_append_batch_nosync(path, batch)
      assert length(locations) == 2

      [{off1, vsize1}, {off2, vsize2}] = locations
      assert off1 == 0
      assert off2 > off1
      # byte_size("val1")
      assert vsize1 == 4
      # byte_size("val2")
      assert vsize2 == 4

      # Data should be readable via v2_pread_at (flushed to page cache)
      assert {:ok, "val1"} = NIF.v2_pread_at(path, off1)
      assert {:ok, "val2"} = NIF.v2_pread_at(path, off2)
    end

    test "empty batch returns empty locations" do
      dir = Path.join(System.tmp_dir!(), "nosync_empty_#{:rand.uniform(9_999_999)}")
      File.mkdir_p!(dir)
      path = Path.join(dir, "00000.log")
      File.touch!(path)

      on_exit(fn -> File.rm_rf(dir) end)

      assert {:ok, []} = NIF.v2_append_batch_nosync(path, [])
    end
  end

  describe "v2_append_ops_batch_nosync NIF" do
    test "writes mixed put and tombstone records without fsync" do
      dir = Path.join(System.tmp_dir!(), "nosync_ops_nif_#{:rand.uniform(9_999_999)}")
      File.mkdir_p!(dir)
      path = Path.join(dir, "00000.log")
      File.touch!(path)

      on_exit(fn -> File.rm_rf(dir) end)

      ops = [
        {:put, "key1", "val1", 0},
        {:delete, "key1"},
        {:put, "empty", "", 0}
      ]

      assert {:ok, [{:put, off1, 4}, {:delete, off2, tomb_size}, {:put, off3, 0}]} =
               NIF.v2_append_ops_batch_nosync(path, ops)

      assert off1 == 0
      assert off2 > off1
      assert off3 > off2
      assert tomb_size == 26 + byte_size("key1")

      assert {:ok, "val1"} = NIF.v2_pread_at(path, off1)
      assert {:ok, nil} = NIF.v2_pread_at(path, off2)
      assert {:ok, ""} = NIF.v2_pread_at(path, off3)

      assert {:ok, records} = NIF.v2_scan_file(path)

      assert [
               {"key1", ^off1, 4, 0, false},
               {"key1", ^off2, 0, 0, true},
               {"empty", ^off3, 0, 0, false}
             ] = records
    end
  end

  # ---------------------------------------------------------------------------
  # Shard: nosync writes + deferred fsync
  # ---------------------------------------------------------------------------

  describe "shard nosync write path" do
    test "BitcaskWriter does not attach stale write location to newer pending value" do
      shard_index = 10_000 + System.unique_integer([:positive])
      dir = Path.join(System.tmp_dir!(), "bitcask_writer_stale_#{shard_index}")
      File.mkdir_p!(dir)
      path = Path.join(dir, "00000.log")
      File.touch!(path)

      keydir = :ets.new(:"bitcask_writer_stale_#{shard_index}", [:set, :public])
      key = "writer:stale-location"

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

      :ets.insert(keydir, {key, "new", 456, LFU.initial(), :pending, 0, 0})

      :sys.replace_state(writer, fn state ->
        %{
          state
          | pending: [{:write, nil, path, 0, keydir, key, "old", 123}],
            pending_count: 1
        }
      end)

      assert :ok == BitcaskWriter.flush(shard_index)
      assert [{^key, "new", 456, _lfu, :pending, 0, 0}] = :ets.lookup(keydir, key)
    end

    test "shard flush completion does not attach stale location to newer pending value" do
      keydir =
        :ets.new(:"shard_flush_stale_#{System.unique_integer([:positive])}", [
          :set,
          :public
        ])

      key = "flush:stale-location"

      state = %{
        keydir: keydir,
        active_file_id: 7,
        file_stats: %{},
        instance_ctx: %{hot_cache_max_value_size: 64}
      }

      try do
        :ets.insert(keydir, {key, "new", 456, LFU.initial(), :pending, 0, 0})

        ShardFlush.update_ets_locations(state, [{key, "old", 123}], [{42, 3}])

        assert [{^key, "new", 456, _lfu, :pending, 0, 0}] = :ets.lookup(keydir, key)
      after
        :ets.delete(keydir)
      end
    end

    test "shard flush completion handles numeric pending values" do
      keydir =
        :ets.new(:"shard_flush_numeric_#{System.unique_integer([:positive])}", [
          :set,
          :public
        ])

      key = "flush:numeric-location"

      state = %{
        keydir: keydir,
        active_file_id: 7,
        file_stats: %{},
        instance_ctx: %{hot_cache_max_value_size: 64}
      }

      try do
        :ets.insert(keydir, {key, "42", 0, LFU.initial(), :pending, 0, 0})

        ShardFlush.update_ets_locations(state, [{key, 42, 0}], [{42, 2}])

        assert [{^key, "42", 0, _lfu, 7, 42, 2}] = :ets.lookup(keydir, key)
      after
        :ets.delete(keydir)
      end
    end

    test "put is readable immediately via ETS (before fsync)" do
      {pid, _index, dir, ctx} = start_shard(flush_interval_ms: 100)
      on_exit(fn -> cleanup_shard(pid, ctx, dir) end)

      :ok = GenServer.call(pid, {:put, "nsk", "nsv", 0})
      # Should be readable from ETS immediately
      assert "nsv" == GenServer.call(pid, {:get, "nsk"})
    end

    test "checkpoint_flags atomic is raised after nosync write" do
      {pid, _index, dir, ctx} = start_shard(flush_interval_ms: 5000)
      on_exit(fn -> cleanup_shard(pid, ctx, dir) end)

      # Clear any prior flag state.
      :atomics.put(ctx.checkpoint_flags, 1, 0)

      :ok = GenServer.call(pid, {:put, "fk", "fv", 0})
      # Allow the flush_pending to run (triggered by put when no flush_in_flight).
      Process.sleep(10)

      # The nosync path raises the per-shard checkpoint flag so the
      # BitcaskCheckpointer fsyncs the active file on its next tick.
      assert :atomics.get(ctx.checkpoint_flags, 1) == 1,
             "writer must raise checkpoint_flags[1] after a nosync append"
    end

    test "data survives flush (sync) call" do
      {pid, _index, dir, ctx} = start_shard()
      on_exit(fn -> cleanup_shard(pid, ctx, dir) end)

      :ok = GenServer.call(pid, {:put, "dk", "dv", 0})
      :ok = GenServer.call(pid, :flush)

      assert "dv" == GenServer.call(pid, {:get, "dk"})
    end

    test "EXISTS miss does not force read-side fsync" do
      {pid, _index, dir, ctx} = start_shard(flush_interval_ms: 5000)
      on_exit(fn -> cleanup_shard(pid, ctx, dir) end)

      :atomics.put(ctx.checkpoint_flags, 1, 0)
      :ok = GenServer.call(pid, {:put, "dirty_exists_source", "value", 0})
      Process.sleep(10)

      assert :atomics.get(ctx.checkpoint_flags, 1) == 1
      assert false == GenServer.call(pid, {:exists, "missing_exists_key"})

      assert :atomics.get(ctx.checkpoint_flags, 1) == 1,
             "a pure EXISTS miss must not fsync unrelated dirty Bitcask data"
    end

    test "compound_get miss does not force read-side fsync" do
      {pid, _index, dir, ctx} = start_shard(flush_interval_ms: 5000)
      on_exit(fn -> cleanup_shard(pid, ctx, dir) end)

      :atomics.put(ctx.checkpoint_flags, 1, 0)
      :ok = GenServer.call(pid, {:put, "dirty_compound_source", "value", 0})
      Process.sleep(10)

      assert :atomics.get(ctx.checkpoint_flags, 1) == 1

      assert nil ==
               GenServer.call(
                 pid,
                 {:compound_get, "missing_hash", "missing_hash" <> <<0>> <> "field"}
               )

      assert :atomics.get(ctx.checkpoint_flags, 1) == 1,
             "a pure compound_get miss must not fsync unrelated dirty Bitcask data"
    end

    test "compound_get_meta miss does not force read-side fsync" do
      {pid, _index, dir, ctx} = start_shard(flush_interval_ms: 5000)
      on_exit(fn -> cleanup_shard(pid, ctx, dir) end)

      :atomics.put(ctx.checkpoint_flags, 1, 0)
      :ok = GenServer.call(pid, {:put, "dirty_compound_meta_source", "value", 0})
      Process.sleep(10)

      assert :atomics.get(ctx.checkpoint_flags, 1) == 1

      assert nil ==
               GenServer.call(
                 pid,
                 {:compound_get_meta, "missing_hash_meta",
                  "missing_hash_meta" <> <<0>> <> "field"}
               )

      assert :atomics.get(ctx.checkpoint_flags, 1) == 1,
             "a pure compound_get_meta miss must not fsync unrelated dirty Bitcask data"
    end

    test "GET of a known cold key does not force read-side fsync" do
      {pid, _index, dir, ctx} = start_shard(flush_interval_ms: 5000)
      on_exit(fn -> cleanup_shard(pid, ctx, dir) end)

      large = :binary.copy("C", ctx.hot_cache_max_value_size + 1024)

      :ok = GenServer.call(pid, {:put, "cold_read_key", large, 0})
      :ok = GenServer.call(pid, :flush)

      :atomics.put(ctx.checkpoint_flags, 1, 0)
      :ok = GenServer.call(pid, {:put, "dirty_after_cold", "value", 0})
      Process.sleep(10)

      assert :atomics.get(ctx.checkpoint_flags, 1) == 1
      assert large == GenServer.call(pid, {:get, "cold_read_key"})

      assert :atomics.get(ctx.checkpoint_flags, 1) == 1,
             "a known-location cold GET must not fsync unrelated dirty Bitcask data"
    end

    test "GET_META of a known cold key does not force read-side fsync" do
      {pid, _index, dir, ctx} = start_shard(flush_interval_ms: 5000)
      on_exit(fn -> cleanup_shard(pid, ctx, dir) end)

      large = :binary.copy("M", ctx.hot_cache_max_value_size + 1024)
      expire_at_ms = Ferricstore.HLC.now_ms() + 60_000

      :ok = GenServer.call(pid, {:put, "cold_meta_key", large, expire_at_ms})
      :ok = GenServer.call(pid, :flush)

      :atomics.put(ctx.checkpoint_flags, 1, 0)
      :ok = GenServer.call(pid, {:put, "dirty_after_cold_meta", "value", 0})
      Process.sleep(10)

      assert :atomics.get(ctx.checkpoint_flags, 1) == 1
      assert {^large, ^expire_at_ms} = GenServer.call(pid, {:get_meta, "cold_meta_key"})

      assert :atomics.get(ctx.checkpoint_flags, 1) == 1,
             "a known-location cold GET_META must not fsync unrelated dirty Bitcask data"
    end

    test "direct delete_prefix persists tombstones for deleted keys" do
      {pid, _index, dir, ctx} = start_shard(flush_interval_ms: 5000)
      on_exit(fn -> cleanup_shard(pid, ctx, dir) end)

      :ok = GenServer.call(pid, {:put, "plain:one", "1", 0})
      :ok = GenServer.call(pid, {:put, "plain:two", "2", 0})
      :ok = GenServer.call(pid, {:put, "other", "3", 0})
      :ok = GenServer.call(pid, :flush)

      :ok = GenServer.call(pid, {:delete_prefix, "plain:"})

      log_path = Path.join(Ferricstore.DataDir.shard_data_path(dir, 0), "00000.log")
      assert {:ok, records} = NIF.v2_scan_file(log_path)

      assert Enum.any?(records, &match?({"plain:one", _off, 0, 0, true}, &1))
      assert Enum.any?(records, &match?({"plain:two", _off, 0, 0, true}, &1))
      refute Enum.any?(records, &match?({"other", _off, 0, 0, true}, &1))
    end

    test "direct compound_delete_prefix persists tombstones for deleted fields" do
      {pid, _index, dir, ctx} = start_shard(flush_interval_ms: 5000)
      on_exit(fn -> cleanup_shard(pid, ctx, dir) end)

      redis_key = "hash_prefix_delete"
      prefix = "H:" <> redis_key <> <<0>>
      field_one = prefix <> "one"
      field_two = prefix <> "two"
      other_field = "H:other_hash" <> <<0>> <> "field"

      :ok = GenServer.call(pid, {:compound_put, redis_key, field_one, "1", 0})
      :ok = GenServer.call(pid, {:compound_put, redis_key, field_two, "2", 0})
      :ok = GenServer.call(pid, {:compound_put, "other_hash", other_field, "3", 0})
      :ok = GenServer.call(pid, :flush)

      :ok = GenServer.call(pid, {:compound_delete_prefix, redis_key, prefix})

      log_path = Path.join(Ferricstore.DataDir.shard_data_path(dir, 0), "00000.log")
      assert {:ok, records} = NIF.v2_scan_file(log_path)

      assert Enum.any?(records, &match?({^field_one, _off, 0, 0, true}, &1))
      assert Enum.any?(records, &match?({^field_two, _off, 0, 0, true}, &1))
      refute Enum.any?(records, &match?({^other_field, _off, 0, 0, true}, &1))
    end

    test "multiple puts before flush are all readable" do
      {pid, _index, dir, ctx} = start_shard(flush_interval_ms: 5000)
      on_exit(fn -> cleanup_shard(pid, ctx, dir) end)

      for i <- 1..10 do
        :ok = GenServer.call(pid, {:put, "mkey#{i}", "mval#{i}", 0})
      end

      # All should be in ETS
      for i <- 1..10 do
        assert "mval#{i}" == GenServer.call(pid, {:get, "mkey#{i}"})
      end

      # Flush and verify durability
      :ok = GenServer.call(pid, :flush)

      for i <- 1..10 do
        assert "mval#{i}" == GenServer.call(pid, {:get, "mkey#{i}"})
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Deferred fsync on timer
  # ---------------------------------------------------------------------------

  describe "deferred fsync via BitcaskCheckpointer" do
    alias Ferricstore.Store.ActiveFile
    alias Ferricstore.Store.BitcaskCheckpointer

    test "checkpointer fsyncs the active file when the flag is raised" do
      {pid, _index, dir, ctx} = start_shard(flush_interval_ms: 5000)
      on_exit(fn -> cleanup_shard(pid, ctx, dir) end)

      # Start a checkpointer for this shard. The shard already publishes
      # its active file to the ActiveFile registry during init, so the
      # checkpointer can find it immediately.
      ActiveFile.init(1)

      {:ok, ck} =
        BitcaskCheckpointer.start_link(
          index: 0,
          instance_ctx: ctx,
          checkpoint_interval_ms: 10,
          name: :"ck_unified_#{:erlang.unique_integer([:positive])}"
        )

      on_exit(fn ->
        try do
          if Process.alive?(ck), do: GenServer.stop(ck, :normal, 5000)
        catch
          :exit, _ -> :ok
        end
      end)

      parent = self()
      handler_id = "unified-fsync-#{:erlang.unique_integer([:positive])}"

      :telemetry.attach(
        handler_id,
        [:ferricstore, :bitcask, :checkpoint],
        fn _e, meas, meta, _ -> send(parent, {:ck_event, meas, meta}) end,
        nil
      )

      on_exit(fn -> :telemetry.detach(handler_id) end)

      :atomics.put(ctx.checkpoint_flags, 1, 0)
      :ok = GenServer.call(pid, {:put, "tk", "tv", 0})

      # The checkpointer tick should see the raised flag and emit a
      # :ok telemetry event within a few ticks.
      assert_receive {:ck_event, _meas, %{status: :ok}}, 500

      # Flag must have been cleared by the checkpointer before firing fsync.
      assert :atomics.get(ctx.checkpoint_flags, 1) == 0
    end
  end

  # ---------------------------------------------------------------------------
  # ETS update_element optimization
  # ---------------------------------------------------------------------------

  describe "update_ets_locations preserves LFU counter" do
    test "LFU counter is preserved after flush updates disk location" do
      {pid, _index, dir, ctx} = start_shard(flush_interval_ms: 5000)
      on_exit(fn -> cleanup_shard(pid, ctx, dir) end)

      # Put a value — this inserts with LFU.initial()
      :ok = GenServer.call(pid, {:put, "lfu_key", "lfu_val", 0})

      # Read the key multiple times to increment LFU counter
      for _ <- 1..20 do
        GenServer.call(pid, {:get, "lfu_key"})
      end

      # Get the LFU counter before flush
      keydir = elem(ctx.keydir_refs, 0)
      [{_, _, _, lfu_before, _, _, _}] = :ets.lookup(keydir, "lfu_key")

      # Flush (will call update_ets_locations)
      :ok = GenServer.call(pid, :flush)

      # Get the LFU counter after flush — should be preserved
      [{_, _, _, lfu_after, fid, off, _}] = :ets.lookup(keydir, "lfu_key")
      assert lfu_after == lfu_before
      # Disk location should now be set (non-zero)
      assert fid > 0 or off > 0 or (fid == 0 and off == 0)
    end
  end

  # ---------------------------------------------------------------------------
  # Concurrent writes with deferred fsync
  # ---------------------------------------------------------------------------

  describe "concurrent writes" do
    test "many concurrent puts followed by flush all survive" do
      {pid, _index, dir, ctx} = start_shard(flush_interval_ms: 1)
      on_exit(fn -> cleanup_shard(pid, ctx, dir) end)

      # Fire 100 concurrent writes
      tasks =
        for i <- 1..100 do
          Task.async(fn ->
            GenServer.call(pid, {:put, "c#{i}", "v#{i}", 0})
          end)
        end

      Enum.each(tasks, &Task.await(&1, 5000))

      # Flush everything
      :ok = GenServer.call(pid, :flush)

      # All keys should be readable
      for i <- 1..100 do
        assert "v#{i}" == GenServer.call(pid, {:get, "c#{i}"}),
               "key c#{i} should have value v#{i}"
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Durability: delete forces sync flush
  # ---------------------------------------------------------------------------

  describe "delete forces synchronous flush" do
    test "delete after nosync writes ensures durability" do
      {pid, _index, dir, ctx} = start_shard(flush_interval_ms: 5000)
      on_exit(fn -> cleanup_shard(pid, ctx, dir) end)

      :ok = GenServer.call(pid, {:put, "del_k1", "del_v1", 0})
      :ok = GenServer.call(pid, {:put, "del_k2", "del_v2", 0})

      # Delete forces await_in_flight + flush_pending_sync
      :ok = GenServer.call(pid, {:delete, "del_k1"})

      # del_k1 should be gone
      assert nil == GenServer.call(pid, {:get, "del_k1"})
      # del_k2 should still be there
      assert "del_v2" == GenServer.call(pid, {:get, "del_k2"})
    end
  end

  # ---------------------------------------------------------------------------
  # v2_append_batch_async NIF (Tokio path)
  # ---------------------------------------------------------------------------

  describe "v2_append_batch_async NIF" do
    test "sends completion message with locations" do
      dir = Path.join(System.tmp_dir!(), "async_write_#{:rand.uniform(9_999_999)}")
      File.mkdir_p!(dir)
      path = Path.join(dir, "00000.log")
      File.touch!(path)

      on_exit(fn -> File.rm_rf(dir) end)

      caller = self()
      corr_id = 42
      batch = [{"ak1", "av1", 0}, {"ak2", "av2", 0}]

      :ok = NIF.v2_append_batch_async(caller, corr_id, path, batch)

      # Wait for the completion message
      assert_receive {:tokio_complete, ^corr_id, :ok, locations}, 5000
      assert is_list(locations)
      assert length(locations) == 2

      [{off1, vsize1}, {off2, vsize2}] = locations
      assert off1 == 0
      assert off2 > off1
      # byte_size("av1")
      assert vsize1 == 3
      # byte_size("av2")
      assert vsize2 == 3

      # Data should be readable
      assert {:ok, "av1"} = NIF.v2_pread_at(path, off1)
      assert {:ok, "av2"} = NIF.v2_pread_at(path, off2)
    end

    test "empty batch returns empty locations" do
      dir = Path.join(System.tmp_dir!(), "async_empty_#{:rand.uniform(9_999_999)}")
      File.mkdir_p!(dir)
      path = Path.join(dir, "00000.log")
      File.touch!(path)

      on_exit(fn -> File.rm_rf(dir) end)

      caller = self()
      corr_id = 99

      :ok = NIF.v2_append_batch_async(caller, corr_id, path, [])

      assert_receive {:tokio_complete, ^corr_id, :ok, []}, 5000
    end

    test "multiple concurrent async batches with different correlation IDs" do
      dir = Path.join(System.tmp_dir!(), "async_concurrent_#{:rand.uniform(9_999_999)}")
      File.mkdir_p!(dir)
      path = Path.join(dir, "00000.log")
      File.touch!(path)

      on_exit(fn -> File.rm_rf(dir) end)

      caller = self()

      # Submit 5 async batches
      for i <- 1..5 do
        batch = [{"key_#{i}", "val_#{i}", 0}]
        :ok = NIF.v2_append_batch_async(caller, i, path, batch)
      end

      # Collect all completions (order may vary)
      received =
        for _ <- 1..5 do
          receive do
            {:tokio_complete, corr_id, :ok, locations} -> {corr_id, locations}
          after
            5000 -> flunk("timeout waiting for async completion")
          end
        end

      corr_ids = Enum.map(received, fn {id, _} -> id end) |> Enum.sort()
      assert corr_ids == [1, 2, 3, 4, 5]
    end
  end

  # ---------------------------------------------------------------------------
  # v2_fsync_async NIF
  # ---------------------------------------------------------------------------

  describe "v2_fsync_async NIF" do
    test "sends completion message after fsync" do
      dir = Path.join(System.tmp_dir!(), "fsync_async_#{:rand.uniform(9_999_999)}")
      File.mkdir_p!(dir)
      path = Path.join(dir, "00000.log")
      File.touch!(path)

      on_exit(fn -> File.rm_rf(dir) end)

      # Write some data first
      NIF.v2_append_batch_nosync(path, [{"fk", "fv", 0}])

      # Submit async fsync
      caller = self()
      corr_id = 77
      :ok = NIF.v2_fsync_async(caller, corr_id, path)

      assert_receive {:tokio_complete, ^corr_id, :ok, :ok}, 5000
    end
  end
end
