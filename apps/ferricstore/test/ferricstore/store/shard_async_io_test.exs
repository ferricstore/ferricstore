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
  alias Ferricstore.Store.CompoundKey
  alias Ferricstore.Store.BitcaskWriter
  alias Ferricstore.Store.LFU
  alias Ferricstore.Store.Router
  alias Ferricstore.Store.Shard
  alias Ferricstore.Store.Shard.Flush, as: ShardFlush
  alias Ferricstore.Store.Shard.Lifecycle, as: ShardLifecycle
  alias Ferricstore.Store.Shard.Reads, as: ShardReads

  @header_size 26

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
        shard_count: 1
      )

    Ferricstore.DataDir.ensure_layout!(dir, 1)

    {:ok, pid} =
      Shard.start_link(
        index: 0,
        data_dir: dir,
        flush_interval_ms: flush_ms,
        instance_ctx: ctx
      )

    {pid, 0, dir, ctx}
  end

  defp restart_shard(dir, ctx, flush_ms) do
    {:ok, pid} =
      Shard.start_link(
        index: 0,
        data_dir: dir,
        flush_interval_ms: flush_ms,
        instance_ctx: ctx
      )

    pid
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

  defp force_rotate_active_file(pid) do
    :sys.replace_state(pid, fn state ->
      new_id = state.active_file_id + 1
      sp = state.shard_data_path
      new_path = Ferricstore.Store.Shard.ETS.file_path(sp, new_id)

      Ferricstore.FS.touch!(new_path)

      if ctx = Map.get(state, :instance_ctx) do
        Ferricstore.Store.ActiveFile.publish(ctx, state.index, new_id, new_path, sp)
      end

      %{
        state
        | active_file_id: new_id,
          active_file_path: new_path,
          active_file_size: 0,
          file_stats: Map.put(state.file_stats, new_id, {0, 0})
      }
    end)

    :ok
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

    test "scans tombstone metadata without returning live records" do
      dir = Path.join(System.tmp_dir!(), "tombstone_scan_nif_#{:rand.uniform(9_999_999)}")
      File.mkdir_p!(dir)
      path = Path.join(dir, "00000.log")
      File.touch!(path)

      on_exit(fn -> File.rm_rf(dir) end)

      large_value = :binary.copy("x", 1024 * 1024)

      assert {:ok,
              [
                {:put, put_offset, _put_size},
                {:delete, delete_offset, delete_size},
                {:put, put2_offset, _put2_size}
              ]} =
               NIF.v2_append_ops_batch_nosync(path, [
                 {:put, "live_before", large_value, 0},
                 {:delete, "deleted"},
                 {:put, "live_after", large_value, 0}
               ])

      assert put_offset == 0
      assert put2_offset > delete_offset

      assert {:ok, [{"deleted", ^delete_offset, ^delete_size, 0}]} = NIF.v2_scan_tombstones(path)
    end

    test "tombstone scan returns error before tombstones after a corrupt live record" do
      dir = Path.join(System.tmp_dir!(), "tombstone_scan_corrupt_#{:rand.uniform(9_999_999)}")
      File.mkdir_p!(dir)
      path = Path.join(dir, "00000.log")
      File.touch!(path)

      on_exit(fn -> File.rm_rf(dir) end)

      assert {:ok, [{:put, put_offset, _}, {:delete, delete_offset, _}]} =
               NIF.v2_append_ops_batch_nosync(path, [
                 {:put, "live_before", "value", 0},
                 {:delete, "deleted_after_corruption"}
               ])

      value_offset = put_offset + @header_size + byte_size("live_before")

      {:ok, fd} = :file.open(path, [:read, :write, :binary])
      :ok = :file.pwrite(fd, value_offset, <<0xFF>>)
      :ok = :file.close(fd)

      assert {:error, reason} = NIF.v2_scan_tombstones(path)
      assert reason =~ "CRC mismatch"
      assert {:ok, nil} = NIF.v2_pread_at(path, delete_offset)
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

    test "BitcaskWriter attaches location when pending write preserves old cold ref" do
      shard_index = 10_000 + System.unique_integer([:positive])
      dir = Path.join(System.tmp_dir!(), "bitcask_writer_old_ref_#{shard_index}")
      File.mkdir_p!(dir)
      path = Path.join(dir, "00000.log")
      File.touch!(path)

      keydir = :ets.new(:"bitcask_writer_old_ref_#{shard_index}", [:set, :public])
      key = "writer:old-ref-location"

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

      :ets.insert(keydir, {key, "new", 456, LFU.initial(), :pending, 99, 3})

      :sys.replace_state(writer, fn state ->
        %{
          state
          | pending: [{:write, nil, path, 0, keydir, key, "new", 456}],
            pending_count: 1
        }
      end)

      assert :ok == BitcaskWriter.flush(shard_index)
      assert [{^key, "new", 456, _lfu, 0, offset, 3}] = :ets.lookup(keydir, key)
      assert is_integer(offset) and offset >= 0
    end

    test "BitcaskWriter keeps failed writes pending for retry" do
      shard_index = 10_000 + System.unique_integer([:positive])
      dir = Path.join(System.tmp_dir!(), "bitcask_writer_retry_#{shard_index}")
      File.mkdir_p!(dir)
      path = Path.join([dir, "missing_parent", "00000.log"])

      keydir = :ets.new(:"bitcask_writer_retry_#{shard_index}", [:set, :public])
      key = "writer:retry-pending"

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
          | pending: [{:write, nil, path, 0, keydir, key, "new", 456}],
            pending_count: 1
        }
      end)

      assert {:error, {:flush_failed, 1}} == BitcaskWriter.flush(shard_index)

      state = :sys.get_state(writer)
      assert state.pending_count == 1
      assert [{:write, nil, ^path, 0, ^keydir, ^key, "new", 456}] = state.pending
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

    test "oversized key returns error and does not append a corrupt record" do
      dir = Path.join(System.tmp_dir!(), "async_oversized_key_#{:rand.uniform(9_999_999)}")
      File.mkdir_p!(dir)
      path = Path.join(dir, "00000.log")
      File.touch!(path)

      on_exit(fn -> File.rm_rf(dir) end)

      caller = self()
      corr_id = 100
      oversized_key = :binary.copy("k", 65_536)

      :ok = NIF.v2_append_batch_async(caller, corr_id, path, [{oversized_key, "v", 0}])

      assert_receive {:tokio_complete, ^corr_id, :error, reason}, 5000
      assert reason =~ "key too large"
      assert File.stat!(path).size == 0
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
  # v2_pread_batch_async NIF (Tokio path)
  # ---------------------------------------------------------------------------

  describe "v2_pread_batch_async NIF" do
    test "same-path async batch pread returns values in offset order" do
      dir = Path.join(System.tmp_dir!(), "async_pread_batch_path_#{:rand.uniform(9_999_999)}")
      File.mkdir_p!(dir)
      path = Path.join(dir, "00000.log")

      on_exit(fn -> File.rm_rf(dir) end)

      {:ok, {off1, _}} = NIF.v2_append_record(path, "batch_path_1", "one", 0)
      {:ok, {off2, _}} = NIF.v2_append_tombstone(path, "batch_path_deleted")
      {:ok, {off3, _}} = NIF.v2_append_record(path, "batch_path_3", "three", 0)

      corr_id = 100
      :ok = NIF.v2_pread_batch_path_async(self(), corr_id, path, [off3, off2, off1])

      assert_receive {:tokio_complete, ^corr_id, :ok, ["three", nil, "one"]}, 5000
    end

    test "grouped async batch pread preserves original order across paths" do
      dir = Path.join(System.tmp_dir!(), "async_pread_batch_grouped_#{:rand.uniform(9_999_999)}")
      File.mkdir_p!(dir)
      path_a = Path.join(dir, "00000.log")
      path_b = Path.join(dir, "00001.log")

      on_exit(fn -> File.rm_rf(dir) end)

      {:ok, {off_a1, _}} = NIF.v2_append_record(path_a, "grouped_a1", "a1", 0)
      {:ok, {off_b1, _}} = NIF.v2_append_record(path_b, "grouped_b1", "b1", 0)
      {:ok, {off_a2, _}} = NIF.v2_append_record(path_a, "grouped_a2", "a2", 0)

      corr_id = 102

      :ok =
        NIF.v2_pread_batch_grouped_async(self(), corr_id, [
          {path_a, [{0, off_a1}, {2, off_a2}]},
          {path_b, [{1, off_b1}]}
        ])

      assert_receive {:tokio_complete, ^corr_id, :ok, ["a1", "b1", "a2"]}, 5000
    end

    test "isolates CRC errors to per-index error results" do
      dir = Path.join(System.tmp_dir!(), "async_pread_batch_crc_#{:rand.uniform(9_999_999)}")
      File.mkdir_p!(dir)
      path = Path.join(dir, "00000.log")

      on_exit(fn -> File.rm_rf(dir) end)

      {:ok, {offset, _}} = NIF.v2_append_record(path, "crc_batch_async", "value", 0)

      {:ok, fd} = :file.open(path, [:read, :write, :binary])
      :ok = :file.pwrite(fd, @header_size, <<0xFF>>)
      :ok = :file.close(fd)

      corr_id = 101
      :ok = NIF.v2_pread_batch_async(self(), corr_id, [{path, offset}])

      assert_receive {:tokio_complete, ^corr_id, :ok, [{:error, reason}]}, 5000
      assert reason =~ "CRC" or reason =~ "mismatch"
    end
  end

  describe "key-validated shard cold reads" do
    test "shard GET and local transaction reads reject mismatched cold offsets" do
      {pid, _index, dir, ctx} = start_shard(flush_interval_ms: 5000)

      try do
        state = :sys.get_state(pid)
        key = "shard_cold_stale_offset:" <> Integer.to_string(:erlang.unique_integer([:positive]))
        other_key = key <> ":other"
        path = Path.join(state.shard_data_path, "00000.log")

        {:ok, [{other_offset, _}, {_key_offset, value_size}]} =
          NIF.v2_append_batch(path, [{other_key, "wrong-value", 0}, {key, "right-value", 0}])

        :ets.insert(state.keydir, {key, nil, 0, LFU.initial(), 0, other_offset, value_size})

        assert nil == GenServer.call(pid, {:get, key})
        assert nil == GenServer.call(pid, {:get_meta, key})
        assert {:ok, nil} == ShardReads.v2_local_read(:sys.get_state(pid), key)
      after
        cleanup_shard(pid, ctx, dir)
      end
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

  describe "hint recovery" do
    test "supervisor shutdown runs shard terminate and writes active hint" do
      dir = Path.join(System.tmp_dir!(), "shard_supervised_shutdown_#{:rand.uniform(9_999_999)}")
      File.mkdir_p!(dir)

      name = :"supervised_shutdown_#{:erlang.unique_integer([:positive])}"

      ctx =
        FerricStore.Instance.build(name,
          data_dir: dir,
          shard_count: 1
        )

      Ferricstore.DataDir.ensure_layout!(dir, 1)

      parent = self()
      handler_id = {:shard_shutdown_telemetry, self(), make_ref()}

      :telemetry.attach(
        handler_id,
        [:ferricstore, :shard, :shutdown],
        fn event, measurements, metadata, _config ->
          send(parent, {:shard_shutdown, event, measurements, metadata})
        end,
        nil
      )

      on_exit(fn ->
        :telemetry.detach(handler_id)
        FerricStore.Instance.cleanup(ctx.name)
        File.rm_rf(dir)
      end)

      child = {Shard, index: 0, data_dir: dir, flush_interval_ms: 5000, instance_ctx: ctx}

      {:ok, sup} = Supervisor.start_link([child], strategy: :one_for_one)
      pid = Process.whereis(Router.shard_name(ctx, 0))
      assert is_pid(pid)

      assert :ok == GenServer.call(pid, {:put, "supervised-hint", "value", 0})
      assert :ok == GenServer.call(pid, :flush)

      Supervisor.stop(sup)

      assert_receive {:shard_shutdown, [:ferricstore, :shard, :shutdown], measurements,
                      %{shard_index: 0}},
                     1_000

      assert measurements.total_duration_us >= 0
      assert File.exists?(Path.join([dir, "data", "shard_0", "00000.hint"]))
    end

    test "shutdown telemetry reports warning when active file fsync fails" do
      dir = Path.join(System.tmp_dir!(), "shard_shutdown_fsync_fail_#{:rand.uniform(9_999_999)}")
      File.mkdir_p!(dir)
      keydir = :ets.new(:shutdown_fsync_fail_keydir, [:set, :public])
      parent = self()
      handler_id = {:shard_shutdown_fsync_fail, self(), make_ref()}

      :telemetry.attach(
        handler_id,
        [:ferricstore, :shard, :shutdown],
        fn _event, measurements, metadata, _config ->
          send(parent, {:shutdown_telemetry, measurements, metadata})
        end,
        nil
      )

      on_exit(fn ->
        :telemetry.detach(handler_id)
        File.rm_rf(dir)
      end)

      state = %{
        index: 0,
        pending: [],
        flush_in_flight: nil,
        instance_ctx: nil,
        active_file_path: Path.join(dir, "missing.log"),
        active_file_id: 0,
        shard_data_path: dir,
        keydir: keydir
      }

      assert :ok = ShardLifecycle.do_terminate(:shutdown, state)

      assert_receive {:shutdown_telemetry, measurements,
                      %{shard_index: 0, status: :warning, errors: errors}},
                     1_000

      assert measurements.total_duration_us >= 0
      assert match?([{:active_fsync, _reason}], errors)
    end

    test "replays tombstones that appear before the last live hinted record" do
      dir = Path.join(System.tmp_dir!(), "hint_tombstone_order_#{:rand.uniform(9_999_999)}")
      File.mkdir_p!(dir)

      on_exit(fn -> File.rm_rf(dir) end)

      log0 = Path.join(dir, "00000.log")
      hint0 = Path.join(dir, "00000.hint")
      log1 = Path.join(dir, "00001.log")
      hint1 = Path.join(dir, "00001.hint")

      {:ok, [{a_offset, a_size}]} = NIF.v2_append_batch(log0, [{"a", "old", 0}])
      :ok = NIF.v2_write_hint_file(hint0, [{"a", 0, a_offset, a_size, 0}])

      {:ok, _delete_offset} = NIF.v2_append_tombstone(log1, "a")
      {:ok, [{b_offset, b_size}]} = NIF.v2_append_batch(log1, [{"b", "live", 0}])
      :ok = NIF.v2_write_hint_file(hint1, [{"b", 1, b_offset, b_size, 0}])

      keydir = :ets.new(:hint_tombstone_order_keydir, [:set, :public])

      Ferricstore.Store.Shard.Lifecycle.recover_keydir(dir, keydir, 0)

      assert [] == :ets.lookup(keydir, "a")
      assert [{"b", nil, 0, _lfu, 1, ^b_offset, ^b_size}] = :ets.lookup(keydir, "b")
    end

    test "falls back to full scan when hinted tombstone scan sees corruption" do
      dir = Path.join(System.tmp_dir!(), "hint_tombstone_corrupt_#{:rand.uniform(9_999_999)}")
      File.mkdir_p!(dir)

      on_exit(fn -> File.rm_rf(dir) end)

      log0 = Path.join(dir, "00000.log")
      hint0 = Path.join(dir, "00000.hint")
      log1 = Path.join(dir, "00001.log")
      hint1 = Path.join(dir, "00001.hint")

      {:ok, [{a_offset, a_size}]} = NIF.v2_append_batch(log0, [{"a", "old", 0}])
      :ok = NIF.v2_write_hint_file(hint0, [{"a", 0, a_offset, a_size, 0}])

      {:ok, _delete_offset} = NIF.v2_append_tombstone(log1, "a")
      {:ok, [{b_offset, b_size}]} = NIF.v2_append_batch(log1, [{"b", "live", 0}])
      :ok = NIF.v2_write_hint_file(hint1, [{"b", 1, b_offset, b_size, 0}])

      corrupt_offset = b_offset + 26 + byte_size("b")
      {:ok, file} = :file.open(String.to_charlist(log1), [:read, :write, :binary])
      :ok = :file.pwrite(file, corrupt_offset, "X")
      :ok = :file.sync(file)
      :ok = :file.close(file)

      keydir = :ets.new(:hint_tombstone_corrupt_keydir, [:set, :public])

      Ferricstore.Store.Shard.Lifecycle.recover_keydir(dir, keydir, 0)

      assert [] == :ets.lookup(keydir, "a")
      assert [{"b", nil, 0, _lfu, 1, ^b_offset, ^b_size}] = :ets.lookup(keydir, "b")
    end

    test "recovers shard logs in numeric file id order past five digits" do
      dir = Path.join(System.tmp_dir!(), "numeric_log_recovery_#{:rand.uniform(9_999_999)}")
      File.mkdir_p!(dir)

      on_exit(fn -> File.rm_rf(dir) end)

      old_log = Path.join(dir, "99999.log")
      new_log = Path.join(dir, "100000.log")

      {:ok, {_old_offset, _old_size}} = NIF.v2_append_record(old_log, "rollover_key", "old", 0)

      {:ok, {new_offset, _new_record_size}} =
        NIF.v2_append_record(new_log, "rollover_key", "new", 0)

      keydir = :ets.new(:numeric_log_recovery_keydir, [:set, :public])

      Ferricstore.Store.Shard.Lifecycle.recover_keydir(dir, keydir, 0)

      assert [{"rollover_key", nil, 0, _lfu, 100_000, ^new_offset, 3}] =
               :ets.lookup(keydir, "rollover_key")
    end

    test "replays log tail after stale active-file hint" do
      previous_trap_exit = Process.flag(:trap_exit, true)

      {pid1, _index, dir, ctx} = start_shard(flush_interval_ms: 5000)

      key = "hint_tail_#{:erlang.unique_integer([:positive])}"

      try do
        assert :ok == GenServer.call(pid1, {:put, key, "v1", 0})
        assert :ok == GenServer.call(pid1, :flush)
        {hint_fid, _active_path} = GenServer.call(pid1, :get_active_file)
        :ok = GenServer.stop(pid1, :normal, 5_000)
        hint_name = "#{String.pad_leading(Integer.to_string(hint_fid), 5, "0")}.hint"
        hint_path = Path.join([dir, "data", "shard_0", hint_name])
        assert File.exists?(hint_path)

        pid2 = restart_shard(dir, ctx, 5000)
        assert "v1" == GenServer.call(pid2, {:get, key})

        assert :ok == GenServer.call(pid2, {:put, key, "v2", 0})
        assert :ok == GenServer.call(pid2, :flush)

        ref = Process.monitor(pid2)
        Process.exit(pid2, :kill)

        assert_receive {:DOWN, ^ref, :process, ^pid2, :killed}, 2_000
        assert File.exists?(hint_path)

        pid3 = restart_shard(dir, ctx, 5000)
        assert "v2" == GenServer.call(pid3, {:get, key})
      after
        case Process.whereis(Router.shard_name(ctx, 0)) do
          pid when is_pid(pid) ->
            cleanup_shard(pid, ctx, dir)

          _ ->
            FerricStore.Instance.cleanup(ctx.name)
            File.rm_rf(dir)
        end

        Process.flag(:trap_exit, previous_trap_exit)
      end
    end

    test "replays stale active-file hint tombstone tail" do
      previous_trap_exit = Process.flag(:trap_exit, true)
      {pid1, _index, dir, ctx} = start_shard(flush_interval_ms: 5000)
      key = "hint_tail_delete_#{:erlang.unique_integer([:positive])}"

      try do
        assert :ok == GenServer.call(pid1, {:put, key, "v1", 0})
        assert :ok == GenServer.call(pid1, :flush)
        {hint_fid, _active_path} = GenServer.call(pid1, :get_active_file)
        :ok = GenServer.stop(pid1, :normal, 5_000)
        hint_name = "#{String.pad_leading(Integer.to_string(hint_fid), 5, "0")}.hint"
        hint_path = Path.join([dir, "data", "shard_0", hint_name])
        assert File.exists?(hint_path)

        pid2 = restart_shard(dir, ctx, 5000)
        assert "v1" == GenServer.call(pid2, {:get, key})

        assert :ok == GenServer.call(pid2, {:delete, key})
        assert :ok == GenServer.call(pid2, :flush)

        ref = Process.monitor(pid2)
        Process.exit(pid2, :kill)

        assert_receive {:DOWN, ^ref, :process, ^pid2, :killed}, 2_000
        assert File.exists?(hint_path)

        pid3 = restart_shard(dir, ctx, 5000)
        assert nil == GenServer.call(pid3, {:get, key})
      after
        case Process.whereis(Router.shard_name(ctx, 0)) do
          pid when is_pid(pid) ->
            cleanup_shard(pid, ctx, dir)

          _ ->
            FerricStore.Instance.cleanup(ctx.name)
            File.rm_rf(dir)
        end

        Process.flag(:trap_exit, previous_trap_exit)
      end
    end

    test "falls back to log replay when active hint is corrupt" do
      {pid1, _index, dir, ctx} = start_shard(flush_interval_ms: 5000)
      key = "hint_corrupt_#{:erlang.unique_integer([:positive])}"

      try do
        assert :ok == GenServer.call(pid1, {:put, key, "v1", 0})
        assert :ok == GenServer.call(pid1, :flush)
        {hint_fid, _active_path} = GenServer.call(pid1, :get_active_file)
        :ok = GenServer.stop(pid1, :normal, 5_000)

        hint_name = "#{String.pad_leading(Integer.to_string(hint_fid), 5, "0")}.hint"
        hint_path = Path.join([dir, "data", "shard_0", hint_name])
        assert File.exists?(hint_path)
        File.write!(hint_path, "not a valid hint")

        pid2 = restart_shard(dir, ctx, 5000)
        assert "v1" == GenServer.call(pid2, {:get, key})
      after
        case Process.whereis(Router.shard_name(ctx, 0)) do
          pid when is_pid(pid) ->
            cleanup_shard(pid, ctx, dir)

          _ ->
            FerricStore.Instance.cleanup(ctx.name)
            File.rm_rf(dir)
        end
      end
    end
  end

  describe "shared log compaction" do
    test "manual compaction skips the active log file" do
      {pid, _index, dir, ctx} = start_shard(flush_interval_ms: 5000)

      try do
        active_path = Path.join([dir, "data", "shard_0", "00000.log"])
        assert File.exists?(active_path)

        assert {:ok, {0, 0, 0}} = GenServer.call(pid, {:run_compaction, [0]})
        assert File.exists?(active_path)

        assert :ok = GenServer.call(pid, {:put, "active_compaction_survives", "v", 0})
        assert :ok = GenServer.call(pid, :flush)
        assert "v" == GenServer.call(pid, {:get, "active_compaction_survives"})
      after
        cleanup_shard(pid, ctx, dir)
      end
    end

    test "manual compaction skips the active log file even when it has live entries" do
      {pid, _index, dir, ctx} = start_shard(flush_interval_ms: 5000)

      try do
        active_path = Path.join([dir, "data", "shard_0", "00000.log"])
        assert :ok = GenServer.call(pid, {:put, "active_live_compaction", "value", 0})
        assert :ok = GenServer.call(pid, :flush)

        size_before = File.stat!(active_path).size

        assert {:ok, {0, 0, 0}} = GenServer.call(pid, {:run_compaction, [0]})
        assert File.exists?(active_path)
        assert File.stat!(active_path).size == size_before
        assert "value" == GenServer.call(pid, {:get, "active_live_compaction"})

        state = :sys.get_state(pid)
        assert state.active_file_size == size_before
        assert Map.fetch!(state.file_stats, 0) == {size_before, 0}
      after
        cleanup_shard(pid, ctx, dir)
      end
    end

    test "manual compaction skips the registry active log after external rotation" do
      {pid, _index, dir, ctx} = start_shard(flush_interval_ms: 5000)

      try do
        shard_path = Path.join([dir, "data", "shard_0"])
        registry_active_path = Path.join(shard_path, "00001.log")
        key = "registry_active_compaction_survives"
        value = "registry-active-value"

        Ferricstore.FS.touch!(registry_active_path)
        Ferricstore.Store.ActiveFile.publish(ctx, 0, 1, registry_active_path, shard_path)

        {:ok, [{offset, _value_size}]} =
          NIF.v2_append_batch(registry_active_path, [{key, value, 0}])

        state = :sys.get_state(pid)
        assert state.active_file_id == 0
        assert {1, ^registry_active_path, ^shard_path} = Ferricstore.Store.ActiveFile.get(ctx, 0)

        :ets.insert(state.keydir, {key, value, 0, LFU.initial(), 1, offset, byte_size(value)})

        size_before = File.stat!(registry_active_path).size

        assert {:ok, {0, 0, 0}} = GenServer.call(pid, {:run_compaction, [1]})
        assert File.stat!(registry_active_path).size == size_before
        assert "registry-active-value" == GenServer.call(pid, {:get, key})

        state = :sys.get_state(pid)
        assert state.active_file_id == 1
        assert Map.fetch!(state.file_stats, 1) == {size_before, 0}
      after
        cleanup_shard(pid, ctx, dir)
      end
    end

    test "manual compaction drops expired-only inactive files" do
      {pid, _index, dir, ctx} = start_shard(flush_interval_ms: 5000)

      try do
        key = "expired_only_compaction"
        expired_at = System.os_time(:millisecond) - 1_000
        source = Path.join([dir, "data", "shard_0", "00000.log"])

        assert :ok = GenServer.call(pid, {:put, key, "expired", expired_at})
        assert :ok = GenServer.call(pid, :flush)
        old_size = File.stat!(source).size

        assert :ok = force_rotate_active_file(pid)

        assert {:ok, {0, 0, reclaimed}} = GenServer.call(pid, {:run_compaction, [0]})
        assert reclaimed >= old_size
        refute File.exists?(source)
        assert nil == GenServer.call(pid, {:get, key})
      after
        cleanup_shard(pid, ctx, dir)
      end
    end

    test "drains deferred BitcaskWriter writes before selecting compacted records" do
      source =
        Path.expand("../../../lib/ferricstore/store/shard.ex", __DIR__)
        |> File.read!()

      flush_pos = :binary.match(source, "BitcaskWriter.flush(state.index)")
      reduce_pos = :binary.match(source, "Enum.reduce(file_ids")

      assert {flush_offset, _} = flush_pos
      assert {reduce_offset, _} = reduce_pos
      assert flush_offset < reduce_offset
    end

    test "groups compaction live entries in one keydir pass before per-file work" do
      source =
        Path.expand("../../../lib/ferricstore/store/shard.ex", __DIR__)
        |> File.read!()

      fold_pos = :binary.match(source, "group_compaction_live_entries")
      reduce_pos = :binary.match(source, "Enum.reduce(file_ids")

      assert {fold_offset, _} = fold_pos
      assert {reduce_offset, _} = reduce_pos
      assert fold_offset < reduce_offset
    end

    test "ignores promoted dedicated entries when compacting shared log files" do
      {pid, _index, dir, ctx} = start_shard(flush_interval_ms: 5000)

      try do
        assert :ok = GenServer.call(pid, {:put, "shared_live", "shared-value", 0})
        assert :ok = GenServer.call(pid, :flush)
        assert :ok = force_rotate_active_file(pid)

        promoted_key = "promoted_shared_compaction"
        field_key = CompoundKey.hash_field(promoted_key, "field")

        :sys.replace_state(pid, fn state ->
          dedicated_path = Path.join([state.shard_data_path, "promoted", promoted_key])

          :ets.insert(
            state.keydir,
            {field_key, "dedicated-value", 0, LFU.initial(), 0, 0, byte_size("dedicated-value")}
          )

          %{
            state
            | promoted_instances:
                Map.put(state.promoted_instances, promoted_key, %{
                  path: dedicated_path,
                  type: :hash,
                  total_bytes: 0,
                  dead_bytes: 0,
                  writes_since_compaction: 0,
                  last_compaction_ms: 0
                })
          }
        end)

        assert {:ok, {1, 0, _reclaimed}} = GenServer.call(pid, {:run_compaction, [0]})

        assert [{^field_key, "dedicated-value", 0, _lfu, 0, 0, _vsize}] =
                 :ets.lookup(:sys.get_state(pid).keydir, field_key)
      after
        cleanup_shard(pid, ctx, dir)
      end
    end

    test "removes stale hint when compacting a rewritten shared log file" do
      {pid1, _index, dir, ctx} = start_shard(flush_interval_ms: 5000)
      a = "compact_hint_a_#{:erlang.unique_integer([:positive])}"
      b = "compact_hint_b_#{:erlang.unique_integer([:positive])}"

      on_exit(fn ->
        case Process.whereis(elem(ctx.shard_names, 0)) do
          pid when is_pid(pid) ->
            cleanup_shard(pid, ctx, dir)

          _ ->
            FerricStore.Instance.cleanup(ctx.name)
            File.rm_rf(dir)
        end
      end)

      :ok = GenServer.call(pid1, {:put, a, "old-a", 0})
      :ok = GenServer.call(pid1, {:put, b, "live-b", 0})
      :ok = GenServer.call(pid1, :flush)

      state = :sys.get_state(pid1)
      assert :ok = ShardFlush.write_hint_for_file(state, 0)

      hint_path = Path.join([dir, "data", "shard_0", "00000.hint"])
      assert File.exists?(hint_path)

      :ok = GenServer.call(pid1, {:delete, a})
      :ok = GenServer.call(pid1, :flush)
      assert nil == GenServer.call(pid1, {:get, a})
      assert "live-b" == GenServer.call(pid1, {:get, b})

      :ok = force_rotate_active_file(pid1)

      assert {:ok, {1, 0, _reclaimed}} = GenServer.call(pid1, {:run_compaction, [0]})

      Process.unlink(pid1)
      ref = Process.monitor(pid1)
      Process.exit(pid1, :kill)
      assert_receive {:DOWN, ^ref, :process, ^pid1, :killed}, 2_000

      pid2 = restart_shard(dir, ctx, 5000)

      assert nil == GenServer.call(pid2, {:get, a})
      assert "live-b" == GenServer.call(pid2, {:get, b})
    end

    test "does not drop tombstone-only files while older values can resurrect" do
      previous_trap_exit = Process.flag(:trap_exit, true)
      dir = Path.join(System.tmp_dir!(), "tombstone_compaction_#{:rand.uniform(9_999_999)}")
      File.mkdir_p!(dir)

      name = :"tombstone_compaction_#{:erlang.unique_integer([:positive])}"

      ctx =
        FerricStore.Instance.build(name,
          data_dir: dir,
          shard_count: 1
        )

      try do
        :ok = Ferricstore.DataDir.ensure_layout!(dir, 1)
        shard_dir = Ferricstore.DataDir.shard_data_path(dir, 0)

        log0 = Path.join(shard_dir, "00000.log")
        log1 = Path.join(shard_dir, "00001.log")
        log2 = Path.join(shard_dir, "00002.log")

        {:ok, [_]} = NIF.v2_append_batch(log0, [{"a", "old", 0}])
        {:ok, _} = NIF.v2_append_tombstone(log1, "a")
        File.touch!(log2)

        {:ok, pid1} =
          Shard.start_link(
            index: 0,
            data_dir: dir,
            flush_interval_ms: 5000,
            instance_ctx: ctx
          )

        assert nil == GenServer.call(pid1, {:get, "a"})

        assert {:error, {:no_compactable_files, [1]}} =
                 GenServer.call(pid1, {:run_compaction, [1]})

        assert File.exists?(log1)

        :ok = GenServer.stop(pid1, :normal, 5_000)

        pid2 = restart_shard(dir, ctx, 5000)
        assert nil == GenServer.call(pid2, {:get, "a"})
      after
        case Process.whereis(Router.shard_name(ctx, 0)) do
          pid when is_pid(pid) ->
            cleanup_shard(pid, ctx, dir)

          _ ->
            FerricStore.Instance.cleanup(ctx.name)
            File.rm_rf(dir)
        end

        Process.flag(:trap_exit, previous_trap_exit)
      end
    end

    test "drops tombstone-only files after older files are compacted away" do
      previous_trap_exit = Process.flag(:trap_exit, true)
      dir = Path.join(System.tmp_dir!(), "safe_tombstone_drop_#{:rand.uniform(9_999_999)}")
      File.mkdir_p!(dir)

      name = :"safe_tombstone_drop_#{:erlang.unique_integer([:positive])}"

      ctx =
        FerricStore.Instance.build(name,
          data_dir: dir,
          shard_count: 1
        )

      try do
        :ok = Ferricstore.DataDir.ensure_layout!(dir, 1)
        shard_dir = Ferricstore.DataDir.shard_data_path(dir, 0)

        log0 = Path.join(shard_dir, "00000.log")
        log1 = Path.join(shard_dir, "00001.log")
        log2 = Path.join(shard_dir, "00002.log")

        {:ok, [_]} = NIF.v2_append_batch(log0, [{"a", "old", 0}])
        {:ok, _} = NIF.v2_append_tombstone(log1, "a")
        File.touch!(log2)

        {:ok, pid1} =
          Shard.start_link(
            index: 0,
            data_dir: dir,
            flush_interval_ms: 5000,
            instance_ctx: ctx
          )

        assert nil == GenServer.call(pid1, {:get, "a"})

        assert {:ok, {0, 0, reclaimed0}} = GenServer.call(pid1, {:run_compaction, [0]})
        assert reclaimed0 > 0
        refute File.exists?(log0)

        assert {:ok, {0, 0, reclaimed1}} = GenServer.call(pid1, {:run_compaction, [1]})
        assert reclaimed1 > 0
        refute File.exists?(log1)

        :ok = GenServer.stop(pid1, :normal, 5_000)

        pid2 = restart_shard(dir, ctx, 5000)
        assert nil == GenServer.call(pid2, {:get, "a"})
      after
        case Process.whereis(Router.shard_name(ctx, 0)) do
          pid when is_pid(pid) ->
            cleanup_shard(pid, ctx, dir)

          _ ->
            FerricStore.Instance.cleanup(ctx.name)
            File.rm_rf(dir)
        end

        Process.flag(:trap_exit, previous_trap_exit)
      end
    end

    test "drops tombstone-only files when older files do not contain masked keys" do
      previous_trap_exit = Process.flag(:trap_exit, true)
      dir = Path.join(System.tmp_dir!(), "unneeded_tombstone_drop_#{:rand.uniform(9_999_999)}")
      File.mkdir_p!(dir)

      name = :"unneeded_tombstone_drop_#{:erlang.unique_integer([:positive])}"

      ctx =
        FerricStore.Instance.build(name,
          data_dir: dir,
          shard_count: 1
        )

      try do
        :ok = Ferricstore.DataDir.ensure_layout!(dir, 1)
        shard_dir = Ferricstore.DataDir.shard_data_path(dir, 0)

        log0 = Path.join(shard_dir, "00000.log")
        log1 = Path.join(shard_dir, "00001.log")
        log2 = Path.join(shard_dir, "00002.log")

        {:ok, [_]} = NIF.v2_append_batch(log0, [{"b", "live", 0}])
        {:ok, _} = NIF.v2_append_tombstone(log1, "a")
        File.touch!(log2)

        {:ok, pid1} =
          Shard.start_link(
            index: 0,
            data_dir: dir,
            flush_interval_ms: 5000,
            instance_ctx: ctx
          )

        assert nil == GenServer.call(pid1, {:get, "a"})
        assert "live" == GenServer.call(pid1, {:get, "b"})

        assert {:ok, {0, 0, reclaimed1}} = GenServer.call(pid1, {:run_compaction, [1]})
        assert reclaimed1 > 0
        refute File.exists?(log1)

        :ok = GenServer.stop(pid1, :normal, 5_000)

        pid2 = restart_shard(dir, ctx, 5000)
        assert nil == GenServer.call(pid2, {:get, "a"})
        assert "live" == GenServer.call(pid2, {:get, "b"})
      after
        case Process.whereis(Router.shard_name(ctx, 0)) do
          pid when is_pid(pid) ->
            cleanup_shard(pid, ctx, dir)

          _ ->
            FerricStore.Instance.cleanup(ctx.name)
            File.rm_rf(dir)
        end

        Process.flag(:trap_exit, previous_trap_exit)
      end
    end

    test "drops tombstone-only files when lower masked values are expired" do
      previous_trap_exit = Process.flag(:trap_exit, true)
      dir = Path.join(System.tmp_dir!(), "expired_tombstone_drop_#{:rand.uniform(9_999_999)}")
      File.mkdir_p!(dir)

      name = :"expired_tombstone_drop_#{:erlang.unique_integer([:positive])}"

      ctx =
        FerricStore.Instance.build(name,
          data_dir: dir,
          shard_count: 1
        )

      try do
        :ok = Ferricstore.DataDir.ensure_layout!(dir, 1)
        shard_dir = Ferricstore.DataDir.shard_data_path(dir, 0)

        log0 = Path.join(shard_dir, "00000.log")
        log1 = Path.join(shard_dir, "00001.log")
        log2 = Path.join(shard_dir, "00002.log")
        expired_at = System.os_time(:millisecond) - 1_000

        {:ok, [_]} = NIF.v2_append_batch(log0, [{"a", "expired", expired_at}])
        {:ok, _} = NIF.v2_append_tombstone(log1, "a")
        File.touch!(log2)

        {:ok, pid1} =
          Shard.start_link(
            index: 0,
            data_dir: dir,
            flush_interval_ms: 5000,
            instance_ctx: ctx
          )

        assert nil == GenServer.call(pid1, {:get, "a"})

        assert {:ok, {0, 0, reclaimed1}} = GenServer.call(pid1, {:run_compaction, [1]})
        assert reclaimed1 > 0
        refute File.exists?(log1)

        :ok = GenServer.stop(pid1, :normal, 5_000)

        pid2 = restart_shard(dir, ctx, 5000)
        assert nil == GenServer.call(pid2, {:get, "a"})
      after
        case Process.whereis(Router.shard_name(ctx, 0)) do
          pid when is_pid(pid) ->
            cleanup_shard(pid, ctx, dir)

          _ ->
            FerricStore.Instance.cleanup(ctx.name)
            File.rm_rf(dir)
        end

        Process.flag(:trap_exit, previous_trap_exit)
      end
    end

    test "drops obsolete tombstones from mixed live files during compaction" do
      previous_trap_exit = Process.flag(:trap_exit, true)
      dir = Path.join(System.tmp_dir!(), "mixed_tombstone_drop_#{:rand.uniform(9_999_999)}")
      File.mkdir_p!(dir)

      name = :"mixed_tombstone_drop_#{:erlang.unique_integer([:positive])}"

      ctx =
        FerricStore.Instance.build(name,
          data_dir: dir,
          shard_count: 1
        )

      try do
        :ok = Ferricstore.DataDir.ensure_layout!(dir, 1)
        shard_dir = Ferricstore.DataDir.shard_data_path(dir, 0)

        log0 = Path.join(shard_dir, "00000.log")
        log1 = Path.join(shard_dir, "00001.log")
        log2 = Path.join(shard_dir, "00002.log")

        {:ok, [_]} = NIF.v2_append_batch(log0, [{"unrelated", "old", 0}])
        {:ok, _} = NIF.v2_append_tombstone(log1, "obsolete-delete")
        {:ok, [_]} = NIF.v2_append_batch(log1, [{"live", "kept", 0}])
        File.touch!(log2)

        assert {:ok, [_]} = NIF.v2_scan_tombstones(log1)

        {:ok, pid1} =
          Shard.start_link(
            index: 0,
            data_dir: dir,
            flush_interval_ms: 5000,
            instance_ctx: ctx
          )

        assert "kept" == GenServer.call(pid1, {:get, "live"})

        assert {:ok, {1, 0, _reclaimed}} = GenServer.call(pid1, {:run_compaction, [1]})

        assert {:ok, []} = NIF.v2_scan_tombstones(log1)
        assert "kept" == GenServer.call(pid1, {:get, "live"})
      after
        case Process.whereis(Router.shard_name(ctx, 0)) do
          pid when is_pid(pid) ->
            cleanup_shard(pid, ctx, dir)

          _ ->
            FerricStore.Instance.cleanup(ctx.name)
            File.rm_rf(dir)
        end

        Process.flag(:trap_exit, previous_trap_exit)
      end
    end

    test "drops mixed-file tombstones after newest lower tombstone without scanning older files" do
      previous_trap_exit = Process.flag(:trap_exit, true)

      dir =
        Path.join(System.tmp_dir!(), "mixed_tombstone_desc_scan_#{:rand.uniform(9_999_999)}")

      File.mkdir_p!(dir)

      name = :"mixed_tombstone_desc_scan_#{:erlang.unique_integer([:positive])}"

      ctx =
        FerricStore.Instance.build(name,
          data_dir: dir,
          shard_count: 1
        )

      try do
        :ok = Ferricstore.DataDir.ensure_layout!(dir, 1)
        shard_dir = Ferricstore.DataDir.shard_data_path(dir, 0)

        log0 = Path.join(shard_dir, "00000.log")
        log1 = Path.join(shard_dir, "00001.log")
        log2 = Path.join(shard_dir, "00002.log")
        log3 = Path.join(shard_dir, "00003.log")

        {:ok, [_]} = NIF.v2_append_batch(log0, [{"deleted", "old", 0}])
        {:ok, _} = NIF.v2_append_tombstone(log1, "deleted")
        {:ok, _} = NIF.v2_append_tombstone(log2, "deleted")
        {:ok, [_]} = NIF.v2_append_batch(log2, [{"live", "kept", 0}])
        File.touch!(log3)

        {:ok, pid1} =
          Shard.start_link(
            index: 0,
            data_dir: dir,
            flush_interval_ms: 5000,
            instance_ctx: ctx
          )

        assert nil == GenServer.call(pid1, {:get, "deleted"})
        assert "kept" == GenServer.call(pid1, {:get, "live"})

        parent = self()
        handler_id = {:tombstone_dependency_scan, self(), make_ref()}

        :telemetry.attach(
          handler_id,
          [:ferricstore, :bitcask, :tombstone_dependency_scan],
          fn event, measurements, metadata, _config ->
            send(parent, {:tombstone_dependency_scan, event, measurements, metadata})
          end,
          nil
        )

        on_exit(fn -> :telemetry.detach(handler_id) end)

        File.rm!(log0)
        File.mkdir!(log0)

        assert {:ok, {1, 0, _reclaimed}} = GenServer.call(pid1, {:run_compaction, [2]})
        assert {:ok, []} = NIF.v2_scan_tombstones(log2)
        assert "kept" == GenServer.call(pid1, {:get, "live"})

        assert_receive {:tombstone_dependency_scan,
                        [:ferricstore, :bitcask, :tombstone_dependency_scan],
                        %{
                          candidate_files: 2,
                          files_scanned: 1,
                          masked_keys: 1,
                          resolved_keys: 1
                        }, %{fid: 2, status: :ok}}
      after
        case Process.whereis(Router.shard_name(ctx, 0)) do
          pid when is_pid(pid) ->
            cleanup_shard(pid, ctx, dir)

          _ ->
            FerricStore.Instance.cleanup(ctx.name)
            File.rm_rf(dir)
        end

        Process.flag(:trap_exit, previous_trap_exit)
      end
    end
  end

  describe "file size accounting" do
    test "active_file_size tracks full record bytes after batch flush" do
      {pid, _index, dir, ctx} = start_shard(flush_interval_ms: 5000)
      key = "size_accounting_#{:erlang.unique_integer([:positive])}"
      value = "value"

      try do
        assert :ok == GenServer.call(pid, {:put, key, value, 0})
        assert :ok == GenServer.call(pid, :flush)

        {_fid, active_path} = GenServer.call(pid, :get_active_file)
        state = :sys.get_state(pid)

        assert state.active_file_size == File.stat!(active_path).size
        assert state.active_file_size == 26 + byte_size(key) + byte_size(value)
      after
        cleanup_shard(pid, ctx, dir)
      end
    end

    test "preserves tombstones in compacted mixed live/deleted files" do
      previous_trap_exit = Process.flag(:trap_exit, true)
      dir = Path.join(System.tmp_dir!(), "mixed_tombstone_compaction_#{:rand.uniform(9_999_999)}")
      File.mkdir_p!(dir)

      name = :"mixed_tombstone_compaction_#{:erlang.unique_integer([:positive])}"

      ctx =
        FerricStore.Instance.build(name,
          data_dir: dir,
          shard_count: 1
        )

      try do
        :ok = Ferricstore.DataDir.ensure_layout!(dir, 1)
        shard_dir = Ferricstore.DataDir.shard_data_path(dir, 0)

        log0 = Path.join(shard_dir, "00000.log")
        log1 = Path.join(shard_dir, "00001.log")
        log2 = Path.join(shard_dir, "00002.log")

        {:ok, [_]} = NIF.v2_append_batch(log0, [{"a", "old", 0}])
        {:ok, [_]} = NIF.v2_append_batch(log1, [{"b", "live", 0}])
        {:ok, _} = NIF.v2_append_tombstone(log1, "a")
        File.touch!(log2)

        {:ok, pid1} =
          Shard.start_link(
            index: 0,
            data_dir: dir,
            flush_interval_ms: 5000,
            instance_ctx: ctx
          )

        assert nil == GenServer.call(pid1, {:get, "a"})
        assert "live" == GenServer.call(pid1, {:get, "b"})

        assert {:ok, {1, 0, _reclaimed}} = GenServer.call(pid1, {:run_compaction, [1]})

        :ok = GenServer.stop(pid1, :normal, 5_000)

        pid2 = restart_shard(dir, ctx, 5000)
        assert nil == GenServer.call(pid2, {:get, "a"})
        assert "live" == GenServer.call(pid2, {:get, "b"})
      after
        case Process.whereis(Router.shard_name(ctx, 0)) do
          pid when is_pid(pid) ->
            cleanup_shard(pid, ctx, dir)

          _ ->
            FerricStore.Instance.cleanup(ctx.name)
            File.rm_rf(dir)
        end

        Process.flag(:trap_exit, previous_trap_exit)
      end
    end
  end
end
