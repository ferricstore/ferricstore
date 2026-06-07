defmodule Ferricstore.Store.ShardAsyncIoTest.Sections.Part02 do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Bitcask.NIF
      alias Ferricstore.Store.{CompoundKey, Promotion}
      alias Ferricstore.Store.BitcaskWriter
      alias Ferricstore.Store.LFU
      alias Ferricstore.Store.Router
      alias Ferricstore.Store.Shard
      alias Ferricstore.Store.Shard.Flush, as: ShardFlush
      alias Ferricstore.Store.Shard.Lifecycle, as: ShardLifecycle
      alias Ferricstore.Store.Shard.Reads, as: ShardReads
      alias Ferricstore.Store.ShardAsyncIoTest.SlowFlushWriter

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
    end
  end
end
