defmodule Ferricstore.Raft.WARaftBackendTest.Sections.SnapshotCopyDurability do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Raft.WARaftBackend
      alias Ferricstore.Store.Router
      alias Ferricstore.Flow.LMDB

      @tag :snapshot_copy_durability
      test "nested snapshot children are durable before metadata is published", %{ctx: ctx} do
        assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
        assert :ok = WARaftBackend.write(0, {:put, "snapshot:durable", "value", 0})
        nested = Path.join(Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0), "nested/a/b")
        File.mkdir_p!(Path.join(nested, "empty"))
        File.write!(Path.join(nested, "payload"), "nested-value")
        previous = Application.get_env(:ferricstore, :waraft_storage_fsync_dir_hook)
        parent = self()
        once = :atomics.new(1, signed: false)

        Application.put_env(:ferricstore, :waraft_storage_fsync_dir_hook, fn path ->
          result = Ferricstore.Bitcask.NIF.v2_fsync_dir(path)

          if String.ends_with?(path, "/data/nested/a/b") and
               :atomics.compare_exchange(once, 1, 0, 1) == :ok do
            send(parent, {:nested_snapshot_synced, self(), path})

            receive do
              :continue -> :ok
            after
              5_000 -> raise "snapshot copy test timeout"
            end
          end

          result
        end)

        task = Task.async(fn -> WARaftBackend.create_snapshot(0) end)

        try do
          assert_receive {:nested_snapshot_synced, writer, copied}, 5_000
          snapshot = Enum.reduce(1..4, copied, fn _, path -> Path.dirname(path) end)
          assert File.read!(Path.join(copied, "payload")) == "nested-value"
          assert File.dir?(Path.join(copied, "empty"))
          refute File.exists?(Path.join(snapshot, "ferricstore_snapshot.term"))
          assert Task.yield(task, 20) == nil
          send(writer, :continue)
          assert {:ok, _position} = Task.await(task, 15_000)
          assert File.exists?(Path.join(snapshot, "ferricstore_snapshot.term"))
        after
          restore_env(:waraft_storage_fsync_dir_hook, previous)
          Task.shutdown(task, :brutal_kill)
        end
      end

      @tag :snapshot_copy_durability
      test "a nested copied-file sync failure cannot publish snapshot metadata and permits retry",
           %{ctx: ctx} do
        assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
        assert :ok = WARaftBackend.write(0, {:put, "snapshot:retry", "value", 0})

        source =
          Path.join(Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0), "nested/a/b/payload")

        File.mkdir_p!(Path.dirname(source))
        File.write!(source, "original")
        previous = Application.get_env(:ferricstore, :waraft_snapshot_fsync_file_hook)
        parent = self()

        Application.put_env(:ferricstore, :waraft_snapshot_fsync_file_hook, fn path ->
          if String.ends_with?(path, "/nested/a/b/payload") do
            send(parent, {:failed_snapshot_leaf, path})
            {:error, :injected_nested_sync_failure}
          else
            Ferricstore.Bitcask.NIF.v2_fsync(path)
          end
        end)

        try do
          assert {:error, _reason} = WARaftBackend.create_snapshot(0)
          assert_receive {:failed_snapshot_leaf, copied}, 1_000
          snapshot = Enum.reduce(1..5, copied, fn _, path -> Path.dirname(path) end)
          refute File.exists?(Path.join(snapshot, "ferricstore_snapshot.term"))
          assert File.read!(source) == "original"
        after
          restore_env(:waraft_snapshot_fsync_file_hook, previous)
        end

        assert {:ok, _position} = WARaftBackend.create_snapshot(0)
        assert Router.get(ctx, "snapshot:retry") == "value"
      end

      @tag :snapshot_copy_durability
      test "snapshot copies LMDB data without runtime locks and retains ordinary lock-named payloads",
           %{ctx: ctx, root: root} do
        assert :ok = WARaftBackend.start(ctx)
        assert :ok = WARaftBackend.write(0, {:put, "snapshot:lmdb", "value", 0})
        shard_path = Ferricstore.DataDir.shard_data_path(root, 0)
        lmdb_path = LMDB.path(shard_path)
        assert :ok = LMDB.write_batch(lmdb_path, [{:put, "snapshot:sentinel", "preserved"}])
        assert File.exists?(Path.join(lmdb_path, "lock.mdb"))
        ordinary = Path.join(shard_path, "nested/flow_lmdb/lock.mdb")
        File.mkdir_p!(Path.dirname(ordinary))
        File.write!(ordinary, "ordinary payload")
        assert {:ok, {:raft_log_pos, index, term}} = WARaftBackend.storage_position(0)

        snapshot =
          Path.join([root, "waraft", "ferricstore_waraft_backend.1", "snapshot.#{index}.#{term}"])

        previous = Application.get_env(:ferricstore, :waraft_snapshot_create_hook)
        parent = self()

        Application.put_env(:ferricstore, :waraft_snapshot_create_hook, fn
          {:copied, :data} ->
            send(
              parent,
              {:snapshot_lock_state,
               File.exists?(Path.join([snapshot, "data", "flow_lmdb", "lock.mdb"])),
               File.exists?(Path.join([snapshot, "data", "flow_lmdb", "data.mdb"]))}
            )

            :ok

          _ ->
            :ok
        end)

        try do
          assert {:ok, _position} = WARaftBackend.create_snapshot(0)
          assert_receive {:snapshot_lock_state, false, true}

          assert File.read!(Path.join([snapshot, "data", "nested", "flow_lmdb", "lock.mdb"])) ==
                   "ordinary payload"

          assert {:ok, "preserved"} =
                   LMDB.get(Path.join([snapshot, "data", "flow_lmdb"]), "snapshot:sentinel")
        after
          restore_env(:waraft_snapshot_create_hook, previous)
          LMDB.release(Path.join([snapshot, "data", "flow_lmdb"]))
        end
      end
    end
  end
end
