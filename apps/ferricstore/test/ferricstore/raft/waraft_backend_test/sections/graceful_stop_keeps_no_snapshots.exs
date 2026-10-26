defmodule Ferricstore.Raft.WARaftBackendTest.Sections.GracefulStopKeepsNoSnapshots do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Raft.WARaftBackend
      alias Ferricstore.Store.Router

      # A snapshot is a full copy of the shard. Stops used to write one per
      # shard, which took minutes on large data and kept two copies per shard
      # on disk, though nothing reads them at startup.
      test "a graceful stop leaves no snapshot behind", %{ctx: ctx, root: root} do
        assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
        assert :ok = WARaftBackend.write(0, {:put, "stop:no-snapshot", "value", 0})
        assert :ok = WARaftBackend.stop()

        assert local_snapshot_names(root, 1) == []

        FerricStore.Instance.cleanup(ctx.name)
        restarted_ctx = build_ctx(root)

        assert :ok =
                 WARaftBackend.start(restarted_ctx,
                   log_module: :ferricstore_waraft_spike_segment_log
                 )

        assert Router.get(restarted_ctx, "stop:no-snapshot") == "value"
      end

      test "startup removes snapshots that earlier stops left", %{ctx: ctx, root: root} do
        assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
        assert :ok = WARaftBackend.write(0, {:put, "stop:stale-snapshot", "value", 0})
        assert {:ok, {:raft_log_pos, index, term}} = WARaftBackend.create_snapshot(0)
        assert :ok = WARaftBackend.stop()

        partition_root = Path.join([root, "waraft", "ferricstore_waraft_backend.1"])
        File.mkdir_p!(Path.join(partition_root, "snapshot.1.1.bootstrap.tmp"))
        File.mkdir_p!(Path.join(partition_root, "snapshot.notes"))
        assert "snapshot.#{index}.#{term}" in local_snapshot_names(root, 1)

        FerricStore.Instance.cleanup(ctx.name)
        restarted_ctx = build_ctx(root)

        assert :ok =
                 WARaftBackend.start(restarted_ctx,
                   log_module: :ferricstore_waraft_spike_segment_log
                 )

        assert local_snapshot_names(root, 1) == ["snapshot.notes"]
        assert Router.get(restarted_ctx, "stop:stale-snapshot") == "value"
      end

      defp local_snapshot_names(root, partition) do
        [root, "waraft", "ferricstore_waraft_backend.#{partition}", "snapshot.*"]
        |> Path.join()
        |> Path.wildcard()
        |> Enum.map(&Path.basename/1)
        |> Enum.sort()
      end
    end
  end
end
