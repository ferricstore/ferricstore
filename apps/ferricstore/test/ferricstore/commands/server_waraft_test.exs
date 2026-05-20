defmodule Ferricstore.Commands.ServerWARaftTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.Server

  test "SAVE skips legacy Ra batcher flushes when WARaft is selected" do
    old_backend = Application.get_env(:ferricstore, :raft_backend)
    Application.put_env(:ferricstore, :raft_backend, :waraft)

    on_exit(fn -> restore_env(:raft_backend, old_backend) end)

    # Use a shard count above the default test app's legacy batchers. Before
    # the WARaft guard this tried to flush Ferricstore.Raft.Batcher.4 and failed
    # even though WARaft does not use those processes.
    dir =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_server_waraft_save_#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(dir)
    path = Path.join(dir, "00000.log")
    File.write!(path, "")
    Ferricstore.Store.ActiveFile.publish(4, 0, path, dir)

    on_exit(fn ->
      if :ets.whereis(:ferricstore_active_files) != :undefined do
        :ets.delete(:ferricstore_active_files, 4)
        :atomics.add(:persistent_term.get(:ferricstore_active_file_gen), 1, 1)
      end

      File.rm_rf!(dir)
    end)

    ctx = %FerricStore.Instance{name: :default, shard_count: 5}

    assert :ok = Server.handle("SAVE", [], ctx)
  end

  test "DEBUG BATCHER-STATS reports WARaft processes instead of legacy Ra processes" do
    old_backend = Application.get_env(:ferricstore, :raft_backend)
    Application.put_env(:ferricstore, :raft_backend, :waraft)

    on_exit(fn -> restore_env(:raft_backend, old_backend) end)

    assert {:simple, stats_line} = Server.handle("DEBUG", ["BATCHER-STATS"], nil)

    assert String.contains?(stats_line, "WA0:")
    assert String.contains?(stats_line, "server=")
    assert String.contains?(stats_line, "acceptor=")
    assert String.contains?(stats_line, "queue=")
    assert String.contains?(stats_line, "storage=")
    assert String.contains?(stats_line, "inflight_bytes=0")

    refute String.contains?(stats_line, "B0")
    refute String.contains?(stats_line, "WAL")
    refute String.contains?(stats_line, "R0")
    refute String.contains?(stats_line, "\n")
  end

  test "INFO raft reports WARaft in-flight commit bytes" do
    old_backend = Application.get_env(:ferricstore, :raft_backend)
    Application.put_env(:ferricstore, :raft_backend, :waraft)

    on_exit(fn -> restore_env(:raft_backend, old_backend) end)

    info = Server.handle("INFO", ["raft"], nil)

    assert String.contains?(info, "shard_0_waraft_inflight_commit_bytes:0")
  end

  defp restore_env(key, nil), do: Application.delete_env(:ferricstore, key)
  defp restore_env(key, value), do: Application.put_env(:ferricstore, key, value)
end
