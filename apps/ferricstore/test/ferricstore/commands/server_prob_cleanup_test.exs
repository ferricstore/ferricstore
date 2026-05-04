defmodule Ferricstore.Commands.ServerProbCleanupTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.Server
  alias Ferricstore.Test.MockStore

  setup do
    old_data_dir = Application.get_env(:ferricstore, :data_dir)

    root =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_server_prob_cleanup_#{System.unique_integer([:positive])}"
      )

    prob_dir = Path.join([root, "data", "shard_0", "prob"])

    File.mkdir_p!(prob_dir)
    File.write!(Path.join(prob_dir, "stale.bloom"), "bits")

    Application.put_env(:ferricstore, :data_dir, root)

    Process.put(:ferricstore_prob_command_fsync_dir_hook, fn ^prob_dir -> {:error, :eio} end)

    on_exit(fn ->
      case old_data_dir do
        nil -> Application.delete_env(:ferricstore, :data_dir)
        value -> Application.put_env(:ferricstore, :data_dir, value)
      end

      Process.delete(:ferricstore_prob_command_fsync_dir_hook)
      File.rm_rf!(root)
    end)

    %{prob_dir: prob_dir}
  end

  test "FLUSHDB surfaces probabilistic directory fsync failures", %{prob_dir: prob_dir} do
    assert {:error, {:fsync_dir_failed, :flush_prob_dir, :eio}} =
             Server.handle("FLUSHDB", [], MockStore.make())

    assert Ferricstore.FS.exists?(prob_dir)
  end
end
