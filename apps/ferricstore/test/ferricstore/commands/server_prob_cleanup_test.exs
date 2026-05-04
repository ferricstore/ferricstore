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

  test "FLUSHDB returns store flush errors before cleanup", %{prob_dir: prob_dir} do
    store = Map.put(MockStore.make(), :flush, fn -> {:error, :flush_failed} end)
    stale_path = Path.join(prob_dir, "stale.bloom")

    Process.put(:ferricstore_prob_command_fsync_dir_hook, fn _path ->
      send(self(), :unexpected_prob_cleanup)
      :ok
    end)

    assert {:error, :flush_failed} = Server.handle("FLUSHDB", [], store)
    assert Ferricstore.FS.exists?(stale_path)
    refute_received :unexpected_prob_cleanup
  end

  test "FLUSHDB partial flush failure preserves sidecars for keys not flushed", %{
    prob_dir: prob_dir
  } do
    flushed_path = Path.join(prob_dir, "flushed.bloom")
    remaining_path = Path.join(prob_dir, "remaining.bloom")

    File.write!(flushed_path, "flushed bits")
    File.write!(remaining_path, "remaining bits")

    Process.put(:ferricstore_prob_command_fsync_dir_hook, fn _path ->
      send(self(), :unexpected_prob_cleanup)
      :ok
    end)

    store =
      Map.put(MockStore.make(%{"remaining" => {"prob meta", 0}}), :flush, fn ->
        File.rm!(flushed_path)
        {:error, {:flush_key_failed, "remaining", :eio}}
      end)

    assert {:error, {:flush_key_failed, "remaining", :eio}} =
             Server.handle("FLUSHDB", [], store)

    refute Ferricstore.FS.exists?(flushed_path)
    assert Ferricstore.FS.exists?(remaining_path)
    assert store.get.("remaining") == "prob meta"
    refute_received :unexpected_prob_cleanup
  end

  test "FLUSHALL returns store flush errors before cleanup", %{prob_dir: prob_dir} do
    store = Map.put(MockStore.make(), :flush, fn -> {:error, :flush_failed} end)
    stale_path = Path.join(prob_dir, "stale.bloom")

    Process.put(:ferricstore_prob_command_fsync_dir_hook, fn _path ->
      send(self(), :unexpected_prob_cleanup)
      :ok
    end)

    assert {:error, :flush_failed} = Server.handle("FLUSHALL", [], store)
    assert Ferricstore.FS.exists?(stale_path)
    refute_received :unexpected_prob_cleanup
  end

  test "FLUSHDB surfaces probabilistic directory listing failures", %{prob_dir: prob_dir} do
    Process.delete(:ferricstore_prob_command_fsync_dir_hook)
    File.rm_rf!(prob_dir)
    File.mkdir_p!(Path.dirname(prob_dir))
    File.write!(prob_dir, "not a directory")

    assert {:error, {:list_prob_dir_failed, ^prob_dir, {:not_a_directory, _message}}} =
             Server.handle("FLUSHDB", [], MockStore.make())
  end

  test "FLUSHDB surfaces unexpected probabilistic cleanup exceptions", %{prob_dir: prob_dir} do
    Process.put(:ferricstore_prob_command_fsync_dir_hook, fn ^prob_dir ->
      raise "prob cleanup exploded"
    end)

    assert {:error, {:flush_prob_dirs_failed, :error, %RuntimeError{message: message}}} =
             Server.handle("FLUSHDB", [], MockStore.make())

    assert message == "prob cleanup exploded"
  end
end
