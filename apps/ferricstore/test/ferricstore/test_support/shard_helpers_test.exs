defmodule Ferricstore.TestSupport.ShardHelpersTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Test.ShardHelpers

  test "isolated data-dir teardown preserves existing original data dir" do
    original_env = Application.get_env(:ferricstore, :data_dir, "data")
    server_started? = application_started?(:ferricstore_server)

    original_dir =
      Path.join(System.tmp_dir!(), "ferricstore_original_#{System.unique_integer([:positive])}")

    sentinel = Path.join(original_dir, "sentinel")

    File.mkdir_p!(original_dir)
    File.write!(sentinel, "keep")
    Application.put_env(:ferricstore, :data_dir, original_dir)

    on_exit(fn ->
      restore_data_dir(original_env, server_started?)
      File.rm_rf!(original_dir)
    end)

    ctx = ShardHelpers.setup_isolated_data_dir()
    ShardHelpers.teardown_isolated_data_dir(ctx)

    assert File.read!(sentinel) == "keep"
  end

  defp restore_data_dir(data_dir, server_started?) do
    stop_app_if_started(:ferricstore_server)
    stop_app_if_started(:ferricstore)

    try do
      :ra_system.stop(Ferricstore.Raft.Cluster.system_name())
    catch
      _, _ -> :ok
    end

    Application.put_env(:ferricstore, :data_dir, data_dir)
    {:ok, _} = Application.ensure_all_started(:ferricstore)
    ShardHelpers.wait_shards_alive()

    if server_started? do
      {:ok, _} = Application.ensure_all_started(:ferricstore_server)
    end
  end

  defp stop_app_if_started(app) do
    if application_started?(app) do
      _ = Application.stop(app)
    end
  end

  defp application_started?(app) do
    Enum.any?(Application.started_applications(), fn {started_app, _desc, _vsn} ->
      started_app == app
    end)
  end
end
