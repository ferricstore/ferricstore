defmodule Ferricstore.Test.DataDirLifecycleTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Test.DataDirLifecycle

  @repo_root Path.expand("../../../..", __DIR__)

  test "generated cleanup stops applications before deleting the data directory" do
    data_dir =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_test_cleanup_#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(data_dir)
    on_exit(fn -> File.rm_rf!(data_dir) end)
    parent = self()

    stop = fn app ->
      send(parent, {:stopped, app})
      :ok
    end

    remove = fn path ->
      send(parent, {:removed, path})
      {:ok, []}
    end

    assert :ok = DataDirLifecycle.cleanup_generated(data_dir, stop, remove)
    assert_receive {:stopped, :ferricstore_server}
    assert_receive {:stopped, :ferricstore}
    assert_receive {:removed, ^data_dir}
  end

  test "cleanup refuses paths outside the generated test-root namespace" do
    parent = self()
    stop = fn app -> send(parent, {:stopped, app}) end
    remove = fn path -> send(parent, {:removed, path}) end

    assert {:error, :unsafe_data_dir} =
             DataDirLifecycle.cleanup_generated(
               Path.join(System.tmp_dir!(), "operator-owned-data"),
               stop,
               remove
             )

    refute_received {:stopped, _app}
    refute_received {:removed, _path}
  end

  test "cleanup preserves data when an application cannot be stopped" do
    data_dir =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_test_cleanup_failure_#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(data_dir)
    on_exit(fn -> File.rm_rf!(data_dir) end)
    parent = self()

    stop = fn
      :ferricstore_server -> {:error, :shutdown_timeout}
      app -> send(parent, {:stopped, app})
    end

    remove = fn path -> send(parent, {:removed, path}) end

    assert {:error, {:application_stop_failed, :ferricstore_server, {:error, :shutdown_timeout}}} =
             DataDirLifecycle.cleanup_generated(data_dir, stop, remove)

    assert File.dir?(data_dir)
    refute_received {:stopped, :ferricstore}
    refute_received {:removed, ^data_dir}
  end

  test "both test entrypoints register generated-root cleanup" do
    for relative <- [
          "apps/ferricstore/test/test_helper.exs",
          "apps/ferricstore_server/test/test_helper.exs"
        ] do
      source = File.read!(Path.join(@repo_root, relative))
      assert source =~ "DataDirLifecycle.register_generated_cleanup()"
    end
  end
end
