defmodule Ferricstore.GracefulShutdownIsolationTest do
  use ExUnit.Case, async: false
  @moduletag :shard_kill

  import ExUnit.CaptureLog

  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  test "isolated data dir setup does not corrupt live Ra WAL state" do
    original_dir = Application.fetch_env!(:ferricstore, :data_dir)

    log =
      capture_log(fn ->
        ctx = ShardHelpers.setup_isolated_data_dir()

        try do
          assert %{original_dir: ^original_dir, tmp_dir: tmp_dir} = ctx
          assert tmp_dir != original_dir
          assert Application.fetch_env!(:ferricstore, :data_dir) == tmp_dir

          Router.put(FerricStore.Instance.get(:default), "iso_restart_key", "value")
          ShardHelpers.flush_all_shards()
        after
          ShardHelpers.teardown_isolated_data_dir(ctx)
        end

        ctx = ShardHelpers.setup_isolated_data_dir()

        try do
          ShardHelpers.wait_shards_alive()
        after
          ShardHelpers.teardown_isolated_data_dir(ctx)
        end
      end)

    refute log =~ "corrupt_log"
    refute log =~ "attempting fresh start with unique UID"
  end
end
