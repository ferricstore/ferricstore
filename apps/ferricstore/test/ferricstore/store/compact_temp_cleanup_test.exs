defmodule Ferricstore.Store.CompactTempCleanupTest do
  @moduledoc """
  Tests that leftover compact_*.log temp files from a crashed compaction
  are cleaned up on shard startup and don't crash discover_active_file.
  """

  use ExUnit.Case, async: false
  @moduletag :global_state

  @moduletag :shard_kill

  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  setup do
    ShardHelpers.flush_all_keys()
    on_exit(fn -> ShardHelpers.wait_shards_alive() end)
    :ok
  end

  describe "compact temp file cleanup on startup" do
    test "shard starts successfully with leftover compact_*.log file" do
      # Write some data so the shard has a real log file
      Router.put(FerricStore.Instance.get(:default), "cleanup_test_key", "value", 0)
      ShardHelpers.flush_all_shards()

      # Get shard 0's data path and create a fake leftover compact file
      data_dir = Application.get_env(:ferricstore, :data_dir, "data")
      shard_path = Ferricstore.DataDir.shard_data_path(data_dir, 0)
      compact_file = Path.join(shard_path, "compact_0.log")
      File.write!(compact_file, "partial compaction data")

      assert File.exists?(compact_file)

      # Kill and restart shard — should NOT crash
      ShardHelpers.kill_shard_safely(0)

      # Shard should be alive and functional
      assert :ok = Router.put(FerricStore.Instance.get(:default), "after_cleanup", "works", 0)
      assert "works" == Router.get(FerricStore.Instance.get(:default), "after_cleanup")

      # The compact file should be gone
      refute File.exists?(compact_file)
    end

    test "shard starts with multiple leftover compact files" do
      data_dir = Application.get_env(:ferricstore, :data_dir, "data")
      shard_path = Ferricstore.DataDir.shard_data_path(data_dir, 0)

      # Create several compact temp files
      for i <- 0..3 do
        File.write!(Path.join(shard_path, "compact_#{i}.log"), "partial_#{i}")
      end

      ShardHelpers.kill_shard_safely(0)

      # All compact files should be cleaned up
      for i <- 0..3 do
        refute File.exists?(Path.join(shard_path, "compact_#{i}.log"))
      end

      # Shard is functional
      assert :ok = Router.put(FerricStore.Instance.get(:default), "multi_cleanup", "ok", 0)
      assert "ok" == Router.get(FerricStore.Instance.get(:default), "multi_cleanup")
    end

    test "original data survives when compact temp file is cleaned" do
      key = ShardHelpers.key_for_shard(0)
      Router.put(FerricStore.Instance.get(:default), key, "original_data", 0)
      ShardHelpers.flush_all_shards()

      # Create a compact temp file (simulates crash mid-compaction)
      data_dir = Application.get_env(:ferricstore, :data_dir, "data")
      shard_path = Ferricstore.DataDir.shard_data_path(data_dir, 0)
      File.write!(Path.join(shard_path, "compact_0.log"), "garbage")

      ShardHelpers.kill_shard_safely(0)

      # Original data still readable after restart
      ShardHelpers.eventually(
        fn ->
          Router.get(FerricStore.Instance.get(:default), key) == "original_data"
        end,
        "original data should survive cleanup of compact temp files"
      )
    end
  end
end
