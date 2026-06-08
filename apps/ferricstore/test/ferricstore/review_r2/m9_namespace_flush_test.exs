defmodule Ferricstore.ReviewR2.M9NamespaceFlushTest do
  @moduledoc """
  Verifies that namespace config changes flush open batcher slots
  so queued commands don't use stale window_ms.
  """

  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.NamespaceConfig
  alias Ferricstore.Raft.Batcher
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  setup do
    ShardHelpers.flush_all_keys()
    NamespaceConfig.reset_all()

    on_exit(fn ->
      NamespaceConfig.reset_all()
      ShardHelpers.flush_all_keys()
    end)

    :ok
  end

  describe "namespace config change flushes batcher slots" do
    test "write completes after config change (slot flushed)" do
      # Set a long window so the slot timer doesn't fire naturally
      NamespaceConfig.set("m9ns", "window_ms", "5000")

      key = "m9ns:test_#{:rand.uniform(999_999)}"
      shard_idx = Router.shard_for(FerricStore.Instance.get(:default), key)

      # Write a key — it enters the batcher slot with 5000ms window
      assert :ok = Batcher.write(shard_idx, {:put, key, "before_change", 0})

      # Change the config — should flush the open slot immediately
      NamespaceConfig.set("m9ns", "window_ms", "10")

      # The write should have been flushed by the config change
      # Give a small window for the flush to propagate
      Process.sleep(50)

      # The key should be readable (slot was flushed, not waiting 5s)
      assert Router.get(FerricStore.Instance.get(:default), key) == "before_change"
    end

    test "subsequent writes use new config after change" do
      NamespaceConfig.set("m9b", "window_ms", "5000")

      key1 = "m9b:first_#{:rand.uniform(999_999)}"
      shard_idx = Router.shard_for(FerricStore.Instance.get(:default), key1)

      # First write with old config
      Batcher.write(shard_idx, {:put, key1, "old_window", 0})

      # Change to fast window
      NamespaceConfig.set("m9b", "window_ms", "1")

      # Second write should use new 1ms window
      key2 = "m9b:second_#{:rand.uniform(999_999)}"
      shard_idx2 = Router.shard_for(FerricStore.Instance.get(:default), key2)
      Batcher.write(shard_idx2, {:put, key2, "new_window", 0})

      # Both should be readable quickly (first flushed by config change,
      # second flushed by 1ms timer)
      Process.sleep(50)

      assert Router.get(FerricStore.Instance.get(:default), key1) == "old_window"
      assert Router.get(FerricStore.Instance.get(:default), key2) == "new_window"
    end
  end
end
