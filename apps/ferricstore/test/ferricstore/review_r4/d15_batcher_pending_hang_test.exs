defmodule Ferricstore.ReviewR4.D15BatcherPendingHangTest do
  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.Raft.Batcher
  alias Ferricstore.Test.ShardHelpers

  @moduletag :review_r4

  setup do
    ShardHelpers.flush_all_keys()

    on_exit(fn ->
      ShardHelpers.flush_all_keys()
    end)

    :ok
  end

  describe "D15: stale quorum pending entries" do
    test "legacy default batcher facade has no orphan pending registry" do
      refute function_exported?(Batcher, :__inject_quorum_pending_at__, 5)
      refute function_exported?(Batcher, :__sweep_pending_now__, 1)
      refute function_exported?(Batcher, :__has_pending__, 2)

      assert :ok = Batcher.flush(0)
    end
  end
end
