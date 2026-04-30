defmodule Ferricstore.ReviewR4.D15BatcherPendingHangTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Raft.Batcher
  alias Ferricstore.Test.ShardHelpers

  @moduletag :review_r4

  setup do
    ShardHelpers.flush_all_keys()

    on_exit(fn ->
      Batcher.reset_pending(0)
      ShardHelpers.flush_all_keys()
    end)

    :ok
  end

  describe "D15: stale quorum pending entries" do
    test "stale single pending entry replies unknown outcome and is removed" do
      corr = make_ref()
      reply_ref = make_ref()

      Batcher.__inject_quorum_pending_at__(0, corr, [{self(), reply_ref}], :single, old_mono())
      assert Batcher.__has_pending__(0, corr)

      assert :ok = Batcher.__sweep_pending_now__(0)

      assert_receive {^reply_ref, {:error, {:timeout, :unknown_outcome}}}, 1_000
      refute Batcher.__has_pending__(0, corr)
    end

    test "stale batch pending entry replies unknown outcome and is removed" do
      corr = make_ref()
      reply_ref = make_ref()

      Batcher.__inject_quorum_pending_at__(0, corr, [{self(), reply_ref}], :batch, old_mono())
      assert Batcher.__has_pending__(0, corr)

      assert :ok = Batcher.__sweep_pending_now__(0)

      assert_receive {^reply_ref, {:error, {:timeout, :unknown_outcome}}}, 1_000
      refute Batcher.__has_pending__(0, corr)
    end
  end

  defp old_mono do
    System.monotonic_time() - System.convert_time_unit(60_000, :millisecond, :native)
  end
end
