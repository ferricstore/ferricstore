defmodule Ferricstore.Store.RouterRecoveryFenceTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Store.Router
  alias Ferricstore.Store.StandaloneTxLog
  alias Ferricstore.Test.IsolatedInstance
  alias Ferricstore.Test.ShardHelpers

  setup do
    ShardHelpers.wait_shards_alive()
    ctx = IsolatedInstance.checkout(shard_count: 2)
    on_exit(fn -> IsolatedInstance.checkin(ctx) end)
    {:ok, ctx: ctx}
  end

  test "write fences do not read the recovery marker from disk on each write", %{
    ctx: ctx
  } do
    marker_path = StandaloneTxLog.recovery_marker_path(ctx.data_dir)
    refute File.exists?(marker_path)

    mfa = {Ferricstore.FS, :read_nofollow, 2}
    :erlang.trace_pattern(mfa, true, [:global])
    # Router and shard processes both fence writes, so trace every process.
    :erlang.trace(:all, true, [:call])

    on_exit(fn ->
      :erlang.trace(:all, false, [:call])
      :erlang.trace_pattern(mfa, false, [:global])
    end)

    for index <- 1..20 do
      assert :ok = Router.put(ctx, "fence:#{index}", "value", 0)
    end

    :erlang.trace(:all, false, [:call])

    assert marker_reads(marker_path) == 0
  end

  test "router write fence still rejects writes once recovery is required", %{ctx: ctx} do
    assert :ok = Router.put(ctx, "fence:before", "value", 0)
    assert :ok = StandaloneTxLog.require_recovery(ctx.data_dir, "router fence test")
    on_exit(fn -> StandaloneTxLog.recover(ctx.data_dir) end)

    assert {:error, "ERR shard writes paused for sync"} =
             Router.put(ctx, "fence:after", "value", 0)
  end

  defp marker_reads(marker_path, count \\ 0) do
    receive do
      {:trace, _pid, :call, {Ferricstore.FS, :read_nofollow, [^marker_path, _limit]}} ->
        marker_reads(marker_path, count + 1)

      {:trace, _pid, :call, _other} ->
        marker_reads(marker_path, count)
    after
      100 -> count
    end
  end
end
