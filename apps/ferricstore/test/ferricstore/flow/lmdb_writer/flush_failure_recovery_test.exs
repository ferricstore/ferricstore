defmodule Ferricstore.Flow.LMDBWriter.FlushFailureRecoveryTest do
  use ExUnit.Case, async: false
  @moduletag :flow
  @moduletag :global_state

  alias FerricStore.Impl
  alias Ferricstore.Flow.{Keys, LMDBMirror, LMDBWriter}
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.{IsolatedInstance, ShardHelpers}

  setup do
    ctx = IsolatedInstance.checkout()

    on_exit(fn ->
      Ferricstore.FaultInjection.clear_hook()
      IsolatedInstance.checkin(ctx)
    end)

    {:ok, ctx: ctx}
  end

  test "a shard whose flush failed is healthy again once a retry succeeds, with no new writes",
       %{ctx: ctx} do
    partition_key = "flush-retry"
    now_ms = System.system_time(:millisecond)

    shard = Router.shard_for(ctx, Keys.state_key("flush-retry-1", partition_key))
    instance = ctx.name

    # Every flush of this shard fails, including the writer's own timer flushes.
    Ferricstore.FaultInjection.put_hook(fn
      :before_flow_lmdb_flush_write, %{instance_name: ^instance, shard_index: ^shard} ->
        {:error, :flush_failure_recovery_test}

      _point, _metadata ->
        :ok
    end)

    assert :ok =
             Impl.flow_create(ctx, "flush-retry-1",
               type: "flush.retry",
               state: "queued",
               partition_key: partition_key,
               now_ms: now_ms
             )

    assert {:error, :flush_failure_recovery_test} = LMDBWriter.flush(instance, shard, 30_000)
    assert LMDBMirror.degraded_flag?(ctx, shard)

    # The fault clears and nothing else is written: the writer's own retry has
    # to bring the shard back.
    Ferricstore.FaultInjection.clear_hook()

    assert :ok =
             ShardHelpers.eventually(
               fn -> not LMDBMirror.degraded_flag?(ctx, shard) end,
               "the shard should leave the degraded state after a successful retry",
               200,
               50
             )

    assert {:ok, %{state: "queued"}} =
             Impl.flow_get(ctx, "flush-retry-1", partition_key: partition_key)
  end
end
