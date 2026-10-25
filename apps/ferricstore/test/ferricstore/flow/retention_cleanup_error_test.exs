defmodule Ferricstore.Flow.RetentionCleanupErrorTest do
  @moduledoc """
  Retention cleanup first flushes the Flow mirror writers and history
  projectors. When a flush failed or timed out, the command answered the raw
  internal reason (`{:timeout, {GenServer, :call, [pid, :flush, 30000]}}`) to
  native clients and the embedded API, whose spec promises a binary.
  """
  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.Flow.LMDBWriter
  alias Ferricstore.Test.ShardHelpers

  setup do
    isolated = ShardHelpers.setup_isolated_data_dir()
    ctx = FerricStore.Instance.get(:default)

    on_exit(fn ->
      LMDBWriter.resume_all(ctx.name, ctx.shard_count)
      ShardHelpers.teardown_isolated_data_dir(isolated)
    end)

    %{ctx: ctx}
  end

  test "a failed Flow storage flush answers a client error", %{ctx: ctx} do
    assert :ok = LMDBWriter.suspend_all(ctx.name, ctx.shard_count, flush: true)

    assert {:error, message} = FerricStore.flow_retention_cleanup(limit: 1)
    assert is_binary(message)
    assert message =~ "ERR flow retention cleanup could not flush Flow storage"

    assert :ok = LMDBWriter.resume_all(ctx.name, ctx.shard_count)
    assert {:ok, %{flows: 0}} = FerricStore.flow_retention_cleanup(limit: 1)
  end
end
