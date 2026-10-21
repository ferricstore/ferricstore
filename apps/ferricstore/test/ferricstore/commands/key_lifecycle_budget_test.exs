defmodule Ferricstore.Commands.KeyLifecycleBudgetTest do
  @moduledoc """
  COPY and RENAME are applied as key-lifecycle entries. The transaction
  store's member budget was never reset between those entries, so after a
  few large copies or renames one threw past the apply and the shard's
  storage process exited, taking the shard down until it restarted.
  """
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.Dispatcher

  test "repeated COPY and RENAME of large collections keep applying" do
    ctx = FerricStore.Instance.get(:default)
    tag = "{lifecycle-budget-#{System.unique_integer([:positive])}}"
    storage = storage_pids(ctx)

    for round <- 1..12 do
      key = "#{tag}:h#{round}"

      assert 300 ==
               Dispatcher.dispatch("HSET", [key | Enum.flat_map(1..300, &["f#{&1}", "v"])], ctx)

      assert 1 == Dispatcher.dispatch("COPY", [key, key <> ":copy"], ctx)
      assert :ok == Dispatcher.dispatch("RENAME", [key, key <> ":ren"], ctx)
      assert 300 == Dispatcher.dispatch("HLEN", [key <> ":ren"], ctx)
    end

    assert storage_pids(ctx) == storage
  end

  defp storage_pids(ctx) do
    for shard <- 0..(ctx.shard_count - 1),
        do: Process.whereis(:"raft_storage_ferricstore_waraft_backend_#{shard}")
  end
end
