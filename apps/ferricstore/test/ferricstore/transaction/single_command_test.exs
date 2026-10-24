defmodule Ferricstore.Transaction.SingleCommandTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Transaction.SingleCommand

  setup do
    %{
      ctx: FerricStore.Instance.get(:default),
      key: "single-command:#{System.unique_integer([:positive])}"
    }
  end

  test "runs a command the shard can apply as a transaction", %{ctx: ctx, key: key} do
    assert :ok == FerricStore.set(key, "v")

    assert 1 ==
             SingleCommand.run(ctx, "PEXPIREAT", [key, "#{System.os_time(:millisecond) + 60_000}"])
  end

  # Callers fall back to the command's own path; a refusal must not become
  # the command's reply.
  test "falls back for a command transactions refuse", %{ctx: ctx, key: key} do
    assert :fallback == SingleCommand.run(ctx, "TDIGEST.CREATE", [key])
    assert :fallback == SingleCommand.run(ctx, "LOCK", [key, "owner", "1000"])
  end

  test "falls back off the default instance and for keys on two shards", %{key: key} do
    assert :fallback == SingleCommand.run(%{}, "PERSIST", [key])

    ctx = FerricStore.Instance.get(:default)
    [a, b] = two_shard_keys(ctx)
    assert :fallback == SingleCommand.run(ctx, "SUNIONSTORE", [a, a, b])
  end

  defp two_shard_keys(ctx) do
    keys = Enum.map(1..64, &"single-command-shard:#{&1}")
    a = hd(keys)
    shard = Ferricstore.Store.Router.shard_for(ctx, a)
    b = Enum.find(keys, &(Ferricstore.Store.Router.shard_for(ctx, &1) != shard))
    [a, b]
  end
end
