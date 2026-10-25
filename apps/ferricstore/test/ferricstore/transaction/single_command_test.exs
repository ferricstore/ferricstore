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
    assert :fallback == SingleCommand.run(ctx, "BF.ADD", [key, "item"])
    assert :fallback == SingleCommand.run(ctx, "LOCK", [key, "owner", "1000"])
  end

  # A refused entry never applied, so the command's own path may run it; an
  # entry whose outcome is unknown may have applied, and running it again
  # would apply BITOP XOR or NOT into a source twice, undoing it.
  test "falls back only when the entry is known not to have applied", %{ctx: ctx, key: key} do
    on_exit(fn -> Process.delete(:ferricstore_tx_backend_write_hook) end)
    args = [key, "#{System.os_time(:millisecond) + 60_000}"]

    Process.put(:ferricstore_tx_backend_write_hook, fn _shard, _command ->
      {:error, :compound_member_apply_budget_exceeded}
    end)

    assert :fallback == SingleCommand.run(ctx, "PEXPIREAT", args)

    for failure <- [
          fn -> exit({:timeout, :synthetic}) end,
          fn -> {:error, {:timeout, :unknown_outcome}} end
        ] do
      Process.put(:ferricstore_tx_backend_write_hook, fn _shard, _command -> failure.() end)
      assert {:error, {:timeout, :unknown_outcome}} == SingleCommand.run(ctx, "PEXPIREAT", args)
    end
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
