defmodule Ferricstore.Store.TypeRegistryFirstClaimIntegrationTest do
  use ExUnit.Case, async: false

  @moduletag :global_state

  alias Ferricstore.Test.ShardHelpers

  setup do
    ShardHelpers.reset_memory_guard_pressure()
    :ok
  end

  test "concurrent replicated creators preserve the first compound type" do
    ctx = FerricStore.Instance.get(:default)
    key = "replicated-type-claim:#{System.unique_integer([:positive])}"

    on_exit(fn -> FerricStore.Impl.del(ctx, [key]) end)

    results =
      [
        fn -> FerricStore.Impl.hset(ctx, key, %{"field" => "value"}) end,
        fn -> FerricStore.Impl.sadd(ctx, key, ["member"]) end
      ]
      |> Enum.map(&Task.async/1)
      |> Task.await_many(10_000)

    assert Enum.count(results, &match?({:ok, 1}, &1)) == 1,
           "expected one first type claim, got: #{inspect(results)}"

    assert Enum.count(results, fn
             {:error, message} when is_binary(message) -> String.contains?(message, "WRONGTYPE")
             _other -> false
           end) == 1

    assert Ferricstore.Store.TypeRegistry.get_type(key, ctx) in ["hash", "set"]
  end
end
