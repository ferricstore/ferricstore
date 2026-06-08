defmodule Ferricstore.ReviewR2.C3MaxKeysLimitTest do
  @moduledoc """
  Verifies that cross-shard operations are limited to 20 keys max.
  Prevents lock TTL expiry during slow multi-key operations.
  """

  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.CrossShardOp
  alias Ferricstore.Test.ShardHelpers

  setup do
    ShardHelpers.flush_all_keys()
    :ok
  end

  describe "cross-shard key limit" do
    test "21 keys returns error" do
      # Generate 21 keys that span at least 2 shards
      keys_with_roles =
        for i <- 1..21 do
          shard = rem(i, 2)
          key = ShardHelpers.key_for_shard(shard) <> "_limit_#{i}"
          {key, :write}
        end

      result =
        CrossShardOp.execute(
          keys_with_roles,
          fn _store -> :should_not_reach end,
          intent: %{command: :test}
        )

      assert {:error, msg} = result
      assert String.contains?(msg, "max key limit")
    end

    test "20 keys is accepted (within limit)" do
      keys_with_roles =
        for i <- 1..20 do
          shard = rem(i, 2)
          key = ShardHelpers.key_for_shard(shard) <> "_ok_#{i}"
          {key, :read}
        end

      # This should attempt execution (not be rejected by limit).
      # It may fail for other reasons (e.g., empty store) but not the key limit.
      result =
        CrossShardOp.execute(
          keys_with_roles,
          fn _store -> :executed end,
          intent: %{command: :test}
        )

      # Should either succeed or fail for reasons other than key limit
      case result do
        {:error, msg} when is_binary(msg) ->
          refute String.contains?(msg, "max key limit"),
                 "Should not be rejected by key limit, got: #{msg}"

        _ ->
          :ok
      end
    end

    test "same-shard operations bypass the limit" do
      # Find 30 keys that all route to shard 0
      keys =
        Stream.iterate(0, &(&1 + 1))
        |> Stream.map(fn i -> "same_shard_limit_#{i}" end)
        |> Stream.filter(fn k ->
          Ferricstore.Store.Router.shard_for(FerricStore.Instance.get(:default), k) == 0
        end)
        |> Enum.take(30)

      keys_with_roles = Enum.map(keys, fn k -> {k, :write} end)

      result =
        CrossShardOp.execute(
          keys_with_roles,
          fn _store -> :same_shard_ok end,
          intent: %{command: :test}
        )

      assert result == :same_shard_ok
    end
  end
end
