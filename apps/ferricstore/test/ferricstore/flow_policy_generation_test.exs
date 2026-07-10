defmodule Ferricstore.FlowPolicyGenerationTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Flow.{Keys, RetryPolicy}
  alias Ferricstore.Store.Router

  test "policy generations are always allocated and advance monotonically" do
    ctx = FerricStore.Instance.get(:default)
    type = "generation-always-on-#{System.unique_integer([:positive, :monotonic])}"

    assert {:ok, _policy} = FerricStore.flow_policy_set(type, max_active_ms: 1_000)
    assert_policy_generation_on_all_shards(ctx, type, 1)

    assert {:ok, _policy} = FerricStore.flow_policy_set(type, max_active_ms: 2_000)
    assert_policy_generation_on_all_shards(ctx, type, 2)
  end

  defp assert_policy_generation_on_all_shards(ctx, type, expected_generation) do
    key = Keys.policy_key(type)

    for shard_index <- 0..(ctx.shard_count - 1) do
      assert {:ok, value} = Router.read_shard_value(ctx, shard_index, key)
      assert {:ok, {^expected_generation, policy}} = RetryPolicy.decode_flow_policy_entry(value)
      assert policy.type == type
    end
  end
end
