defmodule Ferricstore.FlowPolicyRepairTest do
  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.Flow.{Keys, RetryPolicy}
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  setup do
    # The feature level survives FLUSHDB, so raise it only on a throwaway data dir.
    isolated = ShardHelpers.setup_isolated_data_dir()
    on_exit(fn -> ShardHelpers.teardown_isolated_data_dir(isolated) end)
    {:ok, ctx: FerricStore.Instance.get(:default)}
  end

  defp unique(prefix), do: "#{prefix}-#{System.unique_integer([:positive, :monotonic])}"

  defp foreign_policy_value(generation) do
    {:ok, policy} = RetryPolicy.normalize_flow_policy("foreign-policy-type", max_active_ms: 9_000)
    RetryPolicy.encode_flow_policy(policy, generation)
  end

  defp write_raw(shard_index, key, value),
    do: :ok = Ferricstore.Raft.Backend.write(shard_index, {:put, key, value, 0})

  test "replace cannot repair a corrupt policy until feature level 1 is active" do
    type = unique("repair-gated")
    write_raw(0, Keys.policy_key(type), foreign_policy_value(7))

    assert {:error, "ERR corrupt flow policy high-water"} =
             FerricStore.flow_policy_set(type, replace: true, max_active_ms: 5_000)

    assert {:error, "ERR flow policy is corrupt"} = FerricStore.flow_policy_get(type)
  end

  test "replace repairs a policy stored under another type above its generation" do
    type = unique("repair-foreign")
    write_raw(0, Keys.policy_key(type), foreign_policy_value(7))
    assert {:ok, %{active: 1}} = FerricStore.raise_cluster_feature_level(1)

    assert {:ok, %{generation: generation, max_active_ms: 5_000}} =
             FerricStore.flow_policy_set(type, replace: true, max_active_ms: 5_000)

    assert generation > 7

    assert {:ok, %{generation: ^generation, max_active_ms: 5_000}} =
             FerricStore.flow_policy_get(type)

    assert :ok =
             FerricStore.flow_create(unique("repaired-flow"),
               type: type,
               state: "queued",
               partition_key: "repair-tenant"
             )
  end

  test "replace repairs an unreadable policy above every earlier generation" do
    type = unique("repair-garbage")
    assert {:ok, %{generation: 1}} = FerricStore.flow_policy_set(type, max_active_ms: 1_000)
    assert {:ok, %{generation: 2}} = FerricStore.flow_policy_set(type, max_active_ms: 2_000)
    write_raw(0, Keys.policy_key(type), "not-a-policy")
    assert {:ok, %{active: 1}} = FerricStore.raise_cluster_feature_level(1)

    assert {:ok, %{generation: generation, max_active_ms: 3_000}} =
             FerricStore.flow_policy_set(type, replace: true, max_active_ms: 3_000)

    assert generation > 2
    assert {:ok, %{generation: ^generation}} = FerricStore.flow_policy_get(type)
  end

  test "a corrupt policy still needs replace to be overwritten" do
    type = unique("repair-merge")
    write_raw(0, Keys.policy_key(type), foreign_policy_value(3))
    assert {:ok, %{active: 1}} = FerricStore.raise_cluster_feature_level(1)

    assert {:error, "ERR corrupt flow policy high-water"} =
             FerricStore.flow_policy_set(type, max_active_ms: 5_000)

    assert {:error, "ERR stale flow policy generation"} =
             FerricStore.flow_policy_set(type,
               replace: true,
               expected_generation: 99,
               max_active_ms: 5_000
             )
  end

  test "repair also replaces corrupt copies installed on other shards", %{ctx: ctx} do
    type = unique("repair-copies")
    key = Keys.policy_key(type)
    write_raw(0, key, foreign_policy_value(4))

    for shard_index <- 1..(ctx.shard_count - 1) do
      write_raw(
        shard_index,
        key,
        if(rem(shard_index, 2) == 0, do: "garbage", else: foreign_policy_value(4))
      )
    end

    assert {:ok, %{active: 1}} = FerricStore.raise_cluster_feature_level(1)

    assert {:ok, %{generation: generation}} =
             FerricStore.flow_policy_set(type, replace: true, max_active_ms: 6_000)

    for shard_index <- 0..(ctx.shard_count - 1) do
      assert {:ok, value} = Router.read_shard_value(ctx, shard_index, key)

      assert {:ok, {^generation, %{type: ^type, max_active_ms: 6_000}}} =
               RetryPolicy.decode_flow_policy_entry(value),
             "shard #{shard_index}"
    end
  end
end
