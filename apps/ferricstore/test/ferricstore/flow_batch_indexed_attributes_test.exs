defmodule Ferricstore.FlowBatchIndexedAttributesTest do
  @moduledoc """
  A Flow closed by COMPLETE_MANY, FAIL_MANY, CANCEL_MANY or an independent
  (pipelined) batch is indexed under its type's policy, as the single-Flow
  commands index it, from feature level 5. Older releases keep the indexed
  names the record had, so an attribute such a batch merged into a Flow
  created without attributes never became broad-searchable.
  """
  use Ferricstore.Test.FlowCase
  @moduletag :global_state

  alias Ferricstore.Test.ShardHelpers

  @partition "tenant-batch-attrs"

  setup do
    # The feature level survives FLUSHDB, so raise it only on a throwaway data dir.
    isolated = ShardHelpers.setup_isolated_data_dir()
    on_exit(fn -> ShardHelpers.teardown_isolated_data_dir(isolated) end)
    :ok
  end

  test "below level 5 a Flow closed by COMPLETE_MANY keeps the indexed names it had" do
    type = unique_flow_id("batch-attrs-older")
    [single, many] = create_and_claim!(type, 2)

    complete_single!(single)
    complete_many!([many], [])

    assert_eventually(fn -> assert searched_ids(type) == MapSet.new([single.id]) end)
    assert {:ok, %{attributes: %{"tenant" => "acme"}}} = get(many.id)
  end

  test "at level 5 Flows closed in batches are indexed as single commands index them" do
    assert {:ok, %{active: 5}} = FerricStore.raise_cluster_feature_level(5)
    type = unique_flow_id("batch-attrs-level5")

    [single, many, independent, failed, cancelled] = create_and_claim!(type, 5)

    complete_single!(single)
    complete_many!([many], [])
    complete_many!([independent], independent: true)

    assert :ok =
             FerricStore.flow_fail_many(@partition, [
               {failed.id, failed.lease_token,
                fencing_token: failed.fencing_token,
                error: "boom",
                attributes_merge: %{"tenant" => "acme"},
                now_ms: 1_003}
             ])

    assert :ok =
             FerricStore.flow_cancel_many(@partition, [
               {cancelled.id,
                lease_token: cancelled.lease_token,
                fencing_token: cancelled.fencing_token,
                attributes_merge: %{"tenant" => "acme"},
                now_ms: 1_003}
             ])

    expected = MapSet.new([single.id, many.id, independent.id, failed.id, cancelled.id])
    assert_eventually(fn -> assert searched_ids(type) == expected end)
  end

  defp create_and_claim!(type, count) do
    assert {:ok, _policy} = FerricStore.flow_policy_set(type, indexed_attributes: ["tenant"])

    for n <- 1..count do
      assert :ok =
               FerricStore.flow_create(unique_flow_id("batch-attrs-#{n}"),
                 type: type,
                 state: "queued",
                 partition_key: @partition,
                 run_at_ms: 1_000,
                 now_ms: 1_000
               )
    end

    assert {:ok, claimed} =
             FerricStore.flow_claim_due(type,
               states: ["queued"],
               partition_key: @partition,
               worker: "w1",
               limit: count,
               now_ms: 1_001
             )

    assert length(claimed) == count
    claimed
  end

  defp complete_single!(claimed) do
    assert :ok =
             FerricStore.flow_complete(claimed.id, claimed.lease_token,
               fencing_token: claimed.fencing_token,
               partition_key: @partition,
               attributes_merge: %{"tenant" => "acme"},
               now_ms: 1_002
             )
  end

  defp complete_many!(claimed, opts) do
    items =
      Enum.map(claimed, fn flow ->
        {flow.id, flow.lease_token,
         fencing_token: flow.fencing_token, attributes_merge: %{"tenant" => "acme"}, now_ms: 1_002}
      end)

    # An independent batch answers each item.
    expected = if opts[:independent], do: {:ok, Enum.map(items, fn _ -> :ok end)}, else: :ok
    assert ^expected = FerricStore.flow_complete_many(@partition, items, opts)
  end

  defp searched_ids(type) do
    assert {:ok, records} =
             FerricStore.flow_search(
               type: type,
               partition_key: @partition,
               attributes: %{"tenant" => "acme"},
               consistent_projection: true,
               count: 10
             )

    MapSet.new(records, & &1.id)
  end

  defp get(id), do: FerricStore.flow_get(id, partition_key: @partition)
end
