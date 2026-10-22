defmodule Ferricstore.FlowBatchStateWriteErrorTest do
  @moduledoc """
  A state record write that apply refuses (here, a corrupt type catalog row)
  fails COMPLETE_MANY at feature level 5, as it fails FLOW.COMPLETE. Older
  releases went on to write the batch's history and indexes and answered OK.
  """
  use Ferricstore.Test.FlowCase
  @moduletag :global_state

  alias Ferricstore.Flow.Keys
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  @partition "tenant-batch-write-error"

  setup do
    # The feature level survives FLUSHDB, so raise it only on a throwaway data dir.
    isolated = ShardHelpers.setup_isolated_data_dir()
    on_exit(fn -> ShardHelpers.teardown_isolated_data_dir(isolated) end)
    :ok
  end

  test "below level 5 COMPLETE_MANY answers OK past a refused state write" do
    claimed = create_claim_and_corrupt_catalog!(unique_flow_id("batch-write-error-older"))

    assert :ok = complete_many(claimed)
  end

  test "at level 5 COMPLETE_MANY fails on a refused state write, as FLOW.COMPLETE does" do
    assert {:ok, %{active: 5}} = FerricStore.raise_cluster_feature_level(5)
    claimed = create_claim_and_corrupt_catalog!(unique_flow_id("batch-write-error-level5"))

    assert {:error, "ERR flow type catalog entry is corrupt"} =
             FerricStore.flow_complete(claimed.id, claimed.lease_token,
               fencing_token: claimed.fencing_token,
               partition_key: @partition,
               now_ms: 1_002
             )

    assert {:error, "ERR flow type catalog entry is corrupt"} = complete_many(claimed)

    assert {:ok, %{state: "running"}} =
             FerricStore.flow_get(claimed.id, partition_key: @partition)
  end

  defp create_claim_and_corrupt_catalog!(type) do
    id = unique_flow_id("batch-write-error")

    assert :ok =
             FerricStore.flow_create(id,
               type: type,
               state: "queued",
               partition_key: @partition,
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due(type,
               states: ["queued"],
               partition_key: @partition,
               worker: "w1",
               limit: 1,
               now_ms: 1_001
             )

    catalog_key = Keys.type_catalog_member_key(type, Keys.state_key(id, @partition))
    shard_index = Router.shard_for(FerricStore.Instance.get(:default), catalog_key)
    :ok = Ferricstore.Raft.Backend.write(shard_index, {:put, catalog_key, "corrupt", 0})
    claimed
  end

  defp complete_many(claimed) do
    FerricStore.flow_complete_many(@partition, [
      {claimed.id, claimed.lease_token, fencing_token: claimed.fencing_token, now_ms: 1_002}
    ])
  end
end
