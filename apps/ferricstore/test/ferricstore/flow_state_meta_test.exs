defmodule Ferricstore.FlowStateMetaTest do
  use Ferricstore.Test.FlowCase

  alias Ferricstore.Flow

  @partition "tenant-state-meta"

  test "state_meta stores durable per-state metadata and survives later states" do
    type = unique_flow_id("state-meta-type")
    id = unique_flow_id("state-meta")

    assert :ok =
             FerricStore.flow_create(id,
               type: type,
               state: "accept",
               partition_key: @partition,
               state_meta: %{"version" => 1, "owner" => "risk"},
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert {:ok, record} = FerricStore.flow_get(id, partition_key: @partition)

    assert record.state_meta == %{
             "accept" => %{"version" => 1, "owner" => "risk"}
           }

    assert record
           |> Flow.encode_record()
           |> Flow.decode_record()
           |> Map.fetch!(:state_meta) == record.state_meta

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due(type,
               states: ["accept"],
               partition_key: @partition,
               worker: "worker-a",
               limit: 1,
               now_ms: 1_001
             )

    assert :ok =
             FerricStore.flow_transition(id, "running", "wow",
               lease_token: claimed.lease_token,
               fencing_token: claimed.fencing_token,
               partition_key: @partition,
               state_meta: %{"version" => 2},
               run_at_ms: 1_010,
               now_ms: 1_002
             )

    assert {:ok, record} = FerricStore.flow_get(id, partition_key: @partition)

    assert record.state_meta == %{
             "accept" => %{"version" => 1, "owner" => "risk"},
             "wow" => %{"version" => 2}
           }
  end

  test "state_meta version is queryable only when policy indexes that metadata key" do
    type = unique_flow_id("state-meta-index-type")
    indexed_id = unique_flow_id("state-meta-indexed")
    other_id = unique_flow_id("state-meta-other")

    assert {:ok, policy} = FerricStore.flow_policy_set(type, indexed_state_meta: "version")
    assert policy.indexed_state_meta == "version"

    assert :ok =
             FerricStore.flow_create(indexed_id,
               type: type,
               state: "accept",
               partition_key: @partition,
               state_meta: %{"version" => 1},
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert :ok =
             FerricStore.flow_create(other_id,
               type: type,
               state: "accept",
               partition_key: @partition,
               state_meta: %{"version" => 9},
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due(type,
               states: ["accept"],
               partition_key: @partition,
               worker: "worker-a",
               limit: 1,
               now_ms: 1_001
             )

    assert :ok =
             FerricStore.flow_transition(indexed_id, "running", "wow",
               lease_token: claimed.lease_token,
               fencing_token: claimed.fencing_token,
               partition_key: @partition,
               state_meta: %{"version" => 2},
               run_at_ms: 1_010,
               now_ms: 1_002
             )

    assert_eventually(fn ->
      assert {:ok, records} =
               FerricStore.flow_search(
                 type: type,
                 partition_key: @partition,
                 state_meta: %{"accept" => %{"version" => 1}},
                 consistent_projection: true,
                 count: 10
               )

      assert Enum.map(records, & &1.id) == [indexed_id]
    end)
  end

  test "indexed state_meta policy backfills existing flow records" do
    type = unique_flow_id("state-meta-index-backfill-type")
    id = unique_flow_id("state-meta-index-backfill")

    assert :ok =
             FerricStore.flow_create(id,
               type: type,
               state: "accept",
               partition_key: @partition,
               state_meta: %{"version" => 1},
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due(type,
               states: ["accept"],
               partition_key: @partition,
               worker: "worker-a",
               limit: 1,
               now_ms: 1_001
             )

    assert :ok =
             FerricStore.flow_complete(id, claimed.lease_token,
               fencing_token: claimed.fencing_token,
               partition_key: @partition,
               state_meta: %{"version" => 3},
               now_ms: 1_002
             )

    assert {:ok, policy} = FerricStore.flow_policy_set(type, indexed_state_meta: "version")
    assert policy.indexed_state_meta == "version"

    assert_eventually(fn ->
      assert {:ok, records} =
               FerricStore.flow_search(
                 type: type,
                 partition_key: @partition,
                 state_meta: %{"accept" => %{"version" => 1}},
                 consistent_projection: true,
                 count: 10
               )

      assert Enum.map(records, & &1.id) == [id]
      flush_lmdb!()
      assert {:ok, 1} = lmdb_state_meta_query_count(type, "accept", "version", 1)
      assert {:ok, 1} = lmdb_state_meta_query_count(type, "completed", "version", 3)
    end)
  end

  test "changing or removing indexed state_meta policy deletes stale LMDB query rows" do
    type = unique_flow_id("state-meta-index-change-type")
    id = unique_flow_id("state-meta-index-change")

    assert {:ok, _policy} = FerricStore.flow_policy_set(type, indexed_state_meta: "version")

    assert :ok =
             FerricStore.flow_create(id,
               type: type,
               state: "accept",
               partition_key: @partition,
               state_meta: %{"version" => 1, "owner" => "risk"},
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert_eventually(fn ->
      flush_lmdb!()
      assert {:ok, 1} = lmdb_state_meta_query_count(type, "accept", "version", 1)
    end)

    assert {:ok, policy} = FerricStore.flow_policy_set(type, indexed_state_meta: "owner")
    assert policy.indexed_state_meta == "owner"

    assert_eventually(fn ->
      flush_lmdb!()
      assert {:ok, 0} = lmdb_state_meta_query_count(type, "accept", "version", 1)
      assert {:ok, 1} = lmdb_state_meta_query_count(type, "accept", "owner", "risk")
    end)

    assert {:ok, policy} = FerricStore.flow_policy_set(type, indexed_state_meta: nil)
    assert policy.indexed_state_meta == nil

    assert_eventually(fn ->
      flush_lmdb!()
      assert {:ok, 0} = lmdb_state_meta_query_count(type, "accept", "version", 1)
      assert {:ok, 0} = lmdb_state_meta_query_count(type, "accept", "owner", "risk")
    end)
  end

  test "indexed state_meta can query terminal state metadata" do
    type = unique_flow_id("state-meta-terminal-type")
    id = unique_flow_id("state-meta-terminal")

    assert {:ok, _policy} = FerricStore.flow_policy_set(type, indexed_state_meta: "version")

    assert :ok =
             FerricStore.flow_create(id,
               type: type,
               state: "accept",
               partition_key: @partition,
               state_meta: %{"version" => 1},
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due(type,
               states: ["accept"],
               partition_key: @partition,
               worker: "worker-a",
               limit: 1,
               now_ms: 1_001
             )

    assert :ok =
             FerricStore.flow_complete(id, claimed.lease_token,
               fencing_token: claimed.fencing_token,
               partition_key: @partition,
               state_meta: %{"version" => 3},
               now_ms: 1_002
             )

    assert_eventually(fn ->
      assert {:ok, records} =
               FerricStore.flow_search(
                 type: type,
                 partition_key: @partition,
                 state_meta: %{"completed" => %{"version" => 3}},
                 consistent_projection: true,
                 count: 10
               )

      assert Enum.map(records, & &1.id) == [id]
    end)
  end

  test "retention cleanup deletes indexed state_meta query rows from LMDB" do
    type = unique_flow_id("state-meta-retention-type")
    id = unique_flow_id("state-meta-retention")
    now = System.system_time(:millisecond)

    assert {:ok, _policy} = FerricStore.flow_policy_set(type, indexed_state_meta: "version")

    assert :ok =
             FerricStore.flow_create(id,
               type: type,
               state: "accept",
               partition_key: @partition,
               state_meta: %{"version" => 1},
               retention_ttl_ms: 1_000,
               run_at_ms: now,
               now_ms: now
             )

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due(type,
               states: ["accept"],
               partition_key: @partition,
               worker: "worker-a",
               limit: 1,
               now_ms: now + 1
             )

    assert :ok =
             FerricStore.flow_complete(id, claimed.lease_token,
               fencing_token: claimed.fencing_token,
               partition_key: @partition,
               state_meta: %{"version" => 3},
               now_ms: now + 2
             )

    assert_eventually(fn ->
      flush_lmdb!()
      assert {:ok, 1} = lmdb_state_meta_query_count(type, "completed", "version", 3)
    end)

    assert {:ok, cleaned} = FerricStore.flow_retention_cleanup(limit: 10, now_ms: now + 2_000)
    assert cleaned.flows >= 1

    assert_eventually(fn ->
      flush_lmdb!()
      assert {:ok, 0} = lmdb_state_meta_query_count(type, "completed", "version", 3)
    end)
  end

  test "state_meta and indexed_state_meta stay bounded" do
    type = unique_flow_id("state-meta-limits")

    assert {:error, "ERR too many flow state_meta entries"} =
             FerricStore.flow_create(unique_flow_id("state-meta-too-many"),
               type: type,
               state: "accept",
               partition_key: @partition,
               state_meta: Map.new(1..17, &{"k#{&1}", &1}),
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert {:error, "ERR flow indexed_state_meta supports at most 1 key"} =
             FerricStore.flow_policy_set(type, indexed_state_meta: ["version", "owner"])
  end

  test "state_meta update cannot grow a state beyond the entry cap" do
    type = unique_flow_id("state-meta-merge-limits")
    id = unique_flow_id("state-meta-merge-limit")
    initial_meta = Map.new(1..16, &{"k#{&1}", &1})

    assert :ok =
             FerricStore.flow_create(id,
               type: type,
               state: "accept",
               partition_key: @partition,
               state_meta: initial_meta,
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due(type,
               states: ["accept"],
               partition_key: @partition,
               worker: "worker-a",
               limit: 1,
               now_ms: 1_001
             )

    assert :ok =
             FerricStore.flow_transition(id, "running", "accept",
               lease_token: claimed.lease_token,
               fencing_token: claimed.fencing_token,
               partition_key: @partition,
               state_meta: %{"k17" => 17},
               run_at_ms: 1_010,
               now_ms: 1_002
             )

    assert {:ok, record} = FerricStore.flow_get(id, partition_key: @partition)
    assert record.state_meta["accept"] == initial_meta
  end

  defp flush_lmdb! do
    ctx = FerricStore.Instance.get(:default)
    assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, ctx.shard_count, 30_000)
  end

  defp lmdb_state_meta_query_count(type, state, name, value) do
    ctx = FerricStore.Instance.get(:default)
    value = Ferricstore.Flow.StateMeta.index_value(value)
    index_key = Ferricstore.Flow.Keys.state_meta_index_key(type, state, name, value, @partition)
    shard_index = Ferricstore.Store.Router.shard_for(ctx, index_key)

    path =
      ctx.data_dir
      |> Ferricstore.DataDir.shard_data_path(shard_index)
      |> Ferricstore.Flow.LMDB.path()

    Ferricstore.Flow.LMDB.prefix_count(path, Ferricstore.Flow.LMDB.query_index_prefix(index_key))
  end
end
