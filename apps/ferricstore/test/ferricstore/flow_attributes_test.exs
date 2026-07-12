defmodule Ferricstore.FlowAttributesTest do
  use Ferricstore.Test.FlowCase

  alias Ferricstore.Flow

  @partition "tenant-attrs"

  test "create stores attributes in durable Flow state and codec round trips them" do
    id = unique_flow_id("attrs-create")

    assert :ok =
             FerricStore.flow_create(id,
               type: "attrs",
               state: "queued",
               partition_key: @partition,
               attributes: %{
                 "tenant" => "acme",
                 "priority_bucket" => 7,
                 "sandbox" => true,
                 "tags" => ["iot", "fanout"]
               },
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert {:ok, record} = FerricStore.flow_get(id, partition_key: @partition)

    assert record.attributes == %{
             "tenant" => "acme",
             "priority_bucket" => 7,
             "sandbox" => true,
             "tags" => ["iot", "fanout"]
           }

    assert record
           |> Flow.encode_record()
           |> Flow.decode_record()
           |> Map.fetch!(:attributes) == record.attributes
  end

  test "transition merges and deletes attributes without touching payload refs" do
    id = unique_flow_id("attrs-transition")

    assert :ok =
             FerricStore.flow_create(id,
               type: "attrs",
               state: "queued",
               partition_key: @partition,
               attributes: %{"tenant" => "acme", "phase" => "created", "drop_me" => "yes"},
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due("attrs",
               states: ["queued"],
               partition_key: @partition,
               worker: "w1",
               limit: 1,
               now_ms: 1_001
             )

    assert :ok =
             FerricStore.flow_transition(id, "running", "processing",
               lease_token: claimed.lease_token,
               fencing_token: claimed.fencing_token,
               partition_key: @partition,
               attributes_merge: %{"phase" => "processing", "region" => "us"},
               attributes_delete: ["drop_me"],
               run_at_ms: 1_010,
               now_ms: 1_002
             )

    assert {:ok, record} = FerricStore.flow_get(id, partition_key: @partition)

    assert record.attributes == %{
             "tenant" => "acme",
             "phase" => "processing",
             "region" => "us"
           }
  end

  test "list and stats filter by projected attributes" do
    id = unique_flow_id("attrs-list")

    assert :ok =
             FerricStore.flow_create(id,
               type: "attrs-query",
               state: "queued",
               partition_key: @partition,
               attributes: %{"tenant" => "acme", "campaign" => "spring"},
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert :ok =
             FerricStore.flow_create(unique_flow_id("attrs-list-other"),
               type: "attrs-query",
               state: "queued",
               partition_key: @partition,
               attributes: %{"tenant" => "other", "campaign" => "spring"},
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert_eventually(fn ->
      assert {:ok, records} =
               FerricStore.flow_list("attrs-query",
                 state: "queued",
                 partition_key: @partition,
                 attributes: %{"tenant" => "acme"},
                 include_cold: true,
                 consistent_projection: true,
                 count: 10
               )

      assert Enum.map(records, & &1.id) == [id]
    end)

    assert {:ok, stats} =
             FerricStore.flow_stats("attrs-query",
               state: "queued",
               partition_key: @partition,
               attributes: %{"tenant" => "acme"},
               consistent_projection: true
             )

    assert stats.count == 1
    assert stats.attributes == %{"tenant" => "acme"}
  end

  test "claim_due compact job return can include attributes" do
    id = unique_flow_id("attrs-claim")

    assert :ok =
             FerricStore.flow_create(id,
               type: "attrs-claim",
               state: "queued",
               partition_key: @partition,
               attributes: %{"tenant" => "acme", "phase" => "queued"},
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert {:ok, [[^id, @partition, lease_token, 1, attrs]]} =
             FerricStore.flow_claim_due("attrs-claim",
               states: ["queued"],
               partition_key: @partition,
               worker: "w1",
               limit: 1,
               now_ms: 1_001,
               return: :jobs_compact_attrs
             )

    assert is_binary(lease_token)
    assert attrs == %{"tenant" => "acme", "phase" => "queued"}
  end

  test "claim_due records include attributes by default and compact mode omits them" do
    type = unique_flow_id("attrs-claim-default-type")
    record_id = unique_flow_id("attrs-claim-default-record")
    compact_id = unique_flow_id("attrs-claim-default-compact")

    assert :ok =
             FerricStore.flow_create(record_id,
               type: type,
               state: "queued",
               partition_key: @partition,
               attributes: %{"tenant" => "acme", "phase" => "queued"},
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert {:ok, [record]} =
             FerricStore.flow_claim_due(type,
               states: ["queued"],
               partition_key: @partition,
               worker: "w1",
               limit: 1,
               now_ms: 1_001
             )

    assert record.id == record_id
    assert record.attributes == %{"tenant" => "acme", "phase" => "queued"}

    assert :ok =
             FerricStore.flow_create(compact_id,
               type: type,
               state: "queued",
               partition_key: @partition,
               attributes: %{"tenant" => "acme", "phase" => "compact"},
               run_at_ms: 1_010,
               now_ms: 1_002
             )

    assert {:ok, [[^compact_id, @partition, lease_token, 1]]} =
             FerricStore.flow_claim_due(type,
               states: ["queued"],
               partition_key: @partition,
               worker: "w1",
               limit: 1,
               now_ms: 1_011,
               return: :jobs_compact
             )

    assert is_binary(lease_token)
  end

  test "attributes persist through transition complete fail retry and rewind" do
    type = unique_flow_id("attrs-lifecycle-type")
    retry_id = unique_flow_id("attrs-lifecycle-retry")
    fail_id = unique_flow_id("attrs-lifecycle-fail")
    rewind_id = unique_flow_id("attrs-lifecycle-rewind")

    assert :ok =
             FerricStore.flow_create(retry_id,
               type: type,
               state: "queued",
               partition_key: @partition,
               attributes: %{"tenant" => "acme", "phase" => "created"},
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    claimed = claim_one!(type, "queued", 1_001)

    assert :ok =
             FerricStore.flow_retry(retry_id, claimed.lease_token,
               partition_key: @partition,
               fencing_token: claimed.fencing_token,
               error: "temporary",
               run_at_ms: 1_010,
               now_ms: 1_002
             )

    assert_flow_attrs(retry_id, %{"tenant" => "acme", "phase" => "created"})

    claimed = claim_one!(type, "queued", 1_011)

    assert :ok =
             FerricStore.flow_transition(retry_id, "running", "processing",
               lease_token: claimed.lease_token,
               fencing_token: claimed.fencing_token,
               partition_key: @partition,
               attributes_merge: %{"phase" => "processing", "attempt" => 2},
               run_at_ms: 1_020,
               now_ms: 1_012
             )

    assert_flow_attrs(retry_id, %{"tenant" => "acme", "phase" => "processing", "attempt" => 2})

    claimed = claim_one!(type, "processing", 1_021)

    assert :ok =
             FerricStore.flow_complete(retry_id, claimed.lease_token,
               fencing_token: claimed.fencing_token,
               partition_key: @partition,
               now_ms: 1_022
             )

    assert_flow_attrs(retry_id, %{"tenant" => "acme", "phase" => "processing", "attempt" => 2})

    assert :ok =
             FerricStore.flow_create(fail_id,
               type: type,
               state: "queued",
               partition_key: @partition,
               attributes: %{"tenant" => "acme", "phase" => "fail-path"},
               run_at_ms: 2_000,
               now_ms: 2_000
             )

    claimed = claim_one!(type, "queued", 2_001)

    assert :ok =
             FerricStore.flow_fail(fail_id, claimed.lease_token,
               fencing_token: claimed.fencing_token,
               partition_key: @partition,
               error: "fatal",
               now_ms: 2_002
             )

    assert_flow_attrs(fail_id, %{"tenant" => "acme", "phase" => "fail-path"})

    assert :ok =
             FerricStore.flow_create(rewind_id,
               type: type,
               state: "queued",
               partition_key: @partition,
               attributes: %{"tenant" => "acme", "phase" => "created"},
               run_at_ms: 3_000,
               now_ms: 3_000
             )

    assert {:ok, [{created_event_id, _created_event} | _]} =
             FerricStore.flow_history(rewind_id, partition_key: @partition, count: 10)

    claimed = claim_one!(type, "queued", 3_001)

    assert :ok =
             FerricStore.flow_transition(rewind_id, "running", "processing",
               lease_token: claimed.lease_token,
               fencing_token: claimed.fencing_token,
               partition_key: @partition,
               attributes_merge: %{"phase" => "processing"},
               run_at_ms: 3_010,
               now_ms: 3_002
             )

    assert_flow_attrs(rewind_id, %{"tenant" => "acme", "phase" => "processing"})

    assert :ok =
             FerricStore.flow_rewind(rewind_id,
               partition_key: @partition,
               to_event: created_event_id,
               now_ms: 3_003
             )

    assert_flow_attrs(rewind_id, %{"tenant" => "acme", "phase" => "created"})
  end

  test "terminal records keep attributes queryable through async projection" do
    id = unique_flow_id("attrs-terminal")

    assert {:ok, _policy} =
             FerricStore.flow_policy_set("attrs-terminal", indexed_attributes: ["tenant"])

    assert :ok =
             FerricStore.flow_create(id,
               type: "attrs-terminal",
               state: "queued",
               partition_key: @partition,
               attributes: %{"tenant" => "acme", "phase" => "queued"},
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due("attrs-terminal",
               states: ["queued"],
               partition_key: @partition,
               worker: "w1",
               limit: 1,
               now_ms: 1_001
             )

    assert :ok =
             FerricStore.flow_complete(id, claimed.lease_token,
               fencing_token: claimed.fencing_token,
               partition_key: @partition,
               attributes: %{"phase" => "done"},
               now_ms: 1_002
             )

    assert_eventually(fn ->
      assert {:ok, records} =
               FerricStore.flow_list("attrs-terminal",
                 state: "completed",
                 partition_key: @partition,
                 attributes: %{"phase" => "done"},
                 include_cold: true,
                 consistent_projection: true,
                 count: 10
               )

      assert Enum.map(records, & &1.id) == [id]
    end)

    assert {:ok, records} =
             FerricStore.flow_search(
               type: "attrs-terminal",
               partition_key: @partition,
               attributes: %{"tenant" => "acme"},
               include_cold: true,
               consistent_projection: true,
               count: 10
             )

    assert Enum.map(records, & &1.id) == [id]
  end

  test "restart rebuild preserves attribute search correctness" do
    isolated = ShardHelpers.setup_isolated_data_dir()

    on_exit(fn ->
      ShardHelpers.teardown_isolated_data_dir(isolated)
    end)

    type = unique_flow_id("attrs-restart-type")
    id = unique_flow_id("attrs-restart")
    partition = unique_flow_id("tenant-restart")

    assert {:ok, _} = FerricStore.flow_policy_set(type, indexed_attributes: ["tenant", "region"])

    assert :ok =
             FerricStore.flow_create(id,
               type: type,
               state: "queued",
               partition_key: partition,
               attributes: %{"tenant" => "acme", "region" => "us"},
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert_eventually(fn ->
      assert {:ok, records} =
               FerricStore.flow_list(type,
                 state: "queued",
                 partition_key: partition,
                 attributes: %{"tenant" => "acme"},
                 include_cold: true,
                 consistent_projection: true,
                 count: 10
               )

      assert Enum.map(records, & &1.id) == [id]
    end)

    :ok = ShardHelpers.restart_current_data_dir(isolated)

    assert_eventually(fn ->
      assert {:ok, records} =
               FerricStore.flow_list(type,
                 state: "queued",
                 partition_key: partition,
                 attributes: %{"region" => "us"},
                 include_cold: true,
                 consistent_projection: true,
                 count: 10
               )

      assert Enum.map(records, & &1.id) == [id]

      assert {:ok, broad_records} =
               FerricStore.flow_search(
                 partition_key: partition,
                 attributes: %{"tenant" => "acme"},
                 consistent_projection: true,
                 count: 10
               )

      assert Enum.map(broad_records, & &1.id) == [id]

      assert {:ok, attrs} =
               FerricStore.flow_attributes(type,
                 state: "queued",
                 partition_key: partition,
                 consistent_projection: true
               )

      assert %{name: "tenant", count: 1} in attrs
      assert %{name: "region", count: 1} in attrs

      assert {:ok, values} =
               FerricStore.flow_attribute_values(type, "tenant",
                 state: "queued",
                 partition_key: partition,
                 consistent_projection: true
               )

      assert values == [%{value: "acme", count: 1}]
    end)
  end

  test "retention cleanup removes terminal attribute indexes" do
    type = unique_flow_id("attrs-retention-type")
    id = unique_flow_id("attrs-retention")
    now = System.system_time(:millisecond)

    assert {:ok, _} = FerricStore.flow_policy_set(type, indexed_attributes: ["tenant"])

    assert :ok =
             FerricStore.flow_create(id,
               type: type,
               state: "queued",
               partition_key: @partition,
               attributes: %{"tenant" => "acme", "phase" => "terminal"},
               retention_ttl_ms: 60_000,
               run_at_ms: now,
               now_ms: now
             )

    claimed = claim_one!(type, "queued", now + 1)

    assert :ok =
             FerricStore.flow_complete(id, claimed.lease_token,
               fencing_token: claimed.fencing_token,
               partition_key: @partition,
               now_ms: now + 2
             )

    assert {:ok, completed} = FerricStore.flow_get(id, partition_key: @partition)
    cleanup_now_ms = completed.terminal_retention_until_ms + 1

    assert_eventually(fn ->
      assert {:ok, records} =
               FerricStore.flow_list(type,
                 state: "completed",
                 partition_key: @partition,
                 attributes: %{"tenant" => "acme"},
                 include_cold: true,
                 consistent_projection: true,
                 count: 10
               )

      assert Enum.map(records, & &1.id) == [id]

      assert {:ok, type_records} =
               FerricStore.flow_search(
                 type: type,
                 partition_key: @partition,
                 attributes: %{"tenant" => "acme"},
                 consistent_projection: true,
                 count: 10
               )

      assert Enum.map(type_records, & &1.id) == [id]

      assert {:ok, state_records} =
               FerricStore.flow_search(
                 state: "completed",
                 partition_key: @partition,
                 attributes: %{"tenant" => "acme"},
                 consistent_projection: true,
                 count: 10
               )

      assert Enum.map(state_records, & &1.id) == [id]

      assert {:ok, broad_records} =
               FerricStore.flow_search(
                 partition_key: @partition,
                 attributes: %{"tenant" => "acme"},
                 consistent_projection: true,
                 count: 10
               )

      assert Enum.map(broad_records, & &1.id) == [id]
    end)

    assert {:ok, cleaned} =
             FerricStore.flow_retention_cleanup(limit: 10, now_ms: cleanup_now_ms)

    assert cleaned.flows >= 1

    assert_eventually(fn ->
      assert {:ok, nil} = FerricStore.flow_get(id, partition_key: @partition)

      assert {:ok, []} =
               FerricStore.flow_list(type,
                 state: "completed",
                 partition_key: @partition,
                 attributes: %{"tenant" => "acme"},
                 include_cold: true,
                 consistent_projection: true,
                 count: 10
               )
    end)

    assert {:ok, []} =
             FerricStore.flow_search(
               type: type,
               partition_key: @partition,
               attributes: %{"tenant" => "acme"},
               consistent_projection: true,
               count: 10
             )

    assert {:ok, []} =
             FerricStore.flow_search(
               state: "completed",
               partition_key: @partition,
               attributes: %{"tenant" => "acme"},
               consistent_projection: true,
               count: 10
             )

    assert {:ok, []} =
             FerricStore.flow_search(
               partition_key: @partition,
               attributes: %{"tenant" => "acme"},
               consistent_projection: true,
               count: 10
             )
  end

  test "rejects oversized attributes at command boundary" do
    id = unique_flow_id("attrs-invalid")

    assert {:error, "ERR too many flow attributes"} =
             FerricStore.flow_create(id,
               type: "attrs",
               partition_key: @partition,
               attributes: Map.new(1..17, &{"k#{&1}", "v"}),
               run_at_ms: 1_000,
               now_ms: 1_000
             )
  end

  test "rejects oversized attribute keys values and invalid types" do
    assert {:error, "ERR flow attribute key too large"} =
             FerricStore.flow_create(unique_flow_id("attrs-key-large"),
               type: "attrs",
               partition_key: @partition,
               attributes: %{String.duplicate("k", 65) => "v"},
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert {:error, "ERR flow attribute value too large"} =
             FerricStore.flow_create(unique_flow_id("attrs-value-large"),
               type: "attrs",
               partition_key: @partition,
               attributes: %{"k" => String.duplicate("v", 257)},
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert {:error, "ERR flow attribute value must be scalar or string list"} =
             FerricStore.flow_create(unique_flow_id("attrs-invalid-type"),
               type: "attrs",
               partition_key: @partition,
               attributes: %{"k" => %{"nested" => "no"}},
               run_at_ms: 1_000,
               now_ms: 1_000
             )
  end

  test "flow policy limits indexed attributes to three names" do
    type = unique_flow_id("attrs-index-policy")

    assert {:ok, policy} =
             FerricStore.flow_policy_set(type,
               indexed_attributes: ["tenant", :region, "campaign", "tenant"]
             )

    assert policy.indexed_attributes == ["tenant", "region", "campaign"]

    assert {:ok, state_policy} = FerricStore.flow_policy_get(type, state: "queued")
    assert state_policy.indexed_attributes == ["tenant", "region", "campaign"]

    assert {:error, "ERR flow indexed_attributes supports at most 3 keys"} =
             FerricStore.flow_policy_set(unique_flow_id("attrs-index-policy-too-many"),
               indexed_attributes: ["a", "b", "c", "d"]
             )
  end

  test "indexed attributes support broad cross-state and cross-type search" do
    type_a = unique_flow_id("attrs-index-type-a")
    type_b = unique_flow_id("attrs-index-type-b")
    a1 = unique_flow_id("attrs-index-a1")
    a2 = unique_flow_id("attrs-index-a2")
    b1 = unique_flow_id("attrs-index-b1")
    other = unique_flow_id("attrs-index-other")

    assert {:ok, _} = FerricStore.flow_policy_set(type_a, indexed_attributes: ["tenant"])
    assert {:ok, _} = FerricStore.flow_policy_set(type_b, indexed_attributes: ["tenant"])

    assert :ok =
             FerricStore.flow_create(a1,
               type: type_a,
               state: "queued",
               partition_key: @partition,
               attributes: %{"tenant" => "acme"},
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert :ok =
             FerricStore.flow_create(a2,
               type: type_a,
               state: "review",
               partition_key: @partition,
               attributes: %{"tenant" => "acme"},
               run_at_ms: 1_100,
               now_ms: 1_100
             )

    assert :ok =
             FerricStore.flow_create(b1,
               type: type_b,
               state: "queued",
               partition_key: @partition,
               attributes: %{"tenant" => "acme"},
               run_at_ms: 1_200,
               now_ms: 1_200
             )

    assert :ok =
             FerricStore.flow_create(other,
               type: type_b,
               state: "queued",
               partition_key: @partition,
               attributes: %{"tenant" => "other"},
               run_at_ms: 1_300,
               now_ms: 1_300
             )

    assert_eventually(fn ->
      assert {:ok, records} =
               FerricStore.flow_search(
                 partition_key: @partition,
                 attributes: %{"tenant" => "acme"},
                 consistent_projection: true,
                 count: 10
               )

      assert MapSet.new(Enum.map(records, & &1.id)) == MapSet.new([a1, a2, b1])
    end)

    assert {:ok, records} =
             FerricStore.flow_search(
               type: type_a,
               partition_key: @partition,
               attributes: %{"tenant" => "acme"},
               consistent_projection: true,
               count: 10
             )

    assert MapSet.new(Enum.map(records, & &1.id)) == MapSet.new([a1, a2])

    assert {:ok, records} =
             FerricStore.flow_list(type_a,
               state: :any,
               partition_key: @partition,
               attributes: %{"tenant" => "acme"},
               consistent_projection: true,
               count: 10
             )

    assert MapSet.new(Enum.map(records, & &1.id)) == MapSet.new([a1, a2])
  end

  test "non-indexed attributes remain exact-queryable but broad search fails clearly" do
    type = unique_flow_id("attrs-non-indexed-type")
    id = unique_flow_id("attrs-non-indexed")

    assert {:ok, _} = FerricStore.flow_policy_set(type, indexed_attributes: ["tenant"])

    assert :ok =
             FerricStore.flow_create(id,
               type: type,
               state: "queued",
               partition_key: @partition,
               attributes: %{"customer" => "c1"},
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert_eventually(fn ->
      assert {:ok, records} =
               FerricStore.flow_list(type,
                 state: "queued",
                 partition_key: @partition,
                 attributes: %{"customer" => "c1"},
                 consistent_projection: true,
                 count: 10
               )

      assert Enum.map(records, & &1.id) == [id]
    end)

    assert {:error, "ERR flow attribute customer is not indexed for broad search"} =
             FerricStore.flow_search(
               type: type,
               partition_key: @partition,
               attributes: %{"customer" => "c1"},
               consistent_projection: true,
               count: 10
             )

    assert {:error, "ERR flow attribute customer is not indexed for broad search"} =
             FerricStore.flow_search(
               partition_key: @partition,
               attributes: %{"customer" => "c1"},
               consistent_projection: true,
               count: 10
             )
  end

  test "broad search retains indexed policy knowledge when no rows exist" do
    type_a = unique_flow_id("attrs-empty-policy-a")
    type_b = unique_flow_id("attrs-empty-policy-b")

    assert {:ok, _} = FerricStore.flow_policy_set(type_a, indexed_attributes: ["tenant"])
    assert {:ok, _} = FerricStore.flow_policy_set(type_b, indexed_attributes: ["tenant"])

    assert {:ok, []} =
             FerricStore.flow_search(
               partition_key: @partition,
               attributes: %{"tenant" => "missing"},
               consistent_projection: true,
               count: 10
             )

    assert {:ok, _} = FerricStore.flow_policy_set(type_a, indexed_attributes: [])

    assert {:ok, []} =
             FerricStore.flow_search(
               partition_key: @partition,
               attributes: %{"tenant" => "missing"},
               consistent_projection: true,
               count: 10
             )

    assert {:ok, _} = FerricStore.flow_policy_set(type_b, indexed_attributes: [])

    assert {:error, "ERR flow attribute tenant is not indexed for broad search"} =
             FerricStore.flow_search(
               partition_key: @partition,
               attributes: %{"tenant" => "missing"},
               consistent_projection: true,
               count: 10
             )
  end

  test "broad search repairs missing and corrupt indexed-attribute refcounts" do
    type = unique_flow_id("attrs-catalog-repair")
    ctx = FerricStore.Instance.get(:default)

    {:ok, worker} =
      Ferricstore.Flow.PolicyMigrationWorker.start_link(
        instance_ctx: ctx,
        enabled: true,
        initial_delay_ms: 60_000,
        interval_ms: 60_000,
        catchup_delay_ms: 1
      )

    on_exit(fn ->
      if Process.alive?(worker) do
        try do
          GenServer.stop(worker)
        catch
          :exit, _reason -> :ok
        end
      end
    end)

    count_key = Ferricstore.Flow.Keys.policy_indexed_attribute_count_key("tenant")
    member_key = Ferricstore.Flow.Keys.policy_indexed_attribute_member_key("tenant", type)

    assert {:ok, _} = FerricStore.flow_policy_set(type, indexed_attributes: ["tenant"])
    assert <<1::unsigned-big-64>> = Ferricstore.Store.Router.get(ctx, count_key)
    assert <<1>> = Ferricstore.Store.Router.get(ctx, member_key)

    shard_index = Ferricstore.Store.Router.shard_for(ctx, member_key)
    assert :ok = Ferricstore.Flow.LMDBWriter.flush(ctx.name, shard_index)

    lmdb_path =
      ctx.data_dir
      |> Ferricstore.DataDir.shard_data_path(shard_index)
      |> Ferricstore.Flow.LMDB.path()

    assert {:ok, [_member]} =
             Ferricstore.Flow.LMDB.prefix_entries(
               lmdb_path,
               Ferricstore.Flow.Keys.policy_indexed_attribute_member_prefix("tenant"),
               1
             )

    assert :ok = Ferricstore.Store.Router.delete(ctx, count_key)

    assert {:ok, []} =
             FerricStore.flow_search(
               partition_key: @partition,
               attributes: %{"tenant" => "missing"},
               consistent_projection: true,
               count: 10
             )

    assert_eventually(fn ->
      assert <<1::unsigned-big-64>> = Ferricstore.Store.Router.get(ctx, count_key)
    end)

    assert :ok = Ferricstore.Store.Router.put(ctx, count_key, "corrupt", 0)

    assert {:ok, []} =
             FerricStore.flow_search(
               partition_key: @partition,
               attributes: %{"tenant" => "missing"},
               consistent_projection: true,
               count: 10
             )

    assert_eventually(fn ->
      assert <<1::unsigned-big-64>> = Ferricstore.Store.Router.get(ctx, count_key)
    end)

    assert {:ok, _} = FerricStore.flow_policy_set(type, indexed_attributes: [])
    assert <<0::unsigned-big-64>> = Ferricstore.Store.Router.get(ctx, count_key)

    assert {:error, "ERR flow attribute tenant is not indexed for broad search"} =
             FerricStore.flow_search(
               partition_key: @partition,
               attributes: %{"tenant" => "missing"},
               consistent_projection: true,
               count: 10
             )
  end

  test "indexed-attribute repair is fenced by the membership revision" do
    type = unique_flow_id("attrs-catalog-fence")
    name = "region"
    ctx = FerricStore.Instance.get(:default)
    revision_key = Ferricstore.Flow.Keys.policy_indexed_attribute_revision_key(name)
    count_key = Ferricstore.Flow.Keys.policy_indexed_attribute_count_key(name)
    repair_key = Ferricstore.Flow.Keys.policy_indexed_attribute_repair_key(name)
    shard_index = Ferricstore.Store.Router.shard_for(ctx, repair_key)

    assert {:ok, _} = FerricStore.flow_policy_set(type, indexed_attributes: [name])
    assert <<revision::unsigned-big-64>> = Ferricstore.Store.Router.get(ctx, revision_key)
    assert <<1::unsigned-big-64>> = Ferricstore.Store.Router.get(ctx, count_key)

    assert :ok = Ferricstore.Store.Router.flow_policy_attribute_catalog_repair_request(ctx, name)
    assert {:ok, _} = FerricStore.flow_policy_set(type, indexed_attributes: [])

    assert {:ok, %{processed: 0, repaired?: false, stale?: true}} =
             Ferricstore.Store.Router.flow_policy_attribute_catalog_repair(
               ctx,
               shard_index,
               %{name: name, expected_revision: revision, count: 1}
             )

    assert <<0::unsigned-big-64>> = Ferricstore.Store.Router.get(ctx, count_key)
    assert is_binary(Ferricstore.Store.Router.get(ctx, repair_key))

    assert <<next_revision::unsigned-big-64>> =
             Ferricstore.Store.Router.get(ctx, revision_key)

    assert next_revision > revision

    assert {:ok, %{processed: 1, repaired?: true, stale?: false}} =
             Ferricstore.Store.Router.flow_policy_attribute_catalog_repair(
               ctx,
               shard_index,
               %{name: name, expected_revision: next_revision, count: 0}
             )

    assert <<0::unsigned-big-64>> = Ferricstore.Store.Router.get(ctx, count_key)
    assert nil == Ferricstore.Store.Router.get(ctx, repair_key)
  end

  test "indexed attribute policy changes are future-only for broad indexes" do
    type = unique_flow_id("attrs-policy-future-type")
    before_id = unique_flow_id("attrs-policy-future-before")
    after_id = unique_flow_id("attrs-policy-future-after")

    assert :ok =
             FerricStore.flow_create(before_id,
               type: type,
               state: "queued",
               partition_key: @partition,
               attributes: %{"tenant" => "acme"},
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert {:ok, _} = FerricStore.flow_policy_set(type, indexed_attributes: ["tenant"])

    assert :ok =
             FerricStore.flow_create(after_id,
               type: type,
               state: "queued",
               partition_key: @partition,
               attributes: %{"tenant" => "acme"},
               run_at_ms: 2_000,
               now_ms: 2_000
             )

    assert_eventually(fn ->
      assert {:ok, records} =
               FerricStore.flow_list(type,
                 state: "queued",
                 partition_key: @partition,
                 attributes: %{"tenant" => "acme"},
                 consistent_projection: true,
                 count: 10
               )

      assert MapSet.new(Enum.map(records, & &1.id)) == MapSet.new([before_id, after_id])
    end)

    assert {:ok, records} =
             FerricStore.flow_search(
               type: type,
               partition_key: @partition,
               attributes: %{"tenant" => "acme"},
               consistent_projection: true,
               count: 10
             )

    assert Enum.map(records, & &1.id) == [after_id]
  end

  test "indexed attributes added during transition become broad-searchable" do
    type = unique_flow_id("attrs-transition-index-type")
    id = unique_flow_id("attrs-transition-index")

    assert {:ok, _} = FerricStore.flow_policy_set(type, indexed_attributes: ["tenant"])

    assert :ok =
             FerricStore.flow_create(id,
               type: type,
               state: "queued",
               partition_key: @partition,
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    claimed = claim_one!(type, "queued", 1_001)

    assert :ok =
             FerricStore.flow_transition(id, "running", "processing",
               lease_token: claimed.lease_token,
               fencing_token: claimed.fencing_token,
               partition_key: @partition,
               attributes_merge: %{"tenant" => "acme"},
               run_at_ms: 1_010,
               now_ms: 1_002
             )

    assert_eventually(fn ->
      assert {:ok, records} =
               FerricStore.flow_search(
                 type: type,
                 partition_key: @partition,
                 attributes: %{"tenant" => "acme"},
                 consistent_projection: true,
                 count: 10
               )

      assert Enum.map(records, & &1.id) == [id]
    end)
  end

  test "indexed attributes support cross-type state search without partition-scope under-return" do
    type_a = unique_flow_id("attrs-state-index-type-a")
    type_b = unique_flow_id("attrs-state-index-type-b")
    old_review = unique_flow_id("attrs-state-index-review")
    queued = unique_flow_id("attrs-state-index-queued")

    assert {:ok, _} = FerricStore.flow_policy_set(type_a, indexed_attributes: ["tenant"])
    assert {:ok, _} = FerricStore.flow_policy_set(type_b, indexed_attributes: ["tenant"])

    assert :ok =
             FerricStore.flow_create(old_review,
               type: type_a,
               state: "review",
               partition_key: @partition,
               attributes: %{"tenant" => "acme"},
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert :ok =
             FerricStore.flow_create(queued,
               type: type_b,
               state: "queued",
               partition_key: @partition,
               attributes: %{"tenant" => "acme"},
               run_at_ms: 2_000,
               now_ms: 2_000
             )

    assert_eventually(fn ->
      assert {:ok, [record]} =
               FerricStore.flow_search(
                 state: "queued",
                 partition_key: @partition,
                 attributes: %{"tenant" => "acme"},
                 consistent_projection: true,
                 count: 1
               )

      assert record.id == queued
    end)
  end

  test "indexed attribute search honors time reverse and count windows" do
    type = unique_flow_id("attrs-index-window-type")
    old_id = unique_flow_id("attrs-index-window-old")
    new_id = unique_flow_id("attrs-index-window-new")

    assert {:ok, _} = FerricStore.flow_policy_set(type, indexed_attributes: ["tenant"])

    assert :ok =
             FerricStore.flow_create(old_id,
               type: type,
               state: "queued",
               partition_key: @partition,
               attributes: %{"tenant" => "acme"},
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert :ok =
             FerricStore.flow_create(new_id,
               type: type,
               state: "queued",
               partition_key: @partition,
               attributes: %{"tenant" => "acme"},
               run_at_ms: 2_000,
               now_ms: 2_000
             )

    assert_eventually(fn ->
      assert {:ok, [record]} =
               FerricStore.flow_search(
                 type: type,
                 partition_key: @partition,
                 attributes: %{"tenant" => "acme"},
                 from_ms: 1_500,
                 consistent_projection: true,
                 count: 10
               )

      assert record.id == new_id
    end)

    assert {:ok, [record]} =
             FerricStore.flow_search(
               type: type,
               partition_key: @partition,
               attributes: %{"tenant" => "acme"},
               rev: true,
               consistent_projection: true,
               count: 1
             )

    assert record.id == new_id

    assert {:ok, [record]} =
             FerricStore.flow_search(
               type: type,
               partition_key: @partition,
               attributes: %{"tenant" => "acme"},
               to_ms: 1_500,
               consistent_projection: true,
               count: 10
             )

    assert record.id == old_id
  end

  test "attribute discovery lists policy-indexed keys and top values" do
    type = unique_flow_id("attrs-discovery-type")
    ids = Enum.map(1..3, &unique_flow_id("attrs-discovery-#{&1}"))

    assert {:ok, _} = FerricStore.flow_policy_set(type, indexed_attributes: ["tenant", "region"])

    for {id, attrs, now} <- [
          {Enum.at(ids, 0), %{"tenant" => "acme", "region" => "us"}, 1_000},
          {Enum.at(ids, 1), %{"tenant" => "acme"}, 1_100},
          {Enum.at(ids, 2), %{"tenant" => "beta"}, 1_200}
        ] do
      assert :ok =
               FerricStore.flow_create(id,
                 type: type,
                 state: "queued",
                 partition_key: @partition,
                 attributes: attrs,
                 run_at_ms: now,
                 now_ms: now
               )
    end

    assert_eventually(fn ->
      assert {:ok, keys} =
               FerricStore.flow_attributes(type,
                 state: "queued",
                 partition_key: @partition,
                 consistent_projection: true
               )

      assert %{name: "tenant", count: 3} in keys
      assert %{name: "region", count: 1} in keys
    end)

    assert {:ok, values} =
             FerricStore.flow_attribute_values(type, "tenant",
               state: "queued",
               partition_key: @partition,
               consistent_projection: true,
               count: 2
             )

    assert values == [%{value: "acme", count: 2}, %{value: "beta", count: 1}]
  end

  test "attribute discovery preserves string values containing NUL bytes" do
    type = unique_flow_id("attrs-discovery-nul-type")
    value = "acme\0north"

    assert {:ok, _} = FerricStore.flow_policy_set(type, indexed_attributes: ["tenant"])

    assert :ok =
             FerricStore.flow_create(unique_flow_id("attrs-discovery-nul"),
               type: type,
               state: "queued",
               partition_key: @partition,
               attributes: %{"tenant" => value},
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert_eventually(fn ->
      assert {:ok, [%{value: ^value, count: 1}]} =
               FerricStore.flow_attribute_values(type, "tenant",
                 state: "queued",
                 partition_key: @partition,
                 consistent_projection: true
               )
    end)
  end

  test "attribute discovery marks bounded counts as approximate when scan limit is reached" do
    previous_limit = Application.get_env(:ferricstore, :flow_attribute_discovery_scan_limit)
    Application.put_env(:ferricstore, :flow_attribute_discovery_scan_limit, 2)

    on_exit(fn ->
      case previous_limit do
        nil -> Application.delete_env(:ferricstore, :flow_attribute_discovery_scan_limit)
        value -> Application.put_env(:ferricstore, :flow_attribute_discovery_scan_limit, value)
      end
    end)

    type = unique_flow_id("attrs-discovery-approx-type")

    assert {:ok, _} = FerricStore.flow_policy_set(type, indexed_attributes: ["tenant"])

    for idx <- 1..3 do
      assert :ok =
               FerricStore.flow_create(unique_flow_id("attrs-discovery-approx-#{idx}"),
                 type: type,
                 state: "queued",
                 partition_key: @partition,
                 attributes: %{"tenant" => "acme"},
                 run_at_ms: 1_000 + idx,
                 now_ms: 1_000 + idx
               )
    end

    assert_eventually(fn ->
      assert {:ok, [%{name: "tenant", approximate: true}]} =
               FerricStore.flow_attributes(type,
                 state: "queued",
                 partition_key: @partition,
                 consistent_projection: true
               )
    end)

    assert {:ok, [%{value: "acme", approximate: true}]} =
             FerricStore.flow_attribute_values(type, "tenant",
               state: "queued",
               partition_key: @partition,
               consistent_projection: true
             )
  end

  test "multi-attribute search plans from selective candidate before applying count" do
    type = unique_flow_id("attrs-selective-type")
    winner = unique_flow_id("attrs-selective-winner")

    assert {:ok, _} = FerricStore.flow_policy_set(type, indexed_attributes: ["tenant", "region"])

    for idx <- 1..8 do
      assert :ok =
               FerricStore.flow_create(unique_flow_id("attrs-selective-broad-#{idx}"),
                 type: type,
                 state: "queued",
                 partition_key: @partition,
                 attributes: %{"tenant" => "acme", "region" => "eu"},
                 run_at_ms: 1_000 + idx,
                 now_ms: 1_000 + idx
               )
    end

    assert :ok =
             FerricStore.flow_create(winner,
               type: type,
               state: "queued",
               partition_key: @partition,
               attributes: %{"tenant" => "acme", "region" => "us"},
               run_at_ms: 2_000,
               now_ms: 2_000
             )

    assert_eventually(fn ->
      assert {:ok, [record]} =
               FerricStore.flow_search(
                 type: type,
                 partition_key: @partition,
                 attributes: %{"tenant" => "acme", "region" => "us"},
                 consistent_projection: true,
                 count: 1
               )

      assert record.id == winner
    end)
  end

  test "retention cleanup removes attribute discovery entries" do
    type = unique_flow_id("attrs-discovery-retention-type")
    id = unique_flow_id("attrs-discovery-retention")
    now = System.system_time(:millisecond)

    assert {:ok, _} = FerricStore.flow_policy_set(type, indexed_attributes: ["tenant"])

    assert :ok =
             FerricStore.flow_create(id,
               type: type,
               state: "queued",
               partition_key: @partition,
               attributes: %{"tenant" => "acme"},
               retention_ttl_ms: 60_000,
               run_at_ms: now,
               now_ms: now
             )

    claimed = claim_one!(type, "queued", now + 1)

    assert :ok =
             FerricStore.flow_complete(id, claimed.lease_token,
               fencing_token: claimed.fencing_token,
               partition_key: @partition,
               now_ms: now + 2
             )

    assert {:ok, completed} = FerricStore.flow_get(id, partition_key: @partition)
    cleanup_now_ms = completed.terminal_retention_until_ms + 1

    assert_eventually(fn ->
      assert {:ok, [%{name: "tenant", count: 1}]} =
               FerricStore.flow_attributes(type,
                 state: "completed",
                 partition_key: @partition,
                 consistent_projection: true
               )
    end)

    assert {:ok, cleaned} =
             FerricStore.flow_retention_cleanup(limit: 10, now_ms: cleanup_now_ms)

    assert cleaned.flows >= 1

    assert_eventually(fn ->
      assert {:ok, []} =
               FerricStore.flow_attributes(type,
                 state: "completed",
                 partition_key: @partition,
                 consistent_projection: true
               )
    end)
  end

  defp claim_one!(type, state, now_ms) do
    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due(type,
               states: [state],
               partition_key: @partition,
               worker: "w1",
               limit: 1,
               now_ms: now_ms
             )

    claimed
  end

  defp assert_flow_attrs(id, attrs) do
    assert {:ok, record} = FerricStore.flow_get(id, partition_key: @partition)
    assert record.attributes == attrs
  end
end
