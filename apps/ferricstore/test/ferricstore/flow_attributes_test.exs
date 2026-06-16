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
  end

  test "restart rebuild preserves attribute search correctness" do
    isolated = ShardHelpers.setup_isolated_data_dir()

    on_exit(fn ->
      ShardHelpers.teardown_isolated_data_dir(isolated)
    end)

    type = unique_flow_id("attrs-restart-type")
    id = unique_flow_id("attrs-restart")
    partition = unique_flow_id("tenant-restart")

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
    end)
  end

  test "retention cleanup removes terminal attribute indexes" do
    type = unique_flow_id("attrs-retention-type")
    id = unique_flow_id("attrs-retention")
    now = System.system_time(:millisecond)

    assert :ok =
             FerricStore.flow_create(id,
               type: type,
               state: "queued",
               partition_key: @partition,
               attributes: %{"tenant" => "acme", "phase" => "terminal"},
               retention_ttl_ms: 1_000,
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
    end)

    assert {:ok, cleaned} = FerricStore.flow_retention_cleanup(limit: 10, now_ms: now + 2_000)
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
