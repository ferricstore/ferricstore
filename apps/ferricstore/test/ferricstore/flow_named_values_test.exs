defmodule Ferricstore.FlowNamedValuesTest do
  use ExUnit.Case, async: false
  @moduletag :flow
  @moduletag :global_state

  alias Ferricstore.Test.ShardHelpers

  setup_all do
    ShardHelpers.wait_shards_alive()
    :ok
  end

  setup do
    ShardHelpers.flush_all_keys()
    :ok
  end

  defp uid(prefix), do: "#{prefix}:#{System.unique_integer([:positive])}"

  test "create stores multiple named values and get hydrates selected names only" do
    id = uid("named-create")

    assert :ok =
             FerricStore.flow_create(id,
               type: "named-values",
               partition_key: "tenant-a",
               values: %{"order" => "order-bytes", "payment" => "payment-bytes"},
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert {:ok, ref_only} = FerricStore.flow_get(id, partition_key: "tenant-a")
    assert is_map(ref_only.value_refs)
    assert Map.has_key?(ref_only.value_refs, "order")
    refute Map.has_key?(ref_only, :values)

    assert {:ok, hydrated} =
             FerricStore.flow_get(id, partition_key: "tenant-a", values: ["order"])

    assert hydrated.values == %{"order" => "order-bytes"}
    refute Map.has_key?(hydrated.values, "payment")
  end

  test "named value put is idempotent and override creates a new immutable ref" do
    id = uid("named-put")

    assert :ok =
             FerricStore.flow_create(id,
               type: "named-values",
               partition_key: "tenant-a",
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert {:ok, first} =
             FerricStore.flow_value_put("order-v1",
               partition_key: "tenant-a",
               owner_flow_id: id,
               name: "order",
               now_ms: 1_010
             )

    assert first.created == true
    assert first.stored == true

    assert {:ok, replay} =
             FerricStore.flow_value_put("order-v1",
               partition_key: "tenant-a",
               owner_flow_id: id,
               name: "order",
               now_ms: 1_020
             )

    assert replay.ref == first.ref
    assert replay.created == false
    assert replay.stored == false

    assert {:error,
            "ERR flow value order already exists with different digest; use OVERRIDE true"} =
             FerricStore.flow_value_put("order-v2",
               partition_key: "tenant-a",
               owner_flow_id: id,
               name: "order",
               now_ms: 1_030
             )

    assert {:ok, override} =
             FerricStore.flow_value_put("order-v2",
               partition_key: "tenant-a",
               owner_flow_id: id,
               name: "order",
               override: true,
               now_ms: 1_040
             )

    assert override.ref != first.ref
    assert override.version == 2

    assert {:ok, fetched} =
             FerricStore.flow_get(id, partition_key: "tenant-a", values: ["order"])

    assert fetched.values == %{"order" => "order-v2"}
  end

  test "WARaft blob side-channel named value put returns original large value" do
    id = uid("named-put-blob")
    partition_key = "tenant-a"
    payload = :binary.copy("large-named-value", 64)

    original_ctx = FerricStore.Instance.get(:default)
    Process.put(:ferricstore_blob_store_segment_gc_grace_ms, 0)

    try do
      :persistent_term.put(
        {FerricStore.Instance, :default},
        %{original_ctx | blob_side_channel_threshold_bytes: 128}
      )

      assert byte_size(payload) >
               FerricStore.Instance.get(:default).blob_side_channel_threshold_bytes

      assert :ok =
               FerricStore.flow_create(id,
                 type: "named-values",
                 partition_key: partition_key,
                 run_at_ms: 1_000,
                 now_ms: 1_000
               )

      assert {:ok, %{ref: value_ref}} =
               FerricStore.flow_value_put(payload,
                 partition_key: partition_key,
                 owner_flow_id: id,
                 name: "doc",
                 now_ms: 1_010
               )

      assert {:ok, [^payload]} = FerricStore.flow_value_mget([value_ref])
    after
      :persistent_term.put({FerricStore.Instance, :default}, original_ctx)
      Process.delete(:ferricstore_blob_store_segment_gc_grace_ms)
    end
  end

  test "claim_due hydrates selected named values with one logical request" do
    id = uid("named-claim")

    assert :ok =
             FerricStore.flow_create(id,
               type: "named-claim",
               partition_key: "tenant-a",
               values: %{"order" => "order-bytes", "payment" => "payment-bytes"},
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due("named-claim",
               partition_key: "tenant-a",
               worker: "worker-a",
               limit: 1,
               now_ms: 1_000,
               payload: false,
               values: ["payment"]
             )

    assert claimed.id == id
    assert claimed.values == %{"payment" => "payment-bytes"}
    refute Map.has_key?(claimed.values, "order")
  end

  test "value_mget decodes refs and preserves request order" do
    id = uid("named-mget")

    assert :ok =
             FerricStore.flow_create(id,
               type: "named-mget",
               partition_key: "tenant-a",
               values: %{"a" => "value-a", "b" => "value-b"},
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert {:ok, flow} = FerricStore.flow_get(id, partition_key: "tenant-a")
    ref_a = get_in(flow.value_refs, ["a", :ref])
    ref_b = get_in(flow.value_refs, ["b", :ref])

    assert {:ok, ["value-b", "value-a", "value-b"]} =
             FerricStore.flow_value_mget([ref_b, ref_a, ref_b])
  end

  test "transition can add and drop named values without hydrating bytes" do
    id = uid("named-transition")

    assert :ok =
             FerricStore.flow_create(id,
               type: "named-transition",
               partition_key: "tenant-a",
               values: %{"order" => "order-v1"},
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due("named-transition",
               partition_key: "tenant-a",
               worker: "worker-transition",
               limit: 1,
               now_ms: 1_000,
               payload: false
             )

    assert :ok =
             FerricStore.flow_transition(id, "running", "waiting",
               partition_key: "tenant-a",
               lease_token: claimed.lease_token,
               fencing_token: claimed.fencing_token,
               values: %{"payment" => "payment-v1"},
               drop_values: ["order"],
               run_at_ms: 2_000,
               now_ms: 1_100
             )

    assert {:ok, fetched} =
             FerricStore.flow_get(id,
               partition_key: "tenant-a",
               values: ["order", "payment"]
             )

    assert fetched.values == %{"payment" => "payment-v1"}
    refute Map.has_key?(fetched.value_refs, "order")
  end

  test "terminal commands can attach named values" do
    id = uid("named-terminal")

    assert :ok =
             FerricStore.flow_create(id,
               type: "named-terminal",
               partition_key: "tenant-a",
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due("named-terminal",
               partition_key: "tenant-a",
               worker: "worker-terminal",
               limit: 1,
               now_ms: 1_000,
               payload: false
             )

    assert :ok =
             FerricStore.flow_complete(id, claimed.lease_token,
               partition_key: "tenant-a",
               fencing_token: claimed.fencing_token,
               values: %{"receipt" => "receipt-v1"},
               now_ms: 1_100
             )

    assert {:ok, fetched} =
             FerricStore.flow_get(id, partition_key: "tenant-a", values: ["receipt"])

    assert fetched.state == "completed"
    assert fetched.values == %{"receipt" => "receipt-v1"}
  end

  test "retention cleanup removes owned named value blobs" do
    id = uid("named-retention")

    assert :ok =
             FerricStore.flow_create(id,
               type: "named-retention",
               partition_key: "tenant-retention",
               values: %{"order" => "order-v1"},
               retention_ttl_ms: 100,
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert {:ok, created} = FerricStore.flow_get(id, partition_key: "tenant-retention")
    order_ref = get_in(created.value_refs, ["order", :ref])
    assert is_binary(order_ref)
    assert {:ok, _blob} = FerricStore.get(order_ref)

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due("named-retention",
               partition_key: "tenant-retention",
               worker: "worker-retention",
               limit: 1,
               now_ms: 1_000,
               payload: false
             )

    assert :ok =
             FerricStore.flow_complete(id, claimed.lease_token,
               partition_key: "tenant-retention",
               fencing_token: claimed.fencing_token,
               values: %{"receipt" => "receipt-v1"},
               now_ms: 1_100
             )

    assert {:ok, completed} = FerricStore.flow_get(id, partition_key: "tenant-retention")
    receipt_ref = get_in(completed.value_refs, ["receipt", :ref])
    assert is_binary(receipt_ref)

    Process.sleep(150)

    assert {:ok, cleaned} = FerricStore.flow_retention_cleanup(limit: 10)
    assert cleaned.flows >= 1
    assert cleaned.values >= 2

    assert {:ok, nil} = FerricStore.flow_get(id, partition_key: "tenant-retention")
    assert {:ok, nil} = FerricStore.get(order_ref)
    assert {:ok, nil} = FerricStore.get(receipt_ref)
  end

  test "retention cleanup removes overridden named value versions from history" do
    id = uid("named-retention-override")

    assert :ok =
             FerricStore.flow_create(id,
               type: "named-retention-override",
               partition_key: "tenant-retention",
               values: %{"order" => "order-v1"},
               retention_ttl_ms: 100,
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert {:ok, created} = FerricStore.flow_get(id, partition_key: "tenant-retention")
    old_ref = get_in(created.value_refs, ["order", :ref])

    assert {:ok, override} =
             FerricStore.flow_value_put("order-v2",
               partition_key: "tenant-retention",
               owner_flow_id: id,
               name: "order",
               override: true,
               now_ms: 1_050
             )

    assert override.ref != old_ref
    assert {:ok, _old_blob} = FerricStore.get(old_ref)
    assert {:ok, _new_blob} = FerricStore.get(override.ref)

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due("named-retention-override",
               partition_key: "tenant-retention",
               worker: "worker-retention-override",
               limit: 1,
               now_ms: 1_000,
               payload: false
             )

    assert :ok =
             FerricStore.flow_complete(id, claimed.lease_token,
               partition_key: "tenant-retention",
               fencing_token: claimed.fencing_token,
               now_ms: 1_100
             )

    Process.sleep(150)

    assert {:ok, cleaned} = FerricStore.flow_retention_cleanup(limit: 10)
    assert cleaned.values >= 2

    assert {:ok, nil} = FerricStore.get(old_ref)
    assert {:ok, nil} = FerricStore.get(override.ref)
  end

  test "transition_many stores named value blobs for every transitioned flow" do
    partition = "tenant-transition-many"
    type = uid("named-transition-many")
    ids = [uid("named-transition-many-a"), uid("named-transition-many-b")]

    for id <- ids do
      assert :ok =
               FerricStore.flow_create(id,
                 type: type,
                 partition_key: partition,
                 run_at_ms: 1_000,
                 now_ms: 1_000
               )
    end

    assert {:ok, claims} =
             FerricStore.flow_claim_due(type,
               partition_key: partition,
               worker: "worker-transition-many",
               limit: 2,
               now_ms: 1_000,
               payload: false
             )

    items =
      Enum.map(claims, fn claim ->
        %{
          id: claim.id,
          lease_token: claim.lease_token,
          fencing_token: claim.fencing_token
        }
      end)

    assert :ok =
             FerricStore.flow_transition_many(partition, "running", "waiting", items,
               values: %{"step" => "transition-many"},
               run_at_ms: 2_000,
               now_ms: 1_100
             )

    for id <- ids do
      assert {:ok, fetched} =
               FerricStore.flow_get(id,
                 partition_key: partition,
                 values: ["step"]
               )

      assert fetched.state == "waiting"
      assert fetched.values == %{"step" => "transition-many"}
    end
  end

  test "rewind restores named value refs from the target history snapshot" do
    id = uid("named-rewind")

    assert :ok =
             FerricStore.flow_create(id,
               type: "named-rewind",
               values: %{"order" => "order-v1"},
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert {:ok, [{created_event_id, %{"event" => "created"}} | _]} =
             FerricStore.flow_history(id, count: 10)

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due("named-rewind",
               worker: "worker-rewind",
               limit: 1,
               now_ms: 1_000,
               payload: false
             )

    assert :ok =
             FerricStore.flow_transition(id, "running", "waiting",
               lease_token: claimed.lease_token,
               fencing_token: claimed.fencing_token,
               values: %{"order" => "order-v2"},
               override_values: ["order"],
               run_at_ms: 2_000,
               now_ms: 1_100
             )

    assert {:ok, after_transition} = FerricStore.flow_get(id, values: ["order"])
    assert after_transition.values == %{"order" => "order-v2"}

    assert :ok =
             FerricStore.flow_rewind(id,
               to_event: created_event_id,
               expect_state: "waiting",
               run_at_ms: 3_000,
               now_ms: 1_200
             )

    assert {:ok, rewound} = FerricStore.flow_get(id, values: ["order"])
    assert rewound.state == "queued"
    assert rewound.values == %{"order" => "order-v1"}
  end

  test "create_many and spawn_children support per-item named values" do
    parent = uid("named-parent")
    child = uid("named-child")
    created = uid("named-many-created")

    assert :ok =
             FerricStore.flow_create(parent,
               type: "named-parent",
               partition_key: "tenant-named-many",
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert :ok =
             FerricStore.flow_create_many(
               "tenant-named-many",
               [
                 %{
                   id: created,
                   values: %{"order" => "order-created"},
                   value_refs: %{"external" => "external-ref"}
                 }
               ],
               type: "named-many-created",
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert {:ok, created_flow} =
             FerricStore.flow_get(created,
               partition_key: "tenant-named-many",
               values: ["order"]
             )

    assert created_flow.values == %{"order" => "order-created"}
    assert get_in(created_flow.value_refs, ["external", :ref]) == "external-ref"

    assert {:ok, [claimed_parent]} =
             FerricStore.flow_claim_due("named-parent",
               partition_key: "tenant-named-many",
               worker: "worker-named-parent",
               limit: 1,
               now_ms: 1_000,
               payload: false
             )

    assert :ok =
             FerricStore.flow_spawn_children(
               parent,
               [
                 %{
                   id: child,
                   type: "named-child",
                   values: %{"order" => "order-child"},
                   value_refs: %{"external" => "external-child-ref"}
                 }
               ],
               partition_key: "tenant-named-many",
               lease_token: claimed_parent.lease_token,
               fencing_token: claimed_parent.fencing_token,
               group_id: "named-group",
               wait_state: "waiting",
               success: "completed",
               failure: "failed",
               now_ms: 1_100
             )

    assert {:ok, child_flow} =
             FerricStore.flow_get(child,
               partition_key: "tenant-named-many",
               values: ["order"]
             )

    assert child_flow.values == %{"order" => "order-child"}
    assert get_in(child_flow.value_refs, ["external", :ref]) == "external-child-ref"
  end
end
