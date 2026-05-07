defmodule Ferricstore.FlowTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Test.ShardHelpers

  setup_all do
    ShardHelpers.wait_shards_alive()
    :ok
  end

  setup do
    ShardHelpers.flush_all_keys()
    :ok
  end

  defp attach_flow_telemetry(events) do
    test_pid = self()

    handler_ids =
      Enum.map(events, fn event ->
        handler_id = {__MODULE__, self(), event, System.unique_integer([:positive])}

        :ok =
          :telemetry.attach(
            handler_id,
            event,
            &__MODULE__.handle_telemetry/4,
            test_pid
          )

        handler_id
      end)

    on_exit(fn ->
      Enum.each(handler_ids, &:telemetry.detach/1)
    end)
  end

  def handle_telemetry(event, measurements, metadata, test_pid) do
    send(test_pid, {:flow_telemetry, event, measurements, metadata})
  end

  defp uid(prefix), do: "#{prefix}:#{System.unique_integer([:positive])}"

  defp shard_for(key) do
    Ferricstore.Store.Router.shard_for(FerricStore.Instance.get(:default), key)
  end

  defp different_partition_keys do
    base = "tenant:" <> Integer.to_string(System.unique_integer([:positive]))

    first =
      1..64
      |> Enum.map(&"#{base}:#{&1}")
      |> Enum.find(fn key ->
        shard_for(Ferricstore.Flow.Keys.state_key("probe", key)) !=
          shard_for(Ferricstore.Flow.Keys.state_key("probe", nil))
      end)

    second =
      1..64
      |> Enum.map(&"#{base}:other:#{&1}")
      |> Enum.find(fn key ->
        shard_for(Ferricstore.Flow.Keys.state_key("probe", key)) !=
          shard_for(Ferricstore.Flow.Keys.state_key("probe", first))
      end)

    {first, second}
  end

  defp mixed_partition_keys do
    base = "tenant:" <> Integer.to_string(System.unique_integer([:positive]))

    groups =
      1..256
      |> Enum.map(&"#{base}:#{&1}")
      |> Enum.group_by(fn key -> shard_for(Ferricstore.Flow.Keys.state_key("probe", key)) end)

    {same_shard, [same_a, same_b | _]} =
      Enum.find(groups, fn {_shard, keys} -> length(keys) >= 2 end)

    {other_shard, [other | _]} = Enum.find(groups, fn {shard, _keys} -> shard != same_shard end)

    assert same_shard != other_shard
    {same_a, same_b, other}
  end

  defp create_claimed_flow(id, partition_key, flow_type, worker) do
    assert {:ok, _} =
             FerricStore.flow_create(id,
               type: flow_type,
               partition_key: partition_key,
               state: "queued",
               run_at_ms: 1_000
             )

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due(flow_type,
               partition_key: partition_key,
               worker: worker,
               limit: 1,
               now_ms: 1_000
             )

    claimed
  end

  test "flow internal keys use compact partition tags" do
    partition_key = "tenant:device:" <> String.duplicate("abcdef0123456789", 4)

    state_key = Ferricstore.Flow.Keys.state_key("flow-a", partition_key)
    history_key = Ferricstore.Flow.Keys.history_key("flow-a", partition_key)
    due_key = Ferricstore.Flow.Keys.due_key("checkout", "queued", 0, partition_key)

    assert state_key =~ ~r/^f:\{f:[A-Za-z0-9_-]{43}\}:s:flow-a$/
    assert history_key =~ ~r/^f:\{f:[A-Za-z0-9_-]{43}\}:h:flow-a$/
    assert due_key =~ ~r/^f:\{f:[A-Za-z0-9_-]{43}\}:d:checkout:queued:p0$/
    assert Ferricstore.Flow.Keys.state_key?(state_key)
    refute Ferricstore.Flow.Keys.state_key?(history_key)

    old_state_key = "flow:{flow:" <> String.duplicate("0", 64) <> "}:state:flow-a"
    assert byte_size(state_key) < byte_size(old_state_key)
  end

  test "flow state record encoding is compact and decodes old map records" do
    record = %{
      id: "flow-1",
      type: "checkout",
      state: "queued",
      version: 3,
      attempts: 2,
      fencing_token: 9,
      created_at_ms: 1_000,
      updated_at_ms: 1_100,
      next_run_at_ms: 1_200,
      priority: 1,
      ttl_ms: 60_000,
      history_max_events: 100,
      partition_key: "tenant-a",
      payload_ref: "payload:1",
      parent_flow_id: "parent-1",
      root_flow_id: "root-1",
      correlation_id: "order-1",
      result_ref: "result:1",
      error_ref: nil,
      lease_owner: "worker-1",
      lease_token: "lease-1",
      lease_deadline_ms: 2_000,
      run_state: "charge_card",
      rewound_to_event_id: "1000-1"
    }

    compact = Ferricstore.Flow.encode_record(record)
    old_map = :erlang.term_to_binary(record)
    normal_record = Map.delete(record, :rewound_to_event_id)

    assert "FSF1" <> _ = compact
    assert Ferricstore.Flow.decode_record(compact) == record
    assert Ferricstore.Flow.decode_record(old_map) == record

    assert Ferricstore.Flow.decode_record(Ferricstore.Flow.encode_record(normal_record)) ==
             normal_record

    old_compact_without_run_state =
      record
      |> Map.drop([:run_state, :rewound_to_event_id])
      |> Ferricstore.Flow.encode_record()
      |> then(fn encoded -> binary_part(encoded, 0, byte_size(encoded) - 1) end)

    assert Ferricstore.Flow.decode_record(old_compact_without_run_state).run_state == nil

    assert byte_size(compact) < byte_size(old_map)

    assert_raise ArgumentError, fn ->
      Ferricstore.Flow.decode_record("FSF2" <> binary_part(compact, 4, byte_size(compact) - 4))
    end

    assert_raise ArgumentError, fn ->
      Ferricstore.Flow.decode_record(binary_part(compact, 0, byte_size(compact) - 1))
    end
  end

  test "flow history encoding is compact and decodes old field lists" do
    record = %{
      id: "flow-1",
      type: "checkout",
      state: "queued",
      version: 2,
      attempts: 1,
      fencing_token: 4,
      created_at_ms: 1_000,
      updated_at_ms: 1_100,
      next_run_at_ms: 1_200,
      priority: 1,
      lease_deadline_ms: 2_000,
      lease_owner: "worker-1",
      payload_ref: "payload:1",
      parent_flow_id: "parent-1",
      root_flow_id: "root-1",
      correlation_id: "order-1",
      result_ref: nil,
      error_ref: "error:1",
      rewound_to_event_id: nil
    }

    old_fields = [
      "event",
      "retry",
      "version",
      "2",
      "at",
      "1100",
      "id",
      "flow-1",
      "type",
      "checkout",
      "state",
      "queued",
      "priority",
      "1",
      "attempts",
      "1",
      "fencing_token",
      "4",
      "created_at_ms",
      "1000",
      "updated_at_ms",
      "1100",
      "next_run_at_ms",
      "1200",
      "lease_deadline_ms",
      "2000",
      "lease_owner",
      "worker-1",
      "payload_ref",
      "payload:1",
      "parent_flow_id",
      "parent-1",
      "root_flow_id",
      "root-1",
      "correlation_id",
      "order-1",
      "result_ref",
      "",
      "error_ref",
      "error:1",
      "rewound_to_event_id",
      ""
    ]

    compact = Ferricstore.Flow.encode_history_fields(record, "retry", 1_100)
    old = :erlang.term_to_binary(old_fields)

    assert "FSH1" <> _ = compact
    assert Ferricstore.Flow.decode_history_fields(compact) == old_fields
    assert Ferricstore.Flow.decode_history_fields(old) == old_fields
    assert byte_size(compact) < byte_size(old)

    assert Ferricstore.Flow.decode_history_fields(
             "FSH2" <> binary_part(compact, 4, byte_size(compact) - 4)
           ) == []

    assert Ferricstore.Flow.decode_history_fields(binary_part(compact, 0, byte_size(compact) - 1)) ==
             []
  end

  test "flow_create stores state and prevents duplicate ids" do
    id = uid("flow-create")

    assert {:ok, flow} =
             FerricStore.flow_create(id,
               type: "checkout",
               state: "queued",
               payload_ref: "payload:" <> id,
               run_at_ms: 1_000
             )

    assert flow.id == id
    assert flow.type == "checkout"
    assert flow.state == "queued"
    assert flow.version == 1
    assert flow.fencing_token == 0
    assert flow.payload_ref == "payload:" <> id

    assert {:ok, fetched} = FerricStore.flow_get(id)
    assert fetched.id == id
    assert fetched.state == "queued"

    assert {:error, "ERR flow already exists"} =
             FerricStore.flow_create(id, type: "checkout", state: "queued")
  end

  test "flow_get hydrates small payload refs by default and supports opt-out/caps" do
    id = uid("flow-get-payload")
    payload_key = "payload:" <> id
    payload = "payload-body"

    assert :ok = FerricStore.set(payload_key, payload)

    assert {:ok, _flow} =
             FerricStore.flow_create(id,
               type: "checkout",
               state: "queued",
               payload_ref: payload_key,
               run_at_ms: 1_000
             )

    assert {:ok, fetched} = FerricStore.flow_get(id)
    assert fetched.payload == payload
    assert fetched.payload_size == byte_size(payload)
    refute Map.has_key?(fetched, :payload_omitted)

    assert {:ok, no_payload} = FerricStore.flow_get(id, payload: false)
    refute Map.has_key?(no_payload, :payload)
    refute Map.has_key?(no_payload, :payload_omitted)

    assert {:ok, capped} = FerricStore.flow_get(id, payload_max_bytes: 4)
    assert capped.payload_omitted == true
    assert capped.payload_size == byte_size(payload)
    refute Map.has_key?(capped, :payload)
  end

  test "flow_claim_due returns payload inline up to cap without rolling back missing payloads" do
    type = uid("claim-payload")
    id = uid("claim-payload-flow")
    payload_key = "payload:" <> id
    payload = "worker-input"

    assert :ok = FerricStore.set(payload_key, payload)

    assert {:ok, _flow} =
             FerricStore.flow_create(id,
               type: type,
               state: "queued",
               payload_ref: payload_key,
               run_at_ms: 1_000
             )

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due(type,
               worker: "worker-a",
               limit: 1,
               now_ms: 1_000,
               payload_max_bytes: 64
             )

    assert claimed.id == id
    assert claimed.payload == payload
    assert claimed.payload_size == byte_size(payload)

    missing_id = uid("claim-missing-payload-flow")
    missing_payload_key = "payload:" <> missing_id

    assert {:ok, _flow} =
             FerricStore.flow_create(missing_id,
               type: type,
               state: "queued",
               payload_ref: missing_payload_key,
               run_at_ms: 2_000
             )

    assert {:ok, [missing]} =
             FerricStore.flow_claim_due(type,
               worker: "worker-b",
               limit: 1,
               now_ms: 2_000
             )

    assert missing.id == missing_id
    assert missing.payload == nil
    assert missing.payload_missing == true
    assert {:ok, %{state: "running"}} = FerricStore.flow_get(missing_id)
  end

  test "pipeline_read_batch preserves order across get, missing, and history reads" do
    ctx = FerricStore.Instance.get(:default)
    {partition_a, partition_b} = different_partition_keys()
    id_a = uid("flow-pipeline-read-a")
    id_b = uid("flow-pipeline-read-b")

    attach_flow_telemetry([[:ferricstore, :flow, :pipeline_read_batch]])

    assert {:ok, _} =
             FerricStore.flow_create(id_a,
               type: "pipeline-read",
               state: "queued",
               partition_key: partition_a,
               now_ms: 1,
               run_at_ms: 1
             )

    assert {:ok, _} =
             FerricStore.flow_create(id_b,
               type: "pipeline-read",
               state: "queued",
               partition_key: partition_b,
               now_ms: 2,
               run_at_ms: 2
             )

    assert [
             {:ok, %{id: ^id_a, partition_key: ^partition_a}},
             {:ok, nil},
             {:ok, %{id: ^id_b, partition_key: ^partition_b}},
             {:ok, [{_event_id, %{"event" => "created", "state" => "queued"}}]}
           ] =
             Ferricstore.Flow.pipeline_read_batch(ctx, [
               {:get, id_a, [partition_key: partition_a]},
               {:get, "missing-pipeline-read", [partition_key: partition_a]},
               {:get, id_b, [partition_key: partition_b]},
               {:history, id_a, [partition_key: partition_a, count: 10]}
             ])

    assert_receive {:flow_telemetry, [:ferricstore, :flow, :pipeline_read_batch],
                    %{count: 4, gets: 3, histories: 1}, %{source: :pipeline}}
  end

  test "pipeline_read_batch hydrates Flow GET payloads with per-command caps" do
    ctx = FerricStore.Instance.get(:default)
    id_a = uid("flow-pipeline-payload-a")
    id_b = uid("flow-pipeline-payload-b")
    payload_a = "payload-a"
    payload_b = "payload-b"

    assert :ok = FerricStore.set("payload:" <> id_a, payload_a)
    assert :ok = FerricStore.set("payload:" <> id_b, payload_b)

    assert {:ok, _flow} =
             FerricStore.flow_create(id_a,
               type: "pipeline-payload",
               payload_ref: "payload:" <> id_a,
               run_at_ms: 1
             )

    assert {:ok, _flow} =
             FerricStore.flow_create(id_b,
               type: "pipeline-payload",
               payload_ref: "payload:" <> id_b,
               run_at_ms: 1
             )

    assert [
             {:ok, %{id: ^id_a, payload: ^payload_a, payload_size: 9}},
             {:ok, %{id: ^id_b, payload_omitted: true, payload_size: 9}},
             {:ok, no_payload}
           ] =
             Ferricstore.Flow.pipeline_read_batch(ctx, [
               {:get, id_a, []},
               {:get, id_b, [payload_max_bytes: 4]},
               {:get, id_a, [payload: false]}
             ])

    refute Map.has_key?(no_payload, :payload)
    refute Map.has_key?(no_payload, :payload_omitted)
  end

  test "flow_create idempotent retry returns matching existing record and rejects conflicts" do
    id = uid("flow-create-idempotent")

    assert {:ok, created} =
             FerricStore.flow_create(id,
               type: "checkout",
               state: "queued",
               payload_ref: "payload:" <> id,
               run_at_ms: 1_000,
               now_ms: 10,
               idempotent: true
             )

    assert {:ok, retried} =
             FerricStore.flow_create(id,
               type: "checkout",
               state: "queued",
               payload_ref: "payload:" <> id,
               run_at_ms: 1_000,
               now_ms: 20,
               idempotent: true
             )

    assert retried.id == created.id
    assert retried.version == created.version
    assert retried.created_at_ms == created.created_at_ms

    assert {:ok, history} = FerricStore.flow_history(id)
    assert Enum.map(history, fn {_event_id, fields} -> fields["event"] end) == ["created"]

    assert {:error, "ERR flow idempotency conflict"} =
             FerricStore.flow_create(id,
               type: "checkout",
               state: "queued",
               payload_ref: "different:" <> id,
               run_at_ms: 1_000,
               idempotent: true
             )
  end

  test "flow due index stays derived and does not persist per-flow zset members" do
    id = uid("flow-due-derived")
    run_at_ms = 1_234

    assert {:ok, _flow} =
             FerricStore.flow_create(id,
               type: "checkout",
               state: "queued",
               run_at_ms: run_at_ms
             )

    due_key = Ferricstore.Flow.Keys.due_key("checkout", "queued", 0, nil)
    member_key = Ferricstore.Store.CompoundKey.zset_member(due_key, id)
    type_key = Ferricstore.Store.CompoundKey.type_key(due_key)

    assert {:ok, nil} = FerricStore.get(member_key)
    assert {:ok, nil} = FerricStore.get(type_key)

    assert {:ok, [%{id: ^id}]} =
             FerricStore.flow_claim_due("checkout",
               state: "queued",
               worker: "worker-a",
               now_ms: run_at_ms,
               lease_ms: 1_000,
               limit: 10
             )
  end

  test "flow_create stores debug lineage metadata and indexes it" do
    id = uid("flow-lineage-child")
    parent = uid("flow-lineage-parent")
    root = uid("flow-lineage-root")
    correlation = uid("order")
    partition = uid("tenant")

    assert {:ok, flow} =
             FerricStore.flow_create(id,
               type: "lineage",
               partition_key: partition,
               parent_flow_id: parent,
               root_flow_id: root,
               correlation_id: correlation,
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert flow.parent_flow_id == parent
    assert flow.root_flow_id == root
    assert flow.correlation_id == correlation

    assert {:ok, [%{id: ^id}]} =
             FerricStore.flow_by_parent(parent, partition_key: partition, count: 10)

    assert {:ok, []} =
             FerricStore.zrange(Ferricstore.Flow.Keys.parent_index_key(parent, partition), 0, 10)

    assert {:ok, [%{id: ^id}]} =
             FerricStore.flow_by_root(root, partition_key: partition, count: 10)

    assert {:ok, []} =
             FerricStore.zrange(Ferricstore.Flow.Keys.root_index_key(root, partition), 0, 10)

    assert {:ok, [%{id: ^id}]} =
             FerricStore.flow_by_correlation(correlation, partition_key: partition, count: 10)

    assert {:ok, []} =
             FerricStore.zrange(
               Ferricstore.Flow.Keys.correlation_index_key(correlation, partition),
               0,
               10
             )

    assert {:ok, [{_event_id, fields}]} =
             FerricStore.flow_history(id, partition_key: partition, count: 10)

    assert fields["parent_flow_id"] == parent
    assert fields["root_flow_id"] == root
    assert fields["correlation_id"] == correlation
  end

  test "flow_create stores default root without indexing one unique root per flow" do
    id = uid("flow-lineage-root-default")
    partition = uid("tenant")

    assert {:ok, flow} =
             FerricStore.flow_create(id,
               type: "lineage",
               partition_key: partition,
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert flow.root_flow_id == id

    assert {:ok, []} =
             FerricStore.zrange(Ferricstore.Flow.Keys.root_index_key(id, partition), 0, 10)
  end

  test "flow_by_parent root and correlation query lineage indexes" do
    partition = uid("tenant")
    root = uid("flow-root")
    child_a = uid("flow-child-a")
    child_b = uid("flow-child-b")
    grandchild = uid("flow-grandchild")
    correlation = uid("order")

    assert {:ok, %{id: ^root}} =
             FerricStore.flow_create(root,
               type: "lineage",
               partition_key: partition,
               correlation_id: correlation,
               now_ms: 1_000,
               run_at_ms: 1_000
             )

    assert {:ok, %{id: ^child_a}} =
             FerricStore.flow_create(child_a,
               type: "lineage",
               partition_key: partition,
               parent_flow_id: root,
               root_flow_id: root,
               correlation_id: correlation,
               now_ms: 2_000,
               run_at_ms: 2_000
             )

    assert {:ok, %{id: ^child_b}} =
             FerricStore.flow_create(child_b,
               type: "lineage",
               partition_key: partition,
               parent_flow_id: root,
               root_flow_id: root,
               correlation_id: correlation,
               now_ms: 3_000,
               run_at_ms: 3_000
             )

    assert {:ok, %{id: ^grandchild}} =
             FerricStore.flow_create(grandchild,
               type: "lineage",
               partition_key: partition,
               parent_flow_id: child_a,
               root_flow_id: root,
               correlation_id: correlation,
               now_ms: 4_000,
               run_at_ms: 4_000
             )

    assert {:ok, [%{id: ^child_a}, %{id: ^child_b}]} =
             FerricStore.flow_by_parent(root, partition_key: partition, count: 10)

    assert {:ok, []} =
             FerricStore.zrange(Ferricstore.Flow.Keys.parent_index_key(root, partition), 0, 10)

    assert {:ok, [%{id: ^root}, %{id: ^child_a}, %{id: ^child_b}, %{id: ^grandchild}]} =
             FerricStore.flow_by_root(root, partition_key: partition, count: 10)

    assert {:ok, []} =
             FerricStore.zrange(Ferricstore.Flow.Keys.root_index_key(root, partition), 0, 10)

    assert {:ok, [%{id: ^root}, %{id: ^child_a}]} =
             FerricStore.flow_by_correlation(correlation, partition_key: partition, count: 2)

    assert {:ok, []} =
             FerricStore.zrange(
               Ferricstore.Flow.Keys.correlation_index_key(correlation, partition),
               0,
               10
             )
  end

  test "flow_create_many creates one-partition batch atomically" do
    partition = uid("tenant")
    type = uid("bulk-create")
    id_a = uid("bulk-a")
    id_b = uid("bulk-b")

    assert {:ok, flows} =
             FerricStore.flow_create_many(
               partition,
               [
                 %{id: id_a, payload_ref: "payload:" <> id_a},
                 %{id: id_b, payload_ref: "payload:" <> id_b}
               ],
               type: type,
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert Enum.map(flows, & &1.id) == [id_a, id_b]
    assert Enum.all?(flows, &(&1.partition_key == partition))

    assert {:ok, claimed} =
             FerricStore.flow_claim_due(type,
               partition_key: partition,
               worker: "worker-a",
               limit: 10,
               now_ms: 1_000
             )

    assert claimed |> Enum.map(& &1.id) |> MapSet.new() == MapSet.new([id_a, id_b])
  end

  test "flow_create_many lets item attrs override common attrs" do
    partition = uid("tenant")
    type = uid("bulk-create-override")
    id_a = uid("bulk-override-a")
    id_b = uid("bulk-override-b")

    assert {:ok, [flow_a, flow_b]} =
             FerricStore.flow_create_many(
               partition,
               [
                 %{id: id_a},
                 %{id: id_b, payload_ref: "payload:item", correlation_id: "corr:item"}
               ],
               type: type,
               payload_ref: "payload:common",
               correlation_id: "corr:common",
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert flow_a.payload_ref == "payload:common"
    assert flow_a.correlation_id == "corr:common"
    assert flow_b.payload_ref == "payload:item"
    assert flow_b.correlation_id == "corr:item"
  end

  test "flow_create_many rejects missing partition and rolls back duplicate batches" do
    partition = uid("tenant")
    type = uid("bulk-atomic")
    existing_id = uid("bulk-existing")
    new_id = uid("bulk-new")

    assert {:error, "ERR flow partition_key is required"} =
             FerricStore.flow_create_many(nil, [%{id: new_id}], type: type)

    assert {:ok, _} =
             FerricStore.flow_create(existing_id,
               type: type,
               partition_key: partition,
               run_at_ms: 1_000
             )

    assert {:error, "ERR flow already exists"} =
             FerricStore.flow_create_many(
               partition,
               [%{id: existing_id}, %{id: new_id}],
               type: type,
               run_at_ms: 1_000
             )

    assert {:ok, nil} = FerricStore.flow_get(new_id, partition_key: partition)
    assert {:ok, []} = FerricStore.flow_history(new_id, partition_key: partition)

    assert {:error, "ERR flow duplicate id in batch"} =
             FerricStore.flow_create_many(
               partition,
               [%{id: new_id}, %{id: new_id}],
               type: type,
               run_at_ms: 1_000
             )

    assert {:ok, nil} = FerricStore.flow_get(new_id, partition_key: partition)
    assert {:ok, []} = FerricStore.flow_history(new_id, partition_key: partition)
  end

  test "flow_create_many idempotent retry returns existing records without duplicate writes" do
    partition = uid("tenant")
    type = uid("bulk-idempotent")
    existing_id = uid("bulk-existing")
    new_id = uid("bulk-new")

    assert {:ok, %{id: ^existing_id}} =
             FerricStore.flow_create(existing_id,
               type: type,
               partition_key: partition,
               payload_ref: "payload:" <> existing_id,
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert {:ok, records} =
             FerricStore.flow_create_many(
               partition,
               [
                 %{id: existing_id, payload_ref: "payload:" <> existing_id},
                 %{id: new_id, payload_ref: "payload:" <> new_id}
               ],
               type: type,
               run_at_ms: 1_000,
               now_ms: 1_000,
               idempotent: true
             )

    assert Enum.map(records, & &1.id) == [existing_id, new_id]

    assert {:ok, existing_history} =
             FerricStore.flow_history(existing_id, partition_key: partition)

    assert Enum.map(existing_history, fn {_event_id, fields} -> fields["event"] end) == [
             "created"
           ]

    assert {:ok, %{id: ^new_id}} = FerricStore.flow_get(new_id, partition_key: partition)
  end

  test "flow_create_many idempotency conflict rolls back same shard group" do
    partition = uid("tenant")
    type = uid("bulk-idempotency-conflict")
    existing_id = uid("bulk-existing")
    new_id = uid("bulk-new")

    assert {:ok, _} =
             FerricStore.flow_create(existing_id,
               type: type,
               partition_key: partition,
               payload_ref: "payload:old",
               run_at_ms: 1_000
             )

    assert {:error, "ERR flow idempotency conflict"} =
             FerricStore.flow_create_many(
               partition,
               [
                 %{id: existing_id, payload_ref: "payload:new"},
                 %{id: new_id, payload_ref: "payload:" <> new_id}
               ],
               type: type,
               run_at_ms: 1_000,
               idempotent: true
             )

    assert {:ok, nil} = FerricStore.flow_get(new_id, partition_key: partition)
    assert {:ok, []} = FerricStore.flow_history(new_id, partition_key: partition)
  end

  test "flow_create_many spans shards and rolls back failing shard group" do
    {same_a, same_b, other} = mixed_partition_keys()
    type = uid("bulk-mixed-create")
    existing_id = uid("bulk-mixed-existing")
    same_new_id = uid("bulk-mixed-same")
    other_new_id = uid("bulk-mixed-other")

    assert {:ok, _} =
             FerricStore.flow_create(existing_id,
               type: type,
               partition_key: same_a,
               run_at_ms: 1_000
             )

    assert {:ok, results} =
             FerricStore.flow_create_many(
               nil,
               [
                 %{id: existing_id, partition_key: same_a},
                 %{id: same_new_id, partition_key: same_b},
                 %{id: other_new_id, partition_key: other}
               ],
               type: type,
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert [
             {:error, "ERR flow already exists"},
             {:error, "ERR flow already exists"},
             %{id: ^other_new_id, partition_key: ^other}
           ] = results

    assert {:ok, nil} = FerricStore.flow_get(same_new_id, partition_key: same_b)
    assert {:ok, %{id: ^other_new_id}} = FerricStore.flow_get(other_new_id, partition_key: other)
    assert {:ok, []} = FerricStore.flow_history(same_new_id, partition_key: same_b)

    assert {:ok, other_history} = FerricStore.flow_history(other_new_id, partition_key: other)
    assert Enum.map(other_history, fn {_id, fields} -> fields["event"] end) == ["created"]
  end

  test "flow_create emits telemetry without automatic worker pubsub wakeups" do
    id = uid("flow-observe")
    attach_flow_telemetry([[:ferricstore, :flow, :create, :stop]])

    changed_channel = "flow_changed:#{id}"
    due_channel = "flow_due:observability"

    :ok = Ferricstore.PubSub.subscribe(changed_channel, self())
    :ok = Ferricstore.PubSub.subscribe(due_channel, self())

    on_exit(fn ->
      Ferricstore.PubSub.unsubscribe(changed_channel, self())
      Ferricstore.PubSub.unsubscribe(due_channel, self())
    end)

    assert {:ok, %{id: ^id}} =
             FerricStore.flow_create(id,
               type: "observability",
               state: "queued",
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert_receive {:flow_telemetry, [:ferricstore, :flow, :create, :stop], measurements,
                    metadata}

    assert %{duration_ms: duration_ms, count: 1} = measurements
    assert is_integer(duration_ms) and duration_ms >= 0
    assert %{flow_id: ^id, flow_type: "observability", result: :ok, reason: nil} = metadata

    refute_receive {:pubsub_message, ^changed_channel, _message}, 50
    refute_receive {:pubsub_message, ^due_channel, _message}, 50
  end

  test "flow APIs reject malformed inputs before raft apply" do
    assert {:error, "ERR flow id must be a non-empty string"} =
             FerricStore.flow_create("", type: "checkout")

    assert {:error, "ERR flow opts must be a keyword list"} =
             FerricStore.flow_create("bad-opts", ["checkout"])

    assert {:error, "ERR flow type is required"} =
             FerricStore.flow_create("missing-type", state: "queued")

    assert {:error, "ERR flow type must be a non-empty string"} =
             FerricStore.flow_create("empty-type", type: "")

    assert {:error, "ERR flow now_ms must be a non-negative integer"} =
             FerricStore.flow_create("bad-now", type: "checkout", now_ms: -1)

    assert {:error, "ERR flow run_at_ms must be a non-negative integer"} =
             FerricStore.flow_create("bad-run-at", type: "checkout", run_at_ms: -1)

    assert {:error, "ERR flow priority must be between 0 and 2"} =
             FerricStore.flow_create("bad-priority", type: "checkout", priority: 3)

    assert {:error, "ERR flow partition_key must be a non-empty string or :global"} =
             FerricStore.flow_create("bad-partition", type: "checkout", partition_key: "")

    assert {:error, "ERR flow opts must be a keyword list"} =
             FerricStore.flow_get("bad-get", ["bad"])

    assert {:error, "ERR flow type must be a non-empty string"} =
             FerricStore.flow_claim_due("", worker: "worker-a")

    assert {:error, "ERR flow worker is required"} =
             FerricStore.flow_claim_due("email", [])

    assert {:error, "ERR flow lease_ms must be a positive integer"} =
             FerricStore.flow_claim_due("email", worker: "worker-a", lease_ms: 0)

    assert {:error, "ERR flow limit must be a positive integer"} =
             FerricStore.flow_claim_due("email", worker: "worker-a", limit: 0)

    assert {:error, "ERR flow lease_token must be a non-empty string"} =
             FerricStore.flow_complete("flow", "")

    assert {:error, "ERR flow fencing_token is required"} =
             FerricStore.flow_complete("flow", "token")

    assert {:error, "ERR flow now_ms must be a non-negative integer"} =
             FerricStore.flow_retry("flow", "token", fencing_token: 0, now_ms: -1)

    assert {:error, "ERR flow id must be a non-empty string"} =
             FerricStore.flow_history("")

    assert {:error, "ERR flow opts must be a keyword list"} =
             FerricStore.flow_history("flow", ["bad"])

    assert {:error, "ERR flow count must be a positive integer"} =
             FerricStore.flow_history("flow", count: 0)

    assert {:error, "ERR flow count exceeds maximum 10000"} =
             FerricStore.flow_history("flow", count: 10_001)

    assert {:error, "ERR flow id must be a non-empty string"} =
             FerricStore.flow_transition("", "queued", "done")

    assert {:error, "ERR flow from must be a non-empty string"} =
             FerricStore.flow_transition("flow", "", "done")

    assert {:error, "ERR flow to must be a non-empty string"} =
             FerricStore.flow_transition("flow", "queued", "")

    assert {:error, "ERR flow opts must be a keyword list"} =
             FerricStore.flow_transition("flow", "queued", "done", ["bad"])

    assert {:error, "ERR flow lease_token must be a non-empty string"} =
             FerricStore.flow_transition("flow", "queued", "done", lease_token: "")

    assert {:error, "ERR flow fencing_token is required"} =
             FerricStore.flow_transition("flow", "queued", "done")

    assert {:error, "ERR flow lease_token must be a non-empty string"} =
             FerricStore.flow_fail("flow", "")

    assert {:error, "ERR flow fencing_token is required"} =
             FerricStore.flow_fail("flow", "token")

    assert {:error, "ERR flow now_ms must be a non-negative integer"} =
             FerricStore.flow_fail("flow", "token", fencing_token: 0, now_ms: -1)

    assert {:error, "ERR flow id must be a non-empty string"} =
             FerricStore.flow_cancel("")

    assert {:error, "ERR flow opts must be a keyword list"} =
             FerricStore.flow_cancel("flow", ["bad"])

    assert {:error, "ERR flow fencing_token is required"} =
             FerricStore.flow_cancel("flow")

    assert {:error, "ERR flow partition_key must be a non-empty string or :global"} =
             FerricStore.flow_claim_due("email", worker: "worker-a", partition_key: "")

    large_id = String.duplicate("x", 65_536)

    assert {:error, "ERR key too large" <> _} =
             FerricStore.flow_create(large_id, type: "checkout")

    huge_ref = String.duplicate("p", 4_097)

    assert {:error, "ERR flow payload_ref too large" <> _} =
             FerricStore.flow_create("huge-payload-ref", type: "checkout", payload_ref: huge_ref)

    assert {:error, "ERR flow result_ref too large" <> _} =
             FerricStore.flow_complete("flow", "token", fencing_token: 0, result_ref: huge_ref)

    assert {:error, "ERR flow error_ref too large" <> _} =
             FerricStore.flow_retry("flow", "token", fencing_token: 0, error_ref: huge_ref)

    assert {:error, "ERR flow reason_ref too large" <> _} =
             FerricStore.flow_cancel("flow", fencing_token: 0, reason_ref: huge_ref)

    assert {:error, "ERR flow reason_ref too large" <> _} =
             FerricStore.flow_rewind("flow", to_event: "1-1", reason_ref: huge_ref)
  end

  test "flow_claim_due atomically leases due flows and removes them from due set" do
    id = uid("flow-claim")

    assert {:ok, _} =
             FerricStore.flow_create(id,
               type: "email",
               state: "queued",
               payload_ref: "payload:" <> id,
               run_at_ms: 1_000
             )

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due("email",
               state: "queued",
               worker: "worker-a",
               lease_ms: 30_000,
               limit: 10,
               now_ms: 1_000
             )

    assert claimed.id == id
    assert claimed.state == "running"
    assert claimed.lease_owner == "worker-a"
    assert is_binary(claimed.lease_token)
    assert claimed.fencing_token == 1
    assert claimed.version == 2

    assert {:ok, []} =
             FerricStore.flow_claim_due("email",
               state: "queued",
               worker: "worker-b",
               lease_ms: 30_000,
               limit: 10,
               now_ms: 1_000
             )
  end

  test "flow_claim_due leases large batches without duplicates and drains due members" do
    type = uid("flow-claim-large")
    ids = for i <- 1..100, do: "#{type}:#{i}"

    for id <- ids do
      assert {:ok, _} =
               FerricStore.flow_create(id,
                 type: type,
                 state: "queued",
                 payload_ref: "payload:" <> id,
                 run_at_ms: 1_000,
                 now_ms: 1_000
               )
    end

    assert {:ok, claimed} =
             FerricStore.flow_claim_due(type,
               state: "queued",
               worker: "worker-large",
               lease_ms: 30_000,
               limit: 100,
               now_ms: 1_000
             )

    claimed_ids = Enum.map(claimed, & &1.id)
    assert length(claimed_ids) == 100
    assert MapSet.new(claimed_ids) == MapSet.new(ids)
    assert Enum.all?(claimed, &(&1.state == "running"))
    assert Enum.all?(claimed, &(&1.lease_owner == "worker-large"))

    assert {:ok, []} =
             FerricStore.flow_claim_due(type,
               state: "queued",
               worker: "worker-large-2",
               lease_ms: 30_000,
               limit: 100,
               now_ms: 1_000
             )
  end

  test "partition_key keeps related flow keys on one shard and can spread partitions" do
    {partition_a, partition_b} = different_partition_keys()
    id = uid("flow-partition-keys")

    state_a = Ferricstore.Flow.Keys.state_key(id, partition_a)
    history_a = Ferricstore.Flow.Keys.history_key(id, partition_a)
    due_a = Ferricstore.Flow.Keys.due_key("email", "queued", 0, partition_a)
    state_index_a = Ferricstore.Flow.Keys.state_index_key("email", "queued", partition_a)
    inflight_index_a = Ferricstore.Flow.Keys.inflight_index_key("email", partition_a)
    worker_index_a = Ferricstore.Flow.Keys.worker_index_key("worker-a", partition_a)
    state_b = Ferricstore.Flow.Keys.state_key(id, partition_b)

    assert shard_for(state_a) == shard_for(history_a)
    assert shard_for(state_a) == shard_for(due_a)
    assert shard_for(state_a) == shard_for(state_index_a)
    assert shard_for(state_a) == shard_for(inflight_index_a)
    assert shard_for(state_a) == shard_for(worker_index_a)
    assert shard_for(state_a) != shard_for(state_b)
  end

  test "flow lifecycle maintains state, inflight, and worker indexes" do
    id = uid("flow-index")
    type = "indexed"
    worker = "worker-index"
    queued_index = Ferricstore.Flow.Keys.state_index_key(type, "queued")
    running_index = Ferricstore.Flow.Keys.state_index_key(type, "running")
    completed_index = Ferricstore.Flow.Keys.state_index_key(type, "completed")
    inflight_index = Ferricstore.Flow.Keys.inflight_index_key(type)
    worker_index = Ferricstore.Flow.Keys.worker_index_key(worker)
    ctx = FerricStore.Instance.get(:default)

    range_ids = fn index_key ->
      shard_index = Ferricstore.Store.Router.shard_for(ctx, index_key)

      {flow_index, _flow_lookup} =
        Ferricstore.Flow.OrderedIndex.table_names(ctx.name, shard_index)

      flow_index
      |> Ferricstore.Flow.OrderedIndex.range_slice(index_key, :neg_inf, :inf, false, 0, :all)
      |> Enum.map(fn {member, _score} -> member end)
    end

    assert {:ok, _} = FerricStore.flow_create(id, type: type, run_at_ms: 1_000)
    assert [^id] = range_ids.(queued_index)

    assert {:ok, [first_claim]} =
             FerricStore.flow_claim_due(type,
               worker: worker,
               lease_ms: 30_000,
               limit: 1,
               now_ms: 1_000
             )

    assert [] = range_ids.(queued_index)
    assert [^id] = range_ids.(running_index)
    assert [^id] = range_ids.(inflight_index)
    assert [^id] = range_ids.(worker_index)

    assert {:ok, _retried} =
             FerricStore.flow_retry(id, first_claim.lease_token,
               fencing_token: first_claim.fencing_token,
               run_at_ms: 2_000
             )

    assert [^id] = range_ids.(queued_index)
    assert [] = range_ids.(running_index)
    assert [] = range_ids.(inflight_index)
    assert [] = range_ids.(worker_index)

    assert {:ok, [second_claim]} =
             FerricStore.flow_claim_due(type,
               worker: worker,
               lease_ms: 30_000,
               limit: 1,
               now_ms: 2_000
             )

    assert {:ok, _completed} =
             FerricStore.flow_complete(id, second_claim.lease_token,
               fencing_token: second_claim.fencing_token
             )

    assert [] = range_ids.(running_index)
    assert [] = range_ids.(inflight_index)
    assert [] = range_ids.(worker_index)
    assert [^id] = range_ids.(completed_index)
  end

  test "flow_list, flow_info, and flow_stuck read lifecycle indexes" do
    due_id = uid("flow-list-due")
    running_id = uid("flow-list-running")
    done_id = uid("flow-list-done")
    type = "ops"

    assert {:ok, _} = FerricStore.flow_create(due_id, type: type, run_at_ms: 2_000)
    assert {:ok, _} = FerricStore.flow_create(running_id, type: type, run_at_ms: 1_000)
    assert {:ok, _} = FerricStore.flow_create(done_id, type: type, run_at_ms: 1_000)

    assert {:ok, [claimed_running, claimed_done]} =
             FerricStore.flow_claim_due(type,
               worker: "worker-ops",
               lease_ms: 50,
               limit: 2,
               now_ms: 1_000
             )

    assert {:ok, _} =
             FerricStore.flow_complete(claimed_done.id, claimed_done.lease_token,
               fencing_token: claimed_done.fencing_token
             )

    assert {:ok, queued} = FerricStore.flow_list(type, state: "queued", count: 10)
    assert Enum.map(queued, & &1.id) == [due_id]

    assert {:ok, running} = FerricStore.flow_list(type, state: "running", count: 10)
    assert Enum.map(running, & &1.id) == [claimed_running.id]

    assert {:ok, completed} = FerricStore.flow_list(type, state: "completed", count: 10)
    assert Enum.map(completed, & &1.id) == [claimed_done.id]

    assert {:ok, info} = FerricStore.flow_info(type)
    assert info.queued == 1
    assert info.running == 1
    assert info.completed == 1
    assert info.inflight == 1

    assert {:ok, stuck} =
             FerricStore.flow_stuck(type,
               older_than_ms: 0,
               count: 10,
               now_ms: 1_051
             )

    assert Enum.map(stuck, & &1.id) == [claimed_running.id]
  end

  test "flow_info counts pending terminal records before LMDB writer flush" do
    old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)
    old_flush_interval = Application.get_env(:ferricstore, :flow_lmdb_flush_interval_ms)
    old_max_batch_ops = Application.get_env(:ferricstore, :flow_lmdb_max_batch_ops)

    Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)
    Application.put_env(:ferricstore, :flow_lmdb_flush_interval_ms, 60_000)
    Application.put_env(:ferricstore, :flow_lmdb_max_batch_ops, 1_000_000)

    ctx = FerricStore.Instance.get(:default)
    partition = uid("tenant-info-pending")
    type = uid("info-pending")
    id = uid("flow-info-pending")

    on_exit(fn ->
      restore_env(:flow_lmdb_mode, old_mode)
      restore_env(:flow_lmdb_flush_interval_ms, old_flush_interval)
      restore_env(:flow_lmdb_max_batch_ops, old_max_batch_ops)
      Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, ctx.shard_count)
    end)

    assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, ctx.shard_count)

    assert {:ok, _} =
             FerricStore.flow_create(id,
               type: type,
               partition_key: partition,
               run_at_ms: 1,
               now_ms: 1
             )

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due(type,
               worker: "worker-info-pending",
               lease_ms: 30_000,
               limit: 1,
               now_ms: 2,
               partition_key: partition
             )

    assert {:ok, _completed} =
             FerricStore.flow_complete(claimed.id, claimed.lease_token,
               fencing_token: claimed.fencing_token,
               now_ms: 3,
               partition_key: partition
             )

    assert {:ok, info} = FerricStore.flow_info(type, partition_key: partition)
    assert info.completed == 1
  end

  test "flow_info does not write zero LMDB terminal counters for active-only types" do
    old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)
    Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)

    ctx = FerricStore.Instance.get(:default)
    partition = uid("tenant-info-zero")
    type = uid("info-zero")
    id = uid("flow-info-zero")

    on_exit(fn ->
      restore_env(:flow_lmdb_mode, old_mode)
    end)

    assert {:ok, _} =
             FerricStore.flow_create(id,
               type: type,
               partition_key: partition,
               run_at_ms: 1,
               now_ms: 1
             )

    completed_index_key = Ferricstore.Flow.Keys.state_index_key(type, "completed", partition)
    shard_index = Ferricstore.Store.Router.shard_for(ctx, completed_index_key)

    lmdb_path =
      ctx.data_dir
      |> Ferricstore.DataDir.shard_data_path(shard_index)
      |> Ferricstore.Flow.LMDB.path()

    count_key = Ferricstore.Flow.LMDB.terminal_count_key(completed_index_key)

    assert :not_found = Ferricstore.Flow.LMDB.get(lmdb_path, count_key)
    assert {:ok, info} = FerricStore.flow_info(type, partition_key: partition)
    assert info.completed == 0
    assert :not_found = Ferricstore.Flow.LMDB.get(lmdb_path, count_key)
  end

  test "flow_info does not build empty terminal score indexes for active-only types" do
    ctx = FerricStore.Instance.get(:default)
    partition = uid("tenant-info-empty-index")
    type = uid("info-empty-index")
    id = uid("flow-info-empty-index")

    assert {:ok, _} =
             FerricStore.flow_create(id,
               type: type,
               partition_key: partition,
               run_at_ms: 1,
               now_ms: 1
             )

    completed_index_key = Ferricstore.Flow.Keys.state_index_key(type, "completed", partition)
    shard_index = Ferricstore.Store.Router.shard_for(ctx, completed_index_key)

    {_index_table, lookup_table} =
      Ferricstore.Store.Shard.ZSetIndex.table_names(ctx.name, shard_index)

    refute :ets.member(lookup_table, {:ready, completed_index_key})
    assert {:ok, info} = FerricStore.flow_info(type, partition_key: partition)
    assert info.completed == 0
    refute :ets.member(lookup_table, {:ready, completed_index_key})
  end

  test "Flow native due index mirrors create and claim_due" do
    ctx = FerricStore.Instance.get(:default)
    partition = uid("tenant-native-index")
    type = uid("native-index")
    id = uid("flow-native-index")

    assert {:ok, _} =
             FerricStore.flow_create(id,
               type: type,
               partition_key: partition,
               run_at_ms: 1_000,
               now_ms: 900
             )

    due_key = Ferricstore.Flow.Keys.due_key(type, "queued", 0, partition)
    shard_index = Ferricstore.Store.Router.shard_for(ctx, due_key)
    {flow_index, flow_lookup} = Ferricstore.Flow.OrderedIndex.table_names(ctx.name, shard_index)

    {zset_index, zset_lookup} =
      Ferricstore.Store.Shard.ZSetIndex.table_names(ctx.name, shard_index)

    assert [{^id, 1000.0}] =
             Ferricstore.Flow.OrderedIndex.range_slice(
               flow_index,
               due_key,
               :neg_inf,
               {:inclusive, 1_000.0},
               false,
               0,
               10
             )

    assert 0 =
             Ferricstore.Store.Shard.ZSetIndex.count(
               zset_index,
               zset_lookup,
               due_key,
               :neg_inf,
               :inf
             )

    assert {:ok, [%{id: ^id, state: "running"}]} =
             FerricStore.flow_claim_due(type,
               partition_key: partition,
               worker: "worker-native-index",
               limit: 1,
               now_ms: 1_000,
               lease_ms: 30_000
             )

    assert [] =
             Ferricstore.Flow.OrderedIndex.range_slice(
               flow_index,
               due_key,
               :neg_inf,
               {:inclusive, 1_000.0},
               false,
               0,
               10
             )

    inflight_key = Ferricstore.Flow.Keys.inflight_index_key(type, partition)
    assert 1 = Ferricstore.Flow.OrderedIndex.count_all(flow_lookup, inflight_key)
  end

  test "partition_key scopes claim, complete, retry, get, and history" do
    partition = uid("tenant")
    id = uid("flow-partition")

    assert {:ok, flow} =
             FerricStore.flow_create(id,
               type: "email",
               partition_key: partition,
               state: "queued",
               run_at_ms: 1_000,
               now_ms: 999
             )

    assert flow.partition_key == partition

    assert {:ok, nil} = FerricStore.flow_get(id)
    assert {:ok, fetched} = FerricStore.flow_get(id, partition_key: partition)
    assert fetched.id == id

    assert {:ok, []} =
             FerricStore.flow_claim_due("email",
               state: "queued",
               worker: "worker-a",
               lease_ms: 30_000,
               limit: 1,
               now_ms: 1_000
             )

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due("email",
               partition_key: partition,
               state: "queued",
               worker: "worker-a",
               lease_ms: 30_000,
               limit: 1,
               now_ms: 1_000
             )

    assert claimed.partition_key == partition

    assert {:error, "ERR flow not found"} =
             FerricStore.flow_complete(id, claimed.lease_token,
               fencing_token: claimed.fencing_token
             )

    assert {:ok, completed} =
             FerricStore.flow_complete(id, claimed.lease_token,
               fencing_token: claimed.fencing_token,
               partition_key: partition
             )

    assert completed.state == "completed"

    assert {:ok, events} = FerricStore.flow_history(id, partition_key: partition)

    assert Enum.map(events, fn {_id, fields} -> fields["event"] end) == [
             "created",
             "claimed",
             "completed"
           ]
  end

  test "flow_claim_due only scans the selected partition" do
    partition_a = uid("tenant-a")
    partition_b = uid("tenant-b")
    id_a = uid("flow-partition-claim-a")
    id_b = uid("flow-partition-claim-b")

    assert {:ok, _} =
             FerricStore.flow_create(id_a,
               type: "email",
               partition_key: partition_a,
               run_at_ms: 1_000
             )

    assert {:ok, _} =
             FerricStore.flow_create(id_b,
               type: "email",
               partition_key: partition_b,
               run_at_ms: 1_000
             )

    assert {:ok, [claimed_a]} =
             FerricStore.flow_claim_due("email",
               partition_key: partition_a,
               worker: "worker-a",
               lease_ms: 30_000,
               limit: 10,
               now_ms: 1_000
             )

    assert claimed_a.id == id_a
    assert claimed_a.partition_key == partition_a

    assert {:ok, []} =
             FerricStore.flow_claim_due("email",
               partition_key: partition_a,
               worker: "worker-a",
               lease_ms: 30_000,
               limit: 10,
               now_ms: 1_000
             )

    assert {:ok, [claimed_b]} =
             FerricStore.flow_claim_due("email",
               partition_key: partition_b,
               worker: "worker-b",
               lease_ms: 30_000,
               limit: 10,
               now_ms: 1_000
             )

    assert claimed_b.id == id_b
    assert claimed_b.partition_key == partition_b
  end

  test "flow_claim_due skips stale due index members without starving live work" do
    stale_id = "a-" <> uid("flow-stale-due")
    live_id = "z-" <> uid("flow-live-due")

    assert {:ok, _} =
             FerricStore.flow_create(stale_id, type: "stale-scan", run_at_ms: 1_000)

    assert {:ok, _} =
             FerricStore.flow_create(live_id, type: "stale-scan", run_at_ms: 1_000)

    assert {:ok, 1} = FerricStore.del(Ferricstore.Flow.Keys.state_key(stale_id))

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due("stale-scan",
               worker: "worker-a",
               lease_ms: 30_000,
               limit: 1,
               now_ms: 1_000
             )

    assert claimed.id == live_id
  end

  test "flow_claim_due drains higher priorities before lower priorities by default" do
    low_id = uid("flow-low-priority")
    high_id = uid("flow-high-priority")

    assert {:ok, _} =
             FerricStore.flow_create(low_id,
               type: "priority",
               priority: 0,
               run_at_ms: 1_000
             )

    assert {:ok, _} =
             FerricStore.flow_create(high_id,
               type: "priority",
               priority: 2,
               run_at_ms: 1_000
             )

    assert {:ok, [high]} =
             FerricStore.flow_claim_due("priority",
               worker: "worker-priority",
               lease_ms: 30_000,
               limit: 1,
               now_ms: 1_000
             )

    assert high.id == high_id

    assert {:ok, [low]} =
             FerricStore.flow_claim_due("priority",
               worker: "worker-priority",
               lease_ms: 30_000,
               limit: 1,
               now_ms: 1_000
             )

    assert low.id == low_id
  end

  test "flow_claim_due priority option targets one priority band" do
    low_id = uid("flow-low-priority-target")
    high_id = uid("flow-high-priority-target")

    assert {:ok, _} =
             FerricStore.flow_create(low_id,
               type: "priority-target",
               priority: 0,
               run_at_ms: 1_000
             )

    assert {:ok, _} =
             FerricStore.flow_create(high_id,
               type: "priority-target",
               priority: 2,
               run_at_ms: 1_000
             )

    assert {:ok, [low]} =
             FerricStore.flow_claim_due("priority-target",
               worker: "worker-priority",
               lease_ms: 30_000,
               limit: 1,
               priority: 0,
               now_ms: 1_000
             )

    assert low.id == low_id

    assert {:ok, [high]} =
             FerricStore.flow_claim_due("priority-target",
               worker: "worker-priority",
               lease_ms: 30_000,
               limit: 1,
               priority: 2,
               now_ms: 1_000
             )

    assert high.id == high_id
  end

  test "flow_complete enforces lease token guard and writes terminal state" do
    id = uid("flow-complete")

    assert {:ok, _} =
             FerricStore.flow_create(id, type: "image", state: "queued", run_at_ms: 1_000)

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due("image",
               state: "queued",
               worker: "worker-a",
               lease_ms: 30_000,
               limit: 1,
               now_ms: 1_000
             )

    assert {:error, "ERR stale flow lease"} =
             FerricStore.flow_complete(id, "wrong-token",
               fencing_token: claimed.fencing_token,
               result_ref: "result:" <> id
             )

    assert {:error, "ERR stale flow lease"} =
             FerricStore.flow_complete(id, claimed.lease_token,
               fencing_token: claimed.fencing_token + 1,
               result_ref: "result:" <> id
             )

    assert {:ok, completed} =
             FerricStore.flow_complete(claimed.id, claimed.lease_token,
               fencing_token: claimed.fencing_token,
               result_ref: "result:" <> id
             )

    assert completed.state == "completed"
    assert completed.result_ref == "result:" <> id
    assert completed.lease_token == nil
    assert completed.version == 3
  end

  test "flow_retry clears lease and reschedules flow" do
    id = uid("flow-retry")

    assert {:ok, _} =
             FerricStore.flow_create(id, type: "webhook", state: "queued", run_at_ms: 1_000)

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due("webhook",
               state: "queued",
               worker: "worker-a",
               lease_ms: 30_000,
               limit: 1,
               now_ms: 1_000
             )

    assert {:error, "ERR stale flow lease"} =
             FerricStore.flow_retry(claimed.id, claimed.lease_token,
               fencing_token: claimed.fencing_token + 1,
               error_ref: "error:" <> id,
               run_at_ms: 2_000
             )

    assert {:ok, retried} =
             FerricStore.flow_retry(claimed.id, claimed.lease_token,
               fencing_token: claimed.fencing_token,
               error_ref: "error:" <> id,
               run_at_ms: 2_000
             )

    assert retried.state == "queued"
    assert retried.attempts == 1
    assert retried.error_ref == "error:" <> id
    assert retried.lease_token == nil

    assert {:ok, [reclaimed]} =
             FerricStore.flow_claim_due("webhook",
               state: "queued",
               worker: "worker-b",
               lease_ms: 30_000,
               limit: 1,
               now_ms: 2_000
             )

    assert reclaimed.id == id
    assert reclaimed.lease_owner == "worker-b"
  end

  test "flow_retry returns to claimed run_state with computed backoff" do
    id = uid("flow-retry-run-state")

    assert {:ok, _} =
             FerricStore.flow_create(id,
               type: "payment",
               state: "charge_card",
               run_at_ms: 1_000
             )

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due("payment",
               state: "charge_card",
               worker: "worker-charge",
               limit: 1,
               lease_ms: 30_000,
               now_ms: 1_000
             )

    assert claimed.state == "running"
    assert claimed.run_state == "charge_card"

    assert {:ok, retried} =
             FerricStore.flow_retry(claimed.id, claimed.lease_token,
               fencing_token: claimed.fencing_token,
               now_ms: 2_000,
               retry: [
                 max_attempts: 3,
                 backoff: [kind: :exponential, base_ms: 1_000, max_ms: 30_000, jitter_pct: 0],
                 exhausted_to: "payment_failed"
               ]
             )

    assert retried.state == "charge_card"
    assert retried.attempts == 1
    assert retried.next_run_at_ms == 3_000
    assert retried.lease_token == nil

    assert {:ok, []} =
             FerricStore.flow_claim_due("payment",
               state: "charge_card",
               worker: "worker-charge-b",
               limit: 1,
               now_ms: 2_999
             )

    assert {:ok, [reclaimed]} =
             FerricStore.flow_claim_due("payment",
               state: "charge_card",
               worker: "worker-charge-b",
               limit: 1,
               now_ms: 3_000
             )

    assert reclaimed.id == id
    assert reclaimed.run_state == "charge_card"
  end

  test "flow_retry exhausts to configured active state" do
    id = uid("flow-retry-exhaust")

    assert {:ok, _} =
             FerricStore.flow_create(id,
               type: "payment-exhaust",
               state: "charge_card",
               run_at_ms: 1_000
             )

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due("payment-exhaust",
               state: "charge_card",
               worker: "worker-charge",
               limit: 1,
               now_ms: 1_000
             )

    assert {:ok, exhausted} =
             FerricStore.flow_retry(claimed.id, claimed.lease_token,
               fencing_token: claimed.fencing_token,
               now_ms: 2_000,
               retry: [
                 max_attempts: 0,
                 backoff: [kind: :fixed, base_ms: 10_000, max_ms: 10_000, jitter_pct: 0],
                 exhausted_to: "payment_failed"
               ]
             )

    assert exhausted.state == "payment_failed"
    assert exhausted.attempts == 1
    assert exhausted.next_run_at_ms == 2_000
    assert exhausted.lease_token == nil

    assert {:ok, [manual]} =
             FerricStore.flow_claim_due("payment-exhaust",
               state: "payment_failed",
               worker: "worker-manual",
               limit: 1,
               now_ms: 2_000
             )

    assert manual.id == id
  end

  test "flow_retry rejects invalid retry policy" do
    id = uid("flow-retry-policy-invalid")

    assert {:ok, _} =
             FerricStore.flow_create(id, type: "retry-policy-invalid", run_at_ms: 1_000)

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due("retry-policy-invalid",
               worker: "worker-invalid",
               limit: 1,
               now_ms: 1_000
             )

    assert {:error, "ERR flow retry max_attempts must be between 0 and 1000"} =
             FerricStore.flow_retry(claimed.id, claimed.lease_token,
               fencing_token: claimed.fencing_token,
               retry: [max_attempts: 1001]
             )

    assert {:error, "ERR flow retry exhausted_to cannot be running"} =
             FerricStore.flow_retry(claimed.id, claimed.lease_token,
               fencing_token: claimed.fencing_token,
               retry: [exhausted_to: "running"]
             )
  end

  test "flow retry policy accepts thirty day backoff cap" do
    assert {:ok, policy} =
             Ferricstore.Flow.RetryPolicy.normalize(
               max_attempts: 1000,
               backoff: [
                 kind: :exponential,
                 base_ms: 2_592_000_000,
                 max_ms: 2_592_000_000,
                 jitter_pct: 100
               ],
               exhausted_to: "needs_review"
             )

    assert policy.backoff.base_ms == 2_592_000_000
    assert policy.backoff.max_ms == 2_592_000_000
  end

  test "flow_retry_many atomically reschedules one-partition batch" do
    partition = uid("tenant-retry-many")
    type = uid("bulk-retry-many")
    id_a = uid("retry-many-a")
    id_b = uid("retry-many-b")

    assert {:ok, _} =
             FerricStore.flow_create_many(
               partition,
               [%{id: id_a}, %{id: id_b}],
               type: type,
               state: "queued",
               run_at_ms: 1_000
             )

    assert {:ok, claimed} =
             FerricStore.flow_claim_due(type,
               partition_key: partition,
               worker: "worker-retry",
               limit: 2,
               now_ms: 1_000
             )

    items =
      claimed
      |> Enum.sort_by(& &1.id)
      |> Enum.map(fn record ->
        %{id: record.id, lease_token: record.lease_token, fencing_token: record.fencing_token}
      end)

    assert {:ok, retried} =
             FerricStore.flow_retry_many(partition, items,
               error_ref: "retry-error",
               run_at_ms: 2_000,
               now_ms: 2_000
             )

    assert Enum.map(retried, & &1.id) == Enum.map(items, & &1.id)
    assert Enum.all?(retried, &(&1.state == "queued"))
    assert Enum.all?(retried, &(&1.attempts == 1))
    assert Enum.all?(retried, &(&1.error_ref == "retry-error"))

    assert {:ok, reclaimed} =
             FerricStore.flow_claim_due(type,
               partition_key: partition,
               worker: "worker-retry-b",
               limit: 2,
               now_ms: 2_000
             )

    assert reclaimed |> Enum.map(& &1.id) |> Enum.sort() == [id_a, id_b] |> Enum.sort()
  end

  test "flow_retry_many rolls back when any item fails guard" do
    partition = uid("tenant-retry-many-rollback")
    type = uid("bulk-retry-many-rollback")
    id_a = uid("retry-many-good")
    id_b = uid("retry-many-bad")

    assert {:ok, _} =
             FerricStore.flow_create_many(
               partition,
               [%{id: id_a}, %{id: id_b}],
               type: type,
               state: "queued",
               run_at_ms: 1_000
             )

    assert {:ok, claimed} =
             FerricStore.flow_claim_due(type,
               partition_key: partition,
               worker: "worker-retry",
               limit: 2,
               now_ms: 1_000
             )

    items =
      claimed
      |> Enum.sort_by(& &1.id)
      |> Enum.map(fn record ->
        fencing_token =
          if record.id == id_b, do: record.fencing_token + 1, else: record.fencing_token

        %{id: record.id, lease_token: record.lease_token, fencing_token: fencing_token}
      end)

    assert {:error, "ERR stale flow lease"} =
             FerricStore.flow_retry_many(partition, items, now_ms: 2_000, run_at_ms: 2_000)

    assert {:ok, fetched_a} = FerricStore.flow_get(id_a, partition_key: partition)
    assert {:ok, fetched_b} = FerricStore.flow_get(id_b, partition_key: partition)
    assert fetched_a.state == "running"
    assert fetched_b.state == "running"
    assert fetched_a.version == 2
    assert fetched_b.version == 2
  end

  test "flow_retry_many rejects invalid retry policy before mutating records" do
    partition = uid("tenant-retry-many-policy")
    type = uid("bulk-retry-many-policy")
    id_a = uid("retry-many-policy-a")
    id_b = uid("retry-many-policy-b")

    assert {:ok, _} =
             FerricStore.flow_create_many(
               partition,
               [%{id: id_a}, %{id: id_b}],
               type: type,
               state: "queued",
               run_at_ms: 1_000
             )

    assert {:ok, claimed} =
             FerricStore.flow_claim_due(type,
               partition_key: partition,
               worker: "worker-retry-policy",
               limit: 2,
               now_ms: 1_000
             )

    items =
      Enum.map(claimed, fn record ->
        %{id: record.id, lease_token: record.lease_token, fencing_token: record.fencing_token}
      end)

    assert {:error, "ERR flow retry max_attempts must be between 0 and 1000"} =
             FerricStore.flow_retry_many(partition, items,
               retry: [max_attempts: 1001],
               now_ms: 2_000
             )

    assert {:ok, fetched_a} = FerricStore.flow_get(id_a, partition_key: partition)
    assert {:ok, fetched_b} = FerricStore.flow_get(id_b, partition_key: partition)
    assert fetched_a.state == "running"
    assert fetched_b.state == "running"
    assert fetched_a.attempts == 0
    assert fetched_b.attempts == 0
  end

  test "expired running lease can be reclaimed" do
    id = uid("flow-reclaim")

    assert {:ok, _} =
             FerricStore.flow_create(id, type: "lease", state: "queued", run_at_ms: 1_000)

    assert {:ok, [first]} =
             FerricStore.flow_claim_due("lease",
               state: "queued",
               worker: "worker-a",
               lease_ms: 50,
               limit: 1,
               now_ms: 1_000
             )

    assert first.lease_deadline_ms == 1_050

    assert {:ok, []} =
             FerricStore.flow_claim_due("lease",
               state: "running",
               worker: "worker-b",
               lease_ms: 50,
               limit: 1,
               now_ms: 1_049
             )

    assert {:ok, [second]} =
             FerricStore.flow_claim_due("lease",
               state: "running",
               worker: "worker-b",
               lease_ms: 50,
               limit: 1,
               now_ms: 1_050
             )

    assert second.id == id
    assert second.lease_owner == "worker-b"
    assert second.version == 3
    assert second.lease_token != first.lease_token
  end

  test "flow_reclaim exposes expired running lease reclaim" do
    id = uid("flow-reclaim-api")

    assert {:ok, _} =
             FerricStore.flow_create(id, type: "lease-api", state: "queued", run_at_ms: 1_000)

    assert {:ok, [first]} =
             FerricStore.flow_claim_due("lease-api",
               worker: "worker-a",
               lease_ms: 50,
               limit: 1,
               now_ms: 1_000
             )

    assert {:ok, []} =
             FerricStore.flow_reclaim("lease-api",
               worker: "worker-b",
               lease_ms: 50,
               limit: 1,
               now_ms: 1_049
             )

    assert {:ok, [second]} =
             FerricStore.flow_reclaim("lease-api",
               worker: "worker-b",
               lease_ms: 50,
               limit: 1,
               now_ms: 1_050
             )

    assert second.id == id
    assert second.lease_owner == "worker-b"
    assert second.lease_token != first.lease_token
  end

  test "expired running lease reclaim is partition scoped" do
    partition_a = uid("tenant-reclaim-a")
    partition_b = uid("tenant-reclaim-b")
    type = uid("lease-partition")
    id_a = uid("flow-reclaim-a")
    id_b = uid("flow-reclaim-b")

    assert {:ok, _} =
             FerricStore.flow_create(id_a,
               type: type,
               state: "queued",
               partition_key: partition_a,
               run_at_ms: 1_000
             )

    assert {:ok, _} =
             FerricStore.flow_create(id_b,
               type: type,
               state: "queued",
               partition_key: partition_b,
               run_at_ms: 1_000
             )

    assert {:ok, [first_a]} =
             FerricStore.flow_claim_due(type,
               state: "queued",
               partition_key: partition_a,
               worker: "worker-a",
               lease_ms: 50,
               limit: 1,
               now_ms: 1_000
             )

    assert {:ok, [first_b]} =
             FerricStore.flow_claim_due(type,
               state: "queued",
               partition_key: partition_b,
               worker: "worker-a",
               lease_ms: 50,
               limit: 1,
               now_ms: 1_000
             )

    assert {:ok, [second_a]} =
             FerricStore.flow_claim_due(type,
               state: "running",
               partition_key: partition_a,
               worker: "worker-b",
               lease_ms: 50,
               limit: 10,
               now_ms: 1_050
             )

    assert second_a.id == id_a
    assert second_a.lease_owner == "worker-b"
    assert second_a.lease_token != first_a.lease_token

    assert {:ok, fetched_b} = FerricStore.flow_get(id_b, partition_key: partition_b)
    assert fetched_b.lease_owner == "worker-a"
    assert fetched_b.lease_token == first_b.lease_token

    assert {:ok, []} =
             FerricStore.flow_stuck(type,
               partition_key: partition_a,
               older_than_ms: 0,
               count: 10,
               now_ms: 1_050
             )

    assert {:ok, [stuck_b]} =
             FerricStore.flow_stuck(type,
               partition_key: partition_b,
               older_than_ms: 0,
               count: 10,
               now_ms: 1_050
             )

    assert stuck_b.id == id_b
  end

  test "flow_transition atomically moves state, due index, and history" do
    id = uid("flow-transition")

    assert {:ok, _} =
             FerricStore.flow_create(id,
               type: "checkout",
               state: "payment_pending",
               run_at_ms: 1_000,
               now_ms: 900
             )

    assert {:ok, transitioned} =
             FerricStore.flow_transition(id, "payment_pending", "email_pending",
               fencing_token: 0,
               run_at_ms: 2_000,
               now_ms: 1_100
             )

    assert transitioned.state == "email_pending"
    assert transitioned.next_run_at_ms == 2_000
    assert transitioned.version == 2

    assert {:ok, []} =
             FerricStore.flow_claim_due("checkout",
               state: "payment_pending",
               worker: "worker-a",
               lease_ms: 30_000,
               limit: 1,
               now_ms: 2_000
             )

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due("checkout",
               state: "email_pending",
               worker: "worker-a",
               lease_ms: 30_000,
               limit: 1,
               now_ms: 2_000
             )

    assert claimed.id == id

    assert {:ok, events} = FerricStore.flow_history(id)

    assert Enum.map(events, fn {_id, fields} -> fields["event"] end) == [
             "created",
             "transitioned",
             "claimed"
           ]
  end

  test "flow_transition_many atomically moves one-partition batch" do
    partition = uid("tenant-transition")
    type = uid("bulk-transition")
    id_a = uid("transition-a")
    id_b = uid("transition-b")

    assert {:ok, _} =
             FerricStore.flow_create_many(
               partition,
               [%{id: id_a}, %{id: id_b}],
               type: type,
               state: "queued",
               run_at_ms: 1_000,
               now_ms: 900
             )

    assert {:ok, transitioned} =
             FerricStore.flow_transition_many(
               partition,
               "queued",
               "ready",
               [
                 %{id: id_a, fencing_token: 0},
                 %{id: id_b, fencing_token: 0}
               ],
               run_at_ms: 2_000,
               now_ms: 1_100
             )

    assert Enum.map(transitioned, & &1.id) == [id_a, id_b]
    assert Enum.all?(transitioned, &(&1.state == "ready"))
    assert Enum.all?(transitioned, &(&1.partition_key == partition))

    assert {:ok, []} =
             FerricStore.flow_claim_due(type,
               partition_key: partition,
               state: "queued",
               worker: "worker-a",
               limit: 10,
               now_ms: 2_000
             )

    assert {:ok, claimed} =
             FerricStore.flow_claim_due(type,
               partition_key: partition,
               state: "ready",
               worker: "worker-a",
               limit: 10,
               now_ms: 2_000
             )

    assert claimed |> Enum.map(& &1.id) |> MapSet.new() == MapSet.new([id_a, id_b])
  end

  test "flow_transition_many rolls back when any item fails guard" do
    partition = uid("tenant-transition-rollback")
    type = uid("bulk-transition-rollback")
    id_a = uid("transition-good")
    id_b = uid("transition-bad")

    assert {:ok, _} =
             FerricStore.flow_create_many(
               partition,
               [%{id: id_a}, %{id: id_b}],
               type: type,
               state: "queued",
               run_at_ms: 1_000
             )

    assert {:error, "ERR stale flow lease"} =
             FerricStore.flow_transition_many(
               partition,
               "queued",
               "ready",
               [
                 %{id: id_a, fencing_token: 0},
                 %{id: id_b, fencing_token: 1}
               ],
               run_at_ms: 2_000
             )

    assert {:ok, fetched_a} = FerricStore.flow_get(id_a, partition_key: partition)
    assert {:ok, fetched_b} = FerricStore.flow_get(id_b, partition_key: partition)
    assert fetched_a.state == "queued"
    assert fetched_b.state == "queued"
    assert fetched_a.version == 1
    assert fetched_b.version == 1

    assert {:ok, history_a} = FerricStore.flow_history(id_a, partition_key: partition)
    assert {:ok, history_b} = FerricStore.flow_history(id_b, partition_key: partition)
    assert Enum.map(history_a, fn {_id, fields} -> fields["event"] end) == ["created"]
    assert Enum.map(history_b, fn {_id, fields} -> fields["event"] end) == ["created"]

    assert {:ok, claimed} =
             FerricStore.flow_claim_due(type,
               partition_key: partition,
               state: "queued",
               worker: "worker-a",
               limit: 10,
               now_ms: 1_000
             )

    assert claimed |> Enum.map(& &1.id) |> MapSet.new() == MapSet.new([id_a, id_b])
  end

  test "flow_transition_many spans shards and rolls back failing shard group" do
    {same_a, same_b, other} = mixed_partition_keys()
    type = uid("bulk-mixed-transition")
    bad_id = uid("transition-mixed-bad")
    same_id = uid("transition-mixed-same")
    other_id = uid("transition-mixed-other")

    for {id, partition} <- [{bad_id, same_a}, {same_id, same_b}, {other_id, other}] do
      assert {:ok, _} =
               FerricStore.flow_create(id,
                 type: type,
                 partition_key: partition,
                 state: "queued",
                 run_at_ms: 1_000
               )
    end

    assert {:ok, results} =
             FerricStore.flow_transition_many(
               nil,
               "queued",
               "ready",
               [
                 %{id: bad_id, partition_key: same_a, fencing_token: 1},
                 %{id: same_id, partition_key: same_b, fencing_token: 0},
                 %{id: other_id, partition_key: other, fencing_token: 0}
               ],
               run_at_ms: 2_000,
               now_ms: 1_100
             )

    assert [
             {:error, "ERR stale flow lease"},
             {:error, "ERR stale flow lease"},
             %{id: ^other_id, partition_key: ^other, state: "ready"}
           ] = results

    assert {:ok, %{state: "queued"}} = FerricStore.flow_get(bad_id, partition_key: same_a)
    assert {:ok, %{state: "queued"}} = FerricStore.flow_get(same_id, partition_key: same_b)
    assert {:ok, %{state: "ready"}} = FerricStore.flow_get(other_id, partition_key: other)

    assert {:ok, bad_history} = FerricStore.flow_history(bad_id, partition_key: same_a)
    assert {:ok, same_history} = FerricStore.flow_history(same_id, partition_key: same_b)
    assert {:ok, other_history} = FerricStore.flow_history(other_id, partition_key: other)

    assert Enum.map(bad_history, fn {_id, fields} -> fields["event"] end) == ["created"]
    assert Enum.map(same_history, fn {_id, fields} -> fields["event"] end) == ["created"]

    assert Enum.map(other_history, fn {_id, fields} -> fields["event"] end) == [
             "created",
             "transitioned"
           ]
  end

  test "flow_complete_many atomically completes one-partition batch" do
    partition = uid("tenant-complete-many")
    type = uid("bulk-complete-many")
    id_a = uid("complete-many-a")
    id_b = uid("complete-many-b")

    assert {:ok, _} =
             FerricStore.flow_create_many(
               partition,
               [%{id: id_a}, %{id: id_b}],
               type: type,
               state: "queued",
               run_at_ms: 1_000,
               now_ms: 900
             )

    assert {:ok, claimed} =
             FerricStore.flow_claim_due(type,
               partition_key: partition,
               worker: "worker-complete",
               limit: 2,
               now_ms: 1_000
             )

    items =
      claimed
      |> Enum.sort_by(& &1.id)
      |> Enum.map(fn record ->
        %{id: record.id, lease_token: record.lease_token, fencing_token: record.fencing_token}
      end)

    assert {:ok, completed} =
             FerricStore.flow_complete_many(partition, items,
               result_ref: "result-batch",
               now_ms: 2_000
             )

    assert Enum.map(completed, & &1.id) == Enum.map(items, & &1.id)
    assert Enum.all?(completed, &(&1.state == "completed"))
    assert Enum.all?(completed, &(&1.result_ref == "result-batch"))

    assert {:ok, info} = FerricStore.flow_info(type, partition_key: partition)
    assert info.completed == 2
  end

  test "flow_complete_many rolls back when any item fails guard" do
    partition = uid("tenant-complete-many-rollback")
    type = uid("bulk-complete-many-rollback")
    id_a = uid("complete-many-good")
    id_b = uid("complete-many-bad")

    assert {:ok, _} =
             FerricStore.flow_create_many(
               partition,
               [%{id: id_a}, %{id: id_b}],
               type: type,
               state: "queued",
               run_at_ms: 1_000
             )

    assert {:ok, claimed} =
             FerricStore.flow_claim_due(type,
               partition_key: partition,
               worker: "worker-complete",
               limit: 2,
               now_ms: 1_000
             )

    items =
      claimed
      |> Enum.sort_by(& &1.id)
      |> Enum.map(fn record ->
        fencing_token =
          if record.id == id_b, do: record.fencing_token + 1, else: record.fencing_token

        %{id: record.id, lease_token: record.lease_token, fencing_token: fencing_token}
      end)

    assert {:error, "ERR stale flow lease"} =
             FerricStore.flow_complete_many(partition, items, now_ms: 2_000)

    assert {:ok, fetched_a} = FerricStore.flow_get(id_a, partition_key: partition)
    assert {:ok, fetched_b} = FerricStore.flow_get(id_b, partition_key: partition)
    assert fetched_a.state == "running"
    assert fetched_b.state == "running"
    assert fetched_a.version == 2
    assert fetched_b.version == 2

    assert {:ok, history_a} = FerricStore.flow_history(id_a, partition_key: partition)
    assert {:ok, history_b} = FerricStore.flow_history(id_b, partition_key: partition)
    assert Enum.map(history_a, fn {_id, fields} -> fields["event"] end) == ["created", "claimed"]
    assert Enum.map(history_b, fn {_id, fields} -> fields["event"] end) == ["created", "claimed"]
  end

  test "flow_complete_many spans shards and rolls back failing shard group" do
    {same_a, same_b, other} = mixed_partition_keys()
    type = uid("bulk-mixed-complete")
    bad = create_claimed_flow(uid("complete-mixed-bad"), same_a, type, "worker-complete")
    same = create_claimed_flow(uid("complete-mixed-same"), same_b, type, "worker-complete")
    other_flow = create_claimed_flow(uid("complete-mixed-other"), other, type, "worker-complete")

    assert {:ok, results} =
             FerricStore.flow_complete_many(
               nil,
               [
                 %{
                   id: bad.id,
                   partition_key: same_a,
                   lease_token: bad.lease_token,
                   fencing_token: bad.fencing_token + 1
                 },
                 %{
                   id: same.id,
                   partition_key: same_b,
                   lease_token: same.lease_token,
                   fencing_token: same.fencing_token
                 },
                 %{
                   id: other_flow.id,
                   partition_key: other,
                   lease_token: other_flow.lease_token,
                   fencing_token: other_flow.fencing_token
                 }
               ],
               now_ms: 2_000
             )

    assert [
             {:error, "ERR stale flow lease"},
             {:error, "ERR stale flow lease"},
             %{id: other_id, partition_key: ^other, state: "completed"}
           ] = results

    assert other_id == other_flow.id
    assert {:ok, %{state: "running"}} = FerricStore.flow_get(bad.id, partition_key: same_a)
    assert {:ok, %{state: "running"}} = FerricStore.flow_get(same.id, partition_key: same_b)

    assert {:ok, %{state: "completed"}} =
             FerricStore.flow_get(other_flow.id, partition_key: other)
  end

  test "flow_fail_many atomically fails one-partition batch" do
    partition = uid("tenant-fail-many")
    type = uid("bulk-fail-many")
    id_a = uid("fail-many-a")
    id_b = uid("fail-many-b")

    assert {:ok, _} =
             FerricStore.flow_create_many(
               partition,
               [%{id: id_a}, %{id: id_b}],
               type: type,
               state: "queued",
               run_at_ms: 1_000
             )

    assert {:ok, claimed} =
             FerricStore.flow_claim_due(type,
               partition_key: partition,
               worker: "worker-fail",
               limit: 2,
               now_ms: 1_000
             )

    items =
      claimed
      |> Enum.sort_by(& &1.id)
      |> Enum.map(fn record ->
        %{id: record.id, lease_token: record.lease_token, fencing_token: record.fencing_token}
      end)

    assert {:ok, failed} =
             FerricStore.flow_fail_many(partition, items,
               error_ref: "error-batch",
               now_ms: 2_000
             )

    assert Enum.map(failed, & &1.id) == Enum.map(items, & &1.id)
    assert Enum.all?(failed, &(&1.state == "failed"))
    assert Enum.all?(failed, &(&1.error_ref == "error-batch"))

    assert {:ok, info} = FerricStore.flow_info(type, partition_key: partition)
    assert info.failed == 2
  end

  test "flow_fail_many rolls back when any item fails guard" do
    partition = uid("tenant-fail-many-rollback")
    type = uid("bulk-fail-many-rollback")
    id_a = uid("fail-many-good")
    id_b = uid("fail-many-bad")

    assert {:ok, _} =
             FerricStore.flow_create_many(
               partition,
               [%{id: id_a}, %{id: id_b}],
               type: type,
               state: "queued",
               run_at_ms: 1_000
             )

    assert {:ok, claimed} =
             FerricStore.flow_claim_due(type,
               partition_key: partition,
               worker: "worker-fail",
               limit: 2,
               now_ms: 1_000
             )

    items =
      claimed
      |> Enum.sort_by(& &1.id)
      |> Enum.map(fn record ->
        fencing_token =
          if record.id == id_b, do: record.fencing_token + 1, else: record.fencing_token

        %{id: record.id, lease_token: record.lease_token, fencing_token: fencing_token}
      end)

    assert {:error, "ERR stale flow lease"} =
             FerricStore.flow_fail_many(partition, items, now_ms: 2_000)

    assert {:ok, fetched_a} = FerricStore.flow_get(id_a, partition_key: partition)
    assert {:ok, fetched_b} = FerricStore.flow_get(id_b, partition_key: partition)
    assert fetched_a.state == "running"
    assert fetched_b.state == "running"
    assert fetched_a.version == 2
    assert fetched_b.version == 2
  end

  test "flow_retry_many spans shards and rolls back failing shard group" do
    {same_a, same_b, other} = mixed_partition_keys()
    type = uid("bulk-mixed-retry")
    bad = create_claimed_flow(uid("retry-mixed-bad"), same_a, type, "worker-retry")
    same = create_claimed_flow(uid("retry-mixed-same"), same_b, type, "worker-retry")
    other_flow = create_claimed_flow(uid("retry-mixed-other"), other, type, "worker-retry")

    assert {:ok, results} =
             FerricStore.flow_retry_many(
               nil,
               [
                 %{
                   id: bad.id,
                   partition_key: same_a,
                   lease_token: bad.lease_token,
                   fencing_token: bad.fencing_token + 1
                 },
                 %{
                   id: same.id,
                   partition_key: same_b,
                   lease_token: same.lease_token,
                   fencing_token: same.fencing_token
                 },
                 %{
                   id: other_flow.id,
                   partition_key: other,
                   lease_token: other_flow.lease_token,
                   fencing_token: other_flow.fencing_token
                 }
               ],
               run_at_ms: 2_000,
               now_ms: 2_000
             )

    assert [
             {:error, "ERR stale flow lease"},
             {:error, "ERR stale flow lease"},
             %{id: other_id, partition_key: ^other, state: "queued"}
           ] = results

    assert other_id == other_flow.id
    assert {:ok, %{state: "running"}} = FerricStore.flow_get(bad.id, partition_key: same_a)
    assert {:ok, %{state: "running"}} = FerricStore.flow_get(same.id, partition_key: same_b)
    assert {:ok, %{state: "queued"}} = FerricStore.flow_get(other_flow.id, partition_key: other)
  end

  test "flow_fail_many spans shards and rolls back failing shard group" do
    {same_a, same_b, other} = mixed_partition_keys()
    type = uid("bulk-mixed-fail")
    bad = create_claimed_flow(uid("fail-mixed-bad"), same_a, type, "worker-fail")
    same = create_claimed_flow(uid("fail-mixed-same"), same_b, type, "worker-fail")
    other_flow = create_claimed_flow(uid("fail-mixed-other"), other, type, "worker-fail")

    assert {:ok, results} =
             FerricStore.flow_fail_many(
               nil,
               [
                 %{
                   id: bad.id,
                   partition_key: same_a,
                   lease_token: bad.lease_token,
                   fencing_token: bad.fencing_token + 1
                 },
                 %{
                   id: same.id,
                   partition_key: same_b,
                   lease_token: same.lease_token,
                   fencing_token: same.fencing_token
                 },
                 %{
                   id: other_flow.id,
                   partition_key: other,
                   lease_token: other_flow.lease_token,
                   fencing_token: other_flow.fencing_token
                 }
               ],
               now_ms: 2_000
             )

    assert [
             {:error, "ERR stale flow lease"},
             {:error, "ERR stale flow lease"},
             %{id: other_id, partition_key: ^other, state: "failed"}
           ] = results

    assert other_id == other_flow.id
    assert {:ok, %{state: "running"}} = FerricStore.flow_get(bad.id, partition_key: same_a)
    assert {:ok, %{state: "running"}} = FerricStore.flow_get(same.id, partition_key: same_b)
    assert {:ok, %{state: "failed"}} = FerricStore.flow_get(other_flow.id, partition_key: other)
  end

  test "flow_cancel_many atomically cancels one-partition queued batch" do
    partition = uid("tenant-cancel-many")
    type = uid("bulk-cancel-many")
    id_a = uid("cancel-many-a")
    id_b = uid("cancel-many-b")

    assert {:ok, _} =
             FerricStore.flow_create_many(
               partition,
               [%{id: id_a}, %{id: id_b}],
               type: type,
               state: "queued",
               run_at_ms: 1_000
             )

    items = [
      %{id: id_a, fencing_token: 0},
      %{id: id_b, fencing_token: 0}
    ]

    assert {:ok, cancelled} =
             FerricStore.flow_cancel_many(partition, items,
               reason_ref: "cancel-batch",
               now_ms: 2_000
             )

    assert Enum.map(cancelled, & &1.id) == [id_a, id_b]
    assert Enum.all?(cancelled, &(&1.state == "cancelled"))
    assert Enum.all?(cancelled, &(&1.error_ref == "cancel-batch"))

    assert {:ok, info} = FerricStore.flow_info(type, partition_key: partition)
    assert info.cancelled == 2
  end

  test "flow_cancel_many rolls back when any item fails guard" do
    partition = uid("tenant-cancel-many-rollback")
    type = uid("bulk-cancel-many-rollback")
    id_a = uid("cancel-many-good")
    id_b = uid("cancel-many-bad")

    assert {:ok, _} =
             FerricStore.flow_create_many(
               partition,
               [%{id: id_a}, %{id: id_b}],
               type: type,
               state: "queued",
               run_at_ms: 1_000
             )

    items = [
      %{id: id_a, fencing_token: 0},
      %{id: id_b, fencing_token: 1}
    ]

    assert {:error, "ERR stale flow lease"} =
             FerricStore.flow_cancel_many(partition, items, now_ms: 2_000)

    assert {:ok, fetched_a} = FerricStore.flow_get(id_a, partition_key: partition)
    assert {:ok, fetched_b} = FerricStore.flow_get(id_b, partition_key: partition)
    assert fetched_a.state == "queued"
    assert fetched_b.state == "queued"
    assert fetched_a.version == 1
    assert fetched_b.version == 1
  end

  test "flow_cancel_many spans shards and rolls back failing shard group" do
    {same_a, same_b, other} = mixed_partition_keys()
    type = uid("bulk-mixed-cancel")
    bad_id = uid("cancel-mixed-bad")
    same_id = uid("cancel-mixed-same")
    other_id = uid("cancel-mixed-other")

    for {id, partition} <- [{bad_id, same_a}, {same_id, same_b}, {other_id, other}] do
      assert {:ok, _} =
               FerricStore.flow_create(id,
                 type: type,
                 partition_key: partition,
                 state: "queued",
                 run_at_ms: 1_000
               )
    end

    assert {:ok, results} =
             FerricStore.flow_cancel_many(
               nil,
               [
                 %{id: bad_id, partition_key: same_a, fencing_token: 1},
                 %{id: same_id, partition_key: same_b, fencing_token: 0},
                 %{id: other_id, partition_key: other, fencing_token: 0}
               ],
               now_ms: 2_000
             )

    assert [
             {:error, "ERR stale flow lease"},
             {:error, "ERR stale flow lease"},
             %{id: ^other_id, partition_key: ^other, state: "cancelled"}
           ] = results

    assert {:ok, %{state: "queued"}} = FerricStore.flow_get(bad_id, partition_key: same_a)
    assert {:ok, %{state: "queued"}} = FerricStore.flow_get(same_id, partition_key: same_b)
    assert {:ok, %{state: "cancelled"}} = FerricStore.flow_get(other_id, partition_key: other)
  end

  test "flow_transition enforces expected state and running lease guard" do
    id = uid("flow-transition-guard")

    assert {:ok, _} =
             FerricStore.flow_create(id, type: "checkout", state: "queued", run_at_ms: 1_000)

    assert {:error, "ERR flow wrong state"} =
             FerricStore.flow_transition(id, "running", "completed",
               fencing_token: 0,
               run_at_ms: 1_000
             )

    assert {:ok, fetched} = FerricStore.flow_get(id)
    assert fetched.state == "queued"
    assert fetched.version == 1

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due("checkout",
               state: "queued",
               worker: "worker-a",
               lease_ms: 30_000,
               limit: 1,
               now_ms: 1_000
             )

    assert {:error, "ERR stale flow lease"} =
             FerricStore.flow_transition(id, "running", "next",
               fencing_token: claimed.fencing_token,
               run_at_ms: 2_000
             )

    assert {:error, "ERR stale flow lease"} =
             FerricStore.flow_transition(id, "running", "next",
               lease_token: claimed.lease_token,
               fencing_token: claimed.fencing_token + 1,
               run_at_ms: 2_000
             )

    assert {:ok, transitioned} =
             FerricStore.flow_transition(id, "running", "next",
               lease_token: claimed.lease_token,
               fencing_token: claimed.fencing_token,
               run_at_ms: 2_000
             )

    assert transitioned.state == "next"
    assert transitioned.lease_token == nil
  end

  test "flow_transition rolls back index changes when derived keys are invalid" do
    id = uid("flow-transition-rollback")
    huge_state = String.duplicate("x", 65_536)

    assert {:ok, _} =
             FerricStore.flow_create(id, type: "audit", state: "queued", run_at_ms: 1_000)

    assert {:error, "ERR key too large" <> _} =
             FerricStore.flow_transition(id, "queued", huge_state,
               fencing_token: 0,
               run_at_ms: 2_000,
               now_ms: 1_100
             )

    assert {:ok, fetched} = FerricStore.flow_get(id)
    assert fetched.state == "queued"
    assert fetched.version == 1

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due("audit",
               state: "queued",
               worker: "worker-a",
               lease_ms: 30_000,
               limit: 1,
               now_ms: 1_000
             )

    assert claimed.id == id
  end

  test "flow_fail and flow_cancel write terminal states and remove due work" do
    fail_id = uid("flow-fail")
    cancel_id = uid("flow-cancel")
    fail_type = "jobs-fail"
    cancel_type = "jobs-cancel"

    assert {:ok, _} = FerricStore.flow_create(fail_id, type: fail_type, run_at_ms: 1_000)
    assert {:ok, _} = FerricStore.flow_create(cancel_id, type: cancel_type, run_at_ms: 1_000)

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due(fail_type,
               state: "queued",
               worker: "worker-a",
               lease_ms: 30_000,
               limit: 1,
               now_ms: 1_000
             )

    assert claimed.id == fail_id

    assert {:error, "ERR stale flow lease"} =
             FerricStore.flow_fail(fail_id, claimed.lease_token,
               fencing_token: claimed.fencing_token + 1,
               error_ref: "error:" <> fail_id,
               now_ms: 1_500
             )

    assert {:ok, failed} =
             FerricStore.flow_fail(fail_id, claimed.lease_token,
               fencing_token: claimed.fencing_token,
               error_ref: "error:" <> fail_id,
               now_ms: 1_500
             )

    assert failed.state == "failed"
    assert failed.error_ref == "error:" <> fail_id
    assert failed.lease_token == nil
    assert failed.next_run_at_ms == nil

    assert {:ok, cancelled} =
             FerricStore.flow_cancel(cancel_id,
               fencing_token: 0,
               reason_ref: "reason:" <> cancel_id,
               now_ms: 1_500
             )

    assert cancelled.state == "cancelled"
    assert cancelled.error_ref == "reason:" <> cancel_id
    assert cancelled.next_run_at_ms == nil

    assert {:ok, []} =
             FerricStore.flow_claim_due(fail_type,
               state: "queued",
               worker: "worker-b",
               lease_ms: 30_000,
               limit: 10,
               now_ms: 1_500
             )

    assert {:ok, []} =
             FerricStore.flow_claim_due(cancel_type,
               state: "queued",
               worker: "worker-b",
               lease_ms: 30_000,
               limit: 10,
               now_ms: 1_500
             )
  end

  test "flow_history returns transition events" do
    id = uid("flow-history")

    assert {:ok, _} =
             FerricStore.flow_create(id,
               type: "audit",
               state: "queued",
               run_at_ms: 1_000,
               now_ms: 999
             )

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due("audit",
               state: "queued",
               worker: "worker-a",
               lease_ms: 30_000,
               limit: 1,
               now_ms: 1_000
             )

    assert {:ok, _} =
             FerricStore.flow_complete(id, claimed.lease_token,
               fencing_token: claimed.fencing_token
             )

    assert {:ok, events} = FerricStore.flow_history(id, count: 10)

    assert Enum.map(events, fn {_id, fields} -> fields["event"] end) == [
             "created",
             "claimed",
             "completed"
           ]

    history_key = Ferricstore.Flow.Keys.history_key(id)
    shard = shard_for(history_key)
    {flow_index, flow_lookup} = Ferricstore.Flow.OrderedIndex.table_names(:default, shard)

    assert Ferricstore.Flow.OrderedIndex.count_all(flow_lookup, history_key) == 3

    assert Ferricstore.Flow.OrderedIndex.rank_range(flow_index, history_key, 0, 2, false)
           |> length() ==
             3

    assert [] = :ets.lookup(Ferricstore.Stream.Meta, history_key)
  end

  test "flow_history event ids stay monotonic when claim time is behind record time" do
    id = uid("flow-history-monotonic")

    assert {:ok, _} =
             FerricStore.flow_create(id,
               type: "audit-monotonic",
               state: "queued",
               run_at_ms: 1_000,
               now_ms: 10_000
             )

    assert {:ok, [_claimed]} =
             FerricStore.flow_claim_due("audit-monotonic",
               state: "queued",
               worker: "worker-a",
               lease_ms: 30_000,
               limit: 1,
               now_ms: 2_000
             )

    assert {:ok, events} = FerricStore.flow_history(id, count: 10)

    assert Enum.map(events, fn {event_id, _fields} -> event_id end) == ["10000-1", "10000-2"]

    assert Enum.map(events, fn {_event_id, fields} -> fields["event"] end) == [
             "created",
             "claimed"
           ]
  end

  test "flow_history falls back to bounded history key scan when Flow index is unavailable" do
    ctx = Ferricstore.Test.IsolatedInstance.checkout(shard_count: 1)

    on_exit(fn -> Ferricstore.Test.IsolatedInstance.checkin(ctx) end)

    id = uid("flow-history-fallback")
    partition = "tenant-history-fallback"

    assert {:ok, _record} =
             Ferricstore.Flow.create(ctx, id,
               type: "history-fallback",
               partition_key: partition,
               run_at_ms: 1,
               now_ms: 1
             )

    assert {:ok, [claimed]} =
             Ferricstore.Flow.claim_due(ctx, "history-fallback",
               partition_key: partition,
               worker: "worker-history-fallback",
               limit: 1,
               now_ms: 2
             )

    assert {:ok, _record} =
             Ferricstore.Flow.complete(ctx, id, claimed.lease_token,
               partition_key: partition,
               fencing_token: claimed.fencing_token,
               now_ms: 3
             )

    {flow_index, flow_lookup} = Ferricstore.Flow.OrderedIndex.table_names(ctx.name, 0)
    :ets.delete_all_objects(flow_index)
    :ets.delete_all_objects(flow_lookup)

    assert {:ok, events} = Ferricstore.Flow.history(ctx, id, partition_key: partition, count: 10)

    assert Enum.map(events, fn {_event_id, fields} -> fields["event"] end) == [
             "created",
             "claimed",
             "completed"
           ]
  end

  test "flow history retention keeps only latest configured events" do
    id = uid("flow-history-retention")

    assert {:ok, _} =
             FerricStore.flow_create(id,
               type: "audit-retention",
               run_at_ms: 1_000,
               history_max_events: 2
             )

    assert {:ok, [{created_event_id, _fields}]} = FerricStore.flow_history(id, count: 10)

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due("audit-retention",
               worker: "worker-a",
               lease_ms: 30_000,
               limit: 1,
               now_ms: 1_000
             )

    assert {:ok, _} =
             FerricStore.flow_complete(id, claimed.lease_token,
               fencing_token: claimed.fencing_token
             )

    assert {:ok, events} = FerricStore.flow_history(id, count: 10)
    event_ids = Enum.map(events, fn {event_id, _fields} -> event_id end)

    assert Enum.map(events, fn {_id, fields} -> fields["event"] end) == ["claimed", "completed"]
    refute created_event_id in event_ids

    history_key = Ferricstore.Flow.Keys.history_key(id)
    shard = shard_for(history_key)
    {flow_index, flow_lookup} = Ferricstore.Flow.OrderedIndex.table_names(:default, shard)

    assert Ferricstore.Flow.OrderedIndex.count_all(flow_lookup, history_key) == 2

    assert Ferricstore.Flow.OrderedIndex.rank_range(flow_index, history_key, 0, 10, false)
           |> Enum.map(&elem(&1, 0)) ==
             event_ids

    assert [] = :ets.lookup(Ferricstore.Stream.Meta, history_key)

    assert [] =
             Ferricstore.Flow.OrderedIndex.rank_range(flow_index, history_key, 0, 10, false)
             |> Enum.filter(fn {event_id, _score} -> event_id == created_event_id end)
  end

  test "flow history uses configured default retention when omitted" do
    original = Application.get_env(:ferricstore, :flow_default_history_max_events)
    Application.put_env(:ferricstore, :flow_default_history_max_events, 2)

    on_exit(fn ->
      if is_nil(original) do
        Application.delete_env(:ferricstore, :flow_default_history_max_events)
      else
        Application.put_env(:ferricstore, :flow_default_history_max_events, original)
      end
    end)

    id = uid("flow-history-default-retention")

    assert {:ok, created} =
             FerricStore.flow_create(id,
               type: "audit-default-retention",
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert created.history_max_events == 2

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due("audit-default-retention",
               worker: "worker-a",
               lease_ms: 30_000,
               limit: 1,
               now_ms: 1_100
             )

    assert {:ok, _} =
             FerricStore.flow_complete(id, claimed.lease_token,
               fencing_token: claimed.fencing_token,
               now_ms: 1_200
             )

    assert {:ok, events} = FerricStore.flow_history(id, count: 10)
    assert Enum.map(events, fn {_id, fields} -> fields["event"] end) == ["claimed", "completed"]
  end

  test "flow history retention rejects values above configured maximum" do
    original = Application.get_env(:ferricstore, :flow_max_history_max_events)
    Application.put_env(:ferricstore, :flow_max_history_max_events, 2)

    on_exit(fn ->
      if is_nil(original) do
        Application.delete_env(:ferricstore, :flow_max_history_max_events)
      else
        Application.put_env(:ferricstore, :flow_max_history_max_events, original)
      end
    end)

    assert {:error, "ERR flow history_max_events exceeds maximum 2"} =
             FerricStore.flow_create(uid("flow-history-max"),
               type: "audit-history-max",
               history_max_events: 3
             )
  end

  test "flow create_many and transition_many reject oversized batches" do
    original = Application.get_env(:ferricstore, :flow_max_batch_items)
    Application.put_env(:ferricstore, :flow_max_batch_items, 2)

    on_exit(fn ->
      if is_nil(original) do
        Application.delete_env(:ferricstore, :flow_max_batch_items)
      else
        Application.put_env(:ferricstore, :flow_max_batch_items, original)
      end
    end)

    assert {:error, "ERR flow batch item count exceeds maximum 2"} =
             FerricStore.flow_create_many("tenant-batch-cap", ["a", "b", "c"], type: "batch-cap")

    assert {:error, "ERR flow batch item count exceeds maximum 2"} =
             FerricStore.flow_transition_many(
               "tenant-batch-cap",
               "queued",
               "waiting",
               [
                 %{id: "a", fencing_token: 0},
                 %{id: "b", fencing_token: 0},
                 %{id: "c", fencing_token: 0}
               ]
             )
  end

  test "flow claim_due rejects oversized limit" do
    original = Application.get_env(:ferricstore, :flow_max_claim_limit)
    Application.put_env(:ferricstore, :flow_max_claim_limit, 2)

    on_exit(fn ->
      if is_nil(original) do
        Application.delete_env(:ferricstore, :flow_max_claim_limit)
      else
        Application.put_env(:ferricstore, :flow_max_claim_limit, original)
      end
    end)

    assert {:error, "ERR flow limit exceeds maximum 2"} =
             FerricStore.flow_claim_due("claim-limit-cap",
               worker: "worker-a",
               limit: 3
             )

    assert {:error, "ERR flow limit exceeds maximum 2"} =
             FerricStore.flow_reclaim("claim-limit-cap",
               worker: "worker-a",
               limit: 3
             )
  end

  test "flow_rewind rejects trimmed target event with stale stream index" do
    id = uid("flow-rewind-trimmed")

    assert {:ok, _} =
             FerricStore.flow_create(id,
               type: "rewind-trimmed",
               run_at_ms: 1_000,
               history_max_events: 2,
               now_ms: 1_000
             )

    assert {:ok, [{created_event_id, _fields} | _]} = FerricStore.flow_history(id, count: 10)

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due("rewind-trimmed",
               worker: "worker-a",
               lease_ms: 30_000,
               limit: 1,
               now_ms: 1_000
             )

    assert {:ok, _completed} =
             FerricStore.flow_complete(id, claimed.lease_token,
               fencing_token: claimed.fencing_token,
               now_ms: 2_000
             )

    assert {:ok, events} = FerricStore.flow_history(id, count: 10)
    assert Enum.map(events, fn {_id, fields} -> fields["event"] end) == ["claimed", "completed"]

    assert {:error, "ERR flow rewind target event not found"} =
             FerricStore.flow_rewind(id,
               to_event: created_event_id,
               expect_state: "completed",
               now_ms: 3_000
             )
  end

  test "flow_rewind restores a previous history state and reindexes atomically" do
    id = uid("flow-rewind")

    assert {:ok, _} = FerricStore.flow_create(id, type: "rewind", run_at_ms: 1_000, now_ms: 1_000)

    assert {:ok, [{created_event_id, %{"event" => "created", "state" => "queued"}} | _]} =
             FerricStore.flow_history(id, count: 10)

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due("rewind",
               worker: "worker-a",
               lease_ms: 30_000,
               limit: 1,
               now_ms: 1_000
             )

    assert {:ok, completed} =
             FerricStore.flow_complete(id, claimed.lease_token,
               fencing_token: claimed.fencing_token,
               now_ms: 2_000
             )

    assert completed.state == "completed"
    assert {:ok, %{queued: 0, completed: 1}} = FerricStore.flow_info("rewind")

    assert {:ok, rewound} =
             FerricStore.flow_rewind(id,
               to_event: created_event_id,
               run_at_ms: 5_000,
               expect_state: "completed",
               now_ms: 3_000
             )

    assert rewound.state == "queued"
    assert rewound.next_run_at_ms == 5_000
    assert rewound.lease_token == nil
    assert rewound.lease_owner == nil
    assert rewound.fencing_token == completed.fencing_token + 1

    assert {:ok, %{queued: 1, completed: 0}} = FerricStore.flow_info("rewind")

    assert {:ok, [claimed_again]} =
             FerricStore.flow_claim_due("rewind",
               worker: "worker-b",
               lease_ms: 30_000,
               limit: 1,
               now_ms: 5_000
             )

    assert claimed_again.id == id
    assert claimed_again.state == "running"

    assert {:ok, events} = FerricStore.flow_history(id, count: 10)

    assert Enum.any?(events, fn {_event_id, fields} ->
             fields["event"] == "rewound" and fields["rewound_to_event_id"] == created_event_id
           end)
  end

  test "flow_rewind validates target, expected state, and active leases" do
    id = uid("flow-rewind-guard")

    assert {:ok, _} = FerricStore.flow_create(id, type: "rewind-guard", run_at_ms: 1_000)
    assert {:ok, [{created_event_id, _fields} | _]} = FerricStore.flow_history(id, count: 10)

    assert {:error, "ERR flow wrong state"} =
             FerricStore.flow_rewind(id,
               to_event: created_event_id,
               expect_state: "completed"
             )

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due("rewind-guard",
               worker: "worker-a",
               lease_ms: 30_000,
               limit: 1,
               now_ms: 1_000
             )

    assert {:error, "ERR flow cannot rewind leased flow"} =
             FerricStore.flow_rewind(id, to_event: created_event_id)

    assert {:ok, _} =
             FerricStore.flow_complete(id, claimed.lease_token,
               fencing_token: claimed.fencing_token,
               now_ms: 2_000
             )

    assert {:error, "ERR flow rewind target event not found"} =
             FerricStore.flow_rewind(id, to_event: "999999-0")
  end

  test "terminal ttl expires flow state record" do
    id = uid("flow-terminal-ttl")

    assert {:ok, _} = FerricStore.flow_create(id, type: "ttl", run_at_ms: 1_000)

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due("ttl",
               worker: "worker-a",
               lease_ms: 30_000,
               limit: 1,
               now_ms: 1_000
             )

    assert {:ok, _} =
             FerricStore.flow_complete(id, claimed.lease_token,
               fencing_token: claimed.fencing_token,
               ttl_ms: 20
             )

    Process.sleep(40)

    assert {:ok, nil} = FerricStore.flow_get(id)
  end

  defp restore_env(key, nil), do: Application.delete_env(:ferricstore, key)
  defp restore_env(key, value), do: Application.put_env(:ferricstore, key, value)
end
