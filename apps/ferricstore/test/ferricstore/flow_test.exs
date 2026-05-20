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

  defp flow_create_and_get(id, opts) do
    case FerricStore.flow_create(id, opts) do
      :ok -> FerricStore.flow_get(id, flow_partition_opts(Keyword.get(opts, :partition_key)))
      other -> other
    end
  end

  defp flow_create_many_and_get(partition_key, items, opts) do
    case FerricStore.flow_create_many(partition_key, items, opts) do
      :ok ->
        {:ok, Enum.map(items, &fetch_created_flow(&1, partition_key, opts))}

      {:ok, results} when is_list(results) ->
        {:ok, hydrate_create_results(results, items, partition_key, opts)}

      other ->
        other
    end
  end

  defp flow_spawn_children_and_get(parent_id, children, opts) do
    case FerricStore.flow_spawn_children(parent_id, children, opts) do
      :ok ->
        FerricStore.flow_get(parent_id, flow_partition_opts(Keyword.get(opts, :partition_key)))

      other ->
        other
    end
  end

  defp flow_transition_and_get(id, from_state, to_state, opts \\ []) do
    case FerricStore.flow_transition(id, from_state, to_state, opts) do
      :ok -> FerricStore.flow_get(id, flow_partition_opts(Keyword.get(opts, :partition_key)))
      other -> other
    end
  end

  defp flow_transition_many_and_get(partition_key, from_state, to_state, items, opts \\ []) do
    case FerricStore.flow_transition_many(partition_key, from_state, to_state, items, opts) do
      :ok ->
        {:ok, Enum.map(items, &fetch_many_flow(&1, partition_key))}

      {:ok, results} when is_list(results) ->
        {:ok, hydrate_many_results(results, items, partition_key)}

      other ->
        other
    end
  end

  defp flow_complete_and_get(id, lease_token, opts \\ []) do
    case FerricStore.flow_complete(id, lease_token, opts) do
      :ok -> FerricStore.flow_get(id, flow_partition_opts(Keyword.get(opts, :partition_key)))
      other -> other
    end
  end

  defp flow_complete_many_and_get(partition_key, items, opts) do
    case FerricStore.flow_complete_many(partition_key, items, opts) do
      :ok ->
        {:ok, Enum.map(items, &fetch_many_flow(&1, partition_key))}

      {:ok, results} when is_list(results) ->
        {:ok, hydrate_many_results(results, items, partition_key)}

      other ->
        other
    end
  end

  defp flow_retry_and_get(id, lease_token, opts) do
    case FerricStore.flow_retry(id, lease_token, opts) do
      :ok -> FerricStore.flow_get(id, flow_partition_opts(Keyword.get(opts, :partition_key)))
      other -> other
    end
  end

  defp flow_retry_many_and_get(partition_key, items, opts) do
    case FerricStore.flow_retry_many(partition_key, items, opts) do
      :ok ->
        {:ok, Enum.map(items, &fetch_many_flow(&1, partition_key))}

      {:ok, results} when is_list(results) ->
        {:ok, hydrate_many_results(results, items, partition_key)}

      other ->
        other
    end
  end

  defp flow_fail_and_get(id, lease_token, opts \\ []) do
    case FerricStore.flow_fail(id, lease_token, opts) do
      :ok -> FerricStore.flow_get(id, flow_partition_opts(Keyword.get(opts, :partition_key)))
      other -> other
    end
  end

  defp flow_fail_many_and_get(partition_key, items, opts) do
    case FerricStore.flow_fail_many(partition_key, items, opts) do
      :ok ->
        {:ok, Enum.map(items, &fetch_many_flow(&1, partition_key))}

      {:ok, results} when is_list(results) ->
        {:ok, hydrate_many_results(results, items, partition_key)}

      other ->
        other
    end
  end

  defp flow_cancel_and_get(id, opts \\ []) do
    case FerricStore.flow_cancel(id, opts) do
      :ok -> FerricStore.flow_get(id, flow_partition_opts(Keyword.get(opts, :partition_key)))
      other -> other
    end
  end

  defp flow_cancel_many_and_get(partition_key, items, opts) do
    case FerricStore.flow_cancel_many(partition_key, items, opts) do
      :ok ->
        {:ok, Enum.map(items, &fetch_many_flow(&1, partition_key))}

      {:ok, results} when is_list(results) ->
        {:ok, hydrate_many_results(results, items, partition_key)}

      other ->
        other
    end
  end

  defp impl_flow_create_and_get(ctx, id, opts) do
    case FerricStore.Impl.flow_create(ctx, id, opts) do
      :ok ->
        FerricStore.Impl.flow_get(ctx, id, flow_partition_opts(Keyword.get(opts, :partition_key)))

      other ->
        other
    end
  end

  defp impl_flow_spawn_children_and_get(ctx, parent_id, children, opts) do
    case FerricStore.Impl.flow_spawn_children(ctx, parent_id, children, opts) do
      :ok ->
        FerricStore.Impl.flow_get(
          ctx,
          parent_id,
          flow_partition_opts(Keyword.get(opts, :partition_key))
        )

      other ->
        other
    end
  end

  defp impl_flow_retry_and_get(ctx, id, lease_token, opts) do
    case FerricStore.Impl.flow_retry(ctx, id, lease_token, opts) do
      :ok ->
        FerricStore.Impl.flow_get(ctx, id, flow_partition_opts(Keyword.get(opts, :partition_key)))

      other ->
        other
    end
  end

  defp fetch_created_flow(item, partition_key, opts) do
    {id, item_partition_key} = create_item_identity(item, partition_key, opts)
    {:ok, flow} = FerricStore.flow_get(id, flow_partition_opts(item_partition_key))
    flow
  end

  defp fetch_many_flow(item, partition_key) do
    {id, item_partition_key} = many_item_identity(item, partition_key)
    {:ok, flow} = FerricStore.flow_get(id, flow_partition_opts(item_partition_key))
    flow
  end

  defp hydrate_create_results(results, items, partition_key, opts) do
    results
    |> Enum.zip(items)
    |> Enum.map(fn
      {:ok, item} -> fetch_created_flow(item, partition_key, opts)
      {other, _item} -> other
    end)
  end

  defp hydrate_many_results(results, items, partition_key) do
    results
    |> Enum.zip(items)
    |> Enum.map(fn
      {:ok, item} -> fetch_many_flow(item, partition_key)
      {other, _item} -> other
    end)
  end

  defp create_item_identity(%{id: id} = item, partition_key, opts),
    do: {id, Map.get(item, :partition_key) || partition_key || Keyword.get(opts, :partition_key)}

  defp create_item_identity(%{"id" => id} = item, partition_key, opts),
    do: {id, Map.get(item, "partition_key") || partition_key || Keyword.get(opts, :partition_key)}

  defp create_item_identity({id, item_opts}, partition_key, opts)
       when is_binary(id) and is_list(item_opts),
       do:
         {id,
          Keyword.get(item_opts, :partition_key) || partition_key ||
            Keyword.get(opts, :partition_key)}

  defp create_item_identity(id, partition_key, opts) when is_binary(id),
    do: {id, partition_key || Keyword.get(opts, :partition_key)}

  defp many_item_identity(%{id: id} = item, partition_key),
    do: {id, Map.get(item, :partition_key) || partition_key}

  defp many_item_identity(%{"id" => id} = item, partition_key),
    do: {id, Map.get(item, "partition_key") || partition_key}

  defp many_item_identity({id, _lease_token, item_opts}, partition_key)
       when is_binary(id) and is_list(item_opts),
       do: {id, Keyword.get(item_opts, :partition_key) || partition_key}

  defp many_item_identity({id, item_opts}, partition_key)
       when is_binary(id) and is_list(item_opts),
       do: {id, Keyword.get(item_opts, :partition_key) || partition_key}

  defp many_item_identity(
         {:id, id, :partition_key, item_partition_key, :lease_token, _token, :fencing_token,
          _fencing_token},
         _partition_key
       ),
       do: {id, item_partition_key}

  defp many_item_identity(
         {:id, id, :lease_token, _token, :fencing_token, _fencing_token},
         partition_key
       ),
       do: {id, partition_key}

  defp flow_partition_opts(nil), do: []
  defp flow_partition_opts(partition_key), do: [partition_key: partition_key]

  defp encoded_value_size(value) do
    value
    |> Ferricstore.Flow.encode_value()
    |> byte_size()
  end

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
             flow_create_and_get(id,
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

  defp create_claimed_flow_child(id, partition_key, worker) do
    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due("child",
               partition_key: partition_key,
               worker: worker,
               limit: 1,
               now_ms: 9_000_000_000_000
             )

    assert claimed.id == id
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

    assert Ferricstore.Flow.Keys.stream_entry_key_from_history_key(history_key, "1-1") ==
             Ferricstore.Flow.Keys.stream_entry_key("flow-a", "1-1", partition_key)

    old_state_key = "flow:{flow:" <> String.duplicate("0", 64) <> "}:state:flow-a"
    assert byte_size(state_key) < byte_size(old_state_key)
  end

  test "flow create rejects ids whose max history stream entry would exceed key size" do
    partition_key = "stream-boundary"
    max_event_id = "18446744073709551615-18446744073709551615"
    stream_entry_extra = byte_size("X:" <> <<0>> <> max_event_id)
    base_history_key = Ferricstore.Flow.Keys.history_key("", partition_key)

    id_size =
      Ferricstore.Store.Router.max_key_size() - stream_entry_extra - byte_size(base_history_key) +
        1

    id = String.duplicate("x", id_size)
    history_key = Ferricstore.Flow.Keys.history_key(id, partition_key)

    max_stream_key =
      Ferricstore.Flow.Keys.stream_entry_key_from_history_key(history_key, max_event_id)

    assert byte_size(history_key) <= Ferricstore.Store.Router.max_key_size()
    assert byte_size(max_stream_key) > Ferricstore.Store.Router.max_key_size()

    assert {:error, "ERR key too large" <> _} =
             FerricStore.flow_create(id,
               type: "audit",
               state: "queued",
               partition_key: partition_key
             )
  end

  test "flow state record encoding is compact" do
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
      ttl_ms: nil,
      retention_ttl_ms: 60_000,
      terminal_retention_until_ms: nil,
      history_hot_max_events: 100,
      history_max_events: 100_000,
      partition_key: "tenant-a",
      payload_ref: "payload:1",
      parent_flow_id: "parent-1",
      parent_partition_key: nil,
      root_flow_id: "root-1",
      correlation_id: "order-1",
      result_ref: "result:1",
      error_ref: nil,
      lease_owner: "worker-1",
      lease_token: "lease-1",
      lease_deadline_ms: 2_000,
      run_state: "charge_card",
      rewound_to_event_id: "1000-1",
      child_groups: %{}
    }

    compact = Ferricstore.Flow.encode_record(record)

    normal_record = Map.delete(record, :rewound_to_event_id)
    term_record = :erlang.term_to_binary(record)

    assert "FSF4" <> _ = compact
    assert Ferricstore.Flow.decode_record(compact) == record

    assert Ferricstore.Flow.decode_record(Ferricstore.Flow.encode_record(normal_record)) ==
             normal_record

    assert Ferricstore.Flow.encode_record(Map.delete(record, :child_groups)) == compact

    assert byte_size(compact) < byte_size(term_record)

    large_record = %{
      record
      | created_at_ms: 1_777_777_777_777,
        updated_at_ms: 1_777_777_777_888,
        next_run_at_ms: 1_777_777_777_999,
        lease_deadline_ms: 1_777_777_778_111
    }

    assert Ferricstore.Flow.decode_record(Ferricstore.Flow.encode_record(large_record)) ==
             large_record

    assert_raise ArgumentError, fn ->
      Ferricstore.Flow.decode_record("FSF9" <> binary_part(compact, 4, byte_size(compact) - 4))
    end

    assert_raise ArgumentError, fn ->
      Ferricstore.Flow.decode_record(binary_part(compact, 0, byte_size(compact) - 1))
    end
  end

  test "flow state record encoding preserves zeroes nils and empty binaries" do
    record = %{
      id: "flow-zero",
      type: "checkout",
      state: "queued",
      version: 0,
      attempts: 0,
      fencing_token: 0,
      created_at_ms: 0,
      updated_at_ms: 0,
      next_run_at_ms: 0,
      priority: 0,
      ttl_ms: 0,
      retention_ttl_ms: 0,
      terminal_retention_until_ms: 0,
      history_hot_max_events: 0,
      history_max_events: 0,
      partition_key: "",
      payload_ref: nil,
      parent_flow_id: "",
      parent_partition_key: nil,
      root_flow_id: "",
      correlation_id: "",
      result_ref: nil,
      error_ref: "",
      lease_owner: "",
      lease_token: nil,
      lease_deadline_ms: 0,
      run_state: "",
      rewound_to_event_id: nil,
      child_groups: %{}
    }

    assert Ferricstore.Flow.decode_record(Ferricstore.Flow.encode_record(record)) ==
             Map.delete(record, :rewound_to_event_id)
  end

  test "flow history encoding is compact and includes metadata" do
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

    fields = [
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

    compact =
      Ferricstore.Flow.encode_history_fields(record, "retry", 1_100, %{
        "retry_decision" => "scheduled",
        "retry_next_run_at_ms" => nil
      })

    assert compact ==
             record
             |> Ferricstore.Flow.history_snapshot("retry", 1_100, %{
               "retry_decision" => "scheduled",
               "retry_next_run_at_ms" => nil
             })
             |> Ferricstore.Flow.encode_history_snapshot()

    assert "FSH1" <> _ = compact

    assert Ferricstore.Flow.decode_history_fields(compact) ==
             fields ++
               [
                 "retry_decision",
                 "scheduled",
                 "retry_next_run_at_ms",
                 ""
               ]

    assert Ferricstore.Flow.decode_history_fields(
             "BAD!" <> binary_part(compact, 4, byte_size(compact) - 4)
           ) == []

    assert Ferricstore.Flow.decode_history_fields(:erlang.term_to_binary(fields)) == []

    assert Ferricstore.Flow.decode_history_fields(binary_part(compact, 0, byte_size(compact) - 1)) ==
             []
  end

  test "flow_create stores state and prevents duplicate ids" do
    id = uid("flow-create")

    assert {:ok, flow} =
             flow_create_and_get(id,
               type: "checkout",
               state: "queued",
               payload: "payload:" <> id,
               run_at_ms: 1_000
             )

    assert flow.id == id
    assert flow.type == "checkout"
    assert flow.state == "queued"
    assert flow.version == 1
    assert flow.fencing_token == 0
    assert is_binary(flow.payload_ref)
    assert flow.payload_ref != "payload:" <> id

    assert {:ok, fetched} = FerricStore.flow_get(id)
    assert fetched.id == id
    assert fetched.state == "queued"

    assert {:error, "ERR flow already exists"} =
             flow_create_and_get(id, type: "checkout", state: "queued")
  end

  test "flow_get hydrates payload refs only when full or payload is requested" do
    id = uid("flow-get-payload")
    payload = "payload-body"

    assert {:ok, _flow} =
             flow_create_and_get(id,
               type: "checkout",
               state: "queued",
               payload: payload,
               run_at_ms: 1_000
             )

    assert {:ok, ref_only} = FerricStore.flow_get(id)
    refute Map.has_key?(ref_only, :payload)
    refute Map.has_key?(ref_only, :payload_omitted)

    assert {:ok, fetched} = FerricStore.flow_get(id, full: true)
    assert fetched.payload == payload
    assert fetched.payload_size == encoded_value_size(payload)
    refute Map.has_key?(fetched, :payload_omitted)

    assert {:ok, no_payload} = FerricStore.flow_get(id, payload: false)
    refute Map.has_key?(no_payload, :payload)
    refute Map.has_key?(no_payload, :payload_omitted)

    assert {:ok, capped} = FerricStore.flow_get(id, full: true, payload_max_bytes: 4)
    assert capped.payload_omitted == true
    assert capped.payload_size == encoded_value_size(payload)
    refute Map.has_key?(capped, :payload)
  end

  test "flow_claim_due returns payload inline up to cap without rolling back missing payloads" do
    type = uid("claim-payload")
    id = uid("claim-payload-flow")
    payload = "worker-input"

    assert {:ok, _flow} =
             flow_create_and_get(id,
               type: type,
               state: "queued",
               payload: payload,
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
    assert claimed.payload_size == encoded_value_size(payload)

    missing_id = uid("claim-missing-payload-flow")

    assert {:ok, missing_flow} =
             flow_create_and_get(missing_id,
               type: type,
               state: "queued",
               payload: "missing-worker-input",
               run_at_ms: 2_000
             )

    assert {:ok, 1} = FerricStore.del(missing_flow.payload_ref)

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
             flow_create_and_get(id_a,
               type: "pipeline-read",
               state: "queued",
               partition_key: partition_a,
               now_ms: 1,
               run_at_ms: 1
             )

    assert {:ok, _} =
             flow_create_and_get(id_b,
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

  test "pipeline_write_batch_independent works in raft mode" do
    ctx = FerricStore.Instance.get(:default)
    partition_key = uid("flow-pipeline-write-standalone-partition")
    id_a = uid("flow-pipeline-write-standalone-a")
    id_b = uid("flow-pipeline-write-standalone-b")

    assert [:ok, :ok] =
             Ferricstore.Flow.pipeline_write_batch_independent(ctx, [
               {:create, id_a,
                [
                  type: "pipeline-write-standalone",
                  state: "queued",
                  run_at_ms: 1,
                  now_ms: 1,
                  partition_key: partition_key
                ]},
               {:create, id_b,
                [
                  type: "pipeline-write-standalone",
                  state: "queued",
                  run_at_ms: 1,
                  now_ms: 1,
                  partition_key: partition_key
                ]}
             ])

    assert {:ok, %{id: ^id_a}} = FerricStore.flow_get(id_a, partition_key: partition_key)
    assert {:ok, %{id: ^id_b}} = FerricStore.flow_get(id_b, partition_key: partition_key)
  end

  test "pipeline_write_batch_independent terminal commands preserve per-command success" do
    ctx = FerricStore.Instance.get(:default)
    partition_key = uid("flow-pipeline-terminal-partition")
    type = uid("flow-pipeline-terminal")
    now_ms = 1_000
    ids = Enum.map(1..3, &uid("flow-pipeline-terminal-#{&1}"))

    Enum.each(ids, fn id ->
      assert {:ok, _} =
               flow_create_and_get(id,
                 type: type,
                 state: "queued",
                 partition_key: partition_key,
                 now_ms: now_ms,
                 run_at_ms: now_ms
               )
    end)

    assert {:ok, claims} =
             FerricStore.flow_claim_due(type,
               partition_key: partition_key,
               worker: "pipeline-terminal",
               limit: 3,
               now_ms: now_ms,
               lease_ms: 30_000
             )

    claims_by_id = Map.new(claims, fn claim -> {claim.id, claim} end)
    [id_a, id_b, id_c] = ids
    claim_a = Map.fetch!(claims_by_id, id_a)
    claim_b = Map.fetch!(claims_by_id, id_b)
    claim_c = Map.fetch!(claims_by_id, id_c)

    assert [:ok, {:error, _reason}, :ok] =
             Ferricstore.Flow.pipeline_write_batch_independent(ctx, [
               {:complete, id_a, claim_a.lease_token,
                [
                  partition_key: partition_key,
                  fencing_token: claim_a.fencing_token,
                  now_ms: now_ms + 1
                ]},
               {:complete, id_b, "wrong-token",
                [
                  partition_key: partition_key,
                  fencing_token: claim_b.fencing_token,
                  now_ms: now_ms + 1
                ]},
               {:complete, id_c, claim_c.lease_token,
                [
                  partition_key: partition_key,
                  fencing_token: claim_c.fencing_token,
                  now_ms: now_ms + 1
                ]}
             ])

    assert {:ok, %{state: "completed"}} = FerricStore.flow_get(id_a, partition_key: partition_key)
    assert {:ok, %{state: "running"}} = FerricStore.flow_get(id_b, partition_key: partition_key)
    assert {:ok, %{state: "completed"}} = FerricStore.flow_get(id_c, partition_key: partition_key)
  end

  test "pipeline_write_batch_independent terminal commands preserve duplicate flow order" do
    ctx = FerricStore.Instance.get(:default)
    partition_key = uid("flow-pipeline-terminal-dup-partition")
    type = uid("flow-pipeline-terminal-dup")
    now_ms = 1_000
    id = uid("flow-pipeline-terminal-dup")

    assert {:ok, _} =
             flow_create_and_get(id,
               type: type,
               state: "queued",
               partition_key: partition_key,
               now_ms: now_ms,
               run_at_ms: now_ms
             )

    assert {:ok, [claim]} =
             FerricStore.flow_claim_due(type,
               partition_key: partition_key,
               worker: "pipeline-terminal-dup",
               limit: 1,
               now_ms: now_ms,
               lease_ms: 30_000
             )

    command =
      {:complete, id, claim.lease_token,
       [
         partition_key: partition_key,
         fencing_token: claim.fencing_token,
         now_ms: now_ms + 1
       ]}

    assert [:ok, {:error, _reason}] =
             Ferricstore.Flow.pipeline_write_batch_independent(ctx, [command, command])

    assert {:ok, %{state: "completed"}} = FerricStore.flow_get(id, partition_key: partition_key)
  end

  test "pipeline_claim_due_batch coalesces compatible claims and preserves worker boundaries" do
    ctx = FerricStore.Instance.get(:default)
    type = uid("pipeline-claim")
    partition_key = uid("tenant")
    ids = Enum.map(1..3, &uid("pipeline-claim-#{&1}"))

    Enum.each(ids, fn id ->
      assert {:ok, %{id: ^id}} =
               flow_create_and_get(id,
                 type: type,
                 partition_key: partition_key,
                 now_ms: 1,
                 run_at_ms: 1
               )
    end)

    attach_flow_telemetry([[:ferricstore, :flow, :pipeline_claim_due_batch]])

    assert [
             {:ok, [%{lease_owner: "worker-a"} = first]},
             {:ok, [%{lease_owner: "worker-a"} = second]},
             {:ok, [%{lease_owner: "worker-b"} = third]}
           ] =
             Ferricstore.Flow.pipeline_claim_due_batch(ctx, [
               {:claim_due, type,
                [worker: "worker-a", partition_key: partition_key, limit: 1, now_ms: 2]},
               {:claim_due, type,
                [worker: "worker-a", partition_key: partition_key, limit: 1, now_ms: 2]},
               {:claim_due, type,
                [worker: "worker-b", partition_key: partition_key, limit: 1, now_ms: 2]}
             ])

    assert MapSet.new([first.id, second.id, third.id]) == MapSet.new(ids)

    assert_receive {:flow_telemetry, [:ferricstore, :flow, :pipeline_claim_due_batch],
                    %{commands: 3, groups: 2, coalesced_calls: 1}, %{source: :resp_pipeline}}
  end

  test "pipeline_claim_due_batch coalesces job-only claim responses" do
    ctx = FerricStore.Instance.get(:default)
    type = uid("pipeline-claim-jobs")
    partition_key = uid("tenant")
    ids = Enum.map(1..2, &uid("pipeline-claim-jobs-#{&1}"))

    Enum.each(ids, fn id ->
      assert {:ok, %{id: ^id}} =
               flow_create_and_get(id,
                 type: type,
                 partition_key: partition_key,
                 now_ms: 1,
                 run_at_ms: 1
               )
    end)

    attach_flow_telemetry([[:ferricstore, :flow, :pipeline_claim_due_batch]])

    assert [
             {:ok,
              [%{id: first_id, lease_token: first_lease, fencing_token: first_fence} = first]},
             {:ok,
              [%{id: second_id, lease_token: second_lease, fencing_token: second_fence} = second]}
           ] =
             Ferricstore.Flow.pipeline_claim_due_batch(ctx, [
               {:claim_due, type,
                [
                  worker: "worker-a",
                  partition_key: partition_key,
                  limit: 1,
                  now_ms: 2,
                  return: :jobs
                ]},
               {:claim_due, type,
                [
                  worker: "worker-a",
                  partition_key: partition_key,
                  limit: 1,
                  now_ms: 2,
                  return: :jobs
                ]}
             ])

    assert MapSet.new([first_id, second_id]) == MapSet.new(ids)
    assert is_binary(first_lease)
    assert is_binary(second_lease)
    assert is_integer(first_fence)
    assert is_integer(second_fence)
    refute Map.has_key?(first, :version)
    refute Map.has_key?(second, :version)

    assert_receive {:flow_telemetry, [:ferricstore, :flow, :pipeline_claim_due_batch],
                    %{commands: 2, groups: 1, coalesced_calls: 1}, %{source: :resp_pipeline}}
  end

  test "claim_due supports compact job responses" do
    type = uid("claim-jobs-compact")
    partition_key = uid("tenant")
    id = uid("claim-jobs-compact")

    assert {:ok, %{id: ^id}} =
             flow_create_and_get(id,
               type: type,
               partition_key: partition_key,
               now_ms: 1,
               run_at_ms: 1
             )

    assert {:ok, [[^id, ^partition_key, lease_token, fencing_token]]} =
             FerricStore.flow_claim_due(type,
               worker: "worker-compact",
               partition_key: partition_key,
               limit: 1,
               now_ms: 2,
               return: :jobs_compact
             )

    assert is_binary(lease_token)
    assert is_integer(fencing_token)
  end

  test "pipeline_claim_due_batch groups interleaved independent partitions" do
    ctx = FerricStore.Instance.get(:default)
    type = uid("pipeline-claim-partitions")
    partition_a = uid("tenant-a")
    partition_b = uid("tenant-b")

    ids =
      for {partition_key, idx} <- [
            {partition_a, 1},
            {partition_b, 2},
            {partition_a, 3},
            {partition_b, 4}
          ] do
        id = uid("pipeline-claim-partition-#{idx}")

        assert {:ok, %{id: ^id}} =
                 flow_create_and_get(id,
                   type: type,
                   partition_key: partition_key,
                   now_ms: 1,
                   run_at_ms: 1
                 )

        id
      end

    attach_flow_telemetry([[:ferricstore, :flow, :pipeline_claim_due_batch]])

    results =
      Ferricstore.Flow.pipeline_claim_due_batch(ctx, [
        {:claim_due, type, [worker: "worker-a", partition_key: partition_a, limit: 1, now_ms: 2]},
        {:claim_due, type, [worker: "worker-a", partition_key: partition_b, limit: 1, now_ms: 2]},
        {:claim_due, type, [worker: "worker-a", partition_key: partition_a, limit: 1, now_ms: 2]},
        {:claim_due, type, [worker: "worker-a", partition_key: partition_b, limit: 1, now_ms: 2]}
      ])

    assert Enum.all?(results, fn {:ok, records} -> length(records) == 1 end)

    claimed_ids =
      results
      |> Enum.flat_map(fn {:ok, [record]} -> [record.id] end)
      |> MapSet.new()

    assert claimed_ids == MapSet.new(ids)

    assert_receive {:flow_telemetry, [:ferricstore, :flow, :pipeline_claim_due_batch],
                    %{commands: 4, groups: 2, coalesced_calls: 2}, %{source: :resp_pipeline}}
  end

  test "pipeline_claim_due_batch honors per-command partition_keys" do
    ctx = FerricStore.Instance.get(:default)
    type = uid("pipeline-claim-partition-keys")
    partition_a = uid("tenant-a")
    partition_b = uid("tenant-b")
    id_b = uid("pipeline-claim-partition-keys-b")

    assert {:ok, %{id: ^id_b}} =
             flow_create_and_get(id_b,
               type: type,
               partition_key: partition_b,
               now_ms: 1,
               run_at_ms: 1
             )

    assert [
             {:ok, []},
             {:ok, [%{id: ^id_b, partition_key: ^partition_b}]}
           ] =
             Ferricstore.Flow.pipeline_claim_due_batch(ctx, [
               {:claim_due, type,
                [worker: "worker-a", partition_keys: [partition_a], limit: 1, now_ms: 2]},
               {:claim_due, type,
                [worker: "worker-a", partition_keys: [partition_b], limit: 1, now_ms: 2]}
             ])
  end

  test "pipeline_claim_due_batch accepts omitted NOW option" do
    ctx = FerricStore.Instance.get(:default)
    type = uid("pipeline-claim-no-now")
    partition_key = uid("tenant")
    id = uid("pipeline-claim-no-now-id")
    now = System.system_time(:millisecond)

    assert {:ok, %{id: ^id}} =
             flow_create_and_get(id,
               type: type,
               partition_key: partition_key,
               now_ms: now,
               run_at_ms: now
             )

    assert [{:ok, [%{id: ^id, lease_owner: "worker-a"}]}] =
             Ferricstore.Flow.pipeline_claim_due_batch(ctx, [
               {:claim_due, type, [worker: "worker-a", partition_key: partition_key, limit: 1]}
             ])
  end

  test "pipeline_claim_due_batch preserves sequential reclaim preference" do
    ctx = FerricStore.Instance.get(:default)
    type = uid("pipeline-claim-reclaim")
    partition_key = uid("tenant")
    expired_a = uid("pipeline-claim-expired-a")
    expired_b = uid("pipeline-claim-expired-b")
    queued = uid("pipeline-claim-queued")

    for id <- [expired_a, expired_b, queued] do
      assert {:ok, %{id: ^id}} =
               flow_create_and_get(id,
                 type: type,
                 partition_key: partition_key,
                 now_ms: 1_000,
                 run_at_ms: 1_000
               )
    end

    assert {:ok, [%{id: ^expired_a}]} =
             FerricStore.flow_claim_due(type,
               worker: "old-worker",
               partition_key: partition_key,
               lease_ms: 50,
               limit: 1,
               now_ms: 1_000
             )

    assert {:ok, [%{id: ^expired_b}]} =
             FerricStore.flow_claim_due(type,
               worker: "old-worker",
               partition_key: partition_key,
               lease_ms: 50,
               limit: 1,
               now_ms: 1_000
             )

    assert [
             {:ok, [%{id: first}]},
             {:ok, [%{id: second}]}
           ] =
             Ferricstore.Flow.pipeline_claim_due_batch(ctx, [
               {:claim_due, type,
                [worker: "worker-a", partition_key: partition_key, limit: 1, now_ms: 1_100]},
               {:claim_due, type,
                [worker: "worker-a", partition_key: partition_key, limit: 1, now_ms: 1_100]}
             ])

    assert MapSet.new([first, second]) == MapSet.new([expired_a, expired_b])

    assert {:ok, [%{id: ^queued}]} =
             FerricStore.flow_claim_due(type,
               worker: "worker-b",
               partition_key: partition_key,
               now_ms: 1_100
             )
  end

  test "pipeline_read_batch hydrates Flow GET payloads with per-command caps" do
    ctx = FerricStore.Instance.get(:default)
    id_a = uid("flow-pipeline-payload-a")
    id_b = uid("flow-pipeline-payload-b")
    payload_a = "payload-a"
    payload_b = "payload-b"
    payload_a_size = encoded_value_size(payload_a)
    payload_b_size = encoded_value_size(payload_b)

    assert {:ok, _flow} =
             flow_create_and_get(id_a,
               type: "pipeline-payload",
               payload: payload_a,
               run_at_ms: 1
             )

    assert {:ok, _flow} =
             flow_create_and_get(id_b,
               type: "pipeline-payload",
               payload: payload_b,
               run_at_ms: 1
             )

    assert [
             {:ok, %{id: ^id_a, payload: ^payload_a, payload_size: ^payload_a_size}},
             {:ok, %{id: ^id_b, payload_omitted: true, payload_size: ^payload_b_size}},
             {:ok, no_payload}
           ] =
             Ferricstore.Flow.pipeline_read_batch(ctx, [
               {:get, id_a, [full: true]},
               {:get, id_b, [full: true, payload_max_bytes: 4]},
               {:get, id_a, [payload: false]}
             ])

    refute Map.has_key?(no_payload, :payload)
    refute Map.has_key?(no_payload, :payload_omitted)
  end

  test "flow_create idempotent retry returns matching existing record and rejects conflicts" do
    id = uid("flow-create-idempotent")

    assert {:ok, created} =
             flow_create_and_get(id,
               type: "checkout",
               state: "queued",
               payload: "payload:" <> id,
               run_at_ms: 1_000,
               now_ms: 10,
               idempotent: true
             )

    assert {:ok, retried} =
             flow_create_and_get(id,
               type: "checkout",
               state: "queued",
               payload: "payload:" <> id,
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
             flow_create_and_get(id,
               type: "checkout",
               state: "queued",
               payload: "different:" <> id,
               run_at_ms: 1_000,
               idempotent: true
             )

    assert {:error, "ERR flow idempotency conflict"} =
             flow_create_and_get(id,
               type: "checkout",
               state: "queued",
               payload: "payload:" <> id,
               run_at_ms: 2_000,
               idempotent: true
             )

    values_id = uid("flow-create-idempotent-values")

    assert {:ok, values_created} =
             flow_create_and_get(values_id,
               type: "checkout",
               state: "queued",
               values: %{"order" => "order-v1"},
               value_refs: %{"profile" => "profile-ref-v1"},
               idempotent: true,
               run_at_ms: 30,
               now_ms: 30
             )

    assert {:ok, values_retried} =
             flow_create_and_get(values_id,
               type: "checkout",
               state: "queued",
               values: %{"order" => "order-v1"},
               value_refs: %{"profile" => "profile-ref-v1"},
               idempotent: true,
               run_at_ms: 30,
               now_ms: 40
             )

    assert values_retried.id == values_created.id
    assert values_retried.version == values_created.version

    assert {:error, "ERR flow idempotency conflict"} =
             flow_create_and_get(values_id,
               type: "checkout",
               state: "queued",
               values: %{"order" => "order-v2"},
               value_refs: %{"profile" => "profile-ref-v1"},
               run_at_ms: 30,
               idempotent: true
             )

    assert {:error, "ERR flow idempotency conflict"} =
             flow_create_and_get(values_id,
               type: "checkout",
               state: "queued",
               values: %{"order" => "order-v1"},
               value_refs: %{"profile" => "profile-ref-v2"},
               run_at_ms: 30,
               idempotent: true
             )
  end

  test "flow due index stays derived and does not persist per-flow zset members" do
    id = uid("flow-due-derived")
    run_at_ms = 1_234

    assert {:ok, _flow} =
             flow_create_and_get(id,
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
             flow_create_and_get(id,
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

  test "cold lineage query overfetches when stale LMDB candidates are missing from truth" do
    ctx = Ferricstore.Test.IsolatedInstance.checkout(shard_count: 1)

    on_exit(fn -> Ferricstore.Test.IsolatedInstance.checkin(ctx) end)

    id = uid("flow-lineage-cold-valid")
    stale_id = uid("flow-lineage-cold-stale")
    parent = uid("flow-lineage-cold-parent")
    partition = uid("tenant-lineage-cold")

    assert :ok =
             Ferricstore.Flow.create(ctx, id,
               type: "lineage-cold",
               partition_key: partition,
               parent_flow_id: parent,
               run_at_ms: 2_000,
               now_ms: 2_000
             )

    index_key = Ferricstore.Flow.Keys.parent_index_key(parent, partition)
    {flow_index, flow_lookup} = Ferricstore.Flow.OrderedIndex.table_names(ctx.name, 0)
    Ferricstore.Flow.OrderedIndex.delete_member(flow_index, flow_lookup, index_key, id)

    case Ferricstore.Flow.NativeOrderedIndex.get(flow_index, flow_lookup) do
      nil -> :ok
      native -> Ferricstore.Flow.NativeOrderedIndex.delete_member(native, index_key, id)
    end

    lmdb_path =
      ctx.data_dir
      |> Ferricstore.DataDir.shard_data_path(0)
      |> Ferricstore.Flow.LMDB.path()

    stale_key = Ferricstore.Flow.LMDB.query_index_key(index_key, stale_id, 1)
    stale_value = Ferricstore.Flow.LMDB.encode_query_index_value(stale_id, 1, 0)
    valid_key = Ferricstore.Flow.LMDB.query_index_key(index_key, id, 2_000)
    valid_state_key = Ferricstore.Flow.Keys.state_key(id, partition)
    valid_value = Ferricstore.Flow.LMDB.encode_query_index_value(id, 2_000, 0, valid_state_key)

    assert :ok =
             Ferricstore.Flow.LMDB.write_batch(lmdb_path, [
               {:put, stale_key, stale_value},
               {:put, valid_key, valid_value}
             ])

    assert {:ok, [%{id: ^id}]} =
             Ferricstore.Flow.by_parent(ctx, parent,
               partition_key: partition,
               include_cold: true,
               count: 1
             )
  end

  test "flow_create stores default root without indexing one unique root per flow" do
    id = uid("flow-lineage-root-default")
    partition = uid("tenant")

    assert {:ok, flow} =
             flow_create_and_get(id,
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
             flow_create_and_get(root,
               type: "lineage",
               partition_key: partition,
               correlation_id: correlation,
               now_ms: 1_000,
               run_at_ms: 1_000
             )

    assert {:ok, %{id: ^child_a}} =
             flow_create_and_get(child_a,
               type: "lineage",
               partition_key: partition,
               parent_flow_id: root,
               root_flow_id: root,
               correlation_id: correlation,
               now_ms: 2_000,
               run_at_ms: 2_000
             )

    assert {:ok, %{id: ^child_b}} =
             flow_create_and_get(child_b,
               type: "lineage",
               partition_key: partition,
               parent_flow_id: root,
               root_flow_id: root,
               correlation_id: correlation,
               now_ms: 3_000,
               run_at_ms: 3_000
             )

    assert {:ok, %{id: ^grandchild}} =
             flow_create_and_get(grandchild,
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

    assert {:ok, [%{id: ^child_b}, %{id: ^child_a}]} =
             FerricStore.flow_by_parent(root,
               partition_key: partition,
               from_ms: 1_500,
               to_ms: 3_500,
               rev: true,
               count: 10
             )

    assert {:ok, [%{id: ^child_b}]} =
             FerricStore.flow_by_parent(root,
               partition_key: partition,
               rev: true,
               count: 1
             )

    assert {:ok, []} =
             FerricStore.zrange(Ferricstore.Flow.Keys.parent_index_key(root, partition), 0, 10)

    assert {:ok, [%{id: ^root}, %{id: ^child_a}, %{id: ^child_b}, %{id: ^grandchild}]} =
             FerricStore.flow_by_root(root, partition_key: partition, count: 10)

    assert {:ok, [%{id: ^child_b}]} =
             FerricStore.flow_by_root(root,
               partition_key: partition,
               from_ms: 2_500,
               to_ms: 3_500,
               state: "queued",
               count: 10
             )

    assert {:ok, []} =
             FerricStore.zrange(Ferricstore.Flow.Keys.root_index_key(root, partition), 0, 10)

    assert {:ok, [%{id: ^root}, %{id: ^child_a}]} =
             FerricStore.flow_by_correlation(correlation, partition_key: partition, count: 2)

    assert {:ok, child_a_record} = FerricStore.flow_get(child_a, partition_key: partition)

    assert {:ok, _cancelled} =
             flow_cancel_and_get(child_a,
               partition_key: partition,
               fencing_token: child_a_record.fencing_token,
               now_ms: 5_000
             )

    assert {:ok, [%{id: ^child_a, state: "cancelled"}]} =
             FerricStore.flow_by_correlation(correlation,
               partition_key: partition,
               terminal_only: true,
               count: 10
             )

    assert {:ok, []} =
             FerricStore.zrange(
               Ferricstore.Flow.Keys.correlation_index_key(correlation, partition),
               0,
               10
             )
  end

  test "flow lineage queries find default auto-partitioned flows" do
    root = uid("flow-auto-root")
    child = uid("flow-auto-child")
    correlation = uid("flow-auto-correlation")

    assert {:ok, %{id: ^root}} =
             flow_create_and_get(root,
               type: "lineage-auto",
               correlation_id: correlation,
               now_ms: 1_000,
               run_at_ms: 1_000
             )

    assert {:ok, %{id: ^child}} =
             flow_create_and_get(child,
               type: "lineage-auto",
               parent_flow_id: root,
               root_flow_id: root,
               correlation_id: correlation,
               now_ms: 2_000,
               run_at_ms: 2_000
             )

    assert {:ok, by_parent} = FerricStore.flow_by_parent(root, count: 10)
    assert Enum.map(by_parent, & &1.id) == [child]

    assert {:ok, by_root} = FerricStore.flow_by_root(root, count: 10)
    assert Enum.map(by_root, & &1.id) == [root, child]

    assert {:ok, by_correlation} = FerricStore.flow_by_correlation(correlation, count: 10)
    assert Enum.map(by_correlation, & &1.id) == [root, child]
  end

  test "flow_retry requires explicit opts because fencing_token is required" do
    assert {:module, FerricStore} = Code.ensure_loaded(FerricStore)
    assert {:module, FerricStore.Impl} = Code.ensure_loaded(FerricStore.Impl)

    assert function_exported?(FerricStore, :flow_retry, 3)
    refute function_exported?(FerricStore, :flow_retry, 2)
    assert function_exported?(FerricStore.Impl, :flow_retry, 4)
    refute function_exported?(FerricStore.Impl, :flow_retry, 3)
  end

  test "flow lineage queries reject global partition sentinel" do
    assert {:error, reason} = FerricStore.flow_by_parent(uid("parent"), partition_key: :global)
    assert reason =~ "partition_key"

    assert {:error, reason} = FerricStore.flow_by_root(uid("root"), partition_key: :global)
    assert reason =~ "partition_key"

    assert {:error, reason} =
             FerricStore.flow_by_correlation(uid("correlation"), partition_key: :global)

    assert reason =~ "partition_key"
  end

  test "flow lineage queries treat explicit nil partition as auto" do
    root = uid("flow-auto-nil-root")
    child = uid("flow-auto-nil-child")

    assert {:ok, %{id: ^root}} =
             flow_create_and_get(root, type: "lineage-auto-nil", now_ms: 1_000, run_at_ms: 1_000)

    assert {:ok, %{id: ^child}} =
             flow_create_and_get(child,
               type: "lineage-auto-nil",
               parent_flow_id: root,
               root_flow_id: root,
               now_ms: 2_000,
               run_at_ms: 2_000
             )

    assert {:ok, by_parent} = FerricStore.flow_by_parent(root, partition_key: nil, count: 10)
    assert Enum.map(by_parent, & &1.id) == [child]
  end

  test "flow_create_many creates one-partition batch atomically" do
    partition = uid("tenant")
    type = uid("bulk-create")
    id_a = uid("bulk-a")
    id_b = uid("bulk-b")

    assert {:ok, flows} =
             flow_create_many_and_get(
               partition,
               [
                 %{id: id_a, payload: "payload:" <> id_a},
                 %{id: id_b, payload: "payload:" <> id_b}
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

  test "flow_create_many allows same id in different partitions" do
    type = uid("bulk-create-same-id")
    id = uid("same-id")
    partition_a = uid("same-id-a")
    partition_b = uid("same-id-b")

    assert {:ok, [created_a, created_b]} =
             flow_create_many_and_get(
               nil,
               [
                 %{id: id, partition_key: partition_a},
                 %{id: id, partition_key: partition_b}
               ],
               type: type,
               state: "queued",
               run_at_ms: 1_000,
               now_ms: 900
             )

    assert created_a.id == id
    assert created_a.partition_key == partition_a
    assert created_b.id == id
    assert created_b.partition_key == partition_b
  end

  test "flow write APIs return ok and reject return option" do
    partition = uid("tenant-return")
    type = uid("return-api")
    id = uid("return-flow")

    assert :ok =
             FerricStore.flow_create_many(
               partition,
               [%{id: id, payload: "payload:" <> id}],
               type: type,
               state: "queued",
               run_at_ms: 1_000,
               now_ms: 900
             )

    assert :ok =
             FerricStore.flow_transition_many(
               partition,
               "queued",
               "ready",
               [%{id: id, fencing_token: 0}],
               run_at_ms: 1_000,
               now_ms: 950
             )

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due(type,
               partition_key: partition,
               state: "ready",
               worker: "worker-return",
               limit: 1,
               now_ms: 1_000
             )

    assert claimed.id == id
    assert claimed.payload == "payload:" <> id
    assert is_binary(claimed.lease_token)
    assert is_integer(claimed.fencing_token)

    assert :ok =
             FerricStore.flow_complete_many(
               partition,
               [
                 %{
                   id: claimed.id,
                   lease_token: claimed.lease_token,
                   fencing_token: claimed.fencing_token
                 }
               ],
               result: "ok",
               now_ms: 1_100
             )

    rejected_id = uid("return-rejected")

    assert {:error, "ERR flow return option is not supported"} =
             FerricStore.flow_create_many(
               partition,
               [%{id: rejected_id}],
               type: type,
               state: "queued",
               run_at_ms: 1_000,
               now_ms: 900,
               return: :full
             )
  end

  test "flow_create_many lets item attrs override common attrs" do
    partition = uid("tenant")
    type = uid("bulk-create-override")
    id_a = uid("bulk-override-a")
    id_b = uid("bulk-override-b")

    assert {:ok, [flow_a, flow_b]} =
             flow_create_many_and_get(
               partition,
               [
                 %{id: id_a},
                 %{id: id_b, payload: "payload:item", correlation_id: "corr:item"}
               ],
               type: type,
               payload: "payload:common",
               correlation_id: "corr:common",
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert is_binary(flow_a.payload_ref)
    assert flow_a.payload_ref != "payload:common"
    assert flow_a.correlation_id == "corr:common"
    assert is_binary(flow_b.payload_ref)
    assert flow_b.payload_ref != "payload:item"
    assert flow_b.payload_ref != flow_a.payload_ref
    assert flow_b.correlation_id == "corr:item"
  end

  test "flow_create_many auto-partitions missing partition and rolls back duplicate batches" do
    partition = uid("tenant")
    type = uid("bulk-atomic")
    existing_id = uid("bulk-existing")
    new_id = uid("bulk-new")

    assert {:ok, [auto_flow]} =
             flow_create_many_and_get(nil, [%{id: new_id}], type: type)

    assert auto_flow.id == new_id
    assert Ferricstore.Flow.Keys.auto_partition_key?(auto_flow.partition_key)

    assert {:ok, _} =
             flow_create_and_get(existing_id,
               type: type,
               partition_key: partition,
               run_at_ms: 1_000
             )

    assert {:error, "ERR flow already exists"} =
             flow_create_many_and_get(
               partition,
               [%{id: existing_id}, %{id: new_id}],
               type: type,
               run_at_ms: 1_000
             )

    assert {:ok, nil} = FerricStore.flow_get(new_id, partition_key: partition)
    assert {:ok, []} = FerricStore.flow_history(new_id, partition_key: partition)

    assert {:error, "ERR flow duplicate id in batch"} =
             flow_create_many_and_get(
               partition,
               [%{id: new_id}, %{id: new_id}],
               type: type,
               run_at_ms: 1_000
             )

    assert {:ok, nil} = FerricStore.flow_get(new_id, partition_key: partition)
    assert {:ok, []} = FerricStore.flow_history(new_id, partition_key: partition)
  end

  test "flow_create_many independent keeps successful items when one create fails" do
    partition = uid("tenant-create-many-independent")
    type = uid("bulk-independent-create")
    existing_id = uid("bulk-independent-existing")
    new_id = uid("bulk-independent-new")

    assert {:ok, _} =
             flow_create_and_get(existing_id,
               type: type,
               partition_key: partition,
               run_at_ms: 1_000
             )

    assert {:ok, [{:error, "ERR flow already exists"}, :ok]} =
             FerricStore.flow_create_many(
               partition,
               [%{id: existing_id}, %{id: new_id}],
               type: type,
               run_at_ms: 1_000,
               independent: true
             )

    assert {:ok, %{id: ^new_id, state: "queued"}} =
             FerricStore.flow_get(new_id, partition_key: partition)
  end

  test "flow_create_many independent auto-partitions without client-side grouping" do
    type = uid("bulk-independent-auto-create")
    existing_id = uid("bulk-independent-auto-existing")
    new_id = uid("bulk-independent-auto-new")
    ids = Enum.map(1..32, fn idx -> uid("bulk-independent-auto-#{idx}") end)

    assert {:ok, _} =
             flow_create_and_get(existing_id,
               type: type,
               run_at_ms: 1_000
             )

    assert {:ok, results} =
             FerricStore.flow_create_many(
               nil,
               [%{id: existing_id}, %{id: new_id} | Enum.map(ids, &%{id: &1})],
               type: type,
               run_at_ms: 1_000,
               independent: true
             )

    assert [{:error, "ERR flow already exists"} | created_results] = results
    assert Enum.all?(created_results, &(&1 == :ok))

    for id <- [new_id | ids] do
      assert {:ok, %{id: ^id, state: "queued", partition_key: partition_key}} =
               FerricStore.flow_get(id)

      assert Ferricstore.Flow.Keys.auto_partition_key?(partition_key)
    end
  end

  test "flow_create_many idempotent retry returns existing records without duplicate writes" do
    partition = uid("tenant")
    type = uid("bulk-idempotent")
    existing_id = uid("bulk-existing")
    new_id = uid("bulk-new")

    assert {:ok, %{id: ^existing_id}} =
             flow_create_and_get(existing_id,
               type: type,
               partition_key: partition,
               payload: "payload:" <> existing_id,
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert {:ok, records} =
             flow_create_many_and_get(
               partition,
               [
                 %{id: existing_id, payload: "payload:" <> existing_id},
                 %{id: new_id, payload: "payload:" <> new_id}
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
             flow_create_and_get(existing_id,
               type: type,
               partition_key: partition,
               payload: "payload:old",
               run_at_ms: 1_000
             )

    assert {:error, "ERR flow idempotency conflict"} =
             flow_create_many_and_get(
               partition,
               [
                 %{id: existing_id, payload: "payload:new"},
                 %{id: new_id, payload: "payload:" <> new_id}
               ],
               type: type,
               run_at_ms: 1_000,
               idempotent: true
             )

    assert {:ok, nil} = FerricStore.flow_get(new_id, partition_key: partition)
    assert {:ok, []} = FerricStore.flow_history(new_id, partition_key: partition)
  end

  test "flow_spawn_children wait none creates full child flows and advances parent" do
    parent = uid("flow-parent-none")
    child_a = uid("flow-child-none-a")
    child_b = uid("flow-child-none-b")
    partition = uid("tenant")

    assert {:ok, created_parent} =
             flow_create_and_get(parent,
               type: "parent",
               state: "dispatch",
               partition_key: partition,
               now_ms: 1_000
             )

    assert {:ok, advanced} =
             flow_spawn_children_and_get(
               parent,
               [
                 %{id: child_a, type: "child", payload: %{n: 1}},
                 %{id: child_b, type: "child", payload: %{n: 2}}
               ],
               group_id: "fanout-1",
               wait: :none,
               on_child_failed: :ignore,
               on_parent_closed: :abandon_children,
               exhaust_to: %{success: "dispatched", failure: "dispatch_failed"},
               partition_key: partition,
               from_state: "dispatch",
               fencing_token: created_parent.fencing_token,
               now_ms: 1_010
             )

    assert advanced.state == "dispatched"
    assert advanced.child_groups["fanout-1"]["resolved"] == "success"

    assert {:ok, children} = FerricStore.flow_by_parent(parent, partition_key: partition)
    assert Enum.map(children, & &1.id) |> Enum.sort() == Enum.sort([child_a, child_b])
    assert Enum.all?(children, &(&1.root_flow_id == parent))
  end

  test "flow_spawn_children wait all advances parent only after direct children finish" do
    parent = uid("flow-parent-all")
    child_a = uid("flow-child-all-a")
    child_b = uid("flow-child-all-b")
    partition = uid("tenant")

    assert {:ok, created_parent} =
             flow_create_and_get(parent,
               type: "parent",
               state: "dispatch",
               partition_key: partition,
               now_ms: 2_000
             )

    assert {:ok, waiting} =
             flow_spawn_children_and_get(
               parent,
               [
                 %{id: child_a, type: "child", state: "queued"},
                 %{id: child_b, type: "child", state: "queued"}
               ],
               group_id: "fanout-1",
               wait: :all,
               wait_state: "waiting_children",
               on_child_failed: :fail_parent,
               on_parent_closed: :cancel_children,
               exhaust_to: %{success: "children_done", failure: "failed"},
               partition_key: partition,
               from_state: "dispatch",
               fencing_token: created_parent.fencing_token,
               now_ms: 2_010
             )

    assert waiting.state == "waiting_children"
    assert waiting.child_groups["fanout-1"]["summary"]["completed"] == 0

    claimed_a = create_claimed_flow_child(child_a, partition, "worker-a")

    assert {:ok, _child_done_a} =
             flow_complete_and_get(child_a, claimed_a.lease_token,
               partition_key: partition,
               fencing_token: claimed_a.fencing_token,
               result: "child-a-result",
               now_ms: 2_020
             )

    assert {:ok, still_waiting} = FerricStore.flow_get(parent, partition_key: partition)
    assert still_waiting.state == "waiting_children"
    assert still_waiting.child_groups["fanout-1"]["summary"]["completed"] == 1
    child_a_result = still_waiting.child_groups["fanout-1"]["results"][child_a]
    assert child_a_result["status"] == "completed"
    assert is_binary(child_a_result["result_ref"])

    claimed_b = create_claimed_flow_child(child_b, partition, "worker-b")

    assert {:ok, _child_done_b} =
             flow_complete_and_get(child_b, claimed_b.lease_token,
               partition_key: partition,
               fencing_token: claimed_b.fencing_token,
               now_ms: 2_030
             )

    assert {:ok, done_parent} = FerricStore.flow_get(parent, partition_key: partition)
    assert done_parent.state == "children_done"
    assert done_parent.child_groups["fanout-1"]["resolved"] == "success"
    assert done_parent.child_groups["fanout-1"]["summary"]["completed"] == 2
  end

  test "flow_spawn_children rejects missing wait_state when waiting for children" do
    parent = uid("flow-parent-missing-wait")
    child = uid("flow-child-missing-wait")
    partition = uid("tenant")

    assert {:ok, created_parent} =
             flow_create_and_get(parent,
               type: "parent",
               state: "dispatch",
               partition_key: partition
             )

    assert {:error, "ERR flow wait_state is required when waiting for children"} =
             flow_spawn_children_and_get(
               parent,
               [%{id: child, type: "child"}],
               group_id: "fanout-1",
               wait: :all,
               on_child_failed: :fail_parent,
               on_parent_closed: :cancel_children,
               exhaust_to: %{success: "children_done", failure: "failed"},
               partition_key: partition,
               from_state: "dispatch",
               fencing_token: created_parent.fencing_token
             )

    assert {:ok, nil} = FerricStore.flow_get(child, partition_key: partition)

    assert {:ok, unchanged_parent} = FerricStore.flow_get(parent, partition_key: partition)
    assert unchanged_parent.state == "dispatch"
    assert unchanged_parent.child_groups == %{}
  end

  test "flow_spawn_children rejects empty wait_state for running parent" do
    parent = uid("flow-parent-running-empty-wait")
    child = uid("flow-child-running-empty-wait")
    partition = uid("tenant")

    claimed_parent =
      create_claimed_flow(parent, partition, "parent-running-empty-wait", "parent-worker")

    assert {:error, "ERR flow wait_state is required when waiting for children"} =
             flow_spawn_children_and_get(
               parent,
               [%{id: child, type: "child"}],
               group_id: "fanout-1",
               wait: :all,
               wait_state: "",
               on_child_failed: :fail_parent,
               on_parent_closed: :cancel_children,
               exhaust_to: %{success: "children_done", failure: "failed"},
               partition_key: partition,
               lease_token: claimed_parent.lease_token,
               fencing_token: claimed_parent.fencing_token
             )

    assert {:ok, nil} = FerricStore.flow_get(child, partition_key: partition)

    assert {:ok, still_running} = FerricStore.flow_get(parent, partition_key: partition)
    assert still_running.state == "running"
    assert still_running.lease_token == claimed_parent.lease_token
  end

  test "stale child terminal command does not double count parent child group" do
    parent = uid("flow-parent-stale-child")
    child_a = uid("flow-child-stale-a")
    child_b = uid("flow-child-stale-b")
    partition = uid("tenant")

    assert {:ok, created_parent} =
             flow_create_and_get(parent,
               type: "parent",
               state: "dispatch",
               partition_key: partition,
               now_ms: 3_000
             )

    assert {:ok, _waiting} =
             flow_spawn_children_and_get(
               parent,
               [%{id: child_a, type: "child"}, %{id: child_b, type: "child"}],
               group_id: "fanout-1",
               wait: :all,
               wait_state: "waiting_children",
               on_child_failed: :fail_parent,
               on_parent_closed: :cancel_children,
               exhaust_to: %{success: "children_done", failure: "failed"},
               partition_key: partition,
               from_state: "dispatch",
               fencing_token: created_parent.fencing_token,
               now_ms: 3_010
             )

    claimed_a = create_claimed_flow_child(child_a, partition, "worker-a")

    assert {:ok, _child_done_a} =
             flow_complete_and_get(child_a, claimed_a.lease_token,
               partition_key: partition,
               fencing_token: claimed_a.fencing_token,
               now_ms: 3_020
             )

    assert {:ok, after_first} = FerricStore.flow_get(parent, partition_key: partition)
    assert after_first.child_groups["fanout-1"]["summary"]["completed"] == 1
    parent_version = after_first.version

    assert {:error, _reason} =
             flow_complete_and_get(child_a, claimed_a.lease_token,
               partition_key: partition,
               fencing_token: claimed_a.fencing_token,
               now_ms: 3_030
             )

    assert {:ok, after_stale} = FerricStore.flow_get(parent, partition_key: partition)
    assert after_stale.version == parent_version
    assert after_stale.child_groups["fanout-1"]["summary"]["completed"] == 1
    assert after_stale.child_groups["fanout-1"]["children"][child_a] == "completed"
    assert after_stale.child_groups["fanout-1"]["children"][child_b] == "running"
  end

  test "nested child completion resolves parent and propagates to grandparent" do
    grandparent = uid("flow-grandparent")
    middle = uid("flow-middle-parent")
    leaf = uid("flow-leaf-child")
    partition = uid("tenant")

    assert {:ok, created_grandparent} =
             flow_create_and_get(grandparent,
               type: "parent",
               state: "dispatch",
               partition_key: partition,
               now_ms: 4_000
             )

    assert {:ok, _waiting_grandparent} =
             flow_spawn_children_and_get(
               grandparent,
               [%{id: middle, type: "child", state: "dispatch"}],
               group_id: "outer",
               wait: :all,
               wait_state: "waiting_middle",
               on_child_failed: :fail_parent,
               on_parent_closed: :cancel_children,
               exhaust_to: %{success: "completed", failure: "failed"},
               partition_key: partition,
               from_state: "dispatch",
               fencing_token: created_grandparent.fencing_token,
               now_ms: 4_010
             )

    assert {:ok, middle_record} = FerricStore.flow_get(middle, partition_key: partition)

    assert {:ok, _waiting_middle} =
             flow_spawn_children_and_get(
               middle,
               [%{id: leaf, type: "child"}],
               group_id: "inner",
               wait: :all,
               wait_state: "waiting_leaf",
               on_child_failed: :fail_parent,
               on_parent_closed: :cancel_children,
               exhaust_to: %{success: "completed", failure: "failed"},
               partition_key: partition,
               from_state: "dispatch",
               fencing_token: middle_record.fencing_token,
               now_ms: 4_020
             )

    claimed_leaf = create_claimed_flow_child(leaf, partition, "worker-leaf")

    assert {:ok, _leaf_done} =
             flow_complete_and_get(leaf, claimed_leaf.lease_token,
               partition_key: partition,
               fencing_token: claimed_leaf.fencing_token,
               now_ms: 4_030
             )

    assert {:ok, resolved_middle} = FerricStore.flow_get(middle, partition_key: partition)
    assert resolved_middle.state == "completed"
    assert resolved_middle.child_groups["inner"]["resolved"] == "success"

    assert {:ok, resolved_grandparent} =
             FerricStore.flow_get(grandparent, partition_key: partition)

    assert resolved_grandparent.state == "completed"
    assert resolved_grandparent.child_groups["outer"]["resolved"] == "success"
    assert resolved_grandparent.child_groups["outer"]["children"][middle] == "completed"
    assert resolved_grandparent.child_groups["outer"]["summary"]["completed"] == 1
  end

  test "flow_spawn_children can fail parent on child failure or ignore terminal failures" do
    fail_parent = uid("flow-parent-child-fails")
    ignore_parent = uid("flow-parent-child-ignore")
    fail_child = uid("flow-child-fails")
    ignore_child = uid("flow-child-ignore")
    partition = uid("tenant")

    assert {:ok, fail_created} =
             flow_create_and_get(fail_parent,
               type: "parent",
               state: "dispatch",
               partition_key: partition
             )

    assert {:ok, _} =
             flow_spawn_children_and_get(
               fail_parent,
               [%{id: fail_child, type: "child"}],
               group_id: "fanout",
               wait: :all,
               wait_state: "waiting_children",
               on_child_failed: :fail_parent,
               on_parent_closed: :cancel_children,
               exhaust_to: %{success: "children_done", failure: "children_failed"},
               partition_key: partition,
               from_state: "dispatch",
               fencing_token: fail_created.fencing_token
             )

    failed_claim = create_claimed_flow_child(fail_child, partition, "worker-fail")

    assert {:ok, _failed_child} =
             flow_fail_and_get(fail_child, failed_claim.lease_token,
               partition_key: partition,
               fencing_token: failed_claim.fencing_token,
               error: "boom"
             )

    assert {:ok, failed_parent} = FerricStore.flow_get(fail_parent, partition_key: partition)
    assert failed_parent.state == "children_failed"
    assert failed_parent.child_groups["fanout"]["resolved"] == "failure"
    fail_result = failed_parent.child_groups["fanout"]["results"][fail_child]
    assert fail_result["status"] == "failed"
    assert is_binary(fail_result["error_ref"])

    assert {:ok, ignore_created} =
             flow_create_and_get(ignore_parent,
               type: "parent",
               state: "dispatch",
               partition_key: partition
             )

    assert {:ok, _} =
             flow_spawn_children_and_get(
               ignore_parent,
               [%{id: ignore_child, type: "child"}],
               group_id: "fanout",
               wait: :all,
               wait_state: "waiting_children",
               on_child_failed: :ignore,
               on_parent_closed: :abandon_children,
               exhaust_to: %{success: "children_done", failure: "children_failed"},
               partition_key: partition,
               from_state: "dispatch",
               fencing_token: ignore_created.fencing_token
             )

    ignore_claim = create_claimed_flow_child(ignore_child, partition, "worker-ignore")

    assert {:ok, _ignored_child} =
             flow_fail_and_get(ignore_child, ignore_claim.lease_token,
               partition_key: partition,
               fencing_token: ignore_claim.fencing_token,
               error: "ignored"
             )

    assert {:ok, ignored_parent} = FerricStore.flow_get(ignore_parent, partition_key: partition)
    assert ignored_parent.state == "children_done"
    assert ignored_parent.child_groups["fanout"]["resolved"] == "success"
    assert ignored_parent.child_groups["fanout"]["summary"]["failed"] == 1
  end

  test "flow_cancel parent cancels direct running children when configured" do
    parent = uid("flow-parent-cancel-children")
    child_a = uid("flow-child-cancel-a")
    child_b = uid("flow-child-cancel-b")
    partition = uid("tenant")

    assert {:ok, created_parent} =
             flow_create_and_get(parent,
               type: "parent",
               state: "dispatch",
               partition_key: partition
             )

    assert {:ok, waiting} =
             flow_spawn_children_and_get(
               parent,
               [%{id: child_a, type: "child"}, %{id: child_b, type: "child"}],
               group_id: "fanout",
               wait: :all,
               wait_state: "waiting_children",
               on_child_failed: :fail_parent,
               on_parent_closed: :cancel_children,
               exhaust_to: %{success: "children_done", failure: "failed"},
               partition_key: partition,
               from_state: "dispatch",
               fencing_token: created_parent.fencing_token
             )

    assert {:ok, _cancelled_parent} =
             flow_cancel_and_get(parent,
               partition_key: partition,
               fencing_token: waiting.fencing_token,
               reason: "parent closed"
             )

    assert {:ok, child_a_record} = FerricStore.flow_get(child_a, partition_key: partition)
    assert {:ok, child_b_record} = FerricStore.flow_get(child_b, partition_key: partition)
    assert child_a_record.state == "cancelled"
    assert child_b_record.state == "cancelled"

    assert {:ok, final_parent} = FerricStore.flow_get(parent, partition_key: partition)
    assert final_parent.child_groups["fanout"]["resolved"] == "failure"
    assert final_parent.child_groups["fanout"]["summary"]["cancelled"] == 2
    assert final_parent.child_groups["fanout"]["results"][child_a]["status"] == "cancelled"
    assert final_parent.child_groups["fanout"]["results"][child_b]["status"] == "cancelled"
  end

  test "flow_spawn_children is idempotent by group id and rejects conflicts" do
    parent = uid("flow-parent-idempotent-children")
    child = uid("flow-child-idempotent")
    other_child = uid("flow-child-idempotent-conflict")
    partition = uid("tenant")

    assert {:ok, created_parent} =
             flow_create_and_get(parent,
               type: "parent",
               state: "dispatch",
               partition_key: partition
             )

    opts = [
      group_id: "fanout",
      wait: :all,
      wait_state: "waiting_children",
      on_child_failed: :ignore,
      on_parent_closed: :abandon_children,
      exhaust_to: %{success: "children_done", failure: "children_failed"},
      partition_key: partition,
      from_state: "dispatch",
      fencing_token: created_parent.fencing_token
    ]

    assert {:ok, first} =
             flow_spawn_children_and_get(parent, [%{id: child, type: "child"}], opts)

    assert {:ok, same} =
             flow_spawn_children_and_get(parent, [%{id: child, type: "child"}], opts)

    assert same.id == first.id
    assert same.version == first.version

    assert {:error, "ERR flow child group idempotency conflict"} =
             flow_spawn_children_and_get(
               parent,
               [%{id: other_child, type: "child"}],
               opts
             )
  end

  test "flow_spawn_children remains idempotent after child progress" do
    parent = uid("flow-parent-idempotent-progress")
    child_a = uid("flow-child-idempotent-progress-a")
    child_b = uid("flow-child-idempotent-progress-b")
    partition = uid("tenant")

    assert {:ok, created_parent} =
             flow_create_and_get(parent,
               type: "parent",
               state: "dispatch",
               partition_key: partition
             )

    opts = [
      group_id: "fanout",
      wait: :all,
      wait_state: "waiting_children",
      on_child_failed: :ignore,
      on_parent_closed: :abandon_children,
      exhaust_to: %{success: "children_done", failure: "children_failed"},
      partition_key: partition,
      from_state: "dispatch",
      fencing_token: created_parent.fencing_token
    ]

    assert {:ok, waiting} =
             flow_spawn_children_and_get(
               parent,
               [%{id: child_a, type: "child"}, %{id: child_b, type: "child"}],
               opts
             )

    claimed_a = create_claimed_flow_child(child_a, partition, "worker-a")

    assert {:ok, _child_done_a} =
             flow_complete_and_get(child_a, claimed_a.lease_token,
               partition_key: partition,
               fencing_token: claimed_a.fencing_token
             )

    assert {:ok, same} =
             flow_spawn_children_and_get(
               parent,
               [%{id: child_a, type: "child"}, %{id: child_b, type: "child"}],
               opts
             )

    assert same.id == waiting.id
    assert same.child_groups["fanout"]["summary"]["completed"] == 1
    assert same.child_groups["fanout"]["children"][child_a] == "completed"
  end

  test "flow_spawn_children rejects new groups on terminal parents" do
    parent = uid("flow-parent-terminal-spawn")
    child = uid("flow-child-terminal-spawn")
    partition = uid("tenant")

    assert {:ok, created_parent} =
             flow_create_and_get(parent,
               type: "parent",
               state: "dispatch",
               partition_key: partition
             )

    assert {:ok, cancelled_parent} =
             flow_cancel_and_get(parent,
               partition_key: partition,
               fencing_token: created_parent.fencing_token
             )

    assert cancelled_parent.state == "cancelled"

    assert {:error, "ERR flow parent is terminal"} =
             flow_spawn_children_and_get(
               parent,
               [%{id: child, type: "child"}],
               group_id: "fanout",
               wait: :all,
               wait_state: "waiting_children",
               on_child_failed: :ignore,
               on_parent_closed: :abandon_children,
               exhaust_to: %{success: "children_done", failure: "children_failed"},
               partition_key: partition,
               from_state: "cancelled",
               fencing_token: cancelled_parent.fencing_token
             )
  end

  test "child group resolution cancels other open child groups when parent closes" do
    parent = uid("flow-parent-close-cancels-groups")
    failing_child = uid("flow-child-close-failing")
    sibling_child = uid("flow-child-close-sibling")
    partition = uid("tenant")

    assert {:ok, created_parent} =
             flow_create_and_get(parent,
               type: "parent",
               state: "dispatch",
               partition_key: partition
             )

    assert {:ok, waiting_first} =
             flow_spawn_children_and_get(
               parent,
               [%{id: failing_child, type: "child"}],
               group_id: "fanout-a",
               wait: :all,
               wait_state: "waiting_children",
               on_child_failed: :fail_parent,
               on_parent_closed: :cancel_children,
               exhaust_to: %{success: "children_done", failure: "failed"},
               partition_key: partition,
               from_state: "dispatch",
               fencing_token: created_parent.fencing_token
             )

    assert {:ok, _waiting_second} =
             flow_spawn_children_and_get(
               parent,
               [%{id: sibling_child, type: "child"}],
               group_id: "fanout-b",
               wait: :all,
               wait_state: "waiting_children",
               on_child_failed: :ignore,
               on_parent_closed: :cancel_children,
               exhaust_to: %{success: "children_done", failure: "failed"},
               partition_key: partition,
               from_state: "waiting_children",
               fencing_token: waiting_first.fencing_token
             )

    failed_claim = create_claimed_flow_child(failing_child, partition, "worker-fail")

    assert {:ok, _failed_child} =
             flow_fail_and_get(failing_child, failed_claim.lease_token,
               partition_key: partition,
               fencing_token: failed_claim.fencing_token,
               error: "boom"
             )

    assert {:ok, closed_parent} = FerricStore.flow_get(parent, partition_key: partition)
    assert closed_parent.state == "failed"
    assert closed_parent.child_groups["fanout-a"]["resolved"] == "failure"
    assert closed_parent.child_groups["fanout-b"]["children"][sibling_child] == "cancelled"
    assert closed_parent.child_groups["fanout-b"]["resolved"] == "failure"

    assert {:ok, cancelled_sibling} =
             FerricStore.flow_get(sibling_child, partition_key: partition)

    assert cancelled_sibling.state == "cancelled"
  end

  test "flow_spawn_children supports child partition overrides across shards" do
    parent = uid("flow-parent-cross-partition")
    child = uid("flow-child-cross-partition")
    {partition, _same_partition, other_partition} = mixed_partition_keys()

    assert {:ok, created_parent} =
             flow_create_and_get(parent,
               type: "parent",
               state: "dispatch",
               partition_key: partition
             )

    assert {:ok, waiting} =
             flow_spawn_children_and_get(
               parent,
               [%{id: child, type: "child", partition_key: other_partition}],
               group_id: "fanout",
               wait: :all,
               wait_state: "waiting_children",
               on_child_failed: :ignore,
               on_parent_closed: :abandon_children,
               exhaust_to: %{success: "children_done", failure: "children_failed"},
               partition_key: partition,
               from_state: "dispatch",
               fencing_token: created_parent.fencing_token
             )

    assert waiting.state == "waiting_children"
    assert waiting.child_groups["fanout"]["children"][child] == "running"
    assert waiting.child_groups["fanout"]["child_partitions"][child] == other_partition

    assert {:ok, child_record} = FerricStore.flow_get(child, partition_key: other_partition)
    assert child_record.parent_flow_id == parent
    assert child_record.parent_partition_key == partition

    claimed = create_claimed_flow_child(child, other_partition, "worker-cross")

    assert {:ok, _child_done} =
             flow_complete_and_get(child, claimed.lease_token,
               partition_key: other_partition,
               fencing_token: claimed.fencing_token
             )

    assert {:ok, done_parent} = FerricStore.flow_get(parent, partition_key: partition)
    assert done_parent.state == "children_done"
    assert done_parent.child_groups["fanout"]["children"][child] == "completed"
  end

  test "cross-shard child completion survives Ra shard restart without duplicate parent summary" do
    parent = uid("flow-parent-ra-replay")
    child = uid("flow-child-ra-replay")
    {partition, _same_partition, other_partition} = mixed_partition_keys()

    assert {:ok, created_parent} =
             flow_create_and_get(parent,
               type: "parent",
               state: "dispatch",
               partition_key: partition
             )

    assert {:ok, _waiting} =
             flow_spawn_children_and_get(
               parent,
               [%{id: child, type: "child", partition_key: other_partition}],
               group_id: "fanout",
               wait: :all,
               wait_state: "waiting_children",
               on_child_failed: :ignore,
               on_parent_closed: :abandon_children,
               exhaust_to: %{success: "children_done", failure: "children_failed"},
               partition_key: partition,
               from_state: "dispatch",
               fencing_token: created_parent.fencing_token
             )

    claimed = create_claimed_flow_child(child, other_partition, "worker-ra-replay")

    assert {:ok, _completed_child} =
             flow_complete_and_get(child, claimed.lease_token,
               partition_key: other_partition,
               fencing_token: claimed.fencing_token,
               result: "ok"
             )

    assert {:ok, resolved_parent} = FerricStore.flow_get(parent, partition_key: partition)
    assert resolved_parent.state == "children_done"
    assert resolved_parent.child_groups["fanout"]["summary"]["completed"] == 1

    ShardHelpers.compact_wal()
    ShardHelpers.kill_shard_for_key("f:{flow-cross-shard}:tx", timeout: 30_000)

    assert {:ok, replayed_parent} = FerricStore.flow_get(parent, partition_key: partition)
    assert replayed_parent.state == "children_done"
    assert replayed_parent.child_groups["fanout"]["children"][child] == "completed"
    assert replayed_parent.child_groups["fanout"]["summary"]["completed"] == 1

    assert {:ok, parent_history} = FerricStore.flow_history(parent, partition_key: partition)
    parent_events = Enum.map(parent_history, fn {_event_id, fields} -> fields["event"] end)
    assert Enum.count(parent_events, &(&1 == "child_completed")) == 1
  end

  test "terminal parent cancellation cancels cross-shard children" do
    parent = uid("flow-parent-cross-cancel")
    child = uid("flow-child-cross-cancel")
    {partition, _same_partition, other_partition} = mixed_partition_keys()

    assert {:ok, created_parent} =
             flow_create_and_get(parent,
               type: "parent",
               state: "dispatch",
               partition_key: partition
             )

    assert {:ok, waiting} =
             flow_spawn_children_and_get(
               parent,
               [%{id: child, type: "child", partition_key: other_partition}],
               group_id: "fanout",
               wait: :all,
               wait_state: "waiting_children",
               on_child_failed: :ignore,
               on_parent_closed: :cancel_children,
               exhaust_to: %{success: "children_done", failure: "children_failed"},
               partition_key: partition,
               from_state: "dispatch",
               fencing_token: created_parent.fencing_token
             )

    assert {:ok, _cancelled_parent} =
             flow_cancel_and_get(parent,
               partition_key: partition,
               fencing_token: waiting.fencing_token
             )

    assert {:ok, cancelled_child} = FerricStore.flow_get(child, partition_key: other_partition)
    assert cancelled_child.state == "cancelled"

    assert {:ok, final_parent} = FerricStore.flow_get(parent, partition_key: partition)
    assert final_parent.child_groups["fanout"]["children"][child] == "cancelled"
  end

  test "flow_complete_many resolves cross-shard child groups" do
    parent = uid("flow-parent-cross-complete-many")
    child_a = uid("flow-child-cross-complete-many-a")
    child_b = uid("flow-child-cross-complete-many-b")
    {partition, same_partition, other_partition} = mixed_partition_keys()

    assert {:ok, created_parent} =
             flow_create_and_get(parent,
               type: "parent",
               state: "dispatch",
               partition_key: partition
             )

    assert {:ok, _waiting} =
             flow_spawn_children_and_get(
               parent,
               [
                 %{id: child_a, type: "child", partition_key: same_partition},
                 %{id: child_b, type: "child", partition_key: other_partition}
               ],
               group_id: "fanout",
               wait: :all,
               wait_state: "waiting_children",
               on_child_failed: :ignore,
               on_parent_closed: :abandon_children,
               exhaust_to: %{success: "children_done", failure: "children_failed"},
               partition_key: partition,
               from_state: "dispatch",
               fencing_token: created_parent.fencing_token
             )

    claimed_a = create_claimed_flow_child(child_a, same_partition, "worker-cross-a")
    claimed_b = create_claimed_flow_child(child_b, other_partition, "worker-cross-b")

    assert {:ok, completed} =
             flow_complete_many_and_get(
               nil,
               [
                 %{
                   id: child_a,
                   partition_key: same_partition,
                   lease_token: claimed_a.lease_token,
                   fencing_token: claimed_a.fencing_token
                 },
                 %{
                   id: child_b,
                   partition_key: other_partition,
                   lease_token: claimed_b.lease_token,
                   fencing_token: claimed_b.fencing_token
                 }
               ],
               now_ms: 8_000
             )

    assert Enum.map(completed, & &1.state) == ["completed", "completed"]

    assert {:ok, done_parent} = FerricStore.flow_get(parent, partition_key: partition)
    assert done_parent.state == "children_done"
    assert done_parent.child_groups["fanout"]["resolved"] == "success"
    assert done_parent.child_groups["fanout"]["summary"]["completed"] == 2
  end

  test "Raft routing resolves cross-shard child terminal propagation" do
    previous_mode = Ferricstore.ReplicationMode.current()
    Ferricstore.ReplicationMode.put_current(:raft)

    parent = uid("flow-parent-raft-cross")
    child = uid("flow-child-raft-cross")
    {partition, _same_partition, other_partition} = mixed_partition_keys()

    try do
      assert {:ok, created_parent} =
               flow_create_and_get(parent,
                 type: "parent",
                 state: "dispatch",
                 partition_key: partition
               )

      assert {:ok, _waiting} =
               flow_spawn_children_and_get(
                 parent,
                 [%{id: child, type: "child", partition_key: other_partition}],
                 group_id: "fanout",
                 wait: :all,
                 wait_state: "waiting_children",
                 on_child_failed: :fail_parent,
                 on_parent_closed: :cancel_children,
                 exhaust_to: %{success: "children_done", failure: "children_failed"},
                 partition_key: partition,
                 from_state: "dispatch",
                 fencing_token: created_parent.fencing_token
               )

      claimed = create_claimed_flow_child(child, other_partition, "worker-raft-cross")

      assert {:ok, %{id: ^child, state: "completed"}} =
               flow_complete_and_get(child, claimed.lease_token,
                 partition_key: other_partition,
                 fencing_token: claimed.fencing_token
               )

      assert {:ok, done_parent} = FerricStore.flow_get(parent, partition_key: partition)
      assert done_parent.state == "children_done"
      assert done_parent.child_groups["fanout"]["children"][child] == "completed"
    after
      Ferricstore.ReplicationMode.put_current(previous_mode)
    end
  end

  test "cross-shard nested child completion resolves parent and grandparent" do
    grandparent = uid("flow-grandparent-cross")
    middle = uid("flow-middle-cross")
    leaf = uid("flow-leaf-cross")
    {grand_partition, middle_partition, leaf_partition} = mixed_partition_keys()

    assert {:ok, created_grandparent} =
             flow_create_and_get(grandparent,
               type: "parent",
               state: "dispatch",
               partition_key: grand_partition
             )

    assert {:ok, _waiting_grandparent} =
             flow_spawn_children_and_get(
               grandparent,
               [%{id: middle, type: "child", state: "dispatch", partition_key: middle_partition}],
               group_id: "outer",
               wait: :all,
               wait_state: "waiting_middle",
               on_child_failed: :fail_parent,
               on_parent_closed: :cancel_children,
               exhaust_to: %{success: "completed", failure: "failed"},
               partition_key: grand_partition,
               from_state: "dispatch",
               fencing_token: created_grandparent.fencing_token
             )

    assert {:ok, middle_record} = FerricStore.flow_get(middle, partition_key: middle_partition)

    assert {:ok, _waiting_middle} =
             flow_spawn_children_and_get(
               middle,
               [%{id: leaf, type: "child", partition_key: leaf_partition}],
               group_id: "inner",
               wait: :all,
               wait_state: "waiting_leaf",
               on_child_failed: :fail_parent,
               on_parent_closed: :cancel_children,
               exhaust_to: %{success: "completed", failure: "failed"},
               partition_key: middle_partition,
               from_state: "dispatch",
               fencing_token: middle_record.fencing_token
             )

    claimed_leaf = create_claimed_flow_child(leaf, leaf_partition, "worker-cross-leaf")

    assert {:ok, _leaf_done} =
             flow_complete_and_get(leaf, claimed_leaf.lease_token,
               partition_key: leaf_partition,
               fencing_token: claimed_leaf.fencing_token
             )

    assert {:ok, resolved_middle} = FerricStore.flow_get(middle, partition_key: middle_partition)
    assert resolved_middle.state == "completed"
    assert resolved_middle.child_groups["inner"]["resolved"] == "success"

    assert {:ok, resolved_grandparent} =
             FerricStore.flow_get(grandparent, partition_key: grand_partition)

    assert resolved_grandparent.state == "completed"
    assert resolved_grandparent.child_groups["outer"]["children"][middle] == "completed"
  end

  test "flow_spawn_children wait any resolves on first successful child across shards" do
    parent = uid("flow-parent-any-cross")
    child_a = uid("flow-child-any-cross-a")
    child_b = uid("flow-child-any-cross-b")
    {partition, same_partition, other_partition} = mixed_partition_keys()

    assert {:ok, created_parent} =
             flow_create_and_get(parent,
               type: "parent",
               state: "dispatch",
               partition_key: partition
             )

    assert {:ok, waiting} =
             flow_spawn_children_and_get(
               parent,
               [
                 %{id: child_a, type: "child", partition_key: same_partition},
                 %{id: child_b, type: "child", partition_key: other_partition}
               ],
               group_id: "fanout",
               wait: :any,
               wait_state: "waiting_children",
               on_child_failed: :ignore,
               on_parent_closed: :cancel_children,
               exhaust_to: %{success: "completed", failure: "children_failed"},
               partition_key: partition,
               from_state: "dispatch",
               fencing_token: created_parent.fencing_token
             )

    assert waiting.state == "waiting_children"

    claimed_b = create_claimed_flow_child(child_b, other_partition, "worker-cross-any")

    assert {:ok, _child_done} =
             flow_complete_and_get(child_b, claimed_b.lease_token,
               partition_key: other_partition,
               fencing_token: claimed_b.fencing_token
             )

    assert {:ok, done_parent} = FerricStore.flow_get(parent, partition_key: partition)
    assert done_parent.state == "completed"
    assert done_parent.child_groups["fanout"]["resolved"] == "success"
    assert done_parent.child_groups["fanout"]["children"][child_b] == "completed"

    assert {:ok, cancelled_sibling} = FerricStore.flow_get(child_a, partition_key: same_partition)
    assert cancelled_sibling.state == "cancelled"
  end

  test "flow_spawn_children wait any resolves failure when every child fails" do
    parent = uid("flow-parent-any-all-failed")
    child_a = uid("flow-child-any-all-failed-a")
    child_b = uid("flow-child-any-all-failed-b")
    {partition, same_partition, other_partition} = mixed_partition_keys()

    assert {:ok, created_parent} =
             flow_create_and_get(parent,
               type: "parent",
               state: "dispatch",
               partition_key: partition
             )

    assert {:ok, waiting} =
             flow_spawn_children_and_get(
               parent,
               [
                 %{id: child_a, type: "child", partition_key: same_partition},
                 %{id: child_b, type: "child", partition_key: other_partition}
               ],
               group_id: "fanout",
               wait: :any,
               wait_state: "waiting_children",
               on_child_failed: :ignore,
               on_parent_closed: :abandon_children,
               exhaust_to: %{success: "completed", failure: "children_failed"},
               partition_key: partition,
               from_state: "dispatch",
               fencing_token: created_parent.fencing_token
             )

    assert waiting.state == "waiting_children"

    claimed_a = create_claimed_flow_child(child_a, same_partition, "worker-cross-any-fail-a")
    claimed_b = create_claimed_flow_child(child_b, other_partition, "worker-cross-any-fail-b")

    assert {:ok, failed_children} =
             flow_fail_many_and_get(
               nil,
               [
                 %{
                   id: child_a,
                   partition_key: same_partition,
                   lease_token: claimed_a.lease_token,
                   fencing_token: claimed_a.fencing_token
                 },
                 %{
                   id: child_b,
                   partition_key: other_partition,
                   lease_token: claimed_b.lease_token,
                   fencing_token: claimed_b.fencing_token
                 }
               ],
               error: "all failed"
             )

    assert Enum.map(failed_children, & &1.state) == ["failed", "failed"]

    assert {:ok, failed_parent} = FerricStore.flow_get(parent, partition_key: partition)
    assert failed_parent.state == "children_failed"
    assert failed_parent.child_groups["fanout"]["resolved"] == "failure"
    assert failed_parent.child_groups["fanout"]["summary"]["failed"] == 2
  end

  test "flow_fail_many fail_parent policy closes parent and cancels cross-shard siblings" do
    parent = uid("flow-parent-cross-fail-many")
    failed_child = uid("flow-child-cross-fail-many-failed")
    sibling = uid("flow-child-cross-fail-many-sibling")
    {partition, same_partition, other_partition} = mixed_partition_keys()

    assert {:ok, created_parent} =
             flow_create_and_get(parent,
               type: "parent",
               state: "dispatch",
               partition_key: partition
             )

    assert {:ok, _waiting} =
             flow_spawn_children_and_get(
               parent,
               [
                 %{id: failed_child, type: "child", partition_key: other_partition},
                 %{id: sibling, type: "child", partition_key: same_partition}
               ],
               group_id: "fanout",
               wait: :all,
               wait_state: "waiting_children",
               on_child_failed: :fail_parent,
               on_parent_closed: :cancel_children,
               exhaust_to: %{success: "children_done", failure: "children_failed"},
               partition_key: partition,
               from_state: "dispatch",
               fencing_token: created_parent.fencing_token
             )

    claimed_failed = create_claimed_flow_child(failed_child, other_partition, "worker-cross-fail")

    assert {:ok, [%{id: ^failed_child, state: "failed"}]} =
             flow_fail_many_and_get(
               nil,
               [
                 %{
                   id: failed_child,
                   partition_key: other_partition,
                   lease_token: claimed_failed.lease_token,
                   fencing_token: claimed_failed.fencing_token
                 }
               ],
               error: "boom"
             )

    assert {:ok, failed_parent} = FerricStore.flow_get(parent, partition_key: partition)
    assert failed_parent.state == "children_failed"
    assert failed_parent.child_groups["fanout"]["resolved"] == "failure"
    assert failed_parent.child_groups["fanout"]["children"][failed_child] == "failed"
    assert failed_parent.child_groups["fanout"]["children"][sibling] == "cancelled"

    assert {:ok, cancelled_sibling} = FerricStore.flow_get(sibling, partition_key: same_partition)
    assert cancelled_sibling.state == "cancelled"

    assert {:ok, parent_history} = FerricStore.flow_history(parent, partition_key: partition)

    parent_events = Enum.map(parent_history, fn {_event_id, fields} -> fields["event"] end)
    assert "child_failed" in parent_events
    assert "children_cancelled" in parent_events
  end

  test "flow_create_many spans shards and rolls back failing shard group" do
    {same_a, same_b, other} = mixed_partition_keys()
    type = uid("bulk-mixed-create")
    existing_id = uid("bulk-mixed-existing")
    same_new_id = uid("bulk-mixed-same")
    other_new_id = uid("bulk-mixed-other")

    assert {:ok, _} =
             flow_create_and_get(existing_id,
               type: type,
               partition_key: same_a,
               run_at_ms: 1_000
             )

    assert {:ok, results} =
             flow_create_many_and_get(
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

    assert :ok =
             FerricStore.flow_create(id,
               type: "observability",
               state: "queued",
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert_receive {:flow_telemetry, [:ferricstore, :flow, :create, :stop],
                    %{duration_ms: duration_ms, count: 1},
                    %{flow_id: ^id, flow_type: "observability", result: :ok, reason: nil}}

    assert is_integer(duration_ms) and duration_ms >= 0

    refute_receive {:pubsub_message, ^changed_channel, _message}, 50
    refute_receive {:pubsub_message, ^due_channel, _message}, 50
  end

  test "flow APIs reject malformed inputs before raft apply" do
    assert {:error, "ERR flow id must be a non-empty string"} =
             flow_create_and_get("", type: "checkout")

    assert {:error, "ERR flow opts must be a keyword list"} =
             flow_create_and_get("bad-opts", ["checkout"])

    assert {:error, "ERR flow type is required"} =
             flow_create_and_get("missing-type", state: "queued")

    assert {:error, "ERR flow type must be a non-empty string"} =
             flow_create_and_get("empty-type", type: "")

    assert {:error, "ERR flow now_ms must be a non-negative integer"} =
             flow_create_and_get("bad-now", type: "checkout", now_ms: -1)

    assert {:error, "ERR flow run_at_ms must be a non-negative integer"} =
             flow_create_and_get("bad-run-at", type: "checkout", run_at_ms: -1)

    assert {:error, "ERR flow priority must be between 0 and 2"} =
             flow_create_and_get("bad-priority", type: "checkout", priority: 3)

    assert {:error, "ERR flow partition_key must be a non-empty string or :global"} =
             flow_create_and_get("bad-partition", type: "checkout", partition_key: "")

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
             flow_complete_and_get("flow", "")

    assert {:error, "ERR flow fencing_token is required"} =
             flow_complete_and_get("flow", "token")

    assert {:error, "ERR flow now_ms must be a non-negative integer"} =
             flow_retry_and_get("flow", "token", fencing_token: 0, now_ms: -1)

    assert {:error, "ERR flow id must be a non-empty string"} =
             FerricStore.flow_history("")

    assert {:error, "ERR flow opts must be a keyword list"} =
             FerricStore.flow_history("flow", ["bad"])

    assert {:error, "ERR flow count must be a positive integer"} =
             FerricStore.flow_history("flow", count: 0)

    assert {:error, "ERR flow count exceeds maximum 10000"} =
             FerricStore.flow_history("flow", count: 10_001)

    assert {:error, "ERR flow id must be a non-empty string"} =
             flow_transition_and_get("", "queued", "done")

    assert {:error, "ERR flow from must be a non-empty string"} =
             flow_transition_and_get("flow", "", "done")

    assert {:error, "ERR flow to must be a non-empty string"} =
             flow_transition_and_get("flow", "queued", "")

    assert {:error, "ERR flow opts must be a keyword list"} =
             flow_transition_and_get("flow", "queued", "done", ["bad"])

    assert {:error, "ERR flow lease_token must be a non-empty string"} =
             flow_transition_and_get("flow", "queued", "done", lease_token: "")

    assert {:error, "ERR flow fencing_token is required"} =
             flow_transition_and_get("flow", "queued", "done")

    assert {:error, "ERR flow lease_token must be a non-empty string"} =
             flow_fail_and_get("flow", "")

    assert {:error, "ERR flow fencing_token is required"} =
             flow_fail_and_get("flow", "token")

    assert {:error, "ERR flow now_ms must be a non-negative integer"} =
             flow_fail_and_get("flow", "token", fencing_token: 0, now_ms: -1)

    assert {:error, "ERR flow id must be a non-empty string"} =
             flow_cancel_and_get("")

    assert {:error, "ERR flow opts must be a keyword list"} =
             flow_cancel_and_get("flow", ["bad"])

    assert {:error, "ERR flow fencing_token is required"} =
             flow_cancel_and_get("flow")

    assert {:error, "ERR flow partition_key must be a non-empty string or :global"} =
             FerricStore.flow_claim_due("email", worker: "worker-a", partition_key: "")

    large_id = String.duplicate("x", 65_536)

    assert {:error, "ERR key too large" <> _} =
             flow_create_and_get(large_id, type: "checkout")

    assert {:ok, payload_ref_flow} =
             flow_create_and_get("payload-ref-input", type: "checkout", payload_ref: "p")

    assert payload_ref_flow.payload_ref == "p"

    assert {:error, "ERR flow payload_ref cannot be used with payload"} =
             flow_create_and_get("payload-ref-conflict",
               type: "checkout",
               payload_ref: "p",
               payload: "inline"
             )

    assert {:error, "ERR flow result_ref input is not supported; use result"} =
             flow_complete_and_get("flow", "token", fencing_token: 0, result_ref: "r")

    assert {:error, "ERR flow error_ref input is not supported; use error"} =
             flow_retry_and_get("flow", "token", fencing_token: 0, error_ref: "e")

    assert {:error, "ERR flow error_ref input is not supported; use error"} =
             flow_fail_and_get("flow", "token", fencing_token: 0, error_ref: "e")

    assert {:error, "ERR flow reason_ref input is not supported; use reason"} =
             flow_cancel_and_get("flow", fencing_token: 0, reason_ref: "external")

    assert {:error, "ERR flow reason_ref input is not supported; use reason"} =
             flow_cancel_and_get("flow",
               fencing_token: 0,
               reason: "inline",
               reason_ref: "external"
             )

    assert {:error, "ERR flow reason_ref input is not supported; use reason"} =
             FerricStore.flow_rewind("flow", to_event: "1-1", reason_ref: "external")
  end

  test "flow_claim_due atomically leases due flows and removes them from due set" do
    id = uid("flow-claim")

    assert {:ok, _} =
             flow_create_and_get(id,
               type: "email",
               state: "queued",
               payload: "payload:" <> id,
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
               flow_create_and_get(id,
                 type: type,
                 state: "queued",
                 payload: "payload:" <> id,
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
    ctx = FerricStore.Instance.get(:default)

    range_ids = fn index_key ->
      shard_index = Ferricstore.Store.Router.shard_for(ctx, index_key)

      {flow_index, flow_lookup} =
        Ferricstore.Flow.OrderedIndex.table_names(ctx.name, shard_index)

      flow_index
      |> Ferricstore.Flow.NativeOrderedIndex.get(flow_lookup)
      |> Ferricstore.Flow.NativeOrderedIndex.range_slice(
        index_key,
        :neg_inf,
        :inf,
        false,
        0,
        :all
      )
      |> Enum.map(fn {member, _score} -> member end)
    end

    assert {:ok, created} = flow_create_and_get(id, type: type, run_at_ms: 1_000)
    assert Ferricstore.Flow.Keys.auto_partition_key?(created.partition_key)

    queued_index = Ferricstore.Flow.Keys.state_index_key(type, "queued", created.partition_key)
    running_index = Ferricstore.Flow.Keys.state_index_key(type, "running", created.partition_key)

    completed_index =
      Ferricstore.Flow.Keys.state_index_key(type, "completed", created.partition_key)

    inflight_index = Ferricstore.Flow.Keys.inflight_index_key(type, created.partition_key)
    worker_index = Ferricstore.Flow.Keys.worker_index_key(worker, created.partition_key)

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
             flow_retry_and_get(id, first_claim.lease_token,
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
             flow_complete_and_get(id, second_claim.lease_token,
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

    assert {:ok, _} = flow_create_and_get(due_id, type: type, run_at_ms: 2_000)
    assert {:ok, _} = flow_create_and_get(running_id, type: type, run_at_ms: 1_000)
    assert {:ok, _} = flow_create_and_get(done_id, type: type, run_at_ms: 1_000)

    assert {:ok, [claimed_running, claimed_done]} =
             FerricStore.flow_claim_due(type,
               worker: "worker-ops",
               lease_ms: 50,
               limit: 2,
               now_ms: 1_000
             )

    assert {:ok, _} =
             flow_complete_and_get(claimed_done.id, claimed_done.lease_token,
               fencing_token: claimed_done.fencing_token
             )

    assert {:ok, queued} = FerricStore.flow_list(type, state: "queued", count: 10)
    assert Enum.map(queued, & &1.id) == [due_id]

    assert {:ok, running} = FerricStore.flow_list(type, state: "running", count: 10)
    assert Enum.map(running, & &1.id) == [claimed_running.id]

    assert {:ok, completed} = FerricStore.flow_list(type, state: "completed", count: 10)
    assert Enum.map(completed, & &1.id) == [claimed_done.id]

    assert {:ok, terminals} = FerricStore.flow_terminals(type, state: "completed", count: 10)
    assert Enum.map(terminals, & &1.id) == [claimed_done.id]

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
             flow_create_and_get(id,
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
             flow_complete_and_get(claimed.id, claimed.lease_token,
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
             flow_create_and_get(id,
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
             flow_create_and_get(id,
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
             flow_create_and_get(id,
               type: type,
               partition_key: partition,
               run_at_ms: 1_000,
               now_ms: 900
             )

    due_key = Ferricstore.Flow.Keys.due_key(type, "queued", 0, partition)
    shard_index = Ferricstore.Store.Router.shard_for(ctx, due_key)
    {flow_index, flow_lookup} = Ferricstore.Flow.OrderedIndex.table_names(ctx.name, shard_index)
    assert native = Ferricstore.Flow.NativeOrderedIndex.get(flow_index, flow_lookup)

    {zset_index, zset_lookup} =
      Ferricstore.Store.Shard.ZSetIndex.table_names(ctx.name, shard_index)

    assert [{^id, 1000.0}] =
             Ferricstore.Flow.NativeOrderedIndex.range_slice(
               native,
               due_key,
               :neg_inf,
               {:inclusive, 1_000.0},
               false,
               0,
               10
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

    running_due_key = Ferricstore.Flow.Keys.due_key(type, "running", 0, partition)

    assert [] =
             Ferricstore.Flow.NativeOrderedIndex.range_slice(
               native,
               due_key,
               :neg_inf,
               {:inclusive, 1_000.0},
               false,
               0,
               10
             )

    assert [{^id, 31_000.0}] =
             Ferricstore.Flow.NativeOrderedIndex.range_slice(
               native,
               running_due_key,
               :neg_inf,
               {:inclusive, 31_000.0},
               false,
               0,
               10
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

    assert {:ok, []} =
             FerricStore.flow_claim_due(type,
               partition_key: partition,
               worker: "worker-native-index-2",
               limit: 1,
               now_ms: 1_000,
               lease_ms: 30_000
             )

    inflight_key = Ferricstore.Flow.Keys.inflight_index_key(type, partition)
    assert 0 = Ferricstore.Flow.OrderedIndex.count_all(flow_lookup, inflight_key)
    assert 1 = Ferricstore.Flow.NativeOrderedIndex.count_all(native, inflight_key)
  end

  test "flow_claim_due validates record due time when native due score is stale" do
    ctx = FerricStore.Instance.get(:default)
    partition = uid("tenant-native-stale-score")
    type = uid("native-stale-score")
    id = uid("flow-native-stale-score")

    assert {:ok, _} =
             flow_create_and_get(id,
               type: type,
               partition_key: partition,
               run_at_ms: 10_000,
               now_ms: 1_000
             )

    due_key = Ferricstore.Flow.Keys.due_key(type, "queued", 0, partition)
    shard_index = Ferricstore.Store.Router.shard_for(ctx, due_key)
    {flow_index, flow_lookup} = Ferricstore.Flow.OrderedIndex.table_names(ctx.name, shard_index)
    assert native = Ferricstore.Flow.NativeOrderedIndex.get(flow_index, flow_lookup)

    assert :ok = Ferricstore.Flow.NativeOrderedIndex.put_member(native, due_key, id, 1_000)

    assert {:ok, []} =
             FerricStore.flow_claim_due(type,
               partition_key: partition,
               worker: "worker-native-stale-score",
               limit: 1,
               now_ms: 1_000,
               lease_ms: 30_000
             )

    assert {:ok, [%{id: ^id, state: "running"}]} =
             FerricStore.flow_claim_due(type,
               partition_key: partition,
               worker: "worker-native-stale-score",
               limit: 1,
               now_ms: 10_000,
               lease_ms: 30_000
             )
  end

  test "flow_claim_due keeps metadata-indexed flows claimable through generic fallback" do
    partition = uid("tenant-claim-metadata")
    type = uid("claim-metadata")
    id = uid("flow-claim-metadata")
    correlation_id = uid("correlation")

    assert {:ok, _} =
             flow_create_and_get(id,
               type: type,
               partition_key: partition,
               correlation_id: correlation_id,
               run_at_ms: 1_000,
               now_ms: 900
             )

    assert {:ok, [%{id: ^id, state: "running", correlation_id: ^correlation_id}]} =
             FerricStore.flow_claim_due(type,
               partition_key: partition,
               worker: "worker-claim-metadata",
               limit: 1,
               now_ms: 1_000,
               lease_ms: 30_000
             )

    assert {:ok, []} =
             FerricStore.flow_claim_due(type,
               partition_key: partition,
               worker: "worker-claim-metadata-2",
               limit: 1,
               now_ms: 1_000,
               lease_ms: 30_000
             )
  end

  test "flow_claim_due restores native overfetch beyond requested limit" do
    partition = uid("tenant-native-overfetch")
    type = uid("native-overfetch")
    ids = for i <- 1..3, do: uid("flow-native-overfetch-#{i}")

    for id <- ids do
      assert {:ok, _} =
               flow_create_and_get(id,
                 type: type,
                 partition_key: partition,
                 run_at_ms: 1_000,
                 now_ms: 900
               )
    end

    assert {:ok, [%{id: first_id}]} =
             FerricStore.flow_claim_due(type,
               partition_key: partition,
               worker: "worker-native-overfetch-1",
               limit: 1,
               now_ms: 1_000,
               lease_ms: 30_000
             )

    assert first_id in ids

    assert {:ok, [%{id: second_id}]} =
             FerricStore.flow_claim_due(type,
               partition_key: partition,
               worker: "worker-native-overfetch-2",
               limit: 1,
               now_ms: 1_000,
               lease_ms: 30_000
             )

    assert second_id in ids
    assert second_id != first_id
  end

  test "partition_key scopes claim, complete, retry, get, and history" do
    partition = uid("tenant")
    id = uid("flow-partition")

    assert {:ok, flow} =
             flow_create_and_get(id,
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
             flow_complete_and_get(id, claimed.lease_token, fencing_token: claimed.fencing_token)

    assert {:ok, completed} =
             flow_complete_and_get(id, claimed.lease_token,
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
             flow_create_and_get(id_a,
               type: "email",
               partition_key: partition_a,
               run_at_ms: 1_000
             )

    assert {:ok, _} =
             flow_create_and_get(id_b,
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

  test "flow_claim_due can scan selected partition_keys in one call" do
    partition_a = uid("tenant-a")
    partition_b = uid("tenant-b")
    partition_c = uid("tenant-c")
    type = uid("claim-partition-keys")
    id_a = uid("flow-partition-keys-a")
    id_b = uid("flow-partition-keys-b")
    id_c = uid("flow-partition-keys-c")

    assert {:ok, _} =
             flow_create_and_get(id_a,
               type: type,
               partition_key: partition_a,
               run_at_ms: 1_000
             )

    assert {:ok, _} =
             flow_create_and_get(id_b,
               type: type,
               partition_key: partition_b,
               run_at_ms: 1_000
             )

    assert {:ok, _} =
             flow_create_and_get(id_c,
               type: type,
               partition_key: partition_c,
               run_at_ms: 1_000
             )

    assert {:ok, claimed} =
             FerricStore.flow_claim_due(type,
               partition_keys: [partition_a, partition_b],
               worker: "worker-many-partitions",
               lease_ms: 30_000,
               limit: 10,
               now_ms: 1_000
             )

    assert MapSet.new(Enum.map(claimed, & &1.id)) == MapSet.new([id_a, id_b])
    assert Enum.all?(claimed, &(&1.partition_key in [partition_a, partition_b]))

    assert {:ok, [claimed_c]} =
             FerricStore.flow_claim_due(type,
               partition_key: partition_c,
               worker: "worker-c",
               lease_ms: 30_000,
               limit: 10,
               now_ms: 1_000
             )

    assert claimed_c.id == id_c
  end

  test "flow_create without partition auto-spreads and default claim_due fetches across buckets" do
    type = uid("claim-auto-bucket")

    ids =
      1..64
      |> Enum.map(fn i -> uid("flow-auto-bucket-#{i}") end)

    shards =
      ids
      |> Enum.map(fn id -> shard_for(Ferricstore.Flow.Keys.state_key(id)) end)
      |> MapSet.new()

    assert MapSet.size(shards) > 1

    Enum.each(ids, fn id ->
      assert {:ok, flow} =
               flow_create_and_get(id,
                 type: type,
                 run_at_ms: 1_000
               )

      assert flow.id == id
      assert Ferricstore.Flow.Keys.auto_partition_key?(flow.partition_key)
    end)

    claimed =
      1..16
      |> Enum.reduce_while([], fn _round, acc ->
        case FerricStore.flow_claim_due(type,
               worker: "worker-auto-bucket",
               lease_ms: 30_000,
               limit: length(ids),
               now_ms: 1_000
             ) do
          {:ok, []} ->
            {:cont, acc}

          {:ok, records} ->
            next = records ++ acc

            if MapSet.new(Enum.map(next, & &1.id)) == MapSet.new(ids) do
              {:halt, next}
            else
              {:cont, next}
            end

          {:error, _reason} = error ->
            flunk("claim_due failed: #{inspect(error)}")
        end
      end)

    assert MapSet.new(Enum.map(claimed, & &1.id)) == MapSet.new(ids)

    assert claimed
           |> Enum.map(& &1.partition_key)
           |> Enum.all?(&Ferricstore.Flow.Keys.auto_partition_key?/1)
  end

  test "flow_claim_due can scan any partition and selected states" do
    {partition_a, partition_b} = different_partition_keys()
    type = uid("claim-any")
    queued_global = uid("flow-claim-any-global")
    queued_partition = uid("flow-claim-any-queued")
    ready_partition = uid("flow-claim-any-ready")
    held_partition = uid("flow-claim-any-held")

    assert {:ok, _} = flow_create_and_get(queued_global, type: type, run_at_ms: 1_000)

    assert {:ok, _} =
             flow_create_and_get(queued_partition,
               type: type,
               partition_key: partition_b,
               run_at_ms: 1_000
             )

    assert {:ok, _} =
             flow_create_and_get(ready_partition,
               type: type,
               state: "ready",
               partition_key: partition_a,
               run_at_ms: 1_000
             )

    assert {:ok, _} =
             flow_create_and_get(held_partition,
               type: type,
               state: "held",
               partition_key: partition_b,
               run_at_ms: 1_000
             )

    assert {:ok, claimed} =
             FerricStore.flow_claim_due(type,
               partition_key: :any,
               states: ["queued", "ready"],
               worker: "worker-any",
               lease_ms: 30_000,
               limit: 10,
               now_ms: 1_000
             )

    assert MapSet.new(Enum.map(claimed, & &1.id)) ==
             MapSet.new([queued_global, queued_partition, ready_partition])

    assert {:ok, [claimed_held]} =
             FerricStore.flow_claim_due(type,
               partition_key: :any,
               state: :any,
               worker: "worker-any",
               lease_ms: 30_000,
               limit: 10,
               now_ms: 1_000
             )

    assert claimed_held.id == held_partition
    assert claimed_held.partition_key == partition_b
  end

  test "flow_claim_due omitted state claims any due state" do
    type = uid("claim-omitted-state-any")
    queued_id = uid("flow-claim-omitted-queued")
    ready_id = uid("flow-claim-omitted-ready")

    assert {:ok, _} = flow_create_and_get(queued_id, type: type, run_at_ms: 1_000)

    assert {:ok, _} =
             flow_create_and_get(ready_id,
               type: type,
               state: "ready",
               run_at_ms: 1_000
             )

    assert {:ok, claimed} =
             FerricStore.flow_claim_due(type,
               worker: "worker-omitted-state-any",
               lease_ms: 30_000,
               limit: 10,
               now_ms: 1_000
             )

    assert MapSet.new(Enum.map(claimed, & &1.id)) == MapSet.new([queued_id, ready_id])
  end

  test "flow_claim_due any partition drains higher priority across shards first" do
    type = uid("claim-any-priority")
    low_id = uid("flow-claim-any-low")
    high_id = uid("flow-claim-any-high")

    case :ets.whereis(:ferricstore_flow_claim_due_any_cursor) do
      :undefined -> :ok
      table -> :ets.delete_all_objects(table)
    end

    partitions_by_shard =
      1..512
      |> Enum.map(&"#{type}:partition:#{&1}")
      |> Enum.group_by(fn partition ->
        shard_for(Ferricstore.Flow.Keys.state_key("probe", partition))
      end)

    [low_partition | _] = Map.fetch!(partitions_by_shard, 0)

    {_high_shard, [high_partition | _]} =
      Enum.find(partitions_by_shard, fn {shard, _partitions} -> shard != 0 end)

    assert {:ok, _} =
             flow_create_and_get(low_id,
               type: type,
               partition_key: low_partition,
               priority: 0,
               run_at_ms: 1_000
             )

    assert {:ok, _} =
             flow_create_and_get(high_id,
               type: type,
               partition_key: high_partition,
               priority: 2,
               run_at_ms: 1_000
             )

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due(type,
               partition_key: :any,
               state: :any,
               worker: "worker-any-priority",
               lease_ms: 30_000,
               limit: 1,
               now_ms: 1_000
             )

    assert claimed.id == high_id
    assert claimed.partition_key == high_partition
  end

  test "flow_claim_due any partition rotates start shard per type" do
    type = uid("claim-any-rotate")

    case :ets.whereis(:ferricstore_flow_claim_due_any_cursor) do
      :undefined -> :ok
      table -> :ets.delete_all_objects(table)
    end

    partitions_by_shard =
      1..512
      |> Enum.map(&"#{type}:partition:#{&1}")
      |> Enum.group_by(fn partition ->
        shard_for(Ferricstore.Flow.Keys.due_key(type, "queued", 0, partition))
      end)

    [partition_0 | _] = Map.fetch!(partitions_by_shard, 0)
    [partition_1 | _] = Map.fetch!(partitions_by_shard, 1)

    for {partition, suffix} <- [
          {partition_0, "0a"},
          {partition_0, "0b"},
          {partition_1, "1a"},
          {partition_1, "1b"}
        ] do
      assert {:ok, _} =
               flow_create_and_get(uid("flow-claim-any-rotate-#{suffix}"),
                 type: type,
                 partition_key: partition,
                 priority: 0,
                 run_at_ms: 1_000
               )
    end

    assert {:ok, [first]} =
             FerricStore.flow_claim_due(type,
               partition_key: :any,
               state: :any,
               worker: "worker-any-rotate",
               lease_ms: 30_000,
               limit: 1,
               now_ms: 1_000
             )

    assert first.partition_key in [partition_0, partition_1]

    assert {:ok, [second]} =
             FerricStore.flow_claim_due(type,
               partition_key: :any,
               state: :any,
               worker: "worker-any-rotate",
               lease_ms: 30_000,
               limit: 1,
               now_ms: 1_000
             )

    assert second.partition_key in [partition_0, partition_1]
    assert second.partition_key != first.partition_key
  end

  test "flow_claim_due any partition spreads small limits across shards under skew" do
    type = uid("claim-any-skew")

    case :ets.whereis(:ferricstore_flow_claim_due_any_cursor) do
      :undefined -> :ok
      table -> :ets.delete_all_objects(table)
    end

    partitions_by_shard =
      1..512
      |> Enum.map(&"#{type}:partition:#{&1}")
      |> Enum.group_by(fn partition ->
        shard_for(Ferricstore.Flow.Keys.due_key(type, "queued", 0, partition))
      end)

    [hot_partition | _] = Map.fetch!(partitions_by_shard, 0)
    [other_partition | _] = Map.fetch!(partitions_by_shard, 1)

    for suffix <- ["hot-a", "hot-b"] do
      assert {:ok, _} =
               flow_create_and_get(uid("flow-claim-any-skew-#{suffix}"),
                 type: type,
                 partition_key: hot_partition,
                 run_at_ms: 1_000
               )
    end

    assert {:ok, _} =
             flow_create_and_get(uid("flow-claim-any-skew-other"),
               type: type,
               partition_key: other_partition,
               run_at_ms: 1_000
             )

    assert {:ok, claimed} =
             FerricStore.flow_claim_due(type,
               partition_key: :any,
               state: :any,
               worker: "worker-any-skew",
               lease_ms: 30_000,
               limit: 2,
               now_ms: 1_000
             )

    assert length(claimed) == 2

    assert MapSet.new(Enum.map(claimed, & &1.partition_key)) ==
             MapSet.new([hot_partition, other_partition])
  end

  test "flow_claim_due skips stale due index members without starving live work" do
    stale_id = "a-" <> uid("flow-stale-due")
    live_id = "z-" <> uid("flow-live-due")

    assert {:ok, _} =
             flow_create_and_get(stale_id, type: "stale-scan", run_at_ms: 1_000)

    assert {:ok, _} =
             flow_create_and_get(live_id, type: "stale-scan", run_at_ms: 1_000)

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
             flow_create_and_get(low_id,
               type: "priority",
               priority: 0,
               run_at_ms: 1_000
             )

    assert {:ok, _} =
             flow_create_and_get(high_id,
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
             flow_create_and_get(low_id,
               type: "priority-target",
               priority: 0,
               run_at_ms: 1_000
             )

    assert {:ok, _} =
             flow_create_and_get(high_id,
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
             flow_create_and_get(id, type: "image", state: "queued", run_at_ms: 1_000)

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due("image",
               state: "queued",
               worker: "worker-a",
               lease_ms: 30_000,
               limit: 1,
               now_ms: 1_000
             )

    assert {:error, "ERR stale flow lease"} =
             flow_complete_and_get(id, "wrong-token",
               fencing_token: claimed.fencing_token,
               result: "result:" <> id
             )

    assert {:error, "ERR stale flow lease"} =
             flow_complete_and_get(id, claimed.lease_token,
               fencing_token: claimed.fencing_token + 1,
               result: "result:" <> id
             )

    assert {:ok, completed} =
             flow_complete_and_get(claimed.id, claimed.lease_token,
               fencing_token: claimed.fencing_token,
               result: "result:" <> id
             )

    assert completed.state == "completed"
    assert is_binary(completed.result_ref)
    assert completed.result_ref != "result:" <> id
    assert completed.lease_token == nil
    assert completed.version == 3
  end

  test "flow_retry clears lease and reschedules flow" do
    id = uid("flow-retry")

    assert {:ok, _} =
             flow_create_and_get(id, type: "webhook", state: "queued", run_at_ms: 1_000)

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due("webhook",
               state: "queued",
               worker: "worker-a",
               lease_ms: 30_000,
               limit: 1,
               now_ms: 1_000
             )

    assert {:error, "ERR stale flow lease"} =
             flow_retry_and_get(claimed.id, claimed.lease_token,
               fencing_token: claimed.fencing_token + 1,
               error: "error:" <> id,
               run_at_ms: 2_000
             )

    assert {:ok, retried} =
             flow_retry_and_get(claimed.id, claimed.lease_token,
               fencing_token: claimed.fencing_token,
               error: "error:" <> id,
               run_at_ms: 2_000
             )

    assert retried.state == "queued"
    assert retried.attempts == 1
    assert is_binary(retried.error_ref)
    assert retried.error_ref != "error:" <> id
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
             flow_create_and_get(id,
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
             flow_retry_and_get(claimed.id, claimed.lease_token,
               fencing_token: claimed.fencing_token,
               now_ms: 2_000,
               retry: [
                 max_retries: 3,
                 backoff: [kind: :exponential, base_ms: 1_000, max_ms: 30_000, jitter_pct: 0],
                 exhausted_to: "payment_failed"
               ]
             )

    assert retried.state == "charge_card"
    assert retried.attempts == 1
    assert retried.next_run_at_ms == 3_000
    assert retried.lease_token == nil

    assert {:ok, history} = FerricStore.flow_history(id, count: 10)

    {_event_id, retry_fields} =
      Enum.find(history, fn {_event_id, fields} -> fields["event"] == "retry" end)

    assert retry_fields["retry_decision"] == "scheduled"
    assert retry_fields["retry_run_state"] == "charge_card"
    assert retry_fields["retry_next_run_at_ms"] == "3000"
    assert retry_fields["retry_max_retries"] == "3"
    assert retry_fields["retry_backoff_kind"] == "exponential"
    assert retry_fields["retry_backoff_base_ms"] == "1000"
    assert retry_fields["retry_backoff_max_ms"] == "30000"
    assert retry_fields["retry_jitter_pct"] == "0"
    assert retry_fields["retry_exhausted_to"] == "payment_failed"

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
             flow_create_and_get(id,
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
             flow_retry_and_get(claimed.id, claimed.lease_token,
               fencing_token: claimed.fencing_token,
               now_ms: 2_000,
               retry: [
                 max_retries: 0,
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

  test "flow_retry terminal exhaustion keeps stable audit metadata" do
    id = uid("flow-retry-terminal-exhaust")

    assert {:ok, _} =
             flow_create_and_get(id,
               type: "payment-terminal-exhaust",
               state: "charge_card",
               run_at_ms: 1_000
             )

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due("payment-terminal-exhaust",
               state: "charge_card",
               worker: "worker-charge",
               limit: 1,
               now_ms: 1_000
             )

    assert {:ok, exhausted} =
             flow_retry_and_get(claimed.id, claimed.lease_token,
               fencing_token: claimed.fencing_token,
               now_ms: 2_000,
               retry: [
                 max_retries: 0,
                 backoff: [kind: :fixed, base_ms: 10_000, max_ms: 10_000, jitter_pct: 0],
                 exhausted_to: "failed"
               ]
             )

    assert exhausted.state == "failed"
    assert exhausted.next_run_at_ms == nil

    assert {:ok, history} = FerricStore.flow_history(id, count: 10)

    {_event_id, retry_fields} =
      Enum.find(history, fn {_event_id, fields} -> fields["event"] == "retry" end)

    assert retry_fields["retry_decision"] == "exhausted"
    assert retry_fields["retry_next_run_at_ms"] == ""
    assert retry_fields["retry_exhausted_to"] == "failed"
  end

  test "flow_retry terminal exhaustion updates cross-shard parent child group" do
    parent = uid("flow-retry-parent-cross")
    child = uid("flow-retry-child-cross")
    {partition, _same_partition, other_partition} = mixed_partition_keys()

    assert {:ok, created_parent} =
             flow_create_and_get(parent,
               type: "parent",
               state: "dispatch",
               partition_key: partition
             )

    assert {:ok, waiting} =
             flow_spawn_children_and_get(
               parent,
               [%{id: child, type: "child", partition_key: other_partition}],
               group_id: "retry-fanout",
               wait: :all,
               wait_state: "waiting_children",
               on_child_failed: :fail_parent,
               on_parent_closed: :abandon_children,
               exhaust_to: %{success: "children_done", failure: "children_failed"},
               partition_key: partition,
               from_state: "dispatch",
               fencing_token: created_parent.fencing_token
             )

    assert waiting.state == "waiting_children"
    claimed = create_claimed_flow_child(child, other_partition, "worker-retry-cross")

    assert {:ok, exhausted_child} =
             flow_retry_and_get(child, claimed.lease_token,
               partition_key: other_partition,
               fencing_token: claimed.fencing_token,
               now_ms: 2_000,
               retry: [max_retries: 0, exhausted_to: "failed"]
             )

    assert exhausted_child.state == "failed"

    assert {:ok, failed_parent} = FerricStore.flow_get(parent, partition_key: partition)
    assert failed_parent.state == "children_failed"
    assert failed_parent.child_groups["retry-fanout"]["children"][child] == "failed"
    assert failed_parent.child_groups["retry-fanout"]["summary"]["failed"] == 1
  end

  test "flow_retry rejects invalid retry policy" do
    id = uid("flow-retry-policy-invalid")

    assert {:ok, _} =
             flow_create_and_get(id, type: "retry-policy-invalid", run_at_ms: 1_000)

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due("retry-policy-invalid",
               worker: "worker-invalid",
               limit: 1,
               now_ms: 1_000
             )

    assert {:error, "ERR flow retry max_retries must be between 0 and 1000"} =
             flow_retry_and_get(claimed.id, claimed.lease_token,
               fencing_token: claimed.fencing_token,
               retry: [max_retries: 1001]
             )

    assert {:error, "ERR flow retry exhausted_to cannot be running"} =
             flow_retry_and_get(claimed.id, claimed.lease_token,
               fencing_token: claimed.fencing_token,
               retry: [exhausted_to: "running"]
             )
  end

  test "flow policy exposes type and state retry and retention defaults" do
    type = uid("flow-policy")

    assert {:ok, policy} =
             FerricStore.flow_policy_set(type,
               retention: [ttl_ms: 60_000, history_hot_max_events: 128, history_max_events: 512],
               retry: [
                 max_retries: 5,
                 backoff: [kind: :fixed, base_ms: 10_000, max_ms: 30_000, jitter_pct: 0],
                 exhausted_to: "failed"
               ],
               states: %{
                 "charge_card" => [
                   retry: [
                     max_retries: 2,
                     exhausted_to: "payment_failed"
                   ],
                   retention: [
                     ttl_ms: 30_000,
                     history_hot_max_events: 64,
                     history_max_events: 256
                   ]
                 ]
               }
             )

    assert policy.retry.max_retries == 5
    assert policy.retention.ttl_ms == 60_000
    assert policy.retention.history_hot_max_events == 128
    assert policy.retention.history_max_events == 512
    assert policy.states["charge_card"].retry.max_retries == 2
    assert policy.states["charge_card"].retry.backoff.kind == :fixed
    assert policy.states["charge_card"].retry.exhausted_to == "payment_failed"
    assert policy.states["charge_card"].retention.ttl_ms == 30_000
    assert policy.states["charge_card"].retention.history_hot_max_events == 64
    assert policy.states["charge_card"].retention.history_max_events == 256

    assert {:ok, state_policy} = FerricStore.flow_policy_get(type, state: "charge_card")
    assert state_policy.retry.max_retries == 2
    assert state_policy.retry.backoff.base_ms == 10_000
    assert state_policy.retry.exhausted_to == "payment_failed"
    assert state_policy.retention.ttl_ms == 30_000
    assert state_policy.retention.history_hot_max_events == 64
    assert state_policy.retention.history_max_events == 256
  end

  test "flow policy is mirrored to LMDB asynchronously" do
    old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)
    Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)

    ctx = FerricStore.Instance.get(:default)
    type = uid("policy-lmdb")
    policy_key = Ferricstore.Flow.Keys.policy_key(type)
    shard_index = Ferricstore.Store.Router.shard_for(ctx, policy_key)

    lmdb_path =
      ctx.data_dir
      |> Ferricstore.DataDir.shard_data_path(shard_index)
      |> Ferricstore.Flow.LMDB.path()

    on_exit(fn ->
      restore_env(:flow_lmdb_mode, old_mode)
      Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, ctx.shard_count)
    end)

    assert {:ok, _policy} =
             FerricStore.flow_policy_set(type,
               retry: [max_retries: 7, exhausted_to: "failed"]
             )

    assert :ok = Ferricstore.Flow.LMDBWriter.flush(ctx.name, shard_index)
    assert {:ok, blob} = Ferricstore.Flow.LMDB.get(lmdb_path, policy_key)

    assert {:ok, encoded_policy} =
             Ferricstore.Flow.LMDB.decode_value(blob, System.system_time(:millisecond))

    assert {:ok, policy} = Ferricstore.Flow.RetryPolicy.decode_flow_policy(encoded_policy)
    assert policy.retry.max_retries == 7
  end

  test "standalone restart rebuilds stored policy and retry uses it" do
    old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)
    Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)

    name = :"flow_policy_restart_#{System.unique_integer([:positive])}"
    data_dir = Path.join(System.tmp_dir!(), "ferricstore_#{name}")
    ctx = start_flow_restart_instance(name, data_dir)

    on_exit(fn ->
      restore_env(:flow_lmdb_mode, old_mode)

      current_ctx =
        try do
          FerricStore.Instance.get(name)
        rescue
          _ -> ctx
        end

      stop_flow_restart_instance(current_ctx, delete?: true)
    end)

    type = uid("policy-restart")
    id = uid("flow-policy-restart")

    assert {:ok, _policy} =
             FerricStore.Impl.flow_policy_set(ctx, type,
               states: %{
                 "charge_card" => [retry: [max_retries: 0, exhausted_to: "payment_failed"]]
               }
             )

    assert {:ok, _flow} =
             impl_flow_create_and_get(ctx, id,
               type: type,
               state: "charge_card",
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, ctx.shard_count)
    stop_flow_restart_instance(ctx, delete?: false)

    restarted = start_flow_restart_instance(name, data_dir)

    assert {:ok, state_policy} =
             FerricStore.Impl.flow_policy_get(restarted, type, state: "charge_card")

    assert state_policy.retry.max_retries == 0

    assert {:ok, [claimed]} =
             FerricStore.Impl.flow_claim_due(restarted, type,
               state: "charge_card",
               worker: "worker-restart",
               lease_ms: 30_000,
               limit: 1,
               now_ms: 1_000
             )

    assert {:ok, retried} =
             impl_flow_retry_and_get(restarted, claimed.id, claimed.lease_token,
               fencing_token: claimed.fencing_token,
               now_ms: 2_000
             )

    assert retried.state == "payment_failed"
    assert retried.next_run_at_ms == 2_000
  end

  test "native claim batch state write survives standalone restart" do
    old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)
    Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)

    name = :"flow_native_claim_restart_#{System.unique_integer([:positive])}"
    data_dir = Path.join(System.tmp_dir!(), "ferricstore_#{name}")
    ctx = start_flow_restart_instance(name, data_dir)

    on_exit(fn ->
      restore_env(:flow_lmdb_mode, old_mode)

      current_ctx =
        try do
          FerricStore.Instance.get(name)
        rescue
          _ -> ctx
        end

      stop_flow_restart_instance(current_ctx, delete?: true)
    end)

    type = uid("native-claim-restart")
    id = uid("flow-native-claim-restart")

    assert {:ok, _flow} =
             impl_flow_create_and_get(ctx, id,
               type: type,
               state: "queued",
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert {:ok, [claimed]} =
             FerricStore.Impl.flow_claim_due(ctx, type,
               state: "queued",
               worker: "worker-native-restart",
               lease_ms: 30_000,
               limit: 1,
               now_ms: 1_000
             )

    assert claimed.id == id
    assert claimed.state == "running"

    assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, ctx.shard_count)
    stop_flow_restart_instance(ctx, delete?: false)

    restarted = start_flow_restart_instance(name, data_dir)

    assert {:ok, restored} = FerricStore.Impl.flow_get(restarted, id)
    assert restored.state == "running"
    assert restored.lease_token == claimed.lease_token
    assert restored.fencing_token == claimed.fencing_token
    assert restored.lease_owner == "worker-native-restart"
  end

  test "standalone restart preserves spawned child group state" do
    name = :"flow_children_restart_#{System.unique_integer([:positive])}"
    data_dir = Path.join(System.tmp_dir!(), "ferricstore_#{name}")
    ctx = start_flow_restart_instance(name, data_dir)

    on_exit(fn ->
      current_ctx =
        try do
          FerricStore.Instance.get(name)
        rescue
          _ -> ctx
        end

      stop_flow_restart_instance(current_ctx, delete?: true)
    end)

    parent = uid("flow-parent-restart")
    child = uid("flow-child-restart")
    partition = uid("tenant-restart")

    assert {:ok, created_parent} =
             impl_flow_create_and_get(ctx, parent,
               type: "parent",
               state: "dispatch",
               partition_key: partition,
               now_ms: 1_000
             )

    assert {:ok, _waiting} =
             impl_flow_spawn_children_and_get(
               ctx,
               parent,
               [%{id: child, type: "child", state: "queued"}],
               group_id: "fanout",
               wait: :all,
               wait_state: "waiting_children",
               on_child_failed: :fail_parent,
               on_parent_closed: :cancel_children,
               exhaust_to: %{success: "children_done", failure: "children_failed"},
               partition_key: partition,
               from_state: "dispatch",
               fencing_token: created_parent.fencing_token,
               now_ms: 1_010
             )

    assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, ctx.shard_count)
    stop_flow_restart_instance(ctx, delete?: false)

    restarted = start_flow_restart_instance(name, data_dir)

    assert {:ok, restored_parent} =
             FerricStore.Impl.flow_get(restarted, parent, partition_key: partition)

    assert restored_parent.state == "waiting_children"
    assert restored_parent.child_groups["fanout"]["children"][child] == "running"
    assert restored_parent.child_groups["fanout"]["resolved"] == nil

    assert {:ok, restored_child} =
             FerricStore.Impl.flow_get(restarted, child, partition_key: partition)

    assert restored_child.parent_flow_id == parent
  end

  test "stored state retry policy drives retry exhaustion without command override" do
    type = uid("flow-policy-state-exhaust")
    id = uid("flow-policy-state-exhaust-id")

    assert {:ok, _policy} =
             FerricStore.flow_policy_set(type,
               retry: [max_retries: 10, exhausted_to: "failed"],
               states: %{
                 "charge_card" => [retry: [max_retries: 0, exhausted_to: "payment_failed"]]
               }
             )

    assert {:ok, _} =
             flow_create_and_get(id, type: type, state: "charge_card", run_at_ms: 1_000)

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due(type,
               state: "charge_card",
               worker: "worker-policy",
               limit: 1,
               now_ms: 1_000
             )

    assert {:ok, exhausted} =
             flow_retry_and_get(claimed.id, claimed.lease_token,
               fencing_token: claimed.fencing_token,
               now_ms: 2_000
             )

    assert exhausted.state == "payment_failed"
    assert exhausted.next_run_at_ms == 2_000
  end

  test "command retry policy overrides stored state policy" do
    type = uid("flow-policy-command-override")
    id = uid("flow-policy-command-override-id")

    assert {:ok, _policy} =
             FerricStore.flow_policy_set(type,
               states: %{
                 "charge_card" => [retry: [max_retries: 0, exhausted_to: "payment_failed"]]
               }
             )

    assert {:ok, _} =
             flow_create_and_get(id, type: type, state: "charge_card", run_at_ms: 1_000)

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due(type,
               state: "charge_card",
               worker: "worker-policy-override",
               limit: 1,
               now_ms: 1_000
             )

    assert {:ok, retried} =
             flow_retry_and_get(claimed.id, claimed.lease_token,
               fencing_token: claimed.fencing_token,
               now_ms: 2_000,
               retry: [
                 max_retries: 3,
                 backoff: [kind: :fixed, base_ms: 5_000, max_ms: 5_000, jitter_pct: 0],
                 exhausted_to: "needs_review"
               ]
             )

    assert retried.state == "charge_card"
    assert retried.next_run_at_ms == 7_000
  end

  test "flow retry policy accepts thirty day backoff cap" do
    assert {:ok, policy} =
             Ferricstore.Flow.RetryPolicy.normalize(
               max_retries: 1000,
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

  test "flow retry policy computes standard backoff timing" do
    assert {:ok, fixed} =
             Ferricstore.Flow.RetryPolicy.normalize(
               max_retries: 3,
               backoff: [kind: :fixed, base_ms: 100, max_ms: 1_000, jitter_pct: 0],
               exhausted_to: "failed"
             )

    assert Ferricstore.Flow.RetryPolicy.next_run_at_ms(fixed, "flow-a", 1, 1_000) == 1_100
    assert Ferricstore.Flow.RetryPolicy.next_run_at_ms(fixed, "flow-a", 3, 1_000) == 1_100

    assert {:ok, linear} =
             Ferricstore.Flow.RetryPolicy.normalize(
               max_retries: 3,
               backoff: [kind: :linear, base_ms: 100, max_ms: 1_000, jitter_pct: 0],
               exhausted_to: "failed"
             )

    assert Ferricstore.Flow.RetryPolicy.next_run_at_ms(linear, "flow-a", 1, 1_000) == 1_100
    assert Ferricstore.Flow.RetryPolicy.next_run_at_ms(linear, "flow-a", 2, 1_000) == 1_200
    assert Ferricstore.Flow.RetryPolicy.next_run_at_ms(linear, "flow-a", 3, 1_000) == 1_300

    assert {:ok, exponential} =
             Ferricstore.Flow.RetryPolicy.normalize(
               max_retries: 3,
               backoff: [kind: :exponential, base_ms: 100, max_ms: 250, jitter_pct: 0],
               exhausted_to: "failed"
             )

    assert Ferricstore.Flow.RetryPolicy.next_run_at_ms(exponential, "flow-a", 1, 1_000) ==
             1_100

    assert Ferricstore.Flow.RetryPolicy.next_run_at_ms(exponential, "flow-a", 2, 1_000) ==
             1_200

    assert Ferricstore.Flow.RetryPolicy.next_run_at_ms(exponential, "flow-a", 3, 1_000) ==
             1_250

    assert {:ok, jittered} =
             Ferricstore.Flow.RetryPolicy.normalize(
               max_retries: 3,
               backoff: [kind: :exponential, base_ms: 100, max_ms: 250, jitter_pct: 100],
               exhausted_to: "failed"
             )

    next_run = Ferricstore.Flow.RetryPolicy.next_run_at_ms(jittered, "flow-a", 3, 1_000)
    assert next_run >= 1_000
    assert next_run <= 1_250
  end

  test "flow retry policy keeps generated backoff values bounded and deterministic" do
    now_ms = 123_456
    attempts = [0, 1, 2, 3, 10, 1_000]
    base_values = [0, 1, 100, 60_000, 2_592_000_000]
    max_values = [0, 1, 50, 1_000, 2_592_000_000]

    for kind <- [:none, :fixed, :linear, :exponential],
        base_ms <- base_values,
        max_ms <- max_values,
        jitter_pct <- [0, 25, 100],
        attempt <- attempts do
      assert {:ok, policy} =
               Ferricstore.Flow.RetryPolicy.normalize(
                 max_retries: 1_000,
                 backoff: [
                   kind: kind,
                   base_ms: base_ms,
                   max_ms: max_ms,
                   jitter_pct: jitter_pct
                 ],
                 exhausted_to: "failed"
               )

      first = Ferricstore.Flow.RetryPolicy.next_run_at_ms(policy, "flow-prop", attempt, now_ms)
      second = Ferricstore.Flow.RetryPolicy.next_run_at_ms(policy, "flow-prop", attempt, now_ms)

      assert first == second
      assert first >= now_ms
      assert first <= now_ms + max_ms
    end

    assert Ferricstore.Flow.RetryPolicy.attempt_allowed?(
             Ferricstore.Flow.RetryPolicy.default(),
             3
           )

    refute Ferricstore.Flow.RetryPolicy.attempt_allowed?(
             Ferricstore.Flow.RetryPolicy.default(),
             4
           )

    assert {:ok, no_retries} =
             Ferricstore.Flow.RetryPolicy.normalize(max_retries: 0, exhausted_to: "failed")

    refute Ferricstore.Flow.RetryPolicy.attempt_allowed?(no_retries, 1)
  end

  test "flow retry policy rejects old max_attempts name" do
    assert {:error, "ERR flow retry max_attempts was renamed to max_retries"} =
             Ferricstore.Flow.RetryPolicy.normalize(
               max_attempts: 3,
               backoff: [kind: :fixed, base_ms: 100, max_ms: 1_000, jitter_pct: 0],
               exhausted_to: "failed"
             )
  end

  test "flow retry policy decoder rejects old stored max_attempts key" do
    blob =
      :erlang.term_to_binary(
        {:flow_policy_v1,
         %{
           type: "checkout",
           states: %{"charge_card" => %{retry: %{max_attempts: 2}}},
           retry: %{max_attempts: 5}
         }}
      )

    assert :error = Ferricstore.Flow.RetryPolicy.decode_flow_policy(blob)
  end

  test "flow_retry_many atomically reschedules one-partition batch" do
    partition = uid("tenant-retry-many")
    type = uid("bulk-retry-many")
    id_a = uid("retry-many-a")
    id_b = uid("retry-many-b")

    assert {:ok, _} =
             flow_create_many_and_get(
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
             flow_retry_many_and_get(partition, items,
               error: "retry-error",
               run_at_ms: 2_000,
               now_ms: 2_000
             )

    assert Enum.map(retried, & &1.id) == Enum.map(items, & &1.id)
    assert Enum.all?(retried, &(&1.state == "queued"))
    assert Enum.all?(retried, &(&1.attempts == 1))
    assert Enum.all?(retried, &(is_binary(&1.error_ref) and &1.error_ref != "retry-error"))

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
             flow_create_many_and_get(
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
             flow_retry_many_and_get(partition, items, now_ms: 2_000, run_at_ms: 2_000)

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
             flow_create_many_and_get(
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

    assert {:error, "ERR flow retry max_retries must be between 0 and 1000"} =
             flow_retry_many_and_get(partition, items,
               retry: [max_retries: 1001],
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
             flow_create_and_get(id, type: "lease", state: "queued", run_at_ms: 1_000)

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
             flow_create_and_get(id, type: "lease-api", state: "queued", run_at_ms: 1_000)

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

  test "flow_extend_lease extends running lease with fencing guards" do
    type = uid("lease-extend")
    id = uid("flow-lease-extend")

    assert {:ok, _} = flow_create_and_get(id, type: type, run_at_ms: 1_000)

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due(type,
               worker: "worker-a",
               lease_ms: 50,
               limit: 1,
               now_ms: 1_000
             )

    assert {:ok, extended} =
             FerricStore.flow_extend_lease(id, claimed.lease_token,
               fencing_token: claimed.fencing_token,
               lease_ms: 500,
               now_ms: 1_020
             )

    assert extended.state == "running"
    assert extended.version == claimed.version + 1
    assert extended.lease_token == claimed.lease_token
    assert extended.fencing_token == claimed.fencing_token
    assert extended.lease_deadline_ms == 1_520

    assert {:ok, []} =
             FerricStore.flow_reclaim(type,
               worker: "worker-b",
               lease_ms: 50,
               limit: 1,
               now_ms: 1_100
             )

    assert {:error, "ERR stale flow lease"} =
             FerricStore.flow_extend_lease(id, claimed.lease_token,
               fencing_token: claimed.fencing_token + 1,
               lease_ms: 500,
               now_ms: 1_030
             )
  end

  test "expired lease reclaim fences old worker and records takeover history" do
    type = uid("lease-reclaim-fence")
    id = uid("flow-reclaim-fence")

    assert {:ok, _} = flow_create_and_get(id, type: type, run_at_ms: 1_000)

    assert {:ok, [first]} =
             FerricStore.flow_claim_due(type,
               worker: "worker-a",
               lease_ms: 50,
               limit: 1,
               now_ms: 1_000
             )

    assert {:ok, [second]} =
             FerricStore.flow_reclaim(type,
               worker: "worker-b",
               lease_ms: 500,
               limit: 1,
               now_ms: 1_050
             )

    assert second.id == id
    assert second.lease_owner == "worker-b"
    assert second.fencing_token == first.fencing_token + 1
    assert second.lease_token != first.lease_token

    assert {:error, "ERR stale flow lease"} =
             flow_complete_and_get(id, first.lease_token,
               fencing_token: first.fencing_token,
               now_ms: 1_060
             )

    assert {:error, "ERR stale flow lease"} =
             FerricStore.flow_extend_lease(id, first.lease_token,
               fencing_token: first.fencing_token,
               lease_ms: 500,
               now_ms: 1_060
             )

    assert {:ok, history} = FerricStore.flow_history(id, count: 10)

    assert history
           |> Enum.map(fn {_event_id, fields} -> fields["event"] end)
           |> Enum.frequencies() == %{"created" => 1, "claimed" => 2}

    assert Enum.any?(history, fn {_event_id, fields} ->
             fields["event"] == "claimed" and fields["lease_owner"] == "worker-b"
           end)
  end

  test "flow_extend_lease rejects terminal flow" do
    type = uid("lease-extend-terminal")
    id = uid("flow-lease-extend-terminal")

    assert {:ok, _} = flow_create_and_get(id, type: type, run_at_ms: 1_000)

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due(type,
               worker: "worker-a",
               lease_ms: 50,
               limit: 1,
               now_ms: 1_000
             )

    assert {:ok, _completed} =
             flow_complete_and_get(id, claimed.lease_token,
               fencing_token: claimed.fencing_token,
               now_ms: 1_010
             )

    assert {:error, "ERR stale flow lease"} =
             FerricStore.flow_extend_lease(id, claimed.lease_token,
               fencing_token: claimed.fencing_token,
               lease_ms: 500,
               now_ms: 1_020
             )
  end

  test "flow_claim_due automatically reclaims expired leases by ratio" do
    type = uid("lease-ratio")
    expired_ids = Enum.map(1..4, &uid("flow-expired-#{&1}"))
    fresh_ids = Enum.map(1..4, &uid("flow-fresh-#{&1}"))

    for id <- expired_ids do
      assert {:ok, _} = flow_create_and_get(id, type: type, run_at_ms: 1_000)
    end

    assert {:ok, expired_first_claim} =
             FerricStore.flow_claim_due(type,
               worker: "worker-a",
               lease_ms: 50,
               limit: 4,
               now_ms: 1_000
             )

    assert MapSet.new(Enum.map(expired_first_claim, & &1.id)) == MapSet.new(expired_ids)

    for id <- fresh_ids do
      assert {:ok, _} = flow_create_and_get(id, type: type, run_at_ms: 1_050)
    end

    assert {:ok, claimed} =
             FerricStore.flow_claim_due(type,
               worker: "worker-b",
               lease_ms: 50,
               limit: 4,
               now_ms: 1_050,
               reclaim_ratio: 50
             )

    reclaimed = Enum.filter(claimed, &(&1.version == 3))
    fresh = Enum.filter(claimed, &(&1.version == 2))

    assert length(reclaimed) == 2
    assert length(fresh) == 2
    assert Enum.all?(reclaimed, &(&1.id in expired_ids))
    assert Enum.all?(fresh, &(&1.id in fresh_ids))
  end

  test "flow_claim_due can disable automatic expired lease reclaim" do
    type = uid("lease-no-auto-reclaim")
    expired_id = uid("flow-expired")
    fresh_id = uid("flow-fresh")

    assert {:ok, _} = flow_create_and_get(expired_id, type: type, run_at_ms: 1_000)

    assert {:ok, [_]} =
             FerricStore.flow_claim_due(type,
               worker: "worker-a",
               lease_ms: 50,
               limit: 1,
               now_ms: 1_000
             )

    assert {:ok, _} = flow_create_and_get(fresh_id, type: type, run_at_ms: 1_050)

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due(type,
               worker: "worker-b",
               lease_ms: 50,
               limit: 1,
               now_ms: 1_050,
               reclaim_expired: false
             )

    assert claimed.id == fresh_id
    assert claimed.version == 2
  end

  test "flow_claim_due scans past skipped candidates on the same due key" do
    type = uid("claim-skip-scan")
    partition_key = uid("tenant-skip-scan")

    stale_ids =
      for idx <- 1..32 do
        id = uid("flow-stale-#{idx}")

        assert {:ok, _} =
                 flow_create_and_get(id,
                   type: type,
                   partition_key: partition_key,
                   run_at_ms: 1_000
                 )

        id
      end

    assert {:ok, stale_claims} =
             FerricStore.flow_claim_due(type,
               state: :any,
               worker: "worker-a",
               partition_key: partition_key,
               lease_ms: 1,
               limit: 32,
               now_ms: 1_000
             )

    assert Enum.sort(Enum.map(stale_claims, & &1.id)) == Enum.sort(stale_ids)

    fresh_id = uid("flow-fresh")

    assert {:ok, _} =
             flow_create_and_get(fresh_id,
               type: type,
               partition_key: partition_key,
               run_at_ms: 1_050
             )

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due(type,
               state: :any,
               worker: "worker-b",
               partition_key: partition_key,
               lease_ms: 50,
               limit: 1,
               now_ms: 1_050,
               reclaim_expired: false
             )

    assert claimed.id == fresh_id
  end

  test "flow_claim_due accepts partition_keys for any state" do
    type = uid("claim-any-partitions")
    partition_a = uid("tenant-a")
    partition_b = uid("tenant-b")
    id_a = uid("flow-a")
    id_b = uid("flow-b")

    assert {:ok, _} =
             flow_create_and_get(id_a,
               type: type,
               partition_key: partition_a,
               run_at_ms: 1_000
             )

    assert {:ok, _} =
             flow_create_and_get(id_b,
               type: type,
               partition_key: partition_b,
               run_at_ms: 1_000
             )

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due(type,
               state: :any,
               worker: "worker-a",
               partition_keys: [partition_a],
               limit: 1,
               now_ms: 1_000
             )

    assert claimed.id == id_a
    assert claimed.partition_key == partition_a
  end

  test "flow_claim_due scans past skipped candidates across partition_keys" do
    type = uid("claim-skip-scan-partitions")
    partition_a = uid("tenant-stale")
    partition_b = uid("tenant-fresh")

    stale_ids =
      for idx <- 1..32 do
        id = uid("flow-stale-partition-#{idx}")

        assert {:ok, _} =
                 flow_create_and_get(id,
                   type: type,
                   partition_key: partition_a,
                   run_at_ms: 1_000
                 )

        id
      end

    assert {:ok, stale_claims} =
             FerricStore.flow_claim_due(type,
               state: :any,
               worker: "worker-a",
               partition_keys: [partition_a],
               lease_ms: 1,
               limit: 32,
               now_ms: 1_000
             )

    assert Enum.sort(Enum.map(stale_claims, & &1.id)) == Enum.sort(stale_ids)

    fresh_id = uid("flow-fresh-partition")

    assert {:ok, _} =
             flow_create_and_get(fresh_id,
               type: type,
               partition_key: partition_b,
               run_at_ms: 1_050
             )

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due(type,
               state: :any,
               worker: "worker-b",
               partition_keys: [partition_a, partition_b],
               lease_ms: 50,
               limit: 1,
               now_ms: 1_050,
               reclaim_expired: false
             )

    assert claimed.id == fresh_id
    assert claimed.partition_key == partition_b
  end

  test "flow_claim_due partition_keys does not spend all quota on an empty shard group" do
    type = uid("claim-partition-quota")
    state = "queued"
    priority = 0
    base = uid("tenant-quota")

    groups =
      1..256
      |> Enum.map(&"#{base}:#{&1}")
      |> Enum.group_by(fn partition_key ->
        type
        |> Ferricstore.Flow.Keys.due_key(state, priority, partition_key)
        |> shard_for()
      end)

    {empty_shard, [empty_partition | _]} = Enum.min_by(groups, fn {shard, _keys} -> shard end)

    {_due_shard, [due_partition | _]} =
      Enum.find(groups, fn {shard, _keys} -> shard > empty_shard end)

    id = uid("flow-quota-due")

    assert {:ok, _} =
             flow_create_and_get(id,
               type: type,
               state: state,
               partition_key: due_partition,
               run_at_ms: 1_000
             )

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due(type,
               state: state,
               worker: "worker-quota",
               partition_keys: [empty_partition, due_partition],
               limit: 1,
               now_ms: 1_000
             )

    assert claimed.id == id
    assert claimed.partition_key == due_partition
  end

  test "flow_claim_due partition_keys does not lease more records than returned across shard groups" do
    type = uid("claim-partition-overclaim")
    state = "queued"
    priority = 0
    base = uid("tenant-overclaim")

    groups =
      1..256
      |> Enum.map(&"#{base}:#{&1}")
      |> Enum.group_by(fn partition_key ->
        type
        |> Ferricstore.Flow.Keys.due_key(state, priority, partition_key)
        |> shard_for()
      end)
      |> Enum.sort_by(fn {shard, _keys} -> shard end)

    [{_shard_a, [partition_a | _]} | rest] = groups
    {_shard_b, [partition_b | _]} = Enum.find(rest, fn {_shard, keys} -> keys != [] end)

    id_a = uid("flow-overclaim-a")
    id_b = uid("flow-overclaim-b")

    assert {:ok, _} =
             flow_create_and_get(id_a,
               type: type,
               state: state,
               partition_key: partition_a,
               run_at_ms: 1_000
             )

    assert {:ok, _} =
             flow_create_and_get(id_b,
               type: type,
               state: state,
               partition_key: partition_b,
               run_at_ms: 1_000
             )

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due(type,
               state: state,
               worker: "worker-overclaim",
               partition_keys: [partition_a, partition_b],
               limit: 1,
               now_ms: 1_000
             )

    hidden_id = if claimed.id == id_a, do: id_b, else: id_a

    assert {:ok, [second_claim]} =
             FerricStore.flow_claim_due(type,
               state: state,
               worker: "worker-overclaim-second",
               partition_keys: [partition_a, partition_b],
               limit: 1,
               now_ms: 1_001
             )

    assert second_claim.id == hidden_id
  end

  test "expired running lease reclaim is partition scoped" do
    partition_a = uid("tenant-reclaim-a")
    partition_b = uid("tenant-reclaim-b")
    type = uid("lease-partition")
    id_a = uid("flow-reclaim-a")
    id_b = uid("flow-reclaim-b")

    assert {:ok, _} =
             flow_create_and_get(id_a,
               type: type,
               state: "queued",
               partition_key: partition_a,
               run_at_ms: 1_000
             )

    assert {:ok, _} =
             flow_create_and_get(id_b,
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
             flow_create_and_get(id,
               type: "checkout",
               state: "payment_pending",
               run_at_ms: 1_000,
               now_ms: 900
             )

    assert {:ok, transitioned} =
             flow_transition_and_get(id, "payment_pending", "email_pending",
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
             flow_create_many_and_get(
               partition,
               [%{id: id_a}, %{id: id_b}],
               type: type,
               state: "queued",
               run_at_ms: 1_000,
               now_ms: 900
             )

    assert {:ok, transitioned} =
             flow_transition_many_and_get(
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

  test "flow_transition_many allows same id in different partitions" do
    type = uid("bulk-transition-same-id")
    id = uid("same-id-transition")
    partition_a = uid("same-id-transition-a")
    partition_b = uid("same-id-transition-b")

    assert {:ok, [_created_a, _created_b]} =
             flow_create_many_and_get(
               nil,
               [
                 %{id: id, partition_key: partition_a},
                 %{id: id, partition_key: partition_b}
               ],
               type: type,
               state: "queued",
               run_at_ms: 1_000,
               now_ms: 900
             )

    assert {:ok, [transitioned_a, transitioned_b]} =
             flow_transition_many_and_get(
               nil,
               "queued",
               "ready",
               [
                 %{id: id, partition_key: partition_a, fencing_token: 0},
                 %{id: id, partition_key: partition_b, fencing_token: 0}
               ],
               run_at_ms: 2_000,
               now_ms: 1_100
             )

    assert transitioned_a.partition_key == partition_a
    assert transitioned_a.state == "ready"
    assert transitioned_b.partition_key == partition_b
    assert transitioned_b.state == "ready"
  end

  test "flow_transition_many rolls back when any item fails guard" do
    partition = uid("tenant-transition-rollback")
    type = uid("bulk-transition-rollback")
    id_a = uid("transition-good")
    id_b = uid("transition-bad")

    assert {:ok, _} =
             flow_create_many_and_get(
               partition,
               [%{id: id_a}, %{id: id_b}],
               type: type,
               state: "queued",
               run_at_ms: 1_000
             )

    assert {:error, "ERR stale flow lease"} =
             flow_transition_many_and_get(
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

  test "flow_transition_many independent keeps successful items when one item fails" do
    partition = uid("tenant-transition-many-independent")
    type = uid("bulk-transition-independent")
    id_a = uid("transition-independent-bad")
    id_b = uid("transition-independent-good")

    assert {:ok, _} =
             flow_create_many_and_get(
               partition,
               [%{id: id_a}, %{id: id_b}],
               type: type,
               state: "queued",
               run_at_ms: 1_000
             )

    assert {:ok, [{:error, "ERR stale flow lease"}, :ok]} =
             FerricStore.flow_transition_many(
               partition,
               "queued",
               "ready",
               [
                 %{id: id_a, fencing_token: 1},
                 %{id: id_b, fencing_token: 0}
               ],
               run_at_ms: 2_000,
               independent: true
             )

    assert {:ok, %{state: "queued"}} = FerricStore.flow_get(id_a, partition_key: partition)
    assert {:ok, %{state: "ready"}} = FerricStore.flow_get(id_b, partition_key: partition)
  end

  test "flow_transition_many spans shards and rolls back failing shard group" do
    {same_a, same_b, other} = mixed_partition_keys()
    type = uid("bulk-mixed-transition")
    bad_id = uid("transition-mixed-bad")
    same_id = uid("transition-mixed-same")
    other_id = uid("transition-mixed-other")

    for {id, partition} <- [{bad_id, same_a}, {same_id, same_b}, {other_id, other}] do
      assert {:ok, _} =
               flow_create_and_get(id,
                 type: type,
                 partition_key: partition,
                 state: "queued",
                 run_at_ms: 1_000
               )
    end

    assert {:ok, results} =
             flow_transition_many_and_get(
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
             flow_create_many_and_get(
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
             flow_complete_many_and_get(partition, items,
               result: "result-batch",
               now_ms: 2_000
             )

    assert Enum.map(completed, & &1.id) == Enum.map(items, & &1.id)
    assert Enum.all?(completed, &(&1.state == "completed"))
    assert Enum.all?(completed, &(is_binary(&1.result_ref) and &1.result_ref != "result-batch"))

    assert {:ok, info} = FerricStore.flow_info(type, partition_key: partition)
    assert info.completed == 2
  end

  test "flow_complete_many rolls back when any item fails guard" do
    partition = uid("tenant-complete-many-rollback")
    type = uid("bulk-complete-many-rollback")
    id_a = uid("complete-many-good")
    id_b = uid("complete-many-bad")

    assert {:ok, _} =
             flow_create_many_and_get(
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
             flow_complete_many_and_get(partition, items, now_ms: 2_000)

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
             flow_complete_many_and_get(
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
             flow_create_many_and_get(
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
             flow_fail_many_and_get(partition, items,
               error: "error-batch",
               now_ms: 2_000
             )

    assert Enum.map(failed, & &1.id) == Enum.map(items, & &1.id)
    assert Enum.all?(failed, &(&1.state == "failed"))
    assert Enum.all?(failed, &(is_binary(&1.error_ref) and &1.error_ref != "error-batch"))

    assert {:ok, info} = FerricStore.flow_info(type, partition_key: partition)
    assert info.failed == 2
  end

  test "flow_fail_many rolls back when any item fails guard" do
    partition = uid("tenant-fail-many-rollback")
    type = uid("bulk-fail-many-rollback")
    id_a = uid("fail-many-good")
    id_b = uid("fail-many-bad")

    assert {:ok, _} =
             flow_create_many_and_get(
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
             flow_fail_many_and_get(partition, items, now_ms: 2_000)

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
             flow_retry_many_and_get(
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

  test "flow_retry_many terminal exhaustion updates cross-shard parent child group" do
    parent = uid("flow-retry-many-parent-cross")
    child = uid("flow-retry-many-child-cross")
    {partition, _same_partition, other_partition} = mixed_partition_keys()

    assert {:ok, created_parent} =
             flow_create_and_get(parent,
               type: "parent",
               state: "dispatch",
               partition_key: partition
             )

    assert {:ok, waiting} =
             flow_spawn_children_and_get(
               parent,
               [%{id: child, type: "child", partition_key: other_partition}],
               group_id: "retry-many-fanout",
               wait: :all,
               wait_state: "waiting_children",
               on_child_failed: :fail_parent,
               on_parent_closed: :abandon_children,
               exhaust_to: %{success: "children_done", failure: "children_failed"},
               partition_key: partition,
               from_state: "dispatch",
               fencing_token: created_parent.fencing_token
             )

    assert waiting.state == "waiting_children"
    claimed = create_claimed_flow_child(child, other_partition, "worker-retry-many-cross")

    assert {:ok, [exhausted_child]} =
             flow_retry_many_and_get(
               nil,
               [
                 %{
                   id: child,
                   partition_key: other_partition,
                   lease_token: claimed.lease_token,
                   fencing_token: claimed.fencing_token
                 }
               ],
               now_ms: 2_000,
               retry: [max_retries: 0, exhausted_to: "failed"]
             )

    assert exhausted_child.state == "failed"

    assert {:ok, failed_parent} = FerricStore.flow_get(parent, partition_key: partition)
    assert failed_parent.state == "children_failed"
    assert failed_parent.child_groups["retry-many-fanout"]["children"][child] == "failed"
    assert failed_parent.child_groups["retry-many-fanout"]["summary"]["failed"] == 1
  end

  test "flow_fail_many spans shards and rolls back failing shard group" do
    {same_a, same_b, other} = mixed_partition_keys()
    type = uid("bulk-mixed-fail")
    bad = create_claimed_flow(uid("fail-mixed-bad"), same_a, type, "worker-fail")
    same = create_claimed_flow(uid("fail-mixed-same"), same_b, type, "worker-fail")
    other_flow = create_claimed_flow(uid("fail-mixed-other"), other, type, "worker-fail")

    assert {:ok, results} =
             flow_fail_many_and_get(
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
             flow_create_many_and_get(
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
             flow_cancel_many_and_get(partition, items,
               reason: "cancel-batch",
               now_ms: 2_000
             )

    assert Enum.map(cancelled, & &1.id) == [id_a, id_b]
    assert Enum.all?(cancelled, &(&1.state == "cancelled"))
    assert Enum.all?(cancelled, &(is_binary(&1.error_ref) and &1.error_ref != "cancel-batch"))

    assert {:ok, info} = FerricStore.flow_info(type, partition_key: partition)
    assert info.cancelled == 2
  end

  test "flow_cancel_many rolls back when any item fails guard" do
    partition = uid("tenant-cancel-many-rollback")
    type = uid("bulk-cancel-many-rollback")
    id_a = uid("cancel-many-good")
    id_b = uid("cancel-many-bad")

    assert {:ok, _} =
             flow_create_many_and_get(
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
             flow_cancel_many_and_get(partition, items, now_ms: 2_000)

    assert {:ok, fetched_a} = FerricStore.flow_get(id_a, partition_key: partition)
    assert {:ok, fetched_b} = FerricStore.flow_get(id_b, partition_key: partition)
    assert fetched_a.state == "queued"
    assert fetched_b.state == "queued"
    assert fetched_a.version == 1
    assert fetched_b.version == 1
  end

  test "terminal many independent keeps successful items when one item fails" do
    partition = uid("tenant-terminal-many-independent")

    complete_bad =
      create_claimed_flow(
        uid("complete-independent-bad"),
        partition,
        uid("complete-independent"),
        "worker-complete-independent"
      )

    complete_good =
      create_claimed_flow(
        uid("complete-independent-good"),
        partition,
        uid("complete-independent"),
        "worker-complete-independent"
      )

    assert {:ok, [{:error, "ERR stale flow lease"}, :ok]} =
             FerricStore.flow_complete_many(
               partition,
               [
                 %{
                   id: complete_bad.id,
                   lease_token: complete_bad.lease_token,
                   fencing_token: complete_bad.fencing_token + 1
                 },
                 %{
                   id: complete_good.id,
                   lease_token: complete_good.lease_token,
                   fencing_token: complete_good.fencing_token
                 }
               ],
               now_ms: 2_000,
               independent: true
             )

    assert {:ok, %{state: "running"}} =
             FerricStore.flow_get(complete_bad.id, partition_key: partition)

    assert {:ok, %{state: "completed"}} =
             FerricStore.flow_get(complete_good.id, partition_key: partition)

    retry_bad =
      create_claimed_flow(
        uid("retry-independent-bad"),
        partition,
        uid("retry-independent"),
        "worker-retry-independent"
      )

    retry_good =
      create_claimed_flow(
        uid("retry-independent-good"),
        partition,
        uid("retry-independent"),
        "worker-retry-independent"
      )

    assert {:ok, [{:error, "ERR stale flow lease"}, :ok]} =
             FerricStore.flow_retry_many(
               partition,
               [
                 %{
                   id: retry_bad.id,
                   lease_token: retry_bad.lease_token,
                   fencing_token: retry_bad.fencing_token + 1
                 },
                 %{
                   id: retry_good.id,
                   lease_token: retry_good.lease_token,
                   fencing_token: retry_good.fencing_token
                 }
               ],
               run_at_ms: 3_000,
               now_ms: 2_000,
               independent: true
             )

    assert {:ok, %{state: "running"}} =
             FerricStore.flow_get(retry_bad.id, partition_key: partition)

    assert {:ok, %{state: "queued"}} =
             FerricStore.flow_get(retry_good.id, partition_key: partition)

    fail_bad =
      create_claimed_flow(
        uid("fail-independent-bad"),
        partition,
        uid("fail-independent"),
        "worker-fail-independent"
      )

    fail_good =
      create_claimed_flow(
        uid("fail-independent-good"),
        partition,
        uid("fail-independent"),
        "worker-fail-independent"
      )

    assert {:ok, [{:error, "ERR stale flow lease"}, :ok]} =
             FerricStore.flow_fail_many(
               partition,
               [
                 %{
                   id: fail_bad.id,
                   lease_token: fail_bad.lease_token,
                   fencing_token: fail_bad.fencing_token + 1
                 },
                 %{
                   id: fail_good.id,
                   lease_token: fail_good.lease_token,
                   fencing_token: fail_good.fencing_token
                 }
               ],
               now_ms: 2_000,
               independent: true
             )

    assert {:ok, %{state: "running"}} =
             FerricStore.flow_get(fail_bad.id, partition_key: partition)

    assert {:ok, %{state: "failed"}} =
             FerricStore.flow_get(fail_good.id, partition_key: partition)

    cancel_type = uid("cancel-independent")
    cancel_bad = uid("cancel-independent-bad")
    cancel_good = uid("cancel-independent-good")

    assert {:ok, _} =
             flow_create_many_and_get(
               partition,
               [%{id: cancel_bad}, %{id: cancel_good}],
               type: cancel_type,
               state: "queued",
               run_at_ms: 1_000
             )

    assert {:ok, [{:error, "ERR stale flow lease"}, :ok]} =
             FerricStore.flow_cancel_many(
               partition,
               [
                 %{id: cancel_bad, fencing_token: 1},
                 %{id: cancel_good, fencing_token: 0}
               ],
               now_ms: 2_000,
               independent: true
             )

    assert {:ok, %{state: "queued"}} = FerricStore.flow_get(cancel_bad, partition_key: partition)

    assert {:ok, %{state: "cancelled"}} =
             FerricStore.flow_get(cancel_good, partition_key: partition)
  end

  test "flow_cancel_many spans shards and rolls back failing shard group" do
    {same_a, same_b, other} = mixed_partition_keys()
    type = uid("bulk-mixed-cancel")
    bad_id = uid("cancel-mixed-bad")
    same_id = uid("cancel-mixed-same")
    other_id = uid("cancel-mixed-other")

    for {id, partition} <- [{bad_id, same_a}, {same_id, same_b}, {other_id, other}] do
      assert {:ok, _} =
               flow_create_and_get(id,
                 type: type,
                 partition_key: partition,
                 state: "queued",
                 run_at_ms: 1_000
               )
    end

    assert {:ok, results} =
             flow_cancel_many_and_get(
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
             flow_create_and_get(id, type: "checkout", state: "queued", run_at_ms: 1_000)

    assert {:error, "ERR flow wrong state"} =
             flow_transition_and_get(id, "running", "completed",
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
             flow_transition_and_get(id, "running", "next",
               fencing_token: claimed.fencing_token,
               run_at_ms: 2_000
             )

    assert {:error, "ERR stale flow lease"} =
             flow_transition_and_get(id, "running", "next",
               lease_token: claimed.lease_token,
               fencing_token: claimed.fencing_token + 1,
               run_at_ms: 2_000
             )

    assert {:ok, transitioned} =
             flow_transition_and_get(id, "running", "next",
               lease_token: claimed.lease_token,
               fencing_token: claimed.fencing_token,
               run_at_ms: 2_000
             )

    assert transitioned.state == "next"
    assert transitioned.lease_token == nil
  end

  test "flow_transition rejects terminal states so terminal hooks stay centralized" do
    parent = uid("flow-terminal-transition-parent")
    child = uid("flow-terminal-transition-child")
    {partition, _same_partition, other_partition} = mixed_partition_keys()

    assert {:ok, created_parent} =
             flow_create_and_get(parent,
               type: "parent",
               state: "dispatch",
               partition_key: partition
             )

    assert {:ok, waiting} =
             flow_spawn_children_and_get(
               parent,
               [%{id: child, type: "child", partition_key: other_partition}],
               group_id: "fanout",
               wait: :all,
               wait_state: "waiting_children",
               on_child_failed: :ignore,
               on_parent_closed: :abandon_children,
               exhaust_to: %{success: "children_done", failure: "children_failed"},
               partition_key: partition,
               from_state: "dispatch",
               fencing_token: created_parent.fencing_token
             )

    assert waiting.state == "waiting_children"
    claimed = create_claimed_flow_child(child, other_partition, "worker-terminal-transition")

    assert {:error, "ERR terminal flow state requires FLOW.COMPLETE, FLOW.FAIL, or FLOW.CANCEL"} =
             flow_transition_and_get(child, "running", "completed",
               partition_key: other_partition,
               lease_token: claimed.lease_token,
               fencing_token: claimed.fencing_token,
               run_at_ms: 2_000
             )

    assert {:ok, unchanged_parent} = FerricStore.flow_get(parent, partition_key: partition)
    assert unchanged_parent.state == "waiting_children"
    assert unchanged_parent.child_groups["fanout"]["children"][child] == "running"
  end

  test "flow_transition and flow_cancel reject already terminal source records" do
    transition_id = uid("flow-terminal-source-transition")
    cancel_id = uid("flow-terminal-source-cancel")
    transition_type = uid("terminal-source-transition")
    cancel_type = uid("terminal-source-cancel")

    assert {:ok, _} = flow_create_and_get(transition_id, type: transition_type, run_at_ms: 1_000)
    assert {:ok, _} = flow_create_and_get(cancel_id, type: cancel_type, run_at_ms: 1_000)

    assert {:ok, [claimed_transition]} =
             FerricStore.flow_claim_due(transition_type,
               worker: "terminal-source-transition",
               limit: 1,
               now_ms: 1_000
             )

    assert {:ok, completed} =
             flow_complete_and_get(claimed_transition.id, claimed_transition.lease_token,
               fencing_token: claimed_transition.fencing_token,
               now_ms: 1_100
             )

    assert completed.id == transition_id
    assert completed.state == "completed"

    assert {:error, "ERR flow is terminal; use FLOW.REWIND"} =
             flow_transition_and_get(transition_id, "completed", "queued",
               lease_token: nil,
               fencing_token: completed.fencing_token,
               run_at_ms: 2_000,
               now_ms: 1_200
             )

    assert {:ok, [claimed_cancel]} =
             FerricStore.flow_claim_due(cancel_type,
               worker: "terminal-source-cancel",
               limit: 1,
               now_ms: 1_000
             )

    assert {:ok, failed} =
             flow_fail_and_get(claimed_cancel.id, claimed_cancel.lease_token,
               fencing_token: claimed_cancel.fencing_token,
               now_ms: 1_100
             )

    assert failed.id == cancel_id
    assert failed.state == "failed"

    assert {:error, "ERR flow is terminal; use FLOW.REWIND"} =
             flow_cancel_and_get(cancel_id,
               fencing_token: failed.fencing_token,
               now_ms: 1_200
             )
  end

  test "flow_transition rolls back index changes when derived keys are invalid" do
    id = uid("flow-transition-rollback")
    huge_state = String.duplicate("x", 65_536)

    assert {:ok, _} =
             flow_create_and_get(id, type: "audit", state: "queued", run_at_ms: 1_000)

    assert {:error, "ERR key too large" <> _} =
             flow_transition_and_get(id, "queued", huge_state,
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

    assert {:ok, _} = flow_create_and_get(fail_id, type: fail_type, run_at_ms: 1_000)
    assert {:ok, _} = flow_create_and_get(cancel_id, type: cancel_type, run_at_ms: 1_000)

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
             flow_fail_and_get(fail_id, claimed.lease_token,
               fencing_token: claimed.fencing_token + 1,
               error: "error:" <> fail_id,
               now_ms: 1_500
             )

    assert {:ok, failed} =
             flow_fail_and_get(fail_id, claimed.lease_token,
               fencing_token: claimed.fencing_token,
               error: "error:" <> fail_id,
               now_ms: 1_500
             )

    assert failed.state == "failed"
    assert is_binary(failed.error_ref)
    assert failed.error_ref != "error:" <> fail_id
    assert failed.lease_token == nil
    assert failed.next_run_at_ms == nil

    assert {:ok, cancelled} =
             flow_cancel_and_get(cancel_id,
               fencing_token: 0,
               reason: "reason:" <> cancel_id,
               now_ms: 1_500
             )

    assert cancelled.state == "cancelled"
    assert is_binary(cancelled.error_ref)
    assert cancelled.error_ref != "reason:" <> cancel_id
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
             flow_create_and_get(id,
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
             flow_complete_and_get(id, claimed.lease_token, fencing_token: claimed.fencing_token)

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

  test "flow_history supports range reverse event and worker filters" do
    id = uid("flow-history-query")
    type = uid("audit-history-query")
    partition = uid("tenant-history-query")

    assert {:ok, _} =
             flow_create_and_get(id,
               type: type,
               state: "queued",
               partition_key: partition,
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert {:ok, [first_claim]} =
             FerricStore.flow_claim_due(type,
               partition_key: partition,
               state: "queued",
               worker: "worker-a",
               lease_ms: 30_000,
               limit: 1,
               now_ms: 1_100
             )

    assert {:ok, _} =
             flow_transition_and_get(id, "running", "email",
               partition_key: partition,
               lease_token: first_claim.lease_token,
               fencing_token: first_claim.fencing_token,
               run_at_ms: 1_200,
               now_ms: 1_200
             )

    assert {:ok, [second_claim]} =
             FerricStore.flow_claim_due(type,
               partition_key: partition,
               state: "email",
               worker: "worker-b",
               lease_ms: 30_000,
               limit: 1,
               now_ms: 1_250
             )

    assert {:ok, _} =
             flow_fail_and_get(id, second_claim.lease_token,
               partition_key: partition,
               fencing_token: second_claim.fencing_token,
               error: "boom",
               now_ms: 1_300
             )

    assert {:ok, all_events} = FerricStore.flow_history(id, partition_key: partition, count: 10)
    all_ids = Enum.map(all_events, &elem(&1, 0))

    assert Enum.map(all_events, fn {_event_id, fields} -> fields["event"] end) == [
             "created",
             "claimed",
             "transitioned",
             "claimed",
             "failed"
           ]

    assert {:ok, range_events} =
             FerricStore.flow_history(id,
               partition_key: partition,
               from_event: Enum.at(all_ids, 1),
               to_event: Enum.at(all_ids, 3),
               count: 10
             )

    assert Enum.map(range_events, fn {_event_id, fields} -> fields["event"] end) == [
             "claimed",
             "transitioned",
             "claimed"
           ]

    assert {:ok, reverse_events} =
             FerricStore.flow_history(id,
               partition_key: partition,
               from_ms: 1_100,
               to_ms: 1_300,
               rev: true,
               count: 2
             )

    assert Enum.map(reverse_events, fn {_event_id, fields} -> fields["event"] end) == [
             "failed",
             "claimed"
           ]

    assert {:ok, worker_events} =
             FerricStore.flow_history(id,
               partition_key: partition,
               event: "claimed",
               worker: "worker-b",
               count: 10
             )

    assert [{_event_id, %{"event" => "claimed", "lease_owner" => "worker-b"}}] = worker_events

    assert {:ok, version_events} =
             FerricStore.flow_history(id,
               partition_key: partition,
               from_version: 2,
               to_version: 4,
               count: 10
             )

    assert Enum.map(version_events, fn {_event_id, fields} -> fields["version"] end) == [
             "2",
             "3",
             "4"
           ]
  end

  test "flow_history filtered reads scan enough hot history before applying filters" do
    old_limit = Application.get_env(:ferricstore, :flow_lmdb_history_query_scan_limit)

    Application.put_env(:ferricstore, :flow_lmdb_history_query_scan_limit, 2)

    on_exit(fn ->
      if is_nil(old_limit) do
        Application.delete_env(:ferricstore, :flow_lmdb_history_query_scan_limit)
      else
        Application.put_env(:ferricstore, :flow_lmdb_history_query_scan_limit, old_limit)
      end
    end)

    id = uid("flow-history-filter-scan")
    type = uid("audit-history-filter-scan")
    partition = uid("tenant-history-filter-scan")

    assert {:ok, _} =
             flow_create_and_get(id,
               type: type,
               state: "waiting",
               partition_key: partition,
               history_hot_max_events: 32,
               history_max_events: 32,
               now_ms: 1_000
             )

    for idx <- 1..6 do
      assert :ok =
               FerricStore.flow_signal(id,
                 partition_key: partition,
                 signal: "note-#{idx}",
                 now_ms: 1_000 + idx
               )
    end

    assert {:ok, events} =
             FerricStore.flow_history(id,
               partition_key: partition,
               from_ms: 1_006,
               to_ms: 1_006,
               count: 1
             )

    assert [{_event_id, %{"event" => "signaled", "signal" => "note-6"}}] = events
  end

  test "flow_get reports corrupt state record instead of hiding it as missing" do
    ctx = FerricStore.Instance.get(:default)
    id = uid("flow-corrupt-state")
    partition = uid("tenant-corrupt-state")

    assert :ok =
             FerricStore.flow_create(id,
               type: "corrupt-state",
               partition_key: partition,
               now_ms: 1_000
             )

    key = Ferricstore.Flow.Keys.state_key(id, partition)
    assert :ok = Ferricstore.Store.Router.put(ctx, key, "not-a-flow-record", 0)

    assert {:error, "ERR corrupt flow record"} =
             FerricStore.flow_get(id, partition_key: partition)
  end

  test "flow_history reports corrupt history entries instead of returning empty fields" do
    ctx = FerricStore.Instance.get(:default)
    id = uid("flow-corrupt-history")
    partition = uid("tenant-corrupt-history")

    assert :ok =
             FerricStore.flow_create(id,
               type: "corrupt-history",
               partition_key: partition,
               now_ms: 1_000
             )

    assert {:ok, [{event_id, %{"event" => "created"}}]} =
             FerricStore.flow_history(id, partition_key: partition, count: 1)

    history_key = Ferricstore.Flow.Keys.history_key(id, partition)
    compound_key = Ferricstore.Flow.Keys.stream_entry_key(id, event_id, partition)

    assert :ok =
             Ferricstore.Store.Router.compound_put(
               ctx,
               history_key,
               compound_key,
               "not-a-flow-history-record",
               0
             )

    assert {:error, "ERR corrupt flow history"} =
             FerricStore.flow_history(id, partition_key: partition, count: 1)
  end

  test "flow_history range filters avoid reading excluded corrupt older records" do
    ctx = FerricStore.Instance.get(:default)
    id = uid("flow-range-history")
    partition = uid("tenant-range-history")
    type = uid("range-history")

    assert :ok =
             FerricStore.flow_create(id,
               type: type,
               partition_key: partition,
               now_ms: 1_000,
               run_at_ms: 1_000
             )

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due(type,
               partition_key: partition,
               worker: "range-history-worker",
               limit: 1,
               now_ms: 2_000
             )

    assert :ok =
             FerricStore.flow_complete(id, claimed.lease_token,
               partition_key: partition,
               fencing_token: claimed.fencing_token,
               now_ms: 3_000
             )

    assert {:ok, all_events} = FerricStore.flow_history(id, partition_key: partition, count: 10)
    {created_event_id, %{"event" => "created"}} = hd(all_events)

    history_key = Ferricstore.Flow.Keys.history_key(id, partition)
    compound_key = Ferricstore.Flow.Keys.stream_entry_key(id, created_event_id, partition)

    assert :ok =
             Ferricstore.Store.Router.compound_put(
               ctx,
               history_key,
               compound_key,
               "not-a-flow-history-record",
               0
             )

    assert {:ok, ranged_events} =
             FerricStore.flow_history(id,
               partition_key: partition,
               from_ms: 2_000,
               count: 10
             )

    assert Enum.map(ranged_events, fn {_event_id, fields} -> fields["event"] end) == [
             "claimed",
             "completed"
           ]
  end

  test "flow_terminals and flow_failures list terminal records by state and time range" do
    type = uid("flow-failures")
    partition = uid("tenant-flow-failures")
    failed_a = create_claimed_flow(uid("flow-failures-a"), partition, type, "worker-failures")
    failed_b = create_claimed_flow(uid("flow-failures-b"), partition, type, "worker-failures")
    completed = create_claimed_flow(uid("flow-failures-c"), partition, type, "worker-failures")
    cancelled = create_claimed_flow(uid("flow-failures-d"), partition, type, "worker-failures")

    assert {:ok, _} =
             flow_fail_and_get(failed_a.id, failed_a.lease_token,
               partition_key: partition,
               fencing_token: failed_a.fencing_token,
               now_ms: 1_500
             )

    assert {:ok, _} =
             flow_fail_and_get(failed_b.id, failed_b.lease_token,
               partition_key: partition,
               fencing_token: failed_b.fencing_token,
               now_ms: 2_500
             )

    assert {:ok, _} =
             flow_complete_and_get(completed.id, completed.lease_token,
               partition_key: partition,
               fencing_token: completed.fencing_token,
               now_ms: 2_000
             )

    assert {:ok, _} =
             flow_cancel_and_get(cancelled.id,
               partition_key: partition,
               lease_token: cancelled.lease_token,
               fencing_token: cancelled.fencing_token,
               now_ms: 1_750
             )

    assert {:ok, failures} =
             FerricStore.flow_failures(type,
               partition_key: partition,
               from_ms: 1_000,
               to_ms: 2_000,
               count: 10
             )

    assert Enum.map(failures, & &1.id) == [failed_a.id]
    assert Enum.all?(failures, &(&1.state == "failed"))

    assert {:ok, completed_records} =
             FerricStore.flow_terminals(type,
               partition_key: partition,
               state: "completed",
               count: 10
             )

    assert Enum.map(completed_records, & &1.id) == [completed.id]

    assert {:ok, cancelled_records} =
             FerricStore.flow_terminals(type,
               partition_key: partition,
               state: "cancelled",
               count: 10
             )

    assert Enum.map(cancelled_records, & &1.id) == [cancelled.id]

    assert {:ok, terminal_records} =
             FerricStore.flow_terminals(type,
               partition_key: partition,
               state: "any",
               from_ms: 1_400,
               to_ms: 2_100,
               count: 10
             )

    assert Enum.map(terminal_records, & &1.id) == [failed_a.id, cancelled.id, completed.id]

    assert {:ok, reverse_terminal_records} =
             FerricStore.flow_terminals(type,
               partition_key: partition,
               state: "any",
               from_ms: 1_400,
               to_ms: 2_100,
               rev: true,
               count: 2
             )

    assert Enum.map(reverse_terminal_records, & &1.id) == [completed.id, cancelled.id]
  end

  test "flow_history event ids stay monotonic when claim time is behind record time" do
    id = uid("flow-history-monotonic")

    assert {:ok, _} =
             flow_create_and_get(id,
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

    assert :ok =
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

    assert :ok =
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

  test "flow_history fallback preserves hot-window count semantics" do
    id = uid("flow-history-fallback-count")
    partition = "tenant-history-fallback-count"

    assert {:ok, _} =
             flow_create_and_get(id,
               type: "history-fallback-count",
               partition_key: partition,
               history_hot_max_events: 3,
               history_max_events: 10,
               now_ms: 1
             )

    for idx <- 1..3 do
      assert :ok =
               FerricStore.flow_signal(id,
                 partition_key: partition,
                 signal: "note-#{idx}",
                 now_ms: 1 + idx
               )
    end

    assert {:ok, [{expected_event_id, %{"event" => "signaled", "signal" => "note-1"}}]} =
             FerricStore.flow_history(id, partition_key: partition, count: 1)

    history_key = Ferricstore.Flow.Keys.history_key(id, partition)
    shard = shard_for(history_key)
    {flow_index, flow_lookup} = Ferricstore.Flow.OrderedIndex.table_names(:default, shard)

    for event_id <- ["1-1", "2-2", "3-3", "4-4"] do
      Ferricstore.Flow.OrderedIndex.delete_member(flow_index, flow_lookup, history_key, event_id)

      case Ferricstore.Flow.NativeOrderedIndex.get(flow_index, flow_lookup) do
        nil -> :ok
        native -> Ferricstore.Flow.NativeOrderedIndex.delete_member(native, history_key, event_id)
      end
    end

    assert {:ok, [{^expected_event_id, %{"event" => "signaled", "signal" => "note-1"}}]} =
             FerricStore.flow_history(id, partition_key: partition, count: 1)
  end

  test "flow_history falls back when hot history index is partially stale" do
    ctx = Ferricstore.Test.IsolatedInstance.checkout(shard_count: 1)

    on_exit(fn -> Ferricstore.Test.IsolatedInstance.checkin(ctx) end)

    id = uid("flow-history-partial-fallback")
    partition = "tenant-history-partial-fallback"

    assert :ok =
             Ferricstore.Flow.create(ctx, id,
               type: "history-partial-fallback",
               partition_key: partition,
               run_at_ms: 1,
               now_ms: 1
             )

    assert {:ok, [claimed]} =
             Ferricstore.Flow.claim_due(ctx, "history-partial-fallback",
               partition_key: partition,
               worker: "worker-history-partial-fallback",
               limit: 1,
               now_ms: 2
             )

    assert :ok =
             Ferricstore.Flow.complete(ctx, id, claimed.lease_token,
               partition_key: partition,
               fencing_token: claimed.fencing_token,
               now_ms: 3
             )

    assert {:ok, before_events} =
             Ferricstore.Flow.history(ctx, id, partition_key: partition, count: 10)

    {claimed_event_id, _fields} =
      Enum.find(before_events, fn {_event_id, fields} -> fields["event"] == "claimed" end)

    history_key = Ferricstore.Flow.Keys.history_key(id, partition)
    {flow_index, flow_lookup} = Ferricstore.Flow.OrderedIndex.table_names(ctx.name, 0)

    Ferricstore.Flow.OrderedIndex.delete_member(
      flow_index,
      flow_lookup,
      history_key,
      claimed_event_id
    )

    case Ferricstore.Flow.NativeOrderedIndex.get(flow_index, flow_lookup) do
      nil ->
        :ok

      native ->
        Ferricstore.Flow.NativeOrderedIndex.delete_member(native, history_key, claimed_event_id)
    end

    assert {:ok, events} = Ferricstore.Flow.history(ctx, id, partition_key: partition, count: 10)

    assert Enum.map(events, fn {_event_id, fields} -> fields["event"] end) == [
             "created",
             "claimed",
             "completed"
           ]
  end

  test "flow history hot max keeps only latest configured hot events" do
    id = uid("flow-history-retention")

    assert {:ok, _} =
             flow_create_and_get(id,
               type: "audit-retention",
               run_at_ms: 1_000,
               history_hot_max_events: 2
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
             flow_complete_and_get(id, claimed.lease_token, fencing_token: claimed.fencing_token)

    assert {:ok, events} = FerricStore.flow_history(id, count: 10)
    event_ids = Enum.map(events, fn {event_id, _fields} -> event_id end)

    assert Enum.map(events, fn {_id, fields} -> fields["event"] end) == ["claimed", "completed"]
    refute created_event_id in event_ids

    history_key = Ferricstore.Flow.Keys.history_key(id)
    shard = shard_for(history_key)
    {flow_index, flow_lookup} = Ferricstore.Flow.OrderedIndex.table_names(:default, shard)

    assert Ferricstore.Flow.OrderedIndex.count_all(flow_lookup, history_key) == 3

    assert Ferricstore.Flow.OrderedIndex.rank_range(flow_index, history_key, 0, 10, false)
           |> Enum.map(&elem(&1, 0)) ==
             [created_event_id | event_ids]

    assert [] = :ets.lookup(Ferricstore.Stream.Meta, history_key)

    assert [{^created_event_id, _score}] =
             Ferricstore.Flow.OrderedIndex.rank_range(flow_index, history_key, 0, 10, false)
             |> Enum.filter(fn {event_id, _score} -> event_id == created_event_id end)
  end

  test "flow history max events hard-caps stored history records" do
    id = uid("flow-history-hard-cap")

    assert {:ok, _} =
             flow_create_and_get(id,
               type: "audit-hard-cap",
               run_at_ms: 1_000,
               history_hot_max_events: 5,
               history_max_events: 5,
               now_ms: 1_000
             )

    assert {:ok, [{created_event_id, _fields}]} = FerricStore.flow_history(id, count: 10)

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due("audit-hard-cap",
               worker: "worker-a",
               lease_ms: 30_000,
               limit: 1,
               now_ms: 1_010
             )

    1..5
    |> Enum.each(fn idx ->
      assert {:ok, _extended} =
               FerricStore.flow_extend_lease(id, claimed.lease_token,
                 fencing_token: claimed.fencing_token,
                 lease_ms: 30_000,
                 now_ms: 1_020 + idx
               )
    end)

    assert {:ok, _} =
             flow_complete_and_get(id, claimed.lease_token,
               fencing_token: claimed.fencing_token,
               now_ms: 1_100
             )

    assert {:ok, events} = FerricStore.flow_history(id, count: 10)
    event_ids = Enum.map(events, fn {event_id, _fields} -> event_id end)

    assert length(events) == 5
    refute created_event_id in event_ids

    assert Enum.map(events, fn {_id, fields} -> fields["event"] end) == [
             "lease_extended",
             "lease_extended",
             "lease_extended",
             "lease_extended",
             "completed"
           ]

    history_key = Ferricstore.Flow.Keys.history_key(id)
    history_entry_key = Ferricstore.Flow.Keys.stream_entry_key(id, created_event_id)
    shard = shard_for(history_key)
    {_flow_index, flow_lookup} = Ferricstore.Flow.OrderedIndex.table_names(:default, shard)

    assert Ferricstore.Flow.OrderedIndex.count_all(flow_lookup, history_key) == 5
    assert {:ok, nil} = FerricStore.get(history_entry_key)
  end

  test "flow history max events defaults to 100k and rejects invalid caps" do
    id = uid("flow-history-default-hard-cap")

    assert {:ok, created} =
             flow_create_and_get(id,
               type: "audit-default-hard-cap",
               run_at_ms: 1_000
             )

    assert created.history_max_events == 100_000

    assert {:error,
            "ERR flow history_max_events must be greater than or equal to history_hot_max_events"} =
             flow_create_and_get(uid("flow-history-bad-hard-cap"),
               type: "audit-bad-hard-cap",
               history_hot_max_events: 10,
               history_max_events: 5
             )
  end

  test "flow history hard default is clamped by configured maximum" do
    original_hot = Application.get_env(:ferricstore, :flow_default_history_hot_max_events)
    original_hard = Application.get_env(:ferricstore, :flow_default_history_max_events)
    original_max = Application.get_env(:ferricstore, :flow_max_history_max_events)

    Application.put_env(:ferricstore, :flow_default_history_hot_max_events, 10)
    Application.put_env(:ferricstore, :flow_default_history_max_events, 100)
    Application.put_env(:ferricstore, :flow_max_history_max_events, 5)

    on_exit(fn ->
      restore_env(:flow_default_history_hot_max_events, original_hot)
      restore_env(:flow_default_history_max_events, original_hard)
      restore_env(:flow_max_history_max_events, original_max)
    end)

    assert {:ok, created} =
             flow_create_and_get(uid("flow-history-default-hard-clamp"),
               type: "audit-default-hard-clamp",
               run_at_ms: 1_000
             )

    assert created.history_max_events == 5
    assert created.history_hot_max_events == 5
  end

  test "flow history configured maximum cannot exceed hard cap" do
    original_max = Application.get_env(:ferricstore, :flow_max_history_max_events)
    Application.put_env(:ferricstore, :flow_max_history_max_events, 2_000_000)

    on_exit(fn -> restore_env(:flow_max_history_max_events, original_max) end)

    assert {:error, "ERR flow history_max_events exceeds maximum 1000000"} =
             flow_create_and_get(uid("flow-history-hard-cap-env"),
               type: "audit-hard-cap-env",
               history_max_events: 2_000_000
             )
  end

  test "flow history uses configured default hot max when omitted" do
    original = Application.get_env(:ferricstore, :flow_default_history_hot_max_events)
    Application.put_env(:ferricstore, :flow_default_history_hot_max_events, 2)

    on_exit(fn ->
      if is_nil(original) do
        Application.delete_env(:ferricstore, :flow_default_history_hot_max_events)
      else
        Application.put_env(:ferricstore, :flow_default_history_hot_max_events, original)
      end
    end)

    id = uid("flow-history-default-retention")

    assert {:ok, created} =
             flow_create_and_get(id,
               type: "audit-default-retention",
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert created.history_hot_max_events == 2

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due("audit-default-retention",
               worker: "worker-a",
               lease_ms: 30_000,
               limit: 1,
               now_ms: 1_100
             )

    assert {:ok, _} =
             flow_complete_and_get(id, claimed.lease_token,
               fencing_token: claimed.fencing_token,
               now_ms: 1_200
             )

    assert {:ok, events} = FerricStore.flow_history(id, count: 10)
    assert Enum.map(events, fn {_id, fields} -> fields["event"] end) == ["claimed", "completed"]
  end

  test "flow history hot max rejects values above configured maximum" do
    original = Application.get_env(:ferricstore, :flow_max_history_hot_max_events)
    Application.put_env(:ferricstore, :flow_max_history_hot_max_events, 2)

    on_exit(fn ->
      if is_nil(original) do
        Application.delete_env(:ferricstore, :flow_max_history_hot_max_events)
      else
        Application.put_env(:ferricstore, :flow_max_history_hot_max_events, original)
      end
    end)

    assert {:error, "ERR flow history_hot_max_events exceeds maximum 2"} =
             flow_create_and_get(uid("flow-history-max"),
               type: "audit-history-max",
               history_hot_max_events: 3
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
             flow_create_many_and_get("tenant-batch-cap", ["a", "b", "c"], type: "batch-cap")

    assert {:error, "ERR flow batch item count exceeds maximum 2"} =
             flow_transition_many_and_get(
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

  test "flow claim_due allows higher compact job limit without raising full record cap" do
    original = Application.get_env(:ferricstore, :flow_max_claim_limit)
    original_compact = Application.get_env(:ferricstore, :flow_max_compact_claim_limit)
    Application.put_env(:ferricstore, :flow_max_claim_limit, 2)
    Application.put_env(:ferricstore, :flow_max_compact_claim_limit, 4)

    on_exit(fn ->
      if is_nil(original) do
        Application.delete_env(:ferricstore, :flow_max_claim_limit)
      else
        Application.put_env(:ferricstore, :flow_max_claim_limit, original)
      end

      if is_nil(original_compact) do
        Application.delete_env(:ferricstore, :flow_max_compact_claim_limit)
      else
        Application.put_env(:ferricstore, :flow_max_compact_claim_limit, original_compact)
      end
    end)

    assert {:error, "ERR flow limit exceeds maximum 2"} =
             FerricStore.flow_claim_due("claim-limit-cap",
               worker: "worker-a",
               limit: 3,
               return: :jobs
             )

    assert {:ok, []} =
             FerricStore.flow_claim_due("claim-limit-cap",
               worker: "worker-a",
               limit: 3,
               return: :jobs_compact
             )
  end

  test "flow_rewind rejects trimmed target event with stale stream index" do
    id = uid("flow-rewind-trimmed")

    assert {:ok, _} =
             flow_create_and_get(id,
               type: "rewind-trimmed",
               run_at_ms: 1_000,
               history_hot_max_events: 2,
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
             flow_complete_and_get(id, claimed.lease_token,
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

    assert {:ok, _} = flow_create_and_get(id, type: "rewind", run_at_ms: 1_000, now_ms: 1_000)

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
             flow_complete_and_get(id, claimed.lease_token,
               fencing_token: claimed.fencing_token,
               now_ms: 2_000
             )

    assert completed.state == "completed"
    assert {:ok, %{queued: 0, completed: 1}} = FerricStore.flow_info("rewind")

    assert :ok =
             FerricStore.flow_rewind(id,
               to_event: created_event_id,
               run_at_ms: 5_000,
               expect_state: "completed",
               now_ms: 3_000
             )

    assert {:ok, rewound} = FerricStore.flow_get(id)

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

    assert {:ok, _} = flow_create_and_get(id, type: "rewind-guard", run_at_ms: 1_000)
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
             flow_complete_and_get(id, claimed.lease_token,
               fencing_token: claimed.fencing_token,
               now_ms: 2_000
             )

    assert {:error, "ERR flow rewind target event not found"} =
             FerricStore.flow_rewind(id, to_event: "999999-0")
  end

  test "flow_rewind refreshes owned named value retention for terminal snapshots" do
    id = uid("flow-rewind-terminal-value-ttl")
    now_ms = System.system_time(:millisecond)

    assert {:ok, _created} =
             flow_create_and_get(id,
               type: "rewind-terminal-value-ttl",
               run_at_ms: now_ms,
               retention_ttl_ms: 300,
               now_ms: now_ms
             )

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due("rewind-terminal-value-ttl",
               worker: "worker-a",
               lease_ms: 30_000,
               limit: 1,
               now_ms: now_ms
             )

    assert {:ok, _completed} =
             flow_complete_and_get(id, claimed.lease_token,
               fencing_token: claimed.fencing_token,
               values: %{"artifact" => "artifact-v1"},
               override_values: ["artifact"],
               now_ms: now_ms
             )

    assert {:ok, events} = FerricStore.flow_history(id, count: 10)

    assert {completed_event_id, _fields} =
             Enum.find(events, fn {_event_id, fields} -> fields["event"] == "completed" end)

    assert :ok =
             FerricStore.flow_rewind(id,
               to_event: completed_event_id,
               expect_state: "completed",
               now_ms: now_ms + 1_000
             )

    Process.sleep(400)

    assert {:ok, fetched} = FerricStore.flow_get(id, values: ["artifact"])
    assert fetched.state == "completed"
    assert fetched.values["artifact"] == "artifact-v1"
    refute Map.get(fetched, :value_missing, %{})["artifact"]
  end

  test "flow_rewind rejects parent and child flows" do
    parent = uid("flow-rewind-parent")
    child = uid("flow-rewind-child")
    {partition, _same_partition, other_partition} = mixed_partition_keys()

    assert {:ok, created_parent} =
             flow_create_and_get(parent,
               type: "parent",
               state: "dispatch",
               partition_key: partition,
               now_ms: 1_000
             )

    assert {:ok, [{parent_created_event_id, _fields} | _]} =
             FerricStore.flow_history(parent, partition_key: partition, count: 10)

    assert {:ok, _waiting} =
             flow_spawn_children_and_get(
               parent,
               [%{id: child, type: "child", partition_key: other_partition}],
               group_id: "rewind-fanout",
               wait: :all,
               wait_state: "waiting_children",
               on_child_failed: :ignore,
               on_parent_closed: :abandon_children,
               exhaust_to: %{success: "children_done", failure: "children_failed"},
               partition_key: partition,
               from_state: "dispatch",
               fencing_token: created_parent.fencing_token,
               now_ms: 2_000
             )

    assert {:ok, [{child_created_event_id, _fields} | _]} =
             FerricStore.flow_history(child, partition_key: other_partition, count: 10)

    assert {:error, "ERR flow cannot rewind parent or child flow"} =
             FerricStore.flow_rewind(parent,
               partition_key: partition,
               to_event: parent_created_event_id,
               expect_state: "waiting_children",
               now_ms: 3_000
             )

    assert {:error, "ERR flow cannot rewind parent or child flow"} =
             FerricStore.flow_rewind(child,
               partition_key: other_partition,
               to_event: child_created_event_id,
               expect_state: "running",
               now_ms: 3_000
             )
  end

  test "terminal retention from create expires flow state record" do
    id = uid("flow-terminal-ttl")

    assert {:ok, created} =
             flow_create_and_get(id,
               type: "ttl",
               run_at_ms: 1_000,
               retention_ttl_ms: 20,
               now_ms: 1_000
             )

    assert created.retention_ttl_ms == 20
    assert created.terminal_retention_until_ms == nil

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due("ttl",
               worker: "worker-a",
               lease_ms: 30_000,
               limit: 1,
               now_ms: 1_000
             )

    assert {:ok, _} =
             flow_complete_and_get(id, claimed.lease_token, fencing_token: claimed.fencing_token)

    Process.sleep(40)

    assert {:ok, nil} = FerricStore.flow_get(id)
  end

  test "terminal retention uses wall-valid command time" do
    id = uid("flow-terminal-command-time-ttl")
    create_now = System.system_time(:millisecond) + 60_000
    complete_now = create_now + 10_000

    assert {:ok, _created} =
             flow_create_and_get(id,
               type: "ttl-command-time",
               run_at_ms: create_now,
               retention_ttl_ms: 5_000,
               now_ms: create_now
             )

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due("ttl-command-time",
               worker: "worker-command-time",
               lease_ms: 30_000,
               limit: 1,
               now_ms: create_now
             )

    assert {:ok, completed} =
             flow_complete_and_get(id, claimed.lease_token,
               fencing_token: claimed.fencing_token,
               now_ms: complete_now
             )

    assert completed.terminal_retention_until_ms == complete_now + 5_000
  end

  test "terminal retention expires queryable flow history" do
    id = uid("flow-terminal-history-ttl")

    assert {:ok, _created} =
             flow_create_and_get(id,
               type: "history-ttl",
               payload: %{input: 1},
               run_at_ms: 1_000,
               retention_ttl_ms: 100,
               now_ms: 1_000
             )

    assert {:ok, [_created_event]} = FerricStore.flow_history(id, count: 10)

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due("history-ttl",
               worker: "worker-a",
               lease_ms: 30_000,
               limit: 1,
               now_ms: 1_000
             )

    assert {:ok, _} =
             flow_complete_and_get(id, claimed.lease_token, fencing_token: claimed.fencing_token)

    Process.sleep(150)

    assert {:ok, nil} = FerricStore.flow_get(id)
    assert {:ok, []} = FerricStore.flow_history(id, count: 10)
  end

  test "terminal ttl override must be positive" do
    id = uid("flow-terminal-ttl-zero")

    assert {:ok, _created} =
             flow_create_and_get(id,
               type: "ttl-zero",
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due("ttl-zero",
               worker: "worker-a",
               lease_ms: 30_000,
               limit: 1,
               now_ms: 1_000
             )

    assert {:error, "ERR flow ttl_ms must be a positive integer"} =
             flow_complete_and_get(id, claimed.lease_token,
               fencing_token: claimed.fencing_token,
               ttl_ms: 0
             )
  end

  test "flow create inherits retention defaults from policy" do
    type = uid("flow-retention-policy")
    id = uid("flow-retention-policy-id")

    assert {:ok, _policy} =
             FerricStore.flow_policy_set(type,
               retention: [ttl_ms: 5_000, history_hot_max_events: 3, history_max_events: 9]
             )

    assert {:ok, created} =
             flow_create_and_get(id, type: type, state: "queued", now_ms: 10)

    assert created.retention_ttl_ms == 5_000
    assert created.history_hot_max_events == 3
    assert created.history_max_events == 9
    assert created.terminal_retention_until_ms == nil
  end

  test "flow policy retention history hot max respects configured maximum" do
    original = Application.get_env(:ferricstore, :flow_max_history_hot_max_events)
    Application.put_env(:ferricstore, :flow_max_history_hot_max_events, 2)

    on_exit(fn -> restore_env(:flow_max_history_hot_max_events, original) end)

    type = uid("flow-policy-hot-cap")

    assert {:error, "ERR flow retention history_hot_max_events must be between 1 and 2"} =
             FerricStore.flow_policy_set(type,
               retention: [ttl_ms: 5_000, history_hot_max_events: 3]
             )
  end

  defp restore_env(key, nil), do: Application.delete_env(:ferricstore, key)
  defp restore_env(key, value), do: Application.put_env(:ferricstore, key, value)

  defp start_flow_restart_instance(name, data_dir) do
    ctx =
      FerricStore.Instance.build(name,
        data_dir: data_dir,
        shard_count: 1,
        max_memory_bytes: 256 * 1024 * 1024,
        keydir_max_ram: 64 * 1024 * 1024
      )

    Ferricstore.DataDir.ensure_layout!(data_dir, 1)

    {:ok, _writer} =
      Ferricstore.Flow.LMDBWriter.start_link(
        shard_index: 0,
        data_dir: data_dir,
        instance_ctx: ctx
      )

    {:ok, _shard} =
      Ferricstore.Store.Shard.start_link(
        index: 0,
        data_dir: data_dir,
        instance_ctx: ctx
      )

    ShardHelpers.eventually(
      fn ->
        pid = Process.whereis(elem(ctx.shard_names, 0))

        is_pid(pid) and Process.alive?(pid) and
          match?(
            {:ok, _},
            try do
              {:ok, GenServer.call(elem(ctx.shard_names, 0), :shard_stats, 500)}
            catch
              :exit, _ -> :error
            end
          )
      end,
      "restart flow shard not ready",
      50,
      20
    )

    ctx
  end

  defp stop_flow_restart_instance(nil, _opts), do: :ok

  defp stop_flow_restart_instance(ctx, opts) do
    Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, ctx.shard_count)

    stop_registered_process(elem(ctx.shard_names, 0))
    stop_registered_process(Ferricstore.Flow.LMDBWriter.name(ctx.name, 0))

    for table <- [elem(ctx.keydir_refs, 0), ctx.hotness_table, ctx.config_table] do
      try do
        :ets.delete(table)
      rescue
        _ -> :ok
      end
    end

    FerricStore.Instance.cleanup(ctx.name)

    if Keyword.get(opts, :delete?, false) do
      File.rm_rf!(ctx.data_dir)
    end

    :ok
  end

  defp stop_registered_process(name) do
    case Process.whereis(name) do
      nil ->
        :ok

      pid ->
        try do
          GenServer.stop(pid, :normal, 5_000)
        catch
          :exit, _ -> :ok
        end
    end
  end
end
