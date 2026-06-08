defmodule Ferricstore.FlowTest.Sections.FlowHistorySupportsRangeReverseEventWorkerFilters do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Test.ShardHelpers

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

  test "flow_history filtered cold query finds recent tail events beyond oldest scan window" do
    previous_scan = Application.get_env(:ferricstore, :flow_lmdb_history_query_scan_limit)
    Application.put_env(:ferricstore, :flow_lmdb_history_query_scan_limit, 16)

    on_exit(fn ->
      restore_env(:flow_lmdb_history_query_scan_limit, previous_scan)
    end)

    id = uid("flow-history-cold-tail")
    partition = uid("tenant-history-cold-tail")

    assert {:ok, flow} =
             flow_create_and_get(id,
               type: "history-cold-tail",
               state: "s0",
               partition_key: partition,
               run_at_ms: 1_000,
               now_ms: 1_000,
               history_hot_max_events: 0,
               history_max_events: 200
             )

    flow =
      Enum.reduce(1..24, flow, fn step, current ->
        assert {:ok, transitioned} =
                 flow_transition_and_get(id, "s#{step - 1}", "s#{step}",
                   partition_key: partition,
                   fencing_token: current.fencing_token,
                   run_at_ms: 1_000 + step,
                   now_ms: 1_000 + step
                 )

        transitioned
      end)

    assert flow.version == 25

    assert {:ok, events} =
             FerricStore.flow_history(id,
               partition_key: partition,
               from_version: 20,
               count: 10
             )

    assert Enum.map(events, fn {_event_id, fields} -> fields["version"] end) ==
             Enum.map(20..25, &Integer.to_string/1)
  end

  test "flow_history filtered cold query returns latest matching page" do
    previous_scan = Application.get_env(:ferricstore, :flow_lmdb_history_query_scan_limit)
    Application.put_env(:ferricstore, :flow_lmdb_history_query_scan_limit, 16)

    on_exit(fn ->
      restore_env(:flow_lmdb_history_query_scan_limit, previous_scan)
    end)

    id = uid("flow-history-cold-latest")
    partition = uid("tenant-history-cold-latest")

    assert {:ok, flow} =
             flow_create_and_get(id,
               type: "history-cold-latest",
               state: "s0",
               partition_key: partition,
               run_at_ms: 1_000,
               now_ms: 1_000,
               history_hot_max_events: 0,
               history_max_events: 200
             )

    flow =
      Enum.reduce(1..24, flow, fn step, current ->
        assert {:ok, transitioned} =
                 flow_transition_and_get(id, "s#{step - 1}", "s#{step}",
                   partition_key: partition,
                   fencing_token: current.fencing_token,
                   run_at_ms: 1_000 + step,
                   now_ms: 1_000 + step
                 )

        transitioned
      end)

    assert flow.version == 25

    assert {:ok, events} =
             FerricStore.flow_history(id,
               partition_key: partition,
               from_version: 20,
               count: 3
             )

    assert Enum.map(events, fn {_event_id, fields} -> fields["version"] end) == ["23", "24", "25"]
  end

  test "flow_history filtered cold query keeps old head window before filtering" do
    previous_history_scan = Application.get_env(:ferricstore, :flow_lmdb_history_query_scan_limit)
    previous_scan = Application.get_env(:ferricstore, :flow_lmdb_query_scan_limit)

    Application.put_env(:ferricstore, :flow_lmdb_history_query_scan_limit, 16)
    Application.put_env(:ferricstore, :flow_lmdb_query_scan_limit, 16)

    on_exit(fn ->
      restore_env(:flow_lmdb_history_query_scan_limit, previous_history_scan)
      restore_env(:flow_lmdb_query_scan_limit, previous_scan)
    end)

    ctx = FerricStore.Instance.get(:default)
    id = uid("flow-history-cold-head")
    partition = uid("tenant-history-cold-head")

    assert {:ok, flow} =
             flow_create_and_get(id,
               type: "history-cold-head",
               state: "s0",
               partition_key: partition,
               run_at_ms: 1_000,
               now_ms: 1_000,
               history_hot_max_events: 0,
               history_max_events: 200
             )

    flow =
      Enum.reduce(1..24, flow, fn step, current ->
        assert {:ok, transitioned} =
                 flow_transition_and_get(id, "s#{step - 1}", "s#{step}",
                   partition_key: partition,
                   fencing_token: current.fencing_token,
                   run_at_ms: 1_000 + step,
                   now_ms: 1_000 + step
                 )

        transitioned
      end)

    assert flow.version == 25

    assert {:ok, events} =
             FerricStore.flow_history(id,
               partition_key: partition,
               to_version: 5,
               count: 10
             )

    assert Enum.map(events, fn {_event_id, fields} -> fields["version"] end) ==
             Enum.map(1..5, &Integer.to_string/1)

    assert [{:ok, pipeline_events}] =
             Ferricstore.Flow.pipeline_read_batch(ctx, [
               {:history, id, [partition_key: partition, to_version: 5, count: 10]}
             ])

    assert Enum.map(pipeline_events, fn {_event_id, fields} -> fields["version"] end) ==
             Enum.map(1..5, &Integer.to_string/1)
  end

  test "flow_history filtered cold query finds retained middle events beyond sampled edges" do
    previous_history_scan = Application.get_env(:ferricstore, :flow_lmdb_history_query_scan_limit)

    Application.put_env(:ferricstore, :flow_lmdb_history_query_scan_limit, 8)

    on_exit(fn ->
      restore_env(:flow_lmdb_history_query_scan_limit, previous_history_scan)
    end)

    id = uid("flow-history-cold-middle")
    partition = uid("tenant-history-cold-middle")

    assert {:ok, flow} =
             flow_create_and_get(id,
               type: "history-cold-middle",
               state: "s0",
               partition_key: partition,
               run_at_ms: 1_000,
               now_ms: 1_000,
               history_hot_max_events: 0,
               history_max_events: 200
             )

    flow =
      Enum.reduce(1..30, flow, fn step, current ->
        assert {:ok, transitioned} =
                 flow_transition_and_get(id, "s#{step - 1}", "s#{step}",
                   partition_key: partition,
                   fencing_token: current.fencing_token,
                   run_at_ms: 1_000 + step,
                   now_ms: 1_000 + step
                 )

        transitioned
      end)

    assert flow.version == 31

    assert {:ok, events} =
             FerricStore.flow_history(id,
               partition_key: partition,
               from_version: 16,
               to_version: 16,
               count: 1
             )

    assert Enum.map(events, fn {_event_id, fields} -> fields["version"] end) == ["16"]
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

  test "cold terminal query seeks to filtered time window instead of sampling prefix head" do
    previous_limit = Application.get_env(:ferricstore, :flow_lmdb_query_scan_limit)

    try do
      Application.put_env(:ferricstore, :flow_lmdb_query_scan_limit, 2)

      type = uid("flow-terminals-cold-window")
      partition = uid("tenant-terminals-cold-window")

      ids =
        Enum.map(1..5, fn n ->
          claimed =
            create_claimed_flow(
              uid("flow-terminals-cold-window-#{n}"),
              partition,
              type,
              "worker-terminals-cold-window"
            )

          assert {:ok, _completed} =
                   flow_complete_and_get(claimed.id, claimed.lease_token,
                     partition_key: partition,
                     fencing_token: claimed.fencing_token,
                     now_ms: n * 1_000
                   )

          claimed.id
        end)

      assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(:default, 4)

      assert {:ok, [%{id: id}]} =
               FerricStore.flow_terminals(type,
                 partition_key: partition,
                 state: "completed",
                 include_cold: true,
                 consistent_projection: true,
                 from_ms: 5_000,
                 to_ms: 5_000,
                 count: 1
               )

      assert id == List.last(ids)

      assert {:ok, [%{id: reverse_id}]} =
               FerricStore.flow_terminals(type,
                 partition_key: partition,
                 state: "completed",
                 include_cold: true,
                 consistent_projection: true,
                 rev: true,
                 count: 1
               )

      assert reverse_id == List.last(ids)
    after
      restore_env(:flow_lmdb_query_scan_limit, previous_limit)
    end
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

  test "flow_history falls back to bounded history key scan when Flow index misses" do
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

    Ferricstore.Flow.NativeOrderedIndex.register(
      flow_index,
      flow_lookup,
      Ferricstore.Flow.NativeOrderedIndex.new()
    )

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

    assert {:ok, all_events} = FerricStore.flow_history(id, count: 10)

    assert Enum.map(all_events, fn {_id, fields} -> fields["event"] end) == [
             "created",
             "claimed",
             "completed"
           ]

    assert {:ok, hot_events} = FerricStore.flow_history(id, count: 10, include_cold: false)
    hot_event_ids = Enum.map(hot_events, fn {event_id, _fields} -> event_id end)

    assert hot_events == []
    refute created_event_id in hot_event_ids

    history_key = Ferricstore.Flow.Keys.history_key(id)
    shard = shard_for(history_key)
    {flow_index, flow_lookup} = Ferricstore.Flow.OrderedIndex.table_names(:default, shard)

    assert Ferricstore.Flow.OrderedIndex.count_all(flow_lookup, history_key) == 0

    assert Ferricstore.Flow.OrderedIndex.rank_range(flow_index, history_key, 0, 10, false)
           |> Enum.map(&elem(&1, 0)) == []

    assert [] = :ets.lookup(Ferricstore.Stream.Meta, history_key)

    assert [] =
             Ferricstore.Flow.OrderedIndex.rank_range(flow_index, history_key, 0, 10, false)
             |> Enum.filter(fn {event_id, _score} -> event_id == created_event_id end)
  end

  test "flow history hot-only count returns latest events when hot window is larger than total" do
    id = uid("flow-history-hot-latest")
    type = uid("audit-hot-latest")

    assert {:ok, created} =
             flow_create_and_get(id,
               type: type,
               run_at_ms: 1_000,
               history_hot_max_events: 100,
               now_ms: 1_000
             )

    assert created.history_hot_max_events == 100

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due(type,
               worker: "worker-a",
               lease_ms: 30_000,
               limit: 1,
               now_ms: 1_010
             )

    1..18
    |> Enum.each(fn idx ->
      assert {:ok, _extended} =
               FerricStore.flow_extend_lease(id, claimed.lease_token,
                 fencing_token: claimed.fencing_token,
                 lease_ms: 30_000,
                 now_ms: 1_020 + idx
               )
    end)

    assert {:ok, hot_events} = FerricStore.flow_history(id, count: 5, include_cold: false)

    assert Enum.map(hot_events, fn {_event_id, fields} -> fields["version"] end) ==
             ["16", "17", "18", "19", "20"]
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

    assert Ferricstore.Flow.OrderedIndex.count_all(flow_lookup, history_key) == 0
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

  test "flow history LMDB filtered scans stay bounded to requested history count" do
    original = Application.get_env(:ferricstore, :flow_lmdb_history_query_scan_limit)
    Application.put_env(:ferricstore, :flow_lmdb_history_query_scan_limit, 1_000_000)

    on_exit(fn -> restore_env(:flow_lmdb_history_query_scan_limit, original) end)

    assert Ferricstore.Flow.__flow_history_lmdb_query_scan_count_for_test__(100, false) == 400
    assert Ferricstore.Flow.__flow_history_lmdb_query_scan_count_for_test__(100, true) == 100
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

    assert {:ok, []} = FerricStore.flow_history(id, count: 10, include_cold: false)
  end

  test "flow history default hot max keeps no events hot" do
    original = Application.get_env(:ferricstore, :flow_default_history_hot_max_events)
    restore_env(:flow_default_history_hot_max_events, nil)

    on_exit(fn -> restore_env(:flow_default_history_hot_max_events, original) end)

    assert {:ok, created} =
             flow_create_and_get(uid("flow-history-default-hot-zero"),
               type: "audit-default-hot-zero",
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert created.history_hot_max_events == 0
  end

  test "flow history hot max accepts zero" do
    assert {:ok, created} =
             flow_create_and_get(uid("flow-history-hot-zero"),
               type: "audit-history-hot-zero",
               history_hot_max_events: 0,
               history_max_events: 5,
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert created.history_hot_max_events == 0
  end
    end
  end
end
