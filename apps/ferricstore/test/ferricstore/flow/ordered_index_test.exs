defmodule Ferricstore.Flow.OrderedIndexTest do
  use ExUnit.Case, async: true
  @moduletag :flow

  alias Ferricstore.Flow.NativeOrderedIndex
  alias Ferricstore.Flow.OrderedIndex
  alias Ferricstore.Flow.Keys, as: FlowKeys

  setup do
    instance_name = :"flow_ordered_index_test_#{System.unique_integer([:positive, :monotonic])}"
    {index, lookup} = OrderedIndex.table_names(instance_name, 0)
    NativeOrderedIndex.reset(index, lookup)

    {:ok, index: index, lookup: lookup}
  end

  test "registry identifiers stay off the global atom table" do
    assert {
             {NativeOrderedIndex, :index, :default, 987_654_321},
             {NativeOrderedIndex, :lookup, :default, 987_654_321}
           } = NativeOrderedIndex.table_names(:default, 987_654_321)
  end

  test "native range slice stays isolated by key with offset and reverse", %{
    index: index,
    lookup: lookup
  } do
    native = NativeOrderedIndex.get(index, lookup)

    :ok =
      NativeOrderedIndex.put_entries(native, [
        {"state:a", "a-1", 1},
        {"state:a", "a-2", 2},
        {"state:a", "a-3", 3},
        {"state:a:child", "child-1", 0},
        {"state:b", "b-1", 0}
      ])

    assert NativeOrderedIndex.rank_range(native, "state:a", 1, 2, false) == [
             {"a-2", 2.0},
             {"a-3", 3.0}
           ]

    assert NativeOrderedIndex.rank_range(native, "state:a", 0, 1, true) == [
             {"a-3", 3.0},
             {"a-2", 2.0}
           ]

    assert NativeOrderedIndex.range_slice(
             native,
             "state:a",
             {:inclusive, 1},
             {:inclusive, 3},
             false,
             0,
             :all
           ) == [
             {"a-1", 1.0},
             {"a-2", 2.0},
             {"a-3", 3.0}
           ]
  end

  test "reverse range cursor seeks before an exact score and member", %{
    index: index,
    lookup: lookup
  } do
    native = NativeOrderedIndex.get(index, lookup)

    tied =
      for number <- 1..200 do
        {"state:tied", "flow-#{String.pad_leading(Integer.to_string(number), 3, "0")}", 10}
      end

    :ok = NativeOrderedIndex.put_entries(native, [{"state:tied", "older", 9} | tied])

    assert NativeOrderedIndex.range_slice(
             native,
             "state:tied",
             :neg_inf,
             {:cursor_before, 10, "flow-050"},
             true,
             0,
             10
           ) ==
             for(
               number <- 49..40//-1,
               do: {"flow-#{String.pad_leading(Integer.to_string(number), 3, "0")}", 10.0}
             )
  end

  test "range slice NIF runs on a dirty CPU scheduler" do
    source =
      File.read!(Path.expand("../../../native/ferricstore_bitcask/src/flow_index.rs", __DIR__))

    assert source =~
             ~r/#\[rustler::nif\(schedule = "DirtyCpu"\)\]\s+pub fn flow_index_range_slice\b/

    assert source =~
             ~r/#\[rustler::nif\(schedule = "DirtyCpu"\)\]\s+pub fn flow_index_range_after_slice\b/
  end

  test "all range requests page forward and reverse across tied scores after an offset" do
    native = NativeOrderedIndex.new()

    entries =
      for number <- 1..4_100 do
        member = "flow-#{String.pad_leading(Integer.to_string(number), 5, "0")}"
        {"state:paged", member, 10}
      end

    assert :ok = NativeOrderedIndex.put_entries(native, entries)

    forward =
      NativeOrderedIndex.range_slice(
        native,
        "state:paged",
        :neg_inf,
        :inf,
        false,
        3,
        :all
      )

    reverse =
      NativeOrderedIndex.range_slice(
        native,
        "state:paged",
        :neg_inf,
        :inf,
        true,
        3,
        :all
      )

    assert length(forward) == 4_097
    assert List.first(forward) == {"flow-00004", 10.0}
    assert List.last(forward) == {"flow-04100", 10.0}
    assert length(reverse) == 4_097
    assert List.first(reverse) == {"flow-04097", 10.0}
    assert List.last(reverse) == {"flow-00001", 10.0}

    assert [] =
             Ferricstore.Bitcask.NIF.flow_index_range_slice(
               native,
               "state:paged",
               0,
               0.0,
               0,
               0.0,
               false,
               0,
               0
             )

    assert 4_096 =
             native
             |> Ferricstore.Bitcask.NIF.flow_index_range_slice(
               "state:paged",
               0,
               0.0,
               0,
               0.0,
               false,
               0,
               4_096
             )
             |> length()

    assert {:error, _reason} =
             Ferricstore.Bitcask.NIF.flow_index_range_slice(
               native,
               "state:paged",
               0,
               0.0,
               0,
               0.0,
               false,
               0,
               4_097
             )
  end

  test "normal scheduler index NIFs never wait on the shared index lock" do
    source =
      File.read!(Path.expand("../../../native/ferricstore_bitcask/src/flow_index.rs", __DIR__))

    assert source =~ ~r/use std::sync::\{[^}]*RwLock/
    refute source =~ "std::sync::Mutex"
    refute source =~ ".lock()"

    for function <- ~w(
          flow_index_score_of
          flow_index_count_all
          flow_index_restore_count
          flow_index_delete_count
        ) do
      assert source =~
               Regex.compile!(
                 "#\\[rustler::nif\\(schedule = \"Normal\"\\)\\]\\s+pub fn #{function}\\b[\\s\\S]*?try_(?:read|write)_index"
               )
    end

    for function <- ~w(
          flow_index_put_entries
          flow_index_put_new_entries
          flow_index_move_entries
          flow_index_delete_members
          flow_index_delete_entries
          flow_index_apply_batch
          flow_index_take_due
          flow_index_claim_due_candidates
          flow_index_due_keys_present
          flow_index_count_many
          flow_index_count_keys_page
          flow_index_due_count_keys_page
          flow_index_apply_claim_entries
          flow_index_rollback_claim_entries
        ) do
      assert source =~
               Regex.compile!(
                 "#\\[rustler::nif\\(schedule = \"DirtyCpu\"\\)\\]\\s+pub fn #{function}\\b"
               )
    end
  end

  test "busy retries use capped exponential sleeps instead of spinning" do
    {:ok, attempts} = Agent.start_link(fn -> 0 end)
    test_pid = self()

    result =
      NativeOrderedIndex.__retry_busy_for_test__(
        fn ->
          Agent.get_and_update(attempts, fn attempt ->
            next_attempt = attempt + 1
            result = if next_attempt <= 6, do: :busy, else: :ok
            {result, next_attempt}
          end)
        end,
        fn delay_ms -> send(test_pid, {:retry_sleep, delay_ms}) end
      )

    assert result == :ok
    assert Agent.get(attempts, & &1) == 7

    assert_receive {:retry_sleep, 1}
    assert_receive {:retry_sleep, 2}
    assert_receive {:retry_sleep, 4}
    assert_receive {:retry_sleep, 8}
    assert_receive {:retry_sleep, 8}
    assert_receive {:retry_sleep, 8}
    refute_receive {:retry_sleep, _delay_ms}
  end

  test "every native page fetch uses the capped busy retry contract" do
    source =
      File.read!(Path.expand("../../../lib/ferricstore/flow/native_ordered_index.ex", __DIR__))

    for function <- ~w(
          flow_index_count_keys_page
          flow_index_due_count_keys_page
          flow_index_range_slice
          flow_index_range_cursor_slice
          flow_index_range_after_slice
        ) do
      assert source =~
               Regex.compile!("retry_busy\\(fn ->\\s+NIF\\.#{function}\\(")
    end
  end

  test "bulk put entries are parsed once before lock retries" do
    source =
      File.read!(Path.expand("../../../lib/ferricstore/flow/native_ordered_index.ex", __DIR__))

    assert source =~
             ~r/def put_entries\(resource, entries\) do\s+native_entries = parse_entries\(entries\)\s+retry_busy\(fn -> NIF\.flow_index_put_entries\(resource, native_entries\) end\)\s+end/

    assert source =~
             ~r/def put_new_entries\(resource, entries\) do\s+native_entries = parse_entries\(entries\)\s+retry_busy\(fn -> NIF\.flow_index_put_new_entries\(resource, native_entries\) end\)\s+end/
  end

  test "bulk writes reject invalid scores before mutating the index" do
    native = NativeOrderedIndex.new()

    assert_raise ArgumentError, fn ->
      NativeOrderedIndex.put_entries(native, [
        {"state:queued", "flow-1", 1},
        {"state:queued", "flow-2", :invalid}
      ])
    end

    assert [] = NativeOrderedIndex.rank_range(native, "state:queued", 0, 10, false)
  end

  test "grouped writes reject unknown operations before mutating the index" do
    native = NativeOrderedIndex.new()

    assert_raise ArgumentError, fn ->
      NativeOrderedIndex.apply_batch(native, [
        {:put_entries, [{"state:queued", "flow-1", 1}]},
        {:unknown_operation, "state:queued"}
      ])
    end

    assert [] = NativeOrderedIndex.rank_range(native, "state:queued", 0, 10, false)
  end

  test "native bulk delete removes members across keys in one call", %{
    index: index,
    lookup: lookup
  } do
    native = NativeOrderedIndex.get(index, lookup)

    :ok =
      NativeOrderedIndex.put_entries(native, [
        {"history:1", "1000-1", 1_000},
        {"history:1", "1001-2", 1_001},
        {"history:2", "1000-1", 1_000},
        {"history:2", "1001-2", 1_001}
      ])

    assert :ok =
             NativeOrderedIndex.delete_entries(native, [
               {"history:1", "1000-1"},
               {"history:2", "1001-2"},
               {"history:missing", "1000-1"}
             ])

    assert NativeOrderedIndex.range_slice(native, "history:1", :neg_inf, :inf, false, 0, :all) ==
             [{"1001-2", 1_001.0}]

    assert NativeOrderedIndex.range_slice(native, "history:2", :neg_inf, :inf, false, 0, :all) ==
             [{"1000-1", 1_000.0}]
  end

  test "native claim planner encodes next state and index entry" do
    partition_key = "tenant-a"

    record = %{
      id: "flow-1",
      type: "email",
      state: "queued",
      version: 1,
      attempts: 0,
      fencing_token: 0,
      created_at_ms: 10,
      updated_at_ms: 11,
      next_run_at_ms: 20,
      priority: 0,
      ttl_ms: nil,
      history_hot_max_events: nil,
      history_max_events: nil,
      retention_ttl_ms: nil,
      terminal_retention_until_ms: nil,
      partition_key: partition_key,
      payload_ref: "payload-1",
      parent_flow_id: nil,
      parent_partition_key: nil,
      root_flow_id: "flow-1",
      correlation_id: nil,
      result_ref: nil,
      error_ref: nil,
      lease_owner: nil,
      lease_token: nil,
      lease_deadline_ms: nil,
      run_state: "queued",
      rewound_to_event_id: nil,
      child_groups: %{}
    }

    from_due_key = FlowKeys.due_key("email", "queued", 0, partition_key)
    to_due_key = FlowKeys.due_key("email", "running", 0, partition_key)
    from_state_key = FlowKeys.state_index_key("email", "queued", partition_key)
    to_state_key = FlowKeys.state_index_key("email", "running", partition_key)
    inflight_key = FlowKeys.inflight_index_key("email", partition_key)
    worker_key = FlowKeys.worker_index_key("worker-a", partition_key)
    state_key_prefix = FlowKeys.state_key("", partition_key)

    assert {:ok, [{next_value, entry, state_key, 11}], [], 1} =
             NativeOrderedIndex.plan_claims(
               [{"flow-1", 20.0}],
               [Ferricstore.Flow.encode_record(record)],
               "email",
               "queued",
               "worker-a",
               50,
               25,
               10,
               from_due_key,
               to_due_key,
               from_state_key,
               to_state_key,
               inflight_key,
               worker_key,
               state_key_prefix
             )

    next = Ferricstore.Flow.decode_record(next_value)

    assert next.state == "running"
    assert next.version == 2
    assert next.fencing_token == 1
    assert next.lease_owner == "worker-a"
    assert next.lease_token == "worker-a:25:1"
    assert next.lease_deadline_ms == 75
    assert next.next_run_at_ms == 75
    assert next.ttl_ms == nil
    assert next.terminal_retention_until_ms == nil
    assert next.run_state == "queued"
    assert state_key == FlowKeys.state_key("flow-1", partition_key)

    assert entry ==
             {"flow-1", from_due_key, 20.0, to_due_key, 75.0, from_state_key, 11.0, to_state_key,
              25.0, inflight_key, worker_key, 75.0}
  end

  test "native claim planner can produce history-ready claim entries" do
    partition_key = "tenant-a"

    record = %{
      id: "flow-1",
      type: "email",
      state: "queued",
      version: 1,
      attempts: 0,
      fencing_token: 0,
      created_at_ms: 10,
      updated_at_ms: 11,
      next_run_at_ms: 20,
      priority: 0,
      ttl_ms: nil,
      history_hot_max_events: 1,
      history_max_events: 10,
      retention_ttl_ms: nil,
      terminal_retention_until_ms: nil,
      partition_key: partition_key,
      payload_ref: "payload-1",
      parent_flow_id: nil,
      parent_partition_key: nil,
      root_flow_id: "flow-1",
      correlation_id: nil,
      result_ref: nil,
      error_ref: nil,
      lease_owner: nil,
      lease_token: nil,
      lease_deadline_ms: nil,
      run_state: "queued",
      rewound_to_event_id: nil,
      child_groups: %{}
    }

    tag = FlowKeys.tag(partition_key)
    from_due_key = FlowKeys.due_key("email", "queued", 0, partition_key)
    to_due_key = FlowKeys.due_key("email", "running", 0, partition_key)
    from_state_key = FlowKeys.state_index_key("email", "queued", partition_key)
    to_state_key = FlowKeys.state_index_key("email", "running", partition_key)
    inflight_key = FlowKeys.inflight_index_key("email", partition_key)
    worker_key = FlowKeys.worker_index_key("worker-a", partition_key)
    state_key_prefix = FlowKeys.state_key("", partition_key)
    history_key_prefix = "f:" <> tag <> ":h:"

    assert {:ok,
            [
              {
                next_value,
                _entry,
                state_key,
                11,
                {history_key, event_id, event_ms, 2, history_entry_key, history_value, 1, 10,
                 false}
              }
            ], [], 1} =
             NativeOrderedIndex.plan_claims_with_history(
               [{"flow-1", 20.0}],
               [Ferricstore.Flow.encode_record(record)],
               "email",
               "queued",
               "worker-a",
               50,
               25,
               10,
               from_due_key,
               to_due_key,
               from_state_key,
               to_state_key,
               inflight_key,
               worker_key,
               state_key_prefix,
               history_key_prefix
             )

    next = Ferricstore.Flow.decode_record(next_value)

    assert history_value == Ferricstore.Flow.encode_history_fields(next, "claimed", 25, %{})

    history_fields =
      history_value
      |> Ferricstore.Flow.decode_history_fields(next)
      |> Enum.chunk_every(2)
      |> Map.new(fn [key, value] -> {key, value} end)

    assert state_key == FlowKeys.state_key("flow-1", partition_key)
    assert history_key == FlowKeys.history_key("flow-1", partition_key)
    assert event_id == "25-2"
    assert event_ms == 25
    assert history_entry_key == FlowKeys.stream_entry_key("flow-1", event_id, partition_key)
    assert history_fields["event"] == "claimed"
    assert history_fields["version"] == "2"
    assert history_fields["state"] == "running"
    assert history_fields["lease_owner"] == "worker-a"
    assert history_fields["lease_deadline_ms"] == "75"
  end

  test "native claim planner reports stale missing records and skips state mismatches" do
    partition_key = "tenant-a"

    record = %{
      id: "flow-2",
      type: "email",
      state: "paused",
      version: 1,
      attempts: 0,
      fencing_token: 0,
      created_at_ms: 10,
      updated_at_ms: 11,
      next_run_at_ms: 20,
      priority: 0,
      ttl_ms: nil,
      history_hot_max_events: nil,
      history_max_events: nil,
      retention_ttl_ms: nil,
      terminal_retention_until_ms: nil,
      partition_key: partition_key,
      payload_ref: "payload-1",
      parent_flow_id: nil,
      parent_partition_key: nil,
      root_flow_id: "flow-2",
      correlation_id: nil,
      result_ref: nil,
      error_ref: nil,
      lease_owner: nil,
      lease_token: nil,
      lease_deadline_ms: nil,
      run_state: "paused",
      rewound_to_event_id: nil,
      child_groups: %{}
    }

    from_due_key = FlowKeys.due_key("email", "queued", 0, partition_key)
    to_due_key = FlowKeys.due_key("email", "running", 0, partition_key)
    from_state_key = FlowKeys.state_index_key("email", "queued", partition_key)
    to_state_key = FlowKeys.state_index_key("email", "running", partition_key)
    inflight_key = FlowKeys.inflight_index_key("email", partition_key)
    worker_key = FlowKeys.worker_index_key("worker-a", partition_key)
    state_key_prefix = FlowKeys.state_key("", partition_key)

    assert {:ok, [], ["missing"], 0} =
             NativeOrderedIndex.plan_claims(
               [{"missing", 20.0}, {"flow-2", 20.0}],
               [nil, Ferricstore.Flow.encode_record(record)],
               "email",
               "queued",
               "worker-a",
               50,
               25,
               10,
               from_due_key,
               to_due_key,
               from_state_key,
               to_state_key,
               inflight_key,
               worker_key,
               state_key_prefix
             )
  end

  test "range_slice returns members ordered by score then id", %{index: index, lookup: lookup} do
    assert :ok =
             OrderedIndex.put_members(index, lookup, "due:email", [
               {"b", 10},
               {"a", 10},
               {"c", 12}
             ])

    assert [{"a", 10.0}, {"b", 10.0}] =
             OrderedIndex.range_slice(
               index,
               "due:email",
               :neg_inf,
               {:inclusive, 10.0},
               false,
               0,
               10
             )

    assert [{"c", 12.0}, {"b", 10.0}] =
             OrderedIndex.range_slice(index, "due:email", :neg_inf, :inf, true, 0, 2)

    assert [{"b", 10.0}, {"c", 12.0}] =
             OrderedIndex.rank_range(index, "due:email", 1, 2, false)
  end

  test "put_entries batches updates without double-counting", %{index: index, lookup: lookup} do
    assert :ok =
             OrderedIndex.put_entries(index, lookup, [
               {"history:a", "1000-1", 1_000},
               {"history:b", "1000-1", 1_000},
               {"history:a", "2000-2", 2_000}
             ])

    assert 2 = OrderedIndex.count_all(lookup, "history:a")
    assert 1 = OrderedIndex.count_all(lookup, "history:b")

    assert :ok = OrderedIndex.put_entries(index, lookup, [{"history:a", "2000-2", 2_500}])
    assert 2 = OrderedIndex.count_all(lookup, "history:a")

    assert [{"1000-1", 1000.0}, {"2000-2", 2500.0}] =
             OrderedIndex.rank_range(index, "history:a", 0, 10, false)
  end

  test "put_new_entries is append-only for validated new members", %{
    index: index,
    lookup: lookup
  } do
    assert :ok =
             OrderedIndex.put_new_entries(index, lookup, [
               {"history:a", "1000-1", 1_000},
               {"history:b", "1000-1", 1_000},
               {"history:a", "2000-2", 2_000}
             ])

    assert 2 = OrderedIndex.count_all(lookup, "history:a")
    assert 1 = OrderedIndex.count_all(lookup, "history:b")

    assert [{"1000-1", 1000.0}, {"2000-2", 2000.0}] =
             OrderedIndex.rank_range(index, "history:a", 0, 10, false)
  end

  test "delete_members removes lookup, ordered index, and count", %{index: index, lookup: lookup} do
    assert :ok =
             OrderedIndex.put_members(index, lookup, "worker:a", [
               {"flow-1", 3},
               {"flow-2", 4}
             ])

    assert :ok = OrderedIndex.delete_members(index, lookup, "worker:a", ["flow-1", "missing"])

    assert 1 = OrderedIndex.count_all(lookup, "worker:a")

    assert [{"flow-2", 4.0}] =
             OrderedIndex.range_slice(index, "worker:a", :neg_inf, :inf, false, 0, 10)
  end

  test "delete_members removes zero-count keys from lookup table", %{
    index: index,
    lookup: lookup
  } do
    assert :ok = OrderedIndex.put_members(index, lookup, "worker:empty", [{"flow-1", 3}])
    assert :ok = OrderedIndex.delete_members(index, lookup, "worker:empty", ["flow-1"])

    assert 0 = OrderedIndex.count_all(lookup, "worker:empty")
    assert [] = OrderedIndex.count_keys_page(lookup, nil, 4_096)
  end

  test "due count key pages return only positive due keys", %{
    index: index,
    lookup: lookup
  } do
    due_a = "f:{f:a}:d:email:queued:p0"
    due_b = "f:{f:b}:d:email:queued:p0"
    non_due = "f:{f:a}:i:s:email:queued"

    assert :ok = OrderedIndex.put_members(index, lookup, due_a, [{"flow-1", 1}])
    assert :ok = OrderedIndex.put_members(index, lookup, due_b, [{"flow-2", 2}])
    assert :ok = OrderedIndex.put_members(index, lookup, non_due, [{"flow-3", 3}])

    assert MapSet.new([due_a, due_b, non_due]) ==
             MapSet.new(OrderedIndex.count_keys_page(lookup, nil, 4_096))

    assert MapSet.new([due_a, due_b]) ==
             MapSet.new(OrderedIndex.due_count_keys_page(lookup, nil, 4_096))

    assert :ok = OrderedIndex.delete_members(index, lookup, due_a, ["flow-1"])

    assert [due_b] == OrderedIndex.due_count_keys_page(lookup, nil, 4_096)
  end

  test "count key cursor pages are ordered and resume after the last key" do
    native = NativeOrderedIndex.new()
    due_a = "f:{flow:a}:d:email:queued:p0"
    due_b = "f:{flow:b}:d:email:queued:p0"

    assert :ok = NativeOrderedIndex.restore_count(native, "plain:b", 1)
    assert :ok = NativeOrderedIndex.restore_count(native, due_b, 1)
    assert :ok = NativeOrderedIndex.restore_count(native, "plain:a", 1)
    assert :ok = NativeOrderedIndex.restore_count(native, due_a, 1)

    assert [^due_a, ^due_b] = NativeOrderedIndex.count_keys_page(native, nil, 2)
    assert ["plain:a", "plain:b"] = NativeOrderedIndex.count_keys_page(native, due_b, 2)
    assert [^due_a] = NativeOrderedIndex.due_count_keys_page(native, nil, 1)
    assert [^due_b] = NativeOrderedIndex.due_count_keys_page(native, due_a, 1)

    assert [] = Ferricstore.Bitcask.NIF.flow_index_count_keys_page(native, nil, 0)
    assert 4 = length(Ferricstore.Bitcask.NIF.flow_index_count_keys_page(native, nil, 4_096))

    assert {:error, _reason} =
             Ferricstore.Bitcask.NIF.flow_index_count_keys_page(native, nil, 4_097)

    native_source =
      File.read!(Path.expand("../../../native/ferricstore_bitcask/src/flow_index.rs", __DIR__))

    facade_source =
      File.read!(Path.expand("../../../lib/ferricstore/flow/native_ordered_index.ex", __DIR__))

    ordered_source =
      File.read!(Path.expand("../../../lib/ferricstore/flow/ordered_index.ex", __DIR__))

    refute native_source =~ "pub fn flow_index_count_keys<'a>"
    refute native_source =~ "pub fn flow_index_due_count_keys<'a>"
    refute facade_source =~ ~r/^\s*def count_keys\(/m
    refute facade_source =~ ~r/^\s*def due_count_keys\(/m
    refute ordered_source =~ ~r/^\s*def count_keys\(/m
    refute ordered_source =~ ~r/^\s*def due_count_keys\(/m
  end

  test "due count key reducer consumes and discards bounded pages" do
    native = NativeOrderedIndex.new()

    entries =
      for number <- 1..4_097 do
        key = "f:{flow:#{String.pad_leading(Integer.to_string(number), 5, "0")}}:d:t:q:p0"
        {key, "flow", 1}
      end

    assert :ok = NativeOrderedIndex.put_entries(native, entries)

    assert {[4_096, 1], 4_097} =
             NativeOrderedIndex.reduce_due_count_key_pages(
               native,
               {[], 0},
               fn page, {sizes, total} ->
                 {:cont, {[length(page) | sizes], total + length(page)}}
               end
             )
             |> then(fn {sizes, total} -> {Enum.reverse(sizes), total} end)

    assert {:stopped, 4_096} =
             NativeOrderedIndex.reduce_due_count_key_pages(native, :initial, fn page, :initial ->
               {:halt, {:stopped, length(page)}}
             end)
  end

  test "due count key pages filter positive native counts", %{
    index: index,
    lookup: lookup
  } do
    due_key = "f:{f:legacy}:d:email:queued:p0"
    non_due_key = "f:{f:legacy}:i:s:email:queued"
    native = NativeOrderedIndex.get(index, lookup)

    assert :ok = NativeOrderedIndex.restore_count(native, due_key, 2)
    assert :ok = NativeOrderedIndex.restore_count(native, non_due_key, 2)

    assert [due_key] == OrderedIndex.due_count_keys_page(lookup, nil, 4_096)
  end

  test "move_entries moves members across keys and keeps counters consistent", %{
    index: index,
    lookup: lookup
  } do
    assert :ok =
             OrderedIndex.put_members(index, lookup, "queued", [
               {"flow-1", 10},
               {"flow-2", 20}
             ])

    assert :ok =
             OrderedIndex.put_members(index, lookup, "running", [
               {"flow-3", 30}
             ])

    assert :ok =
             OrderedIndex.move_entries(index, lookup, [
               {"queued", "running", "flow-1", 100},
               {"missing", "running", "flow-4", 40}
             ])

    assert [{"flow-2", 20.0}] =
             OrderedIndex.rank_range(index, "queued", 0, 10, false)

    assert [{"flow-3", 30.0}, {"flow-4", 40.0}, {"flow-1", 100.0}] =
             OrderedIndex.rank_range(index, "running", 0, 10, false)

    assert_index_invariants(index, lookup)
  end

  test "move_entries updates same-key scores without changing counts", %{
    index: index,
    lookup: lookup
  } do
    assert :ok = OrderedIndex.put_members(index, lookup, "due", [{"flow-1", 10}])

    assert :ok =
             OrderedIndex.move_entries(index, lookup, [
               {"due", "due", "flow-1", 50}
             ])

    assert 1 = OrderedIndex.count_all(lookup, "due")
    assert [{"flow-1", 50.0}] = OrderedIndex.rank_range(index, "due", 0, 10, false)
    assert_index_invariants(index, lookup)
  end

  test "native ordered index facade matches native core semantics", %{
    index: index,
    lookup: lookup
  } do
    native = NativeOrderedIndex.new()

    assert :ok =
             OrderedIndex.put_members(index, lookup, "due:email", [
               {"b", 10},
               {"a", 10},
               {"c", 12}
             ])

    assert :ok =
             NativeOrderedIndex.put_members(native, "due:email", [
               {"b", 10},
               {"a", 10},
               {"c", 12}
             ])

    assert OrderedIndex.range_slice(index, "due:email", :neg_inf, :inf, false, 0, 10) ==
             NativeOrderedIndex.range_slice(native, "due:email", :neg_inf, :inf, false, 0, 10)

    assert OrderedIndex.range_slice(index, "due:email", :neg_inf, :inf, true, 0, 2) ==
             NativeOrderedIndex.range_slice(native, "due:email", :neg_inf, :inf, true, 0, 2)

    assert :ok =
             OrderedIndex.move_entries(index, lookup, [
               {"due:email", "running", "a", 50},
               {"missing", "running", "d", 40}
             ])

    assert :ok =
             NativeOrderedIndex.move_entries(native, [
               {"due:email", "running", "a", 50},
               {"missing", "running", "d", 40}
             ])

    assert OrderedIndex.rank_range(index, "running", 0, 10, false) ==
             NativeOrderedIndex.rank_range(native, "running", 0, 10, false)

    assert OrderedIndex.count_all(lookup, "due:email") ==
             NativeOrderedIndex.count_all(native, "due:email")

    assert OrderedIndex.count_all(lookup, "running") ==
             NativeOrderedIndex.count_all(native, "running")

    assert {:ok, 50.0} = NativeOrderedIndex.score_of(native, "running", "a")
    assert :miss = NativeOrderedIndex.score_of(native, "running", "missing")

    assert :ok = OrderedIndex.delete_members(index, lookup, "running", ["a"])
    assert :ok = NativeOrderedIndex.delete_members(native, "running", ["a"])

    assert OrderedIndex.rank_range(index, "running", 0, 10, false) ==
             NativeOrderedIndex.rank_range(native, "running", 0, 10, false)
  end

  test "native ordered index facade writes directly to native storage", %{
    index: index,
    lookup: lookup
  } do
    due_key = "f:{f:native}:d:email:queued:p0"

    assert :ok = OrderedIndex.put_members(index, lookup, due_key, [{"flow-1", 1}])

    native = NativeOrderedIndex.get(index, lookup)

    assert [{<<"flow-1">>, 1.0}] =
             NativeOrderedIndex.range_slice(native, due_key, :neg_inf, :inf, false, 0, 10)

    assert [due_key] == NativeOrderedIndex.due_count_keys_page(native, nil, 4_096)
  end

  test "native claim entries mutate and rollback due index" do
    native = NativeOrderedIndex.new()

    assert :ok = NativeOrderedIndex.put_new_member(native, "due:queued", "flow-1", 10.0)
    assert :ok = NativeOrderedIndex.put_new_member(native, "state:queued", "flow-1", 11.0)

    entry =
      {"flow-1", "due:queued", 10.0, "due:running", 20.0, "state:queued", 11.0, "state:running",
       21.0, "inflight", "worker:a", 30.0}

    assert :ok = NativeOrderedIndex.apply_claim_entries(native, [entry])

    assert [] = NativeOrderedIndex.rank_range(native, "due:queued", 0, 10, false)
    assert [{"flow-1", 20.0}] = NativeOrderedIndex.rank_range(native, "due:running", 0, 10, false)
    assert :miss = NativeOrderedIndex.score_of(native, "state:queued", "flow-1")
    assert {:ok, 21.0} = NativeOrderedIndex.score_of(native, "state:running", "flow-1")
    assert [{"flow-1", 30.0}] = NativeOrderedIndex.rank_range(native, "inflight", 0, 10, false)
    assert [{"flow-1", 30.0}] = NativeOrderedIndex.rank_range(native, "worker:a", 0, 10, false)
    assert 0 = NativeOrderedIndex.count_all(native, "due:queued")
    assert 1 = NativeOrderedIndex.count_all(native, "due:running")
    assert 0 = NativeOrderedIndex.count_all(native, "state:queued")
    assert 1 = NativeOrderedIndex.count_all(native, "state:running")
    assert 1 = NativeOrderedIndex.count_all(native, "inflight")
    assert 1 = NativeOrderedIndex.count_all(native, "worker:a")

    assert :ok = NativeOrderedIndex.rollback_claim_entries(native, [entry])

    assert [{"flow-1", 10.0}] = NativeOrderedIndex.rank_range(native, "due:queued", 0, 10, false)
    assert [] = NativeOrderedIndex.rank_range(native, "due:running", 0, 10, false)
    assert {:ok, 11.0} = NativeOrderedIndex.score_of(native, "state:queued", "flow-1")
    assert :miss = NativeOrderedIndex.score_of(native, "state:running", "flow-1")
    assert [] = NativeOrderedIndex.rank_range(native, "inflight", 0, 10, false)
    assert [] = NativeOrderedIndex.rank_range(native, "worker:a", 0, 10, false)
    assert 1 = NativeOrderedIndex.count_all(native, "due:queued")
    assert 0 = NativeOrderedIndex.count_all(native, "due:running")
    assert 1 = NativeOrderedIndex.count_all(native, "state:queued")
    assert 0 = NativeOrderedIndex.count_all(native, "state:running")
    assert 0 = NativeOrderedIndex.count_all(native, "inflight")
    assert 0 = NativeOrderedIndex.count_all(native, "worker:a")
  end

  test "native apply_batch applies mixed ops under one API" do
    native = NativeOrderedIndex.new()

    claim_entry =
      {"flow-2", "due:queued", 20.0, "due:running", 60.0, "state:queued", 21.0, "state:running",
       61.0, "inflight", "worker:b", 70.0}

    assert :ok =
             NativeOrderedIndex.apply_batch(native, [
               {:put_new_entries,
                [
                  {"due:queued", "flow-1", 10},
                  {"due:queued", "flow-2", 20},
                  {"state:queued", "flow-2", 21}
                ]},
               {:put_entries, [{"metadata", "flow-3", 30}]},
               {:move_entries, [{"due:queued", "due:later", "flow-1", 40}]},
               {:delete_members, "metadata", ["flow-3"]},
               {:apply_claim_entries, [claim_entry]}
             ])

    assert [] = NativeOrderedIndex.rank_range(native, "due:queued", 0, 10, false)
    assert [{"flow-1", 40.0}] = NativeOrderedIndex.rank_range(native, "due:later", 0, 10, false)
    assert [{"flow-2", 60.0}] = NativeOrderedIndex.rank_range(native, "due:running", 0, 10, false)
    assert [] = NativeOrderedIndex.rank_range(native, "metadata", 0, 10, false)
    assert :miss = NativeOrderedIndex.score_of(native, "state:queued", "flow-2")
    assert {:ok, 61.0} = NativeOrderedIndex.score_of(native, "state:running", "flow-2")
    assert 0 = NativeOrderedIndex.count_all(native, "due:queued")
    assert 1 = NativeOrderedIndex.count_all(native, "due:later")
    assert 1 = NativeOrderedIndex.count_all(native, "due:running")
    assert 0 = NativeOrderedIndex.count_all(native, "metadata")
    assert 0 = NativeOrderedIndex.count_all(native, "state:queued")
    assert 1 = NativeOrderedIndex.count_all(native, "state:running")
    assert 1 = NativeOrderedIndex.count_all(native, "inflight")
    assert 1 = NativeOrderedIndex.count_all(native, "worker:b")

    assert [0, 1, 1, 0] =
             NativeOrderedIndex.count_many(native, [
               "due:queued",
               "due:later",
               "state:running",
               "metadata"
             ])
  end

  test "native apply_batch preserves grouped op order for duplicate members" do
    native = NativeOrderedIndex.new()

    assert :ok =
             NativeOrderedIndex.apply_batch(native, [
               {:put_entries, [{"idx", "flow-1", 1}]},
               {:put_entries, [{"idx", "flow-1", 2}]},
               {:move_entries, [{"idx", "running", "flow-1", 3}]},
               {:move_entries, [{"running", "later", "flow-1", 4}]}
             ])

    assert [] = NativeOrderedIndex.rank_range(native, "idx", 0, 10, false)
    assert [] = NativeOrderedIndex.rank_range(native, "running", 0, 10, false)
    assert [{"flow-1", 4.0}] = NativeOrderedIndex.rank_range(native, "later", 0, 10, false)
  end

  @tag :flow_native_chunking
  test "native batch chunking applies requests above the item ceiling" do
    native = NativeOrderedIndex.new()

    entries =
      Enum.map(0..100_000, fn index ->
        {"chunked:index", Integer.to_string(index), index}
      end)

    ops = [{:put_entries, entries}]
    expected = {:error, "flow index native request exceeds safety budget"}
    assert ^expected = NativeOrderedIndex.apply_batch(native, ops)

    assert {:ok, [first_chunk, second_chunk]} =
             NativeOrderedIndex.chunk_batch_ops(ops)

    assert :ok = NativeOrderedIndex.apply_batch(native, first_chunk)
    assert :ok = NativeOrderedIndex.apply_batch(native, second_chunk)
    assert 100_001 == NativeOrderedIndex.count_all(native, "chunked:index")
    assert {:ok, zero_score} = NativeOrderedIndex.score_of(native, "chunked:index", "0")
    assert zero_score == 0.0
    assert {:ok, 100_000.0} = NativeOrderedIndex.score_of(native, "chunked:index", "100000")
  end

  @tag :flow_native_chunking
  test "native batch chunking bounds aggregate request bytes without copying binaries" do
    member = :binary.copy("m", 65_535)
    entries = List.duplicate({"byte:index", member, 1.0}, 1_025)
    ops = [{:put_entries, entries}]

    assert {:ok, chunks} = NativeOrderedIndex.chunk_batch_ops(ops)
    assert length(chunks) == 2

    assert 1_025 ==
             Enum.sum(
               Enum.map(chunks, fn [{:put_entries, chunk_entries}] ->
                 length(chunk_entries)
               end)
             )

    Enum.each(chunks, fn chunk ->
      assert :ok =
               Enum.reduce(chunk, :ok, fn op, :ok ->
                 assert {:ok, {items, bytes}} = NativeOrderedIndex.batch_budget(op)
                 NativeOrderedIndex.validate_request_budget(items, bytes)
               end)
    end)
  end

  @tag :flow_native_chunking
  test "native batch chunking applies the six-item claim weight" do
    claim =
      {"flow", "due:queued", 1.0, "due:running", 2.0, "state:queued", 1.0, "state:running", 2.0,
       "inflight", "worker", 3.0}

    ops = [{:apply_claim_entries, List.duplicate(claim, 16_667)}]

    assert {:ok,
            [
              [{:apply_claim_entries, first_chunk}],
              [{:apply_claim_entries, second_chunk}]
            ]} = NativeOrderedIndex.chunk_batch_ops(ops)

    assert length(first_chunk) == 16_666
    assert length(second_chunk) == 1
  end

  test "native take_due returns due members in order and removes them" do
    native = NativeOrderedIndex.new()

    assert :ok =
             NativeOrderedIndex.put_new_entries(native, [
               {"due:queued", "flow-2", 20.0},
               {"due:queued", "flow-1", 10.0},
               {"due:queued", "flow-3", 30.0},
               {"due:other", "flow-4", 5.0}
             ])

    assert [{"flow-1", 10.0}, {"flow-2", 20.0}] =
             NativeOrderedIndex.take_due(native, "due:queued", 25.0, 10)

    assert [{"flow-3", 30.0}] = NativeOrderedIndex.rank_range(native, "due:queued", 0, 10, false)
    assert [{"flow-4", 5.0}] = NativeOrderedIndex.rank_range(native, "due:other", 0, 10, false)
  end

  test "native claim_due_candidates returns ordered due-key runs directly" do
    native = NativeOrderedIndex.new()

    assert :ok =
             NativeOrderedIndex.put_new_entries(native, [
               {"due:queued", "queued-1", 100.0},
               {"due:queued", "queued-2", 101.0},
               {"due:retry", "retry-1", 10.0}
             ])

    assert [
             {"due:retry", [{"retry-1", 10.0}]},
             {"due:queued", [{"queued-1", 100.0}, {"queued-2", 101.0}]}
           ] =
             NativeOrderedIndex.claim_due_candidates(
               native,
               ["due:queued", "due:retry"],
               200.0,
               3,
               16
             )
  end

  defp assert_index_invariants(index, lookup) do
    Enum.each(collect_count_key_pages(lookup, nil, []), fn key ->
      members = OrderedIndex.range_slice(index, key, :neg_inf, :inf, false, 0, :all)
      assert length(members) == OrderedIndex.count_all(lookup, key)
    end)
  end

  defp collect_count_key_pages(lookup, cursor, pages) do
    case OrderedIndex.count_keys_page(lookup, cursor, 4_096) do
      [] ->
        pages
        |> Enum.reverse()
        |> :lists.append()

      keys ->
        collect_count_key_pages(lookup, List.last(keys), [keys | pages])
    end
  end
end
