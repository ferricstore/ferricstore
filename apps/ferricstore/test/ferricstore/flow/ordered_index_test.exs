defmodule Ferricstore.Flow.OrderedIndexTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.NativeOrderedIndex
  alias Ferricstore.Flow.OrderedIndex
  alias Ferricstore.Flow.Keys, as: FlowKeys

  setup do
    index = :ets.new(:flow_ordered_index_test_index, [:ordered_set])
    lookup = :ets.new(:flow_ordered_index_test_lookup, [:set])
    {:ok, index: index, lookup: lookup}
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

    assert {:ok, [], stale_ids, 0} =
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

    assert stale_ids == ["missing", "flow-2"]
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

  test "due_count_keys returns only positive due keys", %{
    index: index,
    lookup: lookup
  } do
    due_a = "f:{f:a}:d:email:queued:p0"
    due_b = "f:{f:b}:d:email:queued:p0"
    non_due = "f:{f:a}:i:s:email:queued"

    assert :ok = OrderedIndex.put_members(index, lookup, due_a, [{"flow-1", 1}])
    assert :ok = OrderedIndex.put_members(index, lookup, due_b, [{"flow-2", 2}])
    assert :ok = OrderedIndex.put_members(index, lookup, non_due, [{"flow-3", 3}])

    assert MapSet.new([due_a, due_b, non_due]) == MapSet.new(OrderedIndex.count_keys(lookup))

    assert MapSet.new([due_a, due_b]) == MapSet.new(OrderedIndex.due_count_keys(lookup))

    assert :ok = OrderedIndex.delete_members(index, lookup, due_a, ["flow-1"])

    assert [due_b] == OrderedIndex.due_count_keys(lookup)
  end

  test "due_count_keys filters legacy positive counts", %{
    lookup: lookup
  } do
    due_key = "f:{f:legacy}:d:email:queued:p0"
    non_due_key = "f:{f:legacy}:i:s:email:queued"

    :ets.insert(lookup, {{:count, due_key}, 2})
    :ets.insert(lookup, {{:count, non_due_key}, 2})

    assert [due_key] == OrderedIndex.due_count_keys(lookup)
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

  test "native ordered index matches ETS ordered index core semantics", %{
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

  test "native ordered index rebuilds and registers from ETS", %{
    index: index,
    lookup: lookup
  } do
    due_key = "f:{f:native}:d:email:queued:p0"

    assert :ok = OrderedIndex.put_members(index, lookup, due_key, [{"flow-1", 1}])
    assert :ok = NativeOrderedIndex.rebuild_from_ets(index, lookup)

    native = NativeOrderedIndex.get(index, lookup)

    assert [{<<"flow-1">>, 1.0}] =
             NativeOrderedIndex.range_slice(native, due_key, :neg_inf, :inf, false, 0, 10)

    assert [due_key] == NativeOrderedIndex.due_count_keys(native)
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

  defp assert_index_invariants(index, lookup) do
    lookup_entries =
      lookup
      |> :ets.tab2list()
      |> Enum.filter(fn
        {{:count, _key}, _count} -> false
        {{_key, _member}, _score} -> true
      end)

    index_entries = :ets.tab2list(index)

    counts =
      Enum.reduce(lookup_entries, %{}, fn {{key, member}, score}, acc ->
        assert :ets.member(index, {key, score, member})
        Map.update(acc, key, 1, &(&1 + 1))
      end)

    Enum.each(index_entries, fn
      {{key, score, member}, true} ->
        assert [{{^key, ^member}, ^score}] = :ets.lookup(lookup, {key, member})
    end)

    Enum.each(counts, fn {key, count} ->
      assert count == OrderedIndex.count_all(lookup, key)
    end)
  end
end
