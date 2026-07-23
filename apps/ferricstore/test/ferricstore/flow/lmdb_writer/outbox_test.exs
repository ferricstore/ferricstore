defmodule Ferricstore.Flow.LMDBWriter.OutboxTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Flow.LMDBWriter.Outbox
  alias Ferricstore.Flow.LMDBWriter.Registry
  alias Ferricstore.Flow.LMDBWriter

  @instance_name :lmdb_outbox_batch_test
  @shard_index 0

  setup do
    table = Registry.ensure_projection_outbox!(@instance_name, @shard_index)
    :ets.delete_all_objects(table)
    generation = :atomics.new(3, signed: false)

    on_exit(fn ->
      case :ets.whereis(Registry.projection_outbox_name(@instance_name, @shard_index)) do
        :undefined -> :ok
        tid -> :ets.delete(tid)
      end
    end)

    {:ok,
     state: %{
       instance_name: @instance_name,
       shard_index: @shard_index,
       enqueue_seq: generation
     },
     table: table,
     generation: generation}
  end

  test "takes projection entries in bounded sequence-ordered batches", %{
    state: state,
    table: table,
    generation: generation
  } do
    :ets.insert(table, {:dirty, generation, 1})

    :ets.insert(
      table,
      for sequence <- 1..2_000 do
        {sequence, generation, :full, "F:#{sequence}", sequence}
      end
    )

    first = Outbox.take_projection_outbox_entries(state)

    assert length(first) == 1_024
    assert Enum.map(first, &elem(&1, 0)) == Enum.to_list(1..1_024)
    assert Outbox.projection_outbox_pending?(state)
    assert :ets.lookup(table, :dirty) == [{:dirty, generation, 1}]

    second = Outbox.take_projection_outbox_entries(state)

    assert length(second) == 976
    assert Enum.map(second, &elem(&1, 0)) == Enum.to_list(1_025..2_000)
    refute Outbox.projection_outbox_pending?(state)
    assert :ets.lookup(table, :dirty) == [{:dirty, generation, 1}]
  end

  test "does not copy the complete outbox before applying its batch limit" do
    source =
      File.read!(Path.expand("../../../../lib/ferricstore/flow/lmdb_writer/outbox.ex", __DIR__))

    refute source =~ ":ets.tab2list"
  end

  test "drops malformed public outbox rows while preserving valid work", %{
    state: state,
    table: table,
    generation: generation
  } do
    :ets.insert(table, [
      {1, generation, :invalid, "F:invalid-mode", 1},
      {2, generation, :full, "F:invalid-version", -1},
      {3, generation, :query_only, "F:valid", 3}
    ])

    assert Outbox.take_projection_outbox_entries(state) == [
             {3, generation, :query_only, "F:valid", 3}
           ]

    refute Outbox.projection_outbox_pending?(state)
  end

  test "does not clear a dirty marker replaced during reconciliation", %{
    state: state,
    table: table,
    generation: generation
  } do
    :ets.insert(table, {:dirty, generation, 1})
    marker = Outbox.projection_dirty_marker(state)

    :ets.insert(table, {:dirty, generation, 2})

    assert :ok = Outbox.clear_projection_dirty_marker(state, marker)
    assert :ets.lookup(table, :dirty) == [{:dirty, generation, 2}]
  end

  test "does not clear a dirty marker before its notification is handled", %{
    state: state,
    table: table,
    generation: generation
  } do
    :ets.insert(table, {:dirty, generation, 1})
    state = Map.put(state, :projection_dirty?, false)

    assert {^state, :ok} = Outbox.maybe_reconcile_dirty_projection_with_reply(state, :ok)
    assert :ets.lookup(table, :dirty) == [{:dirty, generation, 1}]
  end

  test "coalesces current dirty markers without allowing stale generations to overwrite", %{
    table: table,
    generation: generation
  } do
    Registry.publish_enqueue_seq(@instance_name, @shard_index, generation)

    on_exit(fn ->
      :persistent_term.erase(Registry.enqueue_seq_key(@instance_name, @shard_index))
    end)

    assert :ok =
             Registry.put_projection_dirty_marker(
               table,
               @instance_name,
               @shard_index,
               generation
             )

    [{:dirty, ^generation, first_token}] = :ets.lookup(table, :dirty)

    assert :ok =
             Registry.put_projection_dirty_marker(
               table,
               @instance_name,
               @shard_index,
               generation
             )

    [{:dirty, ^generation, second_token} = current] = :ets.lookup(table, :dirty)
    assert second_token > first_token

    stale_generation = :atomics.new(3, signed: false)

    assert {:error, :writer_restarted} =
             Registry.put_projection_dirty_marker(
               table,
               @instance_name,
               @shard_index,
               stale_generation
             )

    assert :ets.lookup(table, :dirty) == [current]
  end

  test "reports remaining work after adding one bounded batch to pending state", %{
    state: identity,
    table: table,
    generation: generation
  } do
    :ets.insert(
      table,
      for sequence <- 1..1_025 do
        {sequence, generation, :full, "F:#{sequence}", sequence}
      end
    )

    state =
      Map.merge(identity, %{
        pending: [],
        pending_after_flush: [],
        count: 0,
        first_pending_at: nil,
        last_enqueue_at: nil,
        timer_ref: nil,
        flush_interval_ms: 60_000,
        instance_ctx: nil,
        requested_index: 0,
        durable_index: 0
      })

    assert {drained, true} = Outbox.drain_projection_outbox(state)
    assert drained.count == 1_024
    assert length(drained.pending) == 1_024
    assert :ets.info(table, :size) == 1

    if drained.timer_ref, do: Process.cancel_timer(drained.timer_ref)
  end

  test "does not overtake earlier sequence-numbered projection enqueues", %{
    state: identity,
    table: table,
    generation: generation
  } do
    :atomics.put(generation, 1, 1)
    :ets.insert(table, {1, generation, :full, "F:terminal", 3})

    state =
      Map.merge(identity, %{
        processed_enqueue_seq: 0,
        pending: [],
        pending_after_flush: [],
        count: 0,
        first_pending_at: nil,
        last_enqueue_at: nil
      })

    assert {^state, true} = Outbox.drain_projection_outbox(state)
    assert :ets.lookup(table, 1) == [{1, generation, :full, "F:terminal", 3}]
  end

  test "carries the expected record version into source projection work", %{
    state: state,
    generation: generation
  } do
    assert {[{:project_flow_state_from_source, "F:terminal", 7}], []} =
             Outbox.projection_outbox_items(state, [
               {1, generation, :full, "F:terminal", 7}
             ])
  end

  test "query-only outbox work never schedules terminal hot-index pruning", %{
    state: state,
    generation: generation
  } do
    assert {[{:project_flow_query_state_from_source, "F:terminal", 7}], []} =
             Outbox.projection_outbox_items(state, [
               {1, generation, :query_only, "F:terminal", 7}
             ])
  end

  test "keeps a retry timer after dirty projection reconciliation fails" do
    unique = System.unique_integer([:positive])
    instance_name = :"lmdb_dirty_retry_#{unique}"
    data_dir = Path.join(System.tmp_dir!(), "ferricstore_lmdb_dirty_retry_#{unique}")
    Ferricstore.DataDir.ensure_layout!(data_dir, 1)

    on_exit(fn -> File.rm_rf!(data_dir) end)

    pid =
      start_supervised!(
        {LMDBWriter, shard_index: 0, data_dir: data_dir, instance_ctx: %{name: instance_name}}
      )

    assert :ok = LMDBWriter.mark_projection_dirty(instance_name, 0)
    assert {:error, :source_keydir_unavailable} = LMDBWriter.flush(instance_name, 0)

    state = :sys.get_state(pid)
    assert state.projection_dirty?
    assert is_reference(state.timer_ref)
  end

  test "treats a previous writer generation as possible volatile projection loss" do
    unique = System.unique_integer([:positive])
    instance_name = :"lmdb_writer_restart_#{unique}"
    data_dir = Path.join(System.tmp_dir!(), "ferricstore_lmdb_writer_restart_#{unique}")
    degraded = :atomics.new(1, signed: false)
    previous_generation = :atomics.new(2, signed: false)
    :atomics.put(previous_generation, 1, 7)
    :atomics.put(previous_generation, 2, 7)
    Registry.publish_enqueue_seq(instance_name, 0, previous_generation)
    Ferricstore.DataDir.ensure_layout!(data_dir, 1)

    on_exit(fn ->
      :persistent_term.erase(Registry.enqueue_seq_key(instance_name, 0))
      File.rm_rf!(data_dir)
    end)

    pid =
      start_supervised!(
        {LMDBWriter,
         shard_index: 0,
         data_dir: data_dir,
         instance_ctx: %{
           name: instance_name,
           flow_lmdb_mirror_degraded: degraded
         }}
      )

    state = :sys.get_state(pid)
    assert :atomics.get(degraded, 1) == 1
    assert state.projection_dirty?
    assert is_reference(state.timer_ref)
  end

  test "projection outbox normalization rejects malformed entries without partial enqueue" do
    assert {:ok, [{:full, "state-a", 1}, {:query_only, "state-b", 2}]} =
             Registry.normalize_projection_outbox_entries([
               {:full, "state-a", 1},
               {:query_only, "state-b", 2}
             ])

    for entries <- [
          [{:full, "state-a", 1}, {:full, "", 2}],
          [{:full, "state-a", 1}, {:full, "state-b", -1}],
          [{:full, "state-a", 1}, {:invalid, "state-b", 2}],
          [{:full, "state-a", 1}, :invalid],
          :invalid
        ] do
      assert {:error, :invalid_projection_outbox_entries} =
               Registry.normalize_projection_outbox_entries(entries)
    end
  end
end
