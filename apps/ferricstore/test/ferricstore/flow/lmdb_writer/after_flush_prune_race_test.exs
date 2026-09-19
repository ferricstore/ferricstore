defmodule Ferricstore.Flow.LMDBWriter.AfterFlushPruneRaceTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Flow.Keys
  alias Ferricstore.Flow.Locator
  alias Ferricstore.Flow.Hibernation
  alias Ferricstore.Flow.LMDBWriter.ProjectionOps
  alias Ferricstore.Flow.NativeOrderedIndex
  alias Ferricstore.Flow.LMDBWriter.AfterFlush
  alias Ferricstore.Flow.Query.QueryRowCodec
  alias Ferricstore.Flow.LMDB
  alias Ferricstore.Raft.WARaftSegmentReader
  alias Ferricstore.Store.Shard.ZSetIndex

  test "terminal prune keeps newer tagged rows and metadata for both actions" do
    for action_kind <- [:direct, :from_source] do
      run_prune_race(action_kind, {:flow_state_version, 1, 0}, {:flow_state_version, 2, 0})
    end
  end

  test "terminal prune keeps newer untagged rows and metadata for both actions" do
    for action_kind <- [:direct, :from_source] do
      run_prune_race(action_kind, 0, 0)
    end
  end

  test "terminal prune fails closed when the source projection is not durable" do
    id = "after-flush-prune-missing-#{System.unique_integer([:positive])}"
    state_key = Keys.state_key(id)
    projection_index = System.unique_integer([:positive])
    ets = :ets.new(:after_flush_prune_missing_keydir, [:set])

    row =
      {state_key, nil, 0, {:flow_state_version, 1, 0},
       {:waraft_apply_projection, projection_index}, 0, 1}

    true = :ets.insert(ets, row)

    action =
      {:prune_terminal_flow, "/tmp/after-flush-prune-missing", 0, ets, nil, nil, nil, nil,
       state_key, "cleanup", "completed", nil, nil, nil, nil, id, 1, nil}

    assert {:error,
            {:source_not_durable,
             {:apply_projection_entry_not_durable, ^projection_index, ^state_key}}} =
             AfterFlush.apply_after_flush(action)

    assert [^row] = :ets.lookup(ets, state_key)
  end

  test "terminal prune keeps a writer publication from racing index cleanup" do
    run_post_delete_race(:terminal)
  end

  test "hibernation eviction keeps a writer publication from racing index cleanup" do
    run_post_delete_race(:hibernate)
  end

  test "terminal prune does not clean indexes when an absent source appears while waiting" do
    test_pid = self()
    hook_ref = make_ref()
    suffix = System.unique_integer([:positive])
    id = "after-flush-absent-source-#{suffix}"
    state_key = Keys.state_key(id)
    ets = :ets.new(:after_flush_absent_source_keydir, [:set, :public])
    zset_index = :ets.new(:after_flush_absent_source_zset, [:ordered_set, :public])
    zset_lookup = :ets.new(:after_flush_absent_source_lookup, [:set, :public])
    instance_name = String.to_atom("after_flush_absent_source_#{suffix}")
    {flow_index, flow_lookup} = NativeOrderedIndex.table_names(instance_name, 0)
    native = NativeOrderedIndex.reset(flow_index, flow_lookup)
    publication_ctx = publication_ctx()
    old_record = flow_record(id, suffix)
    new_record = Map.merge(old_record, %{version: 2, payload_ref: "new-payload"})
    new_encoded = Ferricstore.Flow.encode_record(new_record)

    new_row =
      {state_key, new_encoded, 0, {:flow_state_version, 2, 0}, :memory, 0, byte_size(new_encoded)}

    state_index_key = Keys.state_index_key("cleanup", "completed", nil)

    metadata_index_keys =
      ProjectionOps.terminal_project_metadata_index_keys(
        id,
        nil,
        old_record.parent_flow_id,
        old_record.root_flow_id,
        old_record.correlation_id
      )

    assert :ok = ZSetIndex.mark_ready_empty(zset_index, zset_lookup, state_index_key)
    assert :ok = ZSetIndex.put_member(zset_index, zset_lookup, state_index_key, id, "1")

    Enum.each(metadata_index_keys, fn index_key ->
      assert :ok = NativeOrderedIndex.put_member(native, index_key, id, 1)
    end)

    on_exit(fn ->
      Process.delete(:ferricstore_after_flush_before_publication_write_hook)
      NativeOrderedIndex.unregister(flow_index, flow_lookup)
    end)

    action =
      {:prune_terminal_flow, "/tmp/after-flush-absent-source", 0, ets, zset_index, zset_lookup,
       flow_index, flow_lookup, state_key, "cleanup", "completed", nil, old_record.parent_flow_id,
       old_record.root_flow_id, old_record.correlation_id, id, 1, old_record.incarnation}

    caller =
      spawn(fn ->
        Process.put(:ferricstore_after_flush_before_publication_write_hook, fn
          :prune_absent, _ets, ^state_key ->
            send(test_pid, {hook_ref, :before_write})

            receive do
              :continue -> :ok
            end
        end)

        send(test_pid, {hook_ref, :result, AfterFlush.apply_after_flush(action, publication_ctx)})
      end)

    assert_receive {^hook_ref, :before_write}

    spawn(fn ->
      Ferricstore.Store.PublicationEpoch.with_write(publication_ctx, 0, fn ->
        true = :ets.insert(ets, new_row)
        assert :ok = ZSetIndex.put_member(zset_index, zset_lookup, state_index_key, id, "1")

        Enum.each(metadata_index_keys, fn index_key ->
          assert :ok = NativeOrderedIndex.put_member(native, index_key, id, 2)
        end)

        send(test_pid, {hook_ref, :writer_inserted})
      end)
    end)

    assert_receive {^hook_ref, :writer_inserted}
    send(caller, :continue)
    assert_receive {^hook_ref, :result, :ok}
    assert [^new_row] = :ets.lookup(ets, state_key)
    assert [{{^state_index_key, ^id}, 1.0}] = :ets.lookup(zset_lookup, {state_index_key, id})

    Enum.each(metadata_index_keys, fn index_key ->
      assert {:ok, 2.0} = NativeOrderedIndex.score_of(native, index_key, id)
    end)
  end

  defp run_prune_race(action_kind, old_lfu, new_lfu) do
    test_pid = self()
    hook_ref = make_ref()
    suffix = System.unique_integer([:positive])
    id = "after-flush-prune-race-#{suffix}"
    state_key = Keys.state_key(id)
    projection_index = System.unique_integer([:positive])
    data_dir = Path.join(System.tmp_dir!(), "ferricstore-#{id}")
    Ferricstore.DataDir.ensure_layout!(data_dir, 1)
    lmdb_path = LMDB.path(Ferricstore.DataDir.shard_data_path(data_dir, 0))
    ets = :ets.new(:after_flush_prune_race_keydir, [:set])
    zset_index = :ets.new(:after_flush_prune_race_zset, [:ordered_set])
    zset_lookup = :ets.new(:after_flush_prune_race_lookup, [:set])
    state_index_key = Keys.state_index_key("cleanup", "completed", nil)
    instance_name = String.to_atom("after_flush_prune_race_#{suffix}")
    {flow_index, flow_lookup} = NativeOrderedIndex.table_names(instance_name, 0)
    native = NativeOrderedIndex.reset(flow_index, flow_lookup)

    old_record = flow_record(id, suffix)

    new_record =
      Map.merge(old_record, %{
        state: "queued",
        version: 2,
        updated_at_ms: 3,
        payload_ref: "new-payload",
        terminal_retention_until_ms: nil
      })

    old_encoded = Ferricstore.Flow.encode_record(old_record)
    new_encoded = Ferricstore.Flow.encode_record(new_record)

    old_row =
      {state_key, nil, 0, old_lfu, {:waraft_apply_projection, projection_index}, 0,
       byte_size(old_encoded)}

    new_row =
      {state_key, new_encoded, 0, new_lfu, :memory, 0, byte_size(new_encoded)}

    metadata_index_keys =
      ProjectionOps.terminal_project_metadata_index_keys(
        id,
        nil,
        old_record.parent_flow_id,
        old_record.root_flow_id,
        old_record.correlation_id
      )

    true = :ets.insert(ets, old_row)

    assert :ok =
             WARaftSegmentReader.put_apply_projection(
               data_dir,
               0,
               projection_index,
               [{state_key, old_encoded, 0}]
             )

    seed_query_row!(lmdb_path, state_key, old_record)

    assert :ok = ZSetIndex.mark_ready_empty(zset_index, zset_lookup, state_index_key)
    assert :ok = ZSetIndex.put_member(zset_index, zset_lookup, state_index_key, id, "1")

    Enum.each(metadata_index_keys, fn index_key ->
      assert :ok = NativeOrderedIndex.put_member(native, index_key, id, 1)
    end)

    on_exit(fn ->
      Process.delete(:ferricstore_waraft_apply_projection_disk_read_hook)
      NativeOrderedIndex.unregister(flow_index, flow_lookup)

      WARaftSegmentReader.delete_apply_projection_entries(
        data_dir,
        0,
        [{projection_index, state_key}]
      )

      File.rm_rf!(data_dir)
    end)

    Process.put(
      :ferricstore_waraft_apply_projection_disk_read_hook,
      fn _root, index, source ->
        send(test_pid, {hook_ref, :called, index, source})

        if source == :latest and index == projection_index do
          true = :ets.insert(ets, new_row)
        end

        :ok
      end
    )

    action =
      case action_kind do
        :direct ->
          {:prune_terminal_flow, data_dir, 0, ets, zset_index, zset_lookup, flow_index,
           flow_lookup, state_key, "cleanup", "completed", nil, old_record.parent_flow_id,
           old_record.root_flow_id, old_record.correlation_id, id, 1, old_record.incarnation}

        :from_source ->
          {:prune_terminal_flow_from_source, data_dir, 0, ets, zset_index, zset_lookup,
           flow_index, flow_lookup, state_key, 1}
      end

    assert :ok = AfterFlush.apply_after_flush(action)
    assert_receive {^hook_ref, :called, ^projection_index, :latest}
    assert [^new_row] = :ets.lookup(ets, state_key)

    assert {:ok,
            %{
              id: ^id,
              state: "queued",
              version: 2,
              payload_ref: "new-payload",
              root_flow_id: root_flow_id,
              correlation_id: correlation_id
            }} = AfterFlush.flow_record_from_keydir_row(data_dir, 0, state_key, new_row)

    assert root_flow_id == old_record.root_flow_id
    assert correlation_id == old_record.correlation_id

    assert [{{^state_index_key, ^id}, 1.0}] =
             :ets.lookup(zset_lookup, {state_index_key, id})

    Enum.each(metadata_index_keys, fn index_key ->
      assert {:ok, 1.0} = NativeOrderedIndex.score_of(native, index_key, id)
    end)

    Process.delete(:ferricstore_waraft_apply_projection_disk_read_hook)
    NativeOrderedIndex.unregister(flow_index, flow_lookup)

    WARaftSegmentReader.delete_apply_projection_entries(data_dir, 0, [
      {projection_index, state_key}
    ])

    File.rm_rf!(data_dir)
  end

  defp run_post_delete_race(:terminal) do
    test_pid = self()
    hook_ref = make_ref()
    suffix = System.unique_integer([:positive])
    id = "after-flush-post-delete-terminal-#{suffix}"
    state_key = Keys.state_key(id)
    ets = :ets.new(:after_flush_post_delete_terminal_keydir, [:set, :public])
    zset_index = :ets.new(:after_flush_post_delete_terminal_zset, [:ordered_set, :public])
    zset_lookup = :ets.new(:after_flush_post_delete_terminal_lookup, [:set, :public])
    instance_name = String.to_atom("after_flush_post_delete_terminal_#{suffix}")
    {flow_index, flow_lookup} = NativeOrderedIndex.table_names(instance_name, 0)
    native = NativeOrderedIndex.reset(flow_index, flow_lookup)
    publication_ctx = publication_ctx()

    old_record = flow_record(id, suffix)
    new_record = Map.merge(old_record, %{version: 2, payload_ref: "new-payload"})
    old_encoded = Ferricstore.Flow.encode_record(old_record)
    new_encoded = Ferricstore.Flow.encode_record(new_record)

    old_row =
      {state_key, old_encoded, 0, {:flow_state_version, 1, 0}, :memory, 0, byte_size(old_encoded)}

    new_row =
      {state_key, new_encoded, 0, {:flow_state_version, 2, 0}, :memory, 0, byte_size(new_encoded)}

    state_index_key = Keys.state_index_key("cleanup", "completed", nil)

    metadata_index_keys =
      ProjectionOps.terminal_project_metadata_index_keys(
        id,
        nil,
        old_record.parent_flow_id,
        old_record.root_flow_id,
        old_record.correlation_id
      )

    true = :ets.insert(ets, old_row)
    assert :ok = ZSetIndex.mark_ready_empty(zset_index, zset_lookup, state_index_key)
    assert :ok = ZSetIndex.put_member(zset_index, zset_lookup, state_index_key, id, "1")

    Enum.each(metadata_index_keys, fn index_key ->
      assert :ok = NativeOrderedIndex.put_member(native, index_key, id, 1)
    end)

    on_exit(fn ->
      Process.delete(:ferricstore_after_flush_hot_delete_hook)
      NativeOrderedIndex.unregister(flow_index, flow_lookup)
    end)

    Process.put(:ferricstore_after_flush_hot_delete_hook, fn :terminal, _ets, _row ->
      spawn(fn ->
        Ferricstore.Store.PublicationEpoch.with_write(publication_ctx, 0, fn ->
          true = :ets.insert(ets, new_row)
          assert :ok = ZSetIndex.put_member(zset_index, zset_lookup, state_index_key, id, "1")

          Enum.each(metadata_index_keys, fn index_key ->
            assert :ok = NativeOrderedIndex.put_member(native, index_key, id, 2)
          end)

          send(test_pid, {hook_ref, :writer_done})
        end)
      end)

      refute_receive {^hook_ref, :writer_done}, 50
      send(test_pid, {hook_ref, :hook_returned})
      :ok
    end)

    action =
      {:prune_terminal_flow, "/tmp/after-flush-post-delete-terminal", 0, ets, zset_index,
       zset_lookup, flow_index, flow_lookup, state_key, "cleanup", "completed", nil,
       old_record.parent_flow_id, old_record.root_flow_id, old_record.correlation_id, id, 1,
       old_record.incarnation}

    assert :ok = AfterFlush.apply_after_flush(action, publication_ctx)
    assert_receive {^hook_ref, :hook_returned}
    assert_receive {^hook_ref, :writer_done}
    assert [^new_row] = :ets.lookup(ets, state_key)
    assert [{{^state_index_key, ^id}, 1.0}] = :ets.lookup(zset_lookup, {state_index_key, id})

    Enum.each(metadata_index_keys, fn index_key ->
      assert {:ok, 2.0} = NativeOrderedIndex.score_of(native, index_key, id)
    end)
  end

  defp run_post_delete_race(:hibernate) do
    test_pid = self()
    hook_ref = make_ref()
    suffix = System.unique_integer([:positive])
    id = "after-flush-post-delete-hibernate-#{suffix}"
    state_key = Keys.state_key(id)
    ets = :ets.new(:after_flush_post_delete_hibernate_keydir, [:set, :public])
    zset_index = :ets.new(:after_flush_post_delete_hibernate_zset, [:ordered_set, :public])
    zset_lookup = :ets.new(:after_flush_post_delete_hibernate_lookup, [:set, :public])
    instance_name = String.to_atom("after_flush_post_delete_hibernate_#{suffix}")
    {flow_index, flow_lookup} = NativeOrderedIndex.table_names(instance_name, 0)
    native = NativeOrderedIndex.reset(flow_index, flow_lookup)
    publication_ctx = publication_ctx()

    record =
      flow_record(id, suffix, %{state: "queued", version: 1, next_run_at_ms: 100, priority: 0})

    encoded = Ferricstore.Flow.encode_record(record)
    row = {state_key, encoded, 0, 0, :memory, 0, byte_size(encoded)}

    locator = %Locator{
      flow_id: id,
      kind: :state,
      version: 1,
      raft_index: 1,
      file_id: :memory,
      offset: 0,
      value_size: byte_size(encoded)
    }

    index_keys = Hibernation.hot_index_keys(record, due_any?: true)
    true = :ets.insert(ets, row)

    Enum.each(index_keys, fn index_key ->
      assert :ok = ZSetIndex.mark_ready_empty(zset_index, zset_lookup, index_key)
      assert :ok = ZSetIndex.put_member(zset_index, zset_lookup, index_key, id, "1")
      assert :ok = NativeOrderedIndex.put_member(native, index_key, id, 1)
    end)

    on_exit(fn ->
      Process.delete(:ferricstore_after_flush_hot_delete_hook)
      NativeOrderedIndex.unregister(flow_index, flow_lookup)
    end)

    Process.put(:ferricstore_after_flush_hot_delete_hook, fn :hibernate, _ets, _row ->
      spawn(fn ->
        Ferricstore.Store.PublicationEpoch.with_write(publication_ctx, 0, fn ->
          true = :ets.insert(ets, row)

          Enum.each(index_keys, fn index_key ->
            assert :ok = ZSetIndex.put_member(zset_index, zset_lookup, index_key, id, "1")
            assert :ok = NativeOrderedIndex.put_member(native, index_key, id, 2)
          end)

          send(test_pid, {hook_ref, :writer_done})
        end)
      end)

      refute_receive {^hook_ref, :writer_done}, 50
      send(test_pid, {hook_ref, :hook_returned})
      :ok
    end)

    action =
      {:hibernate_flow_evict_hot,
       %{
         data_dir: "/tmp/after-flush-post-delete-hibernate",
         shard_index: 0,
         ets: ets,
         flow_index: flow_index,
         flow_lookup: flow_lookup,
         zset_index: zset_index,
         zset_lookup: zset_lookup,
         state_key: state_key,
         record: record,
         locator: locator
       }}

    assert :ok = AfterFlush.apply_after_flush(action, publication_ctx)
    assert_receive {^hook_ref, :hook_returned}
    assert_receive {^hook_ref, :writer_done}
    assert [^row] = :ets.lookup(ets, state_key)

    Enum.each(index_keys, fn index_key ->
      assert [{{^index_key, ^id}, 1.0}] = :ets.lookup(zset_lookup, {index_key, id})
      assert {:ok, 2.0} = NativeOrderedIndex.score_of(native, index_key, id)
    end)
  end

  defp publication_ctx do
    latch = :ets.new(:after_flush_publication_latch, [:set, :public])
    %{publication_epoch: :atomics.new(1, signed: false), latch_refs: {latch}}
  end

  defp seed_query_row!(lmdb_path, state_key, record) do
    encoded = Ferricstore.Flow.encode_record(record)

    locator =
      Locator.new!(
        flow_id: record.id,
        kind: :state,
        version: record.version,
        raft_index: record.version,
        file_id: 0,
        offset: 0,
        value_size: byte_size(encoded),
        checksum: :crypto.hash(:sha256, encoded)
      )

    assert {:ok, query_row} = QueryRowCodec.encode(state_key, record, locator, 0)
    assert :ok = LMDB.write_batch(lmdb_path, [{:put, state_key, query_row}])
  end

  defp flow_record(id, suffix, overrides \\ %{}) do
    Map.merge(
      %{
        id: id,
        type: "cleanup",
        state: "completed",
        version: 1,
        attempts: 0,
        fencing_token: 0,
        created_at_ms: 1,
        updated_at_ms: 2,
        next_run_at_ms: nil,
        priority: 0,
        ttl_ms: nil,
        history_hot_max_events: nil,
        history_max_events: nil,
        retention_ttl_ms: nil,
        max_active_ms: nil,
        terminal_retention_until_ms: 1,
        partition_key: nil,
        payload_ref: "old-payload",
        parent_flow_id: "parent-flow-#{suffix}",
        parent_partition_key: nil,
        root_flow_id: "root-flow-#{suffix}",
        correlation_id: "correlation-#{suffix}",
        result_ref: nil,
        error_ref: nil,
        lease_owner: nil,
        lease_token: nil,
        lease_deadline_ms: 0,
        run_state: nil,
        child_groups: %{},
        incarnation: suffix,
        state_enter_seq: suffix
      },
      overrides
    )
  end
end
