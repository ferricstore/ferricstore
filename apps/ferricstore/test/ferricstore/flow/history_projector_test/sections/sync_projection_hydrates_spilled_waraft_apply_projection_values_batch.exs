defmodule Ferricstore.Flow.HistoryProjectorTest.Sections.SyncProjectionHydratesSpilledWaraftApplyProjectionValuesBatch do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Bitcask.NIF
      alias Ferricstore.Flow.HistoryProjector
      alias Ferricstore.Flow.NativeOrderedIndex
      alias Ferricstore.Flow.OrderedIndex

      test "sync projection hydrates spilled WARaft apply-projection values by batch" do
        unique = System.unique_integer([:positive])
        instance_name = :"history_projector_apply_projection_#{unique}"

        data_dir =
          Path.join(System.tmp_dir!(), "ferricstore_history_projector_apply_projection_#{unique}")

        dir = Path.join([data_dir, "data", "shard_0"])

        File.mkdir_p!(dir)

        keydir = :ets.new(:"history_projector_apply_projection_keydir_#{unique}", [:set, :public])
        {flow_index, flow_lookup} = OrderedIndex.table_names(instance_name, 0)
        NativeOrderedIndex.reset(flow_index, flow_lookup)

        on_exit(fn ->
          File.rm_rf(data_dir)
        end)

        ctx = %{
          name: instance_name,
          data_dir: data_dir,
          keydir_refs: {keydir},
          checkpoint_flags: :atomics.new(1, signed: false),
          disk_pressure: :atomics.new(1, signed: false),
          write_version: :counters.new(1, [:write_concurrency]),
          keydir_binary_bytes: :atomics.new(1, signed: true),
          flow_history_projected_index: :atomics.new(1, signed: false),
          flow_history_projector_flush_failures: :atomics.new(1, signed: false)
        }

        projection_index = 42
        id_a = "flow-apply-projection-a"
        id_b = "flow-apply-projection-b"
        value_key_a = "f:{flow-apply-projection-a}:v:p:#{id_a}:1"
        value_key_b = "f:{flow-apply-projection-b}:v:p:#{id_b}:1"
        source_value_a = Ferricstore.Flow.encode_value("payload-a")
        source_value_b = Ferricstore.Flow.encode_value("payload-b")

        assert :ok =
                 Ferricstore.Raft.WARaftSegmentReader.put_apply_projection(
                   data_dir,
                   0,
                   projection_index,
                   [{value_key_a, source_value_a, 0}, {value_key_b, source_value_b, 0}]
                 )

        assert {:ok, 2} =
                 Ferricstore.Raft.WARaftSegmentReader.spill_apply_projection_cache(data_dir, 0)

        for {key, value} <- [{value_key_a, source_value_a}, {value_key_b, source_value_b}] do
          :ets.insert(
            keydir,
            {key, nil, 0, Ferricstore.Store.LFU.initial(),
             {:waraft_apply_projection, projection_index}, 0, byte_size(value)}
          )
        end

        entries =
          [
            {id_a, value_key_a, source_value_a, 1_000, 1},
            {id_b, value_key_b, source_value_b, 1_001, 2}
          ]
          |> Enum.map(fn {id, payload_ref, _source_value, event_ms, version} ->
            record = %{
              id: id,
              type: "audit",
              state: "queued",
              version: version,
              partition_key: nil,
              priority: 0,
              attempts: 0,
              next_run_at_ms: event_ms,
              created_at_ms: event_ms,
              updated_at_ms: event_ms,
              lease_owner: nil,
              lease_deadline_ms: 0,
              payload_ref: payload_ref
            }

            event_id = "#{event_ms}-#{version}"

            %{
              key: Ferricstore.Flow.Keys.stream_entry_key(id, event_id, nil),
              expire_at_ms: 0,
              history_key: Ferricstore.Flow.Keys.history_key(id),
              event_id: event_id,
              event_ms: event_ms,
              version: version,
              value: Ferricstore.Flow.encode_history_fields(record, "created", event_ms, %{}),
              ra_index: projection_index
            }
          end)

        assert :ok = HistoryProjector.write_entries_sync(ctx, 0, dir, entries, projection_index)

        history_path = HistoryProjector.history_file_path(dir, 0)
        assert {:ok, records} = NIF.v2_scan_file(history_path)

        keys =
          Enum.map(records, fn {key, _offset, _value_size, _expire_at_ms, _deleted?} -> key end)

        assert value_key_a in keys
        assert value_key_b in keys
        assert [] = :ets.lookup(keydir, value_key_a)
        assert [] = :ets.lookup(keydir, value_key_b)

        lmdb_path = Ferricstore.Flow.LMDB.path(dir)
        assert {:ok, lmdb_value_a} = Ferricstore.Flow.LMDB.get(lmdb_path, value_key_a)
        assert {:ok, lmdb_value_b} = Ferricstore.Flow.LMDB.get(lmdb_path, value_key_b)

        assert {:ok, {{:flow_history, file_id_a}, offset_a, value_size_a}} =
                 Ferricstore.Flow.LMDB.decode_value_locator(lmdb_value_a, 1_000)

        assert {:ok, {{:flow_history, file_id_b}, offset_b, value_size_b}} =
                 Ferricstore.Flow.LMDB.decode_value_locator(lmdb_value_b, 1_001)

        assert value_size_a == byte_size(source_value_a)
        assert value_size_b == byte_size(source_value_b)
        assert file_id_a == 0
        assert file_id_b == 0
        assert is_integer(offset_a) and offset_a >= 0
        assert is_integer(offset_b) and offset_b >= 0

        assert {:ok, ^source_value_a} =
                 HistoryProjector.read_value(dir, {:flow_history, file_id_a}, offset_a)

        assert {:ok, ^source_value_b} =
                 HistoryProjector.read_value(dir, {:flow_history, file_id_b}, offset_b)

        assert {:ok, pins} =
                 Ferricstore.Flow.LMDB.segment_value_pin_entries_before(
                   lmdb_path,
                   projection_index + 1,
                   100
                 )

        assert pins == []
      end

      test "apply projection cache spill clears only the spilled shard root" do
        unique = System.unique_integer([:positive])
        data_dir = Path.join(System.tmp_dir!(), "ferricstore_apply_projection_spill_#{unique}")
        File.mkdir_p!(data_dir)

        ctx = %{data_dir: data_dir}
        shard0_key_a = "apply-projection-spill-shard0-a"
        shard0_key_b = "apply-projection-spill-shard0-b"
        shard1_key = "apply-projection-spill-shard1"

        on_exit(fn -> File.rm_rf(data_dir) end)

        assert :ok =
                 Ferricstore.Raft.WARaftSegmentReader.put_apply_projection(
                   data_dir,
                   0,
                   101,
                   [{shard0_key_a, "value-a", 0}, {shard0_key_b, "value-b", 0}]
                 )

        assert :ok =
                 Ferricstore.Raft.WARaftSegmentReader.put_apply_projection(
                   data_dir,
                   1,
                   202,
                   [{shard1_key, "value-c", 0}]
                 )

        assert Ferricstore.Raft.WARaftSegmentReader.apply_projection_cache_count(data_dir, 0) == 2
        assert Ferricstore.Raft.WARaftSegmentReader.apply_projection_cache_count(data_dir, 1) == 1

        assert {:ok, 2} =
                 Ferricstore.Raft.WARaftSegmentReader.spill_apply_projection_cache(data_dir, 0)

        assert Ferricstore.Raft.WARaftSegmentReader.apply_projection_cache_count(data_dir, 0) == 0
        assert Ferricstore.Raft.WARaftSegmentReader.apply_projection_cache_count(data_dir, 1) == 1

        assert {:ok, %{^shard0_key_a => "value-a", ^shard0_key_b => "value-b"}} =
                 Ferricstore.Raft.WARaftSegmentReader.read_values_from_location(
                   ctx,
                   0,
                   {:waraft_apply_projection, 101},
                   [shard0_key_a, shard0_key_b]
                 )

        assert {:ok, %{^shard1_key => "value-c"}} =
                 Ferricstore.Raft.WARaftSegmentReader.read_values_from_location(
                   ctx,
                   1,
                   {:waraft_apply_projection, 202},
                   [shard1_key]
                 )
      end

      test "apply projection cache spill can remove a bounded slice" do
        unique = System.unique_integer([:positive])

        data_dir =
          Path.join(System.tmp_dir!(), "ferricstore_apply_projection_bounded_spill_#{unique}")

        File.mkdir_p!(data_dir)

        ctx = %{data_dir: data_dir}
        shard0_key_a = "apply-projection-bounded-shard0-a"
        shard0_key_b = "apply-projection-bounded-shard0-b"
        shard0_key_c = "apply-projection-bounded-shard0-c"
        shard1_key = "apply-projection-bounded-shard1"

        on_exit(fn -> File.rm_rf(data_dir) end)

        assert :ok =
                 Ferricstore.Raft.WARaftSegmentReader.put_apply_projection(
                   data_dir,
                   0,
                   301,
                   [{shard0_key_a, "value-a", 0}, {shard0_key_b, "value-b", 0}]
                 )

        assert :ok =
                 Ferricstore.Raft.WARaftSegmentReader.put_apply_projection(
                   data_dir,
                   0,
                   302,
                   [{shard0_key_c, "value-c", 0}]
                 )

        assert :ok =
                 Ferricstore.Raft.WARaftSegmentReader.put_apply_projection(
                   data_dir,
                   1,
                   401,
                   [{shard1_key, "value-d", 0}]
                 )

        assert Ferricstore.Raft.WARaftSegmentReader.apply_projection_cache_count(data_dir, 0) == 3
        assert Ferricstore.Raft.WARaftSegmentReader.apply_projection_cache_count(data_dir, 1) == 1

        assert {:ok, removed} =
                 Ferricstore.Raft.WARaftSegmentReader.spill_apply_projection_cache(data_dir, 0, 2)

        assert removed <= 3
        assert removed > 0

        assert Ferricstore.Raft.WARaftSegmentReader.apply_projection_cache_count(data_dir, 0) ==
                 3 - removed

        assert Ferricstore.Raft.WARaftSegmentReader.apply_projection_cache_count(data_dir, 1) == 1

        assert {:ok, %{^shard0_key_a => "value-a", ^shard0_key_b => "value-b"}} =
                 Ferricstore.Raft.WARaftSegmentReader.read_values_from_location(
                   ctx,
                   0,
                   {:waraft_apply_projection, 301},
                   [shard0_key_a, shard0_key_b]
                 )

        assert {:ok, %{^shard0_key_c => "value-c"}} =
                 Ferricstore.Raft.WARaftSegmentReader.read_values_from_location(
                   ctx,
                   0,
                   {:waraft_apply_projection, 302},
                   [shard0_key_c]
                 )
      end

      test "bounded apply projection cache spill keeps one apply index atomic" do
        unique = System.unique_integer([:positive])

        data_dir =
          Path.join(System.tmp_dir!(), "ferricstore_apply_projection_atomic_spill_#{unique}")

        File.mkdir_p!(data_dir)

        ctx = %{data_dir: data_dir}
        shard_index = 0
        projection_index = 501
        key_a = "apply-projection-atomic-a"
        key_b = "apply-projection-atomic-b"
        key_c = "apply-projection-atomic-c"

        on_exit(fn -> File.rm_rf(data_dir) end)

        assert :ok =
                 Ferricstore.Raft.WARaftSegmentReader.put_apply_projection(
                   data_dir,
                   shard_index,
                   projection_index,
                   [{key_a, "value-a", 0}, {key_b, "value-b", 0}, {key_c, "value-c", 0}]
                 )

        assert Ferricstore.Raft.WARaftSegmentReader.apply_projection_cache_count(
                 data_dir,
                 shard_index
               ) == 3

        assert {:ok, 3} =
                 Ferricstore.Raft.WARaftSegmentReader.spill_apply_projection_cache(
                   data_dir,
                   shard_index,
                   2
                 )

        assert {:ok, 0} =
                 Ferricstore.Raft.WARaftSegmentReader.spill_apply_projection_cache(
                   data_dir,
                   shard_index,
                   2
                 )

        assert {:ok, %{^key_a => "value-a", ^key_b => "value-b", ^key_c => "value-c"}} =
                 Ferricstore.Raft.WARaftSegmentReader.read_values_from_location(
                   ctx,
                   shard_index,
                   {:waraft_apply_projection, projection_index},
                   [key_a, key_b, key_c]
                 )
      end

      test "value projection filters absent keydir refs before LMDB live checks" do
        unique = System.unique_integer([:positive])
        keydir = :ets.new(:"history_projector_value_refs_#{unique}", [:set, :public])
        present_ref = "f:{flow-present}:v:p:flow-present:1"
        absent_ref = "f:{flow-absent}:v:p:flow-absent:1"
        stale_ref = "f:{flow-stale}:v:p:flow-stale:1"

        :ets.insert(
          keydir,
          {present_ref, nil, 0, Ferricstore.Store.LFU.initial(), 0, 0, 128}
        )

        :ets.insert(
          keydir,
          {stale_ref, nil, 0, Ferricstore.Store.LFU.initial(), :bad_file, 0, 128}
        )

        assert [^present_ref] =
                 HistoryProjector.__projected_flow_value_keydir_refs_for_test__(
                   keydir,
                   [absent_ref, present_ref, stale_ref]
                 )
      end

      test "value projection never flushes the LMDB writer synchronously" do
        source =
          File.read!(
            Path.expand(
              "../../../lib/ferricstore/flow/history_projector/trim.ex",
              __DIR__
            )
          )

        refute source =~ "LMDBWriter.flush",
               "Flow value projection must not block behind the async LMDB writer; it writes value locators itself before deleting keydir refs"
      end

      test "value projection copies WARaft apply-projection values into the history log" do
        unique = System.unique_integer([:positive])
        instance_name = :"history_projector_direct_projection_#{unique}"

        data_dir =
          Path.join(
            System.tmp_dir!(),
            "ferricstore_history_projector_direct_projection_#{unique}"
          )

        dir = Path.join([data_dir, "data", "shard_0"])
        File.mkdir_p!(dir)

        keydir =
          :ets.new(:"history_projector_direct_projection_keydir_#{unique}", [:set, :public])

        on_exit(fn ->
          File.rm_rf(data_dir)
        end)

        ctx = %{
          name: instance_name,
          data_dir: data_dir,
          keydir_refs: {keydir},
          checkpoint_flags: :atomics.new(1, signed: false),
          disk_pressure: :atomics.new(1, signed: false),
          write_version: :counters.new(1, [:write_concurrency]),
          keydir_binary_bytes: :atomics.new(1, signed: true),
          flow_history_projected_index: :atomics.new(1, signed: false),
          flow_history_projector_flush_failures: :atomics.new(1, signed: false)
        }

        projection_index = 4242
        id = "flow-copy-projection"
        value_key = "f:{flow-copy-projection}:v:p:#{id}:1"
        source_value = Ferricstore.Flow.encode_value("payload-copy")

        assert :ok =
                 Ferricstore.Raft.WARaftSegmentReader.put_apply_projection(
                   data_dir,
                   0,
                   projection_index,
                   [{value_key, source_value, 0}]
                 )

        :ets.insert(
          keydir,
          {value_key, nil, 0, Ferricstore.Store.LFU.initial(),
           {:waraft_apply_projection, projection_index}, 0, byte_size(source_value)}
        )

        record = %{
          id: id,
          type: "audit",
          state: "queued",
          version: 1,
          partition_key: nil,
          priority: 0,
          attempts: 0,
          next_run_at_ms: 1_000,
          created_at_ms: 1_000,
          updated_at_ms: 1_000,
          lease_owner: nil,
          lease_deadline_ms: 0,
          payload_ref: value_key
        }

        entry = %{
          key: Ferricstore.Flow.Keys.stream_entry_key(id, "1000-1", nil),
          expire_at_ms: 0,
          history_key: Ferricstore.Flow.Keys.history_key(id),
          event_id: "1000-1",
          event_ms: 1_000,
          version: 1,
          value: Ferricstore.Flow.encode_history_fields(record, "created", 1_000, %{}),
          ra_index: projection_index
        }

        assert :ok = HistoryProjector.write_entries_sync(ctx, 0, dir, [entry], projection_index)
        assert [] = :ets.lookup(keydir, value_key)

        lmdb_path = Ferricstore.Flow.LMDB.path(dir)
        assert {:ok, lmdb_value} = Ferricstore.Flow.LMDB.get(lmdb_path, value_key)

        assert {:ok, {{:flow_history, file_id}, offset, value_size}} =
                 Ferricstore.Flow.LMDB.decode_value_locator(lmdb_value, 1_000)

        assert file_id == 0
        assert is_integer(offset) and offset >= 0
        assert value_size == byte_size(source_value)

        assert {:ok, ^source_value} =
                 HistoryProjector.read_value(dir, {:flow_history, file_id}, offset)
      end

      test "value projection overwrites stale LMDB locator from newer direct segment keydir row" do
        unique = System.unique_integer([:positive])
        instance_name = :"history_projector_stale_value_locator_#{unique}"

        data_dir =
          Path.join(
            System.tmp_dir!(),
            "ferricstore_history_projector_stale_value_locator_#{unique}"
          )

        dir = Path.join([data_dir, "data", "shard_0"])
        File.mkdir_p!(dir)

        keydir =
          :ets.new(:"history_projector_stale_value_locator_keydir_#{unique}", [:set, :public])

        on_exit(fn ->
          File.rm_rf(data_dir)
        end)

        ctx = %{
          name: instance_name,
          data_dir: data_dir,
          keydir_refs: {keydir},
          checkpoint_flags: :atomics.new(1, signed: false),
          disk_pressure: :atomics.new(1, signed: false),
          write_version: :counters.new(1, [:write_concurrency]),
          keydir_binary_bytes: :atomics.new(1, signed: true),
          flow_history_projected_index: :atomics.new(1, signed: false),
          flow_history_projector_flush_failures: :atomics.new(1, signed: false)
        }

        id = "flow-stale-locator"
        value_key = "f:{flow-stale-locator}:v:p:#{id}:2"
        lmdb_path = Ferricstore.Flow.LMDB.path(dir)

        stale_locator =
          Ferricstore.Flow.LMDB.encode_value_locator(0, {:flow_history, 0}, 11, 22)

        assert :ok =
                 Ferricstore.Flow.LMDB.write_batch(lmdb_path, [{:put, value_key, stale_locator}])

        :ets.insert(
          keydir,
          {value_key, nil, 0, Ferricstore.Store.LFU.initial(), {:waraft_segment, 44}, 123, 456}
        )

        record = %{
          id: id,
          type: "audit",
          state: "running",
          version: 2,
          partition_key: nil,
          priority: 0,
          attempts: 0,
          next_run_at_ms: 1_000,
          created_at_ms: 1_000,
          updated_at_ms: 2_000,
          lease_owner: nil,
          lease_deadline_ms: 0,
          payload_ref: value_key
        }

        entry = %{
          key: Ferricstore.Flow.Keys.stream_entry_key(id, "2000-2", nil),
          expire_at_ms: 0,
          history_key: Ferricstore.Flow.Keys.history_key(id),
          event_id: "2000-2",
          event_ms: 2_000,
          version: 2,
          value: Ferricstore.Flow.encode_history_fields(record, "transitioned", 2_000, %{}),
          ra_index: 44
        }

        assert :ok = HistoryProjector.write_entries_sync(ctx, 0, dir, [entry], 44)
        assert [] = :ets.lookup(keydir, value_key)
        assert {:ok, lmdb_value} = Ferricstore.Flow.LMDB.get(lmdb_path, value_key)

        assert {:ok, {{:waraft_segment, 44}, 123, 456}} =
                 Ferricstore.Flow.LMDB.decode_value_locator(lmdb_value, 2_000)
      end

      test "sync projection directly evicts hot history rows when previous event is known" do
        unique = System.unique_integer([:positive])
        instance_name = :"history_projector_direct_hot_evict_#{unique}"

        dir =
          Path.join(System.tmp_dir!(), "ferricstore_history_projector_direct_hot_evict_#{unique}")

        File.mkdir_p!(dir)

        keydir = :ets.new(:"history_projector_direct_hot_evict_keydir_#{unique}", [:set, :public])

        on_exit(fn ->
          File.rm_rf(dir)
        end)

        ctx = %{
          name: instance_name,
          keydir_refs: {keydir},
          checkpoint_flags: :atomics.new(1, signed: false),
          disk_pressure: :atomics.new(1, signed: false),
          write_version: :counters.new(1, [:write_concurrency]),
          keydir_binary_bytes: :atomics.new(1, signed: true),
          flow_history_projected_index: :atomics.new(1, signed: false),
          flow_history_projector_flush_failures: :atomics.new(1, signed: false)
        }

        history_key = Ferricstore.Flow.Keys.history_key("flow-direct-hot")
        old_event_id = "1000-1"
        old_key = Ferricstore.Flow.Keys.stream_entry_key("flow-direct-hot", old_event_id, nil)
        new_event_id = "1001-2"
        new_key = Ferricstore.Flow.Keys.stream_entry_key("flow-direct-hot", new_event_id, nil)

        :ets.insert(
          keydir,
          {old_key, nil, 0, Ferricstore.Store.LFU.initial(), {:flow_history, 0}, 0, 16}
        )

        entry = %{
          key: new_key,
          expire_at_ms: 0,
          history_key: history_key,
          event_id: new_event_id,
          event_ms: 1_001,
          version: 2,
          value: "history-2",
          history_hot_max_events: 1,
          hot_evict_event_ids: [old_event_id],
          ra_index: 44
        }

        assert :ok = HistoryProjector.write_entries_sync(ctx, 0, dir, [entry], 44)

        assert [] = :ets.lookup(keydir, old_key)

        assert [{^new_key, nil, 0, _lfu, {:flow_history, 0}, _offset, _size}] =
                 :ets.lookup(keydir, new_key)
      end

      test "direct hot-history eviction uses one native batch instead of per-flow deletes" do
        unique = System.unique_integer([:positive])
        instance_name = :"history_projector_direct_hot_batch_#{unique}"

        dir =
          Path.join(System.tmp_dir!(), "ferricstore_history_projector_direct_hot_batch_#{unique}")

        File.mkdir_p!(dir)

        keydir = :ets.new(:"history_projector_direct_hot_batch_keydir_#{unique}", [:set, :public])
        {flow_index, flow_lookup} = OrderedIndex.table_names(instance_name, 0)
        native = NativeOrderedIndex.reset(flow_index, flow_lookup)

        on_exit(fn ->
          File.rm_rf(dir)
        end)

        ctx = %{
          name: instance_name,
          keydir_refs: {keydir},
          checkpoint_flags: :atomics.new(1, signed: false),
          disk_pressure: :atomics.new(1, signed: false),
          write_version: :counters.new(1, [:write_concurrency]),
          keydir_binary_bytes: :atomics.new(1, signed: true),
          flow_history_projected_index: :atomics.new(1, signed: false),
          flow_history_projector_flush_failures: :atomics.new(1, signed: false)
        }

        entries =
          for idx <- 1..32 do
            id = "flow-hot-batch-#{idx}"
            history_key = Ferricstore.Flow.Keys.history_key(id)
            old_event_id = "1000-1"
            old_key = Ferricstore.Flow.Keys.stream_entry_key(id, old_event_id, nil)
            new_event_id = "1001-2"

            :ets.insert(
              keydir,
              {old_key, nil, 0, Ferricstore.Store.LFU.initial(), {:flow_history, 0}, idx, 16}
            )

            :ok = NativeOrderedIndex.put_entries(native, [{history_key, old_event_id, 1_000}])

            %{
              key: Ferricstore.Flow.Keys.stream_entry_key(id, new_event_id, nil),
              expire_at_ms: 0,
              history_key: history_key,
              event_id: new_event_id,
              event_ms: 1_001,
              version: 2,
              value: "history-#{idx}",
              history_hot_max_events: 1,
              hot_evict_event_ids: [old_event_id],
              ra_index: 100 + idx
            }
          end

        assert :ok = HistoryProjector.write_entries_sync(ctx, 0, dir, entries, 132)

        source =
          File.read!(
            Path.expand(
              "../../../lib/ferricstore/flow/history_projector/trim.ex",
              __DIR__
            )
          )

        assert source =~ "NativeFlowIndex.delete_entries(native, history_delete_entries(items))"
        refute source =~ "NativeFlowIndex.delete_members(native"
        assert :ets.info(keydir, :size) == 32
      end

      test "zero hot-history cap is planned as direct eviction without rank scans" do
        entries =
          for idx <- 1..3 do
            id = "flow-zero-direct-#{idx}"
            event_id = "100#{idx}-#{idx}"

            %{
              key: Ferricstore.Flow.Keys.stream_entry_key(id, event_id, nil),
              expire_at_ms: 0,
              history_key: Ferricstore.Flow.Keys.history_key(id),
              event_id: event_id,
              event_ms: 1_000 + idx,
              version: idx,
              value: "history-#{idx}",
              history_hot_max_events: 0,
              ra_index: idx
            }
          end

        assert HistoryProjector.__direct_hot_history_evict_items_for_test__(entries) ==
                 Enum.map(entries, fn entry ->
                   {entry.history_key, entry.event_id, entry.key}
                 end)

        assert HistoryProjector.__history_hot_rank_entries_for_test__(entries) == []
      end

      test "terminal hot-history trim keeps rank scan path to evict retained older events" do
        entry = %{
          key: Ferricstore.Flow.Keys.stream_entry_key("flow-terminal-direct", "1009-9", nil),
          expire_at_ms: 0,
          history_key: Ferricstore.Flow.Keys.history_key("flow-terminal-direct"),
          event_id: "1009-9",
          event_ms: 1_009,
          version: 9,
          value: "terminal",
          history_hot_max_events: 3,
          terminal?: true,
          ra_index: 9
        }

        assert HistoryProjector.__history_hot_rank_entries_for_test__([entry]) == [entry]
      end

      test "history index planning partitions new and update entries in order" do
        entries = [
          %{history_key: "h:1", event_id: "1000-1", event_ms: 1_000, version: 1},
          %{history_key: "h:1", event_id: "1001-2", event_ms: 1_001, version: 2},
          %{history_key: "h:2", event_id: "1002-1", event_ms: 1_002, version: 1},
          %{history_key: "h:2", event_id: "1003-4", event_ms: 1_003, version: 4}
        ]

        assert HistoryProjector.__history_index_entries_for_test__(entries) ==
                 {
                   [{"h:1", "1000-1", 1_000}, {"h:2", "1002-1", 1_002}],
                   [{"h:1", "1001-2", 1_001}, {"h:2", "1003-4", 1_003}]
                 }
      end

      test "history cap planning is one pass for unique under-cap entries" do
        entries =
          for idx <- 1..1_000 do
            %{
              history_key: "history:#{idx}",
              event_id: "1000-2",
              event_ms: 1_000,
              version: 2,
              history_max_events: 100_000,
              key: "history:#{idx}:1000-2",
              value: "history"
            }
          end

        requirements =
          HistoryProjector.__trim_cap_requirements_for_test__(entries, fn history_key ->
            flunk("unexpected cap load for #{inspect(history_key)}")
          end)

        assert map_size(requirements) == 1_000
        assert Enum.all?(requirements, fn {_history_key, {100_000, false}} -> true end)
      end

      test "history cap planning loads missing caps once and marks over-cap keys" do
        parent = self()

        entries = [
          %{history_key: "history:one", event_id: "1000-3", event_ms: 1_000, version: 3},
          %{history_key: "history:one", event_id: "1001-5", event_ms: 1_001, version: 5},
          %{history_key: "history:two", event_id: "1002-1", event_ms: 1_002, version: 1}
        ]

        requirements =
          HistoryProjector.__trim_cap_requirements_for_test__(entries, fn
            "history:one" ->
              send(parent, {:loaded_cap, "history:one"})
              4

            "history:two" ->
              send(parent, {:loaded_cap, "history:two"})
              10
          end)

        assert requirements == %{
                 "history:one" => {4, true},
                 "history:two" => {10, false}
               }

        assert_receive {:loaded_cap, "history:one"}
        assert_receive {:loaded_cap, "history:two"}
        refute_receive {:loaded_cap, _}
      end

      test "recover returns an error and emits telemetry when history path is invalid" do
        unique = System.unique_integer([:positive])
        dir = Path.join(System.tmp_dir!(), "ferricstore_history_projector_error_#{unique}")
        File.mkdir_p!(dir)
        File.write!(Path.join(dir, "history"), "not-a-directory")

        handler_id = {:history_projector_recover_error, self(), make_ref()}

        :telemetry.attach(
          handler_id,
          [:ferricstore, :flow, :history_projector, :recover],
          &__MODULE__.handle_recover_telemetry/4,
          self()
        )

        on_exit(fn ->
          :telemetry.detach(handler_id)
          File.rm_rf(dir)
        end)

        assert {:error, _reason} = HistoryProjector.recover(nil, 0, dir)

        assert_receive {:history_projector_recover_error,
                        [:ferricstore, :flow, :history_projector, :recover], %{errors: 1},
                        %{shard_index: 0, reason: _reason}}
      end
    end
  end
end
