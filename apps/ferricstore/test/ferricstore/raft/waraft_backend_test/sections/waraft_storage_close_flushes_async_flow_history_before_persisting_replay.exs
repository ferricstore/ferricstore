defmodule Ferricstore.Raft.WARaftBackendTest.Sections.WaraftStorageCloseFlushesAsyncFlowHistoryBeforePersistingReplay do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      import ExUnit.CaptureLog

      alias Ferricstore.ErrorReasons
      alias Ferricstore.Raft.Cluster, as: RaftCluster
      alias Ferricstore.Raft.WARaftBackend
      alias Ferricstore.Raft.WARaftStorage
      alias Ferricstore.Store.{BlobRef, BlobStore}
      alias Ferricstore.Store.CompoundKey
      alias Ferricstore.Store.Router
      alias Ferricstore.Raft.WARaftBackendTest.LabelCounter
      alias Ferricstore.Raft.WARaftBackendTest.OversizedLabel

      test "WARaft storage close flushes async Flow history before persisting replay cursor", %{
        root: root,
        ctx: ctx
      } do
        previous_async_history = Application.get_env(:ferricstore, :flow_async_history)

        previous_history_flush =
          Application.get_env(:ferricstore, :flow_history_projector_flush_interval_ms)

        previous_history_batch =
          Application.get_env(:ferricstore, :flow_history_projector_batch_size)

        previous_every = Application.get_env(:ferricstore, :waraft_storage_metadata_persist_every)
        flow_type = "router-flow-history-close-#{System.unique_integer([:positive])}"
        flow_id = "router-flow-history-close-id-#{System.unique_integer([:positive])}"
        partition = "tenant-history-close-#{System.unique_integer([:positive])}"
        shard_data_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0)

        try do
          Application.put_env(:ferricstore, :flow_async_history, true)
          Application.put_env(:ferricstore, :flow_history_projector_flush_interval_ms, 60_000)
          Application.put_env(:ferricstore, :flow_history_projector_batch_size, 10_000)
          Application.put_env(:ferricstore, :waraft_storage_metadata_persist_every, :never)

          assert :ok =
                   WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          start_supervised!(
            {Ferricstore.Flow.HistoryProjector,
             [
               shard_index: 0,
               shard_data_path: shard_data_path,
               instance_ctx: ctx,
               recover_on_init: false
             ]}
          )

          assert :ok =
                   Ferricstore.Flow.create(ctx, flow_id,
                     type: flow_type,
                     partition_key: partition,
                     run_at_ms: 1_000,
                     now_ms: 900
                   )

          assert {:ok, {:raft_log_pos, applied_index, _term}} = WARaftBackend.storage_position(0)
          assert Ferricstore.Flow.HistoryProjectedIndex.read(shard_data_path) < applied_index

          assert :ok = WARaftBackend.stop()

          assert Ferricstore.Flow.HistoryProjectedIndex.read(shard_data_path) >= applied_index

          assert %{position: {:raft_log_pos, persisted_index, _persisted_term}} =
                   waraft_latest_storage_metadata(root, 0)

          assert persisted_index >= applied_index
        after
          restore_env(:flow_async_history, previous_async_history)
          restore_env(:flow_history_projector_flush_interval_ms, previous_history_flush)
          restore_env(:flow_history_projector_batch_size, previous_history_batch)
          restore_env(:waraft_storage_metadata_persist_every, previous_every)
        end
      end

      test "Flow WARaft apply projection is cache-only and rebuilds from WAL on restart", %{
        root: root,
        ctx: ctx
      } do
        flow_type = "router-flow-cache-projection-#{System.unique_integer([:positive])}"
        flow_id = "router-flow-cache-projection-id-#{System.unique_integer([:positive])}"
        partition = "tenant-cache-projection-#{System.unique_integer([:positive])}"
        handler_id = {__MODULE__, :flow_apply_projection_cache_only, make_ref()}

        :telemetry.attach(
          handler_id,
          [:ferricstore, :waraft, :segment_log, :append],
          &__MODULE__.handle_segment_log_telemetry/4,
          self()
        )

        on_exit(fn -> :telemetry.detach(handler_id) end)

        try do
          assert :ok =
                   WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          assert :ok =
                   Ferricstore.Flow.create(ctx, flow_id,
                     type: flow_type,
                     partition_key: partition,
                     run_at_ms: 1_000,
                     now_ms: 900
                   )

          assert {:ok, %{id: ^flow_id, state: "queued"}} =
                   Ferricstore.Flow.get(ctx, flow_id, partition_key: partition)

          refute_receive {:waraft_segment_log_telemetry,
                          [:ferricstore, :waraft, :segment_log, :append], _measurements,
                          %{kind: :apply_projection}},
                         250

          apply_projection_root =
            Path.join([
              root,
              "waraft",
              "ferricstore_waraft_backend.1",
              "apply_projection_log"
            ])

          assert [] == Path.wildcard(Path.join([apply_projection_root, "segment_log", "*.seg"]))

          assert :ok = WARaftBackend.stop()
          FerricStore.Instance.cleanup(ctx.name)

          restarted_ctx = build_ctx(root)

          assert :ok =
                   WARaftBackend.start(restarted_ctx,
                     log_module: :ferricstore_waraft_spike_segment_log
                   )

          assert {:ok, %{id: ^flow_id, state: "queued"}} =
                   Ferricstore.Flow.get(restarted_ctx, flow_id, partition_key: partition)
        after
        end
      end

      @tag :apply_projection_cache_compaction
      test "Flow WARaft apply projection cache compacts when the row budget is exceeded", %{
        root: root,
        ctx: ctx
      } do
        previous_limit =
          Application.get_env(:ferricstore, :waraft_apply_projection_cache_max_entries)

        flow_type = "router-flow-cache-compact-#{System.unique_integer([:positive])}"
        flow_id = "router-flow-cache-compact-id-#{System.unique_integer([:positive])}"
        partition = "tenant-cache-compact-#{System.unique_integer([:positive])}"

        try do
          Application.put_env(:ferricstore, :waraft_apply_projection_cache_max_entries, 0)
          clear_apply_projection_cache!()

          assert :ok =
                   WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          assert :ok =
                   Ferricstore.Flow.create(ctx, flow_id,
                     type: flow_type,
                     partition_key: partition,
                     run_at_ms: 1_000,
                     now_ms: 900
                   )

          assert {:ok, %{id: ^flow_id, state: "queued"}} =
                   Ferricstore.Flow.get(ctx, flow_id, partition_key: partition)

          assert eventually(fn -> apply_projection_cache_rows(root, 0) == 0 end)
        after
          restore_env(:waraft_apply_projection_cache_max_entries, previous_limit)
        end
      end

      @tag :apply_projection_cache_compaction
      @tag :apply_projection_byte_budget
      test "WARaft apply projection cache compacts oversized values by byte budget", %{
        root: root,
        ctx: ctx
      } do
        previous_entry_limit =
          Application.get_env(:ferricstore, :waraft_apply_projection_cache_max_entries)

        previous_byte_limit =
          Application.get_env(:ferricstore, :waraft_apply_projection_cache_max_bytes)

        key = "router:list-byte-compact-#{System.unique_integer([:positive])}"
        large_value = :binary.copy("large-inline-value-", 6_000)

        try do
          Application.put_env(
            :ferricstore,
            :waraft_apply_projection_cache_max_entries,
            :infinity
          )

          Application.put_env(:ferricstore, :waraft_apply_projection_cache_max_bytes, 1_024)
          clear_apply_projection_cache!()

          assert :ok =
                   WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          assert 2 =
                   Ferricstore.Commands.List.handle_ast(
                     {:rpush, [key, large_value, "tail"]},
                     ctx
                   )

          assert eventually(fn ->
                   Ferricstore.Raft.WARaftSegmentReader.apply_projection_cache_bytes(root, 0) <=
                     512
                 end)

          assert [large_value, "tail"] ==
                   Ferricstore.Commands.List.handle_ast({:lrange, key, 0, -1}, ctx)

          list_prefix = CompoundKey.list_prefix(key)

          projected_rows =
            ctx.keydir_refs
            |> elem(0)
            |> :ets.tab2list()
            |> Enum.filter(fn
              {compound_key, _value, _expire_at_ms, _lfu, {:waraft_apply_projection, _index},
               _offset, _value_size} ->
                String.starts_with?(compound_key, list_prefix)

              _row ->
                false
            end)

          assert length(projected_rows) == 2

          Enum.each(projected_rows, fn
            {compound_key, _cached, _expire_at_ms, _lfu, file_id, _offset, _value_size} ->
              assert {:ok, value} =
                       Ferricstore.Raft.WARaftSegmentReader.read_value_from_location(
                         ctx,
                         0,
                         file_id,
                         compound_key
                       )

              assert is_binary(value)
          end)
        after
          restore_env(:waraft_apply_projection_cache_max_entries, previous_entry_limit)
          restore_env(:waraft_apply_projection_cache_max_bytes, previous_byte_limit)
        end
      end

      @tag :apply_projection_cache_compaction
      test "WARaft apply projection cache drains writes that arrive during an active spill", %{
        root: root,
        ctx: ctx
      } do
        previous_entry_limit =
          Application.get_env(:ferricstore, :waraft_apply_projection_cache_max_entries)

        previous_byte_limit =
          Application.get_env(:ferricstore, :waraft_apply_projection_cache_max_bytes)

        previous_spill_hook =
          Application.get_env(:ferricstore, :waraft_apply_projection_spill_hook)

        spill_calls = :atomics.new(1, signed: false)
        parent = self()
        first_key = "router:list-byte-spill-first-#{System.unique_integer([:positive])}"
        second_key = "router:list-byte-spill-second-#{System.unique_integer([:positive])}"
        first_value = :binary.copy("first-inline-value-", 6_000)
        second_value = :binary.copy("second-inline-value-", 6_000)

        spill_hook = fn _batches ->
          case :atomics.add_get(spill_calls, 1, 1) do
            1 ->
              send(parent, {:first_apply_projection_spill_selected, self()})

              receive do
                :release_first_apply_projection_spill -> :ok
              after
                5_000 -> :ok
              end

            _later_spill ->
              :ok
          end
        end

        try do
          Application.put_env(
            :ferricstore,
            :waraft_apply_projection_cache_max_entries,
            :infinity
          )

          Application.put_env(:ferricstore, :waraft_apply_projection_cache_max_bytes, 1_024)
          Application.put_env(:ferricstore, :waraft_apply_projection_spill_hook, spill_hook)
          clear_apply_projection_cache!()

          assert :ok =
                   WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          assert 1 =
                   Ferricstore.Commands.List.handle_ast(
                     {:rpush, [first_key, first_value]},
                     ctx
                   )

          assert_receive {:first_apply_projection_spill_selected, first_spill_pid}, 1_000

          assert 1 =
                   Ferricstore.Commands.List.handle_ast(
                     {:rpush, [second_key, second_value]},
                     ctx
                   )

          assert Ferricstore.Raft.WARaftSegmentReader.apply_projection_cache_bytes(root, 0) >
                   1_024

          send(first_spill_pid, :release_first_apply_projection_spill)

          assert eventually(fn ->
                   Ferricstore.Raft.WARaftSegmentReader.apply_projection_cache_bytes(root, 0) <=
                     512
                 end)

          assert :atomics.get(spill_calls, 1) >= 2

          assert [first_value] ==
                   Ferricstore.Commands.List.handle_ast({:lrange, first_key, 0, -1}, ctx)

          assert [second_value] ==
                   Ferricstore.Commands.List.handle_ast({:lrange, second_key, 0, -1}, ctx)
        after
          restore_env(:waraft_apply_projection_cache_max_entries, previous_entry_limit)
          restore_env(:waraft_apply_projection_cache_max_bytes, previous_byte_limit)
          restore_env(:waraft_apply_projection_spill_hook, previous_spill_hook)
        end
      end

      @tag :apply_projection_cache_compaction
      test "WARaft apply projection cache retries after an unlinked compactor dies", %{
        root: root,
        ctx: ctx
      } do
        previous_entry_limit =
          Application.get_env(:ferricstore, :waraft_apply_projection_cache_max_entries)

        previous_byte_limit =
          Application.get_env(:ferricstore, :waraft_apply_projection_cache_max_bytes)

        previous_compact_hook =
          Application.get_env(:ferricstore, :waraft_apply_projection_cache_compact_hook)

        compact_calls = :atomics.new(1, signed: false)
        parent = self()
        first_key = "router:list-dead-compactor-first-#{System.unique_integer([:positive])}"
        second_key = "router:list-dead-compactor-second-#{System.unique_integer([:positive])}"

        compact_hook = fn
          :before_spill, _metadata ->
            case :atomics.add_get(compact_calls, 1, 1) do
              1 ->
                send(parent, {:apply_projection_compactor_will_die, self()})
                Process.exit(self(), :kill)

              _retry ->
                :ok
            end

          _phase, _metadata ->
            :ok
        end

        try do
          Application.put_env(:ferricstore, :waraft_apply_projection_cache_max_entries, 0)

          Application.put_env(
            :ferricstore,
            :waraft_apply_projection_cache_max_bytes,
            :infinity
          )

          Application.put_env(
            :ferricstore,
            :waraft_apply_projection_cache_compact_hook,
            compact_hook
          )

          clear_apply_projection_cache!()

          assert :ok =
                   WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          assert 1 =
                   Ferricstore.Commands.List.handle_ast({:rpush, [first_key, "first"]}, ctx)

          assert_receive {:apply_projection_compactor_will_die, _compactor_pid}, 1_000

          assert eventually(fn ->
                   case WARaftBackend.storage_status(0) do
                     {:ok, status} ->
                       Keyword.get(status, :apply_projection_cache_compaction_pending?) == false

                     _other ->
                       false
                   end
                 end)

          assert 1 =
                   Ferricstore.Commands.List.handle_ast({:rpush, [second_key, "second"]}, ctx)

          assert eventually(fn -> apply_projection_cache_rows(root, 0) == 0 end)
          assert :atomics.get(compact_calls, 1) >= 2

          assert ["first"] ==
                   Ferricstore.Commands.List.handle_ast({:lrange, first_key, 0, -1}, ctx)

          assert ["second"] ==
                   Ferricstore.Commands.List.handle_ast({:lrange, second_key, 0, -1}, ctx)
        after
          restore_env(:waraft_apply_projection_cache_max_entries, previous_entry_limit)
          restore_env(:waraft_apply_projection_cache_max_bytes, previous_byte_limit)
          restore_env(:waraft_apply_projection_cache_compact_hook, previous_compact_hook)
        end
      end

      test "Flow segment-keydir writes do not enter apply-projection cache compaction", %{
        root: root,
        ctx: ctx
      } do
        previous_limit =
          Application.get_env(:ferricstore, :waraft_apply_projection_cache_max_entries)

        previous_hook =
          Application.get_env(:ferricstore, :waraft_apply_projection_cache_compact_hook)

        flow_type = "router-flow-cache-async-compact-#{System.unique_integer([:positive])}"
        flow_id = "router-flow-cache-async-compact-id-#{System.unique_integer([:positive])}"
        partition = "tenant-cache-async-compact-#{System.unique_integer([:positive])}"
        parent = self()

        hook = fn
          :before_spill, metadata ->
            send(parent, {:apply_projection_cache_before_spill, self(), metadata})

            receive do
              :release_apply_projection_cache_compaction -> :ok
            after
              5_000 -> :ok
            end

          _phase, _metadata ->
            :ok
        end

        try do
          Application.put_env(:ferricstore, :waraft_apply_projection_cache_max_entries, 0)
          Application.put_env(:ferricstore, :waraft_apply_projection_cache_compact_hook, hook)
          clear_apply_projection_cache!()

          assert :ok =
                   WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          assert :ok =
                   Ferricstore.Flow.create(ctx, flow_id,
                     type: flow_type,
                     partition_key: partition,
                     payload: :binary.copy("payload-", 128),
                     run_at_ms: 1_000,
                     now_ms: 900
                   )

          receive do
            {:apply_projection_cache_before_spill, compactor_pid, _metadata} ->
              send(compactor_pid, :release_apply_projection_cache_compaction)
          after
            250 -> :ok
          end

          assert eventually(fn -> apply_projection_cache_rows(root, 0) == 0 end)

          assert {:ok, %{id: ^flow_id, state: "queued"}} =
                   Ferricstore.Flow.get(ctx, flow_id, partition_key: partition)
        after
          restore_env(:waraft_apply_projection_cache_max_entries, previous_limit)
          restore_env(:waraft_apply_projection_cache_compact_hook, previous_hook)
        end
      end

      test "hot metadata dependency request does not synchronously spill Flow apply projection cache",
           %{
             ctx: ctx
           } do
        previous_every = Application.get_env(:ferricstore, :waraft_storage_metadata_persist_every)

        previous_limit =
          Application.get_env(:ferricstore, :waraft_apply_projection_cache_max_entries)

        previous_hook = Application.get_env(:ferricstore, :waraft_apply_projection_spill_hook)
        flow_type = "router-flow-cache-metadata-spill-#{System.unique_integer([:positive])}"
        flow_id = "router-flow-cache-metadata-spill-id-#{System.unique_integer([:positive])}"
        partition = "tenant-cache-metadata-spill-#{System.unique_integer([:positive])}"
        parent = self()

        hook = fn batches ->
          send(parent, {:apply_projection_metadata_spill, self(), batches})

          receive do
            :release_apply_projection_metadata_spill -> :ok
          after
            5_000 -> :ok
          end
        end

        try do
          Application.put_env(:ferricstore, :waraft_storage_metadata_persist_every, 1)
          Application.put_env(:ferricstore, :waraft_apply_projection_cache_max_entries, :infinity)
          Application.put_env(:ferricstore, :waraft_apply_projection_spill_hook, hook)
          clear_apply_projection_cache!()

          assert :ok =
                   WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          task =
            Task.async(fn ->
              Ferricstore.Flow.create(ctx, flow_id,
                type: flow_type,
                partition_key: partition,
                payload: :binary.copy("payload-", 128),
                run_at_ms: 1_000,
                now_ms: 900
              )
            end)

          assert {:ok, :ok} = Task.yield(task, 500)

          receive do
            {:apply_projection_metadata_spill, spill_pid, _batches} ->
              send(spill_pid, :release_apply_projection_metadata_spill)
          after
            100 ->
              :ok
          end
        after
          restore_env(:waraft_storage_metadata_persist_every, previous_every)
          restore_env(:waraft_apply_projection_cache_max_entries, previous_limit)
          restore_env(:waraft_apply_projection_spill_hook, previous_hook)
        end
      end

      test "snapshot does not wait on apply-projection compaction for segment-keydir Flow writes",
           %{
             root: root,
             ctx: ctx
           } do
        previous_limit =
          Application.get_env(:ferricstore, :waraft_apply_projection_cache_max_entries)

        previous_hook =
          Application.get_env(:ferricstore, :waraft_apply_projection_cache_compact_hook)

        flow_type = "router-flow-cache-snapshot-compact-#{System.unique_integer([:positive])}"
        flow_id = "router-flow-cache-snapshot-compact-id-#{System.unique_integer([:positive])}"
        partition = "tenant-cache-snapshot-compact-#{System.unique_integer([:positive])}"
        parent = self()

        hook = fn
          :before_spill, metadata ->
            send(parent, {:apply_projection_cache_snapshot_before_spill, self(), metadata})

            receive do
              :release_apply_projection_cache_snapshot_compaction -> :ok
            after
              5_000 -> :ok
            end

          _phase, _metadata ->
            :ok
        end

        try do
          Application.put_env(:ferricstore, :waraft_apply_projection_cache_max_entries, 0)
          Application.put_env(:ferricstore, :waraft_apply_projection_cache_compact_hook, hook)
          clear_apply_projection_cache!()

          assert :ok =
                   WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          assert :ok =
                   Ferricstore.Flow.create(ctx, flow_id,
                     type: flow_type,
                     partition_key: partition,
                     payload: :binary.copy("payload-", 128),
                     run_at_ms: 1_000,
                     now_ms: 900
                   )

          receive do
            {:apply_projection_cache_snapshot_before_spill, compactor_pid, _metadata} ->
              send(compactor_pid, :release_apply_projection_cache_snapshot_compaction)
          after
            250 -> :ok
          end

          assert eventually(fn -> apply_projection_cache_rows(root, 0) == 0 end)

          snapshot_task = Task.async(fn -> WARaftBackend.create_snapshot(0) end)
          assert {:ok, {:raft_log_pos, _index, _term}} = Task.await(snapshot_task, 5_000)
          assert eventually(fn -> apply_projection_cache_rows(root, 0) == 0 end)
        after
          restore_env(:waraft_apply_projection_cache_max_entries, previous_limit)
          restore_env(:waraft_apply_projection_cache_compact_hook, previous_hook)
        end
      end

      test "restart recovers Flow value from segment-keydir while LMDB projection is delayed", %{
        root: root,
        ctx: ctx
      } do
        previous_async_history = Application.get_env(:ferricstore, :flow_async_history)

        previous_history_flush =
          Application.get_env(:ferricstore, :flow_history_projector_flush_interval_ms)

        previous_limit =
          Application.get_env(:ferricstore, :waraft_apply_projection_cache_max_entries)

        flow_type = "router-flow-cache-restart-#{System.unique_integer([:positive])}"
        flow_id = "router-flow-cache-restart-id-#{System.unique_integer([:positive])}"
        partition = "tenant-cache-restart-#{System.unique_integer([:positive])}"

        try do
          Application.put_env(:ferricstore, :flow_async_history, true)
          Application.put_env(:ferricstore, :flow_history_projector_flush_interval_ms, 60_000)
          Application.put_env(:ferricstore, :waraft_apply_projection_cache_max_entries, 0)
          clear_apply_projection_cache!()

          assert :ok =
                   WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          assert :ok =
                   Ferricstore.Flow.create(ctx, flow_id,
                     type: flow_type,
                     partition_key: partition,
                     payload: :binary.copy("payload-", 128),
                     run_at_ms: 1_000,
                     now_ms: 900
                   )

          assert {:ok, %{id: ^flow_id, state: "queued"}} =
                   Ferricstore.Flow.get(ctx, flow_id, partition_key: partition)

          assert :ok = WARaftBackend.stop()
          FerricStore.Instance.cleanup(ctx.name)
          clear_apply_projection_cache!()

          restarted_ctx = build_ctx(root)

          try do
            assert :ok =
                     WARaftBackend.start(restarted_ctx,
                       log_module: :ferricstore_waraft_spike_segment_log
                     )

            assert {:ok, %{id: ^flow_id, state: "queued"}} =
                     Ferricstore.Flow.get(restarted_ctx, flow_id, partition_key: partition)
          after
            FerricStore.Instance.cleanup(restarted_ctx.name)
          end
        after
          restore_env(:flow_async_history, previous_async_history)
          restore_env(:flow_history_projector_flush_interval_ms, previous_history_flush)
          restore_env(:waraft_apply_projection_cache_max_entries, previous_limit)
        end
      end

      test "restart recovers Flow payload from segment-keydir with async history delayed",
           %{
             root: root,
             ctx: ctx
           } do
        previous_async_history = Application.get_env(:ferricstore, :flow_async_history)

        previous_history_flush =
          Application.get_env(:ferricstore, :flow_history_projector_flush_interval_ms)

        previous_limit =
          Application.get_env(:ferricstore, :waraft_apply_projection_cache_max_entries)

        flow_type = "router-flow-cache-memory-restart-#{System.unique_integer([:positive])}"
        flow_id = "router-flow-cache-memory-restart-id-#{System.unique_integer([:positive])}"
        partition = "tenant-cache-memory-restart-#{System.unique_integer([:positive])}"
        payload = :binary.copy("memory-only-payload-", 64)

        try do
          Application.put_env(:ferricstore, :flow_async_history, true)
          Application.put_env(:ferricstore, :flow_history_projector_flush_interval_ms, 60_000)
          Application.put_env(:ferricstore, :waraft_apply_projection_cache_max_entries, 1_000_000)
          clear_apply_projection_cache!()

          assert :ok =
                   WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          assert :ok =
                   Ferricstore.Flow.create(ctx, flow_id,
                     type: flow_type,
                     partition_key: partition,
                     payload: payload,
                     run_at_ms: 1_000,
                     now_ms: 900
                   )

          assert {:ok, %{id: ^flow_id, state: "queued", payload: ^payload}} =
                   Ferricstore.Flow.get(ctx, flow_id, partition_key: partition, payload: true)

          assert :ok = WARaftBackend.stop()
          FerricStore.Instance.cleanup(ctx.name)
          clear_apply_projection_cache!()

          restarted_ctx = build_ctx(root)

          try do
            assert :ok =
                     WARaftBackend.start(restarted_ctx,
                       log_module: :ferricstore_waraft_spike_segment_log
                     )

            assert {:ok, %{id: ^flow_id, state: "queued", payload: ^payload}} =
                     Ferricstore.Flow.get(restarted_ctx, flow_id,
                       partition_key: partition,
                       payload: true
                     )
          after
            FerricStore.Instance.cleanup(restarted_ctx.name)
          end
        after
          restore_env(:flow_async_history, previous_async_history)
          restore_env(:flow_history_projector_flush_interval_ms, previous_history_flush)
          restore_env(:waraft_apply_projection_cache_max_entries, previous_limit)
        end
      end

      test "failed async Flow history projection does not advance WARaft durable position", %{
        ctx: ctx
      } do
        previous_async_history = Application.get_env(:ferricstore, :flow_async_history)

        previous_publish_hook =
          Application.get_env(:ferricstore, :flow_history_projector_lmdb_publish_hook)

        previous_history_flush =
          Application.get_env(:ferricstore, :flow_history_projector_flush_interval_ms)

        previous_history_batch =
          Application.get_env(:ferricstore, :flow_history_projector_batch_size)

        flow_type = "router-flow-history-fail-#{System.unique_integer([:positive])}"
        flow_id = "router-flow-history-fail-id-#{System.unique_integer([:positive])}"
        partition = "tenant-history-fail-#{System.unique_integer([:positive])}"
        shard_data_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0)

        try do
          Application.put_env(:ferricstore, :flow_async_history, true)
          Application.put_env(:ferricstore, :flow_history_projector_flush_interval_ms, 60_000)
          Application.put_env(:ferricstore, :flow_history_projector_batch_size, 10_000)

          Application.put_env(:ferricstore, :flow_history_projector_lmdb_publish_hook, fn
            _shard_data_path, _file_id, _entries -> {:error, :forced_history_publish_failure}
          end)

          assert :ok =
                   WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          start_supervised!(
            {Ferricstore.Flow.HistoryProjector,
             [
               shard_index: 0,
               shard_data_path: shard_data_path,
               instance_ctx: ctx,
               recover_on_init: false
             ]}
          )

          assert {:ok, pre_position} = WARaftBackend.storage_position(0)
          assert Keyword.fetch!(waraft_storage_status(0), :durable_position) == pre_position

          assert :ok =
                   Ferricstore.Flow.create(ctx, flow_id,
                     type: flow_type,
                     partition_key: partition,
                     payload: "payload",
                     run_at_ms: 1_000,
                     now_ms: 900
                   )

          assert {:ok, {:raft_log_pos, applied_index, _term}} = WARaftBackend.storage_position(0)
          assert applied_index > position_index(pre_position)
          assert Keyword.fetch!(waraft_storage_status(0), :durable_position) == pre_position
        after
          restore_env(:flow_async_history, previous_async_history)
          restore_env(:flow_history_projector_lmdb_publish_hook, previous_publish_hook)
          restore_env(:flow_history_projector_flush_interval_ms, previous_history_flush)
          restore_env(:flow_history_projector_batch_size, previous_history_batch)
        end
      end

      test "flow due indexes survive WARaft restart without shard process reads", %{
        root: root,
        ctx: ctx
      } do
        flow_type = "router-flow-restart-#{System.unique_integer([:positive])}"
        flow_id = "router-flow-restart-id-#{System.unique_integer([:positive])}"
        running_id = "router-flow-running-restart-id-#{System.unique_integer([:positive])}"
        partition = "tenant-restart-#{System.unique_integer([:positive])}"

        try do
          assert :ok =
                   WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          start_supervised!(
            {Ferricstore.Flow.LMDBWriter,
             shard_index: 0, data_dir: ctx.data_dir, instance_ctx: ctx}
          )

          assert :ok =
                   Ferricstore.Flow.create(ctx, flow_id,
                     type: flow_type,
                     partition_key: partition,
                     run_at_ms: 1_000,
                     now_ms: 900
                   )

          assert :ok =
                   Ferricstore.Flow.create(ctx, running_id,
                     type: flow_type,
                     partition_key: partition,
                     run_at_ms: 800,
                     now_ms: 700
                   )

          assert {:ok, [%{id: ^running_id}]} =
                   Ferricstore.Flow.claim_due(ctx, flow_type,
                     partition_key: partition,
                     worker: "worker-before-restart",
                     lease_ms: 50,
                     limit: 1,
                     now_ms: 800
                   )

          assert :ok = WARaftBackend.stop()
          FerricStore.Instance.cleanup(ctx.name)

          restarted_ctx = build_ctx(root)

          assert :ok =
                   WARaftBackend.start(restarted_ctx,
                     log_module: :ferricstore_waraft_spike_segment_log
                   )

          assert {:ok, recovered} =
                   Ferricstore.Flow.get(restarted_ctx, flow_id, partition_key: partition)

          assert recovered.id == flow_id

          assert {:ok, info} =
                   Ferricstore.Flow.info(restarted_ctx, flow_type, partition_key: partition)

          assert info.queued == 1
          assert info.running == 1
          assert info.inflight == 1

          assert {:ok, [stuck]} =
                   Ferricstore.Flow.stuck(restarted_ctx, flow_type,
                     partition_key: partition,
                     older_than_ms: 0,
                     count: 10,
                     now_ms: 900
                   )

          assert stuck.id == running_id

          assert {:ok, [listed]} =
                   Ferricstore.Flow.list(restarted_ctx, flow_type,
                     state: "queued",
                     partition_key: partition,
                     count: 10
                   )

          assert listed.id == flow_id

          assert {:ok, [claim]} =
                   Ferricstore.Flow.claim_due(restarted_ctx, flow_type,
                     partition_key: partition,
                     worker: "worker-restart",
                     limit: 1,
                     now_ms: 1_000,
                     reclaim_expired: false
                   )

          assert claim.id == flow_id
        after
        end
      end

      test "Flow rejects dependencies across independent WARaft groups", %{root: root} do
        ctx = build_ctx(Path.join(root, "flow-cross-group-rejection"), shard_count: 2)
        parent_id = "router-flow-parent-#{System.unique_integer([:positive])}"
        child_id = "router-flow-child-#{System.unique_integer([:positive])}"
        parent_partition = flow_partition_for_shard(ctx, parent_id, 0)
        child_partition = flow_partition_for_shard(ctx, child_id, 1)

        try do
          assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          assert :ok =
                   Ferricstore.Flow.create(ctx, parent_id,
                     type: "parent",
                     state: "dispatch",
                     partition_key: parent_partition,
                     now_ms: 1_000
                   )

          assert {:ok, created_parent} =
                   Ferricstore.Flow.get(ctx, parent_id, partition_key: parent_partition)

          assert {:error, "CROSSSLOT Flow dependency keys must hash to the same shard"} =
                   Ferricstore.Flow.spawn_children(
                     ctx,
                     parent_id,
                     [%{id: child_id, type: "child", partition_key: child_partition}],
                     group_id: "fanout",
                     wait: :all,
                     wait_state: "waiting_children",
                     on_child_failed: :ignore,
                     on_parent_closed: :abandon_children,
                     exhaust_to: %{success: "children_done", failure: "children_failed"},
                     partition_key: parent_partition,
                     from_state: "dispatch",
                     fencing_token: created_parent.fencing_token,
                     now_ms: 1_010
                   )

          assert {:ok, unchanged_parent} =
                   Ferricstore.Flow.get(ctx, parent_id, partition_key: parent_partition)

          assert unchanged_parent.state == "dispatch"
          assert unchanged_parent.child_groups == %{}

          child_key = Ferricstore.Flow.Keys.state_key(child_id, child_partition)
          child_shard = Router.shard_for(ctx, child_key)
          assert [] == :ets.lookup(elem(ctx.keydir_refs, child_shard), child_key)
        after
          FerricStore.Instance.cleanup(ctx.name)
        end
      end

      test "Flow colocated child completion resolves parent through WARaft", %{root: root} do
        ctx = build_ctx(Path.join(root, "flow-colocated-complete"), shard_count: 2)
        parent_id = "router-flow-parent-complete-#{System.unique_integer([:positive])}"
        child_id = "router-flow-child-complete-#{System.unique_integer([:positive])}"
        parent_partition = flow_partition_for_shard(ctx, parent_id, 0)
        child_partition = parent_partition

        try do
          assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          assert {:ok, _waiting_parent} =
                   setup_flow_child(
                     ctx,
                     parent_id,
                     child_id,
                     parent_partition,
                     child_partition,
                     group_id: "complete-fanout"
                   )

          claimed = claim_flow_child!(ctx, child_id, child_partition, "worker-complete")

          assert :ok =
                   Ferricstore.Flow.complete(ctx, child_id, claimed.lease_token,
                     partition_key: child_partition,
                     fencing_token: claimed.fencing_token,
                     result: "ok",
                     now_ms: 2_000
                   )

          assert {:ok, completed_child} =
                   Ferricstore.Flow.get(ctx, child_id, partition_key: child_partition)

          assert completed_child.state == "completed"

          assert {:ok, done_parent} =
                   Ferricstore.Flow.get(ctx, parent_id, partition_key: parent_partition)

          assert done_parent.state == "children_done"
          assert done_parent.child_groups["complete-fanout"]["children"][child_id] == "completed"
          assert done_parent.child_groups["complete-fanout"]["summary"]["completed"] == 1
        after
          FerricStore.Instance.cleanup(ctx.name)
        end
      end

      test "Flow colocated retry exhaustion resolves parent through WARaft", %{root: root} do
        ctx = build_ctx(Path.join(root, "flow-colocated-retry"), shard_count: 2)
        parent_id = "router-flow-parent-retry-#{System.unique_integer([:positive])}"
        child_id = "router-flow-child-retry-#{System.unique_integer([:positive])}"
        parent_partition = flow_partition_for_shard(ctx, parent_id, 0)
        child_partition = parent_partition

        try do
          assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          assert {:ok, _waiting_parent} =
                   setup_flow_child(
                     ctx,
                     parent_id,
                     child_id,
                     parent_partition,
                     child_partition,
                     group_id: "retry-fanout",
                     on_child_failed: :fail_parent
                   )

          claimed = claim_flow_child!(ctx, child_id, child_partition, "worker-retry")

          assert :ok =
                   Ferricstore.Flow.retry(ctx, child_id, claimed.lease_token,
                     partition_key: child_partition,
                     fencing_token: claimed.fencing_token,
                     now_ms: 2_000,
                     retry: [max_retries: 0, exhausted_to: "failed"]
                   )

          assert {:ok, failed_child} =
                   Ferricstore.Flow.get(ctx, child_id, partition_key: child_partition)

          assert failed_child.state == "failed"

          assert {:ok, failed_parent} =
                   Ferricstore.Flow.get(ctx, parent_id, partition_key: parent_partition)

          assert failed_parent.state == "children_failed"
          assert failed_parent.child_groups["retry-fanout"]["children"][child_id] == "failed"
          assert failed_parent.child_groups["retry-fanout"]["summary"]["failed"] == 1
        after
          FerricStore.Instance.cleanup(ctx.name)
        end
      end

      test "Flow colocated fail and cancel propagate parent policy through WARaft", %{
        root: root
      } do
        ctx = build_ctx(Path.join(root, "flow-colocated-fail-cancel"), shard_count: 2)
        fail_parent = "router-flow-parent-fail-#{System.unique_integer([:positive])}"
        fail_child = "router-flow-child-fail-#{System.unique_integer([:positive])}"
        cancel_parent = "router-flow-parent-cancel-#{System.unique_integer([:positive])}"
        cancel_child = "router-flow-child-cancel-#{System.unique_integer([:positive])}"
        parent_partition = flow_partition_for_shard(ctx, fail_parent, 0)
        child_partition = parent_partition
        cancel_parent_partition = flow_partition_for_shard(ctx, cancel_parent, 0)
        cancel_child_partition = cancel_parent_partition

        try do
          assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          assert {:ok, _waiting_parent} =
                   setup_flow_child(
                     ctx,
                     fail_parent,
                     fail_child,
                     parent_partition,
                     child_partition,
                     group_id: "fail-fanout",
                     on_child_failed: :fail_parent
                   )

          claimed = claim_flow_child!(ctx, fail_child, child_partition, "worker-fail")

          assert :ok =
                   Ferricstore.Flow.fail(ctx, fail_child, claimed.lease_token,
                     partition_key: child_partition,
                     fencing_token: claimed.fencing_token,
                     error: "boom",
                     now_ms: 2_000
                   )

          assert {:ok, failed_parent} =
                   Ferricstore.Flow.get(ctx, fail_parent, partition_key: parent_partition)

          assert failed_parent.state == "children_failed"
          assert failed_parent.child_groups["fail-fanout"]["children"][fail_child] == "failed"

          assert {:ok, waiting_cancel_parent} =
                   setup_flow_child(
                     ctx,
                     cancel_parent,
                     cancel_child,
                     cancel_parent_partition,
                     cancel_child_partition,
                     group_id: "cancel-fanout",
                     on_parent_closed: :cancel_children
                   )

          assert :ok =
                   Ferricstore.Flow.cancel(ctx, cancel_parent,
                     partition_key: cancel_parent_partition,
                     fencing_token: waiting_cancel_parent.fencing_token,
                     now_ms: 3_000
                   )

          assert {:ok, cancelled_child} =
                   Ferricstore.Flow.get(ctx, cancel_child, partition_key: cancel_child_partition)

          assert cancelled_child.state == "cancelled"

          assert {:ok, cancelled_parent} =
                   Ferricstore.Flow.get(ctx, cancel_parent,
                     partition_key: cancel_parent_partition
                   )

          assert cancelled_parent.child_groups["cancel-fanout"]["children"][cancel_child] ==
                   "cancelled"
        after
          FerricStore.Instance.cleanup(ctx.name)
        end
      end
    end
  end
end
