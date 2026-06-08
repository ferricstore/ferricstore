defmodule Ferricstore.Flow.LMDBTest.Sections.StartupRebuildsActiveFlowIndexesDedicatedLmdbActiveIndex do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Flow.LMDBTest.FlushProbeWriter

      test "startup rebuilds active flow indexes from dedicated LMDB active index" do
        old_scan_limit = Application.get_env(:ferricstore, :flow_lmdb_state_rebuild_scan_limit)
        Application.put_env(:ferricstore, :flow_lmdb_state_rebuild_scan_limit, 1)

        data_dir =
          Path.join(
            System.tmp_dir!(),
            "ferricstore_flow_lmdb_active_rebuild_#{System.unique_integer([:positive])}"
          )

        instance_name = :"flow_lmdb_active_rebuild_#{System.unique_integer([:positive])}"
        shard_index = 0
        source_keydir = :ets.new(:flow_lmdb_active_rebuild_source, [:set])
        empty_recovery_keydir = :ets.new(:flow_lmdb_active_rebuild_empty, [:set])

        on_exit(fn ->
          restore_env(:flow_lmdb_state_rebuild_scan_limit, old_scan_limit)
          if :ets.info(source_keydir) != :undefined, do: :ets.delete(source_keydir)

          if :ets.info(empty_recovery_keydir) != :undefined,
            do: :ets.delete(empty_recovery_keydir)

          File.rm_rf!(data_dir)
        end)

        Ferricstore.DataDir.ensure_layout!(data_dir, 1)
        shard_path = Ferricstore.DataDir.shard_data_path(data_dir, shard_index)
        lmdb_path = Ferricstore.Flow.LMDB.path(shard_path)

        record = %{
          id: "flow-active",
          type: "startup-active",
          state: "queued",
          version: 1,
          attempts: 0,
          fencing_token: 0,
          created_at_ms: 1,
          updated_at_ms: 2,
          next_run_at_ms: 10,
          priority: 0,
          partition_key: "tenant-active",
          root_flow_id: "flow-active"
        }

        state_key = Ferricstore.Flow.Keys.state_key(record.id, record.partition_key)
        encoded = Ferricstore.Flow.encode_record(record)

        :ets.insert(source_keydir, {state_key, encoded, 0, 0, :hot, 0, byte_size(encoded)})

        start_supervised!(
          {Ferricstore.Flow.LMDBWriter,
           shard_index: shard_index,
           data_dir: data_dir,
           instance_name: instance_name,
           instance_ctx: %{name: instance_name, keydir_refs: {source_keydir}}}
        )

        assert :ok =
                 Ferricstore.Flow.LMDBWriter.enqueue(instance_name, shard_index, [
                   {:project_flow_state_from_source, state_key}
                 ])

        assert :ok = Ferricstore.Flow.LMDBWriter.flush(instance_name, shard_index)

        assert :ok =
                 Ferricstore.Flow.LMDB.write_batch(lmdb_path, [
                   {:put, "f:!large-value-before-state", String.duplicate("x", 1024)}
                 ])

        {flow_index, flow_lookup} =
          Ferricstore.Flow.OrderedIndex.table_names(instance_name, shard_index)

        Ferricstore.Flow.NativeOrderedIndex.reset(flow_index, flow_lookup)

        assert :ok =
                 Ferricstore.Flow.LMDBRebuilder.reconcile_shard(
                   shard_path,
                   empty_recovery_keydir,
                   shard_index,
                   nil,
                   nil,
                   nil,
                   flow_index,
                   flow_lookup
                 )

        state_index_key =
          Ferricstore.Flow.Keys.state_index_key(record.type, record.state, record.partition_key)

        due_key =
          Ferricstore.Flow.Keys.due_key(
            record.type,
            record.state,
            record.priority,
            record.partition_key
          )

        assert [{"flow-active", 2.0}] =
                 Ferricstore.Flow.OrderedIndex.range_slice(
                   flow_index,
                   state_index_key,
                   :neg_inf,
                   :inf,
                   false,
                   0,
                   :all
                 )

        assert [{"flow-active", 10.0}] =
                 Ferricstore.Flow.OrderedIndex.range_slice(
                   flow_index,
                   due_key,
                   :neg_inf,
                   :inf,
                   false,
                   0,
                   :all
                 )
      end

      test "LMDB startup rebuild scans terminal reverse projection once per shard" do
        data_dir =
          Path.join(
            System.tmp_dir!(),
            "ferricstore_flow_lmdb_terminal_rebuild_once_#{System.unique_integer([:positive])}"
          )

        shard_index = 0
        keydir = :ets.new(:flow_lmdb_terminal_rebuild_once_keydir, [:set])
        test_pid = self()
        handler_id = {:flow_lmdb_terminal_rebuild_once, self(), make_ref()}

        :telemetry.attach(
          handler_id,
          [:ferricstore, :flow, :lmdb_rebuild],
          fn event, measurements, metadata, _config ->
            send(test_pid, {:flow_lmdb_rebuild, event, measurements, metadata})
          end,
          nil
        )

        on_exit(fn ->
          :telemetry.detach(handler_id)
          if :ets.info(keydir) != :undefined, do: :ets.delete(keydir)
          File.rm_rf!(data_dir)
        end)

        Ferricstore.DataDir.ensure_layout!(data_dir, 1)
        shard_path = Ferricstore.DataDir.shard_data_path(data_dir, shard_index)
        lmdb_path = Ferricstore.Flow.LMDB.path(shard_path)

        stale_state_key = Ferricstore.Flow.Keys.state_key("stale-terminal", "tenant-startup")
        stale_terminal_key = Ferricstore.Flow.LMDB.terminal_index_key("idx", "stale-terminal", 1)

        assert :ok =
                 Ferricstore.Flow.LMDB.write_batch(lmdb_path, [
                   {:put, Ferricstore.Flow.LMDB.terminal_by_state_key_key(stale_state_key),
                    stale_terminal_key}
                 ])

        for i <- 1..513 do
          record = %{
            id: "terminal-#{i}",
            type: "startup-terminal",
            state: "completed",
            version: 1,
            attempts: 0,
            fencing_token: 0,
            created_at_ms: i,
            updated_at_ms: i,
            terminal_retention_until_ms: 10_000,
            partition_key: "tenant-startup",
            root_flow_id: "terminal-#{i}"
          }

          state_key = Ferricstore.Flow.Keys.state_key(record.id, record.partition_key)
          encoded = Ferricstore.Flow.encode_record(record)
          :ets.insert(keydir, {state_key, encoded, 0, 0, :hot, 0, byte_size(encoded)})
        end

        assert :ok =
                 Ferricstore.Flow.LMDBRebuilder.reconcile_shard(
                   shard_path,
                   keydir,
                   shard_index,
                   nil,
                   nil,
                   nil,
                   nil,
                   nil
                 )

        assert_receive {:flow_lmdb_rebuild, [:ferricstore, :flow, :lmdb_rebuild],
                        %{terminal_reverse_cleanup_scans: 1}, %{shard_index: 0}}
      end

      test "default WARaft shard startup uses LMDB active projection without full LMDB reconcile" do
        data_dir =
          Path.join(
            System.tmp_dir!(),
            "ferricstore_flow_lmdb_default_startup_fast_#{System.unique_integer([:positive])}"
          )

        instance_name = :default
        shard_index = 0
        keydir = :ets.new(:flow_lmdb_default_startup_fast_keydir, [:set])
        test_pid = self()
        handler_id = {:flow_lmdb_default_startup_fast, self(), make_ref()}

        :telemetry.attach(
          handler_id,
          [:ferricstore, :flow, :lmdb_rebuild],
          fn event, measurements, metadata, _config ->
            send(test_pid, {:flow_lmdb_rebuild, event, measurements, metadata})
          end,
          nil
        )

        on_exit(fn ->
          :telemetry.detach(handler_id)
          if :ets.info(keydir) != :undefined, do: :ets.delete(keydir)
          File.rm_rf!(data_dir)
        end)

        Ferricstore.DataDir.ensure_layout!(data_dir, 1)
        shard_path = Ferricstore.DataDir.shard_data_path(data_dir, shard_index)
        lmdb_path = Ferricstore.Flow.LMDB.path(shard_path)

        record =
          active_lmdb_record("flow-default-fast", "default-fast", "queued",
            partition_key: "tenant-default-fast",
            updated_at_ms: 10,
            next_run_at_ms: 20
          )

        state_key = Ferricstore.Flow.Keys.state_key(record.id, record.partition_key)

        assert :ok =
                 Ferricstore.Flow.LMDB.write_batch(
                   lmdb_path,
                   elem(
                     Ferricstore.Flow.LMDB.active_index_put_ops_with_reverse(
                       state_key,
                       record,
                       0
                     ),
                     0
                   )
                 )

        {flow_index, flow_lookup} =
          Ferricstore.Flow.OrderedIndex.table_names(instance_name, shard_index)

        Ferricstore.Flow.NativeOrderedIndex.reset(flow_index, flow_lookup)

        assert :ok =
                 Ferricstore.Flow.LMDBRebuilder.reconcile_startup_shard(
                   shard_path,
                   keydir,
                   shard_index,
                   %{name: :default},
                   nil,
                   nil,
                   flow_index,
                   flow_lookup
                 )

        state_index_key =
          Ferricstore.Flow.Keys.state_index_key(record.type, record.state, record.partition_key)

        assert [{"flow-default-fast", 10.0}] =
                 Ferricstore.Flow.OrderedIndex.range_slice(
                   flow_index,
                   state_index_key,
                   :neg_inf,
                   :inf,
                   false,
                   0,
                   :all
                 )

        refute_received {:flow_lmdb_rebuild, [:ferricstore, :flow, :lmdb_rebuild], _measurements,
                         _metadata}
      end

      test "default WARaft startup rebuilds active indexes from keydir when LMDB env is absent" do
        data_dir =
          Path.join(
            System.tmp_dir!(),
            "ferricstore_flow_lmdb_default_startup_no_env_#{System.unique_integer([:positive])}"
          )

        instance_name = :default
        shard_index = 0
        keydir = :ets.new(:flow_lmdb_default_startup_no_env_keydir, [:set])

        on_exit(fn ->
          if :ets.info(keydir) != :undefined, do: :ets.delete(keydir)
          File.rm_rf!(data_dir)
        end)

        Ferricstore.DataDir.ensure_layout!(data_dir, 1)
        assert :ok = Ferricstore.Flow.LMDB.ensure_shard_dirs(data_dir, 1)

        shard_path = Ferricstore.DataDir.shard_data_path(data_dir, shard_index)
        lmdb_path = Ferricstore.Flow.LMDB.path(shard_path)
        assert File.dir?(lmdb_path)
        refute Ferricstore.Flow.LMDB.env_present?(lmdb_path)

        record =
          active_lmdb_record("flow-default-no-env", "default-no-env", "queued",
            partition_key: "tenant-default-no-env",
            updated_at_ms: 10,
            next_run_at_ms: 20
          )

        state_key = Ferricstore.Flow.Keys.state_key(record.id, record.partition_key)
        encoded = Ferricstore.Flow.encode_record(record)
        :ets.insert(keydir, {state_key, encoded, 0, 0, :hot, 0, byte_size(encoded)})

        {flow_index, flow_lookup} =
          Ferricstore.Flow.OrderedIndex.table_names(instance_name, shard_index)

        Ferricstore.Flow.NativeOrderedIndex.reset(flow_index, flow_lookup)

        assert :ok =
                 Ferricstore.Flow.LMDBRebuilder.reconcile_startup_shard(
                   shard_path,
                   keydir,
                   shard_index,
                   %{name: :default},
                   nil,
                   nil,
                   flow_index,
                   flow_lookup
                 )

        state_index_key =
          Ferricstore.Flow.Keys.state_index_key(record.type, record.state, record.partition_key)

        due_key =
          Ferricstore.Flow.Keys.due_key(
            record.type,
            record.state,
            record.priority,
            record.partition_key
          )

        assert [{record.id, 10.0}] ==
                 Ferricstore.Flow.OrderedIndex.range_slice(
                   flow_index,
                   state_index_key,
                   :neg_inf,
                   :inf,
                   false,
                   0,
                   :all
                 )

        assert [{record.id, 20.0}] ==
                 Ferricstore.Flow.OrderedIndex.range_slice(
                   flow_index,
                   due_key,
                   :neg_inf,
                   :inf,
                   false,
                   0,
                   :all
                 )
      end

      test "forced default WARaft startup reconcile materializes blob-backed state records" do
        data_dir =
          Path.join(
            System.tmp_dir!(),
            "ferricstore_flow_lmdb_default_forced_blob_#{System.unique_integer([:positive])}"
          )

        instance_name = :default
        shard_index = 0
        keydir = :ets.new(:flow_lmdb_default_forced_blob_keydir, [:set])

        on_exit(fn ->
          if :ets.info(keydir) != :undefined, do: :ets.delete(keydir)
          File.rm_rf!(data_dir)
        end)

        Ferricstore.DataDir.ensure_layout!(data_dir, 1)
        shard_path = Ferricstore.DataDir.shard_data_path(data_dir, shard_index)
        lmdb_path = Ferricstore.Flow.LMDB.path(shard_path)

        record =
          active_lmdb_record("flow-default-forced-blob", "default-forced-blob", "queued",
            partition_key: "tenant-default-forced-blob",
            updated_at_ms: 10,
            next_run_at_ms: 20
          )

        state_key = Ferricstore.Flow.Keys.state_key(record.id, record.partition_key)
        encoded = Ferricstore.Flow.encode_record(record)
        assert {:ok, blob_ref} = Ferricstore.Store.BlobStore.put(data_dir, shard_index, encoded)
        encoded_ref = Ferricstore.Store.BlobRef.encode!(blob_ref)
        :ets.insert(keydir, {state_key, encoded_ref, 0, 0, :hot, 0, byte_size(encoded_ref)})

        {flow_index, flow_lookup} =
          Ferricstore.Flow.OrderedIndex.table_names(instance_name, shard_index)

        Ferricstore.Flow.NativeOrderedIndex.reset(flow_index, flow_lookup)

        assert :ok =
                 Ferricstore.Flow.LMDBRebuilder.reconcile_startup_shard(
                   shard_path,
                   keydir,
                   shard_index,
                   %{
                     name: :default,
                     data_dir: data_dir,
                     blob_side_channel_threshold_bytes: 128
                   },
                   nil,
                   nil,
                   flow_index,
                   flow_lookup,
                   force_full_reconcile?: true,
                   reason: :segment_replay
                 )

        assert {:ok, lmdb_blob} = Ferricstore.Flow.LMDB.get(lmdb_path, state_key)
        assert {:ok, ^encoded} = Ferricstore.Flow.LMDB.decode_value(lmdb_blob, 30)
      end

      test "default WARaft startup rebuilds active LMDB projection in bounded chunks" do
        data_dir =
          Path.join(
            System.tmp_dir!(),
            "ferricstore_flow_lmdb_default_startup_chunks_#{System.unique_integer([:positive])}"
          )

        instance_name = :default
        shard_index = 0
        keydir = :ets.new(:flow_lmdb_default_startup_chunks_keydir, [:set])
        test_pid = self()
        handler_id = {:flow_lmdb_default_startup_chunks, self(), make_ref()}

        :telemetry.attach(
          handler_id,
          [:ferricstore, :flow, :lmdb_startup_active_index_chunk],
          fn event, measurements, metadata, _config ->
            send(test_pid, {:flow_lmdb_active_chunk, event, measurements, metadata})
          end,
          nil
        )

        on_exit(fn ->
          :telemetry.detach(handler_id)
          if :ets.info(keydir) != :undefined, do: :ets.delete(keydir)
          File.rm_rf!(data_dir)
        end)

        Ferricstore.DataDir.ensure_layout!(data_dir, 1)
        shard_path = Ferricstore.DataDir.shard_data_path(data_dir, shard_index)
        lmdb_path = Ferricstore.Flow.LMDB.path(shard_path)

        1..4097
        |> Enum.chunk_every(512)
        |> Enum.each(fn chunk ->
          ops =
            Enum.flat_map(chunk, fn i ->
              record =
                active_lmdb_record("chunked-#{i}", "chunked-type", "queued",
                  partition_key: "tenant-chunked",
                  updated_at_ms: i
                )

              state_key = Ferricstore.Flow.Keys.state_key(record.id, record.partition_key)

              elem(
                Ferricstore.Flow.LMDB.active_index_put_ops_with_reverse(state_key, record, 0),
                0
              )
            end)

          assert :ok = Ferricstore.Flow.LMDB.write_batch(lmdb_path, ops)
        end)

        {flow_index, flow_lookup} =
          Ferricstore.Flow.OrderedIndex.table_names(instance_name, shard_index)

        Ferricstore.Flow.NativeOrderedIndex.reset(flow_index, flow_lookup)

        assert :ok =
                 Ferricstore.Flow.LMDBRebuilder.reconcile_startup_shard(
                   shard_path,
                   keydir,
                   shard_index,
                   %{name: :default},
                   nil,
                   nil,
                   flow_index,
                   flow_lookup
                 )

        chunks = collect_flow_lmdb_active_chunks()

        assert length(chunks) == 2
        assert Enum.map(chunks, & &1.entries) == [4096, 1]
        assert List.last(chunks).total_active == 4097
      end

      test "startup active LMDB rebuilds are concurrency bounded" do
        limit = Ferricstore.Flow.LMDBRebuilder.__startup_active_rebuild_concurrency_for_test__()
        task_count = max(limit * 3, 4)
        parent = self()
        active = :atomics.new(1, signed: true)

        tasks =
          for _ <- 1..task_count do
            Task.async(fn ->
              Ferricstore.Flow.LMDBRebuilder.__with_startup_active_rebuild_slot_for_test__(fn ->
                current = :atomics.add_get(active, 1, 1)
                send(parent, {:active_lmdb_rebuild_entered, current})
                Process.sleep(25)
                :atomics.sub_get(active, 1, 1)
              end)
            end)
          end

        Task.await_many(tasks, 5_000)

        max_seen =
          1..task_count
          |> Enum.map(fn _ ->
            assert_receive {:active_lmdb_rebuild_entered, current}, 1_000
            current
          end)
          |> Enum.max()

        assert max_seen <= limit
      end

      test "LMDB active projection replaces stale indexes when state changes" do
        fixture = start_active_lmdb_projection_fixture!("replace-active")

        queued =
          active_lmdb_record("flow-replace-active", "replace-active", "queued",
            partition_key: fixture.partition_key,
            updated_at_ms: 2,
            next_run_at_ms: 10
          )

        running =
          queued
          |> Map.merge(%{
            state: "running",
            version: 2,
            updated_at_ms: 20,
            next_run_at_ms: nil,
            lease_owner: "worker-replace-active",
            lease_token: "lease-replace-active",
            lease_deadline_ms: 50
          })

        state_key = project_active_lmdb_record!(fixture, queued)
        assert is_binary(state_key)
        project_active_lmdb_record!(fixture, running)

        queued_state_key =
          Ferricstore.Flow.Keys.state_index_key(queued.type, queued.state, queued.partition_key)

        queued_due_key =
          Ferricstore.Flow.Keys.due_key(
            queued.type,
            queued.state,
            queued.priority,
            queued.partition_key
          )

        running_state_key =
          Ferricstore.Flow.Keys.state_index_key(
            running.type,
            running.state,
            running.partition_key
          )

        worker_key =
          Ferricstore.Flow.Keys.worker_index_key(running.lease_owner, running.partition_key)

        inflight_key =
          Ferricstore.Flow.Keys.inflight_index_key(running.type, running.partition_key)

        assert {:ok, 0} =
                 Ferricstore.Flow.LMDB.prefix_count(
                   fixture.lmdb_path,
                   Ferricstore.Flow.LMDB.active_index_prefix(queued_state_key)
                 )

        assert {:ok, 0} =
                 Ferricstore.Flow.LMDB.prefix_count(
                   fixture.lmdb_path,
                   Ferricstore.Flow.LMDB.active_index_prefix(queued_due_key)
                 )

        assert {:ok, 1} =
                 Ferricstore.Flow.LMDB.prefix_count(
                   fixture.lmdb_path,
                   Ferricstore.Flow.LMDB.active_index_prefix(running_state_key)
                 )

        assert {:ok, 1} =
                 Ferricstore.Flow.LMDB.prefix_count(
                   fixture.lmdb_path,
                   Ferricstore.Flow.LMDB.active_index_prefix(worker_key)
                 )

        assert {:ok, 1} =
                 Ferricstore.Flow.LMDB.prefix_count(
                   fixture.lmdb_path,
                   Ferricstore.Flow.LMDB.active_index_prefix(inflight_key)
                 )
      end

      test "LMDB terminal projection removes stale active indexes" do
        fixture = start_active_lmdb_projection_fixture!("terminal-active")

        queued =
          active_lmdb_record("flow-terminal-active", "terminal-active", "queued",
            partition_key: fixture.partition_key,
            updated_at_ms: 2,
            next_run_at_ms: 10
          )

        completed =
          queued
          |> Map.merge(%{
            state: "completed",
            version: 2,
            updated_at_ms: 20,
            next_run_at_ms: nil
          })

        project_active_lmdb_record!(fixture, queued)
        state_key = project_active_lmdb_record!(fixture, completed)

        queued_state_key =
          Ferricstore.Flow.Keys.state_index_key(queued.type, queued.state, queued.partition_key)

        queued_due_key =
          Ferricstore.Flow.Keys.due_key(
            queued.type,
            queued.state,
            queued.priority,
            queued.partition_key
          )

        completed_state_key =
          Ferricstore.Flow.Keys.state_index_key(
            completed.type,
            completed.state,
            completed.partition_key
          )

        assert {:ok, 0} =
                 Ferricstore.Flow.LMDB.prefix_count(
                   fixture.lmdb_path,
                   Ferricstore.Flow.LMDB.active_index_prefix(queued_state_key)
                 )

        assert {:ok, 0} =
                 Ferricstore.Flow.LMDB.prefix_count(
                   fixture.lmdb_path,
                   Ferricstore.Flow.LMDB.active_index_prefix(queued_due_key)
                 )

        assert {:ok, 1} =
                 Ferricstore.Flow.LMDB.prefix_count(
                   fixture.lmdb_path,
                   Ferricstore.Flow.LMDB.terminal_index_prefix(completed_state_key)
                 )

        assert {:ok, terminal_key} =
                 Ferricstore.Flow.LMDB.get(
                   fixture.lmdb_path,
                   Ferricstore.Flow.LMDB.terminal_by_state_key_key(state_key)
                 )

        assert is_binary(terminal_key)
      end

      test "LMDB reconcile removes stale active indexes for terminal states" do
        data_dir =
          Path.join(
            System.tmp_dir!(),
            "ferricstore_flow_lmdb_reconcile_terminal_active_#{System.unique_integer([:positive])}"
          )

        instance_name =
          :"flow_lmdb_reconcile_terminal_active_#{System.unique_integer([:positive])}"

        shard_index = 0
        keydir = :ets.new(:flow_lmdb_reconcile_terminal_active_keydir, [:set])

        on_exit(fn ->
          if :ets.info(keydir) != :undefined, do: :ets.delete(keydir)
          File.rm_rf!(data_dir)
        end)

        Ferricstore.DataDir.ensure_layout!(data_dir, 1)
        shard_path = Ferricstore.DataDir.shard_data_path(data_dir, shard_index)
        lmdb_path = Ferricstore.Flow.LMDB.path(shard_path)

        queued =
          active_lmdb_record(
            "flow-reconcile-terminal-active",
            "reconcile-terminal-active",
            "queued",
            partition_key: "tenant-reconcile-terminal-active",
            updated_at_ms: 2,
            next_run_at_ms: 10
          )

        completed =
          queued
          |> Map.merge(%{
            state: "completed",
            version: 2,
            updated_at_ms: 20,
            next_run_at_ms: nil
          })

        state_key = Ferricstore.Flow.Keys.state_key(completed.id, completed.partition_key)

        assert :ok =
                 Ferricstore.Flow.LMDB.write_batch(
                   lmdb_path,
                   elem(
                     Ferricstore.Flow.LMDB.active_index_put_ops_with_reverse(
                       state_key,
                       queued,
                       0
                     ),
                     0
                   )
                 )

        encoded_completed = Ferricstore.Flow.encode_record(completed)

        :ets.insert(
          keydir,
          {state_key, encoded_completed, 0, 0, :hot, 0, byte_size(encoded_completed)}
        )

        {flow_index, flow_lookup} =
          Ferricstore.Flow.OrderedIndex.table_names(instance_name, shard_index)

        Ferricstore.Flow.NativeOrderedIndex.reset(flow_index, flow_lookup)

        assert :ok =
                 Ferricstore.Flow.LMDBRebuilder.reconcile_shard(
                   shard_path,
                   keydir,
                   shard_index,
                   %{name: instance_name},
                   nil,
                   nil,
                   flow_index,
                   flow_lookup
                 )

        queued_state_key =
          Ferricstore.Flow.Keys.state_index_key(queued.type, queued.state, queued.partition_key)

        completed_state_key =
          Ferricstore.Flow.Keys.state_index_key(
            completed.type,
            completed.state,
            completed.partition_key
          )

        assert {:ok, 0} =
                 Ferricstore.Flow.LMDB.prefix_count(
                   lmdb_path,
                   Ferricstore.Flow.LMDB.active_index_prefix(queued_state_key)
                 )

        assert [] =
                 Ferricstore.Flow.OrderedIndex.range_slice(
                   flow_index,
                   queued_state_key,
                   :neg_inf,
                   :inf,
                   false,
                   0,
                   :all
                 )

        assert {:ok, 1} =
                 Ferricstore.Flow.LMDB.prefix_count(
                   lmdb_path,
                   Ferricstore.Flow.LMDB.terminal_index_prefix(completed_state_key)
                 )
      end
    end
  end
end
