defmodule Ferricstore.Flow.HistoryProjectorTest.Sections.AsyncEnqueueRejectsAboveConfiguredPendingCapSoApplyFallBackSy do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Bitcask.NIF
      alias Ferricstore.Flow.HistoryProjector
      alias Ferricstore.Flow.NativeOrderedIndex
      alias Ferricstore.Flow.OrderedIndex

      test "async enqueue rejects above configured pending cap so apply can fall back to sync projection" do
        unique = System.unique_integer([:positive])

        old_max_pending =
          Application.get_env(:ferricstore, :flow_history_projector_max_pending_entries)

        old_flush_interval =
          Application.get_env(:ferricstore, :flow_history_projector_flush_interval_ms)

        instance_name = :"history_projector_pending_cap_#{unique}"
        dir = Path.join(System.tmp_dir!(), "ferricstore_history_projector_pending_cap_#{unique}")
        File.mkdir_p!(dir)

        keydir = :ets.new(:"history_projector_pending_cap_keydir_#{unique}", [:set, :public])
        {flow_index, flow_lookup} = OrderedIndex.table_names(instance_name, 0)
        NativeOrderedIndex.reset(flow_index, flow_lookup)

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

        try do
          Application.put_env(:ferricstore, :flow_history_projector_max_pending_entries, 1)
          Application.put_env(:ferricstore, :flow_history_projector_flush_interval_ms, 60_000)

          {:ok, pid} =
            HistoryProjector.start_link(
              shard_index: 0,
              shard_data_path: dir,
              instance_ctx: ctx,
              recover_on_init: false
            )

          entry = fn version ->
            event_ms = 1_000 + version
            event_id = "#{event_ms}-#{version}"

            %{
              key: Ferricstore.Flow.Keys.stream_entry_key("flow-cap", event_id, nil),
              expire_at_ms: 0,
              history_key: Ferricstore.Flow.Keys.history_key("flow-cap"),
              event_id: event_id,
              event_ms: event_ms,
              version: version,
              value: "history-#{version}",
              ra_index: version
            }
          end

          assert :ok = HistoryProjector.enqueue_async(ctx, 0, [entry.(1)], 1)
          assert {:error, :queue_full} = HistoryProjector.enqueue_async(ctx, 0, [entry.(2)], 2)

          assert Process.alive?(pid)
        after
          if Process.whereis(HistoryProjector.name(ctx, 0)),
            do: GenServer.stop(HistoryProjector.name(ctx, 0))

          File.rm_rf(dir)
          restore_env(:flow_history_projector_max_pending_entries, old_max_pending)
          restore_env(:flow_history_projector_flush_interval_ms, old_flush_interval)
        end
      end

      test "fire-and-forget enqueue does not wait behind projector work" do
        unique = System.unique_integer([:positive])
        instance_name = :"history_projector_async_enqueue_#{unique}"

        dir =
          Path.join(System.tmp_dir!(), "ferricstore_history_projector_async_enqueue_#{unique}")

        File.mkdir_p!(dir)

        keydir = :ets.new(:"history_projector_async_enqueue_keydir_#{unique}", [:set, :public])
        {flow_index, flow_lookup} = OrderedIndex.table_names(instance_name, 0)
        NativeOrderedIndex.reset(flow_index, flow_lookup)

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

        try do
          {:ok, pid} =
            HistoryProjector.start_link(
              shard_index: 0,
              shard_data_path: dir,
              instance_ctx: ctx,
              recover_on_init: false
            )

          :ok = :sys.suspend(pid)

          entry = %{
            key: Ferricstore.Flow.Keys.stream_entry_key("flow-async-enqueue", "1001-1", nil),
            expire_at_ms: 0,
            history_key: Ferricstore.Flow.Keys.history_key("flow-async-enqueue"),
            event_id: "1001-1",
            event_ms: 1001,
            version: 1,
            value: "history-1",
            ra_index: 1
          }

          started = System.monotonic_time()
          assert :ok = HistoryProjector.enqueue_async(ctx, 0, [entry], 1)

          elapsed_ms =
            System.convert_time_unit(System.monotonic_time() - started, :native, :millisecond)

          assert elapsed_ms < 50

          :ok = :sys.resume(pid)
          assert :ok = HistoryProjector.flush(ctx, 0)
        after
          if Process.whereis(HistoryProjector.name(ctx, 0)),
            do: GenServer.stop(HistoryProjector.name(ctx, 0))

          File.rm_rf(dir)
        end
      end

      test "requested replay index is not published after async enqueue is lost before projector handles it" do
        unique = System.unique_integer([:positive])
        instance_name = :"history_projector_lost_async_enqueue_#{unique}"
        dir = Path.join(System.tmp_dir!(), "ferricstore_history_projector_lost_async_#{unique}")
        File.mkdir_p!(dir)

        keydir = :ets.new(:"history_projector_lost_async_keydir_#{unique}", [:set, :public])
        {flow_index, flow_lookup} = OrderedIndex.table_names(instance_name, 0)
        NativeOrderedIndex.reset(flow_index, flow_lookup)

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

        try do
          {:ok, pid} =
            HistoryProjector.start_link(
              shard_index: 0,
              shard_data_path: dir,
              instance_ctx: ctx,
              recover_on_init: false
            )

          :ok = :sys.suspend(pid)

          event_id = "1001-1"
          key = Ferricstore.Flow.Keys.stream_entry_key("flow-lost-async", event_id, nil)

          entry = %{
            key: key,
            expire_at_ms: 0,
            history_key: Ferricstore.Flow.Keys.history_key("flow-lost-async"),
            event_id: event_id,
            event_ms: 1001,
            version: 1,
            value: "history-lost-before-handle",
            ra_index: 42
          }

          assert :ok = HistoryProjector.enqueue_async(ctx, 0, [entry], 42)

          ref = Process.monitor(pid)
          Process.unlink(pid)
          Process.exit(pid, :kill)
          assert_receive {:DOWN, ^ref, :process, ^pid, :killed}, 1_000

          {:ok, _pid} =
            HistoryProjector.start_link(
              shard_index: 0,
              shard_data_path: dir,
              instance_ctx: ctx,
              recover_on_init: false
            )

          assert :requested = HistoryProjector.request(ctx, 0, dir, 42)
          assert {:error, :flush_failed} = HistoryProjector.flush(ctx, 0)
          refute HistoryProjector.durable?(ctx, 0, dir, 42)
          assert HistoryProjector.scan_event_value(dir, key) == :miss
        after
          if Process.whereis(HistoryProjector.name(ctx, 0)),
            do: GenServer.stop(HistoryProjector.name(ctx, 0))

          File.rm_rf(dir)
        end
      end

      test "timer-driven projection flushes one bounded chunk at a time" do
        unique = System.unique_integer([:positive])
        instance_name = :"history_projector_chunked_flush_#{unique}"
        dir = Path.join(System.tmp_dir!(), "ferricstore_history_projector_chunked_#{unique}")
        File.mkdir_p!(dir)

        old_batch = Application.get_env(:ferricstore, :flow_history_projector_batch_size)
        old_flush = Application.get_env(:ferricstore, :flow_history_projector_flush_interval_ms)
        old_chunk = Application.get_env(:ferricstore, :flow_history_projector_chunk_interval_ms)

        keydir = :ets.new(:"history_projector_chunked_keydir_#{unique}", [:set, :public])
        {flow_index, flow_lookup} = OrderedIndex.table_names(instance_name, 0)
        NativeOrderedIndex.reset(flow_index, flow_lookup)

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

        try do
          Application.put_env(:ferricstore, :flow_history_projector_batch_size, 2)
          Application.put_env(:ferricstore, :flow_history_projector_flush_interval_ms, 60_000)
          Application.put_env(:ferricstore, :flow_history_projector_chunk_interval_ms, 60_000)

          {:ok, pid} =
            HistoryProjector.start_link(
              shard_index: 0,
              shard_data_path: dir,
              instance_ctx: ctx,
              recover_on_init: false
            )

          entries =
            for version <- 1..5 do
              event_ms = 2_000 + version
              event_id = "#{event_ms}-#{version}"

              %{
                key: Ferricstore.Flow.Keys.stream_entry_key("flow-chunked", event_id, nil),
                expire_at_ms: 0,
                history_key: Ferricstore.Flow.Keys.history_key("flow-chunked"),
                event_id: event_id,
                event_ms: event_ms,
                version: version,
                value: "history-#{version}",
                ra_index: version
              }
            end

          assert :ok = HistoryProjector.enqueue_async(ctx, 0, entries, 5)

          Ferricstore.Test.ShardHelpers.eventually(
            fn ->
              state = :sys.get_state(pid)
              state.pending_count == 3 and :ets.info(keydir, :size) == 2
            end,
            "history projector should flush only one configured chunk",
            50,
            10
          )

          assert :ok = HistoryProjector.flush(ctx, 0)
          assert :ets.info(keydir, :size) == 5
        after
          if Process.whereis(HistoryProjector.name(ctx, 0)),
            do: GenServer.stop(HistoryProjector.name(ctx, 0))

          File.rm_rf(dir)
          restore_env(:flow_history_projector_batch_size, old_batch)
          restore_env(:flow_history_projector_flush_interval_ms, old_flush)
          restore_env(:flow_history_projector_chunk_interval_ms, old_chunk)
        end
      end

      test "start_link can skip recovery when shard startup already imported history" do
        unique = System.unique_integer([:positive])
        instance_name = :"history_projector_skip_recover_#{unique}"
        dir = Path.join(System.tmp_dir!(), "ferricstore_history_projector_skip_#{unique}")
        File.mkdir_p!(dir)

        keydir = :ets.new(:"history_projector_skip_keydir_#{unique}", [:set, :public])
        {flow_index, flow_lookup} = OrderedIndex.table_names(instance_name, 0)
        NativeOrderedIndex.reset(flow_index, flow_lookup)

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

        history_key = Ferricstore.Flow.Keys.history_key("flow-skip")
        event_id = "1000-1"
        key = Ferricstore.Flow.Keys.stream_entry_key("flow-skip", event_id, nil)

        entry = %{
          key: key,
          expire_at_ms: 0,
          history_key: history_key,
          event_id: event_id,
          event_ms: 1_000,
          version: 1,
          value: "encoded-history",
          history_max_events: nil,
          ra_index: 7
        }

        assert :ok = HistoryProjector.write_entries_sync(ctx, 0, dir, [entry], 7)
        :ets.delete_all_objects(keydir)
        NativeOrderedIndex.reset(flow_index, flow_lookup)

        {:ok, pid} =
          HistoryProjector.start_link(
            shard_index: 0,
            shard_data_path: dir,
            instance_ctx: ctx,
            recover_on_init: false
          )

        on_exit(fn ->
          if Process.alive?(pid), do: GenServer.stop(pid)
          File.rm_rf(dir)
        end)

        assert [] = :ets.lookup(keydir, key)
        assert OrderedIndex.count_all(flow_lookup, history_key) == 0
        assert HistoryProjector.durable?(ctx, 0, dir, 7)
      end

      test "projected Flow value refs use apply-stamped refs without decoding history payload" do
        generated_ref = "f:{flow-fast-ref}:v:p:flow-fast-ref:2"
        generated_shared_ref = "f:{flow-fast-ref}:v:s:flow-fast-ref:doc:1"
        external_ref = "external-payload-ref"

        refs =
          HistoryProjector.__projected_flow_value_refs_for_test__([
            %{
              value: "not-a-decodable-history-record",
              value_refs: [generated_ref, generated_shared_ref, external_ref, nil, ""]
            }
          ])

        assert MapSet.member?(refs, generated_ref)
        assert MapSet.member?(refs, generated_shared_ref)
        refute MapSet.member?(refs, external_ref)
      end

      test "projected watermark stays behind when marker persist fails" do
        unique = System.unique_integer([:positive])
        instance_name = :"history_projector_marker_fail_#{unique}"
        dir = Path.join(System.tmp_dir!(), "ferricstore_history_projector_marker_fail_#{unique}")
        File.mkdir_p!(dir)
        File.mkdir_p!(Ferricstore.Flow.HistoryProjectedIndex.path(dir))

        keydir = :ets.new(:"history_projector_marker_fail_keydir_#{unique}", [:set, :public])
        {flow_index, flow_lookup} = OrderedIndex.table_names(instance_name, 0)
        NativeOrderedIndex.reset(flow_index, flow_lookup)

        on_exit(fn -> File.rm_rf(dir) end)

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

        entry = %{
          key: Ferricstore.Flow.Keys.stream_entry_key("flow-marker-fail", "9000-1", nil),
          expire_at_ms: 0,
          history_key: Ferricstore.Flow.Keys.history_key("flow-marker-fail"),
          event_id: "9000-1",
          event_ms: 9_000,
          version: 1,
          value: "history-marker-fail",
          ra_index: 900
        }

        assert {:error, _reason} = HistoryProjector.write_entries_sync(ctx, 0, dir, [entry], 900)
        refute HistoryProjector.durable?(ctx, 0, dir, 900)
        assert :atomics.get(ctx.flow_history_projected_index, 1) == 0
      end

      test "projected index marker persists concurrently without tmp collisions or regression" do
        unique = System.unique_integer([:positive])

        dir =
          Path.join(System.tmp_dir!(), "ferricstore_history_projected_index_concurrent_#{unique}")

        File.mkdir_p!(dir)

        on_exit(fn -> File.rm_rf(dir) end)

        results =
          1..100
          |> Task.async_stream(
            fn index -> Ferricstore.Flow.HistoryProjectedIndex.persist(dir, index) end,
            max_concurrency: 32,
            timeout: 10_000
          )
          |> Enum.to_list()

        assert Enum.all?(results, &match?({:ok, :ok}, &1))
        assert Ferricstore.Flow.HistoryProjectedIndex.read(dir) == 100
      end

      test "projected index marker persistence fsyncs tmp file and parent directory" do
        source = File.read!("lib/ferricstore/flow/history_projected_index.ex")

        assert source =~ "NIF.v2_fsync(tmp_path)",
               "projected marker must fsync tmp contents before rename"

        assert source =~ "NIF.v2_fsync_dir(shard_data_path)",
               "projected marker must fsync the directory after rename"
      end

      test "pending_count fails closed when projector is not started" do
        unique = System.unique_integer([:positive])
        ctx = %{name: :"history_projector_missing_pending_#{unique}"}

        assert {:error, :not_started} = HistoryProjector.pending_count(ctx, 0)
      end

      test "async enqueue uses existing pending registry without waiting on table owner" do
        unique = System.unique_integer([:positive])
        instance_name = :"history_projector_enqueue_fast_registry_#{unique}"
        dir = Path.join(System.tmp_dir!(), "ferricstore_history_projector_enqueue_fast_#{unique}")
        File.mkdir_p!(dir)

        keydir = :ets.new(:"history_projector_enqueue_fast_keydir_#{unique}", [:set, :public])

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

        {:ok, pid} =
          HistoryProjector.start_link(
            shard_index: 0,
            shard_data_path: dir,
            instance_ctx: ctx,
            recover_on_init: false
          )

        owner = Process.whereis(Ferricstore.Flow.HistoryProjector.TableOwner)
        assert is_pid(owner)
        :sys.suspend(owner)

        on_exit(fn ->
          if Process.alive?(owner), do: :sys.resume(owner)
          if Process.alive?(pid), do: GenServer.stop(pid)
          File.rm_rf(dir)
        end)

        entry = %{
          key: Ferricstore.Flow.Keys.stream_entry_key("flow-fast-enqueue", "1000-1", nil),
          expire_at_ms: 0,
          history_key: Ferricstore.Flow.Keys.history_key("flow-fast-enqueue"),
          event_id: "1000-1",
          event_ms: 1_000,
          version: 1,
          value: "encoded-history",
          ra_index: 1
        }

        task = Task.async(fn -> HistoryProjector.enqueue_async(ctx, 0, [entry], 1) end)
        assert :ok = Task.await(task, 100)
      end
    end
  end
end
