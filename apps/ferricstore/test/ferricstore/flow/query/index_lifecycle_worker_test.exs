defmodule Ferricstore.Flow.Query.IndexLifecycleWorkerTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.Query.{
    AdmissionController,
    IndexLifecycleWorker,
    IndexProvider,
    IndexRegistry
  }

  test "resumes a fenced catalog build, validates every definition, and activates it atomically" do
    {ctx, registry, worker, data_dir} = test_context("complete")
    on_exit(fn -> File.rm_rf!(data_dir) end)
    {:ok, registry_pid} = start_registry(ctx, registry)
    parent = self()

    barrier = fn _ctx, 0 ->
      send(parent, :barrier)
      :ok
    end

    snapshot_page = fn _ctx, 0, build_id, _items, _bytes ->
      send(parent, {:snapshot_page, build_id})
      {:ok, %{done?: true, scanned_keys: 2, staged_states: 1}}
    end

    page = fn _ctx, 0, build_id, cursor, _items, _bytes, _opts ->
      send(parent, {:backfill_page, build_id, cursor})

      {:ok,
       %{
         records: [%{state_key: "state", record: %{}, expire_at_ms: 0}],
         cursor: "last-page",
         done?: true,
         scanned_entries: 1,
         hydrated_bytes: 64
       }}
    end

    project = fn _ctx, 0, records, definitions ->
      send(parent, {:project, length(records), length(definitions)})

      {:ok,
       %{
         projected_records: 1,
         written_entries: length(definitions),
         write_ops: length(definitions) + 2,
         written_bytes: 1_024
       }}
    end

    validation = fn _ctx, 0, _build_id, definitions, checkpoint, _items, _bytes ->
      send(parent, {:validate, length(definitions), checkpoint})
      {:ok, advance_validation(checkpoint, length(definitions))}
    end

    cleanup = fn _ctx, 0, build_id ->
      send(parent, {:cleanup, build_id})
      :ok
    end

    {:ok, worker_pid} =
      start_worker(ctx, registry, worker,
        barrier_fun: barrier,
        snapshot_page_fun: snapshot_page,
        page_fun: page,
        project_fun: project,
        validation_fun: validation,
        cleanup_fun: cleanup
      )

    assert {:ok, :build_fenced} = IndexLifecycleWorker.run_once(worker)
    assert_receive :barrier
    assert {:ok, :snapshot_complete} = IndexLifecycleWorker.run_once(worker)
    assert_receive {:snapshot_page, build_id}

    assert {:ok, %{checkpoints: %{0 => %{phase: :backfill, fenced: true}}}} =
             IndexRegistry.build_status(registry, build_id)

    GenServer.stop(worker_pid)

    {:ok, _resumed_pid} =
      start_worker(ctx, registry, worker,
        barrier_fun: barrier,
        snapshot_page_fun: fn _ctx, _shard, _build_id, _items, _bytes ->
          raise "snapshot must not restart"
        end,
        page_fun: page,
        project_fun: project,
        validation_fun: validation,
        cleanup_fun: cleanup
      )

    assert {:ok, :shard_complete} = IndexLifecycleWorker.run_once(worker)
    assert_receive {:backfill_page, ^build_id, ""}
    assert_receive {:project, 1, definition_count}
    assert definition_count >= 3

    assert {:ok, :validation_fenced} = IndexLifecycleWorker.run_once(worker)
    assert_receive :barrier
    assert {:ok, :validation_progress} = IndexLifecycleWorker.run_once(worker)
    assert_receive {:validate, ^definition_count, %{fenced: true, phase: :source}}

    for _phase_step <- 1..(definition_count * 2) do
      assert {:ok, :validation_progress} = IndexLifecycleWorker.run_once(worker)
    end

    assert {:ok, :validation_shard_complete} = IndexLifecycleWorker.run_once(worker)
    assert_receive {:cleanup, ^build_id}
    assert {:ok, :build_activated} = IndexLifecycleWorker.run_once(worker)

    assert {:ok, active} = FerricStore.Flow.QueryIndexProvider.active_indexes(ctx, 0)
    assert length(active) == definition_count
    assert Enum.all?(active, &(&1.coverage.validation == :passed))
    assert Process.alive?(registry_pid)
  end

  test "pauses build work under operational pressure without moving durable checkpoints" do
    {ctx, registry, worker, data_dir} = test_context("pressure_pause")
    on_exit(fn -> File.rm_rf!(data_dir) end)
    {:ok, _registry_pid} = start_registry(ctx, registry)
    {:ok, %{indexes: [index | _]}} = IndexRegistry.snapshot(ctx, 0)
    assert {:ok, before} = IndexRegistry.build_status(registry, index.build_id)
    parent = self()
    pressure_calls = :counters.new(1, [])

    {:ok, _worker_pid} =
      start_worker(ctx, registry, worker,
        pressure_fun: fn ->
          :counters.add(pressure_calls, 1, 1)
          true
        end,
        barrier_fun: fn _ctx, _shard ->
          send(parent, :pressure_barrier)
          :ok
        end,
        snapshot_page_fun: fn _ctx, _shard, _build_id, _items, _bytes ->
          send(parent, :pressure_snapshot)
          {:ok, %{done?: true, scanned_keys: 0, staged_states: 0}}
        end,
        page_fun: fn _ctx, _shard, _build_id, _cursor, _items, _bytes, _opts ->
          send(parent, :pressure_page)
          {:error, :must_not_run}
        end,
        project_fun: fn _ctx, _shard, _records, _definitions ->
          send(parent, :pressure_project)
          {:error, :must_not_run}
        end
      )

    assert {:ok, :pressure_paused} = IndexLifecycleWorker.run_once(worker)
    assert :counters.get(pressure_calls, 1) == 1
    refute_receive :pressure_barrier
    refute_receive :pressure_snapshot
    refute_receive :pressure_page
    refute_receive :pressure_project
    assert {:ok, after_pause} = IndexRegistry.build_status(registry, index.build_id)
    assert after_pause.checkpoints == before.checkpoints
    assert after_pause.validation_checkpoints == before.validation_checkpoints
  end

  test "fails pressure sampling closed before validation work" do
    {ctx, registry, worker, data_dir} = test_context("pressure_failure")
    on_exit(fn -> File.rm_rf!(data_dir) end)
    {:ok, _registry_pid} = start_registry(ctx, registry)
    {:ok, %{indexes: [index | _]}} = IndexRegistry.snapshot(ctx, 0)
    complete_build!(registry, index.build_id)
    assert {:ok, before} = IndexRegistry.build_status(registry, index.build_id)
    parent = self()

    {:ok, _worker_pid} =
      start_worker(ctx, registry, worker,
        pressure_fun: fn -> raise "pressure unavailable" end,
        barrier_fun: fn _ctx, _shard ->
          send(parent, :validation_pressure_barrier)
          :ok
        end,
        validation_fun: fn _ctx, _shard, _build, _definitions, _checkpoint, _items, _bytes ->
          send(parent, :validation_under_pressure)
          {:error, :must_not_run}
        end
      )

    assert {:ok, :pressure_paused} = IndexLifecycleWorker.run_once(worker)
    refute_receive :validation_pressure_barrier
    refute_receive :validation_under_pressure
    assert {:ok, after_pause} = IndexRegistry.build_status(registry, index.build_id)
    assert after_pause.validation_checkpoints == before.validation_checkpoints
  end

  test "does not complete validation while staging cleanup is in progress or blocked" do
    {ctx, registry, worker, data_dir} = test_context("cleanup")
    on_exit(fn -> File.rm_rf!(data_dir) end)
    {:ok, _registry_pid} = start_registry(ctx, registry)

    cleanup_calls = :counters.new(1, [])

    {:ok, _worker_pid} =
      start_worker(ctx, registry, worker,
        barrier_fun: fn _ctx, _shard -> :ok end,
        snapshot_page_fun: fn _ctx, _shard, _build_id, _items, _bytes ->
          {:ok, %{done?: true, scanned_keys: 0, staged_states: 0}}
        end,
        page_fun: fn _ctx, _shard, _build_id, _cursor, _items, _bytes, _opts ->
          {:ok,
           %{
             records: [],
             cursor: "",
             done?: true,
             scanned_entries: 0,
             hydrated_bytes: 0
           }}
        end,
        project_fun: fn _ctx, _shard, _records, _definitions ->
          {:ok, %{projected_records: 0, written_entries: 0, write_ops: 0, written_bytes: 0}}
        end,
        validation_fun: fn _ctx, _shard, _build, definitions, checkpoint, _items, _bytes ->
          {:ok, advance_validation(checkpoint, length(definitions))}
        end,
        cleanup_fun: fn _ctx, _shard, _build_id ->
          :counters.add(cleanup_calls, 1, 1)

          case :counters.get(cleanup_calls, 1) do
            1 -> {:ok, :progress}
            _later -> {:error, :cleanup_blocked}
          end
        end
      )

    assert {:ok, :build_fenced} = IndexLifecycleWorker.run_once(worker)
    assert {:ok, :snapshot_complete} = IndexLifecycleWorker.run_once(worker)
    assert {:ok, :shard_complete} = IndexLifecycleWorker.run_once(worker)
    assert {:ok, :validation_fenced} = IndexLifecycleWorker.run_once(worker)

    assert {:ok, %{indexes: indexes}} = IndexRegistry.snapshot(ctx, 0)

    for _step <- 1..(length(indexes) * 2 + 1) do
      assert {:ok, :validation_progress} = IndexLifecycleWorker.run_once(worker)
    end

    assert {:ok, :validation_cleanup_progress} = IndexLifecycleWorker.run_once(worker)
    assert {:error, :cleanup_blocked} = IndexLifecycleWorker.run_once(worker)

    assert {:ok, %{indexes: [index | _]}} = IndexRegistry.snapshot(ctx, 0)

    assert {:ok, %{entries: statuses, validation_checkpoints: checkpoints}} =
             IndexRegistry.build_status(registry, index.build_id)

    assert Enum.all?(statuses, &(&1.state == :validating))
    assert checkpoints[0].phase == :cleanup
  end

  test "validation mismatch stops candidate projection and records bounded rollback evidence" do
    {ctx, registry, worker, data_dir} = test_context("rollback")
    on_exit(fn -> File.rm_rf!(data_dir) end)
    {:ok, _registry_pid} = start_registry(ctx, registry)
    {:ok, %{indexes: [index | _]}} = IndexRegistry.snapshot(ctx, 0)
    complete_build!(registry, index.build_id)

    {:ok, _worker_pid} =
      start_worker(ctx, registry, worker,
        barrier_fun: fn _ctx, _shard -> :ok end,
        validation_fun: fn _ctx, _shard, _build, _definitions, _checkpoint, _items, _bytes ->
          {:mismatch,
           %{
             checked_records: 12,
             checked_entries: 15,
             mismatches: 1,
             reason: :entry_value_mismatch
           }}
        end
      )

    assert {:ok, :validation_fenced} = IndexLifecycleWorker.run_once(worker)
    assert {:ok, :validation_failed} = IndexLifecycleWorker.run_once(worker)

    assert {:ok, %{entries: statuses}} = IndexRegistry.build_status(registry, index.build_id)
    assert Enum.all?(statuses, &(&1.state == :failed))
    assert Enum.all?(statuses, &(&1.validation.mismatches == 1))
    assert {:ok, []} = FerricStore.Flow.QueryIndexProvider.projection_definitions(ctx, 0)
  end

  test "restarts validation from a durable fence after concurrent fanout growth" do
    {ctx, registry, worker, data_dir} = test_context("validation_restart")
    on_exit(fn -> File.rm_rf!(data_dir) end)
    {:ok, _registry_pid} = start_registry(ctx, registry)
    {:ok, %{indexes: [index | _]}} = IndexRegistry.snapshot(ctx, 0)
    complete_build!(registry, index.build_id)
    parent = self()

    validation = fn _ctx, 0, _build_id, definitions, checkpoint, _items, _bytes ->
      send(parent, {:validation_phase, checkpoint.phase})

      case checkpoint.phase do
        :source -> {:ok, advance_validation(checkpoint, length(definitions))}
        :index -> {:restart, :query_index_validation_concurrent_change}
      end
    end

    {:ok, _worker_pid} =
      start_worker(ctx, registry, worker,
        barrier_fun: fn _ctx, 0 ->
          send(parent, :validation_barrier)
          :ok
        end,
        validation_fun: validation
      )

    assert {:ok, :validation_fenced} = IndexLifecycleWorker.run_once(worker)
    assert_receive :validation_barrier
    assert {:ok, :validation_progress} = IndexLifecycleWorker.run_once(worker)
    assert_receive {:validation_phase, :source}
    assert {:ok, :validation_restarted} = IndexLifecycleWorker.run_once(worker)
    assert_receive {:validation_phase, :index}

    assert {:ok, %{validation_checkpoints: %{0 => checkpoint}}} =
             IndexRegistry.build_status(registry, index.build_id)

    assert checkpoint.phase == :source
    refute checkpoint.fenced
    assert checkpoint.checked_records == 0
    assert checkpoint.checked_entries == 0

    assert {:ok, :validation_fenced} = IndexLifecycleWorker.run_once(worker)
    assert_receive :validation_barrier
  end

  test "fences failed projection before resumable physical cleanup" do
    {ctx, registry, worker, data_dir} = test_context("retirement")
    on_exit(fn -> File.rm_rf!(data_dir) end)
    {:ok, registry_pid} = start_registry(ctx, registry)
    {:ok, %{indexes: [index | _]}} = IndexRegistry.snapshot(ctx, 0)
    complete_build!(registry, index.build_id)

    assert :ok =
             IndexRegistry.validation_failed(registry, index.build_id,
               checked_records: 1,
               checked_entries: 1,
               mismatches: 1,
               reason: :missing_index_entry
             )

    parent = self()
    cleanup_calls = :counters.new(1, [])

    retirement = fn _ctx, 0, definition, checkpoint, _items, _bytes ->
      send(parent, {:retire, definition.id, checkpoint.phase})

      case checkpoint.phase do
        :index -> {:ok, %{checkpoint | phase: :reverse, deleted_entries: 2}}
        :reverse -> {:complete, %{checkpoint | rewritten_reverse_rows: 1}}
      end
    end

    {:ok, worker_pid} =
      start_worker(ctx, registry, worker,
        barrier_fun: fn _ctx, 0 ->
          send(parent, :retirement_barrier)
          :ok
        end,
        retirement_fun: retirement,
        cleanup_fun: fn _ctx, 0, build_id ->
          send(parent, {:retirement_cleanup, build_id})
          :counters.add(cleanup_calls, 1, 1)

          if :counters.get(cleanup_calls, 1) == 1,
            do: {:ok, :progress},
            else: :ok
        end
      )

    assert {:ok, :retirement_fenced} = IndexLifecycleWorker.run_once(worker)
    assert_receive :retirement_barrier
    assert {:ok, :retirement_progress} = IndexLifecycleWorker.run_once(worker)
    assert_receive {:retire, _, :index}
    assert {:ok, :retirement_cleanup_pending} = IndexLifecycleWorker.run_once(worker)
    assert_receive {:retire, _, :reverse}
    refute_receive {:retirement_cleanup, _}

    GenServer.stop(worker_pid)
    GenServer.stop(registry_pid)
    {:ok, _resumed_registry_pid} = start_registry(ctx, registry)

    {:ok, _resumed_worker_pid} =
      start_worker(ctx, registry, worker,
        retirement_fun: fn _ctx, _shard, _definition, _checkpoint, _items, _bytes ->
          raise "retirement deletion must not restart after cleanup was checkpointed"
        end,
        cleanup_fun: fn _ctx, 0, build_id ->
          send(parent, {:retirement_cleanup, build_id})
          :counters.add(cleanup_calls, 1, 1)

          if :counters.get(cleanup_calls, 1) == 1,
            do: {:ok, :progress},
            else: :ok
        end
      )

    assert {:ok, :retirement_cleanup_progress} = IndexLifecycleWorker.run_once(worker)
    assert_receive {:retirement_cleanup, _}

    assert {:ok, %{retirement: %{status: :pending, checkpoints: %{0 => checkpoint}}}} =
             IndexRegistry.status(registry, index.definition.id, index.definition.version)

    assert checkpoint.phase == :cleanup

    assert {:ok, :retirement_shard_complete} = IndexLifecycleWorker.run_once(worker)
    assert_receive {:retirement_cleanup, _}

    assert {:ok, %{retirement: %{status: :complete}}} =
             IndexRegistry.status(registry, index.definition.id, index.definition.version)
  end

  test "continues retirement cleanup under operational pressure" do
    {ctx, registry, worker, data_dir} = test_context("pressure_retirement")
    on_exit(fn -> File.rm_rf!(data_dir) end)
    {:ok, _registry_pid} = start_registry(ctx, registry)
    {:ok, %{indexes: [index | _]}} = IndexRegistry.snapshot(ctx, 0)
    complete_build!(registry, index.build_id)

    assert :ok =
             IndexRegistry.validation_failed(registry, index.build_id,
               checked_records: 1,
               checked_entries: 1,
               mismatches: 1,
               reason: :missing_index_entry
             )

    parent = self()

    {:ok, _worker_pid} =
      start_worker(ctx, registry, worker,
        pressure_fun: fn -> true end,
        barrier_fun: fn _ctx, 0 ->
          send(parent, :pressure_retirement_barrier)
          :ok
        end
      )

    assert {:ok, :retirement_fenced} = IndexLifecycleWorker.run_once(worker)
    assert_receive :pressure_retirement_barrier
  end

  test "waits for admitted queries before fencing retirement" do
    {ctx, registry, worker, data_dir} = test_context("retirement_drain")
    on_exit(fn -> File.rm_rf!(data_dir) end)
    {:ok, _registry_pid} = start_registry(ctx, registry)
    {:ok, %{indexes: [index | _]}} = IndexRegistry.snapshot(ctx, 0)
    complete_build!(registry, index.build_id)

    assert :ok =
             IndexRegistry.validation_failed(registry, index.build_id,
               checked_records: 1,
               checked_entries: 1,
               mismatches: 1,
               reason: :missing_index_entry
             )

    parent = self()
    drain_calls = :counters.new(1, [])

    {:ok, _worker_pid} =
      start_worker(ctx, registry, worker,
        drain_fun: fn received_ctx, received_index ->
          send(parent, {:drain, received_ctx.name, received_index.definition.id})
          :counters.add(drain_calls, 1, 1)
          :counters.get(drain_calls, 1) > 1
        end,
        barrier_fun: fn _ctx, _shard ->
          send(parent, :retirement_barrier)
          :ok
        end
      )

    assert {:ok, :retirement_waiting_for_queries} = IndexLifecycleWorker.run_once(worker)
    assert_receive {:drain, _, _}
    refute_receive :retirement_barrier

    assert {:ok, :retirement_fenced} = IndexLifecycleWorker.run_once(worker)
    assert_receive {:drain, _, _}
    assert_receive :retirement_barrier
  end

  test "default retirement drain waits only for queries pinned to that index" do
    {base_ctx, registry, worker, data_dir} = test_context("retirement_index_drain")
    on_exit(fn -> File.rm_rf!(data_dir) end)

    admission = :"#{base_ctx.name}.QueryAdmission"
    ctx = Map.put(base_ctx, :query_admission_controller, admission)
    catalog_path = Path.join(data_dir, "retirement-catalog.json")

    write_catalog!(catalog_path, 1, [
      catalog_index("retiring-index", 1),
      catalog_index("unrelated-index", 1)
    ])

    {:ok, admission_pid} =
      AdmissionController.start_link(
        name: admission,
        max_scope: 4,
        max_node: 4,
        orphan_grace_ms: 0
      )

    Process.unlink(admission_pid)

    {:ok, registry_pid} =
      IndexRegistry.start_link(instance_ctx: ctx, name: registry, catalog_path: catalog_path)

    Process.unlink(registry_pid)
    {:ok, %{indexes: indexes}} = IndexRegistry.snapshot(ctx, 0)
    activate_build!(registry, hd(indexes).build_id)

    {:ok, %{indexes: active_indexes}} = IndexRegistry.snapshot(ctx, 0)
    index = Enum.find(active_indexes, &(&1.definition.id == "retiring-index"))
    unrelated_index = Enum.find(active_indexes, &(&1.definition.id == "unrelated-index"))

    identity = {index.definition.id, index.definition.version, index.build_id}

    unrelated_identity = {
      unrelated_index.definition.id,
      unrelated_index.definition.version,
      unrelated_index.build_id
    }

    assert {:ok, retiring_lease} = AdmissionController.acquire(admission, ctx, "retiring")
    assert :ok = AdmissionController.pin_index(admission, retiring_lease, ctx, identity)
    assert {:ok, unrelated_lease} = AdmissionController.acquire(admission, ctx, "unrelated")

    assert :ok =
             AdmissionController.pin_index(
               admission,
               unrelated_lease,
               ctx,
               unrelated_identity
             )

    GenServer.stop(registry_pid)
    write_catalog!(catalog_path, 2, [catalog_index("unrelated-index", 1)])

    {:ok, resumed_registry_pid} =
      IndexRegistry.start_link(instance_ctx: ctx, name: registry, catalog_path: catalog_path)

    Process.unlink(resumed_registry_pid)

    assert {:ok, %{indexes: resumed_indexes}} = IndexRegistry.snapshot(ctx, 0)
    assert Enum.find(resumed_indexes, &(&1.definition.id == "retiring-index")).state == :retiring
    assert Enum.find(resumed_indexes, &(&1.definition.id == "unrelated-index")).state == :active

    parent = self()

    {:ok, worker_pid} =
      IndexLifecycleWorker.start_link(
        name: worker,
        instance_ctx: ctx,
        registry: registry,
        auto_run?: false,
        ready_fun: fn _ctx, _shard -> true end,
        barrier_fun: fn _ctx, _shard ->
          send(parent, :index_retirement_barrier)
          :ok
        end,
        retirement_fun: fn _ctx, _shard, _definition, checkpoint, _items, _bytes ->
          case checkpoint.phase do
            :index -> {:ok, %{checkpoint | phase: :reverse}}
            :reverse -> {:complete, checkpoint}
          end
        end,
        cleanup_fun: fn _ctx, _shard, _build_id -> :ok end
      )

    Process.unlink(worker_pid)

    assert {:ok, :retirement_waiting_for_queries} = IndexLifecycleWorker.run_once(worker)

    assert {:error, :query_index_retired} =
             AdmissionController.pin_index(admission, unrelated_lease, ctx, identity)

    refute_receive :index_retirement_barrier
    assert :ok = AdmissionController.release(admission, retiring_lease)

    assert {:ok, :retirement_fenced} = IndexLifecycleWorker.run_once(worker)
    assert_receive :index_retirement_barrier
    assert {:ok, false} = AdmissionController.drained?(admission, ctx)
    assert :ok = AdmissionController.release(admission, unrelated_lease)

    assert {:ok, :retirement_progress} = IndexLifecycleWorker.run_once(worker)
    assert {:ok, :retirement_cleanup_pending} = IndexLifecycleWorker.run_once(worker)
    assert {:ok, :retirement_shard_complete} = IndexLifecycleWorker.run_once(worker)

    assert {:ok, replacement_lease} = AdmissionController.acquire(admission, ctx, "replacement")

    assert {:error, :query_index_retired} =
             AdmissionController.pin_index(admission, replacement_lease, ctx, identity)

    assert :ok = AdmissionController.release(admission, replacement_lease)
  end

  test "contains callback exceptions so the worker can retry" do
    {ctx, registry, worker, data_dir} = test_context("exception")
    on_exit(fn -> File.rm_rf!(data_dir) end)
    {:ok, _registry_pid} = start_registry(ctx, registry)

    {:ok, worker_pid} =
      start_worker(ctx, registry, worker,
        barrier_fun: fn _ctx, _shard -> :ok end,
        snapshot_page_fun: fn _ctx, _shard, _build_id, _items, _bytes ->
          raise "unexpected source failure"
        end
      )

    assert {:ok, :build_fenced} = IndexLifecycleWorker.run_once(worker)

    assert {:error, :query_index_lifecycle_callback_failed} =
             IndexLifecycleWorker.run_once(worker)

    assert Process.alive?(worker_pid)
  end

  test "rejects a non-advancing backfill cursor before repeating projection writes" do
    {ctx, registry, worker, data_dir} = test_context("cursor")
    on_exit(fn -> File.rm_rf!(data_dir) end)
    {:ok, _registry_pid} = start_registry(ctx, registry)
    parent = self()

    {:ok, _worker_pid} =
      start_worker(ctx, registry, worker,
        barrier_fun: fn _ctx, _shard -> :ok end,
        snapshot_page_fun: fn _ctx, _shard, _build_id, _items, _bytes ->
          {:ok, %{done?: true, scanned_keys: 1, staged_states: 1}}
        end,
        page_fun: fn _ctx, _shard, _build_id, _cursor, _items, _bytes, _opts ->
          {:ok,
           %{
             records: [],
             cursor: "cursor-a",
             done?: false,
             scanned_entries: 1,
             hydrated_bytes: 0
           }}
        end,
        project_fun: fn _ctx, _shard, _records, _definitions ->
          send(parent, :project)
          {:ok, %{projected_records: 0, written_entries: 0, write_ops: 0, written_bytes: 0}}
        end
      )

    assert {:ok, :build_fenced} = IndexLifecycleWorker.run_once(worker)
    assert {:ok, :snapshot_complete} = IndexLifecycleWorker.run_once(worker)
    assert {:ok, :backfill_progress} = IndexLifecycleWorker.run_once(worker)
    assert_receive :project

    assert {:error, :query_backfill_made_no_progress} = IndexLifecycleWorker.run_once(worker)
    refute_receive :project
  end

  test "rejects a snapshot callback that reports no progress" do
    {ctx, registry, worker, data_dir} = test_context("snapshot_stall")
    on_exit(fn -> File.rm_rf!(data_dir) end)
    {:ok, _registry_pid} = start_registry(ctx, registry)

    {:ok, _worker_pid} =
      start_worker(ctx, registry, worker,
        barrier_fun: fn _ctx, _shard -> :ok end,
        snapshot_page_fun: fn _ctx, _shard, _build_id, _items, _bytes ->
          {:ok, %{done?: false, scanned_keys: 0, staged_states: 0}}
        end
      )

    assert {:ok, :build_fenced} = IndexLifecycleWorker.run_once(worker)

    assert {:error, :query_backfill_snapshot_made_no_progress} =
             IndexLifecycleWorker.run_once(worker)
  end

  test "rejects projection metrics that do not match the accepted page" do
    {ctx, registry, worker, data_dir} = test_context("projection_metrics")
    on_exit(fn -> File.rm_rf!(data_dir) end)
    {:ok, _registry_pid} = start_registry(ctx, registry)

    {:ok, _worker_pid} =
      start_worker(ctx, registry, worker,
        barrier_fun: fn _ctx, _shard -> :ok end,
        snapshot_page_fun: fn _ctx, _shard, _build_id, _items, _bytes ->
          {:ok, %{done?: true, scanned_keys: 1, staged_states: 1}}
        end,
        page_fun: fn _ctx, _shard, _build_id, _cursor, _items, _bytes, _opts ->
          {:ok,
           %{
             records: [%{state_key: "state", record: nil, expire_at_ms: 0}],
             cursor: "",
             done?: true,
             scanned_entries: 1,
             hydrated_bytes: 0
           }}
        end,
        project_fun: fn _ctx, _shard, _records, _definitions ->
          {:ok, %{projected_records: 0, written_entries: 10_000, write_ops: 0, written_bytes: 0}}
        end
      )

    assert {:ok, :build_fenced} = IndexLifecycleWorker.run_once(worker)
    assert {:ok, :snapshot_complete} = IndexLifecycleWorker.run_once(worker)
    assert {:error, :invalid_query_backfill_metrics} = IndexLifecycleWorker.run_once(worker)

    assert {:ok, %{indexes: [index | _]}} = IndexRegistry.snapshot(ctx, 0)

    assert {:ok, %{checkpoints: %{0 => checkpoint}}} =
             IndexRegistry.build_status(registry, index.build_id)

    assert checkpoint.phase == :backfill
    assert checkpoint.cursor == ""
  end

  test "runs independent shard lifecycle steps with bounded concurrency" do
    {ctx, registry, worker, data_dir} = test_context("parallel_shards")
    ctx = %{ctx | shard_count: 2}
    on_exit(fn -> File.rm_rf!(data_dir) end)
    {:ok, _registry_pid} = start_registry(ctx, registry)
    parent = self()

    barrier = fn _ctx, shard_index ->
      send(parent, {:barrier_entered, shard_index, self()})

      receive do
        :release -> :ok
      after
        1_000 -> {:error, :parallel_shard_timeout}
      end
    end

    {:ok, _worker_pid} =
      start_worker(ctx, registry, worker,
        shard_concurrency: 2,
        barrier_fun: barrier
      )

    runner = Task.async(fn -> IndexLifecycleWorker.run_once(worker) end)

    entered =
      for _shard <- 1..2 do
        assert_receive {:barrier_entered, shard_index, callback}, 500
        {shard_index, callback}
      end

    assert entered |> Enum.map(&elem(&1, 0)) |> Enum.sort() == [0, 1]
    Enum.each(entered, fn {_shard, callback} -> send(callback, :release) end)
    assert {:ok, :build_fenced} = Task.await(runner, 2_000)
  end

  test "skips unavailable shards without starving later ready shards" do
    {ctx, registry, worker, data_dir} = test_context("ready_shard_selection")
    ctx = %{ctx | shard_count: 2}
    on_exit(fn -> File.rm_rf!(data_dir) end)
    {:ok, _registry_pid} = start_registry(ctx, registry)
    parent = self()

    {:ok, _worker_pid} =
      start_worker(ctx, registry, worker,
        shard_concurrency: 1,
        ready_fun: fn _ctx, shard_index -> shard_index == 1 end,
        barrier_fun: fn _ctx, shard_index ->
          send(parent, {:barrier, shard_index})
          :ok
        end
      )

    assert {:ok, :build_fenced} = IndexLifecycleWorker.run_once(worker)
    assert_receive {:barrier, 1}
    refute_receive {:barrier, 0}

    assert {:ok, %{indexes: [index | _]}} = IndexRegistry.snapshot(ctx, 0)

    assert {:ok, %{checkpoints: checkpoints}} =
             IndexRegistry.build_status(registry, index.build_id)

    assert checkpoints[1].fenced
    refute Map.has_key?(checkpoints, 0)
  end

  defp advance_validation(%{phase: :source} = checkpoint, _definition_count),
    do: %{checkpoint | phase: :index, cursor: "", definition_position: 0}

  defp advance_validation(
         %{phase: :index, definition_position: position} = checkpoint,
         definition_count
       )
       when position + 1 < definition_count,
       do: %{checkpoint | cursor: "", definition_position: position + 1}

  defp advance_validation(%{phase: :index} = checkpoint, _definition_count),
    do: %{checkpoint | phase: :counter, cursor: "", definition_position: 0, counter_runs: []}

  defp advance_validation(
         %{phase: :counter, definition_position: position} = checkpoint,
         definition_count
       )
       when position + 1 < definition_count,
       do: %{checkpoint | cursor: "", definition_position: position + 1}

  defp advance_validation(%{phase: :counter} = checkpoint, definition_count),
    do: %{checkpoint | phase: :cleanup, cursor: "", definition_position: definition_count}

  defp start_registry(ctx, registry) do
    {:ok, pid} = IndexRegistry.start_link(instance_ctx: ctx, name: registry)
    Process.unlink(pid)
    {:ok, pid}
  end

  defp start_worker(ctx, registry, worker, opts) do
    defaults = [
      name: worker,
      instance_ctx: ctx,
      registry: registry,
      auto_run?: false,
      pressure_fun: fn -> false end,
      ready_fun: fn _ctx, _shard -> true end,
      drain_fun: fn _ctx, _index -> true end
    ]

    {:ok, pid} = IndexLifecycleWorker.start_link(Keyword.merge(defaults, opts))
    Process.unlink(pid)
    {:ok, pid}
  end

  defp complete_build!(registry, build_id) do
    assert :ok =
             IndexRegistry.checkpoint_build(registry, build_id, 0,
               phase: :snapshot,
               cursor: "",
               fenced: true,
               scanned_records: 0,
               written_entries: 0,
               written_bytes: 0
             )

    assert :ok =
             IndexRegistry.checkpoint_build(registry, build_id, 0,
               phase: :backfill,
               cursor: "",
               fenced: true,
               scanned_records: 0,
               written_entries: 0,
               written_bytes: 0
             )

    assert :ok = IndexRegistry.complete_build_shard(registry, build_id, 0)
  end

  defp activate_build!(registry, build_id) do
    complete_build!(registry, build_id)

    assert {:ok, %{entries: entries}} = IndexRegistry.build_status(registry, build_id)
    definition_count = length(entries)

    checkpoint_validation!(registry, build_id, :source, 0)

    Enum.each(0..(definition_count - 1), fn position ->
      checkpoint_validation!(registry, build_id, :index, position)
    end)

    Enum.each(0..(definition_count - 1), fn position ->
      checkpoint_validation!(registry, build_id, :counter, position)
    end)

    checkpoint_validation!(registry, build_id, :cleanup, definition_count)
    assert :ok = IndexRegistry.complete_validation_shard(registry, build_id, 0)
    assert :ok = IndexRegistry.activate_build(registry, build_id)
  end

  defp checkpoint_validation!(registry, build_id, phase, definition_position) do
    assert :ok =
             IndexRegistry.checkpoint_validation(registry, build_id, 0,
               phase: phase,
               cursor: "",
               fenced: true,
               definition_position: definition_position,
               checked_records: 0,
               checked_entries: 0,
               mismatches: 0
             )
  end

  defp catalog_index(id, version) do
    %{
      "id" => id,
      "version" => version,
      "source" => "runs",
      "workloads" => ["WF-LIST-001"],
      "fields" => [
        %{"name" => "partition_key", "direction" => "asc", "encoding" => "hashed"},
        %{"name" => "updated_at_ms", "direction" => "desc", "encoding" => "ordered"}
      ]
    }
  end

  defp write_catalog!(path, version, indexes) do
    File.mkdir_p!(Path.dirname(path))

    File.write!(
      path,
      Jason.encode!(%{
        "catalog_version" => version,
        "contract_version" => "ferric.flow.query.index-catalog/v1",
        "indexes" => indexes
      })
    )
  end

  defp test_context(label) do
    suffix = System.unique_integer([:positive, :monotonic])
    data_dir = Path.join(System.tmp_dir!(), "ferricstore_query_worker_#{label}_#{suffix}")

    {:ok, metadata_snapshot} =
      FerricStore.Flow.MetadataExtension.configure(
        FerricStore.Flow.MetadataExtension.Disabled,
        []
      )

    ctx = %{
      name: :"query_worker_#{label}_instance_#{suffix}",
      data_dir: data_dir,
      shard_count: 1,
      query_index_provider: IndexProvider,
      flow_metadata_snapshot: metadata_snapshot
    }

    {ctx, :"query_worker_#{label}_registry_#{suffix}", :"query_worker_#{label}_#{suffix}",
     data_dir}
  end
end
