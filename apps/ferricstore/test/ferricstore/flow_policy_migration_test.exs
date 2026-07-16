defmodule Ferricstore.FlowPolicyMigrationTest do
  use Ferricstore.Test.FlowCase

  alias Ferricstore.Flow.Keys
  alias Ferricstore.Flow.LMDB
  alias Ferricstore.Flow.LMDBRebuilder
  alias Ferricstore.Flow.NativeOrderedIndex
  alias Ferricstore.Flow.PolicyMigration
  alias Ferricstore.Flow.PolicyMigrationWorker
  alias Ferricstore.Flow.RetryPolicy
  alias Ferricstore.Store.Router

  @partition "policy-migration-partition"

  defmodule EmbeddedPolicyMigration do
    use FerricStore, shard_count: 1
  end

  test "catalog members store the exact type once per shard instead of once per flow" do
    type = :binary.copy("large-type", 8_000)
    state_key = Keys.state_key("compact-catalog", @partition)
    descriptor = PolicyMigration.encode_type_descriptor(type, 17)
    catalog = PolicyMigration.encode_catalog(type, state_key, 3)

    assert byte_size(catalog) < byte_size(type)

    assert {:ok, %{type: ^type, membership_revision: 17}} =
             PolicyMigration.decode_type_descriptor(descriptor)

    assert {:ok, %{state_key: ^state_key, migration_generation: 3}} =
             PolicyMigration.decode_catalog(catalog)
  end

  test "catalog projection keys sort by generation and retain exact catalog ownership" do
    type = unique_flow_id("projection-key")
    first = Keys.type_catalog_member_key(type, Keys.state_key("a", @partition))
    second = Keys.type_catalog_member_key(type, Keys.state_key("b", @partition))

    generation_one = Keys.policy_catalog_projection_key(type, first, 1)
    generation_two = Keys.policy_catalog_projection_key(type, second, 2)

    assert generation_one < generation_two

    assert {:ok, %{catalog_key: ^first, migration_generation: 1}} =
             Keys.decode_policy_catalog_projection_key(type, generation_one)

    assert :error = Keys.decode_policy_catalog_projection_key(type <> "-other", generation_one)
  end

  test "work cursors reject an after-key outside their staged run" do
    assert PolicyMigration.work_cursor?(PolicyMigration.work_cursor("run"), "run")
    refute PolicyMigration.work_cursor?(<<1, 3, "run", "foreign-key">>, "run")
  end

  test "batch state writes keep catalog generation current and lifetime aligned with state" do
    ctx = FerricStore.Instance.get(:default)

    type = unique_flow_id("batch-catalog-expiry")
    partition = unique_flow_id("batch-catalog-expiry-partition")
    ids = Enum.map(1..2, &unique_flow_id("batch-catalog-expiry-#{&1}"))

    assert :ok =
             FerricStore.flow_create_many(partition, ids,
               type: type,
               state: "queued",
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert {:ok, _policy} = FerricStore.flow_policy_set(type, indexed_attributes: ["owner"])
    assert :ok = Ferricstore.Store.BitcaskWriter.flush_all(ctx.shard_count)

    catalog_rows =
      Enum.map(ids, fn id ->
        state_key = Keys.state_key(id, partition)
        shard_index = Router.shard_for(ctx, state_key)
        keydir = elem(ctx.keydir_refs, shard_index)
        catalog_key = Keys.type_catalog_member_key(type, state_key)

        assert [{^catalog_key, value, _expire_at_ms, lfu, location, offset, size}] =
                 :ets.lookup(keydir, catalog_key)

        true =
          :ets.insert(
            keydir,
            {catalog_key, value, System.system_time(:millisecond) + 60_000, lfu, location, offset,
             size}
          )

        {catalog_key, keydir}
      end)

    assert {:ok, claims} =
             FerricStore.flow_claim_due(type,
               partition_key: partition,
               worker: "batch-catalog-expiry-worker",
               limit: 2,
               now_ms: 1_000
             )

    Enum.each(catalog_rows, fn {catalog_key, keydir} ->
      assert [{^catalog_key, catalog_value, 0, _, _, _, _}] = :ets.lookup(keydir, catalog_key)

      assert {:ok, %{migration_generation: 1}} =
               PolicyMigration.decode_catalog(catalog_value)
    end)

    items =
      Enum.map(claims, fn claim ->
        %{
          id: claim.id,
          lease_token: claim.lease_token,
          fencing_token: claim.fencing_token
        }
      end)

    assert :ok = FerricStore.flow_complete_many(partition, items, now_ms: 2_000)

    completed =
      Enum.map(ids, fn id ->
        assert {:ok, record} = FerricStore.flow_get(id, partition_key: partition)
        record
      end)

    Enum.each(completed, fn record ->
      state_key = Keys.state_key(record.id, partition)
      shard_index = Router.shard_for(ctx, state_key)
      keydir = elem(ctx.keydir_refs, shard_index)
      catalog_key = Keys.type_catalog_member_key(type, state_key)

      assert [{^catalog_key, _value, expire_at_ms, _, _, _, _}] =
               :ets.lookup(keydir, catalog_key)

      assert record.terminal_retention_until_ms > 2_000
      assert expire_at_ms == 0
    end)
  end

  test "terminal catalog survives retention TTL until explicit cleanup and later migration" do
    ctx = FerricStore.Instance.get(:default)
    type = unique_flow_id("terminal-catalog-lifetime")
    id = unique_flow_id("terminal-catalog-lifetime-flow")
    now_ms = System.system_time(:millisecond)
    state_key = Keys.state_key(id, @partition)
    shard_index = Router.shard_for(ctx, state_key)
    keydir = elem(ctx.keydir_refs, shard_index)
    catalog_key = Keys.type_catalog_member_key(type, state_key)
    lmdb_path = flow_lmdb_path(ctx, shard_index)

    assert :ok =
             FerricStore.flow_create(id,
               type: type,
               state: "queued",
               partition_key: @partition,
               state_meta: %{"version" => 1},
               retention_ttl_ms: 5,
               run_at_ms: now_ms,
               now_ms: now_ms
             )

    assert {:ok, [claim]} =
             FerricStore.flow_claim_due(type,
               partition_key: @partition,
               worker: "terminal-catalog-lifetime-worker",
               limit: 1,
               now_ms: now_ms
             )

    assert :ok =
             FerricStore.flow_complete(id, claim.lease_token,
               fencing_token: claim.fencing_token,
               partition_key: @partition,
               now_ms: now_ms + 1
             )

    assert [{^catalog_key, _catalog, 0, _, _, _, _}] = :ets.lookup(keydir, catalog_key)
    Process.sleep(10)
    assert [{^catalog_key, _catalog, 0, _, _, _, _}] = :ets.lookup(keydir, catalog_key)

    :ok = Ferricstore.Flow.LMDBWriter.flush(ctx.name, shard_index, 30_000)

    :ok =
      LMDB.write_batch(lmdb_path, [
        {:delete, Keys.policy_catalog_projection_key(type, catalog_key, 0)}
      ])

    :ok = PolicyMigration.rotate_source_token(lmdb_path)
    complete_catalog_backfill(ctx, shard_index, 100)

    assert {:ok, _policy} = FerricStore.flow_policy_set(type, indexed_state_meta: "version")
    assert_migration_done(ctx, shard_index, 32, 10)

    assert [{^catalog_key, catalog_value, 0, _, _, _, _}] = :ets.lookup(keydir, catalog_key)
    assert {:ok, %{migration_generation: 1}} = PolicyMigration.decode_catalog(catalog_value)
  end

  test "policy changes are independent of unrelated shard keydir cardinality" do
    ctx = FerricStore.Instance.get(:default)
    worker = suspend_policy_migration_worker(ctx)
    keydir = elem(ctx.keydir_refs, 0)
    prefix = "policy-migration-noise:#{System.unique_integer([:positive])}:"

    noise =
      Enum.map(1..10_001, fn index ->
        {prefix <> Integer.to_string(index), <<>>, 0, nil, 0, 0, 0}
      end)

    true = :ets.insert(keydir, noise)

    on_exit(fn ->
      Enum.each(noise, fn {key, _, _, _, _, _, _} -> :ets.delete(keydir, key) end)
      if is_pid(worker) and Process.alive?(worker), do: :sys.resume(worker)
    end)

    type = unique_flow_id("unrelated-cardinality")
    complete_catalog_backfill_all(ctx)

    assert {:ok, %{indexed_state_meta: "version"}} =
             FerricStore.flow_policy_set(type, indexed_state_meta: "version")

    Enum.each(0..(ctx.shard_count - 1), fn shard_index ->
      assert {:ok, %{done?: true}} =
               Router.flow_policy_migration_step(ctx, shard_index, 1)
    end)
  end

  test "bounded policy migration resumes from durable progress after index restart" do
    ctx = FerricStore.Instance.get(:default)
    worker = suspend_policy_migration_worker(ctx)

    on_exit(fn ->
      if is_pid(worker) and Process.alive?(worker), do: :sys.resume(worker)
    end)

    type = unique_flow_id("resumable-policy")
    ids = Enum.map(1..3, &unique_flow_id("resumable-#{&1}"))

    Enum.each(ids, fn id ->
      assert :ok =
               FerricStore.flow_create(id,
                 type: type,
                 state: "accept",
                 partition_key: @partition,
                 state_meta: %{"version" => 1},
                 run_at_ms: 1_000,
                 now_ms: 1_000
               )
    end)

    complete_catalog_backfill_all(ctx)

    assert {:ok, %{indexed_state_meta: "version"}} =
             FerricStore.flow_policy_set(type, indexed_state_meta: "version")

    state_key = Keys.state_key(hd(ids), @partition)
    shard_index = Router.shard_for(ctx, state_key)

    assert {:ok, %{processed: 1, done?: false}} =
             Router.flow_policy_migration_step(ctx, shard_index, 1)

    {flow_index, flow_lookup} = NativeOrderedIndex.table_names(ctx.name, shard_index)
    NativeOrderedIndex.reset(flow_index, flow_lookup)
    assert_migration_done(ctx, shard_index, 1, 10)

    Enum.each(ids, fn id ->
      assert {:ok, %{indexed_state_meta: "version"}} =
               FerricStore.flow_get(id, partition_key: @partition)
    end)
  end

  test "exact-type catalog remains bounded above ten thousand members" do
    ctx = FerricStore.Instance.get(:default)
    worker = suspend_policy_migration_worker(ctx)
    type = unique_flow_id("large-exact-type") <> ":with:separator"
    generation = 0

    state_keys =
      Enum.map(1..10_001, fn index ->
        Keys.state_key("large-exact-type-#{index}", @partition)
      end)

    shard_index = Router.shard_for(ctx, hd(state_keys))
    keydir = elem(ctx.keydir_refs, shard_index)
    descriptor_key = Keys.type_catalog_descriptor_key(type)
    descriptor_value = PolicyMigration.encode_type_descriptor(type, 0)
    lmdb_path = flow_lmdb_path(ctx, shard_index)

    catalog_entries =
      Enum.map(state_keys, fn state_key ->
        catalog_key = Keys.type_catalog_member_key(type, state_key)
        value = PolicyMigration.encode_catalog(type, state_key, generation)
        {catalog_key, value, 0, nil, 0, 0, byte_size(value)}
      end)

    true = :ets.insert(keydir, catalog_entries)

    true =
      :ets.insert(
        keydir,
        {descriptor_key, descriptor_value, 0, nil, 0, 0, byte_size(descriptor_value)}
      )

    projection_ops =
      Enum.map(catalog_entries, fn {key, _, _, _, _, _, _} ->
        {:put, Keys.policy_catalog_projection_key(type, key, generation), <<1>>}
      end)

    :ok =
      LMDB.write_batch(lmdb_path, [
        {:put, descriptor_key, LMDB.encode_value(descriptor_value, 0)} | projection_ops
      ])

    complete_catalog_backfill_all(ctx)

    on_exit(fn ->
      Enum.each(catalog_entries, fn {key, _, _, _, _, _, _} -> :ets.delete(keydir, key) end)
      :ets.delete(keydir, descriptor_key)

      LMDB.write_batch(lmdb_path, [
        {:delete, descriptor_key}
        | Enum.map(projection_ops, fn {:put, key, _value} -> {:delete, key} end)
      ])

      if is_pid(worker) and Process.alive?(worker), do: :sys.resume(worker)
    end)

    assert {:ok, %{indexed_state_meta: "version"}} =
             FerricStore.flow_policy_set(type, indexed_state_meta: "version")

    assert {:ok, %{processed: 32, done?: false}} =
             Router.flow_policy_migration_step(ctx, shard_index, 32)

    assert :ok = Ferricstore.Flow.LMDBWriter.flush(ctx.name, shard_index, 30_000)

    assert {:ok, remaining} =
             LMDB.prefix_count(lmdb_path, Keys.policy_catalog_projection_prefix(type))

    assert remaining == 10_001 - 32

    Enum.each(catalog_entries, fn {key, _, _, _, _, _, _} -> :ets.delete(keydir, key) end)

    :ok =
      LMDB.write_batch(
        lmdb_path,
        Enum.map(projection_ops, fn {:put, key, _value} -> {:delete, key} end)
      )

    Enum.each(0..(ctx.shard_count - 1), fn index ->
      assert {:ok, %{done?: true}} = Router.flow_policy_migration_step(ctx, index, 1)
    end)
  end

  test "flush clears catalog rows and pending migration jobs" do
    ctx = FerricStore.Instance.get(:default)
    worker = suspend_policy_migration_worker(ctx)

    on_exit(fn ->
      if is_pid(worker) and Process.alive?(worker), do: :sys.resume(worker)
    end)

    type = unique_flow_id("flush-policy-catalog")
    id = unique_flow_id("flush-policy-catalog-record")
    state_key = Keys.state_key(id, @partition)
    catalog_key = Keys.type_catalog_member_key(type, state_key)
    job_key = Keys.policy_migration_job_key(type)
    shard_index = Router.shard_for(ctx, state_key)

    assert :ok =
             FerricStore.flow_create(id,
               type: type,
               state: "accept",
               partition_key: @partition,
               state_meta: %{"version" => 1},
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert {:ok, _policy} =
             FerricStore.flow_policy_set(type, indexed_state_meta: "version")

    assert [_entry] = :ets.lookup(elem(ctx.keydir_refs, shard_index), catalog_key)
    assert Enum.any?(Tuple.to_list(ctx.keydir_refs), &(:ets.lookup(&1, job_key) != []))

    assert :ok = ShardHelpers.flush_all_keys()

    assert Enum.all?(Tuple.to_list(ctx.keydir_refs), fn keydir ->
             :ets.lookup(keydir, catalog_key) == [] and :ets.lookup(keydir, job_key) == []
           end)

    assert {:ok, 0} =
             LMDB.prefix_count(
               flow_lmdb_path(ctx, shard_index),
               Keys.policy_catalog_projection_prefix(type)
             )
  end

  test "worker uses configured migration and backfill limits independently" do
    ctx = FerricStore.Instance.get(:default)
    name = :"policy_migration_worker_#{System.unique_integer([:positive])}"
    previous_enabled = Application.get_env(:ferricstore, :flow_policy_migration_worker_enabled)
    previous_batch = Application.get_env(:ferricstore, :flow_policy_migration_worker_batch_size)

    previous_backfill =
      Application.get_env(:ferricstore, :flow_policy_migration_worker_backfill_batch_size)

    Application.put_env(:ferricstore, :flow_policy_migration_worker_enabled, true)
    Application.put_env(:ferricstore, :flow_policy_migration_worker_batch_size, 7)
    Application.put_env(:ferricstore, :flow_policy_migration_worker_backfill_batch_size, 200)

    on_exit(fn ->
      restore_env(:flow_policy_migration_worker_enabled, previous_enabled)
      restore_env(:flow_policy_migration_worker_batch_size, previous_batch)
      restore_env(:flow_policy_migration_worker_backfill_batch_size, previous_backfill)
    end)

    assert {:ok, pid} =
             PolicyMigrationWorker.start_link(
               instance_ctx: ctx,
               name: name,
               initial_delay_ms: 60_000
             )

    assert %{batch_size: 7, backfill_batch_size: 200} = :sys.get_state(pid)
    GenServer.stop(pid)
  end

  test "worker polls shards in bounded round-robin sweeps" do
    parent = self()

    ctx = %{
      name: :policy_migration_round_robin_test,
      shard_count: 5
    }

    assert {:ok, pid} =
             PolicyMigrationWorker.start_link(
               instance_ctx: ctx,
               name: :bounded_round_robin_policy_migration_worker,
               enabled: true,
               initial_delay_ms: 60_000,
               interval_ms: 60_000,
               catchup_delay_ms: 60_000,
               shards_per_run: 2,
               attribute_repair_fun: fn _ctx, shard_index ->
                 send(parent, {:policy_migration_polled, shard_index})
                 {:ok, %{processed: 1}}
               end
             )

    on_exit(fn ->
      if Process.alive?(pid), do: GenServer.stop(pid)
    end)

    for expected_shards <- [[0, 1], [2, 3], [4], [0, 1]] do
      send(pid, :run)
      :sys.get_state(pid)

      assert Enum.map(expected_shards, fn shard_index ->
               assert_receive {:policy_migration_polled, ^shard_index}
               shard_index
             end) == expected_shards

      refute_receive {:policy_migration_polled, _other_shard}, 20
    end
  end

  test "explicit worker enabled option takes precedence over global configuration" do
    ctx = FerricStore.Instance.get(:default)
    previous_enabled = Application.get_env(:ferricstore, :flow_policy_migration_worker_enabled)
    Application.put_env(:ferricstore, :flow_policy_migration_worker_enabled, false)

    on_exit(fn -> restore_env(:flow_policy_migration_worker_enabled, previous_enabled) end)

    assert {:ok, pid} =
             PolicyMigrationWorker.start_link(
               instance_ctx: ctx,
               name: :"explicit_policy_migration_worker_#{System.unique_integer([:positive])}",
               enabled: true,
               initial_delay_ms: 60_000
             )

    GenServer.stop(pid)
  end

  test "worker reports a corrupt migration job projection instead of treating it as idle" do
    ctx = FerricStore.Instance.get(:default)
    complete_catalog_backfill_all(ctx)
    shard_index = 0
    type = unique_flow_id("corrupt-worker-job")
    job_key = Keys.policy_migration_job_key(type)
    lmdb_path = flow_lmdb_path(ctx, shard_index)
    :ok = LMDB.write_batch(lmdb_path, [{:put, job_key, <<"corrupt">>}])
    on_exit(fn -> LMDB.write_batch(lmdb_path, [{:delete, job_key}]) end)

    assert {:ok, pid} =
             PolicyMigrationWorker.start_link(
               instance_ctx: ctx,
               name: :"corrupt_policy_migration_worker_#{System.unique_integer([:positive])}",
               enabled: true,
               initial_delay_ms: 60_000,
               interval_ms: 60_000
             )

    log =
      ExUnit.CaptureLog.capture_log(fn ->
        send(pid, :run)
        :sys.get_state(pid)
      end)

    GenServer.stop(pid)
    assert log =~ "corrupt_policy_migration_job_projection"
  end

  test "job discovery fails closed when a committed job loses its mirror enqueue" do
    ctx = FerricStore.Instance.get(:default)
    type = unique_flow_id("lost-job-mirror")
    complete_catalog_backfill_all(ctx)

    assert {:ok, _policy} = FerricStore.flow_policy_set(type, indexed_state_meta: "version")
    job_key = Keys.policy_migration_job_key(type)
    shard_index = 0
    assert {:ok, primary_job} = Router.read_shard_value(ctx, shard_index, job_key)
    assert is_binary(primary_job)
    :ok = Ferricstore.Flow.LMDBWriter.flush(ctx.name, shard_index, 30_000)
    :ok = LMDB.write_batch(flow_lmdb_path(ctx, shard_index), [{:delete, job_key}])

    degraded = ctx.flow_lmdb_mirror_degraded
    :atomics.put(degraded, shard_index + 1, 1)
    on_exit(fn -> :atomics.put(degraded, shard_index + 1, 0) end)

    assert {:error, :flow_policy_migration_projection_degraded} =
             PolicyMigration.next_job(ctx, shard_index)
  end

  test "worker bounds keydir snapshots and stops its owned snapshot task" do
    ctx = FerricStore.Instance.get(:default)
    parent = self()

    Enum.each(0..(ctx.shard_count - 1), fn shard_index ->
      :ok = PolicyMigration.rotate_source_token(flow_lmdb_path(ctx, shard_index))
    end)

    assert {:ok, pid} =
             PolicyMigrationWorker.start_link(
               instance_ctx: ctx,
               name: :"bounded_policy_snapshot_worker_#{System.unique_integer([:positive])}",
               enabled: true,
               initial_delay_ms: 60_000,
               interval_ms: 60_000,
               catchup_delay_ms: 60_000,
               snapshot_fun: fn _ctx, shard_index, _run_token, _max_items, _max_bytes ->
                 send(parent, {:policy_snapshot_started, shard_index, self()})
                 receive do: (:release_policy_snapshot -> :ok)
               end
             )

    send(pid, :run)
    :sys.get_state(pid)
    send(pid, :run)
    :sys.get_state(pid)

    assert_receive {:policy_snapshot_started, _shard_index, snapshot_pid}, 1_000
    refute_receive {:policy_snapshot_started, _shard_index, _other_pid}, 100

    snapshot_ref = Process.monitor(snapshot_pid)
    GenServer.stop(pid)
    assert_receive {:DOWN, ^snapshot_ref, :process, ^snapshot_pid, :shutdown}, 1_000
  end

  test "worker ignores stale scheduled runs after timer replacement" do
    ctx = FerricStore.Instance.get(:default)

    assert {:ok, pid} =
             PolicyMigrationWorker.start_link(
               instance_ctx: ctx,
               name: :"coalesced_policy_timer_worker_#{System.unique_integer([:positive])}",
               enabled: true,
               initial_delay_ms: 60_000,
               interval_ms: 60_000,
               catchup_delay_ms: 60_000
             )

    initial = :sys.get_state(pid)
    assert is_reference(initial.run_timer_ref)
    assert is_reference(initial.run_timer_token)

    replacement_token = make_ref()
    replacement_ref = Process.send_after(pid, {:run, replacement_token}, 60_000)

    replaced =
      :sys.replace_state(pid, fn state ->
        Process.cancel_timer(state.run_timer_ref)
        %{state | run_timer_ref: replacement_ref, run_timer_token: replacement_token}
      end)

    send(pid, {:run, initial.run_timer_token})
    after_stale = :sys.get_state(pid)
    assert after_stale.run_timer_token == replaced.run_timer_token
    assert after_stale.run_timer_ref == replaced.run_timer_ref
    assert is_integer(Process.read_timer(after_stale.run_timer_ref))
    GenServer.stop(pid)
  end

  test "worker rebuilds missing local staging before resuming a replicated work cursor" do
    ctx = FerricStore.Instance.get(:default)
    parent = self()
    shard_index = 0
    {:ok, source_token} = PolicyMigration.source_token(ctx, shard_index)
    run_token = Base.url_encode64(:crypto.strong_rand_bytes(18), padding: false)

    :ok = PolicyMigration.snapshot_primary_keydir(ctx, shard_index, run_token, 256, 1_024 * 1_024)

    assert {:ok, %{done?: false}} =
             Router.flow_policy_catalog_backfill_step(ctx, shard_index, %{
               run_token: run_token,
               source_token: source_token,
               expected_cursor: "",
               cursor: PolicyMigration.snapshot_cursor(),
               candidates: [],
               done?: false
             })

    assert {:ok, %{done?: false}} =
             Router.flow_policy_catalog_backfill_step(ctx, shard_index, %{
               run_token: run_token,
               source_token: source_token,
               expected_cursor: PolicyMigration.snapshot_cursor(),
               cursor: PolicyMigration.work_cursor(run_token),
               candidates: [],
               done?: false
             })

    :ok = PolicyMigration.cleanup_snapshot(ctx, shard_index, run_token)
    refute PolicyMigration.snapshot_complete?(ctx, shard_index, run_token)

    assert {:ok, pid} =
             PolicyMigrationWorker.start_link(
               instance_ctx: ctx,
               name: :"failover_policy_snapshot_worker_#{System.unique_integer([:positive])}",
               enabled: true,
               initial_delay_ms: 60_000,
               interval_ms: 60_000,
               snapshot_fun: fn _ctx, ^shard_index, ^run_token, _max_items, _max_bytes ->
                 send(parent, {:failover_policy_snapshot_started, self()})
                 receive do: (:release_failover_policy_snapshot -> :ok)
               end,
               backfill_page_fun: fn _ctx, shard, _cursor, _max_items, _max_bytes ->
                 send(parent, {:unexpected_failover_backfill_page, shard})
                 {:ok, %{cursor: PolicyMigration.done_cursor(), candidates: [], done?: true}}
               end
             )

    send(pid, :run)
    :sys.get_state(pid)
    assert_receive {:failover_policy_snapshot_started, snapshot_pid}, 1_000
    refute_receive {:unexpected_failover_backfill_page, ^shard_index}, 100

    snapshot_ref = Process.monitor(snapshot_pid)
    GenServer.stop(pid)
    assert_receive {:DOWN, ^snapshot_ref, :process, ^snapshot_pid, :shutdown}, 1_000
  end

  @tag :resumable_policy_snapshot
  test "keydir snapshot pages persist their cursor and release the fixed table" do
    ctx = FerricStore.Instance.get(:default)
    shard_index = 0
    keydir = elem(ctx.keydir_refs, shard_index)
    run_token = Base.url_encode64(:crypto.strong_rand_bytes(18), padding: false)

    on_exit(fn -> PolicyMigration.cleanup_snapshot(ctx, shard_index, run_token) end)

    first =
      Task.async(fn ->
        PolicyMigration.snapshot_primary_keydir_page(
          ctx,
          shard_index,
          run_token,
          1,
          1_024
        )
      end)
      |> Task.await(5_000)

    assert {:ok, %{done?: false, scanned: 1}} = first
    assert {:ok, first_cursor} = PolicyMigration.snapshot_progress(ctx, shard_index, run_token)
    refute :ets.info(keydir, :safe_fixed)

    second =
      Task.async(fn ->
        PolicyMigration.snapshot_primary_keydir_page(
          ctx,
          shard_index,
          run_token,
          1,
          1_024
        )
      end)
      |> Task.await(5_000)

    assert {:ok, %{done?: false, scanned: 1}} = second
    assert {:ok, second_cursor} = PolicyMigration.snapshot_progress(ctx, shard_index, run_token)
    refute second_cursor == first_cursor
    refute :ets.info(keydir, :safe_fixed)
  end

  @tag :resumable_policy_snapshot
  test "keydir snapshot rejects a non-canonical durable continuation" do
    ctx = FerricStore.Instance.get(:default)
    shard_index = 0
    keydir = elem(ctx.keydir_refs, shard_index)
    run_token = Base.url_encode64(:crypto.strong_rand_bytes(18), padding: false)
    progress_key = <<0, "fpcp:1:", run_token::binary>>
    path = flow_lmdb_path(ctx, shard_index)

    on_exit(fn -> PolicyMigration.cleanup_snapshot(ctx, shard_index, run_token) end)

    assert {:ok, %{done?: false}} =
             PolicyMigration.snapshot_primary_keydir_page(
               ctx,
               shard_index,
               run_token,
               1,
               1_024
             )

    assert {:ok, encoded} = LMDB.get(path, progress_key)
    assert :ok = LMDB.write_batch(path, [{:put, progress_key, encoded <> <<0>>}])

    assert {:error, :corrupt_policy_catalog_snapshot_progress} =
             PolicyMigration.snapshot_primary_keydir_page(
               ctx,
               shard_index,
               run_token,
               1,
               1_024
             )

    refute :ets.info(keydir, :safe_fixed)
  end

  @tag :snapshot_keydir_rebuild
  test "keydir snapshot restarts safely when its ETS table is rebuilt" do
    ctx = FerricStore.Instance.get(:default)
    run_token = Base.url_encode64(:crypto.strong_rand_bytes(18), padding: false)
    first_keydir = :ets.new(:policy_snapshot_keydir, [:set, :public])

    on_exit(fn ->
      PolicyMigration.cleanup_snapshot(ctx, 0, run_token)

      for table <- [first_keydir, Process.get(:rebuilt_policy_snapshot_keydir)],
          is_reference(table) and :ets.info(table) != :undefined do
        :ets.delete(table)
      end
    end)

    true =
      :ets.insert(first_keydir, [
        {Keys.state_key("snapshot-before-rebuild-1", @partition), <<>>, 0, nil, 0, 0, 0},
        {Keys.state_key("snapshot-before-rebuild-2", @partition), <<>>, 0, nil, 0, 0, 0}
      ])

    snapshot_ctx = %{ctx | keydir_refs: {first_keydir}}

    assert {:ok, %{done?: false, scanned: 1}} =
             PolicyMigration.snapshot_primary_keydir_page(snapshot_ctx, 0, run_token, 1, 1_024)

    :ets.delete(first_keydir)
    rebuilt_keydir = :ets.new(:policy_snapshot_keydir, [:set, :public])
    Process.put(:rebuilt_policy_snapshot_keydir, rebuilt_keydir)

    true =
      :ets.insert(
        rebuilt_keydir,
        {Keys.state_key("snapshot-after-rebuild", @partition), <<>>, 0, nil, 0, 0, 0}
      )

    rebuilt_ctx = %{ctx | keydir_refs: {rebuilt_keydir}}

    assert {:ok, %{scanned: 1}} =
             PolicyMigration.snapshot_primary_keydir_page(rebuilt_ctx, 0, run_token, 1, 1_024)

    assert :ok = PolicyMigration.snapshot_primary_keydir(rebuilt_ctx, 0, run_token, 1, 1_024)
    assert PolicyMigration.snapshot_complete?(ctx, 0, run_token)
  end

  @tag :snapshot_invalidated_cursor
  test "keydir snapshot discards an invalidated continuation instead of retrying it forever" do
    ctx = FerricStore.Instance.get(:default)
    shard_index = 0
    keydir = elem(ctx.keydir_refs, shard_index)
    run_token = Base.url_encode64(:crypto.strong_rand_bytes(18), padding: false)
    progress_key = <<0, "fpcp:1:", run_token::binary>>
    path = flow_lmdb_path(ctx, shard_index)

    on_exit(fn -> PolicyMigration.cleanup_snapshot(ctx, shard_index, run_token) end)

    assert {:ok, %{done?: false}} =
             PolicyMigration.snapshot_primary_keydir_page(
               ctx,
               shard_index,
               run_token,
               1,
               1_024
             )

    invalid_progress =
      <<"FPS", 1>> <>
        :erlang.term_to_binary({:ets.info(keydir, :id), {:invalid_continuation}}, [
          :deterministic
        ])

    assert :ok = LMDB.write_batch(path, [{:put, progress_key, invalid_progress}])

    assert {:ok, %{scanned: 1}} =
             PolicyMigration.snapshot_primary_keydir_page(
               ctx,
               shard_index,
               run_token,
               1,
               1_024
             )

    refute :ets.info(keydir, :safe_fixed)
  end

  test "worker does not repeat snapshot cleanup after the manifest is removed" do
    ctx = FerricStore.Instance.get(:default)
    parent = self()
    complete_catalog_backfill_all(ctx)

    run_tokens =
      Map.new(0..(ctx.shard_count - 1), fn shard_index ->
        proof = catalog_backfill_proof(ctx, shard_index)
        {shard_index, proof.run_token}
      end)

    assert {:ok, pid} =
             PolicyMigrationWorker.start_link(
               instance_ctx: ctx,
               name: :"single_policy_cleanup_worker_#{System.unique_integer([:positive])}",
               enabled: true,
               initial_delay_ms: 60_000,
               interval_ms: 60_000,
               catchup_delay_ms: 60_000,
               cleanup_fun: fn cleanup_ctx, shard_index, run_token ->
                 send(parent, {:policy_snapshot_cleanup, shard_index, run_token})
                 PolicyMigration.cleanup_snapshot(cleanup_ctx, shard_index, run_token)
               end
             )

    send(pid, :run)
    :sys.get_state(pid)

    Enum.each(run_tokens, fn {shard_index, run_token} ->
      assert_receive {:policy_snapshot_cleanup, ^shard_index, ^run_token}, 1_000
    end)

    assert_eventually(fn ->
      Enum.each(run_tokens, fn {shard_index, run_token} ->
        refute PolicyMigration.snapshot_complete?(ctx, shard_index, run_token)
      end)
    end)

    send(pid, :run)
    :sys.get_state(pid)
    refute_receive {:policy_snapshot_cleanup, _shard_index, _run_token}, 100
    GenServer.stop(pid)
  end

  test "worker retains the manifest and retries a failed snapshot cleanup" do
    ctx = FerricStore.Instance.get(:default)
    parent = self()
    shard_index = 0
    complete_catalog_backfill_all(ctx)
    run_token = catalog_backfill_proof(ctx, shard_index).run_token
    attempts = :atomics.new(1, signed: false)

    assert {:ok, pid} =
             PolicyMigrationWorker.start_link(
               instance_ctx: ctx,
               name: :"retry_policy_cleanup_worker_#{System.unique_integer([:positive])}",
               enabled: true,
               initial_delay_ms: 60_000,
               interval_ms: 60_000,
               catchup_delay_ms: 60_000,
               cleanup_fun: fn cleanup_ctx, cleanup_shard, cleanup_run ->
                 if cleanup_shard == shard_index do
                   attempt = :atomics.add_get(attempts, 1, 1)
                   send(parent, {:policy_snapshot_cleanup_attempt, attempt, self()})

                   if attempt == 1 do
                     receive do: (:release_failed_policy_cleanup -> {:error, :injected_cleanup})
                   else
                     PolicyMigration.cleanup_snapshot(cleanup_ctx, cleanup_shard, cleanup_run)
                   end
                 else
                   PolicyMigration.cleanup_snapshot(cleanup_ctx, cleanup_shard, cleanup_run)
                 end
               end
             )

    send(pid, :run)
    :sys.get_state(pid)
    assert_receive {:policy_snapshot_cleanup_attempt, 1, cleanup_pid}, 1_000
    assert PolicyMigration.snapshot_complete?(ctx, shard_index, run_token)

    log =
      ExUnit.CaptureLog.capture_log(fn ->
        send(cleanup_pid, :release_failed_policy_cleanup)
        assert_receive {:policy_snapshot_cleanup_attempt, 2, _retry_pid}, 1_000

        assert_eventually(fn ->
          refute PolicyMigration.snapshot_complete?(ctx, shard_index, run_token)
        end)
      end)

    assert log =~ "injected_cleanup"
    GenServer.stop(pid)
  end

  test "catalog pagination requires a final proof after exact-full and byte-truncated pages" do
    ctx = FerricStore.Instance.get(:default)
    shard_index = 0
    type = unique_flow_id("bounded-page")
    lmdb_path = flow_lmdb_path(ctx, shard_index)

    keys =
      Enum.map(1..2, fn index ->
        catalog_key = Keys.type_catalog_member_key(type, Keys.state_key("page-#{index}", nil))
        Keys.policy_catalog_projection_key(type, catalog_key, 0)
      end)

    :ok = LMDB.write_batch(lmdb_path, Enum.map(keys, &{:put, &1, <<1>>}))
    on_exit(fn -> LMDB.write_batch(lmdb_path, Enum.map(keys, &{:delete, &1})) end)

    assert {:ok, %{entries: [_], done?: false}} =
             PolicyMigration.catalog_page(ctx, shard_index, type, 1, 2, 1)

    assert {:ok, %{entries: [_, _], done?: false}} =
             PolicyMigration.catalog_page(ctx, shard_index, type, 1, 2, 1_024 * 1_024)

    :ok = LMDB.write_batch(lmdb_path, Enum.map(keys, &{:delete, &1}))

    assert {:ok, %{entries: [], done?: true}} =
             PolicyMigration.catalog_page(ctx, shard_index, type, 1, 2, 1)
  end

  test "backfill discovers a primary state that is absent from the lagged LMDB mirror" do
    ctx = FerricStore.Instance.get(:default)
    type = unique_flow_id("primary-only-source")
    id = unique_flow_id("primary-only-flow")
    state_key = Keys.state_key(id, @partition)
    shard_index = Router.shard_for(ctx, state_key)
    keydir = elem(ctx.keydir_refs, shard_index)
    catalog_key = Keys.type_catalog_member_key(type, state_key)
    guard_key = Keys.retention_guard_key(id, @partition)
    lmdb_path = flow_lmdb_path(ctx, shard_index)

    assert :ok =
             FerricStore.flow_create(id,
               type: type,
               state: "accept",
               partition_key: @partition,
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert :ok = Ferricstore.Flow.LMDBWriter.flush(ctx.name, shard_index, 30_000)

    :ets.delete(keydir, catalog_key)
    :ets.delete(keydir, guard_key)

    :ok =
      LMDB.write_batch(lmdb_path, [
        {:delete, state_key},
        {:delete, catalog_key},
        {:delete, Keys.policy_catalog_projection_key(type, catalog_key, 0)}
      ])

    run_token = Base.url_encode64(:crypto.strong_rand_bytes(18), padding: false)
    :ok = PolicyMigration.snapshot_primary_keydir(ctx, shard_index, run_token, 10, 1_024)

    candidates = staged_backfill_candidates(ctx, shard_index, run_token, [], 100)
    assert Enum.any?(candidates, &match?(%{kind: :state, state_key: ^state_key}, &1))

    :ok = PolicyMigration.cleanup_snapshot(ctx, shard_index, run_token)
    :ok = PolicyMigration.rotate_source_token(lmdb_path)
    complete_catalog_backfill(ctx, shard_index, 100)

    assert [{^catalog_key, catalog_value, _, _, _, _, _}] = :ets.lookup(keydir, catalog_key)
    assert {:ok, %{state_key: ^state_key}} = PolicyMigration.decode_catalog(catalog_value)
  end

  test "backfill derives a cold LMDB state candidate from its primary registry" do
    ctx = FerricStore.Instance.get(:default)
    type = unique_flow_id("registry-cold-source")
    id = unique_flow_id("registry-cold-flow")
    now_ms = System.system_time(:millisecond)
    state_key = Keys.state_key(id, @partition)
    registry_key = Keys.registry_key(id, @partition)
    shard_index = Router.shard_for(ctx, state_key)
    keydir = elem(ctx.keydir_refs, shard_index)
    catalog_key = Keys.type_catalog_member_key(type, state_key)
    descriptor_key = Keys.type_catalog_descriptor_key(type)
    lmdb_path = flow_lmdb_path(ctx, shard_index)

    assert :ok =
             FerricStore.flow_create(id,
               type: type,
               state: "waiting",
               partition_key: @partition,
               state_meta: %{"version" => 1},
               run_at_ms: now_ms + 301_000,
               now_ms: now_ms
             )

    assert :ok = Ferricstore.Flow.LMDBWriter.flush(ctx.name, shard_index, 30_000)

    assert_eventually(fn ->
      assert [] == :ets.lookup(keydir, state_key)
      assert :ets.member(keydir, registry_key)
      assert {:ok, _park} = LMDB.get(lmdb_path, LMDB.cold_park_key_for_state_key(state_key))
    end)

    :ets.delete(keydir, catalog_key)
    :ets.delete(keydir, descriptor_key)

    :ok =
      LMDB.write_batch(lmdb_path, [
        {:delete, state_key},
        {:delete, catalog_key},
        {:delete, descriptor_key},
        {:delete, Keys.policy_catalog_projection_key(type, catalog_key, 0)}
      ])

    :ok = PolicyMigration.rotate_source_token(lmdb_path)
    complete_catalog_backfill(ctx, shard_index, 100)

    assert [{^catalog_key, catalog_value, _, _, _, _, _}] = :ets.lookup(keydir, catalog_key)
    assert {:ok, %{state_key: ^state_key}} = PolicyMigration.decode_catalog(catalog_value)

    assert {:ok, %{indexed_state_meta: "version"}} =
             FerricStore.flow_policy_set(type, indexed_state_meta: "version")

    assert_migration_done(ctx, shard_index, 32, 10)

    assert {:ok, %{indexed_state_meta: "version"}} =
             FerricStore.flow_get(id, partition_key: @partition)
  end

  test "replicated cold backfill candidate does not read local LMDB during apply" do
    ctx = FerricStore.Instance.get(:default)
    type = unique_flow_id("deterministic-cold-source")
    id = unique_flow_id("deterministic-cold-flow")
    now_ms = System.system_time(:millisecond)
    state_key = Keys.state_key(id, @partition)
    shard_index = Router.shard_for(ctx, state_key)
    keydir = elem(ctx.keydir_refs, shard_index)
    catalog_key = Keys.type_catalog_member_key(type, state_key)
    descriptor_key = Keys.type_catalog_descriptor_key(type)
    lmdb_path = flow_lmdb_path(ctx, shard_index)
    park_key = LMDB.cold_park_key_for_state_key(state_key)

    assert :ok =
             FerricStore.flow_create(id,
               type: type,
               state: "waiting",
               partition_key: @partition,
               run_at_ms: now_ms + 301_000,
               now_ms: now_ms
             )

    assert :ok = Ferricstore.Flow.LMDBWriter.flush(ctx.name, shard_index, 30_000)

    assert_eventually(fn ->
      assert [] == :ets.lookup(keydir, state_key)
      assert {:ok, _park} = LMDB.get(lmdb_path, park_key)
    end)

    :ets.delete(keydir, catalog_key)
    :ets.delete(keydir, descriptor_key)

    :ok =
      LMDB.write_batch(lmdb_path, [
        {:delete, state_key},
        {:delete, catalog_key},
        {:delete, descriptor_key},
        {:delete, Keys.policy_catalog_projection_key(type, catalog_key, 0)}
      ])

    {:ok, source_token} = PolicyMigration.source_token(ctx, shard_index)
    run_token = Base.url_encode64(:crypto.strong_rand_bytes(18), padding: false)

    assert {:ok, _} =
             Router.flow_policy_catalog_backfill_step(ctx, shard_index, %{
               run_token: run_token,
               source_token: source_token,
               expected_cursor: "",
               cursor: PolicyMigration.snapshot_cursor(),
               candidates: [],
               done?: false
             })

    :ok =
      PolicyMigration.snapshot_primary_keydir(ctx, shard_index, run_token, 256, 2 * 1_024 * 1_024)

    work_cursor = PolicyMigration.work_cursor(run_token)

    assert {:ok, _} =
             Router.flow_policy_catalog_backfill_step(ctx, shard_index, %{
               run_token: run_token,
               source_token: source_token,
               expected_cursor: PolicyMigration.snapshot_cursor(),
               cursor: work_cursor,
               candidates: [],
               done?: false
             })

    assert {:ok, %{done?: false} = page} =
             PolicyMigration.backfill_page(
               ctx,
               shard_index,
               work_cursor,
               256,
               2 * 1_024 * 1_024
             )

    assert Enum.any?(page.candidates, fn
             %{kind: :state, state_key: ^state_key, record_value: value} -> is_binary(value)
             _other -> false
           end)

    :ok = LMDB.write_batch(lmdb_path, [{:delete, park_key}])

    assert {:ok, _} =
             Router.flow_policy_catalog_backfill_step(ctx, shard_index, %{
               run_token: run_token,
               source_token: source_token,
               expected_cursor: work_cursor,
               cursor: page.cursor,
               candidates: page.candidates,
               done?: page.done?
             })

    assert [{^catalog_key, catalog_value, _, _, _, _, _}] = :ets.lookup(keydir, catalog_key)
    assert {:ok, %{state_key: ^state_key}} = PolicyMigration.decode_catalog(catalog_value)
  end

  test "planned backfill record cannot recreate membership after retention cleanup" do
    ctx = FerricStore.Instance.get(:default)
    type = unique_flow_id("deleted-after-plan")
    id = unique_flow_id("deleted-after-plan-flow")
    requested_now_ms = 1_000
    state_key = Keys.state_key(id, @partition)
    registry_key = Keys.registry_key(id, @partition)
    guard_key = Keys.retention_guard_key(id, @partition)
    catalog_key = Keys.type_catalog_member_key(type, state_key)
    shard_index = Router.shard_for(ctx, state_key)
    keydir = elem(ctx.keydir_refs, shard_index)

    assert :ok =
             FerricStore.flow_create(id,
               type: type,
               state: "queued",
               partition_key: @partition,
               retention_ttl_ms: 60_000,
               run_at_ms: requested_now_ms,
               now_ms: requested_now_ms
             )

    assert {:ok, [claim]} =
             FerricStore.flow_claim_due(type,
               partition_key: @partition,
               worker: "deleted-after-plan-worker",
               limit: 1,
               now_ms: requested_now_ms
             )

    assert :ok =
             FerricStore.flow_complete(id, claim.lease_token,
               fencing_token: claim.fencing_token,
               partition_key: @partition,
               now_ms: requested_now_ms + 1
             )

    assert {:ok, completed} = FerricStore.flow_get(id, partition_key: @partition)
    assert completed.terminal_retention_until_ms > requested_now_ms + 100
    cleanup_now_ms = completed.terminal_retention_until_ms + 1

    {:ok, source_token} = PolicyMigration.source_token(ctx, shard_index)
    run_token = Base.url_encode64(:crypto.strong_rand_bytes(18), padding: false)

    assert {:ok, _} =
             Router.flow_policy_catalog_backfill_step(ctx, shard_index, %{
               run_token: run_token,
               source_token: source_token,
               expected_cursor: "",
               cursor: PolicyMigration.snapshot_cursor(),
               candidates: [],
               done?: false
             })

    :ok =
      PolicyMigration.snapshot_primary_keydir(ctx, shard_index, run_token, 256, 2 * 1_024 * 1_024)

    work_cursor = PolicyMigration.work_cursor(run_token)

    assert {:ok, _} =
             Router.flow_policy_catalog_backfill_step(ctx, shard_index, %{
               run_token: run_token,
               source_token: source_token,
               expected_cursor: PolicyMigration.snapshot_cursor(),
               cursor: work_cursor,
               candidates: [],
               done?: false
             })

    assert {:ok, %{done?: false} = page} =
             PolicyMigration.backfill_page(
               ctx,
               shard_index,
               work_cursor,
               256,
               2 * 1_024 * 1_024
             )

    assert Enum.any?(page.candidates, &match?(%{kind: :state, state_key: ^state_key}, &1))

    assert {:ok, %{flows: flows}} =
             FerricStore.flow_retention_cleanup(limit: 10, now_ms: cleanup_now_ms)

    assert flows >= 1
    assert [] = :ets.lookup(keydir, registry_key)
    assert [] = :ets.lookup(keydir, guard_key)
    assert [] = :ets.lookup(keydir, catalog_key)

    assert {:ok, _} =
             Router.flow_policy_catalog_backfill_step(ctx, shard_index, %{
               run_token: run_token,
               source_token: source_token,
               expected_cursor: work_cursor,
               cursor: page.cursor,
               candidates: page.candidates,
               done?: page.done?
             })

    assert [] = :ets.lookup(keydir, catalog_key)
  end

  test "replicated cold migration plan does not read local LMDB during apply" do
    ctx = FerricStore.Instance.get(:default)
    type = unique_flow_id("deterministic-cold-migration")
    id = unique_flow_id("deterministic-cold-migration-flow")
    now_ms = System.system_time(:millisecond)
    state_key = Keys.state_key(id, @partition)
    shard_index = Router.shard_for(ctx, state_key)
    keydir = elem(ctx.keydir_refs, shard_index)
    catalog_key = Keys.type_catalog_member_key(type, state_key)
    descriptor_key = Keys.type_catalog_descriptor_key(type)
    lmdb_path = flow_lmdb_path(ctx, shard_index)
    park_key = LMDB.cold_park_key_for_state_key(state_key)

    assert :ok =
             FerricStore.flow_create(id,
               type: type,
               state: "waiting",
               partition_key: @partition,
               state_meta: %{"version" => 1},
               run_at_ms: now_ms + 301_000,
               now_ms: now_ms
             )

    assert :ok = Ferricstore.Flow.LMDBWriter.flush(ctx.name, shard_index, 30_000)

    assert_eventually(fn ->
      assert [] == :ets.lookup(keydir, state_key)
      assert {:ok, _park} = LMDB.get(lmdb_path, park_key)
    end)

    complete_catalog_backfill_all(ctx)

    assert {:ok, %{indexed_state_meta: "version"}} =
             FerricStore.flow_policy_set(type, indexed_state_meta: "version")

    :ok = Ferricstore.Flow.LMDBWriter.flush(ctx.name, shard_index, 30_000)
    assert {:ok, %{key: job_key, job: job}} = PolicyMigration.next_job(ctx, shard_index)
    assert {:ok, descriptor_value} = Router.read_shard_value(ctx, shard_index, descriptor_key)

    assert {:ok, %{membership_revision: membership_revision}} =
             PolicyMigration.decode_type_descriptor(descriptor_value)

    assert {:ok, %{entries: entries, done?: done?}} =
             PolicyMigration.catalog_page(
               ctx,
               shard_index,
               type,
               job.migration_generation,
               32,
               2 * 1_024 * 1_024
             )

    assert Enum.any?(entries, fn
             %{catalog_key: ^catalog_key, record_value: value} -> is_binary(value)
             _other -> false
           end)

    :ok = LMDB.write_batch(lmdb_path, [{:delete, state_key}, {:delete, park_key}])

    assert {:ok, %{processed: processed}} =
             Ferricstore.Raft.WARaftBackend.write(
               shard_index,
               {:flow_policy_migration_step,
                %{
                  job_key: job_key,
                  type: type,
                  migration_generation: job.migration_generation,
                  membership_revision: membership_revision,
                  indexed_state_meta: job.indexed_state_meta,
                  catalog_entries: entries,
                  done?: done?,
                  backfill_proof: catalog_backfill_proof(ctx, shard_index)
                }}
             )

    assert processed > 0
    assert [{^catalog_key, catalog_value, 0, _, _, _, _}] = :ets.lookup(keydir, catalog_key)

    assert {:ok, %{migration_generation: generation}} =
             PolicyMigration.decode_catalog(catalog_value)

    assert generation == job.migration_generation
  end

  test "backfill resolves a stale LMDB record from the current primary record" do
    ctx = FerricStore.Instance.get(:default)
    type = unique_flow_id("stale-lmdb-source")
    id = unique_flow_id("stale-lmdb-source-flow")
    state_key = Keys.state_key(id, @partition)
    shard_index = Router.shard_for(ctx, state_key)
    keydir = elem(ctx.keydir_refs, shard_index)
    guard_key = Keys.retention_guard_key(id, @partition)
    catalog_key = Keys.type_catalog_member_key(type, state_key)

    assert :ok =
             FerricStore.flow_create(id,
               type: type,
               state: "accept",
               partition_key: @partition,
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert :ok = Ferricstore.Flow.LMDBWriter.flush(ctx.name, shard_index, 30_000)

    [{^state_key, encoded, expire_at_ms, lfu, location, offset, _size}] =
      :ets.lookup(keydir, state_key)

    record = Ferricstore.Flow.decode_record(encoded)
    current = %{record | version: record.version + 1, updated_at_ms: record.updated_at_ms + 1}
    current_encoded = Ferricstore.Flow.encode_record(current)
    current_guard = Ferricstore.Flow.RetentionGuard.encode(current)

    true =
      :ets.insert(
        keydir,
        {state_key, current_encoded, expire_at_ms, lfu, location, offset,
         byte_size(current_encoded)}
      )

    replace_keydir_value(keydir, guard_key, current_guard)
    :ets.delete(keydir, catalog_key)

    :ok =
      LMDB.write_batch(flow_lmdb_path(ctx, shard_index), [
        {:delete, catalog_key},
        {:delete, Keys.policy_catalog_projection_key(type, catalog_key, 0)}
      ])

    :ok = PolicyMigration.rotate_source_token(flow_lmdb_path(ctx, shard_index))
    complete_catalog_backfill(ctx, shard_index, 100)

    assert [{^catalog_key, catalog_value, _, _, _, _, _}] = :ets.lookup(keydir, catalog_key)

    assert {:ok, %{state_key: ^state_key}} = PolicyMigration.decode_catalog(catalog_value)
  end

  test "duplicate old projection rows for one catalog member are repaired" do
    ctx = FerricStore.Instance.get(:default)
    type = unique_flow_id("duplicate-projection")
    id = unique_flow_id("duplicate-projection-flow")
    state_key = Keys.state_key(id, @partition)
    shard_index = Router.shard_for(ctx, state_key)
    catalog_key = Keys.type_catalog_member_key(type, state_key)
    lmdb_path = flow_lmdb_path(ctx, shard_index)

    assert :ok =
             FerricStore.flow_create(id,
               type: type,
               state: "accept",
               partition_key: @partition,
               state_meta: %{"v1" => 1, "v2" => 2},
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    complete_catalog_backfill_all(ctx)

    assert {:ok, _} = FerricStore.flow_policy_set(type, indexed_state_meta: "v1")
    assert {:ok, _} = FerricStore.flow_policy_set(type, indexed_state_meta: "v2")

    duplicate_key = Keys.policy_catalog_projection_key(type, catalog_key, 1)
    :ok = LMDB.write_batch(lmdb_path, [{:put, duplicate_key, <<1>>}])

    assert {:ok, %{processed: 2, done?: false}} =
             Router.flow_policy_migration_step(ctx, shard_index, 2)

    assert_migration_done(ctx, shard_index, 2, 5)
    assert :not_found = LMDB.get(lmdb_path, duplicate_key)

    assert {:ok, %{indexed_state_meta: "v2"}} =
             FerricStore.flow_get(id, partition_key: @partition)
  end

  test "planner rejects a primary descriptor that is ahead of its durable mirror" do
    ctx = FerricStore.Instance.get(:default)
    type = unique_flow_id("descriptor-barrier")
    id = unique_flow_id("descriptor-barrier-flow")
    state_key = Keys.state_key(id, @partition)
    shard_index = Router.shard_for(ctx, state_key)
    keydir = elem(ctx.keydir_refs, shard_index)
    descriptor_key = Keys.type_catalog_descriptor_key(type)
    job_key = Keys.policy_migration_job_key(type)
    lmdb_path = flow_lmdb_path(ctx, shard_index)

    assert :ok =
             FerricStore.flow_create(id,
               type: type,
               state: "accept",
               partition_key: @partition,
               state_meta: %{"version" => 1},
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    complete_catalog_backfill_all(ctx)
    assert {:ok, _} = FerricStore.flow_policy_set(type, indexed_state_meta: "version")
    assert :ok = Ferricstore.Flow.LMDBWriter.flush(ctx.name, shard_index, 30_000)

    [{^descriptor_key, descriptor_value, _, _, _, _, _}] = :ets.lookup(keydir, descriptor_key)
    [{^job_key, job_value, _, _, _, _, _}] = :ets.lookup(keydir, job_key)
    {:ok, descriptor} = PolicyMigration.decode_type_descriptor(descriptor_value)
    {:ok, job} = PolicyMigration.decode_job(job_value)
    revision = descriptor.membership_revision + 1
    next_descriptor = PolicyMigration.encode_type_descriptor(type, revision)

    next_job =
      PolicyMigration.encode_job(
        type,
        job.migration_generation,
        revision,
        job.indexed_state_meta,
        :active
      )

    true =
      :ets.insert(
        keydir,
        {descriptor_key, next_descriptor, 0, nil, 0, 0, byte_size(next_descriptor)}
      )

    true = :ets.insert(keydir, {job_key, next_job, 0, nil, 0, 0, byte_size(next_job)})
    :ok = LMDB.write_batch(lmdb_path, [{:put, job_key, LMDB.encode_value(next_job, 0)}])

    assert {:error, "ERR flow policy migration projection pending"} =
             Router.flow_policy_migration_step(ctx, shard_index, 1)

    :ok =
      LMDB.write_batch(lmdb_path, [
        {:put, descriptor_key, LMDB.encode_value(next_descriptor, 0)}
      ])

    assert {:ok, %{done?: done?}} = Router.flow_policy_migration_step(ctx, shard_index, 1)
    assert is_boolean(done?)
  end

  test "membership inserted after planning prevents the stale plan from finishing" do
    ctx = FerricStore.Instance.get(:default)
    type = unique_flow_id("membership-race")
    complete_catalog_backfill_all(ctx)
    assert {:ok, _} = FerricStore.flow_policy_set(type, indexed_state_meta: "version")

    shard_index = Router.shard_for(ctx, Keys.state_key("membership-race-flow", @partition))
    plan = empty_migration_plan(ctx, shard_index, type)

    assert :ok =
             FerricStore.flow_create("membership-race-flow",
               type: type,
               state: "accept",
               partition_key: @partition,
               state_meta: %{"version" => 1},
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert {:ok, %{done?: false}} =
             Ferricstore.Raft.WARaftBackend.write(
               shard_index,
               {:flow_policy_migration_step, plan}
             )

    assert [_active_job] =
             :ets.lookup(elem(ctx.keydir_refs, shard_index), Keys.policy_migration_job_key(type))

    assert_migration_done(ctx, shard_index, 1, 5)
  end

  test "newer completed migration dominates stale policy entries applied out of order" do
    ctx = FerricStore.Instance.get(:default)
    type = unique_flow_id("generation-order")
    shard_index = 0
    complete_catalog_backfill_all(ctx)
    proof = catalog_backfill_proof(ctx, shard_index)

    {:ok, policy_v2} = RetryPolicy.normalize_flow_policy(type, indexed_state_meta: "v2")
    {:ok, policy_v3} = RetryPolicy.normalize_flow_policy(type, indexed_state_meta: "v3")

    plan = %{
      job_key: Keys.policy_migration_job_key(type),
      type: type,
      migration_generation: 3,
      membership_revision: 0,
      indexed_state_meta: "v3",
      catalog_entries: [],
      done?: true,
      backfill_proof: proof
    }

    assert {:ok, %{done?: true}} =
             Ferricstore.Raft.WARaftBackend.write(
               shard_index,
               {:flow_policy_migration_step, plan}
             )

    policy_key = Keys.policy_key(type)

    assert :ok =
             Ferricstore.Raft.WARaftBackend.write(
               shard_index,
               {:flow_policy_put, policy_key, RetryPolicy.encode_flow_policy(policy_v2, 2), 0}
             )

    assert :ok =
             Ferricstore.Raft.WARaftBackend.write(
               shard_index,
               {:flow_policy_put, policy_key, RetryPolicy.encode_flow_policy(policy_v3, 3), 0}
             )

    assert {:ok, value} = Router.read_shard_value(ctx, shard_index, policy_key)
    assert {:ok, {3, decoded}} = RetryPolicy.decode_flow_policy_entry(value)
    assert RetryPolicy.indexed_state_meta(decoded) == "v3"

    assert {:ok, nil} =
             Router.read_shard_value(ctx, shard_index, Keys.policy_migration_job_key(type))
  end

  test "delayed older-policy mutation is rejected and a current mutation converges attributes" do
    ctx = FerricStore.Instance.get(:default)
    type = unique_flow_id("delayed-policy-mutation")
    id = unique_flow_id("delayed-policy-mutation-flow")
    state_key = Keys.state_key(id, @partition)
    shard_index = Router.shard_for(ctx, state_key)
    complete_catalog_backfill_all(ctx)

    assert {:ok, _} =
             FerricStore.flow_policy_set(type,
               indexed_attributes: ["a"],
               indexed_state_meta: "v1"
             )

    migrate_all_shards(ctx, 4)

    assert :ok =
             FerricStore.flow_create(id,
               type: type,
               state: "accept",
               partition_key: @partition,
               attributes: %{"a" => 1, "b" => 2},
               state_meta: %{"v1" => 1, "v2" => 2},
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    {:ok, policy_v1} =
      RetryPolicy.normalize_flow_policy(type,
        indexed_attributes: ["a"],
        indexed_state_meta: "v1"
      )

    {:ok, policy_v2} =
      RetryPolicy.normalize_flow_policy(type,
        indexed_attributes: ["b"],
        indexed_state_meta: "v2"
      )

    assert {:ok, _} =
             FerricStore.flow_policy_set(type,
               indexed_attributes: ["b"],
               indexed_state_meta: "v2"
             )

    migrate_all_shards(ctx, 4)

    assert {:ok, migrated} = FerricStore.flow_get(id, partition_key: @partition)
    assert migrated.indexed_state_meta == "v2"
    assert Ferricstore.Flow.Attributes.indexed_names(migrated) == ["a"]

    delayed_attrs = %{
      id: id,
      type: type,
      state: "accept",
      partition_key: @partition,
      run_at_ms: 2_000,
      now_ms: 1_100,
      policy_ref: policy_reference(policy_v1, 1),
      policy_reference_captured: true
    }

    assert {:error, "ERR stale flow policy generation"} =
             Ferricstore.Raft.WARaftBackend.write(
               shard_index,
               {:flow_schedule_replace, state_key, delayed_attrs}
             )

    assert {:ok, delayed} = FerricStore.flow_get(id, partition_key: @partition)
    assert delayed.indexed_state_meta == "v2"
    assert Ferricstore.Flow.Attributes.indexed_names(delayed) == ["a"]

    current_attrs = %{
      delayed_attrs
      | run_at_ms: 3_000,
        now_ms: 1_200,
        policy_ref: policy_reference(policy_v2, 2)
    }

    assert :ok =
             Ferricstore.Raft.WARaftBackend.write(
               shard_index,
               {:flow_schedule_replace, state_key, current_attrs}
             )

    assert {:ok, converged} = FerricStore.flow_get(id, partition_key: @partition)
    assert converged.indexed_state_meta == "v2"
    assert Ferricstore.Flow.Attributes.indexed_names(converged) == ["b"]
  end

  test "malformed projection generation fails closed without crashing apply" do
    ctx = FerricStore.Instance.get(:default)
    type = unique_flow_id("malformed-plan")
    shard_index = 0
    complete_catalog_backfill_all(ctx)

    plan = %{
      job_key: Keys.policy_migration_job_key(type),
      type: type,
      migration_generation: 1,
      membership_revision: 0,
      indexed_state_meta: "v",
      catalog_entries: [
        %{
          catalog_key: Keys.type_catalog_member_key(type, Keys.state_key("malformed", nil)),
          migration_generation: PolicyMigration.max_exact_score() + 1
        }
      ],
      done?: false,
      backfill_proof: catalog_backfill_proof(ctx, shard_index)
    }

    assert {:error, "ERR invalid flow policy migration plan"} =
             Ferricstore.Raft.WARaftBackend.write(
               shard_index,
               {:flow_policy_migration_step, plan}
             )

    assert {:error, "ERR invalid flow policy migration plan"} =
             Ferricstore.Raft.WARaftBackend.write(
               shard_index,
               {:flow_policy_migration_step,
                %{
                  plan
                  | catalog_entries: [],
                    migration_generation: PolicyMigration.max_exact_score() + 1
                }}
             )

    assert {:error, "ERR invalid flow policy migration plan"} =
             Ferricstore.Raft.WARaftBackend.write(
               shard_index,
               {:flow_policy_migration_step,
                %{plan | catalog_entries: [], membership_revision: 0x1_0000_0000_0000_0000}}
             )
  end

  test "matching durable backfill completion survives worker process restart" do
    ctx = FerricStore.Instance.get(:default)
    complete_catalog_backfill_all(ctx)
    previous_enabled = Application.get_env(:ferricstore, :flow_policy_migration_worker_enabled)
    Application.put_env(:ferricstore, :flow_policy_migration_worker_enabled, true)
    parent = self()

    on_exit(fn -> restore_env(:flow_policy_migration_worker_enabled, previous_enabled) end)

    Enum.each(1..2, fn attempt ->
      name = :"policy_migration_resume_#{attempt}_#{System.unique_integer([:positive])}"

      assert {:ok, pid} =
               PolicyMigrationWorker.start_link(
                 instance_ctx: ctx,
                 name: name,
                 initial_delay_ms: 60_000,
                 backfill_page_fun: fn _, shard, _, _, _ ->
                   send(parent, {:unexpected_backfill_page, attempt, shard})
                   {:ok, %{cursor: "", candidates: [], done?: true}}
                 end
               )

      send(pid, :run)
      :sys.get_state(pid)
      refute_receive {:unexpected_backfill_page, ^attempt, _shard}, 50
      GenServer.stop(pid)
    end)
  end

  test "ordinary embedded restart preserves source token while destructive reconcile rotates it" do
    data_dir =
      Path.join(
        System.tmp_dir!(),
        "policy_migration_restart_#{System.unique_integer([:positive])}"
      )

    File.rm_rf!(data_dir)

    on_exit(fn ->
      EmbeddedPolicyMigration.stop()
      File.rm_rf(data_dir)
    end)

    assert {:ok, _pid} = EmbeddedPolicyMigration.start_link(data_dir: data_dir, shard_count: 1)
    ctx = FerricStore.Instance.get(EmbeddedPolicyMigration)
    lmdb_path = flow_lmdb_path(ctx, 0)
    :ok = PolicyMigration.rotate_source_token(lmdb_path)
    assert {:ok, token} = PolicyMigration.source_token(ctx, 0)

    assert :ok = EmbeddedPolicyMigration.stop()
    assert {:ok, _pid} = EmbeddedPolicyMigration.start_link(data_dir: data_dir, shard_count: 1)
    restarted = FerricStore.Instance.get(EmbeddedPolicyMigration)
    assert {:ok, ^token} = PolicyMigration.source_token(restarted, 0)

    shard_path = Ferricstore.DataDir.shard_data_path(data_dir, 0)

    assert :ok =
             LMDBRebuilder.reconcile_shard(
               shard_path,
               elem(restarted.keydir_refs, 0),
               0,
               restarted,
               nil,
               nil,
               nil,
               nil,
               rotate_policy_source?: true
             )

    assert {:ok, rotated} = PolicyMigration.source_token(restarted, 0)
    refute rotated == token
  end

  defp suspend_policy_migration_worker(ctx) do
    ctx
    |> PolicyMigrationWorker.name()
    |> Process.whereis()
    |> case do
      nil ->
        nil

      pid ->
        :ok = :sys.suspend(pid)
        pid
    end
  end

  defp assert_migration_done(_ctx, _shard_index, _limit, 0),
    do: flunk("policy migration did not converge within the bounded retry count")

  defp assert_migration_done(ctx, shard_index, limit, attempts_left) do
    case Router.flow_policy_migration_step(ctx, shard_index, limit) do
      {:ok, %{done?: true}} ->
        :ok

      {:ok, %{done?: false}} ->
        assert_migration_done(ctx, shard_index, limit, attempts_left - 1)
    end
  end

  defp empty_migration_plan(ctx, shard_index, type) do
    assert :ok = Ferricstore.Flow.LMDBWriter.flush(ctx.name, shard_index, 30_000)
    job_key = Keys.policy_migration_job_key(type)
    assert {:ok, job_value} = Router.read_shard_value(ctx, shard_index, job_key)
    assert {:ok, job} = PolicyMigration.decode_job(job_value)

    %{
      job_key: job_key,
      type: type,
      migration_generation: job.migration_generation,
      membership_revision: job.membership_revision,
      indexed_state_meta: job.indexed_state_meta,
      catalog_entries: [],
      done?: true,
      backfill_proof: catalog_backfill_proof(ctx, shard_index)
    }
  end

  defp catalog_backfill_proof(ctx, shard_index) do
    key = Keys.policy_catalog_backfill_key(shard_index)
    assert {:ok, value} = Router.read_shard_value(ctx, shard_index, key)

    assert {:ok, %{status: :done} = progress} =
             PolicyMigration.decode_backfill_progress(value)

    %{run_token: progress.run_token, source_token: progress.source_token}
  end

  defp complete_catalog_backfill_all(ctx) do
    Enum.each(0..(ctx.shard_count - 1), &complete_catalog_backfill(ctx, &1, 100))
  end

  defp migrate_all_shards(ctx, limit) do
    Enum.each(0..(ctx.shard_count - 1), &assert_migration_done(ctx, &1, limit, 20))
  end

  defp complete_catalog_backfill(_ctx, _shard_index, 0),
    do: flunk("catalog backfill did not converge within the bounded retry count")

  defp complete_catalog_backfill(ctx, shard_index, attempts_left) do
    {:ok, source_token} = PolicyMigration.source_token(ctx, shard_index)
    progress_key = Keys.policy_catalog_backfill_key(shard_index)
    {:ok, progress_value} = Router.read_shard_value(ctx, shard_index, progress_key)

    {run_token, cursor} =
      case PolicyMigration.decode_backfill_progress(progress_value) do
        {:ok, %{source_token: ^source_token, status: :done}} ->
          return_catalog_backfill_done()

        {:ok, %{source_token: ^source_token, status: :active} = progress} ->
          {progress.run_token, progress.cursor}

        _missing_or_stale ->
          {Base.url_encode64(:crypto.strong_rand_bytes(18), padding: false), ""}
      end

    page = catalog_backfill_page(ctx, shard_index, run_token, cursor)

    assert {:ok, result} =
             Router.flow_policy_catalog_backfill_step(ctx, shard_index, %{
               run_token: run_token,
               source_token: source_token,
               expected_cursor: cursor,
               cursor: page.cursor,
               candidates: page.candidates,
               done?: page.done?
             })

    if result.done?,
      do: :ok,
      else: complete_catalog_backfill(ctx, shard_index, attempts_left - 1)
  catch
    :catalog_backfill_done -> :ok
  end

  defp return_catalog_backfill_done, do: throw(:catalog_backfill_done)

  defp catalog_backfill_page(_ctx, _shard_index, _run_token, "") do
    %{cursor: PolicyMigration.snapshot_cursor(), candidates: [], done?: false}
  end

  defp catalog_backfill_page(ctx, shard_index, run_token, cursor) do
    if cursor == PolicyMigration.snapshot_cursor() do
      :ok =
        PolicyMigration.snapshot_primary_keydir(
          ctx,
          shard_index,
          run_token,
          256,
          2 * 1_024 * 1_024
        )

      %{
        cursor: PolicyMigration.work_cursor(run_token),
        candidates: [],
        done?: false
      }
    else
      {:ok, page} =
        PolicyMigration.backfill_page(
          ctx,
          shard_index,
          cursor,
          256,
          2 * 1_024 * 1_024
        )

      page
    end
  end

  defp staged_backfill_candidates(_ctx, _shard_index, _run_token, _acc, 0),
    do: flunk("staged backfill did not converge")

  defp staged_backfill_candidates(ctx, shard_index, run_token, acc, attempts_left) do
    cursor =
      case acc do
        [{:cursor, cursor} | _] -> cursor
        _empty -> PolicyMigration.work_cursor(run_token)
      end

    candidates = Enum.reject(acc, &match?({:cursor, _cursor}, &1))
    {:ok, page} = PolicyMigration.backfill_page(ctx, shard_index, cursor, 256, 2 * 1_024 * 1_024)
    candidates = candidates ++ page.candidates

    if page.done?,
      do: candidates,
      else:
        staged_backfill_candidates(
          ctx,
          shard_index,
          run_token,
          [{:cursor, page.cursor} | candidates],
          attempts_left - 1
        )
  end

  defp flow_lmdb_path(ctx, shard_index) do
    ctx.data_dir
    |> Ferricstore.DataDir.shard_data_path(shard_index)
    |> LMDB.path()
  end

  defp policy_reference(policy, generation) do
    encoded = RetryPolicy.encode_flow_policy(policy, generation)

    %{
      type: Map.fetch!(policy, :type),
      generation: generation,
      digest: :crypto.hash(:sha256, encoded)
    }
  end

  defp replace_keydir_value(keydir, key, value) do
    case :ets.lookup(keydir, key) do
      [{^key, _old, expire_at_ms, lfu, location, offset, _size}] ->
        true =
          :ets.insert(
            keydir,
            {key, value, expire_at_ms, lfu, location, offset, byte_size(value)}
          )

        :ok

      [] ->
        flunk("missing keydir row #{inspect(key)}")
    end
  end

  defp restore_env(key, nil), do: Application.delete_env(:ferricstore, key)
  defp restore_env(key, value), do: Application.put_env(:ferricstore, key, value)
end
