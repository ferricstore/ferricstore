defmodule Ferricstore.Flow.LMDBWriter.RestartRecoveryTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Flow.{Keys, LMDB, Locator}
  alias Ferricstore.Flow.LMDBFlushCoordinator
  alias Ferricstore.Flow.LMDBRebuilder
  alias Ferricstore.Flow.LMDBWriter
  alias Ferricstore.Flow.Query.{CompositeIndex, QueryRowCodec}
  alias Ferricstore.Flow.Query.SourceCatalog
  alias Ferricstore.Store.Shard.ETS, as: ShardETS

  @moduletag :flow

  defmodule CompositeProvider do
    @behaviour FerricStore.Flow.QueryIndexProvider

    alias Ferricstore.Flow.Query.{IndexDefinition, RegisteredIndex, RegistrySnapshot}

    def definition do
      IndexDefinition.new!(%{
        id: "restart_recovery_composite",
        version: 1,
        fields: [
          {:partition_key, :asc},
          {:state, :asc},
          {:updated_at_ms, :desc}
        ]
      })
    end

    @impl true
    def snapshot(_ctx, _shard_index) do
      {:ok,
       RegistrySnapshot.new!(%{
         epoch: 1,
         catalog_version: 1,
         indexes: [RegisteredIndex.new!(definition(), :active, build_id: "restart-recovery")]
       })}
    end
  end

  test "restart reconciliation removes a Flow delete whose async projection was lost" do
    LMDBRebuilder.init_startup_active_rebuild_limiter()

    unique = System.unique_integer([:positive, :monotonic])
    instance_name = String.to_atom("lmdb_writer_delete_restart_#{unique}")

    data_dir =
      Path.join(System.tmp_dir!(), "ferricstore_#{System.pid()}_#{instance_name}")

    Ferricstore.DataDir.ensure_layout!(data_dir, 1)
    keydir = :ets.new(:lmdb_writer_delete_restart_keydir, [:set, :public])

    instance_ctx = %{
      name: instance_name,
      data_dir: data_dir,
      shard_count: 1,
      keydir_refs: {keydir},
      max_value_size: 1_048_576,
      query_index_provider: CompositeProvider
    }

    record = %{
      id: "writer-delete-restart",
      type: "job",
      state: "queued",
      version: 1,
      attempts: 0,
      fencing_token: 0,
      created_at_ms: 1,
      updated_at_ms: 1,
      next_run_at_ms: 10_000,
      priority: 0,
      partition_key: "tenant",
      root_flow_id: "writer-delete-restart",
      state_enter_seq: 1,
      attributes: %{"owner" => "alice"}
    }

    state_key = Keys.state_key(record.id, record.partition_key)
    encoded = Ferricstore.Flow.encode_record(record)
    shard_path = Ferricstore.DataDir.shard_data_path(data_dir, 0)
    lmdb_path = LMDB.path(shard_path)
    source_path = ShardETS.file_path(shard_path, 0)
    File.mkdir_p!(shard_path)
    File.touch!(source_path)

    {:ok, [{offset, value_size}]} =
      Ferricstore.Bitcask.NIF.v2_append_batch(source_path, [{state_key, encoded, 0}])

    :ets.insert(keydir, {state_key, nil, 0, 0, 0, offset, value_size})

    {:ok, coordinator} = LMDBFlushCoordinator.start_link(instance_name: instance_name)
    Process.unlink(coordinator)

    {:ok, writer} =
      LMDBWriter.start_link(shard_index: 0, data_dir: data_dir, instance_ctx: instance_ctx)

    Process.unlink(writer)

    on_exit(fn ->
      stop_process(writer)
      stop_process(coordinator)
      LMDB.release(lmdb_path)
      if :ets.info(keydir) != :undefined, do: :ets.delete(keydir)
      File.rm_rf!(data_dir)
    end)

    assert :ok =
             LMDBWriter.enqueue(instance_name, 0, [
               {:project_flow_state_from_source, state_key, record.version}
             ])

    assert :ok = LMDBWriter.flush(instance_name, 0)
    assert {:ok, _query_row} = LMDB.get(lmdb_path, state_key)

    sibling_record = %{
      record
      | id: "writer-delete-cold-sibling",
        state: "completed",
        partition_key: "tenant-sibling",
        root_flow_id: "writer-delete-cold-sibling"
    }

    sibling_state_key = Keys.state_key(sibling_record.id, sibling_record.partition_key)
    sibling_encoded = Ferricstore.Flow.encode_record(sibling_record)

    sibling_locator =
      Locator.new!(
        flow_id: sibling_record.id,
        kind: :state,
        version: sibling_record.version,
        raft_index: sibling_record.version,
        file_id: 9,
        offset: 0,
        value_size: byte_size(sibling_encoded),
        checksum: :crypto.hash(:sha256, sibling_encoded),
        expire_at_ms: 0
      )

    assert {:ok, sibling_query_row} =
             QueryRowCodec.encode(sibling_state_key, sibling_record, sibling_locator, 0)

    assert :ok = LMDB.write_batch(lmdb_path, [{:put, sibling_state_key, sibling_query_row}])

    fixed_projection_keys =
      Ferricstore.Flow.Attributes.index_entries(record)
      |> Enum.map(fn {index_key, id, score} -> LMDB.query_index_key(index_key, id, score) end)
      |> Enum.uniq()

    active_projection_keys =
      record
      |> LMDB.active_projection_entries()
      |> Enum.map(fn {index_key, id, score} -> LMDB.active_index_key(index_key, id, score) end)
      |> Enum.uniq()

    active_reverse_key = LMDB.active_by_state_key_key(state_key)
    source_catalog_key = Keys.type_catalog_member_key(record.type, state_key)
    source_entry_key = SourceCatalog.entry_prefix() <> source_catalog_key
    composite_definition = CompositeProvider.definition()

    assert {:ok, composite_entries} =
             CompositeIndex.entries(composite_definition, record, state_key, 0)

    composite_projection_keys = Enum.map(composite_entries, & &1.key)
    composite_reverse_key = CompositeIndex.reverse_key(state_key)

    assert fixed_projection_keys != []
    assert active_projection_keys != []
    assert composite_projection_keys != []

    Enum.each(fixed_projection_keys, fn key -> assert {:ok, _value} = LMDB.get(lmdb_path, key) end)

    Enum.each(active_projection_keys, fn key ->
      assert {:ok, _value} = LMDB.get(lmdb_path, key)
    end)

    assert {:ok, _value} = LMDB.get(lmdb_path, active_reverse_key)
    assert {:ok, ^state_key} = LMDB.get(lmdb_path, source_entry_key)
    assert {:ok, _value} = LMDB.get(lmdb_path, composite_reverse_key)

    Enum.each(composite_projection_keys, fn key ->
      assert {:ok, _value} = LMDB.get(lmdb_path, key)
    end)

    :ets.insert(keydir, {state_key, nil, 0, :flow_state_deleted, :deleted, 0, 0})

    ref = Process.monitor(writer)
    assert :ok = :sys.suspend(writer)

    assert :ok =
             LMDBWriter.enqueue_async(
               instance_name,
               0,
               [{:project_flow_state_from_source, state_key, record.version}]
             )

    Process.exit(writer, :kill)
    assert_receive {:DOWN, ^ref, :process, ^writer, _reason}, 5_000

    {:ok, restarted_writer} =
      LMDBWriter.start_link(shard_index: 0, data_dir: data_dir, instance_ctx: instance_ctx)

    Process.unlink(restarted_writer)

    on_exit(fn ->
      if Process.alive?(restarted_writer), do: Process.exit(restarted_writer, :shutdown)
      LMDB.release(lmdb_path)
    end)

    assert :ok = LMDBWriter.flush(instance_name, 0)
    assert :not_found = LMDB.get(lmdb_path, state_key)
    assert {:ok, ^sibling_query_row} = LMDB.get(lmdb_path, sibling_state_key)

    Enum.each(fixed_projection_keys, fn key -> assert :not_found = LMDB.get(lmdb_path, key) end)

    Enum.each(active_projection_keys, fn key -> assert :not_found = LMDB.get(lmdb_path, key) end)

    assert :not_found = LMDB.get(lmdb_path, active_reverse_key)
    assert :not_found = LMDB.get(lmdb_path, source_entry_key)
    assert :not_found = LMDB.get(lmdb_path, composite_reverse_key)

    Enum.each(composite_projection_keys, fn key ->
      assert :not_found = LMDB.get(lmdb_path, key)
    end)

    assert :ok =
             LMDBRebuilder.reconcile_shard(
               shard_path,
               keydir,
               0,
               instance_ctx,
               nil,
               nil,
               nil,
               nil
             )

    assert :not_found = LMDB.get(lmdb_path, state_key)
    assert {:ok, ^sibling_query_row} = LMDB.get(lmdb_path, sibling_state_key)

    Enum.each(fixed_projection_keys, fn key -> assert :not_found = LMDB.get(lmdb_path, key) end)

    Enum.each(active_projection_keys, fn key -> assert :not_found = LMDB.get(lmdb_path, key) end)

    assert :not_found = LMDB.get(lmdb_path, active_reverse_key)
    assert :not_found = LMDB.get(lmdb_path, source_entry_key)
    assert :not_found = LMDB.get(lmdb_path, composite_reverse_key)

    Enum.each(composite_projection_keys, fn key ->
      assert :not_found = LMDB.get(lmdb_path, key)
    end)
  end

  test "restart reconciliation restores a policy mirror whose async write was lost" do
    LMDBRebuilder.init_startup_active_rebuild_limiter()

    unique = System.unique_integer([:positive, :monotonic])
    instance_name = String.to_atom("lmdb_writer_policy_restart_#{unique}")

    data_dir =
      Path.join(System.tmp_dir!(), "ferricstore_#{System.pid()}_#{instance_name}")

    Ferricstore.DataDir.ensure_layout!(data_dir, 1)
    keydir = :ets.new(:lmdb_writer_policy_restart_keydir, [:set, :public])

    type = "restart-policy"
    job_key = Keys.policy_migration_job_key(type)
    job_value = Ferricstore.Flow.PolicyMigration.encode_job(type, 3, 7, "version", :active)
    shard_path = Ferricstore.DataDir.shard_data_path(data_dir, 0)
    lmdb_path = LMDB.path(shard_path)
    source_path = ShardETS.file_path(shard_path, 0)
    File.mkdir_p!(shard_path)
    File.touch!(source_path)

    {:ok, [{offset, value_size}]} =
      Ferricstore.Bitcask.NIF.v2_append_batch(source_path, [{job_key, job_value, 0}])

    :ets.insert(keydir, {job_key, nil, 0, 0, 0, offset, value_size})

    instance_ctx = %{
      name: instance_name,
      data_dir: data_dir,
      shard_count: 1,
      keydir_refs: {keydir},
      max_value_size: 1_048_576,
      query_index_provider: FerricStore.Flow.QueryIndexProvider.Disabled
    }

    {:ok, coordinator} = LMDBFlushCoordinator.start_link(instance_name: instance_name)
    Process.unlink(coordinator)

    {:ok, writer} =
      LMDBWriter.start_link(shard_index: 0, data_dir: data_dir, instance_ctx: instance_ctx)

    Process.unlink(writer)

    on_exit(fn ->
      stop_process(writer)
      stop_process(coordinator)
      LMDB.release(lmdb_path)
      if :ets.info(keydir) != :undefined, do: :ets.delete(keydir)
      File.rm_rf!(data_dir)
    end)

    ref = Process.monitor(writer)
    assert :ok = :sys.suspend(writer)

    encoded_job_value = LMDB.encode_value(job_value, 0)

    assert :ok =
             LMDBWriter.enqueue_async(
               instance_name,
               0,
               [{:put, job_key, encoded_job_value}]
             )

    Process.exit(writer, :kill)
    assert_receive {:DOWN, ^ref, :process, ^writer, _reason}, 5_000

    {:ok, restarted_writer} =
      LMDBWriter.start_link(shard_index: 0, data_dir: data_dir, instance_ctx: instance_ctx)

    Process.unlink(restarted_writer)

    on_exit(fn ->
      stop_process(restarted_writer)
      LMDB.release(lmdb_path)
    end)

    assert :ok = LMDBWriter.flush(instance_name, 0)
    assert {:ok, ^encoded_job_value} = LMDB.get(lmdb_path, job_key)

    ref = Process.monitor(restarted_writer)
    Process.exit(restarted_writer, :shutdown)
    assert_receive {:DOWN, ^ref, :process, ^restarted_writer, _reason}, 5_000

    {:ok, restarted_writer_2} =
      LMDBWriter.start_link(shard_index: 0, data_dir: data_dir, instance_ctx: instance_ctx)

    Process.unlink(restarted_writer_2)

    on_exit(fn ->
      stop_process(restarted_writer_2)
      LMDB.release(lmdb_path)
    end)

    assert :ok = LMDBWriter.flush(instance_name, 0)
    assert {:ok, ^encoded_job_value} = LMDB.get(lmdb_path, job_key)
  end

  defp stop_process(pid) when is_pid(pid) do
    if Process.alive?(pid) do
      ref = Process.monitor(pid)
      Process.exit(pid, :shutdown)

      receive do
        {:DOWN, ^ref, :process, ^pid, _reason} -> :ok
      after
        5_000 -> :timeout
      end
    else
      :ok
    end
  end

  defp stop_process(_pid), do: :ok
end
