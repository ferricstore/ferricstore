defmodule Ferricstore.Flow.Query.CompositeProjectionIntegrationTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Flow.LMDB
  alias Ferricstore.Flow.LMDBWriter.ProjectionOps
  alias Ferricstore.Flow.Query.{CompositeCounter, CompositeIndex, IndexDefinition, QueryRowCodec}

  defmodule Provider do
    @behaviour FerricStore.Flow.QueryIndexProvider

    @impl true
    def snapshot(%{test_pid: test_pid, definitions: definitions}, shard_index) do
      send(test_pid, {:definition_snapshot, shard_index})

      indexes =
        Enum.map(definitions, fn definition ->
          Ferricstore.Flow.Query.RegisteredIndex.new!(definition, :active)
        end)

      {:ok,
       Ferricstore.Flow.Query.RegistrySnapshot.new!(%{
         epoch: 1,
         catalog_version: 1,
         indexes: indexes
       })}
    end
  end

  test "LMDB writer snapshots definitions once and projects the final record version" do
    path = tmp_lmdb_path()
    state_key = Ferricstore.Flow.Keys.state_key("run-1", "tenant-a")

    definition =
      IndexDefinition.new!(%{
        id: "runs_by_state_updated",
        version: 1,
        fields: [{:partition_key, :asc}, {:state, :asc}, {:updated_at_ms, :desc}]
      })

    second = encoded_record("waiting", 2, 200)

    instance_ctx =
      install_sources(
        %{
          query_index_provider: Provider,
          test_pid: self(),
          definitions: [definition]
        },
        Path.join(path, "source"),
        [{state_key, second, 2}]
      )

    writer_state = %{
      path: path,
      shard_index: 0,
      instance_ctx: instance_ctx,
      terminal_count_inits: MapSet.new()
    }

    assert {:ok, ops, _state} =
             ProjectionOps.expand_ops(writer_state, [
               {:project_flow_state_from_source, state_key, 1},
               {:project_flow_state_from_source, state_key, 2}
             ])

    assert_received {:definition_snapshot, 0}
    refute_received {:definition_snapshot, 0}
    assert :ok = LMDB.write_batch(path, ops)

    assert {:ok, reverse_blob} = LMDB.get(path, CompositeIndex.reverse_key(state_key))
    assert {:ok, [entry_key]} = CompositeIndex.decode_reverse_value(reverse_blob, state_key)
    assert {:ok, entry_blob} = LMDB.get(path, entry_key)
    assert {:ok, %{record_version: 2}} = CompositeIndex.decode_entry_value(entry_blob)

    assert {:ok, query_row} = LMDB.get(path, state_key)

    assert {:ok, %{record: %{state: "waiting", version: 2}}} =
             QueryRowCodec.decode(query_row, state_key)
  end

  test "disabled providers add no composite read or write operations" do
    path = tmp_lmdb_path()
    state_key = Ferricstore.Flow.Keys.state_key("run-1", "tenant-a")
    encoded = encoded_record("running", 1, 100)

    instance_ctx =
      install_sources(%{}, Path.join(path, "source"), [{state_key, encoded, 1}])

    assert {:ok, ops, _state} =
             ProjectionOps.expand_ops(
               %{
                 path: path,
                 shard_index: 0,
                 instance_ctx: instance_ctx,
                 terminal_count_inits: MapSet.new()
               },
               [{:project_flow_state_from_source, state_key, 1}]
             )

    refute Enum.any?(ops, fn
             {_operation, key} when is_binary(key) -> composite_key?(key)
             {_operation, key, _value} when is_binary(key) -> composite_key?(key)
             _other -> false
           end)
  end

  test "query-only projection writes hydration and composite rows without lifecycle indexes" do
    path = tmp_lmdb_path()
    state_key = Ferricstore.Flow.Keys.state_key("run-query-only", "tenant-a")

    definition =
      IndexDefinition.new!(%{
        id: "runs_by_state",
        version: 1,
        fields: [{:partition_key, :asc}, {:state, :asc}]
      })

    encoded = encoded_record("running", 1, 100, "run-query-only")

    instance_ctx =
      install_sources(
        %{
          query_index_provider: Provider,
          test_pid: self(),
          definitions: [definition]
        },
        Path.join(path, "source"),
        [{state_key, encoded, 1}]
      )

    writer_state = %{
      path: path,
      shard_index: 0,
      instance_ctx: instance_ctx,
      terminal_count_inits: MapSet.new()
    }

    assert {:ok, ops, _state} =
             ProjectionOps.expand_ops(writer_state, [
               {:project_flow_query_state_from_source, state_key, 1}
             ])

    for terminal_state <- ["completed", "failed", "cancelled"] do
      index_key = Ferricstore.Flow.Keys.state_index_key("invoice", terminal_state, "tenant-a")
      count_key = LMDB.terminal_count_key(index_key)
      refute Enum.any?(ops, &(is_tuple(&1) and count_key in Tuple.to_list(&1)))
    end

    assert :ok = LMDB.write_batch(path, ops)
    assert {:ok, _wrapper} = LMDB.get(path, state_key)
    assert {:ok, _reverse} = LMDB.get(path, CompositeIndex.reverse_key(state_key))
    assert :not_found = LMDB.get(path, LMDB.active_by_state_key_key(state_key))
  end

  test "writer coalescing keeps the highest record version regardless of arrival order" do
    path = tmp_lmdb_path()
    state_key = Ferricstore.Flow.Keys.state_key("run-coalesce-fence", "tenant-a")

    definition =
      IndexDefinition.new!(%{
        id: "runs_by_state_updated",
        version: 1,
        fields: [{:partition_key, :asc}, {:state, :asc}, {:updated_at_ms, :desc}]
      })

    terminal = encoded_record("failed", 3, 300, "run-coalesce-fence")

    instance_ctx =
      install_sources(
        %{
          query_index_provider: Provider,
          test_pid: self(),
          definitions: [definition]
        },
        Path.join(path, "source"),
        [{state_key, terminal, 3}]
      )

    writer_state = %{
      path: path,
      shard_index: 0,
      instance_ctx: instance_ctx,
      terminal_count_inits: MapSet.new()
    }

    assert {:ok, ops, _state} =
             ProjectionOps.expand_ops(writer_state, [
               {:project_flow_state_from_source, state_key, 3},
               {:project_flow_query_state_from_source, state_key, 2}
             ])

    assert :ok = LMDB.write_batch(path, ops)
    assert {:ok, query_row} = LMDB.get(path, state_key)

    assert {:ok, %{record: %{state: "failed", version: 3}}} =
             QueryRowCodec.decode(query_row, state_key)

    assert {:ok, reverse_blob} = LMDB.get(path, CompositeIndex.reverse_key(state_key))
    assert {:ok, [entry_key]} = CompositeIndex.decode_reverse_value(reverse_blob, state_key)
    assert {:ok, entry_blob} = LMDB.get(path, entry_key)
    assert {:ok, %{record_version: 3}} = CompositeIndex.decode_entry_value(entry_blob)
  end

  test "composite projection hydrates records whose logical state key exceeds LMDB limits" do
    path = tmp_lmdb_path()
    id = :binary.copy("r", 60_000)
    state_key = Ferricstore.Flow.Keys.state_key(id, "tenant-a")

    definition =
      IndexDefinition.new!(%{
        id: "long_runs_by_state",
        version: 1,
        fields: [{:partition_key, :asc}, {:state, :asc}]
      })

    encoded = encoded_record("running", 1, 100, id)

    instance_ctx =
      install_sources(
        %{
          query_index_provider: Provider,
          test_pid: self(),
          definitions: [definition]
        },
        Path.join(path, "source"),
        [{state_key, encoded, 1}]
      )

    writer_state = %{
      path: path,
      shard_index: 0,
      instance_ctx: instance_ctx,
      terminal_count_inits: MapSet.new()
    }

    assert {:ok, ops, _state} =
             ProjectionOps.expand_ops(writer_state, [
               {:project_flow_state_from_source, state_key, 1}
             ])

    assert :ok = LMDB.write_batch(path, ops)
    assert {:ok, query_row} = LMDB.get(path, state_key)

    assert {:ok, %{record: %{id: ^id}}} = QueryRowCodec.decode(query_row, state_key)
    assert {:ok, reverse_blob} = LMDB.get(path, CompositeIndex.reverse_key(state_key))
    assert {:ok, [entry_key]} = CompositeIndex.decode_reverse_value(reverse_blob, state_key)
    assert {:ok, entry_blob} = LMDB.get(path, entry_key)

    assert {:ok, %{id: ^id, state_key: ^state_key}} =
             CompositeIndex.decode_entry_value(entry_blob)
  end

  test "writer chunks never split one composite projection transaction" do
    old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)
    old_interval = Application.get_env(:ferricstore, :flow_lmdb_flush_interval_ms)
    old_chunk_ops = Application.get_env(:ferricstore, :flow_lmdb_flush_chunk_ops)
    old_chunk_pause = Application.get_env(:ferricstore, :flow_lmdb_flush_chunk_pause_ms)

    Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)
    Application.put_env(:ferricstore, :flow_lmdb_flush_interval_ms, 60_000)
    Application.put_env(:ferricstore, :flow_lmdb_flush_chunk_ops, 1)
    Application.put_env(:ferricstore, :flow_lmdb_flush_chunk_pause_ms, 20)

    data_dir = tmp_data_dir()
    instance_name = :"composite_atomic_writer_#{System.unique_integer([:positive])}"
    state_key = Ferricstore.Flow.Keys.state_key("run-1", "tenant-a")

    definition =
      IndexDefinition.new!(%{
        id: "runs_by_state_updated",
        version: 1,
        fields: [{:partition_key, :asc}, {:state, :asc}, {:updated_at_ms, :desc}],
        count_prefixes: [2]
      })

    encoded = encoded_record("running", 1, 100)
    record = Ferricstore.Flow.decode_record(encoded)
    assert {:ok, [entry]} = CompositeIndex.entries(definition, record, state_key, 0)
    assert {:ok, prefixes} = CompositeCounter.prefixes_for_keys([definition], [entry.key])
    [{^definition, counter_prefix}] = MapSet.to_list(prefixes)
    counter_key = CompositeCounter.key(definition, counter_prefix)

    instance_ctx =
      install_sources(
        %{
          name: instance_name,
          data_dir: data_dir,
          shard_count: 1,
          query_index_provider: Provider,
          test_pid: self(),
          definitions: [definition]
        },
        data_dir,
        [{state_key, encoded, 1}]
      )

    on_exit(fn ->
      restore_env(:flow_lmdb_mode, old_mode)
      restore_env(:flow_lmdb_flush_interval_ms, old_interval)
      restore_env(:flow_lmdb_flush_chunk_ops, old_chunk_ops)
      restore_env(:flow_lmdb_flush_chunk_pause_ms, old_chunk_pause)
      File.rm_rf!(data_dir)
    end)

    File.mkdir_p!(Ferricstore.DataDir.shard_data_path(data_dir, 0))

    assert {:ok, _pid} =
             Ferricstore.Flow.LMDBWriter.start_link(
               shard_index: 0,
               data_dir: data_dir,
               instance_ctx: instance_ctx
             )

    assert :ok =
             Ferricstore.Flow.LMDBWriter.enqueue(instance_name, 0, [
               {:project_flow_state_from_source, state_key, 1}
             ])

    path =
      data_dir
      |> Ferricstore.DataDir.shard_data_path(0)
      |> LMDB.path()

    parent = self()

    spawn_link(fn ->
      send(parent, {:composite_flush, Ferricstore.Flow.LMDBWriter.flush(instance_name, 0)})
    end)

    assert :ok =
             await_flush_without_partial_projection(path, entry.key, counter_key, 5_000)

    assert {:ok, _counter} = LMDB.get(path, counter_key)
    expected_entry_value = entry.value
    assert {:ok, ^expected_entry_value} = LMDB.get(path, entry.key)
  end

  test "writer re-expands a projection after a concurrent reverse compare conflict" do
    data_dir = tmp_data_dir()
    instance_name = :"composite_retry_writer_#{System.unique_integer([:positive])}"
    first_id = "run-retry-a"
    first_key = Ferricstore.Flow.Keys.state_key(first_id, "tenant-a")
    second_key = Ferricstore.Flow.Keys.state_key("run-retry-b", "tenant-a")

    definition =
      IndexDefinition.new!(%{
        id: "runs_by_state_updated",
        version: 1,
        fields: [{:partition_key, :asc}, {:state, :asc}, {:updated_at_ms, :desc}]
      })

    first = encoded_record("running", 1, 100, first_id)
    second = encoded_record("running", 1, 100, "run-retry-b")

    instance_ctx =
      install_sources(
        %{
          name: instance_name,
          data_dir: data_dir,
          shard_count: 1,
          query_index_provider: Provider,
          test_pid: self(),
          definitions: [definition]
        },
        data_dir,
        [{first_key, first, 1}, {second_key, second, 1}]
      )

    on_exit(fn ->
      Ferricstore.FaultInjection.clear_hook()
      File.rm_rf!(data_dir)
    end)

    File.mkdir_p!(Ferricstore.DataDir.shard_data_path(data_dir, 0))

    assert {:ok, _pid} =
             Ferricstore.Flow.LMDBWriter.start_link(
               shard_index: 0,
               data_dir: data_dir,
               instance_ctx: instance_ctx
             )

    assert :ok =
             Ferricstore.Flow.LMDBWriter.enqueue(instance_name, 0, [
               {:project_flow_query_state_from_source, first_key, 1},
               {:project_flow_query_state_from_source, second_key, 1}
             ])

    path =
      data_dir
      |> Ferricstore.DataDir.shard_data_path(0)
      |> LMDB.path()

    parent = self()
    hits = :atomics.new(1, signed: false)

    Ferricstore.FaultInjection.put_hook(fn
      :before_flow_lmdb_flush_write, %{instance_name: ^instance_name} ->
        if :atomics.add_get(hits, 1, 1) == 1 do
          send(parent, {:projection_write_paused, self()})

          receive do
            :continue_projection_write -> :ok
          end
        end

        :ok

      _point, _metadata ->
        :ok
    end)

    spawn_link(fn ->
      result = Ferricstore.Flow.LMDBWriter.flush(instance_name, 0, 30_000)
      send(parent, {:projection_flush_result, result})
    end)

    assert_receive {:projection_write_paused, writer}, 5_000

    old_record =
      "waiting"
      |> encoded_record(0, 50, first_id)
      |> Ferricstore.Flow.decode_record()

    assert {:ok, [old_entry]} = CompositeIndex.entries(definition, old_record, first_key, 0)
    old_reverse = CompositeIndex.encode_reverse_value(first_key, [old_entry.key])

    assert :ok =
             LMDB.write_batch(path, [
               {:put, old_entry.key, old_entry.value},
               {:put, CompositeIndex.reverse_key(first_key), old_reverse}
             ])

    send(writer, :continue_projection_write)

    assert_receive {:projection_flush_result, flush_result}, 10_000
    assert flush_result == :ok
    assert :atomics.get(hits, 1) >= 2
    assert :not_found = LMDB.get(path, old_entry.key)
  end

  test "deleted source atomically removes every projection derived from the old query row" do
    path = tmp_lmdb_path()
    id = "run-delete-all-projections"
    state_key = Ferricstore.Flow.Keys.state_key(id, "tenant-a")

    definition =
      IndexDefinition.new!(%{
        id: "terminal_by_tier_and_worker",
        version: 1,
        fields: [
          {:partition_key, :asc},
          {{:attribute, "tier"}, :asc},
          {{:state_meta, "running", "worker"}, :asc},
          {:updated_at_ms, :desc}
        ],
        count_prefixes: [3]
      })

    record =
      "completed"
      |> encoded_record(1, 100, id)
      |> Ferricstore.Flow.decode_record()
      |> Map.merge(%{
        parent_flow_id: "parent-1",
        root_flow_id: "root-1",
        correlation_id: "correlation-1",
        attributes: %{"tier" => "gold"},
        indexed_attributes: ["tier"],
        state_meta: %{"running" => %{"worker" => "worker-1"}},
        indexed_state_meta: "worker"
      })

    encoded = Ferricstore.Flow.encode_record(record)

    instance_ctx =
      install_sources(
        %{
          query_index_provider: Provider,
          test_pid: self(),
          definitions: [definition]
        },
        Path.join(path, "source"),
        [{state_key, encoded, 1}]
      )

    writer_state = %{
      path: path,
      shard_index: 0,
      instance_ctx: instance_ctx,
      terminal_count_inits: MapSet.new()
    }

    assert {:ok, [composite_entry]} = CompositeIndex.entries(definition, record, state_key, 0)

    assert {:ok, prefixes} =
             CompositeCounter.prefixes_for_keys([definition], [composite_entry.key])

    [{^definition, counter_prefix}] = MapSet.to_list(prefixes)
    counter_key = CompositeCounter.key(definition, counter_prefix)

    metadata_query_keys =
      ProjectionOps.terminal_project_metadata_index_keys(
        id,
        "tenant-a",
        "parent-1",
        "root-1",
        "correlation-1"
      )
      |> Enum.map(&LMDB.query_index_key(&1, id, 100))

    fixed_query_keys =
      metadata_query_keys ++ ProjectionOps.flow_attribute_query_keys(record)

    assert {:ok, initial_ops, _state} =
             ProjectionOps.expand_ops(writer_state, [
               {:project_flow_state_from_source, state_key, 1}
             ])

    assert :ok = LMDB.write_batch(path, initial_ops)

    for key <- fixed_query_keys do
      assert {:ok, _value} = LMDB.get(path, key)
    end

    assert {:ok, _value} = LMDB.get(path, composite_entry.key)
    assert {:ok, _value} = LMDB.get(path, counter_key)

    keydir = elem(instance_ctx.keydir_refs, 0)
    :ets.insert(keydir, {state_key, nil, 0, :flow_state_deleted, :deleted, 0, 0})

    assert {:ok, delete_ops, delete_state} =
             ProjectionOps.expand_ops(writer_state, [
               {:project_flow_state_from_source, state_key, 1}
             ])

    assert delete_state.write_group_sizes == [length(delete_ops)]
    assert :ok = LMDB.write_batch(path, delete_ops)

    for key <-
          fixed_query_keys ++
            [
              state_key,
              composite_entry.key,
              CompositeIndex.reverse_key(state_key)
            ] do
      assert :not_found = LMDB.get(path, key)
    end

    assert :not_found = LMDB.get(path, counter_key)
  end

  defp composite_key?(key) do
    String.starts_with?(key, IndexDefinition.global_storage_prefix()) or
      String.starts_with?(key, CompositeIndex.reverse_prefix())
  end

  defp encoded_record(state, version, updated_at_ms, id \\ "run-1") do
    %{
      id: id,
      type: "invoice",
      state: state,
      version: version,
      attempts: 0,
      fencing_token: 0,
      created_at_ms: 10,
      updated_at_ms: updated_at_ms,
      next_run_at_ms: 10,
      priority: 0,
      ttl_ms: nil,
      history_hot_max_events: nil,
      history_max_events: nil,
      retention_ttl_ms: nil,
      max_active_ms: nil,
      terminal_retention_until_ms: nil,
      partition_key: "tenant-a",
      payload_ref: nil,
      parent_flow_id: nil,
      parent_partition_key: nil,
      root_flow_id: id,
      correlation_id: nil,
      result_ref: nil,
      error_ref: nil,
      lease_owner: "",
      lease_token: nil,
      lease_deadline_ms: 0,
      run_state: nil,
      state_enter_seq: version,
      child_groups: %{}
    }
    |> Ferricstore.Flow.encode_record()
  end

  defp tmp_lmdb_path do
    path =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_composite_writer_#{System.unique_integer([:positive])}"
      )

    on_exit(fn ->
      File.rm_rf!(path)
    end)

    path
  end

  defp tmp_data_dir do
    Path.join(
      System.tmp_dir!(),
      "ferricstore_composite_atomic_writer_#{System.unique_integer([:positive])}"
    )
  end

  defp install_sources(instance_ctx, data_dir, sources) do
    keydir = :ets.new(:composite_projection_source, [:set, :public])

    sources
    |> Enum.group_by(fn {_state_key, _value, index} -> index end)
    |> Enum.each(fn {index, entries} ->
      projection_entries =
        Enum.map(entries, fn {state_key, value, ^index} -> {state_key, value, 0} end)

      assert :ok =
               Ferricstore.Raft.WARaftSegmentReader.put_apply_projection(
                 data_dir,
                 0,
                 index,
                 projection_entries
               )
    end)

    Enum.each(sources, fn {state_key, value, index} ->
      :ets.insert(
        keydir,
        {state_key, nil, 0, 0, {:waraft_apply_projection, index}, 0, byte_size(value)}
      )
    end)

    on_exit(fn ->
      Ferricstore.Raft.WARaftSegmentReader.clear_apply_projection_cache(data_dir, 0)
    end)

    instance_ctx
    |> Map.put(:data_dir, data_dir)
    |> Map.put(:keydir_refs, {keydir})
  end

  defp await_flush_without_partial_projection(path, entry_key, counter_key, timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_await_flush_without_partial_projection(path, entry_key, counter_key, deadline)
  end

  defp do_await_flush_without_partial_projection(path, entry_key, counter_key, deadline) do
    assert {:ok, [entry, counter]} = LMDB.get_many(path, [entry_key, counter_key])
    entry? = match?({:ok, _value}, entry)
    counter? = match?({:ok, _value}, counter)

    assert entry? == counter?, "composite index entry and counter became partially visible"

    remaining_ms = max(deadline - System.monotonic_time(:millisecond), 0)

    receive do
      {:composite_flush, result} ->
        result
    after
      min(remaining_ms, 2) ->
        if remaining_ms == 0 do
          flunk("timed out waiting for composite LMDB flush")
        else
          do_await_flush_without_partial_projection(path, entry_key, counter_key, deadline)
        end
    end
  end

  defp restore_env(key, nil), do: Application.delete_env(:ferricstore, key)
  defp restore_env(key, value), do: Application.put_env(:ferricstore, key, value)
end
