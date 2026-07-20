defmodule Ferricstore.Flow.Query.CompositeProjectionIntegrationTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.LMDB
  alias Ferricstore.Flow.LMDBWriter.ProjectionOps
  alias Ferricstore.Flow.Query.{CompositeIndex, IndexDefinition}

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

    instance_ctx = %{
      query_index_provider: Provider,
      test_pid: self(),
      definitions: [definition]
    }

    writer_state = %{
      path: path,
      shard_index: 0,
      instance_ctx: instance_ctx,
      terminal_count_inits: MapSet.new()
    }

    first = encoded_record("running", 1, 100)
    second = encoded_record("waiting", 2, 200)

    assert {:ok, ops, _state} =
             ProjectionOps.expand_ops(writer_state, [
               {:project_flow_state, state_key, first, 0},
               {:project_flow_state, state_key, second, 0}
             ])

    assert_received {:definition_snapshot, 0}
    refute_received {:definition_snapshot, 0}
    assert :ok = LMDB.write_batch(path, ops)

    assert {:ok, reverse_blob} = LMDB.get(path, CompositeIndex.reverse_key(state_key))
    assert {:ok, [entry_key]} = CompositeIndex.decode_reverse_value(reverse_blob, state_key)
    assert {:ok, entry_blob} = LMDB.get(path, entry_key)
    assert {:ok, %{record_version: 2}} = CompositeIndex.decode_entry_value(entry_blob)

    assert {:ok, wrapper} = LMDB.get(path, state_key)

    assert {:ok, %{state: "waiting", version: 2}} =
             ProjectionOps.decode_flow_record_value(wrapper)
  end

  test "disabled providers add no composite read or write operations" do
    path = tmp_lmdb_path()
    state_key = Ferricstore.Flow.Keys.state_key("run-1", "tenant-a")
    encoded = encoded_record("running", 1, 100)

    assert {:ok, ops, _state} =
             ProjectionOps.expand_ops(
               %{path: path, terminal_count_inits: MapSet.new()},
               [{:project_flow_state, state_key, encoded, 0}]
             )

    refute Enum.any?(ops, fn
             {_operation, key} when is_binary(key) -> composite_key?(key)
             {_operation, key, _value} when is_binary(key) -> composite_key?(key)
             _other -> false
           end)
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

    writer_state = %{
      path: path,
      shard_index: 0,
      instance_ctx: %{
        query_index_provider: Provider,
        test_pid: self(),
        definitions: [definition]
      },
      terminal_count_inits: MapSet.new()
    }

    assert {:ok, ops, _state} =
             ProjectionOps.expand_ops(writer_state, [
               {:project_flow_state, state_key, encoded_record("running", 1, 100, id), 0}
             ])

    assert :ok = LMDB.write_batch(path, ops)
    assert {:ok, wrapper} = LMDB.get(path, state_key)

    assert {:ok, %{id: ^id}} = ProjectionOps.decode_flow_record_value(wrapper)
    assert {:ok, reverse_blob} = LMDB.get(path, CompositeIndex.reverse_key(state_key))
    assert {:ok, [entry_key]} = CompositeIndex.decode_reverse_value(reverse_blob, state_key)
    assert {:ok, entry_blob} = LMDB.get(path, entry_key)

    assert {:ok, %{id: ^id, state_key: ^state_key}} =
             CompositeIndex.decode_entry_value(entry_blob)
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
end
