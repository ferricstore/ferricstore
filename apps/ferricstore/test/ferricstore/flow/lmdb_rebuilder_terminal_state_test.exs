defmodule Ferricstore.Flow.LMDBRebuilder.TerminalStateTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Flow.{Keys, Locator, StorageScope}
  alias Ferricstore.Flow.LMDBRebuilder.TerminalState
  alias Ferricstore.Flow.Query.QueryRowCodec

  test "terminal state resolution batches only states missing from the hot keydir" do
    keydir = :ets.new(:terminal_state_batch_keydir, [:set])
    hot = record("hot-terminal", "partition-hot", "completed")
    cold = record("cold-active", "partition-cold", "queued")
    missing_key = Keys.state_key("missing", "partition-missing")
    hot_key = Keys.state_key(hot.id, hot.partition_key)
    cold_key = Keys.state_key(cold.id, cold.partition_key)

    true =
      :ets.insert(keydir, [
        {hot_key, "hot", 0, 0, :hot, 0, 3},
        {Keys.registry_key(cold.id, cold.partition_key), "owner", 0, 0, :hot, 0, 5}
      ])

    decode_entry_fun = fn
      {^hot_key, "hot", 0, 0, :hot, 0, 3} -> [{hot_key, "hot", 0, hot, nil}]
    end

    parent = self()
    cold_locator = locator(cold, 11)
    assert {:ok, cold_row} = QueryRowCodec.encode(cold_key, cold, cold_locator, 0)

    read_many_fun = fn keys ->
      send(parent, {:durable_state_keys, keys})

      {:ok, [{:ok, cold_row}]}
    end

    assert {:ok,
            %{
              ^hot_key => :terminal,
              ^cold_key => :active,
              ^missing_key => :missing
            }} =
             TerminalState.statuses_with_reader(
               keydir,
               [hot_key, cold_key, missing_key],
               decode_entry_fun,
               read_many_fun
             )

    assert_receive {:durable_state_keys, [^cold_key]}
    refute_receive {:durable_state_keys, _other}
  end

  test "terminal state resolution fails closed when the authoritative keydir disappears" do
    keydir = :ets.new(:terminal_state_missing_keydir, [:set])
    state_key = Keys.state_key("flow", "partition")
    :ets.delete(keydir)

    assert {:error, :authoritative_flow_state_unavailable} =
             TerminalState.statuses_with_reader(
               keydir,
               [state_key],
               fn _entry -> flunk("deleted keydir cannot contain an entry") end,
               fn _keys -> flunk("durable reads must not run without authoritative ownership") end
             )
  end

  test "terminal state resolution accepts a shared-scope durable query row" do
    keydir = :ets.new(:terminal_state_shared_durable_keydir, [:set])
    logical = record("shared-terminal", "logical-partition", "completed")
    scope_metadata = scope_metadata(42)
    scope_prefix = <<42::unsigned-big-64>>

    assert {:ok, physical_partition} =
             StorageScope.physical_partition_key(logical.partition_key, scope_prefix)

    authoritative =
      logical
      |> Map.put(:partition_key, physical_partition)
      |> Map.put(:system_metadata, scope_metadata)

    state_key = Keys.state_key(authoritative.id, physical_partition)
    registry_key = Keys.registry_key(authoritative.id, physical_partition)
    true = :ets.insert(keydir, {registry_key, "owner", 0, 0, :hot, 0, 1})

    assert {:ok, encoded_row} =
             QueryRowCodec.encode(state_key, authoritative, locator(authoritative, 17), 0)

    assert {:ok, %{^state_key => :terminal}} =
             TerminalState.statuses_with_reader(
               keydir,
               [state_key],
               fn _entry ->
                 flunk("shared durable state must not be read from the hot keydir")
               end,
               fn [^state_key] -> {:ok, [{:ok, encoded_row}]} end
             )
  end

  test "terminal state resolution rejects hot records with inconsistent scope metadata" do
    keydir = :ets.new(:terminal_state_shared_hot_keydir, [:set])
    logical = record("shared-hot", "logical-partition", "queued")

    assert {:ok, physical_partition} =
             StorageScope.physical_partition_key(
               logical.partition_key,
               <<42::unsigned-big-64>>
             )

    forged =
      logical
      |> Map.put(:partition_key, physical_partition)
      |> Map.put(:system_metadata, scope_metadata(99))

    state_key = Keys.state_key(forged.id, physical_partition)
    true = :ets.insert(keydir, {state_key, "hot", 0, 0, :hot, 0, 1})

    decode_entry_fun = fn
      {^state_key, "hot", 0, 0, :hot, 0, 1} -> [{state_key, "hot", 0, forged, nil}]
    end

    assert {:error, :mismatched_authoritative_flow_state} =
             TerminalState.statuses_with_reader(
               keydir,
               [state_key],
               decode_entry_fun,
               fn [] -> {:ok, []} end
             )
  end

  defp record(id, partition_key, state) do
    %{
      id: id,
      type: "job",
      state: state,
      version: 1,
      attempts: 0,
      fencing_token: 0,
      created_at_ms: 1,
      updated_at_ms: 1,
      next_run_at_ms: if(state == "queued", do: 10_000, else: 0),
      priority: 0,
      partition_key: partition_key,
      state_enter_seq: 1,
      root_flow_id: id
    }
  end

  defp locator(record, index) do
    Locator.new!(
      flow_id: record.id,
      kind: :state,
      version: record.version,
      raft_index: index,
      file_id: {:waraft_apply_projection, index},
      offset: 0,
      value_size: 256,
      frame_size: 512,
      segment_generation: 1,
      checksum: :crypto.hash(:sha256, Ferricstore.Flow.encode_record(record)),
      expire_at_ms: 0
    )
  end

  defp scope_metadata(value) do
    %{0x8001 => {1, :uint64, :isolation_scope, value}}
  end
end
