defmodule Ferricstore.Raft.ServerCatalogApplyTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Raft.StateMachine
  alias Ferricstore.ServerCatalog

  setup do
    root = Path.join(System.tmp_dir!(), "server_catalog_#{System.unique_integer([:positive])}")
    shard_path = Ferricstore.DataDir.shard_data_path(root, 0)
    active_file_path = Path.join(shard_path, "00000.log")
    table = :ets.new(:server_catalog_test, [:set, :public])
    instance_name = :"server_catalog_test_#{System.unique_integer([:positive])}"

    File.mkdir_p!(shard_path)
    File.touch!(active_file_path)

    state =
      StateMachine.init(%{
        shard_index: 0,
        shard_data_path: shard_path,
        active_file_id: 0,
        active_file_path: active_file_path,
        ets: table,
        instance_name: instance_name
      })

    on_exit(fn ->
      if :ets.info(table) != :undefined, do: :ets.delete(table)
      File.rm_rf!(root)
    end)

    %{state: state, table: table}
  end

  test "catalog mutation serializes the entire namespace revision", %{
    state: state
  } do
    command = catalog_mutation("alice", nil, nil, "canonical-user", 10)

    assert {state, {:ok, encoded}} =
             apply_result(StateMachine.apply(%{index: 41}, command, state))

    assert {:ok, %{version: 41, value: "canonical-user"}} = ServerCatalog.decode_entry(encoded)

    assert {_state, {:error, :stale_server_catalog_revision}} =
             apply_result(
               StateMachine.apply(
                 %{index: 42},
                 catalog_mutation("bob", nil, nil, "other-user", 10),
                 state
               )
             )

    revision = ServerCatalog.encode_revision(41)
    replacement = catalog_mutation("alice", encoded, revision, "replacement", 10)

    assert {_state, {:ok, replacement_encoded}} =
             apply_result(StateMachine.apply(%{index: 43}, replacement, state))

    assert {:ok, %{version: 43, value: "replacement"}} =
             ServerCatalog.decode_entry(replacement_encoded)
  end

  test "live entry limits are enforced atomically inside apply", %{state: state} do
    assert {state, {:ok, _alice}} =
             apply_result(
               StateMachine.apply(
                 %{index: 7},
                 catalog_mutation("alice", nil, nil, "alice-user", 1),
                 state
               )
             )

    assert {_state, {:error, {:server_catalog_limit_reached, 1}}} =
             apply_result(
               StateMachine.apply(
                 %{index: 8},
                 catalog_mutation(
                   "bob",
                   nil,
                   ServerCatalog.encode_revision(7),
                   "bob-user",
                   1
                 ),
                 state
               )
             )

    count_key = ServerCatalog.live_count_key("acl")

    assert [{^count_key, count, 0, _lfu, _file_id, _offset, _size}] =
             :ets.lookup(state.ets, count_key)

    assert {:ok, 1} = ServerCatalog.decode_live_count(count)
    assert [] = :ets.lookup(state.ets, ServerCatalog.entry_key("acl", "bob"))
  end

  test "deletes remove entries while advancing revision and live count", %{state: state} do
    create = catalog_mutation("alice", nil, nil, "canonical-user", 10)

    assert {state, {:ok, encoded}} =
             apply_result(StateMachine.apply(%{index: 7}, create, state))

    delete =
      catalog_mutation(
        "alice",
        encoded,
        ServerCatalog.encode_revision(7),
        :deleted,
        10
      )

    assert {state, {:ok, tombstone}} =
             apply_result(StateMachine.apply(%{index: 8}, delete, state))

    assert {:ok, %{version: 8, value: :deleted}} = ServerCatalog.decode_entry(tombstone)

    key = ServerCatalog.entry_key("acl", "alice")
    assert [] = :ets.lookup(state.ets, key)

    revision_key = ServerCatalog.revision_key("acl")

    assert [{^revision_key, revision, 0, _lfu, _file_id, _offset, _size}] =
             :ets.lookup(state.ets, revision_key)

    assert {:ok, 8} = ServerCatalog.decode_revision(revision)

    count_key = ServerCatalog.live_count_key("acl")

    assert [{^count_key, count, 0, _lfu, _file_id, _offset, _size}] =
             :ets.lookup(state.ets, count_key)

    assert {:ok, 0} = ServerCatalog.decode_live_count(count)
  end

  test "catalog replacement atomically applies additions, updates, and deletions", %{state: state} do
    assert {state, {:ok, _alice}} =
             apply_result(
               StateMachine.apply(
                 %{index: 7},
                 catalog_mutation("alice", nil, nil, "old", 10),
                 state
               )
             )

    assert {state, {:ok, _bob}} =
             apply_result(
               StateMachine.apply(
                 %{index: 8},
                 catalog_mutation(
                   "bob",
                   nil,
                   ServerCatalog.encode_revision(7),
                   "remove-me",
                   10
                 ),
                 state
               )
             )

    command =
      {:server_catalog_replace, "acl", ServerCatalog.encode_revision(8),
       [{"alice", "new"}, {"bob", :deleted}, {"carol", "added"}], 2, 10}

    assert {state, {:ok, revision}} =
             apply_result(StateMachine.apply(%{index: 9}, command, state))

    assert {:ok, 9} = ServerCatalog.decode_revision(revision)
    assert {:ok, %{version: 9, value: "new"}} = catalog_entry(state, "alice")
    assert {:ok, %{version: 9, value: "added"}} = catalog_entry(state, "carol")
    assert :missing = catalog_entry(state, "bob")

    count_key = ServerCatalog.live_count_key("acl")

    assert [{^count_key, count, 0, _lfu, _file_id, _offset, _size}] =
             :ets.lookup(state.ets, count_key)

    assert {:ok, 2} = ServerCatalog.decode_live_count(count)
  end

  test "opaque server commands are rejected", %{
    state: state
  } do
    command = {:server_command, {:acl_setuser, "alice", [">plaintext"]}}

    assert {_state, {:error, {:unknown_command, ^command}}} =
             apply_result(StateMachine.apply(%{}, command, state))
  end

  test "invalid catalog commands return errors instead of crashing apply", %{state: state} do
    assert {_state, {:error, :invalid_server_catalog_mutation}} =
             apply_result(
               StateMachine.apply(
                 %{index: 1},
                 {:server_catalog_mutate, "ACL:invalid", "alice", nil, nil, "value", 10},
                 state
               )
             )
  end

  defp catalog_mutation(subject, expected, expected_revision, value, max_live_entries) do
    {:server_catalog_mutate, "acl", subject, expected, expected_revision, value, max_live_entries}
  end

  defp catalog_entry(state, subject) do
    key = ServerCatalog.entry_key("acl", subject)

    case :ets.lookup(state.ets, key) do
      [{^key, encoded, 0, _lfu, _file_id, _offset, _size}] -> ServerCatalog.decode_entry(encoded)
      [] -> :missing
    end
  end

  defp apply_result({state, {:applied_at, _index, result}, _effects}), do: {state, result}
  defp apply_result({state, result, _effects}), do: {state, result}
  defp apply_result({state, result}), do: {state, result}
end
