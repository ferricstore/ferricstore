defmodule Ferricstore.Flow.LMDBWriter.AfterFlushGenerationTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Flow.{Keys, LMDB, Locator}
  alias Ferricstore.Flow.LMDBWriter.AfterFlush
  alias Ferricstore.Flow.Query.QueryRowCodec
  alias Ferricstore.Store.LFU

  test "direct terminal pruning keeps a recreated active row with the same version" do
    id = "source-freshness-recreated-active-#{System.unique_integer([:positive])}"
    state_key = Keys.state_key(id)
    ets = :ets.new(:flow_rebuilder_recreated_active_keydir, [:set])
    old_incarnation = 101
    active_record = test_record(id, "queued", 1, 202)
    active_encoded = Ferricstore.Flow.encode_record(active_record)

    active_row =
      {state_key, active_encoded, 0, {:flow_state_version, 1, LFU.initial()}, :memory, 0,
       byte_size(active_encoded)}

    true = :ets.insert(ets, active_row)

    action = prune_action(ets, state_key, id, 1, old_incarnation)

    assert :ok = AfterFlush.apply_after_flush(action)
    assert [^active_row] = :ets.lookup(ets, state_key)
  end

  test "direct terminal pruning keeps a recreated terminal row with the same version" do
    id = "source-freshness-recreated-terminal-#{System.unique_integer([:positive])}"
    state_key = Keys.state_key(id)
    ets = :ets.new(:flow_rebuilder_recreated_terminal_keydir, [:set])
    old_incarnation = 101
    terminal_record = test_record(id, "completed", 1, 202)
    terminal_encoded = Ferricstore.Flow.encode_record(terminal_record)

    terminal_row =
      {state_key, terminal_encoded, 0, {:flow_state_version, 1, LFU.initial()}, :memory, 0,
       byte_size(terminal_encoded)}

    true = :ets.insert(ets, terminal_row)

    action = prune_action(ets, state_key, id, 1, old_incarnation)

    assert :ok = AfterFlush.apply_after_flush(action)
    assert [^terminal_row] = :ets.lookup(ets, state_key)
  end

  test "direct terminal pruning removes the captured terminal incarnation" do
    id = "source-freshness-matching-terminal-#{System.unique_integer([:positive])}"
    state_key = Keys.state_key(id)
    ets = :ets.new(:flow_rebuilder_matching_terminal_keydir, [:set])
    incarnation = 101
    terminal_record = test_record(id, "completed", 1, incarnation)
    terminal_encoded = Ferricstore.Flow.encode_record(terminal_record)

    terminal_row =
      {state_key, terminal_encoded, 0, {:flow_state_version, 1, LFU.initial()}, :memory, 0,
       byte_size(terminal_encoded)}

    true = :ets.insert(ets, terminal_row)

    action = prune_action(ets, state_key, id, 1, incarnation)

    assert :ok = AfterFlush.apply_after_flush(action)
    assert [] = :ets.lookup(ets, state_key)
  end

  test "source pruning is a no-op when the durable query row is a different incarnation" do
    data_dir = temp_data_dir("source-mismatch")
    Ferricstore.DataDir.ensure_layout!(data_dir, 1)
    shard_path = Ferricstore.DataDir.shard_data_path(data_dir, 0)
    lmdb_path = LMDB.path(shard_path)
    id = "source-freshness-source-mismatch-#{System.unique_integer([:positive])}"
    state_key = Keys.state_key(id)
    source_record = test_record(id, "completed", 1, 202)
    durable_record = test_record(id, "completed", 1, 101)
    source_encoded = Ferricstore.Flow.encode_record(source_record)

    row =
      {state_key, source_encoded, 0, {:flow_state_version, 1, LFU.initial()}, :memory, 0,
       byte_size(source_encoded)}

    keydir = :ets.new(:flow_rebuilder_source_mismatch_keydir, [:set])
    true = :ets.insert(keydir, row)
    seed_query_row!(lmdb_path, state_key, durable_record)

    on_exit(fn -> File.rm_rf!(data_dir) end)

    action = source_action(data_dir, keydir, state_key, 1)

    assert :ok = AfterFlush.apply_after_flush(action)
    assert [^row] = :ets.lookup(keydir, state_key)
  end

  test "source pruning removes a terminal row when the durable identity matches" do
    data_dir = temp_data_dir("source-match")
    Ferricstore.DataDir.ensure_layout!(data_dir, 1)
    shard_path = Ferricstore.DataDir.shard_data_path(data_dir, 0)
    lmdb_path = LMDB.path(shard_path)
    id = "source-freshness-source-match-#{System.unique_integer([:positive])}"
    state_key = Keys.state_key(id)
    source_record = test_record(id, "completed", 1, 202)
    source_encoded = Ferricstore.Flow.encode_record(source_record)

    row =
      {state_key, source_encoded, 0, {:flow_state_version, 1, LFU.initial()}, :memory, 0,
       byte_size(source_encoded)}

    keydir = :ets.new(:flow_rebuilder_source_match_keydir, [:set])
    true = :ets.insert(keydir, row)
    seed_query_row!(lmdb_path, state_key, source_record)

    on_exit(fn -> File.rm_rf!(data_dir) end)

    action = source_action(data_dir, keydir, state_key, 1)

    assert :ok = AfterFlush.apply_after_flush(action)
    assert [] = :ets.lookup(keydir, state_key)
  end

  defp prune_action(ets, state_key, id, version, incarnation) do
    {:prune_terminal_flow, "/tmp/flow-rebuilder-source-freshness", 0, ets, nil, nil, nil, nil,
     state_key, "job", "completed", nil, nil, id, nil, id, version, incarnation}
  end

  defp source_action(data_dir, ets, state_key, version) do
    {:prune_terminal_flow_from_source, data_dir, 0, ets, nil, nil, nil, nil, state_key, version}
  end

  defp seed_query_row!(lmdb_path, state_key, record) do
    encoded = Ferricstore.Flow.encode_record(record)

    locator =
      Locator.new!(
        flow_id: record.id,
        kind: :state,
        version: record.version,
        raft_index: record.version,
        file_id: 0,
        offset: 0,
        value_size: byte_size(encoded),
        checksum: :crypto.hash(:sha256, encoded)
      )

    assert {:ok, query_row} = QueryRowCodec.encode(state_key, record, locator, 0)
    assert :ok = LMDB.write_batch(lmdb_path, [{:put, state_key, query_row}])
  end

  defp temp_data_dir(label),
    do:
      Path.join(
        System.tmp_dir!(),
        "flow-rebuilder-#{label}-#{System.unique_integer([:positive])}"
      )

  defp test_record(id, state, version, incarnation) do
    %{
      id: id,
      type: "job",
      state: state,
      version: version,
      attempts: 0,
      fencing_token: 0,
      created_at_ms: 1,
      updated_at_ms: version + 1,
      next_run_at_ms: 0,
      priority: 0,
      partition_key: nil,
      root_flow_id: id,
      incarnation: incarnation,
      state_enter_seq: incarnation
    }
  end
end
