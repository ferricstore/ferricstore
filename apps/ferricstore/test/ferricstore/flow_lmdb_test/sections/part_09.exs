defmodule Ferricstore.Flow.LMDBTest.Sections.Part09 do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Flow.LMDBTest.FlushProbeWriter

  test "async terminal history is cold-only after LMDB projection" do
    old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)
    old_async_history = Application.get_env(:ferricstore, :flow_async_history)

    old_projector_flush =
      Application.get_env(:ferricstore, :flow_history_projector_flush_interval_ms)

    Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)
    Application.put_env(:ferricstore, :flow_async_history, true)
    Application.put_env(:ferricstore, :flow_history_projector_flush_interval_ms, 60_000)

    ctx = Ferricstore.Test.IsolatedInstance.checkout(shard_count: 1, hot_cache_max_value_size: 1)

    on_exit(fn ->
      Ferricstore.Test.IsolatedInstance.checkin(ctx)
      restore_env(:flow_lmdb_mode, old_mode)
      restore_env(:flow_async_history, old_async_history)
      restore_env(:flow_history_projector_flush_interval_ms, old_projector_flush)
    end)

    id = "history-terminal-cold-only"
    partition_key = "tenant-history-terminal-cold-only"
    flow_type = "history-terminal-cold-only"

    assert :ok =
             Ferricstore.Flow.create(ctx, id,
               type: flow_type,
               partition_key: partition_key,
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert {:ok, [claimed]} =
             Ferricstore.Flow.claim_due(ctx, flow_type,
               worker: "worker-history-terminal-cold-only",
               partition_key: partition_key,
               lease_ms: 30_000,
               limit: 1,
               now_ms: 1_001
             )

    assert :ok =
             Ferricstore.Flow.complete(ctx, id, claimed.lease_token,
               partition_key: partition_key,
               fencing_token: claimed.fencing_token,
               now_ms: 1_002
             )

    assert :ok = Ferricstore.Flow.HistoryProjector.flush(ctx, 0, 120_000)

    history_key = Ferricstore.Flow.Keys.history_key(id, partition_key)
    {_flow_index, flow_lookup} = Ferricstore.Flow.OrderedIndex.table_names(ctx.name, 0)

    assert Ferricstore.Flow.OrderedIndex.count_all(flow_lookup, history_key) == 0

    lmdb_path =
      ctx.data_dir
      |> Ferricstore.DataDir.shard_data_path(0)
      |> Ferricstore.Flow.LMDB.path()

    assert {:ok, 3} =
             Ferricstore.Flow.LMDB.prefix_count(
               lmdb_path,
               Ferricstore.Flow.LMDB.history_index_prefix(history_key)
             )

    assert {:ok, history} =
             Ferricstore.Flow.history(ctx, id, partition_key: partition_key, count: 10)

    assert Enum.map(history, fn {_event_id, fields} -> fields["event"] end) == [
             "created",
             "claimed",
             "completed"
           ]

    assert {:ok, []} =
             Ferricstore.Flow.history(ctx, id,
               partition_key: partition_key,
               count: 10,
               include_cold: false
             )
  end

  test "async history compacts generated payload value rows after LMDB projection" do
    old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)
    old_async_history = Application.get_env(:ferricstore, :flow_async_history)

    old_flush_interval =
      Application.get_env(:ferricstore, :flow_history_projector_flush_interval_ms)

    Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)
    Application.put_env(:ferricstore, :flow_async_history, true)
    Application.put_env(:ferricstore, :flow_history_projector_flush_interval_ms, 60_000)

    ctx = Ferricstore.Test.IsolatedInstance.checkout(shard_count: 1)
    shard_data_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0)

    _projector =
      start_supervised!(
        {Ferricstore.Flow.HistoryProjector,
         [
           shard_index: 0,
           shard_data_path: shard_data_path,
           instance_ctx: ctx,
           recover_on_init: false
         ]}
      )

    on_exit(fn ->
      Ferricstore.Test.IsolatedInstance.checkin(ctx)
      restore_env(:flow_lmdb_mode, old_mode)
      restore_env(:flow_async_history, old_async_history)
      restore_env(:flow_history_projector_flush_interval_ms, old_flush_interval)
    end)

    id = "history-dematerialize-payload"
    partition_key = "tenant-history-dematerialize-payload"
    flow_type = "history-dematerialize-payload"
    initial_payload = String.duplicate("a", 1024)
    next_payload = String.duplicate("b", 1024)

    assert :ok =
             Ferricstore.Flow.create(ctx, id,
               type: flow_type,
               partition_key: partition_key,
               payload: initial_payload,
               history_hot_max_events: 1,
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert {:ok, created} =
             Ferricstore.Flow.get(ctx, id, partition_key: partition_key)

    assert {:ok, [claimed]} =
             Ferricstore.Flow.claim_due(ctx, flow_type,
               worker: "worker-history-dematerialize-payload",
               partition_key: partition_key,
               lease_ms: 30_000,
               limit: 1,
               now_ms: 1_001
             )

    assert :ok =
             Ferricstore.Flow.transition(ctx, id, "running", "waiting",
               partition_key: partition_key,
               lease_token: claimed.lease_token,
               fencing_token: claimed.fencing_token,
               payload: next_payload,
               run_at_ms: 2_000,
               now_ms: 1_002
             )

    assert {:ok, latest} =
             Ferricstore.Flow.get(ctx, id, partition_key: partition_key)

    set_apply_projection_value!(
      ctx,
      created.payload_ref,
      Ferricstore.Flow.encode_value(initial_payload),
      101
    )

    set_apply_projection_value!(
      ctx,
      latest.payload_ref,
      Ferricstore.Flow.encode_value(next_payload),
      102
    )

    assert apply_projection_cache_count(ctx, 0) == 2

    assert :ok = Ferricstore.Flow.HistoryProjector.flush(ctx, 0, 120_000)

    refute_keydir_row!(ctx, created.payload_ref)
    refute_keydir_row!(ctx, latest.payload_ref)

    assert {:ok, pins} =
             Ferricstore.Flow.LMDB.segment_value_pin_entries_before(
               Ferricstore.Flow.LMDB.path(shard_data_path),
               103,
               10
             )

    assert pins == []

    history_path = Ferricstore.Flow.HistoryProjector.history_file_path(shard_data_path, 0)
    assert {:ok, history_records} = Ferricstore.Bitcask.NIF.v2_scan_file(history_path)

    history_record_keys =
      Enum.map(history_records, fn {key, _offset, _value_size, _expire_at_ms, _deleted?} ->
        key
      end)

    assert created.payload_ref in history_record_keys
    assert latest.payload_ref in history_record_keys
    assert apply_projection_cache_count(ctx, 0) == 0

    assert {:ok, history} =
             Ferricstore.Flow.history(ctx, id,
               partition_key: partition_key,
               count: 10,
               values: true
             )

    events = Map.new(history, fn {_event_id, fields} -> {fields["event"], fields} end)
    assert events["created"]["payload"] == initial_payload
    assert events["claimed"]["payload"] == initial_payload
    assert events["transitioned"]["payload"] == next_payload
  end

  test "async history skips copying blob-backed generated values already projected to LMDB" do
    old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)
    old_async_history = Application.get_env(:ferricstore, :flow_async_history)

    old_flush_interval =
      Application.get_env(:ferricstore, :flow_history_projector_flush_interval_ms)

    Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)
    Application.put_env(:ferricstore, :flow_async_history, true)
    Application.put_env(:ferricstore, :flow_history_projector_flush_interval_ms, 60_000)

    ctx =
      Ferricstore.Test.IsolatedInstance.checkout(
        shard_count: 1,
        hot_cache_max_value_size: 1,
        blob_side_channel_threshold_bytes: 128
      )

    on_exit(fn ->
      Ferricstore.Test.IsolatedInstance.checkin(ctx)
      restore_env(:flow_lmdb_mode, old_mode)
      restore_env(:flow_async_history, old_async_history)
      restore_env(:flow_history_projector_flush_interval_ms, old_flush_interval)
    end)

    id = "history-skip-blob-copy"
    partition_key = "tenant-history-skip-blob-copy"
    flow_type = "history-skip-blob-copy"
    payload = String.duplicate("p", 1024)

    assert :ok =
             Ferricstore.Flow.create(ctx, id,
               type: flow_type,
               partition_key: partition_key,
               payload: payload,
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert {:ok, created} = Ferricstore.Flow.get(ctx, id, partition_key: partition_key)
    flush_shard!(ctx, 0)

    assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, 1)
    assert :ok = Ferricstore.Flow.HistoryProjector.flush(ctx, 0, 120_000)

    refute_keydir_row!(ctx, created.payload_ref)

    shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0)
    history_path = Ferricstore.Flow.HistoryProjector.history_file_path(shard_path, 0)
    assert {:ok, history_records} = Ferricstore.Bitcask.NIF.v2_scan_file(history_path)

    history_record_keys =
      Enum.map(history_records, fn {key, _offset, _value_size, _expire_at_ms, _deleted?} ->
        key
      end)

    refute created.payload_ref in history_record_keys

    assert {:ok, [^payload]} = Ferricstore.Flow.value_mget(ctx, [created.payload_ref])

    assert {:ok, history} =
             Ferricstore.Flow.history(ctx, id,
               partition_key: partition_key,
               count: 10,
               values: true
             )

    events = Map.new(history, fn {_event_id, fields} -> {fields["event"], fields} end)
    assert events["created"]["payload"] == payload
  end

  test "async history keeps generated value refs readable after LMDB projection" do
    old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)
    old_async_history = Application.get_env(:ferricstore, :flow_async_history)

    old_flush_interval =
      Application.get_env(:ferricstore, :flow_history_projector_flush_interval_ms)

    Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)
    Application.put_env(:ferricstore, :flow_async_history, true)
    Application.put_env(:ferricstore, :flow_history_projector_flush_interval_ms, 60_000)

    ctx = Ferricstore.Test.IsolatedInstance.checkout(shard_count: 1, hot_cache_max_value_size: 1)

    on_exit(fn ->
      Ferricstore.Test.IsolatedInstance.checkin(ctx)
      restore_env(:flow_lmdb_mode, old_mode)
      restore_env(:flow_async_history, old_async_history)
      restore_env(:flow_history_projector_flush_interval_ms, old_flush_interval)
    end)

    id = "history-compact-value-ref"
    partition_key = "tenant-history-compact-value-ref"
    flow_type = "history-compact-value-ref"
    initial_payload = String.duplicate("p", 1024)
    result_payload = String.duplicate("r", 1024)

    assert :ok =
             Ferricstore.Flow.create(ctx, id,
               type: flow_type,
               partition_key: partition_key,
               payload: initial_payload,
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert {:ok, created} =
             Ferricstore.Flow.get(ctx, id, partition_key: partition_key)

    assert {:ok, [claimed]} =
             Ferricstore.Flow.claim_due(ctx, flow_type,
               worker: "worker-history-compact-value-ref",
               partition_key: partition_key,
               lease_ms: 30_000,
               limit: 1,
               now_ms: 1_001
             )

    assert :ok =
             Ferricstore.Flow.complete(ctx, id, claimed.lease_token,
               partition_key: partition_key,
               fencing_token: claimed.fencing_token,
               result: result_payload,
               now_ms: 1_002
             )

    assert {:ok, completed} =
             Ferricstore.Flow.get(ctx, id, partition_key: partition_key)

    flush_shard!(ctx, 0)
    state_key = Ferricstore.Flow.Keys.state_key(id, partition_key)

    case :ets.lookup(elem(ctx.keydir_refs, 0), state_key) do
      [] ->
        assert apply_projection_cache_count(ctx, 0) == 0

      [_row] ->
        state_value = materialize_keydir_value!(ctx, 0, state_key)
        set_apply_projection_value!(ctx, state_key, state_value, 202)
        assert apply_projection_cache_count(ctx, 0) == 1
    end

    assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, 1)
    assert :ok = Ferricstore.Flow.HistoryProjector.flush(ctx, 0, 120_000)

    assert {:ok, [initial_payload, result_payload]} =
             Ferricstore.Flow.value_mget(ctx, [created.payload_ref, completed.result_ref])

    assert {:ok, %{payload: ^initial_payload, result: ^result_payload}} =
             Ferricstore.Flow.get(ctx, id, partition_key: partition_key, full: true)

    assert {:ok, history} =
             Ferricstore.Flow.history(ctx, id,
               partition_key: partition_key,
               count: 10,
               values: true
             )

    events = Map.new(history, fn {_event_id, fields} -> {fields["event"], fields} end)
    assert events["created"]["payload"] == initial_payload
  end

  test "async history keeps generated named value refs readable after LMDB projection" do
    old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)
    old_async_history = Application.get_env(:ferricstore, :flow_async_history)

    old_flush_interval =
      Application.get_env(:ferricstore, :flow_history_projector_flush_interval_ms)

    Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)
    Application.put_env(:ferricstore, :flow_async_history, true)
    Application.put_env(:ferricstore, :flow_history_projector_flush_interval_ms, 60_000)

    ctx = Ferricstore.Test.IsolatedInstance.checkout(shard_count: 1, hot_cache_max_value_size: 1)

    on_exit(fn ->
      Ferricstore.Test.IsolatedInstance.checkin(ctx)
      restore_env(:flow_lmdb_mode, old_mode)
      restore_env(:flow_async_history, old_async_history)
      restore_env(:flow_history_projector_flush_interval_ms, old_flush_interval)
    end)

    id = "history-compact-named-value-ref"
    partition_key = "tenant-history-compact-named-value-ref"
    flow_type = "history-compact-named-value-ref"
    doc = String.duplicate("d", 1024)

    assert :ok =
             Ferricstore.Flow.create(ctx, id,
               type: flow_type,
               partition_key: partition_key,
               values: %{"doc" => doc},
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert {:ok, created} =
             Ferricstore.Flow.get(ctx, id, partition_key: partition_key)

    doc_ref = created.value_refs["doc"].ref
    assert is_binary(doc_ref)

    flush_shard!(ctx, 0)
    assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, 1)
    assert :ok = Ferricstore.Flow.HistoryProjector.flush(ctx, 0, 120_000)

    assert {:ok, [^doc]} = Ferricstore.Flow.value_mget(ctx, [doc_ref])

    assert {:ok, hydrated} =
             Ferricstore.Flow.get(ctx, id, partition_key: partition_key, values: true)

    assert hydrated.values == %{"doc" => doc}
  end

  test "retention cleanup removes value refs that only survive in cold LMDB history" do
    old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)
    old_async_history = Application.get_env(:ferricstore, :flow_async_history)

    old_flush_interval =
      Application.get_env(:ferricstore, :flow_history_projector_flush_interval_ms)

    Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)
    Application.put_env(:ferricstore, :flow_async_history, true)
    Application.put_env(:ferricstore, :flow_history_projector_flush_interval_ms, 60_000)

    ctx = Ferricstore.Test.IsolatedInstance.checkout(shard_count: 1, hot_cache_max_value_size: 1)

    on_exit(fn ->
      Ferricstore.Test.IsolatedInstance.checkin(ctx)
      restore_env(:flow_lmdb_mode, old_mode)
      restore_env(:flow_async_history, old_async_history)
      restore_env(:flow_history_projector_flush_interval_ms, old_flush_interval)
    end)

    id = "retention-cold-history-ref"
    partition_key = "tenant-retention-cold-history-ref"
    flow_type = "retention-cold-history-ref"
    initial_payload = String.duplicate("i", 1024)
    next_payload = String.duplicate("n", 1024)
    now = System.system_time(:millisecond)

    assert :ok =
             Ferricstore.Flow.create(ctx, id,
               type: flow_type,
               partition_key: partition_key,
               payload: initial_payload,
               history_hot_max_events: 0,
               retention_ttl_ms: 1,
               run_at_ms: now,
               now_ms: now
             )

    assert {:ok, created} = Ferricstore.Flow.get(ctx, id, partition_key: partition_key)

    assert {:ok, [first_claim]} =
             Ferricstore.Flow.claim_due(ctx, flow_type,
               worker: "worker-retention-cold-history-ref-1",
               partition_key: partition_key,
               lease_ms: 30_000,
               limit: 1,
               now_ms: now + 1
             )

    assert :ok =
             Ferricstore.Flow.transition(ctx, id, "running", "waiting",
               partition_key: partition_key,
               lease_token: first_claim.lease_token,
               fencing_token: first_claim.fencing_token,
               payload: next_payload,
               run_at_ms: now + 2,
               now_ms: now + 2
             )

    assert {:ok, transitioned} = Ferricstore.Flow.get(ctx, id, partition_key: partition_key)
    assert transitioned.payload_ref != created.payload_ref

    assert {:ok, [second_claim]} =
             Ferricstore.Flow.claim_due(ctx, flow_type,
               worker: "worker-retention-cold-history-ref-2",
               partition_key: partition_key,
               lease_ms: 30_000,
               limit: 1,
               now_ms: now + 3
             )

    assert :ok =
             Ferricstore.Flow.complete(ctx, id, second_claim.lease_token,
               partition_key: partition_key,
               fencing_token: second_claim.fencing_token,
               now_ms: now + 4
             )

    assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, 1)
    assert :ok = Ferricstore.Flow.HistoryProjector.flush(ctx, 0, 120_000)

    refute_keydir_row!(ctx, created.payload_ref)

    assert {:ok, [^initial_payload, ^next_payload]} =
             Ferricstore.Flow.value_mget(ctx, [created.payload_ref, transitioned.payload_ref])

    Process.sleep(5)

    assert {:ok, cleaned} =
             Ferricstore.Flow.retention_cleanup(ctx, limit: 10, now_ms: now + 10_000)

    assert cleaned.flows == 1
    assert cleaned.history >= 1
    assert cleaned.values >= 2

    assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, 1)

    assert {:ok, [nil, nil]} =
             Ferricstore.Flow.value_mget(ctx, [created.payload_ref, transitioned.payload_ref])
  end

  test "retention cleanup still deletes values after terminal query sweep" do
    old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)
    old_async_history = Application.get_env(:ferricstore, :flow_async_history)

    Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)
    Application.put_env(:ferricstore, :flow_async_history, true)

    ctx = Ferricstore.Test.IsolatedInstance.checkout(shard_count: 1, hot_cache_max_value_size: 1)

    on_exit(fn ->
      Ferricstore.Test.IsolatedInstance.checkin(ctx)
      restore_env(:flow_lmdb_mode, old_mode)
      restore_env(:flow_async_history, old_async_history)
    end)

    id = "retention-after-terminal-sweep"
    partition_key = "tenant-retention-after-terminal-sweep"
    flow_type = "retention-after-terminal-sweep"
    payload = String.duplicate("p", 1024)
    now = System.system_time(:millisecond)

    assert :ok =
             Ferricstore.Flow.create(ctx, id,
               type: flow_type,
               partition_key: partition_key,
               payload: payload,
               history_hot_max_events: 0,
               retention_ttl_ms: 1,
               run_at_ms: now,
               now_ms: now
             )

    assert {:ok, created} = Ferricstore.Flow.get(ctx, id, partition_key: partition_key)

    assert {:ok, [claimed]} =
             Ferricstore.Flow.claim_due(ctx, flow_type,
               worker: "worker-retention-after-terminal-sweep",
               partition_key: partition_key,
               lease_ms: 30_000,
               limit: 1,
               now_ms: now + 1
             )

    assert :ok =
             Ferricstore.Flow.complete(ctx, id, claimed.lease_token,
               partition_key: partition_key,
               fencing_token: claimed.fencing_token,
               now_ms: now + 2
             )

    assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, 1)
    assert :ok = Ferricstore.Flow.HistoryProjector.flush(ctx, 0, 120_000)

    lmdb_path = Ferricstore.Flow.LMDB.path(Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0))
    state_key = Ferricstore.Flow.Keys.state_key(id, partition_key)

    Ferricstore.Test.ShardHelpers.eventually(
      fn -> [] == :ets.lookup(elem(ctx.keydir_refs, 0), state_key) end,
      "terminal state key was not hot-pruned",
      200,
      10
    )

    assert {:ok, 1} = Ferricstore.Flow.LMDB.sweep_expired_terminal(lmdb_path, now + 10_000, 100)

    assert {:ok, [state_key]} ==
             Ferricstore.Flow.LMDB.expired_terminal_state_keys(lmdb_path, now + 10_000, 100)

    assert {:ok, cleaned} =
             Ferricstore.Flow.retention_cleanup(ctx, limit: 10, now_ms: now + 10_000)

    assert cleaned.flows == 1
    assert cleaned.values >= 1
    assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, 1)
    assert {:ok, [nil]} = Ferricstore.Flow.value_mget(ctx, [created.payload_ref])
    assert :not_found = Ferricstore.Flow.LMDB.get(lmdb_path, state_key)
  end

  test "retention cleanup pages cold LMDB history without orphaning rows" do
    old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)
    old_async_history = Application.get_env(:ferricstore, :flow_async_history)
    old_scan_limit = Application.get_env(:ferricstore, :flow_lmdb_history_cleanup_scan_limit)

    Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)
    Application.put_env(:ferricstore, :flow_async_history, true)
    Application.put_env(:ferricstore, :flow_lmdb_history_cleanup_scan_limit, 1)

    ctx = Ferricstore.Test.IsolatedInstance.checkout(shard_count: 1, hot_cache_max_value_size: 1)

    on_exit(fn ->
      Ferricstore.Test.IsolatedInstance.checkin(ctx)
      restore_env(:flow_lmdb_mode, old_mode)
      restore_env(:flow_async_history, old_async_history)
      restore_env(:flow_lmdb_history_cleanup_scan_limit, old_scan_limit)
    end)

    id = "retention-cold-history-paged"
    partition_key = "tenant-retention-cold-history-paged"
    flow_type = "retention-cold-history-paged"
    now = System.system_time(:millisecond)

    assert :ok =
             Ferricstore.Flow.create(ctx, id,
               type: flow_type,
               partition_key: partition_key,
               history_hot_max_events: 0,
               retention_ttl_ms: 1,
               run_at_ms: now,
               now_ms: now
             )

    assert {:ok, [claimed]} =
             Ferricstore.Flow.claim_due(ctx, flow_type,
               worker: "worker-retention-cold-history-paged",
               partition_key: partition_key,
               lease_ms: 30_000,
               limit: 1,
               now_ms: now + 1
             )

    assert :ok =
             Ferricstore.Flow.complete(ctx, id, claimed.lease_token,
               partition_key: partition_key,
               fencing_token: claimed.fencing_token,
               now_ms: now + 2
             )

    assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, 1)
    assert :ok = Ferricstore.Flow.HistoryProjector.flush(ctx, 0, 120_000)

    history_key = Ferricstore.Flow.Keys.history_key(id, partition_key)
    state_key = Ferricstore.Flow.Keys.state_key(id, partition_key)
    lmdb_path = Ferricstore.Flow.LMDB.path(Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0))

    assert {:ok, history_count} =
             Ferricstore.Flow.LMDB.prefix_count(
               lmdb_path,
               Ferricstore.Flow.LMDB.history_index_prefix(history_key)
             )

    assert history_count > 1

    Process.sleep(5)

    assert {:ok, first} =
             Ferricstore.Flow.retention_cleanup(ctx, limit: 10, now_ms: now + 10_000)

    assert first.flows == 0
    assert first.history == 1

    assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, 1)
    assert {:ok, _state_blob} = Ferricstore.Flow.LMDB.get(lmdb_path, state_key)

    totals =
      Enum.reduce_while(1..10, %{flows: first.flows, history: first.history}, fn _, acc ->
        {:ok, cleaned} = Ferricstore.Flow.retention_cleanup(ctx, limit: 10, now_ms: now + 10_000)
        assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, 1)

        next = %{flows: acc.flows + cleaned.flows, history: acc.history + cleaned.history}

        if next.flows == 1 do
          {:halt, next}
        else
          {:cont, next}
        end
      end)

    assert totals.flows == 1
    assert totals.history == history_count
    assert :not_found = Ferricstore.Flow.LMDB.get(lmdb_path, state_key)

    assert {:ok, 0} =
             Ferricstore.Flow.LMDB.prefix_count(
               lmdb_path,
               Ferricstore.Flow.LMDB.history_index_prefix(history_key)
             )

    restart_isolated_shard!(ctx, 0)

    assert {:ok, nil} = Ferricstore.Flow.get(ctx, id, partition_key: partition_key)

    assert {:ok, []} =
             Ferricstore.Flow.history(ctx, id,
               partition_key: partition_key,
               count: 10,
               include_cold: true,
               consistent_projection: true
             )
  end

  test "retention cleanup caps cold LMDB history work per command" do
    old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)
    old_async_history = Application.get_env(:ferricstore, :flow_async_history)
    old_scan_limit = Application.get_env(:ferricstore, :flow_lmdb_history_cleanup_scan_limit)

    Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)
    Application.put_env(:ferricstore, :flow_async_history, true)
    Application.put_env(:ferricstore, :flow_lmdb_history_cleanup_scan_limit, 1)

    ctx = Ferricstore.Test.IsolatedInstance.checkout(shard_count: 1, hot_cache_max_value_size: 1)

    on_exit(fn ->
      Ferricstore.Test.IsolatedInstance.checkin(ctx)
      restore_env(:flow_lmdb_mode, old_mode)
      restore_env(:flow_async_history, old_async_history)
      restore_env(:flow_lmdb_history_cleanup_scan_limit, old_scan_limit)
    end)

    id = "retention-cold-history-capped"
    partition_key = "tenant-retention-cold-history-capped"
    flow_type = "retention-cold-history-capped"
    now = System.system_time(:millisecond)

    assert :ok =
             Ferricstore.Flow.create(ctx, id,
               type: flow_type,
               partition_key: partition_key,
               history_hot_max_events: 0,
               retention_ttl_ms: 1,
               run_at_ms: now,
               now_ms: now
             )

    assert {:ok, [claimed]} =
             Ferricstore.Flow.claim_due(ctx, flow_type,
               worker: "worker-retention-cold-history-capped",
               partition_key: partition_key,
               lease_ms: 30_000,
               limit: 1,
               now_ms: now + 1
             )

    assert :ok =
             Ferricstore.Flow.complete(ctx, id, claimed.lease_token,
               partition_key: partition_key,
               fencing_token: claimed.fencing_token,
               now_ms: now + 2
             )

    assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, 1)
    assert :ok = Ferricstore.Flow.HistoryProjector.flush(ctx, 0, 120_000)

    history_key = Ferricstore.Flow.Keys.history_key(id, partition_key)
    state_key = Ferricstore.Flow.Keys.state_key(id, partition_key)
    lmdb_path = Ferricstore.Flow.LMDB.path(Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0))

    assert {:ok, history_count} =
             Ferricstore.Flow.LMDB.prefix_count(
               lmdb_path,
               Ferricstore.Flow.LMDB.history_index_prefix(history_key)
             )

    assert history_count > 1

    Process.sleep(5)

    assert {:ok, first} =
             Ferricstore.Flow.retention_cleanup(ctx, limit: 10, now_ms: now + 10_000)

    assert first.flows == 0
    assert first.history == 1
    assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, 1)

    assert {:ok, remaining_after_first} =
             Ferricstore.Flow.LMDB.prefix_count(
               lmdb_path,
               Ferricstore.Flow.LMDB.history_index_prefix(history_key)
             )

    assert remaining_after_first == history_count - 1
    assert {:ok, _state_blob} = Ferricstore.Flow.LMDB.get(lmdb_path, state_key)

    totals =
      Enum.reduce_while(1..10, %{flows: first.flows, history: first.history}, fn _, acc ->
        {:ok, cleaned} = Ferricstore.Flow.retention_cleanup(ctx, limit: 10, now_ms: now + 10_000)
        next = %{flows: acc.flows + cleaned.flows, history: acc.history + cleaned.history}

        if next.flows == 1 do
          {:halt, next}
        else
          {:cont, next}
        end
      end)

    assert totals.flows == 1
    assert totals.history == history_count
    assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, 1)
    assert :not_found = Ferricstore.Flow.LMDB.get(lmdb_path, state_key)
  end
    end
  end
end
