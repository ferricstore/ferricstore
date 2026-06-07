defmodule Ferricstore.FlowTest.Sections.Part13 do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Test.ShardHelpers

  test "FLUSHDB clears Flow LMDB terminal projections" do
    ctx = FerricStore.Instance.get(:default)
    id = uid("flow-flushdb-lmdb-terminal")
    partition_key = uid("tenant-flushdb-lmdb-terminal")
    create_now = System.system_time(:millisecond) + 60_000
    complete_now = create_now + 1_000
    cleanup_now = complete_now + 1_000

    assert {:ok, _flow} =
             flow_create_and_get(id,
               type: "flushdb-lmdb-terminal",
               state: "queued",
               partition_key: partition_key,
               run_at_ms: create_now,
               retention_ttl_ms: 10,
               now_ms: create_now
             )

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due("flushdb-lmdb-terminal",
               worker: "worker-flushdb-lmdb-terminal",
               partition_key: partition_key,
               lease_ms: 30_000,
               limit: 1,
               now_ms: create_now
             )

    assert {:ok, completed} =
             flow_complete_and_get(id, claimed.lease_token,
               partition_key: partition_key,
               fencing_token: claimed.fencing_token,
               now_ms: complete_now
             )

    shard_index = shard_for(Ferricstore.Flow.Keys.state_key(id, partition_key))
    assert :ok = Ferricstore.Flow.HistoryProjector.flush(ctx, shard_index)
    assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, ctx.shard_count)

    lmdb_path =
      ctx.data_dir
      |> Ferricstore.DataDir.shard_data_path(shard_index)
      |> Ferricstore.Flow.LMDB.path()

    assert {:ok, [_ | _]} =
             Ferricstore.Flow.LMDB.expired_terminal_state_keys(lmdb_path, cleanup_now, 10)

    assert :ok = FerricStore.flushdb()
    assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, ctx.shard_count)

    assert {:ok, []} =
             Ferricstore.Flow.LMDB.expired_terminal_state_keys(lmdb_path, cleanup_now, 10)

    assert {:ok, %{flows: 0}} =
             Ferricstore.Store.Router.flow_retention_cleanup(ctx, %{
               limit: 10,
               now_ms: cleanup_now
             })

    assert FerricStore.flow_get(completed.id, partition_key: partition_key) == {:ok, nil}
  end

  test "retention cleanup deletes owned payload refs from trimmed history" do
    ctx = FerricStore.Instance.get(:default)
    id = uid("flow-history-trim-owned-value")
    create_now = System.system_time(:millisecond) + 60_000
    transition_now = create_now + 100
    complete_now = create_now + 1_000
    cleanup_now = complete_now + 1_000

    assert {:ok, created} =
             flow_create_and_get(id,
               type: "history-trim-owned-value",
               state: "queued",
               payload: "first-payload",
               run_at_ms: create_now,
               retention_ttl_ms: 10,
               history_hot_max_events: 0,
               history_max_events: 1,
               now_ms: create_now
             )

    first_ref = created.payload_ref

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due("history-trim-owned-value",
               worker: "worker-history-trim-owned-value",
               lease_ms: 30_000,
               limit: 1,
               now_ms: create_now
             )

    assert {:ok, transitioned} =
             flow_transition_and_get(id, "running", "ready",
               lease_token: claimed.lease_token,
               fencing_token: claimed.fencing_token,
               payload: "second-payload",
               run_at_ms: transition_now,
               now_ms: transition_now
             )

    second_ref = transitioned.payload_ref
    assert second_ref != first_ref

    shard_index = shard_for(Ferricstore.Flow.Keys.state_key(id, nil))
    assert :ok = Ferricstore.Flow.HistoryProjector.flush(ctx, shard_index)
    assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, ctx.shard_count)

    assert {:ok, [first_ref_value, "second-payload"]} =
             FerricStore.flow_value_mget([first_ref, second_ref])

    assert first_ref_value == "first-payload"

    assert {:ok, [ready]} =
             FerricStore.flow_claim_due("history-trim-owned-value",
               state: "ready",
               worker: "worker-history-trim-owned-value-2",
               lease_ms: 30_000,
               limit: 1,
               now_ms: transition_now
             )

    assert {:ok, completed} =
             flow_complete_and_get(id, ready.lease_token,
               fencing_token: ready.fencing_token,
               now_ms: complete_now
             )

    assert completed.terminal_retention_until_ms == complete_now + 10
    assert :ok = Ferricstore.Flow.HistoryProjector.flush(ctx, shard_index)
    assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, ctx.shard_count)

    assert {:ok, %{flows: 1}} =
             Ferricstore.Store.Router.flow_retention_cleanup(ctx, %{
               limit: 10,
               now_ms: cleanup_now
             })

    assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, ctx.shard_count)
    assert {:ok, [nil, nil]} = FerricStore.flow_value_mget([first_ref, second_ref])
  end

  test "terminal retention expires queryable flow history" do
    id = uid("flow-terminal-history-ttl")

    assert {:ok, _created} =
             flow_create_and_get(id,
               type: "history-ttl",
               payload: %{input: 1},
               run_at_ms: 1_000,
               retention_ttl_ms: 100,
               now_ms: 1_000
             )

    assert {:ok, [_created_event]} = FerricStore.flow_history(id, count: 10)

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due("history-ttl",
               worker: "worker-a",
               lease_ms: 30_000,
               limit: 1,
               now_ms: 1_000
             )

    assert {:ok, _} =
             flow_complete_and_get(id, claimed.lease_token, fencing_token: claimed.fencing_token)

    Process.sleep(150)

    assert {:ok, nil} = FerricStore.flow_get(id)
    assert {:ok, []} = FerricStore.flow_history(id, count: 10)
  end

  test "terminal ttl override must be positive" do
    id = uid("flow-terminal-ttl-zero")

    assert {:ok, _created} =
             flow_create_and_get(id,
               type: "ttl-zero",
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due("ttl-zero",
               worker: "worker-a",
               lease_ms: 30_000,
               limit: 1,
               now_ms: 1_000
             )

    assert {:error, "ERR flow ttl_ms must be a positive integer"} =
             flow_complete_and_get(id, claimed.lease_token,
               fencing_token: claimed.fencing_token,
               ttl_ms: 0
             )
  end

  test "flow create inherits retention defaults from policy" do
    type = uid("flow-retention-policy")
    id = uid("flow-retention-policy-id")

    assert {:ok, _policy} =
             FerricStore.flow_policy_set(type,
               retention: [ttl_ms: 5_000, history_max_events: 9]
             )

    assert {:ok, created} =
             flow_create_and_get(id, type: type, state: "queued", now_ms: 10)

    assert created.retention_ttl_ms == 5_000
    assert created.history_hot_max_events == 0
    assert created.history_max_events == 9
    assert created.terminal_retention_until_ms == nil
  end
    end
  end
end
