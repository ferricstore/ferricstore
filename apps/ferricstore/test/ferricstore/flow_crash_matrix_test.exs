defmodule Ferricstore.FlowCrashMatrixTest do
  use ExUnit.Case, async: false
  @moduletag :flow
  @moduletag :global_state

  @moduletag :crash_matrix
  @moduletag timeout: 240_000

  alias Ferricstore.Flow.HistoryProjector
  alias Ferricstore.Flow.LMDBWriter
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  setup do
    old_threshold = Application.get_env(:ferricstore, :blob_side_channel_threshold_bytes)
    old_reconcile = Application.get_env(:ferricstore, :blob_protection_reconcile_enabled)

    Application.put_env(:ferricstore, :blob_side_channel_threshold_bytes, 64)
    Application.put_env(:ferricstore, :blob_protection_reconcile_enabled, true)
    Ferricstore.FaultInjection.clear_hook()

    isolated = ShardHelpers.setup_isolated_data_dir()

    on_exit(fn ->
      Ferricstore.FaultInjection.clear_hook()
      restore_env(:blob_side_channel_threshold_bytes, old_threshold)
      restore_env(:blob_protection_reconcile_enabled, old_reconcile)
      ShardHelpers.teardown_isolated_data_dir(isolated)
    end)

    :ok
  end

  test "unknown outcome after WARaft commit recovers committed Flow state and hot due index" do
    type = unique("matrix-commit-type")
    partition = unique("matrix-commit-partition")
    id = unique("matrix-commit-flow")
    fault = install_blocking_fault(:after_waraft_commit)

    op =
      start_unlinked(fn ->
        FerricStore.flow_create(id,
          type: type,
          partition_key: partition,
          state: "queued",
          payload: "commit-payload",
          run_at_ms: 1_000,
          now_ms: 1_000,
          history_max_events: 10
        )
      end)

    {pid, _metadata} = await_fault(fault)
    Ferricstore.FaultInjection.clear_hook()

    id
    |> flow_shard(partition)
    |> ShardHelpers.kill_shard_safely(timeout: 45_000)

    Process.exit(pid, :kill)
    shutdown_op(op)
    ShardHelpers.wait_default_quorum_writable(60_000)

    claim = claim_one(type, partition, "queued", id)
    assert claim.payload == "commit-payload"
  end

  test "crash after WARaft apply projection write rebuilds visible Flow state" do
    type = unique("matrix-apply-type")
    partition = unique("matrix-apply-partition")
    id = unique("matrix-apply-flow")
    fault = install_blocking_fault(:after_waraft_apply_projection_write)

    op =
      start_unlinked(fn ->
        FerricStore.flow_create(id,
          type: type,
          partition_key: partition,
          state: "queued",
          payload: "apply-payload",
          run_at_ms: 1_000,
          now_ms: 1_000,
          history_max_events: 10
        )
      end)

    {pid, _metadata} = await_fault(fault)
    Ferricstore.FaultInjection.clear_hook()
    kill_process(pid)
    shutdown_op(op)
    ShardHelpers.wait_default_quorum_writable(60_000)

    ensure_created_or_retry(type, partition, id, "apply-payload")
    claim = claim_one(type, partition, "queued", id)
    assert claim.payload == "apply-payload"
  end

  test "crash during blob side-channel write leaves command retryable and blobs collectible" do
    type = unique("matrix-blob-type")
    partition = unique("matrix-blob-partition")
    id = unique("matrix-blob-flow")
    payload = :binary.copy("blob-payload:", 128)
    fault = install_blocking_fault(:after_blob_store_write)

    op =
      start_unlinked(fn ->
        FerricStore.flow_create(id,
          type: type,
          partition_key: partition,
          state: "queued",
          payload: payload,
          run_at_ms: 1_000,
          now_ms: 1_000,
          history_max_events: 10
        )
      end)

    {pid, _metadata} = await_fault(fault)
    Ferricstore.FaultInjection.clear_hook()
    kill_process(pid)
    shutdown_op(op)
    ShardHelpers.wait_default_quorum_writable(60_000)

    assert :ok =
             FerricStore.flow_create(id,
               type: type,
               partition_key: partition,
               state: "queued",
               payload: payload,
               run_at_ms: 1_000,
               now_ms: 1_000,
               history_max_events: 10
             )

    claim = claim_one(type, partition, "queued", id)
    assert claim.payload == payload
    assert {:ok, _stats} = Router.sweep_blob_garbage(FerricStore.Instance.get(:default))
  end

  test "LMDB writer crash during flush replays terminal projection from durable truth" do
    type = unique("matrix-lmdb-type")
    partition = unique("matrix-lmdb-partition")
    id = unique("matrix-lmdb-flow")
    shard = flow_shard(id, partition)

    create_and_complete(type, partition, id, result: "lmdb-result")
    fault = install_blocking_fault(:before_flow_lmdb_flush_write)
    state_key = Ferricstore.Flow.Keys.state_key(id, partition)

    assert :ok =
             LMDBWriter.enqueue(:default, shard, [{:project_flow_state_from_source, state_key}])

    op =
      start_unlinked(fn ->
        LMDBWriter.flush(:default, shard, 45_000)
      end)

    {pid, _metadata} = await_fault(fault)
    Ferricstore.FaultInjection.clear_hook()
    kill_process(pid)
    shutdown_op(op)
    wait_named_restart(LMDBWriter.name(:default, shard), pid)

    assert :ok = LMDBWriter.flush(:default, shard, 45_000)

    assert {:ok, terminals} =
             FerricStore.flow_terminals(type,
               partition_key: partition,
               state: "completed",
               count: 10,
               include_cold: true,
               consistent_projection: true
             )

    assert Enum.any?(terminals, &(&1.id == id))
  end

  test "history projector crash after fsync replays history and preserves values" do
    type = unique("matrix-history-type")
    partition = unique("matrix-history-partition")
    id = unique("matrix-history-flow")
    shard = flow_shard(id, partition)

    create_and_complete(type, partition, id, result: :binary.copy("history-result:", 128))
    fault = install_blocking_fault(:after_flow_history_fsync)

    op =
      start_unlinked(fn ->
        HistoryProjector.flush(FerricStore.Instance.get(:default), shard, 45_000)
      end)

    {pid, _metadata} = await_fault(fault)
    Ferricstore.FaultInjection.clear_hook()
    kill_process(pid)
    shutdown_op(op)
    wait_named_restart(HistoryProjector.name(FerricStore.Instance.get(:default), shard), pid)
    ShardHelpers.kill_shard_safely(shard, timeout: 45_000)

    assert :ok = HistoryProjector.flush(FerricStore.Instance.get(:default), shard, 45_000)

    assert {:ok, history} =
             FerricStore.flow_history(id,
               partition_key: partition,
               count: 20,
               include_cold: true,
               consistent_projection: true,
               values: true
             )

    events = Enum.map(history, fn {_event_id, fields} -> fields["event"] end)
    assert "created" in events
    assert "completed" in events
  end

  test "blob GC crash after live-ref scan does not delete live Flow blobs" do
    type = unique("matrix-gc-type")
    partition = unique("matrix-gc-partition")
    id = unique("matrix-gc-flow")
    result = :binary.copy("gc-result:", 128)

    create_and_complete(type, partition, id, result: result)
    ShardHelpers.flush_all_shards()

    fault = install_blocking_fault(:after_blob_gc_live_refs)
    op = start_unlinked(fn -> Router.sweep_blob_garbage(FerricStore.Instance.get(:default)) end)

    {pid, _metadata} = await_fault(fault)
    Ferricstore.FaultInjection.clear_hook()
    kill_process(pid)
    shutdown_op(op)

    assert {:ok, _stats} = Router.sweep_blob_garbage(FerricStore.Instance.get(:default))

    assert {:ok, record} = FerricStore.flow_get(id, partition_key: partition, full: true)
    assert record.state == "completed"
    assert record.result == result
  end

  defp install_blocking_fault(point) do
    ref = make_ref()
    owner = self()
    hits = :atomics.new(1, signed: false)

    Ferricstore.FaultInjection.put_hook(fn
      ^point, metadata ->
        if :atomics.add_get(hits, 1, 1) == 1 do
          send(owner, {:flow_crash_matrix_fault, ref, point, self(), metadata})

          receive do
            {:flow_crash_matrix_continue, ^ref} -> :ok
          after
            30_000 -> :ok
          end
        else
          :ok
        end

      _other, _metadata ->
        :ok
    end)

    ref
  end

  defp await_fault(ref) do
    receive do
      {:flow_crash_matrix_fault, ^ref, _point, pid, metadata} -> {pid, metadata}
    after
      30_000 -> flunk("fault injection point was not reached")
    end
  end

  defp kill_process(pid) when is_pid(pid) do
    monitor = Process.monitor(pid)
    Process.exit(pid, :kill)

    receive do
      {:DOWN, ^monitor, :process, ^pid, _reason} -> :ok
    after
      5_000 -> flunk("fault target #{inspect(pid)} did not exit")
    end
  end

  defp start_unlinked(fun) when is_function(fun, 0) do
    owner = self()
    ref = make_ref()

    {pid, monitor} =
      spawn_monitor(fn ->
        send(owner, {:flow_crash_matrix_op_result, ref, fun.()})
      end)

    %{pid: pid, monitor: monitor, ref: ref}
  end

  defp shutdown_op(%{pid: pid, monitor: monitor, ref: ref}) do
    if Process.alive?(pid), do: Process.exit(pid, :kill)

    receive do
      {:DOWN, ^monitor, :process, ^pid, _reason} -> :ok
      {:flow_crash_matrix_op_result, ^ref, _result} -> await_op_down(monitor, pid)
    after
      1_000 -> Process.demonitor(monitor, [:flush])
    end

    :ok
  end

  defp await_op_down(monitor, pid) do
    receive do
      {:DOWN, ^monitor, :process, ^pid, _reason} -> :ok
    after
      1_000 -> Process.demonitor(monitor, [:flush])
    end
  end

  defp wait_named_restart(name, old_pid) do
    ShardHelpers.eventually(
      fn ->
        case Process.whereis(name) do
          pid when is_pid(pid) -> pid != old_pid and Process.alive?(pid)
          _other -> false
        end
      end,
      "#{inspect(name)} should restart after crash",
      300,
      100
    )
  end

  defp create_and_complete(type, partition, id, opts) do
    result = Keyword.fetch!(opts, :result)

    assert :ok =
             FerricStore.flow_create(id,
               type: type,
               partition_key: partition,
               state: "queued",
               payload: :binary.copy("payload:", 128),
               run_at_ms: 1_000,
               now_ms: 1_000,
               history_max_events: 20
             )

    claim = claim_one(type, partition, "queued", id)

    assert :ok =
             FerricStore.flow_complete(id, claim.lease_token,
               partition_key: partition,
               fencing_token: claim.fencing_token,
               result: result,
               now_ms: 1_100
             )

    :ok
  end

  defp ensure_created_or_retry(type, partition, id, payload) do
    case FerricStore.flow_get(id, partition_key: partition, full: true) do
      {:ok, %{state: "queued", payload: ^payload}} ->
        :ok

      {:ok, nil} ->
        assert :ok =
                 FerricStore.flow_create(id,
                   type: type,
                   partition_key: partition,
                   state: "queued",
                   payload: payload,
                   run_at_ms: 1_000,
                   now_ms: 1_000,
                   history_max_events: 10
                 )

      other ->
        flunk("unexpected flow_get result after apply projection crash: #{inspect(other)}")
    end
  end

  defp claim_one(type, partition, state, id, attempts \\ 100)

  defp claim_one(type, partition, state, id, attempts) when attempts > 0 do
    case FerricStore.flow_claim_due(type,
           partition_key: partition,
           state: state,
           worker: unique("matrix-worker"),
           limit: 1,
           now_ms: 2_000
         ) do
      {:ok, [%{id: ^id} = claim]} ->
        claim

      {:ok, []} ->
        Process.sleep(100)
        claim_one(type, partition, state, id, attempts - 1)

      other ->
        flunk("unexpected claim_due result: #{inspect(other)}")
    end
  end

  defp claim_one(_type, _partition, _state, id, _attempts) do
    flunk("flow #{id} was not claimable after recovery")
  end

  defp flow_shard(id, partition) do
    Router.shard_for(
      FerricStore.Instance.get(:default),
      Ferricstore.Flow.Keys.state_key(id, partition)
    )
  end

  defp restore_env(key, nil), do: Application.delete_env(:ferricstore, key)
  defp restore_env(key, value), do: Application.put_env(:ferricstore, key, value)

  defp unique(prefix), do: "#{prefix}-#{System.unique_integer([:positive])}"
end
