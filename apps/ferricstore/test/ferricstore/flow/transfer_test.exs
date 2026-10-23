defmodule Ferricstore.Flow.TransferTest do
  use ExUnit.Case, async: false

  alias FerricStore.Impl
  alias Ferricstore.Flow.Transfer
  alias Ferricstore.Test.{FlowTransferFixture, IsolatedInstance}

  @moduletag :flow

  setup do
    source = IsolatedInstance.checkout()
    target = IsolatedInstance.checkout()

    path =
      Path.join(System.tmp_dir!(), "flow_transfer_#{System.unique_integer([:positive])}.ffx")

    on_exit(fn ->
      IsolatedInstance.checkin(source)
      IsolatedInstance.checkin(target)
      File.rm(path)
    end)

    now_ms = System.system_time(:millisecond)
    manifest = FlowTransferFixture.build!(source, "tx", now_ms)
    {:ok, source: source, target: target, path: path, manifest: manifest, now_ms: now_ms}
  end

  test "a restored store shows the same runs, history, values and policies", ctx do
    before = FlowTransferFixture.snapshot(ctx.source, ctx.manifest)
    # The export must read runs off every tier: a hibernated wait and terminal
    # runs live outside the keydir by now.
    :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.source.name, ctx.source.shard_count, 60_000)

    for id <- ["tx:waiting", "tx:done", "tx:failed"],
        do: refute(in_keydir?(ctx.source, id, "tx:tenant-a"), "#{id} is still in the keydir")

    assert in_keydir?(ctx.source, "tx:queued", "tx:tenant-a")

    # Schedules are runs too; creating one installs its type's policy.
    assert {:ok, counts} = Transfer.export(ctx.source, ctx.path)
    assert counts.flows == length(ctx.manifest.flows) + length(ctx.manifest.schedules)
    assert counts.shared_values == length(ctx.manifest.shared_refs)
    assert counts.policies == length(ctx.manifest.types) + 1

    assert {:ok, ^counts} = Transfer.restore(ctx.target, ctx.path)

    assert FlowTransferFixture.snapshot(ctx.target, ctx.manifest) == before
  end

  # A run whose payload cannot be read must not leave the export without it.
  test "an export fails when a value it carries cannot be read", ctx do
    source = ctx.source
    id = "tx:unreadable"

    assert :ok =
             Impl.flow_create(source, id,
               type: "tx.unreadable",
               state: "queued",
               partition_key: "tx:tenant-a",
               payload: %{"order" => 42},
               run_at_ms: ctx.now_ms,
               now_ms: ctx.now_ms
             )

    assert {:ok, %{payload_ref: ref}} = Impl.flow_get(source, id, partition_key: "tx:tenant-a")
    shard_index = Ferricstore.Store.Router.shard_for(source, ref)
    shard_path = Ferricstore.DataDir.shard_data_path(source.data_dir, shard_index)
    keydir = elem(source.keydir_refs, shard_index)

    # The history projector moves the payload into its log; corrupt that copy.
    Enum.find(1..500, fn _ ->
      :ok = Ferricstore.Flow.HistoryProjector.flush(source, shard_index)
      :ets.lookup(keydir, ref) == [] or (Process.sleep(10) && false)
    end)

    assert {:ok, blob} = Ferricstore.Flow.LMDB.get(Ferricstore.Flow.LMDB.path(shard_path), ref)

    assert {:ok, {:flow_value_locator, 1, _expire, {:flow_history, file_id}, offset, _size}} =
             Ferricstore.TermCodec.decode(blob)

    log = Path.join([shard_path, "history", String.pad_leading("#{file_id}", 5, "0") <> ".log"])
    {:ok, file} = :file.open(log, [:read, :write, :binary])
    :ok = :file.pwrite(file, offset, :binary.copy(<<0xFF>>, 8))
    :ok = :file.close(file)

    assert {:error, {:run_export_failed, _state_key, {:storage_read_failed, _reason}}} =
             Transfer.export(source, ctx.path)
    refute File.exists?(ctx.path)
  end

  test "restored runs keep working: FIFO order, leases, hibernated waits, signals", ctx do
    {:ok, _} = Transfer.export(ctx.source, ctx.path)
    {:ok, _} = Transfer.restore(ctx.target, ctx.path)

    pk_b = "tx:tenant-b"

    # A run created after the restore joins the lane behind the restored ones.
    assert :ok =
             Impl.flow_create(ctx.target, "tx:fifo-4",
               type: "tx.fifo",
               state: "queued",
               partition_key: pk_b,
               now_ms: ctx.now_ms + 10
             )

    # FIFO hands out one run per lane at a time: walk the lane head by head.
    claimed =
      for _n <- 1..4 do
        assert {:ok, [job]} =
                 Impl.flow_claim_due(ctx.target, "tx.fifo",
                   state: "queued",
                   worker: "after-restore",
                   lease_ms: 60_000,
                   partition_key: pk_b,
                   limit: 1
                 )

        assert :ok =
                 Impl.flow_complete(ctx.target, job.id, job.lease_token,
                   partition_key: pk_b,
                   fencing_token: job.fencing_token
                 )

        job.id
      end

    assert claimed == ["tx:fifo-1", "tx:fifo-2", "tx:fifo-3", "tx:fifo-4"]

    {:ok, running} =
      Impl.flow_get(ctx.source, "tx:running", partition_key: "tx:tenant-a")

    assert :ok =
             Impl.flow_complete(ctx.target, "tx:running", running.lease_token,
               partition_key: "tx:tenant-a",
               fencing_token: running.fencing_token,
               result: "done after restore"
             )

    assert {:ok, %{state: "completed"}} =
             Impl.flow_get(ctx.target, "tx:running", partition_key: "tx:tenant-a")

    assert {:ok, %{state: "waiting"}} =
             Impl.flow_get(ctx.target, "tx:waiting", partition_key: "tx:tenant-a")

    # The restored schedule still fires its target run.
    fire_at_ms = ctx.now_ms + 3_600_001

    assert {:ok, %{fired: 1}} =
             Impl.flow_schedule_fire(ctx.target, "tx:schedule",
               now_ms: fire_at_ms,
               fire_at_ms: fire_at_ms
             )

    assert {:ok, %{type: "tx.order"}} =
             Impl.flow_get(ctx.target, "tx:scheduled-target", partition_key: "tx:tenant-a")

    # Only the restored idempotency row can make the first signal a conflict.
    assert {:error, "ERR flow signal idempotency conflict"} =
             Ferricstore.Flow.signal(ctx.target, "tx:waiting",
               partition_key: "tx:tenant-a",
               signal: "another-signal",
               idempotency_key: "nudge-1"
             )

    assert :ok =
             Ferricstore.Flow.signal(ctx.target, "tx:waiting",
               partition_key: "tx:tenant-a",
               signal: "nudge",
               idempotency_key: "nudge-1",
               payload: %{"x" => 1}
             )
  end

  test "the named value's link row is restored", ctx do
    {:ok, _} = Transfer.export(ctx.source, ctx.path)
    {:ok, _} = Transfer.restore(ctx.target, ctx.path)

    {:ok, %{value_refs: %{"doc" => %{ref: ref, version: version}}}} =
      Impl.flow_get(ctx.source, "tx:queued", partition_key: "tx:tenant-a")

    link_key =
      Ferricstore.Flow.Keys.shared_value_link_key("tx:queued", "doc", version, "tx:tenant-a")

    assert Ferricstore.Flow.ValueStore.raw_mget_any(ctx.source, [link_key]) == [ref]
    assert Ferricstore.Flow.ValueStore.raw_mget_any(ctx.target, [link_key]) == [ref]
  end

  test "long histories and many values restore in bounded chunks", ctx do
    pk = "tx:tenant-long"

    :ok =
      Impl.flow_create(ctx.source, "tx:long",
        type: "tx.order",
        state: "waiting",
        partition_key: pk,
        payload: "v0",
        run_at_ms: ctx.now_ms + 3_600_000,
        now_ms: ctx.now_ms
      )

    for n <- 1..60 do
      :ok =
        Ferricstore.Flow.signal(ctx.source, "tx:long",
          partition_key: pk,
          signal: "tick-#{n}",
          values: %{"step-#{n}" => "value #{n}"}
        )
    end

    before = FlowTransferFixture.snapshot(ctx.source, %{ctx.manifest | flows: [{"tx:long", pk}]})
    assert length(before.flows[{"tx:long", pk}].history) > 60

    history_events = length(before.flows[{"tx:long", pk}].history)
    {:ok, _} = Transfer.export(ctx.source, ctx.path)

    test_pid = self()
    handler = {:transfer_chunks, self(), make_ref()}

    :ok =
      :telemetry.attach(
        handler,
        [:ferricstore, :flow, :apply],
        fn
          _event, _measurements, %{command_shape: :flow_restore}, pid ->
            send(pid, :restore_command)

          _event, _measurements, _metadata, _pid ->
            :ok
        end,
        test_pid
      )

    try do
      {:ok, counts} = Transfer.restore(ctx.target, ctx.path, chunk_events: 7, chunk_bytes: 2_048)

      # One command per run and shared value, plus at least one more for every
      # further 7 history events of the long run.
      assert restore_commands() >=
               counts.flows + counts.shared_values + div(history_events, 7) - 1
    after
      :telemetry.detach(handler)
    end

    assert FlowTransferFixture.snapshot(ctx.target, %{ctx.manifest | flows: [{"tx:long", pk}]}) ==
             before
  end

  defp restore_commands(count \\ 0) do
    receive do
      :restore_command -> restore_commands(count + 1)
    after
      200 -> count
    end
  end

  test "payloads stored as blobs travel as their bytes and are stored as blobs again", ctx do
    source = IsolatedInstance.checkout(blob_side_channel_threshold_bytes: 1_024)
    target = IsolatedInstance.checkout(blob_side_channel_threshold_bytes: 1_024)

    on_exit(fn ->
      IsolatedInstance.checkin(source)
      IsolatedInstance.checkin(target)
    end)

    payload = :crypto.strong_rand_bytes(8_192) |> Base.encode64()

    :ok =
      Impl.flow_create(source, "tx:big",
        type: "tx.big",
        state: "queued",
        partition_key: "tx:tenant-big",
        payload: payload,
        now_ms: ctx.now_ms
      )

    assert blob_bytes(source) > 8_000

    {:ok, _} = Transfer.export(source, ctx.path)
    {:ok, _} = Transfer.restore(target, ctx.path)

    assert {:ok, %{payload: ^payload}} =
             Impl.flow_get(target, "tx:big", partition_key: "tx:tenant-big", payload: true)

    assert blob_bytes(target) > 8_000
  end

  # Bytes in the instance's blob segments: large values live there.
  defp blob_bytes(instance) do
    Path.join([instance.data_dir, "blob", "**", "*"])
    |> Path.wildcard()
    |> Enum.filter(&File.regular?/1)
    |> Enum.map(&File.stat!(&1).size)
    |> Enum.sum()
  end

  test "governance state moves with the store and keeps working", ctx do
    gov = FlowTransferFixture.build_governance!(ctx.source, "gx", ctx.now_ms)
    running_scope = FlowTransferFixture.running_limit_scope(gov.type)
    await_running_in_use!(ctx.source, gov, running_scope, 1)

    before = FlowTransferFixture.governance_snapshot(ctx.source, gov)
    assert {:ok, _counts} = Transfer.export(ctx.source, ctx.path)
    assert {:ok, _counts} = Transfer.restore(ctx.target, ctx.path)

    # Policy generations start over in the target, so a running limit takes its
    # configuration from the target's policy again on the next claim.
    restored = FlowTransferFixture.governance_snapshot(ctx.target, gov)
    {:ok, running_owner} = restored.limits[running_scope]
    assert running_owner.config_version == 0

    reset = fn
      %{scope: ^running_scope} = owner -> %{owner | config_version: 0}
      owner -> owner
    end

    {:ok, limit_list} = before.limit_list

    expected = %{
      before
      | limits:
          Map.new(before.limits, fn {scope, {:ok, owner}} -> {scope, {:ok, reset.(owner)}} end),
        limit_list: {:ok, Enum.map(limit_list, reset)}
    }

    assert restored == expected

    # Effects and the ledger belong to their run: retention deletes them with it.
    pk = gov.partition_key

    for owned <- [
          Ferricstore.Flow.Keys.governance_effect_key(gov.running.id, "send", pk),
          Ferricstore.Flow.Keys.governance_ledger_index_key(gov.running.id, pk)
        ] do
      member = Ferricstore.Flow.Keys.retention_cleanup_member_key(gov.running.id, owned, pk)
      assert Ferricstore.Store.Router.get(ctx.target, member), "#{owned} is not owned by its run"
    end

    run = gov.running

    assert {:ok, %{status: :confirmed}} =
             Impl.flow_effect_confirm(ctx.target, run.id, "send",
               partition_key: pk,
               lease_token: run.lease_token,
               fencing_token: run.fencing_token,
               now_ms: gov.now_ms + 20
             )

    assert :ok =
             Impl.flow_complete(ctx.target, run.id, run.lease_token,
               partition_key: pk,
               fencing_token: run.fencing_token
             )

    # The restored run's slot goes back to the restored running limit.
    await_running_in_use!(ctx.target, gov, running_scope, 0)

    assert {:ok, %{status: :approved}} =
             Impl.flow_approval_approve(ctx.target, gov.approval, approver: "operator")

    assert {:ok, %{status: :committed}} =
             Impl.flow_budget_commit(ctx.target, gov.budget, gov.budget_reservation, 1,
               now_ms: gov.now_ms + 20
             )

    assert {:ok, %{leases: %{0 => %{in_use: 0}}}} =
             Impl.flow_limit_release(ctx.target, gov.limit,
               shard_id: 0,
               reservation_ids: gov.limit_reservations
             )
  end

  test "restore refuses limit leases on shards the target lacks before writing anything", ctx do
    wide = IsolatedInstance.checkout(shard_count: 4)
    on_exit(fn -> IsolatedInstance.checkin(wide) end)

    {:ok, _lease} =
      Impl.flow_limit_lease(wide, "tx-wide-limit",
        shard_id: 3,
        amount: 1,
        limit: 2,
        ttl_ms: 600_000,
        now_ms: ctx.now_ms
      )

    assert {:ok, _counts} = Transfer.export(wide, ctx.path)

    assert {:error, {:limit_lease_shard_out_of_range, "tx-wide-limit", 3, 2}} =
             Transfer.restore(ctx.target, ctx.path)

    assert {:ok, nil} = Impl.flow_limit_get(ctx.target, "tx-wide-limit")
  end

  test "export repairs a shard whose LMDB projection is marked degraded", ctx do
    # A mirror enqueue the writer refused marks the shard degraded; history reads
    # then refuse it until the projection is reconciled.
    :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.source.name, ctx.source.shard_count, 60_000)
    key = Ferricstore.Flow.Keys.history_key("tx:done", "tx:tenant-a")
    flag = Ferricstore.Store.Router.shard_for(ctx.source, key) + 1
    :atomics.put(ctx.source.flow_lmdb_mirror_degraded, flag, 1)

    assert {:ok, %{flows: flows}} = Transfer.export(ctx.source, ctx.path)
    assert flows == length(ctx.manifest.flows) + length(ctx.manifest.schedules)
    assert :atomics.get(ctx.source.flow_lmdb_mirror_degraded, flag) == 0
  end

  test "transient storage conditions are retried, other errors are not" do
    {:ok, attempts} = Agent.start_link(fn -> 0 end)

    flaky = fn reason ->
      fn ->
        case Agent.get_and_update(attempts, &{&1, &1 + 1}) do
          n when n < 3 -> {:error, reason}
          _n -> {:ok, :read}
        end
      end
    end

    # History reads refuse a shard while its LMDB mirror is mid-flush.
    assert {:ok, :read} =
             Transfer.retry_transient(flaky.("ERR flow LMDB projection unavailable for k"), 5, 1)

    Agent.update(attempts, fn _ -> 0 end)

    assert {:ok, :read} =
             Transfer.retry_transient(flaky.({:run_read_failed, "k", :unavailable}), 5, 1)

    Agent.update(attempts, fn _ -> 0 end)
    assert {:error, :corrupt} = Transfer.retry_transient(flaky.(:corrupt), 5, 1)
    assert Agent.get(attempts, & &1) == 1

    Agent.update(attempts, fn _ -> 0 end)

    assert {:error, "ERR flow LMDB projection unavailable for k"} =
             Transfer.retry_transient(flaky.("ERR flow LMDB projection unavailable for k"), 2, 1)
  end

  test "export refuses Flow data it cannot carry and leaves no file", ctx do
    # A Flow row of a kind the export does not know, as a newer release or a
    # half-finished internal operation might leave behind.
    unknown = "f:{f}:unknown-family:tx"
    :ok = Ferricstore.Store.Router.put(ctx.source, unknown, "x", 0)

    assert {:error, {:unsupported_flow_data, [^unknown]}} = Transfer.export(ctx.source, ctx.path)

    refute File.exists?(ctx.path)
    refute File.exists?(ctx.path <> ".partial")
  end

  test "restore refuses a store that already holds flows", ctx do
    {:ok, _} = Transfer.export(ctx.source, ctx.path)
    assert {:error, {:target_not_empty, _key}} = Transfer.restore(ctx.source, ctx.path)
  end

  test "the public API validates its arguments" do
    assert {:error, _reason} = FerricStore.export(:not_a_path)
    assert {:error, _reason} = FerricStore.restore("/tmp/x.ffx", :not_opts)
  end

  defp await_running_in_use!(instance, gov, scope, in_use) do
    :ok =
      Ferricstore.Test.ShardHelpers.eventually(
        fn ->
          case Impl.flow_limit_get(instance, scope, now_ms: gov.now_ms + 10) do
            {:ok, %{leases: leases}} ->
              leases |> Map.values() |> Enum.map(& &1.in_use) |> Enum.sum() == in_use

            _other ->
              false
          end
        end,
        "running limit #{scope} should have #{in_use} slot(s) in use",
        200,
        50
      )
  end

  defp in_keydir?(instance, id, partition_key) do
    key = Ferricstore.Flow.Keys.state_key(id, partition_key)
    shard = Ferricstore.Store.Router.shard_for(instance, key)
    :ets.member(elem(instance.keydir_refs, shard), key)
  end
end
