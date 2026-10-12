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
    flows = length(ctx.manifest.flows)

    assert {:ok, %{flows: ^flows, shared_values: 1, policies: 2}} =
             Transfer.export(ctx.source, ctx.path)

    assert {:ok, %{flows: ^flows, shared_values: 1, policies: 2}} =
             Transfer.restore(ctx.target, ctx.path)

    assert FlowTransferFixture.snapshot(ctx.target, ctx.manifest) == before
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

  test "restore refuses a store that already holds flows", ctx do
    {:ok, _} = Transfer.export(ctx.source, ctx.path)
    assert {:error, {:target_not_empty, _key}} = Transfer.restore(ctx.source, ctx.path)
  end
end
