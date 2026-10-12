defmodule Ferricstore.Test.FlowTransferFixture do
  @moduledoc false

  alias FerricStore.Impl

  # Builds Flow data that exercises every stored shape a Flow export must carry
  # (hot, running, waiting/hibernated, terminal, unpartitioned flows; payload,
  # result, error, named and shared values; signals; children; policies) and
  # snapshots everything observable through the public API, so an export and
  # restore can be compared run for run.

  @type manifest :: %{
          flows: [{binary(), binary() | nil}],
          types: [binary()],
          shared_refs: [binary()],
          schedules: [binary()]
        }

  @spec build!(FerricStore.Instance.t(), binary(), integer()) :: manifest()
  def build!(ctx, prefix, now_ms) do
    pk = prefix <> ":tenant-a"
    pk_b = prefix <> ":tenant-b"
    type = prefix <> ".order"
    fifo_type = prefix <> ".fifo"
    id = &(prefix <> ":" <> &1)

    {:ok, _} = Impl.flow_policy_set(ctx, type, indexed_attributes: ["region"])
    {:ok, _} = Impl.flow_policy_set(ctx, fifo_type, states: %{"queued" => [mode: :fifo]})

    :ok =
      Impl.flow_create(ctx, id.("queued"),
        type: type,
        state: "queued",
        partition_key: pk,
        payload: %{"amount" => 10},
        attributes: %{"region" => "eu"},
        correlation_id: "corr-1",
        now_ms: now_ms
      )

    for n <- 1..3 do
      :ok =
        Impl.flow_create(ctx, id.("fifo-#{n}"),
          type: fifo_type,
          state: "queued",
          partition_key: pk_b,
          payload: "fifo-#{n}",
          now_ms: now_ms + n
        )
    end

    :ok =
      Impl.flow_create(ctx, id.("done"),
        type: type,
        state: "to-complete",
        partition_key: pk,
        payload: "to complete",
        attributes: %{"region" => "us"},
        now_ms: now_ms
      )

    :ok =
      Impl.flow_create(ctx, id.("failed"),
        type: type,
        state: "to-fail",
        partition_key: pk,
        now_ms: now_ms
      )

    :ok =
      Impl.flow_create(ctx, id.("running"),
        type: type,
        state: "to-run",
        partition_key: pk,
        payload: %{"step" => 1},
        now_ms: now_ms
      )

    :ok =
      Impl.flow_create(ctx, id.("waiting"),
        type: type,
        state: "waiting",
        partition_key: pk,
        run_at_ms: now_ms + 3_600_000,
        now_ms: now_ms
      )

    :ok =
      Impl.flow_create(ctx, id.("unpartitioned"),
        type: type,
        state: "queued",
        payload: "no partition",
        now_ms: now_ms
      )

    :ok =
      Ferricstore.Flow.signal(ctx, id.("waiting"),
        partition_key: pk,
        signal: "nudge",
        idempotency_key: "nudge-1",
        payload: %{"x" => 1}
      )

    {:ok, _named} =
      Ferricstore.Flow.value_put(ctx, "named document",
        owner_flow_id: id.("queued"),
        name: "doc",
        partition_key: pk,
        now_ms: now_ms
      )

    {:ok, shared} = Ferricstore.Flow.value_put(ctx, "shared document", now_ms: now_ms)

    # Schedules are stored as runs of a reserved type.
    {:ok, _schedule} =
      Impl.flow_schedule_create(ctx, id.("schedule"),
        kind: :delay,
        delay_ms: 3_600_000,
        target: [id: id.("scheduled-target"), type: type, partition_key: pk]
      )

    claim_complete!(ctx, type, "to-complete", pk, id.("done"), now_ms, fn job ->
      Impl.flow_complete(ctx, job.id, job.lease_token,
        partition_key: pk,
        fencing_token: job.fencing_token,
        result: %{"ok" => true}
      )
    end)

    claim_complete!(ctx, type, "to-fail", pk, id.("failed"), now_ms, fn job ->
      Impl.flow_fail(ctx, job.id, job.lease_token,
        partition_key: pk,
        fencing_token: job.fencing_token,
        error: %{"reason" => "boom"}
      )
    end)

    claim_complete!(ctx, type, "to-run", pk, id.("running"), now_ms, fn _job -> :ok end)

    %{
      flows:
        [
          {id.("queued"), pk},
          {id.("done"), pk},
          {id.("failed"), pk},
          {id.("running"), pk},
          {id.("waiting"), pk},
          {id.("unpartitioned"), nil}
        ] ++ for(n <- 1..3, do: {id.("fifo-#{n}"), pk_b}),
      types: [type, fifo_type],
      shared_refs: [shared.ref],
      schedules: [id.("schedule")]
    }
  end

  # Each target sits alone in its state, so the claim returns just it.
  defp claim_complete!(ctx, type, state, pk, target, now_ms, fun) do
    {:ok, jobs} =
      Impl.flow_claim_due(ctx, type,
        state: state,
        worker: "fixture-worker",
        lease_ms: 600_000,
        partition_key: pk,
        limit: 10,
        now_ms: now_ms + 1
      )

    job = Enum.find(jobs, &(&1.id == target)) || raise "fixture claim missed #{target}"
    :ok = fun.(job)
  end

  @doc "Everything a client can observe about the manifest's data."
  def snapshot(ctx, %{flows: flows, types: types, shared_refs: shared_refs} = manifest) do
    # History reads need the LMDB projection caught up with recent writes and
    # refuse a shard a failed flush left degraded (export repairs it the same way).
    :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, ctx.shard_count, 60_000)
    :ok = Ferricstore.Flow.Transfer.repair_degraded_shards(ctx)

    %{
      flows:
        Map.new(flows, fn {id, pk} ->
          opts = if pk, do: [partition_key: pk], else: []
          {:ok, record} = Impl.flow_get(ctx, id, opts ++ [payload: true, values: true])
          # Public history reads refuse a shard mid-flush; retry like a client would.
          {:ok, history} =
            Ferricstore.Flow.Transfer.retry_transient(fn -> Impl.flow_history(ctx, id, opts) end)

          {{id, pk}, %{record: record, history: history}}
        end),
      policies: Map.new(types, &{&1, Impl.flow_policy_get(ctx, &1)}),
      shared_values: Ferricstore.Flow.value_mget(ctx, shared_refs),
      schedules:
        Map.new(Map.get(manifest, :schedules, []), &{&1, Impl.flow_schedule_get(ctx, &1)})
    }
  end
end
