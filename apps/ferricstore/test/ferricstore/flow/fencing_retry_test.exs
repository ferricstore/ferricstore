defmodule Ferricstore.Flow.FencingRetryTest do
  use Ferricstore.Test.FlowCase

  defp create_and_claim(type, id, now_ms) do
    assert :ok =
             FerricStore.flow_create(id,
               type: type,
               state: "queued",
               payload: "payload",
               now_ms: now_ms,
               run_at_ms: now_ms
             )

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due(type,
               state: "queued",
               limit: 1,
               worker: "fencing-worker",
               now_ms: now_ms
             )

    claimed
  end

  defp history_event_count(id, partition_key, event) do
    assert {:ok, history} = FerricStore.flow_history(id, partition_key: partition_key, count: 20)

    Enum.count(history, fn {_event_id, fields} ->
      Map.get(fields, "event") == event or Map.get(fields, :event) == event
    end)
  end

  test "complete retry after committed response loss is a no-op success" do
    type = unique_flow_id("fence-complete-type")
    id = unique_flow_id("fence-complete")
    claimed = create_and_claim(type, id, 1_000)

    opts = [
      partition_key: claimed.partition_key,
      fencing_token: claimed.fencing_token,
      result: "first-result",
      now_ms: 1_100
    ]

    assert :ok = FerricStore.flow_complete(id, claimed.lease_token, opts)

    assert :ok =
             FerricStore.flow_complete(id, claimed.lease_token, Keyword.put(opts, :now_ms, 1_200))

    assert history_event_count(id, claimed.partition_key, "completed") == 1
  end

  test "fail retry after committed response loss is a no-op success" do
    type = unique_flow_id("fence-fail-type")
    id = unique_flow_id("fence-fail")
    claimed = create_and_claim(type, id, 2_000)

    opts = [
      partition_key: claimed.partition_key,
      fencing_token: claimed.fencing_token,
      error: "first-error",
      now_ms: 2_100
    ]

    assert :ok = FerricStore.flow_fail(id, claimed.lease_token, opts)
    assert :ok = FerricStore.flow_fail(id, claimed.lease_token, Keyword.put(opts, :now_ms, 2_200))
    assert history_event_count(id, claimed.partition_key, "failed") == 1
  end

  test "transition retry after committed response loss is a no-op success" do
    type = unique_flow_id("fence-transition-type")
    id = unique_flow_id("fence-transition")
    claimed = create_and_claim(type, id, 3_000)

    opts = [
      partition_key: claimed.partition_key,
      fencing_token: claimed.fencing_token,
      lease_token: claimed.lease_token,
      payload: "next-payload",
      now_ms: 3_100
    ]

    assert :ok = FerricStore.flow_transition(id, "running", "waiting", opts)

    assert :ok =
             FerricStore.flow_transition(
               id,
               "running",
               "waiting",
               Keyword.put(opts, :now_ms, 3_200)
             )

    assert history_event_count(id, claimed.partition_key, "transitioned") == 1
  end

  test "retry retry after committed response loss is guarded by latest history event" do
    type = unique_flow_id("fence-retry-type")
    id = unique_flow_id("fence-retry")
    claimed = create_and_claim(type, id, 4_000)

    retry_opts = [
      partition_key: claimed.partition_key,
      fencing_token: claimed.fencing_token,
      error: "transient-error",
      run_at_ms: 5_000,
      now_ms: 4_100
    ]

    assert :ok = FerricStore.flow_retry(id, claimed.lease_token, retry_opts)

    assert :ok =
             FerricStore.flow_retry(
               id,
               claimed.lease_token,
               Keyword.put(retry_opts, :now_ms, 4_200)
             )

    assert history_event_count(id, claimed.partition_key, "retry") == 1

    assert :ok =
             FerricStore.flow_transition(id, "queued", "waiting",
               partition_key: claimed.partition_key,
               fencing_token: claimed.fencing_token,
               now_ms: 4_300
             )

    assert {:error, "ERR stale flow lease"} =
             FerricStore.flow_retry(
               id,
               claimed.lease_token,
               Keyword.put(retry_opts, :now_ms, 4_400)
             )
  end

  test "retry retry without explicit run_at is a no-op while scheduled in original state" do
    type = unique_flow_id("fence-policy-retry-type")
    id = unique_flow_id("fence-policy-retry")
    claimed = create_and_claim(type, id, 5_000)

    retry_opts = [
      partition_key: claimed.partition_key,
      fencing_token: claimed.fencing_token,
      error: "transient-error",
      now_ms: 5_100
    ]

    assert :ok = FerricStore.flow_retry(id, claimed.lease_token, retry_opts)

    assert :ok =
             FerricStore.flow_retry(
               id,
               claimed.lease_token,
               Keyword.put(retry_opts, :now_ms, 5_200)
             )

    assert history_event_count(id, claimed.partition_key, "retry") == 1
  end

  test "same fencing token does not turn a different terminal effect into success" do
    type = unique_flow_id("fence-different-terminal-type")
    id = unique_flow_id("fence-different-terminal")
    claimed = create_and_claim(type, id, 6_000)

    assert :ok =
             FerricStore.flow_fail(id, claimed.lease_token,
               partition_key: claimed.partition_key,
               fencing_token: claimed.fencing_token,
               error: "failed",
               now_ms: 6_100
             )

    assert {:error, "ERR stale flow lease"} =
             FerricStore.flow_complete(id, claimed.lease_token,
               partition_key: claimed.partition_key,
               fencing_token: claimed.fencing_token,
               result: "done",
               now_ms: 6_200
             )
  end
end
