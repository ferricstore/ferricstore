defmodule Ferricstore.Flow.IdempotentCreateAfterProgressTest do
  @moduledoc """
  An idempotent create retried after its reply was lost must still answer the
  existing Flow once a worker has claimed or finished it: the retry is the
  same request, and the Flow it made has only moved on. A different request
  for the same ID still conflicts.
  """
  use Ferricstore.Test.FlowCase

  defp create(id, type, opts \\ []) do
    FerricStore.flow_create(
      id,
      Keyword.merge(
        [type: type, state: "queued", payload: "payload:" <> id, idempotent: true],
        opts
      )
    )
  end

  defp claim(type) do
    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due(type, state: "queued", limit: 1, worker: "w")

    claimed
  end

  setup do
    %{id: unique_flow_id("idem-progress"), type: unique_flow_id("idem-progress-type")}
  end

  test "a retry after a worker claimed the Flow answers it", %{id: id, type: type} do
    assert :ok = create(id, type)
    claim(type)

    assert :ok = create(id, type)
  end

  test "a retry after the Flow completed answers it", %{id: id, type: type} do
    assert :ok = create(id, type)
    claimed = claim(type)

    assert :ok =
             FerricStore.flow_complete(id, claimed.lease_token,
               partition_key: claimed.partition_key,
               fencing_token: claimed.fencing_token,
               result: "done"
             )

    assert :ok = create(id, type)
  end

  test "a different request for a claimed Flow still conflicts", %{id: id, type: type} do
    assert :ok = create(id, type)
    claim(type)

    assert {:error, "ERR flow idempotency conflict"} = create(id, type, payload: "other")

    assert {:error, "ERR flow idempotency conflict"} =
             create(id, unique_flow_id("other-type"))
  end
end
