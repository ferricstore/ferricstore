defmodule Ferricstore.Flow.InvariantTest do
  use Ferricstore.Test.FlowCase

  describe "claim_due invariants" do
    test "leased and terminal flows are not returned by later claim_due calls" do
      id = unique_flow_id("flow-invariant-leased-terminal")
      type = unique_flow_id("flow-invariant-type")

      assert :ok =
               FerricStore.flow_create(id,
                 type: type,
                 state: "queued",
                 payload: "payload",
                 now_ms: 1_000,
                 run_at_ms: 1_000
               )

      assert {:ok, [claimed]} =
               FerricStore.flow_claim_due(type,
                 state: "queued",
                 limit: 1,
                 worker: "invariant-worker",
                 now_ms: 1_000
               )

      assert {:ok, []} =
               FerricStore.flow_claim_due(type,
                 state: "queued",
                 limit: 10,
                 worker: "invariant-worker",
                 now_ms: 1_001
               )

      assert :ok =
               FerricStore.flow_complete(id, claimed.lease_token,
                 result: "done",
                 fencing_token: claimed.fencing_token,
                 now_ms: 1_100
               )

      assert {:ok, []} =
               FerricStore.flow_claim_due(type,
                 state: "queued",
                 limit: 10,
                 worker: "invariant-worker",
                 now_ms: 2_000
               )
    end

    test "future due work is invisible until its due timestamp" do
      id = unique_flow_id("flow-invariant-future-due")
      type = unique_flow_id("flow-invariant-type")

      assert :ok =
               FerricStore.flow_create(id,
                 type: type,
                 state: "queued",
                 payload: "payload",
                 now_ms: 1_000,
                 run_at_ms: 10_000
               )

      assert {:ok, []} =
               FerricStore.flow_claim_due(type,
                 state: "queued",
                 limit: 10,
                 worker: "invariant-worker",
                 now_ms: 9_999
               )

      assert {:ok, [claimed]} =
               FerricStore.flow_claim_due(type,
                 state: "queued",
                 limit: 10,
                 worker: "invariant-worker",
                 now_ms: 10_000
               )

      assert claimed.id == id
    end
  end

  describe "terminal/history invariants" do
    test "complete writes terminal state and keeps history queryable" do
      id = unique_flow_id("flow-invariant-complete")
      type = unique_flow_id("flow-invariant-type")

      assert :ok =
               FerricStore.flow_create(id,
                 type: type,
                 state: "queued",
                 payload: "payload",
                 now_ms: 1_000,
                 run_at_ms: 1_000
               )

      assert {:ok, [claimed]} =
               FerricStore.flow_claim_due(type,
                 state: "queued",
                 limit: 1,
                 worker: "invariant-worker",
                 now_ms: 1_000
               )

      assert :ok =
               FerricStore.flow_complete(id, claimed.lease_token,
                 result: "done",
                 fencing_token: claimed.fencing_token,
                 now_ms: 1_100
               )

      assert {:ok, record} = FerricStore.flow_get(id)
      assert record.state == "completed"

      assert {:ok, history} = FerricStore.flow_history(id, count: 10)

      assert Enum.any?(history, fn {_event_id, fields} ->
               event = Map.get(fields, :event) || Map.get(fields, "event")
               state = Map.get(fields, :state) || Map.get(fields, "state")
               to_state = Map.get(fields, :to_state) || Map.get(fields, "to_state")

               event in ["complete", "completed", :complete, :completed] or
                 state == "completed" or to_state == "completed"
             end)
    end
  end

  describe "value ref invariants" do
    test "stored value refs are readable through mget without duplicating command flow state" do
      assert {:ok, ref} = FerricStore.flow_value_put("shared-doc", now_ms: 1_000)
      assert {:ok, ["shared-doc"]} = FerricStore.flow_value_mget([ref.ref])
    end
  end
end
