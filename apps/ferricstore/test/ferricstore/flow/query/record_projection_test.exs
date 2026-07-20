defmodule Ferricstore.Flow.Query.RecordProjectionTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.Query.RecordProjection

  test "projects only structural query fields" do
    record = %{
      id: "run-1",
      type: "email",
      state: "ready",
      version: 3,
      priority: 5,
      partition_key: "tenant-a",
      created_at_ms: 10,
      updated_at_ms: 20,
      next_run_at_ms: 30,
      lease_deadline_ms: 40,
      attempts: 2,
      run_state: "active",
      max_active_ms: 50,
      parent_flow_id: "parent-1",
      root_flow_id: "root-1",
      correlation_id: "correlation-1",
      lease_token: "secret-token",
      fencing_token: 9,
      retention_ttl_ms: 60_000,
      parent_partition_key: "secret-parent-partition",
      payload_ref: "secret-payload-ref",
      attributes: %{"secret" => true},
      future_internal_field: "must-not-leak"
    }

    assert {:ok, projected} = RecordProjection.project_result({:ok, record})

    assert projected ==
             Map.take(record, [
               :id,
               :type,
               :state,
               :version,
               :priority,
               :partition_key,
               :created_at_ms,
               :updated_at_ms,
               :next_run_at_ms,
               :lease_deadline_ms,
               :attempts,
               :run_state,
               :max_active_ms,
               :parent_flow_id,
               :root_flow_id,
               :correlation_id
             ])
  end

  test "preserves misses and storage errors" do
    assert {:ok, nil} = RecordProjection.project_result({:ok, nil})
    assert {:error, :unavailable} = RecordProjection.project_result({:error, :unavailable})
  end
end
