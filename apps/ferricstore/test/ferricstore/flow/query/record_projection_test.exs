defmodule Ferricstore.Flow.Query.RecordProjectionTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.Query.RecordProjection

  test "projects structural and bounded user metadata fields" do
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
      attributes: %{"region" => "eu"},
      state_meta: %{"review" => %{"ai.model" => "gpt-5"}},
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
               :correlation_id,
               :attributes,
               :state_meta
             ])
  end

  test "preserves misses and storage errors" do
    assert {:ok, nil} = RecordProjection.project_result({:ok, nil})
    assert {:error, :unavailable} = RecordProjection.project_result({:error, :unavailable})
  end

  test "projects only requested run fields while preserving nested metadata shape" do
    record = %{
      id: "run-1",
      state: "ready",
      lease_token: "must-not-leak",
      attributes: %{"customer" => nil, "region" => "eu"},
      state_meta: %{"review" => %{"owner" => "worker-a", "secret" => "hidden"}}
    }

    projection = [
      :run_id,
      :state,
      {:attribute, "customer"},
      {:attribute, "missing"},
      {:state_meta, "review", "owner"}
    ]

    assert {:ok,
            %{
              id: "run-1",
              state: "ready",
              attributes: %{"customer" => nil},
              state_meta: %{"review" => %{"owner" => "worker-a"}}
            }} = RecordProjection.project_result({:ok, record}, :runs, projection)
  end

  test "projects complete run metadata maps when explicitly requested" do
    record = %{
      id: "run-1",
      attributes: %{"customer" => "acme"},
      state_meta: %{"review" => %{"owner" => "worker-a"}},
      lease_token: "hidden"
    }

    assert {:ok,
            %{
              attributes: %{"customer" => "acme"},
              state_meta: %{"review" => %{"owner" => "worker-a"}}
            }} =
             RecordProjection.project_result(
               {:ok, record},
               :runs,
               [:attributes, :state_meta]
             )
  end

  test "projects bounded history fields without flattening the response contract" do
    event = %{
      event_id: "1000-1",
      fields: %{"event" => "claimed", "worker" => nil, "lease_token" => "hidden"}
    }

    assert {:ok, %{event_id: "1000-1", fields: %{"event" => "claimed", "worker" => nil}}} =
             RecordProjection.project_result(
               {:ok, event},
               :events,
               [:event_id, {:event_field, "event"}, {:event_field, "worker"}]
             )

    assert {:ok, %{}} =
             RecordProjection.project_result(
               {:ok, event},
               :events,
               [{:event_field, "missing"}]
             )
  end
end
