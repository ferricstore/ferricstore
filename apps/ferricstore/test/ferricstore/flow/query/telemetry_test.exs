defmodule Ferricstore.Flow.Query.TelemetryTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Flow.Query.{IndexDefinition, Plan, Request, Telemetry}

  @event [:ferricstore, :flow, :query, :stop]

  setup do
    handler = "query-telemetry-#{System.unique_integer([:positive])}"
    parent = self()

    :ok =
      :telemetry.attach(
        handler,
        @event,
        fn event, measurements, metadata, _config ->
          send(parent, {:query_telemetry, event, measurements, metadata})
        end,
        nil
      )

    on_exit(fn -> :telemetry.detach(handler) end)
  end

  test "emits bounded resource measurements and non-sensitive plan metadata" do
    request = request()
    plan = plan()
    usage = usage(4, 0)
    result = {:ok, %{usage: usage, records: [%{id: "secret-record"}]}}

    assert ^result = Telemetry.observe(request, plan, 100, result, now_us: 250)

    assert_receive {:query_telemetry, @event, measurements, metadata}
    assert measurements.duration_us == 150
    assert measurements.scanned_entries == 4
    assert measurements.hydrated_records == 0
    assert measurements.result_records == 1
    assert metadata.status == :ok
    assert metadata.path == :ordered_range
    assert metadata.index_id == "covering-state"
    assert metadata.covering == :covered

    encoded = inspect({measurements, metadata})
    refute encoded =~ "tenant-secret"
    refute encoded =~ "secret-record"
  end

  test "reports failures without exposing the request" do
    result = {:error, :query_deadline_exceeded}
    assert ^result = Telemetry.observe(request(), plan(), 100, result, now_us: 250)

    assert_receive {:query_telemetry, @event, measurements, metadata}
    assert measurements.duration_us == 150
    assert measurements.scanned_entries == 0
    assert metadata.status == :error
    assert metadata.reason == :query_deadline_exceeded
    assert metadata.covering == :eligible
  end

  defp request do
    struct(Request,
      mode: :execute,
      source: :runs,
      predicate: {:and, [{:eq, :partition_key, {:literal, :keyword, "tenant-secret"}}]},
      return: :record,
      projection: [:run_id, :state],
      order_by: [{:updated_at_ms, :desc}],
      limit: 10
    )
  end

  defp plan do
    definition =
      IndexDefinition.new!(%{
        id: "covering-state",
        version: 1,
        fields: [
          {:partition_key, :asc},
          {:state, :asc},
          {:updated_at_ms, :desc}
        ],
        covering_fields: [:partition_key, :run_id, :state, :updated_at_ms, :version]
      })

    struct(Plan,
      path: :ordered_range,
      record_source: :covering_index,
      index_id: definition.id,
      index_version: definition.version,
      definition: definition,
      residual_predicates: []
    )
  end

  defp usage(scanned, hydrated) do
    %{
      range_seeks: 1,
      range_pages: 1,
      scanned_entries: scanned,
      scanned_bytes: 100,
      hydrated_records: hydrated,
      residual_checks: 0,
      duplicate_entries: 0,
      result_records: 1,
      response_bytes: 120,
      memory_high_water_bytes: 1_024,
      wall_time_us: 140
    }
  end
end
