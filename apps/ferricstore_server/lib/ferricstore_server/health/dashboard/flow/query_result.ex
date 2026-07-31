defmodule FerricstoreServer.Health.Dashboard.Flow.QueryResult do
  @moduledoc false

  alias Ferricstore.Flow.Query.{RecordProjection, Request}

  @page_fields [:has_more, :cursor]
  @quality_fields [:exactness, :freshness, :coverage, :pagination]

  @usage_fields [
    :range_seeks,
    :range_pages,
    :scanned_entries,
    :scanned_bytes,
    :hydrated_records,
    :residual_checks,
    :duplicate_entries,
    :result_records,
    :response_bytes,
    :memory_high_water_bytes,
    :wall_time_us
  ]

  @explain_plan_fields [
    :path,
    :record_source,
    :index,
    :fallback_reason,
    :range_count,
    :order,
    :requested_order,
    :constrained_dimensions,
    :residual_predicates,
    :mandatory_scope,
    :projection,
    :return,
    :limit,
    :ranges,
    :predicates
  ]
  @explain_index_fields [:logical_id, :generation, :build_id]
  @explain_projection_fields [:fields, :source, :index_only, :requires_hydration]

  @explain_metric_fields [
    :range_seeks,
    :scanned_entries,
    :hard_scanned_entries,
    :scanned_bytes,
    :hard_scanned_bytes,
    :hydrated_records,
    :hard_hydrated_records,
    :hydration_bytes,
    :metadata_rows,
    :hard_metadata_rows,
    :metadata_bytes,
    :result_records,
    :residual_checks,
    :sort_rows,
    :planner_memory_bytes,
    :executor_memory_bytes,
    :response_bytes,
    :wall_time_ms,
    :wall_time_us,
    :scan_records,
    :groups,
    :cost
  ]

  @explain_stats_fields [
    :source,
    :confidence,
    :version,
    :age_ms,
    :sample_rate_ppm,
    :source_watermark,
    :state
  ]
  @explain_decision_fields [:reason, :bounded_candidate_count, :cost_model, :tie_breakers]
  @explain_capability_fields [:requested, :available, :missing]
  @explain_diagnostic_fields [:code, :message, :detail, :hint, :retryable, :safe_to_retry]
  @explain_pressure_fields [
    :estimated_limiting_resource,
    :hard_limiting_resource,
    :actual_limiting_resource,
    :resources
  ]
  @explain_resource_fields [
    :name,
    :estimated,
    :hard_estimated,
    :actual,
    :bound,
    :estimated_utilization_ppm,
    :hard_utilization_ppm,
    :actual_utilization_ppm
  ]
  @explain_alternative_fields [
    :path,
    :record_source,
    :index,
    :order,
    :sort_required,
    :range_count,
    :residual_predicate_count,
    :estimate,
    :stats,
    :comparison
  ]
  @explain_comparison_fields [
    :reason_not_selected,
    :cost_delta,
    :hard_scanned_entries_delta
  ]
  @explain_predicate_fields [:field, :operator, :value]
  @explain_redacted_value_fields [:type, :redacted]
  @explain_order_fields [:field, :direction]
  @explain_scope_fields [:mode, :generation, :branch_count, :enforcement, :values_redacted]

  @spec success(binary(), term()) :: map()
  def success(command, response) when is_binary(command) do
    success(command, response, [])
  end

  @spec success(binary(), term(), keyword()) :: map()
  def success(command, response, opts) when is_binary(command) and is_list(opts) do
    result =
      cond do
        explain_response?(response) ->
          explain_result(command, response)

        true ->
          case response_records(response) do
            {:ok, records} -> record_page(command, response, records)
            :error -> scalar_or_legacy(command, response)
          end
      end

    decorate(result, Keyword.get(opts, :request))
  end

  defp record_page(command, response, records) do
    %{
      status: :ok,
      command: command,
      rows: records,
      message: "#{length(records)} row(s)"
    }
    |> maybe_put(:version, known_value(response, :version))
    |> maybe_put_known_map(:page, response, @page_fields)
    |> maybe_put_known_map(:quality, response, @quality_fields)
    |> maybe_put_known_map(:usage, response, @usage_fields)
  end

  defp scalar_or_legacy(command, response) do
    case known_value(response, :result) do
      result when is_map(result) ->
        scalar_result(command, response, result)

      _missing ->
        %{
          status: :ok,
          command: command,
          rows: List.wrap(response),
          message: "1 row"
        }
    end
  end

  defp scalar_result(command, response, result) do
    kind = known_value(result, :kind)
    value = known_value(result, :value)

    %{
      status: :ok,
      command: command,
      rows: [],
      scalar: %{kind: kind, value: value},
      message: scalar_message(kind, value)
    }
    |> maybe_put(:version, known_value(response, :version))
    |> maybe_put_known_map(:quality, response, @quality_fields)
    |> maybe_put_known_map(:usage, response, @usage_fields)
  end

  defp explain_result(command, response) do
    status = known_value(response, :status) || "planned"

    %{
      status: :ok,
      command: command,
      rows: [],
      explain: sanitize_explain(response),
      message: "Plan #{display_value(status)}"
    }
    |> maybe_put(:version, known_value(response, :version))
    |> maybe_put_known_map(:quality, response, @quality_fields)
    |> maybe_put_known_map(:usage, response, :actual, @usage_fields)
  end

  defp explain_response?(response) when is_map(response),
    do: known_value(response, :version) == "ferric.flow.explain/v1"

  defp explain_response?(_response), do: false

  defp sanitize_explain(response) do
    %{}
    |> maybe_put(:version, known_value(response, :version))
    |> maybe_put(:status, known_value(response, :status))
    |> maybe_put(:plan, sanitize_plan(known_value(response, :plan)))
    |> maybe_put(
      :estimate,
      sanitize_map(known_value(response, :estimate), @explain_metric_fields)
    )
    |> maybe_put(:actual, sanitize_map(known_value(response, :actual), @explain_metric_fields))
    |> maybe_put(:bounds, sanitize_map(known_value(response, :bounds), @explain_metric_fields))
    |> maybe_put(:stats, sanitize_map(known_value(response, :stats), @explain_stats_fields))
    |> maybe_put(:quality, sanitize_map(known_value(response, :quality), @quality_fields))
    |> maybe_put(
      :decision,
      sanitize_map(known_value(response, :decision), @explain_decision_fields)
    )
    |> maybe_put(
      :capabilities,
      sanitize_map(known_value(response, :capabilities), @explain_capability_fields)
    )
    |> maybe_put(:pressure, sanitize_pressure(known_value(response, :pressure)))
    |> maybe_put(
      :diagnostic,
      sanitize_map(known_value(response, :diagnostic), @explain_diagnostic_fields)
    )
    |> maybe_put(:alternatives, sanitize_alternatives(known_value(response, :alternatives)))
  end

  defp sanitize_plan(plan) when is_map(plan) do
    plan
    |> take_known(@explain_plan_fields)
    |> Map.update(:index, nil, &sanitize_index/1)
    |> Map.update(:projection, nil, &sanitize_map(&1, @explain_projection_fields))
    |> Map.update(:requested_order, [], &sanitize_list(&1, @explain_order_fields))
    |> Map.update(:constrained_dimensions, [], &sanitize_predicates/1)
    |> Map.update(:residual_predicates, [], &sanitize_predicates/1)
    |> Map.update(:ranges, [], &sanitize_predicates/1)
    |> Map.update(:predicates, [], &sanitize_predicates/1)
    |> Map.update(:mandatory_scope, nil, &sanitize_map(&1, @explain_scope_fields))
    |> Map.update(:order, nil, &sanitize_order/1)
  end

  defp sanitize_plan(_plan), do: nil

  defp sanitize_index(index) when is_map(index), do: sanitize_map(index, @explain_index_fields)
  defp sanitize_index(index) when is_binary(index), do: index
  defp sanitize_index(_index), do: nil

  defp sanitize_order(order) when is_map(order), do: sanitize_map(order, @explain_order_fields)
  defp sanitize_order(order) when is_binary(order), do: order
  defp sanitize_order(_order), do: nil

  defp sanitize_predicates(predicates) when is_list(predicates) do
    predicates
    |> Enum.take(64)
    |> Enum.flat_map(fn predicate ->
      case sanitize_map(predicate, @explain_predicate_fields) do
        nil ->
          []

        sanitized ->
          [Map.update(sanitized, :value, nil, &sanitize_redacted_value/1)]
      end
    end)
  end

  defp sanitize_predicates(_predicates), do: []

  defp sanitize_redacted_value(value) when is_map(value),
    do: sanitize_map(value, @explain_redacted_value_fields)

  defp sanitize_redacted_value(_value), do: nil

  defp sanitize_list(values, fields) when is_list(values) do
    values
    |> Enum.take(64)
    |> Enum.map(&sanitize_map(&1, fields))
    |> Enum.reject(&is_nil/1)
  end

  defp sanitize_list(_values, _fields), do: []

  defp sanitize_pressure(pressure) when is_map(pressure) do
    pressure = take_known(pressure, @explain_pressure_fields)

    Map.update(pressure, :resources, [], fn resources ->
      resources
      |> List.wrap()
      |> Enum.take(32)
      |> Enum.map(&sanitize_map(&1, @explain_resource_fields))
      |> Enum.reject(&is_nil/1)
    end)
  end

  defp sanitize_pressure(_pressure), do: nil

  defp sanitize_alternatives(alternatives) when is_list(alternatives) do
    alternatives
    |> Enum.take(16)
    |> Enum.flat_map(fn alternative ->
      case sanitize_map(alternative, @explain_alternative_fields) do
        nil ->
          []

        sanitized ->
          sanitized =
            sanitized
            |> Map.update(:index, nil, &sanitize_index/1)
            |> Map.update(:estimate, nil, &sanitize_map(&1, @explain_metric_fields))
            |> Map.update(:stats, nil, &sanitize_map(&1, @explain_stats_fields))
            |> Map.update(
              :comparison,
              nil,
              &sanitize_map(&1, @explain_comparison_fields)
            )

          [sanitized]
      end
    end)
  end

  defp sanitize_alternatives(_alternatives), do: []

  defp sanitize_map(value, fields) when is_map(value), do: take_known(value, fields)
  defp sanitize_map(_value, _fields), do: nil

  defp decorate(result, %Request{} = request) do
    result = Map.put(result, :presentation, :workbench)

    if request.return == :record and not Map.has_key?(result, :explain) do
      {columns, selectors} = projection_columns(request)

      result
      |> Map.put(:columns, columns)
      |> Map.put(:column_selectors, selectors)
      |> Map.put(:source, request.source)
    else
      result
    end
  end

  defp decorate(result, _request), do: result

  defp projection_columns(%Request{source: :runs, projection: :all}) do
    selectors =
      RecordProjection.fields()
      |> Enum.map(fn
        :id -> :run_id
        field -> field
      end)

    {RecordProjection.external_names(selectors), selectors}
  end

  defp projection_columns(%Request{source: :events, projection: :all}) do
    {["event_id", "fields"], [:event_id, :fields]}
  end

  defp projection_columns(%Request{projection: projection}) when is_list(projection),
    do: {RecordProjection.external_names(projection), projection}

  defp scalar_message(kind, value), do: "#{display_value(value)} #{display_value(kind)} result"

  defp display_value(nil), do: "query"
  defp display_value(value) when is_binary(value), do: value
  defp display_value(value), do: to_string(value)

  defp response_records(response) when is_map(response) do
    case known_value(response, :records) do
      records when is_list(records) -> {:ok, records}
      _missing -> :error
    end
  end

  defp response_records(response) when is_list(response), do: {:ok, response}
  defp response_records(_response), do: :error

  defp maybe_put_known_map(result, key, source, fields) do
    case known_value(source, key) do
      value when is_map(value) -> maybe_put(result, key, take_known(value, fields))
      _missing -> result
    end
  end

  defp maybe_put_known_map(result, key, source, source_key, fields) do
    case known_value(source, source_key) do
      value when is_map(value) -> maybe_put(result, key, take_known(value, fields))
      _missing -> result
    end
  end

  defp take_known(map, fields) do
    Enum.reduce(fields, %{}, fn field, acc ->
      case fetch_known(map, field) do
        {:ok, value} -> Map.put(acc, field, value)
        :error -> acc
      end
    end)
  end

  defp known_value(map, key) when is_map(map) do
    case fetch_known(map, key) do
      {:ok, value} -> value
      :error -> nil
    end
  end

  defp known_value(_value, _key), do: nil

  defp fetch_known(map, key) do
    case Map.fetch(map, key) do
      {:ok, value} -> {:ok, value}
      :error -> Map.fetch(map, Atom.to_string(key))
    end
  end

  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)
end
