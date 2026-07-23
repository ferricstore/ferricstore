defmodule Ferricstore.Flow.Query.Explain do
  @moduledoc false

  alias Ferricstore.Flow.Query.{Error, Field, MandatoryScope, Request}
  alias Ferricstore.Flow.Query.{Plan, PlannerDiagnostic, Usage}

  @version "ferric.flow.explain/v1"
  @cost_model "ferric.flow.cost/v1"

  @spec render(Plan.t(), Request.t()) :: map()
  def render(%Plan{} = plan, %Request{} = request) do
    %{
      version: @version,
      query_fingerprint: plan.query_fingerprint,
      status: status(plan),
      plan: render_plan(plan, request),
      estimate: render_estimate(plan.estimate),
      actual: nil,
      stats: render_stats(plan.stats),
      quality: quality(plan, request),
      bounds: render_budget(plan.budget),
      pressure: render_pressure(plan),
      decision: render_decision(plan),
      diagnostic: render_diagnostic(plan, request),
      alternatives: render_alternatives(plan)
    }
  end

  @spec executed(Plan.t(), Request.t(), map()) :: {:ok, map()} | {:error, atom()}
  def executed(%Plan{path: path} = plan, %Request{} = request, usage)
      when path != :reject and is_map(usage) do
    if Usage.valid?(usage, plan.budget, usage_kind(request)) do
      explain =
        plan
        |> render(request)
        |> Map.put(:status, "executed")
        |> Map.put(:actual, usage)
        |> put_actual_pressure(usage)

      {:ok, explain}
    else
      {:error, :invalid_query_usage}
    end
  end

  def executed(%Plan{}, %Request{}, _usage), do: {:error, :invalid_query_usage}

  defp render_plan(plan, request) do
    %{
      path: enum(plan.path),
      index: render_index(plan),
      fallback_reason: enum(plan.fallback_reason),
      range_count: length(plan.ranges),
      order: enum(plan.order),
      requested_order:
        Enum.map(request.order_by, fn {field, direction} ->
          %{field: Field.external_name(field), direction: enum(direction)}
        end),
      constrained_dimensions:
        Enum.map(plan.constraint_shapes, fn shape ->
          %{
            field: shape.field,
            operator: enum(shape.operator),
            value: %{redacted: true}
          }
        end),
      residual_predicates:
        plan.residual_predicates
        |> Enum.map(&redacted_predicate/1)
        |> Enum.sort_by(&{&1.field, &1.operator}),
      mandatory_scope: render_mandatory_scope(plan.mandatory_scope),
      return: enum(request.return),
      limit: request.limit
    }
  end

  defp render_mandatory_scope(%MandatoryScope{} = scope) do
    %{
      mode: enum(scope.mode),
      generation: scope.generation,
      branch_count: MandatoryScope.branch_count(scope),
      enforcement: scope_enforcement(scope.mode),
      values_redacted: true
    }
  end

  defp scope_enforcement(:dedicated), do: "logical_partition"
  defp scope_enforcement(:shared), do: "physical_prefix"

  defp render_index(%Plan{index_id: nil}), do: nil

  defp render_index(%Plan{
         index_id: id,
         index_version: version,
         index_build_id: build_id
       }),
       do: %{logical_id: id, generation: version, build_id: build_id}

  defp render_estimate(estimate) do
    %{
      range_seeks: Map.get(estimate, :range_seeks, 0),
      scanned_entries: Map.get(estimate, :scan_entries, 0),
      hard_scanned_entries: Map.get(estimate, :hard_scan_entries, 0),
      scanned_bytes: Map.get(estimate, :scan_bytes, 0),
      hard_scanned_bytes: Map.get(estimate, :hard_scan_bytes, Map.get(estimate, :scan_bytes, 0)),
      hydrated_records: Map.get(estimate, :hydrated_records, 0),
      hard_hydrated_records:
        Map.get(estimate, :hard_hydrated_records, Map.get(estimate, :hydrated_records, 0)),
      result_records: Map.get(estimate, :result_records, 0),
      residual_checks: Map.get(estimate, :residual_checks, 0),
      sort_rows: Map.get(estimate, :sort_rows, 0),
      planner_memory_bytes: nil,
      executor_memory_bytes: Map.get(estimate, :memory_bytes, 0),
      cost: Map.get(estimate, :cost, 0)
    }
  end

  defp render_stats(stats) do
    %{
      source: enum(Map.get(stats, :source, :none)),
      confidence: enum(Map.get(stats, :confidence, :none)),
      version: Map.get(stats, :version),
      age_ms: Map.get(stats, :age_ms),
      sample_rate_ppm: Map.get(stats, :sample_rate_ppm, 0),
      source_watermark: Map.get(stats, :source_watermark),
      state: stats_state(stats)
    }
  end

  defp render_budget(budget) do
    %{
      range_seeks: budget.range_seeks,
      scanned_entries: budget.scan_entries,
      scanned_bytes: budget.scan_bytes,
      hydrated_records: budget.hydrated_records,
      result_records: budget.result_records,
      response_bytes: budget.response_bytes,
      planner_memory_bytes: budget.planner_memory_bytes,
      executor_memory_bytes: budget.executor_memory_bytes,
      wall_time_ms: budget.wall_time_ms
    }
  end

  defp render_alternatives(%Plan{} = chosen) do
    Enum.map(chosen.alternatives, fn alternative ->
      %{
        path: enum(alternative.path),
        index: %{
          logical_id: alternative.index_id,
          generation: alternative.index_version,
          build_id: alternative.index_build_id
        },
        order: enum(alternative.order),
        sort_required: alternative.order == :bounded_top_k,
        range_count: alternative.range_count,
        residual_predicate_count: alternative.residual_predicate_count,
        estimate: render_estimate(alternative.estimate),
        stats: render_stats(alternative.stats),
        comparison: comparison(chosen, alternative)
      }
    end)
  end

  defp quality(%Plan{path: :primary_key}, %Request{}) do
    %{
      exactness: "authoritative",
      freshness: "current",
      coverage: "complete",
      pagination: "none"
    }
  end

  defp quality(%Plan{path: path}, %Request{}) when path in [:history, :lineage] do
    %{
      exactness: "authoritative",
      freshness: "current",
      coverage: "complete",
      pagination: "authenticated_seek"
    }
  end

  defp quality(%Plan{path: path}, %Request{return: :count})
       when path in [:counter_lookup, :count_scan] do
    %{
      exactness: "projected_exact",
      freshness: "projection_watermark",
      coverage: "complete",
      pagination: "none"
    }
  end

  defp quality(%Plan{path: :empty}, %Request{}) do
    %{
      exactness: "exact",
      freshness: "not_applicable",
      coverage: "complete",
      pagination: "none"
    }
  end

  defp quality(%Plan{path: :reject}, %Request{}) do
    %{
      exactness: "not_applicable",
      freshness: "not_applicable",
      coverage: "unavailable",
      pagination: "none"
    }
  end

  defp quality(%Plan{}, %Request{}) do
    %{
      exactness: "projected_exact",
      freshness: "projection_watermark",
      coverage: "complete",
      pagination: "live_seek"
    }
  end

  defp render_decision(%Plan{path: :reject, fallback_reason: reason}) do
    decision(enum(reason), 0)
  end

  defp render_decision(%Plan{path: :empty}), do: decision("predicate_is_empty", 0)

  defp render_decision(%Plan{path: path})
       when path in [:primary_key, :history, :lineage] do
    decision("authoritative_#{path}", 1)
  end

  defp render_decision(%Plan{alternatives: []}), do: decision("only_bounded_candidate", 1)

  defp render_decision(%Plan{alternatives: alternatives}) do
    decision("lowest_cost_bounded_candidate", length(alternatives) + 1)
  end

  defp decision(reason, bounded_candidate_count) do
    %{
      reason: reason,
      bounded_candidate_count: bounded_candidate_count,
      cost_model: @cost_model,
      tie_breakers: [
        "estimated_cost",
        "hard_scanned_entries",
        "native_order",
        "range_count",
        "index_identity"
      ]
    }
  end

  defp render_diagnostic(%Plan{path: :reject} = plan, request) do
    plan
    |> PlannerDiagnostic.error(request)
    |> Error.payload()
  end

  defp render_diagnostic(%Plan{}, %Request{}), do: nil

  defp comparison(chosen, alternative) do
    %{
      reason_not_selected: comparison_reason(chosen, alternative),
      cost_delta: alternative.estimate.cost - chosen.estimate.cost,
      hard_scanned_entries_delta:
        alternative.estimate.hard_scan_entries - chosen.estimate.hard_scan_entries
    }
  end

  defp comparison_reason(chosen, alternative) do
    cond do
      alternative.estimate.cost > chosen.estimate.cost ->
        "higher_estimated_cost"

      alternative.estimate.hard_scan_entries > chosen.estimate.hard_scan_entries ->
        "higher_hard_scan_ceiling"

      chosen.order == :native and alternative.order != :native ->
        "requires_bounded_sort"

      alternative.range_count > length(chosen.ranges) ->
        "more_ranges"

      true ->
        "stable_index_tiebreak"
    end
  end

  defp render_pressure(%Plan{estimate: estimate, budget: budget}) do
    resources = [
      resource("range_seeks", estimate.range_seeks, estimate.range_seeks, budget.range_seeks),
      resource(
        "scanned_entries",
        estimate.scan_entries,
        estimate.hard_scan_entries,
        budget.scan_entries
      ),
      resource(
        "scanned_bytes",
        estimate.scan_bytes,
        Map.get(estimate, :hard_scan_bytes, estimate.scan_bytes),
        budget.scan_bytes
      ),
      resource(
        "hydrated_records",
        estimate.hydrated_records,
        Map.get(estimate, :hard_hydrated_records, estimate.hydrated_records),
        budget.hydrated_records
      ),
      resource(
        "result_records",
        estimate.result_records,
        estimate.result_records,
        budget.result_records
      ),
      resource("planner_memory_bytes", nil, nil, budget.planner_memory_bytes),
      resource(
        "executor_memory_bytes",
        estimate.memory_bytes,
        estimate.memory_bytes,
        budget.executor_memory_bytes
      ),
      resource("response_bytes", nil, nil, budget.response_bytes),
      resource("wall_time_us", nil, nil, budget.wall_time_ms * 1_000)
    ]

    %{
      estimated_limiting_resource: limiting_resource(resources, :estimated_utilization_ppm),
      hard_limiting_resource: limiting_resource(resources, :hard_utilization_ppm),
      actual_limiting_resource: nil,
      resources: resources
    }
  end

  defp resource(name, estimated, hard_estimated, bound) do
    %{
      name: name,
      estimated: estimated,
      hard_estimated: hard_estimated,
      actual: nil,
      bound: bound,
      estimated_utilization_ppm: utilization_ppm(estimated, bound),
      hard_utilization_ppm: utilization_ppm(hard_estimated, bound),
      actual_utilization_ppm: nil
    }
  end

  defp put_actual_pressure(explain, usage) do
    resources =
      Enum.map(explain.pressure.resources, fn resource ->
        case actual_resource(resource.name, usage) do
          nil ->
            resource

          actual ->
            %{
              resource
              | actual: actual,
                actual_utilization_ppm: utilization_ppm(actual, resource.bound)
            }
        end
      end)

    pressure = %{
      explain.pressure
      | resources: resources,
        actual_limiting_resource: limiting_resource(resources, :actual_utilization_ppm)
    }

    Map.put(explain, :pressure, pressure)
  end

  defp actual_resource("range_seeks", usage), do: usage.range_seeks
  defp actual_resource("scanned_entries", usage), do: usage.scanned_entries
  defp actual_resource("scanned_bytes", usage), do: usage.scanned_bytes
  defp actual_resource("hydrated_records", usage), do: usage.hydrated_records
  defp actual_resource("result_records", usage), do: usage.result_records
  defp actual_resource("executor_memory_bytes", usage), do: usage.memory_high_water_bytes
  defp actual_resource("response_bytes", usage), do: usage.response_bytes
  defp actual_resource("wall_time_us", usage), do: usage.wall_time_us
  defp actual_resource("planner_memory_bytes", _usage), do: nil

  defp limiting_resource(resources, field) do
    measured = Enum.filter(resources, &is_integer(Map.get(&1, field)))

    case Enum.max_by(measured, &Map.fetch!(&1, field), fn -> nil end) do
      nil -> "none"
      resource -> if Map.fetch!(resource, field) == 0, do: "none", else: resource.name
    end
  end

  defp utilization_ppm(nil, _bound), do: nil
  defp utilization_ppm(value, bound), do: div(value * 1_000_000, bound)

  defp stats_state(stats) do
    case Map.get(stats, :source, :none) do
      source when source in [:primary_key, :history_index, :lineage_index, :logical_empty] ->
        "current"

      :transactional_counter ->
        "current"

      source when source in [:stale, :invalid_clock] ->
        "stale"

      source when source in [:none, :default_upper_bound] ->
        "unavailable"

      _sampled ->
        "fresh"
    end
  end

  defp redacted_predicate({operator, field, value}) do
    %{
      field: Field.external_name(field),
      operator: enum(operator),
      value: %{type: value_type(value), redacted: true}
    }
  end

  defp redacted_predicate({operator, field, lower, upper}) do
    %{
      field: Field.external_name(field),
      operator: enum(operator),
      lower: %{type: value_type(lower), redacted: true},
      upper: %{type: value_type(upper), redacted: true}
    }
  end

  defp value_type({:literal, type, _value}), do: enum(type)
  defp value_type({:parameter, type, _name}), do: enum(type)
  defp value_type(_value), do: "unknown"

  defp usage_kind(%Request{return: :count}), do: :count
  defp usage_kind(%Request{return: :record}), do: :records

  defp status(%Plan{path: :reject}), do: "rejected"
  defp status(%Plan{}), do: "planned"

  defp enum(value) when is_atom(value), do: Atom.to_string(value)
end
