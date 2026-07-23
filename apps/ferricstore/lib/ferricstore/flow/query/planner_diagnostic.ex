defmodule Ferricstore.Flow.Query.PlannerDiagnostic do
  @moduledoc false

  alias Ferricstore.Flow.Query.{Error, Field, Request}
  alias Ferricstore.Flow.Query.Plan

  @max_suggested_fields 8
  @max_hint_fields 3
  @max_hint_residual_predicates 2
  @status_command "FLOW.QUERY.INDEXES"

  @spec error(Plan.t(), Request.t()) :: Error.t()
  def error(
        %Plan{path: :reject, fallback_reason: reason},
        %Request{} = request
      )
      when reason in [:no_active_bounded_index, :unscoped_query] do
    source = Atom.to_string(request.source)
    predicates = predicate_shapes(request)
    suggestion = suggested_index(request)
    residual_predicates = residual_predicates(predicates, suggestion)

    Error.new(:query_no_bounded_plan,
      detail: "No active index can bound this #{source} query.",
      hint:
        "Inspect index lifecycle with #{@status_command}, then create or activate an index with fields: #{suggested_fields_hint(suggestion)}.#{residual_predicates_hint(residual_predicates)}",
      context: %{
        "planner_reason" => Atom.to_string(reason),
        "source" => source,
        "predicates" => predicates,
        "residual_predicates" => residual_predicates,
        "order_by" => order_shapes(request),
        "suggested_index" => suggestion,
        "status_command" => @status_command
      }
    )
  end

  def error(%Plan{path: :reject, fallback_reason: reason, budget: budget}, %Request{} = request) do
    source = Atom.to_string(request.source)
    predicates = predicate_shapes(request)
    suggestion = suggested_index(request)
    residual_predicates = residual_predicates(predicates, suggestion)

    Error.new(error_reason(reason),
      detail: rejection_detail(reason, source),
      hint: rejection_hint(reason, suggestion, residual_predicates),
      context: %{
        "planner_reason" => Atom.to_string(reason),
        "source" => source,
        "predicates" => predicates,
        "residual_predicates" => residual_predicates,
        "order_by" => order_shapes(request),
        "suggested_index" => suggestion,
        "status_command" => @status_command,
        "bounds" => wire_bounds(budget)
      }
    )
  end

  defp error_reason(:range_budget_exceeded), do: :query_range_budget_exceeded
  defp error_reason(:scan_budget_exceeded), do: :query_scan_budget_exceeded
  defp error_reason(:scan_byte_budget_exceeded), do: :query_scan_byte_budget_exceeded
  defp error_reason(:hydration_budget_exceeded), do: :query_hydration_budget_exceeded
  defp error_reason(:result_budget_exceeded), do: :query_result_budget_exceeded
  defp error_reason(:memory_budget_exceeded), do: :query_memory_budget_exceeded
  defp error_reason(_reason), do: :query_no_bounded_plan

  defp rejection_detail(reason, source) do
    "The planner rejected every active #{source} index because #{rejection_phrase(reason)}."
  end

  defp rejection_phrase(:tenant_range_violation), do: "a range escaped the authorized scope"
  defp rejection_phrase(:range_budget_exceeded), do: "the range-seek ceiling would be exceeded"
  defp rejection_phrase(:scan_budget_exceeded), do: "the scanned-entry ceiling would be exceeded"

  defp rejection_phrase(:scan_byte_budget_exceeded),
    do: "the scanned-byte ceiling would be exceeded"

  defp rejection_phrase(:hydration_budget_exceeded),
    do: "the record-hydration ceiling would be exceeded"

  defp rejection_phrase(:result_budget_exceeded), do: "the result-row ceiling would be exceeded"
  defp rejection_phrase(:memory_budget_exceeded), do: "a memory ceiling would be exceeded"
  defp rejection_phrase(_reason), do: "no bounded plan was available"

  defp rejection_hint(reason, suggestion, residual_predicates) do
    action =
      case reason do
        :result_budget_exceeded ->
          "Lower LIMIT"

        :tenant_range_violation ->
          "Add an exact partition_key predicate"

        _budget_or_plan ->
          "Tighten predicates or activate a narrower composite index"
      end

    "#{action}. Inspect lifecycle and statistics with #{@status_command}; suggested fields: #{suggested_fields_hint(suggestion)}.#{residual_predicates_hint(residual_predicates)}"
  end

  defp wire_bounds(budget) do
    %{
      "range_seeks" => budget.range_seeks,
      "scanned_entries" => budget.scan_entries,
      "scanned_bytes" => budget.scan_bytes,
      "hydrated_records" => budget.hydrated_records,
      "result_records" => budget.result_records,
      "response_bytes" => budget.response_bytes,
      "planner_memory_bytes" => budget.planner_memory_bytes,
      "executor_memory_bytes" => budget.executor_memory_bytes,
      "wall_time_ms" => budget.wall_time_ms
    }
  end

  defp predicate_shapes(%Request{predicate: {:and, predicates}}) do
    predicates
    |> Enum.map(fn predicate ->
      %{
        "field" => predicate |> elem(1) |> Field.external_name(),
        "operator" => predicate_operator(predicate)
      }
    end)
    |> Enum.sort_by(&{&1["field"], &1["operator"]})
  end

  defp predicate_operator({:eq, _field, _value}), do: "eq"
  defp predicate_operator({:in, _field, _values}), do: "in"
  defp predicate_operator({:range, _field, _lower, _upper}), do: "range"
  defp predicate_operator({:time_window, _field, _lower, _upper}), do: "time_window"
  defp predicate_operator({:is, _field, :null}), do: "is_null"
  defp predicate_operator({:is, _field, :missing}), do: "is_missing"

  defp order_shapes(%Request{order_by: order_by}) do
    Enum.map(order_by, fn {field, direction} ->
      %{"field" => Field.external_name(field), "direction" => Atom.to_string(direction)}
    end)
  end

  defp suggested_index(%Request{} = request) do
    suggested_fields =
      request
      |> suggested_fields()
      |> Enum.take(@max_suggested_fields)

    fields =
      suggested_fields
      |> Enum.map(fn {field, direction, encoding} ->
        %{
          "name" => Field.external_name(field),
          "direction" => Atom.to_string(direction),
          "encoding" => Atom.to_string(encoding)
        }
      end)

    suggestion = %{"source" => Atom.to_string(request.source), "fields" => fields}

    case exact_count_prefix(request, suggested_fields) do
      nil -> suggestion
      prefix -> Map.put(suggestion, "count_prefixes", [prefix])
    end
  end

  defp suggested_fields_hint(%{"fields" => fields}) do
    rendered =
      fields
      |> Enum.take(@max_hint_fields)
      |> Enum.map_join(", ", fn field ->
        "#{field["name"]} #{String.upcase(field["direction"])} #{String.upcase(field["encoding"])}"
      end)

    case length(fields) - @max_hint_fields do
      remaining when remaining > 0 -> "#{rendered}, ... (#{remaining} more)"
      _none -> rendered
    end
  end

  defp residual_predicates(predicates, %{"fields" => fields}) do
    covered = MapSet.new(fields, & &1["name"])
    Enum.reject(predicates, &MapSet.member?(covered, &1["field"]))
  end

  defp residual_predicates_hint([]), do: ""

  defp residual_predicates_hint(predicates) do
    rendered =
      predicates
      |> Enum.take(@max_hint_residual_predicates)
      |> Enum.map_join(", ", fn predicate ->
        "#{predicate["field"]} #{String.upcase(predicate["operator"])}"
      end)

    suffix =
      case length(predicates) - @max_hint_residual_predicates do
        remaining when remaining > 0 -> ", ... (#{remaining} more)"
        _none -> ""
      end

    " Residual predicates: #{rendered}#{suffix}."
  end

  defp suggested_fields(%Request{
         source: source,
         predicate: {:and, predicates},
         order_by: order_by
       }) do
    order_directions = Map.new(order_by)

    predicate_fields =
      predicates
      |> Enum.map(&predicate_field(&1, order_directions))
      |> Enum.sort_by(&suggested_field_sort_key/1)

    order_fields =
      Enum.map(order_by, fn {field, direction} -> {field, direction, field_encoding(field)} end)

    predicate_fields
    |> Kernel.++(order_fields)
    |> Enum.uniq_by(&elem(&1, 0))
    |> limit_multivalue_fields()
    |> ensure_minimum_index_fields(source)
  end

  defp limit_multivalue_fields(fields) do
    {reversed, _attribute_seen?} =
      Enum.reduce(fields, {[], false}, fn
        {{:attribute, _name}, _direction, _encoding} = field, {acc, false} ->
          {[field | acc], true}

        {{:attribute, _name}, _direction, _encoding}, {acc, true} ->
          {acc, true}

        field, {acc, attribute_seen?} ->
          {[field | acc], attribute_seen?}
      end)

    Enum.reverse(reversed)
  end

  defp ensure_minimum_index_fields(
         [{:partition_key, :asc, :hashed} = partition],
         :runs
       ),
       do: [partition, {:run_id, :asc, :hashed}]

  defp ensure_minimum_index_fields(fields, _source), do: fields

  defp exact_count_prefix(
         %Request{return: :count, predicate: {:and, predicates}},
         suggested_fields
       ) do
    overlapping_multivalue? =
      Enum.any?(predicates, fn
        {:in, {:attribute, _name}, values} -> length(values) > 1
        _predicate -> false
      end)

    exact_fields =
      Enum.flat_map(predicates, fn
        {operator, field, _value} when operator in [:eq, :in] -> [field]
        {:is, field, _kind} -> [field]
        _range -> []
      end)

    if not overlapping_multivalue? and length(exact_fields) == length(predicates) do
      exact_count = exact_fields |> Enum.uniq() |> length()

      if exact_count <= length(suggested_fields), do: exact_count
    end
  end

  defp exact_count_prefix(%Request{}, _suggested_fields), do: nil

  defp predicate_field({operator, field, _value}, _order_directions)
       when operator in [:eq, :in] do
    {field, :asc, :hashed}
  end

  defp predicate_field({:is, field, _kind}, _order_directions),
    do: {field, :asc, :hashed}

  defp predicate_field({_operator, field, _lower, _upper}, order_directions) do
    {field, Map.get(order_directions, field, :asc), field_encoding(field)}
  end

  defp suggested_field_sort_key({field, direction, encoding}) do
    partition_rank = if field == :partition_key, do: 0, else: 1
    encoding_rank = if encoding == :hashed, do: 0, else: 1
    {partition_rank, encoding_rank, Field.external_name(field), direction}
  end

  defp field_encoding(field) do
    if Field.value_type(field) == :integer, do: :ordered, else: :hashed
  end
end
