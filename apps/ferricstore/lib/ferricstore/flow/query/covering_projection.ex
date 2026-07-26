defmodule Ferricstore.Flow.Query.CoveringProjection do
  @moduledoc false

  alias Ferricstore.Flow.Query.{Field, IndexDefinition, Plan, Request}

  @type storage_field :: :id | Field.builtin()
  @type field_types :: %{storage_field() => :integer | :keyword | :dynamic}
  @type classification :: :ineligible | :eligible | :empty | :covered | :inconsistent
  @covering_paths [:count_scan, :ordered_range, :ordered_range_union, :ordered_filter]

  @spec field_types(Request.t(), IndexDefinition.t(), [term()]) :: field_types() | nil
  def field_types(
        %Request{return: :count},
        %IndexDefinition{covering_fields: [_field | _rest]} = definition,
        residual_predicates
      )
      when is_list(residual_predicates) do
    if residual_fields_covered?(definition, residual_predicates),
      do: covering_field_types(definition),
      else: nil
  end

  def field_types(
        %Request{return: :record, projection: projection, order_by: order_by},
        %IndexDefinition{covering_fields: [_field | _rest]} = definition,
        residual_predicates
      )
      when is_list(projection) and is_list(residual_predicates) do
    if fields_covered?(definition, projection, order_by) and
         residual_fields_covered?(definition, residual_predicates),
       do: covering_field_types(definition),
       else: nil
  end

  def field_types(_request, _definition, _residual_predicates), do: nil

  @spec eligible?(Request.t(), Plan.t()) :: boolean()
  def eligible?(
        %Request{} = request,
        %Plan{
          path: path,
          record_source: :covering_index,
          definition: definition,
          residual_predicates: residual_predicates
        }
      )
      when path in @covering_paths,
      do: covering_eligible?(request, definition, residual_predicates)

  def eligible?(_request, _plan), do: false

  @spec classify(Request.t(), Plan.t(), map() | nil) :: classification()
  def classify(request, plan, usage \\ nil)

  def classify(
        %Request{} = request,
        %Plan{path: path} = plan,
        usage
      )
      when path in @covering_paths do
    if eligible?(request, plan),
      do: classify_usage(usage),
      else: :ineligible
  end

  def classify(_request, _plan, _usage), do: :ineligible

  defp covering_eligible?(
         %Request{} = request,
         %IndexDefinition{} = definition,
         residual_predicates
       )
       when is_list(residual_predicates),
       do: not is_nil(field_types(request, definition, residual_predicates))

  defp covering_eligible?(_request, _definition, _residual_predicates), do: false

  defp classify_usage(nil), do: :eligible

  defp classify_usage(%{scanned_entries: 0}), do: :empty

  defp classify_usage(%{scanned_entries: scanned_entries, hydrated_records: 0})
       when is_integer(scanned_entries) and scanned_entries > 0,
       do: :covered

  defp classify_usage(%{scanned_entries: scanned_entries, hydrated_records: hydrated_records})
       when is_integer(scanned_entries) and scanned_entries > 0 and is_integer(hydrated_records) and
              hydrated_records > 0,
       do: :inconsistent

  defp classify_usage(_usage), do: :eligible

  defp covering_field_types(%IndexDefinition{covering_fields: fields}) do
    Map.new(fields, fn field ->
      {storage_field(field), Field.value_type(field)}
    end)
  end

  defp fields_covered?(%IndexDefinition{covering_fields: covering_fields}, projection, order_by) do
    Enum.all?(projection, &covered?(covering_fields, &1)) and
      Enum.all?(order_by, fn {field, _direction} -> covered?(covering_fields, field) end)
  end

  defp residual_fields_covered?(
         %IndexDefinition{covering_fields: covering_fields},
         residual_predicates
       ) do
    Enum.all?(residual_predicates, fn predicate ->
      case residual_field(predicate) do
        {:ok, field} -> covered?(covering_fields, field)
        :error -> false
      end
    end)
  end

  defp residual_field({operator, field, _value}) when operator in [:eq, :in, :is],
    do: {:ok, field}

  defp residual_field({operator, field, _lower, _upper})
       when operator in [:range, :time_window],
       do: {:ok, field}

  defp residual_field(_predicate), do: :error

  defp covered?(covering_fields, required_field) do
    required_storage_field = storage_field(required_field)
    Enum.any?(covering_fields, &(storage_field(&1) == required_storage_field))
  end

  defp storage_field(:run_id), do: :id
  defp storage_field(field), do: field
end
