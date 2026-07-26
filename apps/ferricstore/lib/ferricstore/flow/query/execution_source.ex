defmodule Ferricstore.Flow.Query.ExecutionSource do
  @moduledoc false

  alias Ferricstore.Flow.Query.{CoveringProjection, IndexDefinition, QueryRowProjection, Request}

  @type t ::
          :covering_index
          | :query_row
          | :authoritative_log
          | :transactional_counter
          | :none

  @scan_paths [:count_scan, :ordered_range, :ordered_range_union, :ordered_filter]

  @spec classify(Request.t(), atom(), IndexDefinition.t() | nil, [term()]) :: t()
  def classify(%Request{}, :counter_lookup, %IndexDefinition{}, _residual_predicates),
    do: :transactional_counter

  def classify(%Request{}, path, _definition, _residual_predicates)
      when path in [:empty, :reject],
      do: :none

  def classify(%Request{}, path, _definition, _residual_predicates)
      when path in [:primary_key, :history, :lineage, :fixed_index],
      do: :authoritative_log

  def classify(
        %Request{} = request,
        path,
        %IndexDefinition{} = definition,
        residual_predicates
      )
      when path in @scan_paths and is_list(residual_predicates) do
    cond do
      CoveringProjection.field_types(request, definition, residual_predicates) != nil ->
        :covering_index

      QueryRowProjection.eligible?(request, path) ->
        :query_row

      true ->
        :authoritative_log
    end
  end

  def classify(%Request{}, _path, _definition, _residual_predicates),
    do: :authoritative_log
end
