defmodule FerricstoreServer.Health.Dashboard.Flow.QueryScope do
  @moduledoc false

  alias Ferricstore.Flow.Query.Request
  alias FerricstoreServer.Health.Dashboard.Flow.TimeFilter

  @scope_keys [:type, :partition_key, :state, :from_ms, :to_ms, :range, :time_mode]
  # Builder represents an omitted upper date with this exact numeric sentinel.
  @open_upper_bound 9_007_199_254_740_991

  def options(%Request{source: source, predicate: {:and, predicates}}, opts) do
    opts = Keyword.drop(opts, @scope_keys)

    opts =
      Enum.reduce([:type, :partition_key], opts, fn field, acc ->
        put_value(acc, field, exact_keyword(predicates, field))
      end)

    if source == :runs do
      opts
      |> put_value(:state, exact_keyword(predicates, :state))
      |> put_updated_bounds(predicates)
    else
      opts
    end
  end

  defp exact_keyword(predicates, field) do
    predicates
    |> Enum.flat_map(fn
      {:eq, ^field, {:literal, :keyword, value}} when is_binary(value) and value != "" -> [value]
      _ -> []
    end)
    |> Enum.uniq()
    |> case do
      [value] -> value
      _ -> nil
    end
  end

  defp put_updated_bounds(opts, predicates) do
    bounds = Enum.flat_map(predicates, &updated_bound/1)

    case bounds do
      [] ->
        opts

      [first | rest] ->
        {lower, upper} =
          Enum.reduce(rest, first, fn {lo, hi}, {from, to} -> {max(lo, from), min(hi, to)} end)

        upper = if upper == @open_upper_bound, do: nil, else: upper
        validated = TimeFilter.validate([from_ms: lower, to_ms: upper], lower, upper)

        if map_size(validated.errors) == 0 do
          opts |> put_value(:from_ms, lower) |> put_value(:to_ms, upper)
        else
          opts
        end
    end
  end

  defp updated_bound(
         {operator, :updated_at_ms, {:literal, :integer, lower}, {:literal, :integer, upper}}
       )
       when operator in [:range, :time_window] do
    [{lower, if(operator == :time_window, do: upper - 1, else: upper)}]
  end

  defp updated_bound({:eq, :updated_at_ms, {:literal, :integer, value}}), do: [{value, value}]
  defp updated_bound(_), do: []

  defp put_value(opts, _key, nil), do: opts
  defp put_value(opts, key, value), do: Keyword.put(opts, key, value)
end
