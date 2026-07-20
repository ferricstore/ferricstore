defmodule Ferricstore.Flow.Query.ReferenceEvaluator do
  @moduledoc false

  alias Ferricstore.Flow.Query.{Field, RecordOrder, Request, TupleCodec}

  @spec execute([map()], Request.t()) ::
          {:ok, [map()]}
          | {:error, :invalid_query_request | :unsupported_query_order_value}
  def execute(records, %Request{} = request) when is_list(records) do
    case Request.validate_bound(request) do
      :ok ->
        matching =
          records
          |> Enum.filter(&matches_all?(&1, request.predicate))

        with {:ok, sorted} <- RecordOrder.sort(matching, request.order_by) do
          {:ok, Enum.take(sorted, request.limit)}
        end

      {:error, _reason} ->
        {:error, :invalid_query_request}
    end
  end

  def execute(_records, _request), do: {:error, :invalid_query_request}

  @spec matches?(map(), Request.predicate()) :: boolean()
  def matches?(record, {:eq, field, value}) do
    expected = literal_value(value)

    if expected in [Field.missing(), nil] do
      false
    else
      case Field.fetch(record, field) do
        :missing -> false
        {:ok, actual} when is_list(actual) -> Enum.any?(actual, &same_value?(&1, expected))
        {:ok, actual} -> same_value?(actual, expected)
      end
    end
  end

  def matches?(record, {:in, field, values}) do
    Enum.any?(values, &matches?(record, {:eq, field, &1}))
  end

  def matches?(record, {:range, field, lower, upper}) do
    range_match?(record, field, literal_value(lower), literal_value(upper), true)
  end

  def matches?(record, {:time_window, field, lower, upper}) do
    range_match?(record, field, literal_value(lower), literal_value(upper), false)
  end

  def matches?(record, {:is, field, :missing}), do: Field.fetch(record, field) == :missing
  def matches?(record, {:is, field, :null}), do: Field.fetch(record, field) == {:ok, nil}
  def matches?(_record, _predicate), do: false

  defp matches_all?(record, {:and, predicates}), do: Enum.all?(predicates, &matches?(record, &1))

  defp range_match?(record, field, lower, upper, upper_inclusive?) do
    case Field.fetch(record, field) do
      {:ok, actual} when is_list(actual) ->
        Enum.any?(actual, &within_range?(&1, lower, upper, upper_inclusive?))

      {:ok, actual} ->
        within_range?(actual, lower, upper, upper_inclusive?)

      :missing ->
        false
    end
  end

  defp within_range?(actual, lower, upper, upper_inclusive?) do
    if same_runtime_type?(actual, lower) and same_runtime_type?(actual, upper) do
      lower_compare = TupleCodec.compare_values(actual, lower)
      upper_compare = TupleCodec.compare_values(actual, upper)

      lower_compare in [:eq, :gt] and
        (upper_compare == :lt or (upper_inclusive? and upper_compare == :eq))
    else
      false
    end
  end

  defp literal_value({:literal, _type, value}), do: value
  defp literal_value(_invalid), do: {:ferric_query, :invalid}

  defp same_value?(left, right), do: same_runtime_type?(left, right) and left == right

  defp same_runtime_type?(left, right) when is_integer(left) and is_integer(right), do: true
  defp same_runtime_type?(left, right) when is_float(left) and is_float(right), do: true
  defp same_runtime_type?(left, right) when is_binary(left) and is_binary(right), do: true
  defp same_runtime_type?(left, right) when is_boolean(left) and is_boolean(right), do: true
  defp same_runtime_type?(nil, nil), do: true
  defp same_runtime_type?(left, right), do: left == Field.missing() and right == Field.missing()
end
