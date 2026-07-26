defmodule Ferricstore.Flow.Query.ReferenceEvaluator do
  @moduledoc false

  alias Ferricstore.Flow.Query.{Field, RecordOrder, RecordProjection, Request, TupleCodec}

  @spec execute([map()], Request.t()) ::
          {:ok, [map()]}
          | {:error,
             :invalid_query_request
             | :query_storage_inconsistent
             | :unsupported_query_order_value}
  def execute(records, %Request{} = request) when is_list(records) do
    case Request.validate_bound(request) do
      :ok ->
        with {:ok, prepared} <- prepare_predicate(request.predicate),
             matching = Enum.filter(records, &matches_all_prepared?(&1, prepared)),
             {:ok, sorted} <- RecordOrder.sort(matching, request.order_by),
             page = Enum.take(sorted, request.limit),
             {:ok, projected} <-
               RecordProjection.project_records(page, request.source, request.projection) do
          {:ok, projected}
        end

      {:error, _reason} ->
        {:error, :invalid_query_request}
    end
  end

  def execute(_records, _request), do: {:error, :invalid_query_request}

  @type prepared_predicate ::
          {:prepared_eq, Field.t(), term()}
          | {:prepared_in, Field.t(), MapSet.t()}
          | {:prepared_range, Field.t(), term(), term(), boolean()}
          | {:prepared_is, Field.t(), :null | :missing}

  @spec prepare([Request.predicate()]) ::
          {:ok, [prepared_predicate()]}
          | {:error, :invalid_query_predicate | :invalid_query_predicates}
  def prepare(predicates) when is_list(predicates) do
    predicates
    |> Enum.reduce_while({:ok, []}, fn predicate, {:ok, prepared} ->
      case prepare_predicate(predicate) do
        {:ok, value} -> {:cont, {:ok, [value | prepared]}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, prepared} -> {:ok, Enum.reverse(prepared)}
      {:error, _reason} = error -> error
    end
  end

  def prepare(_predicates), do: {:error, :invalid_query_predicates}

  @spec matches_prepared?(map(), prepared_predicate()) :: boolean()
  def matches_prepared?(record, {:prepared_eq, field, expected}) when is_map(record),
    do: field_value_match?(Field.fetch(record, field), expected)

  def matches_prepared?(record, {:prepared_in, field, expected}) when is_map(record),
    do: set_value_match?(Field.fetch(record, field), expected)

  def matches_prepared?(
        record,
        {:prepared_range, field, lower, upper, upper_inclusive?}
      )
      when is_map(record) and is_boolean(upper_inclusive?),
      do: range_match?(Field.fetch(record, field), lower, upper, upper_inclusive?)

  def matches_prepared?(record, {:prepared_is, field, :missing}) when is_map(record),
    do: Field.fetch(record, field) == :missing

  def matches_prepared?(record, {:prepared_is, field, :null}) when is_map(record),
    do: Field.fetch(record, field) == {:ok, nil}

  def matches_prepared?(_record, _predicate), do: false

  @spec matches?(map(), Request.predicate()) :: boolean()
  def matches?(record, {:eq, field, value}) do
    expected = literal_value(value)
    field_value_match?(Field.fetch(record, field), expected)
  end

  def matches?(record, {:in, field, values}) do
    expected = Enum.map(values, &literal_value/1)
    any_value_match?(Field.fetch(record, field), expected)
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

  defp prepare_predicate({:and, predicates}), do: prepare(predicates)

  defp prepare_predicate({:eq, field, value}) do
    with true <- Field.valid?(field),
         {:ok, expected} <- prepare_literal(value) do
      {:ok, {:prepared_eq, field, expected}}
    else
      _invalid -> {:error, :invalid_query_predicate}
    end
  end

  defp prepare_predicate({:in, field, [_value | _rest] = values}) do
    with true <- Field.valid?(field),
         {:ok, expected} <- prepare_literals(values) do
      {:ok, {:prepared_in, field, MapSet.new(expected)}}
    else
      _invalid -> {:error, :invalid_query_predicate}
    end
  end

  defp prepare_predicate({:range, field, lower, upper}),
    do: prepare_range(field, lower, upper, true)

  defp prepare_predicate({:time_window, field, lower, upper}),
    do: prepare_range(field, lower, upper, false)

  defp prepare_predicate({:is, field, kind}) when kind in [:null, :missing] do
    if Field.valid?(field),
      do: {:ok, {:prepared_is, field, kind}},
      else: {:error, :invalid_query_predicate}
  end

  defp prepare_predicate(_predicate), do: {:error, :invalid_query_predicate}

  defp prepare_range(field, lower, upper, upper_inclusive?) do
    with true <- Field.valid?(field),
         {:ok, lower} <- prepare_literal(lower),
         {:ok, upper} <- prepare_literal(upper) do
      {:ok, {:prepared_range, field, lower, upper, upper_inclusive?}}
    else
      _invalid -> {:error, :invalid_query_predicate}
    end
  end

  defp prepare_literals(values) do
    values
    |> Enum.reduce_while({:ok, []}, fn value, {:ok, prepared} ->
      case prepare_literal(value) do
        {:ok, literal} -> {:cont, {:ok, [literal | prepared]}}
        :error -> {:halt, :error}
      end
    end)
    |> case do
      {:ok, prepared} -> {:ok, Enum.reverse(prepared)}
      :error -> :error
    end
  end

  defp prepare_literal({:literal, :keyword, value}) when is_binary(value), do: {:ok, value}
  defp prepare_literal({:literal, :integer, value}) when is_integer(value), do: {:ok, value}
  defp prepare_literal({:literal, :float, value}) when is_float(value), do: {:ok, value}
  defp prepare_literal({:literal, :boolean, value}) when is_boolean(value), do: {:ok, value}

  defp prepare_literal(_value), do: :error

  defp matches_all_prepared?(record, predicates),
    do: Enum.all?(predicates, &matches_prepared?(record, &1))

  defp field_value_match?(:missing, _expected), do: false

  defp field_value_match?({:ok, _actual}, nil), do: false
  defp field_value_match?({:ok, _actual}, {:ferric_query, :missing}), do: false

  defp field_value_match?({:ok, actual}, expected) when is_list(actual),
    do: Enum.any?(actual, &same_value?(&1, expected))

  defp field_value_match?({:ok, actual}, expected), do: same_value?(actual, expected)

  defp any_value_match?(:missing, _expected), do: false

  defp any_value_match?({:ok, actual}, expected) when is_list(actual),
    do: Enum.any?(actual, fn value -> Enum.any?(expected, &same_value?(value, &1)) end)

  defp any_value_match?({:ok, actual}, expected),
    do: Enum.any?(expected, &same_value?(actual, &1))

  defp set_value_match?(:missing, _expected), do: false

  defp set_value_match?({:ok, actual}, expected) when is_list(actual),
    do: Enum.any?(actual, &MapSet.member?(expected, &1))

  defp set_value_match?({:ok, actual}, expected), do: MapSet.member?(expected, actual)

  defp range_match?(record, field, lower, upper, upper_inclusive?) do
    range_match?(Field.fetch(record, field), lower, upper, upper_inclusive?)
  end

  defp range_match?(fetched, lower, upper, upper_inclusive?) do
    case fetched do
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
