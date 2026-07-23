defmodule Ferricstore.Flow.Query.Request do
  @moduledoc """
  Canonical, versioned Flow query request.

  FQL and structured request formats normalize into this shape before
  authorization, planning, or execution. Values stay typed and separate from
  field identifiers throughout the pipeline.
  """

  alias Ferricstore.Flow.Query.{Field, Limits, TupleCodec}

  import Bitwise

  @max_predicates Limits.max_predicates()
  @max_in_values Limits.max_in_values()
  @max_order_fields Limits.max_order_fields()
  @max_results Limits.max_results()
  @min_cursor_bytes Limits.min_cursor_bytes()
  @max_cursor_bytes Limits.max_cursor_bytes()
  @modes [:execute, :explain, :analyze]
  @min_i64 -0x8000_0000_0000_0000
  @max_i64 0x7FFF_FFFF_FFFF_FFFF

  @enforce_keys [:mode, :source, :predicate, :return]
  defstruct version: 1,
            mode: :execute,
            source: :runs,
            predicate: nil,
            order_by: [],
            limit: nil,
            cursor: nil,
            return: nil

  @type literal_type :: :keyword | :integer | :float | :boolean | :null | :missing
  @type value ::
          {:literal, literal_type(), term()}
          | {:parameter, literal_type() | :dynamic, binary()}

  @type predicate ::
          {:eq, Field.t(), value()}
          | {:in, Field.t(), [value()]}
          | {:range, Field.t(), value(), value()}
          | {:time_window, Field.t(), value(), value()}
          | {:is, Field.t(), :null | :missing}

  @type t :: %__MODULE__{
          version: 1,
          mode: :execute | :explain | :analyze,
          source: :runs | :events,
          predicate: {:and, [predicate()]},
          order_by: [{Field.t(), :asc | :desc}],
          limit: pos_integer() | nil,
          cursor: nil | value(),
          return: :record | :count
        }

  @doc false
  @spec point_read(:execute | :explain | :analyze, value()) :: t()
  def point_read(mode, run_id) when mode in @modes do
    %__MODULE__{
      mode: mode,
      source: :runs,
      predicate: {:and, [{:eq, :run_id, run_id}]},
      order_by: [],
      limit: 1,
      cursor: nil,
      return: :record
    }
  end

  @doc false
  @spec point_read(:execute | :explain | :analyze, value(), value()) :: t()
  def point_read(mode, partition_key, run_id) when mode in @modes do
    %__MODULE__{
      mode: mode,
      source: :runs,
      predicate:
        {:and,
         [
           {:eq, :partition_key, partition_key},
           {:eq, :run_id, run_id}
         ]},
      order_by: [],
      limit: 1,
      cursor: nil,
      return: :record
    }
  end

  @doc false
  @spec collection(
          :execute | :explain | :analyze,
          [predicate()],
          [{Field.t(), :asc | :desc}],
          pos_integer(),
          :record
        ) :: t()
  def collection(mode, predicates, order_by, limit, return) do
    collection(mode, predicates, order_by, limit, return, nil)
  end

  @doc false
  @spec collection(
          :execute | :explain | :analyze,
          [predicate()],
          [{Field.t(), :asc | :desc}],
          pos_integer(),
          :record,
          nil | value()
        ) :: t()
  def collection(mode, predicates, order_by, limit, return, cursor) do
    %__MODULE__{
      mode: mode,
      source: :runs,
      predicate: {:and, predicates},
      order_by: order_by,
      limit: limit,
      cursor: cursor,
      return: return
    }
  end

  @doc false
  @spec count(:execute | :explain | :analyze, [predicate()]) :: t()
  def count(mode, predicates) when mode in @modes and is_list(predicates) do
    %__MODULE__{
      mode: mode,
      source: :runs,
      predicate: {:and, predicates},
      order_by: [],
      limit: nil,
      cursor: nil,
      return: :count
    }
  end

  @doc false
  @spec history(
          :execute | :explain | :analyze,
          [predicate()],
          :asc | :desc,
          pos_integer(),
          nil | value()
        ) :: t()
  def history(mode, predicates, direction, limit, cursor \\ nil)
      when mode in @modes and direction in [:asc, :desc] do
    %__MODULE__{
      mode: mode,
      source: :events,
      predicate: {:and, predicates},
      order_by: [{:event_id, direction}],
      limit: limit,
      cursor: cursor,
      return: :record
    }
  end

  @doc false
  @spec lineage_descriptor(t()) ::
          {:ok,
           %{
             kind: :parent | :root | :correlation,
             field: :parent_flow_id | :root_flow_id | :correlation_id,
             value: binary(),
             partition_key: binary(),
             direction: :asc | :desc
           }}
          | :error
  def lineage_descriptor(%__MODULE__{
        source: :runs,
        predicate: {:and, predicates},
        order_by: [{:updated_at_ms, direction}],
        limit: limit,
        return: :record
      })
      when direction in [:asc, :desc] and is_list(predicates) and length(predicates) == 2 and
             is_integer(limit) do
    partition =
      Enum.find(predicates, &match?({:eq, :partition_key, {:literal, :keyword, _value}}, &1))

    lineage =
      Enum.find(predicates, fn
        {:eq, field, {:literal, :keyword, _value}}
        when field in [:parent_flow_id, :root_flow_id, :correlation_id] ->
          true

        _predicate ->
          false
      end)

    case {partition, lineage} do
      {{:eq, :partition_key, {:literal, :keyword, partition_key}},
       {:eq, field, {:literal, :keyword, value}}}
      when is_binary(partition_key) and is_binary(value) ->
        {:ok,
         %{
           kind: lineage_kind(field),
           field: field,
           value: value,
           partition_key: partition_key,
           direction: direction
         }}

      _invalid ->
        :error
    end
  end

  def lineage_descriptor(%__MODULE__{}), do: :error

  @doc false
  @spec validate_unbound(t()) :: :ok | {:error, atom()}
  def validate_unbound(%__MODULE__{} = request), do: validate(request, :unbound)

  @doc false
  @spec validate_bound(t()) :: :ok | {:error, atom()}
  def validate_bound(%__MODULE__{} = request), do: validate(request, :bound)

  @doc false
  @spec validate_cursor_order(term()) :: :ok | {:error, atom()}
  def validate_cursor_order([{:event_id, direction}]) when direction in [:asc, :desc], do: :ok

  def validate_cursor_order(order_by)
      when is_list(order_by) and order_by != [] and length(order_by) <= @max_order_fields,
      do: validate_order(order_by)

  def validate_cursor_order(_order_by), do: {:error, :unsupported_query_shape}

  defp validate(%__MODULE__{version: version}, _phase) when version != 1,
    do: {:error, :unsupported_query_version}

  defp validate(%__MODULE__{source: source}, _phase) when source not in [:runs, :events],
    do: {:error, :unsupported_source}

  defp validate(
         %__MODULE__{
           source: :events,
           mode: mode,
           predicate: {:and, predicates},
           order_by: [{:event_id, direction}],
           limit: limit,
           cursor: cursor,
           return: :record
         },
         phase
       )
       when mode in @modes and direction in [:asc, :desc] and is_list(predicates) and
              is_integer(limit) and limit > 0 and limit <= @max_results do
    with :ok <- validate_mode_cursor(mode, cursor),
         :ok <- validate_history_scope(predicates, phase),
         :ok <- validate_cursor(cursor, phase) do
      :ok
    end
  end

  defp validate(
         %__MODULE__{
           source: :runs,
           mode: mode,
           predicate: {:and, [{:eq, :run_id, run_id}]},
           order_by: [],
           limit: 1,
           cursor: nil,
           return: :record
         },
         phase
       )
       when mode in @modes do
    validate_comparison_value(:run_id, run_id, phase)
  end

  defp validate(
         %__MODULE__{
           source: :runs,
           mode: mode,
           predicate:
             {:and,
              [
                {:eq, :partition_key, partition_key},
                {:eq, :run_id, run_id}
              ]},
           order_by: [],
           limit: 1,
           cursor: nil,
           return: :record
         },
         phase
       )
       when mode in @modes do
    with :ok <- validate_comparison_value(:partition_key, partition_key, phase),
         :ok <- validate_comparison_value(:run_id, run_id, phase) do
      :ok
    end
  end

  defp validate(
         %__MODULE__{
           source: :runs,
           mode: mode,
           predicate: {:and, predicates},
           order_by: [],
           limit: nil,
           cursor: nil,
           return: :count
         },
         phase
       )
       when mode in @modes and is_list(predicates) and predicates != [] and
              length(predicates) <= @max_predicates do
    with :ok <- validate_predicates(predicates, phase),
         :ok <- require_tenant_scope(predicates) do
      :ok
    end
  end

  # The point contract is canonical and has a dedicated physical operator.
  # Do not reinterpret a malformed point envelope as a collection query.
  defp validate(
         %__MODULE__{
           source: :runs,
           predicate:
             {:and,
              [
                {:eq, :partition_key, _partition_key},
                {:eq, :run_id, _run_id}
              ]}
         },
         _phase
       ),
       do: {:error, :unsupported_query_shape}

  defp validate(
         %__MODULE__{
           source: :runs,
           mode: mode,
           predicate: {:and, predicates},
           order_by: order_by,
           limit: limit,
           cursor: cursor,
           return: :record
         },
         phase
       )
       when mode in @modes and is_list(predicates) and predicates != [] and
              length(predicates) <= @max_predicates and is_list(order_by) and order_by != [] and
              length(order_by) <= @max_order_fields and is_integer(limit) and limit > 0 and
              limit <= @max_results do
    with :ok <- validate_mode_cursor(mode, cursor),
         :ok <- validate_predicates(predicates, phase),
         :ok <- require_tenant_scope(predicates),
         :ok <- validate_order(order_by),
         :ok <- validate_cursor(cursor, phase) do
      :ok
    end
  end

  defp validate(%__MODULE__{}, _phase), do: {:error, :unsupported_query_shape}

  defp lineage_kind(:parent_flow_id), do: :parent
  defp lineage_kind(:root_flow_id), do: :root
  defp lineage_kind(:correlation_id), do: :correlation

  defp validate_history_scope([{:eq, :run_id, run_id}], phase),
    do: validate_comparison_value(:run_id, run_id, phase)

  defp validate_history_scope(predicates, phase) when length(predicates) == 2 do
    partition = Enum.find(predicates, &match?({:eq, :partition_key, _value}, &1))
    run_id = Enum.find(predicates, &match?({:eq, :run_id, _value}, &1))

    case {partition, run_id} do
      {{:eq, :partition_key, partition_key}, {:eq, :run_id, id}} ->
        with :ok <- validate_comparison_value(:partition_key, partition_key, phase),
             :ok <- validate_comparison_value(:run_id, id, phase) do
          :ok
        end

      _invalid ->
        {:error, :unsupported_query_shape}
    end
  end

  defp validate_history_scope(_predicates, _phase), do: {:error, :unsupported_query_shape}

  defp validate_predicates(predicates, phase) do
    Enum.reduce_while(predicates, :ok, fn predicate, :ok ->
      case validate_predicate(predicate, phase) do
        :ok -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp validate_predicate({:eq, field, value}, phase) do
    with :ok <- validate_field(field), do: validate_comparison_value(field, value, phase)
  end

  defp validate_predicate({:in, field, values}, phase)
       when is_list(values) and values != [] and length(values) <= @max_in_values do
    with :ok <- validate_field(field),
         :ok <- validate_comparison_values(field, values, phase),
         true <- length(values) == length(Enum.uniq(values)) do
      :ok
    else
      false -> {:error, :duplicate_predicate_value}
      {:error, _reason} = error -> error
    end
  end

  defp validate_predicate({:range, field, lower, upper}, phase) do
    with :ok <- validate_field(field),
         :ok <- validate_comparison_value(field, lower, phase),
         :ok <- validate_comparison_value(field, upper, phase),
         :ok <- validate_same_value_type(lower, upper),
         :ok <- validate_range_order(lower, upper) do
      :ok
    end
  end

  defp validate_predicate({:time_window, field, lower, upper}, phase)
       when field in [:created_at_ms, :updated_at_ms, :next_run_at_ms, :lease_deadline_ms] do
    with :ok <- validate_comparison_value(field, lower, phase),
         :ok <- validate_comparison_value(field, upper, phase),
         :ok <- validate_same_value_type(lower, upper),
         :ok <- validate_range_order(lower, upper) do
      :ok
    end
  end

  defp validate_predicate({:is, field, kind}, _phase) when kind in [:null, :missing],
    do: validate_field(field)

  defp validate_predicate(_predicate, _phase), do: {:error, :unsupported_query_shape}

  defp validate_comparison_values(field, values, phase) do
    Enum.reduce_while(values, :ok, fn value, :ok ->
      case validate_comparison_value(field, value, phase) do
        :ok -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp validate_comparison_value(_field, {:literal, type, _value}, _phase)
       when type in [:null, :missing],
       do: {:error, :invalid_parameter_type}

  defp validate_comparison_value(_field, {:parameter, type, _name}, _phase)
       when type in [:null, :missing],
       do: {:error, :invalid_parameter_type}

  defp validate_comparison_value(field, value, phase), do: validate_value(field, value, phase)

  defp validate_field(field) do
    if Field.valid?(field), do: :ok, else: {:error, :unsupported_field}
  end

  defp validate_value(field, {:literal, type, value}, _phase) do
    with :ok <- validate_literal(type, value),
         :ok <- validate_field_value_type(field, type),
         :ok <- validate_field_size(field, value) do
      :ok
    end
  end

  defp validate_value(field, {:parameter, type, name}, :unbound)
       when is_binary(name) and name != "" and byte_size(name) <= 128 do
    expected = Field.value_type(field)

    if type == expected or expected == :dynamic or type == :dynamic,
      do: :ok,
      else: {:error, :invalid_parameter_type}
  end

  defp validate_value(_field, _value, _phase), do: {:error, :invalid_parameter_type}

  defp validate_literal(:keyword, value) when is_binary(value), do: :ok

  defp validate_literal(:integer, value)
       when is_integer(value) and value >= @min_i64 and value <= @max_i64,
       do: :ok

  defp validate_literal(:float, value) when is_float(value), do: validate_finite_float(value)
  defp validate_literal(:boolean, value) when is_boolean(value), do: :ok
  defp validate_literal(:null, nil), do: :ok
  defp validate_literal(:missing, value) when value == {:ferric_query, :missing}, do: :ok
  defp validate_literal(_type, _value), do: {:error, :invalid_parameter_type}

  defp validate_finite_float(value) do
    <<bits::unsigned-big-64>> = <<value::float-big-64>>
    if (bits >>> 52 &&& 0x7FF) == 0x7FF, do: {:error, :invalid_parameter_type}, else: :ok
  end

  defp validate_field_value_type(field, type) do
    case Field.value_type(field) do
      :dynamic -> :ok
      expected when type in [expected, :null, :missing] -> :ok
      _expected -> {:error, :invalid_parameter_type}
    end
  end

  defp validate_field_size(field, "") do
    if Field.value_type(field) == :keyword,
      do: {:error, :invalid_parameter_type},
      else: :ok
  end

  defp validate_field_size(:partition_key, value) when is_binary(value) do
    if Limits.valid_partition_key?(value), do: :ok, else: {:error, :query_value_too_large}
  end

  defp validate_field_size(:run_id, value) when is_binary(value) do
    if Limits.valid_run_id?(value), do: :ok, else: {:error, :query_value_too_large}
  end

  defp validate_field_size(_field, value) when is_binary(value) do
    if byte_size(value) <= 1_024, do: :ok, else: {:error, :query_value_too_large}
  end

  defp validate_field_size(_field, _value), do: :ok

  defp validate_same_value_type({left_kind, type, _left}, {right_kind, type, _right})
       when left_kind in [:literal, :parameter] and right_kind in [:literal, :parameter],
       do: :ok

  defp validate_same_value_type(_left, _right), do: {:error, :invalid_parameter_type}

  defp validate_range_order(
         {:literal, _type, lower},
         {:literal, _same_type, upper}
       ) do
    if TupleCodec.compare_values(lower, upper) == :gt,
      do: {:error, :invalid_predicate_range},
      else: :ok
  end

  defp validate_range_order(_lower, _upper), do: :ok

  defp require_tenant_scope(predicates) do
    case Enum.filter(predicates, &partition_predicate?/1) do
      [{:eq, :partition_key, _value}] -> :ok
      _missing_or_ambiguous -> {:error, :unsupported_query_shape}
    end
  end

  defp partition_predicate?({_operator, :partition_key, _value}), do: true
  defp partition_predicate?({_operator, :partition_key, _lower, _upper}), do: true
  defp partition_predicate?(_predicate), do: false

  defp validate_order(order_by) do
    Enum.reduce_while(order_by, {:ok, MapSet.new()}, fn
      {field, direction}, {:ok, seen} when direction in [:asc, :desc] ->
        cond do
          not Field.valid?(field) ->
            {:halt, {:error, :unsupported_field}}

          Field.metadata?(field) ->
            {:halt, {:error, :unsupported_query_shape}}

          Field.value_type(field) != :integer ->
            {:halt, {:error, :unsupported_query_shape}}

          MapSet.member?(seen, field) ->
            {:halt, {:error, :duplicate_order_field}}

          true ->
            {:cont, {:ok, MapSet.put(seen, field)}}
        end

      _invalid, _acc ->
        {:halt, {:error, :unsupported_query_shape}}
    end)
    |> case do
      {:ok, _seen} -> :ok
      {:error, _reason} = error -> error
    end
  end

  defp validate_mode_cursor(:explain, nil), do: :ok
  defp validate_mode_cursor(:explain, _cursor), do: {:error, :query_cursor_invalid}
  defp validate_mode_cursor(:analyze, nil), do: :ok
  defp validate_mode_cursor(:analyze, _cursor), do: {:error, :query_cursor_invalid}
  defp validate_mode_cursor(:execute, _cursor), do: :ok

  defp validate_cursor(nil, _phase), do: :ok

  defp validate_cursor({:parameter, :keyword, name}, :unbound)
       when is_binary(name) and name != "" and byte_size(name) <= 128,
       do: :ok

  defp validate_cursor({:literal, :keyword, value}, :bound) when is_binary(value) do
    size = byte_size(value)

    cond do
      size > @max_cursor_bytes -> {:error, :query_cursor_too_large}
      size < @min_cursor_bytes -> {:error, :query_cursor_invalid}
      true -> :ok
    end
  end

  defp validate_cursor(_cursor, _phase), do: {:error, :query_cursor_invalid}
end
