defmodule Ferricstore.Flow.Query.Shape do
  @moduledoc false

  alias Ferricstore.Flow.Keys
  alias Ferricstore.Flow.Query.Request

  @ids [
    :runs_by_run_id_record,
    :runs_by_partition_and_run_id_record,
    :runs_by_partition_predicates_ordered_records,
    :runs_by_partition_type_state_ordered_records,
    :runs_by_partition_type_terminals_ordered_records,
    :runs_by_partition_metadata_ordered_records,
    :runs_by_partition_type_running_lease_deadline_ordered_records,
    :runs_by_partition_parent_ordered_records,
    :runs_by_partition_root_ordered_records,
    :runs_by_partition_correlation_ordered_records,
    :runs_by_partition_predicates_count,
    :events_by_run_id_ordered_records
  ]

  @names Map.new(@ids, &{&1, Atom.to_string(&1)})

  @terminal_states MapSet.new(~w(completed failed cancelled))
  @maximum_exact_integer 9_007_199_254_740_991

  @type id ::
          :runs_by_run_id_record
          | :runs_by_partition_and_run_id_record
          | :runs_by_partition_predicates_ordered_records
          | :runs_by_partition_type_state_ordered_records
          | :runs_by_partition_type_terminals_ordered_records
          | :runs_by_partition_metadata_ordered_records
          | :runs_by_partition_type_running_lease_deadline_ordered_records
          | :runs_by_partition_parent_ordered_records
          | :runs_by_partition_root_ordered_records
          | :runs_by_partition_correlation_ordered_records
          | :runs_by_partition_predicates_count
          | :events_by_run_id_ordered_records

  @spec known_names() :: [binary()]
  def known_names, do: Enum.map(@ids, &Map.fetch!(@names, &1))

  @spec execution_names() :: [binary()]
  def execution_names, do: known_names()

  @spec known_names?(term()) :: boolean()
  def known_names?(names) when is_list(names),
    do: Enum.all?(names, &(&1 in Map.values(@names)))

  def known_names?(_names), do: false

  @spec family(id()) :: :point | :collection | :count | :history | :lineage
  def family(id) when id in [:runs_by_run_id_record, :runs_by_partition_and_run_id_record],
    do: :point

  def family(:runs_by_partition_predicates_count), do: :count
  def family(:events_by_run_id_ordered_records), do: :history

  def family(id)
      when id in [
             :runs_by_partition_parent_ordered_records,
             :runs_by_partition_root_ordered_records,
             :runs_by_partition_correlation_ordered_records
           ],
      do: :lineage

  def family(id) when id in @ids, do: :collection

  @spec classify(Request.t()) :: {:ok, id()} | {:error, atom()}
  def classify(%Request{} = request) do
    with :ok <- Request.validate_bound(request) do
      do_classify(request)
    end
  end

  def classify(_request), do: {:error, :unsupported_query_shape}

  @spec point_descriptor(Request.t()) ::
          {:ok,
           %{
             partitioning: :auto | :explicit,
             partition_key: binary(),
             run_id: binary()
           }}
          | {:error, :unsupported_query_shape}
  def point_descriptor(%Request{
        version: 1,
        source: :runs,
        predicate:
          {:and,
           [
             {:eq, :partition_key, {:literal, :keyword, partition_key}},
             {:eq, :run_id, {:literal, :keyword, run_id}}
           ]},
        order_by: [],
        limit: 1,
        cursor: nil,
        return: :record
      })
      when is_binary(partition_key) and partition_key != "" and is_binary(run_id) and
             run_id != "" do
    {:ok, %{partitioning: :explicit, partition_key: partition_key, run_id: run_id}}
  end

  def point_descriptor(%Request{
        version: 1,
        source: :runs,
        predicate: {:and, [{:eq, :run_id, {:literal, :keyword, run_id}}]},
        order_by: [],
        limit: 1,
        cursor: nil,
        return: :record
      })
      when is_binary(run_id) and run_id != "" do
    {:ok,
     %{
       partitioning: :auto,
       partition_key: Keys.auto_partition_key(run_id),
       run_id: run_id
     }}
  end

  def point_descriptor(%Request{}), do: {:error, :unsupported_query_shape}

  @spec history_descriptor(Request.t()) ::
          {:ok,
           %{
             partition_key: binary(),
             run_id: binary(),
             direction: :asc | :desc,
             limit: pos_integer()
           }}
          | {:error, :unsupported_query_shape}
  def history_descriptor(%Request{
        version: 1,
        source: :events,
        predicate: {:and, predicates},
        order_by: [{:event_id, direction}],
        limit: limit,
        return: :record
      })
      when direction in [:asc, :desc] and is_list(predicates) and is_integer(limit) do
    with {:ok, run_id} <- unique_keyword_predicate(predicates, :run_id),
         {:ok, partition_key} <- history_partition(predicates, run_id) do
      {:ok,
       %{
         partition_key: partition_key,
         run_id: run_id,
         direction: direction,
         limit: limit
       }}
    end
  end

  def history_descriptor(%Request{}), do: {:error, :unsupported_query_shape}

  @spec fixed_descriptor(Request.t()) :: {:ok, map()} | {:error, atom()}
  def fixed_descriptor(%Request{
        source: :runs,
        predicate: {:and, predicates},
        order_by: [{order_field, direction}],
        limit: limit,
        return: :record
      })
      when is_list(predicates) and direction in [:asc, :desc] and is_integer(limit) do
    initial = %{
      partition_key: nil,
      type: nil,
      states: nil,
      attributes: %{},
      state_meta: %{},
      lineage: nil,
      updated_range: nil,
      lease_range: nil,
      order_field: order_field,
      direction: direction
    }

    with {:ok, descriptor} <- reduce_predicates(predicates, initial),
         :ok <- validate_fixed_descriptor(descriptor) do
      {:ok, descriptor}
    end
  end

  def fixed_descriptor(%Request{}), do: {:error, :unsupported_query_shape}

  @spec terminal_subset?([binary()] | nil) :: boolean()
  def terminal_subset?(states),
    do: terminal_states?(states) and length(states) > 1 and MapSet.new(states) != @terminal_states

  @spec terminal_states?([binary()] | nil) :: boolean()
  def terminal_states?(states) when is_list(states) and states != [],
    do: Enum.all?(states, &MapSet.member?(@terminal_states, &1))

  def terminal_states?(_states), do: false

  defp do_classify(request) do
    case point_descriptor(request) do
      {:ok, %{partitioning: :auto}} ->
        {:ok, :runs_by_run_id_record}

      {:ok, %{partitioning: :explicit}} ->
        {:ok, :runs_by_partition_and_run_id_record}

      {:error, :unsupported_query_shape} ->
        classify_non_point(request)
    end
  end

  defp classify_non_point(%Request{source: :events} = request) do
    with {:ok, _descriptor} <- history_descriptor(request),
         do: {:ok, :events_by_run_id_ordered_records}
  end

  defp classify_non_point(%Request{source: :runs, return: :count}),
    do: {:ok, :runs_by_partition_predicates_count}

  defp classify_non_point(%Request{source: :runs, return: :record} = request) do
    case Request.lineage_descriptor(request) do
      {:ok, %{kind: :parent}} -> {:ok, :runs_by_partition_parent_ordered_records}
      {:ok, %{kind: :root}} -> {:ok, :runs_by_partition_root_ordered_records}
      {:ok, %{kind: :correlation}} -> {:ok, :runs_by_partition_correlation_ordered_records}
      :error -> classify_collection(request)
    end
  end

  defp classify_non_point(%Request{}), do: {:error, :unsupported_query_shape}

  defp classify_collection(request) do
    case fixed_descriptor(request) do
      {:ok, %{lease_range: {_lower, _upper}}} ->
        {:ok, :runs_by_partition_type_running_lease_deadline_ordered_records}

      {:ok, %{attributes: attributes, state_meta: state_meta}}
      when map_size(attributes) > 0 or map_size(state_meta) > 0 ->
        {:ok, :runs_by_partition_metadata_ordered_records}

      {:ok, %{type: type, states: states}} when not is_nil(type) ->
        if terminal_states?(states) and length(states) > 1,
          do: {:ok, :runs_by_partition_type_terminals_ordered_records},
          else: {:ok, :runs_by_partition_type_state_ordered_records}

      _generic_or_unbounded ->
        {:ok, :runs_by_partition_predicates_ordered_records}
    end
  end

  defp unique_keyword_predicate(predicates, field) do
    case Enum.filter(predicates, &match?({:eq, ^field, _value}, &1)) do
      [{:eq, ^field, {:literal, :keyword, value}}] when is_binary(value) and value != "" ->
        {:ok, value}

      _missing_or_ambiguous ->
        {:error, :unsupported_query_shape}
    end
  end

  defp history_partition(predicates, run_id) do
    case Enum.filter(predicates, &match?({:eq, :partition_key, _value}, &1)) do
      [] ->
        {:ok, Keys.auto_partition_key(run_id)}

      [{:eq, :partition_key, {:literal, :keyword, partition_key}}]
      when is_binary(partition_key) and partition_key != "" ->
        {:ok, partition_key}

      _invalid ->
        {:error, :unsupported_query_shape}
    end
  end

  defp reduce_predicates(predicates, initial) do
    Enum.reduce_while(predicates, {:ok, initial}, fn predicate, {:ok, acc} ->
      case put_predicate(acc, predicate) do
        {:ok, next} -> {:cont, {:ok, next}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp put_predicate(acc, {:eq, :partition_key, value}),
    do: put_once(acc, :partition_key, typed_literal(value, :keyword))

  defp put_predicate(acc, {:eq, :type, value}),
    do: put_once(acc, :type, typed_literal(value, :keyword))

  defp put_predicate(acc, {:eq, :state, value}) do
    with {:ok, state} <- typed_literal(value, :keyword),
         do: put_once(acc, :states, {:ok, [state]})
  end

  defp put_predicate(acc, {:in, :state, values}) when is_list(values) do
    with {:ok, states} <- keyword_values(values), do: put_once(acc, :states, {:ok, states})
  end

  defp put_predicate(acc, {:eq, field, value})
       when field in [:parent_flow_id, :root_flow_id, :correlation_id] do
    with {:ok, id} <- typed_literal(value, :keyword),
         do: put_once(acc, :lineage, {:ok, {field, id}})
  end

  defp put_predicate(acc, {:eq, {:attribute, name}, value}) do
    with {:ok, value} <- literal(value),
         false <- Map.has_key?(acc.attributes, name) do
      {:ok, %{acc | attributes: Map.put(acc.attributes, name, value)}}
    else
      true -> {:error, :unsupported_query_shape}
      {:error, _reason} = error -> error
    end
  end

  defp put_predicate(acc, {:eq, {:state_meta, state, name}, value}) do
    with {:ok, value} <- literal(value),
         false <- get_in(acc.state_meta, [state, name]) != nil do
      state_values = Map.get(acc.state_meta, state, %{})
      state_meta = Map.put(acc.state_meta, state, Map.put(state_values, name, value))
      {:ok, %{acc | state_meta: state_meta}}
    else
      true -> {:error, :unsupported_query_shape}
      {:error, _reason} = error -> error
    end
  end

  defp put_predicate(acc, {operator, :updated_at_ms, lower, upper})
       when operator in [:range, :time_window] do
    with {:ok, lower} <- typed_literal(lower, :integer),
         {:ok, upper} <- typed_literal(upper, :integer),
         upper <- if(operator == :time_window, do: upper - 1, else: upper),
         true <- lower >= 0 and lower <= upper and upper <= @maximum_exact_integer do
      put_once(acc, :updated_range, {:ok, {lower, upper}})
    else
      false -> {:error, :unsupported_query_shape}
      {:error, _reason} = error -> error
    end
  end

  defp put_predicate(acc, {:range, :lease_deadline_ms, lower, upper}) do
    with {:ok, lower} <- typed_literal(lower, :integer),
         {:ok, upper} <- typed_literal(upper, :integer),
         true <- lower >= 0 and lower <= upper and upper <= @maximum_exact_integer do
      put_once(acc, :lease_range, {:ok, {lower, upper}})
    else
      false -> {:error, :unsupported_query_shape}
      {:error, _reason} = error -> error
    end
  end

  defp put_predicate(_acc, _predicate), do: {:error, :unsupported_query_shape}

  defp put_once(acc, field, {:ok, value}) do
    if is_nil(Map.fetch!(acc, field)),
      do: {:ok, Map.put(acc, field, value)},
      else: {:error, :unsupported_query_shape}
  end

  defp put_once(_acc, _field, {:error, _reason} = error), do: error

  defp validate_fixed_descriptor(%{partition_key: nil}),
    do: {:error, :unsupported_query_shape}

  defp validate_fixed_descriptor(%{lineage: {_field, _id}} = descriptor) do
    valid =
      descriptor.order_field == :updated_at_ms and is_nil(descriptor.lease_range) and
        map_size(descriptor.attributes) == 0 and map_size(descriptor.state_meta) == 0 and
        is_nil(descriptor.type) and bounded_state_filter?(descriptor.states)

    if valid, do: :ok, else: {:error, :unsupported_query_shape}
  end

  defp validate_fixed_descriptor(%{lease_range: {_lower, _upper}} = descriptor) do
    valid =
      descriptor.order_field == :lease_deadline_ms and descriptor.type != nil and
        descriptor.states == ["running"] and is_nil(descriptor.updated_range) and
        map_size(descriptor.attributes) == 0 and map_size(descriptor.state_meta) == 0

    if valid, do: :ok, else: {:error, :unsupported_query_shape}
  end

  defp validate_fixed_descriptor(%{type: nil, state_meta: state_meta})
       when map_size(state_meta) > 0,
       do: {:error, :query_no_bounded_plan}

  defp validate_fixed_descriptor(descriptor) do
    valid =
      descriptor.order_field == :updated_at_ms and is_nil(descriptor.lineage) and
        is_nil(descriptor.lease_range) and
        (descriptor.type != nil or map_size(descriptor.attributes) > 0 or
           map_size(descriptor.state_meta) > 0) and
        bounded_state_source?(descriptor)

    if valid, do: :ok, else: {:error, :unsupported_query_shape}
  end

  defp bounded_state_source?(%{states: states, attributes: attributes, state_meta: state_meta}) do
    cond do
      map_size(attributes) > 0 or map_size(state_meta) > 0 -> bounded_state_filter?(states)
      is_list(states) and length(states) == 1 -> true
      terminal_states?(states) -> true
      true -> false
    end
  end

  defp bounded_state_filter?(nil), do: true
  defp bounded_state_filter?([_state]), do: true
  defp bounded_state_filter?(states), do: terminal_states?(states)

  defp keyword_values(values) do
    Enum.reduce_while(values, {:ok, []}, fn value, {:ok, acc} ->
      case typed_literal(value, :keyword) do
        {:ok, decoded} -> {:cont, {:ok, [decoded | acc]}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, reversed} -> {:ok, Enum.reverse(reversed)}
      {:error, _reason} = error -> error
    end
  end

  defp literal({:literal, _type, value}), do: {:ok, value}
  defp literal(_value), do: {:error, :unsupported_query_shape}

  defp typed_literal({:literal, type, value}, type), do: {:ok, value}
  defp typed_literal(_value, _type), do: {:error, :unsupported_query_shape}
end
