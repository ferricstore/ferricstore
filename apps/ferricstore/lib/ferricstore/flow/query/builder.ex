defmodule Ferricstore.Flow.Query.Builder do
  @moduledoc false

  alias Ferricstore.Flow.Query.{Field, RecordProjection}

  @maximum_exact_integer 9_007_199_254_740_991
  @minimum_i64 -9_223_372_036_854_775_808
  @maximum_i64 9_223_372_036_854_775_807
  @terminal_states ~w(completed failed cancelled)
  @lineage_fields %{
    by_parent: "parent_flow_id",
    by_root: "root_flow_id",
    by_correlation: "correlation_id"
  }

  @type kind ::
          :list
          | :search
          | :terminals
          | :failures
          | :stuck
          | :by_parent
          | :by_root
          | :by_correlation

  @spec build(kind(), map()) ::
          {:ok, %{query: binary(), params: map()}}
          | {:error,
             :invalid_query_filter
             | :duplicate_projection_field
             | :query_projection_limit_exceeded
             | :query_filter_required
             | :query_limit_exceeded
             | :query_partition_required
             | :unsupported_field
             | :unsupported_query_shape}
  def build(kind, filters) when is_map(filters) do
    with :ok <- validate_kind(kind),
         {:ok, partition_key} <-
           required_binary(filters, :partition_key, :query_partition_required),
         {:ok, limit} <- limit(filters),
         {:ok, direction} <- direction(filters),
         {:ok, predicates, params, order_field} <- predicates(kind, filters),
         {:ok, predicates, params} <- time_predicate(predicates, params, filters, order_field),
         {:ok, cursor, params} <- cursor(filters, params),
         {:ok, return_clause} <- return_clause(filters) do
      predicates = ["partition_key = @partition_key" | predicates]
      params = Map.put(params, "partition_key", partition_key)

      query =
        IO.iodata_to_binary([
          "FROM runs WHERE ",
          Enum.join(predicates, " AND "),
          " ORDER BY ",
          order_field,
          " ",
          direction,
          " LIMIT ",
          Integer.to_string(limit),
          cursor,
          return_clause
        ])

      {:ok, %{query: query, params: params}}
    end
  end

  def build(_kind, _filters), do: {:error, :invalid_query_filter}

  defp validate_kind(kind)
       when kind in [
              :list,
              :search,
              :terminals,
              :failures,
              :stuck,
              :by_parent,
              :by_root,
              :by_correlation
            ],
       do: :ok

  defp validate_kind(_kind), do: {:error, :unsupported_query_shape}

  defp predicates(kind, filters) when kind in [:by_parent, :by_root, :by_correlation] do
    with {:ok, id} <- required_binary(filters, :id, :invalid_query_filter),
         {:ok, predicates, params} <- optional_common_predicates(filters) do
      field = Map.fetch!(@lineage_fields, kind)

      {:ok, [field <> " = @lineage_id" | predicates], Map.put(params, "lineage_id", id),
       "updated_at_ms"}
    end
  end

  defp predicates(:stuck, filters) do
    with {:ok, type} <- required_binary(filters, :type, :invalid_query_filter),
         {:ok, now_ms} <- non_negative_integer(filters, :now_ms, System.system_time(:millisecond)),
         {:ok, from_ms, to_ms} <- stuck_window(filters, now_ms) do
      predicates = [
        "type = @type",
        "state = @state",
        "lease_deadline_ms BETWEEN @lease_from_ms AND @lease_to_ms"
      ]

      params = %{
        "type" => type,
        "state" => "running",
        "lease_from_ms" => from_ms,
        "lease_to_ms" => to_ms
      }

      {:ok, predicates, params, "lease_deadline_ms"}
    end
  end

  defp predicates(:terminals, filters) do
    with {:ok, type} <- required_binary(filters, :type, :invalid_query_filter),
         {:ok, state_predicate, state_params} <- terminal_predicate(Map.get(filters, :state)) do
      {:ok, ["type = @type", state_predicate], Map.put(state_params, "type", type),
       "updated_at_ms"}
    end
  end

  defp predicates(:failures, filters) do
    with {:ok, type} <- required_binary(filters, :type, :invalid_query_filter) do
      {:ok, ["type = @type", "state = @state"], %{"type" => type, "state" => "failed"},
       "updated_at_ms"}
    end
  end

  defp predicates(:list, filters) do
    filters = default_list_state(filters)

    with {:ok, predicates, params} <- optional_common_predicates(filters),
         :ok <- require_type(:list, filters) do
      {:ok, predicates, params, "updated_at_ms"}
    end
  end

  defp predicates(:search, filters) do
    with {:ok, predicates, params} <- optional_common_predicates(filters),
         :ok <- require_search_filter(:search, filters) do
      {:ok, predicates, params, "updated_at_ms"}
    end
  end

  defp default_list_state(filters) do
    case Map.get(filters, :state) do
      nil -> Map.put(filters, :state, "queued")
      _state -> filters
    end
  end

  defp optional_common_predicates(filters) do
    with {:ok, predicates, params} <- optional_type([], %{}, Map.get(filters, :type)),
         {:ok, predicates, params} <- optional_state(predicates, params, Map.get(filters, :state)),
         {:ok, predicates, params} <- optional_attribute(predicates, params, filters),
         {:ok, predicates, params} <- optional_state_meta(predicates, params, filters) do
      {:ok, Enum.reverse(predicates), params}
    end
  end

  defp optional_type(predicates, params, nil), do: {:ok, predicates, params}
  defp optional_type(predicates, params, "any"), do: {:ok, predicates, params}

  defp optional_type(predicates, params, type) when is_binary(type) and type != "" do
    {:ok, ["type = @type" | predicates], Map.put(params, "type", type)}
  end

  defp optional_type(_predicates, _params, _type), do: {:error, :invalid_query_filter}

  defp optional_state(predicates, params, state) when state in [nil, "any"],
    do: {:ok, predicates, params}

  defp optional_state(predicates, params, state) when is_binary(state) and state != "" do
    {:ok, ["state = @state" | predicates], Map.put(params, "state", state)}
  end

  defp optional_state(_predicates, _params, _state), do: {:error, :invalid_query_filter}

  defp optional_attribute(predicates, params, %{attribute: {name, value}})
       when is_binary(name) do
    field = {:attribute, name}

    with :ok <- validate_scalar(value),
         true <- Field.valid?(field) do
      external = Field.external_name(field)

      {:ok, [external <> " = @attribute_value" | predicates],
       Map.put(params, "attribute_value", value)}
    else
      false -> {:error, :unsupported_field}
      {:error, _reason} = error -> error
    end
  end

  defp optional_attribute(predicates, params, filters) do
    if Map.has_key?(filters, :attribute),
      do: {:error, :invalid_query_filter},
      else: {:ok, predicates, params}
  end

  defp optional_state_meta(predicates, params, %{state_meta: {state, name, value}})
       when is_binary(state) and is_binary(name) do
    field = {:state_meta, state, name}

    with :ok <- validate_scalar(value),
         true <- Field.valid?(field) do
      external = Field.external_name(field)

      {:ok, [external <> " = @state_meta_value" | predicates],
       Map.put(params, "state_meta_value", value)}
    else
      false -> {:error, :unsupported_field}
      {:error, _reason} = error -> error
    end
  end

  defp optional_state_meta(predicates, params, filters) do
    if Map.has_key?(filters, :state_meta),
      do: {:error, :invalid_query_filter},
      else: {:ok, predicates, params}
  end

  defp terminal_predicate(state) when state in [nil, "any"] do
    params =
      @terminal_states
      |> Enum.with_index()
      |> Map.new(fn {value, index} -> {"terminal_#{index}", value} end)

    {:ok, "state IN (@terminal_0, @terminal_1, @terminal_2)", params}
  end

  defp terminal_predicate(state) when state in @terminal_states,
    do: {:ok, "state = @state", %{"state" => state}}

  defp terminal_predicate(_state), do: {:error, :invalid_query_filter}

  defp require_type(:list, %{type: type}) when is_binary(type) and type != "", do: :ok
  defp require_type(:list, _filters), do: {:error, :invalid_query_filter}

  defp require_search_filter(:search, filters) do
    if Map.has_key?(filters, :attribute) or Map.has_key?(filters, :state_meta),
      do: :ok,
      else: {:error, :query_filter_required}
  end

  defp time_predicate(predicates, params, filters, "updated_at_ms") do
    from_ms = Map.get(filters, :from_ms)
    to_ms = Map.get(filters, :to_ms)

    if is_nil(from_ms) and is_nil(to_ms) do
      {:ok, predicates, params}
    else
      with {:ok, from_ms} <- optional_non_negative_integer(from_ms, 0),
           {:ok, to_ms} <- optional_non_negative_integer(to_ms, @maximum_exact_integer),
           true <- from_ms <= to_ms do
        {:ok, predicates ++ ["updated_at_ms BETWEEN @from_ms AND @to_ms"],
         params |> Map.put("from_ms", from_ms) |> Map.put("to_ms", to_ms)}
      else
        false -> {:error, :invalid_query_filter}
        {:error, _reason} = error -> error
      end
    end
  end

  defp time_predicate(predicates, params, _filters, "lease_deadline_ms"),
    do: {:ok, predicates, params}

  defp time_predicate(predicates, params, filters, _order_field) do
    if is_nil(Map.get(filters, :from_ms)) and is_nil(Map.get(filters, :to_ms)),
      do: {:ok, predicates, params},
      else: {:error, :invalid_query_filter}
  end

  defp stuck_window(filters, now_ms) do
    with {:ok, from_ms} <- optional_non_negative_integer(Map.get(filters, :from_ms), 0),
         {:ok, requested_to_ms} <- optional_non_negative_integer(Map.get(filters, :to_ms), now_ms),
         to_ms <- min(requested_to_ms, now_ms),
         true <- from_ms <= to_ms do
      {:ok, from_ms, to_ms}
    else
      false -> {:error, :invalid_query_filter}
      {:error, _reason} = error -> error
    end
  end

  defp limit(filters) do
    case Map.get(filters, :limit, 100) do
      value when is_integer(value) and value > 0 and value <= 100 -> {:ok, value}
      value when is_integer(value) and value > 100 -> {:error, :query_limit_exceeded}
      _invalid -> {:error, :invalid_query_filter}
    end
  end

  defp direction(filters) do
    case Map.get(filters, :direction, if(Map.get(filters, :rev) == true, do: :desc, else: :asc)) do
      :asc -> {:ok, "ASC"}
      :desc -> {:ok, "DESC"}
      _invalid -> {:error, :invalid_query_filter}
    end
  end

  defp cursor(filters, params) do
    case Map.get(filters, :cursor) do
      nil ->
        {:ok, "", params}

      value when is_binary(value) and value != "" ->
        {:ok, " CURSOR @cursor", Map.put(params, "cursor", value)}

      _invalid ->
        {:error, :invalid_query_filter}
    end
  end

  defp return_clause(filters) do
    projection = Map.get(filters, :projection, :all)

    with :ok <- RecordProjection.validate(:runs, projection) do
      case RecordProjection.external_names(projection) do
        :all ->
          {:ok, " RETURN RECORDS"}

        fields ->
          {:ok,
           IO.iodata_to_binary([
             " RETURN RECORDS (",
             Enum.intersperse(fields, ", "),
             ")"
           ])}
      end
    end
  end

  defp required_binary(filters, key, error) do
    case Map.get(filters, key) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _invalid -> {:error, error}
    end
  end

  defp non_negative_integer(filters, key, default) do
    case Map.get(filters, key, default) do
      value when is_integer(value) and value >= 0 and value <= @maximum_exact_integer ->
        {:ok, value}

      _invalid ->
        {:error, :invalid_query_filter}
    end
  end

  defp optional_non_negative_integer(nil, default), do: {:ok, default}

  defp optional_non_negative_integer(value, _default)
       when is_integer(value) and value >= 0 and value <= @maximum_exact_integer,
       do: {:ok, value}

  defp optional_non_negative_integer(_value, _default), do: {:error, :invalid_query_filter}

  defp validate_scalar(value) when is_binary(value), do: :ok

  defp validate_scalar(value)
       when is_integer(value) and value >= @minimum_i64 and value <= @maximum_i64,
       do: :ok

  defp validate_scalar(value) when is_boolean(value), do: :ok

  defp validate_scalar(value) when is_float(value) do
    <<bits::unsigned-big-64>> = <<value::float-big-64>>

    if Bitwise.band(Bitwise.bsr(bits, 52), 0x7FF) == 0x7FF,
      do: {:error, :invalid_query_filter},
      else: :ok
  end

  defp validate_scalar(_value), do: {:error, :invalid_query_filter}
end
