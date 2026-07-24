defmodule Ferricstore.Flow.Query.RecordProjection do
  @moduledoc false

  alias Ferricstore.Flow.Query.{Field, Limits}

  # Query records are an allowlist so newly added storage/control fields never
  # become remotely visible without an explicit query-contract decision.
  @fields [
    :id,
    :type,
    :state,
    :version,
    :priority,
    :partition_key,
    :created_at_ms,
    :updated_at_ms,
    :next_run_at_ms,
    :lease_deadline_ms,
    :attempts,
    :run_state,
    :max_active_ms,
    :parent_flow_id,
    :root_flow_id,
    :correlation_id,
    :attributes,
    :state_meta
  ]
  @event_fields [:event_id, :fields]
  @field_set MapSet.new(@fields)
  @event_field_set MapSet.new(@event_fields)

  @type selector :: Field.t() | :attributes | :state_meta | :fields | {:event_field, binary()}
  @type projection :: :all | [selector()]

  @doc false
  @spec fields() :: [atom()]
  def fields, do: @fields

  @doc false
  @spec supported_external_names(:runs | :events) :: [binary()]
  def supported_external_names(:runs) do
    Field.supported_external_names()
    |> Enum.reject(&(&1 == "event_id"))
    |> Kernel.++(["attributes", "state_meta"])
    |> Enum.sort()
  end

  def supported_external_names(:events),
    do: ["event_id", "fields", "fields['<name>']"]

  @doc false
  @spec allowlisted_record?(term(), :runs | :events) :: boolean()
  def allowlisted_record?(record, source) when is_map(record) and source in [:runs, :events] do
    allowed = if source == :runs, do: @field_set, else: @event_field_set
    Enum.all?(record, fn {field, _value} -> MapSet.member?(allowed, field) end)
  end

  def allowlisted_record?(_record, _source), do: false

  @spec project_result({:ok, map() | nil} | {:error, term()}) ::
          {:ok, map() | nil} | {:error, term()}
  def project_result({:ok, record}) when is_map(record),
    do: {:ok, :maps.with(@fields, record)}

  def project_result(result), do: result

  @doc false
  @spec validate(:runs | :events, projection()) :: :ok | {:error, atom()}
  def validate(source, :all) when source in [:runs, :events], do: :ok

  def validate(source, projection) when source in [:runs, :events] and is_list(projection) do
    cond do
      projection == [] ->
        {:error, :unsupported_query_shape}

      length(projection) > Limits.max_return_fields() ->
        {:error, :query_projection_limit_exceeded}

      length(projection) != length(Enum.uniq(projection)) ->
        {:error, :duplicate_projection_field}

      Enum.all?(projection, &valid_selector?(source, &1)) ->
        :ok

      true ->
        {:error, :unsupported_field}
    end
  end

  def validate(_source, _projection), do: {:error, :unsupported_query_shape}

  @doc false
  @spec external_names(projection()) :: :all | [binary()]
  def external_names(:all), do: :all

  def external_names(projection) when is_list(projection),
    do: Enum.map(projection, &external_name/1)

  @doc false
  @spec project_result(
          {:ok, map() | nil} | {:error, term()},
          :runs | :events,
          projection()
        ) :: {:ok, map() | nil} | {:error, term()}
  def project_result({:ok, record}, :runs, :all) when is_map(record),
    do: {:ok, :maps.with(@fields, record)}

  def project_result({:ok, record}, :events, :all) when is_map(record),
    do: {:ok, :maps.with(@event_fields, record)}

  def project_result({:ok, record}, source, projection)
      when is_map(record) and source in [:runs, :events] and is_list(projection) do
    with :ok <- validate(source, projection) do
      {:ok, project_record(record, source, projection)}
    end
  end

  def project_result({:ok, nil}, source, projection) when source in [:runs, :events] do
    with :ok <- validate(source, projection), do: {:ok, nil}
  end

  def project_result({:error, _reason} = error, _source, _projection), do: error
  def project_result(_result, _source, _projection), do: {:error, :query_storage_inconsistent}

  @doc false
  @spec project_records([map()], :runs | :events, projection()) ::
          {:ok, [map()]} | {:error, atom()}
  def project_records(records, source, projection)
      when is_list(records) and source in [:runs, :events] do
    with :ok <- validate(source, projection) do
      records
      |> Enum.reduce_while({:ok, []}, fn
        record, {:ok, acc} when is_map(record) ->
          {:cont, {:ok, [project_record(record, source, projection) | acc]}}

        _record, _acc ->
          {:halt, {:error, :query_storage_inconsistent}}
      end)
      |> case do
        {:ok, reversed} -> {:ok, Enum.reverse(reversed)}
        {:error, _reason} = error -> error
      end
    end
  end

  def project_records(_records, _source, _projection),
    do: {:error, :query_storage_inconsistent}

  @doc false
  @spec project_validated(map(), :runs | :events, projection()) :: {:ok, map()}
  def project_validated(record, source, projection)
      when is_map(record) and source in [:runs, :events],
      do: {:ok, project_record(record, source, projection)}

  defp valid_selector?(:runs, :event_id), do: false
  defp valid_selector?(:runs, selector) when selector in [:attributes, :state_meta], do: true
  defp valid_selector?(:runs, selector), do: Field.valid?(selector)

  defp valid_selector?(:events, selector) when selector in [:event_id, :fields], do: true

  defp valid_selector?(:events, {:event_field, name}),
    do: Field.valid_dynamic_name?(name)

  defp valid_selector?(_source, _selector), do: false

  defp external_name(:fields), do: "fields"
  defp external_name({:event_field, name}), do: "fields[" <> quote_segment(name) <> "]"
  defp external_name(field), do: Field.external_name(field)

  defp project_record(record, :runs, :all), do: :maps.with(@fields, record)
  defp project_record(record, :events, :all), do: :maps.with(@event_fields, record)

  defp project_record(record, _source, projection),
    do: Enum.reduce(projection, %{}, &project_selector(&1, record, &2))

  defp project_selector(:run_id, record, projected),
    do: put_fetched(projected, :id, Field.fetch(record, :run_id))

  defp project_selector({:attribute, name} = field, record, projected),
    do: put_nested(projected, [:attributes, name], Field.fetch(record, field))

  defp project_selector({:state_meta, state, name} = field, record, projected),
    do: put_nested(projected, [:state_meta, state, name], Field.fetch(record, field))

  defp project_selector({:event_field, name}, record, projected) do
    value =
      with {:ok, fields} when is_map(fields) <- fetch_map(record, :fields, "fields") do
        fetch_dynamic(fields, name)
      else
        _missing -> :missing
      end

    put_nested(projected, [:fields, name], value)
  end

  defp project_selector(:fields, record, projected),
    do: put_fetched(projected, :fields, fetch_map(record, :fields, "fields"))

  defp project_selector(field, record, projected) when is_atom(field),
    do: put_fetched(projected, field, Field.fetch(record, field))

  defp put_fetched(projected, _key, :missing), do: projected
  defp put_fetched(projected, key, {:ok, value}), do: Map.put(projected, key, value)

  defp put_nested(projected, _path, :missing), do: projected

  defp put_nested(projected, [root, name], {:ok, value}) do
    Map.update(projected, root, %{name => value}, &Map.put(&1, name, value))
  end

  defp put_nested(projected, [root, branch, name], {:ok, value}) do
    nested =
      projected
      |> Map.get(root, %{})
      |> Map.update(branch, %{name => value}, &Map.put(&1, name, value))

    Map.put(projected, root, nested)
  end

  defp fetch_map(map, atom_key, string_key) do
    case Map.fetch(map, atom_key) do
      {:ok, value} -> {:ok, value}
      :error -> fetch_dynamic(map, string_key)
    end
  end

  defp fetch_dynamic(map, key) do
    case Map.fetch(map, key) do
      {:ok, value} -> {:ok, value}
      :error -> :missing
    end
  end

  defp quote_segment(value), do: "'" <> String.replace(value, "'", "''") <> "'"
end
