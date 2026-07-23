defmodule Ferricstore.Flow.ScopeBinding do
  @moduledoc false

  alias FerricStore.Flow.MetadataExtension
  alias Ferricstore.Flow.{Keys, StorageScope, SystemMetadata}

  @read_scope_key {__MODULE__, :resolved_read_scope}

  @spec bind_mutation(map(), atom(), map()) :: {:ok, map()} | {:error, binary()}
  def bind_mutation(ctx, operation, attrs)
      when is_map(ctx) and is_atom(operation) and is_map(attrs) do
    with {:ok, metadata} <- resolve_mutation(ctx, operation),
         do: bind_resolved(attrs, metadata)
  end

  @spec bind_mutations(map(), atom(), [map()]) :: {:ok, [map()]} | {:error, binary()}
  def bind_mutations(ctx, operation, attrs_list)
      when is_map(ctx) and is_atom(operation) and is_list(attrs_list) do
    with {:ok, metadata} <- resolve_mutation(ctx, operation),
         do: bind_many_resolved(attrs_list, metadata)
  end

  @doc false
  @spec resolve_mutation(map(), atom()) :: {:ok, SystemMetadata.t()} | {:error, binary()}
  def resolve_mutation(ctx, operation) when is_map(ctx) and is_atom(operation) do
    case MetadataExtension.bind_write(ctx, operation) do
      {:ok, metadata} -> {:ok, metadata}
      {:error, reason} -> mutation_error(reason)
    end
  end

  @doc false
  @spec bind_resolved(map(), SystemMetadata.t()) :: {:ok, map()} | {:error, binary()}
  def bind_resolved(attrs, metadata) when is_map(attrs) and is_map(metadata) do
    case attach_metadata(attrs, metadata) do
      {:ok, scoped} -> {:ok, scoped}
      {:error, reason} -> mutation_error(reason)
    end
  end

  @doc false
  @spec bind_many_resolved([map()], SystemMetadata.t()) ::
          {:ok, [map()]} | {:error, binary()}
  def bind_many_resolved(attrs_list, metadata) when is_list(attrs_list) and is_map(metadata) do
    case attach_many(attrs_list, metadata) do
      {:ok, scoped} -> {:ok, scoped}
      {:error, reason} -> mutation_error(reason)
    end
  end

  @spec batch_partition_key(binary() | nil, [map()]) ::
          {:ok, binary() | nil} | {:error, binary()}
  def batch_partition_key(nil, _attrs_list), do: {:ok, nil}

  def batch_partition_key(partition_key, []) when is_binary(partition_key),
    do: {:ok, partition_key}

  def batch_partition_key(partition_key, [%{partition_key: scoped} | rest])
      when is_binary(partition_key) and is_binary(scoped) do
    if Enum.all?(rest, &(Map.get(&1, :partition_key) == scoped)),
      do: {:ok, scoped},
      else: {:error, "ERR flow partition_key mismatch in batch"}
  end

  def batch_partition_key(_partition_key, _attrs_list),
    do: {:error, "ERR flow partition_key mismatch in batch"}

  @spec bind_read_partition(map(), atom(), binary(), binary() | nil) ::
          {:ok, binary(), SystemMetadata.t()} | {:error, binary()}
  def bind_read_partition(ctx, source, id, partition_key)
      when is_map(ctx) and is_atom(source) and is_binary(id) and
             (is_binary(partition_key) or is_nil(partition_key)) do
    with {:ok, [metadata]} <- MetadataExtension.bind_query_metadata(ctx, source),
         logical_partition_key <- partition_key || Keys.auto_partition_key(id),
         record <- SystemMetadata.put_record(%{partition_key: logical_partition_key}, metadata),
         {:ok, physical_partition_key} <- StorageScope.physical_partition_key(record) do
      {:ok, physical_partition_key, metadata}
    else
      {:ok, _union} -> mutation_error(:flow_scope_union_not_supported)
      {:error, reason} -> mutation_error(reason)
    end
  end

  @doc false
  @spec bind_resolved_read_partition(binary(), binary() | nil, SystemMetadata.t()) ::
          {:ok, binary(), SystemMetadata.t()} | {:error, binary()}
  def bind_resolved_read_partition(id, partition_key, metadata)
      when is_binary(id) and (is_binary(partition_key) or is_nil(partition_key)) and
             is_map(metadata) do
    with :ok <- SystemMetadata.validate(metadata),
         logical_partition_key <- partition_key || Keys.auto_partition_key(id),
         record <- SystemMetadata.put_record(%{partition_key: logical_partition_key}, metadata),
         {:ok, physical_partition_key} <- StorageScope.physical_partition_key(record) do
      {:ok, physical_partition_key, metadata}
    else
      {:error, reason} -> mutation_error(reason)
    end
  end

  def bind_resolved_read_partition(_id, _partition_key, _metadata),
    do: mutation_error(:invalid_flow_system_metadata)

  @spec bind_read_partition_selector(map(), atom(), binary() | nil | :auto) ::
          {:ok, map(), binary() | nil | :auto, SystemMetadata.t()} | {:error, binary()}
  def bind_read_partition_selector(ctx, source, partition_key)
      when is_map(ctx) and is_atom(source) and
             (is_binary(partition_key) or is_nil(partition_key) or partition_key == :auto) do
    with {:ok, [metadata]} <- MetadataExtension.bind_query_metadata(ctx, source),
         {:ok, scope_prefix} <- SystemMetadata.scope_prefix(metadata),
         {:ok, physical_partition_key} <-
           bind_partition_selector(partition_key, scope_prefix) do
      scoped_ctx = Map.put(ctx, @read_scope_key, {metadata, scope_prefix})
      {:ok, scoped_ctx, physical_partition_key, metadata}
    else
      {:ok, _union} -> mutation_error(:flow_scope_union_not_supported)
      {:error, reason} -> mutation_error(reason)
    end
  end

  def bind_read_partition_selector(_ctx, _source, _partition_key),
    do: mutation_error(:invalid_flow_system_metadata)

  @doc false
  @spec put_resolved_read_scope(map(), SystemMetadata.t()) ::
          {:ok, map()} | {:error, binary()}
  def put_resolved_read_scope(ctx, metadata) when is_map(ctx) and is_map(metadata) do
    with :ok <- SystemMetadata.validate(metadata),
         {:ok, scope_prefix} <- SystemMetadata.scope_prefix(metadata) do
      {:ok, Map.put(ctx, @read_scope_key, {metadata, scope_prefix})}
    else
      {:error, reason} -> mutation_error(reason)
    end
  end

  def put_resolved_read_scope(_ctx, _metadata),
    do: mutation_error(:invalid_flow_system_metadata)

  @doc false
  @spec auto_partition_keys(map()) :: [binary()]
  def auto_partition_keys(ctx) when is_map(ctx) do
    case Map.get(ctx, @read_scope_key) do
      {_metadata, nil} ->
        Keys.auto_partition_keys()

      {_metadata, scope_prefix} when is_binary(scope_prefix) ->
        Enum.map(Keys.auto_partition_keys(), fn logical_partition_key ->
          {:ok, physical_partition_key} =
            StorageScope.physical_partition_key(logical_partition_key, scope_prefix)

          physical_partition_key
        end)

      _unbound ->
        Keys.auto_partition_keys()
    end
  end

  @doc false
  @spec auto_partition_key(map(), binary()) :: binary()
  def auto_partition_key(ctx, id) when is_map(ctx) and is_binary(id) do
    logical_partition_key = Keys.auto_partition_key(id)

    case Map.get(ctx, @read_scope_key) do
      {_metadata, scope_prefix} when is_binary(scope_prefix) ->
        {:ok, physical_partition_key} =
          StorageScope.physical_partition_key(logical_partition_key, scope_prefix)

        physical_partition_key

      _dedicated_or_unbound ->
        logical_partition_key
    end
  end

  @doc false
  @spec verify_context_read_result(term(), map()) :: term()
  def verify_context_read_result(result, ctx) when is_map(ctx) do
    case Map.get(ctx, @read_scope_key) do
      {expected_metadata, _scope_prefix} -> verify_read_result(result, expected_metadata)
      _unbound -> result
    end
  end

  @spec verify_read_result(term(), SystemMetadata.t()) :: term()
  def verify_read_result({:ok, record}, expected_metadata) when is_map(record) do
    if Map.get(record, :system_metadata, %{}) == expected_metadata,
      do: {:ok, record},
      else: {:error, "NOPERM Flow scope is not authorized"}
  end

  def verify_read_result({:ok, records}, expected_metadata) when is_list(records) do
    if Enum.all?(records, fn
         record when is_map(record) -> Map.get(record, :system_metadata, %{}) == expected_metadata
         _invalid -> false
       end),
       do: {:ok, records},
       else: {:error, "NOPERM Flow scope is not authorized"}
  end

  def verify_read_result(result, _expected_metadata), do: result

  defp bind_partition_selector(:auto, _scope_prefix), do: {:ok, :auto}
  defp bind_partition_selector(nil, nil), do: {:ok, nil}
  defp bind_partition_selector(nil, scope_prefix) when is_binary(scope_prefix), do: {:ok, :auto}

  defp bind_partition_selector(partition_key, scope_prefix) when is_binary(partition_key),
    do: StorageScope.physical_partition_key(partition_key, scope_prefix)

  defp attach_metadata(attrs, %{} = metadata) when map_size(metadata) == 0 do
    case Map.get(attrs, :system_metadata, %{}) do
      existing when existing == %{} -> {:ok, Map.delete(attrs, :system_metadata)}
      _forged -> {:error, :invalid_flow_system_metadata}
    end
  end

  defp attach_metadata(%{id: id} = attrs, metadata) when is_binary(id) do
    existing = Map.get(attrs, :system_metadata, %{})

    if existing == %{} or existing == metadata do
      logical_partition_key = Map.get(attrs, :partition_key) || Keys.auto_partition_key(id)

      scoped =
        attrs
        |> Map.put(:partition_key, logical_partition_key)
        |> SystemMetadata.put_record(metadata)

      with {:ok, physical_partition_key} <- StorageScope.physical_partition_key(scoped) do
        scoped = Map.put(scoped, :partition_key, physical_partition_key)
        attach_children(scoped, metadata)
      end
    else
      {:error, :invalid_flow_system_metadata}
    end
  end

  defp attach_metadata(_attrs, _metadata), do: {:error, :invalid_flow_system_metadata}

  defp attach_children(%{children: children} = attrs, metadata) when is_list(children) do
    with {:ok, children} <- attach_many(children, metadata),
         do: {:ok, %{attrs | children: children}}
  end

  defp attach_children(attrs, _metadata), do: {:ok, attrs}

  defp attach_many(attrs_list, metadata) do
    Enum.reduce_while(attrs_list, {:ok, []}, fn attrs, {:ok, acc} ->
      case attach_metadata(attrs, metadata) do
        {:ok, scoped} -> {:cont, {:ok, [scoped | acc]}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, reversed} -> {:ok, Enum.reverse(reversed)}
      {:error, _reason} = error -> error
    end
  end

  defp mutation_error(:flow_scope_required),
    do: {:error, "NOPERM Flow scope is required"}

  defp mutation_error(:flow_scope_union_not_supported),
    do: {:error, "NOPERM Flow scope union is not supported for this operation"}

  defp mutation_error(:invalid_flow_system_metadata),
    do: {:error, "ERR invalid Flow system metadata"}

  defp mutation_error(:flow_metadata_extension_unavailable),
    do: {:error, "ERR Flow metadata extension is unavailable"}

  defp mutation_error(_reason),
    do: {:error, "ERR Flow metadata extension failed"}
end
