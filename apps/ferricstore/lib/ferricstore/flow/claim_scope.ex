defmodule Ferricstore.Flow.ClaimScope do
  @moduledoc false

  alias Ferricstore.Flow.{Keys, ScopeBinding, StorageScope, SystemMetadata}

  @spec resolve(map()) :: {:ok, SystemMetadata.t()} | {:error, binary()}
  def resolve(ctx) when is_map(ctx), do: ScopeBinding.resolve_mutation(ctx, :claim_due)
  def resolve(_ctx), do: {:error, "ERR invalid Flow claim scope"}

  @spec bind_attrs(map(), map()) ::
          {:ok, map(), SystemMetadata.t()} | {:error, binary()}
  def bind_attrs(ctx, attrs) when is_map(ctx) and is_map(attrs) do
    with {:ok, metadata} <- resolve(ctx),
         do: bind_resolved_attrs(attrs, metadata)
  end

  def bind_attrs(_ctx, _attrs), do: {:error, "ERR invalid Flow claim scope"}

  @doc false
  @spec bind_resolved_attrs(map(), SystemMetadata.t()) ::
          {:ok, map(), SystemMetadata.t()} | {:error, binary()}
  def bind_resolved_attrs(attrs, metadata) when is_map(attrs) and is_map(metadata) do
    with :ok <- validate_metadata(metadata),
         {:ok, scope_prefix} <- scope_prefix(metadata),
         {:ok, partition_key, partition_keys} <-
           bind_partitions(
             Map.get(attrs, :partition_key),
             Map.get(attrs, :partition_keys),
             scope_prefix
           ) do
      attrs =
        attrs
        |> Map.put(:partition_key, partition_key)
        |> maybe_put_partition_keys(partition_keys)
        |> maybe_put_metadata(metadata)

      {:ok, attrs, metadata}
    end
  end

  def bind_resolved_attrs(_attrs, _metadata), do: {:error, "ERR invalid Flow claim scope"}

  @doc false
  @spec bind_prepared_commands(map(), [term()]) :: [term()]
  def bind_prepared_commands(ctx, commands) when is_map(ctx) and is_list(commands) do
    if Enum.any?(commands, &match?({:ok, _claim}, &1)) do
      case resolve(ctx) do
        {:ok, metadata} -> Enum.map(commands, &bind_prepared_command(&1, metadata))
        {:error, _reason} = error -> Enum.map(commands, &replace_prepared_command(&1, error))
      end
    else
      commands
    end
  end

  def bind_prepared_commands(_ctx, commands) when is_list(commands), do: commands

  @spec verify_records([term()], SystemMetadata.t()) :: :ok | {:error, binary()}
  def verify_records(records, expected_metadata)
      when is_list(records) and is_map(expected_metadata) do
    if Enum.all?(records, fn
         record when is_map(record) ->
           Map.get(record, :system_metadata, %{}) == expected_metadata

         _invalid ->
           false
       end),
       do: :ok,
       else: {:error, "NOPERM Flow scope is not authorized"}
  end

  def verify_records(_records, _expected_metadata),
    do: {:error, "NOPERM Flow scope is not authorized"}

  defp bind_prepared_command({:ok, %{attrs: attrs} = claim}, metadata) do
    case bind_resolved_attrs(attrs, metadata) do
      {:ok, scoped_attrs, expected_metadata} ->
        {:ok,
         claim
         |> Map.put(:attrs, scoped_attrs)
         |> Map.put(:expected_metadata, expected_metadata)}

      {:error, _reason} = error ->
        error
    end
  end

  defp bind_prepared_command(other, _metadata), do: other

  defp replace_prepared_command({:ok, _claim}, error), do: error
  defp replace_prepared_command(other, _error), do: other

  defp validate_metadata(metadata) do
    case SystemMetadata.validate(metadata) do
      :ok -> :ok
      {:error, _reason} -> {:error, "ERR invalid Flow system metadata"}
    end
  end

  defp scope_prefix(metadata) do
    case SystemMetadata.scope_prefix(metadata) do
      {:ok, scope_prefix} -> {:ok, scope_prefix}
      {:error, _reason} -> {:error, "ERR invalid Flow system metadata"}
    end
  end

  defp bind_partitions(partition_key, partition_keys, nil),
    do: {:ok, partition_key, partition_keys}

  defp bind_partitions(_partition_key, partition_keys, scope_prefix)
       when is_list(partition_keys) and partition_keys != [] do
    with {:ok, scoped} <- bind_partition_list(partition_keys, scope_prefix) do
      {:ok, nil, scoped}
    end
  end

  defp bind_partitions(:auto, nil, scope_prefix) do
    with {:ok, scoped} <- bind_partition_list(Keys.auto_partition_keys(), scope_prefix) do
      {:ok, nil, scoped}
    end
  end

  defp bind_partitions(partition_key, nil, scope_prefix) when is_binary(partition_key) do
    with {:ok, scoped} <- StorageScope.physical_partition_key(partition_key, scope_prefix) do
      {:ok, scoped, nil}
    else
      {:error, _reason} -> {:error, "ERR invalid Flow system metadata"}
    end
  end

  defp bind_partitions(partition_key, nil, _scope_prefix) when partition_key in [:any, nil],
    do: {:error, "NOPERM Flow scope requires a bounded partition filter"}

  defp bind_partitions(_partition_key, _partition_keys, _scope_prefix),
    do: {:error, "ERR invalid Flow claim scope"}

  defp bind_partition_list(partition_keys, scope_prefix) do
    Enum.reduce_while(partition_keys, {:ok, []}, fn partition_key, {:ok, acc} ->
      case StorageScope.physical_partition_key(partition_key, scope_prefix) do
        {:ok, scoped} -> {:cont, {:ok, [scoped | acc]}}
        {:error, _reason} -> {:halt, {:error, "ERR invalid Flow system metadata"}}
      end
    end)
    |> case do
      {:ok, reversed} -> {:ok, Enum.reverse(reversed)}
      {:error, _reason} = error -> error
    end
  end

  defp maybe_put_partition_keys(attrs, nil), do: Map.delete(attrs, :partition_keys)

  defp maybe_put_partition_keys(attrs, partition_keys),
    do: Map.put(attrs, :partition_keys, partition_keys)

  defp maybe_put_metadata(attrs, metadata) when map_size(metadata) == 0,
    do: Map.delete(attrs, :system_metadata)

  defp maybe_put_metadata(attrs, metadata), do: Map.put(attrs, :system_metadata, metadata)
end
