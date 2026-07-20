defmodule Ferricstore.Flow.StorageScope do
  @moduledoc false

  alias Ferricstore.Flow.Keys
  alias Ferricstore.Flow.SystemMetadata

  @prefix <<0, 0xFE, "flow-scope", 0>>
  @max_scope_bytes 8 * 1_024

  @spec physical_partition_key(map()) ::
          {:ok, binary()} | {:error, :invalid_flow_system_metadata}
  def physical_partition_key(%{partition_key: partition_key} = record)
      when is_binary(partition_key) do
    with {:ok, scope} <- SystemMetadata.scope_prefix(Map.get(record, :system_metadata, %{})) do
      encode_partition(partition_key, scope)
    end
  end

  def physical_partition_key(%{partition_key: nil} = record) do
    case Map.get(record, :system_metadata, %{}) do
      metadata when metadata == %{} -> {:ok, nil}
      _scoped -> {:error, :invalid_flow_system_metadata}
    end
  end

  def physical_partition_key(_record), do: {:error, :invalid_flow_system_metadata}

  @doc false
  @spec physical_partition_key(binary(), binary() | nil) ::
          {:ok, binary()} | {:error, :invalid_flow_system_metadata}
  def physical_partition_key(partition_key, scope)
      when is_binary(partition_key) and partition_key != "" and
             (is_binary(scope) or is_nil(scope)) do
    encode_partition(partition_key, scope)
  end

  def physical_partition_key(_partition_key, _scope),
    do: {:error, :invalid_flow_system_metadata}

  @doc false
  @spec physical_scope_prefix(binary()) :: {:ok, binary()} | :unscoped
  def physical_scope_prefix(
        <<@prefix::binary, scope_bytes::unsigned-big-16, scope::binary-size(scope_bytes),
          logical_partition_key::binary>>
      )
      when scope_bytes > 0 and scope_bytes <= @max_scope_bytes and logical_partition_key != "",
      do: {:ok, scope}

  def physical_scope_prefix(_partition_key), do: :unscoped

  @doc false
  @spec scoped_auto_partition_scope([binary()]) :: {:ok, binary()} | :error
  def scoped_auto_partition_scope([first | _rest] = partitions) do
    auto_partitions = Keys.auto_partition_keys()

    if length(partitions) == length(auto_partitions) do
      case physical_scope_prefix(first) do
        {:ok, scope} ->
          partitions
          |> Enum.zip(auto_partitions)
          |> Enum.reduce_while({:ok, scope}, fn {physical, logical}, result ->
            if physical_partition_key(logical, scope) == {:ok, physical},
              do: {:cont, result},
              else: {:halt, :error}
          end)

        :unscoped ->
          :error
      end
    else
      :error
    end
  end

  def scoped_auto_partition_scope(_partitions), do: :error

  @spec logical_partition_key(map()) ::
          {:ok, binary()} | {:error, :invalid_flow_system_metadata}
  def logical_partition_key(%{partition_key: partition_key} = record)
      when is_binary(partition_key) do
    with {:ok, scope} <- SystemMetadata.scope_prefix(Map.get(record, :system_metadata, %{})) do
      decode_partition(partition_key, scope)
    end
  end

  def logical_partition_key(%{partition_key: nil} = record) do
    case Map.get(record, :system_metadata, %{}) do
      metadata when metadata == %{} -> {:ok, nil}
      _scoped -> {:error, :invalid_flow_system_metadata}
    end
  end

  def logical_partition_key(_record), do: {:error, :invalid_flow_system_metadata}

  @doc false
  @spec logical_partition_key(binary(), binary()) ::
          {:ok, binary()} | {:error, :invalid_flow_system_metadata}
  def logical_partition_key(partition_key, scope)
      when is_binary(partition_key) and is_binary(scope) and scope != "",
      do: decode_partition(partition_key, scope)

  def logical_partition_key(_partition_key, _scope),
    do: {:error, :invalid_flow_system_metadata}

  defp encode_partition(partition_key, nil), do: {:ok, partition_key}

  defp encode_partition(partition_key, scope)
       when is_binary(scope) and byte_size(scope) > 0 and byte_size(scope) <= @max_scope_bytes do
    case decode_scoped_partition(partition_key, scope) do
      {:ok, _logical_partition_key} ->
        {:ok, partition_key}

      :error ->
        {:ok,
         <<@prefix::binary, byte_size(scope)::unsigned-big-16, scope::binary,
           partition_key::binary>>}
    end
  end

  defp encode_partition(_partition_key, _scope),
    do: {:error, :invalid_flow_system_metadata}

  defp decode_partition(partition_key, nil), do: {:ok, partition_key}

  defp decode_partition(partition_key, scope) when is_binary(scope) do
    case decode_scoped_partition(partition_key, scope) do
      {:ok, logical_partition_key} -> {:ok, logical_partition_key}
      :error -> {:error, :invalid_flow_system_metadata}
    end
  end

  defp decode_scoped_partition(
         <<@prefix::binary, scope_bytes::unsigned-big-16, encoded_scope::binary-size(scope_bytes),
           logical_partition_key::binary>>,
         scope
       )
       when encoded_scope == scope,
       do: {:ok, logical_partition_key}

  defp decode_scoped_partition(_partition_key, _scope), do: :error
end
