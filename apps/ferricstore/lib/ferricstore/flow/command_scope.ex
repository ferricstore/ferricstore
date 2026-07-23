defmodule Ferricstore.Flow.CommandScope do
  @moduledoc false

  alias Ferricstore.Flow.{StorageScope, SystemMetadata}
  alias Ferricstore.Raft.ApplyContext

  @error {:error, "ERR invalid replicated Flow scope"}

  @spec validate(map(), map()) :: :ok | {:error, binary()}
  def validate(%{apply_context: %ApplyContext{} = context}, attrs) when is_map(attrs) do
    with :ok <- validate_container(context, attrs),
         :ok <- validate_children(context, attrs) do
      :ok
    else
      _invalid -> @error
    end
  end

  def validate(_state, _attrs), do: @error

  defp validate_container(context, %{records: records}) when is_list(records) do
    validate_records(context, records)
  end

  defp validate_container(context, %{id: id, partition_key: partition_key} = attrs)
       when is_binary(id) and is_binary(partition_key),
       do: validate_record(context, attrs)

  defp validate_container(context, %{system_metadata: metadata}) when is_map(metadata),
    do: validate_metadata(context, metadata)

  defp validate_container(_context, _attrs), do: :ok

  defp validate_children(context, %{children: children}) when is_list(children),
    do: validate_records(context, children)

  defp validate_children(_context, _attrs), do: :ok

  defp validate_records(context, records) do
    Enum.reduce_while(records, :ok, fn
      record, :ok when is_map(record) ->
        case validate_record(context, record) do
          :ok -> {:cont, :ok}
          _invalid -> {:halt, @error}
        end

      _invalid, :ok ->
        {:halt, @error}
    end)
  end

  defp validate_record(%ApplyContext{} = context, record) do
    metadata = Map.get(record, :system_metadata, %{})

    with :ok <- validate_metadata(context, metadata),
         :ok <- validate_partition_scope(record, metadata) do
      :ok
    else
      _invalid -> @error
    end
  end

  defp validate_partition_scope(%{id: id} = record, _metadata)
       when is_binary(id) and id != "" do
    case record |> Map.put_new(:partition_key, nil) |> StorageScope.logical_partition_key() do
      {:ok, _logical_partition_key} -> :ok
      {:error, _reason} -> @error
    end
  end

  defp validate_partition_scope(_record, _metadata), do: @error

  defp validate_metadata(%ApplyContext{} = context, metadata) do
    with :ok <- SystemMetadata.validate_against(metadata, context.flow_metadata_fields),
         :ok <- validate_mode(context, metadata) do
      :ok
    else
      _invalid -> @error
    end
  end

  defp validate_mode(%ApplyContext{flow_metadata_mode: :dedicated}, metadata) do
    if Enum.any?(metadata, fn {_id, {_version, _type, role, _value}} ->
         role == :isolation_scope
       end),
       do: @error,
       else: :ok
  end

  defp validate_mode(
         %ApplyContext{flow_metadata_mode: :shared, flow_metadata_fields: fields},
         metadata
       ) do
    required_ids =
      fields
      |> Enum.filter(fn {_id, field} ->
        field.role == :isolation_scope and field.required_in in [:shared, :always]
      end)
      |> Enum.map(&elem(&1, 0))

    if required_ids != [] and Enum.all?(required_ids, &Map.has_key?(metadata, &1)),
      do: :ok,
      else: @error
  end
end
