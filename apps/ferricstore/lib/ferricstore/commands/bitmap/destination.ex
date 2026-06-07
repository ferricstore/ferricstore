defmodule Ferricstore.Commands.Bitmap.Destination do
  @moduledoc false

  alias Ferricstore.Store.{CompoundKey, Ops, TypeRegistry}

  @wrongtype_error {:error, "WRONGTYPE Operation against a key holding the wrong kind of value"}

  @spec bitop_compound_destination_type(binary(), map()) :: binary() | nil
  def bitop_compound_destination_type(key, store) do
    if Ops.has_compound?(store), do: Ops.compound_get(store, key, CompoundKey.type_key(key))
  end

  @spec metadata_value_size(map(), binary()) :: non_neg_integer() | nil | :unknown
  def metadata_value_size(%FerricStore.Instance{} = store, key), do: Ops.value_size(store, key)
  def metadata_value_size(%Ferricstore.Store.LocalTxStore{} = store, key), do: Ops.value_size(store, key)

  def metadata_value_size(%{value_size: value_size}, key) when is_function(value_size, 1),
    do: value_size.(key)

  def metadata_value_size(_store, _key), do: :unknown

  @spec ensure_string_key(binary(), map()) :: :ok | {:error, binary()}
  def ensure_string_key(key, store) do
    if compound_data_structure_key?(key, store) do
      @wrongtype_error
    else
      :ok
    end
  end

  @spec clear_compound_data_structure(binary(), map()) :: :ok | {:error, term()}
  def clear_compound_data_structure(key, store) do
    if Ops.has_compound?(store) do
      with :ok <- Ops.compound_delete(store, key, CompoundKey.type_key(key)),
           :ok <- Ops.compound_delete(store, key, CompoundKey.list_meta_key(key)),
           :ok <- Ops.compound_delete(store, key, CompoundKey.stream_meta_key(key)) do
        [
          CompoundKey.hash_prefix(key),
          CompoundKey.list_prefix(key),
          CompoundKey.set_prefix(key),
          CompoundKey.zset_prefix(key),
          CompoundKey.stream_prefix(key),
          CompoundKey.stream_group_prefix(key)
        ]
        |> Enum.reduce_while(:ok, fn prefix, :ok ->
          case Ops.compound_delete_prefix(store, key, prefix) do
            :ok -> {:cont, :ok}
            {:error, _} = error -> {:halt, error}
          end
        end)
      end
    else
      :ok
    end
  end

  @spec compound_destination_backup(binary(), binary(), map()) :: {:compound, list()} | :unrestorable
  def compound_destination_backup(key, type, store) do
    if compound_backup_supported?(store) do
      {:compound,
       compound_backup_meta_entries(key, type, store) ++
         compound_backup_member_entries(key, type, store)}
    else
      :unrestorable
    end
  end

  @spec restore_bitop_destination(map(), binary(), {:compound, list()} | :unrestorable, term()) :: term()
  def restore_bitop_destination(_store, _key, :unrestorable, original_error), do: original_error

  def restore_bitop_destination(store, key, {:compound, entries}, original_error) do
    case Ops.delete(store, key) do
      :ok ->
        case Ops.compound_batch_put(store, key, entries) do
          :ok -> original_error
          {:error, _} = restore_error -> restore_error
        end

      {:error, _} = restore_error ->
        restore_error
    end
  end

  defp compound_data_structure_key?(key, store) do
    Ops.has_compound?(store) and
      compound_type_marker?(key, store) and
      TypeRegistry.get_type(key, store) != "none"
  end

  defp compound_type_marker?(key, store) do
    Ops.compound_get(store, key, CompoundKey.type_key(key)) != nil
  end

  defp compound_backup_supported?(%FerricStore.Instance{}), do: true
  defp compound_backup_supported?(%Ferricstore.Store.LocalTxStore{}), do: true

  defp compound_backup_supported?(store) when is_map(store) do
    is_function(Map.get(store, :compound_scan), 2)
  end

  defp compound_backup_meta_entries(key, type, store) do
    type_key = CompoundKey.type_key(key)

    type_entries =
      case Ops.compound_get_meta(store, key, type_key) do
        nil -> [{type_key, type, 0}]
        {value, expire_at_ms} -> [{type_key, value, expire_at_ms}]
      end

    list_meta_entries =
      if type == "list" do
        list_meta_key = CompoundKey.list_meta_key(key)

        case Ops.compound_get_meta(store, key, list_meta_key) do
          nil -> []
          {value, expire_at_ms} -> [{list_meta_key, value, expire_at_ms}]
        end
      else
        []
      end

    stream_meta_entries =
      if type == "stream" do
        stream_meta_key = CompoundKey.stream_meta_key(key)

        case Ops.compound_get_meta(store, key, stream_meta_key) do
          nil -> []
          {value, expire_at_ms} -> [{stream_meta_key, value, expire_at_ms}]
        end
      else
        []
      end

    type_entries ++ list_meta_entries ++ stream_meta_entries
  end

  defp compound_backup_member_entries(key, type, store) do
    prefixes = compound_backup_prefixes(key, type)

    compound_keys =
      Enum.flat_map(prefixes, fn prefix ->
        store
        |> Ops.compound_scan(key, prefix)
        |> Enum.map(fn {member_or_key, _value} ->
          if String.starts_with?(member_or_key, prefix) do
            member_or_key
          else
            prefix <> member_or_key
          end
        end)
      end)

    store
    |> Ops.compound_batch_get_meta(key, compound_keys)
    |> Enum.zip(compound_keys)
    |> Enum.flat_map(fn
      {nil, _compound_key} -> []
      {{value, expire_at_ms}, compound_key} -> [{compound_key, value, expire_at_ms}]
    end)
  end

  defp compound_backup_prefix(key, "hash"), do: CompoundKey.hash_prefix(key)
  defp compound_backup_prefix(key, "list"), do: CompoundKey.list_prefix(key)
  defp compound_backup_prefix(key, "set"), do: CompoundKey.set_prefix(key)
  defp compound_backup_prefix(key, "zset"), do: CompoundKey.zset_prefix(key)
  defp compound_backup_prefix(key, "stream"), do: CompoundKey.stream_prefix(key)

  defp compound_backup_prefixes(key, "stream"),
    do: [CompoundKey.stream_prefix(key), CompoundKey.stream_group_prefix(key)]

  defp compound_backup_prefixes(key, type), do: [compound_backup_prefix(key, type)]
end
