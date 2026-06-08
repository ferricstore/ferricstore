defmodule Ferricstore.Commands.Strings.Compound do
  @moduledoc false

  alias Ferricstore.Commands.Strings.Delete
  alias Ferricstore.Store.CompoundKey
  alias Ferricstore.Store.Ops
  alias Ferricstore.Store.TypeRegistry

  @wrongtype_error {:error, "WRONGTYPE Operation against a key holding the wrong kind of value"}

  @spec prefixes_for_type(binary(), binary()) :: [binary()]
  def prefixes_for_type(key, "stream"),
    do: [CompoundKey.stream_prefix(key), CompoundKey.stream_group_prefix(key)]

  def prefixes_for_type(key, type) do
    case prefix_for_type(key, type) do
      nil -> []
      prefix -> [prefix]
    end
  end

  @spec ensure_string_key(binary(), map()) :: :ok | {:error, binary()}
  def ensure_string_key(key, store) do
    if data_structure_key?(key, store), do: @wrongtype_error, else: :ok
  end

  @spec replace_string_key(binary(), binary(), non_neg_integer(), map()) :: :ok | {:error, term()}
  def replace_string_key(key, value, expire_at_ms, store) do
    with :ok <- clear_data_structure(key, store) do
      Ops.put(store, key, value, expire_at_ms)
    end
  end

  @spec data_structure_key?(binary(), map()) :: boolean()
  def data_structure_key?(key, store) do
    Ops.has_compound?(store) and
      compound_type_marker?(key, store) and
      TypeRegistry.get_type(key, store) != "none"
  end

  @spec clear_data_structure(binary(), map()) :: :ok | {:error, term()}
  def clear_data_structure(key, store) do
    if Ops.has_compound?(store) do
      type_key = CompoundKey.type_key(key)

      case Ops.compound_get(store, key, type_key) do
        nil ->
          :ok

        type ->
          backup = compound_clear_backup(key, type, store)

          case delete_compound_data_for_replacement(key, type, type_key, store) do
            :ok -> :ok
            {:error, _} = error -> restore_compound_clear_backup(key, backup, store, error)
          end
      end
    else
      :ok
    end
  end

  @spec scanned_key(binary(), binary()) :: binary()
  def scanned_key(prefix, key) do
    if String.starts_with?(key, prefix), do: key, else: prefix <> key
  end

  defp prefix_for_type(key, "hash"), do: CompoundKey.hash_prefix(key)
  defp prefix_for_type(key, "list"), do: CompoundKey.list_prefix(key)
  defp prefix_for_type(key, "set"), do: CompoundKey.set_prefix(key)
  defp prefix_for_type(key, "zset"), do: CompoundKey.zset_prefix(key)
  defp prefix_for_type(key, "stream"), do: CompoundKey.stream_prefix(key)
  defp prefix_for_type(_key, _type), do: nil

  defp compound_type_marker?(key, store) do
    Ops.compound_get(store, key, CompoundKey.type_key(key)) != nil
  end

  defp delete_compound_data_for_replacement(key, type, type_key, store) do
    with :ok <- clear_compound_prefix(key, type, store),
         :ok <- Ops.compound_delete(store, key, type_key) do
      if type == "stream", do: Delete.cleanup_stream_metadata(key)
      :ok
    end
  end

  defp clear_compound_prefix(key, "hash", store),
    do: Ops.compound_delete_prefix(store, key, CompoundKey.hash_prefix(key))

  defp clear_compound_prefix(key, "list", store) do
    with :ok <- Ops.compound_delete_prefix(store, key, CompoundKey.list_prefix(key)),
         :ok <- Ops.compound_delete(store, key, CompoundKey.list_meta_key(key)) do
      :ok
    end
  end

  defp clear_compound_prefix(key, "set", store),
    do: Ops.compound_delete_prefix(store, key, CompoundKey.set_prefix(key))

  defp clear_compound_prefix(key, "zset", store),
    do: Ops.compound_delete_prefix(store, key, CompoundKey.zset_prefix(key))

  defp clear_compound_prefix(key, "stream", store) do
    with :ok <-
           Enum.reduce_while(
             [CompoundKey.stream_prefix(key), CompoundKey.stream_group_prefix(key)],
             :ok,
             fn prefix, :ok ->
               case Ops.compound_delete_prefix(store, key, prefix) do
                 :ok -> {:cont, :ok}
                 {:error, _reason} = error -> {:halt, error}
               end
             end
           ),
         :ok <- Ops.compound_delete(store, key, CompoundKey.stream_meta_key(key)) do
      :ok
    end
  end

  defp clear_compound_prefix(_key, _type, _store), do: :ok

  defp compound_clear_backup(key, type, store) do
    if compound_backup_supported?(store) do
      compound_clear_meta_entries(key, type, store) ++
        compound_clear_member_entries(key, type, store)
    else
      :unsupported
    end
  end

  defp compound_backup_supported?(%FerricStore.Instance{}), do: true
  defp compound_backup_supported?(%Ferricstore.Store.LocalTxStore{}), do: true

  defp compound_backup_supported?(store) when is_map(store) do
    is_function(Map.get(store, :compound_scan), 2)
  end

  defp compound_clear_meta_entries(key, type, store) do
    type_entries = compound_key_backup_entry(key, CompoundKey.type_key(key), store, type)

    list_meta_entries =
      if type == "list" do
        compound_key_backup_entry(key, CompoundKey.list_meta_key(key), store)
      else
        []
      end

    stream_meta_entries =
      if type == "stream" do
        compound_key_backup_entry(key, CompoundKey.stream_meta_key(key), store)
      else
        []
      end

    type_entries ++ list_meta_entries ++ stream_meta_entries
  end

  defp compound_clear_member_entries(key, type, store) do
    prefixes = prefixes_for_type(key, type)

    if prefixes == [] do
      []
    else
      Enum.flat_map(prefixes, &compound_clear_member_entries_for_prefix(key, &1, store))
    end
  end

  defp compound_clear_member_entries_for_prefix(key, prefix, store) do
    pairs =
      store
      |> Ops.compound_scan(key, prefix)
      |> Enum.map(fn {sub_key, _value} ->
        compound_key = scanned_key(prefix, sub_key)
        {compound_key, compound_key}
      end)

    source_keys = Enum.map(pairs, fn {source_key, _destination_key} -> source_key end)
    metas = Ops.compound_batch_get_meta(store, key, source_keys)

    pairs
    |> Enum.zip(metas)
    |> Enum.flat_map(fn
      {{_source_key, _destination_key}, nil} ->
        []

      {{_source_key, destination_key}, {value, expire_at_ms}} ->
        [{destination_key, value, expire_at_ms}]
    end)
  end

  defp compound_key_backup_entry(key, compound_key, store, fallback_value \\ nil) do
    case Ops.compound_get_meta(store, key, compound_key) do
      nil when fallback_value != nil -> [{compound_key, fallback_value, 0}]
      nil -> []
      {value, expire_at_ms} -> [{compound_key, value, expire_at_ms}]
    end
  end

  defp restore_compound_clear_backup(_key, :unsupported, _store, error), do: error

  defp restore_compound_clear_backup(key, backup, store, error) do
    case Ops.compound_batch_put(store, key, backup) do
      :ok ->
        error

      {:error, _} = restore_error ->
        {:error, {:compound_clear_rollback_failed, error, restore_error}}
    end
  end
end
