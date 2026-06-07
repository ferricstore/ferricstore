defmodule Ferricstore.Commands.Set.Destination do
  @moduledoc false

  alias Ferricstore.Store.CompoundKey
  alias Ferricstore.Store.Ops
  alias Ferricstore.Store.TypeRegistry

  @presence_marker "1"

  def store_set_at(destination, members, store) do
    backup = destination_backup(destination, store)

    with :ok <- clear_set_store_destination(destination, store) do
      members_list = MapSet.to_list(members)

      if members_list == [] do
        0
      else
        with type_status when type_status in [:ok, {:ok, :created}] <-
               TypeRegistry.check_or_set_status(destination, :set, store) do
          case put_set_members(store, destination, members_list) do
            :ok ->
              length(members_list)

            {:error, _} = err ->
              destination
              |> rollback_new_set_type_marker(store, type_status, err)
              |> restore_set_store_destination(destination, backup, store)
          end
        end
      end
    end
  end

  defp restore_set_store_destination({:error, _} = original_error, destination, backup, store) do
    case restore_destination_backup(destination, backup, store) do
      :ok -> original_error
      {:error, _} = restore_error -> restore_error
    end
  end

  defp restore_set_store_destination(result, _destination, _backup, _store), do: result

  defp clear_set_store_destination(destination, store) do
    with :ok <- Ops.delete(store, destination),
         :ok <- clear_all_compound_destination_prefixes(destination, store),
         :ok <- Ops.compound_delete(store, destination, CompoundKey.list_meta_key(destination)),
         :ok <- TypeRegistry.delete_type(destination, store) do
      :ok
    end
  end

  defp clear_all_compound_destination_prefixes(destination, store) do
    [
      CompoundKey.hash_prefix(destination),
      CompoundKey.list_prefix(destination),
      CompoundKey.set_prefix(destination),
      CompoundKey.zset_prefix(destination),
      CompoundKey.stream_prefix(destination),
      CompoundKey.stream_group_prefix(destination)
    ]
    |> Enum.reduce_while(:ok, fn prefix, :ok ->
      case Ops.compound_delete_prefix(store, destination, prefix) do
        :ok -> {:cont, :ok}
        {:error, _} = error -> {:halt, error}
      end
    end)
    |> case do
      :ok -> Ops.compound_delete(store, destination, CompoundKey.stream_meta_key(destination))
      {:error, _} = error -> error
    end
  end

  defp destination_backup(destination, store) do
    case TypeRegistry.get_type(destination, store) do
      "none" ->
        :missing

      "string" ->
        case Ops.get_meta(store, destination) do
          nil -> :missing
          {value, expire_at_ms} -> {:plain, value, expire_at_ms}
        end

      type when type in ["hash", "list", "set", "zset", "stream"] ->
        {:compound, compound_backup_entries(destination, type, store)}
    end
  end

  defp restore_destination_backup(destination, :missing, store) do
    clear_set_store_destination(destination, store)
  end

  defp restore_destination_backup(destination, {:plain, value, expire_at_ms}, store) do
    with :ok <- clear_set_store_destination(destination, store) do
      Ops.put(store, destination, value, expire_at_ms)
    end
  end

  defp restore_destination_backup(destination, {:compound, entries}, store) do
    with :ok <- clear_set_store_destination(destination, store) do
      Ops.compound_batch_put(store, destination, entries)
    end
  end

  defp compound_backup_entries(destination, type, store) do
    compound_backup_meta_entries(destination, type, store) ++
      compound_backup_member_entries(destination, type, store)
  end

  defp compound_backup_meta_entries(destination, type, store) do
    type_key = CompoundKey.type_key(destination)

    type_entries =
      case Ops.compound_get_meta(store, destination, type_key) do
        nil -> [{type_key, type, 0}]
        {value, expire_at_ms} -> [{type_key, value, expire_at_ms}]
      end

    list_meta_entries =
      if type == "list" do
        list_meta_key = CompoundKey.list_meta_key(destination)

        case Ops.compound_get_meta(store, destination, list_meta_key) do
          nil -> []
          {value, expire_at_ms} -> [{list_meta_key, value, expire_at_ms}]
        end
      else
        []
      end

    stream_meta_entries =
      if type == "stream" do
        stream_meta_key = CompoundKey.stream_meta_key(destination)

        case Ops.compound_get_meta(store, destination, stream_meta_key) do
          nil -> []
          {value, expire_at_ms} -> [{stream_meta_key, value, expire_at_ms}]
        end
      else
        []
      end

    type_entries ++ list_meta_entries ++ stream_meta_entries
  end

  defp compound_backup_member_entries(destination, type, store) do
    prefixes = destination_prefixes(destination, type)

    compound_keys =
      Enum.flat_map(prefixes, fn prefix ->
        store
        |> Ops.compound_scan(destination, prefix)
        |> Enum.map(fn {member_or_key, _value} ->
          if String.starts_with?(member_or_key, prefix) do
            member_or_key
          else
            prefix <> member_or_key
          end
        end)
      end)

    store
    |> Ops.compound_batch_get_meta(destination, compound_keys)
    |> Enum.zip(compound_keys)
    |> Enum.flat_map(fn
      {nil, _compound_key} -> []
      {{value, expire_at_ms}, compound_key} -> [{compound_key, value, expire_at_ms}]
    end)
  end

  defp destination_prefix(destination, "hash"), do: CompoundKey.hash_prefix(destination)
  defp destination_prefix(destination, "list"), do: CompoundKey.list_prefix(destination)
  defp destination_prefix(destination, "set"), do: CompoundKey.set_prefix(destination)
  defp destination_prefix(destination, "zset"), do: CompoundKey.zset_prefix(destination)
  defp destination_prefix(destination, "stream"), do: CompoundKey.stream_prefix(destination)

  defp destination_prefixes(destination, "stream"),
    do: [CompoundKey.stream_prefix(destination), CompoundKey.stream_group_prefix(destination)]

  defp destination_prefixes(destination, type), do: [destination_prefix(destination, type)]

  defp put_set_members(store, key, members) do
    entries =
      Enum.map(members, fn member ->
        {CompoundKey.set_member(key, member), @presence_marker, 0}
      end)

    Ops.compound_batch_put(store, key, entries)
  end

  defp rollback_new_set_type_marker(key, store, {:ok, :created}, write_error) do
    case TypeRegistry.delete_type(key, store) do
      :ok ->
        write_error

      {:error, _} = rollback_error ->
        {:error, {:set_type_marker_rollback_failed, write_error, rollback_error}}
    end
  end

  defp rollback_new_set_type_marker(_key, _store, :ok, write_error), do: write_error
end
