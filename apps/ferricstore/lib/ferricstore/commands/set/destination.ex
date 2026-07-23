defmodule Ferricstore.Commands.Set.Destination do
  @moduledoc false

  alias Ferricstore.Commands.{CompoundSnapshot, Strings.Delete}
  alias Ferricstore.Store.{CompoundKey, Ops, ReadResult, TypeRegistry}

  @presence_marker "1"

  def store_set_at(destination, members, store) do
    case destination_backup(destination, store) do
      {:ok, backup} -> replace_destination(destination, members, backup, store)
      {:error, {:storage_read_failed, _reason}} = failure -> ReadResult.command_error(failure)
    end
  end

  defp replace_destination(destination, members, backup, store) do
    case clear_set_store_destination(destination, store) do
      :ok ->
        write_destination(destination, MapSet.to_list(members), backup, store)

      {:error, _reason} = error ->
        restore_set_store_destination(error, destination, backup, store)
    end
  end

  defp write_destination(_destination, [], _backup, _store), do: 0

  defp write_destination(destination, members, backup, store) do
    case TypeRegistry.command_check_or_set_status(destination, :set, store) do
      type_status when type_status in [:ok, {:ok, :created}] ->
        case put_set_members(store, destination, members) do
          :ok ->
            length(members)

          {:error, _reason} = error ->
            destination
            |> rollback_new_set_type_marker(store, type_status, error)
            |> restore_set_store_destination(destination, backup, store)
        end

      {:error, _reason} = error ->
        restore_set_store_destination(error, destination, backup, store)
    end
  end

  defp restore_set_store_destination({:error, _} = original_error, destination, backup, store) do
    case restore_destination_backup(destination, backup, store) do
      :ok -> command_error(original_error)
      {:error, _} = restore_error -> restore_error
    end
  end

  defp restore_set_store_destination(result, _destination, _backup, _store), do: result

  defp clear_set_store_destination(destination, store) do
    with :ok <- Ops.delete(store, destination),
         :ok <- clear_all_compound_destination_prefixes(destination, store),
         :ok <- Ops.compound_delete(store, destination, CompoundKey.list_meta_key(destination)),
         :ok <- TypeRegistry.delete_type(destination, store),
         :ok <- Delete.cleanup_stream_metadata(destination, store) do
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
      {:error, {:storage_read_failed, _reason}} = failure ->
        failure

      "none" ->
        {:ok, :missing}

      "string" ->
        case Ops.get_meta(store, destination) do
          {:error, {:storage_read_failed, _reason}} = failure -> failure
          nil -> {:ok, :missing}
          {value, expire_at_ms} -> {:ok, {:plain, value, expire_at_ms}}
        end

      type when type in ["hash", "list", "set", "zset", "stream"] ->
        case CompoundSnapshot.snapshot(destination, type, store) do
          {:ok, entries} -> {:ok, {:compound, entries}}
          {:error, {:storage_read_failed, _reason}} = failure -> failure
        end

      type ->
        ReadResult.failure({:unsupported_destination_type, type})
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

  defp command_error({:error, {:storage_read_failed, _reason}} = failure),
    do: ReadResult.command_error(failure)

  defp command_error(error), do: error
end
