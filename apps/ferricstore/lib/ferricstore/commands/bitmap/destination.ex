defmodule Ferricstore.Commands.Bitmap.Destination do
  @moduledoc false

  alias Ferricstore.Commands.{CompoundSnapshot, Strings.Delete}
  alias Ferricstore.Store.{CompoundKey, Ops, ReadResult, TypeRegistry}

  @wrongtype_error {:error, "WRONGTYPE Operation against a key holding the wrong kind of value"}

  @spec bitop_compound_destination_type(binary(), map()) ::
          binary() | nil | ReadResult.failure()
  def bitop_compound_destination_type(key, store) do
    if Ops.has_compound?(store), do: Ops.compound_get(store, key, CompoundKey.type_key(key))
  end

  @spec metadata_value_size(map(), binary()) ::
          non_neg_integer() | nil | :unknown | ReadResult.failure()
  def metadata_value_size(%FerricStore.Instance{} = store, key), do: Ops.value_size(store, key)

  def metadata_value_size(%Ferricstore.Store.LocalTxStore{} = store, key),
    do: Ops.value_size(store, key)

  def metadata_value_size(%{value_size: value_size}, key) when is_function(value_size, 1),
    do: value_size.(key)

  def metadata_value_size(_store, _key), do: :unknown

  @spec ensure_string_key(binary(), map()) :: :ok | {:error, binary()}
  def ensure_string_key(key, store) do
    if Ops.has_compound?(store) do
      type_key = CompoundKey.type_key(key)

      case Ops.compound_get(store, key, type_key) do
        {:error, {:storage_read_failed, _reason}} = failure ->
          ReadResult.command_error(failure)

        nil ->
          :ok

        type_marker ->
          ensure_resolved_string_key(key, type_marker, store)
      end
    else
      :ok
    end
  end

  defp ensure_resolved_string_key(key, type_marker, store) do
    case TypeRegistry.resolve_type_marker(key, type_marker, store) do
      {:error, {:storage_read_failed, _reason}} = failure ->
        ReadResult.command_error(failure)

      "none" ->
        :ok

      _compound_type ->
        @wrongtype_error
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
        |> cleanup_stream_destination(key, store)
      end
    else
      :ok
    end
  end

  defp cleanup_stream_destination(:ok, key, store),
    do: Delete.cleanup_stream_metadata(key, store)

  defp cleanup_stream_destination({:error, _reason} = error, _key, _store), do: error

  @spec compound_destination_backup(binary(), binary(), map()) ::
          {:ok, {:compound, list()}} | ReadResult.failure()
  def compound_destination_backup(key, type, store) do
    if CompoundSnapshot.supported?(store) do
      case CompoundSnapshot.snapshot(key, type, store) do
        {:ok, entries} -> {:ok, {:compound, entries}}
        {:error, {:storage_read_failed, _reason}} = failure -> failure
      end
    else
      ReadResult.failure(:compound_snapshot_unsupported)
    end
  end

  @spec restore_bitop_destination(map(), binary(), {:compound, list()}, term()) :: term()
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
end
