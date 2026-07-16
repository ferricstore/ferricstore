defmodule Ferricstore.Commands.Strings.Compound do
  @moduledoc false

  alias Ferricstore.Commands.CompoundSnapshot
  alias Ferricstore.Commands.Strings.Delete
  alias Ferricstore.Store.CompoundKey
  alias Ferricstore.Store.Ops
  alias Ferricstore.Store.ReadResult
  alias Ferricstore.Store.TypeRegistry

  @wrongtype_error {:error, "WRONGTYPE Operation against a key holding the wrong kind of value"}

  @spec prefixes_for_type(binary(), binary()) :: [binary()]
  def prefixes_for_type(key, type), do: CompoundSnapshot.prefixes(key, type)

  @spec ensure_string_key(binary(), map()) :: :ok | {:error, binary()}
  def ensure_string_key(key, store) do
    case data_structure_status(key, store) do
      :compound -> @wrongtype_error
      :plain -> :ok
      {:error, {:storage_read_failed, _reason}} = failure -> ReadResult.command_error(failure)
    end
  end

  @spec replace_string_key(binary(), binary(), non_neg_integer(), map()) :: :ok | {:error, term()}
  def replace_string_key(key, value, expire_at_ms, store) do
    with :ok <- clear_data_structure(key, store) do
      Ops.put(store, key, value, expire_at_ms)
    end
  end

  @spec data_structure_status(binary(), map()) ::
          :compound | :plain | ReadResult.failure()
  def data_structure_status(key, store) do
    if Ops.has_compound?(store) do
      case Ops.compound_get(store, key, CompoundKey.type_key(key)) do
        {:error, {:storage_read_failed, _reason}} = failure ->
          failure

        nil ->
          :plain

        _type_marker ->
          case TypeRegistry.get_type(key, store) do
            {:error, {:storage_read_failed, _reason}} = failure -> failure
            "none" -> :plain
            _type -> :compound
          end
      end
    else
      :plain
    end
  end

  @spec data_structure_key?(binary(), map()) :: boolean()
  def data_structure_key?(key, store), do: data_structure_status(key, store) == :compound

  @spec clear_data_structure(binary(), map()) :: :ok | {:error, term()}
  def clear_data_structure(key, store) do
    if Ops.has_compound?(store) do
      type_key = CompoundKey.type_key(key)

      case Ops.compound_get(store, key, type_key) do
        {:error, {:storage_read_failed, _reason}} = failure ->
          ReadResult.command_error(failure)

        nil ->
          :ok

        type ->
          case compound_clear_backup(key, type, store) do
            {:ok, backup} ->
              case delete_compound_data_for_replacement(key, type, type_key, store) do
                :ok -> :ok
                {:error, _} = error -> restore_compound_clear_backup(key, backup, store, error)
              end

            {:error, {:storage_read_failed, _reason}} = failure ->
              ReadResult.command_error(failure)
          end
      end
    else
      :ok
    end
  end

  @spec scanned_key(binary(), binary()) :: binary()
  def scanned_key(prefix, key), do: CompoundSnapshot.scanned_key(prefix, key)

  defp delete_compound_data_for_replacement(key, type, type_key, store) do
    with :ok <- clear_compound_prefix(key, type, store),
         :ok <- Ops.compound_delete(store, key, type_key) do
      if type == "stream", do: Delete.cleanup_stream_metadata(key, store)
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
    if CompoundSnapshot.supported?(store) do
      CompoundSnapshot.snapshot(key, type, store)
    else
      {:ok, :unsupported}
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
