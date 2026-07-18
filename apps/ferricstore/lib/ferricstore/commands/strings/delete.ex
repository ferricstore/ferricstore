defmodule Ferricstore.Commands.Strings.Delete do
  @moduledoc false

  alias Ferricstore.Commands.{Bloom, CMS, Cuckoo, ProbType, TopK}
  alias Ferricstore.Commands.Stream.{CacheKey, Meta, Waiters}
  alias Ferricstore.Store.{CompoundKey, Ops, ReadResult, TypeRegistry}
  alias Ferricstore.TermCodec

  def cleanup_stream_metadata(key, %{defer_stream_cleanup: defer} = store)
      when is_function(defer, 1) do
    defer.(CacheKey.build(store, key))
  end

  def cleanup_stream_metadata(key, store) do
    Meta.cleanup_local(key, store)
    Waiters.notify(key, store)
  end

  def do_del_key(key, store) do
    if Ops.has_compound?(store) do
      delete_key_with_compound_support(key, store)
    else
      delete_plain_without_compound(key, store)
    end
  end

  def cleanup_stream_metadata(key), do: cleanup_stream_metadata(key, nil)

  defp delete_key_with_compound_support(key, store) do
    type_key = CompoundKey.type_key(key)

    case Ops.compound_get(store, key, type_key) do
      {:error, {:storage_read_failed, _reason}} = failure ->
        ReadResult.command_error(failure)

      nil ->
        case maybe_delete_stream_key(key, store) do
          true -> true
          false -> delete_plain_key_if_exists(key, store)
          {:error, _reason} = error -> error
        end

      type_marker ->
        type_str = CompoundKey.type_name(type_marker)

        case delete_typed_key_data(key, type_str, store) do
          :ok -> true
          {:error, _reason} = error -> error
        end
    end
  end

  defp delete_plain_without_compound(key, store) do
    if Ops.exists?(store, key) do
      case Ops.delete(store, key) do
        :ok -> true
        {:error, _reason} = error -> error
      end
    else
      false
    end
  end

  defp compound_prefix(key, "hash"), do: CompoundKey.hash_prefix(key)
  defp compound_prefix(key, "list"), do: CompoundKey.list_prefix(key)
  defp compound_prefix(key, "set"), do: CompoundKey.set_prefix(key)
  defp compound_prefix(key, "zset"), do: CompoundKey.zset_prefix(key)
  defp compound_prefix(key, "stream"), do: CompoundKey.stream_prefix(key)
  defp compound_prefix(_key, _unknown), do: nil

  defp delete_typed_key_data(key, type, store)
       when type in ["bloom", "cms", "cuckoo", "topk"] do
    with :ok <- maybe_delete_prob_file(key, store),
         :ok <- Ops.delete(store, key),
         :ok <- TypeRegistry.delete_type(key, store) do
      :ok
    end
  end

  defp delete_typed_key_data(key, type, store),
    do: delete_compound_key_data(key, type, compound_prefix(key, type), store)

  defp delete_plain_key_if_exists(key, store) do
    if Ops.exists?(store, key) do
      case maybe_delete_prob_file(key, store) do
        :ok ->
          case Ops.delete(store, key) do
            :ok -> true
            {:error, _reason} = error -> error
          end

        {:error, _reason} = error ->
          error
      end
    else
      false
    end
  end

  defp delete_compound_key_data(key, type_str, prefix, store) do
    with :ok <- delete_compound_prefix_if_present(key, prefix, store),
         :ok <- delete_stream_groups_if_needed(key, type_str, store),
         :ok <- delete_stream_durable_meta_if_needed(key, type_str, store),
         :ok <- delete_list_meta_if_needed(key, type_str, store),
         :ok <- delete_stream_metadata_if_needed(key, type_str, store),
         :ok <- TypeRegistry.delete_type(key, store) do
      :ok
    end
  end

  defp delete_compound_prefix_if_present(_key, nil, _store), do: :ok

  defp delete_compound_prefix_if_present(key, prefix, store),
    do: Ops.compound_delete_prefix(store, key, prefix)

  defp delete_list_meta_if_needed(key, "list", store) do
    Ops.compound_delete(store, key, CompoundKey.list_meta_key(key))
  end

  defp delete_list_meta_if_needed(_key, _type_str, _store), do: :ok

  defp delete_stream_groups_if_needed(key, "stream", store) do
    Ops.compound_delete_prefix(store, key, CompoundKey.stream_group_prefix(key))
  end

  defp delete_stream_groups_if_needed(_key, _type_str, _store), do: :ok

  defp delete_stream_durable_meta_if_needed(key, "stream", store) do
    Ops.compound_delete(store, key, CompoundKey.stream_meta_key(key))
  end

  defp delete_stream_durable_meta_if_needed(_key, _type_str, _store), do: :ok

  defp delete_stream_metadata_if_needed(key, "stream", store),
    do: cleanup_stream_metadata(key, store)

  defp delete_stream_metadata_if_needed(_key, _type_str, _store), do: :ok

  defp maybe_delete_prob_file(_key, %FerricStore.Instance{}), do: :ok
  defp maybe_delete_prob_file(_key, %Ferricstore.Store.LocalTxStore{}), do: :ok
  defp maybe_delete_prob_file(_key, %{prob_file_lifecycle: :replicated}), do: :ok
  defp maybe_delete_prob_file(_key, %{prob_write: write_fn}) when is_function(write_fn), do: :ok

  defp maybe_delete_prob_file(key, store) when is_map(store) do
    case prob_type(key, store) do
      :bloom -> Bloom.nif_delete(key, store)
      :cms -> CMS.nif_delete(key, store)
      :cuckoo -> Cuckoo.nif_delete(key, store)
      :topk -> TopK.nif_delete(key, store)
      nil -> :ok
    end
  end

  defp maybe_delete_prob_file(_key, _store), do: :ok

  defp prob_type(key, store) do
    store
    |> Ops.get(key)
    |> decode_prob_meta()
  rescue
    _ -> nil
  end

  defp decode_prob_meta(value) when is_binary(value) do
    case TermCodec.decode(value) do
      {:ok, metadata} -> decode_prob_meta(metadata)
      {:error, :invalid_external_term} -> nil
    end
  end

  defp decode_prob_meta(metadata) do
    case ProbType.metadata_type(metadata) do
      type when type in [:bloom, :cms, :cuckoo, :topk] -> type
      :other -> nil
    end
  end

  defp maybe_delete_stream_key(key, store) do
    prefix = CompoundKey.stream_prefix(key)
    group_prefix = CompoundKey.stream_group_prefix(key)
    meta_key = CompoundKey.stream_meta_key(key)

    with {:ok, stream_entries?} <- compound_prefix_present?(store, key, prefix),
         {:ok, group_entries?} <- compound_prefix_present?(store, key, group_prefix),
         {:ok, stream_meta?} <- compound_key_present?(store, key, meta_key) do
      if stream_entries? or group_entries? or stream_meta? or stream_metadata_exists?(key, store) do
        with :ok <- Ops.compound_delete_prefix(store, key, prefix),
             :ok <- Ops.compound_delete_prefix(store, key, group_prefix),
             :ok <- Ops.compound_delete(store, key, meta_key) do
          case cleanup_stream_metadata(key, store) do
            :ok -> true
            {:error, _reason} = error -> error
          end
        else
          {:error, _reason} = error -> error
        end
      else
        false
      end
    end
  end

  defp compound_prefix_present?(store, key, prefix) do
    case Ops.compound_scan(store, key, prefix) do
      {:error, {:storage_read_failed, _reason}} = failure -> ReadResult.command_error(failure)
      entries when is_list(entries) -> {:ok, entries != []}
    end
  end

  defp compound_key_present?(store, key, compound_key) do
    case Ops.compound_get(store, key, compound_key) do
      {:error, {:storage_read_failed, _reason}} = failure -> ReadResult.command_error(failure)
      nil -> {:ok, false}
      _value -> {:ok, true}
    end
  end

  defp stream_metadata_exists?(key, store) do
    table = Ferricstore.Stream.Meta
    cache_key = CacheKey.build(store, key)
    :ets.whereis(table) != :undefined and :ets.lookup(table, cache_key) != []
  end
end
