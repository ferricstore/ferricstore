defmodule Ferricstore.Commands.Strings.Delete do
  @moduledoc false

  alias Ferricstore.Commands.{Bloom, CMS, Cuckoo, TopK}
  alias Ferricstore.Store.{CompoundKey, Ops, TypeRegistry}

  def do_del_key(key, store) do
    if Ops.has_compound?(store) do
      delete_key_with_compound_support(key, store)
    else
      delete_plain_without_compound(key, store)
    end
  end

  def cleanup_stream_metadata(key) do
    meta_table = Ferricstore.Stream.Meta
    groups_table = Ferricstore.Stream.Groups
    index_table = Ferricstore.Stream.Index
    waiters_table = :ferricstore_stream_waiters

    if :ets.whereis(meta_table) != :undefined do
      :ets.delete(meta_table, key)
    end

    if :ets.whereis(groups_table) != :undefined do
      :ets.match_delete(groups_table, {{key, :_}, :_, :_, :_})
    end

    if :ets.whereis(index_table) != :undefined do
      :ets.select_delete(index_table, [{{{key, :_, :_}, :_, :_}, [], [true]}])
      :ets.delete(index_table, {:ready, key})
    end

    if :ets.whereis(waiters_table) != :undefined do
      :ets.match_delete(waiters_table, {key, :_, :_, :_})
    end

    :ok
  end

  defp delete_key_with_compound_support(key, store) do
    type_key = CompoundKey.type_key(key)

    case Ops.compound_get(store, key, type_key) do
      nil ->
        case maybe_delete_stream_key(key, store) do
          true -> true
          false -> delete_plain_key_if_exists(key, store)
          {:error, _reason} = error -> error
        end

      type_str ->
        case delete_compound_key_data(key, type_str, compound_prefix(key, type_str), store) do
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
         :ok <- delete_stream_metadata_if_needed(key, type_str),
         :ok <- TypeRegistry.delete_type(key, store) do
      :ok
    end
  end

  defp delete_compound_prefix_if_present(_key, nil, _store), do: :ok
  defp delete_compound_prefix_if_present(key, prefix, store), do: Ops.compound_delete_prefix(store, key, prefix)

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

  defp delete_stream_metadata_if_needed(key, "stream") do
    cleanup_stream_metadata(key)
    :ok
  end

  defp delete_stream_metadata_if_needed(_key, _type_str), do: :ok

  defp maybe_delete_prob_file(_key, %FerricStore.Instance{}), do: :ok
  defp maybe_delete_prob_file(_key, %Ferricstore.Store.LocalTxStore{}), do: :ok
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
    try do
      value
      |> :erlang.binary_to_term([:safe])
      |> decode_prob_meta()
    rescue
      _ -> nil
    end
  end

  defp decode_prob_meta({:bloom_meta, _}), do: :bloom
  defp decode_prob_meta({:cms_meta, _}), do: :cms
  defp decode_prob_meta({:cuckoo_meta, _}), do: :cuckoo
  defp decode_prob_meta({:topk_meta, _}), do: :topk
  defp decode_prob_meta({:topk_path, _}), do: :topk
  defp decode_prob_meta(_), do: nil

  defp maybe_delete_stream_key(key, store) do
    prefix = CompoundKey.stream_prefix(key)
    group_prefix = CompoundKey.stream_group_prefix(key)
    meta_key = CompoundKey.stream_meta_key(key)

    if Ops.compound_scan(store, key, prefix) != [] or
         Ops.compound_scan(store, key, group_prefix) != [] or
         Ops.compound_get(store, key, meta_key) != nil or stream_metadata_exists?(key) do
      with :ok <- Ops.compound_delete_prefix(store, key, prefix),
           :ok <- Ops.compound_delete_prefix(store, key, group_prefix),
           :ok <- Ops.compound_delete(store, key, meta_key) do
        cleanup_stream_metadata(key)
        true
      else
        {:error, _reason} = error -> error
      end
    else
      false
    end
  end

  defp stream_metadata_exists?(key) do
    table = Ferricstore.Stream.Meta
    :ets.whereis(table) != :undefined and :ets.lookup(table, key) != []
  end
end
