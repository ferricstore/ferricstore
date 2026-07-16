defmodule Ferricstore.Commands.CompoundSnapshot do
  @moduledoc false

  alias Ferricstore.Store.{CompoundKey, Ops, ReadResult}

  @type entry :: {binary(), binary(), non_neg_integer()}

  @spec supported?(term()) :: boolean()
  def supported?(%FerricStore.Instance{}), do: true
  def supported?(%Ferricstore.Store.LocalTxStore{}), do: true

  def supported?(store) when is_map(store),
    do: is_function(Map.get(store, :compound_scan), 2)

  def supported?(_store), do: false

  @spec snapshot(binary(), binary(), map()) :: {:ok, [entry()]} | ReadResult.failure()
  def snapshot(key, type, store) when type in ["hash", "list", "set", "zset", "stream"] do
    with {:ok, meta_entries} <- meta_entries(key, type, store),
         {:ok, member_entries} <- member_entries(key, type, store) do
      {:ok, meta_entries ++ member_entries}
    end
  end

  def snapshot(_key, _type, _store), do: ReadResult.failure(:invalid_compound_type)

  @spec value_snapshot(binary(), binary(), map()) :: {:ok, [entry()]} | ReadResult.failure()
  def value_snapshot(key, type, store) when type in ["hash", "list", "set", "zset", "stream"] do
    with {:ok, extra_meta} <- extra_value_meta_entries(key, type, store),
         {:ok, members} <- member_value_entries(key, type, store) do
      {:ok, [{CompoundKey.type_key(key), type, 0} | extra_meta ++ members]}
    end
  end

  def value_snapshot(_key, _type, _store), do: ReadResult.failure(:invalid_compound_type)

  @spec copy(binary(), binary(), binary(), map()) :: {:ok, [entry()]} | ReadResult.failure()
  def copy(source, destination, type, store) do
    with {:ok, entries} <- snapshot(source, type, store) do
      rekey_entries(entries, source, destination, type)
    end
  end

  @spec prefixes(binary(), binary()) :: [binary()]
  def prefixes(key, "hash"), do: [CompoundKey.hash_prefix(key)]
  def prefixes(key, "list"), do: [CompoundKey.list_prefix(key)]
  def prefixes(key, "set"), do: [CompoundKey.set_prefix(key)]
  def prefixes(key, "zset"), do: [CompoundKey.zset_prefix(key)]

  def prefixes(key, "stream"),
    do: [CompoundKey.stream_prefix(key), CompoundKey.stream_group_prefix(key)]

  def prefixes(_key, _type), do: []

  @spec scanned_key(binary(), binary()) :: binary()
  def scanned_key(prefix, field), do: prefix <> field

  defp rekey_entries(entries, source, source, _type), do: {:ok, entries}

  defp rekey_entries(entries, source, destination, type) do
    prefix_pairs = Enum.zip(prefixes(source, type), prefixes(destination, type))

    Enum.reduce_while(entries, {:ok, []}, fn {key, value, expire_at_ms}, {:ok, acc} ->
      case rekey(key, source, destination, type, prefix_pairs) do
        {:ok, destination_key} ->
          {:cont, {:ok, [{destination_key, value, expire_at_ms} | acc]}}

        :error ->
          {:halt, ReadResult.failure({:unexpected_compound_snapshot_key, key})}
      end
    end)
    |> case do
      {:ok, copied} -> {:ok, Enum.reverse(copied)}
      {:error, {:storage_read_failed, _reason}} = failure -> failure
    end
  end

  defp rekey(key, source, destination, type, prefix_pairs) do
    cond do
      key == CompoundKey.type_key(source) ->
        {:ok, CompoundKey.type_key(destination)}

      type == "list" and key == CompoundKey.list_meta_key(source) ->
        {:ok, CompoundKey.list_meta_key(destination)}

      type == "stream" and key == CompoundKey.stream_meta_key(source) ->
        {:ok, CompoundKey.stream_meta_key(destination)}

      true ->
        rekey_prefixed(key, prefix_pairs)
    end
  end

  defp rekey_prefixed(key, prefix_pairs) do
    Enum.find_value(prefix_pairs, :error, fn {source_prefix, destination_prefix} ->
      if String.starts_with?(key, source_prefix) do
        suffix_size = byte_size(key) - byte_size(source_prefix)
        {:ok, destination_prefix <> binary_part(key, byte_size(source_prefix), suffix_size)}
      end
    end)
  end

  defp meta_entries(key, type, store) do
    with {:ok, type_entries} <-
           meta_entry(key, CompoundKey.type_key(key), type, store),
         {:ok, extra_entries} <- extra_meta_entries(key, type, store) do
      {:ok, type_entries ++ extra_entries}
    end
  end

  defp extra_meta_entries(key, "list", store),
    do: meta_entry(key, CompoundKey.list_meta_key(key), nil, store)

  defp extra_meta_entries(key, "stream", store),
    do: meta_entry(key, CompoundKey.stream_meta_key(key), nil, store)

  defp extra_meta_entries(_key, _type, _store), do: {:ok, []}

  defp extra_value_meta_entries(key, "list", store),
    do: value_meta_entry(key, CompoundKey.list_meta_key(key), store)

  defp extra_value_meta_entries(key, "stream", store),
    do: value_meta_entry(key, CompoundKey.stream_meta_key(key), store)

  defp extra_value_meta_entries(_key, _type, _store), do: {:ok, []}

  defp value_meta_entry(key, compound_key, store) do
    case Ops.compound_get(store, key, compound_key) do
      {:error, {:storage_read_failed, _reason}} = failure -> failure
      nil -> {:ok, []}
      value -> {:ok, [{compound_key, value, 0}]}
    end
  end

  defp meta_entry(key, compound_key, fallback_value, store) do
    case Ops.compound_get_meta(store, key, compound_key) do
      {:error, {:storage_read_failed, _reason}} = failure ->
        failure

      nil when fallback_value != nil ->
        {:ok, [{compound_key, fallback_value, 0}]}

      nil ->
        {:ok, []}

      {value, expire_at_ms} ->
        {:ok, [{compound_key, value, expire_at_ms}]}
    end
  end

  defp member_entries(key, type, store) do
    with {:ok, compound_keys} <- member_keys(key, type, store) do
      member_meta_entries(key, compound_keys, store)
    end
  end

  defp member_value_entries(key, type, store) do
    key
    |> prefixes(type)
    |> Enum.reduce_while({:ok, []}, fn prefix, {:ok, entries} ->
      case Ops.compound_scan(store, key, prefix) do
        {:error, {:storage_read_failed, _reason}} = failure ->
          {:halt, failure}

        pairs when is_list(pairs) ->
          prefix_entries =
            Enum.map(pairs, fn {field, value} ->
              {scanned_key(prefix, field), value, 0}
            end)

          {:cont, {:ok, prefix_entries ++ entries}}

        invalid ->
          {:halt, ReadResult.failure({:invalid_compound_scan_result, invalid})}
      end
    end)
  end

  defp member_keys(key, type, store) do
    key
    |> prefixes(type)
    |> Enum.reduce_while({:ok, []}, fn prefix, {:ok, keys} ->
      case Ops.compound_scan(store, key, prefix) do
        {:error, {:storage_read_failed, _reason}} = failure ->
          {:halt, failure}

        entries when is_list(entries) ->
          prefix_keys = Enum.map(entries, fn {field, _value} -> scanned_key(prefix, field) end)
          {:cont, {:ok, prefix_keys ++ keys}}

        invalid ->
          {:halt, ReadResult.failure({:invalid_compound_scan_result, invalid})}
      end
    end)
  end

  defp member_meta_entries(_key, [], _store), do: {:ok, []}

  defp member_meta_entries(key, compound_keys, store) do
    case Ops.compound_batch_get_meta(store, key, compound_keys) do
      metas when is_list(metas) and length(metas) == length(compound_keys) ->
        case ReadResult.first_failure(metas) do
          nil ->
            entries =
              compound_keys
              |> Enum.zip(metas)
              |> Enum.flat_map(fn
                {_compound_key, nil} ->
                  []

                {compound_key, {value, expire_at_ms}} ->
                  [{compound_key, value, expire_at_ms}]
              end)

            {:ok, entries}

          failure ->
            failure
        end

      {:error, {:storage_read_failed, _reason}} = failure ->
        failure

      invalid ->
        ReadResult.failure({:invalid_compound_batch_meta_result, invalid})
    end
  end
end
