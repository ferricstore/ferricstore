defmodule Ferricstore.Flow.LMDB.TerminalCounts do
  @moduledoc false

  alias Ferricstore.Flow.LMDB.{Access, IndexCodec}

  @terminal_count_cache :ferricstore_flow_lmdb_terminal_count_cache

  def terminal_count(path, state_index_key) when is_binary(path) and is_binary(state_index_key) do
    count_key = IndexCodec.terminal_count_key(state_index_key)

    case cached_count_key(path, count_key) do
      {:ok, count} -> {:ok, count}
      :miss -> count_key_uncached(path, count_key)
    end
  end

  def terminal_counts(path, state_index_keys)
      when is_binary(path) and is_list(state_index_keys) do
    count_keys = Enum.map(state_index_keys, &IndexCodec.terminal_count_key/1)

    if count_keys == [] do
      {:ok, []}
    else
      {cached, missing} =
        count_keys
        |> Enum.with_index()
        |> Enum.reduce({%{}, []}, fn {count_key, index}, {cached_acc, missing_acc} ->
          case cached_count_key(path, count_key) do
            {:ok, count} -> {Map.put(cached_acc, index, count), missing_acc}
            :miss -> {cached_acc, [{index, count_key} | missing_acc]}
          end
        end)

      missing = Enum.reverse(missing)

      with {:ok, fetched} <- count_keys_uncached(path, Enum.map(missing, &elem(&1, 1))) do
        counts =
          missing
          |> Enum.zip(fetched)
          |> Enum.reduce(cached, fn
            {{index, count_key}, {:cache, count}}, acc ->
              put_cached_count_key(path, count_key, count)
              Map.put(acc, index, count)

            {{index, _count_key}, :missing}, acc ->
              Map.put(acc, index, 0)
          end)

        {:ok, Enum.map(0..(length(count_keys) - 1)//1, &Map.fetch!(counts, &1))}
      end
    end
  end

  def refresh_count_key(path, count_key) when is_binary(path) and is_binary(count_key) do
    case count_key_uncached(path, count_key) do
      {:ok, count} ->
        put_cached_count_key(path, count_key, count)
        {:ok, count}

      :not_found ->
        delete_cached_count_key(path, count_key)
        :not_found

      {:error, _reason} = error ->
        error
    end
  end

  def refresh_after_delete(path, {:ok, count_key}) do
    case count_key_uncached(path, count_key) do
      {:ok, count} -> put_cached_count_key(path, count_key, count)
      :not_found -> put_cached_count_key(path, count_key, 0)
      {:error, _reason} -> delete_cached_count_key(path, count_key)
    end
  end

  def refresh_after_delete(_path, :missing), do: :ok

  def put_cached_count_key(path, count_key, count)
      when is_binary(path) and is_binary(count_key) and is_integer(count) and count >= 0 do
    ensure_cache()
    :ets.insert(@terminal_count_cache, {{path, count_key}, count})
    :ok
  end

  def delete_cached_count_key(path, count_key)
      when is_binary(path) and is_binary(count_key) do
    ensure_cache()
    :ets.delete(@terminal_count_cache, {path, count_key})
    :ok
  end

  def clear_cached_for_path(path) when is_binary(path) do
    ensure_cache()
    :ets.match_delete(@terminal_count_cache, {{path, :_}, :_})
    :ok
  end

  def put_terminal_count(path, state_index_key, count)
      when is_binary(path) and is_binary(state_index_key) and is_integer(count) and count >= 0 do
    count_key = IndexCodec.terminal_count_key(state_index_key)

    case Access.write_batch(
           path,
           [{:put, count_key, IndexCodec.encode_count(count)}]
         ) do
      :ok ->
        put_cached_count_key(path, count_key, count)
        :ok

      {:error, _reason} = error ->
        error
    end
  end

  def read_count_key(path, count_key) do
    case Access.get(path, count_key) do
      {:ok, blob} ->
        case IndexCodec.decode_count(blob) do
          {:ok, count} -> count
          :error -> 0
        end

      _ ->
        0
    end
  end

  defp count_key_uncached(path, count_key) do
    case Access.get(path, count_key) do
      {:ok, blob} ->
        case IndexCodec.decode_count(blob) do
          {:ok, count} -> {:ok, count}
          :error -> :not_found
        end

      :not_found ->
        :not_found

      {:error, _reason} = error ->
        error
    end
  end

  defp count_keys_uncached(_path, []), do: {:ok, []}

  defp count_keys_uncached(path, count_keys) do
    case Access.get_many(path, count_keys) do
      {:ok, results} ->
        counts =
          Enum.map(results, fn
            {:ok, blob} ->
              case IndexCodec.decode_count(blob) do
                {:ok, count} -> {:cache, count}
                :error -> :missing
              end

            :not_found ->
              :missing
          end)

        {:ok, counts}

      {:error, _reason} = error ->
        error
    end
  end

  defp cached_count_key(path, count_key) do
    ensure_cache()

    case :ets.lookup(@terminal_count_cache, {path, count_key}) do
      [{{^path, ^count_key}, count}] when is_integer(count) and count >= 0 -> {:ok, count}
      _ -> :miss
    end
  end

  defp ensure_cache do
    case :ets.whereis(@terminal_count_cache) do
      :undefined ->
        try do
          :ets.new(@terminal_count_cache, [
            :named_table,
            :public,
            :set,
            read_concurrency: true,
            write_concurrency: true
          ])
        rescue
          ArgumentError -> @terminal_count_cache
        end

      _table ->
        @terminal_count_cache
    end
  end
end
