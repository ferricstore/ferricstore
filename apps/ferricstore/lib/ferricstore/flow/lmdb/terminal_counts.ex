defmodule Ferricstore.Flow.LMDB.TerminalCounts do
  @moduledoc false

  alias Ferricstore.BatchResult
  alias Ferricstore.Flow.LMDB.{Access, IndexCodec, TerminalCountCacheOwner}

  @terminal_count_cache :ferricstore_flow_lmdb_terminal_count_cache

  def terminal_count(path, state_index_key) when is_binary(path) and is_binary(state_index_key) do
    count_key = IndexCodec.terminal_count_key(state_index_key)
    count_key_uncached(path, count_key)
  end

  def terminal_counts(path, state_index_keys)
      when is_binary(path) and is_list(state_index_keys) do
    count_keys = Enum.map(state_index_keys, &IndexCodec.terminal_count_key/1)

    if count_keys == [] do
      {:ok, []}
    else
      with {:ok, fetched} <- count_keys_uncached(path, count_keys) do
        {:ok,
         Enum.map(fetched, fn
           {:cache, count} -> count
           :missing -> 0
         end)}
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
    cache_insert({{path, count_key}, count})
    :ok
  end

  def delete_cached_count_key(path, count_key)
      when is_binary(path) and is_binary(count_key) do
    cache_delete({path, count_key})
    :ok
  end

  def clear_cached_for_path(path) when is_binary(path) do
    cache_match_delete({{path, :_}, :_})
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
          {:ok, count} -> {:ok, count}
          :error -> {:error, :invalid_terminal_count_value}
        end

      :not_found ->
        :not_found

      {:error, _reason} = error ->
        error

      _invalid ->
        {:error, :invalid_terminal_count_read}
    end
  end

  defp count_key_uncached(path, count_key) do
    case Access.get(path, count_key) do
      {:ok, blob} ->
        case IndexCodec.decode_count(blob) do
          {:ok, count} -> {:ok, count}
          :error -> {:error, :invalid_terminal_count_value}
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
        decode_count_results(count_keys, results)

      {:error, _reason} = error ->
        error

      invalid ->
        {:error, {:invalid_terminal_count_batch, invalid}}
    end
  end

  @doc false
  def __decode_count_results_for_test__(count_keys, results),
    do: decode_count_results(count_keys, results)

  defp decode_count_results(count_keys, results) do
    case BatchResult.map_exact(count_keys, results, &decode_count_result/2) do
      {:ok, decoded} ->
        Enum.reduce_while(decoded, {:ok, []}, fn
          {:ok, count}, {:ok, acc} -> {:cont, {:ok, [count | acc]}}
          {:error, _reason} = error, _acc -> {:halt, error}
        end)
        |> case do
          {:ok, reversed} -> {:ok, Enum.reverse(reversed)}
          {:error, _reason} = error -> error
        end

      {:error, reason} ->
        {:error, {:invalid_terminal_count_batch, reason}}
    end
  end

  defp decode_count_result(count_key, {:ok, blob}) when is_binary(blob) do
    case IndexCodec.decode_count(blob) do
      {:ok, count} -> {:ok, {:cache, count}}
      :error -> {:error, {:invalid_terminal_count_value, count_key}}
    end
  end

  defp decode_count_result(_count_key, :not_found), do: {:ok, :missing}

  defp decode_count_result(count_key, {:error, reason}),
    do: {:error, {:terminal_count_read_failed, count_key, reason}}

  defp decode_count_result(count_key, invalid),
    do: {:error, {:invalid_terminal_count_result, count_key, invalid}}

  defp cache_insert(record) do
    :ets.insert(@terminal_count_cache, record)
  rescue
    ArgumentError ->
      ensure_owned_cache()
      retry_cache_write(fn -> :ets.insert(@terminal_count_cache, record) end)
  end

  defp cache_delete(key) do
    :ets.delete(@terminal_count_cache, key)
  rescue
    ArgumentError ->
      ensure_owned_cache()
      retry_cache_write(fn -> :ets.delete(@terminal_count_cache, key) end)
  end

  defp cache_match_delete(pattern) do
    :ets.match_delete(@terminal_count_cache, pattern)
  rescue
    ArgumentError ->
      ensure_owned_cache()
      retry_cache_write(fn -> :ets.match_delete(@terminal_count_cache, pattern) end)
  end

  defp retry_cache_write(operation) do
    operation.()
  rescue
    ArgumentError -> true
  end

  defp ensure_owned_cache do
    _ = TerminalCountCacheOwner.ensure_table()
    :ok
  end
end
