defmodule Ferricstore.Flow.LMDB.TerminalCounts do
  @moduledoc false

  alias Ferricstore.BatchResult
  alias Ferricstore.Flow.LMDB.{Access, IndexCodec}

  def terminal_count(path, state_index_key) when is_binary(path) and is_binary(state_index_key) do
    count_key = IndexCodec.terminal_count_key(state_index_key)
    read_count_key(path, count_key)
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
           {:value, count} -> count
           :missing -> 0
         end)}
      end
    end
  end

  def put_terminal_count(path, state_index_key, count)
      when is_binary(path) and is_binary(state_index_key) and is_integer(count) and count >= 0 do
    count_key = IndexCodec.terminal_count_key(state_index_key)

    Access.write_batch(path, [{:put, count_key, IndexCodec.encode_count(count)}])
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
      {:ok, count} -> {:ok, {:value, count}}
      :error -> {:error, {:invalid_terminal_count_value, count_key}}
    end
  end

  defp decode_count_result(_count_key, :not_found), do: {:ok, :missing}

  defp decode_count_result(count_key, {:error, reason}),
    do: {:error, {:terminal_count_read_failed, count_key, reason}}

  defp decode_count_result(count_key, invalid),
    do: {:error, {:invalid_terminal_count_result, count_key, invalid}}
end
