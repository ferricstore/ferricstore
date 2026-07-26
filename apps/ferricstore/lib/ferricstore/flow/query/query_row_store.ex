defmodule Ferricstore.Flow.Query.QueryRowStore do
  @moduledoc false

  alias Ferricstore.Flow.LMDB
  alias Ferricstore.Flow.Query.QueryRowCodec

  @spec read_many(binary(), [binary()], non_neg_integer(), pos_integer()) ::
          {:ok, [struct() | nil], non_neg_integer(), boolean()} | {:error, term()}
  def read_many(path, state_keys, now_ms, max_bytes)
      when is_binary(path) and is_list(state_keys) and is_integer(now_ms) and now_ms >= 0 and
             is_integer(max_bytes) and max_bytes > 0 do
    read_decoded(path, state_keys, now_ms, max_bytes, &QueryRowCodec.decode/3)
  end

  def read_many(_path, _state_keys, _now_ms, _max_bytes),
    do: {:error, :invalid_query_row_read}

  @spec read_references_many(binary(), [binary()], non_neg_integer(), pos_integer()) ::
          {:ok, [struct() | nil], non_neg_integer(), boolean()} | {:error, term()}
  def read_references_many(path, state_keys, now_ms, max_bytes)
      when is_binary(path) and is_list(state_keys) and is_integer(now_ms) and now_ms >= 0 and
             is_integer(max_bytes) and max_bytes > 0 do
    read_decoded(path, state_keys, now_ms, max_bytes, &QueryRowCodec.decode_reference/3)
  end

  def read_references_many(_path, _state_keys, _now_ms, _max_bytes),
    do: {:error, :invalid_query_row_read}

  defp read_decoded(path, state_keys, now_ms, max_bytes, decoder) do
    with {:ok, values, value_bytes, complete?} <-
           LMDB.get_many_prefix_bounded(path, state_keys, max_bytes),
         true <- length(values) <= length(state_keys),
         keys = Enum.take(state_keys, length(values)),
         {:ok, rows} <- decode_rows(keys, values, now_ms, decoder, []) do
      {:ok, rows, value_bytes, complete?}
    else
      false -> {:error, :query_row_result_count_mismatch}
      {:error, _reason} = error -> error
      _invalid -> {:error, :invalid_query_row_read}
    end
  rescue
    _error -> {:error, :query_row_read_failed}
  catch
    _kind, _reason -> {:error, :query_row_read_failed}
  end

  defp decode_rows([], [], _now_ms, _decoder, acc), do: {:ok, Enum.reverse(acc)}

  defp decode_rows([_state_key | state_keys], [:not_found | values], now_ms, decoder, acc),
    do: decode_rows(state_keys, values, now_ms, decoder, [nil | acc])

  defp decode_rows([state_key | state_keys], [{:ok, encoded} | values], now_ms, decoder, acc)
       when is_binary(encoded) do
    case decoder.(encoded, state_key, now_ms) do
      {:ok, row} -> decode_rows(state_keys, values, now_ms, decoder, [row | acc])
      :expired -> decode_rows(state_keys, values, now_ms, decoder, [nil | acc])
      :error -> {:error, :invalid_query_row}
    end
  end

  defp decode_rows(_state_keys, _values, _now_ms, _decoder, _acc),
    do: {:error, :invalid_query_row_read}
end
