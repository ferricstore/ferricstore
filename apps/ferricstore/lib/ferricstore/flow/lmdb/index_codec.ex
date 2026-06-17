defmodule Ferricstore.Flow.LMDB.IndexCodec do
  @moduledoc false

  @u64_decimal_zero_pad "00000000000000000000"

  def terminal_index_prefix(state_index_key) when is_binary(state_index_key) do
    "flow-terminal-index:" <> state_index_key <> <<0>>
  end

  def terminal_index_global_prefix, do: "flow-terminal-index:"

  def terminal_index_key(state_index_key, id, updated_at_ms)
      when is_binary(state_index_key) and is_binary(id) and is_integer(updated_at_ms) do
    terminal_index_prefix(state_index_key) <> pad_u64(updated_at_ms) <> <<0>> <> id
  end

  def terminal_count_key(state_index_key) when is_binary(state_index_key) do
    "flow-terminal-count:" <> state_index_key
  end

  def terminal_count_prefix, do: "flow-terminal-count:"

  def terminal_expire_prefix, do: "flow-terminal-expire:"

  def terminal_expire_key(expire_at_ms, terminal_key)
      when is_integer(expire_at_ms) and expire_at_ms > 0 and is_binary(terminal_key) do
    terminal_expire_prefix() <> pad_u64(expire_at_ms) <> <<0>> <> terminal_key
  end

  def terminal_expire_key(_expire_at_ms, _terminal_key), do: nil

  def terminal_by_state_key_key(state_key) when is_binary(state_key) do
    "flow-terminal-by-state:" <> state_key
  end

  def terminal_by_state_global_prefix, do: "flow-terminal-by-state:"

  def active_index_global_prefix, do: "flow-active-index:"

  def active_index_prefix(index_key) when is_binary(index_key) do
    active_index_global_prefix() <> index_key <> <<0>>
  end

  def active_index_key(index_key, id, score)
      when is_binary(index_key) and is_binary(id) and is_integer(score) do
    active_index_prefix(index_key) <> pad_u64(score) <> <<0>> <> id
  end

  def active_by_state_key_key(state_key) when is_binary(state_key) do
    "flow-active-by-state:" <> state_key
  end

  def active_by_state_global_prefix, do: "flow-active-by-state:"

  def query_index_global_prefix, do: "flow-query-index:"

  def query_index_raw_prefix(index_key_prefix) when is_binary(index_key_prefix) do
    query_index_global_prefix() <> index_key_prefix
  end

  def query_index_prefix(index_key) when is_binary(index_key) do
    query_index_raw_prefix(index_key) <> <<0>>
  end

  def query_index_key(index_key, id, updated_at_ms)
      when is_binary(index_key) and is_binary(id) and is_integer(updated_at_ms) do
    query_index_prefix(index_key) <> pad_u64(updated_at_ms) <> <<0>> <> id
  end

  def query_index_key(index_key, id, updated_at_ms)
      when is_binary(index_key) and is_binary(id) and is_float(updated_at_ms) do
    query_index_key(index_key, id, trunc(updated_at_ms))
  end

  def query_index_key(index_key, id, updated_at_ms)
      when is_binary(index_key) and is_binary(id) and is_binary(updated_at_ms) do
    score =
      case Integer.parse(updated_at_ms) do
        {int, ""} ->
          int

        _ ->
          case Float.parse(updated_at_ms) do
            {float, _rest} -> trunc(float)
            :error -> 0
          end
      end

    query_index_key(index_key, id, score)
  end

  def history_index_prefix(history_key) when is_binary(history_key) do
    "flow-history-index:" <> history_key <> <<0>>
  end

  def history_index_key(history_key, event_id, event_ms)
      when is_binary(history_key) and is_binary(event_id) and is_integer(event_ms) do
    history_index_prefix(history_key) <> pad_u64(event_ms) <> <<0>> <> event_id
  end

  def history_expire_prefix, do: "flow-history-expire:"

  def history_flow_expire_prefix, do: "flow-history-flow-expire:"

  def history_expire_key(expire_at_ms, history_index_key)
      when is_integer(expire_at_ms) and expire_at_ms > 0 and is_binary(history_index_key) do
    history_expire_prefix() <> pad_u64(expire_at_ms) <> <<0>> <> history_index_key
  end

  def history_expire_key(_expire_at_ms, _history_index_key), do: nil

  def history_flow_expire_key(expire_at_ms, history_key)
      when is_integer(expire_at_ms) and expire_at_ms > 0 and is_binary(history_key) do
    history_flow_expire_prefix() <> pad_u64(expire_at_ms) <> <<0>> <> history_key
  end

  def history_flow_expire_key(_expire_at_ms, _history_key), do: nil

  def encode_history_index_value(event_id, event_ms, compound_key, expire_at_ms \\ 0)
      when is_binary(event_id) and is_integer(event_ms) and is_integer(expire_at_ms) and
             is_binary(compound_key) do
    :erlang.term_to_binary({event_id, event_ms, expire_at_ms, compound_key})
  end

  def encode_history_index_value(
        event_id,
        event_ms,
        compound_key,
        expire_at_ms,
        {:flow_history, file_id},
        offset,
        value_size
      )
      when is_binary(event_id) and is_integer(event_ms) and is_binary(compound_key) and
             is_integer(expire_at_ms) and is_integer(file_id) and file_id >= 0 and
             is_integer(offset) and offset >= 0 and is_integer(value_size) and value_size >= 0 do
    :erlang.term_to_binary(
      {event_id, event_ms, expire_at_ms, compound_key, {:flow_history, file_id}, offset,
       value_size}
    )
  end

  def encode_history_expire_value(history_index_key) when is_binary(history_index_key) do
    :erlang.term_to_binary(history_index_key)
  end

  def encode_history_flow_expire_value(history_key, expire_at_ms)
      when is_binary(history_key) and is_integer(expire_at_ms) do
    :erlang.term_to_binary({history_key, expire_at_ms})
  end

  def decode_history_expire_value(blob) when is_binary(blob) do
    case :erlang.binary_to_term(blob, [:safe]) do
      history_index_key when is_binary(history_index_key) -> {:ok, history_index_key}
      _ -> :error
    end
  rescue
    _ -> :error
  end

  def decode_history_flow_expire_value(blob) when is_binary(blob) do
    case :erlang.binary_to_term(blob, [:safe]) do
      {history_key, expire_at_ms} when is_binary(history_key) and is_integer(expire_at_ms) ->
        {:ok, {history_key, expire_at_ms}}

      history_key when is_binary(history_key) ->
        {:ok, {history_key, :infinity}}

      _ ->
        :error
    end
  rescue
    _ -> :error
  end

  def encode_query_index_value(id, updated_at_ms, expire_at_ms \\ 0, state_key \\ nil) do
    :erlang.term_to_binary({id, normalize_ms(updated_at_ms), expire_at_ms, state_key})
  end

  def encode_active_index_value(index_key, id, score, expire_at_ms, state_key)
      when is_binary(index_key) and is_binary(id) and is_integer(score) and
             is_integer(expire_at_ms) and is_binary(state_key) do
    :erlang.term_to_binary({index_key, id, score, expire_at_ms, state_key})
  end

  def decode_active_index_value(blob) when is_binary(blob) do
    case :erlang.binary_to_term(blob, [:safe]) do
      {index_key, id, score, expire_at_ms, state_key}
      when is_binary(index_key) and is_binary(id) and is_integer(score) and
             is_integer(expire_at_ms) and is_binary(state_key) ->
        {:ok, {index_key, id, score, expire_at_ms, state_key}}

      _ ->
        :error
    end
  rescue
    _ -> :error
  end

  def encode_active_index_reverse_value(active_keys) when is_list(active_keys) do
    active_keys =
      Enum.filter(active_keys, fn
        active_key when is_binary(active_key) -> true
        _ -> false
      end)

    :erlang.term_to_binary(active_keys)
  end

  def decode_active_index_reverse_value(blob) when is_binary(blob) do
    case :erlang.binary_to_term(blob, [:safe]) do
      active_keys when is_list(active_keys) ->
        active_keys =
          Enum.filter(active_keys, fn
            active_key when is_binary(active_key) -> true
            _ -> false
          end)

        {:ok, active_keys}

      _ ->
        :error
    end
  rescue
    _ -> :error
  end

  def encode_terminal_index_value(id, updated_at_ms, expire_at_ms \\ 0, state_key \\ nil) do
    :erlang.term_to_binary({id, updated_at_ms, expire_at_ms, state_key})
  end

  def encode_terminal_index_value(id, updated_at_ms, expire_at_ms, state_key, count_key)
      when is_binary(count_key) do
    :erlang.term_to_binary({id, updated_at_ms, expire_at_ms, state_key, count_key})
  end

  def encode_terminal_expire_value(terminal_key, state_key, count_key)
      when is_binary(terminal_key) and is_binary(count_key) do
    :erlang.term_to_binary({terminal_key, state_key, count_key})
  end

  def decode_terminal_expire_value(blob) when is_binary(blob) do
    case :erlang.binary_to_term(blob, [:safe]) do
      {terminal_key, state_key, count_key}
      when is_binary(terminal_key) and (is_binary(state_key) or is_nil(state_key)) and
             is_binary(count_key) ->
        {:ok, {terminal_key, state_key, count_key}}

      _ ->
        :error
    end
  rescue
    _ -> :error
  end

  def encode_count(count) when is_integer(count) and count >= 0 do
    :erlang.term_to_binary(count)
  end

  def decode_count(blob) when is_binary(blob) do
    case :erlang.binary_to_term(blob, [:safe]) do
      count when is_integer(count) and count >= 0 -> {:ok, count}
      _ -> :error
    end
  rescue
    _ -> :error
  end

  def decode_terminal_index_value(blob) when is_binary(blob) do
    case :erlang.binary_to_term(blob, [:safe]) do
      {id, updated_at_ms, expire_at_ms, state_key, count_key}
      when is_binary(id) and is_integer(updated_at_ms) and is_integer(expire_at_ms) and
             (is_binary(state_key) or is_nil(state_key)) and is_binary(count_key) ->
        {:ok, {id, updated_at_ms, expire_at_ms, state_key}}

      {id, updated_at_ms, expire_at_ms, state_key}
      when is_binary(id) and is_integer(updated_at_ms) and is_integer(expire_at_ms) and
             (is_binary(state_key) or is_nil(state_key)) ->
        {:ok, {id, updated_at_ms, expire_at_ms, state_key}}

      {id, updated_at_ms, expire_at_ms}
      when is_binary(id) and is_integer(updated_at_ms) and is_integer(expire_at_ms) ->
        {:ok, {id, updated_at_ms, expire_at_ms, nil}}

      {id, updated_at_ms} when is_binary(id) and is_integer(updated_at_ms) ->
        {:ok, {id, updated_at_ms, 0, nil}}

      _ ->
        :error
    end
  rescue
    _ -> :error
  end

  def decode_query_index_value(blob) when is_binary(blob) do
    case :erlang.binary_to_term(blob, [:safe]) do
      {id, updated_at_ms, expire_at_ms, state_key}
      when is_binary(id) and is_integer(updated_at_ms) and is_integer(expire_at_ms) and
             (is_binary(state_key) or is_nil(state_key)) ->
        {:ok, {id, updated_at_ms, expire_at_ms, state_key}}

      {id, updated_at_ms, expire_at_ms}
      when is_binary(id) and is_integer(updated_at_ms) and is_integer(expire_at_ms) ->
        {:ok, {id, updated_at_ms, expire_at_ms, nil}}

      {id, updated_at_ms}
      when is_binary(id) and is_integer(updated_at_ms) ->
        {:ok, {id, updated_at_ms, 0, nil}}

      _ ->
        :error
    end
  rescue
    _ -> :error
  end

  def decode_history_index_value(blob) when is_binary(blob) do
    case :erlang.binary_to_term(blob, [:safe]) do
      {event_id, event_ms, expire_at_ms, compound_key, {:flow_history, file_id}, offset,
       value_size}
      when is_binary(event_id) and is_integer(event_ms) and is_integer(expire_at_ms) and
             is_binary(compound_key) and is_integer(file_id) and file_id >= 0 and
             is_integer(offset) and offset >= 0 and is_integer(value_size) and value_size >= 0 ->
        {:ok, {event_id, event_ms, expire_at_ms, compound_key}}

      {event_id, event_ms, expire_at_ms, compound_key}
      when is_binary(event_id) and is_integer(event_ms) and is_integer(expire_at_ms) and
             is_binary(compound_key) ->
        {:ok, {event_id, event_ms, expire_at_ms, compound_key}}

      {event_id, event_ms, compound_key}
      when is_binary(event_id) and is_integer(event_ms) and is_binary(compound_key) ->
        {:ok, {event_id, event_ms, 0, compound_key}}

      _ ->
        :error
    end
  rescue
    _ -> :error
  end

  def decode_history_index_location(blob) when is_binary(blob) do
    case :erlang.binary_to_term(blob, [:safe]) do
      {event_id, event_ms, expire_at_ms, compound_key, {:flow_history, file_id}, offset,
       value_size}
      when is_binary(event_id) and is_integer(event_ms) and is_integer(expire_at_ms) and
             is_binary(compound_key) and is_integer(file_id) and file_id >= 0 and
             is_integer(offset) and offset >= 0 and is_integer(value_size) and value_size >= 0 ->
        {:ok,
         {event_id, event_ms, expire_at_ms, compound_key, {:flow_history, file_id}, offset,
          value_size}}

      {event_id, event_ms, expire_at_ms, compound_key}
      when is_binary(event_id) and is_integer(event_ms) and is_integer(expire_at_ms) and
             is_binary(compound_key) ->
        {:ok, {event_id, event_ms, expire_at_ms, compound_key, nil, nil, nil}}

      {event_id, event_ms, compound_key}
      when is_binary(event_id) and is_integer(event_ms) and is_binary(compound_key) ->
        {:ok, {event_id, event_ms, 0, compound_key, nil, nil, nil}}

      _ ->
        :error
    end
  rescue
    _ -> :error
  end

  def terminal_index_count_key(blob) when is_binary(blob) do
    case :erlang.binary_to_term(blob, [:safe]) do
      {_id, _updated_at_ms, _expire_at_ms, _state_key, count_key} when is_binary(count_key) ->
        {:ok, count_key}

      _ ->
        :missing
    end
  rescue
    _ -> :missing
  end

  defp pad_u64(value) do
    encoded = Integer.to_string(value)

    case byte_size(encoded) do
      size when size < 20 -> binary_part(@u64_decimal_zero_pad, 0, 20 - size) <> encoded
      _size -> encoded
    end
  end

  defp normalize_ms(value) when is_integer(value), do: value
  defp normalize_ms(value) when is_float(value), do: trunc(value)

  defp normalize_ms(value) when is_binary(value) do
    case Integer.parse(value) do
      {int, ""} ->
        int

      _ ->
        case Float.parse(value) do
          {float, _rest} -> trunc(float)
          :error -> 0
        end
    end
  end

  defp normalize_ms(_value), do: 0
end
