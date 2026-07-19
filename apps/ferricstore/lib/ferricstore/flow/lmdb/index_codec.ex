defmodule Ferricstore.Flow.LMDB.IndexCodec do
  @moduledoc false

  alias Ferricstore.TermCodec

  @u64_decimal_zero_pad "00000000000000000000"
  @max_u64 18_446_744_073_709_551_615
  @max_active_reverse_keys 6
  @max_lane_key_bytes 1_048_576
  @max_lane_member_bytes 65_551
  @active_reverse_tag :flow_active_reverse

  def terminal_index_prefix(state_index_key) when is_binary(state_index_key) do
    "flow-terminal-index:" <> state_index_key <> <<0>>
  end

  def terminal_index_global_prefix, do: "flow-terminal-index:"

  def terminal_index_key(state_index_key, id, updated_at_ms)
      when is_binary(state_index_key) and is_binary(id) and is_integer(updated_at_ms) and
             updated_at_ms >= 0 and updated_at_ms <= @max_u64 do
    terminal_index_prefix(state_index_key) <> pad_u64(updated_at_ms) <> <<0>> <> id
  end

  def terminal_index_key(_state_index_key, _id, _updated_at_ms),
    do: raise(ArgumentError, "terminal index time must be an unsigned 64-bit integer")

  def terminal_index_entry_key?(key, id, updated_at_ms),
    do: ordered_index_entry_key?(key, terminal_index_global_prefix(), id, updated_at_ms)

  def terminal_count_key(state_index_key) when is_binary(state_index_key) do
    "flow-terminal-count:" <> state_index_key
  end

  def terminal_count_prefix, do: "flow-terminal-count:"

  def terminal_expire_prefix, do: "flow-terminal-expire:"

  def terminal_expire_key(expire_at_ms, terminal_key)
      when is_integer(expire_at_ms) and expire_at_ms > 0 and expire_at_ms <= @max_u64 and
             is_binary(terminal_key) do
    terminal_expire_prefix() <> pad_u64(expire_at_ms) <> <<0>> <> terminal_key
  end

  def terminal_expire_key(0, terminal_key) when is_binary(terminal_key), do: nil

  def terminal_expire_key(_expire_at_ms, _terminal_key),
    do: raise(ArgumentError, "terminal expiration must be an unsigned 64-bit integer")

  def terminal_by_state_key_key(state_key) when is_binary(state_key) do
    "flow-terminal-by-state:" <> state_key
  end

  def terminal_by_state_global_prefix, do: "flow-terminal-by-state:"

  def active_index_global_prefix, do: "flow-active-index:"

  def active_index_prefix(index_key) when is_binary(index_key) do
    active_index_global_prefix() <> index_key <> <<0>>
  end

  def active_index_key(index_key, id, score)
      when is_binary(index_key) and is_binary(id) and is_integer(score) and score >= 0 and
             score <= @max_u64 do
    active_index_prefix(index_key) <> pad_u64(score) <> <<0>> <> id
  end

  def active_index_key(_index_key, _id, _score),
    do: raise(ArgumentError, "active index score must be an unsigned 64-bit integer")

  def active_index_entry_key?(key, index_key, id, score)
      when is_binary(key) and is_binary(index_key) and is_binary(id) and is_integer(score) and
             score >= 0 and score <= @max_u64,
      do: key == active_index_key(index_key, id, score)

  def active_index_entry_key?(_key, _index_key, _id, _score), do: false

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
      when is_binary(index_key) and is_binary(id) and is_integer(updated_at_ms) and
             updated_at_ms >= 0 and updated_at_ms <= @max_u64 do
    query_index_prefix(index_key) <> pad_u64(updated_at_ms) <> <<0>> <> id
  end

  def query_index_key(_index_key, _id, _updated_at_ms),
    do: raise(ArgumentError, "query index time must be an unsigned 64-bit integer")

  def query_index_entry_key?(key, id, updated_at_ms),
    do: ordered_index_entry_key?(key, query_index_global_prefix(), id, updated_at_ms)

  def history_index_prefix(history_key) when is_binary(history_key) do
    "flow-history-index:" <> history_key <> <<0>>
  end

  def history_index_key(history_key, event_id, event_ms)
      when is_binary(history_key) and is_binary(event_id) and is_integer(event_ms) and
             event_ms >= 0 and event_ms <= @max_u64 do
    history_index_prefix(history_key) <> pad_u64(event_ms) <> <<0>> <> event_id
  end

  def history_index_key(_history_key, _event_id, _event_ms),
    do: raise(ArgumentError, "history event time must be an unsigned 64-bit integer")

  def history_index_entry_key?(key, event_id, event_ms),
    do: ordered_index_entry_key?(key, history_index_global_prefix(), event_id, event_ms)

  defp history_index_global_prefix, do: "flow-history-index:"

  def history_expire_prefix, do: "flow-history-expire:"

  def history_flow_expire_prefix, do: "flow-history-flow-expire:"

  def history_expire_key(expire_at_ms, history_index_key)
      when is_integer(expire_at_ms) and expire_at_ms > 0 and expire_at_ms <= @max_u64 and
             is_binary(history_index_key) do
    history_expire_prefix() <> pad_u64(expire_at_ms) <> <<0>> <> history_index_key
  end

  def history_expire_key(0, history_index_key) when is_binary(history_index_key), do: nil

  def history_expire_key(_expire_at_ms, _history_index_key),
    do: raise(ArgumentError, "history expiration must be an unsigned 64-bit integer")

  def history_flow_expire_key(expire_at_ms, history_key)
      when is_integer(expire_at_ms) and expire_at_ms > 0 and expire_at_ms <= @max_u64 and
             is_binary(history_key) do
    history_flow_expire_prefix() <> pad_u64(expire_at_ms) <> <<0>> <> history_key
  end

  def history_flow_expire_key(0, history_key) when is_binary(history_key), do: nil

  def history_flow_expire_key(_expire_at_ms, _history_key),
    do: raise(ArgumentError, "history flow expiration must be an unsigned 64-bit integer")

  def encode_history_index_value(event_id, event_ms, compound_key, expire_at_ms \\ 0)

  def encode_history_index_value(event_id, event_ms, compound_key, expire_at_ms)
      when is_binary(event_id) and is_integer(event_ms) and event_ms >= 0 and
             event_ms <= @max_u64 and is_integer(expire_at_ms) and expire_at_ms >= 0 and
             expire_at_ms <= @max_u64 and is_binary(compound_key) do
    TermCodec.encode({event_id, event_ms, expire_at_ms, compound_key})
  end

  def encode_history_index_value(_event_id, _event_ms, _compound_key, _expire_at_ms),
    do: raise(ArgumentError, "history index fields are invalid")

  def encode_history_index_value(
        event_id,
        event_ms,
        compound_key,
        expire_at_ms,
        {:flow_history, file_id},
        offset,
        value_size
      )
      when is_binary(event_id) and is_integer(event_ms) and event_ms >= 0 and
             event_ms <= @max_u64 and is_binary(compound_key) and is_integer(expire_at_ms) and
             expire_at_ms >= 0 and expire_at_ms <= @max_u64 and is_integer(file_id) and
             file_id >= 0 and file_id <= @max_u64 and is_integer(offset) and offset >= 0 and
             offset <= @max_u64 and is_integer(value_size) and value_size >= 0 and
             value_size <= @max_u64 do
    TermCodec.encode(
      {event_id, event_ms, expire_at_ms, compound_key, {:flow_history, file_id}, offset,
       value_size}
    )
  end

  def encode_history_index_value(
        _event_id,
        _event_ms,
        _compound_key,
        _expire_at_ms,
        _file_ref,
        _offset,
        _value_size
      ),
      do: raise(ArgumentError, "history index location fields are invalid")

  def encode_history_expire_value(history_index_key) when is_binary(history_index_key) do
    TermCodec.encode(history_index_key)
  end

  def encode_history_flow_expire_value(history_key, expire_at_ms)
      when is_binary(history_key) and is_integer(expire_at_ms) and expire_at_ms > 0 and
             expire_at_ms <= @max_u64 do
    TermCodec.encode({history_key, expire_at_ms})
  end

  def encode_history_flow_expire_value(_history_key, _expire_at_ms),
    do: raise(ArgumentError, "history flow expiration fields are invalid")

  def decode_history_expire_value(blob) when is_binary(blob) do
    case TermCodec.decode(blob) do
      {:ok, history_index_key} when is_binary(history_index_key) -> {:ok, history_index_key}
      _ -> :error
    end
  rescue
    _ -> :error
  end

  def decode_history_flow_expire_value(blob) when is_binary(blob) do
    case TermCodec.decode(blob) do
      {:ok, {history_key, expire_at_ms}}
      when is_binary(history_key) and is_integer(expire_at_ms) and expire_at_ms > 0 and
             expire_at_ms <= @max_u64 ->
        {:ok, {history_key, expire_at_ms}}

      _ ->
        :error
    end
  rescue
    _ -> :error
  end

  def encode_query_index_value(id, updated_at_ms, expire_at_ms \\ 0, state_key \\ nil)

  def encode_query_index_value(id, updated_at_ms, expire_at_ms, state_key)
      when is_binary(id) and is_integer(updated_at_ms) and updated_at_ms >= 0 and
             updated_at_ms <= @max_u64 and is_integer(expire_at_ms) and expire_at_ms >= 0 and
             expire_at_ms <= @max_u64 and (is_binary(state_key) or is_nil(state_key)) do
    TermCodec.encode({id, updated_at_ms, expire_at_ms, state_key})
  end

  def encode_query_index_value(_id, _updated_at_ms, _expire_at_ms, _state_key),
    do: raise(ArgumentError, "query index fields are invalid")

  def encode_active_index_value(index_key, id, score, expire_at_ms, state_key)
      when is_binary(index_key) and is_binary(id) and is_integer(score) and score >= 0 and
             score <= @max_u64 and is_integer(expire_at_ms) and expire_at_ms >= 0 and
             expire_at_ms <= @max_u64 and is_binary(state_key) do
    TermCodec.encode({index_key, id, score, expire_at_ms, state_key})
  end

  def encode_active_index_value(_index_key, _id, _score, _expire_at_ms, _state_key),
    do: raise(ArgumentError, "active index fields are invalid")

  def decode_active_index_value(blob) when is_binary(blob) do
    case TermCodec.decode(blob) do
      {:ok, {index_key, id, score, expire_at_ms, state_key}}
      when is_binary(index_key) and is_binary(id) and is_integer(score) and
             score >= 0 and score <= @max_u64 and is_integer(expire_at_ms) and
             expire_at_ms >= 0 and expire_at_ms <= @max_u64 and is_binary(state_key) ->
        {:ok, {index_key, id, score, expire_at_ms, state_key}}

      _ ->
        :error
    end
  rescue
    _ -> :error
  end

  def encode_active_index_reverse_value(active_keys) when is_list(active_keys) do
    if valid_active_reverse_keys?(active_keys) do
      TermCodec.encode({@active_reverse_tag, active_keys, nil})
    else
      raise ArgumentError, "active reverse index keys are invalid"
    end
  end

  def encode_active_index_reverse_value(active_keys, {lane_key, lane_member, lane_score})
      when is_list(active_keys) and lane_score in [-1, 0] do
    if valid_active_reverse_keys?(active_keys, true) and
         valid_fifo_lane_identity?(lane_key, lane_member) do
      TermCodec.encode({@active_reverse_tag, active_keys, {lane_key, lane_member, lane_score}})
    else
      raise ArgumentError, "active reverse index metadata is invalid"
    end
  end

  def decode_active_index_reverse_value(blob) when is_binary(blob) do
    case decode_active_index_reverse_metadata(blob) do
      {:ok, {active_keys, _lane_entry}} -> {:ok, active_keys}
      :error -> :error
    end
  end

  def decode_active_index_reverse_lane_value(blob) when is_binary(blob) do
    case decode_active_index_reverse_metadata(blob) do
      {:ok, {_active_keys, nil}} -> :missing
      {:ok, {_active_keys, lane_entry}} -> {:ok, lane_entry}
      :error -> :error
    end
  end

  def decode_active_index_reverse_metadata(blob) when is_binary(blob) do
    case TermCodec.decode(blob) do
      {:ok, {@active_reverse_tag, active_keys, nil}} when is_list(active_keys) ->
        if valid_active_reverse_keys?(active_keys), do: {:ok, {active_keys, nil}}, else: :error

      {:ok, {@active_reverse_tag, active_keys, {lane_key, lane_member, lane_score}}}
      when is_list(active_keys) and lane_score in [-1, 0] ->
        if valid_active_reverse_keys?(active_keys, true) and
             valid_fifo_lane_identity?(lane_key, lane_member) do
          {:ok, {active_keys, {lane_key, lane_member, lane_score}}}
        else
          :error
        end

      _ ->
        :error
    end
  rescue
    _ -> :error
  end

  defp valid_active_reverse_keys?(active_keys),
    do: valid_active_reverse_keys?(active_keys, false)

  defp valid_active_reverse_keys?(active_keys, allow_empty?),
    do: valid_active_reverse_keys?(active_keys, MapSet.new(), 0, allow_empty?)

  defp valid_active_reverse_keys?([], _seen, count, allow_empty?),
    do: count > 0 or allow_empty?

  defp valid_active_reverse_keys?([key | rest], seen, count, allow_empty?)
       when is_binary(key) and count < @max_active_reverse_keys do
    if String.starts_with?(key, active_index_global_prefix()) and not MapSet.member?(seen, key) do
      valid_active_reverse_keys?(rest, MapSet.put(seen, key), count + 1, allow_empty?)
    else
      false
    end
  end

  defp valid_active_reverse_keys?(_invalid, _seen, _count, _allow_empty?), do: false

  defp valid_fifo_lane_identity?(lane_key, <<_sequence::unsigned-big-128, id::binary>>)
       when is_binary(lane_key) and lane_key != "" and byte_size(lane_key) <= @max_lane_key_bytes and
              id != "" and byte_size(id) + 16 <= @max_lane_member_bytes,
       do: String.starts_with?(lane_key, "f:") and String.contains?(lane_key, ":fl:1:")

  defp valid_fifo_lane_identity?(_lane_key, _lane_member), do: false

  def encode_terminal_index_value(id, updated_at_ms, expire_at_ms, state_key, count_key)
      when is_binary(id) and is_integer(updated_at_ms) and updated_at_ms >= 0 and
             updated_at_ms <= @max_u64 and is_integer(expire_at_ms) and expire_at_ms >= 0 and
             expire_at_ms <= @max_u64 and (is_binary(state_key) or is_nil(state_key)) and
             is_binary(count_key) do
    TermCodec.encode({id, updated_at_ms, expire_at_ms, state_key, count_key})
  end

  def encode_terminal_index_value(
        _id,
        _updated_at_ms,
        _expire_at_ms,
        _state_key,
        _count_key
      ),
      do: raise(ArgumentError, "terminal index fields are invalid")

  def encode_terminal_expire_value(terminal_key, state_key, count_key)
      when is_binary(terminal_key) and (is_binary(state_key) or is_nil(state_key)) and
             is_binary(count_key) do
    TermCodec.encode({terminal_key, state_key, count_key})
  end

  def encode_terminal_expire_value(_terminal_key, _state_key, _count_key),
    do: raise(ArgumentError, "terminal expiration fields are invalid")

  def decode_terminal_expire_value(blob) when is_binary(blob) do
    case TermCodec.decode(blob) do
      {:ok, {terminal_key, state_key, count_key}}
      when is_binary(terminal_key) and (is_binary(state_key) or is_nil(state_key)) and
             is_binary(count_key) ->
        {:ok, {terminal_key, state_key, count_key}}

      _ ->
        :error
    end
  rescue
    _ -> :error
  end

  def encode_count(count) when is_integer(count) and count >= 0 and count <= @max_u64 do
    TermCodec.encode(count)
  end

  def encode_count(_count), do: raise(ArgumentError, "count must be an unsigned 64-bit integer")

  def decode_count(blob) when is_binary(blob) do
    case TermCodec.decode(blob) do
      {:ok, count} when is_integer(count) and count >= 0 and count <= @max_u64 -> {:ok, count}
      _ -> :error
    end
  rescue
    _ -> :error
  end

  def decode_terminal_index_value(blob) when is_binary(blob) do
    case TermCodec.decode(blob) do
      {:ok, {id, updated_at_ms, expire_at_ms, state_key, count_key}}
      when is_binary(id) and is_integer(updated_at_ms) and updated_at_ms >= 0 and
             updated_at_ms <= @max_u64 and is_integer(expire_at_ms) and expire_at_ms >= 0 and
             expire_at_ms <= @max_u64 and
             (is_binary(state_key) or is_nil(state_key)) and is_binary(count_key) ->
        {:ok, {id, updated_at_ms, expire_at_ms, state_key}}

      _ ->
        :error
    end
  rescue
    _ -> :error
  end

  def decode_query_index_value(blob) when is_binary(blob) do
    case TermCodec.decode(blob) do
      {:ok, {id, updated_at_ms, expire_at_ms, state_key}}
      when is_binary(id) and is_integer(updated_at_ms) and updated_at_ms >= 0 and
             updated_at_ms <= @max_u64 and is_integer(expire_at_ms) and expire_at_ms >= 0 and
             expire_at_ms <= @max_u64 and
             (is_binary(state_key) or is_nil(state_key)) ->
        {:ok, {id, updated_at_ms, expire_at_ms, state_key}}

      _ ->
        :error
    end
  rescue
    _ -> :error
  end

  def decode_history_index_value(blob) when is_binary(blob) do
    case TermCodec.decode(blob) do
      {:ok,
       {event_id, event_ms, expire_at_ms, compound_key, {:flow_history, file_id}, offset,
        value_size}}
      when is_binary(event_id) and is_integer(event_ms) and event_ms >= 0 and
             event_ms <= @max_u64 and is_integer(expire_at_ms) and expire_at_ms >= 0 and
             expire_at_ms <= @max_u64 and
             is_binary(compound_key) and is_integer(file_id) and file_id >= 0 and
             file_id <= @max_u64 and is_integer(offset) and offset >= 0 and
             offset <= @max_u64 and is_integer(value_size) and value_size >= 0 and
             value_size <= @max_u64 ->
        {:ok, {event_id, event_ms, expire_at_ms, compound_key}}

      {:ok, {event_id, event_ms, expire_at_ms, compound_key}}
      when is_binary(event_id) and is_integer(event_ms) and event_ms >= 0 and
             event_ms <= @max_u64 and is_integer(expire_at_ms) and expire_at_ms >= 0 and
             expire_at_ms <= @max_u64 and
             is_binary(compound_key) ->
        {:ok, {event_id, event_ms, expire_at_ms, compound_key}}

      _ ->
        :error
    end
  rescue
    _ -> :error
  end

  def decode_history_index_location(blob) when is_binary(blob) do
    case TermCodec.decode(blob) do
      {:ok,
       {event_id, event_ms, expire_at_ms, compound_key, {:flow_history, file_id}, offset,
        value_size}}
      when is_binary(event_id) and is_integer(event_ms) and event_ms >= 0 and
             event_ms <= @max_u64 and is_integer(expire_at_ms) and expire_at_ms >= 0 and
             expire_at_ms <= @max_u64 and
             is_binary(compound_key) and is_integer(file_id) and file_id >= 0 and
             file_id <= @max_u64 and is_integer(offset) and offset >= 0 and
             offset <= @max_u64 and is_integer(value_size) and value_size >= 0 and
             value_size <= @max_u64 ->
        {:ok,
         {event_id, event_ms, expire_at_ms, compound_key, {:flow_history, file_id}, offset,
          value_size}}

      {:ok, {event_id, event_ms, expire_at_ms, compound_key}}
      when is_binary(event_id) and is_integer(event_ms) and event_ms >= 0 and
             event_ms <= @max_u64 and is_integer(expire_at_ms) and expire_at_ms >= 0 and
             expire_at_ms <= @max_u64 and
             is_binary(compound_key) ->
        {:ok, {event_id, event_ms, expire_at_ms, compound_key, nil, nil, nil}}

      _ ->
        :error
    end
  rescue
    _ -> :error
  end

  def terminal_index_count_key(blob) when is_binary(blob) do
    case TermCodec.decode(blob) do
      {:ok, {id, updated_at_ms, expire_at_ms, state_key, count_key}}
      when is_binary(id) and is_integer(updated_at_ms) and updated_at_ms >= 0 and
             updated_at_ms <= @max_u64 and is_integer(expire_at_ms) and expire_at_ms >= 0 and
             expire_at_ms <= @max_u64 and
             (is_binary(state_key) or is_nil(state_key)) and is_binary(count_key) ->
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

  defp ordered_index_entry_key?(key, prefix, id, time)
       when is_binary(key) and is_binary(prefix) and is_binary(id) and is_integer(time) and
              time >= 0 and time <= @max_u64 do
    suffix = <<0>> <> pad_u64(time) <> <<0>> <> id
    prefix_size = byte_size(prefix)
    suffix_size = byte_size(suffix)
    key_size = byte_size(key)

    key_size > prefix_size + suffix_size and
      binary_part(key, 0, prefix_size) == prefix and
      binary_part(key, key_size - suffix_size, suffix_size) == suffix
  end

  defp ordered_index_entry_key?(_key, _prefix, _id, _time), do: false
end
