defmodule Ferricstore.Flow.LMDB do
  @moduledoc false

  alias Ferricstore.Bitcask.NIF

  @default_map_size 64 * 1024 * 1024 * 1024

  def enabled? do
    mode() != :off
  end

  def write_through? do
    mode() == :write_through
  end

  def mirror? do
    mode() == :mirror
  end

  def mode do
    configured = Application.get_env(:ferricstore, :flow_lmdb_mode)

    cond do
      configured != nil ->
        normalize_mode(configured)

      Application.get_env(:ferricstore, :flow_lmdb_enabled, false) in [true, "true", "1"] ->
        :mirror

      true ->
        :off
    end
  end

  def map_size do
    Application.get_env(:ferricstore, :flow_lmdb_map_size, @default_map_size)
  end

  def path(shard_data_path), do: Path.join(shard_data_path, "flow_lmdb")

  def encode_value(value, expire_at_ms), do: :erlang.term_to_binary({expire_at_ms, value})

  def decode_value(blob, now_ms) when is_binary(blob) do
    case :erlang.binary_to_term(blob) do
      {expire_at_ms, value} when is_integer(expire_at_ms) and expire_at_ms > 0 ->
        if expire_at_ms <= now_ms, do: :expired, else: {:ok, value}

      {0, value} ->
        {:ok, value}

      {_expire_at_ms, value} ->
        {:ok, value}
    end
  rescue
    _ -> :error
  end

  def get(path, key) when is_binary(path) and is_binary(key) do
    NIF.lmdb_get(path, key, map_size())
  end

  def write_batch(_path, []), do: :ok

  def write_batch(path, ops) when is_binary(path) and is_list(ops) do
    NIF.lmdb_write_batch(path, ops, map_size())
  end

  def write_batch_with_originals(_path, []), do: {:ok, []}

  def write_batch_with_originals(path, ops) when is_binary(path) and is_list(ops) do
    NIF.lmdb_write_batch_with_originals(path, ops, map_size())
  end

  def prefix_entries(path, prefix, limit)
      when is_binary(path) and is_binary(prefix) and is_integer(limit) and limit >= 0 do
    NIF.lmdb_prefix_entries(path, prefix, limit, map_size())
  end

  def prefix_count(path, prefix) when is_binary(path) and is_binary(prefix) do
    NIF.lmdb_prefix_count(path, prefix, map_size())
  end

  def terminal_state?(state) when state in ["completed", "failed", "cancelled"], do: true
  def terminal_state?(_state), do: false

  def terminal_index_prefix(state_index_key) when is_binary(state_index_key) do
    "flow-terminal-index:" <> state_index_key <> <<0>>
  end

  def terminal_index_key(state_index_key, id, updated_at_ms)
      when is_binary(state_index_key) and is_binary(id) and is_integer(updated_at_ms) do
    terminal_index_prefix(state_index_key) <>
      Integer.to_string(updated_at_ms) <> <<0>> <> id
  end

  def terminal_count_key(state_index_key) when is_binary(state_index_key) do
    "flow-terminal-count:" <> state_index_key
  end

  def terminal_expire_prefix, do: "flow-terminal-expire:"

  def terminal_expire_key(expire_at_ms, terminal_key)
      when is_integer(expire_at_ms) and expire_at_ms > 0 and is_binary(terminal_key) do
    terminal_expire_prefix() <> pad_u64(expire_at_ms) <> <<0>> <> terminal_key
  end

  def terminal_expire_key(_expire_at_ms, _terminal_key), do: nil

  def terminal_by_state_key_key(state_key) when is_binary(state_key) do
    "flow-terminal-by-state:" <> state_key
  end

  def delete_state_artifacts(path, state_key) when is_binary(path) and is_binary(state_key) do
    reverse_key = terminal_by_state_key_key(state_key)

    case get(path, reverse_key) do
      {:ok, terminal_key} when is_binary(terminal_key) ->
        delete_terminal_index_entry(path, terminal_key, state_key)

      _ ->
        write_batch(path, [{:delete, state_key}, {:delete, reverse_key}])
    end
  end

  def delete_terminal_index_entry(path, terminal_key, state_key)
      when is_binary(path) and is_binary(terminal_key) do
    ops = terminal_index_delete_ops(path, terminal_key, state_key)
    write_batch(path, ops)
  end

  def terminal_index_delete_ops(path, terminal_key, state_key)
      when is_binary(path) and is_binary(terminal_key) do
    ops = [{:delete, terminal_key}]

    ops =
      if is_binary(state_key) do
        [{:delete, state_key}, {:delete, terminal_by_state_key_key(state_key)} | ops]
      else
        ops
      end

    case get(path, terminal_key) do
      {:ok, terminal_value} ->
        ops
        |> maybe_decrement_count(path, terminal_value)
        |> maybe_delete_expire_key(terminal_key, terminal_value)

      _ ->
        ops
    end
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
    case :erlang.binary_to_term(blob) do
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
    case :erlang.binary_to_term(blob) do
      count when is_integer(count) and count >= 0 -> {:ok, count}
      _ -> :error
    end
  rescue
    _ -> :error
  end

  def terminal_count(path, state_index_key) when is_binary(path) and is_binary(state_index_key) do
    count_key = terminal_count_key(state_index_key)

    case get(path, count_key) do
      {:ok, blob} ->
        case decode_count(blob) do
          {:ok, count} -> {:ok, count}
          :error -> :not_found
        end

      :not_found ->
        :not_found

      {:error, _reason} = error ->
        error
    end
  end

  def put_terminal_count(path, state_index_key, count)
      when is_binary(path) and is_binary(state_index_key) and is_integer(count) and count >= 0 do
    write_batch(path, [{:put, terminal_count_key(state_index_key), encode_count(count)}])
  end

  def sweep_expired_terminal(path, now_ms, limit)
      when is_binary(path) and is_integer(now_ms) and is_integer(limit) and limit > 0 do
    with {:ok, entries} <- prefix_entries(path, terminal_expire_prefix(), limit) do
      {ops, counts, swept} = expired_terminal_sweep_ops(path, entries, now_ms)

      count_ops =
        Enum.map(counts, fn {count_key, count} -> {:put, count_key, encode_count(count)} end)

      case write_batch(path, count_ops ++ ops) do
        :ok -> {:ok, swept}
        {:error, _reason} = error -> error
      end
    end
  end

  def sweep_expired_terminal(_path, _now_ms, _limit), do: {:ok, 0}

  def decode_terminal_index_value(blob) when is_binary(blob) do
    case :erlang.binary_to_term(blob) do
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

  def terminal_index_count_key(blob) when is_binary(blob) do
    case :erlang.binary_to_term(blob) do
      {_id, _updated_at_ms, _expire_at_ms, _state_key, count_key} when is_binary(count_key) ->
        {:ok, count_key}

      _ ->
        :missing
    end
  rescue
    _ -> :missing
  end

  defp normalize_mode(value) when value in [:off, :mirror, :write_through], do: value
  defp normalize_mode(value) when value in [false, "false", "0", "off"], do: :off
  defp normalize_mode(value) when value in [true, "true", "1", "mirror"], do: :mirror
  defp normalize_mode(value) when value in ["write_through", "write-through"], do: :write_through
  defp normalize_mode(_value), do: :off

  defp expired_terminal_sweep_ops(path, entries, now_ms) do
    Enum.reduce_while(entries, {[], %{}, 0}, fn {expire_key, expire_value},
                                                {ops, counts, swept} ->
      case terminal_expire_key_time(expire_key) do
        {:ok, expire_at_ms} when expire_at_ms > now_ms ->
          {:halt, {ops, counts, swept}}

        {:ok, _expire_at_ms} ->
          {entry_ops, counts, entry_swept} =
            expired_terminal_entry_ops(path, expire_key, expire_value, counts, now_ms)

          {:cont, {entry_ops ++ ops, counts, swept + entry_swept}}

        :error ->
          {:cont, {[{:delete, expire_key} | ops], counts, swept}}
      end
    end)
  end

  defp expired_terminal_entry_ops(path, expire_key, expire_value, counts, now_ms) do
    case decode_terminal_expire_value(expire_value) do
      {:ok, {terminal_key, state_key, count_key}} ->
        case get(path, terminal_key) do
          {:ok, terminal_value} ->
            expired_live_terminal_ops(
              path,
              expire_key,
              terminal_key,
              terminal_value,
              state_key,
              count_key,
              counts,
              now_ms
            )

          :not_found ->
            {[{:delete, expire_key}], counts, 0}

          {:error, _reason} ->
            {[], counts, 0}
        end

      :error ->
        {[{:delete, expire_key}], counts, 0}
    end
  end

  defp expired_live_terminal_ops(
         path,
         expire_key,
         terminal_key,
         terminal_value,
         state_key,
         count_key,
         counts,
         now_ms
       ) do
    case decode_terminal_index_value(terminal_value) do
      {:ok, {_id, _updated_at_ms, expire_at_ms, decoded_state_key}}
      when expire_at_ms > 0 and expire_at_ms <= now_ms ->
        state_key = decoded_state_key || state_key
        reverse_key = if is_binary(state_key), do: terminal_by_state_key_key(state_key), else: nil
        current_count = Map.get_lazy(counts, count_key, fn -> read_count_key(path, count_key) end)
        count = max(current_count - 1, 0)

        ops =
          [{:delete, expire_key}, {:delete, terminal_key}]
          |> maybe_delete_key(reverse_key)
          |> maybe_delete_key(state_key)

        {ops, Map.put(counts, count_key, count), 1}

      _ ->
        {[{:delete, expire_key}], counts, 0}
    end
  end

  defp terminal_expire_key_time(key) do
    prefix = terminal_expire_prefix()
    size = byte_size(prefix)

    if byte_size(key) > size + 21 and binary_part(key, 0, size) == prefix do
      digits = binary_part(key, size, 20)

      case Integer.parse(digits) do
        {value, ""} -> {:ok, value}
        _ -> :error
      end
    else
      :error
    end
  end

  defp maybe_delete_key(ops, key) when is_binary(key), do: [{:delete, key} | ops]
  defp maybe_delete_key(ops, _key), do: ops

  defp maybe_decrement_count(ops, path, terminal_value) do
    case terminal_index_count_key(terminal_value) do
      {:ok, count_key} ->
        count = max(read_count_key(path, count_key) - 1, 0)
        [{:put, count_key, encode_count(count)} | ops]

      :missing ->
        ops
    end
  end

  defp maybe_delete_expire_key(ops, terminal_key, terminal_value) do
    case decode_terminal_index_value(terminal_value) do
      {:ok, {_id, _updated_at_ms, expire_at_ms, _state_key}} when expire_at_ms > 0 ->
        expire_key = terminal_expire_key(expire_at_ms, terminal_key)
        [{:delete, expire_key} | ops]

      _ ->
        ops
    end
  end

  defp read_count_key(path, count_key) do
    case get(path, count_key) do
      {:ok, blob} ->
        case decode_count(blob) do
          {:ok, count} -> count
          :error -> 0
        end

      _ ->
        0
    end
  end

  defp pad_u64(value) do
    value
    |> Integer.to_string()
    |> String.pad_leading(20, "0")
  end
end
