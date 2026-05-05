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

  def terminal_by_state_key_key(state_key) when is_binary(state_key) do
    "flow-terminal-by-state:" <> state_key
  end

  def delete_state_artifacts(path, state_key) when is_binary(path) and is_binary(state_key) do
    reverse_key = terminal_by_state_key_key(state_key)

    ops =
      case get(path, reverse_key) do
        {:ok, terminal_key} when is_binary(terminal_key) ->
          [{:delete, state_key}, {:delete, reverse_key}, {:delete, terminal_key}]

        _ ->
          [{:delete, state_key}, {:delete, reverse_key}]
      end

    write_batch(path, ops)
  end

  def delete_terminal_index_entry(path, terminal_key, state_key)
      when is_binary(path) and is_binary(terminal_key) do
    ops = [{:delete, terminal_key}]

    ops =
      if is_binary(state_key) do
        [{:delete, state_key}, {:delete, terminal_by_state_key_key(state_key)} | ops]
      else
        ops
      end

    write_batch(path, ops)
  end

  def encode_terminal_index_value(id, updated_at_ms, expire_at_ms \\ 0, state_key \\ nil) do
    :erlang.term_to_binary({id, updated_at_ms, expire_at_ms, state_key})
  end

  def decode_terminal_index_value(blob) when is_binary(blob) do
    case :erlang.binary_to_term(blob) do
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

  defp normalize_mode(value) when value in [:off, :mirror, :write_through], do: value
  defp normalize_mode(value) when value in [false, "false", "0", "off"], do: :off
  defp normalize_mode(value) when value in [true, "true", "1", "mirror"], do: :mirror
  defp normalize_mode(value) when value in ["write_through", "write-through"], do: :write_through
  defp normalize_mode(_value), do: :off
end
