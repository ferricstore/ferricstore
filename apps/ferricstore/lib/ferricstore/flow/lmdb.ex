defmodule Ferricstore.Flow.LMDB do
  @moduledoc false

  alias Ferricstore.Bitcask.NIF

  @default_map_size 64 * 1024 * 1024 * 1024

  def enabled? do
    Application.get_env(:ferricstore, :flow_lmdb_enabled, false) in [true, "true", "1"]
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
end
