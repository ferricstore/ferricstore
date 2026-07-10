defmodule Ferricstore.Flow.LMDB.Access do
  @moduledoc false

  alias Ferricstore.Bitcask.NIF

  @default_map_size 16 * 1024 * 1024 * 1024

  def map_size do
    Application.get_env(:ferricstore, :flow_lmdb_map_size, @default_map_size)
  end

  def get(path, key) when is_binary(path) and is_binary(key) do
    if Ferricstore.FS.dir?(path), do: NIF.lmdb_get(path, key, map_size()), else: :not_found
  end

  def get_many(_path, []), do: {:ok, []}

  def get_many(path, keys) when is_binary(path) and is_list(keys) do
    if Ferricstore.FS.dir?(path) do
      NIF.lmdb_get_many(path, keys, map_size())
    else
      {:ok, Enum.map(keys, fn _key -> :not_found end)}
    end
  end

  def write_batch(_path, []), do: :ok

  def write_batch(path, ops) when is_binary(path) and is_list(ops) do
    NIF.lmdb_write_batch(path, ops, map_size())
  end

  def prefix_entries(path, prefix, limit)
      when is_binary(path) and is_binary(prefix) and is_integer(limit) and limit >= 0 do
    if Ferricstore.FS.dir?(path),
      do: NIF.lmdb_prefix_entries(path, prefix, limit, map_size()),
      else: {:ok, []}
  end

  def prefix_entries_after(path, prefix, after_key, limit)
      when is_binary(path) and is_binary(prefix) and is_binary(after_key) and
             is_integer(limit) and limit >= 0 do
    if Ferricstore.FS.dir?(path),
      do: NIF.lmdb_prefix_entries_after(path, prefix, after_key, limit, map_size()),
      else: {:ok, []}
  end

  def prefix_entries_after_bounded(path, prefix, after_key, max_items, max_bytes)
      when is_binary(path) and is_binary(prefix) and is_binary(after_key) and
             is_integer(max_items) and max_items >= 0 and is_integer(max_bytes) and
             max_bytes >= 0 do
    if Ferricstore.FS.dir?(path) do
      NIF.lmdb_prefix_entries_after_bounded(
        path,
        prefix,
        after_key,
        max_items,
        max_bytes,
        map_size()
      )
    else
      {:ok, []}
    end
  end
end
