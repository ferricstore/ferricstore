defmodule Ferricstore.Flow.LMDB.Access do
  @moduledoc false

  alias Ferricstore.Bitcask.NIF

  def map_size, do: Ferricstore.Flow.LMDB.map_size()

  def get(path, key) when is_binary(path) and is_binary(key) do
    if Ferricstore.FS.dir?(path), do: NIF.lmdb_get(path, key, map_size()), else: :not_found
  end

  def get_many(_path, []), do: {:ok, []}

  def get_many(path, keys) when is_binary(path) and is_list(keys) do
    with {:ok, key_count} <- binary_key_count(keys) do
      result =
        if Ferricstore.FS.dir?(path) do
          NIF.lmdb_get_many(path, keys, map_size())
        else
          {:ok, List.duplicate(:not_found, key_count)}
        end

      normalize_get_many_result(result, key_count)
    end
  end

  defp binary_key_count(keys), do: binary_key_count(keys, 0)

  defp binary_key_count([], count), do: {:ok, count}

  defp binary_key_count([key | keys], count) when is_binary(key),
    do: binary_key_count(keys, count + 1)

  defp binary_key_count(_invalid, _count), do: {:error, :badarg}

  @doc false
  def __normalize_get_many_result_for_test__(result, expected_count),
    do: normalize_get_many_result(result, expected_count)

  defp normalize_get_many_result({:ok, results}, expected_count)
       when is_list(results) and is_integer(expected_count) and expected_count >= 0 do
    case validate_get_many_results(results, expected_count, 0) do
      :ok -> {:ok, results}
      {:error, _reason} = error -> error
    end
  end

  defp normalize_get_many_result({:ok, invalid}, _expected_count),
    do: {:error, {:invalid_batch_envelope, {:ok, invalid}}}

  defp normalize_get_many_result({:error, _reason} = error, _expected_count), do: error

  defp normalize_get_many_result(invalid, _expected_count),
    do: {:error, {:invalid_batch_envelope, invalid}}

  defp validate_get_many_results([], expected_count, expected_count), do: :ok

  defp validate_get_many_results([], expected_count, seen),
    do: {:error, {:batch_result_mismatch, expected_count, seen}}

  defp validate_get_many_results(results, expected_count, seen) when seen >= expected_count,
    do: {:error, {:batch_result_mismatch, expected_count, seen + length(results)}}

  defp validate_get_many_results([:not_found | results], expected_count, seen),
    do: validate_get_many_results(results, expected_count, seen + 1)

  defp validate_get_many_results([{:ok, value} | results], expected_count, seen)
       when is_binary(value),
       do: validate_get_many_results(results, expected_count, seen + 1)

  defp validate_get_many_results([invalid | _results], _expected_count, seen),
    do: {:error, {:invalid_batch_result, seen, invalid}}

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
