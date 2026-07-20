defmodule Ferricstore.Flow.LMDB.Access do
  @moduledoc false

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Flow.Keys

  @max_bounded_get_keys 4_096
  @max_bounded_get_key_bytes 8 * 1_024 * 1_024
  @max_lmdb_key_bytes 511
  @physical_state_key_prefix <<0, "flsk:1:">>

  def map_size, do: Ferricstore.Flow.LMDB.map_size()

  def get(path, key) when is_binary(path) and is_binary(key) do
    if Ferricstore.FS.dir?(path),
      do: NIF.lmdb_get(path, physical_key(key), map_size()),
      else: :not_found
  end

  def get_many(_path, []), do: {:ok, []}

  def get_many(path, keys) when is_binary(path) and is_list(keys) do
    with {:ok, key_count, compact?} <- binary_key_count(keys) do
      result =
        if Ferricstore.FS.dir?(path) do
          NIF.lmdb_get_many(path, physical_keys(keys, compact?), map_size())
        else
          {:ok, List.duplicate(:not_found, key_count)}
        end

      normalize_get_many_result(result, key_count)
    end
  end

  def get_many_bounded(_path, [], max_bytes) when is_integer(max_bytes) and max_bytes > 0,
    do: {:ok, [], 0}

  def get_many_bounded(path, keys, max_bytes)
      when is_binary(path) and is_list(keys) and is_integer(max_bytes) and max_bytes > 0 do
    with {:ok, key_count, compact?} <- bounded_binary_key_count(keys, 0, 0, false) do
      result =
        if Ferricstore.FS.dir?(path) do
          NIF.lmdb_get_many_bounded(
            path,
            physical_keys(keys, compact?),
            max_bytes,
            map_size()
          )
        else
          {:ok, List.duplicate(:not_found, key_count), 0}
        end

      normalize_get_many_bounded_result(result, key_count, max_bytes)
    end
  end

  def get_many_bounded(_path, _keys, _max_bytes), do: {:error, :badarg}

  defp bounded_binary_key_count([], count, _bytes, compact?),
    do: {:ok, count, compact?}

  defp bounded_binary_key_count([key | keys], count, bytes, compact?) when is_binary(key) do
    next_count = count + 1
    next_bytes = bytes + byte_size(key)

    if next_count <= @max_bounded_get_keys and next_bytes <= @max_bounded_get_key_bytes do
      bounded_binary_key_count(
        keys,
        next_count,
        next_bytes,
        compact? or compact_state_key?(key)
      )
    else
      {:error, :batch_key_budget_exceeded}
    end
  end

  defp bounded_binary_key_count(_invalid, _count, _bytes, _compact?), do: {:error, :badarg}

  defp binary_key_count(keys), do: binary_key_count(keys, 0, false)

  defp binary_key_count([], count, compact?), do: {:ok, count, compact?}

  defp binary_key_count([key | keys], count, compact?) when is_binary(key),
    do: binary_key_count(keys, count + 1, compact? or compact_state_key?(key))

  defp binary_key_count(_invalid, _count, _compact?), do: {:error, :badarg}

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

  defp normalize_get_many_bounded_result(
         {:ok, results, bytes},
         expected_count,
         max_bytes
       )
       when is_list(results) and is_integer(bytes) and bytes >= 0 and bytes <= max_bytes do
    with :ok <- validate_get_many_results(results, expected_count, 0),
         ^bytes <- result_value_bytes(results, 0) do
      {:ok, results, bytes}
    else
      {:error, _reason} = error -> error
      _mismatched_bytes -> {:error, :invalid_batch_value_bytes}
    end
  end

  defp normalize_get_many_bounded_result({:error, _reason} = error, _count, _max_bytes),
    do: error

  defp normalize_get_many_bounded_result(invalid, _count, _max_bytes),
    do: {:error, {:invalid_batch_envelope, invalid}}

  defp result_value_bytes([], bytes), do: bytes

  defp result_value_bytes([:not_found | results], bytes),
    do: result_value_bytes(results, bytes)

  defp result_value_bytes([{:ok, value} | results], bytes),
    do: result_value_bytes(results, bytes + byte_size(value))

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
    with {:ok, physical_ops, logical_keys} <- physical_ops(ops) do
      path
      |> NIF.lmdb_write_batch(physical_ops, map_size())
      |> translate_compare_failure(logical_keys)
    end
  end

  def write_batch_with_originals(_path, []), do: {:ok, []}

  def write_batch_with_originals(path, ops) when is_binary(path) and is_list(ops) do
    with {:ok, physical_ops, logical_keys} <- physical_ops(ops) do
      path
      |> NIF.lmdb_write_batch_with_originals(physical_ops, map_size())
      |> translate_originals(logical_keys)
    end
  end

  defp physical_keys(keys, false), do: keys
  defp physical_keys(keys, true), do: Enum.map(keys, &physical_key/1)

  defp physical_ops(ops) do
    if Enum.any?(ops, &compact_op?/1) do
      normalize_physical_ops(ops, [], %{})
    else
      {:ok, ops, %{}}
    end
  end

  defp normalize_physical_ops([], reversed, logical_keys),
    do: {:ok, Enum.reverse(reversed), logical_keys}

  defp normalize_physical_ops([op | ops], reversed, logical_keys) do
    case op_key(op) do
      {:ok, logical_key} ->
        physical_key = physical_key(logical_key)

        case logical_keys do
          %{^physical_key => existing} when existing != logical_key ->
            {:error, :physical_state_key_collision}

          _other ->
            normalize_physical_ops(
              ops,
              [replace_op_key(op, physical_key) | reversed],
              Map.put(logical_keys, physical_key, logical_key)
            )
        end

      :error ->
        normalize_physical_ops(ops, [op | reversed], logical_keys)
    end
  end

  defp compact_op?(op) do
    case op_key(op) do
      {:ok, key} -> compact_state_key?(key)
      :error -> false
    end
  end

  defp op_key({:put, key, _value}) when is_binary(key), do: {:ok, key}
  defp op_key({:put_new, key, _value}) when is_binary(key), do: {:ok, key}
  defp op_key({:delete, key}) when is_binary(key), do: {:ok, key}
  defp op_key({:compare, key, _value}) when is_binary(key), do: {:ok, key}
  defp op_key({:compare_missing, key}) when is_binary(key), do: {:ok, key}
  defp op_key(_invalid), do: :error

  defp replace_op_key({:put, _key, value}, key), do: {:put, key, value}
  defp replace_op_key({:put_new, _key, value}, key), do: {:put_new, key, value}
  defp replace_op_key({:delete, _key}, key), do: {:delete, key}
  defp replace_op_key({:compare, _key, value}, key), do: {:compare, key, value}
  defp replace_op_key({:compare_missing, _key}, key), do: {:compare_missing, key}

  defp translate_compare_failure(
         {:error, {:compare_failed, physical_key}},
         logical_keys
       )
       when is_binary(physical_key),
       do: {:error, {:compare_failed, Map.get(logical_keys, physical_key, physical_key)}}

  defp translate_compare_failure(result, _logical_keys), do: result

  defp translate_originals({:ok, originals}, logical_keys) when is_list(originals) do
    {:ok,
     Enum.map(originals, fn
       {physical_key, original} when is_binary(physical_key) ->
         {Map.get(logical_keys, physical_key, physical_key), original}

       invalid ->
         invalid
     end)}
  end

  defp translate_originals(
         {:error, {:compare_failed, physical_key}},
         logical_keys
       )
       when is_binary(physical_key),
       do: {:error, {:compare_failed, Map.get(logical_keys, physical_key, physical_key)}}

  defp translate_originals(result, _logical_keys), do: result

  defp physical_key(key) do
    if compact_state_key?(key),
      do: @physical_state_key_prefix <> :crypto.hash(:sha256, key),
      else: key
  end

  defp compact_state_key?(key) do
    byte_size(key) > @max_lmdb_key_bytes and
      byte_size(key) <= Ferricstore.Store.Router.max_key_size() and Keys.state_key?(key)
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
