defmodule Ferricstore.Flow.Query.CompositeProjection do
  @moduledoc false

  alias Ferricstore.Flow.LMDB
  alias Ferricstore.Flow.Query.{CompositeCounter, CompositeIndex, IndexDefinition}

  @max_u64 0xFFFF_FFFF_FFFF_FFFF

  @type cache :: %{
          reverse_values: %{optional(binary()) => {binary() | nil, [binary()]}},
          counter_values: %{
            optional(binary()) => {binary(), binary() | nil, non_neg_integer()}
          }
        }

  @spec new_cache() :: cache()
  def new_cache, do: %{reverse_values: %{}, counter_values: %{}}

  @spec reconcile(
          binary(),
          binary(),
          map(),
          non_neg_integer(),
          [IndexDefinition.t()],
          cache()
        ) :: {:ok, [tuple()], cache()} | {:error, atom() | term()}
  def reconcile(_path, _state_key, _record, _expire_at_ms, [], cache),
    do: {:ok, [], cache}

  def reconcile(path, state_key, record, expire_at_ms, definitions, cache)
      when is_binary(path) and is_binary(state_key) and state_key != "" and is_map(record) and
             is_list(definitions) and is_map(cache) do
    with {:ok, entries} <- projected_entries(definitions, record, state_key, expire_at_ms),
         {:ok, old_blob, old_keys, cache} <- reverse_value(path, state_key, cache) do
      reconcile_entries(path, state_key, entries, old_blob, old_keys, definitions, cache)
    end
  end

  def reconcile(_path, _state_key, _record, _expire_at_ms, _definitions, _cache),
    do: {:error, :invalid_composite_projection}

  @spec remove(binary(), binary(), [IndexDefinition.t()], cache()) ::
          {:ok, [tuple()], cache()} | {:error, atom() | term()}
  def remove(path, state_key, definitions, cache)
      when is_binary(path) and is_binary(state_key) and state_key != "" and
             is_list(definitions) and is_map(cache) do
    with {:ok, old_blob, old_keys, cache} <- reverse_value(path, state_key, cache) do
      reconcile_entries(path, state_key, [], old_blob, old_keys, definitions, cache)
    end
  end

  def remove(_path, _state_key, _definitions, _cache),
    do: {:error, :invalid_composite_projection}

  defp projected_entries(definitions, record, state_key, expire_at_ms) do
    Enum.reduce_while(definitions, {:ok, []}, fn definition, {:ok, acc} ->
      case CompositeIndex.entries(definition, record, state_key, expire_at_ms) do
        {:ok, entries} ->
          if length(acc) + length(entries) <= CompositeIndex.max_entries_per_record(),
            do: {:cont, {:ok, :lists.reverse(entries, acc)}},
            else: {:halt, {:error, :composite_projection_too_large}}

        {:error, :unscoped_record} ->
          {:cont, {:ok, acc}}

        {:error, _reason} = error ->
          {:halt, error}
      end
    end)
    |> case do
      {:ok, reversed} -> {:ok, Enum.reverse(reversed)}
      {:error, _reason} = error -> error
    end
  end

  defp reverse_value(path, state_key, cache) do
    reverse_values = Map.get(cache, :reverse_values, %{})

    case Map.fetch(reverse_values, state_key) do
      {:ok, {blob, keys}} ->
        {:ok, blob, keys, cache}

      :error ->
        reverse_key = CompositeIndex.reverse_key(state_key)

        case LMDB.get(path, reverse_key) do
          :not_found ->
            {:ok, nil, [], put_reverse_cache(cache, state_key, nil, [])}

          {:ok, blob} when is_binary(blob) ->
            case CompositeIndex.decode_reverse_value(blob, state_key) do
              {:ok, keys} ->
                {:ok, blob, keys, put_reverse_cache(cache, state_key, blob, keys)}

              :error ->
                {:error, :invalid_composite_reverse}
            end

          {:error, _reason} = error ->
            error

          _invalid ->
            {:error, :invalid_composite_reverse_read}
        end
    end
  end

  defp reconcile_entries(path, state_key, entries, old_blob, old_keys, definitions, cache) do
    reverse_key = CompositeIndex.reverse_key(state_key)
    next_keys = entries |> Enum.map(& &1.key) |> Enum.uniq() |> Enum.sort()
    old_set = MapSet.new(old_keys)
    next_set = MapSet.new(next_keys)

    guard_op =
      if is_binary(old_blob),
        do: {:compare, reverse_key, old_blob},
        else: {:compare_missing, reverse_key}

    stale_ops =
      old_set
      |> MapSet.difference(next_set)
      |> Enum.sort()
      |> Enum.map(&{:delete, &1})

    put_ops = Enum.map(entries, &{:put, &1.key, &1.value})

    {reverse_op, next_blob} =
      case next_keys do
        [] ->
          {{:delete, reverse_key}, nil}

        keys ->
          blob = CompositeIndex.encode_reverse_value(state_key, keys)
          {{:put, reverse_key, blob}, blob}
      end

    with {:ok, counter_ops, cache} <-
           reconcile_counters(path, definitions, old_keys, next_keys, cache) do
      ops = [guard_op | counter_ops ++ stale_ops ++ put_ops ++ [reverse_op]]
      {:ok, ops, put_reverse_cache(cache, state_key, next_blob, next_keys)}
    end
  end

  defp reconcile_counters(path, definitions, old_keys, next_keys, cache) do
    with {:ok, old_prefixes} <- CompositeCounter.prefixes_for_keys(definitions, old_keys),
         {:ok, next_prefixes} <- CompositeCounter.prefixes_for_keys(definitions, next_keys) do
      changes =
        old_prefixes
        |> MapSet.union(next_prefixes)
        |> Enum.map(fn {definition, prefix} ->
          delta =
            membership_delta(old_prefixes, next_prefixes, {definition, prefix})

          {definition, prefix, delta}
        end)
        |> Enum.reject(fn {_definition, _prefix, delta} -> delta == 0 end)
        |> Enum.sort_by(fn {definition, prefix, _delta} ->
          {definition.id, definition.version, prefix}
        end)

      Enum.reduce_while(changes, {:ok, [], cache}, fn change, {:ok, acc, cache} ->
        case counter_change(path, change, cache) do
          {:ok, ops, cache} -> {:cont, {:ok, :lists.reverse(ops, acc), cache}}
          {:error, _reason} = error -> {:halt, error}
        end
      end)
      |> case do
        {:ok, reversed, cache} -> {:ok, Enum.reverse(reversed), cache}
        {:error, _reason} = error -> error
      end
    end
  end

  defp membership_delta(old_prefixes, next_prefixes, item) do
    old = if MapSet.member?(old_prefixes, item), do: 1, else: 0
    next = if MapSet.member?(next_prefixes, item), do: 1, else: 0
    next - old
  end

  defp counter_change(path, {definition, prefix, delta}, cache) when delta in [-1, 1] do
    counter_key = CompositeCounter.key(definition, prefix)

    with {:ok, old_blob, count, cache} <-
           counter_value(path, counter_key, prefix, cache),
         {:ok, next_count} <- checked_counter_delta(count, delta) do
      guard =
        if is_binary(old_blob),
          do: {:compare, counter_key, old_blob},
          else: {:compare_missing, counter_key}

      {write, next_blob} =
        if next_count == 0 do
          {{:delete, counter_key}, nil}
        else
          blob = CompositeCounter.encode_value(prefix, next_count)
          {{:put, counter_key, blob}, blob}
        end

      cache = put_counter_cache(cache, counter_key, prefix, next_blob, next_count)
      {:ok, [guard, write], cache}
    end
  end

  defp counter_value(path, counter_key, prefix, cache) do
    values = Map.get(cache, :counter_values, %{})

    case Map.fetch(values, counter_key) do
      {:ok, {^prefix, blob, count}} ->
        {:ok, blob, count, cache}

      {:ok, {_other_prefix, _blob, _count}} ->
        {:error, :composite_counter_collision}

      :error ->
        case LMDB.get(path, counter_key) do
          :not_found ->
            {:ok, nil, 0, put_counter_cache(cache, counter_key, prefix, nil, 0)}

          {:ok, blob} when is_binary(blob) ->
            case CompositeCounter.decode_value(blob, prefix) do
              {:ok, count} ->
                {:ok, blob, count, put_counter_cache(cache, counter_key, prefix, blob, count)}

              :error ->
                {:error, :invalid_composite_counter}
            end

          {:error, _reason} = error ->
            error

          _invalid ->
            {:error, :invalid_composite_counter_read}
        end
    end
  end

  defp checked_counter_delta(0, -1), do: {:error, :composite_counter_underflow}
  defp checked_counter_delta(@max_u64, 1), do: {:error, :composite_counter_overflow}
  defp checked_counter_delta(count, delta), do: {:ok, count + delta}

  defp put_counter_cache(cache, key, prefix, blob, count) do
    values = Map.get(cache, :counter_values, %{})
    Map.put(cache, :counter_values, Map.put(values, key, {prefix, blob, count}))
  end

  defp put_reverse_cache(cache, state_key, blob, keys) do
    reverse_values = Map.get(cache, :reverse_values, %{})
    Map.put(cache, :reverse_values, Map.put(reverse_values, state_key, {blob, keys}))
  end
end
