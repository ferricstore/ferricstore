defmodule Ferricstore.Flow.Query.CompositeProjection do
  @moduledoc false

  alias Ferricstore.Flow.{Keys, LMDB}
  alias Ferricstore.Flow.Query.{CompositeCounter, CompositeIndex, IndexDefinition, Limits}

  @max_u64 0xFFFF_FFFF_FFFF_FFFF
  @max_prefetch_records Limits.max_projection_page_records()
  @max_state_key_bytes Limits.max_state_key_bytes()

  @type cache :: %{
          reverse_values: %{
            optional(binary()) => {binary() | nil, [binary()], non_neg_integer()}
          },
          counter_values: %{
            optional(binary()) =>
              {binary(), binary() | nil, non_neg_integer(), non_neg_integer(), non_neg_integer()}
          }
        }

  @spec new_cache() :: cache()
  def new_cache, do: %{reverse_values: %{}, counter_values: %{}}

  @doc false
  @spec prefetch_reverse_values(binary(), [binary()], cache()) ::
          {:ok, cache()} | {:error, term()}
  def prefetch_reverse_values(path, state_keys, cache)
      when is_binary(path) and is_list(state_keys) and is_map(cache) do
    cond do
      length(state_keys) > @max_prefetch_records ->
        {:error, :composite_reverse_prefetch_too_large}

      not valid_reverse_prefetch_keys?(state_keys) ->
        {:error, :invalid_composite_reverse_prefetch}

      true ->
        prefetch_missing_reverse_values(path, state_keys, cache)
    end
  end

  def prefetch_reverse_values(_path, _state_keys, _cache),
    do: {:error, :invalid_composite_reverse_prefetch}

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
         {:ok, old_blob, old_keys, old_expire_at_ms, cache} <-
           reverse_value(path, state_key, cache) do
      next_expire_at_ms = if entries == [], do: 0, else: expire_at_ms

      reconcile_entries(
        path,
        state_key,
        entries,
        [],
        old_blob,
        old_keys,
        old_expire_at_ms,
        next_expire_at_ms,
        definitions,
        cache
      )
    end
  end

  def reconcile(_path, _state_key, _record, _expire_at_ms, _definitions, _cache),
    do: {:error, :invalid_composite_projection}

  @doc """
  Reconciles only the supplied definitions while preserving rows owned by
  other definitions in the shared reverse record.

  Online backfills use this contract because a build contains only new
  catalog entries. Live projection uses `reconcile/6` with the complete
  projection set.
  """
  @spec reconcile_subset(
          binary(),
          binary(),
          map(),
          non_neg_integer(),
          [IndexDefinition.t()],
          [IndexDefinition.t()],
          cache()
        ) :: {:ok, [tuple()], cache()} | {:error, atom() | term()}
  def reconcile_subset(
        path,
        state_key,
        record,
        expire_at_ms,
        definitions,
        projection_definitions,
        cache
      )
      when is_binary(path) and is_binary(state_key) and state_key != "" and is_map(record) and
             is_list(definitions) and is_list(projection_definitions) and is_map(cache) do
    with {:ok, entries} <- projected_entries(definitions, record, state_key, expire_at_ms),
         {:ok, old_blob, old_keys, old_expire_at_ms, cache} <-
           reverse_value(path, state_key, cache) do
      preserved_keys = preserved_keys(old_keys, definitions, projection_definitions)
      next_expire_at_ms = if entries == [] and preserved_keys == [], do: 0, else: expire_at_ms

      reconcile_entries(
        path,
        state_key,
        entries,
        preserved_keys,
        old_blob,
        old_keys,
        old_expire_at_ms,
        next_expire_at_ms,
        definitions,
        cache
      )
    end
  end

  def reconcile_subset(
        _path,
        _state_key,
        _record,
        _expire_at_ms,
        _definitions,
        _projection_definitions,
        _cache
      ),
      do: {:error, :invalid_composite_projection}

  @spec remove(binary(), binary(), [IndexDefinition.t()], cache()) ::
          {:ok, [tuple()], cache()} | {:error, atom() | term()}
  def remove(path, state_key, definitions, cache)
      when is_binary(path) and is_binary(state_key) and state_key != "" and
             is_list(definitions) and is_map(cache) do
    with {:ok, old_blob, old_keys, old_expire_at_ms, cache} <-
           reverse_value(path, state_key, cache) do
      reconcile_entries(
        path,
        state_key,
        [],
        [],
        old_blob,
        old_keys,
        old_expire_at_ms,
        0,
        definitions,
        cache
      )
    end
  end

  def remove(_path, _state_key, _definitions, _cache),
    do: {:error, :invalid_composite_projection}

  @doc false
  @spec remove_subset(
          binary(),
          binary(),
          [IndexDefinition.t()],
          [IndexDefinition.t()],
          cache()
        ) ::
          {:ok, [tuple()], cache()} | {:error, atom() | term()}
  def remove_subset(path, state_key, definitions, projection_definitions, cache)
      when is_binary(path) and is_binary(state_key) and state_key != "" and
             is_list(definitions) and is_list(projection_definitions) and is_map(cache) do
    with {:ok, old_blob, old_keys, old_expire_at_ms, cache} <-
           reverse_value(path, state_key, cache) do
      preserved_keys = preserved_keys(old_keys, definitions, projection_definitions)
      next_expire_at_ms = if preserved_keys == [], do: 0, else: old_expire_at_ms

      reconcile_entries(
        path,
        state_key,
        [],
        preserved_keys,
        old_blob,
        old_keys,
        old_expire_at_ms,
        next_expire_at_ms,
        definitions,
        cache
      )
    end
  end

  def remove_subset(_path, _state_key, _definitions, _projection_definitions, _cache),
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
      {:ok, {blob, keys, expire_at_ms}} ->
        {:ok, blob, keys, expire_at_ms, cache}

      :error ->
        reverse_key = CompositeIndex.reverse_key(state_key)

        case LMDB.get(path, reverse_key) do
          :not_found ->
            {:ok, nil, [], 0, put_reverse_cache(cache, state_key, nil, [], 0)}

          {:ok, blob} when is_binary(blob) ->
            case CompositeIndex.decode_reverse_state(blob, state_key) do
              {:ok, %{keys: keys, expire_at_ms: expire_at_ms}} ->
                {:ok, blob, keys, expire_at_ms,
                 put_reverse_cache(cache, state_key, blob, keys, expire_at_ms)}

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

  defp valid_reverse_prefetch_keys?(state_keys) do
    length(state_keys) == length(Enum.uniq(state_keys)) and
      Enum.all?(state_keys, fn state_key ->
        is_binary(state_key) and state_key != "" and byte_size(state_key) <= @max_state_key_bytes and
          match?({:ok, _id}, Keys.run_id_from_state_key(state_key))
      end)
  end

  defp prefetch_missing_reverse_values(path, state_keys, cache) do
    case Map.get(cache, :reverse_values) do
      reverse_values when is_map(reverse_values) ->
        with {:ok, missing} <- missing_reverse_values(state_keys, reverse_values) do
          fetch_reverse_values(path, missing, cache)
        end

      _invalid ->
        {:error, :invalid_composite_projection_cache}
    end
  end

  defp missing_reverse_values(state_keys, reverse_values) do
    Enum.reduce_while(state_keys, {:ok, []}, fn state_key, {:ok, missing} ->
      case Map.fetch(reverse_values, state_key) do
        :error ->
          {:cont, {:ok, [state_key | missing]}}

        {:ok, {nil, [], 0}} ->
          {:cont, {:ok, missing}}

        {:ok, {blob, keys, expire_at_ms}}
        when is_binary(blob) and is_list(keys) and is_integer(expire_at_ms) and
               expire_at_ms >= 0 ->
          case CompositeIndex.decode_reverse_state(blob, state_key) do
            {:ok, %{keys: ^keys, expire_at_ms: ^expire_at_ms}} ->
              {:cont, {:ok, missing}}

            _invalid ->
              {:halt, {:error, :invalid_composite_projection_cache}}
          end

        _invalid ->
          {:halt, {:error, :invalid_composite_projection_cache}}
      end
    end)
    |> case do
      {:ok, reversed} -> {:ok, Enum.reverse(reversed)}
      {:error, _reason} = error -> error
    end
  end

  defp fetch_reverse_values(_path, [], cache), do: {:ok, cache}

  defp fetch_reverse_values(path, state_keys, cache) do
    reverse_keys = Enum.map(state_keys, &CompositeIndex.reverse_key/1)
    max_bytes = length(state_keys) * CompositeIndex.max_reverse_value_bytes()

    case LMDB.get_many_bounded(path, reverse_keys, max_bytes) do
      {:ok, values, _read_bytes} ->
        cache_prefetched_reverse_values(state_keys, values, cache)

      {:error, :batch_value_budget_exceeded} ->
        {:error, :invalid_composite_reverse}

      {:error, _reason} = error ->
        error

      _invalid ->
        {:error, :invalid_composite_reverse_read}
    end
  end

  defp cache_prefetched_reverse_values([], [], cache), do: {:ok, cache}

  defp cache_prefetched_reverse_values([state_key | state_keys], [:not_found | values], cache) do
    cache_prefetched_reverse_values(
      state_keys,
      values,
      put_reverse_cache(cache, state_key, nil, [], 0)
    )
  end

  defp cache_prefetched_reverse_values(
         [state_key | state_keys],
         [{:ok, blob} | values],
         cache
       )
       when is_binary(blob) do
    case CompositeIndex.decode_reverse_state(blob, state_key) do
      {:ok, %{keys: keys, expire_at_ms: expire_at_ms}} ->
        cache_prefetched_reverse_values(
          state_keys,
          values,
          put_reverse_cache(cache, state_key, blob, keys, expire_at_ms)
        )

      :error ->
        {:error, :invalid_composite_reverse}
    end
  end

  defp cache_prefetched_reverse_values(_state_keys, _values, _cache),
    do: {:error, :invalid_composite_reverse_read}

  defp reconcile_entries(
         path,
         state_key,
         entries,
         preserved_keys,
         old_blob,
         old_keys,
         old_expire_at_ms,
         next_expire_at_ms,
         definitions,
         cache
       ) do
    reverse_key = CompositeIndex.reverse_key(state_key)
    next_keys = (preserved_keys ++ Enum.map(entries, & &1.key)) |> Enum.uniq() |> Enum.sort()
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

    with {:ok, reverse_op, next_blob} <-
           reverse_write(reverse_key, state_key, next_keys, next_expire_at_ms),
         {:ok, counter_ops, cache} <-
           reconcile_counters(
             path,
             definitions,
             old_keys,
             next_keys,
             old_expire_at_ms,
             next_expire_at_ms,
             cache
           ) do
      ops = [guard_op | counter_ops ++ stale_ops ++ put_ops ++ [reverse_op]]

      {:ok, ops, put_reverse_cache(cache, state_key, next_blob, next_keys, next_expire_at_ms)}
    end
  end

  defp reverse_write(reverse_key, _state_key, [], _expire_at_ms),
    do: {:ok, {:delete, reverse_key}, nil}

  defp reverse_write(reverse_key, state_key, keys, expire_at_ms) do
    if length(keys) <= CompositeIndex.max_entries_per_record() do
      blob = CompositeIndex.encode_reverse_value(state_key, keys, expire_at_ms)
      {:ok, {:put, reverse_key, blob}, blob}
    else
      {:error, :composite_projection_too_large}
    end
  end

  defp preserved_keys(keys, definitions, projection_definitions) do
    managed = definitions |> Enum.map(&IndexDefinition.storage_prefix/1) |> MapSet.new()

    preserved =
      projection_definitions
      |> Enum.map(&IndexDefinition.storage_prefix/1)
      |> Enum.reject(&MapSet.member?(managed, &1))

    Enum.filter(keys, fn key ->
      Enum.any?(preserved, &String.starts_with?(key, &1))
    end)
  end

  defp reconcile_counters(
         path,
         definitions,
         old_keys,
         next_keys,
         old_expire_at_ms,
         next_expire_at_ms,
         cache
       ) do
    with {:ok, old_prefix_counts} <-
           CompositeCounter.prefix_counts_for_keys(definitions, old_keys),
         {:ok, next_prefix_counts} <-
           CompositeCounter.prefix_counts_for_keys(definitions, next_keys) do
      changes =
        old_prefix_counts
        |> Map.keys()
        |> MapSet.new()
        |> MapSet.union(next_prefix_counts |> Map.keys() |> MapSet.new())
        |> Enum.map(fn {definition, prefix} ->
          item = {definition, prefix}
          count_delta = membership_delta(old_prefix_counts, next_prefix_counts, item)

          expiring_delta =
            expiring_membership_delta(
              old_prefix_counts,
              next_prefix_counts,
              item,
              old_expire_at_ms,
              next_expire_at_ms
            )

          physical_delta =
            Map.get(next_prefix_counts, item, 0) - Map.get(old_prefix_counts, item, 0)

          {definition, prefix, count_delta, expiring_delta, physical_delta}
        end)
        |> Enum.reject(fn {_definition, _prefix, count_delta, expiring_delta, physical_delta} ->
          count_delta == 0 and expiring_delta == 0 and physical_delta == 0
        end)
        |> Enum.sort_by(fn {definition, prefix, _count_delta, _expiring_delta, _physical_delta} ->
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
    old = if Map.has_key?(old_prefixes, item), do: 1, else: 0
    next = if Map.has_key?(next_prefixes, item), do: 1, else: 0
    next - old
  end

  defp expiring_membership_delta(
         old_prefixes,
         next_prefixes,
         item,
         old_expire_at_ms,
         next_expire_at_ms
       ) do
    old = if old_expire_at_ms > 0 and Map.has_key?(old_prefixes, item), do: 1, else: 0
    next = if next_expire_at_ms > 0 and Map.has_key?(next_prefixes, item), do: 1, else: 0
    next - old
  end

  defp counter_change(
         path,
         {definition, prefix, count_delta, expiring_delta, physical_delta},
         cache
       )
       when count_delta in [-1, 0, 1] and expiring_delta in [-1, 0, 1] and
              is_integer(physical_delta) do
    counter_key = CompositeCounter.key(definition, prefix)

    with {:ok, old_blob, count, expiring_count, physical_count, cache} <-
           counter_value(path, counter_key, prefix, cache),
         {:ok, next_count, next_expiring_count, next_physical_count} <-
           checked_counter_deltas(
             count,
             expiring_count,
             physical_count,
             count_delta,
             expiring_delta,
             physical_delta
           ) do
      guard =
        if is_binary(old_blob),
          do: {:compare, counter_key, old_blob},
          else: {:compare_missing, counter_key}

      {write, next_blob} =
        if next_count == 0 do
          {{:delete, counter_key}, nil}
        else
          blob =
            CompositeCounter.encode_value(
              prefix,
              next_count,
              next_expiring_count,
              next_physical_count
            )

          {{:put, counter_key, blob}, blob}
        end

      cache =
        put_counter_cache(
          cache,
          counter_key,
          prefix,
          next_blob,
          next_count,
          next_expiring_count,
          next_physical_count
        )

      {:ok, [guard, write], cache}
    end
  end

  defp counter_value(path, counter_key, prefix, cache) do
    values = Map.get(cache, :counter_values, %{})

    case Map.fetch(values, counter_key) do
      {:ok, {^prefix, blob, count, expiring_count, physical_count}} ->
        {:ok, blob, count, expiring_count, physical_count, cache}

      {:ok, {_other_prefix, _blob, _count, _expiring_count, _physical_count}} ->
        {:error, :composite_counter_collision}

      :error ->
        case LMDB.get(path, counter_key) do
          :not_found ->
            {:ok, nil, 0, 0, 0, put_counter_cache(cache, counter_key, prefix, nil, 0, 0, 0)}

          {:ok, blob} when is_binary(blob) ->
            case CompositeCounter.decode_state(blob, prefix) do
              {:ok,
               %{
                 count: count,
                 expiring_count: expiring_count,
                 physical_count: physical_count
               }} ->
                {:ok, blob, count, expiring_count, physical_count,
                 put_counter_cache(
                   cache,
                   counter_key,
                   prefix,
                   blob,
                   count,
                   expiring_count,
                   physical_count
                 )}

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

  defp checked_counter_delta(count, delta) when is_integer(count) and is_integer(delta) do
    next = count + delta

    cond do
      next < 0 -> {:error, :composite_counter_underflow}
      next > @max_u64 -> {:error, :composite_counter_overflow}
      true -> {:ok, next}
    end
  end

  defp checked_counter_deltas(
         count,
         expiring_count,
         physical_count,
         count_delta,
         expiring_delta,
         physical_delta
       ) do
    with {:ok, next_count} <- checked_counter_delta(count, count_delta),
         {:ok, next_expiring_count} <-
           checked_counter_delta(expiring_count, expiring_delta),
         {:ok, next_physical_count} <-
           checked_counter_delta(physical_count, physical_delta),
         true <- next_expiring_count <= next_count,
         true <- next_count <= next_physical_count,
         true <- next_count == 0 == (next_physical_count == 0) do
      {:ok, next_count, next_expiring_count, next_physical_count}
    else
      false -> {:error, :invalid_composite_counter}
      {:error, _reason} = error -> error
    end
  end

  defp put_counter_cache(cache, key, prefix, blob, count, expiring_count, physical_count) do
    values = Map.get(cache, :counter_values, %{})

    Map.put(
      cache,
      :counter_values,
      Map.put(values, key, {prefix, blob, count, expiring_count, physical_count})
    )
  end

  defp put_reverse_cache(cache, state_key, blob, keys, expire_at_ms) do
    reverse_values = Map.get(cache, :reverse_values, %{})

    Map.put(
      cache,
      :reverse_values,
      Map.put(reverse_values, state_key, {blob, keys, expire_at_ms})
    )
  end
end
