defmodule Ferricstore.Store.Shard.ZSetIndex do
  @moduledoc false

  alias Ferricstore.Store.CompoundKey
  alias Ferricstore.Store.Shard.ETS, as: ShardETS

  @index_tag 0

  @spec table_names(atom(), non_neg_integer()) :: {atom(), atom()}
  def table_names(instance_name, shard_index) do
    {
      :"ferricstore_zset_score_index_#{instance_name}_#{shard_index}",
      :"ferricstore_zset_score_lookup_#{instance_name}_#{shard_index}"
    }
  end

  @spec ensure(map(), binary(), binary(), binary() | nil) ::
          {:ok, map()} | Ferricstore.Store.ReadResult.failure()
  def ensure(state, redis_key, prefix, data_path) do
    cond do
      not ready_tables?(state) ->
        {:ok, state}

      ready_key?(state.zset_score_lookup, redis_key) ->
        {:ok, state}

      true ->
        clear_key(state.zset_score_index, state.zset_score_lookup, redis_key)

        case ShardETS.prefix_scan_entries(state, prefix, data_path) do
          {:error, {:storage_read_failed, _reason}} = failure ->
            failure

          entries when is_list(entries) ->
            :ets.insert(state.zset_score_lookup, {{:count, redis_key}, 0})

            Enum.each(entries, fn {member, score_str} ->
              put_member(
                state.zset_score_index,
                state.zset_score_lookup,
                redis_key,
                member,
                score_str
              )
            end)

            :ets.insert(state.zset_score_lookup, {{:ready, redis_key}, true})
            {:ok, %{state | zset_index_ready: MapSet.put(state.zset_index_ready, redis_key)}}
        end
    end
  end

  @spec range(:ets.tid(), binary(), term(), term(), boolean()) :: [{binary(), float()}]
  def range(index_table, redis_key, min_bound, max_bound, reverse?) do
    range_slice(index_table, redis_key, min_bound, max_bound, reverse?, 0, :all)
  end

  @spec range_slice(
          :ets.tid(),
          binary(),
          term(),
          term(),
          boolean(),
          non_neg_integer(),
          non_neg_integer() | :all
        ) :: [{binary(), float()}]
  def range_slice(_index_table, _redis_key, _min_bound, _max_bound, _reverse?, _offset, 0),
    do: []

  def range_slice(index_table, redis_key, min_bound, max_bound, false, offset, count) do
    collect_score_slice(
      forward_range_first(index_table, redis_key, min_bound),
      index_table,
      redis_key,
      min_bound,
      max_bound,
      offset,
      count,
      &next_key/2,
      []
    )
  end

  def range_slice(index_table, redis_key, min_bound, max_bound, true, offset, count) do
    collect_score_slice(
      reverse_range_first(index_table, redis_key, max_bound),
      index_table,
      redis_key,
      min_bound,
      max_bound,
      offset,
      count,
      &prev_key/2,
      []
    )
  end

  @spec count(:ets.tid(), :ets.tid(), binary(), term(), term()) :: non_neg_integer()
  def count(_index_table, lookup_table, redis_key, :neg_inf, :inf) do
    count_all(lookup_table, redis_key)
  end

  def count(index_table, _lookup_table, redis_key, min_bound, max_bound) do
    count_range(
      index_table,
      forward_range_first(index_table, redis_key, min_bound),
      redis_key,
      min_bound,
      max_bound,
      0
    )
  end

  @spec rank_range(:ets.tid(), binary(), non_neg_integer(), non_neg_integer(), boolean()) ::
          [{binary(), float()}]
  def rank_range(_index_table, _redis_key, start_idx, stop_idx, _reverse?)
      when start_idx > stop_idx do
    []
  end

  def rank_range(index_table, redis_key, start_idx, stop_idx, false) do
    index_table
    |> :ets.next(first_before(redis_key))
    |> collect_rank_range(index_table, redis_key, start_idx, stop_idx, 0, &next_key/2, [])
  end

  def rank_range(index_table, redis_key, start_idx, stop_idx, true) do
    index_table
    |> :ets.prev(first_after(redis_key))
    |> collect_rank_range(index_table, redis_key, start_idx, stop_idx, 0, &prev_key/2, [])
  end

  @spec member_rank(:ets.tid(), :ets.tid(), binary(), binary(), boolean()) ::
          non_neg_integer() | nil
  def member_rank(index_table, lookup_table, redis_key, member, false) do
    with [{{^redis_key, ^member}, score}] <- :ets.lookup(lookup_table, {redis_key, member}) do
      target = {redis_key, @index_tag, score, member}

      if :ets.member(index_table, target) do
        rank_until(
          :ets.next(index_table, first_before(redis_key)),
          index_table,
          redis_key,
          target,
          0,
          &next_key/2
        )
      end
    else
      _ -> nil
    end
  end

  def member_rank(index_table, lookup_table, redis_key, member, true) do
    with [{{^redis_key, ^member}, score}] <- :ets.lookup(lookup_table, {redis_key, member}) do
      target = {redis_key, @index_tag, score, member}

      if :ets.member(index_table, target) do
        rank_until(
          :ets.prev(index_table, first_after(redis_key)),
          index_table,
          redis_key,
          target,
          0,
          &prev_key/2
        )
      end
    else
      _ -> nil
    end
  end

  @spec apply_put(map(), binary(), binary(), binary()) :: map()
  def apply_put(state, redis_key, compound_key, score_str) do
    if ready_for_key?(state, redis_key) do
      apply_put_to_tables(
        state.zset_score_index,
        state.zset_score_lookup,
        redis_key,
        compound_key,
        score_str
      )
    end

    state
  end

  @spec apply_put_to_tables(:ets.tid(), :ets.tid(), binary(), binary(), binary()) :: :ok
  def apply_put_to_tables(index_table, lookup_table, redis_key, compound_key, score_str) do
    if ready_key?(lookup_table, redis_key) do
      prefix = CompoundKey.zset_prefix(redis_key)

      if String.starts_with?(compound_key, prefix) do
        member = CompoundKey.extract_subkey(compound_key, prefix)
        put_member(index_table, lookup_table, redis_key, member, score_str)
      end
    end

    :ok
  end

  @spec apply_puts(map(), binary(), [{binary(), binary(), non_neg_integer()}]) :: map()
  def apply_puts(state, redis_key, entries) do
    if ready_for_key?(state, redis_key) do
      Enum.each(entries, fn {compound_key, score_str, _expire_at_ms} ->
        apply_put(state, redis_key, compound_key, score_str)
      end)
    end

    state
  end

  @spec apply_delete(map(), binary(), binary()) :: map()
  def apply_delete(state, redis_key, compound_key) do
    if ready_for_key?(state, redis_key) do
      apply_delete_to_tables(
        state.zset_score_index,
        state.zset_score_lookup,
        redis_key,
        compound_key
      )
    end

    state
  end

  @spec apply_delete_to_tables(:ets.tid(), :ets.tid(), binary(), binary()) :: :ok
  def apply_delete_to_tables(index_table, lookup_table, redis_key, compound_key) do
    if ready_key?(lookup_table, redis_key) do
      prefix = CompoundKey.zset_prefix(redis_key)

      if String.starts_with?(compound_key, prefix) do
        member = CompoundKey.extract_subkey(compound_key, prefix)
        delete_member(index_table, lookup_table, redis_key, member)
      end
    end

    :ok
  end

  @spec ready?(:ets.tid(), binary()) :: boolean()
  def ready?(lookup_table, redis_key), do: ready_key?(lookup_table, redis_key)

  @spec mark_ready_empty(:ets.tid(), :ets.tid(), binary()) :: :ok
  def mark_ready_empty(index_table, lookup_table, redis_key) do
    clear_key(index_table, lookup_table, redis_key)
    mark_new_ready_empty(index_table, lookup_table, redis_key)
  end

  @spec mark_new_ready_empty(:ets.tid(), :ets.tid(), binary()) :: :ok
  def mark_new_ready_empty(_index_table, lookup_table, redis_key) do
    :ets.insert_new(lookup_table, {{:count, redis_key}, 0})
    :ets.insert(lookup_table, {{:ready, redis_key}, true})
    :ok
  end

  @spec clear_ready_key(map(), binary()) :: map()
  def clear_ready_key(state, redis_key) do
    if ready_tables?(state) do
      clear_key(state.zset_score_index, state.zset_score_lookup, redis_key)
      %{state | zset_index_ready: MapSet.delete(state.zset_index_ready, redis_key)}
    else
      state
    end
  end

  @spec reset(map()) :: map()
  def reset(state) when is_map(state) do
    state
    |> zset_table(:zset_score_index, :zset_score_index_name)
    |> delete_all_objects()

    state
    |> zset_table(:zset_score_lookup, :zset_score_lookup_name)
    |> delete_all_objects()

    if Map.has_key?(state, :zset_index_ready) do
      Map.put(state, :zset_index_ready, MapSet.new())
    else
      state
    end
  end

  defp zset_table(state, primary_key, fallback_key) do
    Map.get(state, primary_key) || Map.get(state, fallback_key)
  end

  defp delete_all_objects(nil), do: :ok

  defp delete_all_objects(table) do
    case :ets.info(table) do
      :undefined -> :ok
      _info -> :ets.delete_all_objects(table)
    end
  rescue
    ArgumentError -> :ok
  end

  @spec put_member(:ets.tid(), :ets.tid(), binary(), binary(), binary()) :: :ok
  def put_member(index_table, lookup_table, redis_key, member, score_str) do
    case parse_score(score_str) do
      {:ok, score} ->
        delete_member(index_table, lookup_table, redis_key, member)
        :ets.insert(index_table, {{redis_key, @index_tag, score, member}, true})
        :ets.insert(lookup_table, {{redis_key, member}, score})
        increment_count(lookup_table, redis_key, 1)
        :ok

      :error ->
        :ok
    end
  end

  @spec put_new_member(:ets.tid(), :ets.tid(), binary(), binary(), score_input()) :: :ok
  def put_new_member(index_table, lookup_table, redis_key, member, score_input) do
    case parse_score(score_input) do
      {:ok, score} ->
        :ets.insert(index_table, {{redis_key, @index_tag, score, member}, true})
        :ets.insert(lookup_table, {{redis_key, member}, score})
        increment_count(lookup_table, redis_key, 1)
        :ok

      :error ->
        :ok
    end
  end

  @type score_input :: binary() | integer() | float()

  @spec put_members(:ets.tid(), :ets.tid(), binary(), [{binary(), score_input()}]) :: :ok
  def put_members(index_table, lookup_table, redis_key, member_score_pairs) do
    delta =
      Enum.reduce(member_score_pairs, 0, fn {member, score_str}, acc ->
        case parse_score(score_str) do
          {:ok, score} ->
            existing? =
              case :ets.lookup(lookup_table, {redis_key, member}) do
                [{{^redis_key, ^member}, old_score}] ->
                  :ets.delete(index_table, {redis_key, @index_tag, old_score, member})
                  true

                [] ->
                  false
              end

            :ets.insert(index_table, {{redis_key, @index_tag, score, member}, true})
            :ets.insert(lookup_table, {{redis_key, member}, score})

            if existing?, do: acc, else: acc + 1

          :error ->
            acc
        end
      end)

    if delta != 0 do
      increment_count(lookup_table, redis_key, delta)
    end

    :ok
  end

  @spec put_new_members(:ets.tid(), :ets.tid(), binary(), [{binary(), score_input()}]) :: :ok
  def put_new_members(index_table, lookup_table, redis_key, member_score_pairs) do
    {index_entries, lookup_entries, count} =
      Enum.reduce(member_score_pairs, {[], [], 0}, fn {member, score_input},
                                                      {index_acc, lookup_acc, count} ->
        case parse_score(score_input) do
          {:ok, score} ->
            {
              [{{redis_key, @index_tag, score, member}, true} | index_acc],
              [{{redis_key, member}, score} | lookup_acc],
              count + 1
            }

          :error ->
            {index_acc, lookup_acc, count}
        end
      end)

    if count > 0 do
      :ets.insert(index_table, index_entries)
      :ets.insert(lookup_table, lookup_entries)
      increment_count(lookup_table, redis_key, count)
    end

    :ok
  end

  @spec delete_member(:ets.tid(), :ets.tid(), binary(), binary()) :: :ok
  def delete_member(index_table, lookup_table, redis_key, member) do
    case :ets.take(lookup_table, {redis_key, member}) do
      [{{^redis_key, ^member}, score}] ->
        :ets.delete(index_table, {redis_key, @index_tag, score, member})
        increment_count(lookup_table, redis_key, -1)

      [] ->
        :ok
    end
  end

  @spec delete_members(:ets.tid(), :ets.tid(), binary(), [binary()]) :: :ok
  def delete_members(index_table, lookup_table, redis_key, members) do
    deleted =
      Enum.reduce(members, 0, fn member, acc ->
        case :ets.take(lookup_table, {redis_key, member}) do
          [{{^redis_key, ^member}, score}] ->
            :ets.delete(index_table, {redis_key, @index_tag, score, member})
            acc + 1

          [] ->
            acc
        end
      end)

    if deleted != 0 do
      increment_count(lookup_table, redis_key, -deleted)
    end

    :ok
  end

  @spec clear_key(:ets.tid(), :ets.tid(), binary()) :: :ok
  def clear_key(index_table, lookup_table, redis_key) do
    delete_index_entries(index_table, :ets.next(index_table, first_before(redis_key)), redis_key)
    :ets.match_delete(lookup_table, {{redis_key, :_}, :_})
    :ets.delete(lookup_table, {:ready, redis_key})
    :ets.delete(lookup_table, {:count, redis_key})
    :ok
  end

  @spec rebuild_key(:ets.tid(), :ets.tid(), binary(), [{binary(), score_input()}]) ::
          :ok | {:error, {:invalid_score, binary(), term()}}
  def rebuild_key(index_table, lookup_table, redis_key, member_score_pairs) do
    with :ok <- validate_member_scores(member_score_pairs) do
      clear_key(index_table, lookup_table, redis_key)
      :ets.insert(lookup_table, {{:count, redis_key}, 0})
      put_new_members(index_table, lookup_table, redis_key, member_score_pairs)
      :ets.insert(lookup_table, {{:ready, redis_key}, true})
      :ok
    end
  end

  defp validate_member_scores(member_score_pairs) do
    Enum.reduce_while(member_score_pairs, :ok, fn {member, score}, :ok ->
      case parse_score(score) do
        {:ok, _parsed} -> {:cont, :ok}
        :error -> {:halt, {:error, {:invalid_score, member, score}}}
      end
    end)
  end

  defp count_all(lookup_table, redis_key) do
    case :ets.lookup(lookup_table, {:count, redis_key}) do
      [{{:count, ^redis_key}, count}] when is_integer(count) and count >= 0 -> count
      _ -> 0
    end
  end

  defp increment_count(lookup_table, redis_key, delta) do
    :ets.update_counter(lookup_table, {:count, redis_key}, {2, delta}, {{:count, redis_key}, 0})
    :ok
  end

  defp delete_index_entries(_table, :"$end_of_table", _redis_key), do: :ok

  defp delete_index_entries(table, key, redis_key) do
    next = :ets.next(table, key)

    case key do
      {^redis_key, @index_tag, _score, _member} ->
        :ets.delete(table, key)
        delete_index_entries(table, next, redis_key)

      _ ->
        :ok
    end
  end

  defp count_range(_table, :"$end_of_table", _redis_key, _min_bound, _max_bound, acc), do: acc

  defp count_range(table, key, redis_key, min_bound, max_bound, acc) do
    case key do
      {^redis_key, @index_tag, score, _member} ->
        cond do
          not score_lte_bound?(score, max_bound) ->
            acc

          score_gte_bound?(score, min_bound) ->
            count_range(table, :ets.next(table, key), redis_key, min_bound, max_bound, acc + 1)

          true ->
            count_range(table, :ets.next(table, key), redis_key, min_bound, max_bound, acc)
        end

      _ ->
        acc
    end
  end

  defp collect_score_slice(
         :"$end_of_table",
         _table,
         _redis_key,
         _min_bound,
         _max_bound,
         _offset,
         _count,
         _next,
         acc
       ) do
    Enum.reverse(acc)
  end

  defp collect_score_slice(key, table, redis_key, min_bound, max_bound, offset, count, next, acc) do
    case key do
      {^redis_key, @index_tag, score, member} ->
        cond do
          not score_lte_bound?(score, max_bound) ->
            Enum.reverse(acc)

          not score_gte_bound?(score, min_bound) ->
            Enum.reverse(acc)

          offset > 0 ->
            collect_score_slice(
              next.(table, key),
              table,
              redis_key,
              min_bound,
              max_bound,
              offset - 1,
              count,
              next,
              acc
            )

          count == :all ->
            collect_score_slice(
              next.(table, key),
              table,
              redis_key,
              min_bound,
              max_bound,
              offset,
              count,
              next,
              [{member, score} | acc]
            )

          count > 0 ->
            collect_score_slice(
              next.(table, key),
              table,
              redis_key,
              min_bound,
              max_bound,
              offset,
              count - 1,
              next,
              [{member, score} | acc]
            )

          true ->
            Enum.reverse(acc)
        end

      _ ->
        Enum.reverse(acc)
    end
  end

  defp collect_rank_range(
         :"$end_of_table",
         _table,
         _redis_key,
         _start_idx,
         _stop_idx,
         _rank,
         _next,
         acc
       ) do
    Enum.reverse(acc)
  end

  defp collect_rank_range(key, table, redis_key, start_idx, stop_idx, rank, next, acc) do
    case key do
      {^redis_key, @index_tag, score, member} ->
        cond do
          rank > stop_idx ->
            Enum.reverse(acc)

          rank >= start_idx ->
            collect_rank_range(
              next.(table, key),
              table,
              redis_key,
              start_idx,
              stop_idx,
              rank + 1,
              next,
              [
                {member, score} | acc
              ]
            )

          true ->
            collect_rank_range(
              next.(table, key),
              table,
              redis_key,
              start_idx,
              stop_idx,
              rank + 1,
              next,
              acc
            )
        end

      _ ->
        Enum.reverse(acc)
    end
  end

  defp rank_until(:"$end_of_table", _table, _redis_key, _target, _rank, _next), do: nil

  defp rank_until(key, table, redis_key, target, rank, next) do
    case key do
      ^target ->
        rank

      {^redis_key, @index_tag, _score, _member} ->
        rank_until(next.(table, key), table, redis_key, target, rank + 1, next)

      _ ->
        nil
    end
  end

  defp forward_range_first(table, redis_key, :neg_inf) do
    :ets.next(table, first_before(redis_key))
  end

  defp forward_range_first(table, redis_key, {:inclusive, score}) do
    :ets.next(table, before_score_ties(redis_key, score))
  end

  defp forward_range_first(table, redis_key, {:exclusive, score}) do
    table
    |> :ets.next(before_score_ties(redis_key, score))
    |> after_score_ties(table, redis_key, score)
  end

  defp forward_range_first(_table, _redis_key, :inf), do: :"$end_of_table"

  defp reverse_range_first(table, redis_key, :inf) do
    :ets.prev(table, first_after(redis_key))
  end

  defp reverse_range_first(_table, _redis_key, :neg_inf), do: :"$end_of_table"

  defp reverse_range_first(table, redis_key, {:inclusive, score}) do
    boundary =
      table
      |> :ets.next(before_score_ties(redis_key, score))
      |> after_score_ties(table, redis_key, score)

    :ets.prev(table, boundary)
  end

  defp reverse_range_first(table, redis_key, {:exclusive, score}) do
    :ets.prev(table, before_score_ties(redis_key, score))
  end

  defp before_score_ties(redis_key, score), do: {redis_key, @index_tag, score, nil}

  defp after_score_ties(:"$end_of_table", _table, redis_key, _score), do: first_after(redis_key)

  defp after_score_ties({redis_key, @index_tag, score, _member} = key, table, redis_key, score) do
    table
    |> :ets.next(key)
    |> after_score_ties(table, redis_key, score)
  end

  defp after_score_ties(key, _table, _redis_key, _score), do: key

  defp first_before(redis_key), do: {redis_key, @index_tag - 1, nil, nil}
  defp first_after(redis_key), do: {redis_key, @index_tag + 1, nil, nil}
  defp next_key(table, key), do: :ets.next(table, key)
  defp prev_key(table, key), do: :ets.prev(table, key)

  defp score_gte_bound?(_score, :neg_inf), do: true
  defp score_gte_bound?(_score, :inf), do: false
  defp score_gte_bound?(score, {:exclusive, bound}), do: score > bound
  defp score_gte_bound?(score, {:inclusive, bound}), do: score >= bound

  defp score_lte_bound?(_score, :inf), do: true
  defp score_lte_bound?(_score, :neg_inf), do: false
  defp score_lte_bound?(score, {:exclusive, bound}), do: score < bound
  defp score_lte_bound?(score, {:inclusive, bound}), do: score <= bound

  defp parse_score(score) when is_float(score), do: {:ok, score}
  defp parse_score(score) when is_integer(score), do: {:ok, score * 1.0}

  defp parse_score(score_str) when is_binary(score_str) do
    case Float.parse(score_str) do
      {score, ""} -> {:ok, score}
      _ -> :error
    end
  end

  defp ready_for_key?(state, redis_key) do
    ready_tables?(state) and ready_key?(state.zset_score_lookup, redis_key)
  end

  defp ready_key?(lookup_table, redis_key) do
    :ets.member(lookup_table, {:ready, redis_key})
  end

  defp ready_tables?(%{zset_score_index: index, zset_score_lookup: lookup})
       when index != nil and lookup != nil,
       do: true

  defp ready_tables?(_state), do: false
end
