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

  @spec ensure(map(), binary(), binary(), binary() | nil) :: map()
  def ensure(state, redis_key, prefix, data_path) do
    cond do
      not ready_tables?(state) ->
        state

      ready_key?(state.zset_score_lookup, redis_key) ->
        state

      true ->
        clear_key(state.zset_score_index, state.zset_score_lookup, redis_key)

        state
        |> ShardETS.prefix_scan_entries(prefix, data_path)
        |> Enum.each(fn {member, score_str} ->
          put_member(
            state.zset_score_index,
            state.zset_score_lookup,
            redis_key,
            member,
            score_str
          )
        end)

        :ets.insert(state.zset_score_lookup, {{:ready, redis_key}, true})
        %{state | zset_index_ready: MapSet.put(state.zset_index_ready, redis_key)}
    end
  end

  @spec range(:ets.tid(), binary(), term(), term(), boolean()) :: [{binary(), float()}]
  def range(index_table, redis_key, min_bound, max_bound, reverse?) do
    items =
      index_table
      |> scan_range(redis_key, min_bound, max_bound)
      |> Enum.reverse()

    if reverse?, do: Enum.reverse(items), else: items
  end

  @spec count(:ets.tid(), binary(), term(), term()) :: non_neg_integer()
  def count(index_table, redis_key, min_bound, max_bound) do
    start = range_start(redis_key, min_bound)
    count_range(index_table, :ets.next(index_table, start), redis_key, min_bound, max_bound, 0)
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
          index_table,
          :ets.next(index_table, first_before(redis_key)),
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
          index_table,
          :ets.prev(index_table, first_after(redis_key)),
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

  @spec clear_ready_key(map(), binary()) :: map()
  def clear_ready_key(state, redis_key) do
    if ready_tables?(state) do
      clear_key(state.zset_score_index, state.zset_score_lookup, redis_key)
      %{state | zset_index_ready: MapSet.delete(state.zset_index_ready, redis_key)}
    else
      state
    end
  end

  @spec put_member(:ets.tid(), :ets.tid(), binary(), binary(), binary()) :: :ok
  def put_member(index_table, lookup_table, redis_key, member, score_str) do
    case parse_score(score_str) do
      {:ok, score} ->
        delete_member(index_table, lookup_table, redis_key, member)
        :ets.insert(index_table, {{redis_key, @index_tag, score, member}, true})
        :ets.insert(lookup_table, {{redis_key, member}, score})
        :ok

      :error ->
        :ok
    end
  end

  @spec delete_member(:ets.tid(), :ets.tid(), binary(), binary()) :: :ok
  def delete_member(index_table, lookup_table, redis_key, member) do
    case :ets.lookup(lookup_table, {redis_key, member}) do
      [{{^redis_key, ^member}, score}] ->
        :ets.delete(index_table, {redis_key, @index_tag, score, member})
        :ets.delete(lookup_table, {redis_key, member})

      [] ->
        :ok
    end
  end

  @spec clear_key(:ets.tid(), :ets.tid(), binary()) :: :ok
  def clear_key(index_table, lookup_table, redis_key) do
    delete_index_entries(index_table, :ets.next(index_table, {redis_key, @index_tag}), redis_key)
    :ets.match_delete(lookup_table, {{redis_key, :_}, :_})
    :ets.delete(lookup_table, {:ready, redis_key})
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

  defp scan_range(index_table, redis_key, min_bound, max_bound) do
    start = range_start(redis_key, min_bound)
    do_scan_range(index_table, :ets.next(index_table, start), redis_key, min_bound, max_bound, [])
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

  defp do_scan_range(_table, :"$end_of_table", _redis_key, _min_bound, _max_bound, acc), do: acc

  defp do_scan_range(table, key, redis_key, min_bound, max_bound, acc) do
    case key do
      {^redis_key, @index_tag, score, member} ->
        cond do
          not score_lte_bound?(score, max_bound) ->
            acc

          score_gte_bound?(score, min_bound) ->
            do_scan_range(table, :ets.next(table, key), redis_key, min_bound, max_bound, [
              {member, score} | acc
            ])

          true ->
            do_scan_range(table, :ets.next(table, key), redis_key, min_bound, max_bound, acc)
        end

      _ ->
        acc
    end
  end

  defp range_start(redis_key, :neg_inf), do: {redis_key, @index_tag}
  defp range_start(redis_key, {:inclusive, score}), do: {redis_key, @index_tag, score}
  defp range_start(redis_key, {:exclusive, score}), do: {redis_key, @index_tag, score}

  defp range_start(redis_key, :inf) do
    {redis_key, @index_tag}
  end

  defp first_before(redis_key), do: {redis_key, @index_tag}
  defp first_after(redis_key), do: {redis_key, @index_tag + 1}
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
