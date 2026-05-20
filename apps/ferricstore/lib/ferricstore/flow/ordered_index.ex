defmodule Ferricstore.Flow.OrderedIndex do
  @moduledoc """
  Flow-specific ordered index primitive.

  This is the hot in-memory representation for Flow range/count indexes. It is
  intentionally narrower than Redis sorted sets: callers provide already parsed
  Flow index keys, members, and numeric scores, and the primitive keeps an
  ordered table plus a lookup table so updates/deletes are cheap.
  """

  @spec table_names(atom(), non_neg_integer()) :: {atom(), atom()}
  def table_names(instance_name, shard_index) do
    {
      :"ferricstore_flow_index_#{instance_name}_#{shard_index}",
      :"ferricstore_flow_lookup_#{instance_name}_#{shard_index}"
    }
  end

  @spec put_member(:ets.tid() | atom(), :ets.tid() | atom(), binary(), binary(), score_input()) ::
          :ok
  def put_member(index_table, lookup_table, key, member, score_input) do
    put_members(index_table, lookup_table, key, [{member, score_input}])
  end

  @spec put_new_member(
          :ets.tid() | atom(),
          :ets.tid() | atom(),
          binary(),
          binary(),
          score_input()
        ) :: :ok
  def put_new_member(index_table, lookup_table, key, member, score_input) do
    put_new_members(index_table, lookup_table, key, [{member, score_input}])
  end

  @type score_input :: binary() | integer() | float()

  @spec put_members(:ets.tid() | atom(), :ets.tid() | atom(), binary(), [
          {binary(), score_input()}
        ]) :: :ok
  def put_members(index_table, lookup_table, key, member_score_pairs) do
    delta =
      Enum.reduce(member_score_pairs, 0, fn {member, score_input}, acc ->
        case parse_score(score_input) do
          {:ok, score} ->
            existing? =
              case :ets.lookup(lookup_table, {key, member}) do
                [{{^key, ^member}, old_score}] ->
                  :ets.delete(index_table, {key, old_score, member})
                  true

                [] ->
                  false
              end

            :ets.insert(index_table, {{key, score, member}, true})
            :ets.insert(lookup_table, {{key, member}, score})

            if existing?, do: acc, else: acc + 1

          :error ->
            acc
        end
      end)

    increment_count(lookup_table, key, delta)
  end

  @spec put_entries(:ets.tid() | atom(), :ets.tid() | atom(), [
          {binary(), binary(), score_input()}
        ]) :: :ok
  def put_entries(index_table, lookup_table, key_member_score_triples) do
    {index_entries, lookup_entries, count_deltas} =
      Enum.reduce(key_member_score_triples, {[], [], %{}}, fn {key, member, score_input},
                                                              {index_acc, lookup_acc, count_acc} ->
        case parse_score(score_input) do
          {:ok, score} ->
            existing? =
              case :ets.lookup(lookup_table, {key, member}) do
                [{{^key, ^member}, old_score}] ->
                  :ets.delete(index_table, {key, old_score, member})
                  true

                [] ->
                  false
              end

            count_acc =
              if existing?, do: count_acc, else: Map.update(count_acc, key, 1, &(&1 + 1))

            {[{{key, score, member}, true} | index_acc], [{{key, member}, score} | lookup_acc],
             count_acc}

          :error ->
            {index_acc, lookup_acc, count_acc}
        end
      end)

    if index_entries != [], do: :ets.insert(index_table, index_entries)
    if lookup_entries != [], do: :ets.insert(lookup_table, lookup_entries)

    Enum.each(count_deltas, fn {key, delta} -> increment_count(lookup_table, key, delta) end)

    :ok
  end

  @spec put_new_entries(:ets.tid() | atom(), :ets.tid() | atom(), [
          {binary(), binary(), score_input()}
        ]) :: :ok
  def put_new_entries(index_table, lookup_table, key_member_score_triples) do
    {index_entries, lookup_entries, count_deltas} =
      Enum.reduce(key_member_score_triples, {[], [], %{}}, fn {key, member, score_input},
                                                              {index_acc, lookup_acc, count_acc} ->
        case parse_score(score_input) do
          {:ok, score} ->
            {
              [{{key, score, member}, true} | index_acc],
              [{{key, member}, score} | lookup_acc],
              Map.update(count_acc, key, 1, &(&1 + 1))
            }

          :error ->
            {index_acc, lookup_acc, count_acc}
        end
      end)

    if index_entries != [], do: :ets.insert(index_table, index_entries)
    if lookup_entries != [], do: :ets.insert(lookup_table, lookup_entries)

    Enum.each(count_deltas, fn {key, delta} -> increment_count(lookup_table, key, delta) end)

    :ok
  end

  @spec put_new_members(:ets.tid() | atom(), :ets.tid() | atom(), binary(), [
          {binary(), score_input()}
        ]) :: :ok
  def put_new_members(index_table, lookup_table, key, member_score_pairs) do
    {index_entries, lookup_entries, count} =
      Enum.reduce(member_score_pairs, {[], [], 0}, fn {member, score_input},
                                                      {index_acc, lookup_acc, count} ->
        case parse_score(score_input) do
          {:ok, score} ->
            {
              [{{key, score, member}, true} | index_acc],
              [{{key, member}, score} | lookup_acc],
              count + 1
            }

          :error ->
            {index_acc, lookup_acc, count}
        end
      end)

    if count > 0 do
      :ets.insert(index_table, index_entries)
      :ets.insert(lookup_table, lookup_entries)
    end

    increment_count(lookup_table, key, count)
  end

  @spec move_entries(:ets.tid() | atom(), :ets.tid() | atom(), [
          {binary(), binary(), binary(), score_input()}
        ]) :: :ok
  def move_entries(index_table, lookup_table, key_key_member_score_quads) do
    {index_entries, lookup_entries, count_deltas} =
      Enum.reduce(key_key_member_score_quads, {[], [], %{}}, fn
        {from_key, to_key, member, score_input}, {index_acc, lookup_acc, count_acc} ->
          case parse_score(score_input) do
            {:ok, score} ->
              {source_exists?, count_acc} =
                remove_move_source(index_table, lookup_table, from_key, to_key, member, count_acc)

              {existing_destination?, count_acc} =
                remove_move_destination(
                  index_table,
                  lookup_table,
                  from_key,
                  to_key,
                  member,
                  source_exists?,
                  count_acc
                )

              count_acc =
                if existing_destination? do
                  count_acc
                else
                  Map.update(count_acc, to_key, 1, &(&1 + 1))
                end

              {[{{to_key, score, member}, true} | index_acc],
               [{{to_key, member}, score} | lookup_acc], count_acc}

            :error ->
              {index_acc, lookup_acc, count_acc}
          end
      end)

    if index_entries != [], do: :ets.insert(index_table, index_entries)
    if lookup_entries != [], do: :ets.insert(lookup_table, lookup_entries)

    Enum.each(count_deltas, fn {key, delta} -> increment_count(lookup_table, key, delta) end)

    :ok
  end

  @spec delete_member(:ets.tid() | atom(), :ets.tid() | atom(), binary(), binary()) :: :ok
  def delete_member(index_table, lookup_table, key, member) do
    delete_members(index_table, lookup_table, key, [member])
  end

  @spec delete_members(:ets.tid() | atom(), :ets.tid() | atom(), binary(), [binary()]) :: :ok
  def delete_members(index_table, lookup_table, key, members) do
    deleted =
      Enum.reduce(members, 0, fn member, acc ->
        case :ets.take(lookup_table, {key, member}) do
          [{{^key, ^member}, score}] ->
            :ets.delete(index_table, {key, score, member})
            acc + 1

          [] ->
            acc
        end
      end)

    increment_count(lookup_table, key, -deleted)
  end

  @spec score_of(:ets.tid() | atom(), binary(), binary()) :: {:ok, float()} | :miss
  def score_of(lookup_table, key, member) do
    case :ets.lookup(lookup_table, {key, member}) do
      [{{^key, ^member}, score}] -> {:ok, score}
      [] -> :miss
    end
  end

  @spec range_slice(
          :ets.tid() | atom(),
          binary(),
          term(),
          term(),
          boolean(),
          non_neg_integer(),
          non_neg_integer() | :all
        ) :: [{binary(), float()}]
  def range_slice(_index_table, _key, _min_bound, _max_bound, _reverse?, _offset, 0), do: []

  def range_slice(index_table, key, min_bound, max_bound, false, offset, count) do
    collect_score_slice(
      forward_range_first(index_table, key, min_bound),
      index_table,
      key,
      min_bound,
      max_bound,
      offset,
      count,
      &next_key/2,
      []
    )
  end

  def range_slice(index_table, key, min_bound, max_bound, true, offset, count) do
    collect_score_slice(
      reverse_range_first(index_table, key, max_bound),
      index_table,
      key,
      min_bound,
      max_bound,
      offset,
      count,
      &prev_key/2,
      []
    )
  end

  @spec rank_range(
          :ets.tid() | atom(),
          binary(),
          non_neg_integer(),
          non_neg_integer(),
          boolean()
        ) :: [{binary(), float()}]
  def rank_range(_index_table, _key, start_idx, stop_idx, _reverse?) when start_idx > stop_idx,
    do: []

  def rank_range(index_table, key, start_idx, stop_idx, reverse?) do
    range_slice(index_table, key, :neg_inf, :inf, reverse?, start_idx, stop_idx - start_idx + 1)
  end

  @spec count_all(:ets.tid() | atom(), binary()) :: non_neg_integer()
  def count_all(lookup_table, key) do
    case :ets.lookup(lookup_table, {:count, key}) do
      [{{:count, ^key}, count}] when is_integer(count) and count >= 0 -> count
      _ -> 0
    end
  end

  @spec count_keys(:ets.tid() | atom()) :: [binary()]
  def count_keys(lookup_table) do
    count_keys_from_counts(lookup_table)
  rescue
    ArgumentError -> []
  end

  @spec due_count_keys(:ets.tid() | atom()) :: [binary()]
  def due_count_keys(lookup_table) do
    lookup_table
    |> count_keys_from_counts()
    |> Enum.filter(&due_key?/1)
  rescue
    ArgumentError -> []
  end

  @spec restore_count(:ets.tid() | atom(), binary(), integer()) :: :ok
  def restore_count(lookup_table, key, count) do
    :ets.insert(lookup_table, {{:count, key}, count})
    :ok
  end

  @spec delete_count(:ets.tid() | atom(), binary()) :: :ok
  def delete_count(lookup_table, key) do
    :ets.delete(lookup_table, {:count, key})
    :ok
  end

  defp count_keys_from_counts(lookup_table) do
    :ets.select(lookup_table, [
      {{{:count, :"$1"}, :"$2"}, [{:is_binary, :"$1"}, {:>, :"$2", 0}], [:"$1"]}
    ])
  end

  defp due_key?(key) when is_binary(key) do
    String.starts_with?(key, "f:{f") and match?({_pos, _len}, :binary.match(key, "}:d:"))
  end

  defp due_key?(_key), do: false

  defp increment_count(_lookup_table, _key, 0), do: :ok

  defp increment_count(lookup_table, key, delta) do
    :ets.update_counter(lookup_table, {:count, key}, {2, delta}, {{:count, key}, 0})
    :ok
  end

  defp remove_move_source(index_table, lookup_table, key, key, member, count_acc) do
    case :ets.lookup(lookup_table, {key, member}) do
      [{{^key, ^member}, old_score}] ->
        :ets.delete(index_table, {key, old_score, member})
        {true, count_acc}

      [] ->
        {false, count_acc}
    end
  end

  defp remove_move_source(index_table, lookup_table, from_key, _to_key, member, count_acc) do
    case :ets.take(lookup_table, {from_key, member}) do
      [{{^from_key, ^member}, old_score}] ->
        :ets.delete(index_table, {from_key, old_score, member})
        {true, Map.update(count_acc, from_key, -1, &(&1 - 1))}

      [] ->
        {false, count_acc}
    end
  end

  defp remove_move_destination(
         _index_table,
         _lookup_table,
         key,
         key,
         _member,
         source_exists?,
         count_acc
       ) do
    {source_exists?, count_acc}
  end

  defp remove_move_destination(
         index_table,
         lookup_table,
         _from_key,
         to_key,
         member,
         _source_exists?,
         count_acc
       ) do
    case :ets.lookup(lookup_table, {to_key, member}) do
      [{{^to_key, ^member}, old_score}] ->
        :ets.delete(index_table, {to_key, old_score, member})
        {true, count_acc}

      [] ->
        {false, count_acc}
    end
  end

  defp collect_score_slice(
         :"$end_of_table",
         _table,
         _key,
         _min,
         _max,
         _offset,
         _count,
         _next,
         acc
       ),
       do: Enum.reverse(acc)

  defp collect_score_slice(key_tuple, table, key, min_bound, max_bound, offset, count, next, acc) do
    case key_tuple do
      {^key, score, member} ->
        cond do
          not score_lte_bound?(score, max_bound) ->
            Enum.reverse(acc)

          not score_gte_bound?(score, min_bound) ->
            Enum.reverse(acc)

          offset > 0 ->
            collect_score_slice(
              next.(table, key_tuple),
              table,
              key,
              min_bound,
              max_bound,
              offset - 1,
              count,
              next,
              acc
            )

          count == :all ->
            collect_score_slice(
              next.(table, key_tuple),
              table,
              key,
              min_bound,
              max_bound,
              offset,
              count,
              next,
              [{member, score} | acc]
            )

          count > 0 ->
            collect_score_slice(
              next.(table, key_tuple),
              table,
              key,
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

  defp forward_range_first(table, key, :neg_inf), do: :ets.next(table, first_before(key))
  defp forward_range_first(_table, _key, :inf), do: :"$end_of_table"
  defp forward_range_first(_table, _key, :pos_inf), do: :"$end_of_table"

  defp forward_range_first(table, key, {:inclusive, score}) do
    :ets.next(table, before_score_ties(key, score))
  end

  defp forward_range_first(table, key, {:exclusive, score}) do
    table
    |> :ets.next(before_score_ties(key, score))
    |> after_score_ties(table, key, score)
  end

  defp reverse_range_first(table, key, :inf), do: :ets.prev(table, first_after(key))
  defp reverse_range_first(table, key, :pos_inf), do: :ets.prev(table, first_after(key))
  defp reverse_range_first(_table, _key, :neg_inf), do: :"$end_of_table"

  defp reverse_range_first(table, key, {:inclusive, score}) do
    boundary =
      table
      |> :ets.next(before_score_ties(key, score))
      |> after_score_ties(table, key, score)

    :ets.prev(table, boundary)
  end

  defp reverse_range_first(table, key, {:exclusive, score}) do
    :ets.prev(table, before_score_ties(key, score))
  end

  defp before_score_ties(key, score), do: {key, score, nil}
  defp first_before(key), do: {key, -1.0e308, nil}
  defp first_after(key), do: {key, 1.0e308, nil}
  defp next_key(table, key), do: :ets.next(table, key)
  defp prev_key(table, key), do: :ets.prev(table, key)

  defp after_score_ties(:"$end_of_table", _table, key, _score), do: first_after(key)

  defp after_score_ties({key, score, _member} = current, table, key, score) do
    table
    |> :ets.next(current)
    |> after_score_ties(table, key, score)
  end

  defp after_score_ties(current, _table, _key, _score), do: current

  defp score_gte_bound?(_score, :neg_inf), do: true
  defp score_gte_bound?(_score, :inf), do: false
  defp score_gte_bound?(_score, :pos_inf), do: false
  defp score_gte_bound?(score, {:exclusive, bound}), do: score > bound
  defp score_gte_bound?(score, {:inclusive, bound}), do: score >= bound

  defp score_lte_bound?(_score, :inf), do: true
  defp score_lte_bound?(_score, :pos_inf), do: true
  defp score_lte_bound?(_score, :neg_inf), do: false
  defp score_lte_bound?(score, {:exclusive, bound}), do: score < bound
  defp score_lte_bound?(score, {:inclusive, bound}), do: score <= bound

  defp parse_score(score) when is_float(score), do: {:ok, score}
  defp parse_score(score) when is_integer(score), do: {:ok, score * 1.0}

  defp parse_score(score) when is_binary(score) do
    case Float.parse(score) do
      {score, ""} -> {:ok, score}
      _ -> :error
    end
  end
end
