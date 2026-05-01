defmodule Ferricstore.Store.ListOps do
  @moduledoc """
  Pure-logic module for list data structure operations.

  ## Storage format (compound key / float-position)

  Each list element is stored as an individual compound key entry:
  `L:redis_key\\0{encoded_position} -> element_value`

  A metadata key stores length and position boundaries:
  `LM:redis_key -> :erlang.term_to_binary({length, next_left_pos, next_right_pos})`
  """

  alias Ferricstore.Store.CompoundKey
  alias Ferricstore.Store.Ops

  # Integer positions with large gaps. LPUSH/RPUSH decrement/increment by @position_step.
  # LINSERT picks the integer midpoint. When no room (adjacent positions differ by 1),
  # rebalance redistributes all positions with even spacing.
  @initial_position 0
  @position_step 1_000_000_000

  @spec execute(binary(), map(), term()) :: term()
  def execute(key, store, operation) do
    meta = read_meta(key, store)
    do_execute(key, store, meta, operation)
  end

  @spec execute_lmove(binary(), binary(), map(), :left | :right, :left | :right) ::
          binary() | nil | {:error, binary()}
  def execute_lmove(src_key, dst_key, store, from_dir, to_dir) when is_map(store) do
    src_meta = read_meta(src_key, store)

    case src_meta do
      nil ->
        nil

      {0, _, _} ->
        nil

      {_len, _left, _right} ->
        sorted = sorted_elements(src_key, store)

        if sorted == [] do
          nil
        else
          {pos, element} =
            case from_dir do
              :left -> hd(sorted)
              :right -> List.last(sorted)
            end

          Ops.compound_delete(store, src_key, CompoundKey.list_element(src_key, pos))
          remaining = Enum.reject(sorted, fn {p, _} -> p == pos end)

          if remaining == [] do
            delete_meta(src_key, store)
          else
            update_meta_from_remaining(src_key, store, length(remaining), remaining)
          end

          dst_meta = read_meta(dst_key, store)

          case dst_meta do
            nil ->
              new_pos = @initial_position

              Ops.compound_put(
                store,
                dst_key,
                CompoundKey.list_element(dst_key, new_pos),
                element,
                0
              )

              write_meta(dst_key, store, {1, new_pos - @position_step, new_pos + @position_step})

            {dst_len, dst_left, dst_right} ->
              new_pos =
                case to_dir do
                  :left -> dst_left
                  :right -> dst_right
                end

              Ops.compound_put(
                store,
                dst_key,
                CompoundKey.list_element(dst_key, new_pos),
                element,
                0
              )

              new_left = if to_dir == :left, do: new_pos - @position_step, else: dst_left
              new_right = if to_dir == :right, do: new_pos + @position_step, else: dst_right
              write_meta(dst_key, store, {dst_len + 1, new_left, new_right})
          end

          element
        end
    end
  end

  @doc false
  def read_meta(key, store) do
    meta_key = CompoundKey.list_meta_key(key)

    case Ops.compound_get(store, key, meta_key) do
      nil -> nil
      binary -> :erlang.binary_to_term(binary)
    end
  end

  defp write_meta(key, store, {_len, _left, _right} = meta) do
    Ops.compound_put(store, key, CompoundKey.list_meta_key(key), :erlang.term_to_binary(meta), 0)
    |> write_result()
  end

  defp delete_meta(key, store) do
    Ops.compound_delete(store, key, CompoundKey.list_meta_key(key))
    |> write_result()
  end

  defp sorted_elements(key, store) do
    prefix = CompoundKey.list_prefix(key)

    Ops.compound_scan(store, key, prefix)
    |> Enum.map(fn {encoded_pos, value} -> {CompoundKey.decode_position(encoded_pos), value} end)
  end

  defp ordered_values(key, store) do
    sorted_elements(key, store) |> Enum.map(fn {_pos, value} -> value end)
  end

  # Converts a position (float from old data or integer from new) to integer.
  defp pos_to_int(pos) when is_integer(pos), do: pos
  defp pos_to_int(pos) when is_float(pos), do: round(pos * 1_000_000_000)

  # Rebalances all positions with even spacing. Called when LINSERT runs out
  # of room between two adjacent positions. Deletes old compound keys and
  # re-inserts with evenly spaced integer positions.
  defp rebalance_positions(key, store, sorted) do
    count = length(sorted)
    values = Enum.map(sorted, fn {_pos, val} -> val end)

    # Delete all old entries
    Enum.each(sorted, fn {pos, _val} ->
      Ops.compound_delete(store, key, CompoundKey.list_element(key, pos))
    end)

    # Re-insert with evenly spaced positions
    new_sorted =
      Enum.with_index(values)
      |> Enum.map(fn {val, idx} ->
        new_pos = idx * @position_step
        Ops.compound_put(store, key, CompoundKey.list_element(key, new_pos), val, 0)
        {new_pos, val}
      end)

    # Update metadata
    {min_pos, _} = hd(new_sorted)
    {max_pos, _} = List.last(new_sorted)
    write_meta(key, store, {count, min_pos - @position_step, max_pos + @position_step})

    new_sorted
  end

  defp update_meta_from_remaining(key, store, new_len, remaining) do
    {min_pos, _} = hd(remaining)
    {max_pos, _} = List.last(remaining)
    write_meta(key, store, {new_len, min_pos - @position_step, max_pos + @position_step})
  end

  # LPUSH
  defp do_execute(key, store, nil, {:lpush, new_elements}),
    do: do_lpush_new(key, store, new_elements)

  defp do_execute(key, store, {len, left_pos, right_pos}, {:lpush, new_elements}) do
    reversed = Enum.reverse(new_elements)
    count = length(reversed)

    # reversed=[c,b,a]. Assign: c at left_pos-(count-1)*step, b at left_pos-(count-2)*step, a at left_pos
    new_left = left_pos - (count - 1) * @position_step - @position_step
    new_len = len + length(new_elements)

    writes =
      Enum.map(Enum.with_index(reversed), fn {elem, idx} ->
        pos = left_pos - (count - 1 - idx) * @position_step
        {pos, elem}
      end)

    with :ok <- put_elements(key, store, writes),
         :ok <- write_meta(key, store, {new_len, new_left, right_pos}) do
      new_len
    end
  end

  # RPUSH
  defp do_execute(key, store, nil, {:rpush, new_elements}),
    do: do_rpush_new(key, store, new_elements)

  defp do_execute(key, store, {len, left_pos, right_pos}, {:rpush, new_elements}) do
    writes =
      Enum.map(Enum.with_index(new_elements), fn {elem, idx} ->
        {right_pos + idx * @position_step, elem}
      end)

    new_right = right_pos + length(new_elements) * @position_step

    new_len = len + length(new_elements)

    with :ok <- put_elements(key, store, writes),
         :ok <- write_meta(key, store, {new_len, left_pos, new_right}) do
      new_len
    end
  end

  # LPOP
  defp do_execute(_key, _store, nil, {:lpop, _count}), do: nil
  defp do_execute(_key, _store, {0, _, _}, {:lpop, _count}), do: nil

  defp do_execute(key, store, {len, _, _}, {:lpop, count}) do
    sorted = sorted_elements(key, store)

    if sorted == [] do
      nil
    else
      actual_count = min(count, length(sorted))
      {to_pop, remaining} = Enum.split(sorted, actual_count)

      popped_values = Enum.map(to_pop, fn {_, val} -> val end)

      with :ok <- delete_elements(key, store, to_pop),
           :ok <- update_or_delete_meta(key, store, len - actual_count, remaining) do
        case count do
          1 -> List.first(popped_values)
          _ -> popped_values
        end
      end
    end
  end

  # RPOP
  defp do_execute(_key, _store, nil, {:rpop, _count}), do: nil
  defp do_execute(_key, _store, {0, _, _}, {:rpop, _count}), do: nil

  defp do_execute(key, store, {len, _, _}, {:rpop, count}) do
    sorted = sorted_elements(key, store)

    if sorted == [] do
      nil
    else
      total = length(sorted)
      actual_count = min(count, total)
      {remaining, to_pop} = Enum.split(sorted, total - actual_count)

      popped_values = to_pop |> Enum.map(fn {_, val} -> val end) |> Enum.reverse()

      with :ok <- delete_elements(key, store, to_pop),
           :ok <- update_or_delete_meta(key, store, len - actual_count, remaining) do
        case count do
          1 -> List.first(popped_values)
          _ -> popped_values
        end
      end
    end
  end

  # LRANGE
  defp do_execute(_key, _store, nil, {:lrange, _, _}), do: []

  defp do_execute(key, store, {len, _, _}, {:lrange, start, stop}) do
    ns = normalize_index(start, len)
    ne = normalize_index(stop, len)

    cond do
      ns > ne -> []
      ns >= len -> []
      true -> ordered_values(key, store) |> Enum.slice(ns..ne//1)
    end
  end

  # LLEN
  defp do_execute(_key, _store, nil, :llen), do: 0
  defp do_execute(_key, _store, {len, _, _}, :llen), do: len

  # LINDEX
  defp do_execute(_key, _store, nil, {:lindex, _}), do: nil

  defp do_execute(key, store, {len, _, _}, {:lindex, index}) do
    if index < 0 and len + index < 0 do
      nil
    else
      norm = normalize_index(index, len)
      if norm >= 0 and norm < len, do: ordered_values(key, store) |> Enum.at(norm), else: nil
    end
  end

  # LSET
  defp do_execute(_key, _store, nil, {:lset, _, _}), do: {:error, "ERR no such key"}

  defp do_execute(key, store, {len, _, _}, {:lset, index, element}) do
    norm = normalize_index(index, len)

    if norm >= 0 and norm < len do
      {old_pos, _} = sorted_elements(key, store) |> Enum.at(norm)
      Ops.compound_put(store, key, CompoundKey.list_element(key, old_pos), element, 0)
      :ok
    else
      {:error, "ERR index out of range"}
    end
  end

  # LREM
  defp do_execute(_key, _store, nil, {:lrem, _, _}), do: 0

  defp do_execute(key, store, {len, _, _}, {:lrem, count, element}) do
    sorted = sorted_elements(key, store)
    {to_remove, remaining, removed_count} = select_removals(sorted, count, element)

    cond do
      removed_count == 0 ->
        0

      remaining == [] ->
        with :ok <- delete_elements(key, store, to_remove),
             :ok <- delete_meta(key, store) do
          removed_count
        end

      true ->
        with :ok <- delete_elements(key, store, to_remove),
             :ok <- update_meta_from_remaining(key, store, len - removed_count, remaining) do
          removed_count
        end
    end
  end

  # LTRIM
  defp do_execute(_key, _store, nil, {:ltrim, _, _}), do: :ok

  defp do_execute(key, store, {len, _, _}, {:ltrim, start, stop}) do
    ns = normalize_index(start, len)
    ne = normalize_index(stop, len)
    sorted = sorted_elements(key, store)

    {to_keep, to_delete} =
      cond do
        ns > ne ->
          {[], sorted}

        ns >= len ->
          {[], sorted}

        true ->
          kept = Enum.slice(sorted, ns..ne//1)
          ks = MapSet.new(kept, fn {p, _} -> p end)
          {kept, Enum.reject(sorted, fn {p, _} -> MapSet.member?(ks, p) end)}
      end

    with :ok <- delete_elements(key, store, to_delete) do
      if to_keep == [] do
        delete_meta(key, store)
      else
        {mp, _} = hd(to_keep)
        {xp, _} = List.last(to_keep)
        write_meta(key, store, {length(to_keep), mp - @position_step, xp + @position_step})
      end
    end
  end

  # LPOS
  defp do_execute(_key, _store, nil, {:lpos, _, _, _, _}), do: nil

  defp do_execute(key, store, {_, _, _}, {:lpos, element, rank, count, maxlen}) do
    find_positions(ordered_values(key, store), element, rank, count, maxlen)
  end

  # LINSERT
  defp do_execute(_key, _store, nil, {:linsert, _, _, _}), do: 0

  defp do_execute(key, store, {len, left_pos, right_pos}, {:linsert, direction, pivot, element}) do
    sorted = sorted_elements(key, store)
    values = Enum.map(sorted, fn {_, val} -> val end)

    case Enum.find_index(values, &(&1 == pivot)) do
      nil ->
        -1

      idx ->
        new_pos =
          case direction do
            :before ->
              if idx == 0 do
                pos_to_int(elem(hd(sorted), 0)) - @position_step
              else
                a = pos_to_int(elem(Enum.at(sorted, idx - 1), 0))
                b = pos_to_int(elem(Enum.at(sorted, idx), 0))
                mid = div(a + b, 2)

                if mid == a or mid == b do
                  rebalanced = rebalance_positions(key, store, sorted)
                  a2 = pos_to_int(elem(Enum.at(rebalanced, idx - 1), 0))
                  b2 = pos_to_int(elem(Enum.at(rebalanced, idx), 0))
                  div(a2 + b2, 2)
                else
                  mid
                end
              end

            :after ->
              if idx == length(sorted) - 1 do
                pos_to_int(elem(List.last(sorted), 0)) + @position_step
              else
                a = pos_to_int(elem(Enum.at(sorted, idx), 0))
                b = pos_to_int(elem(Enum.at(sorted, idx + 1), 0))
                mid = div(a + b, 2)

                if mid == a or mid == b do
                  rebalanced = rebalance_positions(key, store, sorted)
                  a2 = pos_to_int(elem(Enum.at(rebalanced, idx), 0))
                  b2 = pos_to_int(elem(Enum.at(rebalanced, idx + 1), 0))
                  div(a2 + b2, 2)
                else
                  mid
                end
              end
          end

        Ops.compound_put(store, key, CompoundKey.list_element(key, new_pos), element, 0)

        write_meta(
          key,
          store,
          {len + 1, min(left_pos, new_pos - @position_step),
           max(right_pos, new_pos + @position_step)}
        )

        len + 1
    end
  end

  defp do_execute(_, _, _, {:lmove, _, _, _}),
    do: {:error, "ERR lmove must be handled at the store layer"}

  # pop_for_move
  defp do_execute(_key, _store, nil, {:pop_for_move, _}), do: nil
  defp do_execute(_key, _store, {0, _, _}, {:pop_for_move, _}), do: nil

  defp do_execute(key, store, {len, _, _}, {:pop_for_move, dir}) do
    sorted = sorted_elements(key, store)

    if sorted == [] do
      nil
    else
      {pos, element} =
        case dir do
          :left -> hd(sorted)
          :right -> List.last(sorted)
        end

      remaining = Enum.reject(sorted, fn {p, _} -> p == pos end)

      with :ok <- delete_elements(key, store, [{pos, element}]),
           :ok <- update_or_delete_meta(key, store, len - 1, remaining) do
        element
      end
    end
  end

  # LPUSHX / RPUSHX
  defp do_execute(_, _, nil, {:lpushx, _}), do: 0

  defp do_execute(key, store, meta, {:lpushx, elems}),
    do: do_execute(key, store, meta, {:lpush, elems})

  defp do_execute(_, _, nil, {:rpushx, _}), do: 0

  defp do_execute(key, store, meta, {:rpushx, elems}),
    do: do_execute(key, store, meta, {:rpush, elems})

  defp do_lpush_new(key, store, elements) do
    reversed = Enum.reverse(elements)
    count = length(reversed)
    # reversed=[c,b,a] for LPUSH key a b c. c should be leftmost (smallest pos).
    # Assign: c at -(count-1)*step, b at -(count-2)*step, ..., a at 0.0
    writes =
      Enum.map(Enum.with_index(reversed), fn {elem, idx} ->
        pos = @initial_position - (count - 1 - idx) * @position_step
        {pos, elem}
      end)

    min_a = @initial_position - (count - 1) * @position_step

    with :ok <- put_elements(key, store, writes),
         :ok <-
           write_meta(
             key,
             store,
             {count, min_a - @position_step, @initial_position + @position_step}
           ) do
      count
    end
  end

  defp do_rpush_new(key, store, elements) do
    count = length(elements)

    writes =
      Enum.map(Enum.with_index(elements), fn {elem, idx} ->
        {@initial_position + idx * @position_step, elem}
      end)

    max_a = @initial_position + (count - 1) * @position_step

    with :ok <- put_elements(key, store, writes),
         :ok <-
           write_meta(
             key,
             store,
             {count, @initial_position - @position_step, max_a + @position_step}
           ) do
      count
    end
  end

  defp put_elements(key, store, writes) do
    Enum.reduce_while(writes, :ok, fn {pos, elem}, :ok ->
      result =
        store
        |> Ops.compound_put(key, CompoundKey.list_element(key, pos), elem, 0)
        |> write_result()

      case result do
        :ok -> {:cont, :ok}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  defp delete_elements(key, store, elements) do
    Enum.reduce_while(elements, :ok, fn {pos, _value}, :ok ->
      result =
        store
        |> Ops.compound_delete(key, CompoundKey.list_element(key, pos))
        |> write_result()

      case result do
        :ok -> {:cont, :ok}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  defp update_or_delete_meta(key, store, _new_len, []), do: delete_meta(key, store)

  defp update_or_delete_meta(key, store, new_len, remaining),
    do: update_meta_from_remaining(key, store, new_len, remaining)

  defp write_result(:ok), do: :ok
  defp write_result(true), do: :ok
  defp write_result({:error, _} = error), do: error

  defp normalize_index(index, len) when index < 0, do: max(0, len + index)
  defp normalize_index(index, _len), do: index

  defp select_removals(sorted, 0, target) do
    {removed, kept} = Enum.split_with(sorted, fn {_, val} -> val == target end)
    {removed, kept, length(removed)}
  end

  defp select_removals(sorted, count, target) when count > 0,
    do: remove_n_from_head(sorted, count, target)

  defp select_removals(sorted, count, target) when count < 0 do
    {removed, remaining_rev, n} = remove_n_from_head(Enum.reverse(sorted), abs(count), target)
    {removed, Enum.reverse(remaining_rev), n}
  end

  defp remove_n_from_head(sorted, max_remove, target) do
    {removed, remaining, _} =
      Enum.reduce(sorted, {[], [], max_remove}, fn {_, val} = entry,
                                                   {rem_acc, keep_acc, budget} ->
        if val == target and budget > 0,
          do: {[entry | rem_acc], keep_acc, budget - 1},
          else: {rem_acc, [entry | keep_acc], budget}
      end)

    {Enum.reverse(removed), Enum.reverse(remaining), length(removed)}
  end

  defp find_positions(elements, element, rank, count, maxlen) do
    {scan_list, reverse?} =
      if rank < 0, do: {Enum.reverse(elements), true}, else: {elements, false}

    abs_rank = abs(rank)
    eff = if maxlen == 0, do: length(scan_list), else: min(maxlen, length(scan_list))
    total_len = length(elements)

    matches =
      Enum.take(scan_list, eff)
      |> Enum.with_index()
      |> Enum.filter(fn {e, _} -> e == element end)
      |> Enum.map(fn {_, idx} -> if reverse?, do: total_len - 1 - idx, else: idx end)

    from_rank = Enum.drop(matches, abs_rank - 1)

    case count do
      nil ->
        case from_rank do
          [pos | _] -> pos
          [] -> nil
        end

      0 ->
        from_rank

      n ->
        Enum.take(from_rank, n)
    end
  end
end
