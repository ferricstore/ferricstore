defmodule Ferricstore.Store.ListOps do
  @moduledoc """
  Pure-logic module for list data structure operations.

  ## Storage format (compound key / float-position)

  Each list element is stored as an individual compound key entry:
  `L:redis_key\\0{encoded_position} -> element_value`

  A metadata key stores length and position boundaries:
  `LM:redis_key -> Erlang term metadata`
  """

  alias Ferricstore.Store.CompoundKey
  alias Ferricstore.Store.Ops
  alias Ferricstore.Store.ReadResult
  alias Ferricstore.TermCodec

  # Integer positions with large gaps. LPUSH/RPUSH decrement/increment by @position_step.
  # LINSERT picks the integer midpoint. When no room (adjacent positions differ by 1),
  # rebalance redistributes all positions with even spacing.
  @initial_position 0
  @position_step 1_000_000_000
  @max_encoded_meta_bytes 128

  @doc false
  @spec read_operation?(term()) :: boolean()
  def read_operation?(:llen), do: true
  def read_operation?({:lrange, _start, _stop}), do: true
  def read_operation?({:lindex, _index}), do: true
  def read_operation?({:lpos, _element, _rank, _count, _maxlen}), do: true
  def read_operation?(_operation), do: false

  @spec execute(binary(), map(), term()) :: term()
  def execute(key, store, operation) do
    case read_meta(key, store) do
      {:error, _} = error -> error
      meta -> do_execute(key, store, meta, operation)
    end
  end

  @spec execute_lmove(binary(), binary(), map(), :left | :right, :left | :right) ::
          binary() | nil | {:error, term()}
  def execute_lmove(src_key, dst_key, store, from_dir, to_dir) when is_map(store) do
    src_meta = read_meta(src_key, store)

    case src_meta do
      nil ->
        nil

      {0, _, _} ->
        nil

      {_len, _left, _right} = src_meta ->
        with {:ok, sorted} <- sorted_elements(src_key, store),
             dst_meta when not is_tuple(dst_meta) or tuple_size(dst_meta) == 3 <-
               lmove_destination_meta(src_key, dst_key, src_meta, store) do
          if sorted == [] do
            nil
          else
            {pos, element} =
              case from_dir do
                :left -> hd(sorted)
                :right -> List.last(sorted)
              end

            remaining = Enum.reject(sorted, fn {p, _} -> p == pos end)

            if src_key == dst_key and (from_dir == to_dir or remaining == []) do
              element
            else
              with :ok <-
                     remove_lmove_source(
                       src_key,
                       store,
                       pos,
                       element,
                       src_meta,
                       remaining
                     ) do
                effective_dst_meta =
                  if src_key == dst_key, do: lmove_meta_from_elements(remaining), else: dst_meta

                case push_moved_element(dst_key, store, element, effective_dst_meta, to_dir) do
                  :ok ->
                    element

                  {:error, _} = error ->
                    rollback_lmove_source_result(
                      src_key,
                      store,
                      pos,
                      element,
                      src_meta,
                      error
                    )
                end
              end
            end
          end
        end

      {:error, _} = error ->
        error
    end
  end

  defp lmove_destination_meta(src_key, src_key, src_meta, _store), do: src_meta
  defp lmove_destination_meta(_src_key, dst_key, _src_meta, store), do: read_meta(dst_key, store)

  defp remove_lmove_source(src_key, store, pos, _element, _src_meta, []) do
    compound_keys = [
      CompoundKey.list_element(src_key, pos),
      CompoundKey.list_meta_key(src_key)
    ]

    store
    |> Ops.compound_batch_delete(src_key, compound_keys)
    |> write_result()
  end

  defp remove_lmove_source(src_key, store, pos, element, src_meta, remaining) do
    with :ok <- delete_elements(src_key, store, [{pos, element}]) do
      case write_meta(src_key, store, lmove_meta_from_elements(remaining)) do
        :ok ->
          :ok

        {:error, _} = error ->
          rollback_lmove_source_result(src_key, store, pos, element, src_meta, error)
      end
    end
  end

  defp lmove_meta_from_elements([]), do: nil

  defp lmove_meta_from_elements(elements) do
    {min_pos, _value} = hd(elements)
    {max_pos, _value} = List.last(elements)
    {length(elements), min_pos - @position_step, max_pos + @position_step}
  end

  defp rollback_lmove_source(src_key, store, pos, element, src_meta) do
    entries = [
      {CompoundKey.list_element(src_key, pos), element, 0},
      {CompoundKey.list_meta_key(src_key), encode_meta(src_meta), 0}
    ]

    store
    |> Ops.compound_batch_put(src_key, entries)
    |> write_result()
  end

  defp rollback_lmove_source_result(src_key, store, pos, element, src_meta, write_error) do
    case rollback_lmove_source(src_key, store, pos, element, src_meta) do
      :ok ->
        write_error

      {:error, _} = rollback_error ->
        {:error, {:lmove_source_rollback_failed, write_error, rollback_error}}
    end
  end

  defp push_moved_element(dst_key, store, element, nil, _to_dir) do
    new_pos = @initial_position

    put_moved_element_and_meta(
      dst_key,
      store,
      {new_pos, element},
      {1, new_pos - @position_step, new_pos + @position_step}
    )
  end

  defp push_moved_element(dst_key, store, element, {dst_len, dst_left, dst_right}, to_dir) do
    new_pos =
      case to_dir do
        :left -> dst_left
        :right -> dst_right
      end

    new_left = if to_dir == :left, do: new_pos - @position_step, else: dst_left
    new_right = if to_dir == :right, do: new_pos + @position_step, else: dst_right

    put_moved_element_and_meta(
      dst_key,
      store,
      {new_pos, element},
      {dst_len + 1, new_left, new_right}
    )
  end

  defp put_moved_element_and_meta(key, store, {pos, element}, meta) do
    entries = [
      {CompoundKey.list_element(key, pos), element, 0},
      {CompoundKey.list_meta_key(key), encode_meta(meta), 0}
    ]

    store
    |> Ops.compound_batch_put(key, entries)
    |> write_result()
  end

  @doc false
  def read_meta(key, store) do
    meta_key = CompoundKey.list_meta_key(key)

    case Ops.compound_get(store, key, meta_key) do
      nil ->
        nil

      {:error, {:storage_read_failed, _reason}} = failure ->
        ReadResult.command_error(failure)

      binary ->
        case decode_meta(binary) do
          nil -> {:error, "ERR storage read failed"}
          meta -> meta
        end
    end
  end

  @doc false
  def encode_meta(meta), do: TermCodec.encode(meta)

  @doc false
  def decode_meta(binary)
      when is_binary(binary) and byte_size(binary) <= @max_encoded_meta_bytes do
    case TermCodec.decode(binary) do
      {:ok, {len, left_pos, right_pos}}
      when is_integer(len) and len >= 0 and is_integer(left_pos) and is_integer(right_pos) ->
        {len, left_pos, right_pos}

      _ ->
        nil
    end
  rescue
    _ -> nil
  end

  def decode_meta(_), do: nil

  defp write_meta(key, store, {_len, _left, _right} = meta) do
    Ops.compound_put(store, key, CompoundKey.list_meta_key(key), encode_meta(meta), 0)
    |> write_result()
  end

  defp delete_meta(key, store) do
    Ops.compound_delete(store, key, CompoundKey.list_meta_key(key))
    |> write_result()
  end

  defp sorted_elements(key, store) do
    prefix = CompoundKey.list_prefix(key)

    case Ops.compound_scan(store, key, prefix) do
      {:error, {:storage_read_failed, _reason}} = failure ->
        ReadResult.command_error(failure)

      pairs when is_list(pairs) ->
        decode_position_pairs(pairs)
    end
  end

  defp ordered_values(key, store) do
    with {:ok, sorted} <- sorted_elements(key, store) do
      {:ok, Enum.map(sorted, fn {_pos, value} -> value end)}
    end
  end

  defp sorted_slice(key, store, start, count, total) do
    prefix = CompoundKey.list_prefix(key)

    case Ops.compound_scan_slice(store, key, prefix, start, count, total) do
      {:error, {:storage_read_failed, _reason}} = failure ->
        ReadResult.command_error(failure)

      pairs when is_list(pairs) ->
        decode_position_pairs(pairs)
    end
  end

  defp decode_position_pairs(pairs) do
    Enum.reduce_while(pairs, {:ok, []}, fn
      {encoded_pos, value}, {:ok, decoded} ->
        case CompoundKey.decode_position_safe(encoded_pos) do
          {:ok, position} -> {:cont, {:ok, [{position, value} | decoded]}}
          :error -> {:halt, corrupt_position_error()}
        end

      _malformed_pair, _decoded ->
        {:halt, corrupt_position_error()}
    end)
    |> case do
      {:ok, decoded} -> {:ok, Enum.reverse(decoded)}
      {:error, _reason} = error -> error
    end
  end

  defp corrupt_position_error do
    :corrupt_list_position
    |> ReadResult.failure()
    |> ReadResult.command_error()
  end

  defp ordered_slice(key, store, start, count, total) do
    with {:ok, sorted} <- sorted_slice(key, store, start, count, total) do
      {:ok, Enum.map(sorted, fn {_pos, value} -> value end)}
    end
  end

  defp single_boundary_values(key, store, left_pos) do
    single_position_values(key, store, left_pos + @position_step)
  end

  defp single_position_values(key, store, pos) do
    compound_key = CompoundKey.list_element(key, pos)

    case Ops.compound_get(store, key, compound_key) do
      nil ->
        ordered_values(key, store)

      {:error, {:storage_read_failed, _reason}} = failure ->
        ReadResult.command_error(failure)

      value ->
        {:ok, [value]}
    end
  end

  # Converts a position (float from old data or integer from new) to integer.
  defp pos_to_int(pos) when is_integer(pos), do: pos
  defp pos_to_int(pos) when is_float(pos), do: round(pos * 1_000_000_000)

  # Produces evenly spaced positions without mutating storage. The caller
  # commits the rewrite and the inserted element in one mixed batch.
  defp rebalance_positions(sorted) do
    values = Enum.map(sorted, fn {_pos, val} -> val end)

    Enum.with_index(values, fn val, idx -> {idx * @position_step, val} end)
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

    put_elements_and_write_meta(key, store, writes, {new_len, new_left, right_pos}, new_len)
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

    put_elements_and_write_meta(key, store, writes, {new_len, left_pos, new_right}, new_len)
  end

  # LPOP
  defp do_execute(_key, _store, nil, {:lpop, _count}), do: nil
  defp do_execute(_key, _store, {0, _, _}, {:lpop, _count}), do: nil
  defp do_execute(_key, _store, {_len, _left, _right}, {:lpop, 0}), do: []

  defp do_execute(key, store, {len, left_pos, right_pos}, {:lpop, 1}) do
    pop_single_boundary(key, store, len, left_pos + @position_step, :left, {left_pos, right_pos})
  end

  defp do_execute(key, store, {len, left_pos, right_pos}, {:lpop, count}) do
    pop_left_by_scan(key, store, len, count, {left_pos, right_pos})
  end

  # RPOP
  defp do_execute(_key, _store, nil, {:rpop, _count}), do: nil
  defp do_execute(_key, _store, {0, _, _}, {:rpop, _count}), do: nil
  defp do_execute(_key, _store, {_len, _left, _right}, {:rpop, 0}), do: []

  defp do_execute(key, store, {len, left_pos, right_pos}, {:rpop, 1}) do
    pop_single_boundary(
      key,
      store,
      len,
      right_pos - @position_step,
      :right,
      {left_pos, right_pos}
    )
  end

  defp do_execute(key, store, {len, left_pos, right_pos}, {:rpop, count}) do
    pop_right_by_scan(key, store, len, count, {left_pos, right_pos})
  end

  # LRANGE
  defp do_execute(_key, _store, nil, {:lrange, _, _}), do: []

  defp do_execute(key, store, {len, left_pos, _right_pos}, {:lrange, 0, 0}) when len > 0 do
    with {:ok, values} <- single_boundary_values(key, store, left_pos), do: values
  end

  defp do_execute(key, store, {len, _left_pos, right_pos}, {:lrange, -1, -1}) when len > 0 do
    with {:ok, values} <- single_position_values(key, store, right_pos - @position_step),
         do: values
  end

  defp do_execute(key, store, {1, left_pos, _right_pos}, {:lrange, start, stop}) do
    ns = normalize_range_start(start, 1)
    ne = normalize_range_stop(stop, 1)

    cond do
      ns > ne -> []
      ns >= 1 -> []
      true -> with {:ok, values} <- single_boundary_values(key, store, left_pos), do: values
    end
  end

  defp do_execute(key, store, {len, _, _}, {:lrange, start, stop}) do
    ns = normalize_range_start(start, len)
    ne = normalize_range_stop(stop, len)

    cond do
      ns > ne ->
        []

      ns >= len ->
        []

      true ->
        count = min(ne, len - 1) - ns + 1
        with {:ok, values} <- ordered_slice(key, store, ns, count, len), do: values
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
      norm = normalize_point_index(index, len)

      if norm >= 0 and norm < len do
        with {:ok, values} <- ordered_slice(key, store, norm, 1, len), do: List.first(values)
      else
        nil
      end
    end
  end

  # LSET
  defp do_execute(_key, _store, nil, {:lset, _, _}), do: {:error, "ERR no such key"}

  defp do_execute(key, store, {len, _, _}, {:lset, index, element}) do
    norm = normalize_point_index(index, len)

    if norm >= 0 and norm < len do
      with {:ok, [{old_pos, _old_value}]} <- sorted_slice(key, store, norm, 1, len) do
        store
        |> Ops.compound_put(key, CompoundKey.list_element(key, old_pos), element, 0)
        |> write_result()
      end
    else
      {:error, "ERR index out of range"}
    end
  end

  # LREM
  defp do_execute(_key, _store, nil, {:lrem, _, _}), do: 0

  defp do_execute(key, store, {len, _, _}, {:lrem, count, element}) do
    with {:ok, sorted} <- sorted_elements(key, store) do
      {to_remove, remaining, removed_count} = select_removals(sorted, count, element)

      cond do
        removed_count == 0 ->
          0

        remaining == [] ->
          with :ok <-
                 delete_elements_and_update_meta(key, store, to_remove, fn ->
                   delete_meta(key, store)
                 end) do
            removed_count
          end

        true ->
          with :ok <-
                 delete_elements_and_update_meta(key, store, to_remove, fn ->
                   update_meta_from_remaining(key, store, len - removed_count, remaining)
                 end) do
            removed_count
          end
      end
    end
  end

  # LTRIM
  defp do_execute(_key, _store, nil, {:ltrim, _, _}), do: :ok

  defp do_execute(key, store, {len, _, _}, {:ltrim, start, stop}) do
    ns = normalize_range_start(start, len)
    ne = normalize_range_stop(stop, len)

    with {:ok, sorted} <- sorted_elements(key, store) do
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

      delete_elements_and_update_meta(key, store, to_delete, fn ->
        if to_keep == [] do
          delete_meta(key, store)
        else
          {mp, _} = hd(to_keep)
          {xp, _} = List.last(to_keep)
          write_meta(key, store, {length(to_keep), mp - @position_step, xp + @position_step})
        end
      end)
    end
  end

  # LPOS
  defp do_execute(_key, _store, nil, {:lpos, _, _, _, _}), do: nil

  defp do_execute(key, store, {len, _, _}, {:lpos, element, rank, count, maxlen}) do
    scan_count = if maxlen == 0, do: len, else: min(maxlen, len)
    start = if rank < 0, do: len - scan_count, else: 0

    with {:ok, values} <- ordered_slice(key, store, start, scan_count, len) do
      find_positions(values, element, rank, count, start)
    end
  end

  # LINSERT
  defp do_execute(_key, _store, nil, {:linsert, _, _, _}), do: 0

  defp do_execute(key, store, {len, left_pos, right_pos}, {:linsert, direction, pivot, element}) do
    with {:ok, sorted} <- sorted_elements(key, store) do
      values = Enum.map(sorted, fn {_, val} -> val end)

      case Enum.find_index(values, &(&1 == pivot)) do
        nil ->
          -1

        idx ->
          case linsert_position(sorted, direction, idx) do
            {:ok, new_pos} ->
              put_one_element_and_write_meta(
                key,
                store,
                {new_pos, element},
                {len + 1, min(left_pos, new_pos - @position_step),
                 max(right_pos, new_pos + @position_step)},
                len + 1
              )

            {:rebalance, new_pos, rebalanced} ->
              commit_rebalanced_insert(key, store, sorted, rebalanced, new_pos, element, len + 1)
          end
      end
    end
  end

  defp do_execute(_, _, _, {:lmove, _, _, _}),
    do: {:error, "ERR lmove must be handled at the store layer"}

  # pop_for_move
  defp do_execute(_key, _store, nil, {:pop_for_move, _}), do: nil
  defp do_execute(_key, _store, {0, _, _}, {:pop_for_move, _}), do: nil

  defp do_execute(key, store, {len, _, _}, {:pop_for_move, dir}) do
    with {:ok, sorted} <- sorted_elements(key, store) do
      if sorted == [] do
        nil
      else
        {pos, element} =
          case dir do
            :left -> hd(sorted)
            :right -> List.last(sorted)
          end

        remaining = Enum.reject(sorted, fn {p, _} -> p == pos end)

        with :ok <-
               delete_elements_and_update_meta(key, store, [{pos, element}], fn ->
                 update_or_delete_meta(key, store, len - 1, remaining)
               end) do
          element
        end
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

  defp pop_single_boundary(key, store, len, pos, direction, {left_pos, right_pos}) do
    compound_key = CompoundKey.list_element(key, pos)

    case Ops.compound_get(store, key, compound_key) do
      nil ->
        case direction do
          :left -> pop_left_by_scan(key, store, len, 1, {left_pos, right_pos})
          :right -> pop_right_by_scan(key, store, len, 1, {left_pos, right_pos})
        end

      {:error, {:storage_read_failed, _reason}} = failure ->
        ReadResult.command_error(failure)

      value ->
        delete_result =
          if len == 1 do
            delete_elements_and_meta(key, store, [{pos, value}])
          else
            delete_elements_and_update_meta(key, store, [{pos, value}], fn ->
              update_single_pop_meta(key, store, len, pos, direction, {left_pos, right_pos})
            end)
          end

        with :ok <- delete_result do
          value
        end
    end
  end

  defp update_single_pop_meta(key, store, 1, _pos, _direction, _bounds),
    do: delete_meta(key, store)

  defp update_single_pop_meta(key, store, len, pos, :left, {_left_pos, right_pos}) do
    write_meta(key, store, {len - 1, pos, right_pos})
  end

  defp update_single_pop_meta(key, store, len, pos, :right, {left_pos, _right_pos}) do
    write_meta(key, store, {len - 1, left_pos, pos})
  end

  defp pop_left_by_scan(key, store, len, count, {_left_pos, right_pos}) do
    actual_count = min(count, len)

    with {:ok, to_pop} <- sorted_slice(key, store, 0, actual_count, len),
         :ok <- validate_pop_window(to_pop, actual_count) do
      {new_left, _value} = List.last(to_pop)
      popped_values = Enum.map(to_pop, fn {_pos, value} -> value end)

      delete_result =
        if actual_count == len do
          delete_elements_and_meta(key, store, to_pop)
        else
          delete_elements_and_update_meta(key, store, to_pop, fn ->
            write_meta(key, store, {len - actual_count, new_left, right_pos})
          end)
        end

      with :ok <- delete_result do
        if count == 1, do: List.first(popped_values), else: popped_values
      end
    end
  end

  defp pop_right_by_scan(key, store, len, count, {left_pos, _right_pos}) do
    actual_count = min(count, len)
    start = len - actual_count

    with {:ok, to_pop} <- sorted_slice(key, store, start, actual_count, len),
         :ok <- validate_pop_window(to_pop, actual_count) do
      {new_right, _value} = hd(to_pop)
      popped_values = to_pop |> Enum.map(fn {_pos, value} -> value end) |> Enum.reverse()

      delete_result =
        if actual_count == len do
          delete_elements_and_meta(key, store, to_pop)
        else
          delete_elements_and_update_meta(key, store, to_pop, fn ->
            write_meta(key, store, {len - actual_count, left_pos, new_right})
          end)
        end

      with :ok <- delete_result do
        if count == 1, do: List.first(popped_values), else: popped_values
      end
    end
  end

  defp validate_pop_window(window, expected_count) do
    if length(window) == expected_count, do: :ok, else: {:error, "ERR storage read failed"}
  end

  defp linsert_position(sorted, :before, 0) do
    {:ok, pos_to_int(elem(hd(sorted), 0)) - @position_step}
  end

  defp linsert_position(sorted, :before, idx) do
    midpoint_or_rebalance(sorted, idx - 1, idx)
  end

  defp linsert_position(sorted, :after, idx) when idx == length(sorted) - 1 do
    {:ok, pos_to_int(elem(List.last(sorted), 0)) + @position_step}
  end

  defp linsert_position(sorted, :after, idx) do
    midpoint_or_rebalance(sorted, idx, idx + 1)
  end

  defp midpoint_or_rebalance(sorted, left_idx, right_idx) do
    a = pos_to_int(elem(Enum.at(sorted, left_idx), 0))
    b = pos_to_int(elem(Enum.at(sorted, right_idx), 0))
    mid = div(a + b, 2)

    if mid == a or mid == b do
      rebalanced = rebalance_positions(sorted)
      a2 = pos_to_int(elem(Enum.at(rebalanced, left_idx), 0))
      b2 = pos_to_int(elem(Enum.at(rebalanced, right_idx), 0))
      {:rebalance, div(a2 + b2, 2), rebalanced}
    else
      {:ok, mid}
    end
  end

  defp commit_rebalanced_insert(key, store, old_sorted, rebalanced, new_pos, element, new_len) do
    {min_pos, _value} = hd(rebalanced)
    {max_pos, _value} = List.last(rebalanced)

    entries =
      Enum.map(rebalanced, fn {pos, value} ->
        {CompoundKey.list_element(key, pos), value, 0}
      end) ++
        [
          {CompoundKey.list_element(key, new_pos), element, 0},
          {CompoundKey.list_meta_key(key),
           encode_meta(
             {new_len, min(min_pos, new_pos) - @position_step,
              max(max_pos, new_pos) + @position_step}
           ), 0}
        ]

    compound_keys =
      Enum.map(old_sorted, fn {pos, _value} -> CompoundKey.list_element(key, pos) end)

    case Ops.compound_batch_mutate(store, key, compound_keys, entries) |> write_result() do
      :ok -> new_len
      {:error, _reason} = error -> error
    end
  end

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

    put_elements_and_write_meta(
      key,
      store,
      writes,
      {count, min_a - @position_step, @initial_position + @position_step},
      count
    )
  end

  defp do_rpush_new(key, store, elements) do
    count = length(elements)

    writes =
      Enum.map(Enum.with_index(elements), fn {elem, idx} ->
        {@initial_position + idx * @position_step, elem}
      end)

    max_a = @initial_position + (count - 1) * @position_step

    put_elements_and_write_meta(
      key,
      store,
      writes,
      {count, @initial_position - @position_step, max_a + @position_step},
      count
    )
  end

  defp put_elements_and_write_meta(key, store, writes, meta, success) do
    with :ok <- put_elements(key, store, writes) do
      case write_meta(key, store, meta) do
        :ok -> success
        {:error, _} = error -> rollback_written_elements(key, store, writes, error)
      end
    end
  end

  defp put_one_element_and_write_meta(key, store, {pos, element}, meta, success) do
    with :ok <-
           store
           |> Ops.compound_put(key, CompoundKey.list_element(key, pos), element, 0)
           |> write_result() do
      case write_meta(key, store, meta) do
        :ok -> success
        {:error, _} = error -> rollback_written_elements(key, store, [{pos, element}], error)
      end
    end
  end

  defp rollback_written_elements(key, store, writes, write_error) do
    case delete_elements(key, store, writes) do
      :ok ->
        write_error

      {:error, _} = rollback_error ->
        {:error, {:list_element_rollback_failed, write_error, rollback_error}}
    end
  end

  defp put_elements(key, store, writes) do
    entries =
      Enum.map(writes, fn {pos, elem} ->
        {CompoundKey.list_element(key, pos), elem, 0}
      end)

    store
    |> Ops.compound_batch_put(key, entries)
    |> write_result()
  end

  defp delete_elements(key, store, elements) do
    compound_keys =
      Enum.map(elements, fn {pos, _value} ->
        CompoundKey.list_element(key, pos)
      end)

    store
    |> Ops.compound_batch_delete(key, compound_keys)
    |> write_result()
  end

  defp delete_elements_and_meta(key, store, elements) do
    compound_keys =
      Enum.map(elements, fn {pos, _value} ->
        CompoundKey.list_element(key, pos)
      end)

    store
    |> Ops.compound_batch_delete(key, compound_keys ++ [CompoundKey.list_meta_key(key)])
    |> write_result()
  end

  defp delete_elements_and_update_meta(key, store, elements, update_fun) do
    with :ok <- delete_elements(key, store, elements) do
      case update_fun.() do
        :ok -> :ok
        {:error, _} = error -> rollback_deleted_elements(key, store, elements, error)
      end
    end
  end

  defp rollback_deleted_elements(key, store, elements, write_error) do
    case put_elements(key, store, elements) do
      :ok ->
        write_error

      {:error, _} = rollback_error ->
        {:error, {:list_delete_rollback_failed, write_error, rollback_error}}
    end
  end

  defp update_or_delete_meta(key, store, _new_len, []), do: delete_meta(key, store)

  defp update_or_delete_meta(key, store, new_len, remaining),
    do: update_meta_from_remaining(key, store, new_len, remaining)

  defp write_result(:ok), do: :ok
  defp write_result(true), do: :ok
  defp write_result({:error, _} = error), do: error

  defp normalize_range_start(index, len) when index < 0, do: max(0, len + index)
  defp normalize_range_start(index, _len), do: index

  defp normalize_range_stop(index, len) when index < 0, do: len + index
  defp normalize_range_stop(index, _len), do: index

  defp normalize_point_index(index, len) when index < 0, do: len + index
  defp normalize_point_index(index, _len), do: index

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

  defp find_positions(elements, element, rank, count, base_index) do
    {scan_list, reverse?} =
      if rank < 0, do: {Enum.reverse(elements), true}, else: {elements, false}

    abs_rank = abs(rank)
    slice_len = length(elements)

    matches =
      scan_list
      |> Enum.with_index()
      |> Enum.filter(fn {e, _} -> e == element end)
      |> Enum.map(fn {_, idx} ->
        if reverse?, do: base_index + slice_len - 1 - idx, else: base_index + idx
      end)

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
