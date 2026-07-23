defmodule Ferricstore.Flow.RecordQuery do
  @moduledoc false

  @default_auto_partition_candidate_limit 10_000

  def fetch_count(count, nil, nil, _scan_count_fun), do: count

  def fetch_count(count, _from_ms, _to_ms, scan_count_fun) when is_function(scan_count_fun, 1),
    do: scan_count_fun.(count)

  def filter_by_ms(records, from_ms, to_ms) do
    Enum.filter(records, fn record ->
      updated_at_ms = Map.get(record, :updated_at_ms, 0)
      ms_after?(updated_at_ms, from_ms) and ms_before?(updated_at_ms, to_ms)
    end)
  end

  def sort_by_update(records) do
    Enum.sort_by(records, fn record ->
      {Map.get(record, :updated_at_ms, 0), Map.get(record, :id, "")}
    end)
  end

  def maybe_reverse(records, true), do: Enum.reverse(records)
  def maybe_reverse(records, false), do: records

  def bounded_auto_partition_records(_partitions, count, _reverse?, _fetch_fun)
      when count <= 0,
      do: {:ok, []}

  def bounded_auto_partition_records(partitions, count, reverse?, fetch_fun)
      when is_list(partitions) and is_integer(count) and is_boolean(reverse?) and
             is_function(fetch_fun, 2) do
    candidate_limit = auto_partition_candidate_limit()

    if count > candidate_limit do
      auto_partition_limit_error(candidate_limit)
    else
      partitions
      |> Enum.reduce_while({:ok, [], 0}, fn partition, {:ok, ranked, candidate_count} ->
        fetch_count = min(count, max(candidate_limit - candidate_count + 1, 1))

        case fetch_fun.(partition, fetch_count) do
          {:ok, records} when is_list(records) ->
            next_candidate_count = candidate_count + length(records)

            if next_candidate_count > candidate_limit do
              {:halt, auto_partition_limit_error(candidate_limit)}
            else
              ranked_records =
                records
                |> sort_by_update()
                |> maybe_reverse(reverse?)

              next_ranked = merge_ranked(ranked, ranked_records, count, reverse?)
              {:cont, {:ok, next_ranked, next_candidate_count}}
            end

          {:error, _reason} = error ->
            {:halt, error}

          invalid ->
            {:halt, {:error, {:invalid_flow_candidate_fetch, invalid}}}
        end
      end)
      |> case do
        {:ok, ranked, _candidate_count} -> {:ok, ranked}
        {:error, _reason} = error -> error
      end
    end
  end

  def bounded_auto_partition_filtered_records(
        _partitions,
        count,
        _reverse?,
        _fetch_fun
      )
      when count <= 0,
      do: {:ok, []}

  def bounded_auto_partition_filtered_records(partitions, count, reverse?, fetch_fun)
      when is_list(partitions) and is_integer(count) and is_boolean(reverse?) and
             is_function(fetch_fun, 3) do
    candidate_limit = auto_partition_candidate_limit()

    bounded_filtered_records(
      partitions,
      count,
      reverse?,
      candidate_limit,
      fetch_fun,
      &auto_partition_limit_error/1
    )
  end

  def bounded_filtered_records(_sources, count, _reverse?, _candidate_limit, _fetch_fun)
      when count <= 0,
      do: {:ok, []}

  def bounded_filtered_records(sources, count, reverse?, candidate_limit, fetch_fun)
      when is_list(sources) and is_integer(count) and is_boolean(reverse?) and
             is_integer(candidate_limit) and candidate_limit > 0 and is_function(fetch_fun, 3) do
    bounded_filtered_records(
      sources,
      count,
      reverse?,
      candidate_limit,
      fetch_fun,
      &candidate_limit_error/1
    )
  end

  def prepend_chunk(chunk, chunks), do: [chunk | chunks]
  def flatten_chunks(chunks), do: Enum.flat_map(chunks, & &1)

  @spec merge_ordered_record_chunks([[map()]], non_neg_integer(), boolean()) :: [map()]
  def merge_ordered_record_chunks(_chunks, count, _reverse?) when count <= 0, do: []
  def merge_ordered_record_chunks([], _count, _reverse?), do: []

  def merge_ordered_record_chunks(chunks, count, reverse?)
      when is_list(chunks) and is_integer(count) and is_boolean(reverse?) do
    heap =
      chunks
      |> Enum.with_index()
      |> Enum.reduce(:gb_sets.empty(), fn
        {[], _source}, heap ->
          heap

        {[record | rest], source}, heap ->
          :gb_sets.add(ordered_heap_entry(record, rest, source, reverse?), heap)
      end)

    take_ordered_records(heap, count, reverse?, MapSet.new(), [])
  end

  defp take_ordered_records(_heap, 0, _reverse?, _seen, acc), do: Enum.reverse(acc)

  defp take_ordered_records(heap, remaining, reverse?, seen, acc) do
    if :gb_sets.is_empty(heap) do
      Enum.reverse(acc)
    else
      {{_rank, _source_order, source, record, rest}, heap} =
        if reverse?, do: :gb_sets.take_largest(heap), else: :gb_sets.take_smallest(heap)

      heap =
        case rest do
          [] -> heap
          [next | tail] -> :gb_sets.add(ordered_heap_entry(next, tail, source, reverse?), heap)
        end

      id = Map.get(record, :id)

      if MapSet.member?(seen, id) do
        take_ordered_records(heap, remaining, reverse?, seen, acc)
      else
        take_ordered_records(
          heap,
          remaining - 1,
          reverse?,
          MapSet.put(seen, id),
          [record | acc]
        )
      end
    end
  end

  defp ordered_heap_entry(record, rest, source, reverse?) do
    source_order = if reverse?, do: -source, else: source
    {update_rank(record), source_order, source, record, rest}
  end

  defp auto_partition_candidate_limit do
    case Application.get_env(
           :ferricstore,
           :flow_auto_partition_candidate_limit,
           @default_auto_partition_candidate_limit
         ) do
      limit when is_integer(limit) and limit > 0 -> limit
      _invalid -> @default_auto_partition_candidate_limit
    end
  end

  defp auto_partition_limit_error(limit),
    do: {:error, "ERR flow auto-partition query candidate limit exceeded (#{limit})"}

  defp candidate_limit_error(limit),
    do: {:error, "ERR flow query candidate limit exceeded (#{limit})"}

  defp bounded_filtered_records(
         sources,
         count,
         reverse?,
         candidate_limit,
         fetch_fun,
         limit_error_fun
       ) do
    if count > candidate_limit do
      limit_error_fun.(candidate_limit)
    else
      sources
      |> Enum.reduce_while({:ok, [], 0}, fn source, {:ok, ranked, scanned_count} ->
        scan_budget = candidate_limit - scanned_count

        if scan_budget <= 0 do
          {:halt, limit_error_fun.(candidate_limit)}
        else
          fetch_count = min(count, scan_budget)

          case fetch_fun.(source, fetch_count, scan_budget) do
            {:ok, records, source_scanned}
            when is_list(records) and is_integer(source_scanned) and source_scanned >= 0 ->
              next_scanned_count = scanned_count + source_scanned

              if next_scanned_count > candidate_limit do
                {:halt, limit_error_fun.(candidate_limit)}
              else
                ranked_records =
                  records
                  |> sort_by_update()
                  |> maybe_reverse(reverse?)

                next_ranked = merge_ranked(ranked, ranked_records, count, reverse?)
                {:cont, {:ok, next_ranked, next_scanned_count}}
              end

            {:error, _reason} = error ->
              {:halt, error}

            invalid ->
              {:halt, {:error, {:invalid_flow_candidate_scan, invalid}}}
          end
        end
      end)
      |> case do
        {:ok, ranked, _scanned_count} -> {:ok, ranked}
        {:error, _reason} = error -> error
      end
    end
  end

  defp merge_ranked(left, right, count, reverse?) do
    left
    |> do_merge_ranked(right, count, reverse?, [])
    |> Enum.reverse()
  end

  defp do_merge_ranked(_left, _right, 0, _reverse?, acc), do: acc
  defp do_merge_ranked([], right, count, _reverse?, acc), do: prepend_ranked(right, count, acc)
  defp do_merge_ranked(left, [], count, _reverse?, acc), do: prepend_ranked(left, count, acc)

  defp do_merge_ranked(
         [left | left_rest] = left_records,
         [right | right_rest] = right_records,
         count,
         reverse?,
         acc
       ) do
    if ranked_before?(left, right, reverse?) do
      do_merge_ranked(left_rest, right_records, count - 1, reverse?, [left | acc])
    else
      do_merge_ranked(left_records, right_rest, count - 1, reverse?, [right | acc])
    end
  end

  defp prepend_ranked(records, count, acc) do
    records
    |> Enum.take(count)
    |> Enum.reverse(acc)
  end

  defp ranked_before?(left, right, false), do: update_rank(left) <= update_rank(right)
  defp ranked_before?(left, right, true), do: update_rank(left) >= update_rank(right)

  defp update_rank(record),
    do: {Map.get(record, :updated_at_ms, 0), Map.get(record, :id, "")}

  defp ms_after?(_event_ms, nil), do: true
  defp ms_after?(event_ms, from_ms), do: event_ms >= from_ms

  defp ms_before?(_event_ms, nil), do: true
  defp ms_before?(event_ms, to_ms), do: event_ms <= to_ms
end
