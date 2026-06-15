defmodule Ferricstore.Flow.RecordRead do
  @moduledoc false

  alias Ferricstore.Flow.IndexMerge
  alias Ferricstore.Flow.IndexQuery
  alias Ferricstore.Flow.IndexZSet
  alias Ferricstore.Flow.Keys
  alias Ferricstore.Flow.LMDBIndexRead
  alias Ferricstore.Flow.RAMIndexRead
  alias Ferricstore.Flow.RecordLoader
  alias Ferricstore.Flow.RecordQuery
  alias Ferricstore.Flow.TerminalQuery
  alias Ferricstore.Store.Router

  def list_records(
        ctx,
        type,
        state,
        :auto,
        count,
        include_cold?,
        consistent?,
        terminal_states,
        scan_limit
      ) do
    cond do
      include_cold? or consistent? or state in terminal_states ->
        list_records_auto_scan(
          ctx,
          type,
          state,
          count,
          include_cold?,
          consistent?,
          terminal_states,
          scan_limit
        )

      true ->
        list_records_auto_hot(ctx, type, state, count)
    end
  end

  def list_records(
        ctx,
        type,
        state,
        partition_key,
        count,
        include_cold?,
        consistent?,
        terminal_states,
        scan_limit
      ) do
    index_key = Keys.state_index_key(type, state, partition_key)

    with :ok <- validate_key_size(index_key),
         {:ok, ids} <-
           index_ids(
             ctx,
             index_key,
             state,
             partition_key,
             count,
             include_cold?,
             consistent?,
             nil,
             terminal_states,
             scan_limit
           ) do
      records_for_ids(ctx, ids, partition_key)
    end
  end

  def terminal_records(
        ctx,
        type,
        state,
        :auto,
        count,
        include_cold?,
        consistent?,
        query,
        terminal_states,
        scan_limit
      ) do
    Keys.auto_partition_keys()
    |> Enum.reduce_while({:ok, []}, fn partition_key, {:ok, acc} ->
      case terminal_records(
             ctx,
             type,
             state,
             partition_key,
             count,
             include_cold?,
             consistent?,
             query,
             terminal_states,
             scan_limit
           ) do
        {:ok, records} -> {:cont, {:ok, RecordQuery.prepend_chunk(records, acc)}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, chunks} -> {:ok, RecordQuery.flatten_chunks(chunks)}
      {:error, _reason} = error -> error
    end
  end

  def terminal_records(
        ctx,
        type,
        "any",
        partition_key,
        count,
        include_cold?,
        consistent?,
        query,
        terminal_states,
        scan_limit
      ) do
    terminal_states
    |> Enum.reduce_while({:ok, []}, fn state, {:ok, acc} ->
      case terminal_ids(
             ctx,
             type,
             state,
             partition_key,
             count,
             include_cold?,
             consistent?,
             query,
             terminal_states,
             scan_limit
           ) do
        {:ok, ids} -> {:cont, {:ok, RecordQuery.prepend_chunk(ids, acc)}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, chunks} ->
        ids = TerminalQuery.ids_from_chunks(chunks, count, terminal_states)

        with {:ok, records} <- records_for_ids(ctx, ids, partition_key) do
          {:ok, TerminalQuery.filter_any(records, terminal_states)}
        end

      {:error, _reason} = error ->
        error
    end
  end

  def terminal_records(
        ctx,
        type,
        state,
        partition_key,
        count,
        include_cold?,
        consistent?,
        query,
        terminal_states,
        scan_limit
      ) do
    with {:ok, ids} <-
           terminal_ids(
             ctx,
             type,
             state,
             partition_key,
             count,
             include_cold?,
             consistent?,
             query,
             terminal_states,
             scan_limit
           ),
         {:ok, records} <- records_for_ids(ctx, ids, partition_key) do
      {:ok, TerminalQuery.filter_state(records, state)}
    end
  end

  def records_for_index(
        ctx,
        index_key,
        partition_key,
        query,
        include_cold?,
        consistent?,
        scan_limit
      ) do
    fetch_count =
      IndexQuery.fetch_count(query, fn count ->
        LMDBIndexRead.query_scan_count(count, scan_limit)
      end)

    with {:ok, ram_entries} <- RAMIndexRead.score_entries(ctx, index_key, query, fetch_count) do
      if include_cold? do
        with {:ok, lmdb_entries} <-
               LMDBIndexRead.query_entries(
                 ctx,
                 index_key,
                 partition_key,
                 fetch_count,
                 consistent?,
                 query,
                 scan_limit
               ) do
          ids =
            IndexMerge.ids_from_query_entries(ram_entries, lmdb_entries, fetch_count, query.rev?)

          records_for_ids(ctx, ids, partition_key)
        end
      else
        ids = Enum.map(ram_entries, fn {id, _score} -> id end)
        records_for_ids(ctx, ids, partition_key)
      end
    end
  end

  def filter_index_records(records, field, value, query, terminal_states) do
    records
    |> IndexQuery.filter_records(field, value, query, terminal_states)
    |> RecordQuery.sort_by_update()
    |> RecordQuery.maybe_reverse(query.rev?)
    |> Enum.take(query.count)
  end

  def root_record(ctx, root_flow_id, partition_key) do
    case RecordLoader.records_for_ids(ctx, [root_flow_id], partition_key) do
      {:ok, [%{root_flow_id: ^root_flow_id} = record | _rest]} -> {:ok, record}
      {:ok, _records} -> {:ok, nil}
      {:error, _reason} = error -> error
    end
  end

  def stuck_records(ctx, type, :auto, cutoff, count) do
    Keys.auto_partition_keys()
    |> Enum.reduce_while({:ok, []}, fn partition_key, {:ok, acc} ->
      case stuck_records(ctx, type, partition_key, cutoff, count) do
        {:ok, records} -> {:cont, {:ok, RecordQuery.prepend_chunk(records, acc)}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, chunks} ->
        {:ok,
         chunks
         |> RecordQuery.flatten_chunks()
         |> RecordQuery.sort_by_update()
         |> Enum.take(count)}

      {:error, _reason} = error ->
        error
    end
  end

  def stuck_records(ctx, type, partition_key, cutoff, count) do
    index_key = Keys.inflight_index_key(type, partition_key)

    with :ok <- validate_key_size(index_key),
         {:ok, ids} <- IndexZSet.range_by_score(ctx, index_key, "-inf", Integer.to_string(cutoff)) do
      records_for_ids(ctx, Enum.take(ids, count), partition_key)
    end
  end

  defp terminal_ids(
         ctx,
         type,
         state,
         partition_key,
         count,
         include_cold?,
         consistent?,
         query,
         terminal_states,
         scan_limit
       ) do
    index_key = Keys.state_index_key(type, state, partition_key)

    with :ok <- validate_key_size(index_key),
         {:ok, ids} <-
           index_ids(
             ctx,
             index_key,
             state,
             partition_key,
             count,
             include_cold?,
             consistent?,
             query,
             terminal_states,
             scan_limit
           ) do
      {:ok, ids}
    end
  end

  defp index_ids(
         ctx,
         index_key,
         state,
         partition_key,
         count,
         include_cold?,
         consistent?,
         query,
         terminal_states,
         scan_limit
       ) do
    if state in terminal_states do
      with {:ok, ram_entries} <- RAMIndexRead.terminal_entries(ctx, index_key, count, query),
           {:ok, lmdb_entries} <-
             LMDBIndexRead.terminal_entries(
               ctx,
               index_key,
               state,
               partition_key,
               count,
               include_cold?,
               consistent?,
               query,
               terminal_states,
               scan_limit
             ) do
        ids =
          IndexMerge.ids_from_scored_entries(
            ram_entries,
            lmdb_entries,
            count,
            RAMIndexRead.reverse?(query)
          )

        {:ok, ids}
      end
    else
      with {:ok, ram_ids} <- IndexZSet.range(ctx, index_key, 0, count - 1),
           {:ok, lmdb_ids} <-
             LMDBIndexRead.terminal_ids(
               ctx,
               index_key,
               state,
               partition_key,
               count,
               include_cold?,
               consistent?,
               nil,
               terminal_states,
               scan_limit
             ) do
        {:ok, (ram_ids ++ lmdb_ids) |> Enum.uniq() |> Enum.take(count)}
      end
    end
  end

  defp records_for_ids(ctx, ids, partition_key) do
    RecordLoader.records_for_ids(ctx, ids, partition_key)
  end

  defp list_records_auto_scan(
         ctx,
         type,
         state,
         count,
         include_cold?,
         consistent?,
         terminal_states,
         scan_limit
       ) do
    Keys.auto_partition_keys()
    |> Enum.reduce_while({:ok, []}, fn partition_key, {:ok, acc} ->
      case list_records(
             ctx,
             type,
             state,
             partition_key,
             count,
             include_cold?,
             consistent?,
             terminal_states,
             scan_limit
           ) do
        {:ok, records} -> {:cont, {:ok, RecordQuery.prepend_chunk(records, acc)}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, chunks} ->
        {:ok,
         chunks
         |> RecordQuery.flatten_chunks()
         |> RecordQuery.sort_by_update()
         |> Enum.take(count)}

      {:error, _reason} = error ->
        error
    end
  end

  defp list_records_auto_hot(_ctx, _type, _state, count) when count <= 0, do: {:ok, []}

  defp list_records_auto_hot(ctx, type, state, count) do
    with {:ok, sources} <- auto_hot_rank_sources(ctx, type, state),
         {:ok, entries} <- auto_hot_rank_entries(ctx, sources, count) do
      records_for_partitioned_entries(ctx, entries)
    end
  end

  defp auto_hot_rank_sources(ctx, type, state) do
    with {:ok, candidates} <- auto_hot_rank_candidates(type, state),
         {:ok, counts} <-
           Router.flow_index_count_all_many(
             ctx,
             Enum.map(candidates, fn {_partition_key, index_key} -> index_key end)
           ) do
      requests =
        candidates
        |> Enum.zip(counts)
        |> Enum.reduce([], fn
          {{partition_key, index_key}, index_count}, acc when index_count > 0 ->
            [{partition_key, index_key, index_count} | acc]

          _empty, acc ->
            acc
        end)

      {:ok, Enum.reverse(requests)}
    end
  end

  defp auto_hot_rank_candidates(type, state) do
    Keys.auto_partition_keys()
    |> Enum.reduce_while({:ok, []}, fn partition_key, {:ok, acc} ->
      index_key = Keys.state_index_key(type, state, partition_key)

      case validate_key_size(index_key) do
        :ok -> {:cont, {:ok, [{partition_key, index_key} | acc]}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, candidates} -> {:ok, Enum.reverse(candidates)}
      {:error, _reason} = error -> error
    end
  end

  defp auto_hot_rank_entries(_ctx, [], _count), do: {:ok, []}

  defp auto_hot_rank_entries(ctx, sources, count) do
    chunk_size = auto_hot_chunk_size(count, length(sources))

    sources =
      Enum.map(sources, fn {partition_key, index_key, index_count} ->
        {partition_key, index_key, index_count, 0, nil, false}
      end)

    auto_hot_rank_entries_loop(ctx, sources, [], count, chunk_size)
  end

  defp auto_hot_chunk_size(count, _source_count) do
    count
    |> max(1)
    |> min(64)
  end

  defp auto_hot_rank_entries_loop(ctx, sources, loaded, count, chunk_size) do
    sorted = sort_query_entries(loaded)
    cutoff = query_cutoff(sorted, count)
    to_fetch = auto_hot_sources_to_fetch(sources, cutoff)

    cond do
      length(sorted) >= count and to_fetch == [] ->
        {:ok, Enum.take(sorted, count)}

      to_fetch == [] ->
        {:ok, Enum.take(sorted, count)}

      true ->
        with {:ok, updated_sources, next_entries} <-
               auto_hot_fetch_source_chunks(ctx, sources, to_fetch, chunk_size) do
          auto_hot_rank_entries_loop(
            ctx,
            updated_sources,
            next_entries ++ loaded,
            count,
            chunk_size
          )
        end
    end
  end

  defp query_cutoff(sorted, count) when length(sorted) >= count do
    sorted
    |> Enum.at(count - 1)
    |> query_entry_rank()
  end

  defp query_cutoff(_sorted, _count), do: nil

  defp auto_hot_sources_to_fetch(sources, nil) do
    Enum.filter(sources, fn {_partition_key, _index_key, _index_count, _next_offset, _last_rank,
                             exhausted?} ->
      not exhausted?
    end)
  end

  defp auto_hot_sources_to_fetch(sources, cutoff_rank) do
    Enum.filter(sources, fn {_partition_key, _index_key, _index_count, _next_offset, last_rank,
                             exhausted?} ->
      not exhausted? and (is_nil(last_rank) or last_rank <= cutoff_rank)
    end)
  end

  defp auto_hot_fetch_source_chunks(ctx, sources, to_fetch, chunk_size) do
    router_requests =
      Enum.map(to_fetch, fn {_partition_key, index_key, index_count, next_offset, _last_rank,
                             _exhausted?} ->
        stop_idx = min(next_offset + chunk_size, index_count) - 1
        {index_key, next_offset, stop_idx, false}
      end)

    case Router.flow_index_rank_range_many(ctx, router_requests) do
      {:ok, results} ->
        fetched =
          to_fetch
          |> Enum.zip(results)
          |> Map.new(fn
            {{partition_key, index_key, index_count, next_offset, _last_rank, _exhausted?},
             entries} ->
              next_offset = next_offset + length(entries)
              exhausted? = next_offset >= index_count or entries == []
              query_entries = Enum.map(entries, fn {id, score} -> {id, score, partition_key} end)
              last_rank = query_entries |> List.last() |> maybe_query_entry_rank()

              {index_key,
               {{partition_key, index_key, index_count, next_offset, last_rank, exhausted?},
                query_entries}}
          end)

        updated_sources =
          Enum.map(sources, fn {_partition_key, index_key, _index_count, _next_offset, _last_rank,
                                _exhausted?} = source ->
            case Map.fetch(fetched, index_key) do
              {:ok, {updated_source, _query_entries}} -> updated_source
              :error -> source
            end
          end)

        next_entries =
          fetched
          |> Map.values()
          |> Enum.flat_map(fn {_updated_source, query_entries} -> query_entries end)

        {:ok, updated_sources, next_entries}

      :unavailable ->
        {:ok, []}
    end
  end

  defp maybe_query_entry_rank(nil), do: nil
  defp maybe_query_entry_rank(entry), do: query_entry_rank(entry)

  defp query_entry_rank({id, score, _partition_key}), do: {score, id}

  defp sort_query_entries(entries), do: Enum.sort_by(entries, &query_entry_rank/1)

  defp records_for_partitioned_entries(_ctx, []), do: {:ok, []}

  defp records_for_partitioned_entries(ctx, entries) do
    RecordLoader.records_for_partitioned_entries(ctx, entries)
  end

  defp validate_key_size(key) do
    if byte_size(key) <= Router.max_key_size() do
      :ok
    else
      {:error, "ERR key too large (max #{Router.max_key_size()} bytes)"}
    end
  end
end
