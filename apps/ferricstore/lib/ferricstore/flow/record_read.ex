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
  alias Ferricstore.Store.Router

  def list_records(
        ctx,
        type,
        state,
        :auto,
        count,
        query,
        include_cold?,
        consistent?,
        terminal_states,
        scan_limit
      ) do
    cond do
      state in terminal_states ->
        terminal_records(
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
        )

      include_cold? or consistent? or ordered_list_query?(query) ->
        list_records_auto_scan(
          ctx,
          type,
          state,
          count,
          query,
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
        query,
        include_cold?,
        consistent?,
        terminal_states,
        scan_limit
      ) do
    cond do
      state in terminal_states ->
        terminal_records(
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
        )

      include_cold? or consistent? or ordered_list_query?(query) ->
        list_records_filtered(
          ctx,
          type,
          state,
          partition_key,
          query,
          include_cold?,
          consistent?,
          terminal_states,
          scan_limit
        )

      true ->
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
          records_for_ids(ctx, ids, partition_key)
        end
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
    sources = terminal_query_sources(state, partition_key, terminal_states)

    RecordQuery.bounded_filtered_records(
      sources,
      count,
      RAMIndexRead.reverse?(query),
      scan_limit,
      fn {source_partition, source_state}, fetch_count, scan_budget ->
        terminal_records_for_source(
          ctx,
          type,
          source_state,
          source_partition,
          fetch_count,
          include_cold?,
          consistent?,
          query,
          terminal_states,
          scan_budget
        )
      end
    )
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

    with {:ok, records, _exhausted?, _scanned_count} <-
           records_for_index_candidate_window(
             ctx,
             index_key,
             partition_key,
             query,
             include_cold?,
             consistent?,
             fetch_count
           ) do
      {:ok, records}
    end
  end

  def records_for_index_filtered(
        ctx,
        index_key,
        partition_key,
        query,
        include_cold?,
        consistent?,
        scan_limit,
        match_fun
      )
      when is_function(match_fun, 1) do
    with {:ok, records, _scanned_count} <-
           records_for_index_filtered_with_count(
             ctx,
             index_key,
             partition_key,
             query,
             include_cold?,
             consistent?,
             scan_limit,
             match_fun
           ) do
      {:ok, records}
    end
  end

  def records_for_index_filtered_with_count(
        ctx,
        index_key,
        partition_key,
        query,
        include_cold?,
        consistent?,
        scan_limit,
        match_fun
      )
      when is_function(match_fun, 1) do
    scan_filtered_candidate_windows_with_count(
      query,
      scan_limit,
      fn limit ->
        records_for_index_candidate_window(
          ctx,
          index_key,
          partition_key,
          query,
          include_cold?,
          consistent?,
          limit
        )
      end,
      match_fun
    )
  end

  def scan_filtered_candidate_windows(query, scan_limit, fetch_fun, match_fun)
      when is_map(query) and is_integer(scan_limit) and scan_limit > 0 and
             is_function(fetch_fun, 1) and is_function(match_fun, 1) do
    with {:ok, records, _scanned_count} <-
           scan_filtered_candidate_windows_with_count(
             query,
             scan_limit,
             fetch_fun,
             match_fun
           ) do
      {:ok, records}
    end
  end

  def scan_filtered_candidate_windows_with_count(query, scan_limit, fetch_fun, match_fun)
      when is_map(query) and is_integer(scan_limit) and scan_limit > 0 and
             is_function(fetch_fun, 1) and is_function(match_fun, 1) do
    count = Map.fetch!(query, :count)

    if count > scan_limit do
      candidate_limit_error(scan_limit)
    else
      scan_filtered_candidate_windows_with_count(
        query,
        scan_limit,
        fetch_fun,
        match_fun,
        count
      )
    end
  end

  defp scan_filtered_candidate_windows_with_count(
         query,
         scan_limit,
         fetch_fun,
         match_fun,
         count
       ) do
    initial_limit = min(LMDBIndexRead.query_scan_count(count, scan_limit), scan_limit)

    with {:ok, records, exhausted?, scanned_count} <-
           fetch_candidate_window(fetch_fun, initial_limit) do
      filtered = filter_candidate_records(records, query, match_fun)

      cond do
        length(filtered) >= count or exhausted? ->
          {:ok, Enum.take(filtered, count), scanned_count}

        initial_limit >= scan_limit ->
          candidate_limit_error(scan_limit)

        true ->
          with {:ok, expanded, expanded_exhausted?, expanded_scanned_count} <-
                 fetch_candidate_window(fetch_fun, scan_limit) do
            filtered = filter_candidate_records(expanded, query, match_fun)

            if length(filtered) >= count or expanded_exhausted? do
              {:ok, Enum.take(filtered, count), expanded_scanned_count}
            else
              candidate_limit_error(scan_limit)
            end
          end
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

  def root_record(ctx, root_flow_id, :auto) do
    root_record(ctx, root_flow_id, Keys.auto_partition_key(root_flow_id))
  end

  def root_record(ctx, root_flow_id, partition_key) do
    case RecordLoader.records_for_ids(ctx, [root_flow_id], partition_key) do
      {:ok, [%{root_flow_id: ^root_flow_id} = record | _rest]} -> {:ok, record}
      {:ok, _records} -> {:ok, nil}
      {:error, _reason} = error -> error
    end
  end

  def stuck_records(ctx, type, :auto, cutoff, count) do
    RecordQuery.bounded_auto_partition_records(
      Keys.auto_partition_keys(),
      count,
      false,
      fn partition_key, fetch_count ->
        stuck_records(ctx, type, partition_key, cutoff, fetch_count)
      end
    )
  end

  def stuck_records(ctx, type, partition_key, cutoff, count) do
    index_key = Keys.inflight_index_key(type, partition_key)

    with :ok <- validate_key_size(index_key),
         {:ok, ids} <-
           IndexZSet.range_by_score(
             ctx,
             index_key,
             "-inf",
             Integer.to_string(cutoff),
             count
           ) do
      records_for_ids(ctx, ids, partition_key)
    end
  end

  defp terminal_query_sources(state, partition_key, terminal_states) do
    states = if state == "any", do: terminal_states, else: [state]

    partitions =
      if partition_key == :auto,
        do: Keys.auto_partition_keys(),
        else: [partition_key]

    for source_partition <- partitions,
        source_state <- states,
        do: {source_partition, source_state}
  end

  defp terminal_records_for_source(
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

    query =
      query
      |> Map.put(:count, count)
      |> Map.put(:state, state)
      |> Map.put(:terminal_only?, true)
      |> Map.put_new(:before_id, nil)

    with :ok <- validate_key_size(index_key) do
      scan_filtered_candidate_windows_with_count(
        query,
        scan_limit,
        fn limit ->
          terminal_record_candidate_window(
            ctx,
            index_key,
            state,
            partition_key,
            query,
            include_cold?,
            consistent?,
            terminal_states,
            limit
          )
        end,
        fn record ->
          Map.get(record, :type) == type and
            IndexQuery.record_matches?(record, query, terminal_states)
        end
      )
    end
  end

  defp terminal_record_candidate_window(
         ctx,
         index_key,
         state,
         partition_key,
         query,
         include_cold?,
         consistent?,
         terminal_states,
         limit
       ) do
    probe_count = limit + 1

    with {:ok, ram_probe_entries} <-
           RAMIndexRead.terminal_entries(ctx, index_key, probe_count, query),
         {:ok, lmdb_entries, lmdb_exhausted?, lmdb_scanned_count} <-
           LMDBIndexRead.terminal_entries_window_with_count(
             ctx,
             index_key,
             state,
             partition_key,
             limit,
             include_cold?,
             consistent?,
             query,
             terminal_states
           ) do
      ram_entries = Enum.take(ram_probe_entries, limit)
      ram_exhausted? = length(ram_probe_entries) <= limit
      ram_scanned_count = length(ram_entries)

      ids =
        IndexMerge.ids_from_scored_entries(
          ram_entries,
          lmdb_entries,
          limit + 1,
          RAMIndexRead.reverse?(query)
        )

      exhausted? = ram_exhausted? and lmdb_exhausted? and length(ids) <= limit
      scanned_count = min(ram_scanned_count + lmdb_scanned_count, limit)
      ids = Enum.take(ids, limit)

      with {:ok, records} <- records_for_ids(ctx, ids, partition_key) do
        {:ok, records, exhausted?, scanned_count}
      end
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
      nonterminal_index_ids(
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
      )
    end
  end

  defp nonterminal_index_ids(
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
    if ordered_list_query?(query) do
      lmdb_result =
        if include_cold? or consistent? do
          LMDBIndexRead.query_entries(
            ctx,
            index_key,
            partition_key,
            count,
            consistent?,
            query,
            scan_limit
          )
        else
          {:ok, []}
        end

      with {:ok, ram_entries} <- RAMIndexRead.score_entries(ctx, index_key, query, count),
           {:ok, lmdb_entries} <- lmdb_result do
        {:ok,
         IndexMerge.ids_from_query_entries(
           ram_entries,
           lmdb_entries,
           count,
           query.rev?
         )}
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

  defp ordered_list_query?(%{
         from_ms: nil,
         to_ms: nil,
         rev?: false,
         before_id: nil,
         terminal_only?: false
       }),
       do: false

  defp ordered_list_query?(query), do: is_map(query)

  defp records_for_ids(ctx, ids, partition_key) do
    RecordLoader.records_for_ids(ctx, ids, partition_key)
  end

  defp records_for_index_candidate_window(
         ctx,
         index_key,
         partition_key,
         query,
         include_cold?,
         consistent?,
         limit
       ) do
    probe_count = limit + 1

    with {:ok, ram_probe_entries} <-
           RAMIndexRead.score_entries(ctx, index_key, query, probe_count),
         {:ok, lmdb_entries, lmdb_exhausted?, lmdb_scanned_count} <-
           maybe_lmdb_candidate_entries(
             ctx,
             index_key,
             partition_key,
             query,
             include_cold?,
             consistent?,
             limit
           ) do
      ram_entries = Enum.take(ram_probe_entries, limit)
      ram_exhausted? = length(ram_probe_entries) <= limit
      ram_scanned_count = length(ram_entries)

      ids =
        IndexMerge.ids_from_query_entries(
          ram_entries,
          lmdb_entries,
          limit + 1,
          Map.get(query, :rev?, false)
        )

      exhausted? = ram_exhausted? and lmdb_exhausted? and length(ids) <= limit
      scanned_count = min(ram_scanned_count + lmdb_scanned_count, limit)
      ids = Enum.take(ids, limit)

      with {:ok, records} <- records_for_ids(ctx, ids, partition_key) do
        {:ok, records, exhausted?, scanned_count}
      end
    end
  end

  defp maybe_lmdb_candidate_entries(
         _ctx,
         _index_key,
         _partition_key,
         _query,
         false,
         _consistent?,
         _limit
       ),
       do: {:ok, [], true, 0}

  defp maybe_lmdb_candidate_entries(
         ctx,
         index_key,
         partition_key,
         query,
         true,
         consistent?,
         limit
       ) do
    LMDBIndexRead.query_entries_window_with_count(
      ctx,
      index_key,
      partition_key,
      limit,
      consistent?,
      query
    )
  end

  defp fetch_candidate_window(fetch_fun, limit) do
    case fetch_fun.(limit) do
      {:ok, records, exhausted?, scanned_count}
      when is_list(records) and is_boolean(exhausted?) and is_integer(scanned_count) and
             scanned_count >= 0 and scanned_count <= limit ->
        {:ok, records, exhausted?, scanned_count}

      {:ok, records, exhausted?} when is_list(records) and is_boolean(exhausted?) ->
        scanned_count = if exhausted?, do: min(length(records), limit), else: limit
        {:ok, records, exhausted?, scanned_count}

      {:error, _reason} = error ->
        error

      invalid ->
        {:error, {:invalid_flow_candidate_window, invalid}}
    end
  end

  defp filter_candidate_records(records, query, match_fun) do
    records
    |> Enum.filter(match_fun)
    |> RecordQuery.sort_by_update()
    |> RecordQuery.maybe_reverse(Map.get(query, :rev?, false))
  end

  defp candidate_limit_error(limit),
    do: {:error, "ERR flow query candidate limit exceeded (#{limit})"}

  defp list_records_auto_scan(
         ctx,
         type,
         state,
         count,
         query,
         include_cold?,
         consistent?,
         terminal_states,
         _scan_limit
       ) do
    RecordQuery.bounded_auto_partition_filtered_records(
      Keys.auto_partition_keys(),
      count,
      RAMIndexRead.reverse?(query),
      fn partition_key, fetch_count, scan_budget ->
        list_records_filtered_with_count(
          ctx,
          type,
          state,
          partition_key,
          %{query | count: fetch_count},
          include_cold?,
          consistent?,
          terminal_states,
          scan_budget
        )
      end
    )
  end

  defp list_records_filtered(
         ctx,
         type,
         state,
         partition_key,
         query,
         include_cold?,
         consistent?,
         terminal_states,
         scan_limit
       ) do
    with {:ok, records, _scanned_count} <-
           list_records_filtered_with_count(
             ctx,
             type,
             state,
             partition_key,
             query,
             include_cold?,
             consistent?,
             terminal_states,
             scan_limit
           ) do
      {:ok, records}
    end
  end

  defp list_records_filtered_with_count(
         ctx,
         type,
         state,
         partition_key,
         query,
         include_cold?,
         consistent?,
         terminal_states,
         scan_limit
       ) do
    index_key = Keys.state_index_key(type, state, partition_key)
    query = Map.put(query, :state, state)

    with :ok <- validate_key_size(index_key) do
      records_for_index_filtered_with_count(
        ctx,
        index_key,
        partition_key,
        query,
        include_cold?,
        consistent?,
        scan_limit,
        fn record ->
          Map.get(record, :type) == type and
            IndexQuery.record_matches?(record, query, terminal_states)
        end
      )
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
    with {:ok, candidates} <- auto_hot_rank_candidates(type, state) do
      case Router.flow_index_count_all_many(
             ctx,
             Enum.map(candidates, fn {_partition_key, index_key} -> index_key end)
           ) do
        {:ok, counts} ->
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

        :unavailable ->
          {:error, :flow_index_unavailable}
      end
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
        {:error, :flow_index_unavailable}
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
