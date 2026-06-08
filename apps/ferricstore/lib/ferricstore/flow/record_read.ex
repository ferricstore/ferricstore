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

  defp validate_key_size(key) do
    if byte_size(key) <= Router.max_key_size() do
      :ok
    else
      {:error, "ERR key too large (max #{Router.max_key_size()} bytes)"}
    end
  end
end
