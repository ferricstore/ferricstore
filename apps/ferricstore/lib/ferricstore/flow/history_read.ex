defmodule Ferricstore.Flow.HistoryRead do
  @moduledoc false

  alias Ferricstore.BatchResult
  alias Ferricstore.CommandTime
  alias Ferricstore.Flow.Codec
  alias Ferricstore.Flow.ScopeBinding
  alias Ferricstore.Store.ReadResult
  alias Ferricstore.Store.Router

  @max_history_max_events 1_000_000
  @default_history_lmdb_sweep_limit 10_000
  @maximum_exact_integer 9_007_199_254_740_991

  def read(ctx, id, partition_key, history_key, query, false, consistent?, value_return) do
    with :ok <- maybe_flush_history_projector(ctx, history_key, consistent?),
         {:ok, state_exists?} <- state_exists(ctx, id, partition_key) do
      if state_exists? do
        fetch_count = query_fetch_count(query)

        case hot_refs(ctx, id, partition_key, history_key, fetch_count) do
          {:ok, []} ->
            hot_fallback_scan(ctx, history_key, query, value_return)

          {:ok, event_refs} ->
            event_ids = Enum.map(event_refs, fn {event_id, _score} -> event_id end)

            with {:ok, events} <-
                   from_event_ids(ctx, id, partition_key, history_key, event_ids, value_return) do
              {:ok, apply_query(events, query)}
            end

          {:error, _reason} = error ->
            error
        end
      else
        {:ok, []}
      end
    end
  end

  def read(ctx, id, partition_key, history_key, query, true, consistent?, value_return) do
    with :ok <- maybe_flush_history_projector(ctx, history_key, consistent?),
         {:ok, state_exists?} <- state_exists(ctx, id, partition_key) do
      if state_exists? do
        fetch_count = query_fetch_count(query)

        with {:ok, hot_refs} <- hot_refs(ctx, id, partition_key, history_key, fetch_count),
             {:ok, cold_refs} <- lmdb_refs(ctx, history_key, fetch_count, consistent?, query) do
          event_ids =
            (hot_refs ++ cold_refs)
            |> candidate_event_ids(query)

          case event_ids do
            [] ->
              hot_fallback_scan(ctx, history_key, query, value_return)

            _ ->
              with {:ok, events} <-
                     from_event_ids(ctx, id, partition_key, history_key, event_ids, value_return) do
                {:ok, apply_query(events, query)}
              end
          end
        end
      else
        {:ok, []}
      end
    end
  end

  @doc false
  def read_page(
        ctx,
        id,
        partition_key,
        history_key,
        limit,
        before_event,
        direction,
        value_return
      )
      when is_map(ctx) and is_binary(id) and is_binary(partition_key) and
             is_binary(history_key) and is_integer(limit) and limit > 0 and
             (is_nil(before_event) or is_binary(before_event)) and direction in [:asc, :desc] do
    with :ok <- maybe_flush_history_projector(ctx, history_key, true),
         {:ok, state_exists?} <- state_exists(ctx, id, partition_key) do
      if state_exists? do
        read_existing_page(
          ctx,
          id,
          partition_key,
          history_key,
          limit,
          before_event,
          direction,
          value_return
        )
      else
        {:ok,
         %{
           events: [],
           has_more: false,
           continuation: nil,
           scanned_entries: 0,
           hydrated_records: 0,
           duplicate_entries: 0,
           memory_high_water_bytes: Ferricstore.TermMemory.bytes([])
         }}
      end
    end
  end

  defp read_existing_page(
         ctx,
         id,
         partition_key,
         history_key,
         limit,
         before_event,
         direction,
         value_return
       ) do
    fetch_count = limit + 1

    with {:ok, hot_refs} <-
           hot_page_refs(ctx, history_key, fetch_count, before_event, direction),
         {:ok, cold_refs} <-
           cold_page_refs(ctx, history_key, fetch_count, before_event, direction),
         {:ok, merged_page} <-
           select_page_refs(hot_refs, cold_refs.refs, fetch_count, direction),
         :ok <- validate_page_coverage(merged_page.refs, fetch_count, cold_refs.exhausted?),
         selected <- Enum.take(merged_page.refs, limit),
         event_ids <- Enum.map(merged_page.refs, &elem(&1, 0)),
         {:ok, candidate_events} <-
           from_event_ids_for_page(
             ctx,
             id,
             partition_key,
             history_key,
             event_ids,
             value_return
           ) do
      events = Enum.take(candidate_events, limit)
      has_more = length(merged_page.refs) > limit
      continuation = if has_more, do: selected |> List.last() |> elem(0), else: nil
      memory_high_water_bytes = Ferricstore.TermMemory.bytes(candidate_events)

      {:ok,
       %{
         events: events,
         has_more: has_more,
         continuation: continuation,
         scanned_entries: length(hot_refs) + cold_refs.scanned_entries,
         hydrated_records: length(event_ids),
         duplicate_entries: merged_page.duplicate_entries,
         memory_high_water_bytes: memory_high_water_bytes
       }}
    end
  end

  defp hot_page_refs(ctx, history_key, count, nil, direction) do
    ctx
    |> Router.flow_index_rank_range(history_key, 0, count - 1, direction == :desc)
    |> normalize_page_hot_refs()
  end

  defp hot_page_refs(ctx, history_key, count, boundary_event, :asc) do
    cursor = {:cursor_after, Ferricstore.Flow.HistoryEvent.ms(boundary_event), boundary_event}

    ctx
    |> Router.flow_index_score_range_slice(
      history_key,
      cursor,
      :inf,
      false,
      0,
      count
    )
    |> normalize_page_hot_refs()
  end

  defp hot_page_refs(ctx, history_key, count, boundary_event, :desc) do
    cursor = {:cursor_before, Ferricstore.Flow.HistoryEvent.ms(boundary_event), boundary_event}

    ctx
    |> Router.flow_index_score_range_slice(
      history_key,
      :neg_inf,
      cursor,
      true,
      0,
      count
    )
    |> normalize_page_hot_refs()
  end

  defp cold_page_refs(ctx, history_key, count, boundary_event, direction) do
    shard_index = Router.shard_for(ctx, history_key)

    with :ok <- maybe_flush_lmdb_shard(ctx, shard_index, true),
         :ok <- require_lmdb_mirror_healthy_shard(ctx, history_key, shard_index) do
      path =
        ctx.data_dir
        |> Ferricstore.DataDir.shard_data_path(shard_index)
        |> Ferricstore.Flow.LMDB.path()

      prefix = Ferricstore.Flow.LMDB.history_index_prefix(history_key)
      now_ms = CommandTime.now_ms()

      with {:ok, entries} <-
             cold_page_entries(
               path,
               prefix,
               history_key,
               boundary_event,
               direction,
               count
             ),
           {:ok, decoded} <-
             Ferricstore.Flow.LMDBIndexDecode.history_query_entries(entries, now_ms),
           {:ok, decoded} <- normalize_page_refs(decoded) do
        {:ok,
         %{
           refs: decoded,
           scanned_entries: length(entries),
           exhausted?: length(entries) < count
         }}
      end
    end
  end

  defp cold_page_entries(path, prefix, _history_key, nil, direction, count),
    do: Ferricstore.Flow.LMDB.prefix_entries(path, prefix, count, direction == :desc)

  defp cold_page_entries(path, prefix, history_key, boundary_event, direction, count) do
    boundary_key =
      Ferricstore.Flow.LMDB.history_index_key(
        history_key,
        boundary_event,
        Ferricstore.Flow.HistoryEvent.ms(boundary_event)
      )

    case direction do
      :asc ->
        Ferricstore.Flow.LMDB.prefix_entries_after(path, prefix, boundary_key, count)

      :desc ->
        Ferricstore.Flow.LMDB.prefix_entries_reverse_before(
          path,
          prefix,
          boundary_key,
          count
        )
    end
  end

  defp select_page_refs(hot_refs, cold_refs, count, direction) do
    refs = hot_refs ++ cold_refs

    unique_refs =
      refs
      |> Enum.sort_by(fn {event_id, score} -> {score, event_id} end, direction)
      |> Enum.uniq_by(&elem(&1, 0))

    {:ok,
     %{
       refs: Enum.take(unique_refs, count),
       duplicate_entries: length(refs) - length(unique_refs)
     }}
  end

  defp normalize_page_hot_refs(result) do
    with {:ok, refs} <- normalize_hot_refs(result), do: normalize_page_refs(refs)
  end

  defp normalize_page_refs(refs) when is_list(refs) do
    refs
    |> Enum.reduce_while({:ok, []}, fn
      {event_id, score}, {:ok, acc} when is_binary(event_id) ->
        with {:ok, event_ms} <- canonical_event_ms(event_id),
             {:ok, normalized_score} <- normalize_page_score(score),
             true <- event_ms == normalized_score do
          {:cont, {:ok, [{event_id, event_ms} | acc]}}
        else
          _invalid -> {:halt, {:error, :query_storage_inconsistent}}
        end

      _invalid, _acc ->
        {:halt, {:error, :query_storage_inconsistent}}
    end)
    |> case do
      {:ok, reversed} -> {:ok, Enum.reverse(reversed)}
      {:error, _reason} = error -> error
    end
  end

  defp canonical_event_ms(event_id) do
    case :binary.split(event_id, "-", [:global]) do
      [milliseconds, version] ->
        with {:ok, ms} <- canonical_event_integer(milliseconds),
             {:ok, _version} <- canonical_event_integer(version) do
          {:ok, ms}
        end

      _invalid ->
        {:error, :query_storage_inconsistent}
    end
  end

  defp canonical_event_integer(value) do
    case Integer.parse(value) do
      {number, ""} when number >= 0 and number <= @maximum_exact_integer ->
        if value == Integer.to_string(number),
          do: {:ok, number},
          else: {:error, :query_storage_inconsistent}

      _invalid ->
        {:error, :query_storage_inconsistent}
    end
  end

  defp normalize_page_score(score) when is_integer(score) and score >= 0,
    do: {:ok, score}

  defp normalize_page_score(score) when is_float(score) and score >= 0 do
    normalized = trunc(score)
    if score == normalized, do: {:ok, normalized}, else: {:error, :query_storage_inconsistent}
  end

  defp normalize_page_score(_score), do: {:error, :query_storage_inconsistent}

  defp validate_page_coverage(refs, count, _cold_exhausted?) when length(refs) >= count,
    do: :ok

  defp validate_page_coverage(_refs, _count, true), do: :ok

  defp validate_page_coverage(_refs, _count, false),
    do: {:error, :query_scan_budget_exceeded}

  if Mix.env() == :test do
    def lmdb_query_scan_count_for_test(count, reverse? \\ false),
      do: history_lmdb_query_scan_count(count, reverse?)
  end

  def hot_values_by_event(event_ids, values) when is_list(event_ids) and is_list(values) do
    case ReadResult.first_failure(values) do
      nil ->
        case BatchResult.map_exact(event_ids, values, fn event_id, value -> {event_id, value} end) do
          {:ok, pairs} ->
            {:ok,
             Enum.reduce(pairs, %{}, fn
               {event_id, value}, acc when is_binary(event_id) and is_binary(value) ->
                 Map.put(acc, event_id, value)

               _missing, acc ->
                 acc
             end)}

          {:error, reason} ->
            ReadResult.failure(reason)
        end

      failure ->
        failure
    end
  end

  def hot_values_by_event(_event_ids, values),
    do: ReadResult.failure({:invalid_batch_results, values})

  def cold_values_by_event(_ctx, _history_key, [], _hot_values), do: {:ok, %{}}

  def cold_values_by_event(ctx, history_key, event_ids, hot_values) do
    missing_ids = Enum.reject(event_ids, &Map.has_key?(hot_values, &1))

    if missing_ids == [] do
      {:ok, %{}}
    else
      shard_index = Router.shard_for(ctx, history_key)
      shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, shard_index)
      lmdb_path = Ferricstore.Flow.LMDB.path(shard_path)

      lmdb_keys =
        Enum.map(missing_ids, fn event_id ->
          Ferricstore.Flow.LMDB.history_index_key(
            history_key,
            event_id,
            Ferricstore.Flow.HistoryEvent.ms(event_id)
          )
        end)

      case Ferricstore.Flow.LMDB.get_many(lmdb_path, lmdb_keys) do
        {:ok, lmdb_values} ->
          cold_values_by_event_results(missing_ids, lmdb_values, fn event_id, lmdb_value ->
            cold_value_from_lmdb(shard_path, event_id, lmdb_value)
          end)

        {:error, reason} ->
          ReadResult.failure(reason)

        invalid ->
          ReadResult.failure({:invalid_batch_results, invalid})
      end
    end
  end

  @doc false
  def __cold_values_by_event_results_for_test__(event_ids, lmdb_values, decoder),
    do: cold_values_by_event_results(event_ids, lmdb_values, decoder)

  defp cold_values_by_event_results(event_ids, lmdb_values, decoder) do
    case BatchResult.map_exact(event_ids, lmdb_values, fn event_id, lmdb_value ->
           {event_id, decoder.(event_id, lmdb_value)}
         end) do
      {:ok, decoded} ->
        Enum.reduce_while(decoded, {:ok, %{}}, fn
          {event_id, {:ok, value}}, {:ok, acc} when is_binary(value) ->
            {:cont, {:ok, Map.put(acc, event_id, value)}}

          {_event_id, :miss}, {:ok, acc} ->
            {:cont, {:ok, acc}}

          {event_id, {:error, reason}}, _acc ->
            {:halt, ReadResult.failure({:cold_value_read_failed, event_id, reason})}

          {event_id, invalid}, _acc ->
            {:halt, ReadResult.failure({:invalid_cold_value_result, event_id, invalid})}
        end)

      {:error, reason} ->
        ReadResult.failure(reason)
    end
  rescue
    exception -> ReadResult.failure({:cold_value_decode_failed, exception.__struct__})
  end

  def decode_context(ctx, id, partition_key) do
    ctx
    |> Router.flow_get(id, partition_key)
    |> decode_context_read(id)
    |> ScopeBinding.verify_context_read_result(ctx)
  rescue
    _ -> {:error, "ERR storage read failed"}
  end

  def decode_context_from_history_key(ctx, history_key) do
    case state_key_from_history_key(history_key) do
      {:ok, state_key, id} -> decode_context_by_state_key(ctx, state_key, id)
      :error -> {:ok, %{}}
    end
  end

  @doc false
  def __decode_context_read_for_test__(result, id), do: decode_context_read(result, id)

  defp decode_context_read(value, _id) when is_binary(value), do: safe_decode_record(value)
  defp decode_context_read(nil, id), do: {:ok, %{id: id}}
  defp decode_context_read(_failure, _id), do: {:error, "ERR storage read failed"}

  defp state_exists(ctx, id, partition_key),
    do: ctx |> Router.flow_get_with_status(id, partition_key) |> normalize_state_read()

  @doc false
  def __normalize_state_read_for_test__(result), do: normalize_state_read(result)

  defp normalize_state_read(value) when is_binary(value), do: {:ok, true}
  defp normalize_state_read(nil), do: {:ok, false}
  defp normalize_state_read(:unavailable), do: {:error, "ERR storage read failed"}
  defp normalize_state_read({:error, _reason}), do: {:error, "ERR storage read failed"}
  defp normalize_state_read(_invalid), do: {:error, "ERR storage read failed"}

  defp candidate_event_ids(refs, query) do
    refs
    |> Enum.sort_by(fn {event_id, score} -> {score, event_id} end)
    |> Enum.uniq_by(fn {event_id, _score} -> event_id end)
    |> limit_candidate_refs(query)
    |> Enum.map(fn {event_id, _score} -> event_id end)
  end

  defp limit_candidate_refs(refs, query) do
    if query_filtering?(query), do: refs, else: Enum.take(refs, -query.count)
  end

  defp hot_refs(ctx, id, partition_key, history_key, count) do
    {start_idx, stop_idx} = hot_range(ctx, id, partition_key, history_key, count)

    ctx
    |> Router.flow_index_rank_range(history_key, start_idx, stop_idx, false)
    |> normalize_hot_refs()
  end

  @doc false
  def __normalize_hot_refs_for_test__(result), do: normalize_hot_refs(result)

  defp normalize_hot_refs({:ok, event_refs}) when is_list(event_refs), do: {:ok, event_refs}
  defp normalize_hot_refs(:unavailable), do: {:error, "ERR storage read failed"}
  defp normalize_hot_refs({:error, _reason}), do: {:error, "ERR storage read failed"}
  defp normalize_hot_refs(_invalid), do: {:error, "ERR storage read failed"}

  def hot_range(ctx, id, partition_key, history_key, count) do
    with {:ok, max} <- hot_max(ctx, id, partition_key),
         true <- is_integer(max) and max > 0,
         {:ok, total} <- Ferricstore.Flow.IndexZSet.card(ctx, history_key) do
      oldest_hot_idx = max(total - max, 0)
      start_idx = max(total - count, oldest_hot_idx)
      {start_idx, total - 1}
    else
      _ -> {0, count - 1}
    end
  end

  def hot_range_for_max(ctx, history_key, count, max) when is_integer(max) and max > 0 do
    case Ferricstore.Flow.IndexZSet.card(ctx, history_key) do
      {:ok, total} ->
        oldest_hot_idx = max(total - max, 0)
        start_idx = max(total - count, oldest_hot_idx)
        {start_idx, total - 1}

      _ ->
        {0, count - 1}
    end
  end

  def hot_range_for_max(_ctx, _history_key, count, _max), do: {0, count - 1}

  defp hot_max(ctx, id, partition_key) do
    case Router.flow_get(ctx, id, partition_key) do
      value when is_binary(value) ->
        case safe_decode_record(value) do
          {:ok, %{history_hot_max_events: max}} -> {:ok, max}
          _ -> :error
        end

      _ ->
        :error
    end
  end

  defp lmdb_refs(_ctx, _history_key, count, _consistent?, _query) when count <= 0, do: {:ok, []}

  defp lmdb_refs(ctx, history_key, count, consistent?, query) do
    shard_index = Router.shard_for(ctx, history_key)

    with :ok <- maybe_flush_lmdb_shard(ctx, shard_index, consistent?),
         :ok <- require_lmdb_mirror_healthy_shard(ctx, history_key, shard_index) do
      path =
        ctx.data_dir
        |> Ferricstore.DataDir.shard_data_path(shard_index)
        |> Ferricstore.Flow.LMDB.path()

      prefix = Ferricstore.Flow.LMDB.history_index_prefix(history_key)
      now_ms = CommandTime.now_ms()
      sweep_limit = history_lmdb_sweep_limit()
      scan_count = history_lmdb_scan_count(count, query)

      with {:ok, _swept} <- Ferricstore.Flow.LMDB.sweep_expired_history(path, now_ms, sweep_limit),
           {:ok, entries} <- lmdb_prefix_entries(path, prefix, scan_count, query),
           {:ok, decoded_entries} <-
             Ferricstore.Flow.LMDBIndexDecode.history_entries(entries, path, now_ms) do
        {:ok, decoded_entries}
      end
    end
  end

  defp history_lmdb_scan_count(count, query) do
    cond do
      requires_full_lmdb_scan?(query) ->
        max_history_max_events()

      query_filtering?(query) ->
        history_lmdb_query_scan_count(count, Map.get(query, :rev?, false))

      true ->
        count
    end
  end

  defp lmdb_prefix_entries(path, prefix, limit, query) do
    result =
      query
      |> lmdb_scan_directions()
      |> Enum.reduce_while({:ok, []}, fn reverse?, {:ok, acc} ->
        case lmdb_prefix_entries(path, prefix, limit, query, reverse?) do
          {:ok, entries} -> {:cont, {:ok, acc ++ entries}}
          {:error, _reason} = error -> {:halt, error}
        end
      end)

    case result do
      {:ok, entries} -> {:ok, Enum.uniq_by(entries, fn {key, _value} -> key end)}
      {:error, _reason} = error -> error
    end
  end

  defp lmdb_scan_directions(query) do
    cond do
      not query_filtering?(query) -> [true]
      requires_full_lmdb_scan?(query) -> [false]
      true -> [true, false]
    end
  end

  defp requires_full_lmdb_scan?(%{
         from_event: nil,
         to_event: nil,
         from_version: nil,
         to_version: nil,
         event: nil,
         worker: nil
       }),
       do: false

  defp requires_full_lmdb_scan?(_query), do: true

  defp lmdb_prefix_entries(path, prefix, limit, %{to_ms: to_ms}, true)
       when is_integer(to_ms) and to_ms >= 0 do
    Ferricstore.Flow.LMDB.prefix_entries_reverse_before(
      path,
      prefix,
      Ferricstore.Flow.LMDBQueryWindow.time_upper_seek_key(prefix, to_ms),
      limit
    )
  end

  defp lmdb_prefix_entries(path, prefix, limit, _query, true) do
    Ferricstore.Flow.LMDB.prefix_entries(path, prefix, limit, true)
  end

  defp lmdb_prefix_entries(path, prefix, limit, %{from_ms: from_ms}, false)
       when is_integer(from_ms) and from_ms >= 0 do
    Ferricstore.Flow.LMDB.prefix_entries_after(
      path,
      prefix,
      Ferricstore.Flow.LMDBQueryWindow.time_seek_key(prefix, from_ms),
      limit
    )
  end

  defp lmdb_prefix_entries(path, prefix, limit, _query, false) do
    Ferricstore.Flow.LMDB.prefix_entries(path, prefix, limit, false)
  end

  defp maybe_flush_lmdb_shard(_ctx, _shard_index, false), do: :ok

  defp maybe_flush_lmdb_shard(ctx, shard_index, true),
    do: Ferricstore.Flow.LMDBWriter.flush(ctx.name, shard_index)

  defp maybe_flush_history_projector(_ctx, _history_key, false), do: :ok

  defp maybe_flush_history_projector(ctx, history_key, true) do
    shard_index = Router.shard_for(ctx, history_key)

    ctx
    |> Ferricstore.Flow.HistoryProjector.flush(shard_index, 120_000)
    |> normalize_history_projector_flush()
  end

  @doc false
  def __normalize_history_projector_flush_for_test__(result),
    do: normalize_history_projector_flush(result)

  defp normalize_history_projector_flush(:ok), do: :ok

  defp normalize_history_projector_flush(_failure),
    do: {:error, "ERR flow history projection unavailable"}

  defp require_lmdb_mirror_healthy_shard(ctx, index_key, shard_index) do
    if Ferricstore.Flow.LMDBMirror.degraded_shard?(ctx, shard_index) do
      {:error, "ERR flow LMDB projection unavailable for #{index_key}"}
    else
      :ok
    end
  end

  def from_event_ids(ctx, id, partition_key, history_key, event_ids, value_return) do
    with {:ok, decode_context} <- decode_context(ctx, id, partition_key) do
      from_event_ids_with_context(
        ctx,
        id,
        partition_key,
        history_key,
        event_ids,
        value_return,
        decode_context
      )
    end
  end

  def from_event_ids_with_context(
        ctx,
        id,
        partition_key,
        history_key,
        event_ids,
        value_return,
        decode_context
      ) do
    case do_from_event_ids_with_context(
           ctx,
           id,
           partition_key,
           history_key,
           event_ids,
           value_return,
           decode_context
         ) do
      {:error, {:storage_read_failed, _reason}} = failure -> ReadResult.command_error(failure)
      result -> result
    end
  end

  defp from_event_ids_for_page(
         ctx,
         id,
         partition_key,
         history_key,
         event_ids,
         value_return
       ) do
    with {:ok, decode_context} <- decode_context(ctx, id, partition_key),
         {:ok, events} <-
           do_from_event_ids_with_context(
             ctx,
             id,
             partition_key,
             history_key,
             event_ids,
             value_return,
             decode_context
           ),
         true <- exact_events?(events, event_ids) do
      {:ok, events}
    else
      {:error, {:storage_read_failed, {:cold_value_read_failed, _event_id, reason}}}
      when reason in [
             :missing_history_value_location,
             :invalid_history_index_location,
             :invalid_history_index_result
           ] ->
        {:error, :query_storage_inconsistent}

      {:error, {:storage_read_failed, _reason}} ->
        {:error, :query_storage_unavailable}

      false ->
        {:error, :query_storage_inconsistent}

      {:error, _reason} = error ->
        error
    end
  end

  defp do_from_event_ids_with_context(
         ctx,
         id,
         partition_key,
         history_key,
         event_ids,
         value_return,
         decode_context
       ) do
    compound_keys =
      Enum.map(event_ids, &Ferricstore.Flow.Keys.stream_entry_key(id, &1, partition_key))

    values = Router.compound_batch_get(ctx, history_key, compound_keys)

    with {:ok, hot_values} <- hot_values_by_event(event_ids, values),
         {:ok, cold_values} <- cold_values_by_event(ctx, history_key, event_ids, hot_values) do
      entries =
        Enum.flat_map(event_ids, fn event_id ->
          value = Map.get(hot_values, event_id) || Map.get(cold_values, event_id)

          if is_binary(value) do
            [{event_id, Codec.decode_history_fields(value, decode_context)}]
          else
            []
          end
        end)

      {:ok,
       entries
       |> Enum.map(&Ferricstore.Flow.HistoryEntry.to_tuple/1)
       |> Ferricstore.Flow.HistoryValues.hydrate(ctx, value_return)}
    end
  end

  defp exact_events?(events, event_ids) when is_list(events) do
    Enum.map(events, fn
      {event_id, _fields} when is_binary(event_id) -> event_id
      _invalid -> :invalid
    end) == event_ids
  end

  defp exact_events?(_events, _event_ids), do: false

  def hot_fallback_scan(ctx, history_key, query, value_return) do
    prefix = "X:" <> history_key <> <<0>>
    prefix_size = byte_size(prefix)

    fetch_count = query_fetch_count(query)

    with {:ok, decode_context} <- decode_context_from_history_key(ctx, history_key) do
      case Router.compound_scan(ctx, history_key, prefix) do
        {:error, {:storage_read_failed, _reason}} = failure ->
          ReadResult.command_error(failure)

        scanned when is_list(scanned) ->
          entries =
            scanned
            |> Enum.flat_map(fn
              {<<^prefix::binary-size(prefix_size), event_id::binary>>, value}
              when is_binary(value) ->
                [{event_id, Codec.decode_history_fields(value, decode_context)}]

              {event_id, value} when is_binary(event_id) and is_binary(value) ->
                [{event_id, Codec.decode_history_fields(value, decode_context)}]

              _other ->
                []
            end)
            |> Enum.sort_by(fn {event_id, _fields} ->
              {Ferricstore.Flow.HistoryEvent.ms(event_id), event_id}
            end)
            |> Enum.take(-fetch_count)

          events =
            entries
            |> Enum.map(&Ferricstore.Flow.HistoryEntry.to_tuple/1)
            |> Ferricstore.Flow.HistoryValues.hydrate(ctx, value_return)

          {:ok, apply_query(events, query)}
      end
    end
  end

  defp cold_value_from_lmdb(shard_path, event_id, {:ok, lmdb_value}),
    do: cold_value_from_lmdb(shard_path, event_id, lmdb_value)

  defp cold_value_from_lmdb(_shard_path, _event_id, :not_found), do: :miss

  defp cold_value_from_lmdb(shard_path, event_id, lmdb_value) when is_binary(lmdb_value) do
    now = CommandTime.now_ms()

    case Ferricstore.Flow.LMDB.decode_history_index_location(lmdb_value) do
      {:ok, {^event_id, _event_ms, expire_at_ms, _compound_key, file_ref, offset, _value_size}} ->
        cond do
          expire_at_ms > 0 and expire_at_ms <= now ->
            :miss

          match?({:flow_history, _file_id}, file_ref) and is_integer(offset) and offset >= 0 ->
            Ferricstore.Flow.HistoryProjector.read_value(shard_path, file_ref, offset)

          true ->
            {:error, :missing_history_value_location}
        end

      _invalid ->
        {:error, :invalid_history_index_location}
    end
  end

  defp cold_value_from_lmdb(_shard_path, _event_id, _lmdb_value),
    do: {:error, :invalid_history_index_result}

  def query_fetch_count(query) do
    if query_filtering?(query) do
      max(Map.fetch!(query, :count), max_history_max_events())
    else
      Map.fetch!(query, :count)
    end
  end

  defp query_filtering?(query), do: Ferricstore.Flow.HistoryQuery.filtering?(query)

  def apply_query(events, query) do
    Ferricstore.Flow.HistoryQuery.apply(events, query, &Ferricstore.Flow.HistoryEvent.ms/1)
  end

  defp history_lmdb_query_scan_count(count, true) when is_integer(count) and count > 0,
    do: Ferricstore.Flow.LMDBQueryWindow.history_query_scan_count(count, true, 0)

  defp history_lmdb_query_scan_count(count, false) when is_integer(count) and count > 0 do
    Ferricstore.Flow.LMDBQueryWindow.history_query_scan_count(
      count,
      false,
      max_history_max_events()
    )
  end

  defp history_lmdb_sweep_limit do
    case Application.get_env(
           :ferricstore,
           :flow_lmdb_history_sweep_limit,
           @default_history_lmdb_sweep_limit
         ) do
      value when is_integer(value) and value >= 0 -> min(value, @max_history_max_events)
      _invalid -> @default_history_lmdb_sweep_limit
    end
  end

  @doc false
  def __history_lmdb_sweep_limit_for_test__, do: history_lmdb_sweep_limit()

  defp max_history_max_events do
    case Application.get_env(
           :ferricstore,
           :flow_max_history_max_events,
           @max_history_max_events
         ) do
      value when is_integer(value) and value > 0 -> min(value, @max_history_max_events)
      _ -> @max_history_max_events
    end
  end

  defp decode_context_by_state_key(ctx, state_key, id) do
    Ferricstore.Stats.with_cache_tracking_disabled(fn -> Router.get(ctx, state_key) end)
    |> decode_context_read(id)
    |> ScopeBinding.verify_context_read_result(ctx)
  rescue
    _ -> {:error, "ERR storage read failed"}
  end

  defp state_key_from_history_key(history_key) when is_binary(history_key) do
    case :binary.match(history_key, "}:h:") do
      {pos, len} ->
        start = pos + len
        id = binary_part(history_key, start, byte_size(history_key) - start)
        tag_prefix = binary_part(history_key, 0, pos + 1)
        {:ok, tag_prefix <> ":s:" <> id, id}

      :nomatch ->
        :error
    end
  end

  defp safe_decode_record(value) when is_binary(value) do
    {:ok, Codec.decode_record(value)}
  rescue
    _ -> {:error, "ERR invalid flow record"}
  end
end
