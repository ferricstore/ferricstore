defmodule Ferricstore.Flow.HistoryRead do
  @moduledoc false

  alias Ferricstore.CommandTime
  alias Ferricstore.Flow.Codec
  alias Ferricstore.Store.Router

  @max_history_max_events 1_000_000

  def read(ctx, id, partition_key, history_key, query, false, consistent?, value_return) do
    with :ok <- maybe_flush_history_projector(ctx, history_key, consistent?) do
      if state_exists?(ctx, id, partition_key) do
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
        end
      else
        {:ok, []}
      end
    end
  end

  def read(ctx, id, partition_key, history_key, query, true, consistent?, value_return) do
    with :ok <- maybe_flush_history_projector(ctx, history_key, consistent?) do
      if state_exists?(ctx, id, partition_key) do
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

  if Mix.env() == :test do
    def lmdb_query_scan_count_for_test(count, reverse? \\ false),
      do: history_lmdb_query_scan_count(count, reverse?)
  end

  def hot_values_by_event(event_ids, values) do
    event_ids
    |> Enum.zip(values)
    |> Enum.reduce(%{}, fn
      {event_id, value}, acc when is_binary(event_id) and is_binary(value) ->
        Map.put(acc, event_id, value)

      _missing, acc ->
        acc
    end)
  end

  def cold_values_by_event(_ctx, _history_key, [], _hot_values), do: %{}

  def cold_values_by_event(ctx, history_key, event_ids, hot_values) do
    missing_ids = Enum.reject(event_ids, &Map.has_key?(hot_values, &1))

    if missing_ids == [] do
      %{}
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
          missing_ids
          |> Enum.zip(lmdb_values)
          |> Enum.reduce(%{}, fn {event_id, lmdb_value}, acc ->
            case cold_value_from_lmdb(shard_path, event_id, lmdb_value) do
              {:ok, value} -> Map.put(acc, event_id, value)
              _miss -> acc
            end
          end)

        _error ->
          %{}
      end
    end
  end

  def decode_context(ctx, id, partition_key) do
    case Router.flow_get(ctx, id, partition_key) do
      value when is_binary(value) ->
        case safe_decode_record(value) do
          {:ok, record} -> record
          _ -> %{id: id}
        end

      _ ->
        %{id: id}
    end
  rescue
    _ -> %{id: id}
  end

  def decode_context_from_history_key(ctx, history_key) do
    case state_key_from_history_key(history_key) do
      {:ok, state_key, id} -> decode_context_by_state_key(ctx, state_key, id)
      :error -> %{}
    end
  end

  defp state_exists?(ctx, id, partition_key) do
    case Router.flow_get(ctx, id, partition_key) do
      value when is_binary(value) -> true
      _ -> false
    end
  end

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

    case Router.flow_index_rank_range(ctx, history_key, start_idx, stop_idx, false) do
      {:ok, event_refs} -> {:ok, event_refs}
      :unavailable -> {:ok, []}
    end
  end

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
           {:ok, entries} <- lmdb_prefix_entries(path, prefix, scan_count, query) do
        {:ok, Ferricstore.Flow.LMDBIndexDecode.history_entries(entries, path, now_ms)}
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

    case Ferricstore.Flow.HistoryProjector.flush(ctx, shard_index, 120_000) do
      :ok -> :ok
      {:error, reason} -> {:error, "ERR flow history projection unavailable: #{inspect(reason)}"}
    end
  end

  defp require_lmdb_mirror_healthy_shard(ctx, index_key, shard_index) do
    if Ferricstore.Flow.LMDBMirror.degraded_shard?(ctx, shard_index) do
      {:error, "ERR flow LMDB projection unavailable for #{index_key}"}
    else
      :ok
    end
  end

  def from_event_ids(ctx, id, partition_key, history_key, event_ids, value_return) do
    compound_keys =
      Enum.map(event_ids, &Ferricstore.Flow.Keys.stream_entry_key(id, &1, partition_key))

    values = Router.compound_batch_get(ctx, history_key, compound_keys)
    hot_values = hot_values_by_event(event_ids, values)
    cold_values = cold_values_by_event(ctx, history_key, event_ids, hot_values)
    decode_context = decode_context(ctx, id, partition_key)

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

  def hot_fallback_scan(ctx, history_key, query, value_return) do
    prefix = "X:" <> history_key <> <<0>>
    prefix_size = byte_size(prefix)

    fetch_count = query_fetch_count(query)
    decode_context = decode_context_from_history_key(ctx, history_key)

    entries =
      ctx
      |> Router.compound_scan(history_key, prefix)
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

  defp cold_value_from_lmdb(shard_path, event_id, {:ok, lmdb_value}),
    do: cold_value_from_lmdb(shard_path, event_id, lmdb_value)

  defp cold_value_from_lmdb(shard_path, event_id, lmdb_value) when is_binary(lmdb_value) do
    now = CommandTime.now_ms()

    with {:ok, {^event_id, _event_ms, expire_at_ms, _compound_key, file_ref, offset, _value_size}} <-
           Ferricstore.Flow.LMDB.decode_history_index_location(lmdb_value),
         true <- expire_at_ms <= 0 or expire_at_ms > now do
      case {file_ref, offset} do
        {{:flow_history, _file_id} = ref, offset} when is_integer(offset) and offset >= 0 ->
          Ferricstore.Flow.HistoryProjector.read_value(shard_path, ref, offset)

        _other ->
          :miss
      end
    else
      _ -> :miss
    end
  end

  defp cold_value_from_lmdb(_shard_path, _event_id, _lmdb_value), do: :miss

  def query_fetch_count(query) do
    Ferricstore.Flow.HistoryQuery.fetch_count(query, &history_lmdb_query_scan_count/2)
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
    Application.get_env(:ferricstore, :flow_lmdb_history_sweep_limit, 10_000)
  end

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
    case Ferricstore.Stats.with_cache_tracking_disabled(fn -> Router.get(ctx, state_key) end) do
      value when is_binary(value) ->
        case safe_decode_record(value) do
          {:ok, record} -> record
          _ -> %{id: id}
        end

      _ ->
        %{id: id}
    end
  rescue
    _ -> %{id: id}
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
    _ -> {:ok, nil}
  end
end
