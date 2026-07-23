defmodule FerricstoreServer.Health.Dashboard.Flow.Sample do
  @moduledoc false

  import FerricstoreServer.Health.Dashboard.FlowRecord

  @flow_dashboard_recent_limit 40
  @flow_dashboard_max_recent_limit 200
  @flow_dashboard_history_default_count 50
  @flow_dashboard_history_max_count 250
  @flow_dashboard_keydir_select_batch 256
  @flow_dashboard_keydir_scan_multiplier 64
  @flow_dashboard_keydir_scan_floor 2_048

  def collect_flow_summary, do: collect_summary()
  def collect_flow_records_sample(limit), do: collect_records_sample(limit)
  def flow_type_summaries(records), do: type_summaries(records)
  def flow_available_types(records), do: available_types(records)

  def filter_flow_records_by_type(records, type_filter),
    do: filter_records_by_type(records, type_filter)

  def filter_flow_records_by_partition(records, partition_key),
    do: filter_records_by_partition(records, partition_key)

  def filter_flow_records_by_name(records, query), do: filter_records_by_name(records, query)
  def flow_overview_filters_from_opts(opts), do: overview_filters_from_opts(opts)
  def filter_flow_records(records, filters), do: filter_records(records, filters)
  def merge_flow_records(records, extra_records), do: merge_records(records, extra_records)
  def prepend_flow_dashboard_chunk(records, acc), do: prepend_chunk(records, acc)
  def flatten_flow_dashboard_chunks(chunks), do: flatten_chunks(chunks)
  def flow_available_states(records), do: available_states(records)
  def maybe_include_flow_state(states, state), do: maybe_include_state(states, state)
  def flow_state_filters_from_opts(opts), do: state_filters_from_opts(opts)
  def normalize_flow_partition_query(value), do: normalize_partition_query(value)
  def normalize_flow_history_count(value), do: normalize_history_count(value)
  def normalize_flow_history_cursor(value), do: normalize_history_cursor(value)
  def normalize_flow_history_after_cursor(value), do: normalize_history_after_cursor(value)
  def normalize_flow_type_filter(value), do: normalize_type_filter(value)
  def normalize_flow_state_filter(value), do: normalize_state_filter(value)
  def normalize_flow_name_filter(value), do: normalize_name_filter(value)
  def normalize_flow_range_filter(value), do: normalize_range_filter(value)
  def normalize_flow_boolean_filter(value), do: normalize_boolean_filter(value)
  def normalize_flow_limit_filter(value), do: normalize_limit_filter(value)
  def parse_flow_time_filter(value), do: parse_time_filter(value)
  def flow_page_summary(types, records), do: page_summary(types, records)
  def flow_recent_records(records, limit), do: recent_records(records, limit)
  def flow_worker_summaries(records), do: worker_summaries(records)
  def flow_state_summaries(records), do: state_summaries(records)
  def flow_oldest_due_ms(records), do: oldest_due_ms(records)
  def flow_oldest_lease_ms(records), do: oldest_lease_ms(records)

  def collect_summary do
    records = collect_records_sample(160)
    types = type_summaries(records)
    page_summary(types, records)
  end

  def collect_records_sample(limit) when is_integer(limit) and limit > 0 do
    case FerricStore.Instance.fetch(:default) do
      {:ok, %{shard_count: sc, keydir_refs: keydir_refs} = ctx}
      when is_integer(sc) and sc > 0 and is_tuple(keydir_refs) and tuple_size(keydir_refs) >= sc ->
        per_shard = max(1, div(limit + sc - 1, sc))

        0..(sc - 1)
        |> Enum.flat_map(&collect_records_from_keydir(ctx, &1, per_shard))
        |> Enum.take(limit)

      :error ->
        []

      _invalid_instance ->
        []
    end
  end

  def collect_records_sample(_limit), do: []

  def type_summaries(records) do
    records
    |> Enum.group_by(&flow_record_type/1)
    |> Enum.reject(fn {type, _records} -> type in [nil, ""] end)
    |> Enum.map(fn {type, type_records} ->
      exact_info = safe_flow_info(type)
      counts = flow_counts_for_type(type_records, exact_info)

      counts
      |> Map.put(:type, type)
      |> Map.put(:sampled, length(type_records))
      |> Map.put(:exact, Map.get(counts, :count_source) == :exact)
    end)
    |> Enum.sort_by(fn type ->
      -(Map.get(type, :active, 0) + Map.get(type, :failed, 0) + Map.get(type, :queued, 0))
    end)
  end

  def available_types(records) do
    records
    |> Enum.map(&flow_record_type/1)
    |> Enum.reject(&(&1 in ["", "unknown"]))
    |> Enum.uniq()
    |> Enum.sort()
  end

  def filter_records_by_type(records, nil), do: records

  def filter_records_by_type(records, type_filter) when is_binary(type_filter) do
    Enum.filter(records, &(flow_record_type(&1) == type_filter))
  end

  def filter_records_by_partition(records, nil), do: records

  def filter_records_by_partition(records, partition_key) when is_binary(partition_key) do
    Enum.filter(records, &(flow_record_partition_key(&1) == partition_key))
  end

  def overview_filters_from_opts(opts) when is_list(opts) do
    %{partition_key: normalize_partition_query(Keyword.get(opts, :partition_key))}
  end

  def filter_records(records, filters) when is_map(filters) do
    records
    |> filter_records_by_state(Map.get(filters, :state))
    |> filter_records_by_name(Map.get(filters, :q))
    |> filter_records_by_updated_range(Map.get(filters, :from_ms), Map.get(filters, :to_ms))
  end

  def merge_records(records, []), do: records

  def merge_records(records, extra_records) do
    (extra_records ++ records)
    |> Enum.reduce({[], MapSet.new()}, fn record, {acc, seen} ->
      key = {flow_record_id(record), flow_record_partition_key(record)}

      if MapSet.member?(seen, key) do
        {acc, seen}
      else
        {[record | acc], MapSet.put(seen, key)}
      end
    end)
    |> elem(0)
    |> Enum.reverse()
  end

  def prepend_chunk([], acc), do: acc
  def prepend_chunk(records, acc), do: [records | acc]

  def flatten_chunks(chunks) do
    chunks
    |> Enum.reverse()
    |> List.flatten()
  end

  def available_states(records) do
    records
    |> Enum.map(&flow_record_state/1)
    |> Enum.reject(&(&1 in ["", "unknown"]))
    |> Enum.uniq()
    |> Enum.sort()
  end

  def maybe_include_state(states, state) when is_binary(state) and state != "" do
    states
    |> Kernel.++([state])
    |> Enum.uniq()
    |> Enum.sort()
  end

  def maybe_include_state(states, _state), do: states

  def state_filters_from_opts(opts) when is_list(opts) do
    range = normalize_range_filter(Keyword.get(opts, :range))
    {from_ms, to_ms} = time_bounds_from_opts(opts, range)

    %{
      type: normalize_type_filter(Keyword.get(opts, :type)),
      state: normalize_state_filter(Keyword.get(opts, :state)),
      partition_key: normalize_partition_query(Keyword.get(opts, :partition_key)),
      q: normalize_name_filter(Keyword.get(opts, :q)),
      range: range,
      from_ms: from_ms,
      to_ms: to_ms,
      limit: normalize_limit_filter(Keyword.get(opts, :limit))
    }
  end

  def maybe_put_query_opt(opts, _key, nil), do: opts
  def maybe_put_query_opt(opts, key, value), do: [{key, value} | opts]

  def normalize_partition_query(partition_key) when is_binary(partition_key) do
    partition_key = String.trim(partition_key)
    if partition_key == "", do: nil, else: partition_key
  end

  def normalize_partition_query(_partition_key), do: nil

  def normalize_history_count(count) when is_integer(count) do
    count |> max(1) |> min(@flow_dashboard_history_max_count)
  end

  def normalize_history_count(count) when is_binary(count) do
    case Integer.parse(String.trim(count)) do
      {parsed, ""} -> normalize_history_count(parsed)
      _ -> @flow_dashboard_history_default_count
    end
  end

  def normalize_history_count(_count), do: @flow_dashboard_history_default_count

  def normalize_history_cursor(cursor) when is_binary(cursor) do
    cursor = String.trim(cursor)
    if cursor == "", do: nil, else: cursor
  end

  def normalize_history_cursor(_cursor), do: nil

  def normalize_history_after_cursor(%{
        "history_before" => before,
        "history_after" => after_cursor
      })
      when is_binary(before) do
    case normalize_history_cursor(before) do
      nil -> normalize_history_cursor(after_cursor)
      _before -> nil
    end
  end

  def normalize_history_after_cursor(params) when is_map(params) do
    normalize_history_cursor(Map.get(params, "history_after"))
  end

  def normalize_type_filter(type) when is_binary(type) do
    type = String.trim(type)

    case String.downcase(type) do
      "" -> nil
      "all" -> nil
      _ -> type
    end
  end

  def normalize_type_filter(_type), do: nil

  def normalize_state_filter(state) when is_binary(state) do
    state = String.trim(state)

    case String.downcase(state) do
      "" -> nil
      "all" -> nil
      _ -> state
    end
  end

  def normalize_state_filter(_state), do: nil

  def normalize_name_filter(query) when is_binary(query) do
    query = String.trim(query)
    if query == "", do: nil, else: query
  end

  def normalize_name_filter(_query), do: nil

  def normalize_range_filter(range) when is_binary(range) do
    range = String.trim(range)
    if time_range_duration_ms(range) > 0, do: range, else: nil
  end

  def normalize_range_filter(_range), do: nil

  def normalize_boolean_filter(value) when value in [true, "true", "1", "on", "yes"], do: true
  def normalize_boolean_filter(_value), do: false

  def normalize_limit_filter(limit) when is_integer(limit) do
    limit |> max(1) |> min(@flow_dashboard_max_recent_limit)
  end

  def normalize_limit_filter(limit) when is_binary(limit) do
    case Integer.parse(String.trim(limit)) do
      {parsed, ""} -> normalize_limit_filter(parsed)
      _ -> @flow_dashboard_recent_limit
    end
  end

  def normalize_limit_filter(_limit), do: @flow_dashboard_recent_limit

  def page_summary(types, records) do
    base = %{
      types: length(types),
      total: 0,
      active: 0,
      queued: 0,
      running: 0,
      terminal: 0,
      failed: 0,
      cancelled: 0,
      inflight: 0
    }

    totals =
      Enum.reduce(types, base, fn type, acc ->
        acc
        |> Map.update!(:total, &(&1 + Map.get(type, :total, 0)))
        |> Map.update!(:active, &(&1 + Map.get(type, :active, 0)))
        |> Map.update!(:queued, &(&1 + Map.get(type, :queued, 0)))
        |> Map.update!(:running, &(&1 + Map.get(type, :running, 0)))
        |> Map.update!(:terminal, &(&1 + Map.get(type, :terminal, 0)))
        |> Map.update!(:failed, &(&1 + Map.get(type, :failed, 0)))
        |> Map.update!(:cancelled, &(&1 + Map.get(type, :cancelled, 0)))
        |> Map.update!(:inflight, &(&1 + Map.get(type, :inflight, 0)))
      end)

    totals
    |> Map.put(:due_now_sampled, Enum.count(records, &flow_due_now?/1))
    |> Map.put(:expired_leases_sampled, Enum.count(records, &flow_expired_lease?/1))
    |> Map.put(:sampled_records, length(records))
  end

  def recent_records(records, limit) do
    records
    |> Enum.sort_by(&flow_record_updated_at_ms/1, :desc)
    |> Enum.take(limit)
  end

  def worker_summaries(records) do
    records
    |> Enum.filter(&(flow_record_state(&1) == "running"))
    |> Enum.group_by(fn record -> flow_record_worker(record) || "unknown" end)
    |> Enum.map(fn {worker, worker_records} ->
      %{
        worker: worker,
        running: length(worker_records),
        expired: Enum.count(worker_records, &flow_expired_lease?/1),
        oldest_lease_ms: oldest_lease_ms(worker_records)
      }
    end)
    |> Enum.sort_by(fn worker -> {-worker.running, worker.worker} end)
  end

  def state_summaries(records) do
    records
    |> Enum.group_by(fn record -> {flow_record_type(record), flow_record_state(record)} end)
    |> Enum.map(fn {{type, state}, state_records} ->
      %{
        type: type,
        state: state,
        count: length(state_records),
        due_now: Enum.count(state_records, &flow_due_now?/1),
        running: Enum.count(state_records, &(flow_record_state(&1) == "running")),
        retrying: Enum.count(state_records, &flow_retrying?/1),
        failed: Enum.count(state_records, &flow_failed?/1),
        expired_leases: Enum.count(state_records, &flow_expired_lease?/1),
        max_attempts_reached: Enum.count(state_records, &flow_max_attempts_reached?/1),
        oldest_due_ms: oldest_due_ms(state_records)
      }
    end)
    |> Enum.sort_by(fn state ->
      {-state.due_now, -state.expired_leases, -state.failed, -state.retrying, state.type,
       state.state}
    end)
  end

  def oldest_due_ms(records) do
    now = System.system_time(:millisecond)

    records
    |> Enum.filter(&flow_due_now?/1)
    |> Enum.map(&flow_record_run_at_ms/1)
    |> Enum.filter(&is_integer/1)
    |> case do
      [] -> 0
      due_times -> max(0, now - Enum.min(due_times))
    end
  end

  def oldest_lease_ms(records) do
    now = System.system_time(:millisecond)

    records
    |> Enum.map(&flow_record_lease_expires_at_ms/1)
    |> Enum.filter(&is_integer/1)
    |> case do
      [] -> 0
      expiries -> max(0, now - Enum.min(expiries))
    end
  end

  defp collect_records_from_keydir(ctx, index, per_shard) do
    keydir = elem(ctx.keydir_refs, index)

    try do
      collect_records_from_keydir_select(
        ctx,
        keydir,
        per_shard,
        max(@flow_dashboard_keydir_scan_floor, per_shard * @flow_dashboard_keydir_scan_multiplier)
      )
    rescue
      ArgumentError -> []
    catch
      :exit, _ -> []
    end
  end

  defp collect_records_from_keydir_select(ctx, keydir, wanted, scan_limit) do
    match_spec = [{{:"$1", :_, :_, :_, :_, :_, :_}, [], [:"$1"]}]

    case :ets.select(keydir, match_spec, @flow_dashboard_keydir_select_batch) do
      :"$end_of_table" ->
        []

      {keys, continuation} ->
        collect_records_from_keydir_continue(
          ctx,
          keys,
          continuation,
          wanted,
          scan_limit,
          [],
          0,
          0
        )
    end
  end

  defp collect_records_from_keydir_continue(
         ctx,
         keys,
         continuation,
         wanted,
         scan_limit,
         records,
         record_count,
         scanned
       ) do
    {records, record_count} =
      Enum.reduce_while(keys, {records, record_count}, fn key, {acc, count} ->
        cond do
          count >= wanted -> {:halt, {acc, count}}
          record = record_from_state_key(ctx, key) -> {:cont, {[record | acc], count + 1}}
          true -> {:cont, {acc, count}}
        end
      end)

    scanned = scanned + length(keys)

    cond do
      record_count >= wanted or scanned >= scan_limit ->
        Enum.reverse(records)

      continuation == :"$end_of_table" ->
        Enum.reverse(records)

      true ->
        case :ets.select(continuation) do
          :"$end_of_table" ->
            Enum.reverse(records)

          {next_keys, next_continuation} ->
            collect_records_from_keydir_continue(
              ctx,
              next_keys,
              next_continuation,
              wanted,
              scan_limit,
              records,
              record_count,
              scanned
            )
        end
    end
  end

  defp record_from_state_key(ctx, key) when is_binary(key) do
    if Ferricstore.Flow.Keys.state_key?(key) do
      case Ferricstore.Store.Router.get(ctx, key) do
        value when is_binary(value) ->
          value
          |> safe_decode_record()
          |> case do
            nil -> nil
            record -> Map.put_new(record, :dashboard_state_key, key)
          end

        _ ->
          nil
      end
    end
  rescue
    _ -> nil
  catch
    :exit, _ -> nil
  end

  defp record_from_state_key(_ctx, _key), do: nil

  defp safe_decode_record(value) do
    case Ferricstore.Flow.decode_record(value) do
      record when is_map(record) -> record
      _ -> nil
    end
  rescue
    _ -> nil
  catch
    :exit, _ -> nil
  end

  defp safe_flow_info(type) do
    case FerricStore.flow_info(type) do
      {:ok, info} when is_map(info) -> info
      _ -> nil
    end
  rescue
    _ -> nil
  catch
    :exit, _ -> nil
  end

  defp flow_counts_for_type(records, info) when is_map(info) do
    states = flow_state_counts(records)
    sampled = flow_counts_for_type(records, nil)
    queued = flow_count(info, :queued)
    running = flow_count(info, :running)
    completed = flow_count(info, :completed)
    failed = flow_count(info, :failed)
    cancelled = flow_count(info, :cancelled)
    terminal = completed + failed + cancelled
    total = queued + running + terminal

    if total < sampled.total do
      sampled
    else
      %{
        total: total,
        active: queued + running,
        queued: queued,
        running: running,
        completed: completed,
        failed: failed,
        cancelled: cancelled,
        terminal: terminal,
        inflight: flow_count(info, :inflight),
        states: states,
        count_source: :exact
      }
    end
  end

  defp flow_counts_for_type(records, _info) do
    states = flow_state_counts(records)
    queued = Map.get(states, "queued", 0)
    running = Map.get(states, "running", 0)
    completed = Map.get(states, "completed", 0)
    failed = Map.get(states, "failed", 0)
    cancelled = Map.get(states, "cancelled", 0)
    terminal = completed + failed + cancelled
    total = length(records)

    %{
      total: total,
      active: max(total - terminal, 0),
      queued: queued,
      running: running,
      completed: completed,
      failed: failed,
      cancelled: cancelled,
      terminal: terminal,
      inflight: running,
      states: states,
      count_source: :sampled
    }
  end

  defp flow_count(map, key) when is_map(map) do
    case flow_field(map, key, 0) do
      n when is_integer(n) and n >= 0 -> n
      _ -> 0
    end
  end

  defp flow_state_counts(records) do
    Enum.reduce(records, %{}, fn record, acc ->
      Map.update(acc, flow_record_state(record), 1, &(&1 + 1))
    end)
  end

  defp filter_records_by_state(records, nil), do: records

  defp filter_records_by_state(records, state) when is_binary(state) do
    Enum.filter(records, &(flow_record_state(&1) == state))
  end

  defp filter_records_by_name(records, nil), do: records

  defp filter_records_by_name(records, query) when is_binary(query) do
    needle = String.downcase(query)

    Enum.filter(records, fn record ->
      record |> flow_record_id() |> String.downcase() |> String.contains?(needle)
    end)
  end

  defp filter_records_by_updated_range(records, nil, nil), do: records

  defp filter_records_by_updated_range(records, from_ms, to_ms) do
    Enum.filter(records, fn record ->
      updated_at = flow_record_updated_at_ms(record)
      after_from? = not is_integer(from_ms) or updated_at >= from_ms
      before_to? = not is_integer(to_ms) or updated_at <= to_ms
      after_from? and before_to?
    end)
  end

  defp time_bounds_from_opts(_opts, range) when is_binary(range) do
    {System.system_time(:millisecond) - time_range_duration_ms(range), nil}
  end

  defp time_bounds_from_opts(opts, _range) do
    {parse_time_filter(Keyword.get(opts, :from_ms)), parse_time_filter(Keyword.get(opts, :to_ms))}
  end

  defp time_range_duration_ms("5m"), do: 5 * 60 * 1_000
  defp time_range_duration_ms("15m"), do: 15 * 60 * 1_000
  defp time_range_duration_ms("1h"), do: 60 * 60 * 1_000
  defp time_range_duration_ms("6h"), do: 6 * 60 * 60 * 1_000
  defp time_range_duration_ms("24h"), do: 24 * 60 * 60 * 1_000
  defp time_range_duration_ms(_range), do: 0

  defp parse_time_filter(value) when is_integer(value), do: value

  defp parse_time_filter(value) when is_binary(value) do
    value = String.trim(value)
    if value == "", do: nil, else: parse_time_integer(value) || parse_time_iso8601(value)
  end

  defp parse_time_filter(_value), do: nil

  defp parse_time_integer(value) do
    case Integer.parse(value) do
      {parsed, ""} -> parsed
      _ -> nil
    end
  end

  defp parse_time_iso8601(value) do
    value = value |> String.replace(" ", "T") |> normalize_datetime_local()

    with {:ok, datetime, _offset} <- DateTime.from_iso8601(value) do
      DateTime.to_unix(datetime, :millisecond)
    else
      _ ->
        case NaiveDateTime.from_iso8601(value) do
          {:ok, naive} ->
            naive |> DateTime.from_naive!("Etc/UTC") |> DateTime.to_unix(:millisecond)

          _ ->
            nil
        end
    end
  end

  defp normalize_datetime_local(value) do
    if String.match?(value, ~r/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}$/), do: value <> ":00", else: value
  end
end
