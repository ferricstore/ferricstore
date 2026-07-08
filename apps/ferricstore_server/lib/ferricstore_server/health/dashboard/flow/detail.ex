defmodule FerricstoreServer.Health.Dashboard.Flow.Detail do
  @moduledoc false

  alias FerricstoreServer.Health.Dashboard.Flow.Fifo
  alias FerricstoreServer.Health.Dashboard.Flow.PolicyRetention

  import FerricstoreServer.Health.Dashboard.Flow.Calls
  import FerricstoreServer.Health.Dashboard.Flow.Sample
  import FerricstoreServer.Health.Dashboard.FlowRecord
  import FerricstoreServer.Health.Dashboard.Format, only: [dashboard_internal_error: 2]
  import FerricstoreServer.Health.Dashboard.Render.FlowDetail, only: [flow_detail_value_refs: 2]

  import FerricstoreServer.Health.Dashboard.Render.FlowHistory,
    only: [flow_detail_path: 3, flow_history_current_state: 1, normalize_flow_history_entry: 1]

  @flow_dashboard_sample_limit 400
  @flow_dashboard_history_default_count 50
  @flow_dashboard_value_ref_limit 40

  @spec apply_rewind_form(map()) :: {:ok, binary(), binary() | nil} | {:error, binary()}
  def apply_rewind_form(params) when is_map(params) do
    with :ok <- flow_rewind_confirmed(params),
         {:ok, id} <- flow_rewind_required_form_value(params, "id", "flow id"),
         partition_key = normalize_flow_partition_query(Map.get(params, "partition_key")),
         {:ok, to_event} <- flow_rewind_required_form_value(params, "to_event", "target event"),
         {:ok, run_at_ms} <- flow_rewind_optional_non_neg_integer(params, "run_at_ms"),
         {:ok, record} <- flow_rewind_current_record(id, partition_key),
         {:ok, _target_state} <- flow_rewind_existing_target_state(id, partition_key, to_event),
         opts =
           flow_rewind_opts(partition_key,
             to_event: to_event,
             run_at_ms: run_at_ms,
             expect_state: flow_record_state(record)
           ),
         :ok <- flow_rewind_apply(id, opts) do
      {:ok, id, flow_detail_url_partition_key(partition_key)}
    else
      {:error, reason} when is_binary(reason) -> {:error, reason}
      {:error, reason} -> {:error, inspect(reason)}
    end
  end

  def apply_rewind_form(_params), do: {:error, "ERR rewind form must be a map"}

  @spec apply_signal_form(map()) :: {:ok, binary(), binary() | nil} | {:error, binary()}
  def apply_signal_form(params) when is_map(params) do
    with {:ok, id} <- flow_rewind_required_form_value(params, "id", "flow id"),
         partition_key = normalize_flow_partition_query(Map.get(params, "partition_key")),
         {:ok, signal} <- flow_rewind_required_form_value(params, "signal", "signal name"),
         {:ok, transition_to} <- flow_signal_optional_binary(params, "transition_to"),
         {:ok, idempotency_key} <- flow_signal_optional_binary(params, "idempotency_key"),
         {:ok, if_state} <- flow_signal_optional_binary(params, "if_state"),
         opts =
           flow_signal_opts(partition_key,
             signal: signal,
             transition_to: transition_to,
             idempotency_key: idempotency_key,
             if_state: if_state
           ),
         :ok <- flow_signal_apply(id, opts) do
      {:ok, id, flow_detail_url_partition_key(partition_key)}
    else
      {:error, reason} when is_binary(reason) -> {:error, reason}
      {:error, reason} -> {:error, inspect(reason)}
    end
  end

  def apply_signal_form(_params), do: {:error, "ERR signal form must be a map"}

  @spec opts_from_query(binary()) :: keyword()
  def opts_from_query(query) when is_binary(query) do
    params = URI.decode_query(query)

    []
    |> maybe_put_query_opt(
      :partition_key,
      normalize_flow_partition_query(Map.get(params, "partition_key"))
    )
    |> maybe_put_query_opt(:flash, flash_from_params(params))
    |> maybe_put_query_opt(
      :history_count,
      normalize_flow_history_count(Map.get(params, "history_count"))
    )
    |> maybe_put_query_opt(
      :history_before,
      normalize_flow_history_cursor(Map.get(params, "history_before"))
    )
    |> maybe_put_query_opt(:history_after, normalize_flow_history_after_cursor(params))
    |> Enum.reverse()
  end

  def opts_from_query(_query), do: []

  @spec collect_page(binary(), keyword()) :: map()
  def collect_page(id, opts \\ [])

  def collect_page(id, opts) when is_binary(id) and is_list(opts) do
    partition_key = flow_detail_partition_key(opts)
    history_page_opts = flow_detail_history_page_opts(opts)
    {record_status, record} = flow_detail_record(id, partition_key)
    {history_status, history, history_page} = flow_detail_history(id, record, history_page_opts)

    {values_status, value_refs, values_by_ref} =
      if Keyword.get(opts, :values, true) do
        flow_detail_values(record, history)
      else
        {:skipped, [], %{}}
      end

    record_partition_key = if is_map(record), do: flow_record_partition_key(record), else: nil
    detail_partition_key = flow_detail_url_partition_key(partition_key || record_partition_key)
    history_page = flow_detail_history_page_links(id, detail_partition_key, history_page)
    {state_mode, fifo_lane} = flow_detail_fifo_lane(record)

    %{
      id: id,
      partition_key: detail_partition_key,
      record: record,
      record_status: record_status,
      history: history,
      history_status: history_status,
      history_page: history_page,
      flash: Keyword.get(opts, :flash),
      value_refs: value_refs,
      values_by_ref: values_by_ref,
      values_status: values_status,
      waiting_reason: flow_waiting_reason(record),
      state_mode: state_mode,
      fifo_lane: fifo_lane,
      generated_at_ms: System.system_time(:millisecond)
    }
  end

  defp flow_detail_fifo_lane(%{} = record) do
    type = flow_record_type(record)
    logical_state = flow_record_logical_state(record)
    partition_key = flow_record_partition_key(record)
    mode = Fifo.effective_state_mode(type, logical_state)

    lane =
      if mode == :fifo and is_binary(partition_key) and partition_key != "" do
        @flow_dashboard_sample_limit
        |> collect_flow_records_sample()
        |> Enum.filter(&same_fifo_lane?(&1, type, logical_state, partition_key))
        |> include_flow_record(record)
        |> Fifo.lane_summaries()
        |> Enum.find(fn lane ->
          lane.type == type and lane.state == logical_state and
            lane.partition_key == partition_key
        end)
      end

    {mode, lane}
  rescue
    _ -> {:parallel, nil}
  catch
    :exit, _ -> {:parallel, nil}
  end

  defp flow_detail_fifo_lane(_record), do: {:parallel, nil}

  defp same_fifo_lane?(record, type, state, partition_key) when is_map(record) do
    flow_record_type(record) == type and
      flow_record_logical_state(record) == state and
      flow_record_partition_key(record) == partition_key
  end

  defp same_fifo_lane?(_record, _type, _state, _partition_key), do: false

  defp include_flow_record(records, %{} = record) when is_list(records) do
    id = flow_record_id(record)

    records
    |> Enum.reject(&(flow_record_id(&1) == id))
    |> Kernel.++([record])
  end

  defp flash_from_params(params) do
    case Map.get(params, "status") do
      "rewound" ->
        %{kind: :ok, message: "Flow rewound"}

      "signaled" ->
        %{kind: :ok, message: "Signal sent successfully"}

      "error" ->
        message =
          params
          |> Map.get("message", "Flow action failed")
          |> PolicyRetention.clean_form_value()

        %{kind: :error, message: if(message == "", do: "Flow action failed", else: message)}

      _ ->
        nil
    end
  rescue
    _ -> nil
  end

  defp flow_signal_optional_binary(params, key) do
    case Map.get(params, key) do
      nil ->
        {:ok, nil}

      "" ->
        {:ok, nil}

      value when is_binary(value) ->
        value = String.trim(value)
        if value == "", do: {:ok, nil}, else: {:ok, value}

      _ ->
        {:error, "ERR #{key} must be a string"}
    end
  end

  defp flow_signal_opts(partition_key, opts) do
    opts = Enum.reject(opts, fn {_key, value} -> is_nil(value) end)

    case partition_key do
      key when is_binary(key) and key != "" -> Keyword.put(opts, :partition_key, key)
      _ -> opts
    end
  end

  defp flow_signal_apply(id, opts) do
    case FerricStore.flow_signal(id, opts) do
      :ok -> :ok
      {:error, reason} when is_binary(reason) -> {:error, reason}
      {:error, reason} -> {:error, dashboard_internal_error("ERR FLOW.SIGNAL failed", reason)}
      other -> {:error, dashboard_internal_error("ERR unexpected FLOW.SIGNAL result", other)}
    end
  end

  defp flow_rewind_confirmed(params) do
    case Map.get(params, "confirm_rewind") do
      value when value in ["true", "on", "yes", "1"] -> :ok
      _ -> {:error, "ERR rewind requires confirm_rewind=true after reviewing the selected event"}
    end
  end

  defp flow_detail_partition_key(opts) do
    case Keyword.get(opts, :partition_key) do
      partition_key when is_binary(partition_key) and partition_key != "" -> partition_key
      _ -> nil
    end
  end

  defp flow_rewind_required_form_value(params, key, label) do
    case Map.get(params, key) do
      value when is_binary(value) ->
        value = String.trim(value)
        if value == "", do: {:error, "ERR #{label} is required"}, else: {:ok, value}

      _ ->
        {:error, "ERR #{label} is required"}
    end
  end

  defp flow_rewind_optional_non_neg_integer(params, key) do
    case Map.get(params, key) do
      nil ->
        {:ok, nil}

      "" ->
        {:ok, nil}

      value when is_binary(value) ->
        value = String.trim(value)

        case Integer.parse(value) do
          {parsed, ""} when parsed >= 0 -> {:ok, parsed}
          _ -> {:error, "ERR #{key} must be a non-negative integer"}
        end

      value when is_integer(value) and value >= 0 ->
        {:ok, value}

      _ ->
        {:error, "ERR #{key} must be a non-negative integer"}
    end
  end

  defp flow_rewind_current_record(id, partition_key) do
    case FerricStore.flow_get(id, flow_dashboard_get_opts(partition_key)) do
      {:ok, %{} = record} -> {:ok, record}
      {:ok, nil} -> {:error, "ERR flow not found"}
      {:error, reason} when is_binary(reason) -> {:error, reason}
      {:error, reason} -> {:error, dashboard_internal_error("ERR FLOW.GET failed", reason)}
      other -> {:error, dashboard_internal_error("ERR unexpected FLOW.GET result", other)}
    end
  end

  defp flow_rewind_existing_target_state(id, partition_key, to_event) do
    opts =
      flow_rewind_opts(partition_key,
        count: 1,
        values: false,
        consistent_projection: true,
        from_event: to_event,
        to_event: to_event
      )

    case FerricStore.flow_history(id, opts) do
      {:ok, [{event_id, fields}]} when is_map(fields) ->
        if to_string(event_id) == to_event do
          case flow_history_current_state(fields) do
            "" -> {:error, "ERR flow rewind target event has no state"}
            state -> {:ok, state}
          end
        else
          {:error, "ERR flow rewind target event is not in this flow history"}
        end

      {:ok, _events} ->
        {:error, "ERR flow rewind target event is not in this flow history"}

      {:error, reason} when is_binary(reason) ->
        {:error, reason}

      {:error, reason} ->
        {:error, dashboard_internal_error("ERR FLOW.HISTORY failed", reason)}

      other ->
        {:error, dashboard_internal_error("ERR unexpected FLOW.HISTORY result", other)}
    end
  end

  defp flow_rewind_opts(partition_key, opts) do
    opts = Enum.reject(opts, fn {_key, value} -> is_nil(value) end)

    case partition_key do
      key when is_binary(key) and key != "" -> Keyword.put(opts, :partition_key, key)
      _ -> opts
    end
  end

  defp flow_rewind_apply(id, opts) do
    case FerricStore.flow_rewind(id, opts) do
      :ok -> :ok
      {:ok, _record} -> :ok
      {:error, reason} when is_binary(reason) -> {:error, reason}
      {:error, reason} -> {:error, dashboard_internal_error("ERR FLOW.REWIND failed", reason)}
      other -> {:error, dashboard_internal_error("ERR unexpected FLOW.REWIND result", other)}
    end
  end

  defp flow_detail_record(id, partition_key) do
    sampled =
      @flow_dashboard_sample_limit
      |> collect_flow_records_sample()
      |> Enum.find(fn record ->
        flow_record_id(record) == id and flow_detail_partition_match?(record, partition_key)
      end)

    case sampled do
      %{} = record ->
        {:ok, record}

      nil ->
        timeout_ms = flow_dashboard_detail_fetch_timeout_ms()
        opts = flow_dashboard_get_opts(partition_key)

        case bounded_dashboard_call(
               fn -> flow_dashboard_flow_get(id, opts) end,
               timeout_ms,
               :record
             ) do
          {:ok, {:ok, %{} = record}} -> {:ok, record}
          {:ok, {:ok, nil}} -> {:not_found, nil}
          {:ok, {:error, reason}} -> {{:error, reason}, nil}
          {:ok, _other} -> {{:error, :unexpected_flow_get_result}, nil}
          {:error, :timeout} -> {:timeout, nil}
          {:error, reason} -> {{:error, reason}, nil}
        end
    end
  rescue
    reason -> {{:error, reason}, nil}
  catch
    :exit, reason -> {{:exit, reason}, nil}
  end

  defp flow_detail_partition_match?(_record, nil), do: true

  defp flow_detail_partition_match?(record, partition_key),
    do: flow_record_partition_key(record) == partition_key

  defp flow_dashboard_get_opts(nil), do: [payload: false]
  defp flow_dashboard_get_opts(partition_key), do: [payload: false, partition_key: partition_key]

  defp flow_detail_history_page_opts(opts) when is_list(opts) do
    before = normalize_flow_history_cursor(Keyword.get(opts, :history_before))

    after_cursor =
      if is_nil(before), do: normalize_flow_history_cursor(Keyword.get(opts, :history_after))

    %{
      count: normalize_flow_history_count(Keyword.get(opts, :history_count)),
      before: before,
      after_cursor: after_cursor,
      has_older: false,
      has_newer: false,
      oldest_event_id: nil,
      newest_event_id: nil
    }
  end

  defp flow_detail_history(_id, nil, page), do: {:skipped, [], page}

  defp flow_detail_history(id, record, page) do
    opts =
      case flow_record_partition_key(record) do
        partition_key when is_binary(partition_key) and partition_key != "" ->
          [
            count: flow_detail_history_fetch_count(page),
            values: false,
            consistent_projection: true,
            partition_key: partition_key
          ]

        _ ->
          [
            count: flow_detail_history_fetch_count(page),
            values: false,
            consistent_projection: true
          ]
      end
      |> flow_detail_history_cursor_opts(page)

    timeout_ms = flow_dashboard_detail_fetch_timeout_ms()

    case bounded_dashboard_call(
           fn -> flow_dashboard_flow_history(id, opts) end,
           timeout_ms,
           :history
         ) do
      {:ok, {:ok, history}} when is_list(history) ->
        {history, page} = flow_detail_history_page(history, page)
        {:ok, history, page}

      {:ok, {:error, reason}} ->
        {{:error, reason}, [], page}

      {:ok, _other} ->
        {{:error, :unexpected_flow_history_result}, [], page}

      {:error, :timeout} ->
        {:timeout, [], page}

      {:error, reason} ->
        {{:error, reason}, [], page}
    end
  rescue
    reason -> {{:error, reason}, [], page}
  catch
    :exit, reason -> {{:exit, reason}, [], page}
  end

  defp flow_detail_history_fetch_count(%{
         before: before,
         after_cursor: after_cursor,
         count: count
       })
       when is_binary(before) or is_binary(after_cursor),
       do: count + 2

  defp flow_detail_history_fetch_count(%{count: count}), do: count + 1

  defp flow_detail_history_cursor_opts(opts, %{before: before}) when is_binary(before) do
    opts
    |> Keyword.put(:to_event, before)
    |> Keyword.put(:rev, true)
  end

  defp flow_detail_history_cursor_opts(opts, %{after_cursor: after_cursor})
       when is_binary(after_cursor),
       do: Keyword.put(opts, :from_event, after_cursor)

  defp flow_detail_history_cursor_opts(opts, _page), do: opts

  defp flow_detail_history_page(history, %{before: before, count: count} = page)
       when is_binary(before) do
    older_desc = flow_history_drop_event(history, before)
    has_older = length(older_desc) > count
    page_events = older_desc |> Enum.take(count) |> Enum.reverse()

    {page_events,
     %{
       page
       | has_older: has_older,
         has_newer: page_events != [],
         oldest_event_id: flow_history_page_oldest_event_id(page_events),
         newest_event_id: flow_history_page_newest_event_id(page_events)
     }}
  end

  defp flow_detail_history_page(history, %{after_cursor: after_cursor, count: count} = page)
       when is_binary(after_cursor) do
    newer_asc = flow_history_drop_event(history, after_cursor)
    has_newer = length(newer_asc) > count
    page_events = Enum.take(newer_asc, count)

    {page_events,
     %{
       page
       | has_older: page_events != [],
         has_newer: has_newer,
         oldest_event_id: flow_history_page_oldest_event_id(page_events),
         newest_event_id: flow_history_page_newest_event_id(page_events)
     }}
  end

  defp flow_detail_history_page(history, %{count: count} = page) do
    has_older = length(history) > count
    page_events = Enum.take(history, -count)

    {page_events,
     %{
       page
       | has_older: has_older,
         has_newer: false,
         oldest_event_id: flow_history_page_oldest_event_id(page_events),
         newest_event_id: flow_history_page_newest_event_id(page_events)
     }}
  end

  defp flow_history_drop_event(history, event_id) do
    Enum.reject(history, fn entry ->
      entry
      |> normalize_flow_history_entry()
      |> elem(0)
      |> to_string() == event_id
    end)
  end

  defp flow_history_page_oldest_event_id([]), do: nil

  defp flow_history_page_oldest_event_id(history) do
    history
    |> List.first()
    |> normalize_flow_history_entry()
    |> elem(0)
    |> to_string()
  end

  defp flow_history_page_newest_event_id([]), do: nil

  defp flow_history_page_newest_event_id(history) do
    history
    |> List.last()
    |> normalize_flow_history_entry()
    |> elem(0)
    |> to_string()
  end

  defp flow_detail_history_page_links(id, partition_key, page) when is_map(page) do
    page
    |> Map.put(:id, id)
    |> Map.put(:partition_key, partition_key)
    |> Map.put(:older_url, flow_detail_history_older_url(id, partition_key, page))
    |> Map.put(:newer_url, flow_detail_history_newer_url(id, partition_key, page))
    |> Map.put(:current_live_params, flow_detail_history_current_params(page))
  end

  defp flow_detail_history_older_url(id, partition_key, %{
         has_older: true,
         oldest_event_id: oldest,
         count: count
       })
       when is_binary(oldest) do
    flow_detail_path(id, partition_key, %{"history_before" => oldest, "history_count" => count})
  end

  defp flow_detail_history_older_url(_id, _partition_key, _page), do: nil

  defp flow_detail_history_newer_url(id, partition_key, %{
         has_newer: true,
         newest_event_id: newest,
         count: count
       })
       when is_binary(newest) do
    flow_detail_path(id, partition_key, %{"history_after" => newest, "history_count" => count})
  end

  defp flow_detail_history_newer_url(_id, _partition_key, _page), do: nil

  defp flow_detail_history_current_params(%{before: before, count: count})
       when is_binary(before) do
    %{"history_before" => before, "history_count" => count}
  end

  defp flow_detail_history_current_params(%{after_cursor: after_cursor, count: count})
       when is_binary(after_cursor) do
    %{"history_after" => after_cursor, "history_count" => count}
  end

  defp flow_detail_history_current_params(%{count: @flow_dashboard_history_default_count}),
    do: %{}

  defp flow_detail_history_current_params(%{count: count}), do: %{"history_count" => count}

  defp flow_detail_values(nil, _history), do: {:skipped, [], %{}}

  defp flow_detail_values(record, history) do
    value_refs =
      record
      |> flow_detail_value_refs(history)
      |> Enum.take(@flow_dashboard_value_ref_limit)

    refs = Enum.map(value_refs, & &1.ref)

    if refs == [] do
      {:ok, value_refs, %{}}
    else
      timeout_ms = flow_dashboard_detail_fetch_timeout_ms()

      case bounded_dashboard_call(
             fn -> flow_dashboard_flow_value_mget(refs) end,
             timeout_ms,
             :values
           ) do
        {:ok, {:ok, values}} when is_list(values) and length(values) == length(refs) ->
          {:ok, value_refs, Map.new(Enum.zip(refs, values))}

        {:ok, {:ok, values}} when is_list(values) ->
          {{:error, :unexpected_flow_value_count}, value_refs, %{}}

        {:ok, {:error, reason}} ->
          {{:error, reason}, value_refs, %{}}

        {:ok, _other} ->
          {{:error, :unexpected_flow_value_result}, value_refs, %{}}

        {:error, :timeout} ->
          {:timeout, value_refs, %{}}

        {:error, reason} ->
          {{:error, reason}, value_refs, %{}}
      end
    end
  rescue
    reason -> {{:error, reason}, [], %{}}
  catch
    :exit, reason -> {{:exit, reason}, [], %{}}
  end
end
