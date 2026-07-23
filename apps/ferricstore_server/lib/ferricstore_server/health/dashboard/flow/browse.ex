defmodule FerricstoreServer.Health.Dashboard.Flow.Browse do
  @moduledoc false

  alias Ferricstore.Flow.Query.Builder
  alias FerricstoreServer.Health.Dashboard.Access, as: DashboardAccess
  alias FerricstoreServer.Health.Dashboard.Flow.Fifo
  alias FerricstoreServer.Health.Dashboard.Flow.Projection
  alias FerricstoreServer.Health.QueryDecoder

  import FerricstoreServer.Health.Dashboard.Flow.Calls
  import FerricstoreServer.Health.Dashboard.Flow.Sample
  import FerricstoreServer.Health.Dashboard.FlowRecord

  @flow_dashboard_sample_limit 400
  @flow_dashboard_recent_limit 40
  @flow_dashboard_overview_recent_limit 10
  @flow_terminal_states ~w(completed failed cancelled)

  @spec collect_overview_page(keyword()) :: map()
  def collect_overview_page(opts \\ []) when is_list(opts) do
    filters = flow_overview_filters_from_opts(opts)
    sampled_records = collect_flow_records_sample(@flow_dashboard_sample_limit)
    acl_username = DashboardAccess.keyspace_acl_username(opts)

    visible_records =
      sampled_records
      |> DashboardAccess.filter_flow_records_for_acl(acl_username)

    records =
      visible_records
      |> filter_flow_records_by_partition(filters.partition_key)

    types = flow_type_summaries(records)

    %{
      summary: flow_page_summary(types, records),
      projection:
        if(is_binary(acl_username), do: %{restricted: true}, else: Projection.collect_health()),
      types: types,
      records: flow_recent_records(records, @flow_dashboard_overview_recent_limit),
      workers: flow_worker_summaries(records),
      filters: filters,
      total_sampled:
        if(is_binary(acl_username), do: length(visible_records), else: length(sampled_records)),
      filtered_sampled: length(records),
      sample_limit: @flow_dashboard_sample_limit,
      generated_at_ms: System.system_time(:millisecond)
    }
  end

  @spec overview_opts_from_query(binary()) :: keyword()
  def overview_opts_from_query(query) when is_binary(query) do
    params = QueryDecoder.decode(query)

    []
    |> maybe_put_query_opt(
      :partition_key,
      normalize_flow_partition_query(Map.get(params, "partition_key"))
    )
    |> Enum.reverse()
  end

  def overview_opts_from_query(_query), do: []

  @spec overview_page_filters(map()) :: map()
  def overview_page_filters(data) when is_map(data) do
    Map.get(data, :filters, %{partition_key: nil})
  end

  @spec collect_states_page(keyword()) :: map()
  def collect_states_page(opts \\ []) do
    filters = flow_state_filters_from_opts(opts)
    acl_username = DashboardAccess.keyspace_acl_username(opts)

    records =
      @flow_dashboard_sample_limit
      |> collect_flow_records_sample()
      |> DashboardAccess.filter_flow_records_for_acl(acl_username)
      |> filter_flow_records_by_partition(filters.partition_key)

    available_types = flow_available_types(records)
    terminal_records = collect_flow_states_terminal_records(filters, available_types)

    type_records =
      records
      |> merge_flow_records(
        DashboardAccess.filter_flow_records_for_acl(terminal_records, acl_username)
      )
      |> filter_flow_records_by_type(filters.type)

    filtered_records = filter_flow_records(type_records, filters)
    fifo_records = filter_flow_records_for_fifo_lanes(type_records, filters)

    %{
      states: filtered_records |> flow_state_summaries() |> Fifo.annotate_state_summaries(),
      fifo_lanes: Fifo.lane_summaries(fifo_records),
      records: flow_recent_records(filtered_records, filters.limit),
      available_types: flow_available_types(type_records ++ records),
      available_states:
        flow_available_states(type_records) |> maybe_include_flow_state(filters.state),
      filters: filters,
      type_filter: filters.type,
      state_filter: filters.state,
      name_filter: filters.q,
      range_filter: filters.range,
      from_ms: filters.from_ms,
      to_ms: filters.to_ms,
      limit: filters.limit,
      total_sampled: length(type_records),
      filtered_sampled: length(filtered_records),
      sample_limit: max(@flow_dashboard_sample_limit, length(type_records))
    }
  end

  @spec states_opts_from_query(binary()) :: keyword()
  def states_opts_from_query(query) when is_binary(query) do
    params = QueryDecoder.decode(query)

    []
    |> maybe_put_query_opt(:type, normalize_flow_type_filter(Map.get(params, "type")))
    |> maybe_put_query_opt(:state, normalize_flow_state_filter(Map.get(params, "state")))
    |> maybe_put_query_opt(
      :partition_key,
      normalize_flow_partition_query(Map.get(params, "partition_key"))
    )
    |> maybe_put_query_opt(:q, normalize_flow_name_filter(Map.get(params, "q")))
    |> maybe_put_query_opt(:range, normalize_flow_range_filter(Map.get(params, "range")))
    |> maybe_put_query_opt(
      :from_ms,
      parse_flow_time_filter(
        Map.get(params, "from_ms") || Map.get(params, "from") || Map.get(params, "from_at")
      )
    )
    |> maybe_put_query_opt(
      :to_ms,
      parse_flow_time_filter(
        Map.get(params, "to_ms") || Map.get(params, "to") || Map.get(params, "to_at")
      )
    )
    |> maybe_put_query_opt(:limit, normalize_flow_limit_filter(Map.get(params, "limit")))
    |> Enum.reverse()
  end

  def states_opts_from_query(_query), do: []

  @spec states_page_filters(map()) :: map()
  def states_page_filters(data) when is_map(data) do
    Map.get(data, :filters, %{
      type: Map.get(data, :type_filter),
      state: Map.get(data, :state_filter),
      partition_key: nil,
      q: Map.get(data, :name_filter),
      range: Map.get(data, :range_filter),
      from_ms: Map.get(data, :from_ms),
      to_ms: Map.get(data, :to_ms),
      limit: Map.get(data, :limit, @flow_dashboard_recent_limit)
    })
  end

  @spec states_page_limit(map()) :: pos_integer()
  def states_page_limit(data) when is_map(data),
    do: Map.get(data, :limit, @flow_dashboard_recent_limit)

  @spec collect_workers_page(keyword()) :: map()
  def collect_workers_page(opts \\ []) do
    records =
      @flow_dashboard_sample_limit
      |> collect_flow_records_sample()
      |> DashboardAccess.filter_flow_records_for_acl(DashboardAccess.keyspace_acl_username(opts))

    %{
      workers: flow_worker_summaries(records),
      running_records: Enum.filter(records, &(flow_record_state(&1) == "running")),
      fifo_lanes: Fifo.lane_summaries(records),
      total_sampled: length(records),
      sample_limit: @flow_dashboard_sample_limit
    }
  end

  @spec collect_due_page(keyword()) :: map()
  def collect_due_page(opts \\ []) do
    records =
      @flow_dashboard_sample_limit
      |> collect_flow_records_sample()
      |> DashboardAccess.filter_flow_records_for_acl(DashboardAccess.keyspace_acl_username(opts))

    %{
      due_now: Enum.filter(records, &flow_due_now?/1),
      scheduled: records |> Enum.filter(&flow_scheduled_future?/1) |> flow_recent_records(80),
      fifo_lanes: Fifo.lane_summaries(records),
      total_sampled: length(records),
      sample_limit: @flow_dashboard_sample_limit
    }
  end

  defp filter_flow_records_for_fifo_lanes(records, filters) when is_map(filters) do
    records
    |> filter_flow_fifo_records_by_logical_state(Map.get(filters, :state))
    |> filter_flow_records_by_name(Map.get(filters, :q))
    |> filter_flow_records_by_updated_window(Map.get(filters, :from_ms), Map.get(filters, :to_ms))
  end

  defp filter_flow_fifo_records_by_logical_state(records, nil), do: records

  defp filter_flow_fifo_records_by_logical_state(records, state) when is_binary(state) do
    Enum.filter(records, &(flow_record_logical_state(&1) == state))
  end

  defp filter_flow_records_by_updated_window(records, nil, nil), do: records

  defp filter_flow_records_by_updated_window(records, from_ms, to_ms) do
    Enum.filter(records, fn record ->
      updated_at = flow_record_updated_at_ms(record)
      after_from? = not is_integer(from_ms) or updated_at >= from_ms
      before_to? = not is_integer(to_ms) or updated_at <= to_ms
      after_from? and before_to?
    end)
  end

  defp collect_flow_states_terminal_records(filters, available_types) do
    terminal_states = flow_states_terminal_fetch_states(filters)

    if terminal_states == [] or not is_binary(filters.partition_key) do
      []
    else
      filters
      |> flow_states_terminal_fetch_types(available_types)
      |> flow_fetch_terminal_records(
        terminal_states,
        filters.limit,
        filters.partition_key
      )
    end
  end

  defp flow_states_terminal_fetch_states(%{state: state}) when state in @flow_terminal_states,
    do: [state]

  defp flow_states_terminal_fetch_states(%{state: nil, type: type})
       when is_binary(type) and type != "",
       do: @flow_terminal_states

  defp flow_states_terminal_fetch_states(_filters), do: []

  defp flow_states_terminal_fetch_types(%{type: type}, _available_types)
       when is_binary(type) and type != "",
       do: [type]

  defp flow_states_terminal_fetch_types(_filters, available_types), do: available_types

  defp flow_fetch_terminal_records(types, terminal_states, limit, partition_key) when limit > 0 do
    types
    |> Enum.reduce_while({[], limit}, fn type, {acc, remaining} ->
      if remaining <= 0 do
        {:halt, {acc, 0}}
      else
        records =
          flow_fetch_terminal_records_for_type(type, terminal_states, remaining, partition_key)

        {:cont, {prepend_flow_dashboard_chunk(records, acc), max(remaining - length(records), 0)}}
      end
    end)
    |> elem(0)
    |> flatten_flow_dashboard_chunks()
    |> Enum.take(limit)
  end

  defp flow_fetch_terminal_records(_types, _terminal_states, _limit, _partition_key), do: []

  defp flow_fetch_terminal_records_for_type(type, terminal_states, limit, partition_key) do
    terminal_states
    |> Enum.reduce_while({[], limit}, fn state, {acc, remaining} ->
      if remaining <= 0 do
        {:halt, {acc, 0}}
      else
        case flow_dashboard_terminal_records(type, state, remaining, partition_key) do
          {:ok, records} ->
            {:cont,
             {prepend_flow_dashboard_chunk(records, acc), max(remaining - length(records), 0)}}

          {:error, _reason} ->
            {:cont, {acc, remaining}}
        end
      end
    end)
    |> elem(0)
    |> flatten_flow_dashboard_chunks()
  end

  defp flow_dashboard_terminal_records(type, state, limit, partition_key) do
    with {:ok, %{query: query, params: params}} <-
           Builder.build(:terminals, %{
             type: type,
             state: state,
             partition_key: partition_key,
             limit: min(limit, 100)
           }) do
      case bounded_dashboard_call(
             fn -> flow_dashboard_flow_query(query, params) end,
             flow_dashboard_list_fetch_timeout_ms(),
             :terminal_records
           ) do
        {:ok, {:ok, records}} when is_list(records) -> {:ok, records}
        {:ok, {:ok, %{records: records}}} when is_list(records) -> {:ok, records}
        {:ok, {:error, reason}} -> {:error, reason}
        {:ok, other} -> {:error, {:unexpected_flow_query_result, other}}
        {:error, reason} -> {:error, reason}
      end
    end
  rescue
    reason -> {:error, reason}
  catch
    :exit, reason -> {:error, {:exit, reason}}
  end
end
