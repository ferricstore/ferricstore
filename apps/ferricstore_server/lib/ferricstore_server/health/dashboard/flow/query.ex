defmodule FerricstoreServer.Health.Dashboard.Flow.Query do
  @moduledoc false

  alias FerricstoreServer.Health.Dashboard.Access, as: DashboardAccess
  alias FerricstoreServer.Health.QueryDecoder

  import FerricstoreServer.Health.Dashboard.Flow.Calls
  import FerricstoreServer.Health.Dashboard.Flow.Sample
  import FerricstoreServer.Health.Dashboard.FlowRecord
  import FerricstoreServer.Health.Dashboard.Render.FlowHistory, only: [flow_signal_rows: 2]

  @flow_dashboard_sample_limit 400
  @flow_dashboard_recent_limit 40
  @flow_dashboard_signal_flow_fetch_limit 80
  @flow_dashboard_signal_history_count 25
  @flow_terminal_states ~w(completed failed cancelled)

  @spec collect_lineage_page(keyword()) :: map()
  def collect_lineage_page(opts \\ []) when is_list(opts) do
    filters = flow_lineage_filters_from_opts(opts)
    acl_username = DashboardAccess.keyspace_acl_username(opts)

    sampled_records =
      @flow_dashboard_sample_limit
      |> collect_flow_records_sample()
      |> DashboardAccess.filter_flow_records_for_acl(acl_username)

    result =
      filters
      |> flow_lineage_query_result()
      |> DashboardAccess.flow_lineage_filter_result_for_acl(acl_username)

    %{
      filters: filters,
      result: result,
      records: Map.get(result, :records, []),
      summary: flow_lineage_summary(Map.get(result, :records, [])),
      hints: flow_lineage_hints(sampled_records),
      total_sampled: length(sampled_records),
      sample_limit: @flow_dashboard_sample_limit,
      generated_at_ms: System.system_time(:millisecond)
    }
  end

  @spec lineage_opts_from_query(binary()) :: keyword()
  def lineage_opts_from_query(query) when is_binary(query) do
    params = QueryDecoder.decode(query)

    []
    |> maybe_put_query_opt(:mode, normalize_flow_lineage_mode(Map.get(params, "mode")))
    |> maybe_put_query_opt(:target, normalize_flow_name_filter(Map.get(params, "id")))
    |> maybe_put_query_opt(
      :partition_key,
      normalize_flow_partition_query(Map.get(params, "partition_key"))
    )
    |> maybe_put_query_opt(:limit, normalize_flow_limit_filter(Map.get(params, "limit")))
    |> Enum.reverse()
  end

  def lineage_opts_from_query(_query), do: []

  @spec collect_query_page(keyword()) :: map()
  def collect_query_page(opts \\ []) when is_list(opts) do
    filters = flow_query_filters_from_opts(opts)
    acl_username = DashboardAccess.keyspace_acl_username(opts)

    sampled_records =
      @flow_dashboard_sample_limit
      |> collect_flow_records_sample()
      |> DashboardAccess.filter_flow_records_for_acl(acl_username)

    {query_result, query_acl_scope} = execute_flow_query_for_acl(filters, acl_username)

    %{
      filters: filters,
      result:
        query_result
        |> DashboardAccess.flow_query_filter_result_for_acl(
          acl_username,
          query_acl_scope
        ),
      available_types: flow_available_types(sampled_records),
      total_sampled: length(sampled_records),
      sample_limit: @flow_dashboard_sample_limit,
      generated_at_ms: System.system_time(:millisecond)
    }
  end

  @spec query_opts_from_query(binary()) :: keyword()
  def query_opts_from_query(query) when is_binary(query) do
    params = QueryDecoder.decode(query)

    []
    |> maybe_put_query_opt(:kind, normalize_flow_query_kind(Map.get(params, "kind")))
    |> maybe_put_query_opt(:type, normalize_flow_type_filter(Map.get(params, "type")))
    |> maybe_put_query_opt(:state, normalize_flow_state_filter(Map.get(params, "state")))
    |> maybe_put_query_opt(
      :attribute_key,
      normalize_flow_name_filter(Map.get(params, "attribute_key"))
    )
    |> maybe_put_query_opt(
      :attribute_value,
      normalize_flow_name_filter(Map.get(params, "attribute_value"))
    )
    |> maybe_put_query_opt(
      :state_meta_state,
      normalize_flow_state_filter(Map.get(params, "state_meta_state"))
    )
    |> maybe_put_query_opt(
      :state_meta_key,
      normalize_flow_name_filter(Map.get(params, "state_meta_key"))
    )
    |> maybe_put_query_opt(
      :state_meta_value,
      normalize_flow_name_filter(Map.get(params, "state_meta_value"))
    )
    |> maybe_put_query_opt(:id, normalize_flow_name_filter(Map.get(params, "id")))
    |> maybe_put_query_opt(
      :partition_key,
      normalize_flow_partition_query(Map.get(params, "partition_key"))
    )
    |> maybe_put_query_opt(:limit, normalize_flow_limit_filter(Map.get(params, "limit")))
    |> maybe_put_query_opt(:from_ms, parse_flow_time_filter(Map.get(params, "from")))
    |> maybe_put_query_opt(:to_ms, parse_flow_time_filter(Map.get(params, "to")))
    |> maybe_put_query_opt(:rev, normalize_flow_boolean_filter(Map.get(params, "rev")))
    |> Enum.reverse()
  end

  def query_opts_from_query(_query), do: []

  defp flow_query_acl_scope(filters) when is_map(filters) do
    partition_key = Map.get(filters, :partition_key)
    id = Map.get(filters, :id)

    cond do
      is_binary(partition_key) and partition_key != "" -> partition_key
      Map.get(filters, :kind) == "history" and is_binary(id) and id != "" -> id
      true -> "*"
    end
  end

  defp execute_flow_query_for_acl(
         %{kind: "history", id: id} = filters,
         username
       )
       when is_binary(id) and id != "" and is_binary(username) do
    opts =
      [payload: false]
      |> maybe_put_query_opt(:partition_key, Map.get(filters, :partition_key))
      |> Enum.reverse()

    case bounded_dashboard_call(
           fn -> flow_dashboard_flow_get(id, opts) end,
           flow_dashboard_detail_fetch_timeout_ms(),
           :query_history_record
         ) do
      {:ok, {:ok, record}} when is_map(record) ->
        execute_authorized_flow_history_query(filters, record, username)

      _other ->
        denied_flow_history_query(filters)
    end
  end

  defp execute_flow_query_for_acl(filters, _username),
    do: {flow_query_execute(filters), flow_query_acl_scope(filters)}

  defp execute_authorized_flow_history_query(filters, record, username) do
    if DashboardAccess.flow_record_allowed_for_acl?(record, username) do
      partition_key = flow_record_partition_key(record)

      filters =
        if is_binary(partition_key) and partition_key != "" do
          Map.put(filters, :partition_key, partition_key)
        else
          filters
        end

      {flow_query_execute(filters), partition_key || flow_record_id(record)}
    else
      denied_flow_history_query(filters)
    end
  end

  defp denied_flow_history_query(filters) do
    result = %{
      status: :ok,
      command: flow_query_kind_command(Map.get(filters, :kind)),
      rows: [],
      message: "0 row(s)"
    }

    {result, "*"}
  end

  @spec collect_signals_page(keyword()) :: map()
  def collect_signals_page(opts \\ []) when is_list(opts) do
    filters = flow_signals_filters_from_opts(opts)

    records =
      @flow_dashboard_sample_limit
      |> collect_flow_records_sample()
      |> DashboardAccess.filter_flow_records_for_acl(DashboardAccess.keyspace_acl_username(opts))

    type_records = filter_flow_records_by_type(records, filters.type)
    filtered_records = filter_flow_records_by_name(type_records, filters.q)

    signals =
      if filters.scan_history do
        filtered_records
        |> flow_recent_records(@flow_dashboard_signal_flow_fetch_limit)
        |> Enum.flat_map(&flow_signal_rows_for_record/1)
        |> filter_flow_signal_rows(filters)
        |> Enum.sort_by(&flow_signal_sort_key/1, :desc)
        |> Enum.take(filters.limit)
      else
        []
      end

    %{
      signals: signals,
      filters: filters,
      available_types: flow_available_types(records),
      total_sampled: length(records),
      filtered_sampled: length(filtered_records),
      sample_limit: @flow_dashboard_sample_limit,
      generated_at_ms: System.system_time(:millisecond)
    }
  end

  @spec signals_opts_from_query(binary()) :: keyword()
  def signals_opts_from_query(query) when is_binary(query) do
    params = QueryDecoder.decode(query)

    []
    |> maybe_put_query_opt(:type, normalize_flow_type_filter(Map.get(params, "type")))
    |> maybe_put_query_opt(:signal, normalize_flow_name_filter(Map.get(params, "signal")))
    |> maybe_put_query_opt(:q, normalize_flow_name_filter(Map.get(params, "q")))
    |> maybe_put_query_opt(:limit, normalize_flow_limit_filter(Map.get(params, "limit")))
    |> maybe_put_query_opt(:scan_history, normalize_flow_boolean_filter(Map.get(params, "scan")))
    |> Enum.reverse()
  end

  def signals_opts_from_query(_query), do: []

  @spec signals_page_filters(map()) :: map()
  def signals_page_filters(data) when is_map(data) do
    Map.get(data, :filters, %{
      type: nil,
      signal: nil,
      q: nil,
      limit: @flow_dashboard_recent_limit,
      scan_history: false
    })
  end

  defp flow_lineage_filters_from_opts(opts) when is_list(opts) do
    %{
      mode: normalize_flow_lineage_mode(Keyword.get(opts, :mode)),
      target: normalize_flow_name_filter(Keyword.get(opts, :target)),
      partition_key: normalize_flow_partition_query(Keyword.get(opts, :partition_key)),
      limit: normalize_flow_limit_filter(Keyword.get(opts, :limit))
    }
  end

  defp normalize_flow_lineage_mode("parent"), do: "parent"
  defp normalize_flow_lineage_mode("root"), do: "root"
  defp normalize_flow_lineage_mode("correlation"), do: "correlation"
  defp normalize_flow_lineage_mode(_mode), do: "root"

  defp flow_lineage_query_result(%{target: nil}) do
    %{status: :idle, records: [], command: "FLOW.BY_ROOT", message: "Enter a lineage id"}
  end

  defp flow_lineage_query_result(%{target: target, mode: mode} = filters) do
    opts =
      [
        count: filters.limit,
        include_cold: true,
        consistent_projection: true
      ]
      |> maybe_put_query_opt(:partition_key, filters.partition_key)
      |> Enum.reverse()

    {command, fun} =
      case mode do
        "parent" ->
          {"FLOW.BY_PARENT", fn -> flow_dashboard_flow_by_parent(target, opts) end}

        "correlation" ->
          {"FLOW.BY_CORRELATION", fn -> flow_dashboard_flow_by_correlation(target, opts) end}

        _ ->
          {"FLOW.BY_ROOT", fn -> flow_dashboard_flow_by_root(target, opts) end}
      end

    case bounded_dashboard_call(fun, flow_dashboard_list_fetch_timeout_ms(), :lineage) do
      {:ok, {:ok, records}} when is_list(records) ->
        %{status: :ok, command: command, records: records, message: "#{length(records)} records"}

      {:ok, {:error, reason}} ->
        %{status: :error, command: command, records: [], message: inspect(reason)}

      {:error, :timeout} ->
        %{status: :timeout, command: command, records: [], message: "query timed out"}

      {:error, reason} ->
        %{status: :error, command: command, records: [], message: inspect(reason)}

      _ ->
        %{status: :error, command: command, records: [], message: "unexpected query result"}
    end
  end

  defp flow_lineage_summary(records) do
    terminal = Enum.count(records, &(flow_record_state(&1) in @flow_terminal_states))

    %{
      total: length(records),
      active: max(length(records) - terminal, 0),
      terminal: terminal,
      failed: Enum.count(records, &flow_failed?/1)
    }
  end

  defp flow_lineage_hints(records) do
    records
    |> Enum.flat_map(fn record ->
      [
        %{mode: "root", label: "root", id: flow_record_root_id(record)},
        %{mode: "parent", label: "parent", id: flow_record_parent_id(record)},
        %{mode: "correlation", label: "correlation", id: flow_record_correlation_id(record)}
      ]
    end)
    |> Enum.filter(&(is_binary(&1.id) and &1.id != ""))
    |> Enum.uniq_by(fn hint -> {hint.mode, hint.id} end)
    |> Enum.take(8)
  end

  defp flow_query_filters_from_opts(opts) when is_list(opts) do
    %{
      kind: normalize_flow_query_kind(Keyword.get(opts, :kind)),
      type: normalize_flow_type_filter(Keyword.get(opts, :type)),
      state: normalize_flow_state_filter(Keyword.get(opts, :state)),
      attribute_key: normalize_flow_name_filter(Keyword.get(opts, :attribute_key)),
      attribute_value: normalize_flow_name_filter(Keyword.get(opts, :attribute_value)),
      state_meta_state: normalize_flow_state_filter(Keyword.get(opts, :state_meta_state)),
      state_meta_key: normalize_flow_name_filter(Keyword.get(opts, :state_meta_key)),
      state_meta_value: normalize_flow_name_filter(Keyword.get(opts, :state_meta_value)),
      id: normalize_flow_name_filter(Keyword.get(opts, :id)),
      partition_key: normalize_flow_partition_query(Keyword.get(opts, :partition_key)),
      limit: normalize_flow_limit_filter(Keyword.get(opts, :limit)),
      from_ms: Keyword.get(opts, :from_ms),
      to_ms: Keyword.get(opts, :to_ms),
      rev: Keyword.get(opts, :rev) == true
    }
  end

  defp normalize_flow_query_kind("terminals"), do: "terminals"
  defp normalize_flow_query_kind("search"), do: "search"
  defp normalize_flow_query_kind("stats"), do: "stats"
  defp normalize_flow_query_kind("failures"), do: "failures"
  defp normalize_flow_query_kind("stuck"), do: "stuck"
  defp normalize_flow_query_kind("history"), do: "history"
  defp normalize_flow_query_kind("by_parent"), do: "by_parent"
  defp normalize_flow_query_kind("by_root"), do: "by_root"
  defp normalize_flow_query_kind("by_correlation"), do: "by_correlation"
  defp normalize_flow_query_kind(_kind), do: "list"

  defp flow_query_execute(filters) do
    case flow_query_plan(filters) do
      {:ok, command, fun} ->
        case bounded_dashboard_call(fun, flow_dashboard_list_fetch_timeout_ms(), :query) do
          {:ok, {:ok, rows}} when is_list(rows) ->
            %{status: :ok, command: command, rows: rows, message: "#{length(rows)} row(s)"}

          {:ok, {:ok, row}} ->
            %{status: :ok, command: command, rows: List.wrap(row), message: "1 row"}

          {:ok, {:error, reason}} ->
            %{status: :error, command: command, rows: [], message: inspect(reason)}

          {:error, :timeout} ->
            %{status: :timeout, command: command, rows: [], message: "query timed out"}

          {:error, reason} ->
            %{status: :error, command: command, rows: [], message: inspect(reason)}

          _other ->
            %{status: :error, command: command, rows: [], message: "unexpected query result"}
        end

      {:idle, command, message} ->
        %{status: :idle, command: command, rows: [], message: message}
    end
  end

  defp flow_query_plan(%{kind: kind, type: type})
       when kind in ["list", "search", "stats", "terminals", "failures", "stuck"] and
              (not is_binary(type) or type == "") do
    {:idle, flow_query_kind_command(kind), "Enter a workflow type"}
  end

  defp flow_query_plan(%{kind: kind, id: id})
       when kind in ["history", "by_parent", "by_root", "by_correlation"] and
              (not is_binary(id) or id == "") do
    {:idle, flow_query_kind_command(kind), "Enter an id"}
  end

  defp flow_query_plan(%{kind: "history", id: id} = filters) do
    opts =
      [count: filters.limit, values: false, consistent_projection: true]
      |> maybe_put_query_opt(:partition_key, filters.partition_key)
      |> Enum.reverse()

    {:ok, "FLOW.HISTORY", fn -> flow_dashboard_flow_history(id, opts) end}
  end

  defp flow_query_plan(%{kind: "by_parent", id: id} = filters) do
    opts = flow_query_index_opts(filters)
    {:ok, "FLOW.BY_PARENT", fn -> flow_dashboard_flow_by_parent(id, opts) end}
  end

  defp flow_query_plan(%{kind: "by_root", id: id} = filters) do
    opts = flow_query_index_opts(filters)
    {:ok, "FLOW.BY_ROOT", fn -> flow_dashboard_flow_by_root(id, opts) end}
  end

  defp flow_query_plan(%{kind: "by_correlation", id: id} = filters) do
    opts = flow_query_index_opts(filters)
    {:ok, "FLOW.BY_CORRELATION", fn -> flow_dashboard_flow_by_correlation(id, opts) end}
  end

  defp flow_query_plan(%{kind: "search"} = filters) do
    attributes = flow_query_attribute_filter(filters)
    state_meta = flow_query_state_meta_filter(filters)

    if is_nil(attributes) and is_nil(state_meta) do
      {:idle, "FLOW.SEARCH", "Enter an indexed attribute or state metadata filter"}
    else
      opts =
        [
          type: filters.type,
          count: filters.limit,
          consistent_projection: true
        ]
        |> maybe_put_query_opt(:state, filters.state)
        |> maybe_put_query_opt(:partition_key, filters.partition_key)
        |> maybe_put_query_opt(:from_ms, filters.from_ms)
        |> maybe_put_query_opt(:to_ms, filters.to_ms)
        |> maybe_put_query_opt(:rev, if(filters.rev, do: true, else: nil))
        |> maybe_put_query_opt(:attributes, attributes)
        |> maybe_put_query_opt(:state_meta, state_meta)
        |> Enum.reverse()

      {:ok, "FLOW.SEARCH", fn -> flow_dashboard_flow_search(opts) end}
    end
  end

  defp flow_query_plan(%{kind: "terminals", type: type} = filters) do
    opts = flow_query_terminal_opts(filters)
    {:ok, "FLOW.TERMINALS", fn -> flow_dashboard_flow_terminals(type, opts) end}
  end

  defp flow_query_plan(%{kind: "failures", type: type} = filters) do
    opts = flow_query_terminal_opts(filters)
    {:ok, "FLOW.FAILURES", fn -> flow_dashboard_flow_failures(type, opts) end}
  end

  defp flow_query_plan(%{kind: "stuck", type: type} = filters) do
    opts = flow_query_index_opts(filters)
    {:ok, "FLOW.STUCK", fn -> flow_dashboard_flow_stuck(type, opts) end}
  end

  defp flow_query_plan(%{kind: "stats", type: type} = filters) do
    opts =
      flow_query_index_opts(filters)
      |> maybe_put_query_opt(:state, filters.state)

    {:ok, "FLOW.STATS", fn -> flow_dashboard_flow_stats(type, opts) end}
  end

  defp flow_query_plan(%{type: type} = filters) do
    opts =
      flow_query_index_opts(filters)
      |> maybe_put_query_opt(:state, filters.state)

    {:ok, "FLOW.LIST", fn -> flow_dashboard_flow_list(type, opts) end}
  end

  defp flow_query_index_opts(filters) do
    [
      count: filters.limit,
      include_cold: true,
      consistent_projection: true
    ]
    |> maybe_put_query_opt(:partition_key, filters.partition_key)
    |> maybe_put_query_opt(:from_ms, filters.from_ms)
    |> maybe_put_query_opt(:to_ms, filters.to_ms)
    |> maybe_put_query_opt(:rev, if(filters.rev, do: true, else: nil))
    |> maybe_put_query_opt(:attributes, flow_query_attribute_filter(filters))
    |> Enum.reverse()
  end

  defp flow_query_attribute_filter(%{attribute_key: key, attribute_value: value})
       when is_binary(key) and key != "" and is_binary(value) and value != "",
       do: %{key => value}

  defp flow_query_attribute_filter(_filters), do: nil

  defp flow_query_state_meta_filter(%{
         state_meta_state: state,
         state_meta_key: key,
         state_meta_value: value
       })
       when is_binary(state) and state != "" and is_binary(key) and key != "" and
              is_binary(value) and value != "",
       do: %{state => %{key => value}}

  defp flow_query_state_meta_filter(_filters), do: nil

  defp flow_query_terminal_opts(filters) do
    filters
    |> flow_query_index_opts()
    |> maybe_put_query_opt(:state, filters.state)
  end

  defp flow_query_kind_command("terminals"), do: "FLOW.TERMINALS"
  defp flow_query_kind_command("search"), do: "FLOW.SEARCH"
  defp flow_query_kind_command("stats"), do: "FLOW.STATS"
  defp flow_query_kind_command("failures"), do: "FLOW.FAILURES"
  defp flow_query_kind_command("stuck"), do: "FLOW.STUCK"
  defp flow_query_kind_command("history"), do: "FLOW.HISTORY"
  defp flow_query_kind_command("by_parent"), do: "FLOW.BY_PARENT"
  defp flow_query_kind_command("by_root"), do: "FLOW.BY_ROOT"
  defp flow_query_kind_command("by_correlation"), do: "FLOW.BY_CORRELATION"
  defp flow_query_kind_command(_kind), do: "FLOW.LIST"

  defp flow_signals_filters_from_opts(opts) when is_list(opts) do
    %{
      type: normalize_flow_type_filter(Keyword.get(opts, :type)),
      signal: normalize_flow_name_filter(Keyword.get(opts, :signal)),
      q: normalize_flow_name_filter(Keyword.get(opts, :q)),
      limit: normalize_flow_limit_filter(Keyword.get(opts, :limit)),
      scan_history: normalize_flow_boolean_filter(Keyword.get(opts, :scan_history))
    }
  end

  defp flow_signal_rows_for_record(record) when is_map(record) do
    id = flow_record_id(record)
    partition_key = flow_record_partition_key(record)

    opts =
      [
        count: @flow_dashboard_signal_history_count,
        values: false,
        consistent_projection: true
      ]
      |> maybe_put_query_opt(:partition_key, flow_detail_url_partition_key(partition_key))

    timeout_ms = flow_dashboard_detail_fetch_timeout_ms()

    case bounded_dashboard_call(
           fn -> flow_dashboard_flow_history(id, opts) end,
           timeout_ms,
           :signals_history
         ) do
      {:ok, {:ok, history}} when is_list(history) -> flow_signal_rows(record, history)
      _ -> []
    end
  rescue
    _ -> []
  catch
    :exit, _ -> []
  end

  defp filter_flow_signal_rows(rows, filters) when is_map(filters) do
    case Map.get(filters, :signal) do
      nil ->
        rows

      signal when is_binary(signal) ->
        needle = String.downcase(signal)

        Enum.filter(rows, fn row ->
          row.signal
          |> to_string()
          |> String.downcase()
          |> String.contains?(needle)
        end)
    end
  end

  defp flow_signal_sort_key(row) do
    {Map.get(row, :time_ms) || -1, Map.get(row, :event_id, "")}
  end
end
