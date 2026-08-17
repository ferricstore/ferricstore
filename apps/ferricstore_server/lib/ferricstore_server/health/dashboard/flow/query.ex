defmodule FerricstoreServer.Health.Dashboard.Flow.Query do
  @moduledoc false

  alias Ferricstore.Commands.PreparedCommand
  alias Ferricstore.Flow.Attributes
  alias Ferricstore.Flow.Query.{Builder, Field, Limits, Request}
  alias FerricstoreServer.Health.Dashboard.Access, as: DashboardAccess

  alias FerricstoreServer.Health.Dashboard.Flow.{
    QueryDiscovery,
    QueryResult,
    QueryVisualization,
    QueryWorkbench
  }

  alias FerricstoreServer.Health.QueryDecoder

  import FerricstoreServer.Health.Dashboard.Flow.Calls
  import FerricstoreServer.Health.Dashboard.Flow.Sample
  import FerricstoreServer.Health.Dashboard.FlowRecord
  import FerricstoreServer.Health.Dashboard.Render.FlowHistory, only: [flow_signal_rows: 2]

  @flow_dashboard_sample_limit 400
  @flow_dashboard_recent_limit 40
  @flow_dashboard_signal_scan_max_flows 16
  @flow_dashboard_signal_scan_max_concurrency 4
  @flow_dashboard_signal_scan_fetch_timeout_ms 1_000
  @flow_dashboard_signal_scan_max_fetch_timeout_ms 5_000
  @flow_dashboard_signal_history_count 25
  @flow_terminal_states ~w(completed failed cancelled)
  @flow_query_predicates [
    :type,
    :state,
    :run_state,
    :attribute,
    :state_meta,
    :id,
    :time,
    :direction
  ]
  @flow_query_predicate_labels %{
    type: "Workflow type",
    state: "Lifecycle state",
    run_state: "Workflow step",
    attribute: "Attribute filters",
    state_meta: "State metadata",
    id: "ID",
    time: "Time range",
    direction: "Newest-first ordering"
  }
  @flow_query_allowed_predicates %{
    "list" => [:type, :state, :run_state, :attribute, :time, :direction],
    "search" => [:type, :state, :run_state, :attribute, :state_meta, :time, :direction],
    "stats" => [:type, :state, :attribute],
    "terminals" => [:type, :state, :time, :direction],
    "failures" => [:type, :time, :direction],
    "stuck" => [:type, :time, :direction],
    "history" => [:id],
    "by_parent" => [:id, :time, :direction],
    "by_root" => [:id, :time, :direction],
    "by_correlation" => [:id, :time, :direction]
  }

  @spec collect_lineage_page(keyword()) :: map()
  def collect_lineage_page(opts \\ []) when is_list(opts) do
    filters = flow_lineage_filters_from_opts(opts)
    acl_username = DashboardAccess.keyspace_acl_username(opts)

    sampled_records =
      collect_flow_records_sample_for_acl(@flow_dashboard_sample_limit, acl_username)

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
    |> maybe_put_query_opt(:limit, normalize_flow_query_limit(Map.get(params, "limit")))
    |> Enum.reverse()
  end

  def lineage_opts_from_query(_query), do: []

  @spec collect_query_page(keyword()) :: map()
  def collect_query_page(opts \\ []) when is_list(opts) do
    filters = flow_query_filters_from_opts(opts)
    acl_username = DashboardAccess.keyspace_acl_username(opts)
    pending_discovery = QueryDiscovery.start(filters, opts)

    {query_result, query_acl_scope} =
      if filters.inspect do
        {flow_query_inspection_result(filters), flow_query_acl_scope(filters)}
      else
        execute_flow_query_for_acl(filters, acl_username)
      end

    result =
      query_result
      |> maybe_filter_inspection_result_for_acl(
        filters.inspect,
        acl_username,
        query_acl_scope
      )
      |> QueryVisualization.attach()

    discovery = QueryDiscovery.finish(pending_discovery, filters, result)
    result = maybe_put_inspection_discovery_message(result, filters.inspect, discovery)

    %{
      filters: filters,
      result: result,
      discovery: discovery,
      workbench: QueryWorkbench.default_form(filters),
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
      :run_state,
      normalize_flow_state_filter(Map.get(params, "run_state"))
    )
    |> maybe_put_query_opt(
      :attribute_key,
      normalize_flow_name_filter(Map.get(params, "attribute_key"))
    )
    |> maybe_put_query_opt(
      :attribute_value_type,
      normalize_flow_name_filter(Map.get(params, "attribute_value_type"))
    )
    |> maybe_put_raw_query_opt(:attribute_value, params, "attribute_value")
    |> maybe_put_query_opt(
      :state_meta_state,
      normalize_flow_state_filter(Map.get(params, "state_meta_state"))
    )
    |> maybe_put_query_opt(
      :state_meta_key,
      normalize_flow_name_filter(Map.get(params, "state_meta_key"))
    )
    |> maybe_put_query_opt(
      :state_meta_value_type,
      normalize_flow_name_filter(Map.get(params, "state_meta_value_type"))
    )
    |> maybe_put_raw_query_opt(:state_meta_value, params, "state_meta_value")
    |> maybe_put_query_opt(:id, normalize_flow_name_filter(Map.get(params, "id")))
    |> maybe_put_query_opt(
      :partition_key,
      normalize_flow_partition_query(Map.get(params, "partition_key"))
    )
    |> maybe_put_query_opt(:limit, normalize_flow_query_limit(Map.get(params, "limit")))
    |> maybe_put_query_opt(:from_ms, parse_flow_time_filter(Map.get(params, "from")))
    |> maybe_put_query_opt(:to_ms, parse_flow_time_filter(Map.get(params, "to")))
    |> maybe_put_query_opt(:rev, normalize_flow_boolean_filter(Map.get(params, "rev")))
    |> maybe_put_query_opt(
      :inspect,
      normalize_flow_boolean_filter(Map.get(params, "inspect"))
    )
    |> Enum.reverse()
  end

  def query_opts_from_query(_query), do: []

  defp maybe_put_raw_query_opt(opts, key, params, param_key) do
    case Map.fetch(params, param_key) do
      {:ok, value} when is_binary(value) -> [{key, value} | opts]
      _missing_or_invalid -> opts
    end
  end

  @spec collect_workbench_page(PreparedCommand.t(), QueryWorkbench.form(), keyword()) :: map()
  def collect_workbench_page(%PreparedCommand{} = prepared, form, opts \\ [])
      when is_map(form) and is_list(opts) do
    opts = workbench_page_opts(form, opts) |> workbench_discovery_opts(prepared)
    filters = flow_query_filters_from_opts(opts)
    pending_discovery = QueryDiscovery.start(filters, opts)
    acl_username = DashboardAccess.keyspace_acl_username(opts)
    acl_scope = prepared_acl_scope(prepared)

    result =
      prepared
      |> QueryWorkbench.execute()
      |> DashboardAccess.flow_query_filter_result_for_acl(acl_username, acl_scope)
      |> QueryVisualization.attach()
      |> QueryWorkbench.attach_continuation(form)

    workbench_page_data(form, result, filters, pending_discovery)
  end

  @spec collect_workbench_error_page(QueryWorkbench.form(), binary(), keyword()) :: map()
  def collect_workbench_error_page(form, message, opts \\ [])
      when is_map(form) and is_binary(message) and is_list(opts) do
    result = %{
      status: :error,
      command: workbench_command(form),
      rows: [],
      message: message
    }

    workbench_page_data(form, result, opts)
  end

  defp workbench_page_data(form, result, opts) do
    filters = form |> workbench_page_opts(opts) |> flow_query_filters_from_opts()
    pending_discovery = QueryDiscovery.start(filters, opts)
    workbench_page_data(form, result, filters, pending_discovery)
  end

  defp workbench_page_data(form, result, filters, pending_discovery) do
    %{
      filters: filters,
      result: result,
      discovery: QueryDiscovery.finish(pending_discovery, filters, result),
      workbench: form,
      generated_at_ms: System.system_time(:millisecond)
    }
  end

  defp prepared_acl_scope(%PreparedCommand{acl_keys: [scope | _rest]}) when is_binary(scope),
    do: scope

  defp prepared_acl_scope(%PreparedCommand{}), do: "*"

  defp workbench_command(%{action: :explain}), do: "FLOW.QUERY EXPLAIN"
  defp workbench_command(%{action: :analyze}), do: "FLOW.QUERY EXPLAIN ANALYZE"
  defp workbench_command(_form), do: "FLOW.QUERY"

  defp workbench_discovery_opts(
         opts,
         %PreparedCommand{ast: {:flow_query, %Request{predicate: {:and, predicates}}}}
       )
       when is_list(predicates) do
    opts
    |> maybe_put_workbench_discovery_opt(:type, exact_query_literal(predicates, :type))
    |> maybe_put_workbench_discovery_opt(
      :partition_key,
      exact_query_literal(predicates, :partition_key)
    )
  end

  defp workbench_discovery_opts(opts, %PreparedCommand{}), do: opts

  defp workbench_page_opts(%{mode: :guided, guided_query: query}, opts)
       when is_binary(query) and query != "" and is_list(opts) do
    query_opts_from_query(query) ++ opts
  end

  defp workbench_page_opts(_form, opts), do: opts

  defp exact_query_literal(predicates, field) do
    case Enum.flat_map(predicates, fn
           {:eq, ^field, {:literal, _type, value}} when is_binary(value) and value != "" ->
             [value]

           _other ->
             []
         end) do
      [value] -> value
      _missing_or_ambiguous -> nil
    end
  end

  defp maybe_put_workbench_discovery_opt(opts, _key, nil), do: opts
  defp maybe_put_workbench_discovery_opt(opts, key, value), do: Keyword.put(opts, key, value)

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

    acl_username = DashboardAccess.keyspace_acl_username(opts)

    records = collect_flow_records_sample_for_acl(@flow_dashboard_sample_limit, acl_username)

    type_records = filter_flow_records_by_type(records, filters.type)
    filtered_records = filter_flow_records_by_name(type_records, filters.q)

    {signals, signal_scan} = collect_flow_signal_rows(filtered_records, filters)

    %{
      signals: signals,
      filters: filters,
      available_types: flow_available_types(records),
      total_sampled: length(records),
      filtered_sampled: length(filtered_records),
      sample_limit: @flow_dashboard_sample_limit,
      signal_scan: signal_scan,
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
      limit: normalize_flow_query_limit(Keyword.get(opts, :limit))
    }
  end

  defp normalize_flow_lineage_mode("parent"), do: "parent"
  defp normalize_flow_lineage_mode("root"), do: "root"
  defp normalize_flow_lineage_mode("correlation"), do: "correlation"
  defp normalize_flow_lineage_mode(_mode), do: "root"

  defp flow_lineage_query_result(%{target: nil}) do
    %{status: :idle, records: [], command: "FLOW.QUERY", message: "Enter a lineage id"}
  end

  defp flow_lineage_query_result(%{partition_key: nil}) do
    %{status: :idle, records: [], command: "FLOW.QUERY", message: "Enter a partition key"}
  end

  defp flow_lineage_query_result(%{target: target, mode: mode} = filters) do
    kind =
      case mode do
        "parent" -> :by_parent
        "correlation" -> :by_correlation
        _ -> :by_root
      end

    with {:ok, built} <-
           Builder.build(kind, %{
             partition_key: filters.partition_key,
             id: target,
             limit: filters.limit
           }) do
      query_fun = fn -> flow_dashboard_flow_query(built.query, built.params) end

      case bounded_dashboard_call(query_fun, flow_dashboard_list_fetch_timeout_ms(), :lineage) do
        {:ok, {:ok, %{records: records}}} when is_list(records) ->
          lineage_success(records)

        {:ok, {:ok, records}} when is_list(records) ->
          lineage_success(records)

        {:ok, {:error, reason}} ->
          %{status: :error, command: "FLOW.QUERY", records: [], message: inspect(reason)}

        {:error, :timeout} ->
          %{status: :timeout, command: "FLOW.QUERY", records: [], message: "query timed out"}

        {:error, reason} ->
          %{status: :error, command: "FLOW.QUERY", records: [], message: inspect(reason)}

        _ ->
          %{
            status: :error,
            command: "FLOW.QUERY",
            records: [],
            message: "unexpected query result"
          }
      end
    else
      {:error, reason} ->
        %{status: :error, command: "FLOW.QUERY", records: [], message: inspect(reason)}
    end
  end

  defp lineage_success(records) do
    %{status: :ok, command: "FLOW.QUERY", records: records, message: "#{length(records)} records"}
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
    attribute_key = normalize_flow_name_filter(Keyword.get(opts, :attribute_key))
    attribute_value_type = metadata_value_type(opts, :attribute_value_type)

    {attribute_value_set?, attribute_value, attribute_value_input, attribute_value_error} =
      metadata_value_from_opts(
        opts,
        :attribute_value,
        attribute_value_type,
        "Attribute value",
        attribute_key
      )

    state_meta_key = normalize_flow_name_filter(Keyword.get(opts, :state_meta_key))
    state_meta_value_type = metadata_value_type(opts, :state_meta_value_type)

    {state_meta_value_set?, state_meta_value, state_meta_value_input, state_meta_value_error} =
      metadata_value_from_opts(
        opts,
        :state_meta_value,
        state_meta_value_type,
        "State metadata value",
        state_meta_key
      )

    %{
      kind: normalize_flow_query_kind(Keyword.get(opts, :kind)),
      type: normalize_flow_type_filter(Keyword.get(opts, :type)),
      state: normalize_flow_state_filter(Keyword.get(opts, :state)),
      run_state: normalize_flow_state_filter(Keyword.get(opts, :run_state)),
      attribute_key: attribute_key,
      attribute_value_type: attribute_value_type,
      attribute_value: attribute_value,
      attribute_value_input: attribute_value_input,
      attribute_value_set?: attribute_value_set?,
      attribute_value_error: attribute_value_error,
      state_meta_state: normalize_flow_state_filter(Keyword.get(opts, :state_meta_state)),
      state_meta_key: state_meta_key,
      state_meta_value_type: state_meta_value_type,
      state_meta_value: state_meta_value,
      state_meta_value_input: state_meta_value_input,
      state_meta_value_set?: state_meta_value_set?,
      state_meta_value_error: state_meta_value_error,
      id: normalize_flow_name_filter(Keyword.get(opts, :id)),
      partition_key: normalize_flow_partition_query(Keyword.get(opts, :partition_key)),
      limit: normalize_flow_query_limit(Keyword.get(opts, :limit)),
      from_ms: Keyword.get(opts, :from_ms),
      to_ms: Keyword.get(opts, :to_ms),
      rev: Keyword.get(opts, :rev) == true,
      inspect: Keyword.get(opts, :inspect) == true
    }
  end

  defp metadata_value_type(opts, key) do
    case Keyword.get(opts, key, "string") do
      type when type in ["string", "integer", "float", "boolean", "null"] -> type
      _invalid -> "invalid"
    end
  end

  defp metadata_value_from_opts(_opts, _key, "null", _label, metadata_key) do
    {is_binary(metadata_key), nil, nil, nil}
  end

  defp metadata_value_from_opts(opts, key, type, label, metadata_key) do
    case Keyword.fetch(opts, key) do
      {:ok, ""} when is_nil(metadata_key) ->
        {false, nil, nil, nil}

      {:ok, input} when is_binary(input) ->
        case parse_metadata_value(input, type) do
          {:ok, value} -> {true, value, input, nil}
          {:error, detail} -> {true, nil, input, "#{label} #{detail}"}
        end

      {:ok, _invalid} ->
        {true, nil, nil, "#{label} must be supplied as text"}

      :error ->
        {false, nil, nil, nil}
    end
  end

  defp parse_metadata_value(value, "string") do
    if Attributes.valid_scalar?(value),
      do: {:ok, value},
      else: {:error, "is too large"}
  end

  defp parse_metadata_value(value, "integer") do
    case Integer.parse(String.trim(value)) do
      {parsed, ""} ->
        if Attributes.valid_scalar?(parsed),
          do: {:ok, parsed},
          else: {:error, "must be a signed 64-bit integer"}

      _invalid ->
        {:error, "must be an integer"}
    end
  end

  defp parse_metadata_value(value, "float") do
    case Float.parse(String.trim(value)) do
      {parsed, ""} ->
        if Attributes.valid_scalar?(parsed),
          do: {:ok, parsed},
          else: {:error, "must be a finite number"}

      _invalid ->
        {:error, "must be a finite number"}
    end
  end

  defp parse_metadata_value(value, "boolean") do
    case String.downcase(String.trim(value)) do
      "true" -> {:ok, true}
      "false" -> {:ok, false}
      _invalid -> {:error, "must be true or false"}
    end
  end

  defp parse_metadata_value(_value, _invalid),
    do: {:error, "type must be string, integer, float, boolean, or null"}

  defp flow_query_inspection_result(%{type: type}) when is_binary(type) and type != "" do
    %{
      status: :idle,
      command: "FLOW.QUERY OPTIONS",
      rows: [],
      message: "Options loaded for #{type}"
    }
  end

  defp flow_query_inspection_result(_filters) do
    %{
      status: :idle,
      command: "FLOW.QUERY OPTIONS",
      rows: [],
      message: "Enter a workflow type"
    }
  end

  defp maybe_filter_inspection_result_for_acl(result, true, _username, _scope), do: result

  defp maybe_filter_inspection_result_for_acl(result, false, username, scope) do
    DashboardAccess.flow_query_filter_result_for_acl(result, username, scope)
  end

  defp maybe_put_inspection_discovery_message(
         result,
         true,
         %{status: :forbidden, required_command: command}
       )
       when is_binary(command) do
    Map.put(result, :message, "Type options require +#{command}")
  end

  defp maybe_put_inspection_discovery_message(
         result,
         true,
         %{status: :forbidden, denied_scope: :type}
       ) do
    Map.put(result, :message, "Type options are not available for this workflow type")
  end

  defp maybe_put_inspection_discovery_message(result, true, %{status: :forbidden}) do
    Map.put(result, :message, "Type options are not available for this partition")
  end

  defp maybe_put_inspection_discovery_message(result, true, %{status: :unavailable}) do
    Map.put(result, :message, "Type options are temporarily unavailable")
  end

  defp maybe_put_inspection_discovery_message(
         result,
         true,
         %{status: :ready, type: nil, available_types: types} = discovery
       )
       when is_list(types) do
    partition_key = Map.get(discovery, :scope)

    message =
      case partition_key do
        value when is_binary(value) -> "Type options loaded for #{value}"
        _missing -> "Type options loaded"
      end

    Map.put(result, :message, message)
  end

  defp maybe_put_inspection_discovery_message(result, _inspect?, _discovery), do: result

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
    case validate_flow_query_predicates(filters) do
      :ok ->
        execute_flow_query_plan(flow_query_plan(filters))

      {:error, message} ->
        %{
          status: :idle,
          command: flow_query_kind_command(filters.kind),
          rows: [],
          message: message
        }
    end
  end

  defp execute_flow_query_plan(plan) do
    case plan do
      {:ok, command, fun} ->
        execute_flow_query_call(command, fun, nil)

      {:ok, command, fun, continuation_form} ->
        execute_flow_query_call(command, fun, continuation_form)

      {:idle, command, message} ->
        %{status: :idle, command: command, rows: [], message: message}
    end
  end

  defp execute_flow_query_call(command, fun, continuation_form) do
    case bounded_dashboard_call(fun, flow_dashboard_list_fetch_timeout_ms(), :query) do
      {:ok, {:ok, response}} ->
        command
        |> QueryResult.success(response)
        |> maybe_attach_guided_continuation(continuation_form)

      {:ok, {:error, reason}} ->
        %{status: :error, command: command, rows: [], message: inspect(reason)}

      {:error, :timeout} ->
        %{status: :timeout, command: command, rows: [], message: "query timed out"}

      {:error, reason} ->
        %{status: :error, command: command, rows: [], message: inspect(reason)}

      _other ->
        %{status: :error, command: command, rows: [], message: "unexpected query result"}
    end
  end

  defp maybe_attach_guided_continuation(result, nil), do: result

  defp maybe_attach_guided_continuation(result, form),
    do: QueryWorkbench.attach_continuation(result, form)

  defp validate_flow_query_predicates(filters) do
    with :ok <-
           validate_optional_pair(
             filters.attribute_key,
             filters.attribute_value_set?,
             filters.attribute_value_error
           ),
         :ok <-
           validate_optional_triple(
             filters.state_meta_state,
             filters.state_meta_key,
             filters.state_meta_value_set?,
             filters.state_meta_value_error
           ),
         :ok <- validate_query_field_names(filters),
         :ok <- validate_flow_time_range(filters),
         :ok <- validate_supported_flow_query_predicates(filters) do
      :ok
    end
  end

  defp validate_optional_pair(_key, _value_set?, error) when is_binary(error),
    do: {:error, error}

  defp validate_optional_pair(nil, false, nil), do: :ok
  defp validate_optional_pair(key, true, nil) when is_binary(key), do: :ok

  defp validate_optional_pair(_key, _value_set?, _error),
    do: {:error, "Enter both attribute key and value"}

  defp validate_optional_triple(_state, _key, _value_set?, error) when is_binary(error),
    do: {:error, error}

  defp validate_optional_triple(nil, nil, false, nil), do: :ok

  defp validate_optional_triple(state, key, true, nil)
       when is_binary(state) and is_binary(key),
       do: :ok

  defp validate_optional_triple(_state, _key, _value_set?, _error),
    do: {:error, "Enter state metadata state, key, and value"}

  defp validate_query_field_names(filters) do
    with :ok <- validate_attribute_field_name(Map.get(filters, :attribute_key)),
         :ok <-
           validate_state_meta_field_name(
             Map.get(filters, :state_meta_state),
             Map.get(filters, :state_meta_key)
           ) do
      :ok
    end
  end

  defp validate_attribute_field_name(nil), do: :ok

  defp validate_attribute_field_name(key) when is_binary(key) do
    if Field.valid?({:attribute, key}),
      do: :ok,
      else:
        {:error, "Attribute key is not queryable. Choose an indexed attribute from Show options"}
  end

  defp validate_attribute_field_name(_invalid),
    do: {:error, "Attribute key is not queryable. Choose an indexed attribute from Show options"}

  defp validate_state_meta_field_name(nil, nil), do: :ok

  defp validate_state_meta_field_name(state, key) when is_binary(state) and is_binary(key) do
    if Field.valid?({:state_meta, state, key}),
      do: :ok,
      else:
        {:error,
         "State metadata state or key is not queryable. Choose the policy-indexed field from Show options"}
  end

  defp validate_state_meta_field_name(_state, _key), do: :ok

  defp validate_flow_time_range(%{from_ms: from_ms, to_ms: to_ms})
       when is_integer(from_ms) and is_integer(to_ms) and from_ms > to_ms,
       do: {:error, "From UTC must not be later than To UTC"}

  defp validate_flow_time_range(_filters), do: :ok

  defp validate_supported_flow_query_predicates(%{kind: kind} = filters) do
    allowed = Map.fetch!(@flow_query_allowed_predicates, kind)

    case Enum.find(@flow_query_predicates, fn predicate ->
           query_predicate_present?(filters, predicate) and predicate not in allowed
         end) do
      nil ->
        :ok

      predicate ->
        {:error,
         "#{Map.fetch!(@flow_query_predicate_labels, predicate)} is not supported by #{kind}"}
    end
  end

  defp query_predicate_present?(filters, :type), do: is_binary(filters.type)
  defp query_predicate_present?(filters, :state), do: is_binary(filters.state)
  defp query_predicate_present?(filters, :run_state), do: is_binary(filters.run_state)

  defp query_predicate_present?(filters, :attribute),
    do: is_binary(filters.attribute_key) or filters.attribute_value_set?

  defp query_predicate_present?(filters, :state_meta),
    do:
      is_binary(filters.state_meta_state) or is_binary(filters.state_meta_key) or
        filters.state_meta_value_set?

  defp query_predicate_present?(filters, :id), do: is_binary(filters.id)

  defp query_predicate_present?(filters, :time),
    do: not is_nil(filters.from_ms) or not is_nil(filters.to_ms)

  defp query_predicate_present?(filters, :direction), do: filters.rev == true

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

  defp flow_query_plan(%{kind: kind, partition_key: partition_key})
       when kind in [
              "list",
              "search",
              "terminals",
              "failures",
              "stuck",
              "by_parent",
              "by_root",
              "by_correlation"
            ] and (not is_binary(partition_key) or partition_key == "") do
    {:idle, "FLOW.QUERY", "Enter a partition key"}
  end

  defp flow_query_plan(%{kind: "stats", state: nil}),
    do: {:idle, "FLOW.STATS", "Enter a workflow state"}

  defp flow_query_plan(%{kind: "history", id: id} = filters) do
    opts =
      [count: filters.limit, values: false, consistent_projection: true]
      |> maybe_put_query_opt(:partition_key, filters.partition_key)
      |> Enum.reverse()

    {:ok, "FLOW.HISTORY", fn -> flow_dashboard_flow_history(id, opts) end}
  end

  defp flow_query_plan(%{kind: "by_parent", id: id} = filters) do
    query_builder_plan(:by_parent, Map.put(query_builder_filters(filters), :id, id), filters)
  end

  defp flow_query_plan(%{kind: "by_root", id: id} = filters) do
    query_builder_plan(:by_root, Map.put(query_builder_filters(filters), :id, id), filters)
  end

  defp flow_query_plan(%{kind: "by_correlation", id: id} = filters) do
    query_builder_plan(:by_correlation, Map.put(query_builder_filters(filters), :id, id), filters)
  end

  defp flow_query_plan(%{kind: "search"} = filters) do
    attribute = flow_query_attribute_builder_filter(filters)
    state_meta = flow_query_state_meta_builder_filter(filters)

    if is_nil(attribute) and is_nil(state_meta) do
      {:idle, "FLOW.QUERY", "Enter an indexed attribute or state metadata filter"}
    else
      builder_filters =
        filters
        |> query_builder_filters()
        |> maybe_put_builder_filter(:attribute, attribute)
        |> maybe_put_builder_filter(:state_meta, state_meta)

      query_builder_plan(:search, builder_filters, filters)
    end
  end

  defp flow_query_plan(%{kind: "terminals"} = filters),
    do: query_builder_plan(:terminals, query_builder_filters(filters), filters)

  defp flow_query_plan(%{kind: "failures"} = filters),
    do: query_builder_plan(:failures, query_builder_filters(filters), filters)

  defp flow_query_plan(%{kind: "stuck"} = filters),
    do:
      query_builder_plan(
        :stuck,
        Map.put(query_builder_filters(filters), :now_ms, System.system_time(:millisecond)),
        filters
      )

  defp flow_query_plan(%{kind: "stats", type: type} = filters) do
    opts =
      [consistent_projection: true]
      |> maybe_put_query_opt(:partition_key, filters.partition_key)
      |> maybe_put_query_opt(:attributes, flow_query_attribute_filter(filters))
      |> maybe_put_query_opt(:state, filters.state)
      |> Enum.reverse()

    {:ok, "FLOW.STATS", fn -> flow_dashboard_flow_stats(type, opts) end}
  end

  defp flow_query_plan(%{kind: "list"} = filters) do
    builder_filters =
      filters
      |> query_builder_filters()
      |> maybe_put_builder_filter(:attribute, flow_query_attribute_builder_filter(filters))

    query_builder_plan(:list, builder_filters, filters)
  end

  defp flow_query_plan(filters),
    do: query_builder_plan(:list, query_builder_filters(filters), filters)

  defp query_builder_plan(kind, filters, guided_filters) do
    case Builder.build(kind, filters) do
      {:ok, built} ->
        continuation_form =
          QueryWorkbench.guided_form(
            built.query,
            built.params,
            guided_query_from_filters(guided_filters)
          )

        {:ok, "FLOW.QUERY", fn -> flow_dashboard_flow_query(built.query, built.params) end,
         continuation_form}

      {:error, reason} ->
        {:idle, "FLOW.QUERY", query_builder_message(reason)}
    end
  end

  defp guided_query_from_filters(filters) when is_map(filters) do
    %{
      "kind" => Map.get(filters, :kind),
      "type" => Map.get(filters, :type),
      "state" => Map.get(filters, :state),
      "run_state" => Map.get(filters, :run_state),
      "attribute_key" => Map.get(filters, :attribute_key),
      "attribute_value_type" =>
        if(
          Map.get(filters, :attribute_value_set?) and
            Map.get(filters, :attribute_value_type) != "string",
          do: Map.get(filters, :attribute_value_type),
          else: nil
        ),
      "attribute_value" =>
        if(
          Map.get(filters, :attribute_value_set?) and
            Map.get(filters, :attribute_value_type) != "null",
          do: Map.get(filters, :attribute_value_input),
          else: nil
        ),
      "state_meta_state" => Map.get(filters, :state_meta_state),
      "state_meta_key" => Map.get(filters, :state_meta_key),
      "state_meta_value_type" =>
        if(
          Map.get(filters, :state_meta_value_set?) and
            Map.get(filters, :state_meta_value_type) != "string",
          do: Map.get(filters, :state_meta_value_type),
          else: nil
        ),
      "state_meta_value" =>
        if(
          Map.get(filters, :state_meta_value_set?) and
            Map.get(filters, :state_meta_value_type) != "null",
          do: Map.get(filters, :state_meta_value_input),
          else: nil
        ),
      "id" => Map.get(filters, :id),
      "partition_key" => Map.get(filters, :partition_key),
      "limit" => Map.get(filters, :limit),
      "from" => Map.get(filters, :from_ms),
      "to" => Map.get(filters, :to_ms),
      "rev" => if(Map.get(filters, :rev), do: "true", else: nil)
    }
    |> Enum.reject(fn {_key, value} -> is_nil(value) end)
    |> Map.new(fn {key, value} -> {key, to_string(value)} end)
    |> URI.encode_query()
  end

  defp query_builder_filters(filters) do
    %{
      partition_key: filters.partition_key,
      type: filters.type,
      state: query_builder_state(filters.state),
      run_state: query_builder_state(filters.run_state),
      limit: filters.limit,
      direction: if(filters.rev, do: :desc, else: :asc),
      from_ms: filters.from_ms,
      to_ms: filters.to_ms
    }
  end

  defp query_builder_state(nil), do: "any"
  defp query_builder_state(state), do: state

  defp maybe_put_builder_filter(filters, _key, nil), do: filters
  defp maybe_put_builder_filter(filters, key, value), do: Map.put(filters, key, value)

  defp query_builder_message(:query_partition_required), do: "Enter a partition key"
  defp query_builder_message(:query_filter_required), do: "Enter a query filter"
  defp query_builder_message(reason), do: inspect(reason)

  defp flow_query_attribute_filter(%{
         attribute_key: key,
         attribute_value: value,
         attribute_value_set?: true,
         attribute_value_error: nil
       })
       when is_binary(key) and key != "",
       do: %{key => value}

  defp flow_query_attribute_filter(_filters), do: nil

  defp flow_query_attribute_builder_filter(%{
         attribute_key: key,
         attribute_value_type: "null",
         attribute_value_set?: true,
         attribute_value_error: nil
       })
       when is_binary(key) and key != "",
       do: {:is, key, :null}

  defp flow_query_attribute_builder_filter(%{
         attribute_key: key,
         attribute_value: value,
         attribute_value_set?: true,
         attribute_value_error: nil
       })
       when is_binary(key) and key != "",
       do: {key, value}

  defp flow_query_attribute_builder_filter(_filters), do: nil

  defp flow_query_state_meta_builder_filter(%{
         state_meta_state: state,
         state_meta_key: key,
         state_meta_value_type: "null",
         state_meta_value_set?: true,
         state_meta_value_error: nil
       })
       when is_binary(state) and state != "" and is_binary(key) and key != "",
       do: {:is, state, key, :null}

  defp flow_query_state_meta_builder_filter(%{
         state_meta_state: state,
         state_meta_key: key,
         state_meta_value: value,
         state_meta_value_set?: true,
         state_meta_value_error: nil
       })
       when is_binary(state) and state != "" and is_binary(key) and key != "",
       do: {state, key, value}

  defp flow_query_state_meta_builder_filter(_filters), do: nil

  defp flow_query_kind_command("terminals"), do: "FLOW.QUERY"
  defp flow_query_kind_command("search"), do: "FLOW.QUERY"
  defp flow_query_kind_command("stats"), do: "FLOW.STATS"
  defp flow_query_kind_command("failures"), do: "FLOW.QUERY"
  defp flow_query_kind_command("stuck"), do: "FLOW.QUERY"
  defp flow_query_kind_command("history"), do: "FLOW.HISTORY"
  defp flow_query_kind_command("by_parent"), do: "FLOW.QUERY"
  defp flow_query_kind_command("by_root"), do: "FLOW.QUERY"
  defp flow_query_kind_command("by_correlation"), do: "FLOW.QUERY"
  defp flow_query_kind_command(_kind), do: "FLOW.QUERY"

  defp normalize_flow_query_limit(value) do
    value
    |> normalize_flow_limit_filter()
    |> min(Limits.max_results())
  end

  defp flow_signals_filters_from_opts(opts) when is_list(opts) do
    %{
      type: normalize_flow_type_filter(Keyword.get(opts, :type)),
      signal: normalize_flow_name_filter(Keyword.get(opts, :signal)),
      q: normalize_flow_name_filter(Keyword.get(opts, :q)),
      limit: normalize_flow_limit_filter(Keyword.get(opts, :limit)),
      scan_history: normalize_flow_boolean_filter(Keyword.get(opts, :scan_history))
    }
  end

  defp collect_flow_signal_rows(filtered_records, %{scan_history: false}) do
    {[],
     %{
       requested: false,
       sampled_flows: length(filtered_records),
       inspected_flows: 0,
       completed_flows: 0,
       failed_flows: 0,
       truncated: false,
       auto_refresh: true
     }}
  end

  defp collect_flow_signal_rows(filtered_records, filters) do
    max_flows = flow_signal_scan_max_flows()
    records = filtered_records |> flow_recent_records(max_flows)
    timeout_ms = flow_signal_scan_fetch_timeout_ms()

    {rows, completed, failed} =
      records
      |> Task.async_stream(
        &flow_signal_rows_for_record(&1, timeout_ms),
        max_concurrency: min(flow_signal_scan_max_concurrency(), max(length(records), 1)),
        ordered: false,
        on_timeout: :kill_task,
        timeout: timeout_ms + 250
      )
      |> Enum.reduce({[], 0, 0}, fn
        {:ok, {:ok, record_rows}}, {rows, completed, failed} ->
          {[record_rows | rows], completed + 1, failed}

        _failure, {rows, completed, failed} ->
          {rows, completed, failed + 1}
      end)

    signals =
      rows
      |> List.flatten()
      |> filter_flow_signal_rows(filters)
      |> Enum.sort_by(&flow_signal_sort_key/1, :desc)
      |> Enum.take(filters.limit)

    {signals,
     %{
       requested: true,
       sampled_flows: length(filtered_records),
       inspected_flows: length(records),
       completed_flows: completed,
       failed_flows: failed,
       truncated: length(filtered_records) > length(records),
       auto_refresh: false
     }}
  end

  defp flow_signal_rows_for_record(record, timeout_ms) when is_map(record) do
    id = flow_record_id(record)
    partition_key = flow_record_partition_key(record)

    opts =
      [
        count: @flow_dashboard_signal_history_count,
        values: false,
        consistent_projection: true
      ]
      |> maybe_put_query_opt(:partition_key, flow_detail_url_partition_key(partition_key))

    case bounded_dashboard_call(
           fn -> flow_dashboard_flow_history(id, opts) end,
           timeout_ms,
           :signals_history
         ) do
      {:ok, {:ok, history}} when is_list(history) -> {:ok, flow_signal_rows(record, history)}
      _ -> {:error, :history_unavailable}
    end
  rescue
    _ -> {:error, :history_unavailable}
  catch
    :exit, _ -> {:error, :history_unavailable}
  end

  defp flow_signal_scan_max_flows do
    dashboard_positive_limit(
      :flow_dashboard_signal_scan_max_flows,
      @flow_dashboard_signal_scan_max_flows,
      @flow_dashboard_sample_limit
    )
  end

  defp flow_signal_scan_max_concurrency do
    dashboard_positive_limit(
      :flow_dashboard_signal_scan_max_concurrency,
      @flow_dashboard_signal_scan_max_concurrency,
      16
    )
  end

  defp flow_signal_scan_fetch_timeout_ms do
    dashboard_positive_limit(
      :flow_dashboard_signal_scan_fetch_timeout_ms,
      @flow_dashboard_signal_scan_fetch_timeout_ms,
      @flow_dashboard_signal_scan_max_fetch_timeout_ms
    )
  end

  defp dashboard_positive_limit(key, default, maximum) do
    case Application.get_env(:ferricstore, key, default) do
      value when is_integer(value) and value > 0 -> min(value, maximum)
      _other -> default
    end
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
