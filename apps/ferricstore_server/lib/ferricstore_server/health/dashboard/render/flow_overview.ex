defmodule FerricstoreServer.Health.Dashboard.Render.FlowOverview do
  import FerricstoreServer.Health.Dashboard.Format
  import FerricstoreServer.Health.Dashboard.QueryParams

  def render_flow_overview(summary, total_sampled, sample_limit) do
    types_count = Map.get(summary, :types, 0)
    active_count = Map.get(summary, :active, 0)
    queued_count = Map.get(summary, :queued, 0)
    running_count = Map.get(summary, :running, 0)
    failed_count = Map.get(summary, :failed, 0)
    due_count = Map.get(summary, :due_now_sampled, 0)

    """
    <div class="section-title">Flow Overview <span class="badge badge-idle">#{bounded_sample_label(total_sampled, total_sampled, sample_limit)}</span></div>
    <dl class="flow-overview-ribbon" aria-label="Workflow overview metrics">
      #{render_flow_overview_metric("Types", types_count, "discovered workflow types")}
      #{render_flow_overview_metric("Active", active_count, "all nonterminal states")}
      #{render_flow_overview_metric("Queued", queued_count, "ready or scheduled")}
      #{render_flow_overview_metric("Running", running_count, "leased by workers")}
      #{render_flow_overview_metric("Failed", failed_count, "terminal failures")}
      #{render_flow_overview_metric("Due now", due_count, "scheduled time reached")}
    </dl>
    """
  end

  defp render_flow_overview_metric(label, value, detail) do
    """
    <div>
      <dt>#{escape(label)}</dt>
      <dd>#{format_number(value)}</dd>
      <span>#{escape(detail)}</span>
    </div>
    """
  end

  def render_flow_stat_card(label, value, detail) do
    rendered_value =
      case value do
        value when is_binary(value) -> escape(value)
        value when is_integer(value) -> format_number(value)
        value -> escape(to_string(value))
      end

    """
    <div class="flow-card">
      <div class="flow-card-label">#{escape(label)}</div>
      <div class="flow-card-value">#{rendered_value}</div>
      <div class="flow-card-detail">#{escape(detail)}</div>
    </div>
    """
  end

  def render_flow_context_tools, do: render_flow_context_tools(%{}, nil)

  def flow_failure_investigation_path(data) do
    context = flow_investigation_context(data)
    flow_context_path("/dashboard/flow/failures", flow_failures_context_params(context))
  end

  def render_flow_context_tools(data, active) when is_map(data) do
    render_flow_context_tools(data, active, [])
  end

  def render_flow_context_tools(data, active, opts) when is_map(data) and is_list(opts) do
    context = flow_investigation_context(data)
    partition_key = Map.get(context, :partition_key) || ""

    """
    <div class="flow-nav-row flow-context-tools">
      <form class="flow-search" action="/dashboard/flow/lookup" method="get" aria-label="Flow lookup">
        <input class="flow-search-input mono" type="search" name="id" placeholder="Search flow ID" autocomplete="off" aria-label="Flow ID" title="Open a flow by ID.">
        <input class="flow-search-input mono" type="search" name="partition_key" value="#{escape_attr(partition_key)}" placeholder="Partition key" autocomplete="off" aria-label="Partition key" title="With a Flow ID, scopes the detail lookup. Without a Flow ID, filters the overview to this partition.">
        <button class="flow-search-button" type="submit" title="Open a flow by ID or filter overview by partition">Search</button>
      </form>
      #{render_flow_investigation_context(context, active, opts)}
    </div>
    """
  end

  defp render_flow_investigation_context(context, active, opts) do
    links = [
      {"flow_states", "/dashboard/flow/states", "States", flow_states_context_params(context)},
      {"flow_failures", "/dashboard/flow/failures", "Failures",
       flow_failures_context_params(context)},
      {"flow_query", "/dashboard/flow/query", "Query Studio", flow_query_context_params(context)},
      {"flow_signals", "/dashboard/flow/signals", "Signals",
       flow_signals_context_params(context)},
      {"flow_lineage", "/dashboard/flow/lineage", "Lineage", flow_lineage_context_params(context)}
    ]

    rendered_links =
      Enum.map_join(links, "", fn {key, path, label, params} ->
        current = if key == active, do: ~s( aria-current="page"), else: ""

        href = flow_context_path(path, params)

        ~s(<a class="flow-context-link" href="#{escape_attr(href)}"#{current}>#{escape(label)}</a>)
      end)

    scope =
      if Keyword.get(opts, :show_scope, true) do
        """
        <span class="flow-investigation-context-label">Scope</span>
        <span class="flow-investigation-scope">#{render_flow_context_chips(context)}</span>
        """
      else
        ~s(<span class="flow-investigation-context-label">Investigate</span>)
      end

    """
    <nav class="flow-investigation-context" aria-label="Workflow investigation context">
      #{scope}
      <span class="flow-context-links">#{rendered_links}</span>
    </nav>
    """
  end

  defp render_flow_context_chips(context) do
    [
      {:type, "type"},
      {:partition_key, "partition"},
      {:state, "state"},
      {:range, "updated"}
    ]
    |> Enum.flat_map(fn {key, label} ->
      case Map.get(context, key) do
        value when is_binary(value) and value != "" ->
          [
            ~s(<span class="flow-context-chip mono" title="#{escape_attr(label)} #{escape_attr(value)}">#{escape(label)} #{escape(value)}</span>)
          ]

        _missing ->
          []
      end
    end)
    |> case do
      [] -> ~s(<span class="flow-context-chip">all visible workflows</span>)
      chips -> Enum.join(chips)
    end
  end

  defp flow_investigation_context(data) do
    filters = map_value(data, :filters, %{})
    record = map_value(data, :record, %{})

    %{
      type:
        first_context_value([
          map_value(filters, :type),
          map_value(data, :type_filter),
          map_value(record, :type)
        ]),
      partition_key:
        first_context_value([
          map_value(filters, :partition_key),
          map_value(data, :partition_key),
          map_value(record, :partition_key)
        ]),
      state: first_context_value([map_value(filters, :state), map_value(record, :state)]),
      range: first_context_value([map_value(filters, :range)]),
      from_ms: map_value(filters, :from_ms),
      to_ms: map_value(filters, :to_ms),
      id: map_value(record, :id),
      root_flow_id: map_value(record, :root_flow_id),
      parent_flow_id: map_value(record, :parent_flow_id),
      correlation_id: map_value(record, :correlation_id)
    }
  end

  defp flow_states_context_params(context) do
    []
    |> put_context_param("type", Map.get(context, :type))
    |> put_context_param("partition_key", Map.get(context, :partition_key))
    |> put_context_param("state", Map.get(context, :state))
    |> put_context_param("range", Map.get(context, :range))
    |> put_context_time_params(context)
  end

  defp flow_failures_context_params(context) do
    []
    |> put_context_param("type", Map.get(context, :type))
    |> put_context_param("partition_key", Map.get(context, :partition_key))
  end

  defp flow_query_context_params(context) do
    []
    |> put_context_param("kind", "list")
    |> put_context_param("type", Map.get(context, :type))
    |> put_context_param("partition_key", Map.get(context, :partition_key))
    |> put_context_param("state", Map.get(context, :state))
    |> put_context_time_params(context)
  end

  defp flow_signals_context_params(context) do
    []
    |> put_context_param("type", Map.get(context, :type))
    |> put_context_param("partition_key", Map.get(context, :partition_key))
  end

  defp flow_lineage_context_params(context) do
    {mode, id} =
      cond do
        present_context_value?(Map.get(context, :root_flow_id)) ->
          {"root", Map.get(context, :root_flow_id)}

        present_context_value?(Map.get(context, :parent_flow_id)) ->
          {"parent", Map.get(context, :parent_flow_id)}

        present_context_value?(Map.get(context, :correlation_id)) ->
          {"correlation", Map.get(context, :correlation_id)}

        true ->
          {nil, nil}
      end

    []
    |> put_context_param("mode", mode)
    |> put_context_param("id", id)
    |> put_context_param("partition_key", Map.get(context, :partition_key))
  end

  defp present_context_value?(value), do: is_binary(value) and value != ""

  defp put_context_time_params(params, %{range: range}) when is_binary(range) and range != "",
    do: params

  defp put_context_time_params(params, context) do
    params
    |> put_context_param("from", format_context_time(Map.get(context, :from_ms)))
    |> put_context_param("to", format_context_time(Map.get(context, :to_ms)))
  end

  defp put_context_param(params, _key, nil), do: params
  defp put_context_param(params, _key, ""), do: params
  defp put_context_param(params, key, value), do: [{key, to_string(value)} | params]

  defp flow_context_path(path, []), do: path
  defp flow_context_path(path, params), do: path <> "?" <> URI.encode_query(Enum.reverse(params))

  defp format_context_time(value) when is_integer(value) do
    case DateTime.from_unix(value, :millisecond) do
      {:ok, datetime} -> Calendar.strftime(datetime, "%Y-%m-%dT%H:%M")
      {:error, _reason} -> nil
    end
  end

  defp format_context_time(_value), do: nil

  defp first_context_value(values) do
    Enum.find_value(values, fn
      value when is_binary(value) ->
        case String.trim(value) do
          "" -> nil
          normalized -> normalized
        end

      _invalid ->
        nil
    end)
  end

  defp map_value(map, key, default \\ nil)

  defp map_value(map, key, default) when is_map(map),
    do: Map.get(map, key, Map.get(map, Atom.to_string(key), default))

  defp map_value(_map, _key, default), do: default

  def render_flow_failures_flash(%{flash: %{kind: :ok, message: message}}),
    do:
      ~s(<div class="flow-alert flow-alert-ok" role="status" aria-live="polite" data-dashboard-transient-query="status,count,message">#{escape(message)}</div>)

  def render_flow_failures_flash(%{flash: %{kind: :error, message: message}}),
    do:
      ~s(<div class="flow-alert flow-alert-error" role="alert" data-dashboard-transient-query="status,count,message">#{escape(message)}</div>)

  def render_flow_failures_flash(_data), do: ""

  def render_flow_exact_scan_status(data) do
    filters = flow_failures_page_filters(data)

    if Map.get(filters, :scan_exact, false) do
      status = Map.get(data, :exact_scan_status, %{failures: :skipped, stuck: :skipped})

      status
      |> Enum.flat_map(fn {source, source_status} ->
        case source_status do
          {:error, reason} ->
            [{source, reason}]

          _ ->
            []
        end
      end)
      |> Enum.uniq_by(fn {source, reason} ->
        {flow_recovery_source_command(source), flow_recovery_error_detail(reason, filters)}
      end)
      |> Enum.map_join("", fn {source, reason} ->
        """
        <div class="flow-alert flow-alert-error">
          Exact scan issue: #{flow_recovery_source_command(source)} #{escape(flow_recovery_error_detail(reason, filters))}. Sampled rows are still shown; zero candidates is not authoritative.
        </div>
        """
      end)
    else
      ""
    end
  end

  def flow_recovery_source_command(source) when source in [:failures, :stuck], do: "FLOW.QUERY"
  def flow_recovery_source_command(source), do: source |> to_string() |> String.upcase()

  defp flow_recovery_error_detail(
         :query_partition_required,
         %{type: type, partition_key: partition_key}
       )
       when type in [nil, ""] and partition_key in [nil, ""],
       do: "requires a workflow type and partition key"

  defp flow_recovery_error_detail(:query_type_required, _filters),
    do: "requires one workflow type"

  defp flow_recovery_error_detail(:query_partition_required, _filters),
    do: "requires a partition key"

  defp flow_recovery_error_detail(reason, _filters),
    do: "failed with #{inspect(reason, limit: 8)}"
end
