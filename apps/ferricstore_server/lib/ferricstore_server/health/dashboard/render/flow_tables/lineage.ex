defmodule FerricstoreServer.Health.Dashboard.Render.FlowTables.Lineage do
  @moduledoc false

  import FerricstoreServer.Health.Dashboard.Format
  import FerricstoreServer.Health.Dashboard.FlowRecord
  import FerricstoreServer.Health.Dashboard.Render.FlowHistory

  def render_flow_lineage_hints([]), do: ""

  def render_flow_lineage_hints(hints) do
    links =
      Enum.map_join(hints, " ", fn hint ->
        params = [
          {"id", hint.id},
          {"mode", hint.mode},
          {"partition_key", hint.partition_key}
        ]

        href = "/dashboard/flow/lineage?" <> URI.encode_query(params)

        ~s(<a class="flow-pill flow-link" href="#{escape_attr(href)}">#{escape(hint.partition_key)} · #{escape(hint.label)} #{escape(hint.id)}</a>)
      end)

    ~s(<details class="dashboard-disclosure flow-lineage-hints"><summary>Recent lineage hints</summary><div class="flow-lineage-hint-links">#{links}</div></details>)
  end

  def flow_lineage_result_label(%{status: :idle, message: message}), do: message
  def flow_lineage_result_label(%{status: :ok, command: command}), do: "#{command} result"
  def flow_lineage_result_label(%{status: status, message: message}), do: "#{status}: #{message}"
  def flow_lineage_result_label(_result), do: "lineage result"

  def render_flow_lineage_status(%{result: %{status: :ok} = result} = data) do
    empty =
      if data.records == [] do
        message =
          if get_in(result, [:page, :has_more]) == true,
            do: "No visible records on this page. Continue to the next page.",
            else:
              "No visible lineage records returned. Check the id, partition, and query quality."

        ~s(<p class="flow-section-note" role="status">#{message}</p>)
      else
        ""
      end

    quality =
      FerricstoreServer.Health.Dashboard.Render.FlowQueryResults.render_flow_query_metadata(
        Map.take(result, [:quality])
      )

    empty <> quality
  end

  def render_flow_lineage_status(%{result: %{status: :idle}, filters: filters}) do
    render_flow_lineage_nodes([], filters)
  end

  def render_flow_lineage_status(%{result: result, filters: filters}) do
    heading =
      if result.status == :timeout, do: "Lineage query timed out", else: "Lineage query failed"

    """
    <div class="flow-alert flow-alert-error" role="alert">
      <strong>#{heading}</strong>: #{escape(result.message)}.
      Check the scope and query service, then retry. No results are available for this request.
      <a class="flow-link" href="#{escape_attr(lineage_path(filters, Map.get(filters, :cursor)))}">Retry query</a>
      <a class="flow-link" href="#{escape_attr(lineage_path(filters, nil))}">First page</a>
    </div>
    """
  end

  def render_flow_lineage_pagination(%{result: result, filters: filters}) do
    first =
      if Map.get(filters, :cursor) not in [nil, ""],
        do:
          ~s(<a class="flow-history-page-link" href="#{escape_attr(lineage_path(filters, nil))}">First page</a>),
        else: ""

    next =
      case Map.get(result, :page) do
        %{has_more: true, cursor: cursor} when is_binary(cursor) and cursor != "" ->
          ~s(<a class="flow-history-page-link" rel="next" href="#{escape_attr(lineage_path(filters, cursor))}">Next page</a>)

        %{has_more: true} ->
          ~s(<span class="flow-section-note">More records exist, but no continuation is available. Narrow the lookup or retry from the first page.</span>)

        _ ->
          ""
      end

    ~s(<nav class="flow-history-pages" aria-label="Lineage pages">#{first}#{next}</nav>)
  end

  defp lineage_path(filters, cursor) do
    query =
      %{
        "mode" => Map.get(filters, :mode, "root"),
        "id" => Map.get(filters, :target),
        "partition_key" => Map.get(filters, :partition_key),
        "limit" => Map.get(filters, :limit, 40),
        "cursor" => cursor
      }
      |> Enum.reject(fn {_key, value} -> value in [nil, ""] end)
      |> URI.encode_query()

    "/dashboard/flow/lineage?" <> query
  end

  def render_flow_lineage_nodes([], filters) do
    target = Map.get(filters, :target)
    partition_key = Map.get(filters, :partition_key)

    cond do
      not is_binary(target) or target == "" ->
        ~s(<div class="flow-lineage-empty">Choose parent, root, or correlation and enter an id.</div>)

      not is_binary(partition_key) or partition_key == "" ->
        ~s(<div class="flow-lineage-empty">Enter a partition key to run this bounded lineage query.</div>)

      true ->
        ~s(<div class="flow-lineage-empty">No lineage records matched this query.</div>)
    end
  end

  def render_flow_lineage_nodes(records, _filters) do
    count = length(records)

    notice =
      ~s(<p class="flow-section-note flow-lineage-preview-count">#{min(count, 40)} of #{count} loaded records. <a class="flow-link" href="#flow-lineage-records">View loaded table</a></p>)

    nodes =
      records
      |> Enum.take(40)
      |> Enum.map_join("\n", fn record ->
        state = flow_record_state(record)
        id = flow_record_id(record)
        type = flow_record_type(record)
        parent = flow_record_parent_id(record)

        parent_badge =
          if parent && parent != "",
            do: ~s(<span class="flow-pill">parent: #{escape(parent)}</span>),
            else: ""

        """
        <a class="flow-lineage-node #{flow_state_class(state)}" href="#{flow_detail_path(id, flow_record_partition_key(record))}">
          <div style="display:flex; justify-content:space-between; align-items:center; gap:8px;">
            <span class="flow-lineage-node-id">#{escape(id)}</span>
            <span class="badge #{flow_state_badge_class(state)}">#{escape(state)}</span>
          </div>
          <div class="flow-lineage-node-meta">type: #{escape(type)} #{parent_badge}</div>
        </a>
        """
      end)

    notice <> ~s(<div class="flow-lineage-map">#{nodes}</div>)
  end

  def render_flow_lineage_rows([]) do
    ~s(<tr><td colspan="8" class="c-muted">No lineage records loaded.</td></tr>)
  end

  def render_flow_lineage_rows(records) do
    Enum.map_join(records, "\n", fn record ->
      state = flow_record_state(record)

      """
      <tr>
        <td class="mono">#{render_flow_id_link(flow_record_id(record), flow_record_partition_key(record))}</td>
        <td class="mono">#{escape(flow_record_type(record))}</td>
        <td class="#{flow_state_class(state)}">#{escape(state)}</td>
        <td class="mono">#{relationship_link("parent", flow_record_parent_id(record), record)}</td>
        <td class="mono">#{relationship_link("root", flow_record_root_id(record), record)}</td>
        <td class="mono">#{relationship_link("correlation", flow_record_correlation_id(record), record)}</td>
        <td>#{format_timestamp_ms_or_dash(flow_record_updated_at_ms(record))}</td>
        <td>#{render_lineage_values(record)}</td>
      </tr>
      """
    end)
  end

  defp relationship_link(mode, id, record) do
    FerricstoreServer.Health.Dashboard.Render.FlowNavigation.relationship_link(mode, id, record)
  end

  defp render_lineage_values(record) do
    case flow_value_ref_entries(record, "current state") do
      [] ->
        ~s(<a class="flow-link" href="#{escape_attr(flow_detail_path(flow_record_id(record), flow_record_partition_key(record)))}" title="Values are not included in this query. Open workflow details.">Inspect values</a>)

      _refs ->
        render_flow_value_ref_badges(record, :detail_link)
    end
  end
end
