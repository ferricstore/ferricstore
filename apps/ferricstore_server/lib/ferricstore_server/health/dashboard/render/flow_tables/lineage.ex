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

    ~s(<div class="flow-section-note">Recent lineage hints: #{links}</div>)
  end

  def flow_lineage_result_label(%{status: :idle, message: message}), do: message
  def flow_lineage_result_label(%{status: :ok, command: command}), do: "#{command} result"
  def flow_lineage_result_label(%{status: status, message: message}), do: "#{status}: #{message}"
  def flow_lineage_result_label(_result), do: "lineage result"

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
        <td class="mono">#{escape(flow_record_parent_id(record) || "-")}</td>
        <td class="mono">#{escape(flow_record_root_id(record) || "-")}</td>
        <td class="mono">#{escape(flow_record_correlation_id(record) || "-")}</td>
        <td>#{format_timestamp_ms_or_dash(flow_record_updated_at_ms(record))}</td>
        <td>#{render_flow_value_ref_badges(record, :detail_link)}</td>
      </tr>
      """
    end)
  end
end
