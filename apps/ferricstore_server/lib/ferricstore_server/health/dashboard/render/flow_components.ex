defmodule FerricstoreServer.Health.Dashboard.Render.FlowComponents do
  require EEx

  alias FerricstoreServer.Health.Dashboard.Flow.Recovery

  import FerricstoreServer.Health.Dashboard.Format
  import FerricstoreServer.Health.Dashboard.Render.FlowQueryPolicy
  import FerricstoreServer.Health.Dashboard.Render.FlowTables

  @templates_dir Path.expand("../templates", __DIR__)

  EEx.function_from_file(
    :defp,
    :component_flow_failures_controls,
    Path.join(@templates_dir, "flow_failures_controls.html.eex"),
    [:assigns]
  )

  EEx.function_from_file(
    :defp,
    :component_flow_recovery_actions,
    Path.join(@templates_dir, "flow_recovery_actions.html.eex"),
    [:assigns]
  )

  EEx.function_from_file(
    :defp,
    :component_flow_failures_summary,
    Path.join(@templates_dir, "flow_failures_summary.html.eex"),
    [:assigns]
  )

  EEx.function_from_file(
    :defp,
    :component_flow_failures_table,
    Path.join(@templates_dir, "flow_failures_table.html.eex"),
    [:assigns]
  )

  EEx.function_from_file(
    :defp,
    :component_flow_lineage_controls,
    Path.join(@templates_dir, "flow_lineage_controls.html.eex"),
    [:assigns]
  )

  EEx.function_from_file(
    :defp,
    :component_flow_lineage_summary,
    Path.join(@templates_dir, "flow_lineage_summary.html.eex"),
    [:assigns]
  )

  EEx.function_from_file(
    :defp,
    :component_flow_lineage_graph,
    Path.join(@templates_dir, "flow_lineage_graph.html.eex"),
    [:assigns]
  )

  EEx.function_from_file(
    :defp,
    :component_flow_lineage_table,
    Path.join(@templates_dir, "flow_lineage_table.html.eex"),
    [:assigns]
  )

  EEx.function_from_file(
    :defp,
    :component_flow_query_controls,
    Path.join(@templates_dir, "flow_query_controls.html.eex"),
    [:assigns]
  )

  EEx.function_from_file(
    :defp,
    :component_flow_query_result,
    Path.join(@templates_dir, "flow_query_result.html.eex"),
    [:assigns]
  )

  def render_flow_failures_controls(data), do: component_flow_failures_controls(%{data: data})
  def render_flow_recovery_actions(data), do: component_flow_recovery_actions(%{data: data})
  def render_flow_failures_summary(data), do: component_flow_failures_summary(%{data: data})
  def render_flow_failures_table(data), do: component_flow_failures_table(%{data: data})
  def render_flow_lineage_controls(data), do: component_flow_lineage_controls(%{data: data})
  def render_flow_lineage_summary(data), do: component_flow_lineage_summary(%{data: data})
  def render_flow_lineage_graph(data), do: component_flow_lineage_graph(%{data: data})
  def render_flow_lineage_table(data), do: component_flow_lineage_table(%{data: data})
  def render_flow_query_controls(data), do: component_flow_query_controls(%{data: data})
  def render_flow_query_result(data), do: component_flow_query_result(%{data: data})

  defp flow_failures_page_filters(data), do: Recovery.page_filters(data)
end
