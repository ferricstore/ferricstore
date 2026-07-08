defmodule FerricstoreServer.Health.Dashboard.Render.FlowTables do
  @moduledoc false

  alias FerricstoreServer.Health.Dashboard.Render.FlowTables.Lineage
  alias FerricstoreServer.Health.Dashboard.Render.FlowTables.Projection
  alias FerricstoreServer.Health.Dashboard.Render.FlowTables.Records
  alias FerricstoreServer.Health.Dashboard.Render.FlowTables.Signals

  defdelegate default_flow_projection_health(), to: Projection
  defdelegate flow_projection_rollup(metrics), to: Projection
  defdelegate flow_projection_health_class(rollup), to: Projection
  defdelegate render_flow_projection_health(data), to: Projection

  defdelegate render_flow_states_table(
                states,
                total_sampled,
                filtered_sampled,
                sample_limit,
                filters
              ),
              to: Records

  defdelegate flow_state_operational_hint(state), to: Records
  defdelegate render_flow_state_breakdown(types), to: Records
  defdelegate render_flow_custom_states(states), to: Records
  defdelegate render_flow_fifo_lanes(lanes, total_sampled, sample_limit), to: Records
  defdelegate render_flow_workers(workers), to: Records
  defdelegate render_flow_running_records(records, total_sampled, sample_limit), to: Records
  defdelegate render_flow_due_records(title, records, total_sampled, sample_limit), to: Records
  defdelegate render_flow_failures_rows(records), to: Records
  defdelegate render_flow_recent_records(records, limit \\ nil), to: Records

  defdelegate render_flow_signals_table(
                signals,
                total_sampled,
                filtered_sampled,
                sample_limit,
                filters
              ),
              to: Signals

  defdelegate render_flow_signals_table(
                signals,
                total_sampled,
                filtered_sampled,
                sample_limit,
                filters,
                mode
              ),
              to: Signals

  defdelegate render_flow_signal_row(row, mode), to: Signals
  defdelegate render_flow_signals_table_head(mode), to: Signals

  defdelegate flow_signals_table_title(
                total_sampled,
                filtered_sampled,
                sample_limit,
                filters,
                mode
              ),
              to: Signals

  defdelegate flow_signal_state_move_html(row), to: Signals
  defdelegate flow_signal_event_href(row, mode), to: Signals
  defdelegate flow_signal_refs_summary_html(row, mode), to: Signals

  defdelegate render_flow_lineage_hints(hints), to: Lineage
  defdelegate flow_lineage_result_label(result), to: Lineage
  defdelegate render_flow_lineage_nodes(records, filters), to: Lineage
  defdelegate render_flow_lineage_rows(records), to: Lineage
  defdelegate flow_query_result_command(result), to: Lineage
  defdelegate render_flow_query_status(result), to: Lineage
  defdelegate render_flow_query_rows(result), to: Lineage
end
