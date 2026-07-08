defmodule FerricstoreServer.Health.Dashboard.Render.FlowQueryPolicy do
  @moduledoc """
  Compatibility facade for Flow dashboard query, policy, and retention render helpers.

  New code should import the focused modules directly:

    * `FlowQueryControls`
    * `FlowPolicy`
    * `FlowRetention`
  """

  defdelegate render_flow_query_kind_help(kind),
    to: FerricstoreServer.Health.Dashboard.Render.FlowQueryControls

  defdelegate render_flow_query_type_field(filters),
    to: FerricstoreServer.Health.Dashboard.Render.FlowQueryControls

  defdelegate render_flow_query_state_field(filters),
    to: FerricstoreServer.Health.Dashboard.Render.FlowQueryControls

  defdelegate render_flow_query_attribute_fields(filters),
    to: FerricstoreServer.Health.Dashboard.Render.FlowQueryControls

  defdelegate render_flow_query_state_meta_fields(filters),
    to: FerricstoreServer.Health.Dashboard.Render.FlowQueryControls

  defdelegate render_flow_query_id_field(filters),
    to: FerricstoreServer.Health.Dashboard.Render.FlowQueryControls

  defdelegate render_flow_query_partition_field(filters),
    to: FerricstoreServer.Health.Dashboard.Render.FlowQueryControls

  defdelegate render_flow_query_time_fields(filters),
    to: FerricstoreServer.Health.Dashboard.Render.FlowQueryControls

  defdelegate render_flow_query_direction_field(filters),
    to: FerricstoreServer.Health.Dashboard.Render.FlowQueryControls

  defdelegate render_flow_query_dynamic_script,
    to: FerricstoreServer.Health.Dashboard.Render.FlowQueryControls

  defdelegate flow_query_hidden_attr(filters, kinds),
    to: FerricstoreServer.Health.Dashboard.Render.FlowQueryControls

  defdelegate flow_query_disabled_attr(filters, kinds),
    to: FerricstoreServer.Health.Dashboard.Render.FlowQueryControls

  defdelegate flow_query_kinds_attr(kinds),
    to: FerricstoreServer.Health.Dashboard.Render.FlowQueryControls

  defdelegate render_flow_type_options(types, selected_type),
    to: FerricstoreServer.Health.Dashboard.Render.FlowQueryControls

  defdelegate render_flow_lineage_mode_options(selected_mode),
    to: FerricstoreServer.Health.Dashboard.Render.FlowQueryControls

  defdelegate render_flow_query_kind_options(selected_kind),
    to: FerricstoreServer.Health.Dashboard.Render.FlowQueryControls

  defdelegate flow_query_kind_options,
    to: FerricstoreServer.Health.Dashboard.Render.FlowQueryControls

  defdelegate flow_query_kind_doc(kind),
    to: FerricstoreServer.Health.Dashboard.Render.FlowQueryControls

  defdelegate flow_query_kind_docs,
    to: FerricstoreServer.Health.Dashboard.Render.FlowQueryControls

  defdelegate render_flow_overview_filter(data),
    to: FerricstoreServer.Health.Dashboard.Render.FlowQueryControls

  defdelegate flow_overview_live_url(filters),
    to: FerricstoreServer.Health.Dashboard.Render.FlowQueryControls

  defdelegate flow_states_live_url(filters),
    to: FerricstoreServer.Health.Dashboard.Render.FlowQueryControls

  defdelegate flow_states_filter_query(filters),
    to: FerricstoreServer.Health.Dashboard.Render.FlowQueryControls

  defdelegate flow_signals_live_url(filters),
    to: FerricstoreServer.Health.Dashboard.Render.FlowQueryControls

  defdelegate flow_signals_filter_query(filters),
    to: FerricstoreServer.Health.Dashboard.Render.FlowQueryControls

  defdelegate maybe_put_query_param(params, key, value),
    to: FerricstoreServer.Health.Dashboard.Render.FlowQueryControls

  defdelegate flow_filter_limit_query_value(limit),
    to: FerricstoreServer.Health.Dashboard.Render.FlowQueryControls

  defdelegate flow_policy_editor_data(type),
    to: FerricstoreServer.Health.Dashboard.Render.FlowPolicy

  defdelegate flow_policy_clean_form_value(value),
    to: FerricstoreServer.Health.Dashboard.Render.FlowPolicy

  defdelegate render_flow_policy_editor(data),
    to: FerricstoreServer.Health.Dashboard.Render.FlowPolicy

  defdelegate render_flow_policy_flash(flash),
    to: FerricstoreServer.Health.Dashboard.Render.FlowPolicy

  defdelegate render_flow_policy_preview(editor),
    to: FerricstoreServer.Health.Dashboard.Render.FlowPolicy

  defdelegate render_flow_policy_backoff_select(current),
    to: FerricstoreServer.Health.Dashboard.Render.FlowPolicy

  defdelegate render_flow_policy_mode_select(current),
    to: FerricstoreServer.Health.Dashboard.Render.FlowPolicy

  defdelegate render_flow_policy_commands,
    to: FerricstoreServer.Health.Dashboard.Render.FlowPolicy

  defdelegate flow_policy_command_reference,
    to: FerricstoreServer.Health.Dashboard.Render.FlowPolicy

  defdelegate render_flow_policies_table(policies, policy_scan),
    to: FerricstoreServer.Health.Dashboard.Render.FlowPolicy

  defdelegate render_flow_policy_scan_note(policy_scan),
    to: FerricstoreServer.Health.Dashboard.Render.FlowPolicy

  defdelegate render_flow_policy_row(row),
    to: FerricstoreServer.Health.Dashboard.Render.FlowPolicy

  defdelegate flow_policy_edit_url(type),
    to: FerricstoreServer.Health.Dashboard.Render.FlowPolicy

  defdelegate flow_policy_source_class(source),
    to: FerricstoreServer.Health.Dashboard.Render.FlowPolicy

  defdelegate render_flow_policy_state_overrides(states),
    to: FerricstoreServer.Health.Dashboard.Render.FlowPolicy

  defdelegate flow_policy_backoff_summary(backoff),
    to: FerricstoreServer.Health.Dashboard.Render.FlowPolicy

  defdelegate flow_policy_retention_summary(retention),
    to: FerricstoreServer.Health.Dashboard.Render.FlowPolicy

  defdelegate flow_policy_field(map, key, default),
    to: FerricstoreServer.Health.Dashboard.Render.FlowPolicy

  defdelegate render_flow_retention_summary(data),
    to: FerricstoreServer.Health.Dashboard.Render.FlowRetention

  defdelegate render_flow_retention_controls(data),
    to: FerricstoreServer.Health.Dashboard.Render.FlowRetention

  defdelegate render_flow_retention_flash(flash),
    to: FerricstoreServer.Health.Dashboard.Render.FlowRetention

  defdelegate render_flow_retention_commands,
    to: FerricstoreServer.Health.Dashboard.Render.FlowRetention

  defdelegate flow_retention_command_reference,
    to: FerricstoreServer.Health.Dashboard.Render.FlowRetention

  defdelegate render_flow_retention_candidates(data),
    to: FerricstoreServer.Health.Dashboard.Render.FlowRetention

  defdelegate render_flow_retention_candidate_row(record, now_ms),
    to: FerricstoreServer.Health.Dashboard.Render.FlowRetention
end
