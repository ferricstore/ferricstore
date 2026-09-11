alias FerricstoreServer.Health.Dashboard.Flow.{PolicyEditor, Schedules}
alias FerricstoreServer.Health.Dashboard.Layout
alias FerricstoreServer.Health.Dashboard.Layout.Styles
alias FerricstoreServer.Health.Dashboard.Render.{FlowPolicy, FlowRetention, FlowSchedules}

output =
  List.first(System.argv()) ||
    Path.join(System.tmp_dir!(), "ferricstore-management-fourth-fixtures")

File.mkdir_p!(output)

schedule = %{
  id: "daily-customer-reconciliation",
  kind: :cron,
  cron: "0 9 * * *",
  timezone: "Etc/UTC",
  state: "active",
  next_run_at_ms: 1_800_000_000_000,
  target: %{
    type: "customer-reconciliation",
    partition_key: "tenant-europe-" <> String.duplicate("opaque", 60)
  },
  fire_count: 12,
  version: 3
}

params = %{
  "id" => "unsaved-payload-draft",
  "schedule_kind" => "interval",
  "every_ms" => "60000",
  "target_type" => "customer-reconciliation",
  "target_payload" => "{invalid"
}

error = Schedules.create_error_page(params, "target payload must be valid JSON")
editor = %{PolicyEditor.empty() | type: "customer-reconciliation"}

pages = %{
  "schedules" =>
    FlowSchedules.render_flow_schedule_create_form() <>
      FlowSchedules.render_flow_schedules_filters(%{}) <>
      FlowSchedules.render_flow_schedules_table([schedule, %{schedule | id: "weekly-archive"}]),
  "schedule-error" =>
    FlowSchedules.render_flow_schedules_flash(error.flash) <>
      FlowSchedules.render_flow_schedule_create_form(error) <>
      FlowSchedules.render_flow_schedules_filters(%{}),
  "policies" =>
    FlowPolicy.render_flow_policy_editor(%{editor: editor}) <>
      FlowPolicy.render_flow_policies_table(
        [
          %{
            type: "customer-reconciliation",
            source: "configured",
            retry: %{max_retries: 15},
            generation: 3
          }
        ],
        %{}
      ),
  "policy-error" =>
    FlowPolicy.render_flow_policy_editor(%{
      editor: %{editor | max_retries: "1e3"},
      flash: %{kind: :error, message: "ERR max retries must be an integer >= 0"}
    }),
  "retention" =>
    FlowRetention.render_flow_retention_summary(%{projection: %{metrics: %{lmdb_pending: 17}}}) <>
      FlowRetention.render_flow_retention_controls(%{
        limit: 200,
        flash: %{kind: :dry_run, limit: 200}
      })
}

for {name, content} <- pages do
  html = """
  <!doctype html><html lang="en"><head><meta charset="utf-8"><title>Management regression fixture</title>
  <style>#{Styles.stylesheet()}</style></head><body>
  <div class="layout"><nav class="sidebar"><a href="/leave">Leave page</a></nav>
  <main class="main-content" id="dashboard-main"><div class="content">#{content}</div></main></div>
  #{Layout.dashboard_live_script()}
  </body></html>
  """

  File.write!(Path.join(output, name <> ".html"), html)
end

IO.puts("Management fixtures: " <> Path.expand(output))
