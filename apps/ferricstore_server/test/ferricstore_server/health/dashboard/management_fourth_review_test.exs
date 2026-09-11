defmodule FerricstoreServer.Health.Dashboard.ManagementFourthReviewTest do
  use ExUnit.Case, async: true

  alias FerricstoreServer.Health.Dashboard.Flow.{PolicyEditor, Schedules}

  alias FerricstoreServer.Health.Dashboard.Render.{
    FlowFormScripts,
    FlowPolicy,
    FlowRetention,
    FlowSchedules
  }

  test "schedule draft has a navigation guard without payload persistence" do
    script = FlowFormScripts.schedule_script()
    assert script =~ "beforeunload"
    assert script =~ "initialDraft"
    assert script =~ "submitting = true"
    refute script =~ "sessionStorage"
    refute script =~ "localStorage"
    assert FlowSchedules.render_flow_schedule_create_form() =~ "data-schedule-dirty-status"
  end

  test "schedule payload errors are structured, associated, retained and focusable" do
    data = Schedules.create_error_page(schedule_params(), "target payload must be valid JSON")
    assert data.field_errors == %{"target_payload" => "target payload must be valid JSON"}
    assert data.catalog_loaded == false
    assert data.draft["target_payload"] == "{invalid"
    html = FlowSchedules.render_flow_schedule_create_form(data)
    assert html =~ ~r/<textarea[^>]*name="target_payload"[^>]*aria-invalid="true"/
    assert html =~ ~s(aria-describedby="schedule-create-target_payload-error")
    assert html =~ ~s(id="schedule-create-target_payload-error")
    assert html =~ "data-schedule-field-error"
    assert FlowFormScripts.schedule_script() =~ "firstError.focus()"
  end

  test "schedule timing validation localizes errors without catalog reads" do
    params = schedule_params() |> Map.put("target_payload", "") |> Map.put("every_ms", "1e3")

    data =
      Schedules.create_error_page(
        params,
        "interval (every_ms) must be a positive integer in milliseconds"
      )

    assert data.field_errors == %{
             "every_ms" => "interval (every_ms) must be a positive integer in milliseconds"
           }

    html = FlowSchedules.render_flow_schedule_create_form(data)
    assert html =~ ~s(aria-describedby="schedule-create-every_ms-error")

    assert FlowSchedules.render_flow_schedules_flash(data.flash) =~
             ~s(id="flow-schedule-create-error")
  end

  test "policy numeric fields expose one validity and accessible error contract" do
    editor = %{PolicyEditor.empty() | type: "review-policy"}
    html = FlowPolicy.render_flow_policy_editor(%{editor: editor})

    for field <-
          ~w(max_retries base_ms max_ms jitter_pct max_active_ms retention_ttl_ms history_max_events) do
      assert html =~ ~s(aria-describedby="policy-#{field}-error")
      assert html =~ ~s(id="policy-#{field}-error")
    end

    assert FlowFormScripts.policy_script() =~ "setCustomValidity"
    assert FlowFormScripts.policy_script() =~ "aria-invalid"
  end

  test "retention refresh does not claim to simulate global cleanup" do
    html = FlowRetention.render_flow_retention_controls(%{limit: 200})
    assert html =~ ">Refresh sampled preview</button>"
    refute html =~ ">Dry Run</button>"
    assert html =~ "does not simulate global cleanup"
    refute html =~ ~s(name="confirm_cleanup")
    assert html =~ ~s(value="review_cleanup")
    refute html =~ ">Run Cleanup</button>"
    flash = FlowRetention.render_flow_retention_flash(%{kind: :dry_run, limit: 200})
    assert flash =~ "Sampled preview refreshed"
    assert flash =~ "does not apply the global cleanup limit"
    refute flash =~ "Dry run ready"
  end

  test "retention metrics count pending operations rather than projection lag" do
    html =
      FlowRetention.render_flow_retention_summary(%{projection: %{metrics: %{lmdb_pending: 17}}})

    assert html =~ "Pending index operations"
    assert html =~ "17"
    refute html =~ "Query Index Lag"
  end

  test "policy identity links directly to the correctly escaped editor scope" do
    html = FlowPolicy.render_flow_policy_row(%{type: "tenant/a&b", source: "configured"})
    [_, first_cell] = Regex.run(~r/<td class="mono">(.*?)<\/td>/s, html)
    assert first_cell =~ ~s(<a class="flow-link")

    assert first_cell =~
             ~s(href="/dashboard/flow/policies?edit=tenant%2Fa%26b&amp;edit_state=#flow-policy-editor")

    assert first_cell =~ "tenant/a&amp;b</a>"
  end

  test "schedule definition spans a separate row without expanding the ID cell" do
    html =
      FlowSchedules.render_flow_schedules_table([
        %{id: "stable-row", state: "active", kind: :interval, every_ms: 60_000}
      ])

    [_, first_cell] = Regex.run(~r/<tbody>\s*<tr[^>]*>\s*<td[^>]*>(.*?)<\/td>/s, html)
    refute first_cell =~ "<details"
    assert html =~ ~s(<tr class="flow-schedule-definition-row">)
    assert html =~ ~s(<td colspan="12">)
    assert html =~ ~s(class="flow-schedules-table")
    assert html =~ "Schedule definition"
  end

  test "schedule catalog filters have visible labels separate from exact lookup" do
    html = FlowSchedules.render_flow_schedules_filters(%{})

    for label <- ["ID contains", "State", "Kind", "Limit"] do
      assert html =~ "<span>#{label}</span>"
    end

    assert html =~ "<span>Exact schedule ID</span>"
  end

  defp schedule_params do
    %{
      "id" => "review-draft",
      "schedule_kind" => "interval",
      "every_ms" => "60000",
      "target_type" => "review-type",
      "target_payload" => "{invalid"
    }
  end
end
