defmodule FerricstoreServer.Health.Dashboard.FlowScheduleSafetyTest do
  use FerricstoreServer.Test.DashboardCase

  alias FerricstoreServer.Health.Dashboard.Flow.Schedules
  alias FerricstoreServer.Health.Dashboard.Render.FlowSchedules

  setup do
    {:ok, _} = Application.ensure_all_started(:ferricstore_server)
    :ok
  end

  test "Fire and Delete render server-validated confirmation forms" do
    html =
      FlowSchedules.render_flow_schedules_table(
        [%{id: "daily", state: "active", version: 7, target: %{type: "email"}}],
        %{state: "active", kind: :interval, q: "day", limit: 25}
      )

    assert html =~ "Confirm Fire"
    assert html =~ "Confirm Delete"
    assert html =~ ~s(name="confirm_action" value="true")
    assert html =~ ~s(name="expected_state" value="active")
    assert html =~ ~s(name="expected_version" value="7")
    assert html =~ ~s(name="state" value="active")
    assert html =~ ~s(name="kind" value="interval")
    assert html =~ ~s(name="q" value="day")
    assert html =~ ~s(name="limit" value="25")
    assert html =~ ~s(data-dashboard-single-submit)
  end

  test "Fire requires explicit confirmation and does not mutate on rejection" do
    schedule = create_schedule("confirm")

    assert {:error, message} =
             Schedules.apply_form(%{
               "id" => schedule.id,
               "action" => "fire",
               "expected_state" => schedule.state,
               "expected_version" => Integer.to_string(schedule.version)
             })

    assert message =~ "confirmation"
    assert {:ok, current} = FerricStore.flow_schedule_get(schedule.id)
    assert current.fire_count == 0
  end

  test "stale destructive submissions are rejected and duplicate Fire is harmless" do
    schedule = create_schedule("stale")
    params = destructive_params(schedule, "fire")

    assert {:ok, _message} = Schedules.apply_form(params)
    assert {:error, message} = Schedules.apply_form(params)
    assert message =~ "changed"

    assert {:ok, current} = FerricStore.flow_schedule_get(schedule.id)
    assert current.fire_count == 1

    stale_delete = destructive_params(schedule, "delete")
    assert {:error, delete_message} = Schedules.apply_form(stale_delete)
    assert delete_message =~ "changed"
    assert {:ok, %{} = _current} = FerricStore.flow_schedule_get(schedule.id)
  end

  defp create_schedule(label) do
    id = "dashboard-schedule-safety-#{label}-#{System.unique_integer([:positive])}"
    now_ms = System.system_time(:millisecond)

    assert {:ok, _created} =
             FerricStore.flow_schedule_create(id,
               kind: :interval,
               every_ms: 60_000,
               start_at_ms: now_ms + 60_000,
               now_ms: now_ms,
               target: [id_prefix: id <> "-target", type: "dashboard-schedule-safety"]
             )

    assert {:ok, schedule} = FerricStore.flow_schedule_get(id)
    schedule
  end

  defp destructive_params(schedule, action) do
    %{
      "id" => schedule.id,
      "action" => action,
      "confirm_action" => "true",
      "expected_state" => schedule.state,
      "expected_version" => Integer.to_string(schedule.version)
    }
  end
end
