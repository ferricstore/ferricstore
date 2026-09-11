defmodule FerricstoreServer.Health.Dashboard.ScheduleThirdReviewTest do
  use FerricstoreServer.Test.DashboardCase

  alias FerricstoreServer.Health.Dashboard.Flow.Schedules
  alias FerricstoreServer.Health.Dashboard.Render.FlowSchedules
  alias FerricstoreServer.Health.Endpoint.RouteRequirements
  alias FerricstoreServer.Health.Endpoint

  setup do
    {:ok, _} = Application.ensure_all_started(:ferricstore_server)
    previous = Application.get_env(:ferricstore, :protected_mode)
    Application.put_env(:ferricstore, :protected_mode, false)
    on_exit(fn -> restore_env(:protected_mode, previous) end)
    :ok
  end

  test "replacement cannot bypass review and does not reactivate a paused schedule" do
    {id, _paused} = paused_schedule()
    assert {:error, reason} = Schedules.apply_form(params(id))
    assert reason =~ "review"
    assert {:ok, current} = FerricStore.flow_schedule_get(id)
    assert current.state == "paused"
    assert current.every_ms == 60_000
  end

  test "replacement review includes old and new definitions and binds mutation to reviewed version" do
    {id, paused} = paused_schedule()
    assert {:ok, data} = Schedules.preview_form(params(id))
    assert data.review.current.version == paused.version
    assert data.review.planned.every_ms == 120_000
    html = FlowSchedules.render_flow_schedule_create_form(data)
    assert html =~ "Paused schedule will become active"
    assert html =~ "Current definition"
    assert html =~ "Replacement definition"
    assert html =~ "Fire count resets to 0"
    assert html =~ "Confirm replacement"
    assert html =~ "old-schedule-target-prefix"

    confirmed = Map.merge(params(id), data.review.fields) |> Map.put("confirm_replace", "true")
    assert {:ok, _} = FerricStore.flow_schedule_resume(id)
    assert {:error, message} = Schedules.apply_form(confirmed)
    assert message =~ "changed"
    assert {:ok, current} = FerricStore.flow_schedule_get(id)
    assert current.every_ms == 60_000
  end

  test "edited replacement drafts require a fresh review and current review can replace" do
    {id, _paused} = paused_schedule()
    assert {:ok, data} = Schedules.preview_form(params(id))
    confirmed = Map.merge(params(id), data.review.fields) |> Map.put("confirm_replace", "true")
    assert {:error, message} = Schedules.apply_form(Map.put(confirmed, "every_ms", "300000"))
    assert message =~ "review"
    assert {:ok, _} = Schedules.apply_form(confirmed)
    assert {:ok, current} = FerricStore.flow_schedule_get(id)
    assert current.state == "active"
    assert current.every_ms == 120_000
    assert {:error, _} = Schedules.apply_form(confirmed)
  end

  test "review accepts equivalent submitted defaults but rejects expired confirmation" do
    {id, _paused} = paused_schedule()
    original = Map.put(params(id), "overwrite", "on")
    assert {:ok, data} = Schedules.preview_form(original)

    confirmed =
      Map.merge(params(id), data.review.fields)
      |> Map.merge(%{
        "confirm_replace" => "true",
        "overlap_policy" => "skip",
        "target_payload" => "",
        "target_partition" => ""
      })

    assert {:ok, _} = Schedules.apply_form(confirmed)

    old = System.system_time(:millisecond) - 300_001
    assert {:ok, expired} = Schedules.preview_form(params(id), old)

    expired_params =
      Map.merge(params(id), expired.review.fields) |> Map.put("confirm_replace", "true")

    assert {:error, message} = Schedules.apply_form(expired_params)
    assert message =~ "expired"
  end

  test "cron preview uses the server parser, timezone and exact UTC bounds without creating" do
    id = "cron-preview-#{System.unique_integer([:positive])}"

    cron = %{
      "action" => "create",
      "id" => id,
      "schedule_kind" => "cron",
      "cron" => "0 9 * * *",
      "timezone" => "Asia/Jerusalem",
      "target_type" => "test",
      "start_at_utc" => "2026-09-10T00:00",
      "end_at_utc" => "2026-09-11T00:00",
      "max_fires" => "2"
    }

    now = DateTime.to_unix(~U[2026-09-10 00:00:00Z], :millisecond)
    assert {:ok, data} = Schedules.preview_form(cron, now)

    assert data.review.planned.next_run_at_ms ==
             DateTime.to_unix(~U[2026-09-10 06:00:00Z], :millisecond)

    html = FlowSchedules.render_flow_schedule_create_form(data)
    assert html =~ "Next fire (UTC)"
    assert html =~ "2026-09-10 06:00:00.000 UTC"
    assert html =~ "Asia/Jerusalem"
    assert html =~ "09:00:00"
    assert {:ok, nil} = FerricStore.flow_schedule_get(id)
    assert {:error, reason} = Schedules.preview_form(Map.put(cron, "cron", "0 9 31 2 *"), now)
    assert reason =~ "cannot occur"

    assert {:error, _} =
             Schedules.preview_form(Map.put(cron, "end_at_utc", "2026-09-10T05:00"), now)
  end

  test "overwrite review requires schedule read permission as well as create" do
    assert RouteRequirements.flow_schedule_form_requirement(params("schedule")) == [
             {"FLOW.SCHEDULE.CREATE", key: {"*", :write}},
             {"FLOW.SCHEDULE.GET", []}
           ]
  end

  test "guarded native replacement does not recreate a schedule deleted after review" do
    {id, paused} = paused_schedule()
    assert :ok = FerricStore.flow_schedule_delete(id)

    assert {:error, message} =
             FerricStore.flow_schedule_create(id,
               every_ms: 120_000,
               overwrite: true,
               expected_state: paused.state,
               expected_version: paused.version,
               target: [type: "after"]
             )

    assert message =~ "changed"
    assert {:ok, current} = FerricStore.flow_schedule_get(id)
    assert current.state == "cancelled"
    assert current.every_ms == 60_000
  end

  test "HTTP schedule preview returns review without creating a schedule" do
    id = "http-preview-#{System.unique_integer([:positive])}"
    {token, cookie} = FerricstoreServer.Health.Endpoint.Session.csrf_pair()
    cookie = cookie |> String.split(";", parts: 2) |> hd()

    response =
      http_post_form(
        Endpoint.port(),
        "/dashboard/flow/schedules",
        %{
          "action" => "create",
          "preview" => "true",
          "id" => id,
          "schedule_kind" => "cron",
          "cron" => "0 9 * * *",
          "target_type" => "test",
          "_csrf_token" => token
        },
        [{"Cookie", cookie}]
      )

    assert extract_status_code(response) == 200
    assert extract_body(response) =~ "Next fire (UTC)"
    assert extract_body(response) =~ ~s(data-schedule-confirm)
    assert {:ok, nil} = FerricStore.flow_schedule_get(id)
  end

  defp paused_schedule do
    id = "replace-review-#{System.unique_integer([:positive])}"

    assert {:ok, _} =
             FerricStore.flow_schedule_create(id,
               every_ms: 60_000,
               target: [type: "before", id_prefix: "old-schedule-target-prefix"]
             )

    assert {:ok, _} = FerricStore.flow_schedule_pause(id)
    assert {:ok, paused} = FerricStore.flow_schedule_get(id)
    {id, paused}
  end

  defp params(id),
    do: %{
      "action" => "create",
      "id" => id,
      "schedule_kind" => "interval",
      "every_ms" => "120000",
      "target_type" => "after",
      "overwrite" => "true"
    }
end
