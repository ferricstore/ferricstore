defmodule FerricstoreServer.Health.DashboardTest.Sections.FlowSchedules do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias FerricstoreServer.Health.Dashboard

      describe "Flow schedules dashboard" do
        test "schedules page renders summary filters actions and failed schedules" do
          html =
            Dashboard.render_flow_schedules_page(%{
              schedules: [
                %{
                  id: "campaign-daily",
                  state: "active",
                  kind: :cron,
                  next_run_at_ms: 1_700_000_000_000,
                  last_fire_at_ms: 1_699_999_000_000,
                  fire_count: 4,
                  overlap_policy: :skip,
                  max_fires: 10,
                  target: %{type: "email"}
                },
                %{
                  id: "campaign-failed",
                  state: "failed",
                  kind: :interval,
                  fire_count: 2,
                  last_overlap_reason: "previous target still active",
                  last_overlap_at_ms: 1_700_000_100_000,
                  target: %{type: "email"}
                }
              ],
              failed_schedules: [
                %{
                  id: "campaign-failed",
                  last_overlap_reason: "previous target still active",
                  last_overlap_at_ms: 1_700_000_100_000
                }
              ],
              summary: %{:total => 2, "active" => 1, "failed" => 1},
              filters: %{state: :all, kind: nil, q: nil, limit: 100},
              flash: %{kind: :ok, message: "paused campaign-daily"}
            })

          assert String.contains?(html, "FerricFlow Schedules")
          assert String.contains?(html, "campaign-daily")
          assert String.contains?(html, "campaign-failed")
          assert String.contains?(html, "Pause")
          assert String.contains?(html, "Delete")
          assert String.contains?(html, "Failed Schedules")
          assert String.contains?(html, "previous target still active")
        end

        test "schedules collector returns durable schedules and filters by id" do
          schedule_id = "dashboard-schedule-#{System.unique_integer([:positive])}"

          assert {:ok, _schedule} =
                   FerricStore.flow_schedule_create(schedule_id,
                     kind: :interval,
                     every_ms: 1_000,
                     start_at_ms: 10_000,
                     now_ms: 9_000,
                     target: [
                       id_prefix: schedule_id <> "-target",
                       type: "dashboard-schedule-type"
                     ]
                   )

          data = Dashboard.collect_flow_schedules_page(state: :all, q: schedule_id, limit: 20)

          assert Enum.any?(data.schedules, &(&1.id == schedule_id))
          assert data.summary.total >= 1
        end

        test "POST schedules action pauses schedule and redirects with status" do
          schedule_id = "dashboard-schedule-post-#{System.unique_integer([:positive])}"

          assert {:ok, _schedule} =
                   FerricStore.flow_schedule_create(schedule_id,
                     kind: :interval,
                     every_ms: 1_000,
                     start_at_ms: 10_000,
                     now_ms: 9_000,
                     target: [
                       id_prefix: schedule_id <> "-target",
                       type: "dashboard-schedule-post-type"
                     ]
                   )

          response =
            http_post_form(
              FerricstoreServer.Health.Endpoint.port(),
              "/dashboard/flow/schedules",
              %{
                "id" => schedule_id,
                "action" => "pause"
              }
            )

          assert extract_status_code(response) == 302
          assert extract_header(response, "location") =~ "/dashboard/flow/schedules?"

          assert {:ok, paused} = FerricStore.flow_schedule_get(schedule_id)
          assert paused.state == "paused"
        end
      end
    end
  end
end
