defmodule FerricstoreServer.Health.Dashboard.ExtremeTimestampsTest do
  use FerricstoreServer.Test.DashboardCase

  alias FerricstoreServer.Health.Dashboard
  alias FerricstoreServer.Health.Dashboard.Format

  # Past year 9999, DateTime.from_unix!/2 raises; a run or schedule carrying
  # such a time must not take a dashboard page down.
  @far 300_000_000_000_000

  test "clock formatting falls back instead of raising" do
    assert Format.format_clock_ms(@far) == "-"
  end

  test "a run scheduled past year 9999 does not break the run pages" do
    type = "far-future-#{System.unique_integer([:positive])}"

    case FerricStore.flow_create("#{type}-1",
           type: type,
           state: "queued",
           partition_key: "p",
           run_at_ms: @far
         ) do
      :ok ->
        assert Dashboard.render_flow_due_page(Dashboard.collect_flow_due_page(type: type)) =~ type

        assert Dashboard.render_flow_states_page(Dashboard.collect_flow_states_page(type: type)) =~
                 type

      {:error, _reason} ->
        # The store refuses such times; the formatter test above still guards the pages.
        :ok
    end
  end

  test "a schedule whose next run is past year 9999 still renders" do
    schedule = %{
      id: "far",
      state: "active",
      kind: :interval,
      every_ms: 60_000,
      next_run_at_ms: @far,
      last_fire_at_ms: @far,
      target: %{type: "t"},
      action_capabilities: %{fire: true, pause: true, resume: true, delete: true, edit: true}
    }

    html =
      FerricstoreServer.Health.Dashboard.Render.FlowSchedules.render_flow_schedules_table(
        [schedule],
        %{}
      )

    assert html =~ "far"
  end
end
