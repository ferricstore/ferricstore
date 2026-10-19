defmodule FerricstoreServer.Health.Dashboard.SchedulesRedesignTest do
  use ExUnit.Case, async: true

  alias FerricstoreServer.Health.Dashboard.Render.FlowSchedules

  test "cadence reads in plain words" do
    interval = table([schedule(kind: :interval, every_ms: 300_000)])
    assert interval =~ ~s(<td class="flow-schedule-cadence">Every 5 min</td>)

    cron = table([schedule(kind: :cron, cron: "0 * * * *", timezone: "Europe/Berlin")])
    assert cron =~ ~s(Cron <code>0 * * * *</code> · Europe/Berlin)

    once = table([schedule(kind: :once, delay_ms: 90_000)])
    assert once =~ ~s(<td class="flow-schedule-cadence">Once, 1 min 30 s after creation</td>)
  end

  test "next and last runs are relative, with the exact time beside them" do
    now = System.system_time(:millisecond)

    html =
      table([
        schedule(
          next_run_at_ms: now + 240_500,
          last_fire_at_ms: now - 61_000,
          last_target_id: "cache-warm:1:1"
        )
      ])

    assert html =~ "in 4 min"
    assert html =~ "1 min ago"
    assert html =~ ">cache-warm:1:1</a>"
  end

  test "run now and pause sit on the row; edit and delete are under More" do
    html = table([schedule()])

    assert html =~ "Confirm Run now"
    assert html =~ ~s(name="action" value="pause")
    [_, menu] = Regex.run(~r{<details class="flow-row-menu">(.*?)</div>\s*</details>}s, html)
    assert menu =~ ">Edit</a>"
    assert menu =~ "Confirm Delete"
    assert html =~ ~s(aria-label="More actions for daily")
  end

  test "the table has eight columns and the rest of the definition folds below" do
    html =
      table([
        schedule(
          catchup_policy: :fire_once,
          coalesced_count: 5,
          last_coalesced_count: 3,
          max_fires: 10
        )
      ])

    assert length(Regex.scan(~r/<th scope="col">/, html)) == 8
    assert html =~ ~s(<td colspan="8">)
    [_, definition] = Regex.run(~r{<tr class="flow-schedule-definition-row">(.*?)</tr>}s, html)
    assert definition =~ "<dt>Catch-up</dt>"
    assert definition =~ "5 coalesced"
    assert definition =~ "<dt>Ends</dt>"
    assert definition =~ "max 10"
  end

  defp table(schedules), do: FlowSchedules.render_flow_schedules_table(schedules, %{})

  defp schedule(overrides \\ []) do
    Map.merge(
      %{
        id: "daily",
        state: "active",
        version: 1,
        kind: :interval,
        every_ms: 86_400_000,
        target: %{type: "schedule.daily", partition_key: "ops"},
        fire_count: 1,
        overlap_policy: :allow,
        action_capabilities: %{fire: true, pause: true, resume: true, delete: true, edit: true}
      },
      Map.new(overrides)
    )
  end
end
