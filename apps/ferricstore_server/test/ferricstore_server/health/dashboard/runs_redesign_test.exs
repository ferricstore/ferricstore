defmodule FerricstoreServer.Health.Dashboard.RunsRedesignTest do
  use ExUnit.Case, async: true

  alias FerricstoreServer.Health.Dashboard.Render.FlowTables.Records

  test "the states table has six columns, attention first, without help icons" do
    html =
      Records.render_flow_states_table([state("running", expired_leases: 9)], 102, 102, 400, %{})

    headers =
      Regex.scan(~r/<th[^>]*>([^<]+)<\/th>/, html, capture: :all_but_first) |> List.flatten()

    assert headers == ["Type", "State", "Attention", "Oldest due", "Runs", "Mode"]
    refute html =~ "info-icon"
    assert html =~ ~s(<span class="badge badge-warning">9 expired</span>)
    assert html =~ "does not establish claimability"
  end

  test "attention lists each kind of problem, and falls back to the state's hint" do
    html =
      Records.render_flow_states_table(
        [
          state("queued", failed: 2, max_attempts_reached: 1, retrying: 3),
          state("queued", due_now: 4)
        ],
        10,
        10,
        400,
        %{}
      )

    assert html =~ ~s(<span class="badge badge-failed">2 failed</span>)
    assert html =~ ~s(<span class="badge badge-failed">1 out of retries</span>)
    assert html =~ ~s(<span class="badge badge-retrying">3 retrying</span>)
    assert html =~ "Due time reached"
  end

  test "the overview names what needs attention and shows the state split" do
    html =
      Records.render_flow_states_overview(
        [
          state("running", count: 31, running: 31, expired_leases: 31),
          state("queued", count: 70, due_now: 70),
          state("completed", count: 1)
        ],
        102,
        :ok,
        %{type: "email.send"}
      )

    assert html =~ "runs-status is-warning"
    assert html =~ "<strong>Needs attention</strong>"
    assert html =~ "31 with expired leases"
    # The kinds overlap (a failed run can also be out of retries), so no total is claimed.
    refute html =~ "runs need attention"
    assert html =~ ~s(href="/dashboard/flow/failures?type=email.send")
    assert html =~ ~s(style="flex: 70")
    assert html =~ ~s(style="flex: 31")
    assert html =~ "70 queued, 31 running, 1 completed"
  end

  test "a clean sample says so, and an unavailable one says nothing" do
    clean = Records.render_flow_states_overview([state("queued", count: 5)], 5, :ok, %{})
    assert clean =~ "runs-status is-healthy"
    assert clean =~ "No runs need attention in this sample of 5"

    assert Records.render_flow_states_overview([], 0, :unavailable, %{}) == ""
  end

  defp state(name, overrides) do
    Map.merge(
      %{
        type: "email.send",
        state: name,
        count: 9,
        failed: 0,
        expired_leases: 0,
        max_attempts_reached: 0,
        retrying: 0,
        due_now: 0,
        running: 0,
        oldest_due_ms: 0,
        mode: :parallel
      },
      Map.new(overrides)
    )
  end
end
