defmodule FerricstoreServer.Health.Dashboard.WorkersRedesignTest do
  use ExUnit.Case, async: true

  alias FerricstoreServer.Health.Dashboard.Render.FlowTables.Records

  test "expired leases lead the page with a link to Failures in scope" do
    html =
      Records.render_flow_workers_status(
        [worker("a", 5, 5), worker("b", 4, 0), worker("c", 1, 1)],
        %{type: "email.send"}
      )

    assert html =~ "workers-status is-warning"
    assert html =~ "<strong>6 expired leases</strong>"
    assert html =~ "on 2 of 3 workers"
    assert html =~ "10 leases held"
    assert html =~ ~s(href="/dashboard/flow/failures?type=email.send")
  end

  test "current leases and an empty sample read plainly" do
    assert Records.render_flow_workers_status([worker("a", 2, 0)], %{}) =~
             "<strong>1 worker holding 2 leases</strong>"

    assert Records.render_flow_workers_status([], %{}) =~
             "<strong>No leases held in this sample</strong>"
  end

  test "each worker row shows its leases as a bar with the expired share" do
    html = Records.render_flow_workers([worker("a", 4, 1), worker("b", 2, 0)], %{})

    headers =
      Regex.scan(~r/<th[^>]*>([^<]+)<\/th>/, html, capture: :all_but_first) |> List.flatten()

    assert headers == ["Worker", "Leases", "Status", "Oldest expired by"]

    assert html =~
             ~s(<span class="worker-lease-bar" style="width: 100%"><span class="is-expired" style="width: 25%"></span></span>)

    assert html =~
             ~s(<span class="worker-lease-bar" style="width: 50%"><span class="is-expired" style="width: 0%"></span></span>)

    assert html =~ ~s(<span class="badge badge-warning">1 expired</span>)
    assert html =~ ~s(<span class="badge badge-ok">Current</span>)
  end

  test "the order control sits beside the section title" do
    html = Records.render_flow_workers([worker("a", 1, 0)], %{})

    assert html =~
             ~r{<div class="section-head"><h2 class="section-title">Workers <span class="badge badge-idle">1</span></h2><form class="section-head-form"}
  end

  test "an empty worker list is a prompt, not an empty table" do
    html = Records.render_flow_workers([], %{})

    assert html =~ ~s(<p class="section-empty">No worker holds a lease in this sample.</p>)
    refute html =~ "<table"
  end

  test "leases say how long until, or since, they expire" do
    now = System.system_time(:millisecond)

    lease = fn expires ->
      Records.render_flow_running_records(
        [%{id: "r", type: "t", state: "running", worker: "w", lease_expires_at_ms: expires}],
        1,
        400
      )
    end

    assert lease.(now + 25_500) =~ ~r{<span class="flow-due-when">in 2\d\.\ds</span>}
    assert lease.(now - 18_500) =~ ~r{<span class="flow-due-when">expired 1\d\.\ds ago</span>}
  end

  defp worker(name, running, expired),
    do: %{
      worker: name,
      running: running,
      expired: expired,
      oldest_lease_ms: if(expired > 0, do: 18_000, else: 0)
    }
end
