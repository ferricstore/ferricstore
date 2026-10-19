defmodule FerricstoreServer.Health.Dashboard.FailuresRedesignTest do
  use FerricstoreServer.Test.DashboardCase

  alias FerricstoreServer.Health.Dashboard.Render.FlowComponents

  test "expired leases are grouped by type and partition, largest group first" do
    html = FlowComponents.render_flow_failures_table(data())
    groups = Regex.scan(~r/<details class="failure-group"/, html)

    assert length(groups) == 2
    assert position(html, "initech") < position(html, "acme")
    assert html =~ ~s(<span class="failure-group-count">3</span>)
    assert html =~ ~s(<span class="failure-group-count">1</span>)

    for id <- ["expired-1", "expired-2", "expired-3", "expired-4"] do
      assert html =~ ">#{id}</a>"
    end
  end

  test "a group reclaims only its own type and partition, after confirmation" do
    html = FlowComponents.render_flow_failures_table(data())
    initech = group(html, "initech")

    assert initech =~ ~s(<input type="hidden" name="action" value="reclaim">)
    assert initech =~ ~s(<input type="hidden" name="type" value="email.send">)
    assert initech =~ ~s(<input type="hidden" name="partition_key" value="initech">)
    assert initech =~ ~s(name="confirm_reclaim" value="true" required)
    assert initech =~ ~s(name="return_q" value="send")
    assert initech =~ "Reclaim up to 25 expired leases in email.send · initech"
    assert initech =~ "including ones this sample does not show"
    assert initech =~ "data-dashboard-single-submit"
  end

  test "a group without an exact partition says why it cannot be reclaimed" do
    data =
      put_in(data().candidates, [
        expired("loose-1", "email.send", nil)
      ])

    html = FlowComponents.render_flow_failures_table(data)

    # Only the general by-type form remains; the group itself offers no reclaim.
    assert length(Regex.scan(~r/name="confirm_reclaim"/, html)) == 1
    assert group(html, "loose-1") =~ "Reclaim needs an exact partition"
  end

  test "the general reclaim form is folded away until a type is chosen" do
    closed = FlowComponents.render_flow_recovery_actions(data())
    assert closed =~ ~s(<details class="flow-reclaim-advanced">)
    assert closed =~ ~s(id="flow-reclaim-form")
    assert closed =~ ~s(name="confirm_reclaim" value="true" required)

    open =
      FlowComponents.render_flow_recovery_actions(put_in(data().filters.type, "email.send"))

    assert open =~ ~s(<details class="flow-reclaim-advanced" open>)
  end

  test "failed and out-of-retry runs use compact rows" do
    html = FlowComponents.render_flow_failures_table(data())

    assert html =~ ~s(<table class="failure-runs failure-terminal">)
    assert html =~ ">failed-1</a>"
    assert html =~ "terminal failed"
    refute html =~ "<th>Runtime status</th>"
  end

  test "an empty sample says so in one line per section" do
    html = FlowComponents.render_flow_failures_table(%{data() | candidates: []})

    assert html =~ "No expired leases in this sample."
    assert html =~ "No failed or out-of-retry runs in this sample."
    refute html =~ "<details class=\"failure-group\""
  end

  defp data do
    %{
      filters: %{type: nil, partition_key: nil, q: "send", limit: 40, scan_exact: false},
      total_sampled: 102,
      sample_limit: 400,
      candidates: [
        expired("expired-1", "email.send", "initech"),
        expired("expired-2", "email.send", "initech"),
        expired("expired-3", "email.send", "initech"),
        expired("expired-4", "email.send", "acme"),
        %{
          id: "failed-1",
          type: "email.send",
          state: "failed",
          partition_key: "acme",
          attempts: 3,
          max_attempts: 3,
          updated_at_ms: System.system_time(:millisecond)
        }
      ]
    }
  end

  defp expired(id, type, partition) do
    now_ms = System.system_time(:millisecond)

    %{
      id: id,
      type: type,
      state: "running",
      partition_key: partition,
      worker: "worker-#{type}",
      attempts: 1,
      lease_expires_at_ms: now_ms - 120_000,
      updated_at_ms: now_ms - 150_000
    }
  end

  defp position(html, text), do: html |> :binary.match(text) |> elem(0)

  defp group(html, text) do
    html
    |> String.split(~s(<details class="failure-group"))
    |> Enum.drop(1)
    |> Enum.find(&(&1 =~ text))
  end
end
