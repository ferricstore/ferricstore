defmodule FerricstoreServer.Health.Dashboard.FlowLifecycleTest do
  use ExUnit.Case, async: true

  alias FerricstoreServer.Health.Dashboard.Render.{FlowDetail, FlowHistory}

  test "the path lists each state the run entered, linked to its journal entry" do
    html =
      FlowDetail.render_flow_lifecycle(
        data("completed", [
          {"1000-1", %{"event" => "created", "state" => "queued"}},
          {"2000-1", %{"event" => "claimed", "state" => "running"}},
          {"3000-1", %{"event" => "completed", "state" => "completed"}}
        ])
      )

    assert states(html) == ["queued", "running", "completed"]

    for id <- ["1000-1", "2000-1", "3000-1"] do
      assert html =~ ~s(href="#journal-#{FlowHistory.flow_history_event_anchor(id)}")
    end

    assert html =~ ~r/class="flow-lifecycle-step is-completed is-current"/
    assert length(Regex.scan(~r/is-current/, html)) == 1
  end

  test "an expired lease is the current step, with a way to recovery" do
    now = System.system_time(:millisecond)

    html =
      FlowDetail.render_flow_lifecycle(
        data(
          "running",
          [
            {"1000-1", %{"event" => "created", "state" => "queued"}},
            {"2000-1", %{"event" => "claimed", "state" => "running"}}
          ],
          lease_expires_at_ms: now - 10_000
        )
      )

    assert states(html) == ["queued", "running", "lease expired"]
    assert html =~ ~r/class="flow-lifecycle-step is-expired is-current"/
    assert html =~ ~s(href="/dashboard/flow/failures?partition_key=initech&amp;type=email.send")
  end

  test "repeated visits keep their order" do
    html =
      FlowDetail.render_flow_lifecycle(
        data("running", [
          {"1000-1", %{"event" => "created", "state" => "queued"}},
          {"2000-1", %{"event" => "claimed", "state" => "running"}},
          {"3000-1", %{"event" => "retry", "state" => "queued"}},
          {"4000-1", %{"event" => "claimed", "state" => "running"}}
        ])
      )

    assert states(html) == ["queued", "running", "queued", "running"]
  end

  test "a long path keeps the latest steps and counts the earlier ones" do
    history =
      for n <- 1..12 do
        {"#{n * 1000}-1", %{"event" => "transition", "state" => "step_#{n}"}}
      end

    html = FlowDetail.render_flow_lifecycle(data("step_12", history))

    assert length(states(html)) == 8
    assert hd(states(html)) == "step_5"
    assert html =~ "+4 earlier"
  end

  test "a run with no recorded state changes says so" do
    html = FlowDetail.render_flow_lifecycle(data("queued", []))
    assert html =~ "No state changes recorded yet."
  end

  test "the lifecycle is its own live component at the top of the execution section" do
    html = FerricstoreServer.Health.Dashboard.Templates.flow_detail(%{data: page_data()})

    assert html =~ ~s(<h1 class="page-title">email.send-41</h1>)
    lifecycle = :binary.match(html, ~s(data-live-component="flow_lifecycle")) |> elem(0)
    chart = :binary.match(html, ~s(data-live-component="flow_timeline_chart")) |> elem(0)
    assert lifecycle < chart
  end

  defp states(html) do
    Regex.scan(~r/<strong class="flow-lifecycle-state">([^<]+)<\/strong>/, html,
      capture: :all_but_first
    )
    |> List.flatten()
  end

  defp data(state, history, extra \\ []) do
    %{
      record:
        Map.merge(
          %{
            id: "email.send-41",
            type: "email.send",
            partition_key: "initech",
            state: state,
            worker: "worker-a"
          },
          Map.new(extra)
        ),
      history: history
    }
  end

  defp page_data do
    data("queued", [{"1000-1", %{"event" => "created", "state" => "queued"}}])
    |> Map.merge(%{
      id: "email.send-41",
      partition_key: "initech",
      waiting_reason: "waiting in queued",
      state_mode: :parallel,
      fifo_lane: nil,
      history_status: :ok,
      history_page: nil,
      flash: nil
    })
  end
end
