defmodule FerricstoreServer.Health.Dashboard.DetailEvidenceReviewTest do
  use ExUnit.Case, async: true

  alias FerricstoreServer.Health.Dashboard.Flow.ActionForm
  alias FerricstoreServer.Health.Dashboard.Render.{FlowCharts, FlowDetail, FlowHistory}

  @time 1_783_627_200_000

  test "each action repeats the escaped workflow snapshot beside the review" do
    html = FlowDetail.render_flow_actions(data())
    assert length(Regex.scan(~r/class="flow-action-target"/, html)) == 2
    assert html =~ "flow&lt;42&gt;"
    assert html =~ "type&lt;orders&gt;"
    assert html =~ "partition&lt;west&gt;"
    assert html =~ "<dt>State</dt><dd>ready&lt;review&gt;</dd>"
    assert html =~ "<dt>Version</dt><dd>9</dd>"
    assert html =~ "data-flow-signal-review"
  end

  test "rewind requires a deliberate readable event selection and explicit schedule mode" do
    html = FlowDetail.render_flow_rewind_action(data())
    assert html =~ ~s(name="to_event" required)
    assert html =~ ~s(<option value="" selected disabled>Choose a target event</option>)
    assert html =~ "2026-07-09"
    assert html =~ "Created"
    assert html =~ "queued&lt;old&gt;"
    assert html =~ ~s(name="schedule_mode")
    assert html =~ "Keep event schedule"
    assert html =~ "Run now"
    assert html =~ "UTC date and time"
    assert html =~ "data-flow-rewind-target-review"
    assert html =~ "data-flow-rewind-schedule-review"
    assert html =~ ~s(data-flow-event-schedule="2026-07-09 20:00:00.000 UTC")
    refute html =~ "Run at ms"
    refute html =~ ~s(placeholder="current")
  end

  test "schedule choices resolve without silently interpreting local time or stale fields" do
    assert {:ok, nil} =
             ActionForm.resolve_schedule(
               %{"schedule_mode" => "keep", "run_at_utc" => "bad"},
               @time
             )

    assert {:ok, @time} = ActionForm.resolve_schedule(%{"schedule_mode" => "now"}, @time)

    assert {:ok, @time} =
             ActionForm.resolve_schedule(
               %{"schedule_mode" => "at", "run_at_utc" => "2026-07-09T20:00"},
               @time
             )

    assert {:error, _} =
             ActionForm.resolve_schedule(
               %{"schedule_mode" => "at", "run_at_utc" => "bad<script>"},
               @time
             )

    assert {:error, _} = ActionForm.resolve_schedule(%{"schedule_mode" => "unknown"}, @time)

    assert {:error, _} =
             ActionForm.resolve_schedule(
               %{"schedule_mode" => "at", "run_at_utc" => "2026-07-09T18:40+03:00"},
               @time
             )
  end

  test "selected and raw event inspectors expose signal identity and bounded expanded diagnostics" do
    error = String.duplicate("failure ", 100) <> "TAIL<recover>"

    fields = %{
      event: "signaled",
      state: "ready",
      signal: "paid<script>",
      idempotency_key: "key<42>",
      error: error
    }

    html = FlowHistory.render_flow_history_timeline([{"123-4", fields}], :ok, nil)
    assert length(Regex.scan(~r/<dt>Signal name<\/dt>/, html)) == 2
    assert length(Regex.scan(~r/<dt>Idempotency key<\/dt>/, html)) == 2
    assert html =~ "paid&lt;script&gt;"
    assert html =~ "key&lt;42&gt;"
    assert html =~ "TAIL&lt;recover&gt;"
    assert html =~ "flow-history-full-detail"
    refute html =~ "paid<script>"

    huge =
      FlowHistory.render_flow_history_timeline(
        [{"124-4", %{fields | error: String.duplicate("x", 20_000) <> "OUTSIDE_BOUND"}}],
        :ok,
        nil
      )

    assert huge =~ "Error preview limited to 8 KiB"
    refute huge =~ "OUTSIDE_BOUND"
  end

  test "timing is labeled as event intervals including intervening signals" do
    history = [
      {"1000-0", %{event: "created", state: "queued"}},
      {"2000-0", %{event: "signaled", state: "queued"}},
      {"4000-0", %{event: "claimed", state: "running"}}
    ]

    html = FlowCharts.render_flow_timeline_chart(history)
    assert html =~ "Event intervals"
    assert html =~ "To next event"
    assert html =~ "No next event"
    refute html =~ "Step Waterfall"
    refute html =~ "duration "
  end

  test "many hostile inline diagnostics share a bounded page budget without raw-event duplication" do
    for count <- [50, 250] do
      history =
        for n <- 1..count,
            do:
              {"#{n}-0",
               %{
                 event: "failed",
                 error: String.duplicate("<&", 20_000),
                 reason: String.duplicate("<&", 20_000)
               }}

      html = FlowHistory.render_flow_history_timeline(history, :ok, nil)
      assert html =~ ~s(data-flow-history-detail-byte-budget="65536")
      assert html =~ "64 KiB page budget"
      assert html =~ "Inspect error and reason"
      assert byte_size(html) < 2_000_000
      assert length(Regex.scan(~r/class="flow-history-full-detail"/, html)) == count * 2

      expanded_bytes =
        Regex.scan(~r/<details class="flow-history-full-detail">.*?<pre>(.*?)<\/pre>/s, html,
          capture: :all_but_first
        )
        |> Enum.reduce(0, fn [value], bytes -> bytes + byte_size(value) end)

      assert expanded_bytes <= 65_536 * 6
    end
  end

  test "current and historical links preserve distinct provenance even for the same reference" do
    current =
      FlowHistory.render_flow_value_ref_badges(%{data().record | payload_ref: "same<ref>"})

    historical =
      FlowHistory.render_flow_history_timeline(
        [
          {"historic<event>",
           %{event: "signaled", at: @time, state: "old", payload_ref: "same<ref>"}}
        ],
        :ok,
        nil
      )

    assert current =~ ~s(data-flow-value-source="current")
    current_anchor = FlowHistory.flow_value_ref_anchor("same<ref>")

    historical_anchor =
      current_anchor <> ":event:" <> Base.url_encode64("historic<event>", padding: false)

    assert current =~ ~s(href="##{current_anchor}")
    assert historical =~ ~s(href="##{historical_anchor}")
    refute historical =~ ~s(href="##{current_anchor}")
    assert current =~ ~s(data-flow-value-workflow="flow&lt;42&gt;")
    assert current =~ ~s(data-flow-value-partition="partition&lt;west&gt;")
    assert historical =~ ~s(data-flow-value-source="historical")
    assert historical =~ ~s(data-flow-value-event="historic&lt;event&gt;")
    assert historical =~ ~s(data-flow-value-action="Signaled")
    assert historical =~ ~s(data-flow-value-time="2026-07-09)
    refute historical =~ ~s(data-flow-value-source="current")
    assert FlowDetail.render_flow_value_modal() =~ ~s(id="flow-value-modal-provenance")
  end

  defp data do
    %{
      record: %{
        id: "flow<42>",
        type: "type<orders>",
        partition_key: "partition<west>",
        state: "ready<review>",
        version: 9,
        payload_ref: nil
      },
      history: [
        {"#{@time}-0",
         %{event: "created", state: "queued<old>", at: @time, next_run_at_ms: to_string(@time)}}
      ]
    }
  end
end
