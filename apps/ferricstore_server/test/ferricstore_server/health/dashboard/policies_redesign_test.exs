defmodule FerricstoreServer.Health.Dashboard.PoliciesRedesignTest do
  use ExUnit.Case, async: true

  alias FerricstoreServer.Health.Dashboard.Layout.Styles
  alias FerricstoreServer.Health.Dashboard.Render.FlowPolicy

  test "the table has nine columns, with generation and indexes under the type" do
    html = FlowPolicy.render_flow_policies_table([policy("email.send", generation: 4)], %{})

    headers =
      Regex.scan(~r/<th[^>]*>([^<]+)<\/th>/, html, capture: :all_but_first) |> List.flatten()

    assert headers == [
             "Type",
             "Source",
             "Retries",
             "Backoff",
             "When exhausted",
             "Max active",
             "Retention",
             "State overrides",
             "Actions"
           ]

    assert html =~ ~s(<span class="flow-run-secondary">generation 4 · no indexes</span>)
  end

  test "backoff and retention read compactly" do
    html = FlowPolicy.render_flow_policies_table([policy("email.send")], %{})

    assert html =~ "<td>exponential 1.0s → 30.0s ±20%</td>"
    assert html =~ "<td>7.0d · 1,000 events</td>"
  end

  test "a shared policy is stated once above the table" do
    same = FlowPolicy.render_flow_policy_summary([policy("a"), policy("b"), policy("c")])
    assert same =~ "policies-status is-idle"
    assert same =~ "<strong>3 types share one policy</strong>"
    assert same =~ "3 retries, exponential 1.0s → 30.0s ±20%, then failed"

    mixed =
      FlowPolicy.render_flow_policy_summary([
        policy("a"),
        policy("b", retry: %{max_retries: 9, backoff: %{kind: :none}, exhausted_to: "dead"})
      ])

    assert mixed =~ "<strong>2 types, 2 different policies</strong>"
    assert FlowPolicy.render_flow_policy_summary([]) == ""
  end

  test "types with different state overrides do not share one policy" do
    override = %{state: "review", mode: :fifo, retry: %{max_retries: 1}, retention: %{}}

    html =
      FlowPolicy.render_flow_policy_summary([policy("a"), policy("b", states: [override])])

    assert html =~ "<strong>2 types, 2 different policies</strong>"
  end

  test "types whose policy failed to load are counted apart" do
    failed = %{type: "c", source: "configured", error: "timeout"}
    html = FlowPolicy.render_flow_policy_summary([policy("a"), policy("b"), failed])

    assert html =~ "<strong>2 types share one policy</strong>"
    assert html =~ "1 type could not be loaded"

    none = FlowPolicy.render_flow_policy_summary([failed])
    assert none =~ "policies-status is-warning"
    assert none =~ "<strong>Policies unavailable</strong>"
  end

  test "the actions column fits its button instead of the schedule column width" do
    css = Styles.stylesheet()

    assert css =~
             ".flow-policy-table tr > :last-child:not([colspan]) { min-width: 96px; width: 96px; }"
  end

  test "summaries stay on one line, index names fit, and the row filter is laid out" do
    css = Styles.stylesheet()

    assert css =~
             ".flow-policy-table td:nth-child(4), .flow-policy-table td:nth-child(7) { white-space: nowrap; }"

    assert css =~ ".flow-index-identity { width: 44ch;"
    assert css =~ ".flow-filter-row { display: flex;"
  end

  defp policy(type, overrides \\ []) do
    Map.merge(
      %{
        type: type,
        source: "configured",
        generation: 0,
        retry: %{
          max_retries: 3,
          backoff: %{kind: :exponential, base_ms: 1_000, max_ms: 30_000, jitter_pct: 20},
          exhausted_to: "failed"
        },
        retention: %{ttl_ms: 604_800_000, history_max_events: 1_000},
        states: []
      },
      Map.new(overrides)
    )
  end
end
