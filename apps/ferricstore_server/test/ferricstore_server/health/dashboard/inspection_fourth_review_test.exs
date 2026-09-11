defmodule FerricstoreServer.Health.Dashboard.InspectionFourthReviewTest do
  use ExUnit.Case, async: true

  alias FerricstoreServer.Health.Dashboard.Layout.Styles

  alias FerricstoreServer.Health.Dashboard.Render.{
    FlowDetail,
    FlowFilters,
    FlowGovernance,
    FlowHistory,
    FlowComponents,
    KVPages
  }

  test "relationships route parents by their own partition and lineage by the inspected partition" do
    html = FlowDetail.render_flow_detail_table(record())
    assert html =~ ~s(href="/dashboard/flow/parent%3C42%3E?partition_key=parent%2Fpartition")
    assert html =~ "mode=root"
    assert html =~ "id=root%3C42%3E"
    assert html =~ "mode=correlation"
    assert html =~ "id=correlation%3C42%3E"
    assert html =~ "partition_key=child%2Fpartition"
    refute html =~ ~s(href="/dashboard/flow/root%3C42%3E?partition_key=child%2Fpartition")
    refute html =~ "parent<42>"
  end

  test "unknown parent and approval partitions require explicit lookup instead of scope guessing" do
    html = FlowDetail.render_flow_detail_table(Map.delete(record(), :parent_partition_key))
    assert html =~ "Look up workflow"
    assert html =~ ~s(name="id" value="parent&lt;42&gt;")
    assert html =~ ~s(name="partition_key" required)
    refute html =~ ~s(href="/dashboard/flow/parent%3C42%3E?partition_key=child%2Fpartition")

    approval = %{
      id: "approval",
      flow_id: "approved<42>",
      scope: "not-a-partition",
      status: :approved
    }

    approval_html = FlowGovernance.render_flow_governance_approvals([approval])
    assert approval_html =~ ~s(name="id" value="approved&lt;42&gt;")
    assert approval_html =~ ~s(name="partition_key" required)
    refute approval_html =~ "partition_key=not-a-partition"
  end

  test "signal names are escaped and visible in the journal trigger" do
    html =
      FlowHistory.render_flow_history_timeline(
        [{"1000-0", %{event: "signaled", signal: "paid<script>", state: "ready"}}],
        :ok,
        nil
      )

    [_, trigger] =
      Regex.run(
        ~r/<div class="journal-step-trigger".*?<div class="journal-step-meta">(.*?)<\/div>/s,
        html
      )

    assert trigger =~ "paid&lt;script&gt;"
    refute html =~ "paid<script>"

    empty =
      FlowHistory.render_flow_history_timeline(
        [{"1000-0", %{event: "signaled", state: "ready"}}],
        :ok,
        nil
      )

    refute empty =~ "journal-signal-name"
  end

  test "detail metadata overflow is expandable without a value fetch" do
    html = FlowDetail.render_flow_state_meta_badges(record())
    assert html =~ ~s(<details class="flow-metadata-overflow dashboard-disclosure")
    assert html =~ "Show 17 more entries"
    assert html =~ "z_match.risk=&lt;high&gt;"
    refute html =~ "data-flow-value-ref"
    refute html =~ "<high>"
    assert byte_size(html) < 20_000
  end

  test "large signal names have a bounded UTF-8 journal summary" do
    html =
      FlowHistory.render_flow_history_timeline(
        [
          {"1000-0",
           %{event: "signaled", signal: String.duplicate("\u00e9", 100_000), state: "ready"}}
        ],
        :ok,
        nil
      )

    [_, summary] = Regex.run(~r/<span class="journal-signal-name mono">(.*?)<\/span>/s, html)
    assert byte_size(summary) < 512
    assert String.valid?(summary)
    assert summary =~ "truncated"
  end

  test "governance keeps the matching metadata visible and links to full detail without expanding all rows" do
    html =
      FlowGovernance.render_flow_governance_state_meta(%{
        filters: %{meta_state: "z_match", meta_key: "risk"},
        state_meta_result: %{status: :ok, rows: [record()]}
      })

    assert html =~ ~s(<span class="badge badge-ok">z_match.risk=&lt;high&gt;</span>)
    assert html =~ "Inspect all 49 entries"
    assert html =~ "partition_key=child%2Fpartition#workflow-data"
    refute html =~ "Show 17 more entries"
    assert length(Regex.scan(~r/class="badge badge-(?:idle|ok)"/, html)) <= 33
  end

  test "empty metadata remains a true empty state" do
    html = FlowDetail.render_flow_state_meta_badges(%{state_meta: %{}})
    assert html =~ ">none</span>"
    refute html =~ "details"
  end

  test "recovery guidance names rewind rather than promising a retry control" do
    html =
      FlowComponents.render_flow_recovery_actions(%{
        filters: %{q: nil, type: nil, partition_key: nil, limit: 40, scan_exact: false}
      })

    refute html =~ "retried or rewound"
    assert html =~ "rewound from the Flow detail page"
  end

  test "states identities and keyboard table focus use readable shared styling" do
    css = Styles.stylesheet()
    assert css =~ ".flow-states-table th:nth-child(2) { width: 240px; }"
    assert css =~ ".table-scroll:focus-visible { outline: 2px solid var(--accent)"
    refute css =~ ".table-scroll:focus { outline: 2px solid rgba"
    assert css =~ ".flow-alert code { overflow-wrap: anywhere;"
  end

  test "no-JavaScript time fields are usable before submitting the mode" do
    html = FlowFilters.render_flow_type_filter(%{})

    for name <- ["range", "from", "to"] do
      [field] = Regex.run(~r/<(?:input|select)[^>]*name="#{name}"[^>]*>/, html)
      refute field =~ "disabled"
    end

    refute html =~ ~s(data-flow-time-mode="custom" hidden)
    assert html =~ "Time mode controls which bounds are applied"
  end

  test "keyspace distinguishes visible results from scanned entries and active exclusions" do
    html =
      KVPages.render_keyspace_table(%{
        rows: [],
        searched?: true,
        total_sampled: 0,
        scanned_count: 243,
        filters: %{include_internal: false, mode: "prefix"}
      })

    assert html =~ "0 keys returned"
    assert html =~ "243 entries scanned"
    assert html =~ "Compound metadata is excluded"
    assert html =~ "Protected workflow and server records remain hidden"

    restricted =
      KVPages.render_keyspace_table(%{
        rows: [],
        searched?: true,
        total_sampled: 0,
        scanned_count: nil,
        filters: %{include_internal: false}
      })

    refute restricted =~ "entries scanned"
  end

  defp record do
    metadata =
      for state <- ["a", "b", "c"], into: %{}, do: {state, Map.new(1..16, &{"key#{&1}", &1})}

    %{
      id: "child<42>",
      type: "orders",
      state: "ready",
      partition_key: "child/partition",
      parent_flow_id: "parent<42>",
      parent_partition_key: "parent/partition",
      root_flow_id: "root<42>",
      correlation_id: "correlation<42>",
      state_meta: Map.put(metadata, "z_match", %{"risk" => "<high>"})
    }
  end
end
