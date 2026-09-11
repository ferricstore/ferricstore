alias FerricstoreServer.Health.Dashboard.Flow.QueryVisualization
alias FerricstoreServer.Health.Dashboard.Layout
alias FerricstoreServer.Health.Dashboard.Render.{FlowQueryExport, FlowQueryResults}

projected = fn rows, selectors, columns ->
  %{status: :ok, presentation: :workbench, source: :runs, rows: rows, column_selectors: selectors, columns: columns}
end

category = fn values ->
  values
  |> Enum.map(&%{attributes: %{"risk" => &1}})
  |> projected.([{:attribute, "risk"}], ["attribute.risk"])
  |> QueryVisualization.attach()
  |> FlowQueryResults.render_flow_query_visualization()
end

json = Map.new(1..49, &{"key#{&1}", %{"number" => &1, "text" => "full value #{&1}"}})
result = projected.([%{id: "query-json", state_meta: json}], [:state_meta], ["state_meta"])
structured = FlowQueryResults.render_flow_query_table(result) <> FlowQueryExport.render(result)
ticks = for maximum <- [1, 3, 5], into: %{} do
  chart = %{kind: :time, field: "updated_at_ms", values: [%{from_ms: 1, to_ms: 2, count: maximum}]}
  html = FlowQueryResults.render_flow_query_visualization(%{visualization: %{scope: :current_page, row_count: maximum + 1, charts: [chart]}})
  {maximum, html}
end

IO.write(Jason.encode!(%{
  css: Layout.Styles.stylesheet(),
  typed: category.([1, "1", true, "true", 1.0]),
  empty: category.(["high", "", nil, %{"nested" => true}]),
  remainder: category.(List.duplicate("Other", 3) ++ Enum.map(1..13, &"value-#{&1}")),
  binary: category.([<<255, 0>>, "Base64 /wA="]),
  structured: structured,
  expected_json: json,
  ticks: ticks
}))
