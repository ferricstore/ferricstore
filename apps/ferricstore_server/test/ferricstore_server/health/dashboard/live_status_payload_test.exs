defmodule FerricstoreServer.Health.Dashboard.LiveStatusPayloadTest do
  use FerricstoreServer.Test.DashboardCase

  alias FerricstoreServer.Health.Dashboard

  # A component missing from the live payload freezes on the page while its
  # neighbours update, so a status line could contradict the tables below it.
  for {path, collect, render, status} <- [
        {"flow/states", :collect_flow_states_page, :render_flow_states_page,
         "flow_states_overview"},
        {"flow/due", :collect_flow_due_page, :render_flow_due_page, "flow_due_summary"},
        {"flow/workers", :collect_flow_workers_page, :render_flow_workers_page,
         "flow_workers_status"}
      ] do
    test "#{path} live updates carry every component the page renders, its status line included" do
      html = apply(Dashboard, unquote(render), [apply(Dashboard, unquote(collect), [])])

      rendered =
        ~r/data-live-component="([^"]+)"/
        |> Regex.scan(html, capture: :all_but_first)
        |> List.flatten()
        |> MapSet.new()

      assert unquote(status) in rendered
      assert {:ok, %{components: components}} = Dashboard.live_payload(unquote(path))
      assert MapSet.subset?(rendered, components |> Map.keys() |> MapSet.new())
    end
  end
end
