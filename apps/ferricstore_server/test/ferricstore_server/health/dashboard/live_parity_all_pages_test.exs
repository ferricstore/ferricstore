defmodule FerricstoreServer.Health.Dashboard.LiveParityAllPagesTest do
  use FerricstoreServer.Test.DashboardCase

  alias FerricstoreServer.Health.Endpoint, as: HealthEndpoint

  # Every live page's poll must return each component the page rendered;
  # a component left out stays frozen while its neighbours update.
  @pages ~w(
    /dashboard /dashboard/flow /dashboard/flow/states /dashboard/flow/due
    /dashboard/flow/workers /dashboard/flow/signals /dashboard/flow/failures
    /dashboard/flow/schedules /dashboard/keyspace /dashboard/prefixes /dashboard/reads
    /dashboard/storage /dashboard/merge /dashboard/commands /dashboard/streams
    /dashboard/pubsub /dashboard/raft /dashboard/clients /dashboard/slowlog
  )

  setup do
    previous = Application.get_env(:ferricstore, :protected_mode)
    Application.put_env(:ferricstore, :protected_mode, false)
    on_exit(fn -> restore_env(:protected_mode, previous) end)
  end

  test "each live page's update carries every component it renders" do
    port = HealthEndpoint.port()

    checked =
      for path <- @pages,
          html = path |> then(&http_get(port, &1)) |> extract_body(),
          [_, live_url] <- [Regex.run(~r/data-dashboard-live-url="([^"]+)"/, html)],
          live_url != "" do
        rendered =
          ~r/data-live-component="([^"]+)"/
          |> Regex.scan(html, capture: :all_but_first)
          |> List.flatten()
          |> MapSet.new()

        payload =
          port
          |> http_get(String.replace(live_url, "&amp;", "&"))
          |> extract_body()
          |> Jason.decode!()

        missing = MapSet.difference(rendered, payload["components"] |> Map.keys() |> MapSet.new())
        {path, MapSet.size(rendered), MapSet.to_list(missing)}
      end

    # Most of these pages poll; a short list means the pages did not render.
    assert length(checked) >= 15
    assert Enum.all?(checked, fn {_path, rendered, _missing} -> rendered > 0 end)
    assert Enum.reject(checked, fn {_path, _rendered, missing} -> missing == [] end) == []
  end
end
