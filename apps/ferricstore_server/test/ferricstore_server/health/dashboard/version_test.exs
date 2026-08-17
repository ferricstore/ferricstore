defmodule FerricstoreServer.Health.Dashboard.VersionTest do
  use ExUnit.Case, async: true

  alias FerricstoreServer.Health.Dashboard.Data.Operational
  alias FerricstoreServer.Health.Dashboard.Render.Overview

  test "overview and footer use the running application version" do
    expected = :ferricstore_server |> Application.spec(:vsn) |> to_string()
    overview = Operational.collect_overview()

    assert overview.version == expected

    html =
      Overview.render_footer(%{
        overview: overview,
        hotcold: %{sample_rate: 100}
      })

    assert html =~ "v#{expected}"
    refute html =~ "v0.1.0"
  end
end
