defmodule FerricstoreServer.Health.Dashboard.FlowProjectionTest do
  use ExUnit.Case, async: true

  alias FerricstoreServer.Health.Dashboard.Render.FlowTables.Projection

  test "degraded projection rows are not counted as persistence failures" do
    metrics = [
      %{
        name: ~s(ferricstore_flow_lmdb_degraded{shard_index="0"}),
        value: "1"
      }
    ]

    assert %{
             health: "degraded",
             degraded: 1,
             failures: 0
           } = Projection.flow_projection_rollup(metrics)
  end
end
