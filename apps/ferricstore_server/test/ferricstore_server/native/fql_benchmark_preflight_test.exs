defmodule FerricstoreServer.Native.FQLBenchmarkPreflightTest do
  use ExUnit.Case, async: false

  @support Path.expand("../../../../../bench/support/fql_planner_context.exs", __DIR__)

  test "benchmark planner context executes the production explain path" do
    Code.require_file(@support)
    {ctx, admission} = apply(Ferricstore.Bench.FQLPlannerContext, :start!, [])
    on_exit(fn -> if Process.alive?(admission), do: GenServer.stop(admission) end)

    assert {:ok,
            %{
              status: "planned",
              version: "ferric.flow.explain/v1",
              plan: %{path: "primary_key"}
            }} =
             FerricstoreServer.Native.FlowQuery.execute(
               ctx,
               "FQL1",
               "EXPLAIN FROM runs WHERE partition_key = @partition AND run_id = @flow_id RETURN RECORD",
               %{"partition" => "tenant-a", "flow_id" => "run-123"}
             )
  end
end
