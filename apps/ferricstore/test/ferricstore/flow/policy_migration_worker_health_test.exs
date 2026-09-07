defmodule Ferricstore.Flow.PolicyMigrationWorkerHealthTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Flow.PolicyMigrationWorker

  test "publishes and clears bounded typed shard failures" do
    name = :"policy_migration_health_#{System.unique_integer([:positive])}"
    ctx = %{name: name, shard_count: 1}
    {:ok, result} = Agent.start_link(fn -> {:error, :policy_catalog_state_projection_pending} end)

    assert {:ok, pid} =
             PolicyMigrationWorker.start_link(
               instance_ctx: ctx,
               name: name,
               enabled: true,
               initial_delay_ms: 60_000,
               interval_ms: 60_000,
               catchup_delay_ms: 60_000,
               shards_per_run: 1,
               attribute_repair_fun: fn _ctx, _shard -> Agent.get(result, & &1) end
             )

    send(pid, :run)
    :sys.get_state(pid)

    assert %{
             status: :warning,
             issues: [
               %{
                 shard: 0,
                 reason: :policy_catalog_state_projection_pending,
                 occurrences: 1
               }
             ]
           } = PolicyMigrationWorker.health_snapshot(name)

    Agent.update(result, fn _ -> {:ok, %{processed: 1}} end)
    send(pid, :run)
    :sys.get_state(pid)

    assert %{status: :healthy, issues: [], updated_at_ms: published_at_ms} =
             PolicyMigrationWorker.health_snapshot(name)

    Process.sleep(2)
    send(pid, :run)
    :sys.get_state(pid)

    assert %{status: :healthy, issues: [], updated_at_ms: ^published_at_ms} =
             PolicyMigrationWorker.health_snapshot(name)
  end
end
